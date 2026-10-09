//! Decode one direct buy from instruction limits, without inventing execution balances.
use solana_sdk::pubkey::Pubkey;

use super::{source_route, transaction::DecodeContext};
use crate::{
    domain::{AssetId, DexKind, ObservedTransaction, TradeIntent},
    error::{CopyTraderError, Result},
    routing::pump_fun,
    token::accounts::associated_token_address,
};

pub(super) fn decode(observed: &ObservedTransaction, wallet: Pubkey) -> Result<TradeIntent> {
    let defer =
        || CopyTraderError::Unsupported("preconfirmation requires processed metadata".into());
    let context = DecodeContext::new(observed)?;
    source_route::require_pump_trade_with_context(&context)?;
    let message = &observed.transaction.message;
    let wallet_index = message
        .static_account_keys()
        .iter()
        .position(|key| *key == wallet)
        .ok_or_else(defer)?;
    if !message.is_signer(wallet_index) || observed.meta.err.is_some() {
        return Err(defer());
    }
    // Unknown programs may invoke swaps through CPI. Never infer a single trade
    // from a transaction whose remaining instructions we cannot classify.
    let allowed = [
        Pubkey::default(),
        spl_token::id(),
        spl_token_2022::id(),
        spl_associated_token_account::id(),
        Pubkey::from_str_const("ComputeBudget111111111111111111111111111111"),
        pump_fun::PROGRAM_ID,
        DexKind::PumpSwap.program_id(),
    ];
    if context.instructions.iter().any(|ix| {
        context
            .keys
            .get(ix.program_id_index as usize)
            .is_none_or(|key| !allowed.contains(key))
    }) {
        return Err(defer());
    }
    let ix = context
        .instructions
        .iter()
        .find(|ix| {
            context
                .keys
                .get(ix.program_id_index as usize)
                .is_some_and(|key| {
                    *key == pump_fun::PROGRAM_ID || *key == DexKind::PumpSwap.program_id()
                })
        })
        .ok_or_else(defer)?;
    let program = context.keys[ix.program_id_index as usize];
    let discriminator: [u8; 8] = ix.data[..8].try_into().map_err(|_| defer())?;
    let account = |index: usize| -> Result<Pubkey> {
        ix.accounts
            .get(index)
            .and_then(|i| context.keys.get(*i as usize))
            .copied()
            .ok_or_else(defer)
    };
    let (mint_index, user_index, ata_index, token_program_index, quote, exact_input) = if program
        == pump_fun::PROGRAM_ID
    {
        match discriminator {
            pump_fun::BUY_DISCRIMINATOR => (2, 6, 5, 8, None, false),
            pump_fun::BUY_V2_DISCRIMINATOR => (1, 13, 14, 3, Some((2, 4, 15)), false),
            pump_fun::BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR => (1, 13, 14, 3, Some((2, 4, 15)), true),
            pump_fun::BUY_V3_DISCRIMINATOR => (1, 8, 9, 3, Some((2, 4, 10)), false),
            pump_fun::BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR => (1, 8, 9, 3, Some((2, 4, 10)), true),
            _ => return Err(defer()), // Sells need the source's pre-sell position.
        }
    } else {
        match discriminator {
            [102, 6, 61, 18, 1, 218, 235, 234] => (3, 1, 5, 11, Some((4, 12, 6)), false),
            [198, 46, 21, 82, 180, 217, 232, 112] => (3, 1, 5, 11, Some((4, 12, 6)), true),
            _ => return Err(defer()),
        }
    };
    let mint = account(mint_index)?;
    let token_program = account(token_program_index)?;
    let user_account_index = ix.accounts[user_index] as usize;
    if account(user_index)? != wallet
        || !message.is_signer(user_account_index)
        || mint == AssetId::NativeSol.routing_mint()
        || ![spl_token::id(), spl_token_2022::id()].contains(&token_program)
        || account(ata_index)? != associated_token_address(&wallet, &mint, &token_program)
    {
        return Err(defer());
    }
    if let Some((quote_mint, quote_program, quote_ata)) = quote {
        let native = AssetId::NativeSol.routing_mint();
        if account(quote_mint)? != native
            || account(quote_program)? != spl_token::id()
            || account(quote_ata)? != associated_token_address(&wallet, &native, &spl_token::id())
        {
            return Err(defer());
        }
    }
    let first = u64::from_le_bytes(ix.data[8..16].try_into().map_err(|_| defer())?);
    let second = u64::from_le_bytes(ix.data[16..24].try_into().map_err(|_| defer())?);
    // Exact-input: input and minimum output. Exact-output: maximum input and output.
    // These are conservative instruction limits, not observed fill amounts.
    let (input, output) = if exact_input {
        (first, second)
    } else {
        (second, first)
    };
    if input == 0 || output == 0 {
        return Err(defer());
    }
    let source_instruction = source_route::extract_source_instruction_with_context(
        observed,
        &context,
        wallet,
        AssetId::NativeSol,
        AssetId::Token(mint),
    )
    .ok_or_else(defer)?;
    if !source_instruction
        .wallet_token_accounts
        .iter()
        .any(|(address, known, program)| {
            *address == account(ata_index).unwrap_or_default()
                && *known == mint
                && *program == token_program
        })
    {
        return Err(defer());
    }
    Ok(TradeIntent {
        source_pool: source_route::extract_source_pool_with_context(&context),
        source_instruction: Some(source_instruction),
        source_signature: observed.signature,
        slot: observed.slot,
        input_asset: AssetId::NativeSol,
        output_asset: AssetId::Token(mint),
        source_input_amount: input,
        source_output_amount: output,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{SignalOrigin, TransactionMeta};
    use solana_sdk::{
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        message::{Message, VersionedMessage},
        signature::{Keypair, Signer},
        transaction::VersionedTransaction,
    };

    pub(crate) fn buy_fixture(
        dex: DexKind,
        discriminator: [u8; 8],
    ) -> (ObservedTransaction, Pubkey, Pubkey) {
        let signer = Keypair::new();
        let wallet = signer.pubkey();
        let mint = Pubkey::new_unique();
        let swap = dex == DexKind::PumpSwap;
        let modern = !swap
            && discriminator != pump_fun::BUY_DISCRIMINATOR
            && discriminator != pump_fun::SELL_DISCRIMINATOR;
        let v3 = modern
            && [
                pump_fun::BUY_V3_DISCRIMINATOR,
                pump_fun::BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR,
            ]
            .contains(&discriminator);
        let (count, mint_i, user_i, ata_i, program_i, quote) = if swap {
            (23, 3, 1, 5, 11, Some((4, 12, 6)))
        } else if v3 {
            (17, 1, 8, 9, 3, Some((2, 4, 10)))
        } else if modern {
            (27, 1, 13, 14, 3, Some((2, 4, 15)))
        } else {
            (16, 2, 6, 5, 8, None)
        };
        let mut accounts = (0..count)
            .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
            .collect::<Vec<_>>();
        accounts[mint_i].pubkey = mint;
        accounts[user_i] = AccountMeta::new(wallet, true);
        accounts[ata_i].pubkey = associated_token_address(&wallet, &mint, &spl_token::id());
        accounts[program_i].pubkey = spl_token::id();
        if let Some((mint_i, program_i, ata_i)) = quote {
            accounts[mint_i].pubkey = AssetId::NativeSol.routing_mint();
            accounts[program_i].pubkey = spl_token::id();
            accounts[ata_i].pubkey = associated_token_address(
                &wallet,
                &AssetId::NativeSol.routing_mint(),
                &spl_token::id(),
            );
        }
        let mut data = discriminator.to_vec();
        data.extend_from_slice(&10_000_u64.to_le_bytes());
        data.extend_from_slice(&20_000_u64.to_le_bytes());
        let ix = Instruction {
            program_id: dex.program_id(),
            accounts,
            data,
        };
        let message = Message::new_with_blockhash(&[ix], Some(&wallet), &Hash::new_unique());
        let transaction =
            VersionedTransaction::try_new(VersionedMessage::Legacy(message), &[&signer]).unwrap();
        (
            ObservedTransaction {
                signature: transaction.signatures[0],
                slot: 100,
                block_time: None,
                origin: SignalOrigin::Preconfirmation,
                transaction,
                meta: TransactionMeta::default(),
                raw_payload: String::new(),
                received_bytes: 0,
            },
            wallet,
            mint,
        )
    }

    #[test]
    fn direct_buys_decode_limits_without_balance_metadata() {
        for (dex, discriminator, exact_input) in [
            (DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR, false),
            (DexKind::PumpFun, pump_fun::BUY_V2_DISCRIMINATOR, false),
            (
                DexKind::PumpFun,
                pump_fun::BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR,
                true,
            ),
            (DexKind::PumpFun, pump_fun::BUY_V3_DISCRIMINATOR, false),
            (
                DexKind::PumpFun,
                pump_fun::BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR,
                true,
            ),
            (DexKind::PumpSwap, [102, 6, 61, 18, 1, 218, 235, 234], false),
            (
                DexKind::PumpSwap,
                [198, 46, 21, 82, 180, 217, 232, 112],
                true,
            ),
        ] {
            let (observed, wallet, mint) = buy_fixture(dex, discriminator);
            let intent = decode(&observed, wallet).expect("direct buy");
            assert_eq!(intent.input_asset, AssetId::NativeSol);
            assert_eq!(intent.output_asset, AssetId::Token(mint));
            assert_eq!(
                (intent.source_input_amount, intent.source_output_amount),
                if exact_input {
                    (10_000, 20_000)
                } else {
                    (20_000, 10_000)
                }
            );
            assert!(
                intent
                    .source_instruction
                    .unwrap()
                    .wallet_token_accounts
                    .iter()
                    .any(|(_, known, _)| *known == mint)
            );
        }
    }

    #[test]
    fn ambiguous_accounts_zero_limits_and_sells_defer() {
        let (mut observed, wallet, _) = buy_fixture(DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR);
        let VersionedMessage::Legacy(message) = &mut observed.transaction.message else {
            unreachable!()
        };
        let token_index = message.instructions[0].accounts[5] as usize;
        message.account_keys[token_index] = Pubkey::new_unique();
        assert!(decode(&observed, wallet).is_err());
        let (mut observed, wallet, _) = buy_fixture(DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR);
        let VersionedMessage::Legacy(message) = &mut observed.transaction.message else {
            unreachable!()
        };
        message.instructions[0].data[8..16].fill(0);
        assert!(decode(&observed, wallet).is_err());
        let (observed, wallet, _) = buy_fixture(DexKind::PumpFun, pump_fun::SELL_DISCRIMINATOR);
        assert!(decode(&observed, wallet).is_err());
    }

    #[test]
    fn multiple_swaps_unknown_programs_and_wrong_signers_defer() {
        for scenario in ["multiple", "unknown", "signer", "truncated"] {
            let (mut observed, wallet, _) =
                buy_fixture(DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR);
            let VersionedMessage::Legacy(message) = &mut observed.transaction.message else {
                unreachable!()
            };
            match scenario {
                "multiple" => message.instructions.push(message.instructions[0].clone()),
                "unknown" => {
                    message.account_keys.push(Pubkey::new_unique());
                    message.instructions.push(
                        solana_sdk::message::compiled_instruction::CompiledInstruction {
                            program_id_index: (message.account_keys.len() - 1) as u8,
                            accounts: vec![],
                            data: vec![],
                        },
                    );
                }
                "signer" => message.header.num_required_signatures = 0,
                _ => message.instructions[0].data.truncate(8),
            }
            assert!(decode(&observed, wallet).is_err(), "{scenario}");
        }
    }
}

#[cfg(test)]
pub(crate) use tests::buy_fixture;
