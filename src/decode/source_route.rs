use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use std::str::FromStr;
use tracing::{debug, info};

use crate::domain::{
    AssetId, DexKind, ObservedTransaction, SourceInstruction, SourcePool, UnsupportedReason,
};
use crate::error::{CopyTraderError, Result};

use super::transaction::{all_instructions, full_account_keys};
use crate::routing::pump_fun;

const PUMP_SWAP_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const ANCHOR_EVENT: [u8; 8] = 0x1d9acb512ea545e4_u64.to_le_bytes();

pub(super) fn extract_source_pool(observed: &ObservedTransaction) -> Option<SourcePool> {
    let keys = full_account_keys(observed).ok()?;
    let mut hint = None;
    for instruction in all_instructions(observed).ok()? {
        let program = keys.get(usize::from(instruction.program_id_index))?;
        let Some(dex) = [DexKind::PumpSwap]
            .into_iter()
            .find(|dex| dex.program_id() == *program)
        else {
            continue;
        };
        let discriminator: [u8; 8] = instruction.data.get(..8)?.try_into().ok()?;
        // PumpSwap emits event CPIs alongside the swap.
        if discriminator == ANCHOR_EVENT && instruction.data.len() >= 16 {
            continue;
        }

        // Scope admission rejects unknown layouts and multiple swaps before routing.
        let (pool_index, minimum_accounts, minimum_data) = layout(dex, discriminator)?;
        if hint.is_some()
            || instruction.accounts.len() < minimum_accounts
            || instruction.data.len() < minimum_data
        {
            return None;
        }
        let pool = *instruction.accounts.get(pool_index)?;
        hint = Some(SourcePool {
            dex,
            address: *keys.get(usize::from(pool))?,
        });
    }
    hint
}

pub(super) fn extract_source_instruction(
    observed: &ObservedTransaction,
    source_wallet: Pubkey,
    input_asset: AssetId,
    output_asset: AssetId,
) -> Option<SourceInstruction> {
    let keys = full_account_keys(observed).ok()?;
    let message = &observed.transaction.message;
    let mut found = None;
    let mut inspected = Vec::new();
    for instruction in all_instructions(observed).ok()? {
        let Some(program) = keys.get(usize::from(instruction.program_id_index)).copied() else {
            continue;
        };
        if inspected.len() < 32 {
            let Some(discriminator): Option<[u8; 8]> = instruction
                .data
                .get(..8)
                .and_then(|data| data.try_into().ok())
            else {
                continue;
            };
            inspected.push((
                program,
                discriminator,
                instruction.accounts.len(),
                instruction.data.len(),
            ));
        }
        if program != DexKind::PumpSwap.program_id() && program != pump_fun::PROGRAM_ID {
            continue;
        }
        let Some(discriminator) = instruction
            .data
            .get(..8)
            .and_then(|data| data.try_into().ok())
        else {
            debug!(
                program = %program,
                accounts = instruction.accounts.len(),
                "source DEX instruction has no discriminator"
            );
            continue;
        };
        if discriminator == ANCHOR_EVENT && instruction.data.len() >= 16 {
            continue;
        }
        let dex = if program == pump_fun::PROGRAM_ID {
            DexKind::PumpFun
        } else {
            DexKind::PumpSwap
        };
        info!(
            dex = dex.as_str(),
            discriminator = ?discriminator,
            accounts = instruction.accounts.len(),
            data_len = instruction.data.len(),
            "source DEX instruction inspected"
        );
        let Some((_, minimum_accounts, minimum_data)) = layout(dex, discriminator) else {
            debug!(
                dex = dex.as_str(),
                discriminator = ?discriminator,
                accounts = instruction.accounts.len(),
                data_len = instruction.data.len(),
                "unsupported source DEX instruction skipped"
            );
            continue;
        };
        if instruction.accounts.len() < minimum_accounts || instruction.data.len() < minimum_data {
            debug!(
                dex = dex.as_str(),
                discriminator = ?discriminator,
                accounts = instruction.accounts.len(),
                required_accounts = minimum_accounts,
                data_len = instruction.data.len(),
                required_data = minimum_data,
                "truncated source DEX instruction skipped"
            );
            continue;
        }
        info!(
            dex = dex.as_str(),
            discriminator = ?discriminator,
            account_count = instruction.accounts.len(),
            key_count = keys.len(),
            "source DEX instruction passed layout validation"
        );
        let mut metas = Vec::with_capacity(instruction.accounts.len());
        for account_index in &instruction.accounts {
            let index = usize::from(*account_index);
            let Some(pubkey) = keys.get(index).copied() else {
                info!(
                    dex = dex.as_str(),
                    discriminator = ?discriminator,
                    account_index = index,
                    account_key_count = keys.len(),
                    "source DEX instruction references an unavailable account"
                );
                metas.clear();
                break;
            };
            metas.push(AccountMeta {
                pubkey,
                is_signer: message.is_signer(index),
                is_writable: message.is_maybe_writable(index, None),
            });
        }
        if metas.len() != instruction.accounts.len() {
            info!(
                dex = dex.as_str(),
                discriminator = ?discriminator,
                resolved_accounts = metas.len(),
                expected_accounts = instruction.accounts.len(),
                "source DEX instruction account resolution incomplete"
            );
            continue;
        }
        let source_wallet_present = metas.iter().any(|meta| meta.pubkey == source_wallet);
        info!(
            dex = dex.as_str(),
            discriminator = ?discriminator,
            source_wallet_present,
            source_wallet = %source_wallet,
            account_keys = ?metas.iter().map(|meta| meta.pubkey).collect::<Vec<_>>(),
            "source DEX account metas resolved"
        );
        if !source_wallet_present {
            info!(
                dex = dex.as_str(),
                discriminator = ?discriminator,
                source_wallet = %source_wallet,
                "source DEX instruction does not contain source wallet"
            );
            continue;
        }
        if found.is_some() {
            debug!(dex = dex.as_str(), "multiple source DEX instructions found");
            return None;
        }
        let mut wallet_token_accounts = Vec::new();
        for asset in [input_asset, output_asset] {
            let mint = asset.routing_mint();
            for balance in observed
                .meta
                .pre_token_balances
                .iter()
                .chain(observed.meta.post_token_balances.iter())
                .flatten()
            {
                if balance.owner.as_deref() != Some(&source_wallet.to_string())
                    || Pubkey::from_str(&balance.mint).ok() != Some(mint)
                {
                    continue;
                }
                let Some(address) = keys.get(usize::from(balance.account_index)).copied() else {
                    continue;
                };
                if !metas.iter().any(|meta| meta.pubkey == address) {
                    continue;
                }
                let Some(program) = balance
                    .program_id
                    .as_deref()
                    .and_then(|program| Pubkey::from_str(program).ok())
                else {
                    continue;
                };
                if [spl_token::id(), spl_token_2022::id()].contains(&program)
                    && !wallet_token_accounts
                        .iter()
                        .any(|(known, _, _)| *known == address)
                {
                    wallet_token_accounts.push((address, mint, program));
                }
            }
            for program in [spl_token::id(), spl_token_2022::id()] {
                let address = crate::token::accounts::associated_token_address(
                    &source_wallet,
                    &mint,
                    &program,
                );
                if metas.iter().any(|meta| meta.pubkey == address)
                    && !wallet_token_accounts
                        .iter()
                        .any(|(known, _, _)| *known == address)
                {
                    wallet_token_accounts.push((address, mint, program));
                }
            }
        }
        if wallet_token_accounts.is_empty()
            && (input_asset != AssetId::NativeSol || output_asset != AssetId::NativeSol)
        {
            debug!(
                dex = dex.as_str(),
                discriminator = ?discriminator,
                input = ?input_asset,
                output = ?output_asset,
                "source DEX instruction has no wallet token account mapping"
            );
        }
        found = Some(SourceInstruction {
            instruction: Instruction {
                program_id: program,
                accounts: metas,
                data: instruction.data.clone(),
            },
            source_wallet,
            wallet_token_accounts,
        });
    }
    if found.is_none() && !inspected.is_empty() {
        info!(
            source_wallet = %source_wallet,
            instruction_count = inspected.len(),
            instructions = ?inspected,
            "no supported source-copy instruction found"
        );
    }
    found
}

// Account ordering follows the DEX IDLs; only the pool identity is reused.
fn layout(dex: DexKind, discriminator: [u8; 8]) -> Option<(usize, usize, usize)> {
    match (dex, discriminator) {
        (DexKind::PumpSwap, PUMP_SWAP_BUY | [198, 46, 21, 82, 180, 217, 232, 112]) => {
            Some((0, 23, 24))
        }
        (DexKind::PumpSwap, [51, 230, 133, 164, 1, 127, 131, 173]) => Some((0, 21, 24)),
        (
            DexKind::PumpSwap,
            [184, 23, 238, 97, 103, 197, 211, 61]
            | [194, 171, 28, 70, 104, 77, 91, 47]
            | [93, 246, 130, 60, 231, 233, 64, 178],
        ) => Some((0, 17, 24)),
        (DexKind::PumpFun, discriminator) if pump_fun::is_trade_discriminator(discriminator) => {
            Some((
                3,
                pump_fun::minimum_account_count_for_discriminator(discriminator)?,
                24,
            ))
        }
        _ => None,
    }
}

pub(super) fn require_pump_trade(observed: &ObservedTransaction) -> Result<()> {
    let keys = full_account_keys(observed)?;
    let mut swaps = 0_usize;
    let mut other_dex = false;
    for instruction in all_instructions(observed)? {
        let program = keys
            .get(usize::from(instruction.program_id_index))
            .ok_or_else(|| {
                CopyTraderError::OutOfScope(
                    UnsupportedReason::UnsupportedInstruction,
                    "source instruction references an unavailable program".to_owned(),
                )
            })?;
        if [
            DexKind::RaydiumCpmm,
            DexKind::RaydiumClmm,
            DexKind::OrcaWhirlpool,
            DexKind::MeteoraDlmm,
        ]
        .iter()
        .any(|dex| dex.program_id() == *program)
        {
            other_dex = true;
            continue;
        }
        let dex = if *program == DexKind::PumpFun.program_id() {
            DexKind::PumpFun
        } else if *program == DexKind::PumpSwap.program_id() {
            DexKind::PumpSwap
        } else {
            continue;
        };
        let unsupported = || {
            CopyTraderError::OutOfScope(
                UnsupportedReason::UnsupportedInstruction,
                format!("unrecognized or malformed {} instruction", dex.as_str()),
            )
        };
        let discriminator = instruction
            .data
            .get(..8)
            .and_then(|d| d.try_into().ok())
            .ok_or_else(unsupported)?;
        if discriminator == ANCHOR_EVENT && instruction.data.len() >= 16 {
            continue;
        }
        let (_, accounts, data) = layout(dex, discriminator).ok_or_else(unsupported)?;
        if instruction.accounts.len() < accounts
            || instruction.data.len() < data
            || instruction
                .accounts
                .iter()
                .any(|index| usize::from(*index) >= keys.len())
        {
            return Err(unsupported());
        }
        swaps = swaps.saturating_add(1);
    }
    if swaps > 1 || (swaps > 0 && other_dex) {
        return Err(CopyTraderError::OutOfScope(
            UnsupportedReason::MultiHop,
            "multiple swap instructions are outside the single-hop Pump scope".to_owned(),
        ));
    }
    if swaps == 0 {
        return Err(CopyTraderError::OutOfScope(
            UnsupportedReason::UnsupportedDex,
            "source transaction contains no supported Pump.fun or PumpSwap trade".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        LoadedAddresses, SignalOrigin, TransactionMeta, UiCompiledInstruction, UiInnerInstructions,
    };
    use solana_sdk::{
        hash::Hash,
        message::{
            Message, MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction, v0,
        },
        pubkey::Pubkey,
        signature::Signature,
        transaction::VersionedTransaction,
    };

    fn observation(dex: DexKind, discriminator: [u8; 8]) -> ObservedTransaction {
        let (index, accounts, bytes) = layout(dex, discriminator).unwrap();
        let mut instruction = CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0; accounts],
            data: vec![0; bytes],
        };
        instruction.accounts[index] = 2;
        instruction.data[..8].copy_from_slice(&discriminator);
        ObservedTransaction {
            signature: Signature::default(),
            slot: 42,
            block_time: None,
            origin: SignalOrigin::Live,
            transaction: VersionedTransaction {
                signatures: vec![],
                message: VersionedMessage::Legacy(Message {
                    header: MessageHeader::default(),
                    account_keys: vec![
                        Pubkey::new_unique(),
                        dex.program_id(),
                        Pubkey::new_unique(),
                    ],
                    recent_blockhash: Hash::default(),
                    instructions: vec![instruction],
                }),
            },
            meta: TransactionMeta::default(),
            raw_payload: String::new(),
            received_bytes: 0,
        }
    }

    #[test]
    fn extracts_supported_outer_swaps_and_rejects_truncation() {
        for (dex, discriminator) in [
            (DexKind::PumpSwap, [102, 6, 61, 18, 1, 218, 235, 234]),
            (DexKind::PumpSwap, [51, 230, 133, 164, 1, 127, 131, 173]),
            (DexKind::PumpSwap, [194, 171, 28, 70, 104, 77, 91, 47]),
        ] {
            let mut observed = observation(dex, discriminator);
            let pool = observed.transaction.message.static_account_keys()[2];
            assert_eq!(
                extract_source_pool(&observed),
                Some(SourcePool { dex, address: pool })
            );
            let VersionedMessage::Legacy(ref mut message) = observed.transaction.message else {
                unreachable!()
            };
            message.instructions[0].data.truncate(8);
            assert_eq!(extract_source_pool(&observed), None);
        }
    }

    #[test]
    fn extracts_pump_fun_sell_source_instruction() {
        for discriminator in [
            pump_fun::SELL_DISCRIMINATOR,
            pump_fun::SELL_V2_DISCRIMINATOR,
            pump_fun::SELL_V3_DISCRIMINATOR,
        ] {
            let mut observed = observation(DexKind::PumpFun, discriminator);
            let source_wallet = observed.transaction.message.static_account_keys()[0];
            let source = extract_source_instruction(
                &observed,
                source_wallet,
                AssetId::Token(Pubkey::new_unique()),
                AssetId::NativeSol,
            );
            assert!(source.is_some());
            let VersionedMessage::Legacy(ref mut message) = observed.transaction.message else {
                unreachable!()
            };
            message.instructions[0].data.truncate(8);
            assert!(
                extract_source_instruction(
                    &observed,
                    source_wallet,
                    AssetId::Token(Pubkey::new_unique()),
                    AssetId::NativeSol,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn extracts_native_pumpswap_wallet_and_wsol_accounts() {
        let source_wallet = Pubkey::new_unique();
        let output_mint = Pubkey::new_unique();
        let source_wsol = crate::token::accounts::associated_token_address(
            &source_wallet,
            &Pubkey::from_str_const(crate::domain::NATIVE_MINT),
            &spl_token::id(),
        );
        let mut observed = observation(DexKind::PumpSwap, [102, 6, 61, 18, 1, 218, 235, 234]);
        let VersionedMessage::Legacy(ref mut message) = observed.transaction.message else {
            unreachable!()
        };
        message.header.num_required_signatures = 1;
        message.account_keys = vec![
            source_wallet,
            DexKind::PumpSwap.program_id(),
            Pubkey::new_unique(),
            source_wsol,
        ];
        message.instructions[0].accounts[1] = 3;
        message.instructions[0].accounts[2] = 0;
        let source = extract_source_instruction(
            &observed,
            source_wallet,
            AssetId::NativeSol,
            AssetId::Token(output_mint),
        )
        .expect("native PumpSwap source instruction");
        assert_eq!(source.source_wallet, source_wallet);
        assert!(
            source
                .wallet_token_accounts
                .iter()
                .any(|(address, mint, program)| {
                    *address == source_wsol
                        && *mint == Pubkey::from_str_const(crate::domain::NATIVE_MINT)
                        && *program == spl_token::id()
                })
        );
    }

    #[test]
    fn resolves_inner_swap_pool_and_program_from_lookup_table_addresses() {
        let mut observed = observation(DexKind::PumpSwap, PUMP_SWAP_BUY);
        let pool = observed.transaction.message.static_account_keys()[2];
        let mut swap = observed.transaction.message.instructions()[0].clone();
        swap.program_id_index = 2;
        swap.accounts[0] = 1;
        observed.transaction.message = VersionedMessage::V0(v0::Message {
            header: MessageHeader::default(),
            account_keys: vec![Pubkey::new_unique()],
            recent_blockhash: Hash::default(),
            instructions: vec![],
            address_table_lookups: vec![v0::MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        observed.meta.loaded_addresses = Some(LoadedAddresses {
            writable: vec![pool.to_string()],
            readonly: vec![DexKind::PumpSwap.program_id().to_string()],
        });
        observed.meta.inner_instructions = Some(vec![UiInnerInstructions {
            index: 0,
            instructions: vec![UiCompiledInstruction {
                program_id_index: swap.program_id_index,
                accounts: swap.accounts,
                data: bs58::encode(swap.data).into_string(),
                stack_height: Some(2),
            }],
        }]);
        assert_eq!(
            extract_source_pool(&observed),
            Some(SourcePool {
                dex: DexKind::PumpSwap,
                address: pool
            })
        );
        observed.meta.loaded_addresses = None;
        assert_eq!(extract_source_pool(&observed), None);
    }

    #[test]
    fn ambiguous_unknown_and_malformed_instructions_are_unsupported() {
        for variant in 0..5 {
            let mut observed = observation(DexKind::PumpSwap, PUMP_SWAP_BUY);
            let VersionedMessage::Legacy(ref mut message) = observed.transaction.message else {
                unreachable!()
            };
            match variant {
                0 => message.instructions.push(message.instructions[0].clone()),
                1 => message.instructions[0].data[..8].fill(0),
                2 => message.instructions[0].accounts[0] = 255,
                3 => message.instructions[0].program_id_index = 255,
                _ => message.account_keys[1] = Pubkey::new_unique(),
            }
            assert_eq!(extract_source_pool(&observed), None);
            assert!(require_pump_trade(&observed).is_err());
        }
    }

    #[test]
    fn event_cpis_do_not_turn_a_single_swap_into_an_unknown_route() {
        let mut observed = observation(DexKind::PumpSwap, [102, 6, 61, 18, 1, 218, 235, 234]);
        let expected = extract_source_pool(&observed);
        let mut event = ANCHOR_EVENT.to_vec();
        event.extend_from_slice(&[0; 32]);
        observed.meta.inner_instructions = Some(vec![UiInnerInstructions {
            index: 0,
            instructions: vec![UiCompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: bs58::encode(event).into_string(),
                stack_height: Some(2),
            }],
        }]);
        assert!(expected.is_some());
        assert_eq!(extract_source_pool(&observed), expected);
    }

    #[test]
    fn arbitrary_truncations_and_indices_do_not_panic() {
        for length in 0..45 {
            for index in 0..=u8::MAX {
                let mut observed = observation(DexKind::PumpSwap, PUMP_SWAP_BUY);
                let VersionedMessage::Legacy(ref mut message) = observed.transaction.message else {
                    unreachable!()
                };
                message.instructions[0].data.truncate(length);
                message.instructions[0].accounts[0] = index;
                let scope = require_pump_trade(&observed);
                let result = extract_source_pool(&observed);
                if length < 24 || index > 2 {
                    assert!(result.is_none());
                    assert!(scope.is_err());
                }
            }
        }
    }
}
