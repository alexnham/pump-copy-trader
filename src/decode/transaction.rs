use std::str::FromStr;

use solana_sdk::{message::compiled_instruction::CompiledInstruction, pubkey::Pubkey};

use crate::{
    decode::balance_delta::wallet_swap_delta,
    domain::{ObservedTransaction, TradeIntent},
    error::{CopyTraderError, Result},
};

pub struct TransactionDecoder {
    source_wallet: Pubkey,
}

impl TransactionDecoder {
    pub const fn new(source_wallet: Pubkey) -> Self {
        Self { source_wallet }
    }

    pub fn decode(&self, observed: &ObservedTransaction) -> Result<TradeIntent> {
        if observed.origin == crate::domain::SignalOrigin::Preconfirmation {
            return super::preconfirmation::decode(observed, self.source_wallet);
        }
        if observed.meta.err.is_some() {
            return Err(CopyTraderError::Unsupported(
                "source transaction failed".to_owned(),
            ));
        }
        let context = DecodeContext::new(observed)?;
        super::source_route::require_pump_trade_with_context(&context)?;
        let message = &observed.transaction.message;
        let source_index = message
            .static_account_keys()
            .iter()
            .position(|key| key == &self.source_wallet)
            .ok_or_else(|| {
                CopyTraderError::Unsupported("source wallet is not a static account".to_owned())
            })?;
        if !message.is_signer(source_index) {
            return Err(CopyTraderError::Unsupported(
                "source wallet did not sign the transaction".to_owned(),
            ));
        }
        let native_adjustment = native_adjustment(observed, source_index, &context)?;
        let delta = wallet_swap_delta(
            &observed.meta,
            &self.source_wallet,
            source_index,
            native_adjustment,
        )?;
        let source_instruction = super::source_route::extract_source_instruction_with_context(
            observed,
            &context,
            self.source_wallet,
            delta.input_asset,
            delta.output_asset,
        );
        let source_instruction = source_instruction.ok_or_else(|| {
            CopyTraderError::OutOfScope(
                crate::domain::UnsupportedReason::AmbiguousTrade,
                "Pump instruction cannot be attributed to the source wallet and token pair"
                    .to_owned(),
            )
        })?;
        Ok(TradeIntent {
            source_pool: super::source_route::extract_source_pool_with_context(&context),
            source_instruction: Some(source_instruction),
            source_signature: observed.signature,
            slot: observed.slot,
            input_asset: delta.input_asset,
            output_asset: delta.output_asset,
            source_input_amount: delta.input_amount,
            source_output_amount: delta.output_amount,
        })
    }
}

fn native_adjustment(
    observed: &ObservedTransaction,
    wallet_index: usize,
    context: &DecodeContext,
) -> Result<u64> {
    let keys = &context.keys;
    let system_program = Pubkey::default();
    let wallet_index = u8::try_from(wallet_index)
        .map_err(|_| CopyTraderError::Decode("wallet account index exceeds u8".to_owned()))?;
    let mut adjustment = 0_u64;
    for instruction in &context.instructions {
        let Some(program_id) = keys.get(usize::from(instruction.program_id_index)) else {
            continue;
        };
        if *program_id != system_program
            || instruction.accounts.first().copied() != Some(wallet_index)
        {
            continue;
        }
        let Some(discriminator) = instruction.data.get(..4) else {
            continue;
        };
        let kind = u32::from_le_bytes(discriminator.try_into().map_err(|_| {
            CopyTraderError::Decode("invalid system instruction discriminator".to_owned())
        })?);
        let Some(raw_amount) = instruction.data.get(4..12) else {
            continue;
        };
        let amount = u64::from_le_bytes(raw_amount.try_into().map_err(|_| {
            CopyTraderError::Decode("invalid system instruction amount".to_owned())
        })?);
        let neutralize = match kind {
            0 => instruction.accounts.get(1).is_some_and(|index| {
                let index = usize::from(*index);
                observed
                    .meta
                    .pre_balances
                    .get(index)
                    .copied()
                    .unwrap_or_default()
                    == 0
                    && observed
                        .meta
                        .post_balances
                        .get(index)
                        .copied()
                        .unwrap_or_default()
                        > 0
            }),
            2 => instruction
                .accounts
                .get(1)
                .and_then(|index| keys.get(usize::from(*index)))
                .is_some_and(is_known_tip_account),
            _ => false,
        };
        if neutralize {
            adjustment = adjustment
                .checked_add(amount)
                .ok_or_else(|| CopyTraderError::Decode("native adjustment overflow".to_owned()))?;
        }
    }
    Ok(adjustment)
}

pub(super) struct DecodeContext {
    pub keys: Vec<Pubkey>,
    pub instructions: Vec<CompiledInstruction>,
}
impl DecodeContext {
    pub fn new(observed: &ObservedTransaction) -> Result<Self> {
        Ok(Self {
            keys: full_account_keys(observed)?,
            instructions: all_instructions(observed)?,
        })
    }
}

pub(super) fn all_instructions(observed: &ObservedTransaction) -> Result<Vec<CompiledInstruction>> {
    let mut instructions = observed.transaction.message.instructions().to_vec();
    if let Some(inner) = &observed.meta.live_inner_instructions {
        instructions.extend_from_slice(inner);
        return Ok(instructions);
    }
    for group in observed
        .meta
        .inner_instructions
        .as_deref()
        .unwrap_or_default()
    {
        for instruction in &group.instructions {
            instructions.push(CompiledInstruction {
                program_id_index: instruction.program_id_index,
                accounts: instruction.accounts.clone(),
                data: bs58::decode(&instruction.data)
                    .into_vec()
                    .map_err(|error| {
                        CopyTraderError::Decode(format!("invalid inner instruction data: {error}"))
                    })?,
            });
        }
    }
    Ok(instructions)
}

pub(super) fn full_account_keys(observed: &ObservedTransaction) -> Result<Vec<Pubkey>> {
    let mut keys = observed.transaction.message.static_account_keys().to_vec();
    if let Some(loaded) = &observed.meta.live_loaded_addresses {
        keys.extend_from_slice(loaded);
        return Ok(keys);
    }
    if let Some(loaded) = &observed.meta.loaded_addresses {
        for address in loaded.writable.iter().chain(&loaded.readonly) {
            keys.push(Pubkey::from_str(address).map_err(|error| {
                CopyTraderError::Decode(format!("invalid loaded address: {error}"))
            })?);
        }
    }
    Ok(keys)
}

fn is_known_tip_account(account: &Pubkey) -> bool {
    const TIPS: [Pubkey; 10] = [
        Pubkey::from_str_const("4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE"),
        Pubkey::from_str_const("D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ"),
        Pubkey::from_str_const("9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta"),
        Pubkey::from_str_const("5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn"),
        Pubkey::from_str_const("2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD"),
        Pubkey::from_str_const("2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ"),
        Pubkey::from_str_const("wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF"),
        Pubkey::from_str_const("3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT"),
        Pubkey::from_str_const("4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey"),
        Pubkey::from_str_const("4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or"),
    ];
    TIPS.contains(account)
}
