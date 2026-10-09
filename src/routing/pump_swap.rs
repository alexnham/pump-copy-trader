use crate::{
    domain::{AssetId, DexKind, NATIVE_MINT, PreparedRoute, SizedTrade, SourceInstruction},
    error::{CopyTraderError, Result},
    token::accounts::associated_token_address,
};
use solana_sdk::pubkey::Pubkey;

pub(crate) const COMPUTE_UNIT_LIMIT: u32 = 350_000;

#[cfg(test)]
pub(crate) fn copy_source_instruction(
    source: &SourceInstruction,
    trade: &SizedTrade,
    copier: Pubkey,
    expected_output: u64,
    minimum_output: u64,
) -> Result<PreparedRoute> {
    copy_source_instruction_with_wsol(source, trade, copier, expected_output, minimum_output, 0)
}

pub(crate) fn copy_source_instruction_with_wsol(
    source: &SourceInstruction,
    trade: &SizedTrade,
    copier: Pubkey,
    expected_output: u64,
    minimum_output: u64,
    cached_wsol: u64,
) -> Result<PreparedRoute> {
    if (trade.intent.input_asset == AssetId::NativeSol
        || trade.intent.output_asset == AssetId::NativeSol)
        && !source
            .wallet_token_accounts
            .iter()
            .any(|(_, mint, program)| {
                *mint == Pubkey::from_str_const(NATIVE_MINT) && *program == spl_token::id()
            })
    {
        return Err(CopyTraderError::Unsupported(
            "PumpSwap source instruction has no canonical WSOL account".to_owned(),
        ));
    }
    for asset in [trade.intent.input_asset, trade.intent.output_asset] {
        let mint = asset.routing_mint();
        if !source
            .wallet_token_accounts
            .iter()
            .any(|(_, source_mint, _)| *source_mint == mint)
        {
            return Err(CopyTraderError::Unsupported(
                "PumpSwap source instruction has an unmapped wallet token account".to_owned(),
            ));
        }
    }
    let discriminator: [u8; 8] = source
        .instruction
        .data
        .get(..8)
        .ok_or_else(|| CopyTraderError::Unsupported("unsupported PumpSwap instruction".to_owned()))?
        .try_into()
        .map_err(|_| CopyTraderError::Unsupported("unsupported PumpSwap instruction".to_owned()))?;
    let (first, second) = match discriminator {
        // buy_exact_quote_in: spendable quote input, minimum base output.
        [198, 46, 21, 82, 180, 217, 232, 112] => (trade.input_amount, minimum_output),
        // buy: exact base output, maximum quote input.
        [102, 6, 61, 18, 1, 218, 235, 234] => (minimum_output, trade.input_amount),
        // sell and supported exact-input sell layouts.
        [51, 230, 133, 164, 1, 127, 131, 173]
        | [184, 23, 238, 97, 103, 197, 211, 61]
        | [194, 171, 28, 70, 104, 77, 91, 47]
        | [93, 246, 130, 60, 231, 233, 64, 178] => (trade.input_amount, minimum_output),
        _ => {
            return Err(CopyTraderError::Unsupported(
                "unsupported PumpSwap swap instruction layout".to_owned(),
            ));
        }
    };
    let mut data = source.instruction.data.clone();
    if data.len() < 24 {
        return Err(CopyTraderError::Unsupported(
            "unsupported PumpSwap swap instruction data".to_owned(),
        ));
    }
    data[8..16].copy_from_slice(&first.to_le_bytes());
    data[16..24].copy_from_slice(&second.to_le_bytes());
    let source_volume = crate::token::accounts::user_volume_address(
        &source.instruction.program_id,
        &source.source_wallet,
    );
    let copier_volume =
        crate::token::accounts::user_volume_address(&source.instruction.program_id, &copier);
    let volume_accounts = source
        .wallet_token_accounts
        .iter()
        .map(|(_, mint, program)| {
            (
                associated_token_address(&source_volume, mint, program),
                associated_token_address(&copier_volume, mint, program),
            )
        })
        .collect::<Vec<_>>();
    let mut accounts = source.instruction.accounts.clone();
    for meta in &mut accounts {
        if meta.pubkey == source.source_wallet {
            meta.pubkey = copier;
        } else if meta.pubkey == source_volume {
            meta.pubkey = copier_volume;
        } else if let Some((_, replacement)) = volume_accounts
            .iter()
            .find(|(address, _)| *address == meta.pubkey)
        {
            meta.pubkey = *replacement;
        } else if let Some((_, mint, token_program)) = source
            .wallet_token_accounts
            .iter()
            .find(|(address, _, _)| *address == meta.pubkey)
        {
            meta.pubkey = associated_token_address(&copier, mint, token_program);
        }
    }
    let pool = accounts.first().map(|meta| meta.pubkey).ok_or_else(|| {
        CopyTraderError::Unsupported("PumpSwap source pool is missing".to_owned())
    })?;
    let market_accounts = accounts
        .iter()
        .filter(|meta| meta.is_writable && meta.pubkey != copier)
        .map(|meta| meta.pubkey)
        .collect();
    let mut instructions = Vec::with_capacity(5);
    let mut prepared_mints = Vec::new();
    for (address, mint, token_program) in &source.wallet_token_accounts {
        if prepared_mints.contains(mint) {
            continue;
        }
        prepared_mints.push(*mint);
        instructions.push(
            crate::token::accounts::create_associated_token_account_idempotent(
                &copier,
                &copier,
                mint,
                token_program,
            ),
        );
        if *mint == Pubkey::from_str_const(NATIVE_MINT)
            && trade.intent.input_asset == AssetId::NativeSol
        {
            let shortfall = trade
                .input_amount
                .checked_sub(cached_wsol.min(trade.input_amount))
                .ok_or_else(|| {
                    CopyTraderError::Execution("WSOL funding subtraction failed".into())
                })?;
            if shortfall > 0 {
                instructions.extend(pump_rust_client::token::wrap_sol_instructions(
                    &copier, shortfall,
                ));
            }
        }
        let _ = address;
    }
    instructions.push(solana_sdk::instruction::Instruction {
        program_id: source.instruction.program_id,
        accounts,
        data,
    });
    Ok(PreparedRoute {
        dex: DexKind::PumpSwap,
        pool,
        instructions,
        additional_signers: Vec::new(),
        market_accounts,
        expected_output,
        minimum_output,
        compute_unit_limit: COMPUTE_UNIT_LIMIT,
    })
}
