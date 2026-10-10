use solana_sdk::{instruction::Instruction, pubkey::Pubkey};

use crate::{
    domain::{AssetId, DexKind, NATIVE_MINT, PreparedRoute, SizedTrade, SourceInstruction},
    error::{CopyTraderError, Result},
    token::accounts::{associated_token_address, create_associated_token_account_idempotent},
};

pub const PROGRAM_ID: Pubkey = solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
pub const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
pub const BUY_V2_DISCRIMINATOR: [u8; 8] = [184, 23, 238, 97, 103, 197, 211, 61];
pub const BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR: [u8; 8] = [194, 171, 28, 70, 104, 77, 91, 47];
pub const BUY_V3_DISCRIMINATOR: [u8; 8] = [7, 5, 29, 196, 245, 23, 101, 7];
pub const BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR: [u8; 8] = [225, 247, 80, 30, 213, 179, 132, 136];
pub const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
pub const SELL_V2_DISCRIMINATOR: [u8; 8] = [93, 246, 130, 60, 231, 233, 64, 178];
pub const SELL_V3_DISCRIMINATOR: [u8; 8] = [28, 146, 222, 119, 38, 196, 105, 213];
pub const BUY_ACCOUNT_COUNT: usize = 16;
const V2_USER_INDEX: usize = 13;
const V2_BASE_USER_ATA_INDEX: usize = 14;
const V2_QUOTE_USER_ATA_INDEX: usize = 15;
const V2_USER_VOLUME_INDEX: usize = 20;
const V2_ASSOCIATED_USER_VOLUME_INDEX: usize = 21;
const V2_BONDING_CURVE_INDEX: usize = 10;
const V3_USER_INDEX: usize = 8;
const V3_BASE_USER_ATA_INDEX: usize = 9;
const V3_QUOTE_USER_ATA_INDEX: usize = 10;
const V3_USER_VOLUME_INDEX: usize = 11;
const V3_BONDING_CURVE_INDEX: usize = 5;

pub fn is_buy(data: &[u8]) -> bool {
    let Some(discriminator) = data.get(..8) else {
        return false;
    };
    data.len() >= 24
        && [
            BUY_DISCRIMINATOR,
            BUY_V2_DISCRIMINATOR,
            BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR,
            BUY_V3_DISCRIMINATOR,
            BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR,
        ]
        .iter()
        .any(|candidate| discriminator == candidate)
}

pub fn is_sell(data: &[u8]) -> bool {
    data.len() >= 24
        && data.get(..8).is_some_and(|discriminator| {
            [
                SELL_DISCRIMINATOR,
                SELL_V2_DISCRIMINATOR,
                SELL_V3_DISCRIMINATOR,
            ]
            .iter()
            .any(|candidate| discriminator == candidate)
        })
}

pub fn is_exact_quote_in(data: &[u8]) -> bool {
    data.get(..8).is_some_and(|discriminator| {
        discriminator == BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR
            || discriminator == BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR
    })
}

pub fn minimum_account_count(data: &[u8]) -> Option<usize> {
    if data.get(..8) == Some(BUY_DISCRIMINATOR.as_slice()) {
        Some(BUY_ACCOUNT_COUNT)
    } else if data.get(..8).is_some_and(|discriminator| {
        discriminator == BUY_V2_DISCRIMINATOR
            || discriminator == BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR
    }) {
        Some(27)
    } else if data.get(..8).is_some_and(|discriminator| {
        discriminator == BUY_V3_DISCRIMINATOR
            || discriminator == BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR
            || discriminator == SELL_V3_DISCRIMINATOR
    }) {
        Some(17)
    } else if data.get(..8) == Some(SELL_V2_DISCRIMINATOR.as_slice()) {
        Some(26)
    } else if data.get(..8) == Some(SELL_DISCRIMINATOR.as_slice()) {
        Some(14)
    } else {
        None
    }
}

pub fn is_buy_discriminator(discriminator: [u8; 8]) -> bool {
    matches!(
        discriminator,
        BUY_DISCRIMINATOR
            | BUY_V2_DISCRIMINATOR
            | BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR
            | BUY_V3_DISCRIMINATOR
            | BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR
    )
}

pub fn is_trade_discriminator(discriminator: [u8; 8]) -> bool {
    is_buy_discriminator(discriminator)
        || matches!(
            discriminator,
            SELL_DISCRIMINATOR | SELL_V2_DISCRIMINATOR | SELL_V3_DISCRIMINATOR
        )
}

pub fn minimum_account_count_for_discriminator(discriminator: [u8; 8]) -> Option<usize> {
    minimum_account_count(&discriminator)
}

pub(crate) fn copy_source_instruction(
    source: &SourceInstruction,
    trade: &SizedTrade,
    copier: Pubkey,
    expected_output: u64,
    minimum_output: u64,
) -> Result<PreparedRoute> {
    if is_sell(&source.instruction.data) {
        return copy_source_sell_instruction(
            source,
            trade,
            copier,
            expected_output,
            minimum_output,
        );
    }
    if source.instruction.program_id != PROGRAM_ID
        || !is_buy(&source.instruction.data)
        || trade.intent.input_asset != AssetId::NativeSol
        || !matches!(trade.intent.output_asset, AssetId::Token(_))
    {
        return Err(CopyTraderError::Unsupported(
            "unsupported Pump.fun bonding-curve instruction".to_owned(),
        ));
    }
    // A native wallet delta can include a wrapper's quote-token conversion.
    // Copying only its Pump instruction cannot reproduce that funding leg.
    if source
        .instruction
        .data
        .get(..8)
        .is_some_and(|d| d == BUY_V2_DISCRIMINATOR || d == BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR)
        && source
            .instruction
            .accounts
            .get(2)
            .is_some_and(|a| a.pubkey != Pubkey::from_str_const(NATIVE_MINT))
    {
        return Err(CopyTraderError::Unsupported(
            "Pump.fun V2 quote-token buy requires a funding conversion; cannot copy it as native SOL".into(),
        ));
    }
    let output_mint = trade.intent.output_asset.routing_mint();
    let output_program = source
        .wallet_token_accounts
        .iter()
        .find(|(_, mint, _)| *mint == output_mint)
        .map(|(_, _, program)| *program)
        .ok_or_else(|| {
            CopyTraderError::Unsupported(
                "Pump.fun buy has no mapped source output token account".to_owned(),
            )
        })?;
    let mut data = source.instruction.data.clone();
    // Pump.fun's Anchor buy args are amount (token units), max_sol_cost (lamports).
    let (first, second) = if is_exact_quote_in(&data) {
        (trade.input_amount, minimum_output)
    } else {
        // Exact-output buys must reduce the requested tokens to honor
        // slippage without increasing the configured SOL spending cap.
        (minimum_output, trade.input_amount)
    };
    data[8..16].copy_from_slice(&first.to_le_bytes());
    data[16..24].copy_from_slice(&second.to_le_bytes());
    let mut accounts = source.instruction.accounts.clone();
    for meta in &mut accounts {
        if meta.pubkey == source.source_wallet {
            meta.pubkey = copier;
        } else if let Some((_, mint, token_program)) = source
            .wallet_token_accounts
            .iter()
            .find(|(address, _, _)| *address == meta.pubkey)
        {
            meta.pubkey = associated_token_address(&copier, mint, token_program);
        }
    }
    // Legacy buys can also carry the wallet-derived volume accumulator among
    // their trailing accounts. Preserve layout/order and remap it by identity.
    let source_volume =
        crate::token::accounts::user_volume_address(&PROGRAM_ID, &source.source_wallet);
    let copier_volume = crate::token::accounts::user_volume_address(&PROGRAM_ID, &copier);
    for meta in &mut accounts {
        if meta.pubkey == source_volume {
            meta.pubkey = copier_volume;
        }
    }
    let discriminator: [u8; 8] = data[..8].try_into().map_err(|_| {
        CopyTraderError::Execution("Pump.fun discriminator is truncated".to_owned())
    })?;
    let (bonding_curve_index, base_user_ata, quote_user_ata) = if matches!(
        discriminator,
        BUY_V2_DISCRIMINATOR | BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR
    ) {
        let user_volume = crate::token::accounts::user_volume_address(&PROGRAM_ID, &copier);
        let quote_mint = accounts.get(2).map(|meta| meta.pubkey).ok_or_else(|| {
            CopyTraderError::Unsupported("Pump.fun quote mint is missing".to_owned())
        })?;
        let quote_program = accounts.get(4).map(|meta| meta.pubkey).ok_or_else(|| {
            CopyTraderError::Unsupported("Pump.fun quote token program is missing".to_owned())
        })?;
        let base_program = accounts.get(3).map(|meta| meta.pubkey).ok_or_else(|| {
            CopyTraderError::Unsupported("Pump.fun base token program is missing".to_owned())
        })?;
        let quote_user_ata = associated_token_address(&copier, &quote_mint, &quote_program);
        let base_user_ata = associated_token_address(&copier, &accounts[1].pubkey, &base_program);
        accounts[V2_USER_INDEX].pubkey = copier;
        accounts[V2_BASE_USER_ATA_INDEX].pubkey = base_user_ata;
        accounts[V2_QUOTE_USER_ATA_INDEX].pubkey = quote_user_ata;
        accounts[V2_USER_VOLUME_INDEX].pubkey = user_volume;
        accounts[V2_ASSOCIATED_USER_VOLUME_INDEX].pubkey =
            associated_token_address(&user_volume, &quote_mint, &quote_program);
        (V2_BONDING_CURVE_INDEX, base_user_ata, quote_user_ata)
    } else if matches!(
        discriminator,
        BUY_V3_DISCRIMINATOR | BUY_EXACT_QUOTE_IN_V3_DISCRIMINATOR
    ) {
        let user_volume = crate::token::accounts::user_volume_address(&PROGRAM_ID, &copier);
        accounts[V3_USER_INDEX].pubkey = copier;
        accounts[V3_USER_VOLUME_INDEX].pubkey = user_volume;
        (
            V3_BONDING_CURVE_INDEX,
            accounts[V3_BASE_USER_ATA_INDEX].pubkey,
            accounts[V3_QUOTE_USER_ATA_INDEX].pubkey,
        )
    } else {
        (
            3,
            accounts.get(3).map(|meta| meta.pubkey).unwrap_or_default(),
            Pubkey::default(),
        )
    };
    let pool = accounts
        .get(bonding_curve_index)
        .map(|meta| meta.pubkey)
        .ok_or_else(|| {
            CopyTraderError::Unsupported("Pump.fun bonding curve is missing".to_owned())
        })?;
    let mut instructions = vec![create_associated_token_account_idempotent(
        &copier,
        &copier,
        &output_mint,
        &output_program,
    )];
    let _ = (base_user_ata, quote_user_ata);
    instructions.push(Instruction {
        program_id: PROGRAM_ID,
        accounts,
        data,
    });
    let market_accounts = instructions[1]
        .accounts
        .iter()
        .filter(|meta| meta.is_writable && meta.pubkey != copier)
        .map(|meta| meta.pubkey)
        .collect();
    let _ = minimum_output;
    Ok(PreparedRoute {
        dex: DexKind::PumpFun,
        pool,
        instructions,
        additional_signers: Vec::new(),
        market_accounts,
        expected_output,
        minimum_output,
        compute_unit_limit: 300_000,
    })
}

fn copy_source_sell_instruction(
    source: &SourceInstruction,
    trade: &SizedTrade,
    copier: Pubkey,
    expected_output: u64,
    minimum_output: u64,
) -> Result<PreparedRoute> {
    let AssetId::Token(base_mint) = trade.intent.input_asset else {
        return Err(CopyTraderError::Unsupported(
            "Pump.fun sell requires a token input".to_owned(),
        ));
    };
    if source.instruction.program_id != PROGRAM_ID || !is_sell(&source.instruction.data) {
        return Err(CopyTraderError::Unsupported(
            "unsupported Pump.fun sell instruction".to_owned(),
        ));
    }
    let discriminator = &source.instruction.data[..8];
    let legacy = discriminator == SELL_DISCRIMINATOR;
    let v2 = discriminator == SELL_V2_DISCRIMINATOR;
    let minimum_accounts = minimum_account_count(&source.instruction.data).ok_or_else(|| {
        CopyTraderError::Unsupported("unsupported Pump.fun sell layout".to_owned())
    })?;
    let mut accounts = source.instruction.accounts.clone();
    if accounts.len() < minimum_accounts {
        return Err(CopyTraderError::Unsupported(
            "truncated Pump.fun sell accounts".to_owned(),
        ));
    }
    let (mint_index, program_index, curve_index, user_index, base_ata_index) = if legacy {
        (2, 9, 3, 6, 5)
    } else if v2 {
        (1, 3, 10, 13, 14)
    } else {
        (1, 3, 5, 8, 9)
    };
    let base_program = accounts[program_index].pubkey;
    let source_base_ata = accounts[base_ata_index].pubkey;
    if accounts[mint_index].pubkey != base_mint
        || ![spl_token::id(), spl_token_2022::id()].contains(&base_program)
        || accounts[user_index].pubkey != source.source_wallet
        || !accounts[user_index].is_signer
        || !source
            .wallet_token_accounts
            .iter()
            .any(|(address, mint, program)| {
                *address == source_base_ata && *mint == base_mint && *program == base_program
            })
        || source_base_ata
            != associated_token_address(&source.source_wallet, &base_mint, &base_program)
    {
        return Err(CopyTraderError::Unsupported(
            "Pump.fun sell source accounts do not match the token input".to_owned(),
        ));
    }
    let output_mint = trade.intent.output_asset.routing_mint();
    let quote_program = if legacy {
        if trade.intent.output_asset != AssetId::NativeSol {
            return Err(CopyTraderError::Unsupported(
                "legacy Pump.fun sell requires native SOL output".to_owned(),
            ));
        }
        None
    } else {
        let quote_mint = accounts[2].pubkey;
        let quote_program = accounts[4].pubkey;
        if quote_mint != output_mint
            || (trade.intent.output_asset == AssetId::NativeSol
                && (quote_mint != Pubkey::from_str_const(NATIVE_MINT)
                    || quote_program != spl_token::id()))
            || ![spl_token::id(), spl_token_2022::id()].contains(&quote_program)
        {
            return Err(CopyTraderError::Unsupported(
                "Pump.fun sell quote accounts do not match the output".to_owned(),
            ));
        }
        Some(quote_program)
    };
    let curve = accounts[curve_index].pubkey;
    if curve != Pubkey::find_program_address(&[b"bonding-curve", base_mint.as_ref()], &PROGRAM_ID).0
    {
        return Err(CopyTraderError::Unsupported(
            "Pump.fun sell bonding curve does not match the input mint".to_owned(),
        ));
    }
    accounts[user_index].pubkey = copier;
    accounts[base_ata_index].pubkey = associated_token_address(&copier, &base_mint, &base_program);
    let mut instructions = Vec::new();
    if let Some(quote_program) = quote_program {
        let quote_user_index = if v2 { 15 } else { 10 };
        accounts[quote_user_index].pubkey =
            associated_token_address(&copier, &output_mint, &quote_program);
        let volume_index = if v2 { 19 } else { 11 };
        let user_volume = crate::token::accounts::user_volume_address(&PROGRAM_ID, &copier);
        accounts[volume_index].pubkey = user_volume;
        if v2 {
            accounts[20].pubkey =
                associated_token_address(&user_volume, &output_mint, &quote_program);
        }
        if trade.intent.output_asset != AssetId::NativeSol {
            instructions.push(create_associated_token_account_idempotent(
                &copier,
                &copier,
                &output_mint,
                &quote_program,
            ));
        }
    } else {
        let source_volume =
            crate::token::accounts::user_volume_address(&PROGRAM_ID, &source.source_wallet);
        let copier_volume = crate::token::accounts::user_volume_address(&PROGRAM_ID, &copier);
        for meta in &mut accounts {
            if meta.pubkey == source_volume {
                meta.pubkey = copier_volume;
            }
        }
    }
    let mut data = source.instruction.data.clone();
    data[8..16].copy_from_slice(&trade.input_amount.to_le_bytes());
    data[16..24].copy_from_slice(&minimum_output.to_le_bytes());
    let market_accounts = accounts
        .iter()
        .filter(|meta| meta.is_writable && meta.pubkey != copier)
        .map(|meta| meta.pubkey)
        .collect();
    instructions.push(Instruction {
        program_id: PROGRAM_ID,
        accounts,
        data,
    });
    Ok(PreparedRoute {
        dex: DexKind::PumpFun,
        pool: curve,
        instructions,
        additional_signers: Vec::new(),
        market_accounts,
        expected_output,
        minimum_output,
        compute_unit_limit: 300_000,
    })
}
