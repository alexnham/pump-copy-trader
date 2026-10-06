use super::pump_fun::*;
use super::*;
use crate::domain::{SourceInstruction, TradeIntent};
use solana_sdk::instruction::{AccountMeta, Instruction};

#[test]
fn pump_fun_buy_rewrites_wallet_output_and_buy_arguments() {
    let source_wallet = Pubkey::new_unique();
    let copier = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    let curve = Pubkey::new_unique();
    let source_ata = associated_token_address(&source_wallet, &mint, &spl_token::id());
    let mut accounts = (0..BUY_ACCOUNT_COUNT)
        .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
        .collect::<Vec<_>>();
    accounts[3].pubkey = curve;
    accounts[5].pubkey = source_ata;
    accounts[6] = AccountMeta::new(source_wallet, true);
    let mut data = vec![0; 26];
    data[..8].copy_from_slice(&BUY_DISCRIMINATOR);
    data[8..16].copy_from_slice(&7_u64.to_le_bytes());
    data[16..24].copy_from_slice(&9_u64.to_le_bytes());
    let source = SourceInstruction {
        instruction: Instruction {
            program_id: PROGRAM_ID,
            accounts,
            data,
        },
        source_wallet,
        wallet_token_accounts: vec![(source_ata, mint, spl_token::id())],
    };
    let trade = SizedTrade {
        intent: TradeIntent {
            source_pool: None,
            source_instruction: Some(source.clone()),
            source_signature: Default::default(),
            slot: 1,
            input_asset: AssetId::NativeSol,
            output_asset: AssetId::Token(mint),
            source_input_amount: 10,
            source_output_amount: 20,
        },
        input_amount: 100,
    };
    let route = copy_source_instruction(&source, &trade, copier, 55, 50).unwrap();
    let swap = route
        .instructions
        .iter()
        .find(|instruction| instruction.program_id == PROGRAM_ID)
        .unwrap();
    assert_eq!(swap.accounts[6].pubkey, copier);
    assert_eq!(
        swap.accounts[5].pubkey,
        associated_token_address(&copier, &mint, &spl_token::id())
    );
    assert_eq!(u64::from_le_bytes(swap.data[8..16].try_into().unwrap()), 55);
    assert_eq!(
        u64::from_le_bytes(swap.data[16..24].try_into().unwrap()),
        100
    );
    assert_eq!(route.pool, curve);
}

#[test]
fn pump_fun_buy_discriminator_matches_without_argument_bytes() {
    assert!(is_buy_discriminator(BUY_V2_DISCRIMINATOR));
    assert_eq!(
        minimum_account_count_for_discriminator(BUY_V2_DISCRIMINATOR),
        Some(27)
    );
}

#[test]
fn pump_fun_rejects_unknown_layouts() {
    assert!(!is_buy(&[0; 24]));
    assert!(!is_buy(&BUY_DISCRIMINATOR[..7]));
    assert!(!is_sell(&[0; 24]));
    assert!(!is_sell(&SELL_DISCRIMINATOR[..7]));
}

#[test]
fn pump_fun_sell_rewrites_legacy_v2_and_v3_accounts_and_minimum_output() {
    for (discriminator, count, mint_index, program_index, curve_index, user_index, ata_index) in [
        (SELL_DISCRIMINATOR, 14, 2, 9, 3, 6, 5),
        (SELL_V2_DISCRIMINATOR, 26, 1, 3, 10, 13, 14),
        (SELL_V3_DISCRIMINATOR, 17, 1, 3, 5, 8, 9),
    ] {
        let source_wallet = Pubkey::new_unique();
        let copier = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let curve = Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &PROGRAM_ID).0;
        let source_ata = associated_token_address(&source_wallet, &mint, &spl_token::id());
        let mut accounts = (0..count)
            .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
            .collect::<Vec<_>>();
        accounts[mint_index].pubkey = mint;
        accounts[program_index].pubkey = spl_token::id();
        accounts[curve_index].pubkey = curve;
        accounts[user_index] = AccountMeta::new(source_wallet, true);
        accounts[ata_index].pubkey = source_ata;
        if discriminator != SELL_DISCRIMINATOR {
            accounts[2].pubkey = Pubkey::from_str_const(NATIVE_MINT);
            accounts[4].pubkey = spl_token::id();
        }
        let source_volume = Pubkey::find_program_address(
            &[b"user_volume_accumulator", source_wallet.as_ref()],
            &PROGRAM_ID,
        )
        .0;
        if discriminator == SELL_V2_DISCRIMINATOR {
            accounts[19].pubkey = source_volume;
        } else if discriminator == SELL_V3_DISCRIMINATOR {
            accounts[11].pubkey = source_volume;
        }
        let source = SourceInstruction {
            instruction: Instruction {
                program_id: PROGRAM_ID,
                accounts,
                data: [discriminator.as_slice(), &[0; 16]].concat(),
            },
            source_wallet,
            wallet_token_accounts: vec![(source_ata, mint, spl_token::id())],
        };
        let trade = SizedTrade {
            intent: TradeIntent {
                source_pool: None,
                source_instruction: Some(source.clone()),
                source_signature: Default::default(),
                slot: 1,
                input_asset: AssetId::Token(mint),
                output_asset: AssetId::NativeSol,
                source_input_amount: 10,
                source_output_amount: 20,
            },
            input_amount: 40,
        };
        let route = copy_source_instruction(&source, &trade, copier, 80, 60).unwrap();
        let sell = route.instructions.last().unwrap();
        assert_eq!(route.instructions.len(), 1);
        assert_eq!(route.pool, curve);
        assert_eq!(sell.accounts[user_index].pubkey, copier);
        assert_eq!(
            sell.accounts[ata_index].pubkey,
            associated_token_address(&copier, &mint, &spl_token::id())
        );
        assert_eq!(u64::from_le_bytes(sell.data[8..16].try_into().unwrap()), 40);
        assert_eq!(
            u64::from_le_bytes(sell.data[16..24].try_into().unwrap()),
            60
        );
        if discriminator == SELL_V2_DISCRIMINATOR {
            assert_eq!(
                sell.accounts[19].pubkey,
                Pubkey::find_program_address(
                    &[b"user_volume_accumulator", copier.as_ref()],
                    &PROGRAM_ID
                )
                .0
            );
        } else if discriminator == SELL_V3_DISCRIMINATOR {
            assert_eq!(
                sell.accounts[11].pubkey,
                Pubkey::find_program_address(
                    &[b"user_volume_accumulator", copier.as_ref()],
                    &PROGRAM_ID
                )
                .0
            );
        }
    }
}

#[test]
fn pump_fun_sell_rejects_mismatched_source_mint() {
    let source_wallet = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    let other_mint = Pubkey::new_unique();
    let mut accounts = (0..14)
        .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
        .collect::<Vec<_>>();
    accounts[2].pubkey = other_mint;
    accounts[3].pubkey =
        Pubkey::find_program_address(&[b"bonding-curve", other_mint.as_ref()], &PROGRAM_ID).0;
    accounts[5].pubkey = associated_token_address(&source_wallet, &other_mint, &spl_token::id());
    accounts[6] = AccountMeta::new(source_wallet, true);
    accounts[9].pubkey = spl_token::id();
    let source = SourceInstruction {
        instruction: Instruction {
            program_id: PROGRAM_ID,
            accounts,
            data: [SELL_DISCRIMINATOR.as_slice(), &[0; 16]].concat(),
        },
        source_wallet,
        wallet_token_accounts: vec![(
            associated_token_address(&source_wallet, &other_mint, &spl_token::id()),
            other_mint,
            spl_token::id(),
        )],
    };
    let trade = SizedTrade {
        intent: TradeIntent {
            source_pool: None,
            source_instruction: Some(source.clone()),
            source_signature: Default::default(),
            slot: 1,
            input_asset: AssetId::Token(mint),
            output_asset: AssetId::NativeSol,
            source_input_amount: 10,
            source_output_amount: 20,
        },
        input_amount: 40,
    };
    assert!(matches!(
        copy_source_instruction(&source, &trade, Pubkey::new_unique(), 80, 60),
        Err(CopyTraderError::Unsupported(_))
    ));
}

#[test]
fn pump_fun_sell_v2_creates_copier_quote_ata_for_token_quote() {
    let source_wallet = Pubkey::new_unique();
    let copier = Pubkey::new_unique();
    let base_mint = Pubkey::new_unique();
    let quote_mint = Pubkey::new_unique();
    let curve =
        Pubkey::find_program_address(&[b"bonding-curve", base_mint.as_ref()], &PROGRAM_ID).0;
    let base_ata = associated_token_address(&source_wallet, &base_mint, &spl_token_2022::id());
    let mut accounts = (0..26)
        .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
        .collect::<Vec<_>>();
    accounts[1].pubkey = base_mint;
    accounts[2].pubkey = quote_mint;
    accounts[3].pubkey = spl_token_2022::id();
    accounts[4].pubkey = spl_token::id();
    accounts[10].pubkey = curve;
    accounts[13] = AccountMeta::new(source_wallet, true);
    accounts[14].pubkey = base_ata;
    let source = SourceInstruction {
        instruction: Instruction {
            program_id: PROGRAM_ID,
            accounts,
            data: [SELL_V2_DISCRIMINATOR.as_slice(), &[0; 16]].concat(),
        },
        source_wallet,
        wallet_token_accounts: vec![(base_ata, base_mint, spl_token_2022::id())],
    };
    let trade = SizedTrade {
        intent: TradeIntent {
            source_pool: None,
            source_instruction: Some(source.clone()),
            source_signature: Default::default(),
            slot: 1,
            input_asset: AssetId::Token(base_mint),
            output_asset: AssetId::Token(quote_mint),
            source_input_amount: 10,
            source_output_amount: 20,
        },
        input_amount: 40,
    };
    let route = copy_source_instruction(&source, &trade, copier, 80, 60).unwrap();
    assert_eq!(route.instructions.len(), 2);
    assert_eq!(
        route.instructions[0].program_id,
        spl_associated_token_account::id()
    );
    let sell = &route.instructions[1];
    assert_eq!(
        sell.accounts[15].pubkey,
        associated_token_address(&copier, &quote_mint, &spl_token::id())
    );
    let volume =
        Pubkey::find_program_address(&[b"user_volume_accumulator", copier.as_ref()], &PROGRAM_ID).0;
    assert_eq!(sell.accounts[19].pubkey, volume);
    assert_eq!(
        sell.accounts[20].pubkey,
        associated_token_address(&volume, &quote_mint, &spl_token::id())
    );
}
