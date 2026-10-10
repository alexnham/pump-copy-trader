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
        minimum_output_override: None,
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
    assert_eq!(u64::from_le_bytes(swap.data[8..16].try_into().unwrap()), 50);
    assert_eq!(
        u64::from_le_bytes(swap.data[16..24].try_into().unwrap()),
        100
    );
    assert_eq!(route.pool, curve);
    // A 50% slippage allowance changes requested tokens, never the spend cap.
    let (expected, minimum) = super::source_outputs(&trade, 5000).unwrap();
    let adjusted = copy_source_instruction(&source, &trade, copier, expected, minimum).unwrap();
    let buy = adjusted
        .instructions
        .iter()
        .find(|ix| ix.program_id == PROGRAM_ID)
        .unwrap();
    assert_eq!(u64::from_le_bytes(buy.data[8..16].try_into().unwrap()), 100);
    assert_eq!(
        u64::from_le_bytes(buy.data[16..24].try_into().unwrap()),
        100
    );
    // With zero slippage, the original expected token amount is requested.
    let (expected, minimum) = super::source_outputs(&trade, 0).unwrap();
    let unchanged = copy_source_instruction(&source, &trade, copier, expected, minimum).unwrap();
    let buy = unchanged
        .instructions
        .iter()
        .find(|ix| ix.program_id == PROGRAM_ID)
        .unwrap();
    assert_eq!(u64::from_le_bytes(buy.data[8..16].try_into().unwrap()), 200);
    assert_eq!(
        u64::from_le_bytes(buy.data[16..24].try_into().unwrap()),
        100
    );
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
            minimum_output_override: None,
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
        minimum_output_override: None,
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
        minimum_output_override: None,
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

#[test]
fn recorded_legacy_buy_rewrites_volume_pda_for_the_copier() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/pump_fun_legacy_buy_volume.json"
    ))
    .unwrap();
    let key = |name: &str| fixture[name].as_str().unwrap().parse::<Pubkey>().unwrap();
    let source_wallet = key("source_wallet");
    let copier = key("copier");
    let mint = key("mint");
    let accounts = fixture["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| AccountMeta {
            pubkey: a["pubkey"].as_str().unwrap().parse().unwrap(),
            is_signer: a["signer"].as_bool().unwrap(),
            is_writable: a["writable"].as_bool().unwrap(),
        })
        .collect::<Vec<_>>();
    let source = SourceInstruction {
        minimum_output_override: None,
        instruction: Instruction {
            program_id: PROGRAM_ID,
            accounts: accounts.clone(),
            data: bs58::decode(fixture["data"].as_str().unwrap())
                .into_vec()
                .unwrap(),
        },
        source_wallet,
        wallet_token_accounts: vec![(accounts[5].pubkey, mint, spl_token_2022::id())],
    };
    assert_eq!(
        crate::token::accounts::user_volume_address(&PROGRAM_ID, &source_wallet),
        key("source_volume")
    );
    assert_eq!(
        crate::token::accounts::user_volume_address(&PROGRAM_ID, &copier),
        key("copier_volume")
    );
    let trade = SizedTrade {
        intent: TradeIntent {
            source_pool: None,
            source_instruction: Some(source.clone()),
            source_signature: Default::default(),
            slot: 454960830,
            input_asset: AssetId::NativeSol,
            output_asset: AssetId::Token(mint),
            source_input_amount: 1040225003,
            source_output_amount: 31185032285426,
        },
        input_amount: 30000000,
    };
    let route =
        copy_source_instruction(&source, &trade, copier, 899373660376, 449686830188).unwrap();
    let swap = route
        .instructions
        .iter()
        .find(|ix| ix.program_id == PROGRAM_ID)
        .unwrap();
    assert_eq!(swap.accounts.len(), 18);
    assert_eq!(swap.accounts[13].pubkey, key("copier_volume"));
    assert_eq!(swap.accounts[6].pubkey, copier);
    assert_eq!(
        swap.accounts[5].pubkey,
        associated_token_address(&copier, &mint, &spl_token_2022::id())
    );
    for index in [0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17] {
        assert_eq!(swap.accounts[index], accounts[index]);
    }
    assert!(
        !swap
            .accounts
            .iter()
            .any(|a| a.pubkey == key("source_volume"))
    );
}

#[test]
fn native_sol_delta_cannot_fund_arbitrary_v2_quote_tokens() {
    let source_wallet = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    let mut data = vec![0; 24];
    data[..8].copy_from_slice(&BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR);
    let mut accounts = (0..27)
        .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
        .collect::<Vec<_>>();
    accounts[1].pubkey = mint;
    accounts[2].pubkey = Pubkey::from_str_const("8TiMkgvsrat9tM2esko8zVTt99LZLpefUM4SnZaziXaQ");
    let source = SourceInstruction {
        minimum_output_override: None,
        instruction: Instruction {
            program_id: PROGRAM_ID,
            accounts,
            data,
        },
        source_wallet,
        wallet_token_accounts: vec![],
    };
    let trade = SizedTrade {
        intent: TradeIntent {
            source_pool: None,
            source_instruction: Some(source.clone()),
            source_signature: Default::default(),
            slot: 1,
            input_asset: AssetId::NativeSol,
            output_asset: AssetId::Token(mint),
            source_input_amount: 1_000_000_000,
            source_output_amount: 1000,
        },
        input_amount: 30_000_000,
    };
    let error = copy_source_instruction(&source, &trade, Pubkey::new_unique(), 30, 15).unwrap_err();
    assert!(error.to_string().contains("funding conversion"));
}
