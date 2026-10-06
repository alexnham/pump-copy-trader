use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;

use super::pump_swap::*;
use super::*;
use crate::{
    config::HttpConfig,
    domain::{SourceInstruction, TradeIntent},
    http::HttpTransport,
    test_rpc::TestRpc,
};
use solana_sdk::account::Account;
use solana_sdk::instruction::{AccountMeta, Instruction};

fn account(data: Vec<u8>, owner: Pubkey) -> Account {
    Account {
        data,
        owner,
        lamports: 1,
        executable: false,
        rent_epoch: 0,
    }
}

fn mint(program: Pubkey) -> Account {
    let mut data = vec![0; 82];
    data[36..44].copy_from_slice(&1_000_000_000_u64.to_le_bytes());
    data[44] = 6;
    data[45] = 1;
    account(data, program)
}

fn vault(mint: Pubkey, authority: Pubkey, program: Pubkey) -> Account {
    let mut data = vec![0; 165];
    data[..32].copy_from_slice(mint.as_ref());
    data[32..64].copy_from_slice(authority.as_ref());
    data[64..72].copy_from_slice(&1_000_000_000_u64.to_le_bytes());
    data[108] = 1;
    account(data, program)
}

#[tokio::test]
async fn pump_swap_batches_state_and_preserves_wsol_and_account_checks() {
    for scenario in [
        "buy",
        "fast",
        "sell",
        "token_2022",
        "existing_wsol",
        "missing_vault",
        "wrong_vault_mint",
        "wrong_vault_owner",
        "truncated_mint",
        "rpc_failure",
    ] {
        let pool_address = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str_const(crate::domain::NATIVE_MINT);
        let copier = Pubkey::new_unique();
        let program = if scenario == "token_2022" {
            spl_token_2022::id()
        } else {
            spl_token::id()
        };
        let base_vault = associated_token_address(&pool_address, &base, &program);
        let quote_vault = associated_token_address(&pool_address, &quote, &spl_token::id());
        let mut pool_data = vec![0; 400];
        pool_data[..8].copy_from_slice(&[241, 154, 109, 4, 17, 177, 109, 188]);
        for (offset, key) in [
            (43, base),
            (75, quote),
            (139, base_vault),
            (171, quote_vault),
        ] {
            pool_data[offset..offset + 32].copy_from_slice(key.as_ref());
        }

        let pool_account = account(pool_data, DexKind::PumpSwap.program_id());
        let mut global_data = vec![0; 1024];
        global_data[..8].copy_from_slice(&[149, 8, 156, 202, 160, 252, 176, 217]);
        for start in [57, 643] {
            for index in 0..8 {
                let offset = start + index * 32;
                global_data[offset..offset + 32].copy_from_slice(Pubkey::new_unique().as_ref());
            }
        }
        let mut fee_data = vec![0; 128];
        fee_data[..8].copy_from_slice(&[143, 52, 146, 187, 219, 123, 76, 155]);
        let mut base_account = mint(program);
        if scenario == "truncated_mint" {
            base_account.data.truncate(40);
        }
        let mut base_vault_account = vault(base, pool_address, program);
        if scenario == "wrong_vault_mint" {
            base_vault_account.data[..32].fill(0);
        }
        if scenario == "wrong_vault_owner" {
            base_vault_account.owner = Pubkey::new_unique();
        }
        let states = [
            Some(account(global_data, DexKind::PumpSwap.program_id())),
            Some(account(fee_data, Pubkey::new_unique())),
            Some(base_account),
            Some(mint(spl_token::id())),
            if scenario == "missing_vault" {
                None
            } else {
                Some(base_vault_account)
            },
            Some(vault(quote, pool_address, spl_token::id())),
            if scenario == "existing_wsol" {
                Some(vault(quote, copier, spl_token::id()))
            } else {
                None
            },
        ];
        let server = TestRpc::start(move |request| {
            match request["method"].as_str().unwrap() {
                "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
                "getMultipleAccounts" => {
                    assert_eq!(request["params"][0].as_array().unwrap().len(), 7);
                    assert_eq!(request["params"][0][4], base_vault.to_string());
                    assert_eq!(request["params"][0][5], quote_vault.to_string());
                    if scenario == "rpc_failure" { return json!({"error":{"code":-32602,"message":"state read failed"}}); }
                    let values = states.iter().map(|state| state.as_ref().map(|account| json!({"lamports":1,"owner":account.owner.to_string(),"data":[STANDARD.encode(&account.data),"base64"],"executable":false,"rentEpoch":0}))).collect::<Vec<_>>();
                    json!({"context":{"slot":42},"value":values})
                }
                other => panic!("unexpected RPC {other}"),
            }
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let trade = SizedTrade {
            intent: TradeIntent {
                source_pool: None,
                source_instruction: None,
                source_signature: solana_sdk::signature::Signature::default(),
                slot: 42,
                input_asset: if scenario == "sell" {
                    AssetId::Token(base)
                } else {
                    AssetId::NativeSol
                },
                output_asset: if scenario == "sell" {
                    AssetId::NativeSol
                } else {
                    AssetId::Token(base)
                },
                source_input_amount: 100_000,
                source_output_amount: 100_000,
            },
            input_amount: 10_000,
        };
        let result = PumpSwapAdapter
            .prepare(
                &transport.solana_rpc(&server.url),
                &PoolDescriptor {
                    dex: DexKind::PumpSwap,
                    address: pool_address,
                    mint_a: base,
                    mint_b: quote,
                    liquidity_hint: 0,
                },
                &trade,
                &RouteContext {
                    copier,
                    slippage_bps: 100,
                    source_outputs: (scenario == "fast").then_some((10000, 5000)),
                },
                Some(&pool_account),
            )
            .await;
        if matches!(scenario, "buy" | "sell" | "token_2022" | "fast") {
            let route = result.unwrap_or_else(|error| panic!("{scenario}: {error}"));
            if scenario == "fast" {
                assert_eq!((route.expected_output, route.minimum_output), (10000, 5000));
            }
            assert!(route.expected_output > 0);
            assert!(route.minimum_output <= route.expected_output);
            assert!(!route.instructions.is_empty());
        } else {
            let error = result.expect_err(scenario).to_string();
            if scenario == "existing_wsol" {
                assert!(error.contains("persistent WSOL"), "{error}");
            }
        }
        assert_eq!(server.count("getMultipleAccounts"), 1, "{scenario}");
        assert_eq!(server.count("getAccountInfo"), 0);
        assert_eq!(server.count("getTokenSupply"), 0);
        assert_eq!(server.count("getTokenAccountBalance"), 0);
        assert_eq!(server.count("sendTransaction"), 0);
    }
}

#[test]
fn pump_swap_source_copy_rewrites_wallet_accounts_and_amounts() {
    let source_wallet = Pubkey::new_unique();
    let copier = Pubkey::new_unique();
    let pool = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    let output_mint = Pubkey::new_unique();
    let source_ata = associated_token_address(&source_wallet, &mint, &spl_token::id());
    let source_output_ata =
        associated_token_address(&source_wallet, &output_mint, &spl_token::id());
    let instruction = SourceInstruction {
        instruction: Instruction {
            program_id: DexKind::PumpSwap.program_id(),
            accounts: vec![
                AccountMeta::new(pool, false),
                AccountMeta::new(source_ata, false),
                AccountMeta::new(source_output_ata, false),
                AccountMeta::new(source_wallet, true),
            ],
            data: [
                [102, 6, 61, 18, 1, 218, 235, 234].as_slice(),
                &100_u64.to_le_bytes(),
                &200_u64.to_le_bytes(),
            ]
            .concat(),
        },
        source_wallet,
        wallet_token_accounts: vec![
            (source_ata, mint, spl_token::id()),
            (source_output_ata, output_mint, spl_token::id()),
        ],
    };
    let trade = SizedTrade {
        intent: TradeIntent {
            source_pool: Some(SourcePool {
                dex: DexKind::PumpSwap,
                address: pool,
            }),
            source_instruction: Some(instruction.clone()),
            source_signature: solana_sdk::signature::Signature::default(),
            slot: 1,
            input_asset: AssetId::Token(mint),
            output_asset: AssetId::Token(output_mint),
            source_input_amount: 200,
            source_output_amount: 100,
        },
        input_amount: 77,
    };
    let route = copy_source_instruction(&instruction, &trade, copier, 31, 29).unwrap();
    let copied = route
        .instructions
        .iter()
        .find(|instruction| instruction.program_id == DexKind::PumpSwap.program_id())
        .unwrap();
    assert_eq!(
        copied.accounts[1].pubkey,
        associated_token_address(&copier, &mint, &spl_token::id())
    );
    assert_eq!(
        copied.accounts[2].pubkey,
        associated_token_address(&copier, &output_mint, &spl_token::id())
    );
    assert_eq!(copied.accounts[3].pubkey, copier);
    assert_eq!(
        u64::from_le_bytes(copied.data[8..16].try_into().unwrap()),
        31
    );
    assert_eq!(
        u64::from_le_bytes(copied.data[16..24].try_into().unwrap()),
        77
    );
}

#[test]
fn pump_swap_source_copy_supports_native_buy_and_sell() {
    for (input, output, discriminator) in [
        (
            AssetId::NativeSol,
            AssetId::Token(Pubkey::new_unique()),
            [102, 6, 61, 18, 1, 218, 235, 234],
        ),
        (
            AssetId::Token(Pubkey::new_unique()),
            AssetId::NativeSol,
            [194, 171, 28, 70, 104, 77, 91, 47],
        ),
    ] {
        let source_wallet = Pubkey::new_unique();
        let copier = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint = if let AssetId::Token(mint) = input {
            mint
        } else {
            output.routing_mint()
        };
        let source_wsol = associated_token_address(
            &source_wallet,
            &Pubkey::from_str_const(NATIVE_MINT),
            &spl_token::id(),
        );
        let source_ata = associated_token_address(&source_wallet, &mint, &spl_token::id());
        let wallet_account = if input == AssetId::NativeSol {
            source_wsol
        } else {
            source_ata
        };
        let source_instruction = SourceInstruction {
            instruction: Instruction {
                program_id: DexKind::PumpSwap.program_id(),
                accounts: vec![
                    AccountMeta::new(pool, false),
                    AccountMeta::new(wallet_account, false),
                    AccountMeta::new(source_wsol, false),
                    AccountMeta::new(source_ata, false),
                    AccountMeta::new(source_wallet, true),
                ],
                data: [
                    discriminator.as_slice(),
                    &100_u64.to_le_bytes(),
                    &200_u64.to_le_bytes(),
                ]
                .concat(),
            },
            source_wallet,
            wallet_token_accounts: vec![
                (
                    wallet_account,
                    if input == AssetId::NativeSol {
                        Pubkey::from_str_const(NATIVE_MINT)
                    } else {
                        mint
                    },
                    spl_token::id(),
                ),
                (
                    source_wsol,
                    Pubkey::from_str_const(NATIVE_MINT),
                    spl_token::id(),
                ),
                (source_ata, mint, spl_token::id()),
            ],
        };
        let trade = SizedTrade {
            intent: TradeIntent {
                source_pool: Some(SourcePool {
                    dex: DexKind::PumpSwap,
                    address: pool,
                }),
                source_instruction: Some(source_instruction.clone()),
                source_signature: solana_sdk::signature::Signature::default(),
                slot: 1,
                input_asset: input,
                output_asset: output,
                source_input_amount: 200,
                source_output_amount: 100,
            },
            input_amount: 77,
        };
        let route = copy_source_instruction(&source_instruction, &trade, copier, 31, 29).unwrap();
        let copied = route
            .instructions
            .iter()
            .find(|instruction| instruction.program_id == DexKind::PumpSwap.program_id())
            .unwrap();
        assert!(
            copied
                .accounts
                .iter()
                .any(|account| account.pubkey == copier)
        );
        assert!(copied.accounts.iter().any(|account| {
            account.pubkey
                == associated_token_address(
                    &copier,
                    &Pubkey::from_str_const(NATIVE_MINT),
                    &spl_token::id(),
                )
        }));
        if output == AssetId::NativeSol {
            assert_eq!(
                route.instructions.last().unwrap().program_id,
                spl_token::id()
            );
        } else {
            assert!(route.instructions.iter().any(|instruction| {
                instruction.program_id == spl_associated_token_account::id()
            }));
        }
    }
}
