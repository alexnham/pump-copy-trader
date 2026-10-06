use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;

use super::pump_swap::*;
use super::*;
use crate::{
    config::HttpConfig,
    domain::{SourceInstruction, SourcePool, TradeIntent},
    http::HttpTransport,
    test_rpc::TestRpc,
};
use solana_sdk::instruction::{AccountMeta, Instruction};

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

#[tokio::test]
async fn source_build_uses_only_instruction_accounts_and_copier_wsol_check() {
    use crate::{config::MainnetConfig, mainnet::MainnetClient};
    use solana_sdk::hash::Hash;

    for scenario in [
        "token",
        "native_buy",
        "native_sell",
        "persistent_wsol",
        "wsol_rpc_failure",
        "pool_mismatch",
        "missing_source",
        "unsupported_program",
        "cold_blockhash",
    ] {
        let source_wallet = Pubkey::new_unique();
        let signer = Keypair::new();
        let base = Pubkey::new_unique();
        let quote = if matches!(
            scenario,
            "native_buy" | "native_sell" | "persistent_wsol" | "wsol_rpc_failure"
        ) {
            Pubkey::from_str_const(NATIVE_MINT)
        } else {
            Pubkey::new_unique()
        };
        let pool = Pubkey::new_unique();
        let source_base = associated_token_address(&source_wallet, &base, &spl_token::id());
        let source_quote = associated_token_address(&source_wallet, &quote, &spl_token::id());
        let copier_wsol = associated_token_address(&signer.pubkey(), &quote, &spl_token::id());
        let selling = scenario == "native_sell";
        let native = quote == Pubkey::from_str_const(NATIVE_MINT);
        let hash = Hash::new_unique();
        let server = TestRpc::start(move |request| match request["method"].as_str().expect("method") {
            "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":hash.to_string(),"lastValidBlockHeight":1000}}),
            "getBlockHeight" => json!(100),
            "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
            "getAccountInfo" => {
                assert_eq!(request["params"][0], copier_wsol.to_string(), "only inspect the copier WSOL account");
                if scenario == "wsol_rpc_failure" { return json!({"error":{"code":-32602,"message":"WSOL read failed"}}); }
                let value = if scenario == "persistent_wsol" {
                    let mut data = vec![0;165]; data[..32].copy_from_slice(quote.as_ref());
                    json!({"lamports":1,"owner":spl_token::id().to_string(),"data":[STANDARD.encode(data),"base64"],"executable":false,"rentEpoch":0})
                } else { json!(null) };
                json!({"context":{"slot":42},"value":value})
            }
            method => panic!("unexpected RPC on source-direct route: {method}"),
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let client = Arc::new(MainnetClient::new(
            server.url.clone(),
            &MainnetConfig {
                source_direct: true,
                fixed_priority_fee_micro_lamports: Some(100),
                sender_url: server.url.clone(),
                tip_lamports: 5000,
                priority_level: "High".to_owned(),
                max_priority_fee_micro_lamports: 100,
            },
            transport.clone(),
        ));
        if scenario != "cold_blockhash" {
            client.latest_blockhash().await.expect("warm blockhash");
        }
        let backend = ExecutionBackend::Mainnet(client);
        let router = Router::new(
            Arc::new(transport.solana_rpc(&server.url)),
            Duration::from_secs(2),
        );
        let source_volume =
            pump_rust_client::pda::pump_amm::user_volume_accumulator(&source_wallet).0;
        let copier_volume =
            pump_rust_client::pda::pump_amm::user_volume_accumulator(&signer.pubkey()).0;
        let mut accounts = (0..24)
            .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
            .collect::<Vec<_>>();
        accounts[0] = AccountMeta::new(pool, false);
        accounts[1] = AccountMeta::new(source_wallet, true);
        accounts[5] = AccountMeta::new(source_base, false);
        accounts[6] = AccountMeta::new(source_quote, false);
        accounts[20] = AccountMeta::new(source_volume, false);
        accounts[23] = AccountMeta::new(
            associated_token_address(&source_volume, &quote, &spl_token::id()),
            false,
        );
        let discriminator = if selling {
            [194, 171, 28, 70, 104, 77, 91, 47]
        } else {
            [102, 6, 61, 18, 1, 218, 235, 234]
        };
        let source = SourceInstruction {
            instruction: Instruction {
                program_id: DexKind::PumpSwap.program_id(),
                accounts,
                data: [
                    discriminator.as_slice(),
                    &200_u64.to_le_bytes(),
                    &100_u64.to_le_bytes(),
                ]
                .concat(),
            },
            source_wallet,
            wallet_token_accounts: vec![
                (source_base, base, spl_token::id()),
                (source_quote, quote, spl_token::id()),
            ],
        };
        let mut trade = SizedTrade {
            intent: TradeIntent {
                source_pool: Some(SourcePool {
                    dex: DexKind::PumpSwap,
                    address: if scenario == "pool_mismatch" {
                        Pubkey::new_unique()
                    } else {
                        pool
                    },
                }),
                source_instruction: Some(source),
                source_signature: Default::default(),
                slot: 42,
                input_asset: if selling {
                    AssetId::Token(base)
                } else if native {
                    AssetId::NativeSol
                } else {
                    AssetId::Token(quote)
                },
                output_asset: if selling {
                    AssetId::NativeSol
                } else {
                    AssetId::Token(base)
                },
                source_input_amount: 200,
                source_output_amount: 100,
            },
            input_amount: 100,
        };
        if scenario == "missing_source" {
            trade.intent.source_instruction = None;
        }
        if scenario == "unsupported_program" {
            trade
                .intent
                .source_instruction
                .as_mut()
                .expect("source")
                .instruction
                .program_id = Pubkey::new_unique();
        }
        let result = router
            .build(
                &trade,
                &signer,
                &backend,
                100,
                &mut RoutingTimings::default(),
            )
            .await;
        if scenario == "cold_blockhash" {
            assert!(
                matches!(result, Err(CopyTraderError::Execution(message)) if message.contains("blockhash cache"))
            );
        } else if scenario == "missing_source" {
            assert!(
                matches!(result, Err(CopyTraderError::Unsupported(message)) if message.contains("source instruction"))
            );
        } else if scenario == "unsupported_program" {
            assert!(matches!(
                result,
                Err(CopyTraderError::OutOfScope(
                    crate::domain::UnsupportedReason::UnsupportedDex,
                    _
                ))
            ));
        } else if scenario == "persistent_wsol" {
            assert!(matches!(
                result,
                Err(CopyTraderError::OutOfScope(
                    crate::domain::UnsupportedReason::UnsupportedToken,
                    _
                ))
            ));
        } else if scenario == "wsol_rpc_failure" {
            assert!(
                matches!(result, Err(CopyTraderError::Execution(message)) if message.contains("WSOL"))
            );
        } else if scenario == "pool_mismatch" {
            assert!(
                matches!(result, Err(CopyTraderError::Unsupported(message)) if message.contains("does not match"))
            );
        } else {
            let winner = result.expect("uncached source pool must execute directly");
            assert_eq!(winner.route.pool, pool);
            assert!(winner.transaction.verify().is_ok());
            let instructions = &winner.route.instructions;
            let swap_index = instructions
                .iter()
                .position(|ix| ix.program_id == DexKind::PumpSwap.program_id())
                .expect("swap");
            let swap = &instructions[swap_index];
            assert_eq!(swap.accounts[0].pubkey, pool);
            assert_eq!(swap.accounts[1].pubkey, signer.pubkey());
            assert_eq!(swap.accounts[20].pubkey, copier_volume);
            assert_eq!(
                swap.accounts[23].pubkey,
                associated_token_address(&copier_volume, &quote, &spl_token::id())
            );
            assert_eq!(
                swap.accounts[5].pubkey,
                associated_token_address(&signer.pubkey(), &base, &spl_token::id())
            );
            assert_eq!(
                swap.accounts[6].pubkey,
                associated_token_address(&signer.pubkey(), &quote, &spl_token::id())
            );
            if native {
                let create_index = instructions
                    .iter()
                    .position(|ix| {
                        ix.program_id == spl_associated_token_account::id()
                            && ix.accounts[1].pubkey == copier_wsol
                    })
                    .expect("create WSOL ATA");
                assert!(create_index < swap_index);
                let close_index = instructions
                    .iter()
                    .position(|ix| ix.program_id == spl_token::id() && ix.data.first() == Some(&9))
                    .expect("close WSOL after swap");
                assert!(close_index > swap_index);
                assert_eq!(instructions[close_index].accounts[0].pubkey, copier_wsol);
                if !selling {
                    let sync_index = instructions
                        .iter()
                        .position(|ix| {
                            ix.program_id == spl_token::id() && ix.data.first() == Some(&17)
                        })
                        .expect("sync native before swap");
                    assert!(create_index < sync_index && sync_index < swap_index);
                }
            }
        }
        assert_eq!(server.count("getAccountInfo"), usize::from(native));
        for method in [
            "getProgramAccounts",
            "getMultipleAccounts",
            "simulateTransaction",
            "getPriorityFeeEstimate",
            "sendTransaction",
        ] {
            assert_eq!(server.count(method), 0, "{scenario}: {method}");
        }
    }
}
