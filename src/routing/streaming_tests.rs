use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde_json::json;
use tokio::sync::Notify;

use super::*;
use crate::{
    config::{HttpConfig, MainnetConfig},
    domain::TradeIntent,
    http::HttpTransport,
    mainnet::MainnetClient,
    test_rpc::TestRpc,
};

struct DiscoveryGuard(Arc<AtomicBool>);
impl Drop for DiscoveryGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct StreamingAdapter {
    dex: DexKind,
    scenario: &'static str,
    gate: Arc<Notify>,
    dropped: Arc<AtomicBool>,
    preparations: Arc<AtomicUsize>,
}

#[async_trait]
impl RouteDexAdapter for StreamingAdapter {
    fn kind(&self) -> DexKind {
        self.dex
    }

    async fn discover(&self, _: &RpcClient, mints: &[Pubkey]) -> Result<Vec<PoolDescriptor>> {
        let _guard = DiscoveryGuard(self.dropped.clone());
        let first = self.dex == DexKind::PumpSwap;
        if self.scenario == "deadline"
            || (!first && matches!(self.scenario, "slow_loser" | "overlap"))
        {
            std::future::pending::<()>().await;
        }
        if !first && matches!(self.scenario, "prepare_failure" | "simulation_failure") {
            self.gate.notified().await;
        }
        if self.scenario == "all_discovery_failures"
            || (first && self.scenario == "discovery_failure")
        {
            return Err(CopyTraderError::Execution("discovery failed".to_owned()));
        }
        if self.scenario == "empty" {
            return Ok(Vec::new());
        }
        // Give every discovery a chance to start before the first result arrives.
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(vec![PoolDescriptor {
            dex: self.dex,
            address: Pubkey::new_unique(),
            mint_a: mints[0],
            mint_b: mints[1],
            liquidity_hint: 1,
        }])
    }

    async fn prepare(
        &self,
        _: &RpcClient,
        pool: &PoolDescriptor,
        _: &SizedTrade,
        _: &RouteContext,
        _: Option<&solana_sdk::account::Account>,
    ) -> Result<PreparedRoute> {
        self.preparations.fetch_add(1, Ordering::SeqCst);
        if self.scenario == "overlap" {
            self.gate.notify_one();
        }
        if self.dex == DexKind::PumpSwap && self.scenario == "prepare_failure" {
            self.gate.notify_one();
            return Err(CopyTraderError::Execution("quote failed".to_owned()));
        }
        Ok(PreparedRoute {
            dex: self.dex,
            pool: pool.address,
            instructions: Vec::new(),
            additional_signers: Vec::new(),
            market_accounts: Vec::new(),
            expected_output: 100,
            minimum_output: 90,
            compute_unit_limit: 100_000,
        })
    }
}

#[tokio::test]
async fn discoveries_stream_into_hot_path_race_and_cancel_on_success_or_deadline() {
    for scenario in [
        "slow_loser",
        "overlap",
        "prepare_failure",
        "discovery_failure",
        "all_discovery_failures",
        "empty",
        "deadline",
    ] {
        let gate = Arc::new(Notify::new());
        let server = TestRpc::start(move |request| match request["method"].as_str().unwrap() {
            "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
            "getMultipleAccounts" => json!({"test_delay_ms":if scenario == "overlap" {500} else {0}, "test_result":{"context":{"slot":42},"value":[null,null]}}),
            "getBlockHeight" => json!(100),
                "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":solana_sdk::hash::Hash::new_unique().to_string(),"lastValidBlockHeight":1000}}),
                "getFeeForMessage" => json!({"context":{"slot":42},"value":5000}),
                "getBalance" => json!({"context":{"slot":42},"value":1000000000}),
                "getTokenAccountBalance" => json!({"context":{"slot":42},"value":{"amount":"123","decimals":6,"uiAmount":0.000123,"uiAmountString":"0.000123"}}),
            "getPriorityFeeEstimate" => json!({"priorityFeeEstimate":10}),
            other => panic!("unexpected RPC {other}"),
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let backend = ExecutionBackend::Mainnet(Arc::new(MainnetClient::new(
            server.url.clone(),
            &MainnetConfig {
                source_direct: false,
                fixed_priority_fee_micro_lamports: None,
                sender_url: server.url.clone(),
                tip_lamports: 5000,
                priority_level: "High".to_owned(),
                max_priority_fee_micro_lamports: 100,
            },
            transport.clone(),
        )));
        backend
            .mainnet()
            .expect("mainnet")
            .latest_blockhash()
            .await
            .expect("warm blockhash");
        let dropped = Arc::new(AtomicBool::new(false));
        let preparations = Arc::new(AtomicUsize::new(0));
        let router = Router {
            adapters: vec![DexKind::PumpSwap, DexKind::PumpFun]
                .into_iter()
                .map(|dex| {
                    Arc::new(StreamingAdapter {
                        dex,
                        scenario,
                        gate: gate.clone(),
                        dropped: if dex == DexKind::PumpFun {
                            dropped.clone()
                        } else {
                            Arc::new(AtomicBool::new(false))
                        },
                        preparations: preparations.clone(),
                    }) as Arc<dyn RouteDexAdapter>
                })
                .collect(),
            catalog: PoolCatalog::default(),
            discovery_rpc: Arc::new(transport.solana_rpc(&server.url)),
            max_pools_per_dex: 2,
            race_timeout: if scenario == "deadline" {
                Duration::from_millis(50)
            } else {
                Duration::from_secs(2)
            },
        };
        let trade = SizedTrade {
            intent: TradeIntent {
                source_pool: None,
                source_instruction: None,
                source_signature: solana_sdk::signature::Signature::default(),
                slot: 42,
                input_asset: AssetId::Token(Pubkey::new_unique()),
                output_asset: AssetId::Token(Pubkey::new_unique()),
                source_input_amount: 100,
                source_output_amount: 100,
            },
            input_amount: 100,
        };
        let info = MintInfo {
            decimals: 6,
            token_program: spl_token::id(),
            has_transfer_fee: false,
        };
        let mut timings = RoutingTimings::default();
        let signer = Keypair::new();
        let result = router
            .race_with_timings(&trade, &signer, &backend, 100, (&info, &info), &mut timings)
            .await;
        let expected_error = match scenario {
            "deadline" => Some("DEX race timed out"),
            "empty" => Some("no direct pool"),
            "all_discovery_failures" => Some("discovery failed"),
            _ => None,
        };
        if let Some(message) = expected_error {
            assert!(
                result.err().unwrap().to_string().contains(message),
                "{scenario}"
            );
            assert_eq!(server.count("simulateTransaction"), 0);
        } else {
            let winner = result.unwrap_or_else(|error| panic!("{scenario}: {error}"));
            assert!(winner.transaction.verify().is_ok());
            assert_eq!(
                winner.route.dex,
                if matches!(scenario, "slow_loser" | "overlap") {
                    DexKind::PumpSwap
                } else {
                    DexKind::PumpFun
                }
            );
            assert_eq!(server.count("getBalance"), 0, "{scenario}");
            assert_eq!(server.count("getTokenAccountBalance"), 0, "{scenario}");
            let stages = timings.stages.snapshot();
            for stage in ["route_shared_preparation_ms", "route_quote_ms"] {
                assert!(stages.contains_key(stage), "{scenario}: {stage}");
            }
        }
        assert!(
            dropped.load(Ordering::SeqCst),
            "{scenario}: discovery was not dropped"
        );
        assert!(server.count("getMultipleAccounts") <= 1, "{scenario}");
        assert!(server.count("getLatestBlockhash") <= 1, "{scenario}");
        assert_eq!(server.count("sendTransaction"), 0);
        if scenario == "prepare_failure" {
            assert_eq!(preparations.load(Ordering::SeqCst), 2);
        }
        if scenario == "deadline" {
            assert!(timings.discovery_ms >= 40);
        }
    }
}
