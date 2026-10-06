use serde_json::json;
use solana_sdk::signature::Signature;

use super::*;
use crate::{
    config::{HttpConfig, MainnetConfig},
    domain::TradeIntent,
    http::HttpTransport,
    mainnet::MainnetClient,
    test_rpc::TestRpc,
};

#[tokio::test]
async fn discovery_is_bounded_by_route_deadline_and_records_cancelled_time() {
    let server = TestRpc::start(|request| match request["method"].as_str().unwrap() {
        "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
        _ => json!({"test_delay_ms":1000,"test_result":[]}),
    })
    .await;
    let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
    let router = Router::supported(
        Arc::new(transport.solana_rpc(&server.url)),
        4,
        Duration::from_millis(50),
    );
    let backend = ExecutionBackend::Mainnet(Arc::new(MainnetClient::new(
        server.url.clone(),
        &MainnetConfig {
            source_direct: false,
            fixed_priority_fee_micro_lamports: None,
            sender_url: server.url.clone(),
            tip_lamports: 5000,
            priority_level: "high".to_owned(),
            max_priority_fee_micro_lamports: 100,
        },
        transport,
    )));
    let trade = SizedTrade {
        intent: TradeIntent {
            source_pool: None,
            source_instruction: None,
            source_signature: Signature::default(),
            slot: 42,
            input_asset: AssetId::Token(Pubkey::new_unique()),
            output_asset: AssetId::Token(Pubkey::new_unique()),
            source_input_amount: 100,
            source_output_amount: 100,
        },
        input_amount: 100,
    };
    let info = mint_info();
    let mut timings = RoutingTimings::default();
    let result = router
        .race_with_timings(
            &trade,
            &Keypair::new(),
            &backend,
            100,
            (&info, &info),
            &mut timings,
        )
        .await;
    assert!(
        result
            .err()
            .expect("timeout")
            .to_string()
            .contains("timed out")
    );
    assert!(timings.discovery_ms >= 40);
    assert_eq!(server.count("simulateTransaction"), 0);
}

#[tokio::test]
async fn refresh_preserves_learned_pairs_and_remember_preserves_alternatives() {
    let catalog = PoolCatalog::default();
    let input = Pubkey::new_unique();
    let output = Pubkey::new_unique();
    let learned = PoolDescriptor {
        dex: DexKind::PumpSwap,
        address: Pubkey::new_unique(),
        mint_a: input,
        mint_b: output,
        liquidity_hint: 0,
    };
    let mut alternative = learned.clone();
    alternative.address = Pubkey::new_unique();
    catalog.remember(alternative.clone(), 4).await;
    catalog.remember(learned.clone(), 4).await;
    catalog.remember(learned.clone(), 4).await;
    catalog
        .refresh(
            DexKind::PumpSwap,
            &[Pubkey::new_unique(), Pubkey::new_unique()],
            vec![],
        )
        .await;
    let candidates = catalog
        .candidates(DexKind::PumpSwap, output, input, 4)
        .await;
    assert_eq!(candidates.len(), 2);
    assert!(candidates.contains(&learned));
    assert!(candidates.contains(&alternative));
    catalog
        .refresh(DexKind::PumpSwap, &[input, output], vec![])
        .await;
    assert!(!catalog.contains_pair(input, output).await);
}

#[test]
fn source_price_minimum_is_scaled_and_checked() {
    let mut trade = SizedTrade {
        input_amount: 1000,
        intent: TradeIntent {
            source_pool: None,
            source_instruction: None,
            source_signature: Signature::default(),
            slot: 42,
            input_asset: AssetId::Token(Pubkey::new_unique()),
            output_asset: AssetId::Token(Pubkey::new_unique()),
            source_input_amount: 10000,
            source_output_amount: 8000,
        },
    };
    assert_eq!(super::source_outputs(&trade, 5000).unwrap(), (800, 400));
    assert!(super::source_outputs(&trade, 10001).is_err());
    trade.intent.source_input_amount = 0;
    assert!(super::source_outputs(&trade, 5000).is_err());
    trade.intent.source_input_amount = 1;
    trade.intent.source_output_amount = u64::MAX;
    trade.input_amount = u64::MAX;
    assert!(super::source_outputs(&trade, 5000).is_err());
    trade.input_amount = 0;
    assert!(super::source_outputs(&trade, 5000).is_err());
}

fn mint_info() -> MintInfo {
    MintInfo {
        decimals: 6,
        token_program: spl_token::id(),
        has_transfer_fee: false,
    }
}
