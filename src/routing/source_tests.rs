use super::*;
use crate::domain::TradeIntent;
use solana_sdk::signature::Signature;

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
