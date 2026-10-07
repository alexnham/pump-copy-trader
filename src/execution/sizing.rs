use crate::{
    config::{SizingConfig, TokenRule},
    domain::{AssetId, SizedTrade, SkipReason, TradeIntent},
    error::{CopyTraderError, Result},
    token::extensions::decimal_to_atomic,
};

pub struct SizingPolicy<'a> {
    sizing: &'a SizingConfig,
}

impl<'a> SizingPolicy<'a> {
    pub const fn new(sizing: &'a SizingConfig) -> Self {
        Self { sizing }
    }

    pub fn size_exit(
        &self,
        intent: TradeIntent,
        source_before: u64,
        copier_balance: u64,
    ) -> Result<std::result::Result<SizedTrade, SkipReason>> {
        if !matches!(intent.input_asset, AssetId::Token(_))
            || intent.output_asset != AssetId::NativeSol
            || source_before == 0
            || intent.source_input_amount == 0
            || intent.source_input_amount > source_before
        {
            return Err(CopyTraderError::Execution(
                "invalid source position for sell sizing".to_owned(),
            ));
        }
        if copier_balance == 0 {
            return Ok(Err(SkipReason::InsufficientBalance));
        }
        let input_amount = u128::from(copier_balance)
            .checked_mul(u128::from(intent.source_input_amount))
            .and_then(|value| value.checked_div(u128::from(source_before)))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| {
                CopyTraderError::Execution("position-relative sell sizing overflow".to_owned())
            })?;
        if input_amount == 0 {
            return Ok(Err(SkipReason::BelowMinimum));
        }
        Ok(Ok(SizedTrade {
            intent,
            input_amount,
        }))
    }

    pub fn size_trade(
        &self,
        intent: TradeIntent,
        rule: &TokenRule,
        decimals: u8,
    ) -> Result<std::result::Result<SizedTrade, SkipReason>> {
        let input_amount = match self.sizing {
            SizingConfig::Percent { percent_bps } => {
                let value = u128::from(intent.source_input_amount)
                    .checked_mul(u128::from(*percent_bps))
                    .and_then(|amount| amount.checked_div(10_000))
                    .ok_or_else(|| {
                        CopyTraderError::Execution("percentage sizing overflow".to_owned())
                    })?;
                u64::try_from(value).map_err(|_| {
                    CopyTraderError::Execution("percentage sizing exceeds u64".to_owned())
                })?
            }
            SizingConfig::Fixed { amount } => decimal_to_atomic(amount, decimals)?,
        };
        let minimum = decimal_to_atomic(&rule.minimum_input, decimals)?;
        let maximum = decimal_to_atomic(&rule.maximum_input, decimals)?;
        if input_amount < minimum {
            return Ok(Err(SkipReason::BelowMinimum));
        }
        if input_amount > maximum {
            return Ok(Err(SkipReason::AboveMaximum));
        }
        Ok(Ok(SizedTrade {
            intent,
            input_amount,
        }))
    }
}

#[cfg(test)]
mod tests {
    use solana_sdk::{pubkey::Pubkey, signature::Signature};

    use super::*;
    use crate::domain::{AssetId, TradeIntent};

    fn intent(input: u64) -> TradeIntent {
        TradeIntent {
            source_pool: None,
            source_instruction: None,
            source_signature: Signature::default(),
            slot: 1,
            input_asset: AssetId::Token(Pubkey::new_unique()),
            output_asset: AssetId::Token(Pubkey::new_unique()),
            source_input_amount: input,
            source_output_amount: 2_000_000,
        }
    }

    #[test]
    fn percentage_sizing_uses_checked_integer_math() {
        let sizing = SizingConfig::Percent { percent_bps: 100 };
        let policy = SizingPolicy::new(&sizing);
        let rule = TokenRule {
            mint: Pubkey::new_unique(),
            minimum_input: "0.001".to_owned(),
            maximum_input: "10".to_owned(),
        };
        let result = policy.size_trade(intent(1_000_000), &rule, 6);
        let sized = result.ok().and_then(|value| value.ok());
        assert_eq!(sized.as_ref().map(|value| value.input_amount), Some(10_000));
    }
    #[test]
    fn absolute_sizing_keeps_input_limits() {
        let config = SizingConfig::Percent { percent_bps: 10000 };
        let policy = SizingPolicy::new(&config);
        let rule = TokenRule {
            mint: Pubkey::new_unique(),
            minimum_input: "0.001".to_owned(),
            maximum_input: "0.1".to_owned(),
        };
        let mut buy = intent(100_000_001);
        buy.input_asset = AssetId::NativeSol;
        assert!(matches!(
            policy.size_trade(buy, &rule, 9).expect("sizing"),
            Err(SkipReason::AboveMaximum)
        ));
        assert!(matches!(
            policy
                .size_trade(intent(100_001), &rule, 6)
                .expect("sizing"),
            Err(SkipReason::AboveMaximum)
        ));
        let mut dust = intent(999);
        dust.output_asset = AssetId::NativeSol;
        assert!(matches!(
            policy.size_trade(dust, &rule, 6).expect("sizing"),
            Err(SkipReason::BelowMinimum)
        ));
    }
    #[test]
    fn exits_follow_source_fraction_without_applying_buy_size_twice() {
        for config in [
            SizingConfig::Percent { percent_bps: 100 },
            SizingConfig::Fixed {
                amount: "0.001".to_owned(),
            },
        ] {
            let policy = SizingPolicy::new(&config);
            let mut sell = intent(35_622_698_280);
            sell.output_asset = AssetId::NativeSol;
            assert_eq!(
                policy
                    .size_exit(sell.clone(), 35_622_698_280, 30_000_000_000)
                    .expect("size")
                    .expect("full exit")
                    .input_amount,
                30_000_000_000
            );
            sell.source_input_amount = 50;
            assert_eq!(
                policy
                    .size_exit(sell.clone(), 100, 81)
                    .expect("size")
                    .expect("half exit")
                    .input_amount,
                40
            );
            sell.source_input_amount = 50;
            assert_eq!(
                policy
                    .size_exit(sell, 50, 41)
                    .expect("size")
                    .expect("remaining exit")
                    .input_amount,
                41
            );
        }
    }

    #[test]
    fn exits_are_bounded_and_invalid_source_balances_are_rejected() {
        let config = SizingConfig::Percent { percent_bps: 10000 };
        let policy = SizingPolicy::new(&config);
        let mut sell = intent(1);
        sell.output_asset = AssetId::NativeSol;
        assert!(matches!(
            policy.size_exit(sell.clone(), 1, 0).expect("size"),
            Err(SkipReason::InsufficientBalance)
        ));
        assert!(matches!(
            policy.size_exit(sell.clone(), 2, 1).expect("size"),
            Err(SkipReason::BelowMinimum)
        ));
        assert_eq!(
            policy
                .size_exit(sell.clone(), 1, 1)
                .expect("size")
                .expect("dust full exit")
                .input_amount,
            1
        );
        assert!(policy.size_exit(sell.clone(), 0, 10).is_err());
        sell.source_input_amount = 2;
        assert!(policy.size_exit(sell.clone(), 1, 10).is_err());
        sell.source_input_amount = u64::MAX;
        assert_eq!(
            policy
                .size_exit(sell, u64::MAX, u64::MAX)
                .expect("size")
                .expect("large exit")
                .input_amount,
            u64::MAX
        );
    }
}
