use crate::{
    config::{SizingConfig, TokenRule},
    domain::{SizedTrade, SkipReason, TradeIntent},
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
}
