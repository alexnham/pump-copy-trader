use crate::error::{CopyTraderError, Result};

pub(super) fn apply_slippage(amount: u64, slippage_bps: u16) -> Result<u64> {
    let retained = 10_000_u128
        .checked_sub(u128::from(slippage_bps))
        .ok_or_else(|| CopyTraderError::Execution("slippage underflow".to_owned()))?;
    let value = u128::from(amount)
        .checked_mul(retained)
        .and_then(|value| value.checked_div(10_000))
        .ok_or_else(|| CopyTraderError::Execution("slippage math overflow".to_owned()))?;
    u64::try_from(value)
        .map_err(|_| CopyTraderError::Execution("minimum output exceeds u64".to_owned()))
}
