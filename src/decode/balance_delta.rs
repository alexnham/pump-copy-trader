use std::{collections::HashMap, str::FromStr};

use solana_sdk::pubkey::Pubkey;

use crate::{
    domain::{AssetId, TransactionMeta, UiTokenBalance},
    error::{CopyTraderError, Result},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletSwapDelta {
    pub input_asset: AssetId,
    pub output_asset: AssetId,
    pub input_amount: u64,
    pub output_amount: u64,
}

pub fn wallet_swap_delta(
    meta: &TransactionMeta,
    wallet: &Pubkey,
    wallet_index: usize,
    native_adjustment: u64,
) -> Result<WalletSwapDelta> {
    let pre = owned_balances(meta.pre_token_balances.as_deref(), wallet)?;
    let post = owned_balances(meta.post_token_balances.as_deref(), wallet)?;
    let mut debits = Vec::new();
    let mut credits = Vec::new();
    let mints = pre
        .keys()
        .chain(post.keys())
        .copied()
        .collect::<std::collections::HashSet<_>>();

    for mint in mints {
        let before = pre.get(&mint).copied().unwrap_or_default();
        let after = post.get(&mint).copied().unwrap_or_default();
        if before > after {
            debits.push((AssetId::Token(mint), before - after));
        } else if after > before {
            credits.push((AssetId::Token(mint), after - before));
        }
    }

    if let Some(delta) = native_delta(meta, wallet_index, native_adjustment)? {
        if delta < 0 {
            debits.push((
                AssetId::NativeSol,
                checked_u64(delta.unsigned_abs(), "native SOL debit")?,
            ));
        } else if delta > 0 {
            credits.push((
                AssetId::NativeSol,
                checked_u64(delta as u128, "native SOL credit")?,
            ));
        }
    }

    if debits.len() != 1 || credits.len() != 1 {
        return Err(CopyTraderError::Unsupported(format!(
            "wallet delta is ambiguous: {} debits and {} credits",
            debits.len(),
            credits.len()
        )));
    }
    let (input_asset, input_amount) = debits.remove(0);
    let (output_asset, output_amount) = credits.remove(0);
    if input_asset.routing_mint() == output_asset.routing_mint() {
        return Err(CopyTraderError::Unsupported(
            "input and output assets resolve to the same mint".to_owned(),
        ));
    }
    Ok(WalletSwapDelta {
        input_asset,
        output_asset,
        input_amount,
        output_amount,
    })
}

fn native_delta(
    meta: &TransactionMeta,
    wallet_index: usize,
    native_adjustment: u64,
) -> Result<Option<i128>> {
    let Some(before) = meta.pre_balances.get(wallet_index).copied() else {
        return Ok(None);
    };
    let Some(after) = meta.post_balances.get(wallet_index).copied() else {
        return Ok(None);
    };
    let fee = if wallet_index == 0 { meta.fee } else { 0 };
    let economic_after = u128::from(after)
        .checked_add(u128::from(fee))
        .and_then(|value| value.checked_add(u128::from(native_adjustment)))
        .ok_or_else(|| CopyTraderError::Decode("native balance correction overflow".to_owned()))?;
    let before = i128::from(before);
    let after = i128::try_from(economic_after)
        .map_err(|_| CopyTraderError::Decode("native post-balance exceeds i128".to_owned()))?;
    Ok(Some(after - before))
}

fn checked_u64(value: u128, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| CopyTraderError::Decode(format!("{field} exceeds u64")))
}

fn owned_balances(
    balances: Option<&[UiTokenBalance]>,
    wallet: &Pubkey,
) -> Result<HashMap<Pubkey, u64>> {
    let mut result = HashMap::new();
    let wallet = wallet.to_string();
    for balance in balances.unwrap_or_default() {
        if balance.owner.as_deref() != Some(wallet.as_str()) {
            continue;
        }
        let mint = Pubkey::from_str(&balance.mint).map_err(|error| {
            CopyTraderError::Decode(format!("invalid token-balance mint: {error}"))
        })?;
        let amount = balance
            .ui_token_amount
            .amount
            .parse::<u64>()
            .map_err(|error| {
                CopyTraderError::Decode(format!("invalid raw token amount: {error}"))
            })?;
        let total = result.entry(mint).or_insert(0_u64);
        *total = total.checked_add(amount).ok_or_else(|| {
            CopyTraderError::Decode("aggregated token balance overflow".to_owned())
        })?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::UiTokenAmount;

    fn token(index: u8, owner: Pubkey, mint: Pubkey, amount: u64) -> UiTokenBalance {
        UiTokenBalance {
            account_index: index,
            mint: mint.to_string(),
            ui_token_amount: UiTokenAmount {
                amount: amount.to_string(),
                decimals: 6,
            },
            owner: Some(owner.to_string()),
            program_id: None,
        }
    }

    #[test]
    fn aggregates_accounts_and_corrects_fee_paid_native_input() {
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let meta = TransactionMeta {
            pre_token_balances: Some(vec![token(1, wallet, mint, 1), token(2, wallet, mint, 2)]),
            post_token_balances: Some(vec![token(1, wallet, mint, 5), token(2, wallet, mint, 8)]),
            pre_balances: vec![2_000_000],
            post_balances: vec![994_000],
            fee: 6_000,
            ..TransactionMeta::default()
        };
        assert_eq!(
            wallet_swap_delta(&meta, &wallet, 0, 0).ok(),
            Some(WalletSwapDelta {
                input_asset: AssetId::NativeSol,
                output_asset: AssetId::Token(mint),
                input_amount: 1_000_000,
                output_amount: 10,
            })
        );
    }

    #[test]
    fn rejects_multiple_economic_credits() {
        let wallet = Pubkey::new_unique();
        let input = Pubkey::new_unique();
        let output_a = Pubkey::new_unique();
        let output_b = Pubkey::new_unique();
        let meta = TransactionMeta {
            pre_token_balances: Some(vec![token(1, wallet, input, 100)]),
            post_token_balances: Some(vec![
                token(1, wallet, input, 50),
                token(2, wallet, output_a, 1),
                token(3, wallet, output_b, 1),
            ]),
            ..TransactionMeta::default()
        };
        assert!(wallet_swap_delta(&meta, &wallet, 0, 0).is_err());
    }
}
