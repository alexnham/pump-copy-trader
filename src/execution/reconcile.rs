use std::time::Duration;

use serde_json::{Value, json};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_request::RpcRequest};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio::time::{sleep, timeout};

use crate::{
    domain::AssetId,
    error::{CopyTraderError, Result},
};

pub(super) async fn received_output(
    rpc: &RpcClient,
    signature: &Signature,
    asset: AssetId,
    account: Pubkey,
    tip: u64,
    wait: Duration,
) -> Result<u64> {
    timeout(wait, async {
        loop {
            let transaction: Value = rpc.send(RpcRequest::GetTransaction, json!([
                signature.to_string(),
                {"encoding":"json", "commitment":"confirmed", "maxSupportedTransactionVersion":0}
            ])).await.map_err(|error| CopyTraderError::Execution(format!("landed transaction lookup failed: {error}")))?;
            if !transaction.is_null() {
                return output_delta(&transaction, asset, account, tip);
            }
            sleep(Duration::from_millis(250)).await;
        }
    }).await.map_err(|_| CopyTraderError::Execution("landed transaction metadata timed out".to_owned()))?
}

fn output_delta(transaction: &Value, asset: AssetId, account: Pubkey, tip: u64) -> Result<u64> {
    let invalid =
        || CopyTraderError::Execution("invalid landed output balance metadata".to_owned());
    let keys = transaction["transaction"]["message"]["accountKeys"]
        .as_array()
        .ok_or_else(invalid)?;
    let address = account.to_string();
    let index = keys
        .iter()
        .position(|key| key.as_str() == Some(address.as_str()))
        .ok_or_else(invalid)?;
    let meta = &transaction["meta"];
    if !meta.is_object() || meta.get("err") != Some(&Value::Null) {
        return Err(invalid());
    }
    let (before, after, costs) = if asset == AssetId::NativeSol {
        let before = meta["preBalances"][index].as_u64().ok_or_else(invalid)?;
        let after = meta["postBalances"][index].as_u64().ok_or_else(invalid)?;
        let costs = meta["fee"]
            .as_u64()
            .ok_or_else(invalid)?
            .checked_add(tip)
            .ok_or_else(invalid)?;
        (before, after, costs)
    } else {
        let mint = asset.routing_mint().to_string();
        let balance = |field: &str, allow_missing: bool| -> Result<u64> {
            let balances = meta[field].as_array().ok_or_else(invalid)?;
            let row = balances
                .iter()
                .find(|row| row["accountIndex"].as_u64() == u64::try_from(index).ok());
            match row {
                Some(row) if row["mint"].as_str() == Some(mint.as_str()) => {
                    row["uiTokenAmount"]["amount"]
                        .as_str()
                        .ok_or_else(invalid)?
                        .parse()
                        .map_err(|_| invalid())
                }
                None if allow_missing => Ok(0),
                _ => Err(invalid()),
            }
        };
        (
            balance("preTokenBalances", true)?,
            balance("postTokenBalances", false)?,
            0,
        )
    };
    after
        .checked_add(costs)
        .and_then(|value| value.checked_sub(before))
        .ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_rpc::TestRpc;

    #[tokio::test]
    async fn reconciles_existing_and_new_accounts_after_metadata_becomes_available() {
        for existing in [false, true] {
            let account = Pubkey::new_unique();
            let mint = Pubkey::new_unique();
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let server = TestRpc::start(move |request| {
                assert_eq!(request["method"], "getTransaction");
                if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 { return Value::Null; }
                json!({"transaction":{"message":{"accountKeys":[account.to_string()]}},"meta":{
                    "err":null,
                    "preTokenBalances":if existing { json!([{"accountIndex":0,"mint":mint.to_string(),"uiTokenAmount":{"amount":"100"}}]) } else { json!([]) },
                    "postTokenBalances":[{"accountIndex":0,"mint":mint.to_string(),"uiTokenAmount":{"amount":"150"}}]
                }})
            }).await;
            let rpc = RpcClient::new(server.url.to_string());
            let received = received_output(
                &rpc,
                &Signature::default(),
                AssetId::Token(mint),
                account,
                5000,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
            assert_eq!(received, if existing { 50 } else { 150 });
            assert_eq!(server.count("getTransaction"), 2);
            assert_eq!(server.count("getTokenAccountBalance"), 0);
        }
    }

    #[test]
    fn persistent_wsol_output_counts_only_this_trade() {
        let account = Pubkey::new_unique();
        let mint = spl_token::native_mint::id();
        let transaction = json!({"transaction":{"message":{"accountKeys":[account.to_string()]}},"meta":{
            "err":null, "fee":5000, "preBalances":[10000], "postBalances":[15000],
            "preTokenBalances":[{"accountIndex":0,"mint":mint.to_string(),"uiTokenAmount":{"amount":"1000000"}}],
            "postTokenBalances":[{"accountIndex":0,"mint":mint.to_string(),"uiTokenAmount":{"amount":"1300000"}}]
        }});
        assert_eq!(
            output_delta(&transaction, AssetId::Token(mint), account, 5000).expect("WSOL delta"),
            300000
        );
    }

    #[test]
    fn native_output_adjusts_fees_and_tip_and_rejects_missing_metadata() {
        let account = Pubkey::new_unique();
        let mut transaction = json!({"transaction":{"message":{"accountKeys":[account.to_string()]}},"meta":{"err":null,"fee":5,"preBalances":[100],"postBalances":[140]}});
        assert_eq!(
            output_delta(&transaction, AssetId::NativeSol, account, 7).unwrap(),
            52
        );
        transaction["meta"]["postBalances"] = json!([]);
        assert!(output_delta(&transaction, AssetId::NativeSol, account, 7).is_err());
    }

    #[test]
    fn rejects_wrong_mint_missing_post_balance_and_negative_delta() {
        let account = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let mut transaction = json!({"transaction":{"message":{"accountKeys":[account.to_string()]}},"meta":{"err":null,"preTokenBalances":[],"postTokenBalances":[]}});
        assert!(output_delta(&transaction, AssetId::Token(mint), account, 0).is_err());
        transaction["meta"]["postTokenBalances"] = json!([{"accountIndex":0,"mint":Pubkey::new_unique().to_string(),"uiTokenAmount":{"amount":"150"}}]);
        assert!(output_delta(&transaction, AssetId::Token(mint), account, 0).is_err());
        transaction["meta"]["postTokenBalances"][0]["mint"] = json!(mint.to_string());
        transaction["meta"]["preTokenBalances"] = transaction["meta"]["postTokenBalances"].clone();
        transaction["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("200");
        assert!(output_delta(&transaction, AssetId::Token(mint), account, 0).is_err());
    }
}
