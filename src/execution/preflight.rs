use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{message::Message, pubkey::Pubkey};

use crate::{
    domain::AssetId,
    error::{CopyTraderError, Result},
};

#[allow(dead_code)]
pub(crate) async fn transaction_fee(rpc: &RpcClient, message: &Message) -> Result<u64> {
    rpc.get_fee_for_message(message).await.map_err(|error| {
        CopyTraderError::Execution(format!(
            "failed to calculate mainnet transaction fee: {error}"
        ))
    })
}

#[allow(dead_code)]
pub(crate) async fn read_balances(
    rpc: &RpcClient,
    owner: &Pubkey,
    output_asset: AssetId,
    output_account: &Pubkey,
) -> Result<(u64, u64)> {
    let (available_sol, token_output) = tokio::try_join!(
        async {
            rpc.get_balance(owner).await.map_err(|error| {
                CopyTraderError::Execution(format!("failed to read mainnet SOL balance: {error}"))
            })
        },
        async {
            if matches!(output_asset, AssetId::NativeSol) {
                return Ok(0);
            }
            // Preserve the existing missing-output-account baseline of zero.
            let balance = rpc
                .get_token_account_balance(output_account)
                .await
                .ok()
                .and_then(|value| value.amount.parse::<u64>().ok())
                .unwrap_or(0);
            Ok::<_, CopyTraderError>(balance)
        }
    )?;
    let output_before = if matches!(output_asset, AssetId::NativeSol) {
        available_sol
    } else {
        token_output
    };
    Ok((available_sol, output_before))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::HttpConfig, http::HttpTransport, test_rpc::TestRpc};
    use serde_json::json;

    #[tokio::test]
    async fn native_output_reuses_the_checked_sol_balance() {
        let server = TestRpc::start(|request| {
            assert_eq!(request["method"], "getBalance");
            json!({"context":{"slot":42},"value":123456})
        })
        .await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let owner = Pubkey::new_unique();
        let balances = read_balances(
            &transport.solana_rpc(&server.url),
            &owner,
            AssetId::NativeSol,
            &owner,
        )
        .await
        .unwrap();
        assert_eq!(balances, (123456, 123456));
        assert_eq!(server.count("getBalance"), 1);
        assert_eq!(server.count("getTokenAccountBalance"), 0);
        assert_eq!(server.count("sendTransaction"), 0);
    }
}
