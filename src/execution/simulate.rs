use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSimulateTransactionConfig;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::transaction::Transaction;

use crate::error::{CopyTraderError, Result};

#[allow(dead_code)]
pub async fn simulate_exact(rpc: &RpcClient, transaction: &Transaction) -> Result<String> {
    let response = rpc
        .simulate_transaction_with_config(
            transaction,
            RpcSimulateTransactionConfig {
                sig_verify: true,
                commitment: Some(CommitmentConfig::confirmed()),
                ..RpcSimulateTransactionConfig::default()
            },
        )
        .await
        .map_err(|error| CopyTraderError::Execution(format!("simulation RPC failed: {error}")))?;
    if let Some(error) = &response.value.err {
        return Err(CopyTraderError::Execution(format!(
            "simulation failed: {error:?}; logs: {:?}",
            response.value.logs
        )));
    }
    serde_json::to_string(&response.value).map_err(CopyTraderError::from)
}
