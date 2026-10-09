use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use tokio::time::{Instant, sleep};
use tracing::debug;

use crate::error::{CopyTraderError, Result};

pub struct ConfirmedTransaction {
    pub slot: u64,
    pub error: Option<String>,
}

pub async fn wait_for_confirmation(
    rpc: &RpcClient,
    target: &str,
    signature: &Signature,
    timeout: Duration,
) -> Result<ConfirmedTransaction> {
    let deadline = Instant::now() + timeout;
    let mut polls = 0_u64;
    while Instant::now() < deadline {
        polls = polls.saturating_add(1);
        let response = rpc
            .get_signature_statuses(&[*signature])
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("confirmation query failed: {error}"))
            })?;
        if let Some(Some(status)) = response.value.first()
            && status.satisfies_commitment(CommitmentConfig::confirmed())
        {
            debug!(
                target,
                polls,
                landed_slot = status.slot,
                "transaction confirmation observed"
            );
            return Ok(ConfirmedTransaction {
                slot: status.slot,
                error: status
                    .err
                    .as_ref()
                    .map(|error| format!("{target} transaction failed: {error}")),
            });
        }
        sleep(Duration::from_millis(250)).await;
    }
    Err(CopyTraderError::Execution(format!(
        "{target} confirmation timed out"
    )))
}

pub async fn wait_for_any_confirmation(
    rpc: &RpcClient,
    signatures: &[Signature],
    timeout: Duration,
) -> Result<(Signature, ConfirmedTransaction)> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let response = tokio::time::timeout_at(
            deadline,
            rpc.get_signature_statuses_with_history(signatures),
        )
        .await;
        if let Ok(Ok(response)) = response {
            for (signature, status) in signatures.iter().zip(response.value) {
                if let Some(status) = status
                    && status.satisfies_commitment(CommitmentConfig::confirmed())
                {
                    return Ok((
                        *signature,
                        ConfirmedTransaction {
                            slot: status.slot,
                            error: status
                                .err
                                .map(|e| format!("fanout transaction failed: {e}")),
                        },
                    ));
                }
            }
        }
        // Transient errors and relay timeouts do not establish non-execution.
        sleep(Duration::from_millis(250)).await;
    }
    Err(CopyTraderError::Execution(
        "fanout confirmation timed out; nonce remains reserved until finalized advancement".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::HttpConfig, http::HttpTransport, test_rpc::TestRpc};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn fanout_confirms_second_variant_after_transient_rpc_error() {
        let polls = AtomicUsize::new(0);
        let server = TestRpc::start(move |request| {
            assert_eq!(request["params"][0].as_array().unwrap().len(), 2);
            assert_eq!(request["params"][1]["searchTransactionHistory"], true);
            if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                return json!({"error":{"code":-32000,"message":"temporary failure"}});
            }
            json!({"context":{"slot":43},"value":[null,{"slot":42,"confirmations":1,"err":null,
                "status":{"Ok":null},"confirmationStatus":"confirmed"}]})
        })
        .await;
        let rpc = HttpTransport::new(&HttpConfig::default())
            .unwrap()
            .solana_rpc(&server.url);
        let signatures = [Signature::new_unique(), Signature::new_unique()];
        let (winner, status) = wait_for_any_confirmation(&rpc, &signatures, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(winner, signatures[1]);
        assert_eq!(status.slot, 42);
    }

    #[tokio::test]
    async fn returns_transaction_slot_only_after_confirmation_including_failures() {
        for failed in [false, true] {
            let polls = AtomicUsize::new(0);
            let server = TestRpc::start(move |_| {
                let poll = polls.fetch_add(1, Ordering::SeqCst);
                let error = if failed { json!("AccountNotFound") } else { json!(null) };
                let status = if poll == 0 { json!(null) } else {
                    json!({"slot":42,"confirmations":1,"err":error,
                        "status":if failed { json!({"Err":"AccountNotFound"}) } else { json!({"Ok":null}) },
                        "confirmationStatus":if poll == 1 { "processed" } else { "confirmed" }})
                };
                json!({"context":{"slot":999},"value":[status]})
            }).await;
            let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
            let confirmation = wait_for_confirmation(
                &transport.solana_rpc(&server.url),
                "test",
                &Signature::default(),
                Duration::from_secs(3),
            )
            .await
            .expect("confirmation");
            assert_eq!(confirmation.slot, 42);
            assert_eq!(confirmation.error.is_some(), failed);
            assert_eq!(server.count("getSignatureStatuses"), 3);
        }
    }

    #[tokio::test]
    async fn absent_transaction_times_out_without_inventing_a_slot() {
        let server = TestRpc::start(|_| json!({"context":{"slot":999},"value":[null]})).await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let result = wait_for_confirmation(
            &transport.solana_rpc(&server.url),
            "test",
            &Signature::default(),
            Duration::from_millis(20),
        )
        .await;
        assert!(
            matches!(result, Err(CopyTraderError::Execution(message)) if message.contains("timed out"))
        );
    }
}
