use std::{sync::Arc, time::Duration};

use serde::Deserialize;
use serde_json::json;
use tokio::time::MissedTickBehavior;
use url::Url;

use super::Store;
use crate::{
    error::{CopyTraderError, Result},
    http::HttpTransport,
};

const MAX_SLOT_SPAN: u64 = 512;

#[derive(sqlx::FromRow)]
pub(super) struct GapAttempt {
    pub id: i64,
    pub source_signature: String,
    pub local_signature: String,
    pub source_slot: i64,
    pub landed_slot: i64,
}

#[derive(Deserialize)]
struct Block {
    signatures: Vec<String>,
}

fn invalid() -> CopyTraderError {
    CopyTraderError::Execution("transaction gap could not be verified".to_owned())
}

fn count_between(blocks: &[Block], source: &str, copy: &str) -> Result<i64> {
    let mut source_index = None;
    let mut copy_index = None;
    let mut offset = 0usize;
    for block in blocks {
        for (index, signature) in block.signatures.iter().enumerate() {
            let position = offset.checked_add(index).ok_or_else(invalid)?;
            if signature == source && source_index.replace(position).is_some() {
                return Err(invalid());
            }
            if signature == copy && copy_index.replace(position).is_some() {
                return Err(invalid());
            }
        }
        offset = offset
            .checked_add(block.signatures.len())
            .ok_or_else(invalid)?;
    }
    let gap = copy_index
        .ok_or_else(invalid)?
        .checked_sub(source_index.ok_or_else(invalid)?)
        .and_then(|distance| distance.checked_sub(1))
        .ok_or_else(invalid)?;
    i64::try_from(gap).map_err(|_| invalid())
}

async fn lookup(transport: &HttpTransport, endpoint: &Url, row: &GapAttempt) -> Result<i64> {
    let source = u64::try_from(row.source_slot).map_err(|_| invalid())?;
    let copy = u64::try_from(row.landed_slot).map_err(|_| invalid())?;
    if copy.checked_sub(source).ok_or_else(invalid)? > MAX_SLOT_SPAN {
        return Err(invalid());
    }
    let finalized: u64 = transport
        .rpc(endpoint, "getSlot", json!([{"commitment":"finalized"}]))
        .await?;
    if finalized < copy {
        return Err(invalid());
    }
    let slots: Vec<u64> = transport
        .rpc(
            endpoint,
            "getBlocks",
            json!([source,copy,{"commitment":"finalized"}]),
        )
        .await?;
    if slots.first() != Some(&source)
        || slots.last() != Some(&copy)
        || slots.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid());
    }
    let mut blocks = Vec::with_capacity(slots.len());
    for slot in slots {
        let block: Block = transport
            .rpc(
                endpoint,
                "getBlock",
                json!([slot,{
                    "commitment":"finalized", "transactionDetails":"signatures", "rewards":false
                }]),
            )
            .await?;
        blocks.push(block);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !blocks
        .first()
        .is_some_and(|block| block.signatures.contains(&row.source_signature))
        || !blocks
            .last()
            .is_some_and(|block| block.signatures.contains(&row.local_signature))
    {
        return Err(invalid());
    }
    count_between(&blocks, &row.source_signature, &row.local_signature)
}

pub(crate) async fn run(store: Store, endpoint: Url, transport: Arc<HttpTransport>) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let Ok(rows) = store.pending_transaction_gaps().await else {
            continue;
        };
        for row in rows {
            let gap =
                tokio::time::timeout(Duration::from_secs(60), lookup(&transport, &endpoint, &row))
                    .await
                    .ok()
                    .and_then(Result::ok);
            if store.record_transaction_gap(&row, gap).await.is_err() {
                tracing::warn!(attempt_id = row.id, "transaction gap storage failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn block(signatures: &[&str]) -> Block {
        Block {
            signatures: signatures.iter().map(|s| (*s).to_owned()).collect(),
        }
    }
    #[tokio::test]
    async fn rpc_counts_produced_blocks_and_waits_for_finality() {
        let rpc =
            crate::test_rpc::TestRpc::start(|request| match request["method"].as_str().unwrap() {
                "getSlot" => json!(13),
                "getBlocks" => json!([10, 13]),
                "getBlock" => {
                    assert_eq!(request["params"][1]["transactionDetails"], "signatures");
                    assert_eq!(request["params"][1]["commitment"], "finalized");
                    if request["params"][0] == 10 {
                        json!({"signatures":["s","a"]})
                    } else {
                        json!({"signatures":["b","c"]})
                    }
                }
                _ => panic!("unexpected method"),
            })
            .await;
        let transport = HttpTransport::new(&crate::config::HttpConfig::default()).unwrap();
        let mut row = GapAttempt {
            id: 1,
            source_signature: "s".into(),
            local_signature: "c".into(),
            source_slot: 10,
            landed_slot: 13,
        };
        assert_eq!(lookup(&transport, &rpc.url, &row).await.unwrap(), 2);
        row.landed_slot = 14;
        assert!(lookup(&transport, &rpc.url, &row).await.is_err());
        assert_eq!(rpc.count("getBlocks"), 1);
        assert_eq!(rpc.count("getBlock"), 2);
    }

    #[test]
    fn same_and_cross_block_counts() {
        assert_eq!(count_between(&[block(&["s", "c"])], "s", "c").unwrap(), 0);
        assert_eq!(
            count_between(&[block(&["x", "s", "vote", "failed", "c"])], "s", "c").unwrap(),
            2
        );
        assert_eq!(
            count_between(
                &[
                    block(&["s", "a"]),
                    block(&["b", "d"]),
                    block(&["e", "c", "z"])
                ],
                "s",
                "c"
            )
            .unwrap(),
            4
        );
    }
    #[test]
    fn refuses_missing_reversed_and_duplicate_signatures() {
        for signatures in [
            &["c", "s"][..],
            &["s", "x"],
            &["s", "s", "c"],
            &["s", "c", "c"],
        ] {
            assert!(count_between(&[block(signatures)], "s", "c").is_err());
        }
    }
}
