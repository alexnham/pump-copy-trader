use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use tokio::{
    sync::{Mutex, RwLock},
    time::{Instant, timeout},
};

use crate::error::{CopyTraderError, Result};

const MAX_AGE: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(1_500);
const EXPIRY_MARGIN_BLOCKS: u64 = 20;

#[derive(Clone, Copy)]
struct Entry {
    hash: Hash,
    fetched_at: Instant,
    last_valid_block_height: u64,
    observed_block_height: u64,
}

impl Entry {
    fn usable(&self) -> bool {
        self.fetched_at.elapsed() < MAX_AGE
            && self
                .last_valid_block_height
                .saturating_sub(self.observed_block_height)
                > EXPIRY_MARGIN_BLOCKS
    }
}

#[derive(Default)]
pub(super) struct BlockhashCache {
    entry: RwLock<Option<Entry>>,
    refresh: Mutex<()>,
}

impl BlockhashCache {
    pub fn cached(&self) -> Option<Hash> {
        self.entry
            .try_read()
            .ok()?
            .as_ref()
            .filter(|entry| entry.usable())
            .map(|entry| entry.hash)
    }

    pub async fn get(&self, rpc: &RpcClient) -> Result<Hash> {
        if let Some(entry) = self
            .entry
            .read()
            .await
            .as_ref()
            .filter(|entry| entry.usable())
        {
            return Ok(entry.hash);
        }
        let _guard = self.refresh.lock().await;
        // A concurrent cold reader or the background worker may have filled it.
        if let Some(entry) = self
            .entry
            .read()
            .await
            .as_ref()
            .filter(|entry| entry.usable())
        {
            return Ok(entry.hash);
        }
        self.fetch(rpc).await
    }

    pub async fn refresh(&self, rpc: &RpcClient) -> Result<Hash> {
        let _guard = self.refresh.lock().await;
        self.fetch(rpc).await
    }

    async fn fetch(&self, rpc: &RpcClient) -> Result<Hash> {
        let fetched_at = Instant::now();
        let response = timeout(REQUEST_TIMEOUT, async {
            tokio::try_join!(
                rpc.get_latest_blockhash_with_commitment(CommitmentConfig::confirmed()),
                rpc.get_block_height_with_commitment(CommitmentConfig::confirmed()),
            )
        })
        .await
        .map_err(|_| CopyTraderError::Execution("blockhash refresh timed out".to_owned()))?
        .map_err(|error| {
            CopyTraderError::Execution(format!("cannot refresh blockhash: {error}"))
        })?;
        let ((hash, last_valid_block_height), observed_block_height) = response;
        let entry = Entry {
            hash,
            fetched_at,
            last_valid_block_height,
            observed_block_height,
        };
        if !entry.usable() {
            *self.entry.write().await = None;
            return Err(CopyTraderError::Execution(
                "RPC blockhash is too close to expiry".to_owned(),
            ));
        }
        *self.entry.write().await = Some(entry);
        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::HttpConfig, http::HttpTransport, test_rpc::TestRpc};
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn cold_readers_share_one_fetch_and_warm_reads_do_no_rpc() {
        let hash = Hash::new_unique();
        let server = TestRpc::start(move |request| match request["method"].as_str() {
            Some("getLatestBlockhash") => json!({"context":{"slot":42},"value":{"blockhash":hash.to_string(),"lastValidBlockHeight":200}}),
            Some("getBlockHeight") => json!(100),
            _ => panic!("unexpected RPC"),
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let rpc = transport.solana_rpc(&server.url);
        let cache = BlockhashCache::default();
        let (first, second) = tokio::join!(cache.get(&rpc), cache.get(&rpc));
        assert_eq!(first.unwrap(), hash);
        assert_eq!(second.unwrap(), hash);
        assert_eq!(cache.get(&rpc).await.unwrap(), hash);
        assert_eq!(server.count("getLatestBlockhash"), 1);
        assert_eq!(server.count("getBlockHeight"), 1);
    }

    #[tokio::test]
    async fn stale_cache_refreshes_and_does_not_hide_rpc_failure() {
        let failing = Arc::new(AtomicBool::new(false));
        let failure = failing.clone();
        let server = TestRpc::start(move |request| {
            if failure.load(Ordering::SeqCst) {
                return json!({"error":{"code":-32602,"message":"unavailable"}});
            }
            match request["method"].as_str() {
                Some("getLatestBlockhash") => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":200}}),
                Some("getBlockHeight") => json!(100),
                _ => panic!("unexpected RPC"),
            }
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let rpc = transport.solana_rpc(&server.url);
        let cache = BlockhashCache::default();
        let first = cache.get(&rpc).await.unwrap();
        cache.entry.write().await.as_mut().unwrap().fetched_at = Instant::now() - MAX_AGE;
        assert_ne!(cache.get(&rpc).await.unwrap(), first);
        failing.store(true, Ordering::SeqCst);
        assert!(cache.refresh(&rpc).await.is_err());
        assert!(cache.get(&rpc).await.is_ok());
        cache.entry.write().await.as_mut().unwrap().fetched_at = Instant::now() - MAX_AGE;
        assert!(cache.get(&rpc).await.is_err());
    }

    #[tokio::test]
    async fn timed_out_refresh_releases_lock_and_does_not_cache_response() {
        let slow = Arc::new(AtomicBool::new(true));
        let delayed = slow.clone();
        let server = TestRpc::start(move |request| {
            let result = match request["method"].as_str() {
                Some("getLatestBlockhash") => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":200}}),
                Some("getBlockHeight") => json!(100),
                _ => panic!("unexpected RPC"),
            };
            json!({"test_delay_ms": if delayed.load(Ordering::SeqCst) { 3000 } else { 0 }, "test_result": result})
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let rpc = transport.solana_rpc(&server.url);
        let cache = BlockhashCache::default();
        let error = cache.get(&rpc).await.unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert!(cache.entry.read().await.is_none());
        slow.store(false, Ordering::SeqCst);
        assert!(cache.get(&rpc).await.is_ok());
    }

    #[tokio::test]
    async fn near_expired_rpc_response_is_rejected() {
        let server = TestRpc::start(move |request| match request["method"].as_str() {
            Some("getLatestBlockhash") => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":120}}),
            Some("getBlockHeight") => json!(100),
            _ => panic!("unexpected RPC"),
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let cache = BlockhashCache::default();
        assert!(cache.get(&transport.solana_rpc(&server.url)).await.is_err());
        assert!(cache.entry.read().await.is_none());
    }
}
