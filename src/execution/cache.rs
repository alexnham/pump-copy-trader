use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock;
use tracing::debug;

use crate::{
    domain::AssetId,
    error::{CopyTraderError, Result},
    token::accounts::associated_token_address,
};

const REFRESH_DELAY: Duration = Duration::from_millis(500);

#[derive(Clone, Copy)]
struct Entry {
    amount: u64,
}

#[derive(Clone, Copy)]
struct WatchedAccount {
    asset: AssetId,
    owner: Pubkey,
    entry: Option<Entry>,
    revision: u64,
}

#[derive(Clone, Default)]
pub struct WalletBalanceCache {
    accounts: Arc<RwLock<HashMap<Pubkey, WatchedAccount>>>,
}

impl WalletBalanceCache {
    fn address(asset: AssetId, owner: Pubkey, token_account: Pubkey) -> Pubkey {
        if asset == AssetId::NativeSol {
            owner
        } else {
            token_account
        }
    }

    pub async fn get(&self, address: &Pubkey) -> Option<u64> {
        let accounts = self.accounts.read().await;
        let entry = accounts.get(address)?.entry?;
        Some(entry.amount)
    }

    async fn watch(&self, asset: AssetId, owner: Pubkey, address: Pubkey) {
        self.accounts
            .write()
            .await
            .entry(address)
            .or_insert(WatchedAccount {
                asset,
                owner,
                entry: None,
                revision: 0,
            });
    }

    pub async fn invalidate(&self, address: &Pubkey) -> Result<()> {
        if let Some(account) = self.accounts.write().await.get_mut(address) {
            account.entry = None;
            account.revision = account.revision.checked_add(1).ok_or_else(|| {
                CopyTraderError::Execution("balance cache revision overflow".to_owned())
            })?;
        }
        Ok(())
    }

    async fn store(&self, address: Pubkey, amount: u64, revision: u64) {
        let mut accounts = self.accounts.write().await;
        if let Some(account) = accounts.get_mut(&address)
            && account.revision == revision
        {
            account.entry = Some(Entry { amount });
        }
    }

    pub async fn get_or_fetch(
        &self,
        rpc: &RpcClient,
        asset: AssetId,
        owner: Pubkey,
        token_account: Pubkey,
    ) -> Result<u64> {
        let address = Self::address(asset, owner, token_account);
        self.watch(asset, owner, address).await;
        if let Some(amount) = self.get(&address).await {
            return Ok(amount);
        }
        self.fetch(rpc, asset, owner, token_account).await
    }

    /// Reconciliation reads always hit RPC and update the same cache used by execution.
    pub async fn fetch(
        &self,
        rpc: &RpcClient,
        asset: AssetId,
        owner: Pubkey,
        token_account: Pubkey,
    ) -> Result<u64> {
        let address = Self::address(asset, owner, token_account);
        self.watch(asset, owner, address).await;
        let revision = {
            let mut accounts = self.accounts.write().await;
            let account = accounts.get_mut(&address).ok_or_else(|| {
                CopyTraderError::Execution("balance account is not tracked".to_owned())
            })?;
            account.revision = account.revision.checked_add(1).ok_or_else(|| {
                CopyTraderError::Execution("balance cache revision overflow".to_owned())
            })?;
            account.revision
        };
        let amount = if asset == AssetId::NativeSol {
            rpc.get_balance(&owner).await.map_err(|error| {
                CopyTraderError::Execution(format!("failed to read SOL balance: {error}"))
            })?
        } else {
            match rpc.get_token_account_balance(&address).await {
                Ok(balance) => balance.amount.parse::<u64>().map_err(|error| {
                    CopyTraderError::Execution(format!("invalid token balance: {error}"))
                })?,
                Err(error) => {
                    // A missing ATA is zero; transport errors or existing invalid accounts are not.
                    let account = rpc.get_account_with_commitment(&address, CommitmentConfig::confirmed())
                        .await.map_err(|lookup| CopyTraderError::Execution(
                            format!("failed to read token balance: {error}; account lookup failed: {lookup}")))?;
                    if account.value.is_some() {
                        return Err(CopyTraderError::Execution(format!(
                            "failed to read token balance: {error}"
                        )));
                    }
                    0
                }
            }
        };
        self.store(address, amount, revision).await;
        Ok(amount)
    }

    pub async fn refresh(&self, rpc: &RpcClient, owner: Pubkey, mints: &[Pubkey]) {
        self.watch(AssetId::NativeSol, owner, owner).await;
        for mint in mints {
            for program in [spl_token::id(), spl_token_2022::id()] {
                let address = associated_token_address(&owner, mint, &program);
                self.watch(AssetId::Token(*mint), owner, address).await;
            }
        }
        let watched = self
            .accounts
            .read()
            .await
            .iter()
            .filter(|(_, account)| {
                account.asset == AssetId::NativeSol
                    || account.asset == AssetId::Token(spl_token::native_mint::id())
                    || account.entry.is_none_or(|entry| entry.amount > 0)
            })
            .map(|(address, account)| (*address, account.asset, account.owner))
            .collect::<Vec<_>>();
        stream::iter(watched)
            .map(|(address, asset, owner)| async move {
                if let Err(error) = self.fetch(rpc, asset, owner, address).await {
                    debug!(%address, %error, "balance cache refresh failed");
                }
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
    }

    pub async fn run(self, rpc: Arc<RpcClient>, owner: Pubkey, mints: Vec<Pubkey>) {
        loop {
            self.refresh(&rpc, owner, &mints).await;
            tokio::time::sleep(REFRESH_DELAY).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::HttpConfig, http::HttpTransport, test_rpc::TestRpc};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn token() -> (AssetId, Pubkey, Pubkey) {
        let owner = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        (
            AssetId::Token(mint),
            owner,
            associated_token_address(&owner, &mint, &spl_token::id()),
        )
    }

    fn balance(amount: u64) -> serde_json::Value {
        json!({"context":{"slot":42},"value":{"amount":amount.to_string(),"decimals":6,"uiAmount":null,"uiAmountString":"0"}})
    }

    #[tokio::test]
    async fn zero_wsol_keeps_refreshing_and_invalidated_entries_are_not_reused() {
        let owner = Pubkey::new_unique();
        let mint = spl_token::native_mint::id();
        let address = associated_token_address(&owner, &mint, &spl_token::id());
        let reads = AtomicUsize::new(0);
        let server =
            TestRpc::start(
                move |request| match request["method"].as_str().expect("method") {
                    "getBalance" => json!({"context":{"slot":42},"value":1_000_000}),
                    "getTokenAccountBalance" => {
                        balance(if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                            0
                        } else {
                            200
                        })
                    }
                    method => panic!("unexpected {method}"),
                },
            )
            .await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let rpc = transport.solana_rpc(&server.url);
        let cache = WalletBalanceCache::default();
        assert_eq!(
            cache
                .fetch(&rpc, AssetId::Token(mint), owner, address)
                .await
                .expect("warm"),
            0
        );
        cache.refresh(&rpc, owner, &[]).await;
        assert_eq!(cache.get(&address).await, Some(200));
        assert_eq!(server.count("getTokenAccountBalance"), 2);
        cache.invalidate(&address).await.expect("invalidate");
        assert_eq!(cache.get(&address).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn cached_balance_survives_age_but_invalidation_refetches() {
        let (asset, owner, address) = token();
        let reads = AtomicUsize::new(0);
        let server = TestRpc::start(move |request| {
            assert_eq!(request["method"], "getTokenAccountBalance");
            assert_eq!(request["params"][0], address.to_string());
            balance(if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                50
            } else {
                100
            })
        })
        .await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let rpc = transport.solana_rpc(&server.url);
        let cache = WalletBalanceCache::default();
        assert_eq!(
            cache
                .get_or_fetch(&rpc, asset, owner, address)
                .await
                .expect("miss"),
            50
        );
        assert_eq!(
            cache
                .get_or_fetch(&rpc, asset, owner, address)
                .await
                .expect("fresh hit"),
            50
        );
        assert_eq!(server.count("getTokenAccountBalance"), 1);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(cache.get(&address).await, Some(50));
        assert_eq!(
            cache
                .get_or_fetch(&rpc, asset, owner, address)
                .await
                .expect("old hit"),
            50
        );
        assert_eq!(server.count("getTokenAccountBalance"), 1);
        cache.invalidate(&address).await.expect("invalidate");
        assert_eq!(cache.get(&address).await, None);
        assert_eq!(
            cache
                .get_or_fetch(&rpc, asset, owner, address)
                .await
                .expect("invalidated refresh"),
            100
        );
        assert_eq!(server.count("getTokenAccountBalance"), 2);
        assert_eq!(cache.get(&address).await, Some(100));
    }

    #[tokio::test]
    async fn dynamic_accounts_join_background_refresh_without_configured_mints() {
        let (asset, owner, address) = token();
        let reads = AtomicUsize::new(0);
        let server =
            TestRpc::start(
                move |request| match request["method"].as_str().expect("method") {
                    "getTokenAccountBalance" => {
                        balance(if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                            50
                        } else {
                            100
                        })
                    }
                    "getBalance" => {
                        assert_eq!(request["params"][0], owner.to_string());
                        json!({"context":{"slot":42},"value":5000})
                    }
                    other => panic!("unexpected RPC {other}"),
                },
            )
            .await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let rpc = transport.solana_rpc(&server.url);
        let cache = WalletBalanceCache::default();
        cache
            .get_or_fetch(&rpc, asset, owner, address)
            .await
            .expect("new token");
        cache.refresh(&rpc, owner, &[]).await;
        assert_eq!(cache.get(&address).await, Some(100));
        assert_eq!(cache.get(&owner).await, Some(5000));
        assert_eq!(server.count("getTokenAccountBalance"), 2);
        assert_eq!(server.count("getProgramAccounts"), 0);
    }

    #[tokio::test]
    async fn old_background_read_cannot_overwrite_post_confirmation_balance() {
        let (asset, owner, address) = token();
        let reads = AtomicUsize::new(0);
        let server = TestRpc::start(move |request| {
            assert_eq!(request["method"], "getTokenAccountBalance");
            let first = reads.fetch_add(1, Ordering::SeqCst) == 0;
            json!({"test_delay_ms":if first {150} else {0},"test_result":balance(if first {50} else {100})})
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let rpc = Arc::new(transport.solana_rpc(&server.url));
        let cache = WalletBalanceCache::default();
        let old_cache = cache.clone();
        let old_rpc = rpc.clone();
        let pending =
            tokio::spawn(async move { old_cache.fetch(&old_rpc, asset, owner, address).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while server.count("getTokenAccountBalance") == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("old read started");
        cache
            .invalidate(&address)
            .await
            .expect("invalidate at confirmation");
        assert_eq!(cache.get(&address).await, None);
        assert_eq!(
            cache
                .fetch(&rpc, asset, owner, address)
                .await
                .expect("reconciliation"),
            100
        );
        assert_eq!(pending.await.expect("task").expect("old response"), 50);
        assert_eq!(cache.get(&address).await, Some(100));
    }

    #[tokio::test]
    async fn only_verified_missing_accounts_are_zero_and_rpc_failures_remain_errors() {
        for scenario in [
            "missing",
            "rpc_failure",
            "existing_account",
            "invalid_amount",
        ] {
            let (asset, owner, address) = token();
            let server = TestRpc::start(move |request| match request["method"].as_str().expect("method") {
                "getTokenAccountBalance" if scenario == "invalid_amount" => {
                    let mut value = balance(1); value["value"]["amount"] = json!("invalid"); value
                }
                "getTokenAccountBalance" => json!({"error":{"code":-32602,"message":"balance read failed"}}),
                "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
                "getBalance" => json!({"context":{"slot":42},"value":5000}),
                "getAccountInfo" if scenario == "rpc_failure" => json!({"error":{"code":-32602,"message":"lookup failed"}}),
                "getAccountInfo" => json!({"context":{"slot":42},"value":if scenario == "missing" {json!(null)} else {
                    json!({"lamports":1,"owner":spl_token::id().to_string(),"data":["","base64"],"executable":false,"rentEpoch":0})
                }}),
                other => panic!("unexpected RPC {other}"),
            }).await;
            let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
            let rpc = transport.solana_rpc(&server.url);
            let cache = WalletBalanceCache::default();
            let result = cache.get_or_fetch(&rpc, asset, owner, address).await;
            if scenario == "missing" {
                assert_eq!(result.expect("missing is zero"), 0);
                assert_eq!(cache.get(&address).await, Some(0));
                cache.refresh(&rpc, owner, &[]).await;
                assert_eq!(
                    server.count("getTokenAccountBalance"),
                    1,
                    "zero accounts are not polled while idle"
                );
            } else {
                assert!(
                    matches!(result, Err(CopyTraderError::Execution(_))),
                    "{scenario}"
                );
                assert_eq!(
                    cache.get(&address).await,
                    None,
                    "failed reads must not enter cache"
                );
            }
        }
    }

    #[tokio::test]
    async fn native_balance_uses_wallet_address_and_reconciliation_forces_an_update() {
        let owner = Pubkey::new_unique();
        let misleading_token_address = Pubkey::new_unique();
        let reads = AtomicUsize::new(0);
        let server = TestRpc::start(move |request| {
            assert_eq!(request["method"], "getBalance");
            assert_eq!(request["params"][0], owner.to_string());
            json!({"context":{"slot":42},"value":if reads.fetch_add(1, Ordering::SeqCst)==0 {1000} else {800}})
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let rpc = transport.solana_rpc(&server.url);
        let cache = WalletBalanceCache::default();
        assert_eq!(
            cache
                .get_or_fetch(&rpc, AssetId::NativeSol, owner, misleading_token_address)
                .await
                .expect("wallet"),
            1000
        );
        assert_eq!(
            cache
                .fetch(&rpc, AssetId::NativeSol, owner, misleading_token_address)
                .await
                .expect("confirmed balance"),
            800
        );
        assert_eq!(cache.get(&owner).await, Some(800));
        assert_eq!(cache.get(&misleading_token_address).await, None);
    }
}
