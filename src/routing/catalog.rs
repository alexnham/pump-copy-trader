use std::{cmp::Reverse, collections::HashMap, sync::Arc};

use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::RwLock;

use crate::domain::DexKind;
use crate::domain::NATIVE_MINT;

type PoolKey = (DexKind, Pubkey, Pubkey);
type PoolMap = HashMap<PoolKey, Vec<PoolDescriptor>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolDescriptor {
    pub dex: DexKind,
    pub address: Pubkey,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub liquidity_hint: u128,
}

#[derive(Clone, Default)]
pub struct PoolCatalog {
    pools: Arc<RwLock<PoolMap>>,
    accounts: Arc<RwLock<HashMap<Pubkey, Arc<Account>>>>,
}

impl PoolCatalog {
    pub async fn cache_account(&self, address: Pubkey, account: Account) {
        self.accounts
            .write()
            .await
            .insert(address, Arc::new(account));
    }

    pub async fn account(&self, address: Pubkey) -> Option<Arc<Account>> {
        self.accounts.read().await.get(&address).cloned()
    }

    pub fn cached_account(&self, address: Pubkey) -> Option<Arc<Account>> {
        self.accounts.try_read().ok()?.get(&address).cloned()
    }

    /// Refresh only the requested pairs, preserving pools learned from other trades.
    pub async fn refresh(&self, dex: DexKind, mints: &[Pubkey], discovered: Vec<PoolDescriptor>) {
        let mut refreshed = PoolMap::new();
        for pool in discovered {
            let (a, b) = ordered_pair(pool.mint_a, pool.mint_b);
            refreshed.entry((dex, a, b)).or_default().push(pool);
        }
        for pools in refreshed.values_mut() {
            pools.sort_unstable_by_key(|pool| Reverse(pool.liquidity_hint));
        }
        let mut catalog = self.pools.write().await;
        let native = Pubkey::from_str_const(NATIVE_MINT);
        catalog.retain(|(kind, a, b), _| {
            *kind != dex || *a == native || *b == native || !mints.contains(a) || !mints.contains(b)
        });
        catalog.extend(refreshed);
    }

    /// Keep a successfully simulated source pool at the front of its pair's candidates.
    pub async fn remember(&self, pool: PoolDescriptor, limit: usize) {
        let (a, b) = ordered_pair(pool.mint_a, pool.mint_b);
        let mut catalog = self.pools.write().await;
        let pools = catalog.entry((pool.dex, a, b)).or_default();
        pools.retain(|existing| existing.address != pool.address);
        pools.insert(0, pool);
        pools.truncate(limit);
    }

    pub async fn replace(&self, dex: DexKind, discovered: Vec<PoolDescriptor>) {
        let mut by_pair: HashMap<(Pubkey, Pubkey), Vec<PoolDescriptor>> = HashMap::new();
        for pool in discovered {
            let pair = ordered_pair(pool.mint_a, pool.mint_b);
            by_pair.entry(pair).or_default().push(pool);
        }
        let mut catalog = self.pools.write().await;
        catalog.retain(|(kind, _, _), _| *kind != dex);
        for ((mint_a, mint_b), mut pools) in by_pair {
            pools.sort_unstable_by_key(|pool| Reverse(pool.liquidity_hint));
            catalog.insert((dex, mint_a, mint_b), pools);
        }
    }

    pub async fn merge_pair(&self, dex: DexKind, discovered: Vec<PoolDescriptor>) {
        let mut by_pair: HashMap<(Pubkey, Pubkey), Vec<PoolDescriptor>> = HashMap::new();
        for pool in discovered {
            let pair = ordered_pair(pool.mint_a, pool.mint_b);
            by_pair.entry(pair).or_default().push(pool);
        }
        let mut catalog = self.pools.write().await;
        for ((mint_a, mint_b), mut pools) in by_pair {
            pools.sort_unstable_by_key(|pool| Reverse(pool.liquidity_hint));
            catalog.insert((dex, mint_a, mint_b), pools);
        }
    }

    pub async fn contains_pair(&self, input: Pubkey, output: Pubkey) -> bool {
        let (mint_a, mint_b) = ordered_pair(input, output);
        self.pools
            .read()
            .await
            .keys()
            .any(|(_, left, right)| *left == mint_a && *right == mint_b)
    }

    pub async fn candidates(
        &self,
        dex: DexKind,
        input: Pubkey,
        output: Pubkey,
        limit: usize,
    ) -> Vec<PoolDescriptor> {
        let (mint_a, mint_b) = ordered_pair(input, output);
        self.pools
            .read()
            .await
            .get(&(dex, mint_a, mint_b))
            .map(|pools| pools.iter().take(limit).cloned().collect())
            .unwrap_or_default()
    }
}

fn ordered_pair(left: Pubkey, right: Pubkey) -> (Pubkey, Pubkey) {
    if left.to_bytes() <= right.to_bytes() {
        (left, right)
    } else {
        (right, left)
    }
}
