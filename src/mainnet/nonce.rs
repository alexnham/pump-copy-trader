//! A lease is released on abandoned preparation, but submitted nonces stay held
//! until a finalized account read proves that the old value is invalid.
use crate::{
    error::{CopyTraderError, Result},
    storage::Store,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_nonce::{state::State, versions::Versions};
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Entry {
    account: Pubkey,
    hash: Hash,
    held: bool,
    submitted: bool,
}
#[derive(Default)]
pub struct NoncePool {
    entries: Arc<Mutex<Vec<Entry>>>,
}
pub struct NonceLease {
    pub account: Pubkey,
    pub hash: Hash,
    entries: Arc<Mutex<Vec<Entry>>>,
}
impl NonceLease {
    pub fn mark_submitted(&self) {
        let mut entries = self.entries.lock().expect("nonce pool mutex");
        let entry = entries
            .iter_mut()
            .find(|e| e.account == self.account)
            .expect("reserved nonce");
        entry.submitted = true;
    }
}
impl Drop for NonceLease {
    fn drop(&mut self) {
        let mut entries = self.entries.lock().expect("nonce pool mutex");
        if let Some(entry) = entries.iter_mut().find(|e| e.account == self.account)
            && entry.hash == self.hash
            && !entry.submitted
        {
            entry.held = false;
        }
    }
}
impl NoncePool {
    pub async fn initialize(
        &self,
        rpc: &RpcClient,
        accounts: &[String],
        authority: Pubkey,
        store: &Store,
    ) -> Result<()> {
        let mut entries = vec![];
        for account in accounts {
            let account = Pubkey::from_str(account)
                .map_err(|_| CopyTraderError::Configuration("invalid nonce pubkey".into()))?;
            let hash = read_nonce(rpc, account, authority).await?;
            let held = store
                .nonce_was_used(&account.to_string(), &hash.to_string())
                .await?;
            entries.push(Entry {
                account,
                hash,
                held,
                submitted: held,
            });
        }
        *self.entries.lock().expect("nonce pool mutex") = entries;
        Ok(())
    }
    pub fn reserve(&self) -> Result<NonceLease> {
        let mut entries = self.entries.lock().expect("nonce pool mutex");
        let entry = entries.iter_mut().find(|e| !e.held).ok_or_else(|| {
            CopyTraderError::Execution(
                "nonce pool exhausted; outstanding nonces remain reserved".into(),
            )
        })?;
        entry.held = true;
        Ok(NonceLease {
            account: entry.account,
            hash: entry.hash,
            entries: self.entries.clone(),
        })
    }
    pub async fn refresh(&self, rpc: &RpcClient, authority: Pubkey) {
        let held: Vec<_> = self
            .entries
            .lock()
            .expect("nonce pool mutex")
            .iter()
            .filter(|e| e.submitted)
            .map(|e| (e.account, e.hash))
            .collect();
        for (account, old) in held {
            match read_nonce(rpc, account, authority).await {
                Ok(new) if new != old => {
                    let mut entries = self.entries.lock().expect("nonce pool mutex");
                    if let Some(entry) = entries
                        .iter_mut()
                        .find(|e| e.account == account && e.hash == old && e.submitted)
                    {
                        entry.hash = new;
                        entry.held = false;
                        entry.submitted = false;
                    }
                }
                Err(error) => {
                    tracing::warn!(%account, %error, "nonce refresh failed; account stays reserved")
                }
                _ => {}
            }
        }
    }
    pub async fn keep_fresh(&self, rpc: &RpcClient, authority: Pubkey) {
        loop {
            self.refresh(rpc, authority).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
async fn read_nonce(rpc: &RpcClient, account: Pubkey, authority: Pubkey) -> Result<Hash> {
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        rpc.get_account_with_commitment(&account, CommitmentConfig::finalized()),
    )
    .await
    .map_err(|_| CopyTraderError::Execution("nonce read timed out".into()))?
    .map_err(|e| CopyTraderError::Execution(format!("cannot read nonce account: {e}")))?;
    let value = response.value.ok_or_else(|| {
        CopyTraderError::Configuration(format!("nonce account {account} does not exist"))
    })?;
    if value.owner != solana_system_interface::program::ID || value.executable {
        return Err(CopyTraderError::Configuration(format!(
            "nonce account {account} must be System Program owned"
        )));
    }
    let versions: Versions = bincode::deserialize(&value.data)
        .map_err(|_| CopyTraderError::Configuration(format!("invalid nonce account {account}")))?;
    match versions {
        Versions::Current(state) => match *state {
            State::Initialized(data) if data.authority == authority => Ok(data.blockhash()),
            _ => Err(CopyTraderError::Configuration(format!(
                "nonce account {account} must be initialized with copier wallet as authority"
            ))),
        },
        _ => Err(CopyTraderError::Configuration(
            "legacy nonce accounts are unsupported".into(),
        )),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{config::HttpConfig, http::HttpTransport, test_rpc::TestRpc};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde_json::json;
    use solana_nonce::state::{Data, DurableNonce};

    pub fn nonce_value(authority: Pubkey, blockhash: Hash) -> serde_json::Value {
        let data = bincode::serialize(&Versions::new(State::Initialized(Data::new(
            authority,
            DurableNonce::from_blockhash(&blockhash),
            5000,
        ))))
        .unwrap();
        json!({"lamports":2_000_000,"owner":solana_system_interface::program::ID.to_string(),
            "data":[STANDARD.encode(data),"base64"],"executable":false,"rentEpoch":0})
    }

    #[tokio::test]
    async fn abandoned_lease_releases_but_submitted_requires_finalized_advancement() {
        let authority = Pubkey::new_unique();
        let account = Pubkey::new_unique();
        let blockhash = Arc::new(Mutex::new(Hash::new_unique()));
        let response_hash = blockhash.clone();
        let server = TestRpc::start(move |request| {
            assert_eq!(request["params"][1]["commitment"], "finalized");
            json!({"context":{"slot":42},"value":nonce_value(authority, *response_hash.lock().unwrap())})
        }).await;
        let rpc = HttpTransport::new(&HttpConfig::default())
            .unwrap()
            .solana_rpc(&server.url);
        let store = Store::connect("sqlite::memory:").await.unwrap();
        let pool = NoncePool::default();
        pool.initialize(&rpc, &[account.to_string()], authority, &store)
            .await
            .unwrap();
        let lease = pool.reserve().unwrap();
        assert!(pool.reserve().is_err());
        drop(lease);
        let lease = pool.reserve().unwrap();
        let old = lease.hash;
        lease.mark_submitted();
        drop(lease);
        pool.refresh(&rpc, authority).await;
        assert!(pool.reserve().is_err());
        *blockhash.lock().unwrap() = Hash::new_unique();
        pool.refresh(&rpc, authority).await;
        assert_ne!(pool.reserve().unwrap().hash, old);
    }

    #[tokio::test]
    async fn startup_rejects_wrong_authority_and_holds_journaled_nonce() {
        let authority = Pubkey::new_unique();
        let account = Pubkey::new_unique();
        let blockhash = Hash::new_unique();
        let server = TestRpc::start(
            move |_| json!({"context":{"slot":42},"value":nonce_value(authority, blockhash)}),
        )
        .await;
        let rpc = HttpTransport::new(&HttpConfig::default())
            .unwrap()
            .solana_rpc(&server.url);
        let store = Store::connect("sqlite::memory:").await.unwrap();
        let pool = NoncePool::default();
        assert!(
            pool.initialize(&rpc, &[account.to_string()], Pubkey::new_unique(), &store)
                .await
                .is_err()
        );
        pool.initialize(&rpc, &[account.to_string()], authority, &store)
            .await
            .unwrap();
        let lease = pool.reserve().unwrap();
        store
            .record_recovered_signature("source", 42)
            .await
            .unwrap();
        store
            .reserve_attempt("source", crate::config::ExecutionTarget::Mainnet, 1, 1)
            .await
            .unwrap();
        store
            .persist_fanout(
                "source",
                &account.to_string(),
                &lease.hash.to_string(),
                &[("signature".into(), vec![1], "route".into())],
                "{}",
            )
            .await
            .unwrap();
        let restarted = NoncePool::default();
        restarted
            .initialize(&rpc, &[account.to_string()], authority, &store)
            .await
            .unwrap();
        assert!(restarted.reserve().is_err());
    }
}
