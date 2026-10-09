use std::{str::FromStr, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::{
    instruction::Instruction, pubkey::Pubkey, signature::Signature, transaction::Transaction,
};
use solana_system_interface::instruction::transfer;
use tracing::{debug, warn};
use url::Url;

use crate::{
    config::MainnetConfig,
    error::{CopyTraderError, Result},
    http::HttpTransport,
};

const TIP_ACCOUNTS: [&str; 10] = [
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
    "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
    "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD",
    "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
    "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
    "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
];

pub struct MainnetClient {
    pub rpc: RpcClient,
    rpc_url: Url,
    sender_url: Url,
    tip_lamports: u64,
    fixed_priority_fee: u64,
    http: Arc<HttpTransport>,
    blockhash: super::blockhash::BlockhashCache,
    pub nonce_pool: super::nonce::NoncePool,
    pub fanout: crate::config::FanoutConfig,
}

impl MainnetClient {
    pub(crate) fn is_sender_tip_account(account: &str) -> bool {
        TIP_ACCOUNTS.contains(&account)
    }
    pub fn new(rpc_url: Url, config: &MainnetConfig, http: Arc<HttpTransport>) -> Self {
        Self {
            rpc: http.solana_rpc(&rpc_url),
            rpc_url,
            sender_url: config.sender_url.clone(),
            tip_lamports: config.tip_lamports,
            fixed_priority_fee: config
                .fixed_priority_fee_micro_lamports
                .unwrap_or(config.max_priority_fee_micro_lamports)
                .min(config.max_priority_fee_micro_lamports),
            http,
            blockhash: super::blockhash::BlockhashCache::default(),
            nonce_pool: Default::default(),
            fanout: config.fanout.clone(),
        }
    }

    pub async fn warm(&self) -> Result<u64> {
        self.http.warm(&self.rpc_url).await
    }

    /// Reuses a fresh confirmed blockhash, fetching on a cold or stale cache.
    pub async fn latest_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        self.blockhash.get(&self.rpc).await
    }

    pub fn cached_blockhash(&self) -> Option<solana_sdk::hash::Hash> {
        self.blockhash.cached()
    }

    /// Run alongside the service; dropping this future stops refreshes.
    pub async fn keep_blockhash_fresh(&self) {
        loop {
            if self.blockhash.refresh(&self.rpc).await.is_err() {
                warn!("blockhash refresh failed; stale entries remain unusable");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn warm_sender(&self) {
        let endpoint = sender_ping_url(&self.sender_url);
        match self
            .http
            .client()
            .get(endpoint)
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                if response.bytes().await.is_ok() {
                    debug!("Sender connection warmed");
                } else {
                    warn!("Sender warmup body failed");
                }
            }
            Ok(response) => warn!(status = response.status().as_u16(), "Sender warmup failed"),
            Err(_) => warn!("Sender warmup request failed"),
        }
    }

    pub async fn keep_sender_warm(&self) {
        warm_periodically(|| self.warm_sender()).await;
    }

    pub async fn finalize_instructions(
        &self,
        payer: &Pubkey,
        source_signature: &Signature,
        compute_limit: u32,
        mut instructions: Vec<Instruction>,
    ) -> Result<Vec<Instruction>> {
        let price = self.fixed_priority_fee;
        let tip = choose_tip_account(source_signature)?;
        debug!(compute_limit, priority_fee = price, tip_lamports = self.tip_lamports, tip_account = %tip, "mainnet fees selected");
        let mut finalized = Vec::with_capacity(instructions.len() + 3);
        finalized.push(ComputeBudgetInstruction::set_compute_unit_limit(
            compute_limit,
        ));
        finalized.push(ComputeBudgetInstruction::set_compute_unit_price(price));
        finalized.append(&mut instructions);
        finalized.push(transfer(payer, &tip, self.tip_lamports));
        Ok(finalized)
    }

    pub const fn tip_lamports(&self) -> u64 {
        self.tip_lamports
    }

    pub fn fanout_instructions(
        &self,
        payer: &Pubkey,
        limit: u32,
        instructions: &[Instruction],
        lease: &super::nonce::NonceLease,
        route: &crate::config::FanoutRouteConfig,
    ) -> Result<Vec<Instruction>> {
        let tip = Pubkey::from_str(&route.tip_account)
            .map_err(|_| CopyTraderError::Configuration("invalid fanout tip account".into()))?;
        let mut result = vec![
            solana_system_interface::instruction::advance_nonce_account(&lease.account, payer),
            ComputeBudgetInstruction::set_compute_unit_limit(limit),
            ComputeBudgetInstruction::set_compute_unit_price(route.priority_fee_micro_lamports),
        ];
        result.extend_from_slice(instructions);
        if route.tip_lamports > 0 {
            result.push(transfer(payer, &tip, route.tip_lamports));
        }
        Ok(result)
    }

    /// All requests finish (bounded) before settlement; acknowledgments do not
    /// identify the winner. Even total transport failure can have landed.
    pub async fn send_fanout(&self, transactions: &[Transaction]) {
        use futures_util::{StreamExt, stream::FuturesUnordered};
        let mut requests = FuturesUnordered::new();
        for (route, transaction) in self.fanout.routes.iter().zip(transactions) {
            requests.push(async move {
                let params = sender_params(transaction)?;
                let result: String = self
                    .http
                    .rpc_with_timeout(
                        &route.url,
                        "sendTransaction",
                        params,
                        Duration::from_millis(self.fanout.submit_timeout_ms),
                    )
                    .await?;
                let signature = parse_sender_signature(&result)?;
                if transaction.signatures.first() != Some(&signature) {
                    return Err(CopyTraderError::Execution(
                        "fanout endpoint returned an unexpected signature".into(),
                    ));
                }
                Ok::<(), CopyTraderError>(())
            });
        }
        while let Some(result) = requests.next().await {
            if let Err(error) = result {
                warn!(%error, "fanout submission uncertain; checking all signatures");
            }
        }
    }

    pub async fn send(&self, transaction: &Transaction) -> Result<Signature> {
        let params = sender_params(transaction)?;
        debug!("submitting mainnet transaction");
        let result: String = self
            .http
            .rpc(&self.sender_url, "sendTransaction", params)
            .await?;
        parse_sender_signature(&result)
    }
}

async fn warm_periodically<F, Fut>(mut warm: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        warm().await;
    }
}

fn sender_ping_url(sender: &Url) -> Url {
    let mut endpoint = sender.clone();
    endpoint.set_path("/ping");
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    endpoint
}

fn sender_params(transaction: &Transaction) -> Result<Value> {
    let bytes = bincode::serialize(transaction).map_err(|error| {
        CopyTraderError::Execution(format!("cannot serialize transaction: {error}"))
    })?;
    Ok(json!([STANDARD.encode(bytes), {
        "encoding": "base64",
        "skipPreflight": true,
        "maxRetries": 0
    }]))
}

fn parse_sender_signature(value: &str) -> Result<Signature> {
    Signature::from_str(value).map_err(|error| {
        CopyTraderError::Execution(format!("Sender returned invalid signature: {error}"))
    })
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriorityFeeResult {
    priority_fee_estimate: f64,
}

#[allow(dead_code)]
fn capped_priority_fee(estimate: f64, maximum: u64) -> Result<u64> {
    let estimate = estimate.ceil();
    if !estimate.is_finite() || estimate.is_sign_negative() {
        return Err(CopyTraderError::Execution(
            "priority fee estimate is invalid".to_owned(),
        ));
    }
    Ok((estimate as u64).min(maximum))
}

fn choose_tip_account(source_signature: &Signature) -> Result<Pubkey> {
    let index = usize::from(source_signature.as_ref()[0]) % TIP_ACCOUNTS.len();
    Pubkey::from_str(TIP_ACCOUNTS[index]).map_err(|error| {
        CopyTraderError::Configuration(format!("invalid Sender tip account: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fanout_signs_shared_nonce_variants_and_submits_concurrently() {
        use crate::{
            config::{AppConfig, FanoutRouteConfig, HttpConfig},
            storage::Store,
            test_rpc::TestRpc,
        };
        use solana_sdk::{
            hash::Hash,
            signature::{Keypair, Signer},
        };
        let signer = Keypair::new();
        let authority = signer.pubkey();
        let account = Pubkey::new_unique();
        let blockhash = Hash::new_unique();
        let server = TestRpc::start(move |request| {
            if request["method"] == "getAccountInfo" {
                return json!({"context":{"slot":42},"value":crate::mainnet::nonce::tests::nonce_value(authority, blockhash)});
            }
            let tx: Transaction = bincode::deserialize(&STANDARD.decode(request["params"][0].as_str().unwrap()).unwrap()).unwrap();
            tx.verify().unwrap();
            assert_eq!(tx.message.recent_blockhash, solana_nonce::state::Data::new(authority, solana_nonce::state::DurableNonce::from_blockhash(&blockhash), 5000).blockhash());
            let first = &tx.message.instructions[0];
            assert_eq!(tx.message.account_keys[first.program_id_index as usize], solana_system_interface::program::ID);
            assert_eq!(tx.message.account_keys[first.accounts[0] as usize], account);
            assert_eq!(first.data, solana_system_interface::instruction::advance_nonce_account(&account, &authority).data);
            assert_eq!(request["params"][1]["maxRetries"], 0);
            json!({"test_delay_ms":50,"test_result":tx.signatures[0].to_string()})
        }).await;
        let mut config = AppConfig::load(std::path::Path::new("config.example.toml"))
            .unwrap()
            .mainnet
            .unwrap();
        config.fanout.enabled = true;
        config.fanout.nonce_accounts = vec![account.to_string()];
        config.fanout.routes = (0..2)
            .map(|index| FanoutRouteConfig {
                name: format!("route-{index}"),
                url: server.url.clone(),
                tip_account: TIP_ACCOUNTS[index].into(),
                tip_lamports: 5000,
                priority_fee_micro_lamports: 100000 + index as u64,
            })
            .collect();
        let client = MainnetClient::new(
            server.url.clone(),
            &config,
            HttpTransport::new(&HttpConfig::default()).unwrap(),
        );
        let store = Store::connect("sqlite::memory:").await.unwrap();
        client
            .nonce_pool
            .initialize(
                &client.rpc,
                &config.fanout.nonce_accounts,
                authority,
                &store,
            )
            .await
            .unwrap();
        let lease = client.nonce_pool.reserve().unwrap();
        let swap = vec![transfer(&authority, &Pubkey::new_unique(), 1)];
        let variants = config
            .fanout
            .routes
            .iter()
            .map(|route| {
                let instructions = client
                    .fanout_instructions(&authority, 100000, &swap, &lease, route)
                    .unwrap();
                Transaction::new_signed_with_payer(
                    &instructions,
                    Some(&authority),
                    &[&signer],
                    lease.hash,
                )
            })
            .collect::<Vec<_>>();
        assert_ne!(variants[0].signatures[0], variants[1].signatures[0]);
        client.send_fanout(&variants).await;
        assert_eq!(server.count("sendTransaction"), 2);
        assert_eq!(
            server
                .peak_in_flight
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
    }

    #[tokio::test]
    async fn fanout_route_timeout_is_bounded() {
        use crate::{
            config::{AppConfig, FanoutRouteConfig, HttpConfig},
            test_rpc::TestRpc,
        };
        let server = TestRpc::start(
            |_| json!({"test_delay_ms":3000,"test_result":Signature::default().to_string()}),
        )
        .await;
        let mut config = AppConfig::load(std::path::Path::new("config.example.toml"))
            .unwrap()
            .mainnet
            .unwrap();
        config.fanout.submit_timeout_ms = 30;
        config.fanout.routes = vec![FanoutRouteConfig {
            name: "slow".into(),
            url: server.url.clone(),
            tip_account: TIP_ACCOUNTS[0].into(),
            tip_lamports: 5000,
            priority_fee_micro_lamports: 100000,
        }];
        let client = MainnetClient::new(
            server.url.clone(),
            &config,
            HttpTransport::new(&HttpConfig::default()).unwrap(),
        );
        let started = std::time::Instant::now();
        client.send_fanout(&[Transaction::default()]).await;
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(server.count("sendTransaction"), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn warmer_waits_thirty_seconds_and_stops_when_dropped() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let task = tokio::spawn(async move {
            warm_periodically(|| async {
                observed.fetch_add(1, Ordering::SeqCst);
            })
            .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn warmup_is_bounded_and_does_not_submit_transactions() {
        use crate::{config::HttpConfig, test_rpc::TestRpc};
        let server = TestRpc::start(|_| json!({"test_delay_ms":3000,"test_result":"pong"})).await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let config = MainnetConfig {
            fanout: Default::default(),
            source_direct: false,
            fixed_priority_fee_micro_lamports: None,
            sender_url: server.url.clone(),
            tip_lamports: 5000,
            priority_level: "High".to_owned(),
            max_priority_fee_micro_lamports: 100,
        };
        let client = MainnetClient::new(server.url.clone(), &config, transport);
        let started = std::time::Instant::now();
        client.warm_sender().await;
        assert!(started.elapsed() >= Duration::from_secs(2));
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_eq!(server.count("ping"), 1);
        assert_eq!(server.count("sendTransaction"), 0);
    }

    #[test]
    fn ping_uses_sender_origin_without_query_or_fragment() {
        let sender =
            Url::parse("https://sender.helius-rpc.com/fast?swqos_only=true#fragment").expect("url");
        assert_eq!(
            sender_ping_url(&sender).as_str(),
            "https://sender.helius-rpc.com/ping"
        );
    }

    #[test]
    fn priority_fee_is_rounded_up_and_capped() {
        assert_eq!(capped_priority_fee(42.1, 100).ok(), Some(43));
        assert_eq!(capped_priority_fee(101.0, 100).ok(), Some(100));
        assert!(capped_priority_fee(f64::NAN, 100).is_err());
        assert!(capped_priority_fee(-1.0, 100).is_err());
    }

    #[test]
    fn tip_account_selection_is_deterministic() {
        let signature = Signature::default();
        let first = choose_tip_account(&signature).ok();
        let second = choose_tip_account(&signature).ok();
        assert_eq!(first, second);
        assert!(first.is_some());
    }

    #[test]
    fn sender_receives_the_exact_wire_transaction_and_required_options() {
        let transaction = Transaction::default();
        let payload = sender_params(&transaction).ok();
        assert!(payload.is_some());
        let Some(payload) = payload else { return };
        let expected = STANDARD.encode(bincode::serialize(&transaction).unwrap_or_default());
        assert_eq!(payload[0], expected);
        assert_eq!(payload[1]["encoding"], "base64");
        assert_eq!(payload[1]["skipPreflight"], true);
        assert_eq!(payload[1]["maxRetries"], 0);
    }

    #[test]
    fn malformed_sender_signature_is_rejected() {
        assert!(parse_sender_signature("not-a-signature").is_err());
    }
}
