use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client::{http_sender::HttpSender, rpc_client::RpcClientConfig};
use tracing::debug;
use url::Url;

use crate::{
    config::HttpConfig,
    error::{CopyTraderError, Result},
};

#[derive(Clone)]
pub struct HttpTransport {
    client: Client,
}

impl HttpTransport {
    pub fn new(config: &HttpConfig) -> Result<Arc<Self>> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .tcp_keepalive(Some(Duration::from_secs(config.tcp_keepalive_seconds)))
            .http2_keep_alive_interval(Some(Duration::from_secs(config.http2_keepalive_seconds)))
            .http2_keep_alive_timeout(Duration::from_secs(config.http2_keepalive_timeout_seconds))
            .http2_keep_alive_while_idle(true)
            .pool_idle_timeout(Some(Duration::from_secs(config.pool_idle_timeout_seconds)))
            .pool_max_idle_per_host(config.max_idle_connections_per_host)
            .build()
            .map_err(|error| {
                CopyTraderError::Configuration(format!("cannot build HTTP transport: {error}"))
            })?;
        Ok(Arc::new(Self { client }))
    }

    pub const fn client(&self) -> &Client {
        &self.client
    }

    pub fn solana_rpc(&self, endpoint: &Url) -> RpcClient {
        let sender = HttpSender::new_with_client(endpoint.as_str(), self.client.clone());
        RpcClient::new_sender(
            sender,
            RpcClientConfig::with_commitment(CommitmentConfig::processed()),
        )
    }

    pub async fn rpc<T: DeserializeOwned>(
        &self,
        endpoint: &Url,
        method: &str,
        params: Value,
    ) -> Result<T> {
        self.rpc_with_timeout(endpoint, method, params, Duration::from_secs(5))
            .await
    }

    pub async fn rpc_with_timeout<T: DeserializeOwned>(
        &self,
        endpoint: &Url,
        method: &str,
        params: Value,
        request_timeout: Duration,
    ) -> Result<T> {
        let started = Instant::now();
        let response = self
            .client
            .post(endpoint.clone())
            .timeout(request_timeout)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("{method} request failed: {error}"))
            })?
            .error_for_status()
            .map_err(|error| CopyTraderError::Execution(format!("{method} HTTP failure: {error}")))?
            .json::<RpcEnvelope<T>>()
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("invalid {method} response: {error}"))
            })?;
        if let Some(error) = response.error {
            return Err(CopyTraderError::Execution(format!(
                "{method} returned an error: {error}"
            )));
        }
        debug!(
            method,
            elapsed_ms = started.elapsed().as_millis(),
            "RPC request complete"
        );
        response
            .result
            .ok_or_else(|| CopyTraderError::Execution(format!("{method} response omitted result")))
    }

    pub async fn warm(&self, endpoint: &Url) -> Result<u64> {
        self.rpc(endpoint, "getSlot", json!([{ "commitment": "processed" }]))
            .await
    }
}

#[derive(Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<Value>,
}
