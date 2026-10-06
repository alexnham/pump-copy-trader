use serde::Deserialize;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info};
use url::Url;

use crate::{
    error::{CopyTraderError, Result},
    http::HttpTransport,
    storage::Store,
    telemetry::compact_id,
};

#[derive(Clone)]
pub struct RecoveryClient {
    http: std::sync::Arc<HttpTransport>,
    endpoint: Url,
    wallet: Pubkey,
    store: Store,
}

#[derive(Debug, Deserialize)]
struct SignatureInfo {
    signature: String,
    slot: u64,
}

impl RecoveryClient {
    pub fn new(
        endpoint: Url,
        wallet: Pubkey,
        store: Store,
        http: std::sync::Arc<HttpTransport>,
    ) -> Self {
        Self {
            http,
            endpoint,
            wallet,
            store,
        }
    }

    pub async fn establish_or_recover(&self) -> Result<()> {
        let cursor = self.store.cursor(&self.wallet.to_string()).await?;
        let newest = self.page(None).await?;
        if cursor.is_none() {
            if let Some(latest) = newest.first() {
                self.store
                    .update_cursor(&self.wallet.to_string(), &latest.signature, latest.slot)
                    .await?;
                info!(slot = latest.slot, source = %compact_id(&latest.signature), "recovery cursor established");
            } else {
                info!("recovery cursor awaiting first transaction");
            }
            return Ok(());
        }
        let (cursor_signature, _) = cursor.ok_or_else(|| {
            CopyTraderError::Storage("cursor disappeared during recovery".to_owned())
        })?;
        let mut before = None;
        let mut recovered = 0_u64;
        loop {
            let page = if before.is_none() {
                newest
                    .iter()
                    .map(|item| SignatureInfo {
                        signature: item.signature.clone(),
                        slot: item.slot,
                    })
                    .collect()
            } else {
                self.page(before.as_deref()).await?
            };
            if page.is_empty() {
                break;
            }
            let mut reached_cursor = false;
            for item in &page {
                if item.signature == cursor_signature {
                    reached_cursor = true;
                    break;
                }
                self.store
                    .record_recovered_signature(&item.signature, item.slot)
                    .await?;
                recovered = recovered.saturating_add(1);
            }
            if reached_cursor {
                break;
            }
            before = page.last().map(|item| item.signature.clone());
        }
        if recovered == 0 {
            debug!("recovery audit complete; no missed transactions");
        } else {
            info!(recovered, "recovery audit recorded missed transactions");
        }
        Ok(())
    }

    async fn page(&self, before: Option<&str>) -> Result<Vec<SignatureInfo>> {
        let mut options = json!({ "limit": 1_000, "commitment": "confirmed" });
        if let Some(before) = before {
            options["before"] = json!(before);
        }
        self.http
            .rpc(
                &self.endpoint,
                "getSignaturesForAddress",
                json!([self.wallet.to_string(), options]),
            )
            .await
    }
}
