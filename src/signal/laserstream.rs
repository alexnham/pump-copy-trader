use std::collections::HashMap;

use async_trait::async_trait;
use futures_util::StreamExt;
use helius_laserstream::{
    ChannelOptions, LaserstreamConfig,
    grpc::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof,
    },
    subscribe,
};
use solana_sdk::pubkey::Pubkey;
use tokio::{sync::mpsc::Sender, time};
use tracing::{debug, info, warn};

use crate::{
    error::{CopyTraderError, Result},
    signal::{RecoveryClient, SignalSource, payload::decode_update},
    storage::Store,
    telemetry::compact_id,
};

const TRANSACTION_FILTER: &str = "copy-trader";
const SLOT_FILTER: &str = "connection-watch";
const RECONNECT_GAP_SLOTS: u64 = 4;

pub async fn check_connection(endpoint: String, api_key: &str) -> Result<()> {
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "doctor".to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };
    let config = LaserstreamConfig::new(endpoint, api_key.to_owned())
        .with_replay(false)
        .with_channel_options(channel_options())
        .with_max_reconnect_attempts(1);
    let (stream, _handle) = subscribe(config, request);
    tokio::pin!(stream);
    let update = time::timeout(std::time::Duration::from_secs(10), stream.next())
        .await
        .map_err(|_| CopyTraderError::Signal("LaserStream check timed out".to_owned()))?
        .ok_or_else(|| CopyTraderError::Signal("LaserStream check ended".to_owned()))?
        .map_err(|error| CopyTraderError::Signal(format!("LaserStream check failed: {error}")))?;
    if !matches!(update.update_oneof, Some(UpdateOneof::Slot(_))) {
        return Err(CopyTraderError::Signal(
            "LaserStream check returned an unexpected update".to_owned(),
        ));
    }
    Ok(())
}

pub struct LaserstreamSource {
    endpoint: String,
    api_key: String,
    wallet: Pubkey,
    store: Store,
    recovery: RecoveryClient,
    commitment: CommitmentLevel,
    lookups: Option<super::LookupCache>,
}

impl LaserstreamSource {
    pub fn new(
        endpoint: String,
        api_key: String,
        wallet: Pubkey,
        store: Store,
        recovery: RecoveryClient,
        commitment: &str,
    ) -> Self {
        Self {
            endpoint,
            api_key,
            wallet,
            store,
            recovery,
            lookups: None,
            commitment: match commitment {
                "confirmed" => CommitmentLevel::Confirmed,
                _ => CommitmentLevel::Processed,
            },
        }
    }

    pub(crate) fn with_lookup_cache(mut self, lookups: super::LookupCache) -> Self {
        self.lookups = Some(lookups);
        self
    }

    fn request(&self) -> SubscribeRequest {
        let transactions = HashMap::from([(
            TRANSACTION_FILTER.to_owned(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![self.wallet.to_string()],
                ..Default::default()
            },
        )]);
        let slots = HashMap::from([(
            SLOT_FILTER.to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]);
        SubscribeRequest {
            transactions,
            slots,
            commitment: Some(self.commitment as i32),
            ..Default::default()
        }
    }
}

#[async_trait]
impl SignalSource for LaserstreamSource {
    async fn run(&self, output: Sender<super::QueuedObservation>) -> Result<()> {
        // The application deliberately owns missed-trade classification. SDK replay
        // is disabled so a transaction received after reconnect cannot be mistaken
        // for a live signal and copied.
        let config = LaserstreamConfig::new(self.endpoint.clone(), self.api_key.clone())
            .with_replay(false)
            .with_channel_options(channel_options());
        let (stream, _handle) = subscribe(config, self.request());
        tokio::pin!(stream);
        let mut recovery_established = false;
        let mut last_slot: Option<u64> = None;

        info!(wallet = %compact_id(self.wallet.to_string()), "signal stream connecting");
        while let Some(update) = stream.next().await {
            let received_at = std::time::Instant::now();
            let update = update.map_err(|error| {
                CopyTraderError::Signal(format!("LaserStream subscription failed: {error}"))
            })?;

            if !recovery_established {
                self.recovery.establish_or_recover().await?;
                recovery_established = true;
                info!(wallet = %compact_id(self.wallet.to_string()), "signal stream is live");
            }

            match &update.update_oneof {
                Some(UpdateOneof::Slot(slot)) => {
                    if last_slot.is_some_and(|previous| {
                        slot.slot > previous.saturating_add(RECONNECT_GAP_SLOTS)
                    }) {
                        warn!(
                            previous_slot = last_slot.unwrap_or_default(),
                            slot = slot.slot,
                            "signal slot gap detected; auditing missed transactions"
                        );
                        self.recovery.establish_or_recover().await?;
                    }
                    last_slot = Some(slot.slot);
                }
                Some(UpdateOneof::Transaction(_)) => {
                    let payload_started = std::time::Instant::now();
                    let decoded = decode_update(update);
                    let payload_decode_us =
                        u64::try_from(payload_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                    match decoded {
                        Ok(Some(observed)) => {
                            if let Some(lookups) = &self.lookups {
                                lookups.observe(&observed)?;
                            }
                            let signature = observed.signature.to_string();
                            let slot = observed.slot;
                            let received_bytes = observed.received_bytes;
                            let database_timings = crate::storage::DatabaseTimings::default();
                            let observation_started = std::time::Instant::now();
                            let recorded = self
                                .store
                                .with_timings(database_timings.clone())
                                .record_observation(&observed)
                                .await?;
                            if !recorded {
                                continue;
                            }
                            output
                                .send(super::QueuedObservation {
                                    observed,
                                    received_at,
                                    queued_at: std::time::Instant::now(),
                                    payload_decode_us,
                                    observation_enqueue_us: u64::try_from(
                                        observation_started.elapsed().as_micros(),
                                    )
                                    .unwrap_or(u64::MAX),
                                    database_timings,
                                })
                                .await
                                .map_err(|_| {
                                    CopyTraderError::Signal("execution worker stopped".to_owned())
                                })?;
                            self.store
                                .update_cursor(&self.wallet.to_string(), &signature, slot)
                                .await?;
                            debug!(received_bytes, slot, source = %compact_id(&signature), "signal queued");
                        }
                        Ok(None) => {}
                        Err(error) => warn!(%error, "invalid signal discarded"),
                    }
                }
                _ => {}
            }
        }
        Err(CopyTraderError::Signal(
            "LaserStream subscription ended".to_owned(),
        ))
    }
}

fn channel_options() -> ChannelOptions {
    ChannelOptions {
        http2_keep_alive_interval_secs: Some(20),
        keep_alive_timeout_secs: Some(5),
        keep_alive_while_idle: Some(true),
        tcp_keepalive_secs: Some(30),
        tcp_nodelay: Some(true),
        ..ChannelOptions::default()
    }
}
