use super::diagnostics::Counter;
use super::{QueuedObservation, SignalSource, lookup_cache::LookupCache};
use crate::domain::signal::SourceV1Config;
use crate::{
    decode::TransactionDecoder,
    domain::{ObservedTransaction, SignalOrigin, TransactionMeta},
    error::{CopyTraderError, Result},
    storage::{DatabaseTimings, Store},
};
use agave_transaction_view::{
    sanitize::SanitizeConfig, transaction_version::TransactionVersion,
    transaction_view::TransactionView,
};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use solana_sdk::{
    hash::Hash,
    message::{
        Message, MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction, v0,
    },
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::time::{Duration, Instant};
use tokio::{net::TcpStream, sync::mpsc::Sender, time};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message as WsMessage, protocol::WebSocketConfig},
};
use tracing::{debug, info, warn};
use url::Url;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const SUBSCRIPTION_ID: u64 = 1;

pub struct PreconfirmationSource {
    endpoint: Url,
    wallet: Pubkey,
    store: Store,
    lookups: LookupCache,
    observation_only: bool,
}
impl PreconfirmationSource {
    pub(crate) fn new(
        mut endpoint: Url,
        api_key: &str,
        wallet: Pubkey,
        store: Store,
        lookups: LookupCache,
    ) -> Self {
        // Replace an existing API key without logging the authenticated URL.
        let parameters = endpoint
            .query_pairs()
            .filter(|(key, _)| key != "api-key")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        endpoint.set_query(None);
        endpoint
            .query_pairs_mut()
            .extend_pairs(parameters)
            .append_pair("api-key", api_key);
        Self {
            endpoint,
            wallet,
            store,
            lookups,
            observation_only: false,
        }
    }
    pub(crate) async fn diagnose(mut self, seconds: u64) -> Result<()> {
        self.observation_only = true;
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let diagnostics = self.lookups.1.clone();
        tokio::select! {
            result = self.run(sender) => result?,
            _ = time::sleep(Duration::from_secs(seconds)) => {}
        }
        let snapshot = diagnostics.snapshot();
        println!(
            "{}",
            serde_json::json!({"wallet": self.wallet.to_string(), "seconds": seconds, "diagnostics": snapshot})
        );
        std::fs::write(
            "/tmp/pump-copy-preconfirmation-probe.json",
            snapshot.to_string(),
        )?;
        Ok(())
    }
    fn request(&self) -> Value {
        // Receive both sources and filter known failures before execution.
        let filter = json!({"includeBam":true,"accountInclude":[self.wallet.to_string()],
            "signerInclude":[self.wallet.to_string()]});
        json!({"jsonrpc":"2.0", "id":SUBSCRIPTION_ID, "method":"preconfSubscribe", "params":[filter]})
    }
    async fn connect(&self) -> Result<Socket> {
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(64 * 1024);
        config.max_frame_size = Some(64 * 1024);
        let (mut socket, _) = time::timeout(
            Duration::from_secs(10),
            connect_async_with_config(self.endpoint.as_str(), Some(config), true),
        )
        .await
        .map_err(|_| signal_error("preconfirmation connection timed out"))?
        .map_err(|_| signal_error("preconfirmation connection failed"))?;
        socket
            .send(WsMessage::Text(self.request().to_string().into()))
            .await
            .map_err(|_| signal_error("preconfirmation subscribe send failed"))?;
        time::timeout(Duration::from_secs(10), async {
            while let Some(message) = socket.next().await {
                match message
                    .map_err(|_| signal_error("preconfirmation acknowledgment read failed"))?
                {
                    WsMessage::Text(text) => {
                        let response: Value = serde_json::from_str(&text)?;
                        if response["id"] != SUBSCRIPTION_ID {
                            continue;
                        }
                        if let Some(error) = response.get("error") {
                            return Err(signal_error(&format!(
                                "preconfirmation subscription rejected (code {})",
                                error["code"].as_i64().unwrap_or_default()
                            )));
                        }
                        if response["result"].as_u64().is_some() {
                            return Ok(());
                        }
                        return Err(signal_error("invalid preconfirmation acknowledgment"));
                    }
                    WsMessage::Ping(payload) => socket
                        .send(WsMessage::Pong(payload))
                        .await
                        .map_err(|_| signal_error("preconfirmation pong failed"))?,
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }
            Err(signal_error(
                "preconfirmation connection closed before acknowledgment",
            ))
        })
        .await
        .map_err(|_| signal_error("preconfirmation subscription timed out"))??;
        Ok(socket)
    }
    pub(crate) async fn check_connection(&self) -> Result<()> {
        let mut socket = self.connect().await?;
        let _ = socket.close(None).await;
        Ok(())
    }
    async fn session(&self, output: &Sender<QueuedObservation>) -> Result<()> {
        let mut socket = self.connect().await?;
        self.lookups.1.increment(Counter::Subscriptions);
        info!(wallet = %self.wallet, include_bam = true, all_statuses = true, "preconfirmation stream subscribed");
        let decoder = TransactionDecoder::new(self.wallet);
        let mut heartbeat = time::interval(Duration::from_secs(30));
        heartbeat.tick().await;
        let mut diagnostics_tick = time::interval(Duration::from_secs(30));
        diagnostics_tick.tick().await;
        let mut pong_deadline = None;
        loop {
            let deadline =
                pong_deadline.unwrap_or_else(|| time::Instant::now() + Duration::from_secs(3600));
            tokio::select! {
                _ = diagnostics_tick.tick() => {
                    let diagnostics = self.lookups.1.snapshot();
                    info!(%diagnostics, "preconfirmation diagnostics");
                    let observation_only = self.observation_only;
                    tokio::task::spawn_blocking(move || {
                        let path = if observation_only { "/tmp/pump-copy-preconfirmation-probe.json" } else { "/tmp/pump-copy-preconfirmation-diagnostics.json" };
                        let temp = format!("{path}.tmp");
                        if std::fs::write(&temp, diagnostics.to_string()).is_ok() { let _ = std::fs::rename(&temp, path); }
                    });
                }
                _ = output.closed() => { let _ = socket.close(None).await; return Ok(()); }
                _ = time::sleep_until(deadline), if pong_deadline.is_some() =>
                    return Err(signal_error("preconfirmation heartbeat timed out")),
                _ = heartbeat.tick() => {
                    socket.send(WsMessage::Ping(Vec::new().into())).await
                        .map_err(|_| signal_error("preconfirmation heartbeat send failed"))?;
                    pong_deadline = Some(time::Instant::now() + Duration::from_secs(10));
                }
                message = socket.next() => {
                    let received_at = Instant::now();
                    match message.ok_or_else(|| signal_error("preconfirmation stream ended"))?
                        .map_err(|_| signal_error("preconfirmation stream read failed"))? {
                        WsMessage::Binary(frame) => {
                            self.lookups.1.increment(Counter::BinaryReceived);
                            if frame.len() >= 19 && frame[0] == 1 {
                                match frame[17] {
                                    2 => self.lookups.1.increment(Counter::UnknownStatusReceived),
                                    0 => self.lookups.1.increment(Counter::FailedStatusReceived),
                                    _ => {}
                                }
                            }
                            let decode_started = Instant::now();
                            let observed = match decode_frame(&frame, &self.lookups) {
                                Ok(Some(observed)) => observed,
                                Ok(None) => {
                                    self.lookups.1.increment(if frame.get(17) == Some(&1) { Counter::LookupCacheMisses } else { Counter::StatusIgnored });
                                    continue;
                                },
                                Err(error) => { self.lookups.1.increment(Counter::InvalidFrames); warn!(%error, "invalid preconfirmation discarded"); continue; }
                            };
                            self.lookups.1.observe(observed.signature, true);
                            // Unsupported early signals must not create a durable unsupported
                            // classification or prevent the later processed signal from trading.
                            if let Err(error) = decoder.decode(&observed) {
                                let reason = error.to_string();
                                self.lookups.1.increment(if reason.contains("wrapper requires") { Counter::WrapperRejections } else if reason.contains("sell requires") { Counter::SellRejections } else { Counter::OtherDecodeRejections });
                                if crate::decode::terminal::present(&observed) { info!(signature = %observed.signature, %error, "Terminal preconfirmation deferred to processed stream"); } else { debug!(%error, "preconfirmation deferred to processed stream"); }
                                continue;
                            }
                            self.lookups.1.increment(Counter::Ready);
                            if self.observation_only { continue; }
                            let payload_decode_us = micros(decode_started.elapsed());
                            let permit = match output.try_reserve() {
                                Ok(permit) => permit,
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    self.lookups.1.increment(Counter::QueueFull);
                                    warn!("preconfirmation queue full; deferring to processed stream"); continue;
                                }
                                Err(_) => return Ok(()),
                            };
                            let enqueue_started = Instant::now();
                            let database_timings = DatabaseTimings::default();
                            if !self.store.with_timings(database_timings.clone()).record_observation(&observed).await? { self.lookups.1.increment(Counter::Duplicate); continue; }
                            self.lookups.1.increment(Counter::Enqueued);
                            permit.send(QueuedObservation {
                                observed, received_at, queued_at: Instant::now(), payload_decode_us,
                                observation_enqueue_us: micros(enqueue_started.elapsed()), database_timings,
                            });
                        }
                        WsMessage::Pong(_) => pong_deadline = None,
                        WsMessage::Ping(payload) => socket.send(WsMessage::Pong(payload)).await
                            .map_err(|_| signal_error("preconfirmation pong failed"))?,
                        WsMessage::Close(_) => return Err(signal_error("preconfirmation stream closed")),
                        _ => {}
                    }
                }
            }
        }
    }
}
#[async_trait]
impl SignalSource for PreconfirmationSource {
    async fn run(&self, output: Sender<QueuedObservation>) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            let started = Instant::now();
            let result = self.session(&output).await;
            if output.is_closed() {
                return Ok(());
            }
            if let Err(error) = result {
                self.lookups.1.increment(Counter::Reconnects);
                warn!(%error, retry_seconds = backoff.as_secs(), "preconfirmation stream unavailable; LaserStream remains active");
            }
            if started.elapsed() > Duration::from_secs(60) {
                backoff = Duration::from_secs(1);
            }
            tokio::select! { _ = output.closed() => return Ok(()), _ = time::sleep(backoff) => {} }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}
fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
fn signal_error(message: &str) -> CopyTraderError {
    CopyTraderError::Signal(message.to_owned())
}

fn decode_frame(frame: &[u8], lookups: &LookupCache) -> Result<Option<ObservedTransaction>> {
    if frame.len() < 19 {
        return Err(signal_error("truncated preconfirmation frame"));
    }
    if frame[0] != 1 {
        return Err(signal_error("unsupported preconfirmation schema version"));
    }
    match frame[17] {
        0 => return Ok(None), // Known failures never trigger execution.
        1 | 2 => {}
        _ => return Err(signal_error("invalid preconfirmation status")),
    }
    let slot = u64::from_le_bytes(frame[1..9].try_into().expect("checked header"));
    let tx_index = u64::from_le_bytes(frame[9..17].try_into().expect("checked header"));
    let view = TransactionView::try_new_sanitized(
        &frame[18..],
        &SanitizeConfig {
            min_requested_heap_size: 32 * 1024,
            max_requested_heap_size: 256 * 1024,
            max_instructions: 64,
            max_accounts_per_instruction: 256,
        },
    )
    .map_err(|error| signal_error(&format!("invalid preconfirmation transaction: {error:?}")))?;
    let signature = Signature::try_from(view.signatures()[0].as_ref())
        .map_err(|_| signal_error("invalid preconfirmation signature"))?;
    let header = MessageHeader {
        num_required_signatures: view.num_required_signatures(),
        num_readonly_signed_accounts: view.num_readonly_signed_static_accounts(),
        num_readonly_unsigned_accounts: view.num_readonly_unsigned_static_accounts(),
    };
    let account_keys = view
        .static_account_keys()
        .iter()
        .map(|key| Pubkey::new_from_array(key.to_bytes()))
        .collect();
    let recent_blockhash = Hash::new_from_array(view.recent_blockhash().to_bytes());
    let instructions = view
        .instructions_iter()
        .map(|ix| CompiledInstruction {
            program_id_index: ix.program_id_index,
            accounts: ix.accounts.to_vec(),
            data: ix.data.to_vec(),
        })
        .collect();
    let message = match view.version() {
        TransactionVersion::V0 => VersionedMessage::V0(v0::Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups: view
                .address_table_lookup_iter()
                .map(|lookup| v0::MessageAddressTableLookup {
                    account_key: Pubkey::new_from_array(lookup.account_key.to_bytes()),
                    writable_indexes: lookup.writable_indexes.to_vec(),
                    readonly_indexes: lookup.readonly_indexes.to_vec(),
                })
                .collect(),
        }),
        // Normalize v1 to the same read-only inline-account view as LaserStream.
        TransactionVersion::Legacy | TransactionVersion::V1 => VersionedMessage::Legacy(Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        }),
    };
    let Some(loaded) = lookups.resolve(&message)? else {
        return Ok(None);
    };
    let source_v1_config = view.transaction_config().map(|config| SourceV1Config {
        priority_fee: config.priority_fee_lamports(),
        compute_unit_limit: config.compute_unit_limit(),
        loaded_accounts_data_size_limit: config.loaded_accounts_data_size_limit(),
        heap_size: config.requested_heap_size(),
    });
    Ok(Some(ObservedTransaction {
        signature, slot, block_time: None, origin: SignalOrigin::Preconfirmation,
        transaction: VersionedTransaction {
            signatures: view.signatures().iter().map(|sig| Signature::try_from(sig.as_ref()).expect("signature size")).collect(),
            message,
        },
        meta: TransactionMeta { live_loaded_addresses: Some(loaded), source_v1_config, preconfirmation_status: Some(frame[17]), ..Default::default() },
        raw_payload: json!({"feed":"preconfirmation", "slot":slot,"transactionIndex":tx_index,"status":if frame[17] == 1 { "success" } else { "unknown" }}).to_string(),
        received_bytes: frame.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode::preconfirmation::buy_fixture, domain::DexKind, routing::pump_fun};

    fn frame(observed: &ObservedTransaction, status: u8) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&observed.slot.to_le_bytes());
        bytes.extend_from_slice(&5_u64.to_le_bytes());
        bytes.push(status);
        if crate::decode::terminal::present(observed) {
            let VersionedMessage::Legacy(message) = &observed.transaction.message else {
                panic!("inline fixture")
            };
            let mut wire = vec![
                0x81,
                message.header.num_required_signatures,
                message.header.num_readonly_signed_accounts,
                message.header.num_readonly_unsigned_accounts,
            ];
            wire.extend_from_slice(&7u32.to_le_bytes());
            wire.extend_from_slice(message.recent_blockhash.as_ref());
            wire.push(message.instructions.len() as u8);
            wire.push(message.account_keys.len() as u8);
            for key in &message.account_keys {
                wire.extend_from_slice(key.as_ref());
            }
            wire.extend_from_slice(&100u64.to_le_bytes());
            wire.extend_from_slice(&300_000u32.to_le_bytes());
            for ix in &message.instructions {
                wire.push(ix.program_id_index);
                wire.push(ix.accounts.len() as u8);
                wire.extend_from_slice(&(ix.data.len() as u16).to_le_bytes());
            }
            for ix in &message.instructions {
                wire.extend_from_slice(&ix.accounts);
                wire.extend_from_slice(&ix.data);
            }
            for signature in &observed.transaction.signatures {
                wire.extend_from_slice(signature.as_ref());
            }
            bytes.extend(wire);
        } else {
            bytes.extend(bincode::serialize(&observed.transaction).unwrap());
        }
        bytes
    }

    #[test]
    fn binary_frames_preserve_signature_and_accept_unknown_status() {
        let (observed, wallet, _) = buy_fixture(DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR);
        let cache = LookupCache::default();
        let bytes = frame(&observed, 1);
        let decoded = decode_frame(&bytes, &cache).unwrap().unwrap();
        assert_eq!(decoded.signature, observed.signature);
        assert_eq!(decoded.slot, 100);
        assert_eq!(decoded.origin, SignalOrigin::Preconfirmation);
        assert!(TransactionDecoder::new(wallet).decode(&decoded).is_ok());
        let unknown = decode_frame(&frame(&observed, 2), &cache).unwrap().unwrap();
        assert_eq!(unknown.meta.preconfirmation_status, Some(2));
        assert_eq!(
            serde_json::from_str::<Value>(&unknown.raw_payload).unwrap()["status"],
            "unknown"
        );
        assert!(TransactionDecoder::new(wallet).decode(&unknown).is_ok());

        for status in [0] {
            assert!(
                decode_frame(&frame(&observed, status), &cache)
                    .unwrap()
                    .is_none()
            );
        }
        assert!(decode_frame(&frame(&observed, 3), &cache).is_err());
        for len in [0, 17, 18, 20, bytes.len() - 1] {
            assert!(decode_frame(&bytes[..len], &cache).is_err());
        }
        let mut invalid = bytes.clone();
        invalid[0] = 2;
        assert!(decode_frame(&invalid, &cache).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_frame(&trailing, &cache).is_err());
    }

    #[test]
    fn v0_defers_until_all_lookup_indices_are_known_in_solana_order() {
        let (mut observed, wallet, _) = buy_fixture(DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR);
        let VersionedMessage::Legacy(message) = observed.transaction.message.clone() else {
            unreachable!()
        };
        observed.transaction.message = VersionedMessage::V0(v0::Message {
            header: message.header,
            account_keys: message.account_keys,
            recent_blockhash: message.recent_blockhash,
            instructions: message.instructions,
            address_table_lookups: vec![
                v0::MessageAddressTableLookup {
                    account_key: Pubkey::new_unique(),
                    writable_indexes: vec![3],
                    readonly_indexes: vec![5],
                },
                v0::MessageAddressTableLookup {
                    account_key: Pubkey::new_unique(),
                    writable_indexes: vec![7],
                    readonly_indexes: vec![9],
                },
            ],
        });
        let cache = LookupCache::default();
        let bytes = frame(&observed, 1);
        assert!(decode_frame(&bytes, &cache).unwrap().is_none());
        let addresses = (0..4).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        observed.meta.live_loaded_addresses = Some(addresses.clone());
        cache.observe(&observed).unwrap();
        let decoded = decode_frame(&bytes, &cache).unwrap().unwrap();
        assert_eq!(decoded.meta.live_loaded_addresses, Some(addresses));
        assert!(TransactionDecoder::new(wallet).decode(&decoded).is_ok());
        let VersionedMessage::V0(message) = &mut observed.transaction.message else {
            unreachable!()
        };
        message.address_table_lookups[0].readonly_indexes[0] = 6;
        assert!(
            decode_frame(&frame(&observed, 1), &cache)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn message_first_v1_is_normalized_with_its_budget_configuration() {
        let (observed, wallet, _) = buy_fixture(DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR);
        let VersionedMessage::Legacy(message) = &observed.transaction.message else {
            unreachable!()
        };
        // SIMD-0385: fixed header, inline keys, config words, instruction headers,
        // all instruction payloads, then signatures (no signature count prefix).
        let mut wire = vec![
            0x81,
            message.header.num_required_signatures,
            message.header.num_readonly_signed_accounts,
            message.header.num_readonly_unsigned_accounts,
        ];
        wire.extend_from_slice(&7_u32.to_le_bytes()); // fee u64 + compute limit u32
        wire.extend_from_slice(message.recent_blockhash.as_ref());
        wire.push(message.instructions.len() as u8);
        wire.push(message.account_keys.len() as u8);
        for key in &message.account_keys {
            wire.extend_from_slice(key.as_ref());
        }
        wire.extend_from_slice(&100_u64.to_le_bytes());
        wire.extend_from_slice(&300_000_u32.to_le_bytes());
        for ix in &message.instructions {
            wire.push(ix.program_id_index);
            wire.push(ix.accounts.len() as u8);
            wire.extend_from_slice(&(ix.data.len() as u16).to_le_bytes());
        }
        for ix in &message.instructions {
            wire.extend_from_slice(&ix.accounts);
            wire.extend_from_slice(&ix.data);
        }
        for signature in &observed.transaction.signatures {
            wire.extend_from_slice(signature.as_ref());
        }
        let mut bytes = frame(&observed, 1);
        bytes.truncate(18);
        bytes.extend(wire);
        let decoded = decode_frame(&bytes, &LookupCache::default())
            .unwrap()
            .unwrap();
        assert_eq!(decoded.signature, observed.signature);
        let config = decoded.meta.source_v1_config.unwrap();
        assert_eq!(config.priority_fee, Some(100));
        assert_eq!(config.compute_unit_limit, Some(300_000));
        assert!(TransactionDecoder::new(wallet).decode(&decoded).is_ok());
    }

    #[tokio::test]
    async fn websocket_subscription_filters_wallet_and_only_queues_eligible_successes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("ws://{}/", listener.local_addr().unwrap())).unwrap();
        let (observed, wallet) = crate::decode::terminal::tests::observation(
            &crate::decode::terminal::tests::fixtures()[0],
        );
        let roundtrip = decode_frame(&frame(&observed, 1), &LookupCache::default())
            .expect("fixture frame")
            .expect("frame decoded");
        TransactionDecoder::new(wallet)
            .decode(&roundtrip)
            .expect("fixture trade");
        let signature = observed.signature;
        let valid_frame = frame(&observed, 1);
        let failed_frame = frame(&observed, 0);
        let unknown_frame = frame(&observed, 2);
        let (sell, _, _) = buy_fixture(DexKind::PumpFun, pump_fun::SELL_DISCRIMINATOR);
        let sell_frame = frame(&sell, 1);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let WsMessage::Text(text) = ws.next().await.unwrap().unwrap() else {
                panic!("subscribe")
            };
            let request: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(request["method"], "preconfSubscribe");
            assert_eq!(request["params"][0]["includeBam"], true);
            assert!(request["params"][0].get("failed").is_none());
            assert_eq!(
                request["params"][0]["accountInclude"][0],
                wallet.to_string()
            );
            assert_eq!(request["params"][0]["signerInclude"][0], wallet.to_string());
            ws.send(WsMessage::Text(
                json!({"jsonrpc":"2.0","id":1,"result":123})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            for frame in [failed_frame, unknown_frame, sell_frame, valid_frame] {
                ws.send(WsMessage::Binary(frame.into())).await.unwrap();
            }
            while let Some(message) = ws.next().await {
                if matches!(message, Ok(WsMessage::Close(_))) {
                    break;
                }
            }
        });
        let store = Store::connect("sqlite::memory:").await.unwrap();
        let source = PreconfirmationSource::new(
            endpoint,
            "test-key",
            wallet,
            store.clone(),
            LookupCache::default(),
        );
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(async move { source.run(sender).await });
        let queued = time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queued.observed.signature, signature);
        assert_eq!(queued.observed.origin, SignalOrigin::Preconfirmation);
        assert_eq!(queued.observed.meta.preconfirmation_status, Some(2));
        assert!(receiver.try_recv().is_err());
        let rows = store.status(10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].signature, signature.to_string());
        drop(receiver);
        time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn subscription_errors_are_reported_without_echoing_credentials() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("ws://{}/", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(WsMessage::Text(
                json!({"id":1,"error":{"code":-32000,"message":"secret-key unauthorized"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        });
        let source = PreconfirmationSource::new(
            endpoint,
            "secret-key",
            Pubkey::new_unique(),
            Store::connect("sqlite::memory:").await.unwrap(),
            LookupCache::default(),
        );
        let error = source.check_connection().await.unwrap_err().to_string();
        assert!(error.contains("-32000"));
        assert!(!error.contains("secret-key"));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn probe_includes_bam_and_all_statuses_without_journaling() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("ws://{}/", listener.local_addr().unwrap())).unwrap();
        let (observed, wallet, _) = buy_fixture(DexKind::PumpFun, pump_fun::BUY_DISCRIMINATOR);
        let unknown = frame(&observed, 2);
        let failed = frame(&observed, 0);
        let success = frame(&observed, 1);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let WsMessage::Text(text) = ws.next().await.unwrap().unwrap() else {
                panic!("expected request")
            };
            let request: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(request["params"][0]["includeBam"], true);
            assert!(request["params"][0].get("failed").is_none());
            ws.send(WsMessage::Text(
                json!({"jsonrpc":"2.0","id":1,"result":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            for data in [unknown, failed, success] {
                ws.send(WsMessage::Binary(data.into())).await.unwrap();
            }
            while ws.next().await.is_some() {}
        });
        let store = Store::connect("sqlite::memory:").await.unwrap();
        let lookups = LookupCache::default();
        let stats = lookups.1.clone();
        let mut source =
            PreconfirmationSource::new(endpoint, "test-key", wallet, store.clone(), lookups);
        source.observation_only = true;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(async move { source.session(&sender).await });
        time::timeout(Duration::from_secs(3), async {
            while stats.snapshot()["ready"] != 2 {
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot["binary_received"], 3);
        assert_eq!(snapshot["unknown_status_received"], 1);
        assert_eq!(snapshot["failed_status_received"], 1);
        assert_eq!(snapshot["status_ignored"], 1);
        assert_eq!(snapshot["enqueued"], 0);
        assert!(receiver.try_recv().is_err());
        assert!(store.status(10).await.unwrap().is_empty());
        task.abort();
        server.abort();
    }
}
