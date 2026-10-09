use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use tokio::{
    sync::{Mutex, mpsc::Receiver},
    task::JoinSet,
};
use tracing::{error, info, warn};

use crate::{
    config::AppConfig,
    decode::TransactionDecoder,
    domain::{
        AssetId, AttemptStatus, ObservedTransaction, SizedTrade, SkipReason, TradeIntent,
        UiTokenBalance, UnsupportedReason,
    },
    error::{CopyTraderError, Result},
    execution::{
        ExecutionBackend, cache::WalletBalanceCache, confirm::wait_for_confirmation,
        sizing::SizingPolicy,
    },
    routing::{ExecutableRoute, Router, RoutingTimings, pump_fun},
    signal::QueuedObservation,
    storage::{DatabaseTimings, Store, TimingWriter},
    token::{
        accounts::associated_token_address,
        extensions::{MintInfo, TokenSafetyClient},
    },
};

pub struct ExecutionWorker {
    config: Arc<AppConfig>,
    signer: Arc<Keypair>,
    store: Store,
    backend: Arc<ExecutionBackend>,
    token_safety: TokenSafetyClient,
    decoder: TransactionDecoder,
    router: Arc<Router>,
    balance_cache: WalletBalanceCache,
    last_submitted_slot: u64,
    pending: JoinSet<()>,
    reservations: Arc<Mutex<BalanceReservations>>,
    dispatched: bool,
    reservation_gate: Arc<Mutex<()>>,
    handled_signatures: std::collections::HashSet<solana_sdk::signature::Signature>,
    execution_admitted: bool,
}

impl ExecutionWorker {
    pub fn new(
        config: Arc<AppConfig>,
        signer: Arc<Keypair>,
        store: Store,
        backend: Arc<ExecutionBackend>,
        token_safety: TokenSafetyClient,
        decoder: TransactionDecoder,
        router: Arc<Router>,
    ) -> Self {
        Self {
            config,
            signer,
            store,
            backend,
            token_safety,
            decoder,
            router,
            balance_cache: WalletBalanceCache::default(),
            last_submitted_slot: 0,
            pending: JoinSet::new(),
            reservations: Default::default(),
            dispatched: false,
            reservation_gate: Default::default(),
            handled_signatures: Default::default(),
            execution_admitted: false,
        }
    }

    pub fn with_balance_cache(mut self, balance_cache: WalletBalanceCache) -> Self {
        self.balance_cache = balance_cache;
        self
    }

    pub async fn run(mut self, input: Receiver<QueuedObservation>) -> Result<()> {
        let writer = TimingWriter::new(self.store.clone());
        let result = self.run_inner(input, &writer).await;
        while let Some(result) = self.pending.join_next().await {
            if let Err(error) = result {
                error!(%error, "background settlement task failed; reservations retained");
            }
        }
        writer.finish().await;
        result
    }

    async fn run_inner(
        &mut self,
        mut input: Receiver<QueuedObservation>,
        writer: &TimingWriter,
    ) -> Result<()> {
        while let Some(queued) = input.recv().await {
            if self.handled_signatures.contains(&queued.observed.signature) {
                continue;
            }
            let preconfirmation =
                queued.observed.origin == crate::domain::SignalOrigin::Preconfirmation;
            if preconfirmation
                && queued.received_at.elapsed().as_secs()
                    > self.config.execution.max_signal_age_seconds
            {
                warn!("stale queued preconfirmation deferred to processed stream");
                continue;
            }
            while let Some(result) = self.pending.try_join_next() {
                if let Err(error) = result {
                    error!(%error, "background settlement task failed; reservations retained");
                }
            }
            // Bound settlement RPC fanout during sustained bursts.
            if self.pending.len() >= 64 {
                if let Some(Err(error)) = self.pending.join_next().await {
                    error!(%error, "background settlement task failed; reservations retained");
                }
            }
            self.dispatched = false;
            self.execution_admitted = false;
            let worker_started = Instant::now();
            let queue_wait_us =
                CopyTimings::micros(worker_started.duration_since(queued.queued_at));
            let source_signature = queued.observed.signature.to_string();
            let signature = queued.observed.signature;
            let mut timings = CopyTimings::new(queued.received_at);
            timings
                .values
                .insert("preconfirmation", u64::from(preconfirmation));
            timings.values.insert("queue_wait_us", queue_wait_us);
            timings
                .values
                .insert("payload_decode_us", queued.payload_decode_us);
            timings
                .values
                .insert("observation_enqueue_us", queued.observation_enqueue_us);
            timings.values.insert(
                "ingress_to_worker_us",
                CopyTimings::micros(worker_started.duration_since(queued.received_at)),
            );
            timings.database = queued.database_timings;
            self.store = self.store.with_timings(timings.database.clone());
            let result = self.handle(queued.observed, &mut timings, writer).await;
            if !preconfirmation || self.execution_admitted {
                self.handled_signatures.insert(signature);
            }
            timings.finish();
            if let Err(error) = result {
                if preconfirmation && !self.execution_admitted {
                    warn!(%error, %source_signature, "preconfirmation preparation deferred to processed stream");
                    continue;
                }
                if let CopyTraderError::OutOfScope(reason, message) = &error {
                    self.store
                        .mark_unsupported(&source_signature, *reason, message)
                        .await?;
                } else if let CopyTraderError::Unsupported(message) = &error {
                    self.store
                        .mark_unsupported(
                            &source_signature,
                            UnsupportedReason::UnsupportedInstruction,
                            message,
                        )
                        .await?;
                } else {
                    self.store
                        .update_attempt(
                            &source_signature,
                            AttemptStatus::Failed,
                            Some(&error.to_string()),
                        )
                        .await?;
                    error!(%error, %source_signature, "copy attempt failed");
                }
            }
            if self.dispatched {
                continue;
            }
            let json = timings.json()?;
            info!(%source_signature, timings = %json, "copy timings");
            writer.enqueue(source_signature, json);
        }
        Ok(())
    }

    async fn handle(
        &mut self,
        observed: ObservedTransaction,
        timings: &mut CopyTimings,
        writer: &TimingWriter,
    ) -> Result<()> {
        let reservation_gate = self.reservation_gate.clone();
        let _reservation_guard = reservation_gate.lock().await;
        let checks_started = Instant::now();
        let source_signature = observed.signature.to_string();
        if observed.slot < self.last_submitted_slot {
            self.store
                .mark_skipped(&source_signature, SkipReason::OutOfOrder)
                .await?;
            return Ok(());
        }
        if self.is_stale(&observed)? {
            self.store
                .mark_skipped(&source_signature, SkipReason::StaleSignal)
                .await?;
            return Ok(());
        }
        if observed.meta.err.is_some() {
            self.store
                .mark_skipped(&source_signature, SkipReason::FailedSource)
                .await?;
            return Ok(());
        }
        timings.values.insert(
            "pre_decode_checks_us",
            CopyTimings::micros(checks_started.elapsed()),
        );
        let decode_started = Instant::now();
        let decoded = self.decoder.decode(&observed);
        timings
            .values
            .insert("decode_us", CopyTimings::micros(decode_started.elapsed()));
        let intent = match decoded {
            Ok(intent) => intent,
            Err(CopyTraderError::Unsupported(reason)) => {
                return Err(CopyTraderError::OutOfScope(
                    UnsupportedReason::AmbiguousTrade,
                    reason,
                ));
            }
            Err(CopyTraderError::Decode(reason)) => {
                return Err(CopyTraderError::OutOfScope(
                    UnsupportedReason::UnsupportedInstruction,
                    reason,
                ));
            }
            Err(error) => return Err(error),
        };
        let preparation_started = Instant::now();
        timings.mark("decode_complete_ms");
        if intent.source_pool.is_some() {
            timings.mark("source_pool_identified_ms");
        }
        info!(
            %source_signature,
            source_pool = intent.source_pool.map(|pool| pool.dex.as_str()),
            source_instruction_program = intent
                .source_instruction
                .as_ref()
                .map(|source| source.instruction.program_id.to_string()),
            source_instruction_data_len = intent
                .source_instruction
                .as_ref()
                .map(|source| source.instruction.data.len()),
            source_instruction_accounts = intent
                .source_instruction
                .as_ref()
                .map(|source| source.instruction.accounts.len()),
            source_instruction_present = intent.source_instruction.is_some(),
            "source copy candidates decoded"
        );
        let input_mint = intent.input_asset.routing_mint();
        let output_mint = intent.output_asset.routing_mint();
        let Some(input_rule) = self.config.token_rule(input_mint) else {
            self.store.mark_intent(&source_signature, &intent).await?;
            self.store
                .mark_skipped(&source_signature, SkipReason::MintNotAllowed)
                .await?;
            return Ok(());
        };
        if self.config.token_rule(output_mint).is_none() {
            self.store.mark_intent(&source_signature, &intent).await?;
            self.store
                .mark_skipped(&source_signature, SkipReason::MintNotAllowed)
                .await?;
            return Ok(());
        }
        let mut routing_timings = RoutingTimings::default();
        let direct_mints =
            direct_source_mints(&intent, observed.meta.post_token_balances.as_deref());
        let (intent_write, reads) = tokio::join!(
            self.store.mark_intent(&source_signature, &intent),
            self.initial_reads(&intent, direct_mints),
        );
        intent_write?;
        let InitialReads {
            input_info,
            output_info,
            native_balance,
            mint_read_ms,
            mint_from_source,
        } = reads?;
        timings.values.insert("mint_read_ms", mint_read_ms);
        timings
            .values
            .insert("mint_from_source", u64::from(mint_from_source));
        let sizing = SizingPolicy::new(&self.config.sizing);
        let input_account = associated_token_address(
            &self.signer.pubkey(),
            &input_mint,
            &input_info.token_program,
        );
        let exiting_to_sol = matches!(intent.input_asset, AssetId::Token(_))
            && intent.output_asset == AssetId::NativeSol;
        let exit_balance = if exiting_to_sol {
            Some(
                self.cached_asset_balance(intent.input_asset, &input_account)
                    .await?,
            )
        } else {
            None
        };
        let result = if let Some(balance) = exit_balance {
            let source_before = source_position_before(
                &intent,
                self.config.signal.wallet,
                observed.meta.pre_token_balances.as_deref(),
            )?;
            timings
                .values
                .insert("source_position_before_raw", source_before);
            timings.values.insert("copier_position_before_raw", balance);
            sizing.size_exit(intent, source_before, balance)?
        } else {
            sizing.size_trade(intent, &input_rule, input_info.decimals)?
        };
        let sized = match result {
            Ok(sized) => sized,
            Err(reason) => {
                self.store.mark_skipped(&source_signature, reason).await?;
                return Ok(());
            }
        };
        let pump_swap_buy = sized.intent.input_asset == AssetId::NativeSol
            && sized
                .intent
                .source_instruction
                .as_ref()
                .is_some_and(|source| {
                    source.instruction.program_id == crate::domain::DexKind::PumpSwap.program_id()
                });
        let cached_wsol = if pump_swap_buy {
            let reserved = self
                .reservations
                .lock()
                .await
                .remaining
                .get(&input_account)
                .copied();
            if let Some(amount) = reserved {
                amount
            } else {
                self.balance_cache.get(&input_account).await.unwrap_or(0)
            }
        } else {
            0
        };
        let available = if let Some(balance) = exit_balance.or(native_balance) {
            balance
        } else {
            self.cached_asset_balance(sized.intent.input_asset, &input_account)
                .await?
        };
        let wrap_lamports = sized
            .input_amount
            .checked_sub(cached_wsol.min(sized.input_amount))
            .ok_or_else(|| CopyTraderError::Execution("WSOL funding subtraction failed".into()))?;
        let native_available = available;
        let available = available
            .checked_add(cached_wsol)
            .ok_or_else(|| CopyTraderError::Execution("combined SOL balance overflow".into()))?;
        if pump_swap_buy {
            timings.values.insert("cached_wsol_lamports", cached_wsol);
            timings.values.insert("wrap_lamports", wrap_lamports);
            if cached_wsol > 0 {
                let mainnet = self.config.mainnet.as_ref().ok_or_else(|| {
                    CopyTraderError::Execution("mainnet configuration missing".into())
                })?;
                let price = mainnet.execution_priority_fee();
                let priority = u64::from(crate::routing::pump_swap::COMPUTE_UNIT_LIMIT)
                    .checked_mul(price)
                    .and_then(|value| value.checked_add(999_999))
                    .map(|value| value / 1_000_000)
                    .ok_or_else(|| CopyTraderError::Execution("priority fee overflow".into()))?;
                let required_native = wrap_lamports
                    .checked_add(priority)
                    .and_then(|value| value.checked_add(5_000))
                    .and_then(|value| value.checked_add(mainnet.execution_tip_budget()))
                    .ok_or_else(|| CopyTraderError::Execution("native funding overflow".into()))?;
                if native_available < required_native {
                    self.store
                        .mark_skipped(&source_signature, SkipReason::InsufficientBalance)
                        .await?;
                    return Ok(());
                }
            }
        }
        if available < sized.input_amount {
            self.store
                .mark_skipped(&source_signature, SkipReason::InsufficientBalance)
                .await?;
            return Ok(());
        }
        let preconfirmation = observed.origin == crate::domain::SignalOrigin::Preconfirmation;
        if !preconfirmation
            && !self
                .store
                .reserve_attempt(
                    &source_signature,
                    self.backend.target(),
                    sized.input_amount,
                    0,
                )
                .await?
        {
            return Ok(());
        }
        self.execution_admitted = !preconfirmation;

        timings.values.insert(
            "pre_route_preparation_us",
            CopyTimings::micros(preparation_started.elapsed()),
        );
        timings.stage("route_ms");
        let started = Instant::now();
        let result = self
            .router
            .build_with_wsol(
                &sized,
                self.signer.as_ref(),
                self.backend.as_ref(),
                self.config.execution.slippage_bps,
                &mut routing_timings,
                cached_wsol,
            )
            .await;
        timings.finish();
        let route_latency_ms = CopyTimings::millis(started.elapsed());
        timings
            .values
            .insert("route_wall_ms", CopyTimings::millis(started.elapsed()));
        timings
            .values
            .insert("route_wall_us", CopyTimings::micros(started.elapsed()));
        timings.values.insert("route_ms", route_latency_ms);
        timings.values.extend(routing_timings.stages.snapshot());
        let winner = result?;
        // Build early candidates before claiming, so unsupported routes can still
        // be retried with the processed transaction's execution metadata.
        if preconfirmation {
            if !self
                .store
                .reserve_attempt(
                    &source_signature,
                    self.backend.target(),
                    sized.input_amount,
                    winner.route.minimum_output,
                )
                .await?
            {
                self.execution_admitted = true; // Another feed or a previous run already claimed it.
                return Ok(());
            }
            self.execution_admitted = true;
        }
        timings.mark("quote_complete_ms");
        timings.mark("checks_complete_ms");
        timings.stage("post_route_ms");

        let post_route_started = Instant::now();
        let output_account = associated_token_address(
            &self.signer.pubkey(),
            &output_mint,
            &output_info.token_program,
        );
        let serialization_started = Instant::now();
        let signed = bincode::serialize(&winner.transaction).map_err(|error| {
            CopyTraderError::Execution(format!("failed to serialize signed transaction: {error}"))
        })?;
        timings.values.insert(
            "serialization_us",
            CopyTimings::micros(serialization_started.elapsed()),
        );
        timings.mark("transaction_built_ms");
        let local_signature = winner
            .transaction
            .signatures
            .first()
            .copied()
            .ok_or_else(|| {
                CopyTraderError::Execution("signed transaction has no signature".to_owned())
            })?;
        timings.mark("transaction_signed_ms");
        timings
            .values
            .insert("db_pre_send_us", timings.database.total_us());
        let native = self
            .cached_asset_balance(AssetId::NativeSol, &self.signer.pubkey())
            .await?;
        let mainnet =
            self.config.mainnet.as_ref().ok_or_else(|| {
                CopyTraderError::Execution("mainnet configuration missing".into())
            })?;
        // Reserve worst-case CU fees plus room for ATA/account rent and base fees.
        let fee_budget = mainnet
            .execution_priority_fee()
            .checked_mul(1_400_000)
            .and_then(|v| v.checked_add(999_999))
            .map(|v| v / 1_000_000)
            .and_then(|v| v.checked_add(mainnet.execution_tip_budget()))
            .and_then(|v| v.checked_add(10_000_000))
            .ok_or_else(|| CopyTraderError::Execution("fee reservation overflow".into()))?;
        let native_input = if sized.intent.input_asset == AssetId::NativeSol {
            if pump_swap_buy {
                wrap_lamports
            } else {
                sized.input_amount
            }
        } else {
            0
        };
        let native_debit = native_input
            .checked_add(fee_budget)
            .ok_or_else(|| CopyTraderError::Execution("native reservation overflow".into()))?;
        let mut debits = vec![(self.signer.pubkey(), native, native_debit)];
        if sized.intent.input_asset != AssetId::NativeSol {
            debits.push((input_account, available, sized.input_amount));
        } else if pump_swap_buy && cached_wsol > 0 {
            debits.push((
                input_account,
                cached_wsol,
                cached_wsol.min(sized.input_amount),
            ));
        }
        if !self.reservations.lock().await.can_reserve(&debits) {
            self.store
                .update_attempt(
                    &source_signature,
                    AttemptStatus::Failed,
                    Some("insufficient unreserved balance for input, fees, tip, and rent"),
                )
                .await?;
            self.store
                .mark_skipped(&source_signature, SkipReason::InsufficientBalance)
                .await?;
            return Ok(());
        }
        if let Some(lease) = &winner.nonce {
            let mainnet = self.backend.mainnet().expect("mainnet backend");
            let variants = winner
                .variants
                .iter()
                .zip(&mainnet.fanout.routes)
                .map(|(tx, route)| {
                    Ok((
                        tx.signatures[0].to_string(),
                        bincode::serialize(tx).map_err(|e| {
                            CopyTraderError::Execution(format!(
                                "cannot serialize fanout variant: {e}"
                            ))
                        })?,
                        route.name.clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            // Arm before awaiting the commit: cancellation during a commit must
            // conservatively keep this nonce held until a restart checks the DB.
            lease.mark_submitted();
            self.store
                .persist_fanout(
                    &source_signature,
                    &lease.account.to_string(),
                    &lease.hash.to_string(),
                    &variants,
                    &winner.simulation_json,
                )
                .await?;
        }
        let invalidation_started = Instant::now();
        self.invalidate_balances(
            sized.intent.input_asset,
            &input_account,
            sized.intent.output_asset,
            &output_account,
        )
        .await?;
        // The gate prevents settlement from resetting the budget between the
        // admission check and this debit. No fallible preparation follows it.
        if !self.reservations.lock().await.reserve(&debits) {
            return Err(CopyTraderError::Execution(
                "balance reservation changed during preparation".into(),
            ));
        }
        timings.values.insert(
            "cache_invalidation_us",
            CopyTimings::micros(invalidation_started.elapsed()),
        );
        timings.values.insert(
            "post_route_preparation_us",
            CopyTimings::micros(post_route_started.elapsed()),
        );
        timings.stage("sender_request_ms");
        timings.mark("sender_request_started_ms");
        timings.since_receipt("receipt_to_send_start_ms");
        timings.values.insert(
            "receipt_to_send_start_us",
            CopyTimings::micros(timings.received_at.elapsed()),
        );
        let sender_started = Instant::now();
        self.last_submitted_slot = self.last_submitted_slot.max(observed.slot);
        let send_result = if winner.nonce.is_some() {
            self.backend
                .mainnet()
                .expect("mainnet backend")
                .send_fanout(&winner.variants)
                .await;
            // Poll all locally known signatures even when every request errored.
            Ok(local_signature)
        } else {
            self.backend.send(&winner.transaction).await
        };
        timings.values.insert(
            "sender_request_us",
            CopyTimings::micros(sender_started.elapsed()),
        );
        timings.mark("sender_response_received_ms");
        timings.since_receipt("receipt_to_send_response_ms");
        timings.finish();
        timings.stage("post_send_journal_ms");
        if let Err(error) = self
            .store
            .mark_route(
                &source_signature,
                winner.route.dex.as_str(),
                &winner.route.pool.to_string(),
                winner.route.minimum_output,
                winner.route.expected_output,
                route_latency_ms,
            )
            .await
        {
            warn!(%error, %source_signature, %local_signature, "route journal write failed after submission");
        }
        if winner.nonce.is_none() {
            if let Err(error) = self
                .store
                .persist_signed(
                    &source_signature,
                    &local_signature.to_string(),
                    &signed,
                    &winner.simulation_json,
                )
                .await
            {
                warn!(%error, %source_signature, %local_signature, "signed transaction journal write failed after submission");
            }
        }
        timings.finish();
        match send_result {
            Ok(returned) if returned == local_signature => {}
            Ok(returned) => {
                let mut reservations = self.reservations.lock().await;
                reservations.uncertain = true;
                reservations.pending -= 1;
                drop(reservations);
                self.store
                    .update_attempt(
                        &source_signature,
                        AttemptStatus::Unknown,
                        Some(&format!("backend returned unexpected signature {returned}")),
                    )
                    .await?;
                return Ok(());
            }
            Err(error) => {
                let mut reservations = self.reservations.lock().await;
                reservations.uncertain = true;
                reservations.pending -= 1;
                drop(reservations);
                self.store
                    .update_attempt(
                        &source_signature,
                        AttemptStatus::Unknown,
                        Some(&error.to_string()),
                    )
                    .await?;
                return Ok(());
            }
        }
        let mut settlement = Self::new(
            self.config.clone(),
            self.signer.clone(),
            self.store.clone(),
            self.backend.clone(),
            self.token_safety.clone(),
            TransactionDecoder::new(self.config.signal.wallet),
            self.router.clone(),
        )
        .with_balance_cache(self.balance_cache.clone());
        settlement.reservations = self.reservations.clone();
        settlement.reservation_gate = self.reservation_gate.clone();
        let timings = timings.clone();
        self.dispatched = true;
        let timing_sender = writer.sender();
        self.pending.spawn(settlement.run_settlement(
            PendingCopy {
                observed,
                sized,
                winner,
                input_account,
                output_account,
                local_signature,
                route_latency_ms,
            },
            timings,
            timing_sender,
        ));
        Ok(())
    }

    async fn run_settlement(
        mut self,
        copy: PendingCopy,
        mut timings: CopyTimings,
        timing_sender: tokio::sync::mpsc::Sender<(String, String)>,
    ) {
        let source_signature = copy.observed.signature.to_string();
        let result = self.settle(copy, &mut timings).await;
        if let Err(error) = result {
            if let Err(journal_error) = self
                .store
                .update_attempt(
                    &source_signature,
                    AttemptStatus::Failed,
                    Some(&error.to_string()),
                )
                .await
            {
                error!(%journal_error, %source_signature, "background outcome journal failed");
            }
            error!(%error, %source_signature, "copy settlement failed");
        }
        // Keep debits conservative across the entire overlapping batch. Unknown
        // submissions hold their budget until startup recovery.
        let _gate = self.reservation_gate.lock().await;
        let mut reservations = self.reservations.lock().await;
        reservations.pending -= 1;
        if reservations.pending == 0 && !reservations.uncertain {
            for address in reservations.remaining.keys() {
                if let Err(error) = self.balance_cache.invalidate(address).await {
                    error!(%error, "reservation cache invalidation failed");
                    reservations.uncertain = true;
                    break;
                }
            }
            if !reservations.uncertain {
                reservations.remaining.clear();
            }
        }
        drop(reservations);
        timings.finish();
        if let Ok(json) = timings.json() {
            info!(%source_signature, timings = %json, "copy timings");
            if let Err(error) = timing_sender.try_send((source_signature, json)) {
                let (source_signature, _) = error.into_inner();
                warn!(%source_signature, "timing queue unavailable; copy timings remain in logs");
            }
        }
    }

    async fn settle(&mut self, copy: PendingCopy, timings: &mut CopyTimings) -> Result<()> {
        let PendingCopy {
            observed,
            sized,
            winner,
            input_account,
            output_account,
            mut local_signature,
            route_latency_ms,
        } = copy;
        let source_signature = observed.signature.to_string();
        timings.stage("confirmation_ms");
        let confirmation_result = if winner.nonce.is_some() {
            let signatures = winner
                .variants
                .iter()
                .map(|tx| tx.signatures[0])
                .collect::<Vec<_>>();
            super::confirm::wait_for_any_confirmation(
                self.backend.rpc(),
                &signatures,
                Duration::from_secs(self.config.execution.confirmation_timeout_seconds),
            )
            .await
        } else {
            wait_for_confirmation(
                self.backend.rpc(),
                self.backend.label(),
                &local_signature,
                Duration::from_secs(self.config.execution.confirmation_timeout_seconds),
            )
            .await
            .map(|status| (local_signature, status))
        };
        let confirmation = match confirmation_result {
            Ok((signature, confirmation)) => {
                local_signature = signature;
                if winner.nonce.is_some() {
                    self.store
                        .select_variant(&source_signature, &signature.to_string())
                        .await?;
                }
                confirmation
            }
            Err(error) => {
                self.reservations.lock().await.uncertain = true;
                self.store
                    .update_attempt(
                        &source_signature,
                        AttemptStatus::Unknown,
                        Some(&error.to_string()),
                    )
                    .await?;
                return Ok(());
            }
        };
        self.store
            .record_landed_slot(&source_signature, confirmation.slot)
            .await?;
        let slot_delta = confirmation.slot.saturating_sub(observed.slot);
        timings.values.insert("slot_delta", slot_delta);
        self.invalidate_balances(
            sized.intent.input_asset,
            &input_account,
            sized.intent.output_asset,
            &output_account,
        )
        .await?;
        if let Some(error) = confirmation.error {
            if let Err(balance_error) = self
                .asset_balance_network(AssetId::NativeSol, &self.signer.pubkey())
                .await
            {
                warn!(%balance_error, %source_signature, "cannot refresh SOL balance after failed landed transaction");
            }
            self.store
                .update_attempt(&source_signature, AttemptStatus::Failed, Some(&error))
                .await?;
            return Ok(());
        }

        timings.stage("reconciliation_ms");
        let owner = self.signer.pubkey();
        tokio::try_join!(
            self.asset_balance_network(sized.intent.input_asset, &input_account),
            self.asset_balance_network(sized.intent.output_asset, &output_account),
            async {
                if sized.intent.input_asset != AssetId::NativeSol
                    && sized.intent.output_asset != AssetId::NativeSol
                {
                    self.asset_balance_network(AssetId::NativeSol, &owner).await
                } else {
                    Ok(0)
                }
            },
        )?;
        if winner.route.dex == crate::domain::DexKind::PumpSwap {
            self.asset_balance_network(
                AssetId::Token(spl_token::native_mint::id()),
                &associated_token_address(
                    &self.signer.pubkey(),
                    &spl_token::native_mint::id(),
                    &spl_token::id(),
                ),
            )
            .await?;
        }

        let landed_tip = self.backend.mainnet().map_or(0, |mainnet| {
            if winner.nonce.is_some() {
                winner
                    .variants
                    .iter()
                    .position(|tx| tx.signatures[0] == local_signature)
                    .map_or(0, |index| mainnet.fanout.routes[index].tip_lamports)
            } else {
                mainnet.tip_lamports()
            }
        });
        let metadata_started = Instant::now();
        let persistent_wsol_output = winner.route.dex == crate::domain::DexKind::PumpSwap
            && sized.intent.output_asset == AssetId::NativeSol;
        let received = super::reconcile::received_output(
            self.backend.rpc(),
            &local_signature,
            if persistent_wsol_output {
                AssetId::Token(sized.intent.output_asset.routing_mint())
            } else {
                sized.intent.output_asset
            },
            if sized.intent.output_asset == AssetId::NativeSol && !persistent_wsol_output {
                self.signer.pubkey()
            } else {
                output_account
            },
            landed_tip,
            Duration::from_secs(self.config.execution.confirmation_timeout_seconds),
        )
        .await;
        timings.values.insert(
            "reconciliation_metadata_ms",
            CopyTimings::millis(metadata_started.elapsed()),
        );
        let received = received?;
        if received < winner.route.minimum_output {
            self.store
                .update_attempt(
                    &source_signature,
                    AttemptStatus::Failed,
                    Some("landed output did not satisfy route minimum"),
                )
                .await?;
            return Ok(());
        }
        self.store
            .update_attempt(&source_signature, AttemptStatus::Landed, None)
            .await?;
        info!(%source_signature, %local_signature, dex = winner.route.dex.as_str(), pool = %winner.route.pool, source_slot = observed.slot, landed_slot = confirmation.slot, route_latency_ms, "copy landed");
        Ok(())
    }

    fn is_stale(&self, observed: &ObservedTransaction) -> Result<bool> {
        let Some(block_time) = observed.block_time else {
            return Ok(false);
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                CopyTraderError::Execution(format!("system clock is before epoch: {error}"))
            })?
            .as_secs();
        let block_time = u64::try_from(block_time)
            .map_err(|_| CopyTraderError::Execution("source block time is negative".to_owned()))?;
        Ok(now.saturating_sub(block_time) > self.config.execution.max_signal_age_seconds)
    }

    async fn initial_reads(
        &self,
        intent: &TradeIntent,
        direct_mints: Option<(MintInfo, MintInfo)>,
    ) -> Result<InitialReads> {
        let preparation = async {
            tokio::try_join!(
                async {
                    if let Some(infos) = direct_mints {
                        let input = self
                            .token_safety
                            .source_info(intent.input_asset.routing_mint(), infos.0)
                            .await?;
                        let output = self
                            .token_safety
                            .source_info(intent.output_asset.routing_mint(), infos.1)
                            .await?;
                        return Ok(((input, output), 0, true));
                    }
                    let started = Instant::now();
                    let infos = self
                        .token_safety
                        .inspect_pair(
                            intent.input_asset.routing_mint(),
                            intent.output_asset.routing_mint(),
                        )
                        .await?;
                    Ok::<_, CopyTraderError>((infos, CopyTimings::millis(started.elapsed()), false))
                },
                async {
                    if matches!(intent.input_asset, AssetId::NativeSol) {
                        self.cached_asset_balance(AssetId::NativeSol, &self.signer.pubkey())
                            .await
                            .map(Some)
                    } else {
                        Ok(None)
                    }
                }
            )
        };
        let (((input_info, output_info), mint_read_ms, mint_from_source), native_balance) =
            preparation.await?;
        Ok(InitialReads {
            input_info,
            output_info,
            native_balance,
            mint_read_ms,
            mint_from_source,
        })
    }

    async fn invalidate_balances(
        &self,
        input: AssetId,
        input_account: &Pubkey,
        output: AssetId,
        output_account: &Pubkey,
    ) -> Result<()> {
        self.balance_cache.invalidate(&self.signer.pubkey()).await?;
        self.balance_cache
            .invalidate(&associated_token_address(
                &self.signer.pubkey(),
                &spl_token::native_mint::id(),
                &spl_token::id(),
            ))
            .await?;
        for (asset, account) in [(input, input_account), (output, output_account)] {
            if asset != AssetId::NativeSol {
                self.balance_cache.invalidate(account).await?;
            }
        }
        Ok(())
    }

    async fn cached_asset_balance(&self, asset: AssetId, token_account: &Pubkey) -> Result<u64> {
        let address = if asset == AssetId::NativeSol {
            self.signer.pubkey()
        } else {
            *token_account
        };
        // A pending batch uses its original balance minus all admitted debits.
        // RPC refreshes cannot credit that budget until the batch is settled.
        if let Some(amount) = self
            .reservations
            .lock()
            .await
            .remaining
            .get(&address)
            .copied()
        {
            return Ok(amount);
        }
        let amount = self
            .balance_cache
            .get_or_fetch(
                self.backend.rpc(),
                asset,
                self.signer.pubkey(),
                *token_account,
            )
            .await?;
        let address = if asset == AssetId::NativeSol {
            self.signer.pubkey()
        } else {
            *token_account
        };
        Ok(self.reservations.lock().await.available(address, amount))
    }

    async fn asset_balance_network(&self, asset: AssetId, token_account: &Pubkey) -> Result<u64> {
        self.balance_cache
            .fetch(
                self.backend.rpc(),
                asset,
                self.signer.pubkey(),
                *token_account,
            )
            .await
    }
}

fn source_position_before(
    intent: &TradeIntent,
    wallet: Pubkey,
    balances: Option<&[UiTokenBalance]>,
) -> Result<u64> {
    let wallet = wallet.to_string();
    let mint = intent.input_asset.routing_mint().to_string();
    let mut before = 0_u64;
    let mut seen = std::collections::HashSet::new();
    for row in balances
        .unwrap_or_default()
        .iter()
        .filter(|row| row.owner.as_deref() == Some(wallet.as_str()) && row.mint == mint)
    {
        if !seen.insert(row.account_index) {
            return Err(CopyTraderError::Decode(
                "duplicate source position balance".to_owned(),
            ));
        }
        let amount =
            row.ui_token_amount.amount.parse::<u64>().map_err(|_| {
                CopyTraderError::Decode("invalid source position balance".to_owned())
            })?;
        before = before.checked_add(amount).ok_or_else(|| {
            CopyTraderError::Decode("source position balance overflow".to_owned())
        })?;
    }
    if before == 0 {
        return Err(CopyTraderError::Decode(
            "missing source pre-sell position balance".to_owned(),
        ));
    }
    Ok(before)
}

struct InitialReads {
    input_info: MintInfo,
    output_info: MintInfo,
    native_balance: Option<u64>,
    mint_read_ms: u64,
    mint_from_source: bool,
}

fn direct_source_mints(
    intent: &TradeIntent,
    post_balances: Option<&[UiTokenBalance]>,
) -> Option<(MintInfo, MintInfo)> {
    let source = intent.source_instruction.as_ref()?;
    let program = source.instruction.program_id;
    if program != pump_fun::PROGRAM_ID && program != crate::domain::DexKind::PumpSwap.program_id() {
        return None;
    }
    let balances = post_balances?;
    let info = |asset: AssetId| -> Option<MintInfo> {
        if asset.routing_mint() == AssetId::NativeSol.routing_mint() {
            return Some(crate::token::extensions::native_mint_info());
        }
        let mint = asset.routing_mint();
        let mut accounts = source
            .wallet_token_accounts
            .iter()
            .filter(|(_, known_mint, _)| *known_mint == mint);
        let (address, _, token_program) = accounts.next()?;
        if accounts.next().is_some()
            || (*token_program != spl_token::id() && *token_program != spl_token_2022::id())
            || *address != associated_token_address(&source.source_wallet, &mint, token_program)
            || !source
                .instruction
                .accounts
                .iter()
                .any(|meta| meta.pubkey == *address)
        {
            return None;
        }
        let wallet_string = source.source_wallet.to_string();
        let mint_string = mint.to_string();
        let program_string = token_program.to_string();
        let mut matching = balances.iter().filter(|balance| {
            balance.owner.as_deref() == Some(wallet_string.as_str())
                && balance.mint == mint_string
                && balance.program_id.as_deref() == Some(program_string.as_str())
        });
        let balance = matching.next()?;
        if matching.next().is_some() {
            return None;
        }
        Some(MintInfo {
            decimals: balance.ui_token_amount.decimals,
            token_program: *token_program,
            has_transfer_fee: false,
        })
    };
    Some((info(intent.input_asset)?, info(intent.output_asset)?))
}

#[derive(Clone)]
struct CopyTimings {
    received_at: Instant,
    database: DatabaseTimings,
    active: Option<(&'static str, Instant)>,
    values: std::collections::BTreeMap<&'static str, u64>,
}

impl CopyTimings {
    fn new(received_at: Instant) -> Self {
        let mut timings = Self {
            received_at,
            database: DatabaseTimings::default(),
            active: None,
            values: Default::default(),
        };
        timings.since_receipt("ingestion_queue_ms");
        timings.stage("preparation_ms");
        timings
    }
    fn json(&self) -> Result<String> {
        let mut values = serde_json::to_value(&self.values)?;
        let database = self.database.snapshot();
        let total_us = database.values().fold(0_u64, |total, timing| {
            total.saturating_add(timing.elapsed_us)
        });
        values["database"] = serde_json::to_value(database)?;
        values["db_total_us"] = total_us.into();
        if let Some(before_send) = self.values.get("db_pre_send_us") {
            values["db_post_send_us"] = total_us.saturating_sub(*before_send).into();
        }
        serde_json::to_string(&values).map_err(Into::into)
    }

    fn micros(duration: Duration) -> u64 {
        u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
    }

    fn millis(duration: Duration) -> u64 {
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
    }
    fn since_receipt(&mut self, name: &'static str) {
        self.values
            .insert(name, Self::millis(self.received_at.elapsed()));
    }

    fn mark(&mut self, name: &'static str) {
        self.values
            .insert(name, Self::millis(self.received_at.elapsed()));
    }
    fn stage(&mut self, name: &'static str) {
        self.finish();
        self.active = Some((name, Instant::now()));
    }
    fn finish(&mut self) {
        if let Some((name, started)) = self.active.take() {
            self.values.insert(name, Self::millis(started.elapsed()));
        }
    }
}

#[cfg(test)]
mod timing_tests {
    use super::*;

    #[test]
    fn source_sell_position_uses_only_owned_input_mint_balances() {
        let owner = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let intent = TradeIntent {
            source_pool: None,
            source_instruction: None,
            source_signature: Default::default(),
            slot: 1,
            input_asset: AssetId::Token(mint),
            output_asset: AssetId::NativeSol,
            source_input_amount: 50,
            source_output_amount: 10,
        };
        let row = |account_index, owner: Pubkey, mint: Pubkey, amount| UiTokenBalance {
            account_index,
            mint: mint.to_string(),
            owner: Some(owner.to_string()),
            program_id: Some(spl_token::id().to_string()),
            ui_token_amount: crate::domain::UiTokenAmount {
                amount: format!("{amount}"),
                decimals: 6,
            },
        };
        let rows = vec![
            row(1, owner, mint, 40),
            row(2, owner, mint, 60),
            row(3, Pubkey::new_unique(), mint, 1000),
            row(4, owner, Pubkey::new_unique(), 1000),
        ];
        assert_eq!(
            source_position_before(&intent, owner, Some(&rows)).expect("source position"),
            100
        );
        assert!(source_position_before(&intent, owner, None).is_err());
        let mut duplicate = rows.clone();
        duplicate.push(rows[0].clone());
        assert!(source_position_before(&intent, owner, Some(&duplicate)).is_err());
        let mut malformed = rows.clone();
        malformed[0].ui_token_amount.amount = "bad".to_owned();
        assert!(source_position_before(&intent, owner, Some(&malformed)).is_err());
        let overflow = vec![row(1, owner, mint, u64::MAX), row(2, owner, mint, 1)];
        assert!(source_position_before(&intent, owner, Some(&overflow)).is_err());
    }

    use solana_sdk::instruction::{AccountMeta, Instruction};

    #[test]
    fn fresh_classic_pump_fun_buy_uses_source_mint_evidence() {
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let ata = associated_token_address(&wallet, &mint, &spl_token::id());
        let source = crate::domain::SourceInstruction {
            instruction: Instruction {
                program_id: pump_fun::PROGRAM_ID,
                accounts: vec![AccountMeta::new(ata, false)],
                data: [pump_fun::BUY_DISCRIMINATOR.as_slice(), &[0; 16]].concat(),
            },
            source_wallet: wallet,
            wallet_token_accounts: vec![(ata, mint, spl_token::id())],
        };
        let intent = TradeIntent {
            source_pool: None,
            source_instruction: Some(source),
            source_signature: Default::default(),
            slot: 42,
            input_asset: AssetId::NativeSol,
            output_asset: AssetId::Token(mint),
            source_input_amount: 100,
            source_output_amount: 200,
        };
        let balance = UiTokenBalance {
            account_index: 0,
            mint: mint.to_string(),
            ui_token_amount: crate::domain::UiTokenAmount {
                amount: "200".to_owned(),
                decimals: 6,
            },
            owner: Some(wallet.to_string()),
            program_id: Some(spl_token::id().to_string()),
        };
        let infos = direct_source_mints(&intent, Some(std::slice::from_ref(&balance)))
            .expect("direct mint evidence");
        assert_eq!(infos.0.decimals, 9);
        assert_eq!(infos.1.decimals, 6);
        assert_eq!(infos.1.token_program, spl_token::id());
        assert!(!infos.1.has_transfer_fee);

        let mut unsupported = balance.clone();
        unsupported.program_id = Some(spl_token_2022::id().to_string());
        assert!(direct_source_mints(&intent, Some(&[unsupported])).is_none());
        assert!(direct_source_mints(&intent, None).is_none());
        let mut ambiguous = intent.clone();
        ambiguous
            .source_instruction
            .as_mut()
            .unwrap()
            .wallet_token_accounts
            .push((Pubkey::new_unique(), mint, spl_token_2022::id()));
        assert!(direct_source_mints(&ambiguous, Some(std::slice::from_ref(&balance))).is_none());
        let mut wrong_instruction = intent.clone();
        wrong_instruction
            .source_instruction
            .as_mut()
            .unwrap()
            .instruction
            .program_id = Pubkey::new_unique();
        assert!(direct_source_mints(&wrong_instruction, Some(&[balance])).is_none());
    }

    #[test]
    fn pump_swap_token_2022_metadata_supports_buys_and_sells() {
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let program = spl_token_2022::id();
        let ata = associated_token_address(&wallet, &mint, &program);
        let mut intent = TradeIntent {
            source_pool: Some(crate::domain::SourcePool {
                dex: crate::domain::DexKind::PumpSwap,
                address: Pubkey::new_unique(),
            }),
            source_instruction: Some(crate::domain::SourceInstruction {
                instruction: Instruction {
                    program_id: crate::domain::DexKind::PumpSwap.program_id(),
                    accounts: vec![AccountMeta::new(ata, false)],
                    data: [pump_fun::BUY_DISCRIMINATOR.as_slice(), &[0; 16]].concat(),
                },
                source_wallet: wallet,
                wallet_token_accounts: vec![(ata, mint, program)],
            }),
            source_signature: Default::default(),
            slot: 42,
            input_asset: AssetId::NativeSol,
            output_asset: AssetId::Token(mint),
            source_input_amount: 100,
            source_output_amount: 200,
        };
        let balance = UiTokenBalance {
            account_index: 0,
            mint: mint.to_string(),
            ui_token_amount: crate::domain::UiTokenAmount {
                amount: "200".to_owned(),
                decimals: 6,
            },
            owner: Some(wallet.to_string()),
            program_id: Some(program.to_string()),
        };
        let buy = direct_source_mints(&intent, Some(std::slice::from_ref(&balance))).unwrap();
        assert_eq!(buy.0.decimals, 9);
        assert_eq!(buy.1.token_program, program);
        assert_eq!(buy.1.decimals, 6);
        std::mem::swap(&mut intent.input_asset, &mut intent.output_asset);
        let sell = direct_source_mints(&intent, Some(std::slice::from_ref(&balance))).unwrap();
        assert_eq!(sell.0.token_program, program);
        assert_eq!(sell.1.decimals, 9);
        assert!(direct_source_mints(&intent, Some(&[balance.clone(), balance])).is_none());
    }

    #[tokio::test]
    async fn direct_mint_evidence_avoids_mint_rpc() {
        use crate::{
            config::{HttpConfig, MainnetConfig},
            http::HttpTransport,
            mainnet::MainnetClient,
            test_rpc::TestRpc,
        };
        use serde_json::json;

        let server = TestRpc::start(|request| match request["method"].as_str().unwrap() {
            "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
            "getBalance" => json!({"context":{"slot":42},"value":1_000_000}),
            "getAccountInfo" => {
                json!({"context":{"slot":42},"value":crate::test_rpc::mint_account()})
            }
            method => panic!("unexpected RPC {method}"),
        })
        .await;
        let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
        let config =
            Arc::new(AppConfig::load(std::path::Path::new("config.example.toml")).unwrap());
        let backend = Arc::new(ExecutionBackend::Mainnet(Arc::new(MainnetClient::new(
            server.url.clone(),
            &MainnetConfig {
                fanout: Default::default(),
                source_direct: true,
                fixed_priority_fee_micro_lamports: None,
                sender_url: server.url.clone(),
                tip_lamports: 5000,
                priority_level: "High".to_owned(),
                max_priority_fee_micro_lamports: 100,
            },
            transport.clone(),
        ))));
        let worker = ExecutionWorker::new(
            config.clone(),
            Arc::new(Keypair::new()),
            Store::connect("sqlite::memory:").await.unwrap(),
            backend,
            TokenSafetyClient::new(&server.url, &transport),
            TransactionDecoder::new(config.signal.wallet),
            Arc::new(Router::new(
                Arc::new(transport.solana_rpc(&server.url)),
                Duration::from_secs(2),
            )),
        );
        let intent = TradeIntent {
            source_pool: None,
            source_instruction: None,
            source_signature: Default::default(),
            slot: 42,
            input_asset: AssetId::NativeSol,
            output_asset: AssetId::Token(Pubkey::new_unique()),
            source_input_amount: 100,
            source_output_amount: 200,
        };
        let infos = (
            MintInfo {
                decimals: 9,
                token_program: spl_token::id(),
                has_transfer_fee: false,
            },
            MintInfo {
                decimals: 6,
                token_program: spl_token::id(),
                has_transfer_fee: false,
            },
        );
        let reads = worker.initial_reads(&intent, Some(infos)).await.unwrap();
        assert!(reads.mint_from_source);
        assert_eq!(reads.mint_read_ms, 0);
        assert_eq!(reads.native_balance, Some(1_000_000));
        assert_eq!(server.count("getMultipleAccounts"), 0);
    }

    #[tokio::test]
    async fn initial_mint_and_balance_reads_overlap_and_preserve_rpc_failures() {
        use crate::{
            config::{HttpConfig, MainnetConfig},
            domain::{DexKind, SourcePool},
            http::HttpTransport,
            mainnet::MainnetClient,
            test_rpc::{TestRpc, mint_account},
        };
        use serde_json::json;
        for scenario in ["native", "token", "mint_failure", "balance_failure"] {
            let server = TestRpc::start(move |request| {
                let value = match request["method"].as_str().unwrap() {
                    "getVersion" => return json!({"solana-core":"3.1.0","feature-set":1}),
                    "getMultipleAccounts" if scenario == "mint_failure" => {
                        return json!({"error":{"code":-32602,"message":"mint failure"}});
                    }
                    "getMultipleAccounts" => json!([mint_account(), mint_account()]),
                    "getBalance" if scenario == "balance_failure" => {
                        return json!({"error":{"code":-32602,"message":"balance failure"}});
                    }
                    "getBalance" => json!(1000000),
                    other => panic!("unexpected RPC {other}"),
                };
                json!({"test_delay_ms":75,"test_result":{"context":{"slot":42},"value":value}})
            })
            .await;
            let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
            let config =
                Arc::new(AppConfig::load(std::path::Path::new("config.example.toml")).unwrap());
            let backend = Arc::new(ExecutionBackend::Mainnet(Arc::new(MainnetClient::new(
                server.url.clone(),
                &MainnetConfig {
                    fanout: Default::default(),
                    source_direct: false,
                    fixed_priority_fee_micro_lamports: None,
                    sender_url: server.url.clone(),
                    tip_lamports: 5000,
                    priority_level: "High".to_owned(),
                    max_priority_fee_micro_lamports: 100,
                },
                transport.clone(),
            ))));
            let worker = ExecutionWorker::new(
                config.clone(),
                Arc::new(Keypair::new()),
                Store::connect("sqlite::memory:").await.unwrap(),
                backend,
                TokenSafetyClient::new(&server.url, &transport),
                TransactionDecoder::new(config.signal.wallet),
                Arc::new(Router::new(
                    Arc::new(transport.solana_rpc(&server.url)),
                    Duration::from_secs(2),
                )),
            );
            let intent = TradeIntent {
                source_pool: Some(SourcePool {
                    dex: DexKind::PumpSwap,
                    address: Pubkey::new_unique(),
                }),
                source_instruction: None,
                source_signature: solana_sdk::signature::Signature::default(),
                slot: 42,
                input_asset: if scenario == "token" {
                    AssetId::Token(Pubkey::new_unique())
                } else {
                    AssetId::NativeSol
                },
                output_asset: AssetId::Token(Pubkey::new_unique()),
                source_input_amount: 100,
                source_output_amount: 100,
            };
            let result = tokio::time::timeout(
                Duration::from_millis(700),
                worker.initial_reads(&intent, None),
            )
            .await
            .expect("initial reads stalled");
            assert_eq!(
                result.is_err(),
                matches!(scenario, "mint_failure" | "balance_failure"),
                "{scenario}"
            );
            if let Ok(reads) = result {
                assert_eq!(
                    reads.native_balance,
                    if scenario == "token" {
                        None
                    } else {
                        Some(1000000)
                    }
                );
                assert!(
                    server
                        .peak_in_flight
                        .load(std::sync::atomic::Ordering::SeqCst)
                        >= if scenario == "token" { 1 } else { 2 }
                );
                assert_eq!(
                    server.count("getBalance"),
                    if scenario == "token" { 0 } else { 1 }
                );
            }
            assert_eq!(server.count("sendTransaction"), 0);
        }
    }

    #[tokio::test]
    async fn queued_observation_keeps_ingestion_metrics_through_a_skip() {
        use crate::{
            config::HttpConfig,
            domain::{SignalOrigin, TransactionMeta},
            http::HttpTransport,
            mainnet::MainnetClient,
        };
        let base = Store::connect("sqlite::memory:").await.expect("store");
        let database_timings = DatabaseTimings::default();
        let observed = ObservedTransaction {
            signature: solana_sdk::signature::Signature::default(),
            slot: 42,
            block_time: Some(0),
            origin: SignalOrigin::Live,
            transaction: solana_sdk::transaction::VersionedTransaction::default(),
            meta: TransactionMeta::default(),
            raw_payload: "{}".to_owned(),
            received_bytes: 2,
        };
        base.with_timings(database_timings.clone())
            .record_observation(&observed)
            .await
            .expect("observation");
        let config =
            Arc::new(AppConfig::load(std::path::Path::new("config.example.toml")).expect("config"));
        let endpoint = url::Url::parse("http://127.0.0.1:1").expect("unused endpoint");
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let backend = Arc::new(ExecutionBackend::Mainnet(Arc::new(MainnetClient::new(
            endpoint.clone(),
            config.mainnet.as_ref().expect("mainnet"),
            transport.clone(),
        ))));
        let worker = ExecutionWorker::new(
            config.clone(),
            Arc::new(Keypair::new()),
            base.clone(),
            backend,
            TokenSafetyClient::new(&endpoint, &transport),
            TransactionDecoder::new(config.signal.wallet),
            Arc::new(Router::new(
                Arc::new(transport.solana_rpc(&endpoint)),
                Duration::from_secs(2),
            )),
        );
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(QueuedObservation {
                observed,
                received_at: Instant::now() - Duration::from_millis(10),
                queued_at: Instant::now() - Duration::from_millis(5),
                payload_decode_us: 123,
                observation_enqueue_us: 45,
                database_timings: database_timings.clone(),
            })
            .await
            .expect("enqueue");
        drop(sender);
        worker.run(receiver).await.expect("worker");
        let rows = base.status(1).await.expect("rows");
        assert_eq!(rows[0].source_status, "skipped");
        let saved: serde_json::Value =
            serde_json::from_str(rows[0].timings_json.as_deref().expect("timings")).expect("JSON");
        assert_eq!(saved["database"]["record_observation"]["calls"], 1);
        assert_eq!(saved["database"]["mark_skipped"]["calls"], 1);
        assert!(saved["database"].get("record_timings").is_none());
        assert!(!database_timings.snapshot().contains_key("record_timings"));
        assert!(saved["ingestion_queue_ms"].as_u64().expect("delay") >= 10);
        assert!(saved["queue_wait_us"].as_u64().expect("queue wait") >= 5000);
        assert_eq!(saved["payload_decode_us"], 123);
        assert_eq!(saved["observation_enqueue_us"], 45);
    }

    #[tokio::test]
    async fn timing_write_is_measured_in_log_snapshot_without_a_second_write() {
        let base = Store::connect("sqlite::memory:").await.expect("store");
        base.record_recovered_signature("source", 42)
            .await
            .expect("source");
        let timings = CopyTimings::new(Instant::now());
        let store = base.with_timings(timings.database.clone());
        store
            .mark_skipped("source", SkipReason::StaleSignal)
            .await
            .expect("skip");
        let saved = timings.json().expect("JSON");
        store
            .record_timings("source", &saved)
            .await
            .expect("timing write");
        let log: serde_json::Value =
            serde_json::from_str(&timings.json().expect("JSON")).expect("log");
        assert_eq!(log["database"]["record_timings"]["calls"], 1);
        assert!(log["database"]["mark_skipped"]["elapsed_us"].is_u64());
        assert!(log.get("db_pre_send_us").is_none());
        assert!(log.get("db_post_send_us").is_none());
        let rows = base.status(1).await.expect("rows");
        assert_eq!(rows[0].timings_json.as_deref(), Some(saved.as_str()));
        let stored: serde_json::Value = serde_json::from_str(&saved).expect("stored");
        assert!(stored["database"].get("record_timings").is_none());
    }

    #[test]
    fn timings_include_queue_and_failed_stage_but_omit_unreached_stages() {
        let mut timings = CopyTimings::new(Instant::now() - Duration::from_millis(50));
        timings.stage("route_ms");
        timings.finish();
        assert!(timings.values["ingestion_queue_ms"] >= 50);
        assert!(timings.values.contains_key("preparation_ms"));
        assert!(timings.values.contains_key("route_ms"));
        assert!(!timings.values.contains_key("confirmation_ms"));
        assert!(!timings.values.contains_key("receipt_to_send_start_ms"));
        timings.since_receipt("receipt_to_send_start_ms");
        timings.since_receipt("receipt_to_send_response_ms");
        assert!(
            timings.values["receipt_to_send_response_ms"]
                >= timings.values["receipt_to_send_start_ms"]
        );
        let before = timings.values.clone();
        timings.finish();
        assert_eq!(timings.values, before);
    }
}

#[cfg(test)]
mod unsupported_tests {
    use super::*;
    use crate::{
        config::{HttpConfig, SizingConfig, TokenPolicyConfig},
        domain::{DexKind, SignalOrigin, TransactionMeta, UiTokenAmount},
        http::HttpTransport,
        mainnet::MainnetClient,
        test_rpc::{TestRpc, mint_account},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::json;
    use solana_sdk::{
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        message::{Message, VersionedMessage},
        signature::Signature,
        transaction::{Transaction, VersionedTransaction},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn observation(wallet: Pubkey, id: u8) -> ObservedTransaction {
        let mint = Pubkey::new_unique();
        let ata = associated_token_address(&wallet, &mint, &spl_token::id());
        let mut accounts = (0..pump_fun::BUY_ACCOUNT_COUNT)
            .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
            .collect::<Vec<_>>();
        accounts[2] = AccountMeta::new_readonly(mint, false);
        accounts[5] = AccountMeta::new(ata, false);
        accounts[6] = AccountMeta::new(wallet, true);
        let mut data = vec![0; 26];
        data[..8].copy_from_slice(&pump_fun::BUY_DISCRIMINATOR);
        let message = Message::new(
            &[Instruction {
                program_id: pump_fun::PROGRAM_ID,
                accounts,
                data,
            }],
            Some(&wallet),
        );
        let ata_index = message
            .account_keys
            .iter()
            .position(|key| *key == ata)
            .expect("ATA") as u8;
        let token_balance = UiTokenBalance {
            account_index: ata_index,
            mint: mint.to_string(),
            owner: Some(wallet.to_string()),
            program_id: Some(spl_token::id().to_string()),
            ui_token_amount: UiTokenAmount {
                amount: "5000".to_owned(),
                decimals: 6,
            },
        };
        let mut pre_balances = vec![0; message.account_keys.len()];
        let mut post_balances = pre_balances.clone();
        pre_balances[0] = 2_000_000_000;
        post_balances[0] = 1_990_000_000;
        ObservedTransaction {
            signature: Signature::try_from([id; 64].as_slice()).expect("signature"),
            slot: 42,
            block_time: None,
            origin: SignalOrigin::Live,
            transaction: VersionedTransaction {
                signatures: vec![],
                message: VersionedMessage::Legacy(message),
            },
            meta: TransactionMeta {
                pre_balances,
                post_balances,
                fee: 5000,
                post_token_balances: Some(vec![token_balance]),
                ..Default::default()
            },
            raw_payload: "{}".to_owned(),
            received_bytes: 2,
        }
    }

    #[tokio::test]
    async fn background_settlement_overlaps_and_reservations_survive_ambiguous_results() {
        use std::sync::atomic::AtomicBool;
        for scenario in [
            "overlap",
            "limit",
            "insufficient",
            "send_error",
            "signature_mismatch",
            "timeout",
            "failed_confirmation",
        ] {
            let released = Arc::new(AtomicBool::new(false));
            let release = released.clone();
            let sends = Arc::new(AtomicUsize::new(0));
            let sent = sends.clone();
            let metadata = std::sync::Mutex::new(std::collections::HashMap::new());
            let server = TestRpc::start(move |request| match request["method"].as_str().unwrap() {
                "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
                "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":1000}}),
                "getBlockHeight" => json!(100),
                // Deliberately keep reporting the pre-send balance: the reservation
                // must protect funds even when processed RPC/cache data is stale.
                "getBalance" => json!({"context":{"slot":42},"value":if matches!(scenario, "overlap" | "limit") {1_000_000_000} else {20_000_000}}),
                "getAccountInfo" => json!({"context":{"slot":42},"value":mint_account()}),
                "getMultipleAccounts" => json!({"context":{"slot":42},"value":[mint_account(),mint_account()]}),
                "getTokenAccountBalance" => json!({"context":{"slot":43},"value":{"amount":"1000","decimals":6,"uiAmount":0.001,"uiAmountString":"0.001"}}),
                "sendTransaction" => {
                    let bytes = STANDARD.decode(request["params"][0].as_str().unwrap()).unwrap();
                    let transaction: Transaction = bincode::deserialize(&bytes).unwrap();
                    let swap = transaction.message.instructions.iter().find(|i| transaction.message.account_keys[usize::from(i.program_id_index)] == pump_fun::PROGRAM_ID).unwrap();
                    let mint = transaction.message.account_keys[usize::from(swap.accounts[2])];
                    metadata.lock().unwrap().insert(transaction.signatures[0].to_string(), json!({
                        "transaction":{"message":{"accountKeys":transaction.message.account_keys.iter().map(ToString::to_string).collect::<Vec<_>>() }},
                        "meta":{"err":null,"preTokenBalances":[],"postTokenBalances":[{"accountIndex":swap.accounts[5],"mint":mint.to_string(),"uiTokenAmount":{"amount":"1000"}}]}
                    }));
                    sent.fetch_add(1, Ordering::SeqCst);
                    if scenario == "send_error" { json!({"error":{"code":-32000,"message":"ambiguous submission"}}) }
                    else if scenario == "signature_mismatch" { json!(Signature::new_unique().to_string()) }
                    else { json!(transaction.signatures[0].to_string()) }
                }
                "getSignatureStatuses" if !release.load(Ordering::SeqCst) => json!({"context":{"slot":43},"value":[null]}),
                "getSignatureStatuses" if scenario == "failed_confirmation" => json!({"context":{"slot":43},"value":[{"slot":43,"confirmations":1,"err":{"InstructionError":[0,"InvalidArgument"]},"status":{"Err":{"InstructionError":[0,"InvalidArgument"]}},"confirmationStatus":"confirmed"}]}),
                "getSignatureStatuses" => json!({"context":{"slot":43},"value":[{"slot":43,"confirmations":1,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"}]}),
                "getTransaction" => metadata.lock().unwrap().get(request["params"][0].as_str().unwrap()).unwrap().clone(),
                method => panic!("unexpected RPC {method}"),
            }).await;
            let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
            let mut config = AppConfig::load(std::path::Path::new("config.example.toml")).unwrap();
            config.sizing = SizingConfig::Fixed {
                amount: "0.001".into(),
            };
            config.token_policy = TokenPolicyConfig::All {
                minimum_input: "0.000001".into(),
                maximum_input: "1".into(),
            };
            if scenario == "timeout" {
                config.execution.confirmation_timeout_seconds = 0;
            }
            let mainnet = config.mainnet.as_mut().unwrap();
            mainnet.source_direct = true;
            mainnet.sender_url = server.url.clone();
            let client = Arc::new(MainnetClient::new(
                server.url.clone(),
                mainnet,
                transport.clone(),
            ));
            client.latest_blockhash().await.unwrap();
            let store = Store::connect("sqlite::memory:").await.unwrap();
            let wallet = config.signal.wallet;
            let worker = ExecutionWorker::new(
                Arc::new(config),
                Arc::new(Keypair::new()),
                store.clone(),
                Arc::new(ExecutionBackend::Mainnet(client)),
                TokenSafetyClient::new(&server.url, &transport),
                TransactionDecoder::new(wallet),
                Arc::new(Router::new(
                    Arc::new(transport.solana_rpc(&server.url)),
                    Duration::from_secs(2),
                )),
            );
            let reservations = worker.reservations.clone();
            let (sender, receiver) = tokio::sync::mpsc::channel(128);
            for id in 1..=if scenario == "limit" { 65 } else { 2 } {
                let observed = observation(wallet, id);
                store.record_observation(&observed).await.unwrap();
                sender
                    .send(QueuedObservation {
                        observed,
                        received_at: Instant::now(),
                        queued_at: Instant::now(),
                        payload_decode_us: 0,
                        observation_enqueue_us: 0,
                        database_timings: DatabaseTimings::default(),
                    })
                    .await
                    .unwrap();
            }
            drop(sender);
            let task = tokio::spawn(worker.run(receiver));
            let expected_sends = match scenario {
                "overlap" => 2,
                "limit" => 64,
                _ => 1,
            };
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let rows = store.status(100).await.unwrap();
                    let skipped = rows.iter().any(|r| r.source_status == "skipped");
                    if sends.load(Ordering::SeqCst) == expected_sends
                        && (matches!(scenario, "overlap" | "limit") || skipped)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("next observation must progress while confirmation is blocked");
            if matches!(
                scenario,
                "overlap" | "limit" | "insufficient" | "failed_confirmation"
            ) {
                assert!(
                    !task.is_finished(),
                    "worker shutdown must drain settlement tasks"
                );
                assert!(
                    store
                        .status(10)
                        .await
                        .unwrap()
                        .iter()
                        .all(|r| r.landed_slot.is_none())
                );
            }
            released.store(true, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let expected_sends = if scenario == "limit" {
                65
            } else {
                expected_sends
            };
            assert_eq!(sends.load(Ordering::SeqCst), expected_sends);
            let rows = store.status(100).await.unwrap();
            let settled = rows
                .iter()
                .filter(|r| r.local_signature.is_some())
                .collect::<Vec<_>>();
            assert_eq!(settled.len(), expected_sends);
            for row in settled {
                let unknown = matches!(scenario, "send_error" | "signature_mismatch" | "timeout");
                assert_eq!(
                    row.copy_status.as_deref(),
                    Some(if unknown {
                        "unknown"
                    } else if scenario == "failed_confirmation" {
                        "failed"
                    } else {
                        "landed"
                    })
                );
                let timings: serde_json::Value =
                    serde_json::from_str(row.timings_json.as_deref().unwrap()).unwrap();
                if !unknown && scenario != "failed_confirmation" {
                    assert!(timings["reconciliation_ms"].is_number());
                }
            }
            let budget = reservations.lock().await;
            assert_eq!(budget.pending, 0);
            assert_eq!(
                budget.uncertain,
                matches!(scenario, "send_error" | "signature_mismatch" | "timeout")
            );
            assert_eq!(budget.remaining.is_empty(), !budget.uncertain);
        }
    }

    #[test]
    fn reservations_are_atomic_and_do_not_credit_unsettled_balances() {
        let sol = Pubkey::new_unique();
        let token = Pubkey::new_unique();
        let mut budget = BalanceReservations::default();
        assert!(budget.reserve(&[(sol, 100, 20), (token, 50, 30)]));
        assert_eq!(budget.available(sol, 100), 80);
        assert_eq!(budget.available(token, 1000), 20);
        assert!(!budget.reserve(&[(sol, 100, 10), (token, 50, 30)]));
        assert_eq!(
            budget.available(sol, 100),
            80,
            "failed multi-account reservation cannot debit SOL"
        );
        assert_eq!(budget.pending, 1);
        assert!(budget.reserve(&[(sol, 70, 10), (token, 20, 20)]));
        assert_eq!(budget.available(sol, 100), 60);
        assert_eq!(budget.available(token, 50), 0);
    }
    #[tokio::test]
    async fn preconfirmation_copies_once_and_route_failures_allow_processed_fallback() {
        for scenario in ["early", "route_failure", "processed_first", "fanout"] {
            let signer = Arc::new(Keypair::new());
            let authority = signer.pubkey();
            let nonce_account = Pubkey::new_unique();
            let nonce_blockhash = Hash::new_unique();
            let landed_signature = Arc::new(std::sync::Mutex::new(String::new()));
            let selected_signature = landed_signature.clone();
            let landed = std::sync::Mutex::new(serde_json::Value::Null);
            let server = TestRpc::start(move |request| match request["method"].as_str().unwrap() {
                "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
                "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":1000}}),
                "getBlockHeight" => json!(100),
                "getBalance" => json!({"context":{"slot":42},"value":1_000_000_000}),
                "getAccountInfo" => {
                    let value = if request["params"][0] == nonce_account.to_string() {
                        crate::mainnet::nonce::tests::nonce_value(authority, nonce_blockhash)
                    } else { mint_account() };
                    json!({"context":{"slot":42},"value":value})
                },
                "getMultipleAccounts" => {
                    let mut native = mint_account();
                    let mut bytes = STANDARD.decode(native["data"][0].as_str().unwrap()).unwrap();
                    bytes[44] = 9;
                    native["data"][0] = json!(STANDARD.encode(bytes));
                    json!({"context":{"slot":42},"value":[native,mint_account()]})
                },
                "getTokenAccountBalance" => json!({"context":{"slot":43},"value":{"amount":"1000","decimals":6,"uiAmount":0.001,"uiAmountString":"0.001"}}),
                "sendTransaction" => {
                    let transaction: Transaction = bincode::deserialize(&STANDARD.decode(request["params"][0].as_str().unwrap()).unwrap()).unwrap();
                    transaction.verify().unwrap();
                    if scenario == "fanout" {
                        assert_eq!(transaction.message.instructions[0].data,
                            solana_system_interface::instruction::advance_nonce_account(&nonce_account, &authority).data);
                        let price: u64 = bincode::deserialize(&transaction.message.instructions[2].data[1..]).unwrap();
                        if price == 100000 { return json!({"error":{"code":-32000,"message":"relay rejected first variant"}}); }
                        *selected_signature.lock().unwrap() = transaction.signatures[0].to_string();
                    }
                    let swap = transaction.message.instructions.iter().find(|ix| transaction.message.account_keys[ix.program_id_index as usize] == pump_fun::PROGRAM_ID).unwrap();
                    let output_index = swap.accounts[5];
                    let mint = transaction.message.account_keys[swap.accounts[2] as usize];
                    *landed.lock().unwrap() = json!({"transaction":{"message":{"accountKeys":transaction.message.account_keys.iter().map(ToString::to_string).collect::<Vec<_>>() }},
                        "meta":{"err":null,"preTokenBalances":[],"postTokenBalances":[{"accountIndex":output_index,"mint":mint.to_string(),"uiTokenAmount":{"amount":"1000"}}]}});
                    if scenario == "fanout" { json!({"error":{"code":-32000,"message":"second variant landed but acknowledgment lost"}}) }
                    else { json!(transaction.signatures[0].to_string()) }
                },
                "getTransaction" => {
                    if scenario == "fanout" { assert_eq!(request["params"][0], *selected_signature.lock().unwrap()); }
                    landed.lock().unwrap().clone()
                },
                "getSignatureStatuses" => {
                    let status = json!({"slot":43,"confirmations":1,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"});
                    let statuses = request["params"][0].as_array().unwrap().iter().map(|signature|
                        if scenario != "fanout" || signature.as_str().unwrap() == *selected_signature.lock().unwrap() { status.clone() } else { json!(null) }).collect::<Vec<_>>();
                    json!({"context":{"slot":43},"value":statuses})
                },
                method => panic!("unexpected RPC {method}"),
            }).await;
            let transport = HttpTransport::new(&HttpConfig::default()).unwrap();
            let mut config = AppConfig::load(std::path::Path::new("config.example.toml")).unwrap();
            config.sizing = SizingConfig::Fixed {
                amount: "0.001".into(),
            };
            config.token_policy = TokenPolicyConfig::All {
                minimum_input: "0.000001".into(),
                maximum_input: "1".into(),
            };
            let mainnet = config.mainnet.as_mut().unwrap();
            mainnet.sender_url = server.url.clone();
            if scenario == "fanout" {
                mainnet.fanout.enabled = true;
                mainnet.fanout.nonce_accounts = vec![nonce_account.to_string()];
                mainnet.fanout.routes = (0..2)
                    .map(|index| crate::config::FanoutRouteConfig {
                        name: format!("route-{index}"),
                        url: server.url.clone(),
                        tip_account: Pubkey::new_unique().to_string(),
                        tip_lamports: 5000,
                        priority_fee_micro_lamports: 100000 + index,
                    })
                    .collect();
            }
            let client = Arc::new(MainnetClient::new(
                server.url.clone(),
                mainnet,
                transport.clone(),
            ));
            client.latest_blockhash().await.unwrap();
            let base = Store::connect("sqlite::memory:").await.unwrap();
            if scenario == "fanout" {
                client
                    .nonce_pool
                    .initialize(
                        &client.rpc,
                        &mainnet.fanout.nonce_accounts,
                        authority,
                        &base,
                    )
                    .await
                    .unwrap();
            }
            let (store, mut journal) = base.background_journal().await.unwrap();
            let wallet = config.signal.wallet;
            let worker = ExecutionWorker::new(
                Arc::new(config),
                signer,
                store.clone(),
                Arc::new(ExecutionBackend::Mainnet(client)),
                TokenSafetyClient::new(&server.url, &transport),
                TransactionDecoder::new(wallet),
                Arc::new(Router::new(
                    Arc::new(transport.solana_rpc(&server.url)),
                    Duration::from_secs(2),
                )),
            );
            let mut processed = observation(wallet, 77);
            let VersionedMessage::Legacy(message) = &mut processed.transaction.message else {
                unreachable!()
            };
            let program_index = message.instructions[0].accounts[8] as usize;
            message.account_keys[program_index] = spl_token::id();
            message.instructions[0].data[8..16].copy_from_slice(&5000_u64.to_le_bytes());
            message.instructions[0].data[16..24].copy_from_slice(&10_000_000_u64.to_le_bytes());
            let mut early = processed.clone();
            early.origin = SignalOrigin::Preconfirmation;
            early.meta = TransactionMeta::default();
            if scenario == "route_failure" {
                let VersionedMessage::Legacy(message) = &mut early.transaction.message else {
                    unreachable!()
                };
                message.instructions[0].data[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
                message.instructions[0].data[16..24].copy_from_slice(&1_u64.to_le_bytes());
            }
            let signature = processed.signature.to_string();
            let observations = if scenario == "processed_first" {
                vec![processed, early.clone(), early]
            } else {
                vec![early.clone(), early, processed.clone(), processed]
            };
            let (sender, receiver) = tokio::sync::mpsc::channel(8);
            for observed in observations {
                store.record_observation(&observed).await.unwrap();
                sender
                    .send(QueuedObservation {
                        observed,
                        received_at: Instant::now(),
                        queued_at: Instant::now(),
                        payload_decode_us: 0,
                        observation_enqueue_us: 0,
                        database_timings: DatabaseTimings::default(),
                    })
                    .await
                    .unwrap();
            }
            drop(sender);
            worker.run(receiver).await.unwrap();
            drop(store);
            journal.wait().await.unwrap();
            let rows = base.status(10).await.unwrap();
            assert_eq!(rows.len(), 1, "{scenario}");
            assert_eq!(rows[0].signature, signature);
            assert_eq!(
                rows[0].copy_status.as_deref(),
                Some("landed"),
                "{scenario}: {:?}",
                rows[0].error
            );
            assert_eq!(
                server.count("sendTransaction"),
                if scenario == "fanout" { 2 } else { 1 },
                "{scenario}"
            );
            if scenario == "fanout" {
                assert_eq!(
                    rows[0].local_signature.as_deref(),
                    Some(landed_signature.lock().unwrap().as_str())
                );
                assert_eq!(base.variant_signatures(&signature).await.unwrap().len(), 2);
            }
            let timing: serde_json::Value =
                serde_json::from_str(rows[0].timings_json.as_ref().unwrap()).unwrap();
            assert_eq!(
                timing["preconfirmation"],
                if matches!(scenario, "early" | "fanout") {
                    1
                } else {
                    0
                },
                "{scenario}"
            );
        }
    }

    #[tokio::test]
    async fn unsupported_sources_do_not_send_and_next_valid_pump_copy_lands() {
        let token_reads = AtomicUsize::new(0);
        let landed_metadata = std::sync::Mutex::new(serde_json::Value::Null);
        let server = TestRpc::start(move |request| match request["method"].as_str().expect("method") {
            "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
            "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":1000}}),
            "getBlockHeight" => json!(100),
            "getBalance" => json!({"context":{"slot":42},"value":1_000_000_000}),
            "getAccountInfo" => json!({"context":{"slot":42},"value":mint_account()}),
            "getMultipleAccounts" => {
                let mut invalid = mint_account();
                invalid["owner"] = json!(Pubkey::new_unique().to_string());
                json!({"context":{"slot":42},"value":[mint_account(),invalid]})
            }
            "getTokenAccountBalance" => {
                token_reads.fetch_add(1, Ordering::SeqCst);
                json!({"context":{"slot":43},"value":{"amount":"1000","decimals":6,"uiAmount":0.001,"uiAmountString":"0.001"}})
            }
            "sendTransaction" => {
                let signed = STANDARD.decode(request["params"][0].as_str().expect("encoded transaction")).expect("base64");
                let transaction: Transaction = bincode::deserialize(&signed).expect("transaction");
                assert!(transaction.verify().is_ok());
                assert_eq!(token_reads.load(Ordering::SeqCst), 0, "output RPC must follow submission");
                let swap = transaction.message.instructions.iter().find(|instruction| transaction.message.account_keys[usize::from(instruction.program_id_index)] == pump_fun::PROGRAM_ID).expect("swap");
                let output_index = swap.accounts[5];
                let mint = transaction.message.account_keys[usize::from(swap.accounts[2])];
                *landed_metadata.lock().expect("metadata") = json!({
                    "transaction":{"message":{"accountKeys":transaction.message.account_keys.iter().map(ToString::to_string).collect::<Vec<_>>() }},
                    "meta":{"err":null,"preTokenBalances":[],"postTokenBalances":[{"accountIndex":output_index,"mint":mint.to_string(),"uiTokenAmount":{"amount":"1000"}}]}
                });
                json!(transaction.signatures[0].to_string())
            }
            "getTransaction" => landed_metadata.lock().expect("metadata").clone(),
            "getSignatureStatuses" => json!({"context":{"slot":43},"value":[{"slot":43,"confirmations":1,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"}]}),
            method => panic!("unexpected RPC {method}"),
        }).await;
        let transport = HttpTransport::new(&HttpConfig::default()).expect("transport");
        let mut config =
            AppConfig::load(std::path::Path::new("config.example.toml")).expect("config");
        config.sizing = SizingConfig::Fixed {
            amount: "0.001".to_owned(),
        };
        config.token_policy = TokenPolicyConfig::All {
            minimum_input: "0.000001".to_owned(),
            maximum_input: "1".to_owned(),
        };
        let mainnet_config = config.mainnet.as_mut().expect("mainnet");
        mainnet_config.source_direct = true;
        mainnet_config.sender_url = server.url.clone();
        let client = Arc::new(MainnetClient::new(
            server.url.clone(),
            mainnet_config,
            transport.clone(),
        ));
        client.latest_blockhash().await.expect("warm blockhash");
        let store = Store::connect("sqlite::memory:").await.expect("store");
        let source_wallet = config.signal.wallet;
        let worker = ExecutionWorker::new(
            Arc::new(config),
            Arc::new(Keypair::new()),
            store.clone(),
            Arc::new(ExecutionBackend::Mainnet(client)),
            TokenSafetyClient::new(&server.url, &transport),
            TransactionDecoder::new(source_wallet),
            Arc::new(Router::new(
                Arc::new(transport.solana_rpc(&server.url)),
                Duration::from_secs(2),
            )),
        );
        let cache = worker.balance_cache.clone();
        let copier = worker.signer.pubkey();
        let mut observations = Vec::new();
        for id in 1..=6 {
            let mut observed = observation(source_wallet, id);
            let VersionedMessage::Legacy(message) = &mut observed.transaction.message else {
                unreachable!()
            };
            match id {
                1 => {
                    let index = usize::from(message.instructions[0].program_id_index);
                    message.account_keys[index] = DexKind::RaydiumCpmm.program_id();
                }
                2 => message.instructions[0].data[..8].fill(0),
                3 => message.instructions[0].data.truncate(7),
                4 => message.instructions.push(message.instructions[0].clone()),
                5 => observed.meta.post_token_balances = None,
                6 => {
                    observed
                        .meta
                        .post_token_balances
                        .as_mut()
                        .expect("balances")[0]
                        .program_id = Some(spl_token_2022::id().to_string())
                }
                _ => unreachable!(),
            }
            observations.push(observed);
        }
        let valid_observation = observation(source_wallet, 7);
        let output_mint: Pubkey = valid_observation
            .meta
            .post_token_balances
            .as_ref()
            .expect("balances")[0]
            .mint
            .parse()
            .expect("mint");
        let copied_output = associated_token_address(&copier, &output_mint, &spl_token::id());
        observations.push(valid_observation);
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        for observed in observations {
            store.record_observation(&observed).await.expect("journal");
            sender
                .send(QueuedObservation {
                    observed,
                    received_at: Instant::now(),
                    queued_at: Instant::now(),
                    payload_decode_us: 0,
                    observation_enqueue_us: 0,
                    database_timings: DatabaseTimings::default(),
                })
                .await
                .expect("enqueue");
        }
        drop(sender);
        worker.run(receiver).await.expect("worker continues");
        let rows = store.status(10).await.expect("status");
        for id in 1..=6 {
            let signature = Signature::try_from([id; 64].as_slice())
                .expect("signature")
                .to_string();
            let row = rows
                .iter()
                .find(|row| row.signature == signature)
                .expect("source");
            assert_eq!(
                row.source_status, "unsupported",
                "source {id}: {:?}",
                row.error
            );
            assert!(row.local_signature.is_none());
            assert!(row.landed_slot.is_none());
            let reason: serde_json::Value =
                serde_json::from_str(row.error.as_deref().expect("reason")).expect("reason JSON");
            assert_eq!(
                reason["code"],
                match id {
                    1 => "unsupported_dex",
                    2 | 3 => "unsupported_instruction",
                    4 => "multi_hop",
                    5 => "ambiguous_trade",
                    6 => "unsupported_token",
                    _ => unreachable!(),
                }
            );
            assert!(row.timings_json.is_some());
        }
        let valid_signature = Signature::try_from([7; 64].as_slice())
            .expect("signature")
            .to_string();
        let valid = rows
            .iter()
            .find(|row| row.signature == valid_signature)
            .expect("valid source");
        assert_eq!(
            valid.copy_status.as_deref(),
            Some("landed"),
            "{:?}",
            valid.error
        );
        assert_eq!(valid.landed_slot, Some(43));
        assert_eq!(
            cache.get(&copier).await,
            None,
            "released reservations invalidate input snapshots"
        );
        assert_eq!(
            cache.get(&copied_output).await,
            Some(1000),
            "newly acquired token is cached for its next sell"
        );
        assert_eq!(
            server.count("getTokenAccountBalance"),
            1,
            "output cache refresh only, after submission"
        );
        assert_eq!(server.count("sendTransaction"), 1);
        assert_eq!(server.count("simulateTransaction"), 0);
        assert_eq!(server.count("getProgramAccounts"), 0);
        assert_eq!(server.count("getMultipleAccounts"), 1);
    }
}

struct PendingCopy {
    observed: ObservedTransaction,
    sized: SizedTrade,
    winner: ExecutableRoute,
    input_account: Pubkey,
    output_account: Pubkey,
    local_signature: solana_sdk::signature::Signature,
    route_latency_ms: u64,
}

#[derive(Default)]
struct BalanceReservations {
    remaining: std::collections::HashMap<Pubkey, u64>,
    pending: usize,
    uncertain: bool,
}

impl BalanceReservations {
    fn available(&self, address: Pubkey, observed: u64) -> u64 {
        self.remaining
            .get(&address)
            .map_or(observed, |remaining| observed.min(*remaining))
    }

    fn can_reserve(&self, debits: &[(Pubkey, u64, u64)]) -> bool {
        debits
            .iter()
            .all(|(address, observed, debit)| self.available(*address, *observed) >= *debit)
    }

    fn reserve(&mut self, debits: &[(Pubkey, u64, u64)]) -> bool {
        if !self.can_reserve(debits) {
            return false;
        }
        for (address, observed, debit) in debits {
            self.remaining
                .insert(*address, self.available(*address, *observed) - *debit);
        }
        self.pending += 1;
        true
    }
}
