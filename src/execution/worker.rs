use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

use crate::{
    config::AppConfig,
    decode::TransactionDecoder,
    domain::{
        AssetId, AttemptStatus, ObservedTransaction, SkipReason, TradeIntent, UiTokenBalance,
        UnsupportedReason,
    },
    error::{CopyTraderError, Result},
    execution::{
        ExecutionBackend, cache::WalletBalanceCache, confirm::wait_for_confirmation,
        sizing::SizingPolicy,
    },
    routing::{Router, RoutingTimings, pump_fun},
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
    last_executed_slot: u64,
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
            last_executed_slot: 0,
        }
    }

    pub fn with_balance_cache(mut self, balance_cache: WalletBalanceCache) -> Self {
        self.balance_cache = balance_cache;
        self
    }

    pub async fn run(mut self, input: Receiver<QueuedObservation>) -> Result<()> {
        let writer = TimingWriter::new(self.store.clone());
        let result = self.run_inner(input, &writer).await;
        writer.finish().await;
        result
    }

    async fn run_inner(
        &mut self,
        mut input: Receiver<QueuedObservation>,
        writer: &TimingWriter,
    ) -> Result<()> {
        while let Some(queued) = input.recv().await {
            let worker_started = Instant::now();
            let queue_wait_us =
                CopyTimings::micros(worker_started.duration_since(queued.queued_at));
            let source_signature = queued.observed.signature.to_string();
            let mut timings = CopyTimings::new(queued.received_at);
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
            let result = self.handle(queued.observed, &mut timings).await;
            timings.finish();
            if let Err(error) = result {
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
    ) -> Result<()> {
        let checks_started = Instant::now();
        let source_signature = observed.signature.to_string();
        if observed.slot < self.last_executed_slot {
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
            self.balance_cache.get(&input_account).await.unwrap_or(0)
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
                let price = mainnet.fixed_priority_fee_micro_lamports.ok_or_else(|| {
                    CopyTraderError::Execution("fixed priority fee missing".into())
                })?;
                let priority = u64::from(crate::routing::pump_swap::COMPUTE_UNIT_LIMIT)
                    .checked_mul(price)
                    .and_then(|value| value.checked_add(999_999))
                    .map(|value| value / 1_000_000)
                    .ok_or_else(|| CopyTraderError::Execution("priority fee overflow".into()))?;
                let required_native = wrap_lamports
                    .checked_add(priority)
                    .and_then(|value| value.checked_add(5_000))
                    .and_then(|value| value.checked_add(mainnet.tip_lamports))
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
        if !self
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
        let invalidation_started = Instant::now();
        self.invalidate_balances(
            sized.intent.input_asset,
            &input_account,
            sized.intent.output_asset,
            &output_account,
        )
        .await?;
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
        let send_result = self.backend.send(&winner.transaction).await;
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
        timings.finish();
        match send_result {
            Ok(returned) if returned == local_signature => {}
            Ok(returned) => {
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
        timings.stage("confirmation_ms");
        let confirmation = match wait_for_confirmation(
            self.backend.rpc(),
            self.backend.label(),
            &local_signature,
            Duration::from_secs(self.config.execution.confirmation_timeout_seconds),
        )
        .await
        {
            Ok(confirmation) => confirmation,
            Err(error) => {
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
        let (input_after, _, wsol_after) = tokio::try_join!(
            self.asset_balance_network(sized.intent.input_asset, &input_account),
            self.asset_balance_network(sized.intent.output_asset, &output_account),
            async {
                if winner.route.dex == crate::domain::DexKind::PumpSwap
                    && (sized.intent.input_asset == AssetId::NativeSol
                        || sized.intent.output_asset == AssetId::NativeSol)
                {
                    self.asset_balance_network(
                        AssetId::Token(spl_token::native_mint::id()),
                        &associated_token_address(
                            &self.signer.pubkey(),
                            &spl_token::native_mint::id(),
                            &spl_token::id(),
                        ),
                    )
                    .await
                } else if sized.intent.input_asset != AssetId::NativeSol
                    && sized.intent.output_asset != AssetId::NativeSol
                {
                    self.asset_balance_network(AssetId::NativeSol, &self.signer.pubkey())
                        .await
                } else {
                    Ok(0)
                }
            }
        )?;
        let input_after = if pump_swap_buy {
            input_after.checked_add(wsol_after).ok_or_else(|| {
                CopyTraderError::Execution("combined post-trade SOL balance overflow".into())
            })?
        } else {
            input_after
        };
        let spent = available.checked_sub(input_after).ok_or_else(|| {
            CopyTraderError::Execution("input balance increased after swap".to_owned())
        })?;
        if spent < sized.input_amount {
            warn!(
                %source_signature,
                %local_signature,
                observed_debit = spent,
                prepared_input = sized.input_amount,
                "landed transaction confirmed, but the post-confirmation input balance under-reported the debit"
            );
        }

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
            self.backend
                .mainnet()
                .map_or(0, |mainnet| mainnet.tip_lamports()),
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
        self.last_executed_slot = observed.slot;
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
        self.balance_cache
            .get_or_fetch(
                self.backend.rpc(),
                asset,
                self.signer.pubkey(),
                *token_account,
            )
            .await
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
    async fn unsupported_sources_do_not_send_and_next_valid_pump_copy_lands() {
        let token_reads = AtomicUsize::new(0);
        let landed_metadata = std::sync::Mutex::new(serde_json::Value::Null);
        let server = TestRpc::start(move |request| match request["method"].as_str().expect("method") {
            "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
            "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":1000}}),
            "getBlockHeight" => json!(100),
            "getBalance" => json!({"context":{"slot":42},"value":1_000_000_000}),
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
        assert_eq!(cache.get(&copier).await, Some(1_000_000_000));
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
