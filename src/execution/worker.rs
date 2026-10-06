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
            let source_signature = queued.observed.signature.to_string();
            let mut timings = CopyTimings::new(queued.received_at);
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
        let intent = match self.decoder.decode(&observed) {
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
            direct_pump_fun_mints(&intent, observed.meta.post_token_balances.as_deref());
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
        let sized = match sizing.size_trade(intent, &input_rule, input_info.decimals)? {
            Ok(sized) => sized,
            Err(reason) => {
                self.store.mark_skipped(&source_signature, reason).await?;
                return Ok(());
            }
        };
        let input_account = associated_token_address(
            &self.signer.pubkey(),
            &input_mint,
            &input_info.token_program,
        );
        let available = if let Some(balance) = native_balance {
            balance
        } else {
            self.cached_asset_balance(sized.intent.input_asset, &input_account)
                .await?
        };
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

        timings.stage("route_ms");
        let started = Instant::now();
        let result = self
            .router
            .build(
                &sized,
                self.signer.as_ref(),
                self.backend.as_ref(),
                self.config.execution.slippage_bps,
                &mut routing_timings,
            )
            .await;
        timings.finish();
        let route_latency_ms = CopyTimings::millis(started.elapsed());
        timings
            .values
            .insert("route_wall_ms", CopyTimings::millis(started.elapsed()));
        timings.values.insert("route_ms", route_latency_ms);
        timings.values.extend(routing_timings.stages.snapshot());
        let winner = result?;
        timings.mark("quote_complete_ms");
        timings.mark("checks_complete_ms");
        timings.stage("post_route_ms");

        let output_account = associated_token_address(
            &self.signer.pubkey(),
            &output_mint,
            &output_info.token_program,
        );
        let output_before = if let Some(balance) = winner.output_balance_before {
            balance
        } else {
            self.asset_balance_or_zero(sized.intent.output_asset, &output_account)
                .await?
        };
        let signed = bincode::serialize(&winner.transaction).map_err(|error| {
            CopyTraderError::Execution(format!("failed to serialize signed transaction: {error}"))
        })?;
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
        timings.stage("sender_request_ms");
        timings.mark("sender_request_started_ms");
        timings.since_receipt("receipt_to_send_start_ms");
        let send_result = self.backend.send(&winner.transaction).await;
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
        if let Some(error) = confirmation.error {
            self.store
                .update_attempt(&source_signature, AttemptStatus::Failed, Some(&error))
                .await?;
            return Ok(());
        }

        timings.stage("reconciliation_ms");
        let input_after = self
            .asset_balance_network(sized.intent.input_asset, &input_account)
            .await?;
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

        let output_after = self
            .asset_balance_network(sized.intent.output_asset, &output_account)
            .await?;
        let native_cost_adjustment = if matches!(sized.intent.output_asset, AssetId::NativeSol) {
            let fee = self
                .backend
                .rpc()
                .get_fee_for_message(&winner.transaction.message)
                .await
                .map_err(|error| {
                    CopyTraderError::Execution(format!(
                        "failed to calculate landed transaction fee: {error}"
                    ))
                })?;
            fee.checked_add(
                self.backend
                    .mainnet()
                    .map_or(0, |mainnet| mainnet.tip_lamports()),
            )
            .ok_or_else(|| CopyTraderError::Execution("landed fee overflow".to_owned()))?
        } else {
            0
        };
        let adjusted_output_after = output_after
            .checked_add(native_cost_adjustment)
            .ok_or_else(|| CopyTraderError::Execution("output balance overflow".to_owned()))?;
        let received = adjusted_output_after
            .checked_sub(output_before)
            .ok_or_else(|| {
                CopyTraderError::Execution("output balance decreased after swap".to_owned())
            })?;
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
                        return Ok((infos, 0, true));
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

    async fn cached_asset_balance(&self, _asset: AssetId, token_account: &Pubkey) -> Result<u64> {
        if let Some(balance) = self.balance_cache.get(token_account).await {
            return Ok(balance);
        }
        #[cfg(test)]
        {
            return self.asset_balance_network(_asset, token_account).await;
        }
        #[cfg(not(test))]
        {
            let _ = token_account;
            Err(CopyTraderError::Execution(format!(
                "balance cache is not ready for account {token_account}"
            )))
        }
    }

    async fn asset_balance_network(&self, asset: AssetId, token_account: &Pubkey) -> Result<u64> {
        if matches!(asset, AssetId::NativeSol) {
            return self
                .backend
                .rpc()
                .get_balance(&self.signer.pubkey())
                .await
                .map_err(|error| {
                    CopyTraderError::Execution(format!("failed to read SOL balance: {error}"))
                });
        }
        self.backend
            .rpc()
            .get_token_account_balance(token_account)
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("failed to read token balance: {error}"))
            })?
            .amount
            .parse::<u64>()
            .map_err(|error| CopyTraderError::Execution(format!("invalid token balance: {error}")))
    }

    async fn asset_balance_or_zero(&self, asset: AssetId, token_account: &Pubkey) -> Result<u64> {
        match self.cached_asset_balance(asset, token_account).await {
            Ok(balance) => Ok(balance),
            Err(_) => Ok(0),
        }
    }
}

struct InitialReads {
    input_info: MintInfo,
    output_info: MintInfo,
    native_balance: Option<u64>,
    mint_read_ms: u64,
    mint_from_source: bool,
}

fn direct_pump_fun_mints(
    intent: &TradeIntent,
    post_balances: Option<&[UiTokenBalance]>,
) -> Option<(MintInfo, MintInfo)> {
    if intent.input_asset != AssetId::NativeSol || intent.source_pool.is_some() {
        return None;
    }
    let AssetId::Token(output_mint) = intent.output_asset else {
        return None;
    };
    let source = intent.source_instruction.as_ref()?;
    if source.instruction.program_id != pump_fun::PROGRAM_ID
        || !pump_fun::is_buy(&source.instruction.data)
    {
        return None;
    }
    let output_ata =
        associated_token_address(&source.source_wallet, &output_mint, &spl_token::id());
    let mut output_accounts = source
        .wallet_token_accounts
        .iter()
        .filter(|(_, mint, _)| *mint == output_mint);
    if !matches!(output_accounts.next(), Some((address, _, program)) if *address == output_ata && *program == spl_token::id())
        || output_accounts.next().is_some()
        || !source
            .instruction
            .accounts
            .iter()
            .any(|meta| meta.pubkey == output_ata)
    {
        return None;
    }
    let source_wallet = source.source_wallet.to_string();
    let output_mint = output_mint.to_string();
    let token_program = spl_token::id().to_string();
    let mut matching = post_balances?.iter().filter(|balance| {
        balance.owner.as_deref() == Some(source_wallet.as_str())
            && balance.mint == output_mint
            && balance.program_id.as_deref() == Some(token_program.as_str())
    });
    let output = matching.next()?;
    if matching.next().is_some() {
        return None;
    }
    Some((
        MintInfo {
            decimals: 9,
            token_program: spl_token::id(),
            has_transfer_fee: false,
        },
        MintInfo {
            decimals: output.ui_token_amount.decimals,
            token_program: spl_token::id(),
            has_transfer_fee: false,
        },
    ))
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
        let infos = direct_pump_fun_mints(&intent, Some(std::slice::from_ref(&balance)))
            .expect("direct mint evidence");
        assert_eq!(infos.0.decimals, 9);
        assert_eq!(infos.1.decimals, 6);
        assert_eq!(infos.1.token_program, spl_token::id());
        assert!(!infos.1.has_transfer_fee);

        let mut unsupported = balance.clone();
        unsupported.program_id = Some(spl_token_2022::id().to_string());
        assert!(direct_pump_fun_mints(&intent, Some(&[unsupported])).is_none());
        assert!(direct_pump_fun_mints(&intent, None).is_none());
        let mut ambiguous = intent.clone();
        ambiguous
            .source_instruction
            .as_mut()
            .unwrap()
            .wallet_token_accounts
            .push((Pubkey::new_unique(), mint, spl_token_2022::id()));
        assert!(direct_pump_fun_mints(&ambiguous, Some(std::slice::from_ref(&balance))).is_none());
        let mut wrong_instruction = intent.clone();
        wrong_instruction
            .source_instruction
            .as_mut()
            .unwrap()
            .instruction
            .program_id = Pubkey::new_unique();
        assert!(direct_pump_fun_mints(&wrong_instruction, Some(&[balance])).is_none());
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
                let amount = if token_reads.fetch_add(1, Ordering::SeqCst) == 0 { "0" } else { "1000" };
                json!({"context":{"slot":42},"value":{"amount":amount,"decimals":6,"uiAmount":0.0,"uiAmountString":"0"}})
            }
            "sendTransaction" => {
                let signed = STANDARD.decode(request["params"][0].as_str().expect("encoded transaction")).expect("base64");
                let transaction: Transaction = bincode::deserialize(&signed).expect("transaction");
                assert!(transaction.verify().is_ok());
                json!(transaction.signatures[0].to_string())
            }
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
        observations.push(observation(source_wallet, 7));
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        for observed in observations {
            store.record_observation(&observed).await.expect("journal");
            sender
                .send(QueuedObservation {
                    observed,
                    received_at: Instant::now(),
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
        assert_eq!(server.count("sendTransaction"), 1);
        assert_eq!(server.count("simulateTransaction"), 0);
        assert_eq!(server.count("getProgramAccounts"), 0);
        assert_eq!(server.count("getMultipleAccounts"), 1);
    }
}
