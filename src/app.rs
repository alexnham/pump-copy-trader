use std::{env, path::Path, str::FromStr, sync::Arc};

use solana_sdk::signature::{Keypair, Signature, Signer, read_keypair_file};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{
    cli::{Cli, Command},
    config::{AppConfig, ExecutionTarget},
    decode::TransactionDecoder,
    domain::AttemptStatus,
    error::{CopyTraderError, Result},
    execution::{ExecutionBackend, ExecutionWorker, cache::WalletBalanceCache},
    http::HttpTransport,
    mainnet::MainnetClient,
    routing::Router,
    signal::{
        LaserstreamSource, LookupCache, PreconfirmationSource, RecoveryClient, SignalSource,
        check_connection,
    },
    storage::Store,
    telemetry::compact_id,
    token::{
        accounts::associated_token_address,
        extensions::{TokenSafetyClient, decimal_to_atomic},
    },
};

pub async fn run(cli: Cli) -> Result<()> {
    let config = Arc::new(AppConfig::load(&cli.config)?);
    info!(config = %cli.config.display(), target = config.execution.target.as_str(), tokens = config.tokens.len(), "configuration loaded");
    let store = Store::connect(&config.storage.database_url).await?;
    match cli.command {
        Command::Run => run_service(config, store).await,
        Command::PreconfirmationDiagnostics { seconds, wallet } => {
            let api_key = required_env("HELIUS_API_KEY")?;
            PreconfirmationSource::new(
                config.signal.preconfirmations.websocket_url.clone(),
                &api_key,
                wallet.unwrap_or(config.signal.wallet),
                store,
                LookupCache::default(),
            )
            .diagnose(seconds)
            .await
        }
        Command::Doctor => doctor(&config, &store).await,
        Command::Status { limit } => status(&store, limit).await,
        Command::Latency { limit } => latency(&store, limit).await,
        Command::TransactionGaps => {
            let api_key = required_env("HELIUS_API_KEY")?;
            let endpoint = config.helius_http_url(&api_key)?;
            crate::storage::run_transaction_gap_worker(
                store,
                endpoint,
                HttpTransport::new(&config.http)?,
            )
            .await
        }
    }
}

async fn run_service(config: Arc<AppConfig>, store: Store) -> Result<()> {
    config.require_live_execution()?;
    let api_key = required_env("HELIUS_API_KEY")?;
    let keypair_path = required_env("COPY_TRADER_KEYPAIR_PATH")?;
    let signer = Arc::new(load_keypair(Path::new(&keypair_path))?);
    for program in [
        crate::domain::DexKind::PumpSwap.program_id(),
        crate::routing::pump_fun::PROGRAM_ID,
    ] {
        crate::token::accounts::user_volume_address(&program, &signer.pubkey());
        crate::token::accounts::user_volume_address(&program, &config.signal.wallet);
    }
    let laserstream_endpoint = config.laserstream_endpoint(&api_key)?;
    let helius_http = config.helius_http_url(&api_key)?;
    let transport = HttpTransport::new(&config.http)?;
    let helius_slot = transport.warm(&helius_http).await?;
    info!(helius_slot, "Helius HTTP transport warmed");
    let backend =
        Arc::new(build_backend(&config, helius_http.clone(), transport.clone(), &api_key).await?);
    if let Some(mainnet) = backend.mainnet() {
        mainnet.warm_sender().await;
        if mainnet.fanout.enabled {
            mainnet
                .nonce_pool
                .initialize(
                    &mainnet.rpc,
                    &mainnet.fanout.nonce_accounts,
                    signer.pubkey(),
                    &store,
                )
                .await?;
        }
    }
    resolve_uncertain(&store, &backend).await?;
    let wsol = associated_token_address(
        &signer.pubkey(),
        &spl_token::native_mint::id(),
        &spl_token::id(),
    );
    let existing = backend
        .rpc()
        .get_account_with_commitment(
            &wsol,
            solana_commitment_config::CommitmentConfig::confirmed(),
        )
        .await
        .map_err(|error| {
            CopyTraderError::Execution(format!(
                "cannot validate copier WSOL account at startup: {error}"
            ))
        })?;
    if let Some(account) = existing.value {
        crate::token::accounts::validate_wsol_account(&account, &signer.pubkey())?;
    }

    let (store, mut journal_writer) = store.background_journal().await?;
    let recovery = RecoveryClient::new(
        helius_http.clone(),
        config.signal.wallet,
        store.clone(),
        transport.clone(),
    );
    let lookups = LookupCache::default();
    let preconfirmation_source = config.signal.preconfirmations.enabled.then(|| {
        PreconfirmationSource::new(
            config.signal.preconfirmations.websocket_url.clone(),
            &api_key,
            config.signal.wallet,
            store.clone(),
            lookups.clone(),
        )
    });
    let source = LaserstreamSource::new(
        laserstream_endpoint,
        api_key,
        config.signal.wallet,
        store.clone(),
        recovery,
        &config.signal.commitment,
    )
    .with_lookup_cache(lookups);
    let decoder = TransactionDecoder::new(config.signal.wallet);
    let execution_rpc = Arc::new(transport.solana_rpc(&helius_http));
    let allowed_mints = config
        .tokens
        .iter()
        .map(|rule| rule.mint)
        .collect::<Vec<_>>();
    let router = Arc::new(Router::new(
        execution_rpc.clone(),
        std::time::Duration::from_millis(config.routing.timeout_ms),
    ));
    let balance_cache = WalletBalanceCache::default();
    let holdings = balance_cache
        .preload_holdings(&execution_rpc, signer.pubkey())
        .await?;
    tracing::info!(holdings, "wallet token holdings preloaded");
    balance_cache
        .fetch(
            &execution_rpc,
            crate::domain::AssetId::Token(spl_token::native_mint::id()),
            signer.pubkey(),
            wsol,
        )
        .await?;
    tokio::spawn(balance_cache.clone().run(
        execution_rpc.clone(),
        signer.pubkey(),
        allowed_mints.clone(),
    ));
    let worker = ExecutionWorker::new(
        config.clone(),
        signer.clone(),
        store,
        backend.clone(),
        TokenSafetyClient::new(&helius_http, &transport),
        decoder,
        router,
    )
    .with_balance_cache(balance_cache);
    let (sender, receiver) = mpsc::channel(config.signal.queue_capacity);
    if let Some(mainnet) = backend.mainnet() {
        mainnet.latest_blockhash().await?;
    }
    info!(
        wallet = %compact_id(config.signal.wallet.to_string()),
        "starting tracking wallet stream"
    );
    let preconfirmation_task = preconfirmation_source.map(|source| {
        let sender = sender.clone();
        tokio::spawn(async move { source.run(sender).await })
    });
    let mut source_task = tokio::spawn(async move { source.run(sender).await });
    let mut worker_task = tokio::spawn(async move { worker.run(receiver).await });
    info!(wallet = %compact_id(config.signal.wallet.to_string()), target = backend.label(), "copy trader running");

    let sender_warming = async {
        if let Some(mainnet) = backend.mainnet() {
            tokio::join!(
                mainnet.keep_sender_warm(),
                mainnet.keep_blockhash_fresh(),
                mainnet.nonce_pool.keep_fresh(&mainnet.rpc, signer.pubkey())
            );
        } else {
            std::future::pending::<()>().await;
        }
    };
    let mut journal_finished = false;
    let mut source_finished = false;
    let mut worker_finished = false;
    let mut result = tokio::select! {
        result = journal_writer.wait() => { journal_finished = true; result },
        () = sender_warming => Ok(()),
        result = &mut source_task => { source_finished = true; join_result("signal source", result) },
        result = &mut worker_task => { worker_finished = true; join_result("execution worker", result) },
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(CopyTraderError::Io)?;
            info!("shutdown requested");
            Ok(())
        }
    };
    source_task.abort();
    if let Some(task) = preconfirmation_task {
        task.abort();
        let _ = task.await;
    }
    if result.is_err() {
        worker_task.abort();
    }
    if !source_finished {
        let _ = source_task.await;
    }
    if !worker_finished {
        if result.is_ok() {
            if let Err(error) = join_result("execution worker", worker_task.await) {
                result = Err(error);
            }
        } else {
            let _ = worker_task.await;
        }
    }
    if !journal_finished {
        journal_writer.wait().await?;
    }
    result
}

async fn doctor(config: &AppConfig, store: &Store) -> Result<()> {
    let api_key = required_env("HELIUS_API_KEY")?;
    let keypair_path = required_env("COPY_TRADER_KEYPAIR_PATH")?;
    let signer = load_keypair(Path::new(&keypair_path))?;
    let laserstream_endpoint = config.laserstream_endpoint(&api_key)?;
    check_connection(laserstream_endpoint, &api_key).await?;
    if config.signal.preconfirmations.enabled {
        PreconfirmationSource::new(
            config.signal.preconfirmations.websocket_url.clone(),
            &api_key,
            config.signal.wallet,
            store.clone(),
            LookupCache::default(),
        )
        .check_connection()
        .await?;
        println!("Helius preconfirmation WebSocket: reachable");
    }
    let helius_http = config.helius_http_url(&api_key)?;
    let transport = HttpTransport::new(&config.http)?;
    let _ = transport.warm(&helius_http).await?;
    let backend = build_backend(config, helius_http.clone(), transport.clone(), &api_key).await?;
    let token_safety = TokenSafetyClient::new(&helius_http, &transport);
    for rule in &config.tokens {
        let mint = token_safety.inspect_mint(&rule.mint).await?;
        let minimum = decimal_to_atomic(&rule.minimum_input, mint.decimals)?;
        let maximum = decimal_to_atomic(&rule.maximum_input, mint.decimals)?;
        if minimum > maximum {
            return Err(CopyTraderError::Configuration(format!(
                "minimum_input exceeds maximum_input for {}",
                rule.mint
            )));
        }
        if config.execution.target == ExecutionTarget::Mainnet {
            let account =
                associated_token_address(&signer.pubkey(), &rule.mint, &mint.token_program);
            let balance = match backend.rpc().get_token_account_balance(&account).await {
                Ok(balance) => balance.amount,
                Err(_) => "0".to_owned(),
            };
            println!("token {} balance_atomic={balance}", rule.mint);
        }
    }
    let _ = store.status(1).await?;
    println!("configuration: ok");
    println!("copier signer: {}", signer.pubkey());
    println!("Helius LaserStream gRPC: reachable");
    println!("execution target: {}", backend.label());
    if config.execution.target == ExecutionTarget::Mainnet {
        let lamports = backend
            .rpc()
            .get_balance(&signer.pubkey())
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("failed to read copier SOL balance: {error}"))
            })?;
        println!("copier SOL balance: {lamports} lamports");
    }
    println!("SQLite schema: ok");
    Ok(())
}

async fn latency(store: &Store, limit: u32) -> Result<()> {
    let rows = store.status(limit).await?;
    let timings = rows
        .iter()
        .filter_map(|row| {
            row.timings_json
                .as_deref()
                .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        })
        .collect::<Vec<_>>();
    for key in [
        "receipt_to_send_start_us",
        "payload_decode_us",
        "ingress_to_worker_us",
        "queue_wait_us",
        "route_wall_us",
        "signing_only_us",
        "variant_build_us",
        "variant_size_checks_us",
        "fee_balance_lookup_us",
        "submission_wait_us",
        "sender_request_us",
    ] {
        let mut values = timings
            .iter()
            .filter_map(|timing| timing[key].as_u64())
            .collect::<Vec<_>>();
        values.sort_unstable();
        if values.is_empty() {
            println!("{key}: no samples");
            continue;
        }
        let percentile = |percent: usize| values[(values.len() * percent).div_ceil(100) - 1];
        println!(
            "{key}: samples={} p50={} p95={} p99={} max={}",
            values.len(),
            percentile(50),
            percentile(95),
            percentile(99),
            values[values.len() - 1]
        );
        if key == "receipt_to_send_start_us" {
            let below = values.iter().filter(|&&value| value < 1000).count();
            println!(
                "receipt_to_send_under_1ms: {below}/{} ({:.1}%)",
                values.len(),
                below as f64 * 100.0 / values.len() as f64
            );
        }
    }
    Ok(())
}

async fn status(store: &Store, limit: u32) -> Result<()> {
    for row in store.status(limit).await? {
        println!(
            "source_slot={} copy_slot={} slot_delta={} source={} signature={} dex={} pool={} target={} copy={} local={} landed_route={} error={} timings={}",
            row.slot,
            row.landed_slot
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            row.slot_delta()
                .map(|delta| delta.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            row.source_status,
            row.signature,
            row.dex.as_deref().unwrap_or("-"),
            row.pool.as_deref().unwrap_or("-"),
            row.execution_target.as_deref().unwrap_or("-"),
            row.copy_status.as_deref().unwrap_or("-"),
            row.local_signature.as_deref().unwrap_or("-"),
            row.landed_route.as_deref().unwrap_or("-"),
            row.error.as_deref().unwrap_or("-"),
            row.timings_json.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

async fn resolve_uncertain(store: &Store, backend: &ExecutionBackend) -> Result<()> {
    let abandoned = store.mark_unjournaled_attempts_unknown().await?;
    if abandoned > 0 {
        warn!(
            abandoned,
            "marked attempts without persisted signatures as unknown"
        );
    }
    let mut unresolved_fanout = false;
    for (source_signature, local_signature) in store.unresolved_attempts(backend.target()).await? {
        let variants = store.variant_signatures(&source_signature).await?;
        let is_fanout = !variants.is_empty();
        let candidates = if variants.is_empty() {
            vec![local_signature.clone()]
        } else {
            variants
        };
        let signatures = candidates
            .iter()
            .map(|s| Signature::from_str(s))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| CopyTraderError::Storage(format!("invalid variant signature: {e}")))?;
        let statuses = backend
            .rpc()
            .get_signature_statuses_with_history(&signatures)
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("uncertain signature query failed: {error}"))
            })?;
        let selected = statuses.value.iter().enumerate().find(|(_, status)| {
            status.as_ref().is_some_and(|status| {
                status
                    .satisfies_commitment(solana_client::rpc_config::CommitmentConfig::confirmed())
            })
        });
        if let Some((index, _)) = selected {
            store
                .select_variant(&source_signature, &candidates[index])
                .await?;
        }
        match selected.map(|(_, status)| status) {
            Some(Some(status))
                if status.satisfies_commitment(
                    solana_client::rpc_config::CommitmentConfig::confirmed(),
                ) =>
            {
                store
                    .record_landed_slot(&source_signature, status.slot)
                    .await?;
                if let Some(error) = &status.err {
                    store
                        .update_attempt(
                            &source_signature,
                            AttemptStatus::Failed,
                            Some(&error.to_string()),
                        )
                        .await?;
                } else {
                    store
                        .update_attempt(&source_signature, AttemptStatus::Landed, None)
                        .await?;
                }
            }
            _ => {
                unresolved_fanout |= is_fanout;
                warn!(source = %compact_id(&source_signature), local = %compact_id(&local_signature), "submission remains uncertain")
            }
        }
    }
    if unresolved_fanout {
        return Err(CopyTraderError::Execution("unresolved nonce fanout from a previous run; resolve its on-chain outcome before restarting trading".into()));
    }
    Ok(())
}

async fn build_backend(
    config: &AppConfig,
    helius_http: url::Url,
    transport: Arc<HttpTransport>,
    api_key: &str,
) -> Result<ExecutionBackend> {
    match config.execution.target {
        ExecutionTarget::Mainnet => {
            let mainnet_config = config.mainnet.as_ref().ok_or_else(|| {
                CopyTraderError::Configuration("missing [mainnet] configuration".to_owned())
            })?;
            let resolved_config = mainnet_config.with_helius_api_key(api_key)?;
            let client = Arc::new(MainnetClient::new(helius_http, &resolved_config, transport));
            let slot = client.warm().await?;
            info!(target = "mainnet", slot, "execution backend ready");
            Ok(ExecutionBackend::Mainnet(client))
        }
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).map_err(|_| {
        CopyTraderError::Configuration(format!("required environment variable {name} is missing"))
    })
}

fn load_keypair(path: &Path) -> Result<Keypair> {
    read_keypair_file(path).map_err(|error| {
        CopyTraderError::Configuration(format!(
            "cannot read copier keypair {}: {error}",
            path.display()
        ))
    })
}

fn join_result(
    task: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(result) => result,
        Err(error) => Err(CopyTraderError::Execution(format!(
            "{task} task panicked or was cancelled: {error}"
        ))),
    }
}

#[cfg(test)]
mod landing_tests {
    use super::*;
    use crate::{
        config::{HttpConfig, MainnetConfig},
        test_rpc::TestRpc,
    };
    use serde_json::json;

    #[tokio::test]
    async fn fanout_recovery_selects_landed_variant_and_blocks_unknown_restart() {
        use crate::{
            config::{HttpConfig, MainnetConfig},
            test_rpc::TestRpc,
        };
        use serde_json::json;
        for confirmed in [true, false] {
            let first = Signature::new_unique();
            let second = Signature::new_unique();
            let server = TestRpc::start(move |request| {
                let signatures = request["params"][0].as_array().unwrap();
                assert_eq!(signatures.len(), 2);
                let statuses = signatures.iter().map(|signature| {
                    if confirmed && signature.as_str().unwrap() == second.to_string() {
                        json!({"slot":45,"confirmations":1,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"})
                    } else { json!(null) }
                }).collect::<Vec<_>>();
                json!({"context":{"slot":999},"value":statuses})
            }).await;
            let config = MainnetConfig {
                fanout: Default::default(),
                source_direct: false,
                fixed_priority_fee_micro_lamports: None,
                sender_url: server.url.clone(),
                tip_lamports: 5000,
                priority_level: "High".into(),
                max_priority_fee_micro_lamports: 100,
            };
            let backend = ExecutionBackend::Mainnet(Arc::new(MainnetClient::new(
                server.url.clone(),
                &config,
                HttpTransport::new(&HttpConfig::default()).unwrap(),
            )));
            let store = Store::connect("sqlite::memory:").await.unwrap();
            store
                .record_recovered_signature("source", 42)
                .await
                .unwrap();
            store
                .reserve_attempt("source", ExecutionTarget::Mainnet, 100, 90)
                .await
                .unwrap();
            store
                .persist_fanout(
                    "source",
                    "account",
                    "nonce",
                    &[
                        (first.to_string(), vec![1], "first".into()),
                        (second.to_string(), vec![2], "second".into()),
                    ],
                    "{}",
                )
                .await
                .unwrap();
            let result = resolve_uncertain(&store, &backend).await;
            assert_eq!(result.is_ok(), confirmed);
            let rows = store.status(1).await.unwrap();
            if confirmed {
                assert_eq!(
                    rows[0].local_signature.as_deref(),
                    Some(second.to_string().as_str())
                );
                assert_eq!(rows[0].copy_status.as_deref(), Some("landed"));
                assert_eq!(rows[0].landed_slot, Some(45));
            }
            assert_eq!(server.count("sendTransaction"), 0);
        }
    }

    #[tokio::test]
    async fn recovery_records_confirmed_slots_and_leaves_processed_uncertain() {
        for (commitment, failed) in [
            ("confirmed", false),
            ("finalized", false),
            ("confirmed", true),
            ("processed", false),
            ("processed", true),
        ] {
            let server = TestRpc::start(move |_| {
                let error = if failed { json!("AccountNotFound") } else { json!(null) };
                json!({"context":{"slot":999},"value":[{"slot":45,"confirmations":1,"err":error,
                    "status":if failed { json!({"Err":"AccountNotFound"}) } else { json!({"Ok":null}) },
                    "confirmationStatus":commitment}]})
            }).await;
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
            let backend = ExecutionBackend::Mainnet(Arc::new(MainnetClient::new(
                server.url.clone(),
                &config,
                transport,
            )));
            let store = Store::connect("sqlite::memory:").await.expect("store");
            store
                .record_recovered_signature("source", 42)
                .await
                .expect("source");
            store
                .reserve_attempt("source", ExecutionTarget::Mainnet, 100, 90)
                .await
                .expect("reserve");
            store
                .persist_signed("source", &Signature::default().to_string(), &[], "{}")
                .await
                .expect("signed");
            resolve_uncertain(&store, &backend).await.expect("recovery");
            let rows = store.status(1).await.expect("status");
            let row = &rows[0];
            if commitment == "processed" {
                assert_eq!(row.landed_slot, None);
                assert_eq!(row.copy_status.as_deref(), Some("submitting"));
            } else {
                assert_eq!(row.landed_slot, Some(45));
                assert_eq!(row.slot_delta(), Some(3));
                assert_eq!(
                    row.copy_status.as_deref(),
                    Some(if failed { "failed" } else { "landed" })
                );
            }
            assert_eq!(server.count("sendTransaction"), 0);
        }
    }
}
