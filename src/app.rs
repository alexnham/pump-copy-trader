use std::{env, path::Path, str::FromStr, sync::Arc};

use solana_sdk::signature::{Keypair, Signature, Signer, read_keypair_file};
use tokio::{sync::mpsc, time::MissedTickBehavior};
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
    signal::{LaserstreamSource, RecoveryClient, SignalSource, check_connection},
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
        Command::Doctor => doctor(&config, &store).await,
        Command::Status { limit } => status(&store, limit).await,
    }
}

async fn run_service(config: Arc<AppConfig>, store: Store) -> Result<()> {
    config.require_live_execution()?;
    let api_key = required_env("HELIUS_API_KEY")?;
    let keypair_path = required_env("COPY_TRADER_KEYPAIR_PATH")?;
    let signer = Arc::new(load_keypair(Path::new(&keypair_path))?);
    let laserstream_endpoint = config.laserstream_endpoint(&api_key)?;
    let helius_http = config.helius_http_url(&api_key)?;
    let transport = HttpTransport::new(&config.http)?;
    let helius_slot = transport.warm(&helius_http).await?;
    info!(helius_slot, "Helius HTTP transport warmed");
    let backend = Arc::new(build_backend(&config, helius_http.clone(), transport.clone()).await?);
    if let Some(mainnet) = backend.mainnet() {
        mainnet.warm_sender().await;
    }
    resolve_uncertain(&store, &backend).await?;

    let recovery = RecoveryClient::new(
        helius_http.clone(),
        config.signal.wallet,
        store.clone(),
        transport.clone(),
    );
    let source = LaserstreamSource::new(
        laserstream_endpoint,
        api_key,
        config.signal.wallet,
        store.clone(),
        recovery,
        &config.signal.commitment,
    );
    let decoder = TransactionDecoder::new(config.signal.wallet);
    let discovery_rpc = Arc::new(transport.solana_rpc(&helius_http));
    let allowed_mints = config
        .tokens
        .iter()
        .map(|rule| rule.mint)
        .collect::<Vec<_>>();
    let router = Arc::new(Router::supported(
        discovery_rpc.clone(),
        config.routing.max_pools_per_dex,
        std::time::Duration::from_millis(config.routing.race_timeout_ms),
    ));
    let balance_cache = WalletBalanceCache::default();
    tokio::spawn(balance_cache.clone().run(
        discovery_rpc.clone(),
        signer.pubkey(),
        allowed_mints.clone(),
    ));
    if !config
        .mainnet
        .as_ref()
        .is_some_and(|mainnet| mainnet.source_direct)
    {
        let mut warm_mints = Vec::with_capacity(allowed_mints.len() + 1);
        warm_mints.push(solana_sdk::pubkey!(
            "So11111111111111111111111111111111111111112"
        ));
        warm_mints.extend(allowed_mints.iter().copied());
        let warm_configured_pairs = !allowed_mints.is_empty() && !config.allows_all_tokens();
        let warm_sol_pools = allowed_mints.is_empty() || config.allows_all_tokens();
        if warm_configured_pairs || warm_sol_pools {
            let initial_router = router.clone();
            let initial_mints = warm_mints.clone();
            tokio::spawn(async move {
                info!(
                    configured_pairs = warm_configured_pairs,
                    sol_pools = warm_sol_pools,
                    "initial route catalog warming running in background"
                );
                if warm_configured_pairs
                    && let Err(error) = initial_router.warm(&initial_mints).await
                {
                    warn!(%error, "initial configured pair warming failed");
                }
                if warm_sol_pools && let Err(error) = initial_router.warm_sol_pools().await {
                    warn!(%error, "initial SOL pool warming failed");
                }
                info!("initial route catalog warming complete");
            });
        }
        spawn_router_maintenance(
            router.clone(),
            warm_mints,
            !allowed_mints.is_empty() && !config.allows_all_tokens(),
            config.routing.pool_refresh_seconds,
        );
    }
    let worker = ExecutionWorker::new(
        config.clone(),
        signer,
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
    let source_task = tokio::spawn(async move { source.run(sender).await });
    let worker_task = tokio::spawn(async move { worker.run(receiver).await });
    info!(wallet = %compact_id(config.signal.wallet.to_string()), target = backend.label(), "copy trader running");

    let sender_warming = async {
        if let Some(mainnet) = backend.mainnet() {
            tokio::join!(mainnet.keep_sender_warm(), mainnet.keep_blockhash_fresh());
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        () = sender_warming => Ok(()),
        result = source_task => join_result("signal source", result),
        result = worker_task => join_result("execution worker", result),
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(CopyTraderError::Io)?;
            info!("shutdown requested");
            Ok(())
        }
    }
}

fn spawn_router_maintenance(
    router: Arc<Router>,
    allowed_mints: Vec<solana_sdk::pubkey::Pubkey>,
    warm_configured_pairs: bool,
    pool_refresh_seconds: u64,
) {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(pool_refresh_seconds));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            info!("periodic router maintenance started");
            if warm_configured_pairs && let Err(error) = router.warm(&allowed_mints).await {
                warn!(%error, "periodic pool catalog refresh failed");
            }
            if let Err(error) = router.warm_sol_pools().await {
                warn!(%error, "periodic SOL pool warming failed");
            }
            info!("periodic router maintenance complete");
        }
    });
}

async fn doctor(config: &AppConfig, store: &Store) -> Result<()> {
    let api_key = required_env("HELIUS_API_KEY")?;
    let keypair_path = required_env("COPY_TRADER_KEYPAIR_PATH")?;
    let signer = load_keypair(Path::new(&keypair_path))?;
    let laserstream_endpoint = config.laserstream_endpoint(&api_key)?;
    check_connection(laserstream_endpoint, &api_key).await?;
    let helius_http = config.helius_http_url(&api_key)?;
    let transport = HttpTransport::new(&config.http)?;
    let _ = transport.warm(&helius_http).await?;
    let backend = build_backend(config, helius_http.clone(), transport.clone()).await?;
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

async fn status(store: &Store, limit: u32) -> Result<()> {
    for row in store.status(limit).await? {
        println!(
            "source_slot={} copy_slot={} slot_delta={} source={} signature={} dex={} pool={} target={} copy={} local={} error={} timings={}",
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
    for (source_signature, local_signature) in store.unresolved_attempts(backend.target()).await? {
        let signature = Signature::from_str(&local_signature).map_err(|error| {
            CopyTraderError::Storage(format!("invalid stored local signature: {error}"))
        })?;
        let statuses = backend
            .rpc()
            .get_signature_statuses_with_history(&[signature])
            .await
            .map_err(|error| {
                CopyTraderError::Execution(format!("uncertain signature query failed: {error}"))
            })?;
        match statuses.value.first() {
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
                warn!(source = %compact_id(&source_signature), local = %compact_id(&local_signature), "submission remains uncertain")
            }
        }
    }
    Ok(())
}

async fn build_backend(
    config: &AppConfig,
    helius_http: url::Url,
    transport: Arc<HttpTransport>,
) -> Result<ExecutionBackend> {
    match config.execution.target {
        ExecutionTarget::Mainnet => {
            let mainnet_config = config.mainnet.as_ref().ok_or_else(|| {
                CopyTraderError::Configuration("missing [mainnet] configuration".to_owned())
            })?;
            let client = Arc::new(MainnetClient::new(helius_http, mainnet_config, transport));
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
