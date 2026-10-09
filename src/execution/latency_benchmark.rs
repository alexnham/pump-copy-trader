// Manual, loopback-only benchmark using the production ingestion and worker path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual 1000-transaction latency benchmark"]
async fn benchmark_random_laserstream_to_sender() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .try_init();
    use helius_laserstream::{
        grpc::{
            SubscribeUpdate, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            subscribe_update::UpdateOneof,
        },
        solana::storage::confirmed_block as pb,
    };
    let metadata = std::sync::Mutex::new(std::collections::HashMap::new());
    let server = TestRpc::start(move |request| match request["method"].as_str().unwrap() {
        "getVersion" => json!({"solana-core":"3.1.0","feature-set":1}),
        "getLatestBlockhash" => json!({"context":{"slot":42},"value":{"blockhash":Hash::new_unique().to_string(),"lastValidBlockHeight":10000}}),
        "getBlockHeight" => json!(100),
        "getBalance" => json!({"context":{"slot":42},"value":100_000_000_000u64}),
        "getAccountInfo" => json!({"context":{"slot":42},"value":mint_account()}),
        "getMultipleAccounts" => json!({"context":{"slot":42},"value":[mint_account(),mint_account()]}),
        "getTokenAccountBalance" => json!({"context":{"slot":43},"value":{"amount":"1000000000","decimals":6,"uiAmount":0.001,"uiAmountString":"0.001"}}),
        "sendTransaction" => {
            let bytes = STANDARD.decode(request["params"][0].as_str().unwrap()).unwrap();
            let tx: Transaction = bincode::deserialize(&bytes).unwrap();
            let swap = tx.message.instructions.iter().find(|i| tx.message.account_keys[usize::from(i.program_id_index)] == pump_fun::PROGRAM_ID).unwrap();
            let mint = tx.message.account_keys[usize::from(swap.accounts[2])];
            metadata.lock().unwrap().insert(tx.signatures[0].to_string(), json!({"transaction":{"message":{"accountKeys":tx.message.account_keys.iter().map(ToString::to_string).collect::<Vec<_>>() }},"meta":{"err":null,"preTokenBalances":[],"postTokenBalances":[{"accountIndex":swap.accounts[5],"mint":mint.to_string(),"uiTokenAmount":{"amount":"1000000000"}}]}}));
            json!(tx.signatures[0].to_string())
        }
        "getSignatureStatuses" => json!({"context":{"slot":43},"value":[{"slot":43,"confirmations":1,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"}]}),
        "getTransaction" => metadata.lock().unwrap().get(request["params"][0].as_str().unwrap()).unwrap().clone(),
        method => panic!("unexpected RPC {method}"),
    }).await;
    let transport = HttpTransport::new(&crate::config::HttpConfig::default()).unwrap();
    let mut config = AppConfig::load(std::path::Path::new("config.example.toml")).unwrap();
    config.sizing = SizingConfig::Fixed {
        amount: "0.001".into(),
    };
    config.token_policy = TokenPolicyConfig::All {
        minimum_input: "0.000001".into(),
        maximum_input: "1".into(),
    };
    if let Ok(limit) = std::env::var("BENCH_CONCURRENT_SENDS") {
        config.execution.max_concurrent_sends = limit.parse().unwrap();
    }
    if let Ok(count) = std::env::var("BENCH_PREPARATION_WORKERS") {
        config.execution.preparation_workers = count.parse().unwrap();
    }
    let worker_count = config.execution.preparation_workers;
    let send_limit = config.execution.max_concurrent_sends;
    let mainnet = config.mainnet.as_mut().unwrap();
    mainnet.source_direct = true;
    mainnet.sender_url = server.url.clone();
    let client = Arc::new(MainnetClient::new(
        server.url.clone(),
        mainnet,
        transport.clone(),
    ));
    client.latest_blockhash().await.unwrap();
    let refreshing = client.clone();
    let refresher = tokio::spawn(async move { refreshing.keep_blockhash_fresh().await });
    let dir = tempfile::tempdir().unwrap();
    let disk = Store::connect(&format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("bench.sqlite").display()
    ))
    .await
    .unwrap();
    let (store, mut journal) = disk.background_journal().await.unwrap();
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
    // Generate the source protobufs before starting receipt timing.
    let updates = (0..1000)
        .map(|_| {
            let mut o = observation(wallet, 1);
            o.signature = Signature::new_unique();
            let m = match &o.transaction.message {
                VersionedMessage::Legacy(m) => m,
                _ => unreachable!(),
            };
            let b = &o.meta.post_token_balances.as_ref().unwrap()[0];
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot: 42,
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: o.signature.as_ref().to_vec(),
                        transaction: Some(pb::Transaction {
                            signatures: vec![o.signature.as_ref().to_vec()],
                            message: Some(pb::Message {
                                header: Some(pb::MessageHeader {
                                    num_required_signatures: m
                                        .header
                                        .num_required_signatures
                                        .into(),
                                    num_readonly_signed_accounts: m
                                        .header
                                        .num_readonly_signed_accounts
                                        .into(),
                                    num_readonly_unsigned_accounts: m
                                        .header
                                        .num_readonly_unsigned_accounts
                                        .into(),
                                }),
                                account_keys: m
                                    .account_keys
                                    .iter()
                                    .map(|k| k.to_bytes().to_vec())
                                    .collect(),
                                recent_blockhash: Hash::new_unique().to_bytes().to_vec(),
                                instructions: m
                                    .instructions
                                    .iter()
                                    .map(|i| pb::CompiledInstruction {
                                        program_id_index: i.program_id_index.into(),
                                        accounts: i.accounts.clone(),
                                        data: i.data.clone(),
                                    })
                                    .collect(),
                                ..Default::default()
                            }),
                        }),
                        meta: Some(pb::TransactionStatusMeta {
                            fee: 5000,
                            pre_balances: o.meta.pre_balances,
                            post_balances: o.meta.post_balances,
                            post_token_balances: vec![pb::TokenBalance {
                                account_index: b.account_index.into(),
                                mint: b.mint.clone(),
                                owner: b.owner.clone().unwrap(),
                                program_id: b.program_id.clone().unwrap(),
                                ui_token_amount: Some(pb::UiTokenAmount {
                                    amount: (1000
                                        + u64::from_le_bytes(
                                            o.signature.as_ref()[..8].try_into().unwrap(),
                                        ) % 100000)
                                        .to_string(),
                                    decimals: 6,
                                    ..Default::default()
                                }),
                            }],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                })),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();
    let (sender, receiver) = tokio::sync::mpsc::channel(256);
    let task = tokio::spawn(worker.run(receiver));
    let burst = std::env::var_os("BENCH_BURST").is_some();
    for update in updates {
        let received_at = Instant::now();
        let observed = crate::signal::benchmark_decode_update(update)
            .unwrap()
            .unwrap();
        let payload_decode_us = received_at.elapsed().as_micros() as u64;
        let database_timings = DatabaseTimings::default();
        let enqueue = Instant::now();
        store
            .with_timings(database_timings.clone())
            .record_observation(&observed)
            .await
            .unwrap();
        sender
            .send(QueuedObservation {
                observed,
                received_at,
                queued_at: Instant::now(),
                payload_decode_us,
                observation_enqueue_us: enqueue.elapsed().as_micros() as u64,
                database_timings,
            })
            .await
            .unwrap();
        if !burst {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    drop(sender);
    tokio::time::timeout(Duration::from_secs(120), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(store);
    journal.wait().await.unwrap();
    refresher.abort();
    let rows = disk.status(1000).await.unwrap();
    let mut outcomes = std::collections::BTreeMap::new();
    for r in &rows {
        *outcomes
            .entry(format!("{:?} {:?}", r.source_status, r.error))
            .or_insert(0) += 1;
    }
    println!("outcomes={outcomes:?}");
    assert_eq!(server.count("sendTransaction"), 1000);
    assert_eq!(rows.len(), 1000);
    assert!(
        rows.iter()
            .all(|row| row.copy_status.as_deref() == Some("landed"))
    );
    let samples = rows
        .iter()
        .map(|r| {
            serde_json::from_str::<serde_json::Value>(r.timings_json.as_ref().unwrap()).unwrap()
        })
        .collect::<Vec<_>>();
    let used_workers = samples
        .iter()
        .map(|t| t["preparation_worker_id"].as_u64().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(used_workers.len(), worker_count);
    let mode = if burst { "burst" } else { "paced" };
    std::fs::create_dir_all("benchmark-results").unwrap();
    std::fs::write(
        format!("benchmark-results/laserstream-sender-{mode}-concurrent-{send_limit}-workers-{worker_count}.json"),
        serde_json::to_string_pretty(&samples).unwrap(),
    )
    .unwrap();
    println!(
        "mode={mode}, workers={worker_count}, concurrency={send_limit}, sends={}, samples={}",
        server.count("sendTransaction"),
        samples.len()
    );
    for key in [
        "receipt_to_send_start_us",
        "payload_decode_us",
        "observation_enqueue_us",
        "queue_wait_us",
        "submission_wait_us",
        "decode_us",
        "pre_route_preparation_us",
        "route_wall_us",
        "transaction_build_us",
        "transaction_sign_us",
        "sender_request_us",
    ] {
        let mut v = samples
            .iter()
            .filter_map(|t| t[key].as_u64())
            .collect::<Vec<_>>();
        v.sort_unstable();
        if !v.is_empty() {
            println!(
                "{key}: n={} min={} p50={} p95={} p99={} max={} mean={:.1}",
                v.len(),
                v[0],
                v[(v.len() * 50).div_ceil(100) - 1],
                v[(v.len() * 95).div_ceil(100) - 1],
                v[(v.len() * 99).div_ceil(100) - 1],
                v[v.len() - 1],
                v.iter().sum::<u64>() as f64 / v.len() as f64
            );
        }
    }
}
