use pump_copy_trader::{config::ExecutionTarget, storage::Store};

#[tokio::test]
async fn source_signature_can_reserve_only_one_attempt() {
    let directory = tempfile::tempdir().ok();
    assert!(directory.is_some());
    let Some(directory) = directory else { return };
    let database = directory.path().join("journal.sqlite");
    let url = format!("sqlite://{}?mode=rwc", database.display());
    let store = Store::connect(&url).await.ok();
    assert!(store.is_some());
    let Some(store) = store else { return };

    let signature = solana_sdk::signature::Signature::default().to_string();
    assert!(
        store
            .record_recovered_signature(&signature, 42)
            .await
            .is_ok()
    );
    assert_eq!(
        store
            .reserve_attempt(&signature, ExecutionTarget::Mainnet, 100, 90)
            .await
            .ok(),
        Some(true)
    );
    assert_eq!(
        store
            .reserve_attempt(&signature, ExecutionTarget::Mainnet, 100, 90)
            .await
            .ok(),
        Some(false)
    );
    assert_eq!(
        store.mark_unjournaled_attempts_unknown().await.ok(),
        Some(1)
    );
    assert_eq!(
        store
            .reserve_attempt(&signature, ExecutionTarget::Mainnet, 100, 90)
            .await
            .ok(),
        Some(false),
        "an interrupted submission must never be retried"
    );
    assert_eq!(
        store.mark_unjournaled_attempts_unknown().await.ok(),
        Some(0)
    );
    let rows = store.status(1).await.ok();
    assert_eq!(
        rows.and_then(|rows| rows.first().and_then(|row| row.copy_status.clone())),
        Some("unknown".to_owned())
    );
}

#[tokio::test]
async fn timing_migration_preserves_historical_rows_and_records_skips() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .expect("database");
    for sql in [
        include_str!("../../migrations/0001_initial.sql"),
        include_str!("../../migrations/0002_attempt_execution_target.sql"),
        include_str!("../../migrations/0003_intent_routing.sql"),
    ] {
        sqlx::raw_sql(sql).execute(&pool).await.expect("old schema");
    }
    sqlx::query("INSERT INTO source_transactions (signature,slot,origin,status,observed_at,updated_at) VALUES ('historical',42,'live','skipped',100,101)").execute(&pool).await.expect("old row");
    sqlx::raw_sql(include_str!("../../migrations/0004_copy_timings.sql"))
        .execute(&pool)
        .await
        .expect("migration");
    let row: (i64, i64, Option<String>) =
        sqlx::query_as("SELECT observed_at,updated_at,timings_json FROM source_transactions")
            .fetch_one(&pool)
            .await
            .expect("historical row");
    assert_eq!(row, (100, 101, None));

    let store = Store::connect("sqlite::memory:").await.expect("store");
    store
        .record_recovered_signature("skipped", 42)
        .await
        .expect("source");
    store
        .record_timings("skipped", r#"{"preparation_ms":12}"#)
        .await
        .expect("timings");
    let rows = store.status(1).await.expect("status");
    assert_eq!(
        rows[0].timings_json.as_deref(),
        Some(r#"{"preparation_ms":12}"#)
    );
    assert!(rows[0].copy_status.is_none());
}

#[tokio::test]
async fn landing_slot_survives_failed_reconciliation_and_has_mainnet_delta() {
    let store = Store::connect("sqlite::memory:").await.expect("store");
    for (source, target) in [("mainnet", ExecutionTarget::Mainnet)] {
        store
            .record_recovered_signature(source, 42)
            .await
            .expect("source");
        store
            .reserve_attempt(source, target, 100, 90)
            .await
            .expect("reserve");
        store.record_landed_slot(source, 45).await.expect("slot");
        store
            .update_attempt(
                source,
                pump_copy_trader::domain::AttemptStatus::Failed,
                Some("reconciliation failed"),
            )
            .await
            .expect("status");
    }
    let rows = store.status(2).await.expect("rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        assert_eq!(row.landed_slot, Some(45));
        assert_eq!(row.copy_status.as_deref(), Some("failed"));
        assert_eq!(
            row.slot_delta(),
            if row.execution_target.as_deref() == Some("mainnet") {
                Some(3)
            } else {
                None
            }
        );
    }
    assert!(store.record_landed_slot("mainnet", u64::MAX).await.is_err());
}

#[tokio::test]
async fn landing_migration_leaves_historical_attempts_unmeasured() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .expect("database");
    for sql in [
        include_str!("../../migrations/0001_initial.sql"),
        include_str!("../../migrations/0002_attempt_execution_target.sql"),
        include_str!("../../migrations/0003_intent_routing.sql"),
        include_str!("../../migrations/0004_copy_timings.sql"),
    ] {
        sqlx::raw_sql(sql).execute(&pool).await.expect("old schema");
    }
    sqlx::query("INSERT INTO source_transactions (signature,slot,origin,status,observed_at,updated_at) VALUES ('historical',42,'live','decoded',100,101)").execute(&pool).await.expect("source");
    sqlx::query("INSERT INTO copy_attempts (source_signature,status,created_at,updated_at) VALUES ('historical','landed',100,101)").execute(&pool).await.expect("copy");
    sqlx::raw_sql(include_str!("../../migrations/0005_copy_landed_slot.sql"))
        .execute(&pool)
        .await
        .expect("migration");
    let row: (String, i64, Option<i64>) =
        sqlx::query_as("SELECT status,updated_at,landed_slot FROM copy_attempts")
            .fetch_one(&pool)
            .await
            .expect("row");
    assert_eq!(row, ("landed".to_owned(), 101, None));
}
