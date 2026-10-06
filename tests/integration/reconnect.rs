use pump_copy_trader::{domain::AttemptStatus, storage::Store};

#[tokio::test]
async fn recovered_transactions_are_terminal_without_an_attempt() {
    let directory = tempfile::tempdir().ok();
    assert!(directory.is_some());
    let Some(directory) = directory else { return };
    let database = directory.path().join("reconnect.sqlite");
    let url = format!("sqlite://{}?mode=rwc", database.display());
    let store = Store::connect(&url).await.ok();
    assert!(store.is_some());
    let Some(store) = store else { return };

    let signature = solana_sdk::signature::Signature::default().to_string();
    assert!(
        store
            .record_recovered_signature(&signature, 99)
            .await
            .is_ok()
    );
    let rows = store.status(10).await.ok();
    assert!(rows.is_some());
    let Some(rows) = rows else { return };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source_status, AttemptStatus::MissedOffline.as_str());
    assert!(rows[0].copy_status.is_none());
}
