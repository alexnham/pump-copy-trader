use pump_copy_trader::{
    config::ExecutionTarget,
    domain::{AttemptStatus, UnsupportedReason},
    storage::Store,
};

#[tokio::test]
async fn unsupported_is_terminal_before_and_after_reservation() {
    let store = Store::connect("sqlite::memory:").await.expect("store");
    for (signature, reserved) in [("before", false), ("after", true)] {
        store
            .record_recovered_signature(signature, 42)
            .await
            .expect("observation");
        if reserved {
            assert!(
                store
                    .reserve_attempt(signature, ExecutionTarget::Mainnet, 100, 90)
                    .await
                    .expect("reserve")
            );
        }
        store
            .mark_unsupported(
                signature,
                UnsupportedReason::UnsupportedInstruction,
                "unsupported Pump layout",
            )
            .await
            .expect("unsupported");
        assert!(store.is_unsupported(signature).await.expect("terminal"));
        assert!(
            !store
                .reserve_attempt(signature, ExecutionTarget::Mainnet, 100, 90)
                .await
                .expect("no retry")
        );
    }
    assert_eq!(
        store
            .mark_unjournaled_attempts_unknown()
            .await
            .expect("recovery"),
        0
    );
    assert!(
        store
            .unresolved_attempts(ExecutionTarget::Mainnet)
            .await
            .expect("unresolved")
            .is_empty()
    );
    let rows = store.status(10).await.expect("status");
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row.source_status, "unsupported");
        assert_eq!(
            row.copy_status.as_deref(),
            if row.signature == "after" {
                Some("unsupported")
            } else {
                None
            }
        );
        assert_eq!(row.local_signature, None);
        assert_eq!(row.landed_slot, None);
        assert!(
            row.error
                .expect("reason")
                .contains("unsupported_instruction")
        );
    }
}

#[tokio::test]
async fn submitted_attempt_cannot_be_reclassified_as_unsupported() {
    let store = Store::connect("sqlite::memory:").await.expect("store");
    store
        .record_recovered_signature("submitted", 42)
        .await
        .expect("observation");
    store
        .reserve_attempt("submitted", ExecutionTarget::Mainnet, 100, 90)
        .await
        .expect("reserve");
    store
        .persist_signed("submitted", "copy-signature", &[], "{}")
        .await
        .expect("signed");
    store
        .update_attempt("submitted", AttemptStatus::Landed, None)
        .await
        .expect("landed");
    store
        .mark_unsupported(
            "submitted",
            UnsupportedReason::UnsupportedDex,
            "replayed source",
        )
        .await
        .expect("ignore");
    let row = store.status(1).await.expect("status").remove(0);
    assert_eq!(row.copy_status.as_deref(), Some("landed"));
    assert_eq!(row.local_signature.as_deref(), Some("copy-signature"));
}

#[tokio::test]
async fn repeated_observation_keeps_unsupported_terminal_and_does_not_enqueue_again() {
    use pump_copy_trader::domain::{
        ObservedTransaction, SignalOrigin, SkipReason, TransactionMeta,
    };
    let store = Store::connect("sqlite::memory:").await.expect("store");
    let observed = ObservedTransaction {
        signature: Default::default(),
        slot: 42,
        block_time: None,
        origin: SignalOrigin::Live,
        transaction: Default::default(),
        meta: TransactionMeta::default(),
        raw_payload: "{}".to_owned(),
        received_bytes: 2,
    };
    assert!(
        store
            .record_observation(&observed)
            .await
            .expect("first observation")
    );
    let signature = observed.signature.to_string();
    store
        .mark_unsupported(
            &signature,
            UnsupportedReason::UnsupportedDex,
            "outside Pump scope",
        )
        .await
        .expect("unsupported");
    assert!(
        !store
            .record_observation(&observed)
            .await
            .expect("duplicate observation")
    );
    store
        .mark_skipped(&signature, SkipReason::StaleSignal)
        .await
        .expect("queued duplicate skip");
    let row = store.status(1).await.expect("status").remove(0);
    assert_eq!(row.source_status, "unsupported");
    assert!(row.error.expect("reason").contains("outside Pump scope"));
}
