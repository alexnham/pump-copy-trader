use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tracing::info;

use crate::{
    config::ExecutionTarget,
    domain::{
        AssetId, AttemptStatus, ObservedTransaction, SignalOrigin, SkipReason, TradeIntent,
        UnsupportedReason,
    },
    error::{CopyTraderError, Result},
    storage::StatusRow,
};

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    timings: Option<super::DatabaseTimings>,
    journal: Option<std::sync::Arc<super::journal::Journal>>,
}

impl Store {
    pub async fn nonce_was_used(&self, account: &str, nonce: &str) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM nonce_uses WHERE account=? AND nonce=?",
        )
        .bind(account)
        .bind(nonce)
        .fetch_one(&self.pool)
        .await?
            > 0)
    }

    /// Wait for earlier observation/admission writes before committing fan-out.
    pub async fn persist_fanout(
        &self,
        source: &str,
        account: &str,
        nonce: &str,
        variants: &[(String, Vec<u8>, String)],
        simulation: &str,
    ) -> Result<()> {
        if let Some(journal) = &self.journal {
            let (tx, rx) = tokio::sync::oneshot::channel();
            journal.enqueue(Box::pin(async move {
                let _ = tx.send(());
                Ok(())
            }))?;
            rx.await
                .map_err(|_| CopyTraderError::Storage("journal barrier failed".into()))?;
        }
        let first = variants
            .first()
            .ok_or_else(|| CopyTraderError::Storage("empty fanout".into()))?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO nonce_uses(account,nonce,source_signature) VALUES(?,?,?)")
            .bind(account)
            .bind(nonce)
            .bind(source)
            .execute(&mut *tx)
            .await?;
        for (signature, bytes, route) in variants {
            sqlx::query("INSERT INTO copy_variants(source_signature,local_signature,signed_transaction,route_name) VALUES(?,?,?,?) ON CONFLICT(source_signature,local_signature) DO NOTHING")
                .bind(source).bind(signature).bind(bytes).bind(route).execute(&mut *tx).await?;
        }
        let updated = sqlx::query("UPDATE copy_attempts SET local_signature=?,signed_transaction=?,simulation_json=?,status='submitting',updated_at=unixepoch() WHERE source_signature=? AND status='prepared'")
            .bind(&first.0).bind(&first.1).bind(simulation).bind(source).execute(&mut *tx).await?;
        if updated.rows_affected() != 1 {
            return Err(CopyTraderError::Storage(
                "fanout attempt was not prepared".into(),
            ));
        }
        tx.commit().await?;
        if let Some(journal) = &self.journal {
            journal.submitted(source)?;
        }
        Ok(())
    }

    pub async fn variant_signatures(&self, source: &str) -> Result<Vec<String>> {
        Ok(
            sqlx::query_scalar(
                "SELECT local_signature FROM copy_variants WHERE source_signature=?",
            )
            .bind(source)
            .fetch_all(&self.pool)
            .await?,
        )
    }
    pub async fn select_variant(&self, source: &str, signature: &str) -> Result<()> {
        sqlx::query("UPDATE copy_attempts SET local_signature=?, signed_transaction=(SELECT signed_transaction FROM copy_variants WHERE source_signature=? AND local_signature=?) WHERE source_signature=? AND EXISTS(SELECT 1 FROM copy_variants WHERE source_signature=? AND local_signature=?)")
            .bind(signature).bind(source).bind(signature).bind(source).bind(source).bind(signature).execute(&self.pool).await?;
        Ok(())
    }

    fn persistence_store(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            timings: None,
            journal: None,
        }
    }

    pub(crate) fn has_background_journal(&self) -> bool {
        self.journal.is_some()
    }

    pub(crate) async fn background_journal(&self) -> Result<(Self, super::journal::JournalWriter)> {
        let attempts: Vec<String> =
            sqlx::query_scalar("SELECT source_signature FROM copy_attempts")
                .fetch_all(&self.pool)
                .await?;
        let unsupported: Vec<String> = sqlx::query_scalar(
            "SELECT signature FROM source_transactions WHERE status = 'unsupported'",
        )
        .fetch_all(&self.pool)
        .await?;
        let submitted: Vec<String> = sqlx::query_scalar("SELECT source_signature FROM copy_attempts WHERE local_signature IS NOT NULL OR status NOT IN ('prepared', 'unsupported')").fetch_all(&self.pool).await?;
        let (journal, writer) = super::journal::Journal::start(attempts, unsupported, submitted);
        let mut store = self.clone();
        store.journal = Some(journal);
        Ok((store, writer))
    }

    pub(super) async fn pending_transaction_gaps(
        &self,
    ) -> Result<Vec<super::transaction_gap::GapAttempt>> {
        Ok(sqlx::query_as("SELECT a.id, a.source_signature, a.local_signature, s.slot AS source_slot, a.landed_slot FROM copy_attempts a JOIN source_transactions s ON s.signature=a.source_signature WHERE a.execution_target='mainnet' AND a.landed_slot IS NOT NULL AND a.local_signature IS NOT NULL AND a.transaction_gap IS NULL AND (a.gap_checked_at IS NULL OR a.gap_checked_at < unixepoch()-300) ORDER BY a.gap_checked_at IS NOT NULL, a.id DESC LIMIT 4")
            .fetch_all(&self.pool).await?)
    }

    pub(super) async fn record_transaction_gap(
        &self,
        row: &super::transaction_gap::GapAttempt,
        gap: Option<i64>,
    ) -> Result<()> {
        sqlx::query("UPDATE copy_attempts SET transaction_gap=?, gap_checked_at=unixepoch() WHERE id=? AND local_signature=? AND landed_slot=? AND transaction_gap IS NULL")
            .bind(gap).bind(row.id).bind(&row.local_signature).bind(row.landed_slot).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn record_observation(&self, observed: &ObservedTransaction) -> Result<bool> {
        if let Some(journal) = &self.journal {
            if journal.is_unsupported(&observed.signature.to_string())? {
                return Ok(false);
            }
            let store = self.persistence_store();
            let observed = observed.clone();
            journal.enqueue(Box::pin(async move {
                store
                    .record_observation_persist(&observed)
                    .await
                    .map(|_| ())
            }))?;
            return Ok(true);
        }
        self.record_observation_persist(observed).await
    }
    pub async fn mark_intent(&self, signature: &str, intent: &TradeIntent) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let signature = signature.to_owned();
            let intent = intent.clone();
            journal.enqueue(Box::pin(async move {
                store
                    .mark_intent_persist(&signature, &intent)
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.mark_intent_persist(signature, intent).await
    }
    pub async fn mark_decoded(&self, signature: &str, dex: &str, pool: &str) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let signature = signature.to_owned();
            let dex = dex.to_owned();
            let pool = pool.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .mark_decoded_persist(&signature, &dex, &pool)
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.mark_decoded_persist(signature, dex, pool).await
    }
    pub async fn mark_skipped(&self, signature: &str, reason: SkipReason) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let signature = signature.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .mark_skipped_persist(&signature, reason)
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.mark_skipped_persist(signature, reason).await
    }
    pub async fn mark_unsupported(
        &self,
        signature: &str,
        reason: UnsupportedReason,
        message: &str,
    ) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.unsupported(signature)?;
            let store = self.persistence_store();
            let signature = signature.to_owned();
            let message = message.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .mark_unsupported_persist(&signature, reason, &message)
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.mark_unsupported_persist(signature, reason, message)
            .await
    }
    pub async fn reserve_attempt(
        &self,
        source_signature: &str,
        target: ExecutionTarget,
        input_amount: u64,
        minimum_output: u64,
    ) -> Result<bool> {
        if let Some(journal) = &self.journal {
            if !journal.claim(source_signature)? {
                return Ok(false);
            }
            let store = self.persistence_store();
            let source_signature = source_signature.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .reserve_attempt_persist(
                        &source_signature,
                        target,
                        input_amount,
                        minimum_output,
                    )
                    .await
                    .map(|_| ())
            }))?;
            return Ok(true);
        }
        self.reserve_attempt_persist(source_signature, target, input_amount, minimum_output)
            .await
    }
    pub async fn persist_signed(
        &self,
        source_signature: &str,
        local_signature: &str,
        signed_transaction: &[u8],
        simulation_json: &str,
    ) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.submitted(source_signature)?;
            let store = self.persistence_store();
            let source_signature = source_signature.to_owned();
            let local_signature = local_signature.to_owned();
            let signed_transaction = signed_transaction.to_vec();
            let simulation_json = simulation_json.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .persist_signed_persist(
                        &source_signature,
                        &local_signature,
                        &signed_transaction,
                        &simulation_json,
                    )
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.persist_signed_persist(
            source_signature,
            local_signature,
            signed_transaction,
            simulation_json,
        )
        .await
    }
    pub async fn record_landed_slot(&self, source_signature: &str, slot: u64) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let source_signature = source_signature.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .record_landed_slot_persist(&source_signature, slot)
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.record_landed_slot_persist(source_signature, slot)
            .await
    }
    pub async fn update_attempt(
        &self,
        source_signature: &str,
        status: AttemptStatus,
        error: Option<&str>,
    ) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let source_signature = source_signature.to_owned();
            let error = error.map(str::to_owned);
            journal.enqueue(Box::pin(async move {
                store
                    .update_attempt_persist(&source_signature, status, error.as_deref())
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.update_attempt_persist(source_signature, status, error)
            .await
    }
    pub async fn update_cursor(&self, wallet: &str, signature: &str, slot: u64) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let wallet = wallet.to_owned();
            let signature = signature.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .update_cursor_persist(&wallet, &signature, slot)
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.update_cursor_persist(wallet, signature, slot).await
    }
    pub async fn mark_route(
        &self,
        signature: &str,
        dex: &str,
        pool: &str,
        minimum_output: u64,
        quoted_output: u64,
        route_latency_ms: u64,
    ) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let signature = signature.to_owned();
            let dex = dex.to_owned();
            let pool = pool.to_owned();
            journal.enqueue(Box::pin(async move {
                store
                    .mark_route_persist(
                        &signature,
                        &dex,
                        &pool,
                        minimum_output,
                        quoted_output,
                        route_latency_ms,
                    )
                    .await
                    .map(|_| ())
            }))?;
            return Ok(());
        }
        self.mark_route_persist(
            signature,
            dex,
            pool,
            minimum_output,
            quoted_output,
            route_latency_ms,
        )
        .await
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|error| {
                CopyTraderError::Storage(format!("database migration failed: {error}"))
            })?;
        info!("storage ready");
        Ok(Self {
            pool,
            timings: None,
            journal: None,
        })
    }

    pub fn with_timings(&self, timings: super::DatabaseTimings) -> Self {
        Self {
            pool: self.pool.clone(),
            timings: Some(timings),
            journal: self.journal.clone(),
        }
    }

    pub(super) async fn record_observation_persist(
        &self,
        observed: &ObservedTransaction,
    ) -> Result<bool> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("record_observation"));
        let now = unix_timestamp()?;
        let slot = i64::try_from(observed.slot)
            .map_err(|_| CopyTraderError::Storage("slot exceeds SQLite INTEGER".to_owned()))?;
        let status = match observed.origin {
            SignalOrigin::Live | SignalOrigin::Preconfirmation => AttemptStatus::ObservedLive,
            SignalOrigin::Recovery => AttemptStatus::MissedOffline,
        };
        let result = sqlx::query(
            r#"
            INSERT INTO source_transactions
                (signature, slot, block_time, origin, status, raw_payload, observed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signature) DO UPDATE SET
                slot = CASE WHEN excluded.origin = 'live' THEN excluded.slot ELSE source_transactions.slot END,
                block_time = COALESCE(excluded.block_time, source_transactions.block_time),
                origin = CASE
                    WHEN excluded.origin = 'live' THEN 'live'
                    ELSE source_transactions.origin
                END,
                status = CASE
                    WHEN excluded.origin IN ('live', 'preconfirmation') AND source_transactions.status = 'missed_offline'
                        THEN 'observed_live'
                    ELSE source_transactions.status
                END,
                raw_payload = COALESCE(source_transactions.raw_payload, excluded.raw_payload),
                updated_at = excluded.updated_at
            WHERE source_transactions.status != 'unsupported'
            "#,
        )
        .bind(observed.signature.to_string())
        .bind(slot)
        .bind(observed.block_time)
        .bind(observed.origin.as_str())
        .bind(status.as_str())
        .bind(&observed.raw_payload)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn record_recovered_signature(&self, signature: &str, slot: u64) -> Result<()> {
        let now = unix_timestamp()?;
        let slot = i64::try_from(slot)
            .map_err(|_| CopyTraderError::Storage("slot exceeds SQLite INTEGER".to_owned()))?;
        sqlx::query(
            r#"
            INSERT INTO source_transactions
                (signature, slot, origin, status, skip_reason, observed_at, updated_at)
            VALUES (?, ?, 'recovery', 'missed_offline', ?, ?, ?)
            ON CONFLICT(signature) DO NOTHING
            "#,
        )
        .bind(signature)
        .bind(slot)
        .bind(SkipReason::RecoveredOffline.as_json())
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn mark_decoded_persist(
        &self,
        signature: &str,
        dex: &str,
        pool: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE source_transactions SET status = 'decoded', dex = ?, pool = ?, updated_at = ? WHERE signature = ?",
        )
        .bind(dex)
        .bind(pool)
        .bind(unix_timestamp()?)
        .bind(signature)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn mark_intent_persist(
        &self,
        signature: &str,
        intent: &TradeIntent,
    ) -> Result<()> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("mark_intent"));
        sqlx::query(
            r#"
            UPDATE source_transactions SET
                status = 'decoded', input_asset = ?, output_asset = ?,
                observed_input_amount = ?, observed_output_amount = ?, updated_at = ?
            WHERE signature = ? AND status != 'unsupported'
            "#,
        )
        .bind(asset_string(intent.input_asset))
        .bind(asset_string(intent.output_asset))
        .bind(intent.source_input_amount.to_string())
        .bind(intent.source_output_amount.to_string())
        .bind(unix_timestamp()?)
        .bind(signature)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn mark_route_persist(
        &self,
        signature: &str,
        dex: &str,
        pool: &str,
        minimum_output: u64,
        quoted_output: u64,
        route_latency_ms: u64,
    ) -> Result<()> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("mark_route"));
        let latency = i64::try_from(route_latency_ms).map_err(|_| {
            CopyTraderError::Storage("route latency exceeds SQLite INTEGER".to_owned())
        })?;
        let now = unix_timestamp()?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE source_transactions SET dex = ?, pool = ?, updated_at = ? WHERE signature = ?",
        )
        .bind(dex)
        .bind(pool)
        .bind(now)
        .bind(signature)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE copy_attempts SET minimum_output = ?, quoted_output = ?, route_latency_ms = ?, updated_at = ? WHERE source_signature = ?",
        )
        .bind(minimum_output.to_string())
        .bind(quoted_output.to_string())
        .bind(latency)
        .bind(now)
        .bind(signature)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn mark_skipped_persist(
        &self,
        signature: &str,
        reason: SkipReason,
    ) -> Result<()> {
        let timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("mark_skipped"));
        let result = sqlx::query(
            "UPDATE source_transactions SET status = 'skipped', skip_reason = ?, updated_at = ? WHERE signature = ? AND status != 'unsupported'",
        )
        .bind(reason.as_json())
        .bind(unix_timestamp()?)
        .bind(signature)
        .execute(&self.pool)
        .await?;
        drop(timer);
        if result.rows_affected() > 0 {
            info!(source_signature = signature, skip_reason = ?reason, "copy skipped");
        }
        Ok(())
    }

    pub async fn is_unsupported(&self, signature: &str) -> Result<bool> {
        if let Some(journal) = &self.journal {
            return journal.is_unsupported(signature);
        }
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM source_transactions WHERE signature = ? AND status = 'unsupported'"
        ).bind(signature).fetch_one(&self.pool).await? > 0)
    }

    pub(super) async fn mark_unsupported_persist(
        &self,
        signature: &str,
        reason: UnsupportedReason,
        message: &str,
    ) -> Result<()> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("mark_unsupported"));
        let detail =
            serde_json::to_string(&serde_json::json!({"code": reason, "message": message}))?;
        let now = unix_timestamp()?;
        let mut transaction = self.pool.begin().await?;
        // Submitted attempts cannot be reclassified as an unsupported source.
        let submitted: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM copy_attempts WHERE source_signature = ? AND (local_signature IS NOT NULL OR status NOT IN ('prepared', 'unsupported'))"
        ).bind(signature).fetch_one(&mut *transaction).await?;
        if submitted > 0 {
            return Ok(());
        }
        sqlx::query("UPDATE source_transactions SET status = 'unsupported', skip_reason = ?, updated_at = ? WHERE signature = ?")
            .bind(&detail).bind(now).bind(signature).execute(&mut *transaction).await?;
        sqlx::query("UPDATE copy_attempts SET status = 'unsupported', error = ?, updated_at = ? WHERE source_signature = ? AND local_signature IS NULL AND status IN ('prepared', 'unsupported')")
            .bind(&detail).bind(now).bind(signature).execute(&mut *transaction).await?;
        transaction.commit().await?;
        info!(
            source_signature = signature,
            ?reason,
            message,
            "copy unsupported"
        );
        Ok(())
    }

    pub(super) async fn reserve_attempt_persist(
        &self,
        source_signature: &str,
        target: ExecutionTarget,
        input_amount: u64,
        minimum_output: u64,
    ) -> Result<bool> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("reserve_attempt"));
        let now = unix_timestamp()?;
        let result = sqlx::query(
            r#"
            INSERT OR IGNORE INTO copy_attempts
                (source_signature, execution_target, input_amount, minimum_output, status, created_at, updated_at)
            SELECT ?, ?, ?, ?, 'prepared', ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM source_transactions WHERE signature = ? AND status = 'unsupported')
            "#,
        )
        .bind(source_signature)
        .bind(target.as_str())
        .bind(input_amount.to_string())
        .bind(minimum_output.to_string())
        .bind(now)
        .bind(now)
        .bind(source_signature)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub(super) async fn persist_signed_persist(
        &self,
        source_signature: &str,
        local_signature: &str,
        signed_transaction: &[u8],
        simulation_json: &str,
    ) -> Result<()> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("persist_signed"));
        sqlx::query(
            r#"
            UPDATE copy_attempts SET
                local_signature = ?, signed_transaction = ?, simulation_json = ?,
                status = 'submitting', updated_at = ?
            WHERE source_signature = ? AND status = 'prepared'
            "#,
        )
        .bind(local_signature)
        .bind(signed_transaction)
        .bind(simulation_json)
        .bind(unix_timestamp()?)
        .bind(source_signature)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn record_landed_slot_persist(
        &self,
        source_signature: &str,
        slot: u64,
    ) -> Result<()> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("record_landed_slot"));
        let slot = i64::try_from(slot).map_err(|_| {
            CopyTraderError::Storage("landed slot exceeds SQLite INTEGER".to_owned())
        })?;
        sqlx::query(
            "UPDATE copy_attempts SET landed_slot = ?, updated_at = ? WHERE source_signature = ?",
        )
        .bind(slot)
        .bind(unix_timestamp()?)
        .bind(source_signature)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn update_attempt_persist(
        &self,
        source_signature: &str,
        status: AttemptStatus,
        error: Option<&str>,
    ) -> Result<()> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("update_attempt"));
        sqlx::query(
            "UPDATE copy_attempts SET status = ?, error = ?, updated_at = ? WHERE source_signature = ?",
        )
        .bind(status.as_str())
        .bind(error)
        .bind(unix_timestamp()?)
        .bind(source_signature)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn update_cursor_persist(
        &self,
        wallet: &str,
        signature: &str,
        slot: u64,
    ) -> Result<()> {
        let slot = i64::try_from(slot)
            .map_err(|_| CopyTraderError::Storage("slot exceeds SQLite INTEGER".to_owned()))?;
        sqlx::query(
            r#"
            INSERT INTO stream_cursor
                (wallet, last_signature, last_slot, connection_generation, updated_at)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                last_signature = excluded.last_signature,
                last_slot = excluded.last_slot,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(wallet)
        .bind(signature)
        .bind(slot)
        .bind(unix_timestamp()?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn cursor(&self, wallet: &str) -> Result<Option<(String, u64)>> {
        let row: Option<(Option<String>, Option<i64>)> =
            sqlx::query_as("SELECT last_signature, last_slot FROM stream_cursor WHERE wallet = ?")
                .bind(wallet)
                .fetch_optional(&self.pool)
                .await?;
        row.and_then(|(signature, slot)| signature.zip(slot))
            .map(|(signature, slot)| {
                let slot = u64::try_from(slot).map_err(|_| {
                    CopyTraderError::Storage("stored cursor slot is negative".to_owned())
                })?;
                Ok((signature, slot))
            })
            .transpose()
    }

    pub async fn record_timings(&self, signature: &str, timings: &str) -> Result<()> {
        let _timer = self
            .timings
            .as_ref()
            .map(|timings| timings.start("record_timings"));
        sqlx::query("UPDATE source_transactions SET timings_json = ? WHERE signature = ?")
            .bind(timings)
            .bind(signature)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn record_timing_batch(&self, records: &[(String, String)]) -> Result<()> {
        if let Some(journal) = &self.journal {
            let store = self.persistence_store();
            let records = records.to_vec();
            return journal.enqueue(Box::pin(async move {
                for (signature, json) in records {
                    store.record_timings(&signature, &json).await?;
                }
                Ok(())
            }));
        }

        let mut transaction = self.pool.begin().await?;
        for (signature, timings) in records {
            sqlx::query("UPDATE source_transactions SET timings_json = ? WHERE signature = ?")
                .bind(timings)
                .bind(signature)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn status(&self, limit: u32) -> Result<Vec<StatusRow>> {
        let rows = sqlx::query_as::<_, StatusRow>(
            r#"
            SELECT
                s.signature,
                s.slot,
                s.status AS source_status,
                s.dex,
                s.pool,
                a.execution_target,
                a.status AS copy_status,
                a.local_signature,
                a.landed_slot,
                COALESCE(a.error, s.skip_reason) AS error,
                s.timings_json
            FROM source_transactions s
            LEFT JOIN copy_attempts a ON a.source_signature = s.signature
            ORDER BY s.slot DESC, s.observed_at DESC
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn unresolved_attempts(
        &self,
        target: ExecutionTarget,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT source_signature, local_signature
            FROM copy_attempts
            WHERE execution_target = ?
              AND status IN ('submitting', 'unknown')
              AND local_signature IS NOT NULL
            "#,
        )
        .bind(target.as_str())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn mark_unjournaled_attempts_unknown(&self) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE copy_attempts
            SET status = 'unknown', error = 'interrupted before signature persistence; submission outcome unknown', updated_at = ?
            WHERE status = 'prepared' AND local_signature IS NULL
            "#,
        )
        .bind(unix_timestamp()?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

fn unix_timestamp() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CopyTraderError::Storage(format!("system clock is before epoch: {error}"))
        })?;
    i64::try_from(duration.as_secs())
        .map_err(|_| CopyTraderError::Storage("timestamp exceeds SQLite INTEGER".to_owned()))
}

fn asset_string(asset: AssetId) -> String {
    match asset {
        AssetId::NativeSol => "native_sol".to_owned(),
        AssetId::Token(mint) => mint.to_string(),
    }
}

#[cfg(test)]
mod timing_tests {
    use super::*;
    use crate::storage::DatabaseTimings;
    use std::time::Duration;

    #[tokio::test]
    async fn wal_keeps_readers_available_during_a_write_and_preserves_durability() {
        let directory = tempfile::tempdir().expect("directory");
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("journal.sqlite").display()
        );
        let store = Store::connect(&url).await.expect("store");
        store
            .record_recovered_signature("source", 42)
            .await
            .expect("source");
        let mut connections = Vec::new();
        for _ in 0..5 {
            let mut connection = store.pool.acquire().await.expect("connection");
            let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
                .fetch_one(&mut *connection)
                .await
                .expect("journal");
            let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
                .fetch_one(&mut *connection)
                .await
                .expect("durability");
            assert_eq!(journal, "wal");
            assert_eq!(synchronous, 2);
            connections.push(connection);
        }
        drop(connections);
        let mut transaction = store.pool.begin().await.expect("transaction");
        sqlx::query(
            "UPDATE source_transactions SET timings_json = '{}' WHERE signature = 'source'",
        )
        .execute(&mut *transaction)
        .await
        .expect("write lock");
        let rows = tokio::time::timeout(std::time::Duration::from_secs(1), store.status(1))
            .await
            .expect("reader blocked by writer")
            .expect("rows");
        assert!(rows[0].timings_json.is_none());
        transaction.commit().await.expect("commit");
        store.pool.close().await;
        let reopened = Store::connect(&url).await.expect("reopen");
        assert_eq!(
            reopened.status(1).await.expect("rows")[0]
                .timings_json
                .as_deref(),
            Some("{}")
        );
    }

    #[tokio::test]
    async fn operation_timer_includes_pool_wait_and_records_failed_calls() {
        let base = Store::connect("sqlite::memory:").await.expect("store");
        base.record_recovered_signature("source", 42)
            .await
            .expect("source");
        let timings = DatabaseTimings::default();
        let measured = base.with_timings(timings.clone());
        let mut held = Vec::new();
        for _ in 0..5 {
            held.push(base.pool.acquire().await.expect("connection"));
        }
        let writer = measured.clone();
        let task =
            tokio::spawn(
                async move { writer.mark_skipped("source", SkipReason::StaleSignal).await },
            );
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!task.is_finished());
        drop(held.pop());
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("timeout")
            .expect("task")
            .expect("write");
        drop(held);
        let snapshot = timings.snapshot();
        assert_eq!(snapshot["mark_skipped"].calls, 1);
        assert!(snapshot["mark_skipped"].elapsed_us >= 20_000);
        assert!(
            measured
                .record_landed_slot("source", u64::MAX)
                .await
                .is_err()
        );
        assert_eq!(timings.snapshot()["record_landed_slot"].calls, 1);
    }

    #[tokio::test]
    async fn scoped_metrics_aggregate_calls_and_do_not_leak_to_other_attempts() {
        let base = Store::connect("sqlite::memory:").await.expect("store");
        base.record_recovered_signature("first", 42)
            .await
            .expect("source");
        base.record_recovered_signature("second", 43)
            .await
            .expect("source");
        let first = DatabaseTimings::default();
        let second = DatabaseTimings::default();
        let store = base.with_timings(first.clone());
        store
            .reserve_attempt("first", ExecutionTarget::Mainnet, 100, 90)
            .await
            .expect("reserve");
        store
            .mark_route("first", "pump_swap", "pool", 90, 100, 12)
            .await
            .expect("route");
        store
            .update_attempt("first", AttemptStatus::Failed, Some("first"))
            .await
            .expect("update");
        store
            .update_attempt("first", AttemptStatus::Failed, Some("second"))
            .await
            .expect("update");
        base.with_timings(second.clone())
            .mark_skipped("second", SkipReason::StaleSignal)
            .await
            .expect("skip");
        let snapshot = first.snapshot();
        assert_eq!(snapshot["reserve_attempt"].calls, 1);
        assert_eq!(snapshot["mark_route"].calls, 1);
        assert_eq!(snapshot["update_attempt"].calls, 2);
        assert!(!snapshot.contains_key("mark_skipped"));
        assert_eq!(second.snapshot().len(), 1);
        let stored: (String,String,String) = sqlx::query_as("SELECT s.dex,a.minimum_output,a.quoted_output FROM source_transactions s JOIN copy_attempts a ON a.source_signature=s.signature WHERE s.signature='first'").fetch_one(&base.pool).await.expect("committed route");
        assert_eq!(
            stored,
            ("pump_swap".to_owned(), "90".to_owned(), "100".to_owned())
        );
    }
}

#[cfg(test)]
mod background_tests {
    use super::*;
    use crate::domain::TransactionMeta;
    use solana_sdk::{signature::Signature, transaction::VersionedTransaction};

    #[tokio::test]
    async fn blocked_database_does_not_block_claims_or_journal_enqueue() {
        let base = Store::connect("sqlite::memory:").await.expect("store");
        let (store, mut writer) = base.background_journal().await.expect("writer");
        let mut connections = Vec::new();
        for _ in 0..5 {
            connections.push(base.pool.acquire().await.expect("connection"));
        }
        let observed = ObservedTransaction {
            signature: Signature::default(),
            slot: 42,
            block_time: None,
            origin: SignalOrigin::Live,
            transaction: VersionedTransaction::default(),
            meta: TransactionMeta::default(),
            raw_payload: "{}".into(),
            received_bytes: 2,
        };
        let signature = observed.signature.to_string();
        tokio::time::timeout(std::time::Duration::from_millis(100), async {
            assert!(
                store
                    .record_observation(&observed)
                    .await
                    .expect("observation")
            );
            assert!(
                store
                    .reserve_attempt(&signature, ExecutionTarget::Mainnet, 100, 90)
                    .await
                    .expect("claim")
            );
            assert!(
                !store
                    .reserve_attempt(&signature, ExecutionTarget::Mainnet, 100, 90)
                    .await
                    .expect("duplicate")
            );
            store
                .persist_signed(&signature, "local", &[1, 2], "{}")
                .await
                .expect("signed");
            store
                .record_landed_slot(&signature, 43)
                .await
                .expect("slot");
            store
                .update_attempt(&signature, AttemptStatus::Landed, None)
                .await
                .expect("landed");
            store
                .update_cursor("wallet", &signature, 42)
                .await
                .expect("cursor");
            store
                .record_timing_batch(&[(signature.clone(), "{}".into())])
                .await
                .expect("timings");
        })
        .await
        .expect("enqueue must not wait for pool");
        drop(connections);
        drop(store);
        writer.wait().await.expect("drain");
        let rows = base.status(10).await.expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].copy_status.as_deref(), Some("landed"));
        assert_eq!(rows[0].timings_json.as_deref(), Some("{}"));
        let (restarted, mut writer) = base.background_journal().await.expect("restart");
        assert!(
            !restarted
                .reserve_attempt(&signature, ExecutionTarget::Mainnet, 100, 90)
                .await
                .expect("persisted duplicate")
        );
        drop(restarted);
        writer.wait().await.expect("drain");
    }
}

#[cfg(test)]
mod fanout_tests {
    use super::*;
    #[tokio::test]
    async fn fanout_commit_is_atomic_and_nonce_reuse_is_rejected() {
        let store = Store::connect("sqlite::memory:").await.unwrap();
        let (journal, mut writer) = store.background_journal().await.unwrap();
        journal
            .record_recovered_signature("first", 42)
            .await
            .unwrap();
        journal
            .reserve_attempt("first", ExecutionTarget::Mainnet, 10, 9)
            .await
            .unwrap();
        let variants = vec![
            ("a".into(), vec![1], "route-a".into()),
            ("b".into(), vec![2], "route-b".into()),
            ("a".into(), vec![1], "same-wire-other-route".into()),
        ];
        journal
            .persist_fanout("first", "account", "nonce", &variants, "{}")
            .await
            .unwrap();
        assert!(store.nonce_was_used("account", "nonce").await.unwrap());
        assert_eq!(store.variant_signatures("first").await.unwrap().len(), 2);
        store.select_variant("first", "b").await.unwrap();
        let row: (String, Vec<u8>) = sqlx::query_as("SELECT local_signature,signed_transaction FROM copy_attempts WHERE source_signature='first'").fetch_one(&store.pool).await.unwrap();
        assert_eq!(row, ("b".into(), vec![2]));
        store
            .record_recovered_signature("second", 43)
            .await
            .unwrap();
        store
            .reserve_attempt("second", ExecutionTarget::Mainnet, 10, 9)
            .await
            .unwrap();
        assert!(
            store
                .persist_fanout("second", "account", "nonce", &variants, "{}")
                .await
                .is_err()
        );
        assert!(store.variant_signatures("second").await.unwrap().is_empty());
        assert!(
            store
                .persist_fanout("missing", "account", "fresh", &variants, "{}")
                .await
                .is_err()
        );
        assert!(!store.nonce_was_used("account", "fresh").await.unwrap());
        drop(journal);
        writer.wait().await.unwrap();
    }
}
