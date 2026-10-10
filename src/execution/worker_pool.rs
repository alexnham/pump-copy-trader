impl ExecutionWorker {
    /// New preparation state, sharing wallet-wide admission and submission limits.
    fn shared_worker(&self, id: usize) -> Self {
        let mut worker = Self::new(
            self.config.clone(),
            self.signer.clone(),
            self.store.clone(),
            self.backend.clone(),
            self.token_safety.clone(),
            TransactionDecoder::new(self.config.signal.wallet),
            self.router.clone(),
        )
        .with_balance_cache(self.balance_cache.clone());
        worker.reservations = self.reservations.clone();
        worker.reservation_gate = self.reservation_gate.clone();
        worker.submission_slots = self.submission_slots.clone();
        worker.lifecycle_slots = self.lifecycle_slots.clone();
        worker.last_submitted_slot = self.last_submitted_slot.clone();
        worker.preparation_worker_id = id;
        worker
    }

    async fn run_pool(self, mut input: Receiver<QueuedObservation>) -> Result<()> {
        use std::hash::{Hash, Hasher};
        let count = self.config.execution.preparation_workers;
        // Keep the configured ingress queue as the main burst buffer; adding
        // workers should not multiply buffered inputs and their queue latency.
        let capacity = 1;
        let mut workers = JoinSet::new();
        let mut queues = Vec::with_capacity(count);
        for id in 0..count {
            let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
            queues.push(sender);
            let worker = self.shared_worker(id);
            workers.spawn(worker.run_single(receiver));
        }
        let mut outcome = Ok(());
        loop {
            tokio::select! {
                result = workers.join_next() => {
                    outcome = match result {
                        Some(Ok(Err(error))) => Err(error),
                        Some(Err(error)) => Err(CopyTraderError::Execution(format!("preparation worker failed: {error}"))),
                        _ => Err(CopyTraderError::Execution("preparation worker ended before its input closed".into())),
                    };
                    break;
                }
                queued = input.recv() => {
                    let Some(queued) = queued else { break };
                    // Both feeds and duplicates of a signature share one FIFO lane.
                    // An early preparation failure completes before processed fallback.
                    let mut hash = std::collections::hash_map::DefaultHasher::new();
                    queued.observed.signature.hash(&mut hash);
                    let index = (hash.finish() % count as u64) as usize;
                    if queues[index].send(queued).await.is_err() {
                        outcome = Err(CopyTraderError::Execution("preparation worker input closed".into()));
                        break;
                    }
                }
            }
        }
        // Close lanes, then drain every worker's submissions, settlement and timing
        // writer, including when dispatch or one worker failed.
        drop(queues);
        drop(input);
        while let Some(result) = workers.join_next().await {
            let result = match result {
                Ok(result) => result,
                Err(error) => Err(CopyTraderError::Execution(format!(
                    "preparation worker failed: {error}"
                ))),
            };
            if outcome.is_ok() {
                outcome = result;
            }
        }
        outcome
    }
}
