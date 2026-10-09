use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{error, warn};

use super::Store;

const QUEUE_CAPACITY: usize = 256;
const BATCH_SIZE: usize = 32;

pub(crate) struct TimingWriter {
    sender: mpsc::Sender<(String, String)>,
    task: JoinHandle<()>,
}

impl TimingWriter {
    pub(crate) fn new(store: Store) -> Self {
        let (sender, mut receiver) = mpsc::channel(QUEUE_CAPACITY);
        let task = tokio::spawn(async move {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            while receiver.recv_many(&mut batch, BATCH_SIZE).await != 0 {
                if let Err(error) = store.record_timing_batch(&batch).await {
                    warn!(%error, records = batch.len(), "timing batch was not persisted; copy timings remain in logs");
                }
                batch.clear();
            }
        });
        Self { sender, task }
    }

    pub(crate) fn sender(&self) -> mpsc::Sender<(String, String)> {
        self.sender.clone()
    }

    pub(crate) fn enqueue(&self, signature: String, json: String) {
        if let Err(error) = self.sender.try_send((signature, json)) {
            let (source_signature, _) = error.into_inner();
            warn!(%source_signature, "timing queue unavailable; copy timings remain in logs");
        }
    }

    pub(crate) async fn finish(self) {
        drop(self.sender);
        if let Err(error) = self.task.await {
            error!(%error, "timing writer task failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn finish_drains_multiple_batches() {
        let store = Store::connect("sqlite::memory:").await.expect("store");
        for slot in 0..70 {
            store
                .record_recovered_signature(&slot.to_string(), slot)
                .await
                .expect("row");
        }
        let writer = TimingWriter::new(store.clone());
        for slot in 0..70 {
            writer.enqueue(slot.to_string(), format!("{{\"test\":{slot}}}"));
        }
        writer.finish().await;
        let rows = store.status(100).await.expect("rows");
        assert_eq!(rows.len(), 70);
        for row in rows {
            assert!(row.timings_json.is_some());
        }
    }

    #[tokio::test]
    async fn full_queue_does_not_wait_for_database() {
        let (sender, mut receiver) = mpsc::channel(1);
        let writer = TimingWriter {
            sender,
            task: tokio::spawn(async {}),
        };
        writer.enqueue("first".into(), "{}".into());
        writer.enqueue("second".into(), "{}".into());
        assert_eq!(receiver.try_recv().expect("first").0, "first");
        assert!(receiver.try_recv().is_err());
        writer.finish().await;
    }
}
