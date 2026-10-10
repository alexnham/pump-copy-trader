mod diagnostics;
mod laserstream;
mod lookup_cache;
mod payload;
mod preconfirmation;
mod recovery;

use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use crate::{domain::ObservedTransaction, error::Result};

pub use laserstream::{LaserstreamSource, check_connection};
pub(crate) use lookup_cache::LookupCache;
pub use preconfirmation::PreconfirmationSource;
pub use recovery::RecoveryClient;

#[async_trait]
pub trait SignalSource: Send + Sync {
    async fn run(&self, output: Sender<QueuedObservation>) -> Result<()>;
}

#[derive(Debug)]
pub struct QueuedObservation {
    pub observed: ObservedTransaction,
    pub received_at: std::time::Instant,
    pub queued_at: std::time::Instant,
    pub payload_decode_us: u64,
    pub observation_enqueue_us: u64,
    pub database_timings: crate::storage::DatabaseTimings,
}

#[cfg(test)]
pub(crate) use payload::decode_update as benchmark_decode_update;
