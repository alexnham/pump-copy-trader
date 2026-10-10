use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use serde::Serialize;

/// A collector scoped to one observation, shared only by its journal operations.
#[derive(Clone, Debug, Default)]
pub struct DatabaseTimings(Arc<Mutex<BTreeMap<&'static str, OperationTiming>>>);

#[derive(Clone, Debug, Default, Serialize)]
pub struct OperationTiming {
    pub elapsed_us: u64,
    pub calls: u64,
}

impl DatabaseTimings {
    pub(crate) fn start(&self, operation: &'static str) -> OperationTimer {
        OperationTimer {
            started: Instant::now(),
            operation,
            timings: self.clone(),
        }
    }

    pub fn snapshot(&self) -> BTreeMap<&'static str, OperationTiming> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn total_us(&self) -> u64 {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            // The parent covers these nested persistence phases; count it once.
            .filter(|(name, _)| {
                !matches!(
                    **name,
                    "fanout_journal_barrier"
                        | "fanout_db_begin"
                        | "fanout_db_writes"
                        | "fanout_db_commit"
                )
            })
            .fold(0_u64, |total, (_, timing)| {
                total.saturating_add(timing.elapsed_us)
            })
    }
}

pub(crate) struct OperationTimer {
    started: Instant,
    operation: &'static str,
    timings: DatabaseTimings,
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        let elapsed_us = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let mut timings = self
            .timings
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let timing = timings.entry(self.operation).or_default();
        timing.elapsed_us = timing.elapsed_us.saturating_add(elapsed_us);
        timing.calls = timing.calls.saturating_add(1);
    }
}
