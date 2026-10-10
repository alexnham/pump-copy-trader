use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

/// Summed elapsed time per stage, including failed and cancelled candidates.
/// Concurrent candidate stages overlap and are not a critical-path duration.
#[derive(Clone, Default)]
pub struct RouteStages(Arc<Mutex<BTreeMap<&'static str, u64>>>);

impl RouteStages {
    pub fn snapshot(&self) -> BTreeMap<&'static str, u64> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub(super) fn record_us(&self, name: &'static str, elapsed: std::time::Duration) {
        let mut values = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let value = values.entry(name).or_default();
        *value = value.saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
    }

    pub(super) fn start(&self, name: &'static str) -> StageTimer {
        StageTimer {
            stages: self.clone(),
            name,
            started: Instant::now(),
        }
    }
}

pub(super) struct StageTimer {
    stages: RouteStages,
    name: &'static str,
    started: Instant,
}

impl Drop for StageTimer {
    fn drop(&mut self) {
        let mut values = self
            .stages
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let value = values.entry(self.name).or_default();
        let elapsed = self.started.elapsed();
        *value = value.saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
        if let Some(name) = self.name.strip_suffix("_ms") {
            let name = match name {
                "route_instruction_build" => "route_instruction_build_us",
                "route_shared_preparation" => "route_shared_preparation_us",
                "transaction_build" => "transaction_build_us",
                "transaction_sign" => "transaction_sign_us",
                _ => return,
            };
            let value = values.entry(name).or_default();
            *value = value.saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
        }
    }
}
