use serde_json::{Value, json};
use solana_sdk::signature::Signature;
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

const NAMES: [&str; 17] = [
    "subscriptions",
    "reconnects",
    "binary_received",
    "status_ignored",
    "invalid_frames",
    "lookup_cache_misses",
    "wrapper_rejections",
    "sell_rejections",
    "other_decode_rejections",
    "ready",
    "queue_full",
    "duplicate",
    "enqueued",
    "processed_first",
    "preconfirmation_first",
    "unknown_status_received",
    "failed_status_received",
];
#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum Counter {
    Subscriptions,
    Reconnects,
    BinaryReceived,
    StatusIgnored,
    InvalidFrames,
    LookupCacheMisses,
    WrapperRejections,
    SellRejections,
    OtherDecodeRejections,
    Ready,
    QueueFull,
    Duplicate,
    Enqueued,
    ProcessedFirst,
    PreconfirmationFirst,
    UnknownStatusReceived,
    FailedStatusReceived,
}

pub(crate) struct Diagnostics {
    counts: [AtomicU64; 17],
    arrivals: Mutex<HashMap<Signature, u8>>,
}
impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            arrivals: Mutex::new(HashMap::new()),
        }
    }
}
impl Diagnostics {
    pub(crate) fn increment(&self, c: Counter) {
        self.counts[c as usize].fetch_add(1, Ordering::Relaxed);
    }
    // Compare receipt order for a bounded recent window, including rejected early signals.
    pub(crate) fn observe(&self, signature: Signature, preconfirmation: bool) {
        let Ok(mut arrivals) = self.arrivals.lock() else {
            return;
        };
        let flag = if preconfirmation { 1 } else { 2 };
        if let Some(previous) = arrivals.get_mut(&signature) {
            if *previous != 3 && *previous != flag {
                self.increment(if preconfirmation {
                    Counter::ProcessedFirst
                } else {
                    Counter::PreconfirmationFirst
                });
            }
            *previous |= flag;
        } else {
            if arrivals.len() >= 4096 {
                arrivals.clear();
            }
            arrivals.insert(signature, flag);
        }
    }
    pub(crate) fn snapshot(&self) -> Value {
        let mut value = json!({});
        for (i, name) in NAMES.iter().enumerate() {
            value[*name] = json!(self.counts[i].load(Ordering::Relaxed));
        }
        value
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn receipt_order_is_counted_once_and_includes_rejected_signals() {
        let d = Diagnostics::default();
        let a = Signature::new_unique();
        let b = Signature::new_unique();
        d.observe(a, true);
        d.observe(a, false);
        d.observe(a, false);
        d.observe(a, true);
        d.observe(b, false);
        d.observe(b, true);
        assert_eq!(d.snapshot()["preconfirmation_first"], 1);
        assert_eq!(d.snapshot()["processed_first"], 1);
    }
}
