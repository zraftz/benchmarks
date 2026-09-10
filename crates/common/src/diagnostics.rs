//! Optional shared stage histograms. Use separate diagnostic and timing runs.
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Debug, Default, Serialize)]
pub struct Metric {
    samples: u64,
    total: u64,
    max: u64,
    buckets_log2: Vec<u64>,
}
#[derive(Clone, Debug, Default)]
pub struct Diagnostics {
    enabled: bool,
    metrics: Arc<Mutex<BTreeMap<String, Metric>>>,
}
impl Diagnostics {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Self::default()
        }
    }
    pub fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }
    pub fn elapsed(&self, name: &str, start: Option<Instant>) {
        if let Some(start) = start {
            self.observe(
                name,
                u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
        }
    }
    pub fn observe(&self, name: &str, value: u64) {
        if !self.enabled {
            return;
        }
        let mut all = self.metrics.lock().unwrap();
        let metric = all.entry(name.to_owned()).or_default();
        metric.samples += 1;
        metric.total = metric.total.saturating_add(value);
        metric.max = metric.max.max(value);
        metric.buckets_log2.resize(64, 0);
        metric.buckets_log2[63 - value.max(1).leading_zeros() as usize] += 1;
    }
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"enabled":self.enabled,"metrics":*self.metrics.lock().unwrap()})
    }
}
