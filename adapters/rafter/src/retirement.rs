//! Bounded retired-log destruction and benchmark activity counters.

use rafter::RetiredLogEntries;
use rafter_runtime::{LogRetirementWorker, LogRetirementWorkerOptions};

const MAX_ENTRIES: usize = 262_144;
const MAX_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;

pub(super) struct Retirement {
    worker: Option<LogRetirementWorker>,
    accepted_batches: u64,
    accepted_entries: u64,
    inline_fallback_batches: u64,
}

impl Retirement {
    pub(super) fn start(enabled: bool) -> std::io::Result<Self> {
        let worker = enabled
            .then(|| {
                LogRetirementWorker::start(LogRetirementWorkerOptions::new(
                    MAX_ENTRIES,
                    MAX_PAYLOAD_BYTES,
                ))
            })
            .transpose()?;
        Ok(Self {
            worker,
            accepted_batches: 0,
            accepted_entries: 0,
            inline_fallback_batches: 0,
        })
    }

    pub(super) fn retire(&mut self, retired: RetiredLogEntries) {
        if retired.is_empty() {
            return;
        }
        let entries = u64::try_from(retired.len()).unwrap_or(u64::MAX);
        let Some(worker) = &self.worker else {
            self.inline(retired);
            return;
        };
        match worker.try_submit(retired) {
            Ok(()) => {
                self.accepted_batches = self.accepted_batches.saturating_add(1);
                self.accepted_entries = self.accepted_entries.saturating_add(entries);
            }
            Err(error) => self.inline(error.into_retired_entries()),
        }
    }

    fn inline(&mut self, retired: RetiredLogEntries) {
        self.inline_fallback_batches = self.inline_fallback_batches.saturating_add(1);
        drop(retired);
    }

    pub(super) fn stats(&self) -> serde_json::Value {
        let Some(worker) = &self.worker else {
            return serde_json::json!({"enabled":false});
        };
        serde_json::json!({
            "enabled":true,
            "max_inflight_entries":MAX_ENTRIES,
            "max_inflight_payload_bytes":MAX_PAYLOAD_BYTES,
            "accepted_batches":self.accepted_batches,
            "accepted_entries":self.accepted_entries,
            "inline_fallback_batches":self.inline_fallback_batches,
            "inflight_entries":worker.inflight_entries(),
            "inflight_payload_bytes":worker.inflight_payload_bytes(),
        })
    }
}
