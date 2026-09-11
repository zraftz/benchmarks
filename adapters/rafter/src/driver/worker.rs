//! One ordered I/O thread, one queued request, one completion; no task per batch.
use crate::storage::{Completion, Work};
use anyhow::{anyhow, Result};
use std::{sync::mpsc, thread};
pub type Counters = Vec<(&'static str, rafter_storage::telemetry::Metric)>;
type Completed = (Completion, Counters);

pub struct Worker {
    requests: Option<mpsc::SyncSender<Box<Work>>>,
    completions: mpsc::Receiver<Completed>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Worker {
    pub fn start(diagnostics: bool) -> Result<Self> {
        let (requests, receive) = mpsc::sync_channel::<Box<Work>>(1);
        let (send, completions) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("rafter-persistence".into())
            .spawn(move || {
                rafter_storage::telemetry::set_enabled(diagnostics);
                while let Ok(work) = receive.recv() {
                    let completion = work.persist();
                    let counters = if diagnostics {
                        rafter_storage::telemetry::snapshot()
                    } else {
                        Vec::new()
                    };
                    // Never block shutdown on an abandoned or already occupied completion slot.
                    if send.try_send((completion, counters)).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self {
            requests: Some(requests),
            completions,
            thread: Some(thread),
        })
    }
    pub fn submit(&self, work: Box<Work>) -> Result<()> {
        self.requests
            .as_ref()
            .ok_or_else(|| anyhow!("persistence worker stopped"))?
            .try_send(work)
            .map_err(|_| anyhow!("persistence worker request queue unavailable"))
    }
    pub fn complete(&self) -> Result<Completed> {
        self.completions
            .recv()
            .map_err(|_| anyhow!("persistence worker stopped before completion"))
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.requests.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
