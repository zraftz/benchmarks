//! One ordered application owner with bounded work and nonblocking completions.
//! Credits cover queued, in-progress, and completed-but-unconsumed entries.
//! The Raft owner never blocks sending work or receiving a completion.
use crate::{
    diagnostics::Diagnostics,
    model::{Applied, DurableModel, Outcome},
    Reply,
};
use anyhow::{bail, Result};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::Instant;
use tokio::sync::oneshot;

mod run;

pub const BATCH_ENTRIES: usize = 64;
pub const BATCH_BYTES: usize = 256 * 1024;
pub const QUEUE_ENTRIES: usize = 4096;
pub const QUEUE_BYTES: usize = 16 * 1024 * 1024;

pub trait ApplicationStore: Send + 'static {
    fn apply(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>>;
    fn index(&self) -> u64;
    fn stats(&self) -> serde_json::Value;
    fn dump(&self) -> serde_json::Value;
}
impl ApplicationStore for DurableModel {
    fn apply(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>> {
        self.apply(entries)
    }
    fn index(&self) -> u64 {
        self.index
    }
    fn stats(&self) -> serde_json::Value {
        self.stats()
    }
    fn dump(&self) -> serde_json::Value {
        serde_json::json!({"values":self.model.values,"applied_index":self.index})
    }
}

pub fn payload_bytes(entry: &Applied) -> usize {
    128 + entry.command.as_ref().map_or(0, |c| {
        c.client.len()
            + c.kind.len()
            + c.key.len()
            + c.value.len()
            + c.expected.as_ref().map_or(0, String::len)
    })
}
fn retained_bytes(entry: &Applied) -> usize {
    // Include the bounded result value even for a short get/CAS request.
    payload_bytes(entry)
        .saturating_mul(2)
        .saturating_add(64 * 1024)
}
#[derive(Default)]
struct Credits {
    entries: usize,
    bytes: usize,
}
struct Item {
    entry: Applied,
    queued: Option<Instant>,
}
pub struct Completion {
    pub entries: Vec<Applied>,
    pub outcomes: Vec<Option<Outcome>>,
    pub queued: Vec<Option<Instant>>,
    bytes: usize,
}
pub struct Query {
    pub reply: oneshot::Sender<Reply>,
    pub response: Reply,
    pub dump: bool,
}
enum Work {
    Apply(Vec<Item>),
    Query(Query),
}

pub struct ApplyWorker {
    tx: Option<mpsc::Sender<Work>>,
    complete: mpsc::Receiver<Result<Completion>>,
    credits: Arc<Mutex<Credits>>,
    queries: Arc<AtomicUsize>,
    durable: Arc<AtomicU64>,
    applying: Arc<AtomicU64>,
    diagnostics: Diagnostics,
    thread: Option<JoinHandle<()>>,
}
impl ApplyWorker {
    pub fn start(
        store: impl ApplicationStore,
        diagnostics: Diagnostics,
        wake: impl Fn() + Send + 'static,
    ) -> Self {
        let (tx, rx) = mpsc::channel();
        let (done, complete) = mpsc::channel();
        let credits = Arc::new(Mutex::new(Credits::default()));
        let queries = Arc::new(AtomicUsize::new(0));
        let durable = Arc::new(AtomicU64::new(store.index()));
        let applying = Arc::new(AtomicU64::new(0));
        let state = run::State {
            store,
            rx,
            done,
            queries: queries.clone(),
            durable: durable.clone(),
            applying: applying.clone(),
            diagnostics: diagnostics.clone(),
            wake,
        };
        let thread = std::thread::spawn(move || state.run());
        Self {
            tx: Some(tx),
            complete,
            credits,
            queries,
            durable,
            applying,
            diagnostics,
            thread: Some(thread),
        }
    }
    pub fn applied_index(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }
    pub fn applying_index(&self) -> u64 {
        self.applying.load(Ordering::Acquire)
    }
    pub fn available(&self) -> (usize, usize) {
        let c = self.credits.lock().unwrap();
        (QUEUE_ENTRIES - c.entries, QUEUE_BYTES - c.bytes)
    }
    pub fn has_capacity(&self) -> bool {
        let (entries, bytes) = self.available();
        entries > BATCH_ENTRIES && bytes > BATCH_BYTES
    }
    pub fn try_submit(&self, entries: Vec<Applied>) -> Result<bool> {
        if entries.is_empty() {
            return Ok(true);
        }
        if entries
            .iter()
            .any(|entry| payload_bytes(entry) > BATCH_BYTES)
        {
            bail!("application entry exceeds decoded byte cap")
        }
        let bytes = entries.iter().map(retained_bytes).sum::<usize>();
        let mut c = self.credits.lock().unwrap();
        if entries.len() > QUEUE_ENTRIES - c.entries || bytes > QUEUE_BYTES - c.bytes {
            return Ok(false);
        }
        c.entries += entries.len();
        c.bytes += bytes;
        self.diagnostics.application_dispatched(&entries);
        let items = entries
            .into_iter()
            .map(|entry| Item {
                entry,
                queued: self.diagnostics.start(),
            })
            .collect();
        if self.tx.as_ref().unwrap().send(Work::Apply(items)).is_err() {
            bail!("application worker stopped")
        }
        Ok(true)
    }
    pub fn poll(&self) -> Result<Option<Completion>> {
        match self.complete.try_recv() {
            Ok(Ok(completion)) => {
                let mut c = self.credits.lock().unwrap();
                c.entries -= completion.entries.len();
                c.bytes -= completion.bytes;
                Ok(Some(completion))
            }
            Ok(Err(error)) => Err(error),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => anyhow::bail!("application worker stopped"),
        }
    }
    pub fn query(&self, query: Query) -> Result<()> {
        if self
            .queries
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < 16).then_some(n + 1)
            })
            .is_err()
        {
            let _ = query.reply.send(Reply::status("overloaded"));
            return Ok(());
        }
        self.tx
            .as_ref()
            .unwrap()
            .send(Work::Query(query))
            .map_err(|_| anyhow::anyhow!("application worker stopped"))
    }
}
impl Drop for ApplyWorker {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
#[path = "apply_worker_test.rs"]
mod tests;
