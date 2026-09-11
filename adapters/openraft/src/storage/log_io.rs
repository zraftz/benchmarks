//! Ordered synchronous or callback-driven journal publication for the OpenRaft control.

use super::Record;
use crate::Types;
use bench_common::journal::Journal;
use openraft::storage::LogFlushed;
use serde_json::json;
use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
};
use tokio::sync::oneshot;

const QUEUE_CAPACITY: usize = 64;

#[derive(Debug)]
pub(super) enum LogIo {
    Synchronous(Journal),
    Asynchronous(AsyncFlusher),
}

pub(super) enum Fence {
    Durable,
    Pending(oneshot::Receiver<Result<(), String>>),
}

enum Completion {
    Append(LogFlushed<Types>),
    Fence(oneshot::Sender<Result<(), String>>),
}

struct Work {
    record: Record,
    completion: Completion,
}

#[derive(Default)]
struct AsyncStats {
    syncs: AtomicU64,
    bytes: AtomicU64,
    pending: AtomicUsize,
    max_pending: AtomicUsize,
    failure: Mutex<Option<String>>,
}

pub(super) struct AsyncFlusher {
    requests: Option<mpsc::SyncSender<Work>>,
    stats: Arc<AsyncStats>,
    thread: Option<thread::JoinHandle<()>>,
}

impl fmt::Debug for AsyncFlusher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AsyncFlusher")
            .field("pending", &self.stats.pending.load(Ordering::Acquire))
            .field(
                "max_pending",
                &self.stats.max_pending.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl LogIo {
    pub(super) fn new(journal: Journal, asynchronous: bool) -> std::io::Result<Self> {
        if asynchronous {
            AsyncFlusher::start(journal).map(Self::Asynchronous)
        } else {
            Ok(Self::Synchronous(journal))
        }
    }

    pub(super) const fn asynchronous(&self) -> bool {
        matches!(self, Self::Asynchronous(_))
    }

    pub(super) fn write_synchronous(&mut self, record: &Record) -> Result<(), String> {
        let Self::Synchronous(journal) = self else {
            return Err("synchronous write requested from async OpenRaft flusher".to_owned());
        };
        journal.append(record).map_err(|error| error.to_string())
    }

    pub(super) fn submit_append(
        &self,
        record: Record,
        callback: LogFlushed<Types>,
    ) -> Result<(), String> {
        let Self::Asynchronous(flusher) = self else {
            return Err("async append requested from synchronous OpenRaft storage".to_owned());
        };
        flusher.submit(Work {
            record,
            completion: Completion::Append(callback),
        })
    }

    pub(super) fn submit_fence(&self, record: Record) -> Result<Fence, String> {
        let Self::Asynchronous(flusher) = self else {
            return Err("async fence requested from synchronous OpenRaft storage".to_owned());
        };
        let (send, receive) = oneshot::channel();
        flusher.submit(Work {
            record,
            completion: Completion::Fence(send),
        })?;
        Ok(Fence::Pending(receive))
    }

    pub(super) fn stats(&self, retained_entries: usize) -> serde_json::Value {
        match self {
            Self::Synchronous(journal) => json!({
                "raft_syncs": journal.syncs,
                "raft_bytes": journal.bytes,
                "retained_entries": retained_entries,
                "storage": "benchmark journal + BTreeMap",
                "flush_mode": "synchronous",
            }),
            Self::Asynchronous(flusher) => json!({
                "raft_syncs": flusher.stats.syncs.load(Ordering::Acquire),
                "raft_bytes": flusher.stats.bytes.load(Ordering::Acquire),
                "retained_entries": retained_entries,
                "storage": "benchmark journal + BTreeMap",
                "flush_mode": "callback-driven ordered worker",
                "flush_queue_capacity": QUEUE_CAPACITY,
                "flush_queue_depth": flusher.stats.pending.load(Ordering::Acquire),
                "flush_queue_max_depth": flusher.stats.max_pending.load(Ordering::Relaxed),
                "flush_failed": flusher.stats.failure.lock().unwrap().is_some(),
            }),
        }
    }
}

impl Fence {
    pub(super) async fn wait(self) -> Result<(), String> {
        match self {
            Self::Durable => Ok(()),
            Self::Pending(receive) => receive
                .await
                .map_err(|_| "async OpenRaft log flusher stopped before its fence".to_owned())?,
        }
    }
}

impl AsyncFlusher {
    fn start(mut journal: Journal) -> std::io::Result<Self> {
        let (requests, receive) = mpsc::sync_channel::<Work>(QUEUE_CAPACITY);
        let stats = Arc::new(AsyncStats::default());
        let worker_stats = stats.clone();
        let thread = thread::Builder::new()
            .name("openraft-log-flush".to_owned())
            .spawn(move || {
                while let Ok(work) = receive.recv() {
                    let result = journal
                        .append(&work.record)
                        .map_err(|error| error.to_string());
                    if result.is_ok() {
                        worker_stats.syncs.store(journal.syncs, Ordering::Release);
                        worker_stats.bytes.store(journal.bytes, Ordering::Release);
                    }
                    if let Err(error) = &result {
                        *worker_stats.failure.lock().unwrap() = Some(error.clone());
                    }
                    worker_stats.pending.fetch_sub(1, Ordering::AcqRel);
                    work.completion.complete(result.clone());
                    if let Err(error) = result {
                        while let Ok(queued) = receive.try_recv() {
                            worker_stats.pending.fetch_sub(1, Ordering::AcqRel);
                            queued.completion.complete(Err(error.clone()));
                        }
                        break;
                    }
                }
            })?;
        Ok(Self {
            requests: Some(requests),
            stats,
            thread: Some(thread),
        })
    }

    fn submit(&self, work: Work) -> Result<(), String> {
        let Some(sender) = self.requests.as_ref() else {
            let error = "async OpenRaft log flusher is stopped".to_owned();
            work.completion.complete(Err(error.clone()));
            return Err(error);
        };
        if let Some(error) = self.stats.failure.lock().unwrap().clone() {
            work.completion.complete(Err(error.clone()));
            return Err(error);
        }
        let pending = self.stats.pending.fetch_add(1, Ordering::AcqRel) + 1;
        self.stats.max_pending.fetch_max(pending, Ordering::Relaxed);
        match sender.try_send(work) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.stats.pending.fetch_sub(1, Ordering::AcqRel);
                let (message, work) = match error {
                    mpsc::TrySendError::Full(work) => {
                        ("async OpenRaft log flush queue is full", work)
                    }
                    mpsc::TrySendError::Disconnected(work) => {
                        ("async OpenRaft log flusher is disconnected", work)
                    }
                };
                let message = message.to_owned();
                work.completion.complete(Err(message.clone()));
                Err(message)
            }
        }
    }
}

impl Completion {
    fn complete(self, result: Result<(), String>) {
        match self {
            Self::Append(callback) => {
                callback.log_io_completed(result.map_err(std::io::Error::other))
            }
            Self::Fence(send) => {
                let _ = send.send(result);
            }
        }
    }
}

impl Drop for AsyncFlusher {
    fn drop(&mut self) {
        self.requests.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
