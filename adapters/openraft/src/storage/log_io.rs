//! Ordered synchronous or callback-driven journal publication for the OpenRaft control.

use super::Record;
use crate::Types;
use bench_common::journal::Journal;
use openraft::storage::LogFlushed;
use serde_json::json;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
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
    #[cfg(test)]
    fail_next: AtomicBool,
    #[cfg(test)]
    before_failure: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    failure_lock_attempt: Mutex<Option<mpsc::SyncSender<()>>>,
    #[cfg(test)]
    failure_recorded: Mutex<Option<mpsc::SyncSender<()>>>,
    #[cfg(test)]
    before_admission: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}

#[cfg(test)]
pub(super) struct FailureControl {
    pub(super) failure_ready: mpsc::Receiver<()>,
    pub(super) release_failure: mpsc::SyncSender<()>,
    pub(super) failure_lock_attempt: mpsc::Receiver<()>,
    pub(super) failure_recorded: mpsc::Receiver<()>,
}

#[cfg(test)]
pub(super) struct AdmissionControl {
    pub(super) admission_ready: mpsc::Receiver<()>,
    pub(super) release_admission: mpsc::SyncSender<()>,
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

    #[cfg(test)]
    pub(super) fn fail_next_before_recording(&self) -> FailureControl {
        let Self::Asynchronous(flusher) = self else {
            panic!("failure control requires an asynchronous flusher");
        };
        flusher.fail_next_before_recording()
    }

    #[cfg(test)]
    pub(super) fn pause_next_admission_after_failure_check(&self) -> AdmissionControl {
        let Self::Asynchronous(flusher) = self else {
            panic!("admission control requires an asynchronous flusher");
        };
        flusher.pause_next_admission_after_failure_check()
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
                    #[cfg(test)]
                    let injected = worker_stats.fail_next.swap(false, Ordering::AcqRel);
                    #[cfg(test)]
                    let result = if injected {
                        Err("injected async OpenRaft log flush failure".to_owned())
                    } else {
                        journal
                            .append(&work.record)
                            .map_err(|error| error.to_string())
                    };
                    #[cfg(not(test))]
                    let result = journal
                        .append(&work.record)
                        .map_err(|error| error.to_string());
                    if result.is_ok() {
                        worker_stats.syncs.store(journal.syncs, Ordering::Release);
                        worker_stats.bytes.store(journal.bytes, Ordering::Release);
                    }
                    if let Err(error) = &result {
                        #[cfg(test)]
                        if let Some((ready, release)) =
                            worker_stats.before_failure.lock().unwrap().take()
                        {
                            let _ = ready.send(());
                            let _ = release.recv();
                        }
                        #[cfg(test)]
                        if let Some(attempt) =
                            worker_stats.failure_lock_attempt.lock().unwrap().take()
                        {
                            let _ = attempt.send(());
                        }
                        *worker_stats.failure.lock().unwrap() = Some(error.clone());
                        #[cfg(test)]
                        if let Some(recorded) = worker_stats.failure_recorded.lock().unwrap().take()
                        {
                            let _ = recorded.send(());
                        }
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
        // Failure publication and queue admission share one lock. Otherwise a
        // submitter could observe no failure, lose a race with the worker's
        // final queue drain, then enqueue work whose durability callback is
        // dropped with the receiver.
        let failure = self.stats.failure.lock().unwrap();
        if let Some(error) = failure.clone() {
            drop(failure);
            work.completion.complete(Err(error.clone()));
            return Err(error);
        }
        #[cfg(test)]
        if let Some((ready, release)) = self.stats.before_admission.lock().unwrap().take() {
            let _ = ready.send(());
            let _ = release.recv();
        }
        let pending = self.stats.pending.fetch_add(1, Ordering::AcqRel) + 1;
        self.stats.max_pending.fetch_max(pending, Ordering::Relaxed);
        match sender.try_send(work) {
            Ok(()) => {
                drop(failure);
                Ok(())
            }
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
                drop(failure);
                work.completion.complete(Err(message.clone()));
                Err(message)
            }
        }
    }

    #[cfg(test)]
    fn fail_next_before_recording(&self) -> FailureControl {
        let (failure_ready, ready) = mpsc::sync_channel(1);
        let (release, release_failure) = mpsc::sync_channel(1);
        let (attempt, failure_lock_attempt) = mpsc::sync_channel(1);
        let (recorded, failure_recorded) = mpsc::sync_channel(1);
        *self.stats.before_failure.lock().unwrap() = Some((failure_ready, release_failure));
        *self.stats.failure_lock_attempt.lock().unwrap() = Some(attempt);
        *self.stats.failure_recorded.lock().unwrap() = Some(recorded);
        self.stats.fail_next.store(true, Ordering::Release);
        FailureControl {
            failure_ready: ready,
            release_failure: release,
            failure_lock_attempt,
            failure_recorded,
        }
    }

    #[cfg(test)]
    fn pause_next_admission_after_failure_check(&self) -> AdmissionControl {
        let (admission_ready, ready) = mpsc::sync_channel(1);
        let (release, release_admission) = mpsc::sync_channel(1);
        *self.stats.before_admission.lock().unwrap() = Some((admission_ready, release_admission));
        AdmissionControl {
            admission_ready: ready,
            release_admission: release,
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
