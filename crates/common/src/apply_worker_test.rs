//! Delayed and failed application persistence never becomes a completion early.
use super::*;
use crate::model::Command;
use std::{
    path::PathBuf,
    sync::{atomic::AtomicUsize, mpsc},
    time::Duration,
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Store {
    model: DurableModel,
    pause: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
    fail: bool,
}
impl ApplicationStore for Store {
    fn apply(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>> {
        if let Some((entered, release)) = self.pause.take() {
            entered.send(()).unwrap();
            release.recv().unwrap();
        }
        if self.fail {
            bail!("injected application sync failure")
        }
        self.model.apply(entries)
    }
    fn index(&self) -> u64 {
        self.model.index
    }
    fn stats(&self) -> serde_json::Value {
        self.model.stats()
    }
    fn dump(&self) -> serde_json::Value {
        serde_json::json!({"values":self.model.model.values,"applied_index":self.model.index})
    }
}
fn entry(index: u64) -> Applied {
    Applied {
        index,
        command: Some(Command {
            client: "c".into(),
            sequence: index,
            kind: "put".into(),
            key: "k".into(),
            value: index.to_string(),
            expected: None,
        }),
        metadata: None,
    }
}
fn paused(fail: bool) -> (PathBuf, ApplyWorker, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let path = std::env::temp_dir().join(format!(
        "apply-worker-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let (entered, waiting) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let store = Store {
        model: DurableModel::open(&path).unwrap(),
        pause: Some((entered, resume)),
        fail,
    };
    let worker = ApplyWorker::start(store, Diagnostics::new(true), || {});
    (path, worker, waiting, release)
}
fn completion(worker: &ApplyWorker) -> Result<Completion> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(done) = worker.poll()? {
            return Ok(done);
        }
        assert!(Instant::now() < deadline, "worker did not complete");
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn timing_mode_retains_no_per_entry_diagnostic_queue_state() {
    let path = std::env::temp_dir().join(format!(
        "apply-worker-timing-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let worker = ApplyWorker::start(
        DurableModel::open(&path).unwrap(),
        Diagnostics::default(),
        || {},
    );
    assert!(worker.queued.is_none());
    assert!(worker.try_submit(vec![entry(1)]).unwrap());
    let completed = completion(&worker).unwrap();
    assert_eq!(completed.entries[0].index, 1);
    assert!(completed.queued.is_none());
    assert!(worker.queued.is_none());
    drop(worker);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn submissions_preserve_entry_and_byte_batch_limits() {
    let path = std::env::temp_dir().join(format!(
        "apply-worker-batches-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let worker = ApplyWorker::start(
        DurableModel::open(&path).unwrap(),
        Diagnostics::default(),
        || {},
    );

    assert!(worker.try_submit(vec![entry(1), entry(2)]).unwrap());
    assert_eq!(
        completion(&worker)
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let mut third = entry(3);
    third.command.as_mut().unwrap().kind = "cas".into();
    third.command.as_mut().unwrap().value = "x".repeat(64 * 1024);
    third.command.as_mut().unwrap().expected = Some("x".repeat(64 * 1024));
    let mut fourth = entry(4);
    fourth.command.as_mut().unwrap().kind = "cas".into();
    fourth.command.as_mut().unwrap().value = "y".repeat(64 * 1024);
    fourth.command.as_mut().unwrap().expected = Some("y".repeat(64 * 1024));
    assert!(payload_bytes(&third) < BATCH_BYTES);
    assert!(payload_bytes(&third) + payload_bytes(&fourth) > BATCH_BYTES);
    assert!(worker.try_submit(vec![third, fourth]).unwrap());
    assert_eq!(completion(&worker).unwrap().entries[0].index, 3);
    assert_eq!(completion(&worker).unwrap().entries[0].index, 4);

    drop(worker);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn idle_worker_installs_snapshot_and_accepts_the_contiguous_suffix() {
    let source_path = std::env::temp_dir().join(format!(
        "apply-worker-snapshot-source-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let target_path = std::env::temp_dir().join(format!(
        "apply-worker-snapshot-target-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut source = DurableModel::open(&source_path).unwrap();
    source.apply(&[entry(1)]).unwrap();
    let snapshot = source.snapshot();

    let mut worker = ApplyWorker::start(
        DurableModel::open(&target_path).unwrap(),
        Diagnostics::default(),
        || {},
    );
    worker.install_snapshot(snapshot.clone()).unwrap();
    assert_eq!(worker.applied_index(), 1);
    assert_eq!(worker.snapshot().unwrap(), Some(snapshot));
    assert!(worker.try_submit(vec![entry(2)]).unwrap());
    assert_eq!(worker.complete().unwrap().entries[0].index, 2);
    drop(worker);

    let reopened = DurableModel::open(&target_path).unwrap();
    assert_eq!(reopened.index, 2);
    assert_eq!(reopened.model.values["k"], "2");
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(target_path).unwrap();
}
#[test]
fn delayed_sync_keeps_completions_and_durable_position_behind() {
    let (path, worker, waiting, release) = paused(false);
    assert!(worker.try_submit(vec![entry(1)]).unwrap());
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    std::thread::sleep(Duration::from_millis(10));
    assert!(worker.poll().unwrap().is_none());
    assert_eq!(worker.applied_index(), 0);
    assert_eq!(worker.applying_index(), 1);
    assert!(worker.try_submit(vec![entry(2), entry(3)]).unwrap());
    release.send(()).unwrap();
    let first = completion(&worker).unwrap();
    assert_eq!(first.entries[0].index, 1);
    assert_eq!(first.queued.as_ref().unwrap().len(), 1);
    let next = completion(&worker).unwrap();
    assert_eq!(
        next.entries
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    drop(worker);
    let reopened = DurableModel::open(&path).unwrap();
    assert_eq!(reopened.index, 3);
    assert_eq!(reopened.model.values["k"], "3");
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn credits_cover_in_progress_and_unconsumed_completions() {
    let (path, worker, waiting, release) = paused(false);
    let entries: Vec<_> = (1..=200).map(entry).collect();
    assert!(worker.try_submit(entries).unwrap());
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!worker.try_submit((201..=400).map(entry).collect()).unwrap());
    let before = worker.available();
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while worker.applied_index() < 200 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        worker.available(),
        before,
        "unconsumed results still own credits"
    );
    let mut applied = 0;
    while applied < 200 {
        let done = completion(&worker).unwrap();
        assert!(done.entries.len() <= 64);
        applied += done.entries.len();
    }
    assert_eq!(worker.available(), (QUEUE_ENTRIES, QUEUE_BYTES));
    drop(worker);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn worker_failure_returns_no_success_or_later_apply() {
    let (path, worker, waiting, release) = paused(true);
    worker.try_submit(vec![entry(1)]).unwrap();
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.try_submit(vec![entry(2)]).unwrap();
    release.send(()).unwrap();
    assert!(completion(&worker).is_err());
    assert_eq!(worker.applied_index(), 0);
    let (reply, _) = oneshot::channel();
    assert!(worker
        .query(Query {
            reply,
            dump: false,
            response: Reply::status("ok"),
        })
        .is_err());
    drop(worker);
    let recovered = DurableModel::open(&path).unwrap();
    assert_eq!(recovered.index, 0);
    drop(recovered);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn shutdown_during_apply_joins_and_preserves_durable_work() {
    let (path, worker, waiting, release) = paused(false);
    worker.try_submit(vec![entry(1)]).unwrap();
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    let (finished, joined) = mpsc::channel();
    std::thread::spawn(move || {
        drop(worker);
        finished.send(()).unwrap();
    });
    assert!(joined.try_recv().is_err());
    release.send(()).unwrap();
    joined.recv_timeout(Duration::from_secs(5)).unwrap();
    let recovered = DurableModel::open(&path).unwrap();
    assert_eq!(recovered.index, 1);
    drop(recovered);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn status_queries_read_the_worker_after_its_durable_fence() {
    let (path, worker, waiting, release) = paused(false);
    worker.try_submit(vec![entry(1)]).unwrap();
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    let (reply, mut response) = oneshot::channel();
    worker
        .query(Query {
            reply,
            dump: false,
            response: Reply {
                status: "ok".into(),
                info: Some(serde_json::json!({"application_dispatched_index":1})),
                ..Reply::default()
            },
        })
        .unwrap();
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    let _ = completion(&worker).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Ok(result) = response.try_recv() {
            break result;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(status.info.unwrap()["application"]["applied_index"], 1);
    drop(worker);
    std::fs::remove_file(path).unwrap();
}
