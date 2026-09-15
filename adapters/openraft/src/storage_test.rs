//! Ordered-flusher recovery scenarios for the stronger OpenRaft control.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(std::path::PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "raft-bench-openraft-async-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn callback_worker_fences_metadata_and_recovers_in_order() {
    let directory = TestDirectory::new();
    let path = directory.0.join("raft.wal");
    let store = LogStore::open(&path, true).unwrap();
    let vote = Vote::new(7, 2);

    store.write_fenced(Record::Vote(vote)).await.unwrap();
    store.write_fenced(Record::Committed(None)).await.unwrap();
    let stats = store.stats();
    assert_eq!(stats["flush_mode"], "callback-driven ordered worker");
    assert_eq!(stats["raft_syncs"], 2);
    assert_eq!(stats["flush_queue_depth"], 0);
    drop(store);

    let mut reopened = LogStore::open(&path, false).unwrap();
    assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
    assert_eq!(reopened.read_committed().await.unwrap(), None);
    assert_eq!(reopened.stats()["flush_mode"], "synchronous");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_admission_cannot_cross_terminal_failure_publication() {
    let directory = TestDirectory::new();
    let path = directory.0.join("raft.wal");
    let store = LogStore::open(&path, true).unwrap();
    let failure = store.fail_next_before_recording();
    let first_store = store.clone();
    let first = tokio::spawn(async move {
        first_store
            .write_fenced(Record::Vote(Vote::new(7, 2)))
            .await
    });
    failure
        .failure_ready
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();

    let admission = store.pause_next_admission_after_failure_check();
    let second_store = store.clone();
    let second =
        tokio::spawn(async move { second_store.write_fenced(Record::Committed(None)).await });
    admission
        .admission_ready
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    failure.release_failure.send(()).unwrap();
    failure
        .failure_lock_attempt
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert!(failure
        .failure_recorded
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());

    admission.release_admission.send(()).unwrap();
    failure
        .failure_recorded
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), first)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(store.stats()["flush_queue_depth"], 0);
    assert_eq!(store.stats()["flush_failed"], true);
}
