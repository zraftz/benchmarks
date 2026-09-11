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
