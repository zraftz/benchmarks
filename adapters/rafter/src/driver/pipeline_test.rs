//! Real selected native stores, an owned I/O worker, and application recovery.
use super::*;
use bench_common::model::{Applied, Command, DurableModel};
use rafter::{LogIndex, Message, NodeConfig};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "pipeline-adapter-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn node(root: &Path, id: u64, applied: LogIndex) -> storage::Node {
    let dir = root.join(format!("raft-{id}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (hard, log, snapshot) = storage::NodeStores::open(&dir).unwrap().into_parts();
    let config = NodeConfig::new(
        NodeId(id),
        (1..=3).filter(|n| *n != id).map(NodeId).collect(),
        1,
    )
    .unwrap();
    storage::Node::recover_with_storage_and_snapshot_store_applied_through(
        config, hard, log, snapshot, applied,
    )
    .unwrap()
    .into_parts()
    .0
}
fn to(outputs: &[Output], id: u64) -> Message {
    outputs
        .iter()
        .find_map(|o| match o {
            Output::Send { to, message } if *to == NodeId(id) => Some(message.clone()),
            _ => None,
        })
        .expect("expected peer message")
}
fn elected(root: &Path) -> (storage::Node, storage::Node) {
    let mut leader = node(root, 1, LogIndex::ZERO);
    let mut follower = node(root, 2, LogIndex::ZERO);
    let mut outputs = leader.step(Input::Tick).unwrap();
    // Exchange the real pre-vote, vote, and leadership-no-op requests/replies.
    for _ in 0..3 {
        let reply = follower
            .step(Input::Message {
                from: NodeId(1),
                message: to(&outputs, 2),
            })
            .unwrap();
        outputs = leader
            .step(Input::Message {
                from: NodeId(2),
                message: to(&reply, 1),
            })
            .unwrap();
    }
    assert_eq!(leader.commit_index(), LogIndex(1));
    (leader, follower)
}
fn command() -> Command {
    Command {
        client: "native-pipeline".into(),
        sequence: 1,
        kind: "put".into(),
        key: "key".into(),
        value: "durable".into(),
        expected: None,
    }
}
fn proposal() -> Vec<Input> {
    vec![Input::ClientProposal {
        payload: serde_json::to_vec(&command()).unwrap(),
    }]
}
#[test]
fn native_pipeline_fences_quorum_and_reopens_after_durable_application_completion() {
    let directory = Directory::new();
    let (leader, mut follower) = elected(&directory.0);
    let mut pipeline = Pipeline::new(leader, true).unwrap();
    let replicated = pipeline.step(proposal()).unwrap();
    assert!(pipeline.node.ready_node().is_none());
    assert_eq!(pipeline.node.progress().accepted, LogIndex(2));
    assert_eq!(pipeline.node.progress().durable, LogIndex(1));
    assert_eq!(pipeline.node.progress().committed, LogIndex(1));
    assert!(replicated.iter().all(|o| matches!(o, Output::Send { .. })));
    let response = follower
        .step(Input::Message {
            from: NodeId(1),
            message: to(&replicated, 2),
        })
        .unwrap();
    let ack = Input::Message {
        from: NodeId(2),
        message: to(&response, 1),
    };
    assert!(pipeline.step(vec![ack.clone()]).is_err());
    assert!(
        pipeline.step(proposal()).is_err(),
        "a second outstanding operation is forbidden"
    );
    assert!(pipeline
        .complete()
        .unwrap()
        .unwrap()
        .iter()
        .all(|o| !matches!(o, Output::Apply { .. })));
    assert_eq!(pipeline.node.progress().durable, LogIndex(2));
    let sync_calls: u64 = pipeline
        .counters
        .iter()
        .filter(|(name, _)| matches!(*name, "log_sync" | "batch_sync"))
        .map(|(_, m)| m.calls)
        .sum();
    assert!(
        sync_calls >= 1,
        "worker-thread persistence must be represented in telemetry"
    );
    let outputs = pipeline.step(vec![ack]).unwrap();
    let applies = outputs
        .into_iter()
        .filter_map(|o| match o {
            Output::Apply { index, payload, .. } => Some(Applied {
                index: index.0,
                command: Some(serde_json::from_slice(payload.as_ref()).unwrap()),
                metadata: None,
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(applies.len(), 1);
    assert_eq!(applies[0].index, 2);
    let mut application = DurableModel::open(&directory.0.join("application.wal")).unwrap();
    let outcomes = application.apply(&applies).unwrap();
    assert!(outcomes[0].as_ref().unwrap().error.is_none());
    assert_eq!(pipeline.submitted, 1);
    assert_eq!(pipeline.completed, 1);
    drop(application);
    drop(pipeline);
    drop(follower);
    let recovered_application = DurableModel::open(&directory.0.join("application.wal")).unwrap();
    assert_eq!(recovered_application.model.values["key"], "durable");
    let recovered = node(&directory.0, 1, LogIndex(recovered_application.index));
    assert_eq!(recovered.commit_index(), LogIndex(2));
    assert_eq!(recovered.last_log_index(), LogIndex(2));
}
#[test]
fn shutdown_joins_outstanding_native_persistence_without_publishing_a_commit() {
    let directory = Directory::new();
    let (leader, follower) = elected(&directory.0);
    let mut pipeline = Pipeline::new(leader, false).unwrap();
    pipeline.step(proposal()).unwrap();
    assert!(pipeline.node.pending_operation().is_some());
    // Drop joins the single worker; its unconsumed completion cannot release outputs.
    drop(pipeline);
    drop(follower);
    let recovered = node(&directory.0, 1, LogIndex::ZERO);
    assert_eq!(recovered.last_log_index(), LogIndex(2));
    assert_eq!(recovered.commit_index(), LogIndex(1));
}
