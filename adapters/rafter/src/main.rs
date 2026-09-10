//! Rafter native durable stores + shared client/transport/application embedding.
use anyhow::Result;
use bench_common::{
    actor::{Actor, Effect, Engine},
    model::{Command, DurableModel},
    net, Config,
};
use rafter::{Input, LogIndex, Message, NodeConfig, NodeId, Output, Role};
use rafter_runtime::DurableRaftNode;
#[cfg(not(feature = "journal-hard-state"))]
use rafter_storage::{FileRaftHardStateStore as HardState, FileRaftNodeStores as NodeStores};
use rafter_storage::{FileRaftLogSegment, FileRaftSnapshotStore};
#[cfg(feature = "journal-hard-state")]
use rafter_storage::{JournalRaftHardStateStore as HardState, JournalRaftNodeStores as NodeStores};

const HARD_STATE_BACKEND: &str = if cfg!(feature = "journal-hard-state") {
    "journal"
} else {
    "replace"
};

type Node = DurableRaftNode<HardState, FileRaftLogSegment, FileRaftSnapshotStore>;
struct Rafter {
    node: Node,
}
fn effects(outputs: Vec<Output>) -> Result<Vec<Effect>> {
    let mut result = Vec::new();
    for output in outputs {
        match output {
            Output::Send { to, message } => result.push(Effect::Send {
                to: to.0,
                data: rafter_codec::encode_message(&message)?,
            }),
            Output::Apply { index, payload, .. } => result.push(Effect::Apply {
                index: index.0,
                command: Some(serde_json::from_slice(payload.as_ref())?),
            }),
            _ => {} // Static membership, no reads outside the log, no snapshot/compaction triggers.
        }
    }
    Ok(result)
}
#[cfg(feature = "peer-group-commit")]
type PeerGate = rafter_runtime::PeerBatchGate;
#[cfg(not(feature = "peer-group-commit"))]
type PeerGate = ();

impl Engine for Rafter {
    type Peer = (NodeId, Message);
    type Gate = PeerGate;
    fn start(&mut self, diagnostics: bool) {
        #[cfg(feature = "peer-group-commit")]
        rafter_storage::telemetry::set_enabled(diagnostics);
        #[cfg(not(feature = "peer-group-commit"))]
        let _ = diagnostics;
    }
    fn decode_peer(&self, from: u64, data: &[u8]) -> Result<Self::Peer> {
        Ok((NodeId(from), rafter_codec::decode_message(data)?))
    }
    fn peer_gate(&self, max_events: usize, max_bytes: usize) -> PeerGate {
        #[cfg(feature = "peer-group-commit")]
        {
            self.node.peer_batch_gate(max_events, max_bytes)
        }
        #[cfg(not(feature = "peer-group-commit"))]
        {
            let _ = (max_events, max_bytes);
        }
    }
    fn admit_peer(gate: &mut PeerGate, peer: &Self::Peer) -> bool {
        #[cfg(feature = "peer-group-commit")]
        {
            gate.admit(peer.0, &peer.1)
        }
        #[cfg(not(feature = "peer-group-commit"))]
        {
            let _ = (gate, peer);
            false
        }
    }
    fn leader(&self) -> Option<u64> {
        self.node.leader_hint().map(|n| n.0)
    }
    fn is_leader(&self) -> bool {
        self.node.role() == Role::Leader
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        effects(self.node.step(Input::Tick)?)
    }
    fn peer_batch(&mut self, peers: Vec<Self::Peer>) -> Result<Vec<Effect>> {
        effects(
            self.node.step_batch(
                peers
                    .into_iter()
                    .map(|(from, message)| Input::Message { from, message })
                    .collect(),
            )?,
        )
    }
    fn propose(&mut self, commands: Vec<Command>) -> Result<Vec<Effect>> {
        let inputs = commands
            .iter()
            .map(|c| {
                Ok(Input::ClientProposal {
                    payload: serde_json::to_vec(c)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        effects(self.node.step_batch(inputs)?)
    }
    fn stats(&self) -> serde_json::Value {
        #[cfg(feature = "peer-group-commit")]
        let persistence: serde_json::Value = rafter_storage::telemetry::snapshot()
            .into_iter()
            .map(|(name, m)| {
                (
                    name.to_owned(),
                    serde_json::json!({"calls":m.calls,
                "total_ns":m.total_ns,"max_ns":m.max_ns,"buckets_ns_log2":m.buckets}),
                )
            })
            .collect();
        #[cfg(not(feature = "peer-group-commit"))]
        let persistence = serde_json::Value::Null;
        serde_json::json!({"term":self.node.current_term().0,
        "commit_index":self.node.commit_index().0,"last_log_index":self.node.last_log_index().0,
        "persistence_diagnostics":persistence,"peer_group_commit_available":cfg!(feature = "peer-group-commit"),
        "storage":"rafter native file stores","hard_state_backend":HARD_STATE_BACKEND,"election_ticks":"50 + 7*(node_id-1)"})
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let config = Config::load()?;
    let _lock = config.lock_directory("rafter")?;
    let model = DurableModel::open(&config.data_dir.join("application.wal"))?;
    let raft_dir = config.data_dir.join("raft");
    std::fs::create_dir_all(&raft_dir)?;
    std::fs::File::open(&config.data_dir)?.sync_all()?;
    let (hard, log, snap) = NodeStores::open(&raft_dir)?.into_parts();
    let peers = config
        .peers
        .keys()
        .copied()
        .filter(|&id| id != config.id)
        .map(NodeId)
        .collect();
    let node_config = NodeConfig::new(NodeId(config.id), peers, 50 + 7 * (config.id - 1))?;
    let recovered = Node::recover_with_storage_and_snapshot_store_applied_through(
        node_config,
        hard,
        log,
        snap,
        LogIndex(model.index),
    )?;
    let (node, outputs) = recovered.into_parts();
    let handler = Actor::start(
        config.clone(),
        "rafter",
        Rafter { node },
        model,
        effects(outputs)?,
    );
    net::serve(config, handler).await
}
