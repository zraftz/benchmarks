//! Rafter native durable stores + shared client/transport/application embedding.
use anyhow::Result;
use bench_common::{
    actor::{Actor, Effect, Engine},
    model::{Command, DurableModel},
    net, Config,
};
use rafter::{Input, LogIndex, NodeConfig, NodeId, Output, Role};
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
impl Engine for Rafter {
    fn leader(&self) -> Option<u64> {
        self.node.leader_hint().map(|n| n.0)
    }
    fn is_leader(&self) -> bool {
        self.node.role() == Role::Leader
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        effects(self.node.step(Input::Tick)?)
    }
    fn peer(&mut self, from: u64, data: &[u8]) -> Result<Vec<Effect>> {
        effects(self.node.step(Input::Message {
            from: NodeId(from),
            message: rafter_codec::decode_message(data)?,
        })?)
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
        serde_json::json!({"term":self.node.current_term().0,
        "commit_index":self.node.commit_index().0,"last_log_index":self.node.last_log_index().0,
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
