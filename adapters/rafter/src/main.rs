//! Rafter native durable stores + shared client/transport/application embedding.
mod diagnostics;
mod driver;

use anyhow::Result;
use bench_common::{
    actor::{Actor, Effect, Engine},
    model::{ApplicationSnapshot, Applied, Command, DurableModel},
    net, Config,
};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LogIndex, Message, NodeConfig, NodeId, Output, RaftSnapshot, RaftSnapshotMetadata, Role,
    SnapshotChunkRequest, SnapshotChunkSource, SnapshotGroupId,
};
use rafter_storage::PersistedRaftSnapshot;
mod storage;
use storage::{Node, NodeStores, HARD_STATE_BACKEND};
struct Rafter {
    node: driver::Driver,
    node_id: NodeId,
    snapshot_group: SnapshotGroupId,
    ordered_apply: bool,
    empty_appends: diagnostics::EmptyAppends,
    replication_windows: diagnostics::ReplicationWindows,
}
const APPLICATION_SNAPSHOT_KIND: &str = "raft-bench-durable-model";
const APPLICATION_SNAPSHOT_VERSION: u16 = 1;
const SNAPSHOT_CHUNK_BYTES: u64 = 64 * 1024;
const MAX_APPLICATION_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;
fn can_batch_same_term_append_responses(
    role: Role,
    term: rafter::Term,
    peers: &[(NodeId, Message)],
) -> bool {
    role == Role::Leader
        && peers.iter().all(|(_, message)| {
            matches!(message, Message::AppendEntriesResponse(response) if response.term == term)
        })
}
fn read_snapshot_payload(node: &Node, snapshot: &RaftSnapshot) -> Result<Vec<u8>> {
    if snapshot.application_payload_len > MAX_APPLICATION_SNAPSHOT_BYTES {
        anyhow::bail!("application snapshot exceeds benchmark adapter bound")
    }
    let mut payload = Vec::with_capacity(snapshot.application_payload_len as usize);
    let mut offset = 0_u64;
    while offset < snapshot.application_payload_len {
        let len = (snapshot.application_payload_len - offset).min(SNAPSHOT_CHUNK_BYTES) as u32;
        let chunk = node
            .snapshot_store()
            .snapshot_chunk(SnapshotChunkRequest {
                transfer_id: snapshot.transfer_id(),
                metadata: &snapshot.metadata,
                total_payload_len: snapshot.application_payload_len,
                application_payload_crc32: snapshot.application_payload_crc32,
                offset,
                len,
            })
            .ok_or_else(|| anyhow::anyhow!("promoted snapshot payload is unavailable"))?;
        if chunk.len() != len as usize {
            anyhow::bail!("snapshot source returned a short chunk")
        }
        payload.extend_from_slice(&chunk);
        offset += u64::from(len);
    }
    Ok(payload)
}

fn effects(node: Option<&Node>, outputs: Vec<Output>, ordered: bool) -> Result<Vec<Effect>> {
    let mut result = Vec::new();
    for output in outputs {
        match output {
            Output::Send { to, message } => result.push(Effect::Send {
                to: to.0,
                data: rafter_codec::encode_message(&message)?,
            }),
            Output::Apply { index, payload, .. } => result.push(Effect::Apply {
                index: index.0,
                command: if ordered {
                    None
                } else {
                    Some(serde_json::from_slice(payload.as_ref())?)
                },
            }),
            Output::ApplySnapshot { snapshot } => {
                if snapshot.metadata.application.kind.as_str() != APPLICATION_SNAPSHOT_KIND
                    || snapshot.metadata.application.version.get() != APPLICATION_SNAPSHOT_VERSION
                {
                    anyhow::bail!("unsupported application snapshot format")
                }
                let node = node.ok_or_else(|| {
                    anyhow::anyhow!("snapshot application output escaped pending persistence")
                })?;
                let application =
                    ApplicationSnapshot::decode(&read_snapshot_payload(node, &snapshot)?)?;
                if application.applied_index != snapshot.metadata.last_included_index.0 {
                    anyhow::bail!("application snapshot boundary differs from Raft metadata")
                }
                result.push(Effect::InstallSnapshot {
                    snapshot: application,
                });
            }
            _ => {} // Static membership, no reads outside the log, no snapshot/compaction triggers.
        }
    }
    Ok(result)
}
#[cfg(feature = "peer-group-commit")]
type PeerGate = rafter_runtime::PeerBatchGate;
#[cfg(not(feature = "peer-group-commit"))]
type PeerGate = ();

impl Rafter {
    fn observe_replication_windows(&mut self) {
        if !self.replication_windows.enabled() {
            return;
        }
        let windows = self.node.ready().leader_replication_windows();
        self.replication_windows.observe(windows);
    }
    fn drive(&mut self, inputs: Vec<Input>) -> Result<Vec<Effect>> {
        let origin = inputs
            .first()
            .map(|input| diagnostics::origin(input, self.node.role()));
        let progress = if self.empty_appends.enabled() {
            self.node.ready().leader_replication_progress()
        } else {
            Vec::new()
        };
        let outputs = self.node.step_batch(inputs)?;
        if let Some(origin) = origin {
            self.empty_appends.record(&outputs, origin, &progress);
        }
        if !self.node.pending() {
            self.observe_replication_windows();
        }
        let ready = (!self.node.pending()).then(|| self.node.ready());
        effects(ready, outputs, self.ordered_apply)
    }
}
impl Engine for Rafter {
    fn persistence_pending(&self) -> bool {
        self.node.pending()
    }
    fn complete_persistence(&mut self) -> Result<Option<Vec<Effect>>> {
        let completed = self
            .node
            .complete()?
            .map(|outputs| effects(Some(self.node.ready()), outputs, self.ordered_apply))
            .transpose()?;
        if completed.is_some() {
            self.observe_replication_windows();
        }
        Ok(completed)
    }
    fn term(&self) -> u64 {
        self.node.current_term().0
    }
    type Peer = (NodeId, Message);
    type Gate = PeerGate;
    fn start(&mut self, diagnostics: bool) {
        #[cfg(feature = "peer-group-commit")]
        rafter_storage::telemetry::set_enabled(diagnostics);
        #[cfg(not(feature = "peer-group-commit"))]
        let _ = diagnostics;
        self.observe_replication_windows();
    }
    fn decode_peer(&self, from: u64, data: &[u8]) -> Result<Self::Peer> {
        Ok((NodeId(from), rafter_codec::decode_message(data)?))
    }
    fn peer_queue_metric(peer: &Self::Peer) -> Option<&'static str> {
        matches!(
            peer.1,
            Message::AppendEntriesResponse(_) | Message::InstallSnapshotResponse(_)
        )
        .then_some("owner_replication_ack_queue_ns")
    }
    fn peer_gate(&self, max_events: usize, max_bytes: usize) -> PeerGate {
        #[cfg(feature = "peer-group-commit")]
        {
            self.node.ready().peer_batch_gate(max_events, max_bytes)
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
    fn committed_through(&self) -> u64 {
        self.node.ready().commit_index().0
    }
    fn last_index(&self) -> u64 {
        self.node.ready().last_log_index().0
    }
    fn snapshot_compaction_supported(&self) -> bool {
        true
    }
    fn snapshot_index(&self) -> u64 {
        self.node.ready().snapshot_index().0
    }
    fn compact_snapshot(&mut self, applied_index: u64, payload: Vec<u8>) -> Result<()> {
        if self.node.pending() {
            anyhow::bail!("cannot compact while persistence owns the Raft node")
        }
        if payload.len() as u64 > MAX_APPLICATION_SNAPSHOT_BYTES {
            anyhow::bail!("application snapshot exceeds benchmark adapter bound")
        }
        let node = self.node.ready_mut();
        let applied = LogIndex(applied_index);
        let term = node
            .term_at_index(applied)
            .ok_or_else(|| anyhow::anyhow!("snapshot boundary term is unavailable"))?;
        let metadata = RaftSnapshotMetadata::new(
            self.snapshot_group.clone(),
            self.node_id,
            applied,
            term,
            node.current_term(),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new(APPLICATION_SNAPSHOT_KIND)?,
                ApplicationSnapshotVersion::new(APPLICATION_SNAPSHOT_VERSION)?,
            ),
        )?;
        node.compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: payload,
        })?;
        self.observe_replication_windows();
        Ok(())
    }
    fn committed_entries(&self, after: u64, count: usize, bytes: usize) -> Result<Vec<Applied>> {
        #[cfg(feature = "ordered-apply")]
        {
            if after < self.node.ready().snapshot_index().0 {
                anyhow::bail!("application floor is behind a snapshot")
            }
            let mut result = Vec::new();
            let mut used = 0;
            for (offset, entry) in self
                .node
                .ready()
                .log_entries_slice_from(LogIndex(after + 1))
                .iter()
                .take(count)
                .enumerate()
            {
                let index = after + 1 + offset as u64;
                if index > self.node.ready().commit_index().0 {
                    break;
                }
                let applied = Applied {
                    index,
                    command: entry
                        .application_payload()
                        .map(serde_json::from_slice)
                        .transpose()?,
                    metadata: None,
                };
                let size = bench_common::apply_worker::payload_bytes(&applied);
                if !result.is_empty() && used + size > bytes {
                    break;
                }
                used += size;
                result.push(applied);
            }
            Ok(result)
        }
        #[cfg(not(feature = "ordered-apply"))]
        {
            let _ = (after, count, bytes);
            anyhow::bail!("ordered apply requires the ordered-apply build feature")
        }
    }
    fn leader(&self) -> Option<u64> {
        self.node.leader_hint().map(|n| n.0)
    }
    fn is_leader(&self) -> bool {
        self.node.role() == Role::Leader
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        self.drive(vec![Input::Tick])
    }
    fn peer_batch(&mut self, peers: Vec<Self::Peer>) -> Result<Vec<Effect>> {
        self.drive(
            peers
                .into_iter()
                .map(|(from, message)| Input::Message { from, message })
                .collect(),
        )
    }

    fn can_batch_proposals_with_peer(&self, peers: &[Self::Peer]) -> bool {
        can_batch_same_term_append_responses(self.node.role(), self.node.current_term(), peers)
    }

    fn can_prioritize_peer_before_proposals(&self, peers: &[Self::Peer]) -> bool {
        can_batch_same_term_append_responses(self.node.role(), self.node.current_term(), peers)
    }

    fn peer_batch_and_propose(
        &mut self,
        peers: Vec<Self::Peer>,
        commands: Vec<Command>,
    ) -> Result<Vec<Effect>> {
        let mut inputs = peers
            .into_iter()
            .map(|(from, message)| Input::Message { from, message })
            .collect::<Vec<_>>();
        inputs.extend(
            commands
                .into_iter()
                .map(|command| {
                    Ok(Input::ClientProposal {
                        payload: serde_json::to_vec(&command)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        );
        self.drive(inputs)
    }

    fn propose_and_peer_batch(
        &mut self,
        commands: Vec<Command>,
        peers: Vec<Self::Peer>,
    ) -> Result<Vec<Effect>> {
        let mut inputs = commands
            .into_iter()
            .map(|command| {
                Ok(Input::ClientProposal {
                    payload: serde_json::to_vec(&command)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        inputs.extend(
            peers
                .into_iter()
                .map(|(from, message)| Input::Message { from, message }),
        );
        self.drive(inputs)
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
        self.drive(inputs)
    }
    fn stats(&self) -> serde_json::Value {
        #[cfg(feature = "peer-group-commit")]
        let persistence: serde_json::Value = self
            .node
            .native_counters()
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
        "commit_index":self.node.ready().commit_index().0,"last_log_index":self.node.ready().last_log_index().0,
        "empty_appends_by_reason":self.empty_appends.snapshot(),"replication_windows":self.replication_windows.snapshot(),
        "persistence_pipeline":self.node.stats(),
        "persistence_diagnostics":persistence,"peer_group_commit_available":cfg!(feature = "peer-group-commit"),
        "storage":"rafter native file stores","hard_state_backend":HARD_STATE_BACKEND,"election_ticks":"50 + 7*(node_id-1)"})
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let config = Config::load()?;
    if config.ordered_apply && !cfg!(feature = "ordered-apply") {
        anyhow::bail!("ordered apply is unavailable in this build")
    }
    if config.pipelined_durability && !cfg!(feature = "pipelined-durability") {
        anyhow::bail!("pipelined durability is unavailable in this build")
    }
    if config.pipelined_durability && (!config.ordered_apply || !config.peer_message_stream) {
        anyhow::bail!("pipelined durability requires ordered application and message transport")
    }
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
    let node_config = NodeConfig::new(NodeId(config.id), peers, 50 + 7 * (config.id - 1))?
        .with_max_inflight_appends(config.max_inflight_appends);
    let recovered = Node::recover_with_storage_and_snapshot_store_applied_through(
        node_config,
        hard,
        log,
        snap,
        LogIndex(model.index),
    )?;
    let (node, outputs) = recovered.into_parts();
    let snapshot_group = SnapshotGroupId::new(config.cluster.clone())?;
    let driver = driver::Driver::new(
        node,
        config.pipelined_durability,
        config.diagnostics,
        config.max_speculative_proposals,
    )?;
    let recovered_effects = effects(Some(driver.ready()), outputs, config.ordered_apply)?;
    let handler = Actor::start(
        config.clone(),
        "rafter",
        Rafter {
            node: driver,
            node_id: NodeId(config.id),
            snapshot_group,
            ordered_apply: config.ordered_apply,
            empty_appends: diagnostics::EmptyAppends::new(config.diagnostics),
            replication_windows: diagnostics::ReplicationWindows::new(config.diagnostics),
        },
        model,
        recovered_effects,
    );
    net::serve(config, handler).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafter::{AppendEntriesResponse, LogIndex, Term};

    fn response(term: u64) -> (NodeId, Message) {
        (
            NodeId(2),
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(term),
                follower_id: NodeId(2),
                success: true,
                match_index: LogIndex(1),
                sequence: 1,
            }),
        )
    }

    #[test]
    fn only_same_term_leader_append_responses_admit_ready_proposals() {
        assert!(can_batch_same_term_append_responses(
            Role::Leader,
            Term(3),
            &[response(3)]
        ));
        assert!(!can_batch_same_term_append_responses(
            Role::Follower,
            Term(3),
            &[response(3)]
        ));
        assert!(!can_batch_same_term_append_responses(
            Role::Leader,
            Term(3),
            &[response(4)]
        ));
    }
}
