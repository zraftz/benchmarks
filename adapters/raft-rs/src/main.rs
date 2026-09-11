//! raft-rs RawNode with a synced benchmark journal mirrored into MemStorage.
//! Conservative Ready processing: sync required data before releasing messages.
//! This is a declared embedding baseline, not TiKV's production storage stack.
use anyhow::{bail, Result};
use bench_common::{
    actor::{Actor, Effect, Engine},
    journal::Journal,
    model::{Applied, Command, DurableModel},
    net, Config,
};
use prost::Message as ProstMessage;
use raft::{
    prelude::{Config as RaftConfig, Entry, HardState, Message, Snapshot},
    storage::MemStorage,
    RawNode, StateRole, Storage,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Record {
    entries: Vec<Vec<u8>>,
    hard: Option<Vec<u8>>,
}
struct RaftRs {
    node: RawNode<MemStorage>,
    journal: Journal,
    ordered_apply: bool,
}
impl RaftRs {
    fn open(c: &Config, applied: u64) -> Result<Self> {
        let (journal, records) = Journal::open::<Record>(&c.data_dir.join("raft.wal"))?;
        let store = MemStorage::new_with_conf_state((
            c.peers.keys().copied().collect::<Vec<_>>(),
            Vec::<u64>::new(),
        ));
        for record in records {
            let entries = record
                .entries
                .iter()
                .map(|e| Entry::decode(e.as_slice()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            store.wl().append(&entries)?;
            if let Some(h) = record.hard {
                store.wl().set_hardstate(HardState::decode(h.as_slice())?);
            }
        }
        // An application record was synced only after commitment. Recovering it is
        // additional durable commit evidence if LightReady's commit pointer lagged.
        let mut hard = store.initial_state()?.hard_state;
        hard.commit = hard.commit.max(applied);
        store.wl().set_hardstate(hard);
        let cfg = RaftConfig {
            id: c.id,
            election_tick: (50 + 7 * (c.id - 1)) as usize,
            heartbeat_tick: 5,
            applied,
            pre_vote: true,
            check_quorum: true,
            max_size_per_msg: 512 * 1024,
            max_inflight_msgs: 256,
            ..RaftConfig::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let node = RawNode::new(&cfg, store, &logger)?;
        Ok(Self {
            node,
            journal,
            ordered_apply: c.ordered_apply,
        })
    }
    fn committed(entries: Vec<Entry>, out: &mut Vec<Effect>, ordered: bool) -> Result<()> {
        for entry in entries {
            if entry.get_entry_type() != raft::prelude::EntryType::EntryNormal {
                bail!("dynamic membership not supported by this scenario");
            }
            let command = if ordered || entry.data.is_empty() {
                None
            } else {
                Some(serde_json::from_slice::<Command>(&entry.data)?)
            };
            out.push(Effect::Apply {
                index: entry.index,
                command,
            });
        }
        Ok(())
    }
    fn messages(messages: Vec<Message>, out: &mut Vec<Effect>) {
        for m in messages {
            out.push(Effect::Send {
                to: m.to,
                data: m.encode_to_vec(),
            });
        }
    }
    fn drain(&mut self) -> Result<Vec<Effect>> {
        let mut out = Vec::new();
        while self.node.has_ready() {
            let mut ready = self.node.ready();
            if *ready.snapshot() != Snapshot::default() {
                bail!("snapshot received in log-only benchmark");
            }
            let store = self.node.raft.raft_log.store.clone();
            if !ready.entries().is_empty() || ready.hs().is_some() {
                let record = Record {
                    entries: ready.entries().iter().map(|e| e.encode_to_vec()).collect(),
                    hard: ready.hs().map(|h| h.encode_to_vec()),
                };
                self.journal.append(&record)?;
                store.wl().append(ready.entries())?;
                if let Some(hard) = ready.hs() {
                    store.wl().set_hardstate(hard.clone());
                }
            }
            Self::messages(ready.take_messages(), &mut out);
            Self::messages(ready.take_persisted_messages(), &mut out);
            Self::committed(ready.take_committed_entries(), &mut out, self.ordered_apply)?;
            let mut light = self.node.advance_append(ready);
            if let Some(commit) = light.commit_index() {
                store.wl().mut_hard_state().set_commit(commit);
            }
            Self::messages(light.take_messages(), &mut out);
            Self::committed(light.take_committed_entries(), &mut out, self.ordered_apply)?;
        }
        Ok(out)
    }
}
impl Engine for RaftRs {
    fn term(&self) -> u64 {
        self.node.raft.term
    }
    fn applied(&mut self, index: u64) {
        self.node.advance_apply_to(index);
    }
    fn committed_through(&self) -> u64 {
        self.node.raft.raft_log.committed
    }
    fn last_index(&self) -> u64 {
        self.node.raft.raft_log.last_index()
    }
    fn committed_entries(&self, after: u64, count: usize, bytes: usize) -> Result<Vec<Applied>> {
        let through = self.committed_through().min(after + count as u64);
        if through <= after {
            return Ok(Vec::new());
        }
        let entries = self.node.store().entries(
            after + 1,
            through + 1,
            Some(bytes as u64),
            raft::GetEntriesContext::empty(false),
        )?;
        let mut result = Vec::new();
        let mut used = 0;
        for entry in entries {
            if entry.get_entry_type() != raft::prelude::EntryType::EntryNormal {
                bail!("unsupported committed configuration")
            }
            let applied = Applied {
                index: entry.index,
                command: if entry.data.is_empty() {
                    None
                } else {
                    Some(serde_json::from_slice(&entry.data)?)
                },
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
    type Peer = Message;
    type Gate = ();
    fn peer_gate(&self, _: usize, _: usize) {}
    fn admit_peer(_: &mut (), _: &Message) -> bool {
        false
    }
    fn decode_peer(&self, from: u64, data: &[u8]) -> Result<Message> {
        let message = Message::decode(data)?;
        if message.from != from || message.to != self.node.raft.id {
            bail!("peer envelope identity mismatch");
        }
        Ok(message)
    }
    fn leader(&self) -> Option<u64> {
        let id = self.node.raft.leader_id;
        (id != 0).then_some(id)
    }
    fn is_leader(&self) -> bool {
        self.node.raft.state == StateRole::Leader
    }
    fn tick(&mut self) -> Result<Vec<Effect>> {
        self.node.tick();
        self.drain()
    }
    fn peer_batch(&mut self, peers: Vec<Message>) -> Result<Vec<Effect>> {
        let mut out = Vec::new();
        for message in peers {
            self.node.step(message)?;
            out.extend(self.drain()?);
        }
        Ok(out)
    }
    fn propose(&mut self, commands: Vec<Command>) -> Result<Vec<Effect>> {
        for c in commands {
            self.node.propose(Vec::new(), serde_json::to_vec(&c)?)?;
        }
        self.drain()
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::json!({"term":self.node.raft.term,
        "commit_index":self.node.raft.raft_log.committed,"raft_syncs":self.journal.syncs,
        "raft_bytes":self.journal.bytes,"storage":"benchmark journal + MemStorage",
        "election_ticks":"50 + 7*(node_id-1), raft-rs randomized timeout"})
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let config = Config::load()?;
    if config.pipelined_durability {
        anyhow::bail!("pipelined durability is a Rafter-only mode")
    }

    let _lock = config.lock_directory("raft-rs")?;
    let model = DurableModel::open(&config.data_dir.join("application.wal"))?;
    let mut engine = RaftRs::open(&config, model.index)?;
    let recovered = engine.drain()?;
    let handler = Actor::start(config.clone(), "raft-rs", engine, model, recovered);
    net::serve(config, handler).await
}
