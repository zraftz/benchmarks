//! Synced benchmark log and application journals. No compaction/snapshots in v1.
use crate::Types;
use anyhow::Result;
use bench_common::{
    diagnostics::Diagnostics,
    journal::Journal,
    model::{Applied, DurableModel, Outcome},
};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{self, Cursor},
    ops::RangeBounds,
    path::Path,
    sync::{Arc, Mutex},
};

fn failure(e: impl ToString) -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Write,
        io::Error::other(e.to_string()),
    )
}
fn unsupported() -> StorageError<u64> {
    failure("snapshot/compaction unsupported: this is a retained-log benchmark")
}
#[derive(Debug, Serialize, Deserialize)]
enum Record {
    Append(Vec<Entry<Types>>),
    Vote(Vote<u64>),
    Committed(Option<LogId<u64>>),
    Truncate(LogId<u64>),
}
#[derive(Debug)]
struct LogInner {
    journal: Journal,
    logs: BTreeMap<u64, Entry<Types>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
}
impl LogInner {
    fn replay(&mut self, r: Record) {
        match r {
            Record::Append(entries) => {
                for e in entries {
                    self.logs.insert(e.log_id.index, e);
                }
            }
            Record::Vote(v) => self.vote = Some(v),
            Record::Committed(c) => self.committed = c,
            Record::Truncate(id) => {
                self.logs.split_off(&id.index);
            }
        }
    }
    fn write(&mut self, r: Record) -> Result<()> {
        self.journal.append(&r)?;
        self.replay(r);
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct LogStore {
    inner: Arc<Mutex<LogInner>>,
}
impl LogStore {
    pub fn open(path: &Path) -> Result<Self> {
        let (journal, records) = Journal::open::<Record>(path)?;
        let mut inner = LogInner {
            journal,
            logs: BTreeMap::new(),
            vote: None,
            committed: None,
        };
        for r in records {
            inner.replay(r);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
    pub fn stats(&self) -> serde_json::Value {
        let i = self.inner.lock().unwrap();
        serde_json::json!({"raft_syncs":i.journal.syncs,
            "raft_bytes":i.journal.bytes,"retained_entries":i.logs.len(),"storage":"benchmark journal + BTreeMap"})
    }
}
impl RaftLogReader<Types> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<Types>>, StorageError<u64>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .logs
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}
impl RaftLogStorage<Types> for LogStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> std::result::Result<LogState<Types>, StorageError<u64>> {
        let i = self.inner.lock().unwrap();
        Ok(LogState {
            last_purged_log_id: None,
            last_log_id: i.logs.values().next_back().map(|e| e.log_id),
        })
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, v: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        self.inner
            .lock()
            .unwrap()
            .write(Record::Vote(*v))
            .map_err(failure)
    }
    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().vote)
    }
    async fn save_committed(
        &mut self,
        c: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.inner
            .lock()
            .unwrap()
            .write(Record::Committed(c))
            .map_err(failure)
    }
    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().committed)
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<Types>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<Types>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect();
        let result = self.inner.lock().unwrap().write(Record::Append(entries));
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.log_io_completed(Err(io::Error::other(e.to_string())));
                Err(failure(e))
            }
        }
    }
    async fn truncate(&mut self, id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        self.inner
            .lock()
            .unwrap()
            .write(Record::Truncate(id))
            .map_err(failure)
    }
    async fn purge(&mut self, _id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        Err(unsupported())
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Meta {
    last: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
}
#[derive(Debug)]
struct AppInner {
    app: DurableModel,
    meta: Meta,
}
#[derive(Clone, Debug)]
pub struct StateMachine {
    inner: Arc<Mutex<AppInner>>,
    diagnostics: Diagnostics,
}
impl StateMachine {
    pub fn open(path: &Path, diagnostics: Diagnostics) -> Result<Self> {
        let mut app = DurableModel::open(path)?;
        app.set_diagnostics(diagnostics.clone());
        let meta = match &app.metadata {
            Some(v) => serde_json::from_value(v.clone())?,
            None => Meta::default(),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(AppInner { app, meta })),
            diagnostics,
        })
    }
    pub fn stats(&self) -> serde_json::Value {
        self.inner.lock().unwrap().app.stats()
    }
    pub fn dump(&self) -> serde_json::Value {
        let i = self.inner.lock().unwrap();
        serde_json::json!({"values":i.app.model.values,"applied_index":i.app.index})
    }
}
impl RaftSnapshotBuilder<Types> for StateMachine {
    async fn build_snapshot(&mut self) -> std::result::Result<Snapshot<Types>, StorageError<u64>> {
        Err(unsupported())
    }
}
impl RaftStateMachine<Types> for StateMachine {
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        let i = self.inner.lock().unwrap();
        Ok((i.meta.last, i.meta.membership.clone()))
    }
    async fn apply<I>(&mut self, entries: I) -> std::result::Result<Vec<Outcome>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<Types>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut meta = inner.meta.clone();
        let mut records = Vec::new();
        for entry in entries {
            self.diagnostics
                .raft_commit_observed(entry.log_id.index, "state_machine_apply_callback");
            meta.last = Some(entry.log_id);
            let command = match entry.payload {
                EntryPayload::Blank => None,
                EntryPayload::Normal(c) => Some(c),
                EntryPayload::Membership(m) => {
                    meta.membership = StoredMembership::new(Some(entry.log_id), m);
                    None
                }
            };
            records.push(Applied {
                index: entry.log_id.index,
                command,
                metadata: None,
            });
        }
        if let Some(last) = records.last_mut() {
            last.metadata = Some(serde_json::to_value(&meta).map_err(failure)?);
        }
        self.diagnostics.application_dispatched(&records);
        self.diagnostics.application_started(&records);
        let outcomes = inner.app.apply(&records).map_err(failure)?;
        self.diagnostics.application_durable(&records);
        inner.meta = meta;
        Ok(outcomes
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect())
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Err(unsupported())
    }
    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        Err(unsupported())
    }
    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<Types>>, StorageError<u64>> {
        Ok(None)
    }
}
