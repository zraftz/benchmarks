//! Replicated KV model with bounded-per-session retry deduplication.
//! Every client session has one outstanding operation at a time. Sequence reuse
//! with different contents and stale retries are rejected, never executed again.
use crate::journal::Journal;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub client: String,
    pub sequence: u64,
    pub kind: String,
    pub key: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub expected: Option<String>,
}
impl Command {
    pub fn validate(&self) -> Result<()> {
        if self.client.is_empty()
            || self.client.len() > 128
            || self.sequence == 0
            || self.key.is_empty()
            || self.key.len() > 256
            || self.value.len() > 64 * 1024
            || self.expected.as_ref().is_some_and(|s| s.len() > 64 * 1024)
            || !["put", "get", "cas"].contains(&self.kind.as_str())
        {
            bail!("invalid command");
        }
        Ok(())
    }
    pub fn identity(&self) -> (String, u64) {
        (self.client.clone(), self.sequence)
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Outcome {
    pub value: Option<String>,
    pub swapped: Option<bool>,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Session {
    sequence: u64,
    command: Command,
    outcome: Outcome,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Model {
    pub values: BTreeMap<String, String>,
    sessions: BTreeMap<String, Session>,
}

const APPLICATION_SNAPSHOT_SCHEMA: u32 = 1;

/// Complete durable application state at one applied Raft-log boundary.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationSnapshot {
    snapshot_schema: u32,
    pub applied_index: u64,
    model: Model,
    metadata: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ApplicationRecord {
    // Ordinary records retain the legacy JSON-array representation so old
    // application journals reopen without migration.
    Entries(Vec<Applied>),
    Snapshot(ApplicationSnapshot),
}

impl ApplicationSnapshot {
    fn validate(&self) -> Result<()> {
        if self.snapshot_schema != APPLICATION_SNAPSHOT_SCHEMA {
            bail!(
                "unsupported application snapshot schema {}",
                self.snapshot_schema
            );
        }
        if self.applied_index == 0 {
            bail!("application snapshot boundary must be nonzero");
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        let snapshot: Self = serde_json::from_slice(payload)?;
        snapshot.validate()?;
        Ok(snapshot)
    }
}
impl Model {
    pub fn apply(&mut self, cmd: &Command) -> Outcome {
        if let Some(old) = self.sessions.get(&cmd.client) {
            if cmd.sequence < old.sequence {
                return Outcome {
                    error: Some("stale_sequence".into()),
                    ..Outcome::default()
                };
            }
            if cmd.sequence == old.sequence {
                return if cmd == &old.command {
                    old.outcome.clone()
                } else {
                    Outcome {
                        error: Some("identity_conflict".into()),
                        ..Outcome::default()
                    }
                };
            }
        }
        let mut out = Outcome::default();
        match cmd.kind.as_str() {
            "put" => {
                self.values.insert(cmd.key.clone(), cmd.value.clone());
            }
            "get" => {}
            "cas" => {
                let matches = self.values.get(&cmd.key) == cmd.expected.as_ref();
                out.swapped = Some(matches);
                if matches {
                    self.values.insert(cmd.key.clone(), cmd.value.clone());
                }
            }
            _ => {
                out.error = Some("invalid_command".into());
                return out;
            }
        }
        out.value = self.values.get(&cmd.key).cloned();
        self.sessions.insert(
            cmd.client.clone(),
            Session {
                sequence: cmd.sequence,
                command: cmd.clone(),
                outcome: out.clone(),
            },
        );
        out
    }
    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Applied {
    pub index: u64,
    pub command: Option<Command>,
    /// Adapter-specific committed metadata, atomically journaled with application state.
    pub metadata: Option<serde_json::Value>,
}
#[derive(Debug)]
pub struct DurableModel {
    journal: Journal,
    pub model: Model,
    pub index: u64,
    has_applied: bool,
    pub metadata: Option<serde_json::Value>,
}
impl DurableModel {
    pub fn open(path: &Path) -> Result<Self> {
        let (journal, records) = Journal::open::<ApplicationRecord>(path)?;
        let mut s = Self {
            journal,
            model: Model::default(),
            index: 0,
            has_applied: false,
            metadata: None,
        };
        for record in records {
            match record {
                ApplicationRecord::Entries(batch) => {
                    s.replay(&batch)?;
                }
                ApplicationRecord::Snapshot(snapshot) => s.restore_snapshot(snapshot)?,
            }
        }
        Ok(s)
    }
    fn restore_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<()> {
        snapshot.validate()?;
        if self.has_applied && snapshot.applied_index < self.index {
            bail!(
                "application snapshot index regression: {} < {}",
                snapshot.applied_index,
                self.index
            );
        }
        if self.has_applied
            && snapshot.applied_index == self.index
            && (snapshot.model != self.model || snapshot.metadata != self.metadata)
        {
            bail!("application snapshot conflicts at index {}", self.index);
        }
        self.index = snapshot.applied_index;
        self.has_applied = true;
        self.model = snapshot.model;
        self.metadata = snapshot.metadata;
        Ok(())
    }
    fn replay(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>> {
        let mut out = Vec::new();
        for entry in entries {
            if self.has_applied && entry.index <= self.index {
                bail!("application journal index is not increasing");
            }
            self.index = entry.index;
            self.has_applied = true;
            if let Some(m) = &entry.metadata {
                self.metadata = Some(m.clone());
            }
            out.push(entry.command.as_ref().map(|c| self.model.apply(c)));
        }
        Ok(out)
    }
    pub fn apply(&mut self, entries: &[Applied]) -> Result<Vec<Option<Outcome>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut index = self.index;
        let mut any = self.has_applied;
        for e in entries {
            if any && e.index <= index {
                bail!("application index regression: {} <= {index}", e.index);
            }
            if let Some(c) = &e.command {
                c.validate()?;
            }
            index = e.index;
            any = true;
        }
        self.journal.append(entries)?;
        self.replay(entries)
    }
    pub fn snapshot(&self) -> ApplicationSnapshot {
        ApplicationSnapshot {
            snapshot_schema: APPLICATION_SNAPSHOT_SCHEMA,
            applied_index: self.index,
            model: self.model.clone(),
            metadata: self.metadata.clone(),
        }
    }
    /// Durably replaces application state with an authoritative Raft snapshot.
    pub fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<()> {
        snapshot.validate()?;
        if self.has_applied && snapshot.applied_index < self.index {
            bail!(
                "application snapshot index regression: {} < {}",
                snapshot.applied_index,
                self.index
            );
        }
        if self.has_applied
            && snapshot.applied_index == self.index
            && snapshot.model == self.model
            && snapshot.metadata == self.metadata
        {
            return Ok(());
        }
        if self.has_applied && snapshot.applied_index == self.index {
            bail!("application snapshot conflicts at index {}", self.index);
        }
        self.journal.append(&snapshot)?;
        self.restore_snapshot(snapshot)
    }
    pub fn set_diagnostics(&mut self, diagnostics: crate::diagnostics::Diagnostics) {
        self.journal.diagnostics = diagnostics;
    }
    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!({"applied_index":self.index,"keys":self.model.values.len(),
            "sessions":self.model.sessions(),"application_syncs":self.journal.syncs,
            "application_bytes":self.journal.bytes,"diagnostics":self.journal.diagnostics.snapshot()})
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "raft-bench-model-{label}-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn command(n: u64, kind: &str, value: &str) -> Command {
        Command {
            client: "c".into(),
            sequence: n,
            kind: kind.into(),
            key: "k".into(),
            value: value.into(),
            expected: None,
        }
    }
    #[test]
    fn retries_do_not_execute_again() {
        let mut m = Model::default();
        let c = command(1, "cas", "a");
        let result = m.apply(&c);
        assert_eq!(result.swapped, Some(true));
        assert_eq!(m.apply(&c), result);
        assert_eq!(
            m.apply(&command(1, "put", "b")).error.as_deref(),
            Some("identity_conflict")
        );
        m.apply(&command(2, "put", "b"));
        assert_eq!(m.apply(&c).error.as_deref(), Some("stale_sequence"));
        assert_eq!(m.values.get("k").map(String::as_str), Some("b"));
    }
    #[test]
    fn reads_and_failed_cas() {
        let mut m = Model::default();
        m.apply(&command(1, "put", "a"));
        assert_eq!(m.apply(&command(2, "get", "")).value.as_deref(), Some("a"));
        assert_eq!(m.apply(&command(3, "cas", "b")).swapped, Some(false));
    }

    #[test]
    fn installed_snapshot_and_following_entries_survive_reopen() {
        let source_path = path("snapshot-source");
        let target_path = path("snapshot-target");
        let mut source = DurableModel::open(&source_path).unwrap();
        source
            .apply(&[Applied {
                index: 1,
                command: Some(command(1, "put", "before")),
                metadata: None,
            }])
            .unwrap();
        let encoded = source.snapshot().encode().unwrap();
        let snapshot = ApplicationSnapshot::decode(&encoded).unwrap();

        let mut target = DurableModel::open(&target_path).unwrap();
        target.install_snapshot(snapshot).unwrap();
        target
            .apply(&[Applied {
                index: 2,
                command: Some(command(2, "put", "after")),
                metadata: None,
            }])
            .unwrap();
        drop(target);

        let reopened = DurableModel::open(&target_path).unwrap();
        assert_eq!(reopened.index, 2);
        assert_eq!(
            reopened.model.values.get("k").map(String::as_str),
            Some("after")
        );
        std::fs::remove_file(source_path).unwrap();
        std::fs::remove_file(target_path).unwrap();
    }

    #[test]
    fn snapshot_install_refuses_application_regression() {
        let older_path = path("snapshot-older");
        let newer_path = path("snapshot-newer");
        let mut older = DurableModel::open(&older_path).unwrap();
        older
            .apply(&[Applied {
                index: 1,
                command: Some(command(1, "put", "old")),
                metadata: None,
            }])
            .unwrap();
        let snapshot = older.snapshot();

        let mut newer = DurableModel::open(&newer_path).unwrap();
        newer
            .apply(&[
                Applied {
                    index: 1,
                    command: Some(command(1, "put", "old")),
                    metadata: None,
                },
                Applied {
                    index: 2,
                    command: Some(command(2, "put", "new")),
                    metadata: None,
                },
            ])
            .unwrap();
        assert!(newer.install_snapshot(snapshot).is_err());
        assert_eq!(newer.index, 2);
        assert_eq!(newer.model.values.get("k").map(String::as_str), Some("new"));
        std::fs::remove_file(older_path).unwrap();
        std::fs::remove_file(newer_path).unwrap();
    }

    #[test]
    fn conflicting_equal_index_snapshot_does_not_touch_journal() {
        let source_path = path("snapshot-conflict-source");
        let target_path = path("snapshot-conflict-target");
        let mut source = DurableModel::open(&source_path).unwrap();
        source
            .apply(&[Applied {
                index: 1,
                command: Some(command(1, "put", "source")),
                metadata: None,
            }])
            .unwrap();
        let mut target = DurableModel::open(&target_path).unwrap();
        target
            .apply(&[Applied {
                index: 1,
                command: Some(command(1, "put", "target")),
                metadata: None,
            }])
            .unwrap();
        let before = std::fs::metadata(&target_path).unwrap().len();
        assert!(target.install_snapshot(source.snapshot()).is_err());
        assert_eq!(std::fs::metadata(&target_path).unwrap().len(), before);
        drop(target);
        assert_eq!(
            DurableModel::open(&target_path).unwrap().model.values["k"],
            "target"
        );
        std::fs::remove_file(source_path).unwrap();
        std::fs::remove_file(target_path).unwrap();
    }

    #[test]
    fn snapshot_decoder_rejects_unknown_schema() {
        let source_path = path("snapshot-schema");
        let mut source = DurableModel::open(&source_path).unwrap();
        source
            .apply(&[Applied {
                index: 1,
                command: Some(command(1, "put", "value")),
                metadata: None,
            }])
            .unwrap();
        let mut value = serde_json::to_value(source.snapshot()).unwrap();
        value["snapshot_schema"] = 2.into();
        assert!(ApplicationSnapshot::decode(&serde_json::to_vec(&value).unwrap()).is_err());
        std::fs::remove_file(source_path).unwrap();
    }

    #[test]
    fn ordinary_application_record_retains_legacy_array_payload() {
        let model_path = path("legacy-entry-payload");
        let mut model = DurableModel::open(&model_path).unwrap();
        let entries = vec![Applied {
            index: 1,
            command: Some(command(1, "put", "value")),
            metadata: None,
        }];
        model.apply(&entries).unwrap();
        drop(model);

        let bytes = std::fs::read(&model_path).unwrap();
        let payload_len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(payload_len, bytes.len() - 8);
        assert_eq!(&bytes[8..], serde_json::to_vec(&entries).unwrap());
        std::fs::remove_file(model_path).unwrap();
    }

    #[test]
    fn application_snapshot_record_retains_legacy_object_payload() {
        let source_path = path("snapshot-payload-source");
        let target_path = path("snapshot-payload-target");
        let mut source = DurableModel::open(&source_path).unwrap();
        source
            .apply(&[Applied {
                index: 1,
                command: Some(command(1, "put", "value")),
                metadata: Some(serde_json::json!({"term": 1})),
            }])
            .unwrap();
        let snapshot = source.snapshot();
        let expected = serde_json::to_vec(&ApplicationRecord::Snapshot(snapshot.clone())).unwrap();
        assert_eq!(expected, serde_json::to_vec(&snapshot).unwrap());

        let mut target = DurableModel::open(&target_path).unwrap();
        target.install_snapshot(snapshot).unwrap();
        drop(target);
        let bytes = std::fs::read(&target_path).unwrap();
        let payload_len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(payload_len, bytes.len() - 8);
        assert_eq!(&bytes[8..], expected);
        let reopened = DurableModel::open(&target_path).unwrap();
        assert_eq!(reopened.index, 1);
        assert_eq!(
            reopened.model.values.get("k").map(String::as_str),
            Some("value")
        );
        assert_eq!(reopened.metadata, Some(serde_json::json!({"term": 1})));
        drop(reopened);

        std::fs::remove_file(source_path).unwrap();
        std::fs::remove_file(target_path).unwrap();
    }
}
