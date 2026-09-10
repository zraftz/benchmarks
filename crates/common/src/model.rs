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
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Session {
    sequence: u64,
    command: Command,
    outcome: Outcome,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Model {
    pub values: BTreeMap<String, String>,
    sessions: BTreeMap<String, Session>,
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
        let (journal, batches) = Journal::open::<Vec<Applied>>(path)?;
        let mut s = Self {
            journal,
            model: Model::default(),
            index: 0,
            has_applied: false,
            metadata: None,
        };
        for batch in batches {
            s.replay(&batch)?;
        }
        Ok(s)
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
        self.journal.append(&entries)?;
        self.replay(entries)
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
}
