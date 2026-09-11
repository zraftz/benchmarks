//! Shared *benchmark embedding*, not a production Raft transport or database.
//! Every adapter uses the same framed client protocol and durable application journal.
pub mod actor;
pub mod apply_worker;
pub mod diagnostics;
pub mod journal;
pub mod model;
pub mod net;

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::PathBuf,
};

pub const FRAME_LIMIT: usize = 8 * 1024 * 1024;
pub const CONTRACT: &str = "durable-log+durable-application-v1/logged-reads";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub id: u64,
    pub cluster: String,
    pub client: String,
    pub peer: String,
    pub peers: BTreeMap<u64, String>,
    pub data_dir: PathBuf,
    #[serde(default = "tick_ms")]
    pub tick_ms: u64,
    #[serde(default = "capacity")]
    pub capacity: usize,
    #[serde(default = "batch_size")]
    pub batch_size: usize,
    #[serde(default = "peer_batch_size")]
    pub peer_batch_size: usize,
    #[serde(default)]
    pub diagnostics: bool,
    #[serde(default)]
    pub ordered_apply: bool,
    #[serde(default)]
    pub peer_message_stream: bool,
}
fn peer_batch_size() -> usize {
    1
}
fn tick_ms() -> u64 {
    20
}
fn capacity() -> usize {
    4096
}
fn batch_size() -> usize {
    64
}
impl Config {
    pub fn load() -> Result<Self> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 3 || args[1] != "--config" {
            bail!("usage: {} --config node.json", args[0]);
        }
        let c: Self = serde_json::from_slice(&fs::read(&args[2])?)?;
        if c.id == 0
            || c.cluster.is_empty()
            || !c.peers.contains_key(&c.id)
            || c.peers.keys().copied().collect::<Vec<_>>() != vec![1, 2, 3]
            || c.tick_ms != 20
            || c.capacity == 0
            || (c.ordered_apply && c.capacity < 2)
            || c.capacity > 65536
            || c.batch_size == 0
            || c.batch_size > 64
            || c.peer_batch_size == 0
            || c.peer_batch_size > 64
        {
            bail!("v1 requires voters 1,2,3, 20 ms ticks, capacity <= 65536, batch size 1..64");
        }
        if c.peers.get(&c.id) != Some(&c.peer) {
            bail!("own peer address does not match peers map");
        }
        Ok(c)
    }
    /// Lock the node directory across process lifetimes. A killed process releases the OS lock.
    /// The identity marker rejects accidental reuse for another node, implementation or voter set.
    pub fn lock_directory(&self, implementation: &str) -> Result<File> {
        fs::create_dir_all(&self.data_dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.data_dir.join("LOCK"))?;
        lock.try_lock_exclusive()
            .context("node directory is already in use")?;
        let identity = serde_json::json!({"schema":1,"node":self.id,"cluster":self.cluster,
            "implementation":implementation,"voters":self.peers});
        let path = self.data_dir.join("IDENTITY.json");
        if path.exists() {
            let old: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
            if old != identity {
                bail!("data-directory identity mismatch");
            }
        } else {
            use std::io::Write;
            let mut f = OpenOptions::new().create_new(true).write(true).open(path)?;
            f.write_all(&serde_json::to_vec(&identity)?)?;
            f.sync_all()?;
            File::open(&self.data_dir)?.sync_all()?;
            if let Some(parent) = self.data_dir.parent() {
                File::open(parent)?.sync_all()?;
            }
        }
        Ok(lock)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub op: String,
    #[serde(default)]
    pub command: Option<model::Command>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Reply {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<model::Outcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<serde_json::Value>,
}
impl Reply {
    pub fn status(s: &str) -> Self {
        Self {
            status: s.into(),
            ..Self::default()
        }
    }
    pub fn error(s: impl ToString) -> Self {
        Self {
            status: "error".into(),
            detail: Some(s.to_string()),
            ..Self::default()
        }
    }
    pub fn applied(result: model::Outcome) -> Self {
        Self {
            status: "ok".into(),
            result: Some(result),
            ..Self::default()
        }
    }
}
