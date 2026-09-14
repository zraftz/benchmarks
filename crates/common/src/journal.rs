//! Checksum-framed journal with atomic single-record checkpoints. Each append is synced
//! before return. Incomplete EOF frames are truncated on open. A complete frame with a
//! bad checksum is rejected, including at EOF: corruption is never silently treated as
//! a clean log.
use crate::diagnostics::Diagnostics;
use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MAX_RECORD: usize = 64 * 1024 * 1024;
const RETAINED_FRAME_CAPACITY: usize = 4 * 1024 * 1024;
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    file: File,
    frame: Vec<u8>,
    poisoned: bool,
    pub diagnostics: Diagnostics,
    pub syncs: u64,
    pub bytes: u64,
}
impl Journal {
    pub fn open<T: DeserializeOwned>(path: &Path) -> Result<(Self, Vec<T>)> {
        let existed = path.exists();
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        if !existed {
            f.sync_all()?;
            if let Some(p) = path.parent() {
                File::open(p)?.sync_all()?;
            }
        }
        let length = f.metadata()?.len();
        let mut valid = 0u64;
        let mut records = Vec::new();
        let mut bytes = Vec::new();
        while valid < length {
            if length - valid < 8 {
                break;
            }
            let mut header = [0; 8];
            f.read_exact(&mut header)?;
            let size = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            let checksum = u32::from_be_bytes(header[4..].try_into().unwrap());
            if size == 0
                && header.iter().all(|byte| *byte == 0)
                && zero_filled(&mut f, length - valid - 8)?
            {
                // A killed append can leave its newly allocated EOF extent
                // visible as zeros even though no frame bytes reached storage.
                // Prior appends are serialized and synced before the next starts,
                // so this is the incomplete current append, not a valid record.
                break;
            }
            if size == 0 || size > MAX_RECORD {
                bail!("invalid journal record length at {valid}");
            }
            if length - valid - 8 < size as u64 {
                break;
            }
            bytes.resize(size, 0);
            f.read_exact(&mut bytes)?;
            if crc32(&bytes) != checksum {
                bail!("journal checksum mismatch at {valid}");
            }
            records.push(serde_json::from_slice(&bytes)?);
            clear_frame(&mut bytes);
            valid += 8 + size as u64;
        }
        if valid < length {
            f.set_len(valid)?;
            f.sync_all()?;
        }
        f.seek(SeekFrom::Start(valid))?;
        Ok((
            Self {
                path: path.to_path_buf(),
                file: f,
                frame: Vec::new(),
                poisoned: false,
                diagnostics: Diagnostics::default(),
                syncs: 0,
                bytes: 0,
            },
            records,
        ))
    }
    pub fn append<T: Serialize + ?Sized>(&mut self, record: &T) -> Result<()> {
        if self.poisoned {
            bail!("application journal is poisoned after failed checkpoint publication");
        }
        let started = self.diagnostics.start();
        debug_assert!(self.frame.is_empty());
        self.frame.resize(8, 0);
        if let Err(error) = serde_json::to_writer(&mut self.frame, record) {
            clear_frame(&mut self.frame);
            return Err(error.into());
        }
        let payload_len = self.frame.len() - 8;
        if payload_len == 0 || payload_len > MAX_RECORD {
            clear_frame(&mut self.frame);
            bail!("journal record exceeds limit");
        }
        let checksum = crc32(&self.frame[8..]);
        self.frame[..4].copy_from_slice(&(payload_len as u32).to_be_bytes());
        self.frame[4..8].copy_from_slice(&checksum.to_be_bytes());
        self.diagnostics.elapsed("journal_encode_ns", started);
        let started = self.diagnostics.start();
        let frame_len = self.frame.len();
        let write_result = self.file.write_all(&self.frame);
        clear_frame(&mut self.frame);
        write_result?;
        self.diagnostics.elapsed("journal_write_ns", started);
        let started = self.diagnostics.start();
        self.file.sync_data()?;
        self.diagnostics.elapsed("journal_sync_ns", started);
        self.syncs += 1;
        self.bytes += frame_len as u64;
        Ok(())
    }

    /// Replaces all prior records with one already-encoded record.
    ///
    /// The temporary file is synced before rename and the parent directory is
    /// synced before this writer resumes. Any failure poisons the current
    /// writer; reopening uses only the authoritative path and never the temp.
    pub fn checkpoint_payload(&mut self, payload: &[u8]) -> Result<()> {
        self.checkpoint_payload_inner(payload, CheckpointFailure::None)
    }

    fn checkpoint_payload_inner(
        &mut self,
        payload: &[u8],
        failure: CheckpointFailure,
    ) -> Result<()> {
        if self.poisoned {
            bail!("application journal is poisoned after failed checkpoint publication");
        }
        if payload.is_empty() || payload.len() > MAX_RECORD {
            bail!("journal checkpoint record exceeds limit");
        }
        debug_assert!(self.frame.is_empty());
        self.frame.resize(8, 0);
        self.frame.extend_from_slice(payload);
        self.frame[..4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        self.frame[4..8].copy_from_slice(&crc32(payload).to_be_bytes());
        let frame_len = self.frame.len();
        let temporary = checkpoint_path(&self.path);
        self.poisoned = true;

        let started = self.diagnostics.start();
        let result = (|| -> Result<File> {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&self.frame)?;
            self.diagnostics
                .elapsed("journal_checkpoint_write_ns", started);

            let started = self.diagnostics.start();
            file.sync_all()?;
            self.diagnostics
                .elapsed("journal_checkpoint_sync_ns", started);
            if failure == CheckpointFailure::AfterTemporarySync {
                bail!("injected failure after application checkpoint sync");
            }

            let started = self.diagnostics.start();
            fs::rename(&temporary, &self.path)?;
            if failure == CheckpointFailure::AfterRename {
                bail!("injected failure after application checkpoint rename");
            }
            if let Some(parent) = self.path.parent() {
                File::open(parent)?.sync_all()?;
            }
            self.diagnostics
                .elapsed("journal_checkpoint_publish_ns", started);

            let mut reopened = OpenOptions::new().read(true).write(true).open(&self.path)?;
            reopened.seek(SeekFrom::End(0))?;
            Ok(reopened)
        })();
        clear_frame(&mut self.frame);
        let reopened = result?;
        self.file = reopened;
        self.poisoned = false;
        self.syncs = self.syncs.saturating_add(1);
        self.bytes = self.bytes.saturating_add(frame_len as u64);
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckpointFailure {
    None,
    AfterTemporarySync,
    AfterRename,
}

fn checkpoint_path(path: &Path) -> PathBuf {
    let mut value = OsString::from(path.as_os_str());
    value.push(".checkpoint.tmp");
    PathBuf::from(value)
}

fn clear_frame(frame: &mut Vec<u8>) {
    if frame.capacity() > RETAINED_FRAME_CAPACITY {
        *frame = Vec::new();
    } else {
        frame.clear();
    }
}

fn zero_filled(file: &mut File, mut remaining: u64) -> Result<bool> {
    let mut buffer = [0u8; 8192];
    while remaining != 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..length])?;
        if buffer[..length].iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
        remaining -= length as u64;
    }
    Ok(true)
}

pub fn crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}
#[cfg(test)]
#[path = "journal_test.rs"]
mod tests;
