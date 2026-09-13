//! Append-only checksum-framed journal. Each appended batch is synced before return.
//! Incomplete EOF frames are truncated on open. A complete frame with a bad checksum
//! is rejected, including at EOF: corruption is never silently treated as a clean log.
use crate::diagnostics::Diagnostics;
use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

const MAX_RECORD: usize = 64 * 1024 * 1024;
const RETAINED_FRAME_CAPACITY: usize = 4 * 1024 * 1024;
#[derive(Debug)]
pub struct Journal {
    file: File,
    frame: Vec<u8>,
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
            let mut bytes = vec![0; size];
            f.read_exact(&mut bytes)?;
            if crc32(&bytes) != checksum {
                bail!("journal checksum mismatch at {valid}");
            }
            records.push(serde_json::from_slice(&bytes)?);
            valid += 8 + size as u64;
        }
        if valid < length {
            f.set_len(valid)?;
            f.sync_all()?;
        }
        f.seek(SeekFrom::Start(valid))?;
        Ok((
            Self {
                file: f,
                frame: Vec::new(),
                diagnostics: Diagnostics::default(),
                syncs: 0,
                bytes: 0,
            },
            records,
        ))
    }
    pub fn append<T: Serialize>(&mut self, record: &T) -> Result<()> {
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
