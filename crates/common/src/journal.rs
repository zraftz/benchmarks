//! Append-only checksum-framed journal. Each appended batch is synced before return.
//! Incomplete EOF frames are truncated on open. A complete frame with a bad checksum
//! is rejected, including at EOF: corruption is never silently treated as a clean log.
use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

const MAX_RECORD: usize = 64 * 1024 * 1024;
#[derive(Debug)]
pub struct Journal {
    file: File,
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
                syncs: 0,
                bytes: 0,
            },
            records,
        ))
    }
    pub fn append<T: Serialize>(&mut self, record: &T) -> Result<()> {
        let bytes = serde_json::to_vec(record)?;
        if bytes.is_empty() || bytes.len() > MAX_RECORD {
            bail!("journal record exceeds limit");
        }
        self.file.write_all(&(bytes.len() as u32).to_be_bytes())?;
        self.file.write_all(&crc32(&bytes).to_be_bytes())?;
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;
        self.syncs += 1;
        self.bytes += bytes.len() as u64 + 8;
        Ok(())
    }
}
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "raft-bench-wal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    #[test]
    fn standard_crc() {
        assert_eq!(crc32(b"123456789"), 0xcbf43926);
    }
    #[test]
    fn recover_and_trim_partial_tail() {
        let p = path();
        let (mut j, _) = Journal::open::<Vec<u64>>(&p).unwrap();
        j.append(&vec![1u64, 2, 3]).unwrap();
        drop(j);
        let len = std::fs::metadata(&p).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap()
            .write_all(&[0, 0, 0])
            .unwrap();
        let (j, records) = Journal::open::<Vec<u64>>(&p).unwrap();
        assert_eq!(records, vec![vec![1, 2, 3]]);
        assert_eq!(std::fs::metadata(&p).unwrap().len(), len);
        drop(j);
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn reject_complete_corruption() {
        let p = path();
        let (mut j, _) = Journal::open::<Vec<u64>>(&p).unwrap();
        j.append(&vec![1u64]).unwrap();
        drop(j);
        let mut b = std::fs::read(&p).unwrap();
        b[8] ^= 1;
        std::fs::write(&p, b).unwrap();
        assert!(Journal::open::<Vec<u64>>(&p).is_err());
        std::fs::remove_file(p).unwrap();
    }
}
