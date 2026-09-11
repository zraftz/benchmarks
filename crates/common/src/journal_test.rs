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

// Independent pre-optimization algorithm authenticates the existing file format.
fn legacy_crc(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn legacy_frame<T: Serialize>(record: &T) -> Vec<u8> {
    let payload = serde_json::to_vec(record).unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&legacy_crc(&payload).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

#[test]
fn accelerated_crc_matches_legacy_for_alignment_and_length_boundaries() {
    let bytes: Vec<_> = (0u32..131_088)
        .map(|i| (i.wrapping_mul(97) ^ (i >> 8)) as u8)
        .collect();
    for offset in 0..16 {
        for len in (0..80).chain([
            127, 128, 129, 255, 256, 257, 4095, 4096, 4097, 65_536, 131_072,
        ]) {
            assert_eq!(
                crc32(&bytes[offset..offset + len]),
                legacy_crc(&bytes[offset..offset + len])
            );
        }
    }
}

#[test]
fn coalesced_frames_are_byte_compatible_and_legacy_records_reopen() {
    let p = path();
    let first = vec![1u64, 2, 3];
    let second = vec![0u64, u64::MAX, 42];
    let mut expected = legacy_frame(&first);
    std::fs::write(&p, &expected).unwrap();
    let (mut j, records) = Journal::open::<Vec<u64>>(&p).unwrap();
    assert_eq!(records, vec![first.clone()]);
    j.append(&second).unwrap();
    expected.extend_from_slice(&legacy_frame(&second));
    assert_eq!(j.syncs, 1);
    assert_eq!(j.bytes, legacy_frame(&second).len() as u64);
    drop(j);
    assert_eq!(std::fs::read(&p).unwrap(), expected);
    let (j, records) = Journal::open::<Vec<u64>>(&p).unwrap();
    assert_eq!(records, vec![first, second]);
    drop(j);
    std::fs::remove_file(p).unwrap();
}

#[test]
fn every_partial_frame_recovers_only_the_complete_prefix() {
    let p = path();
    let first = legacy_frame(&vec![1u64, 2, 3]);
    let second = legacy_frame(&vec![4u64, 5, 6]);
    for cut in 0..second.len() {
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second[..cut]);
        std::fs::write(&p, bytes).unwrap();
        let (j, records) = Journal::open::<Vec<u64>>(&p).unwrap();
        assert_eq!(records, vec![vec![1, 2, 3]], "cut {cut}");
        assert_eq!(std::fs::metadata(&p).unwrap().len(), first.len() as u64);
        drop(j);
    }
    std::fs::remove_file(p).unwrap();
}
