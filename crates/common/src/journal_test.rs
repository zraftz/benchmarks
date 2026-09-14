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
fn recover_and_trim_zero_filled_eof_extent() {
    let p = path();
    let (mut j, _) = Journal::open::<Vec<u64>>(&p).unwrap();
    j.append(&vec![1u64, 2, 3]).unwrap();
    drop(j);
    let valid = std::fs::metadata(&p).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&p)
        .unwrap()
        .set_len(valid + 4096)
        .unwrap();
    let (j, records) = Journal::open::<Vec<u64>>(&p).unwrap();
    assert_eq!(records, vec![vec![1, 2, 3]]);
    assert_eq!(std::fs::metadata(&p).unwrap().len(), valid);
    drop(j);
    std::fs::remove_file(p).unwrap();
}

#[test]
fn reject_zero_length_header_with_nonzero_tail() {
    let p = path();
    let (mut j, _) = Journal::open::<Vec<u64>>(&p).unwrap();
    j.append(&vec![1u64, 2, 3]).unwrap();
    drop(j);
    OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(&[0; 8])
        .unwrap();
    OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(&[1])
        .unwrap();
    assert!(Journal::open::<Vec<u64>>(&p).is_err());
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
fn append_reuses_bounded_frame_capacity_and_oversized_scratch_is_released() {
    let p = path();
    let (mut journal, _) = Journal::open::<Vec<u8>>(&p).unwrap();
    journal.append(&vec![1; 1024]).unwrap();
    let retained = journal.frame.capacity();
    assert!(retained >= 1024);
    assert!(journal.frame.is_empty());

    journal.append(&vec![2; 1024]).unwrap();
    assert_eq!(journal.frame.capacity(), retained);
    assert!(journal.frame.is_empty());

    drop(journal);

    let (journal, records) = Journal::open::<Vec<u8>>(&p).unwrap();
    assert_eq!(records, vec![vec![1; 1024], vec![2; 1024]]);
    drop(journal);
    std::fs::remove_file(p).unwrap();

    let mut oversized = Vec::with_capacity(RETAINED_FRAME_CAPACITY + 1);
    oversized.push(1);
    clear_frame(&mut oversized);
    assert_eq!(oversized.capacity(), 0);
}

#[test]
fn append_clears_reused_frame_after_serialization_failure() {
    struct FailingRecord;

    impl Serialize for FailingRecord {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("injected serialization failure"))
        }
    }

    let p = path();
    let (mut journal, _) = Journal::open::<Vec<u8>>(&p).unwrap();
    assert!(journal.append(&FailingRecord).is_err());
    assert!(journal.frame.is_empty());
    journal.append(&vec![4; 1024]).unwrap();
    drop(journal);

    let (journal, records) = Journal::open::<Vec<u8>>(&p).unwrap();
    assert_eq!(records, vec![vec![4; 1024]]);
    drop(journal);
    std::fs::remove_file(p).unwrap();
}

#[test]
fn checkpoint_replaces_history_and_following_appends_reopen() {
    let p = path();
    let (mut journal, _) = Journal::open::<Vec<u64>>(&p).unwrap();
    journal.append(&vec![1, 2]).unwrap();
    journal.append(&vec![3, 4]).unwrap();
    let checkpoint = serde_json::to_vec(&vec![9u64]).unwrap();
    journal.checkpoint_payload(&checkpoint).unwrap();
    journal.append(&vec![10]).unwrap();
    drop(journal);

    let (journal, records) = Journal::open::<Vec<u64>>(&p).unwrap();
    assert_eq!(records, vec![vec![9], vec![10]]);
    drop(journal);
    std::fs::remove_file(p).unwrap();
}

#[test]
fn checkpoint_does_not_stage_a_second_payload_copy() {
    let p = path();
    let (mut journal, _) = Journal::open::<Vec<u8>>(&p).unwrap();
    let checkpoint = serde_json::to_vec(&vec![9u8; 1024]).unwrap();
    journal.checkpoint_payload(&checkpoint).unwrap();
    assert!(journal.frame.is_empty());
    assert_eq!(journal.frame.capacity(), 0);
    drop(journal);

    let (journal, records) = Journal::open::<Vec<u8>>(&p).unwrap();
    assert_eq!(records, vec![vec![9u8; 1024]]);
    drop(journal);
    std::fs::remove_file(p).unwrap();
}

#[test]
fn failed_checkpoint_sync_keeps_authoritative_history_and_poisons_writer() {
    let p = path();
    let (mut journal, _) = Journal::open::<Vec<u64>>(&p).unwrap();
    journal.append(&vec![1, 2]).unwrap();
    let checkpoint = serde_json::to_vec(&vec![9u64]).unwrap();
    assert!(journal
        .checkpoint_payload_inner(&checkpoint, CheckpointFailure::AfterTemporarySync)
        .is_err());
    assert!(journal.append(&vec![3]).is_err());
    drop(journal);

    let (journal, records) = Journal::open::<Vec<u64>>(&p).unwrap();
    assert_eq!(records, vec![vec![1, 2]]);
    drop(journal);
    std::fs::remove_file(&p).unwrap();
    std::fs::remove_file(checkpoint_path(&p)).unwrap();
}

#[test]
fn failed_checkpoint_after_rename_recovers_published_state() {
    let p = path();
    let (mut journal, _) = Journal::open::<Vec<u64>>(&p).unwrap();
    journal.append(&vec![1, 2]).unwrap();
    let checkpoint = serde_json::to_vec(&vec![9u64]).unwrap();
    assert!(journal
        .checkpoint_payload_inner(&checkpoint, CheckpointFailure::AfterRename)
        .is_err());
    assert!(journal.append(&vec![3]).is_err());
    drop(journal);

    let (journal, records) = Journal::open::<Vec<u64>>(&p).unwrap();
    assert_eq!(records, vec![vec![9]]);
    drop(journal);
    std::fs::remove_file(p).unwrap();
}

#[test]
fn orphan_checkpoint_never_overrides_authoritative_history() {
    let p = path();
    let (mut journal, _) = Journal::open::<Vec<u64>>(&p).unwrap();
    journal.append(&vec![1, 2]).unwrap();
    drop(journal);
    std::fs::write(checkpoint_path(&p), legacy_frame(&vec![9u64])).unwrap();

    let (journal, records) = Journal::open::<Vec<u64>>(&p).unwrap();
    assert_eq!(records, vec![vec![1, 2]]);
    drop(journal);
    std::fs::remove_file(&p).unwrap();
    std::fs::remove_file(checkpoint_path(&p)).unwrap();
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
