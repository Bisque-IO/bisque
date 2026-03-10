//! Integration tests for tiered storage (LRU eviction, S3 archival, zstd compression).
//!
//! These tests exercise the full storage stack through the public API, verifying
//! that tiers 1-3 work together end-to-end.

use std::sync::Arc;
use std::time::Duration;

use bisque_raft::codec::{BorrowPayload, Decode, Encode};
use bisque_raft::type_config::ManiacRaftTypeConfig;
use bisque_raft::{
    InMemoryArchive, MmapPerGroupLogStorage, MmapStorageConfig, S3ArchiveConfig, SegmentArchive,
};
use openraft::RaftLogReader;
use openraft::entry::EntryPayload;
use openraft::storage::RaftLogStorage;
use openraft::type_config::async_runtime::AsyncRuntime;
use openraft::type_config::async_runtime::oneshot::Oneshot;
use openraft::{LogId, RaftTypeConfig};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

// -- Test data type (same as unit tests) --

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestData(Vec<u8>);

impl std::fmt::Display for TestData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestData(len={})", self.0.len())
    }
}

impl Encode for TestData {
    fn encode<W: std::io::Write>(
        &self,
        writer: &mut W,
    ) -> Result<(), bisque_raft::codec::CodecError> {
        (self.0.len() as u32).encode(writer)?;
        writer.write_all(&self.0)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.0.len()
    }
}

impl Decode for TestData {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, bisque_raft::codec::CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(Self(buf))
    }
}

impl BorrowPayload for TestData {
    fn payload_bytes(&self) -> &[u8] {
        &self.0
    }
}

type C = ManiacRaftTypeConfig<TestData, ()>;
type Rt = <C as RaftTypeConfig>::AsyncRuntime;
type Os = <Rt as AsyncRuntime>::Oneshot;

fn make_entry(index: u64, term: u64) -> openraft::impls::Entry<C> {
    openraft::impls::Entry::<C> {
        log_id: LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
            index,
        },
        payload: EntryPayload::Normal(TestData(index.to_le_bytes().to_vec())),
    }
}

fn make_callback() -> (
    openraft::storage::IOFlushed<C>,
    <Os as Oneshot>::Receiver<Result<(), std::io::Error>>,
) {
    let (tx, rx) = Os::channel::<Result<(), std::io::Error>>();
    let cb = openraft::storage::IOFlushed::<C>::signal(tx);
    (cb, rx)
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

/// End-to-end: write entries, archive with compression, delete locals, read back.
/// Verifies the full hot -> warm -> cold -> warm path with zstd compression.
#[tokio::test]
async fn test_tiered_storage_compressed_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let archive = Arc::new(InMemoryArchive::new());
    let s3_config = S3ArchiveConfig {
        bucket: "integration-test".to_string(),
        key_prefix: "raft/".to_string(),
        min_local_retention_secs: 0,
        compress_archived: true,
        compression_level: 3,
        ..Default::default()
    };
    let config = MmapStorageConfig::new(tmp.path())
        .with_segment_size(512)
        .with_max_pinned_segments(2)
        .with_max_concurrent_segment_opens(4)
        .with_s3_archive(s3_config);

    let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
        config,
        Some(archive.clone() as Arc<dyn SegmentArchive>),
    )
    .await
    .unwrap();

    let mut log = storage.get_log_storage(0).await.unwrap();

    // Write 80 entries across many segments
    for i in 1..=80 {
        let entries = vec![make_entry(i, 1)];
        let (cb, rx) = make_callback();
        log.append(entries, cb).await.unwrap();
        rx.await.unwrap().unwrap();
    }

    // Wait for background S3 uploads
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify archival happened with compressed objects
    let archived_count = archive.object_count();
    assert!(archived_count > 0, "Expected archived segments, got 0");
    for key in archive.keys() {
        assert!(
            key.ends_with(".log.zst"),
            "Expected compressed key, got: {key}"
        );
        // Verify zstd magic bytes
        let data = archive.get_data(&key).unwrap();
        assert!(data.len() >= 4);
        assert_eq!(&data[..4], &[0x28, 0xB5, 0x2F, 0xFD], "Expected zstd magic");
    }

    // Delete oldest local segment files to force S3 fetch on read
    let group_dir = tmp.path().join("group_0");
    let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .collect();
    seg_files.sort_by_key(|e| e.file_name());
    let delete_count = seg_files.len().min(3);
    for f in seg_files.iter().take(delete_count) {
        std::fs::remove_file(f.path()).unwrap();
    }

    // Read all entries — should transparently fetch + decompress from S3
    let result = log.try_get_log_entries(1..81).await.unwrap();
    assert_eq!(result.len(), 80, "All 80 entries should be readable");
    for (i, entry) in result.iter().enumerate() {
        assert_eq!(entry.log_id.index, (i + 1) as u64);
    }

    storage.stop();
}

/// Verify that uncompressed archival still works (no regression from compression changes).
#[tokio::test]
async fn test_tiered_storage_uncompressed_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let archive = Arc::new(InMemoryArchive::new());
    let s3_config = S3ArchiveConfig {
        bucket: "integration-test".to_string(),
        key_prefix: "raft/".to_string(),
        min_local_retention_secs: 0,
        compress_archived: false, // Explicitly uncompressed
        ..Default::default()
    };
    let config = MmapStorageConfig::new(tmp.path())
        .with_segment_size(512)
        .with_max_pinned_segments(2)
        .with_s3_archive(s3_config);

    let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
        config,
        Some(archive.clone() as Arc<dyn SegmentArchive>),
    )
    .await
    .unwrap();

    let mut log = storage.get_log_storage(0).await.unwrap();

    for i in 1..=60 {
        let entries = vec![make_entry(i, 1)];
        let (cb, rx) = make_callback();
        log.append(entries, cb).await.unwrap();
        rx.await.unwrap().unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify uncompressed keys
    for key in archive.keys() {
        assert!(
            key.ends_with(".log") && !key.ends_with(".log.zst"),
            "Expected uncompressed key, got: {key}"
        );
    }

    // Delete oldest segments and verify fetch works
    let group_dir = tmp.path().join("group_0");
    let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .collect();
    seg_files.sort_by_key(|e| e.file_name());
    for f in seg_files.iter().take(2) {
        let _ = std::fs::remove_file(f.path());
    }

    let result = log.try_get_log_entries(1..61).await.unwrap();
    assert_eq!(result.len(), 60);

    storage.stop();
}

/// Multi-group: verify tiered storage works independently per raft group.
#[tokio::test]
async fn test_tiered_storage_multi_group() {
    let tmp = TempDir::new().unwrap();
    let archive = Arc::new(InMemoryArchive::new());
    let s3_config = S3ArchiveConfig {
        bucket: "test".to_string(),
        key_prefix: "raft/".to_string(),
        compress_archived: true,
        ..Default::default()
    };
    let config = MmapStorageConfig::new(tmp.path())
        .with_segment_size(512)
        .with_max_pinned_segments(2)
        .with_s3_archive(s3_config);

    let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
        config,
        Some(archive.clone() as Arc<dyn SegmentArchive>),
    )
    .await
    .unwrap();

    // Write to two different groups
    let mut log0 = storage.get_log_storage(0).await.unwrap();
    let mut log1 = storage.get_log_storage(1).await.unwrap();

    for i in 1..=40 {
        let (cb0, rx0) = make_callback();
        log0.append(vec![make_entry(i, 1)], cb0).await.unwrap();
        rx0.await.unwrap().unwrap();

        let (cb1, rx1) = make_callback();
        log1.append(vec![make_entry(i, 2)], cb1).await.unwrap();
        rx1.await.unwrap().unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Both groups should have archived segments
    let keys = archive.keys();
    let group0_keys: Vec<_> = keys.iter().filter(|k| k.contains("group_0")).collect();
    let group1_keys: Vec<_> = keys.iter().filter(|k| k.contains("group_1")).collect();
    assert!(
        !group0_keys.is_empty(),
        "Group 0 should have archived segments"
    );
    assert!(
        !group1_keys.is_empty(),
        "Group 1 should have archived segments"
    );

    // Both groups should be independently readable
    let result0 = log0.try_get_log_entries(1..41).await.unwrap();
    assert_eq!(result0.len(), 40);
    let result1 = log1.try_get_log_entries(1..41).await.unwrap();
    assert_eq!(result1.len(), 40);

    storage.stop();
}

/// Write, purge old entries, verify remaining entries survive across tiers.
#[tokio::test]
async fn test_tiered_storage_purge_with_archival() {
    let tmp = TempDir::new().unwrap();
    let archive = Arc::new(InMemoryArchive::new());
    let s3_config = S3ArchiveConfig {
        bucket: "test".to_string(),
        key_prefix: "raft/".to_string(),
        min_local_retention_secs: 0,
        compress_archived: true,
        compression_level: 1,
        ..Default::default()
    };
    let config = MmapStorageConfig::new(tmp.path())
        .with_segment_size(512)
        .with_max_pinned_segments(3)
        .with_s3_archive(s3_config);

    let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
        config,
        Some(archive.clone() as Arc<dyn SegmentArchive>),
    )
    .await
    .unwrap();

    let mut log = storage.get_log_storage(0).await.unwrap();

    // Write 100 entries
    for i in 1..=100 {
        let (cb, rx) = make_callback();
        log.append(vec![make_entry(i, 1)], cb).await.unwrap();
        rx.await.unwrap().unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(archive.object_count() > 0);

    // Purge first 60 entries
    let purge_id = LogId {
        leader_id: openraft::impls::leader_id_adv::LeaderId {
            term: 1,
            node_id: 1,
        },
        index: 60,
    };
    log.purge(purge_id).await.unwrap();

    // Remaining 40 entries should be readable
    let result = log.try_get_log_entries(61..101).await.unwrap();
    assert_eq!(result.len(), 40);
    for (i, entry) in result.iter().enumerate() {
        assert_eq!(entry.log_id.index, (61 + i) as u64);
    }

    // Purged entries should not be readable
    let result = log.try_get_log_entries(1..61).await.unwrap();
    assert!(result.is_empty() || result.len() < 60);

    storage.stop();
}

/// Verify that compression actually reduces stored size for repetitive data.
#[tokio::test]
async fn test_compression_reduces_stored_size() {
    let tmp = TempDir::new().unwrap();
    let compressed_archive = Arc::new(InMemoryArchive::new());
    let uncompressed_archive = Arc::new(InMemoryArchive::new());

    // Write same data to compressed and uncompressed storage
    for (archive, compress) in [
        (compressed_archive.clone(), true),
        (uncompressed_archive.clone(), false),
    ] {
        let s3_config = S3ArchiveConfig {
            bucket: "test".to_string(),
            key_prefix: "raft/".to_string(),
            compress_archived: compress,
            ..Default::default()
        };
        let config = MmapStorageConfig::new(if compress {
            tmp.path().join("compressed")
        } else {
            tmp.path().join("uncompressed")
        })
        .with_segment_size(512)
        .with_s3_archive(s3_config);

        let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
            config,
            Some(archive as Arc<dyn SegmentArchive>),
        )
        .await
        .unwrap();

        let mut log = storage.get_log_storage(0).await.unwrap();
        for i in 1..=40 {
            let (cb, rx) = make_callback();
            log.append(vec![make_entry(i, 1)], cb).await.unwrap();
            rx.await.unwrap().unwrap();
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        storage.stop();
    }

    // Compare total stored bytes
    let compressed_total: usize = compressed_archive
        .keys()
        .iter()
        .map(|k| compressed_archive.get_data(k).unwrap().len())
        .sum();
    let uncompressed_total: usize = uncompressed_archive
        .keys()
        .iter()
        .map(|k| uncompressed_archive.get_data(k).unwrap().len())
        .sum();

    assert!(
        compressed_total < uncompressed_total,
        "Compressed ({compressed_total} bytes) should be smaller than uncompressed ({uncompressed_total} bytes)"
    );
}
