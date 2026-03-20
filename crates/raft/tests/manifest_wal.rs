//! Comprehensive integration tests for the manifest WAL system.
//!
//! Tests the full ManifestLog and ManifestWriter APIs: open, append,
//! recover, rotate, merge semantics, crash recovery, and the async writer.

use bisque_raft::manifest::{ManifestLog, ManifestWriter, SegmentLocation, SegmentMeta};
use bisque_raft::record_format::RecordTypeFlags;

fn make_segment_meta(group_id: u64, segment_id: u64) -> SegmentMeta {
    SegmentMeta {
        group_id,
        segment_id,
        valid_bytes: 4096,
        min_index: Some(1),
        max_index: Some(100),
        min_ts: Some(1000),
        max_ts: Some(2000),
        sealed: true,
        record_count: 50,
        record_type_flags: RecordTypeFlags {
            has_vote: false,
            has_entry: true,
            has_truncate: false,
            has_purge: false,
        },
        entry_count: 50,
        first_entry_offset: 16,
        location: SegmentLocation::Local,
    }
}

// =============================================================================
// SegmentMeta encoding tests
// =============================================================================

#[test]
fn segment_meta_encode_decode_entry_roundtrip() {
    let meta = make_segment_meta(1, 42);
    let encoded = meta.encode_entry();
    assert_eq!(encoded.len(), 89);
    assert_eq!(encoded[0], 0); // KIND_SEGMENT_META

    let decoded = SegmentMeta::decode_entry(&encoded).unwrap();
    assert_eq!(decoded.group_id, 1);
    assert_eq!(decoded.segment_id, 42);
    assert_eq!(decoded.valid_bytes, 4096);
    assert_eq!(decoded.min_index, Some(1));
    assert_eq!(decoded.max_index, Some(100));
    assert_eq!(decoded.min_ts, Some(1000));
    assert_eq!(decoded.max_ts, Some(2000));
    assert!(decoded.sealed);
    assert_eq!(decoded.record_count, 50);
    assert!(decoded.record_type_flags.has_entry);
    assert!(!decoded.record_type_flags.has_vote);
    assert_eq!(decoded.entry_count, 50);
    assert_eq!(decoded.first_entry_offset, 16);
    assert_eq!(decoded.location, SegmentLocation::Local);
}

#[test]
fn segment_meta_value_encoding_preserves_all_fields() {
    for &loc in &[
        SegmentLocation::Local,
        SegmentLocation::Remote,
        SegmentLocation::Both,
    ] {
        let mut meta = make_segment_meta(1, 1);
        meta.location = loc;
        let encoded = meta.encode_value();
        let decoded = SegmentMeta::decode_value(1, 1, &encoded).unwrap();
        assert_eq!(decoded.location, loc);
    }
}

#[test]
fn segment_meta_decode_rejects_wrong_size() {
    assert!(SegmentMeta::decode_entry(&[0u8; 10]).is_none());
    assert!(SegmentMeta::decode_value(0, 0, &[0u8; 10]).is_none());
}

#[test]
fn segment_meta_optional_fields_none() {
    let mut meta = make_segment_meta(1, 1);
    meta.min_index = None;
    meta.max_index = None;
    meta.min_ts = None;
    meta.max_ts = None;
    meta.sealed = false;

    let encoded = meta.encode_value();
    let decoded = SegmentMeta::decode_value(1, 1, &encoded).unwrap();
    assert_eq!(decoded.min_index, None);
    assert_eq!(decoded.max_index, None);
    assert_eq!(decoded.min_ts, None);
    assert_eq!(decoded.max_ts, None);
    assert!(!decoded.sealed);
}

// =============================================================================
// SegmentMeta merge tests
// =============================================================================

#[test]
fn segment_meta_merge_takes_max_values() {
    let mut base = make_segment_meta(1, 1);
    base.valid_bytes = 1000;
    base.record_count = 10;
    base.entry_count = 10;
    base.sealed = false;

    let mut update = make_segment_meta(1, 1);
    update.valid_bytes = 2000;
    update.record_count = 20;
    update.entry_count = 20;
    update.sealed = true;

    base.merge_from(update);
    assert_eq!(base.valid_bytes, 2000);
    assert_eq!(base.record_count, 20);
    assert_eq!(base.entry_count, 20);
    assert!(base.sealed); // OR semantics
}

#[test]
fn segment_meta_merge_expands_index_range() {
    let mut base = make_segment_meta(1, 1);
    base.min_index = Some(10);
    base.max_index = Some(20);

    let mut update = make_segment_meta(1, 1);
    update.min_index = Some(5);
    update.max_index = Some(30);

    base.merge_from(update);
    assert_eq!(base.min_index, Some(5));
    assert_eq!(base.max_index, Some(30));
}

#[test]
fn segment_meta_merge_none_index_filled() {
    let mut base = make_segment_meta(1, 1);
    base.min_index = None;
    base.max_index = None;

    let update = make_segment_meta(1, 1);
    base.merge_from(update);
    assert_eq!(base.min_index, Some(1));
    assert_eq!(base.max_index, Some(100));
}

#[test]
fn segment_meta_merge_location_upgrade() {
    let mut base = make_segment_meta(1, 1);
    base.location = SegmentLocation::Local;

    let mut update = make_segment_meta(1, 1);
    update.location = SegmentLocation::Remote;

    base.merge_from(update);
    assert_eq!(base.location, SegmentLocation::Both);
}

#[test]
fn segment_meta_merge_first_entry_offset_takes_smallest() {
    let mut base = make_segment_meta(1, 1);
    base.first_entry_offset = 100;

    let mut update = make_segment_meta(1, 1);
    update.first_entry_offset = 50;

    base.merge_from(update);
    assert_eq!(base.first_entry_offset, 50);
}

#[test]
fn segment_meta_merge_record_type_flags_union() {
    let mut base = make_segment_meta(1, 1);
    base.record_type_flags = RecordTypeFlags {
        has_vote: true,
        has_entry: false,
        has_truncate: false,
        has_purge: false,
    };

    let mut update = make_segment_meta(1, 1);
    update.record_type_flags = RecordTypeFlags {
        has_vote: false,
        has_entry: true,
        has_truncate: false,
        has_purge: true,
    };

    base.merge_from(update);
    assert!(base.record_type_flags.has_vote);
    assert!(base.record_type_flags.has_entry);
    assert!(!base.record_type_flags.has_truncate);
    assert!(base.record_type_flags.has_purge);
}

// =============================================================================
// ManifestLog open/recover tests
// =============================================================================

#[tokio::test]
async fn open_empty_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");
    let (manifest, recovery) = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
    assert!(recovery.segments.is_empty());
    assert!(recovery.engine_blobs.is_empty());
    assert_eq!(manifest.wal_size(), 16); // header only
}

#[tokio::test]
async fn open_creates_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("deep").join("nested").join("manifest");
    assert!(!dir.exists());
    let _ = ManifestLog::open(&dir, 1024 * 1024).await.unwrap();
    assert!(dir.exists());
}

#[tokio::test]
async fn append_and_recover_segment_meta() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
        m.append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();
        m.append_segment_meta(make_segment_meta(1, 2))
            .await
            .unwrap();
        m.append_segment_meta(make_segment_meta(2, 1))
            .await
            .unwrap();
        m.sync().await.unwrap();
    }

    let (m, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 3);
    assert!(recovery.segments.contains_key(&(1, 1)));
    assert!(recovery.segments.contains_key(&(1, 2)));
    assert!(recovery.segments.contains_key(&(2, 1)));

    let g1 = m.read_group_segments(1);
    assert_eq!(g1.len(), 2);
    let g2 = m.read_group_segments(2);
    assert_eq!(g2.len(), 1);
    let g3 = m.read_group_segments(99);
    assert_eq!(g3.len(), 0);
}

#[tokio::test]
async fn append_and_recover_engine_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
        m.append_engine_blob(1, 100, b"hello").await.unwrap();
        m.append_engine_blob(1, 200, b"world").await.unwrap();
        m.sync().await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.engine_blobs.len(), 2);
    assert_eq!(recovery.engine_blobs[0], (1, 100, b"hello".to_vec()));
    assert_eq!(recovery.engine_blobs[1], (1, 200, b"world".to_vec()));
}

#[tokio::test]
async fn segment_meta_merge_during_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();

        let mut v1 = make_segment_meta(1, 1);
        v1.valid_bytes = 1000;
        v1.sealed = false;
        m.append_segment_meta(v1).await.unwrap();

        let mut v2 = make_segment_meta(1, 1);
        v2.valid_bytes = 2000;
        v2.sealed = true;
        m.append_segment_meta(v2).await.unwrap();

        m.sync().await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    let meta = recovery.segments.get(&(1, 1)).unwrap();
    assert_eq!(meta.valid_bytes, 2000);
    assert!(meta.sealed);
}

// =============================================================================
// WAL rotation tests
// =============================================================================

#[tokio::test]
async fn rotate_and_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
        m.append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();
        m.append_engine_blob(1, 50, b"before").await.unwrap();

        m.rotate(b"engine_v1").await.unwrap();

        m.append_segment_meta(make_segment_meta(1, 2))
            .await
            .unwrap();
        m.append_engine_blob(1, 100, b"after").await.unwrap();
        m.sync().await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 2);
    assert!(recovery.segments.contains_key(&(1, 1))); // from snapshot
    assert!(recovery.segments.contains_key(&(1, 2))); // from WAL

    // Snapshot engine blob + post-rotation WAL blob.
    assert_eq!(recovery.engine_blobs.len(), 2);
    assert_eq!(recovery.engine_blobs[0].2, b"engine_v1");
    assert_eq!(recovery.engine_blobs[1], (1, 100, b"after".to_vec()));
}

#[tokio::test]
async fn multiple_rotations() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    for i in 0..5 {
        m.append_segment_meta(make_segment_meta(1, i))
            .await
            .unwrap();
        m.rotate(format!("snap_{i}").as_bytes()).await.unwrap();
    }
    m.append_segment_meta(make_segment_meta(1, 100))
        .await
        .unwrap();
    m.sync().await.unwrap();
    drop(m);

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 6); // 5 rotations + 1 final
    assert_eq!(recovery.engine_blobs.len(), 1); // only latest snapshot
    assert_eq!(recovery.engine_blobs[0].2, b"snap_4");
}

#[tokio::test]
async fn wal_size_threshold_signals_rotation() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let (mut m, _) = ManifestLog::open(&dir, 200).await.unwrap();
    // Header = 16. Entry = 4 + 89 + 8 = 101. Total after 1st = 117.
    let exceeded = m
        .append_segment_meta(make_segment_meta(1, 1))
        .await
        .unwrap();
    if !exceeded {
        // Total after 2nd = 218 > 200.
        let exceeded = m
            .append_segment_meta(make_segment_meta(1, 2))
            .await
            .unwrap();
        assert!(exceeded);
    }
    assert!(m.needs_rotation());
}

// =============================================================================
// Crash recovery / truncation tests
// =============================================================================

#[tokio::test]
async fn truncated_wal_entry_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
        m.append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();
        m.append_segment_meta(make_segment_meta(1, 2))
            .await
            .unwrap();
        m.sync().await.unwrap();
    }

    // Append garbage to simulate crash mid-write.
    let mut wal_found = false;
    let mut entries = tokio::fs::read_dir(&dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("wal_") && name.ends_with(".bin") {
            let mut f = tokio::fs::OpenOptions::new()
                .append(true)
                .open(entry.path())
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut f, &[0xFF; 50])
                .await
                .unwrap();
            wal_found = true;
        }
    }
    assert!(wal_found, "should have found a WAL file");

    // Recovery should still find both valid entries.
    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 2);
    assert!(recovery.segments.contains_key(&(1, 1)));
    assert!(recovery.segments.contains_key(&(1, 2)));
}

#[tokio::test]
async fn incomplete_snapshot_cleaned_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    // Create manifest first so directory exists.
    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
        m.append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();
        m.sync().await.unwrap();
    }

    // Write a bogus snapshot.tmp (simulating crash during snapshot write).
    tokio::fs::write(dir.join("snapshot.tmp"), b"incomplete")
        .await
        .unwrap();

    // Open should clean it up and recover normally.
    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 1);
    assert!(!dir.join("snapshot.tmp").exists());
}

// =============================================================================
// In-memory state tests
// =============================================================================

#[tokio::test]
async fn read_group_segments_filters_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    m.append_segment_meta(make_segment_meta(1, 10))
        .await
        .unwrap();
    m.append_segment_meta(make_segment_meta(1, 20))
        .await
        .unwrap();
    m.append_segment_meta(make_segment_meta(2, 10))
        .await
        .unwrap();
    m.append_segment_meta(make_segment_meta(3, 1))
        .await
        .unwrap();

    assert_eq!(m.read_group_segments(1).len(), 2);
    assert_eq!(m.read_group_segments(2).len(), 1);
    assert_eq!(m.read_group_segments(3).len(), 1);
    assert_eq!(m.read_group_segments(4).len(), 0);
    assert_eq!(m.segments().len(), 4);
}

#[tokio::test]
async fn remove_group_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    m.append_segment_meta(make_segment_meta(1, 1))
        .await
        .unwrap();
    m.append_segment_meta(make_segment_meta(2, 1))
        .await
        .unwrap();

    m.remove_group_segments(1);
    assert_eq!(m.read_group_segments(1).len(), 0);
    assert_eq!(m.read_group_segments(2).len(), 1);
}

// =============================================================================
// Snapshot edge cases
// =============================================================================

#[tokio::test]
async fn snapshot_with_no_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
        m.rotate(b"empty_state").await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert!(recovery.segments.is_empty());
    assert_eq!(recovery.engine_blobs.len(), 1);
    assert_eq!(recovery.engine_blobs[0].2, b"empty_state");
}

#[tokio::test]
async fn snapshot_with_empty_engine_data() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
        m.append_segment_meta(make_segment_meta(1, 1))
            .await
            .unwrap();
        m.rotate(b"").await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 1);
    assert!(recovery.engine_blobs.is_empty());
}

#[tokio::test]
async fn snapshot_preserves_segments_across_rotations() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let (mut m, _) = ManifestLog::open(&dir, 1 << 20).await.unwrap();

    // Add segments, rotate, add more, rotate again.
    m.append_segment_meta(make_segment_meta(1, 1))
        .await
        .unwrap();
    m.rotate(b"v1").await.unwrap();

    m.append_segment_meta(make_segment_meta(1, 2))
        .await
        .unwrap();
    m.rotate(b"v2").await.unwrap();

    // Both segments should be in the second snapshot.
    drop(m);
    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 2);
    assert!(recovery.segments.contains_key(&(1, 1)));
    assert!(recovery.segments.contains_key(&(1, 2)));
}

// =============================================================================
// ManifestWriter async wrapper tests
// =============================================================================

#[tokio::test]
async fn writer_segment_update_and_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (writer, _) = ManifestWriter::open(&dir, 1 << 20).await.unwrap();
        writer.send_segment_update(make_segment_meta(1, 1));
        writer.send_segment_update(make_segment_meta(1, 2));
        writer.send_segment_update(make_segment_meta(2, 1));
        // Sync to ensure all writes are flushed.
        writer.sync().await.unwrap();
    }
    // Writer dropped → task exits.

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 3);
}

#[tokio::test]
async fn writer_engine_blob_and_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (writer, _) = ManifestWriter::open(&dir, 1 << 20).await.unwrap();
        writer.send_engine_blob(1, 100, b"data".to_vec());
        writer.sync().await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.engine_blobs.len(), 1);
    assert_eq!(recovery.engine_blobs[0], (1, 100, b"data".to_vec()));
}

#[tokio::test]
async fn writer_rotate_and_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    {
        let (writer, _) = ManifestWriter::open(&dir, 1 << 20).await.unwrap();
        writer.send_segment_update(make_segment_meta(1, 1));
        writer.rotate(b"snap_data".to_vec()).await.unwrap();
        writer.send_segment_update(make_segment_meta(1, 2));
        writer.sync().await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 2);
    assert_eq!(recovery.engine_blobs.len(), 1);
    assert_eq!(recovery.engine_blobs[0].2, b"snap_data");
}

#[tokio::test]
async fn writer_many_concurrent_updates() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let (writer, _) = ManifestWriter::open(&dir, 1 << 20).await.unwrap();

    // Fire 100 updates rapidly.
    for i in 0..100u64 {
        writer.send_segment_update(make_segment_meta(1, i));
    }
    writer.sync().await.unwrap();
    drop(writer);

    let (_, recovery) = ManifestLog::open(&dir, 1 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 100);
}

// =============================================================================
// Large data tests
// =============================================================================

#[tokio::test]
async fn large_engine_blob() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let large_blob = vec![0xABu8; 1024 * 1024]; // 1 MB

    {
        let (mut m, _) = ManifestLog::open(&dir, 10 << 20).await.unwrap();
        m.append_engine_blob(1, 1, &large_blob).await.unwrap();
        m.sync().await.unwrap();
    }

    let (_, recovery) = ManifestLog::open(&dir, 10 << 20).await.unwrap();
    assert_eq!(recovery.engine_blobs.len(), 1);
    assert_eq!(recovery.engine_blobs[0].2.len(), 1024 * 1024);
    assert_eq!(recovery.engine_blobs[0].2[0], 0xAB);
}

#[tokio::test]
async fn many_segments_across_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("manifest");

    let (mut m, _) = ManifestLog::open(&dir, 10 << 20).await.unwrap();

    // 10 groups × 50 segments = 500 entries.
    for group in 0..10 {
        for seg in 0..50 {
            m.append_segment_meta(make_segment_meta(group, seg))
                .await
                .unwrap();
        }
    }
    m.sync().await.unwrap();
    drop(m);

    let (m, recovery) = ManifestLog::open(&dir, 10 << 20).await.unwrap();
    assert_eq!(recovery.segments.len(), 500);
    for group in 0..10 {
        assert_eq!(m.read_group_segments(group).len(), 50);
    }
}
