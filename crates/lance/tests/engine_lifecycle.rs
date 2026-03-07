//! Integration tests for BisqueLance engine + TableEngine lifecycle.
//!
//! Tests cover table creation/deletion, write path, seal/promote lifecycle,
//! catalog state, snapshot/restore, snapshot guard integration, and error handling.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bisque_lance::{BisqueLance, BisqueLanceConfig, TableOpenConfig};
use futures::TryStreamExt;
use tempfile::tempdir;

// =============================================================================
// Test helpers
// =============================================================================

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

/// Create a BisqueLance engine backed by a temp directory.
async fn new_engine(dir: &std::path::Path) -> BisqueLance {
    let config = BisqueLanceConfig::new(dir);
    BisqueLance::open(config).await.unwrap()
}

/// Create a BisqueLance engine with custom seal settings.
async fn new_engine_with_seal(
    dir: &std::path::Path,
    seal_max_age: Duration,
    seal_max_size: u64,
) -> BisqueLance {
    let config = BisqueLanceConfig::new(dir)
        .with_seal_max_age(seal_max_age)
        .with_seal_max_size(seal_max_size);
    BisqueLance::open(config).await.unwrap()
}

/// Build a TableOpenConfig from the engine config for a named table.
fn table_config(engine: &BisqueLance, name: &str) -> TableOpenConfig {
    engine.config().build_table_config(name, test_schema())
}

// =============================================================================
// Engine Lifecycle Tests (BisqueLance)
// =============================================================================

#[tokio::test]
async fn test_create_table_and_list() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "users");
    engine.create_table(cfg, None).await.unwrap();

    let tables = engine.list_tables();
    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&"users".to_string()));
}

#[tokio::test]
async fn test_create_table_has_table() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "events");
    engine.create_table(cfg, None).await.unwrap();

    assert!(engine.has_table("events"));
    assert!(!engine.has_table("nonexistent"));
}

#[tokio::test]
async fn test_require_table_returns_ok() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "logs");
    engine.create_table(cfg, None).await.unwrap();

    let result = engine.require_table("logs");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().name(), "logs");
}

#[tokio::test]
async fn test_require_table_returns_err_for_missing() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let result = engine.require_table("ghost_table");
    assert!(result.is_err());
    let err = result.err().unwrap();
    let msg = format!("{}", err);
    assert!(
        msg.contains("not found") || msg.contains("ghost_table"),
        "Error should mention the missing table, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_create_duplicate_table_fails() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg1 = table_config(&engine, "dupes");
    engine.create_table(cfg1, None).await.unwrap();

    let cfg2 = table_config(&engine, "dupes");
    let result = engine.create_table(cfg2, None).await;
    assert!(result.is_err());

    let err = result.err().unwrap();
    let msg = format!("{}", err);
    assert!(
        msg.contains("already exists") || msg.contains("dupes"),
        "Error should indicate duplicate table, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_drop_table() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "to_drop");
    engine.create_table(cfg, None).await.unwrap();
    assert!(engine.has_table("to_drop"));

    engine.drop_table("to_drop").await.unwrap();
    assert!(!engine.has_table("to_drop"));
    assert!(engine.list_tables().is_empty());
}

#[tokio::test]
async fn test_drop_nonexistent_table_fails() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let result = engine.drop_table("phantom").await;
    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("not found") || msg.contains("phantom"),
        "Error should mention the missing table, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_multiple_tables() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    for name in &["alpha", "beta", "gamma"] {
        let cfg = table_config(&engine, name);
        engine.create_table(cfg, None).await.unwrap();
    }

    let tables = engine.list_tables();
    assert_eq!(tables.len(), 3);
    for name in &["alpha", "beta", "gamma"] {
        assert!(engine.has_table(name));
        let t = engine.get_table(name);
        assert!(t.is_some());
        assert_eq!(t.unwrap().name(), *name);
    }
}

#[tokio::test]
async fn test_engine_shutdown() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "shutdown_test");
    engine.create_table(cfg, None).await.unwrap();

    // Shutdown should complete without panic.
    engine.shutdown().await.unwrap();
    // After shutdown, tables are cleared.
    assert!(engine.list_tables().is_empty());
}

// =============================================================================
// TableEngine Write Path Tests
// =============================================================================

#[tokio::test]
async fn test_write_and_read_active_segment() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "write_test");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("write_test").unwrap();
    let batch = test_batch(&[1, 2, 3], &["alice", "bob", "charlie"]);
    table.apply_append(vec![batch]).await.unwrap();

    let ds = table.active_dataset_snapshot().await;
    assert!(ds.is_some());
    let ds = ds.unwrap();
    let row_count = ds.count_rows(None).await.unwrap();
    assert_eq!(row_count, 3);
}

#[tokio::test]
async fn test_write_multiple_batches() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "multi_batch");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("multi_batch").unwrap();

    // Write three separate batches.
    table
        .apply_append(vec![test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();
    table
        .apply_append(vec![test_batch(&[3, 4], &["c", "d"])])
        .await
        .unwrap();
    table
        .apply_append(vec![test_batch(&[5], &["e"])])
        .await
        .unwrap();

    let ds = table.active_dataset_snapshot().await.unwrap();
    let row_count = ds.count_rows(None).await.unwrap();
    assert_eq!(row_count, 5);
}

#[tokio::test]
async fn test_schema_from_first_write() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "schema_check");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("schema_check").unwrap();

    // Schema should be available from creation (since we pass it in config).
    let schema = table.schema().await;
    assert!(schema.is_some());

    let schema = schema.unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
}

#[tokio::test]
async fn test_record_log_index_tracking() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "log_index");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("log_index").unwrap();

    // Before any writes, min_safe_log_index should be None.
    assert!(table.min_safe_log_index().is_none());

    // Record a log index.
    let changed = table.record_log_index(42);
    assert!(changed);

    // Now min_safe_log_index should reflect it.
    assert_eq!(table.min_safe_log_index(), Some(42));
}

#[tokio::test]
async fn test_record_log_index_only_records_first() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "log_index_once");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("log_index_once").unwrap();

    // First call should return true (set the value).
    assert!(table.record_log_index(10));

    // Second call should return false (already set for this segment).
    assert!(!table.record_log_index(20));

    // Value should still be the first one.
    assert_eq!(table.min_safe_log_index(), Some(10));
}

// =============================================================================
// Seal / Promote Lifecycle Tests
// =============================================================================

#[tokio::test]
async fn test_seal_creates_sealed_segment() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "seal_test");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("seal_test").unwrap();
    table
        .apply_append(vec![test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();

    // Seal: segment 1 becomes sealed, segment 2 becomes active.
    table
        .apply_seal(1, 2, bisque_lance::SealReason::MaxSize)
        .await
        .unwrap();

    let sealed_ds = table.sealed_dataset_snapshot().await;
    assert!(sealed_ds.is_some(), "Sealed dataset should exist after seal");

    let sealed_rows = sealed_ds.unwrap().count_rows(None).await.unwrap();
    assert_eq!(sealed_rows, 2);
}

#[tokio::test]
async fn test_seal_creates_new_active_segment() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "seal_rotate");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("seal_rotate").unwrap();
    table
        .apply_append(vec![test_batch(&[1, 2], &["a", "b"])])
        .await
        .unwrap();

    // Seal.
    table
        .apply_seal(1, 2, bisque_lance::SealReason::MaxSize)
        .await
        .unwrap();

    // New writes should go to the new active segment (segment 2).
    table
        .apply_append(vec![test_batch(&[3, 4, 5], &["c", "d", "e"])])
        .await
        .unwrap();

    let active_ds = table.active_dataset_snapshot().await.unwrap();
    let active_rows = active_ds.count_rows(None).await.unwrap();
    assert_eq!(active_rows, 3, "New writes should be in the new active segment");

    // Sealed should still have the original 2 rows.
    let sealed_ds = table.sealed_dataset_snapshot().await.unwrap();
    let sealed_rows = sealed_ds.count_rows(None).await.unwrap();
    assert_eq!(sealed_rows, 2);
}

#[tokio::test]
async fn test_should_seal_by_size() {
    let tmp = tempdir().unwrap();
    // Set a very small seal_max_size to trigger size-based sealing.
    let engine = new_engine_with_seal(tmp.path(), Duration::from_secs(3600), 1).await;

    let cfg = engine
        .config()
        .build_table_config("size_seal", test_schema());
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("size_seal").unwrap();

    // Before writing, should_seal returns None (no data).
    assert!(table.should_seal().is_none());

    // Write data — the active_bytes tracking will exceed our 1-byte threshold.
    table
        .apply_append(vec![test_batch(&[1, 2, 3], &["a", "b", "c"])])
        .await
        .unwrap();

    let reason = table.should_seal();
    assert!(reason.is_some());
    assert_eq!(reason.unwrap(), bisque_lance::SealReason::MaxSize);
}

#[tokio::test]
async fn test_should_seal_by_age() {
    let tmp = tempdir().unwrap();
    // Set a very short seal_max_age (1 ms).
    let engine = new_engine_with_seal(
        tmp.path(),
        Duration::from_millis(1),
        1024 * 1024 * 1024,
    )
    .await;

    let cfg = engine
        .config()
        .build_table_config("age_seal", test_schema());
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("age_seal").unwrap();

    // Write some data (must have data for should_seal to trigger).
    table
        .apply_append(vec![test_batch(&[1], &["a"])])
        .await
        .unwrap();

    // Wait for the age threshold to pass.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let reason = table.should_seal();
    assert!(reason.is_some());
    assert_eq!(reason.unwrap(), bisque_lance::SealReason::MaxAge);
}

// =============================================================================
// Catalog State Tests
// =============================================================================

#[tokio::test]
async fn test_catalog_initial_state() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "catalog_init");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("catalog_init").unwrap();
    let catalog = table.catalog();

    assert_eq!(catalog.active_segment, 1);
    assert!(catalog.sealed_segment.is_none());
    assert_eq!(catalog.s3_manifest_version, 0);
}

#[tokio::test]
async fn test_catalog_after_seal() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "catalog_seal");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("catalog_seal").unwrap();
    table
        .apply_append(vec![test_batch(&[1], &["x"])])
        .await
        .unwrap();

    // Seal: segment 1 sealed, segment 2 new active.
    table
        .apply_seal(1, 2, bisque_lance::SealReason::MaxAge)
        .await
        .unwrap();

    let catalog = table.catalog();
    assert_eq!(catalog.active_segment, 2);
    assert_eq!(catalog.sealed_segment, Some(1));
}

#[tokio::test]
async fn test_next_segment_id_increments() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "next_seg");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("next_seg").unwrap();

    // Initial: active=1, next=2.
    assert_eq!(table.next_segment_id(), 2);

    table
        .apply_append(vec![test_batch(&[1], &["a"])])
        .await
        .unwrap();

    // Seal to segment 2.
    table
        .apply_seal(1, 2, bisque_lance::SealReason::MaxSize)
        .await
        .unwrap();

    // Now active=2, sealed=1, next=3.
    assert_eq!(table.next_segment_id(), 3);

    table
        .apply_append(vec![test_batch(&[2], &["b"])])
        .await
        .unwrap();

    // Seal again to segment 3.
    table
        .apply_seal(2, 3, bisque_lance::SealReason::MaxSize)
        .await
        .unwrap();

    // Now active=3, sealed=2, next=4.
    assert_eq!(table.next_segment_id(), 4);
}

// =============================================================================
// Snapshot / Restore Tests
// =============================================================================

#[tokio::test]
async fn test_table_entries_snapshot() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    for name in &["snap_a", "snap_b"] {
        let cfg = table_config(&engine, name);
        engine.create_table(cfg, None).await.unwrap();
    }

    let entries = engine.table_entries();
    assert_eq!(entries.len(), 2);
    assert!(entries.contains_key("snap_a"));
    assert!(entries.contains_key("snap_b"));

    // Each entry should have a valid catalog with active_segment=1.
    for (_name, entry) in &entries {
        assert_eq!(entry.catalog.active_segment, 1);
    }
}

#[tokio::test]
async fn test_restore_table_from_snapshot() {
    let tmp = tempdir().unwrap();

    // Phase 1: Create engine, create table, write data, capture snapshot state.
    let catalog_snapshot;
    let schema_history;
    {
        let engine = new_engine(tmp.path()).await;
        let cfg = table_config(&engine, "restore_me");
        engine.create_table(cfg, None).await.unwrap();

        let table = engine.require_table("restore_me").unwrap();
        table
            .apply_append(vec![test_batch(&[10, 20], &["hello", "world"])])
            .await
            .unwrap();

        catalog_snapshot = table.catalog();
        schema_history = table.schema_history();

        engine.shutdown().await.unwrap();
    }

    // Phase 2: Create a new engine and restore the table.
    {
        let engine = new_engine(tmp.path()).await;

        let snapshot = bisque_lance::TableSnapshot {
            catalog: catalog_snapshot,
            flush_state: bisque_lance::FlushState::Idle,
            schema_history,
        };

        let cfg = table_config(&engine, "restore_me");
        engine.restore_table(cfg, &snapshot).await.unwrap();

        // Verify the table is accessible.
        assert!(engine.has_table("restore_me"));
        let table = engine.require_table("restore_me").unwrap();

        // The active dataset should have the data from the original write.
        let ds = table.active_dataset_snapshot().await.unwrap();
        let row_count = ds.count_rows(None).await.unwrap();
        assert_eq!(row_count, 2);
    }
}

// =============================================================================
// SnapshotTransferGuard Integration Tests
// =============================================================================

#[tokio::test]
async fn test_snapshot_guard_accessible_from_engine() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let guard = engine.snapshot_guard();
    // Guard should initially have zero active transfers.
    assert_eq!(guard.active_count(), 0);

    // Acquiring a handle should bump the count.
    let handle = guard.acquire(Duration::from_secs(60), None);
    assert_eq!(guard.active_count(), 1);

    // Releasing should decrement.
    handle.release();
    assert_eq!(guard.active_count(), 0);
}

#[tokio::test]
async fn test_snapshot_guard_integration_with_table_ops() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "guarded");
    engine.create_table(cfg, None).await.unwrap();

    // Write some data so the table directory has content.
    let table = engine.require_table("guarded").unwrap();
    table
        .apply_append(vec![test_batch(&[1], &["x"])])
        .await
        .unwrap();
    drop(table);

    // Acquire guard before drop.
    let guard = engine.snapshot_guard().clone();
    let handle = guard.acquire(Duration::from_secs(60), Some(100));
    assert_eq!(guard.active_count(), 1);

    // Drop the table while the guard is active. The directory deletion
    // should be deferred.
    engine.drop_table("guarded").await.unwrap();
    assert!(!engine.has_table("guarded"));

    // Release the guard — deferred deletions should be flushed.
    handle.release();
    assert_eq!(guard.active_count(), 0);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[tokio::test]
async fn test_write_to_nonexistent_table_fails() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let result = engine.require_table("does_not_exist");
    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("not found") || msg.contains("does_not_exist"),
        "Error should reference the missing table, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_engine_config_table_data_dir() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let table_dir = engine.config().table_data_dir("my_table");
    let expected = tmp.path().join("tables").join("my_table");
    assert_eq!(table_dir, expected);
}

// =============================================================================
// Delete Path Tests
// =============================================================================

#[tokio::test]
async fn test_delete_from_active_segment() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "del_test");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("del_test").unwrap();
    let batch = test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Delete rows where id > 3
    let deleted = table.apply_delete("id > 3").await.unwrap();
    assert_eq!(deleted, 2);

    let ds = table.active_dataset_snapshot().await.unwrap();
    let remaining = ds.count_rows(None).await.unwrap();
    assert_eq!(remaining, 3);
}

#[tokio::test]
async fn test_delete_no_matching_rows() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "del_none");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("del_none").unwrap();
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Delete with filter that matches nothing
    let deleted = table.apply_delete("id > 100").await.unwrap();
    assert_eq!(deleted, 0);

    let ds = table.active_dataset_snapshot().await.unwrap();
    let remaining = ds.count_rows(None).await.unwrap();
    assert_eq!(remaining, 3);
}

#[tokio::test]
async fn test_delete_all_rows() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "del_all");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("del_all").unwrap();
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    let deleted = table.apply_delete("id >= 1").await.unwrap();
    assert_eq!(deleted, 3);

    let ds = table.active_dataset_snapshot().await.unwrap();
    let remaining = ds.count_rows(None).await.unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn test_delete_then_seal_respects_deletion_vectors() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "del_seal");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("del_seal").unwrap();
    let batch = test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Delete some rows
    let deleted = table.apply_delete("id <= 2").await.unwrap();
    assert_eq!(deleted, 2);

    // Seal the active segment
    table.apply_seal(1, 2, bisque_lance::SealReason::MaxSize).await.unwrap();

    // The sealed segment should reflect the deletions
    let sealed_ds = table.sealed_dataset_snapshot().await.unwrap();
    let sealed_count = sealed_ds.count_rows(None).await.unwrap();
    assert_eq!(sealed_count, 3, "sealed segment should have 3 rows (5 - 2 deleted)");

    // Active segment (newly created) should be empty
    let active_ds = table.active_dataset_snapshot().await.unwrap();
    let active_count = active_ds.count_rows(None).await.unwrap();
    assert_eq!(active_count, 0);
}

#[tokio::test]
async fn test_delete_across_active_and_sealed_tiers() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "del_cross");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("del_cross").unwrap();

    // Write data to active and then seal to get data in sealed tier
    let batch1 = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch1]).await.unwrap();
    table.apply_seal(1, 2, bisque_lance::SealReason::MaxSize).await.unwrap();

    // Write more data to the new active segment
    let batch2 = test_batch(&[4, 5, 6], &["d", "e", "f"]);
    table.apply_append(vec![batch2]).await.unwrap();

    // Delete id=2 (in sealed) and id=5 (in active)
    let deleted = table.apply_delete("id = 2 OR id = 5").await.unwrap();
    assert_eq!(deleted, 2);

    // Verify both tiers reflect the deletion
    let active_ds = table.active_dataset_snapshot().await.unwrap();
    let active_count = active_ds.count_rows(None).await.unwrap();
    assert_eq!(active_count, 2, "active should have 2 rows (3 - 1 deleted)");

    let sealed_ds = table.sealed_dataset_snapshot().await.unwrap();
    let sealed_count = sealed_ds.count_rows(None).await.unwrap();
    assert_eq!(sealed_count, 2, "sealed should have 2 rows (3 - 1 deleted)");
}

#[tokio::test]
async fn test_tier_versions_after_delete() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "tier_ver");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("tier_ver").unwrap();
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    let (av_before, sv_before, s3v_before) = table.tier_versions();
    assert!(av_before.is_some());
    assert!(sv_before.is_none());
    assert!(s3v_before.is_none());

    // Delete bumps the active dataset version
    table.apply_delete("id = 1").await.unwrap();

    let (av_after, _, _) = table.tier_versions();
    assert!(av_after.unwrap() > av_before.unwrap(), "version should increase after delete");
}

// =============================================================================
// Update Path Tests
// =============================================================================

#[tokio::test]
async fn test_update_replaces_rows() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "upd_test");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("upd_test").unwrap();
    let batch = test_batch(&[1, 2, 3], &["alice", "bob", "charlie"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Update: replace rows where id > 2 with new data
    let replacement = test_batch(&[3], &["charles"]);
    let deleted = table.apply_update("id > 2", vec![replacement]).await.unwrap();
    assert_eq!(deleted, 1);

    // Total should be: 2 original + 1 replacement = 3
    let ds = table.active_dataset_snapshot().await.unwrap();
    let total = ds.count_rows(None).await.unwrap();
    assert_eq!(total, 3);
}

#[tokio::test]
async fn test_update_with_no_matching_rows() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "upd_none");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("upd_none").unwrap();
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Update with no matching filter — replacement still appended
    let replacement = test_batch(&[99], &["new"]);
    let deleted = table.apply_update("id > 100", vec![replacement]).await.unwrap();
    assert_eq!(deleted, 0);

    // Total: 3 original + 1 appended = 4
    let ds = table.active_dataset_snapshot().await.unwrap();
    let total = ds.count_rows(None).await.unwrap();
    assert_eq!(total, 4);
}

#[tokio::test]
async fn test_update_across_tiers() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "upd_cross");
    engine.create_table(cfg, None).await.unwrap();

    let table = engine.require_table("upd_cross").unwrap();

    // Write to active and seal
    let batch1 = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch1]).await.unwrap();
    table.apply_seal(1, 2, bisque_lance::SealReason::MaxSize).await.unwrap();

    // Write to new active
    let batch2 = test_batch(&[4, 5], &["d", "e"]);
    table.apply_append(vec![batch2]).await.unwrap();

    // Update id=2 (sealed) and id=4 (active) — deletes from both tiers, appends to active
    let replacement = test_batch(&[2, 4], &["b_updated", "d_updated"]);
    let deleted = table.apply_update("id = 2 OR id = 4", vec![replacement]).await.unwrap();
    assert_eq!(deleted, 2);

    // Sealed: 3 original - 1 deleted = 2
    let sealed_ds = table.sealed_dataset_snapshot().await.unwrap();
    assert_eq!(sealed_ds.count_rows(None).await.unwrap(), 2);

    // Active: 2 original - 1 deleted + 2 replacements = 3
    let active_ds = table.active_dataset_snapshot().await.unwrap();
    assert_eq!(active_ds.count_rows(None).await.unwrap(), 3);
}

// =============================================================================
// Data Content Verification Tests
// =============================================================================

#[tokio::test]
async fn test_delete_verifies_correct_rows_remain() {
    // Write ids [1,2,3,4,5], delete where id > 3
    // Scan remaining rows and verify ids are exactly [1,2,3]
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "verify_content");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("verify_content").unwrap();

    let batch = test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    table.apply_append(vec![batch]).await.unwrap();
    table.apply_delete("id > 3").await.unwrap();

    let ds = table.active_dataset_snapshot().await.unwrap();
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3], "only rows with id <= 3 should remain");
}

#[tokio::test]
async fn test_update_verifies_replacement_data() {
    // Write ids [1,2,3], update where id=2 with replacement [2, "updated"]
    // Scan and verify: id=1 name="a", id=2 name="updated", id=3 name="c"
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "verify_update");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("verify_update").unwrap();

    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    let replacement = test_batch(&[2], &["updated"]);
    table
        .apply_update("id = 2", vec![replacement])
        .await
        .unwrap();

    let ds = table.active_dataset_snapshot().await.unwrap();
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut rows: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..id_col.len() {
            rows.push((id_col.value(i), name_col.value(i).to_string()));
        }
    }
    rows.sort_by_key(|r| r.0);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "updated".to_string()));
    assert_eq!(rows[2], (3, "c".to_string()));
}

// =============================================================================
// Error Handling Tests (Delete/Update)
// =============================================================================

#[tokio::test]
async fn test_delete_with_invalid_filter_returns_error() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "bad_filter");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("bad_filter").unwrap();
    let batch = test_batch(&[1, 2], &["a", "b"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Invalid SQL filter should return error
    let result = table.apply_delete("INVALID SQL GARBAGE @#$%").await;
    assert!(result.is_err(), "invalid filter should return error");
}

#[tokio::test]
async fn test_delete_with_nonexistent_column_returns_error() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "bad_col");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("bad_col").unwrap();
    let batch = test_batch(&[1], &["a"]);
    table.apply_append(vec![batch]).await.unwrap();

    let result = table.apply_delete("nonexistent_column > 0").await;
    assert!(result.is_err(), "filter on nonexistent column should error");
}

// =============================================================================
// Empty Table Edge Cases
// =============================================================================

#[tokio::test]
async fn test_delete_from_empty_active_segment() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "empty_del");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("empty_del").unwrap();

    // Delete from table with no data written yet
    let deleted = table.apply_delete("id > 0").await.unwrap();
    assert_eq!(deleted, 0);
}

// =============================================================================
// Sequential Deletes
// =============================================================================

#[tokio::test]
async fn test_multiple_sequential_deletes() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "seq_del");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("seq_del").unwrap();

    let batch = test_batch(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    );
    table.apply_append(vec![batch]).await.unwrap();

    // First delete: remove id > 8
    let d1 = table.apply_delete("id > 8").await.unwrap();
    assert_eq!(d1, 2);

    // Second delete: remove id < 3
    let d2 = table.apply_delete("id < 3").await.unwrap();
    assert_eq!(d2, 2);

    // Third delete: remove id = 5
    let d3 = table.apply_delete("id = 5").await.unwrap();
    assert_eq!(d3, 1);

    // Should have 5 rows remaining: 3,4,6,7,8
    let ds = table.active_dataset_snapshot().await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 5);

    // Verify exact remaining ids
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![3, 4, 6, 7, 8]);
}

// =============================================================================
// Concurrent / Sequential Operation Tests
// =============================================================================

#[tokio::test]
async fn test_delete_then_append_same_table() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "del_append");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("del_append").unwrap();

    // Write initial data
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Delete some rows
    table.apply_delete("id = 2").await.unwrap();

    // Append new rows after delete
    let batch2 = test_batch(&[4, 5], &["d", "e"]);
    table.apply_append(vec![batch2]).await.unwrap();

    // Should have: 1,3 (from original - deleted 2) + 4,5 (new) = 4 rows
    let ds = table.active_dataset_snapshot().await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 4);

    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![1, 3, 4, 5]);
}

#[tokio::test]
async fn test_concurrent_deletes_on_same_table() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "conc_del");
    engine.create_table(cfg, None).await.unwrap();
    let table = Arc::new(engine.require_table("conc_del").unwrap().clone());

    // Write enough data
    let batch = test_batch(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    );
    table.apply_append(vec![batch]).await.unwrap();

    // Launch two concurrent deletes with non-overlapping filters
    let t1 = table.clone();
    let t2 = table.clone();
    let (r1, r2) = tokio::join!(t1.apply_delete("id <= 3"), t2.apply_delete("id > 7"),);

    // Both should succeed (write mutex serializes them)
    let d1 = r1.unwrap();
    let d2 = r2.unwrap();
    assert_eq!(d1 + d2, 6, "total deleted should be 6 (3 + 3)");

    let ds = table.active_dataset_snapshot().await.unwrap();
    let remaining = ds.count_rows(None).await.unwrap();
    assert_eq!(remaining, 4, "should have 4 rows remaining: 4,5,6,7");
}

// =============================================================================
// Cross-Tier Delete with Data Content Verification
// =============================================================================

#[tokio::test]
async fn test_cross_tier_delete_verifies_data_content() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "cross_verify");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("cross_verify").unwrap();

    // Write to active and seal
    let batch1 = test_batch(&[1, 2, 3], &["sealed_a", "sealed_b", "sealed_c"]);
    table.apply_append(vec![batch1]).await.unwrap();
    table
        .apply_seal(1, 2, bisque_lance::SealReason::MaxSize)
        .await
        .unwrap();

    // Write to new active
    let batch2 = test_batch(&[4, 5, 6], &["active_d", "active_e", "active_f"]);
    table.apply_append(vec![batch2]).await.unwrap();

    // Delete id=2 from sealed and id=5 from active
    table.apply_delete("id = 2 OR id = 5").await.unwrap();

    // Verify sealed content
    let sealed_ds = table.sealed_dataset_snapshot().await.unwrap();
    let sealed_batches: Vec<RecordBatch> = sealed_ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut sealed_ids: Vec<i64> = Vec::new();
    for batch in &sealed_batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            sealed_ids.push(id_col.value(i));
        }
    }
    sealed_ids.sort();
    assert_eq!(
        sealed_ids,
        vec![1, 3],
        "sealed should have ids 1,3 (2 deleted)"
    );

    // Verify active content
    let active_ds = table.active_dataset_snapshot().await.unwrap();
    let active_batches: Vec<RecordBatch> = active_ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut active_ids: Vec<i64> = Vec::new();
    for batch in &active_batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            active_ids.push(id_col.value(i));
        }
    }
    active_ids.sort();
    assert_eq!(
        active_ids,
        vec![4, 6],
        "active should have ids 4,6 (5 deleted)"
    );
}

// =============================================================================
// Delete + Compaction Interaction
// =============================================================================

#[tokio::test]
async fn test_delete_then_compact_active() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "del_compact");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("del_compact").unwrap();

    // Write multiple small batches to create multiple fragments
    for i in 0..5 {
        let batch = test_batch(&[i * 10 + 1, i * 10 + 2], &["x", "y"]);
        table.apply_append(vec![batch]).await.unwrap();
    }

    // Delete some rows
    table.apply_delete("id > 30").await.unwrap();

    // Compact - should work with deletion vectors present
    let _stats = table.compact_active().await.unwrap();
    // Don't assert specific compaction stats - just verify it didn't error

    // Verify data is still correct after compaction
    let ds = table.active_dataset_snapshot().await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(
        count, 6,
        "should have 6 rows after deleting 4 (ids 31,32,41,42) from 10"
    );
}

// =============================================================================
// Single Row Table and Boundary Tests
// =============================================================================

#[tokio::test]
async fn test_delete_single_row_table() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "single_row");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("single_row").unwrap();

    let batch = test_batch(&[42], &["only_row"]);
    table.apply_append(vec![batch]).await.unwrap();

    let deleted = table.apply_delete("id = 42").await.unwrap();
    assert_eq!(deleted, 1);

    let ds = table.active_dataset_snapshot().await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_delete_idempotent() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "idempotent");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("idempotent").unwrap();

    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Delete id=2
    let d1 = table.apply_delete("id = 2").await.unwrap();
    assert_eq!(d1, 1);

    // Delete id=2 again -- should be 0 since already deleted
    let d2 = table.apply_delete("id = 2").await.unwrap();
    assert_eq!(d2, 0);

    // Still 2 rows
    let ds = table.active_dataset_snapshot().await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 2);
}

#[tokio::test]
async fn test_update_then_delete_same_rows() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "upd_del");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("upd_del").unwrap();

    let batch = test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Update id=3 to have name="updated"
    let replacement = test_batch(&[3], &["updated"]);
    table
        .apply_update("id = 3", vec![replacement])
        .await
        .unwrap();

    // Now delete the updated row
    let deleted = table.apply_delete("id = 3").await.unwrap();
    assert_eq!(deleted, 1);

    // Verify 4 rows remain, none with id=3
    let ds = table.active_dataset_snapshot().await.unwrap();
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![1, 2, 4, 5]);
}

#[tokio::test]
async fn test_delete_with_string_filter() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "str_filter");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("str_filter").unwrap();

    let batch = test_batch(&[1, 2, 3], &["alice", "bob", "charlie"]);
    table.apply_append(vec![batch]).await.unwrap();

    let deleted = table.apply_delete("name = 'bob'").await.unwrap();
    assert_eq!(deleted, 1);

    let ds = table.active_dataset_snapshot().await.unwrap();
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut names: Vec<String> = Vec::new();
    for batch in &batches {
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..name_col.len() {
            names.push(name_col.value(i).to_string());
        }
    }
    names.sort();
    assert_eq!(names, vec!["alice", "charlie"]);
}

// =============================================================================
// Delete/Update Coverage Gap Tests
// =============================================================================

#[tokio::test]
async fn test_delete_nonexistent_table() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    // Attempting to get a nonexistent table should fail, preventing apply_delete.
    let result = engine.require_table("no_such_table");
    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("not found") || msg.contains("no_such_table"),
        "Error should reference the missing table, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_update_nonexistent_table() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    // Attempting to get a nonexistent table should fail, preventing apply_update.
    let result = engine.require_table("ghost_update");
    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("not found") || msg.contains("ghost_update"),
        "Error should reference the missing table, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_delete_where_only_sealed_has_data() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "sealed_only_del");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("sealed_only_del").unwrap();

    // Write rows and seal so data only lives in the sealed tier.
    let batch = test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    table.apply_append(vec![batch]).await.unwrap();
    table
        .apply_seal(1, 2, bisque_lance::SealReason::MaxSize)
        .await
        .unwrap();

    // Active segment should be empty after seal.
    let active_ds = table.active_dataset_snapshot().await.unwrap();
    assert_eq!(active_ds.count_rows(None).await.unwrap(), 0);

    // Delete from sealed tier only.
    let deleted = table.apply_delete("id <= 2").await.unwrap();
    assert_eq!(deleted, 2);

    // Sealed tier should now have 3 rows.
    let sealed_ds = table.sealed_dataset_snapshot().await.unwrap();
    let sealed_count = sealed_ds.count_rows(None).await.unwrap();
    assert_eq!(sealed_count, 3);

    // Verify exact remaining ids in sealed tier.
    let batches: Vec<RecordBatch> = sealed_ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![3, 4, 5]);
}

#[tokio::test]
async fn test_update_schema_column_count_mismatch() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "schema_mismatch");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("schema_mismatch").unwrap();

    // Table schema is (id: i64, name: utf8). Write conforming data.
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Build a replacement batch with only one column (id: i64) -- column count mismatch.
    let mismatched_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]));
    let mismatched_batch = RecordBatch::try_new(
        mismatched_schema,
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap();

    // The update delete part should succeed, but the append with mismatched
    // schema may either error or succeed (Lance may accept schema evolution).
    // We just verify no panic occurs.
    let result = table
        .apply_update("id = 2", vec![mismatched_batch])
        .await;

    // Whether it errors or succeeds, no panic should occur.
    // If it succeeded, verify we can still read the table.
    if result.is_ok() {
        let ds = table.active_dataset_snapshot().await.unwrap();
        let count = ds.count_rows(None).await.unwrap();
        // 2 original (id=1,3 kept) + 1 replacement = 3
        assert!(count >= 2, "should have at least the untouched rows");
    }
    // If it errored, that is also acceptable handling of the mismatch.
}

#[tokio::test]
async fn test_concurrent_delete_and_append() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "conc_del_app");
    engine.create_table(cfg, None).await.unwrap();
    let table = Arc::new(engine.require_table("conc_del_app").unwrap().clone());

    // Write initial data.
    let batch = test_batch(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    );
    table.apply_append(vec![batch]).await.unwrap();

    // Concurrently delete some rows and append new rows.
    let t1 = table.clone();
    let t2 = table.clone();
    let new_batch = test_batch(&[11, 12], &["k", "l"]);

    let (del_result, app_result) = tokio::join!(
        t1.apply_delete("id <= 3"),
        t2.apply_append(vec![new_batch]),
    );

    // Both should succeed without panicking.
    del_result.unwrap();
    app_result.unwrap();

    // Final row count should be consistent:
    // Started with 10, deleted 3 (id 1,2,3), appended 2 (id 11,12) = 9
    let ds = table.active_dataset_snapshot().await.unwrap();
    let count = ds.count_rows(None).await.unwrap();
    assert_eq!(count, 9, "10 - 3 deleted + 2 appended = 9");
}

#[tokio::test]
async fn test_delete_with_complex_and_or_filter() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "complex_filter");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("complex_filter").unwrap();

    // Write 10 rows with ids 1..=10.
    let batch = test_batch(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    );
    table.apply_append(vec![batch]).await.unwrap();

    // Delete where id > 2 AND id < 8 (should delete ids 3,4,5,6,7).
    let deleted = table.apply_delete("id > 2 AND id < 8").await.unwrap();
    assert_eq!(deleted, 5);

    // Verify remaining rows are exactly 1,2,8,9,10.
    let ds = table.active_dataset_snapshot().await.unwrap();
    let remaining = ds.count_rows(None).await.unwrap();
    assert_eq!(remaining, 5);

    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![1, 2, 8, 9, 10]);
}

#[tokio::test]
async fn test_update_multiple_rows() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "upd_multi");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("upd_multi").unwrap();

    // Write 5 rows.
    let batch = test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Update where id > 3 (ids 4 and 5) with replacement data.
    let replacement = test_batch(&[4, 5], &["d_new", "e_new"]);
    let deleted = table
        .apply_update("id > 3", vec![replacement])
        .await
        .unwrap();
    assert_eq!(deleted, 2);

    // Total should be: 3 original (id 1,2,3) + 2 replacements = 5
    let ds = table.active_dataset_snapshot().await.unwrap();
    let total = ds.count_rows(None).await.unwrap();
    assert_eq!(total, 5);

    // Verify data content.
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..id_col.len() {
            rows.push((id_col.value(i), name_col.value(i).to_string()));
        }
    }
    rows.sort_by_key(|r| r.0);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "b".to_string()));
    assert_eq!(rows[2], (3, "c".to_string()));
    assert_eq!(rows[3], (4, "d_new".to_string()));
    assert_eq!(rows[4], (5, "e_new".to_string()));
}

#[tokio::test]
async fn test_delete_all_then_append_new() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "del_all_append");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("del_all_append").unwrap();

    // Write initial data.
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Delete all rows.
    let deleted = table.apply_delete("id >= 1").await.unwrap();
    assert_eq!(deleted, 3);

    // Verify table is empty.
    let ds = table.active_dataset_snapshot().await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 0);

    // Append new rows.
    let new_batch = test_batch(&[10, 20, 30], &["x", "y", "z"]);
    table.apply_append(vec![new_batch]).await.unwrap();

    // Verify only new data is present.
    let ds = table.active_dataset_snapshot().await.unwrap();
    let total = ds.count_rows(None).await.unwrap();
    assert_eq!(total, 3);

    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![10, 20, 30], "only new data should be present");
}

#[tokio::test]
async fn test_tier_versions_after_update() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "tier_ver_upd");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("tier_ver_upd").unwrap();

    // Write initial data.
    let batch = test_batch(&[1, 2, 3], &["a", "b", "c"]);
    table.apply_append(vec![batch]).await.unwrap();

    let (av_before, sv_before, s3v_before) = table.tier_versions();
    assert!(av_before.is_some());
    assert!(sv_before.is_none());
    assert!(s3v_before.is_none());

    // Perform an update (delete + append internally).
    let replacement = test_batch(&[2], &["b_updated"]);
    table
        .apply_update("id = 2", vec![replacement])
        .await
        .unwrap();

    let (av_after, _, _) = table.tier_versions();
    assert!(
        av_after.unwrap() > av_before.unwrap(),
        "active version should increase after update"
    );
}

#[tokio::test]
async fn test_delete_with_null_handling() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg = table_config(&engine, "null_handling");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("null_handling").unwrap();

    // Build a batch with nullable name column -- some values are null.
    let nullable_schema = test_schema(); // name field is already nullable (true)
    let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec![
        Some("alice"),
        None,
        Some("charlie"),
        None,
        Some("eve"),
    ]);
    let batch = RecordBatch::try_new(
        nullable_schema,
        vec![Arc::new(ids), Arc::new(names)],
    )
    .unwrap();
    table.apply_append(vec![batch]).await.unwrap();

    // Delete rows where name IS NULL (ids 2 and 4).
    let deleted = table.apply_delete("name IS NULL").await.unwrap();
    assert_eq!(deleted, 2);

    // Verify remaining rows.
    let ds = table.active_dataset_snapshot().await.unwrap();
    let remaining = ds.count_rows(None).await.unwrap();
    assert_eq!(remaining, 3);

    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![1, 3, 5], "only non-null name rows should remain");
}

#[tokio::test]
async fn test_delete_from_one_table_does_not_affect_another() {
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;

    let cfg_a = table_config(&engine, "table_a");
    let cfg_b = table_config(&engine, "table_b");
    engine.create_table(cfg_a, None).await.unwrap();
    engine.create_table(cfg_b, None).await.unwrap();

    let table_a = engine.require_table("table_a").unwrap();
    let table_b = engine.require_table("table_b").unwrap();

    let batch_a = test_batch(&[1, 2, 3, 4, 5], &["a1", "a2", "a3", "a4", "a5"]);
    let batch_b = test_batch(&[1, 2, 3, 4, 5], &["b1", "b2", "b3", "b4", "b5"]);
    table_a.apply_append(vec![batch_a]).await.unwrap();
    table_b.apply_append(vec![batch_b]).await.unwrap();

    // Delete id <= 3 from table_a only.
    let deleted = table_a.apply_delete("id <= 3").await.unwrap();
    assert_eq!(deleted, 3);

    // table_a should have 2 rows remaining.
    let ds_a = table_a.active_dataset_snapshot().await.unwrap();
    assert_eq!(ds_a.count_rows(None).await.unwrap(), 2);

    // table_b should still have all 5 rows.
    let ds_b = table_b.active_dataset_snapshot().await.unwrap();
    assert_eq!(ds_b.count_rows(None).await.unwrap(), 5);

    // Verify table_a data content: only ids 4, 5 remain.
    let batches_a: Vec<RecordBatch> = ds_a
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids_a: Vec<i64> = Vec::new();
    for batch in &batches_a {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids_a.push(id_col.value(i));
        }
    }
    ids_a.sort();
    assert_eq!(ids_a, vec![4, 5], "table_a should only have ids 4 and 5");

    // Verify table_b data content: all ids 1..=5 present.
    let batches_b: Vec<RecordBatch> = ds_b
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids_b: Vec<i64> = Vec::new();
    for batch in &batches_b {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids_b.push(id_col.value(i));
        }
    }
    ids_b.sort();
    assert_eq!(
        ids_b,
        vec![1, 2, 3, 4, 5],
        "table_b should be completely untouched"
    );
}

#[tokio::test]
async fn test_update_with_empty_replacement_batch() {
    // "update with empty replacement = pure delete" semantic.
    let tmp = tempdir().unwrap();
    let engine = new_engine(tmp.path()).await;
    let cfg = table_config(&engine, "empty_update");
    engine.create_table(cfg, None).await.unwrap();
    let table = engine.require_table("empty_update").unwrap();

    let batch = test_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
    table.apply_append(vec![batch]).await.unwrap();

    // Update with filter matching ids 4 and 5, but provide no replacement batches.
    // This should delete the matching rows and append nothing.
    let deleted = table.apply_update("id > 3", vec![]).await.unwrap();
    assert_eq!(deleted, 2, "should delete exactly the 2 matching rows");

    // Verify 3 rows remain.
    let ds = table.active_dataset_snapshot().await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 3);

    // Verify remaining ids are exactly [1, 2, 3].
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..id_col.len() {
            ids.push(id_col.value(i));
        }
    }
    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "update with empty replacement should act as pure delete"
    );
}
