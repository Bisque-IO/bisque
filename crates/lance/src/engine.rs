//! BisqueLance multi-table storage engine — manages a registry of tables,
//! each with its own segment lifecycle (active → sealed → deep storage).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::config::{BisqueLanceConfig, TableOpenConfig};
use crate::error::{Error, Result};
use crate::ipc;
use crate::table_engine::TableEngine;
use crate::types::{PersistedTableEntry, SegmentCatalog, TableSnapshot};

// =============================================================================
// Snapshot Transfer Guard
// =============================================================================

/// Prevents file deletion while snapshot transfers are in flight.
///
/// When a leader builds a snapshot for a fresh follower, the file manifest
/// lists segment files that must not be deleted until the transfer completes.
/// This guard defers `remove_dir_all` calls (from `apply_promote` and
/// `drop_table`) while any snapshot transfer is active, then flushes
/// deferred deletions when the last transfer completes.
///
/// Also tracks per-transfer purge floor watermarks so that
/// `update_purge_floor()` never raises the floor above the minimum
/// watermark of any active transfer.
pub struct SnapshotTransferGuard {
    /// Number of active snapshot transfers.
    active_count: AtomicUsize,
    /// Paths queued for deletion once all transfers complete.
    deferred_deletions: Mutex<Vec<PathBuf>>,
    /// Purge floor watermarks for active transfers. Each entry is
    /// the `last_applied` index at the time the snapshot was built.
    /// The minimum of these constrains log purging while transfers
    /// are in flight.
    active_watermarks: Mutex<Vec<u64>>,
}

impl SnapshotTransferGuard {
    /// Create a new guard with no active transfers.
    pub fn new() -> Self {
        Self {
            active_count: AtomicUsize::new(0),
            deferred_deletions: Mutex::new(Vec::new()),
            active_watermarks: Mutex::new(Vec::new()),
        }
    }

    /// Acquire the guard — increments the active transfer count.
    ///
    /// `watermark` is the `last_applied` log index at the time the snapshot
    /// was built. It constrains log purging: `min_watermark()` returns the
    /// minimum across all active handles so `update_purge_floor()` never
    /// raises the floor above it.
    ///
    /// Returns a [`SnapshotGuardHandle`] that auto-releases on `Drop`
    /// (panic-safe). The caller should store this handle for the duration
    /// of the snapshot transfer. If the handle is not dropped within
    /// `timeout`, a background task forcibly releases it.
    ///
    /// Multiple handles can be active concurrently (e.g., multiple nodes
    /// joining simultaneously). File deletions are deferred until ALL
    /// handles are released.
    pub fn acquire(
        self: &Arc<Self>,
        timeout: Duration,
        watermark: Option<u64>,
    ) -> SnapshotGuardHandle {
        self.active_count.fetch_add(1, Ordering::SeqCst);
        if let Some(w) = watermark {
            self.active_watermarks.lock().push(w);
        }
        let released = Arc::new(AtomicBool::new(false));

        // Spawn timeout task as safety net — if the handle is never dropped
        // (e.g., follower crashes and snapshot builder is leaked), this
        // ensures the guard doesn't stay active forever.
        let guard = self.clone();
        let released_clone = released.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if !released_clone.swap(true, Ordering::SeqCst) {
                warn!(
                    timeout_secs = timeout.as_secs(),
                    "Snapshot transfer guard timed out, auto-releasing"
                );
                guard.do_release(watermark);
            }
        });

        info!(
            active = self.active_count.load(Ordering::SeqCst),
            ?watermark,
            "Snapshot transfer guard acquired"
        );

        SnapshotGuardHandle {
            guard: self.clone(),
            released,
            watermark,
        }
    }

    /// Returns the minimum purge floor watermark across all active
    /// snapshot transfers, or `None` if no transfers are active.
    ///
    /// Used by `update_purge_floor()` to ensure the floor never rises
    /// above the earliest snapshot point that still has an active file
    /// transfer in progress.
    pub fn min_watermark(&self) -> Option<u64> {
        let watermarks = self.active_watermarks.lock();
        watermarks.iter().copied().min()
    }

    /// Check if any snapshot transfer is active and defer the deletion if so.
    ///
    /// Returns `true` if the path was deferred (caller should NOT delete).
    /// Returns `false` if no transfer is active (caller should delete now).
    pub fn defer_or_delete(&self, path: PathBuf) -> bool {
        if self.active_count.load(Ordering::SeqCst) > 0 {
            info!(
                path = %path.display(),
                "Deferring deletion — snapshot transfer in progress"
            );
            self.deferred_deletions.lock().push(path);
            true
        } else {
            false
        }
    }

    /// Returns the number of active snapshot transfers.
    pub fn active_count(&self) -> usize {
        self.active_count.load(Ordering::SeqCst)
    }

    /// Internal: decrement count, remove watermark, and flush deferred deletions if last.
    fn do_release(&self, watermark: Option<u64>) {
        // Remove this handle's watermark from the active set
        if let Some(w) = watermark {
            let mut watermarks = self.active_watermarks.lock();
            if let Some(pos) = watermarks.iter().position(|&v| v == w) {
                watermarks.swap_remove(pos);
            }
        }

        let prev = self.active_count.fetch_sub(1, Ordering::SeqCst);
        info!(active = prev - 1, "Snapshot transfer guard released");

        if prev == 1 {
            // Last transfer done — flush deferred deletions
            let paths: Vec<PathBuf> = self.deferred_deletions.lock().drain(..).collect();
            if !paths.is_empty() {
                info!(
                    count = paths.len(),
                    "Flushing deferred deletions after snapshot transfer"
                );
                for path in paths {
                    if path.exists() {
                        if let Err(e) = std::fs::remove_dir_all(&path) {
                            warn!(
                                path = %path.display(),
                                "Deferred deletion failed: {}", e
                            );
                        }
                    }
                }
            }
        }
    }
}

/// RAII handle returned by [`SnapshotTransferGuard::acquire`].
///
/// Releases the guard on [`Drop`] — this makes it panic-safe. The caller
/// must store this handle for the duration of the snapshot transfer.
/// Dropping it early releases the guard early. Multiple concurrent handles
/// are supported; file deletions are deferred until the last handle drops.
///
/// A background timeout task provides a safety net: if the handle is somehow
/// leaked (not dropped), the timeout will release it after the configured
/// duration. The `released` flag ensures exactly one release per handle,
/// regardless of whether Drop or the timeout fires first.
pub struct SnapshotGuardHandle {
    guard: Arc<SnapshotTransferGuard>,
    released: Arc<AtomicBool>,
    /// The purge floor watermark this handle was acquired with.
    watermark: Option<u64>,
}

impl SnapshotGuardHandle {
    /// Check if this handle has already been released (by Drop, explicit
    /// release, or timeout).
    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::SeqCst)
    }

    /// Explicitly release the guard. Safe to call multiple times and safe
    /// to call before drop — only the first release has any effect.
    pub fn release(&self) {
        if !self.released.swap(true, Ordering::SeqCst) {
            self.guard.do_release(self.watermark);
        }
    }
}

impl Drop for SnapshotGuardHandle {
    fn drop(&mut self) {
        if !self.released.swap(true, Ordering::SeqCst) {
            self.guard.do_release(self.watermark);
        }
    }
}

/// Multi-table storage engine. Holds a registry of [`TableEngine`] instances
/// keyed by table name.
///
/// Use [`create_table`](Self::create_table) to add tables and
/// [`get_table`](Self::get_table) to access them by name.
pub struct BisqueLance {
    config: BisqueLanceConfig,
    tables: DashMap<String, Arc<TableEngine>>,
    snapshot_guard: Arc<SnapshotTransferGuard>,
    catalog_name: String,
}

impl BisqueLance {
    /// Create a new multi-table engine with no tables.
    ///
    /// Tables are added via [`create_table`](Self::create_table) or
    /// restored from a snapshot via [`restore_tables`](Self::restore_tables).
    pub async fn open(config: BisqueLanceConfig) -> Result<Self> {
        Self::open_with_catalog(config, String::new()).await
    }

    /// Open the engine with a catalog name for metric labeling.
    pub async fn open_with_catalog(
        config: BisqueLanceConfig,
        catalog_name: String,
    ) -> Result<Self> {
        // Ensure the base data directory exists
        tokio::fs::create_dir_all(&config.local_data_dir).await?;

        info!(
            data_dir = %config.local_data_dir.display(),
            "BisqueLance multi-table engine opened"
        );

        Ok(Self {
            config,
            tables: DashMap::new(),
            snapshot_guard: Arc::new(SnapshotTransferGuard::new()),
            catalog_name,
        })
    }

    /// Create a new table and register it in the engine.
    ///
    /// Returns an `Arc<TableEngine>` for the newly created table.
    pub async fn create_table(
        &self,
        config: TableOpenConfig,
        catalog: Option<SegmentCatalog>,
    ) -> Result<Arc<TableEngine>> {
        let name = config.name.clone();

        // Idempotent: return existing table if it already exists.
        if let Some(existing) = self.tables.get(&name) {
            debug!(table = %name, "create_table: already exists (idempotent)");
            return Ok(existing.clone());
        }

        let engine = Arc::new(
            TableEngine::open(
                config,
                catalog,
                None,
                self.snapshot_guard.clone(),
                self.catalog_name.clone(),
            )
            .await?,
        );
        self.tables.insert(name.clone(), engine.clone());

        info!(table = %name, "Table created");
        Ok(engine)
    }

    /// Create or restore a table from snapshot data (catalog + schema history).
    pub async fn restore_table(
        &self,
        config: TableOpenConfig,
        snapshot: &TableSnapshot,
    ) -> Result<Arc<TableEngine>> {
        let name = config.name.clone();

        let engine = Arc::new(
            TableEngine::open(
                config,
                Some(snapshot.catalog.clone()),
                Some(snapshot.schema_history.clone()),
                self.snapshot_guard.clone(),
                self.catalog_name.clone(),
            )
            .await?,
        );
        self.tables.insert(name.clone(), engine.clone());

        info!(table = %name, "Table restored from snapshot");
        Ok(engine)
    }

    /// Get a table engine by name.
    pub fn get_table(&self, name: &str) -> Option<Arc<TableEngine>> {
        self.tables.get(name).map(|r| r.value().clone())
    }

    /// Get a table engine by name, returning an error if not found.
    pub fn require_table(&self, name: &str) -> Result<Arc<TableEngine>> {
        self.get_table(name)
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    /// List all table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|r| r.key().clone()).collect()
    }

    /// Iterate over all tables without allocating a name list.
    pub fn for_each_table(&self, mut f: impl FnMut(&str, &Arc<TableEngine>)) {
        for entry in self.tables.iter() {
            f(entry.key(), entry.value());
        }
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Drop a table — shuts it down and removes from registry.
    ///
    /// Also deletes the table's local data directory.
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        let engine = match self.tables.remove(name) {
            Some((_, v)) => v,
            None => {
                debug!(table = %name, "drop_table: already dropped (idempotent)");
                // Clean up orphaned data directory if it exists (crash recovery).
                let table_dir = self.config.table_data_dir(name);
                if table_dir.exists() {
                    if !self.snapshot_guard.defer_or_delete(table_dir.clone()) {
                        tokio::fs::remove_dir_all(&table_dir).await?;
                        debug!(table = %name, path = %table_dir.display(), "Removed orphaned table data directory");
                    }
                }
                return Ok(());
            }
        };

        engine.shutdown().await?;

        // Remove local data directory (deferred if snapshot transfer is active)
        let table_dir = self.config.table_data_dir(name);
        if table_dir.exists() {
            if !self.snapshot_guard.defer_or_delete(table_dir.clone()) {
                tokio::fs::remove_dir_all(&table_dir).await?;
                info!(table = %name, path = %table_dir.display(), "Removed table data directory");
            }
        }

        info!(table = %name, "Table dropped");
        Ok(())
    }

    /// Get the node-level config.
    pub fn config(&self) -> &BisqueLanceConfig {
        &self.config
    }

    /// Get the snapshot transfer guard for coordinating file deletion
    /// with active snapshot transfers.
    pub fn snapshot_guard(&self) -> &Arc<SnapshotTransferGuard> {
        &self.snapshot_guard
    }

    /// Collect full persisted entries for all tables (config + state).
    ///
    /// Used by the snapshot builder — this is the system-of-record data
    /// that gets serialized into Raft snapshots.
    pub fn table_entries(&self) -> HashMap<String, PersistedTableEntry> {
        self.tables
            .iter()
            .map(|entry| {
                let pe = PersistedTableEntry {
                    config: entry.value().config().to_persisted(),
                    catalog: entry.value().catalog(),
                    flush_state: entry.value().flush_state(),
                    schema_history: entry.value().schema_history(),
                };
                (entry.key().clone(), pe)
            })
            .collect()
    }

    /// Restore tables from MDBX manifest data (full persisted entries).
    ///
    /// Called during crash recovery — reads per-table entries from the
    /// manifest and recreates each `TableEngine` with the persisted config,
    /// catalog, flush state, and schema history.
    pub async fn restore_from_persisted_entries(
        &self,
        entries: HashMap<String, PersistedTableEntry>,
    ) -> Result<()> {
        for (table_name, entry) in &entries {
            let schema = if let Some(latest) = entry.schema_history.last() {
                match ipc::schema_from_ipc(&latest.schema_ipc) {
                    Ok(s) => Some(Arc::new(s)),
                    Err(e) => {
                        warn!(table = %table_name, "Failed to decode schema from manifest: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            let config =
                TableOpenConfig::from_persisted(table_name, &entry.config, schema, &self.config);

            let snapshot = entry.to_snapshot();
            if let Err(e) = self.restore_table(config, &snapshot).await {
                warn!(table = %table_name, "Failed to restore table from manifest: {}", e);
            }
        }

        info!(tables = entries.len(), "Restored tables from MDBX manifest");
        Ok(())
    }

    /// Gracefully shutdown all tables.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down BisqueLance engine (all tables)");
        let tables: Vec<(String, Arc<TableEngine>)> = self
            .tables
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();

        for (name, engine) in tables {
            if let Err(e) = engine.shutdown().await {
                warn!(table = %name, "Error shutting down table: {}", e);
            }
        }

        self.tables.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_guard_defers_deletion_while_active() {
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir = tmp.path().join("tables/t1/segments/1.lance");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("data.lance"), b"test data").unwrap();

        // Acquire guard handle
        let handle = guard.acquire(Duration::from_secs(60), None);

        // defer_or_delete should return true (deferred)
        assert!(guard.defer_or_delete(dir.clone()));
        assert!(dir.exists());

        // Release via handle — should flush deferred deletions
        handle.release();
        assert!(!dir.exists());
    }

    #[test]
    fn test_guard_immediate_delete_when_inactive() {
        let guard = Arc::new(SnapshotTransferGuard::new());

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("some_dir");
        std::fs::create_dir_all(&dir).unwrap();

        // No active guard — should return false (caller deletes)
        assert!(!guard.defer_or_delete(dir.clone()));
    }

    #[tokio::test]
    async fn test_guard_multiple_acquires_concurrent() {
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir = tmp.path().join("segment_dir");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("data"), b"content").unwrap();

        // Two concurrent snapshot transfers (e.g., two nodes joining)
        let h1 = guard.acquire(Duration::from_secs(60), None);
        let h2 = guard.acquire(Duration::from_secs(60), None);
        assert_eq!(guard.active_count(), 2);

        assert!(guard.defer_or_delete(dir.clone()));

        // First drop — should NOT flush (still one active)
        drop(h1);
        assert_eq!(guard.active_count(), 1);
        assert!(dir.exists(), "Should not delete until all transfers done");

        // Second drop — should flush
        drop(h2);
        assert_eq!(guard.active_count(), 0);
        assert!(!dir.exists(), "Should delete after all transfers done");
    }

    #[tokio::test]
    async fn test_guard_timeout_auto_release() {
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir = tmp.path().join("timeout_dir");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("data"), b"content").unwrap();

        // Short timeout — handle is held but timeout fires first
        let _handle = guard.acquire(Duration::from_millis(100), None);
        assert!(guard.defer_or_delete(dir.clone()));
        assert!(dir.exists());

        // Wait for timeout to fire
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(guard.active_count(), 0);
        assert!(
            !dir.exists(),
            "Timeout should have flushed deferred deletions"
        );
    }

    #[tokio::test]
    async fn test_guard_handle_prevents_double_release() {
        let guard = Arc::new(SnapshotTransferGuard::new());
        let handle = guard.acquire(Duration::from_secs(60), None);

        assert_eq!(guard.active_count(), 1);
        handle.release();
        assert_eq!(guard.active_count(), 0);
        // Second release is a no-op
        handle.release();
        assert_eq!(guard.active_count(), 0);
        // Drop after release is also a no-op
        drop(handle);
        assert_eq!(guard.active_count(), 0);
    }

    #[tokio::test]
    async fn test_guard_drop_releases_automatically() {
        // Verify that dropping a handle (without explicit release) releases
        // the guard — this is what happens during panic stack unwinding.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir = tmp.path().join("drop_dir");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("data"), b"content").unwrap();

        {
            let handle = guard.acquire(Duration::from_secs(60), None);
            assert_eq!(guard.active_count(), 1);
            guard.defer_or_delete(dir.clone());
            assert!(dir.exists());
            drop(handle); // simulates what happens during panic unwinding
        }

        assert_eq!(guard.active_count(), 0);
        assert!(!dir.exists(), "Drop should have flushed deferred deletions");
    }

    // =========================================================================
    // Concurrent deferred deletions
    // =========================================================================

    #[tokio::test]
    async fn test_guard_multiple_deferred_deletions_flushed_atomically() {
        // Multiple directories deferred during a single transfer are all
        // deleted when the transfer completes.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dirs: Vec<PathBuf> = (0..5)
            .map(|i| {
                let d = tmp.path().join(format!("seg_{i}"));
                std::fs::create_dir_all(&d).unwrap();
                std::fs::write(d.join("data"), b"x").unwrap();
                d
            })
            .collect();

        let handle = guard.acquire(Duration::from_secs(60), None);

        for d in &dirs {
            assert!(guard.defer_or_delete(d.clone()));
        }

        // All still exist while transfer active
        for d in &dirs {
            assert!(d.exists(), "Deferred dir should exist during transfer");
        }

        drop(handle);

        // All deleted after release
        for d in &dirs {
            assert!(!d.exists(), "Deferred dir should be deleted after release");
        }
    }

    #[tokio::test]
    async fn test_guard_interleaved_acquire_defer_release() {
        // Transfer A starts, defers dir1. Transfer B starts, defers dir2.
        // A finishes — neither should be deleted (B still active).
        // B finishes — both should be deleted.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir1 = tmp.path().join("dir1");
        let dir2 = tmp.path().join("dir2");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(dir1.join("data"), b"1").unwrap();
        std::fs::write(dir2.join("data"), b"2").unwrap();

        let handle_a = guard.acquire(Duration::from_secs(60), None);
        assert!(guard.defer_or_delete(dir1.clone()));

        let handle_b = guard.acquire(Duration::from_secs(60), None);
        assert!(guard.defer_or_delete(dir2.clone()));

        // Release A — both dirs still exist (B still active)
        drop(handle_a);
        assert_eq!(guard.active_count(), 1);
        assert!(
            dir1.exists(),
            "dir1 should survive until all transfers done"
        );
        assert!(
            dir2.exists(),
            "dir2 should survive until all transfers done"
        );

        // Release B — now both are deleted
        drop(handle_b);
        assert_eq!(guard.active_count(), 0);
        assert!(!dir1.exists(), "dir1 should be deleted");
        assert!(!dir2.exists(), "dir2 should be deleted");
    }

    #[tokio::test]
    async fn test_guard_deferred_deletion_of_already_gone_path() {
        // If the path was already removed (e.g., by another process) before
        // flush, the guard should silently skip it.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir = tmp.path().join("vanished");
        std::fs::create_dir_all(&dir).unwrap();

        let handle = guard.acquire(Duration::from_secs(60), None);
        assert!(guard.defer_or_delete(dir.clone()));

        // Manually remove before release
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(!dir.exists());

        // Release should not panic when path is already gone
        drop(handle);
        assert_eq!(guard.active_count(), 0);
    }

    // =========================================================================
    // Timeout + deferred deletions interaction
    // =========================================================================

    #[tokio::test]
    async fn test_guard_timeout_flushes_deferred_deletions() {
        // When timeout fires, it should flush all pending deferred deletions.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir1 = tmp.path().join("timeout_flush_1");
        let dir2 = tmp.path().join("timeout_flush_2");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(dir1.join("f"), b"1").unwrap();
        std::fs::write(dir2.join("f"), b"2").unwrap();

        // Very short timeout
        let _handle = guard.acquire(Duration::from_millis(100), None);
        assert!(guard.defer_or_delete(dir1.clone()));
        assert!(guard.defer_or_delete(dir2.clone()));

        assert!(dir1.exists());
        assert!(dir2.exists());

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(guard.active_count(), 0);
        assert!(!dir1.exists(), "Timeout should flush dir1");
        assert!(!dir2.exists(), "Timeout should flush dir2");
    }

    #[tokio::test]
    async fn test_guard_timeout_with_multiple_handles_partial() {
        // Handle A has short timeout, handle B has long timeout.
        // When A times out, B is still active — no flush.
        // When B is dropped, flush happens.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir = tmp.path().join("partial_timeout");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("data"), b"x").unwrap();

        let _handle_a = guard.acquire(Duration::from_millis(100), None); // short timeout
        let handle_b = guard.acquire(Duration::from_secs(60), None); // long timeout

        assert!(guard.defer_or_delete(dir.clone()));

        // Wait for A's timeout
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(guard.active_count(), 1, "Only A should have timed out");
        assert!(dir.exists(), "Should not flush while B is still active");

        // Drop B
        drop(handle_b);
        assert_eq!(guard.active_count(), 0);
        assert!(!dir.exists(), "Should flush after all handles released");
    }

    // =========================================================================
    // Panic safety and cleanup guarantees
    // =========================================================================

    #[tokio::test]
    async fn test_guard_drop_in_spawned_task_panic() {
        // Simulate a panic inside a spawned task that holds a guard handle.
        // The Drop impl should release the guard even though the task panicked.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let dir = tmp.path().join("panic_task");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("data"), b"x").unwrap();

        // Spawn a task that acquires, defers a deletion, then panics
        let g = guard.clone();
        let d = dir.clone();
        let result = tokio::spawn(async move {
            let _handle = g.acquire(Duration::from_secs(60), None);
            // Defer deletion while guard is active (inside the task)
            assert!(g.defer_or_delete(d));
            panic!("simulated panic during snapshot handling");
        })
        .await;

        assert!(result.is_err(), "Task should have panicked");
        // Give a small moment for Drop to execute
        tokio::time::sleep(Duration::from_millis(10)).await;
        // The guard should be released by Drop during panic unwinding.
        assert_eq!(
            guard.active_count(),
            0,
            "Guard should be released after panic"
        );
        // Deferred deletion should have been flushed by the Drop
        assert!(
            !dir.exists(),
            "Deferred deletion should be flushed after panic"
        );
    }

    #[tokio::test]
    async fn test_guard_explicit_release_then_drop_is_idempotent() {
        // Calling release() explicitly and then letting Drop run
        // should not double-decrement.
        let guard = Arc::new(SnapshotTransferGuard::new());

        let handle = guard.acquire(Duration::from_secs(60), None);
        assert_eq!(guard.active_count(), 1);

        handle.release();
        assert_eq!(guard.active_count(), 0);

        // Drop runs — should be a no-op
        drop(handle);
        assert_eq!(guard.active_count(), 0);
    }

    #[tokio::test]
    async fn test_guard_release_and_timeout_race() {
        // If explicit release happens just before timeout fires,
        // only one release should occur.
        let guard = Arc::new(SnapshotTransferGuard::new());

        let handle = guard.acquire(Duration::from_millis(100), None);
        assert_eq!(guard.active_count(), 1);

        // Release just before timeout would fire
        tokio::time::sleep(Duration::from_millis(80)).await;
        handle.release();
        assert_eq!(guard.active_count(), 0);

        // Wait past the timeout
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Should still be 0 — timeout should not double-release
        assert_eq!(guard.active_count(), 0);
    }

    // =========================================================================
    // defer_or_delete correctness under transitions
    // =========================================================================

    #[tokio::test]
    async fn test_guard_defer_during_release_window() {
        // Verify that if a deletion arrives exactly when the guard transitions
        // from active to inactive (last handle just dropped), the deletion
        // is either deferred and flushed, or returns false for immediate delete.
        // Either outcome is safe. This is a stress test.
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        for round in 0..50 {
            let dir = tmp.path().join(format!("race_{round}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("data"), b"x").unwrap();

            let handle = guard.acquire(Duration::from_secs(60), None);

            let g = guard.clone();
            let d = dir.clone();
            let defer_handle = tokio::spawn(async move {
                // Small jitter
                tokio::time::sleep(Duration::from_micros(round * 10)).await;
                g.defer_or_delete(d)
            });

            let drop_handle = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_micros(round * 10)).await;
                drop(handle);
            });

            let was_deferred = defer_handle.await.unwrap();
            drop_handle.await.unwrap();

            // If it was deferred, the guard flush should have deleted it.
            // If it wasn't deferred, we delete it ourselves.
            if !was_deferred && dir.exists() {
                std::fs::remove_dir_all(&dir).unwrap();
            }

            // Either way, after everything settles, the dir should be gone
            // (we handled immediate case above).
            assert_eq!(guard.active_count(), 0);
        }
    }

    // =========================================================================
    // Engine-level: drop_table defers during active transfer
    // =========================================================================

    #[tokio::test]
    async fn test_drop_table_defers_deletion_logic() {
        // Tests the core logic used by BisqueLance::drop_table():
        // when guard is active, defer_or_delete returns true and the dir
        // survives; after release, dir is cleaned up.
        //
        // This directly tests the guard + filesystem interaction without
        // needing to construct a real TableEngine (which requires a Lance dataset).
        let tmp = tempfile::tempdir().unwrap();
        let guard = Arc::new(SnapshotTransferGuard::new());

        let table_dir = tmp.path().join("tables").join("doomed_table");
        std::fs::create_dir_all(table_dir.join("segments/1.lance")).unwrap();
        std::fs::write(table_dir.join("segments/1.lance/data"), b"important").unwrap();

        // Simulate active snapshot transfer
        let handle = guard.acquire(Duration::from_secs(60), None);

        // Simulate the drop_table path: defer if active
        if table_dir.exists() {
            if !guard.defer_or_delete(table_dir.clone()) {
                tokio::fs::remove_dir_all(&table_dir).await.unwrap();
            }
        }

        // Table data should still exist (deferred)
        assert!(
            table_dir.exists(),
            "Table dir should still exist during active transfer"
        );

        // Release guard — should flush deferred deletion
        drop(handle);
        assert!(
            !table_dir.exists(),
            "Table dir should be deleted after transfer completes"
        );
    }

    // =========================================================================
    // Guard handles Vec pruning (as used in state machine)
    // =========================================================================

    #[tokio::test]
    async fn test_guard_handles_vec_pruning() {
        // Simulate the pattern used in get_snapshot_builder():
        // maintain a Vec<SnapshotGuardHandle>, prune released ones.
        let guard = Arc::new(SnapshotTransferGuard::new());
        let mut handles: Vec<SnapshotGuardHandle> = Vec::new();

        // Simulate 5 snapshot builds
        for _ in 0..5 {
            handles.retain(|h| !h.is_released());
            handles.push(guard.acquire(Duration::from_secs(60), None));
        }

        assert_eq!(guard.active_count(), 5);
        assert_eq!(handles.len(), 5);

        // Release first 3 explicitly
        for h in handles.iter().take(3) {
            h.release();
        }

        // Prune
        handles.retain(|h| !h.is_released());
        assert_eq!(handles.len(), 2);
        assert_eq!(guard.active_count(), 2);

        // Drop remaining
        handles.clear();
        assert_eq!(guard.active_count(), 0);
    }

    // =========================================================================
    // Watermark tracking for purge floor protection
    // =========================================================================

    #[tokio::test]
    async fn test_guard_min_watermark_single_handle() {
        let guard = Arc::new(SnapshotTransferGuard::new());
        assert_eq!(guard.min_watermark(), None);

        let handle = guard.acquire(Duration::from_secs(60), Some(500));
        assert_eq!(guard.min_watermark(), Some(500));

        drop(handle);
        assert_eq!(guard.min_watermark(), None);
    }

    #[tokio::test]
    async fn test_guard_min_watermark_multiple_handles() {
        let guard = Arc::new(SnapshotTransferGuard::new());

        let h1 = guard.acquire(Duration::from_secs(60), Some(500));
        let h2 = guard.acquire(Duration::from_secs(60), Some(700));
        let h3 = guard.acquire(Duration::from_secs(60), Some(600));

        // min_watermark should be the minimum across all active handles
        assert_eq!(guard.min_watermark(), Some(500));

        // Release h1 (watermark 500) — min should jump to 600
        drop(h1);
        assert_eq!(guard.min_watermark(), Some(600));

        // Release h3 (watermark 600) — min should be 700
        drop(h3);
        assert_eq!(guard.min_watermark(), Some(700));

        // Release h2 — no more watermarks
        drop(h2);
        assert_eq!(guard.min_watermark(), None);
    }

    #[tokio::test]
    async fn test_guard_watermark_with_none() {
        // Handles with None watermark don't affect min_watermark
        let guard = Arc::new(SnapshotTransferGuard::new());

        let h1 = guard.acquire(Duration::from_secs(60), None);
        assert_eq!(guard.min_watermark(), None);

        let h2 = guard.acquire(Duration::from_secs(60), Some(500));
        assert_eq!(guard.min_watermark(), Some(500));

        drop(h1);
        assert_eq!(guard.min_watermark(), Some(500));

        drop(h2);
        assert_eq!(guard.min_watermark(), None);
    }

    #[tokio::test]
    async fn test_guard_watermark_timeout_removes_watermark() {
        let guard = Arc::new(SnapshotTransferGuard::new());

        // Short timeout with watermark
        let _handle = guard.acquire(Duration::from_millis(100), Some(500));
        assert_eq!(guard.min_watermark(), Some(500));

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(guard.min_watermark(), None);
        assert_eq!(guard.active_count(), 0);
    }

    #[tokio::test]
    async fn test_guard_watermark_explicit_release_removes_watermark() {
        let guard = Arc::new(SnapshotTransferGuard::new());

        let handle = guard.acquire(Duration::from_secs(60), Some(500));
        assert_eq!(guard.min_watermark(), Some(500));

        handle.release();
        assert_eq!(guard.min_watermark(), None);

        // Drop after release is idempotent
        drop(handle);
        assert_eq!(guard.min_watermark(), None);
    }

    #[tokio::test]
    async fn test_guard_watermark_concurrent_snapshots_purge_floor_scenario() {
        // Reproduces the bug scenario:
        // Snapshot 1 at log 500, snapshot 2 at log 700.
        // The min_watermark must stay at 500 until handle 1 is released.
        let guard = Arc::new(SnapshotTransferGuard::new());

        let h1 = guard.acquire(Duration::from_secs(60), Some(500));
        assert_eq!(guard.min_watermark(), Some(500));

        // Second snapshot built later
        let h2 = guard.acquire(Duration::from_secs(60), Some(700));
        // min_watermark must NOT jump to 700 — it must stay at 500
        assert_eq!(guard.min_watermark(), Some(500));

        // Transfer 1 completes
        drop(h1);
        // Now it can rise to 700
        assert_eq!(guard.min_watermark(), Some(700));

        // Transfer 2 completes
        drop(h2);
        assert_eq!(guard.min_watermark(), None);
    }

    #[tokio::test]
    async fn test_guard_watermark_duplicate_values() {
        // Two handles with the same watermark — releasing one should leave the other
        let guard = Arc::new(SnapshotTransferGuard::new());

        let h1 = guard.acquire(Duration::from_secs(60), Some(500));
        let h2 = guard.acquire(Duration::from_secs(60), Some(500));
        assert_eq!(guard.min_watermark(), Some(500));

        drop(h1);
        // Still one handle with watermark 500
        assert_eq!(guard.min_watermark(), Some(500));

        drop(h2);
        assert_eq!(guard.min_watermark(), None);
    }

    // =========================================================================
    // Purge Floor & Log Index Integration Tests
    //
    // These test the full lifecycle: BisqueLance engine with real tables,
    // log index tracking (record_log_index, min_safe_log_index), and
    // purge floor computation (compute_min_safe_log_index equivalent).
    // =========================================================================

    use arrow_schema::{DataType, Field, Schema};

    /// Create a minimal BisqueLance engine in a temp dir.
    async fn test_engine(dir: &std::path::Path) -> Arc<BisqueLance> {
        let config = crate::config::BisqueLanceConfig {
            local_data_dir: dir.to_path_buf(),
            seal_max_age: Duration::from_secs(3600),
            seal_max_size: 1024 * 1024 * 1024,
            schema: None,
            s3_uri: None,
            s3_storage_options: HashMap::new(),
            s3_max_rows_per_file: 5_000_000,
            s3_max_rows_per_group: 50_000,
            seal_indices: Vec::new(),
            compaction_target_rows_per_fragment: 1_048_576,
            compaction_materialize_deletions: true,
            compaction_deletion_threshold: 0.1,
            compaction_min_fragments: 4,
        };
        Arc::new(BisqueLance::open(config).await.unwrap())
    }

    fn test_schema() -> Arc<arrow_schema::Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn test_batch(id: i64) -> arrow_array::RecordBatch {
        use arrow_array::{Int64Array, StringArray};
        arrow_array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![format!("val_{id}")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_record_log_index_first_write_only() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // First write records the index
        assert!(table.record_log_index(100));
        assert_eq!(table.min_safe_log_index(), Some(100));

        // Subsequent writes don't change it
        assert!(!table.record_log_index(101));
        assert!(!table.record_log_index(200));
        assert_eq!(table.min_safe_log_index(), Some(100));
    }

    #[tokio::test]
    async fn test_min_safe_log_index_no_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // No writes yet — no constraint
        assert_eq!(table.min_safe_log_index(), None);
    }

    #[tokio::test]
    async fn test_seal_rotates_log_indices() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // Write to active segment
        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(100);
        assert_eq!(table.min_safe_log_index(), Some(100));

        // Seal: active (100) → sealed, new active starts fresh
        table
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();

        // sealed_first_log_index should now be 100, active should be None
        assert_eq!(table.min_safe_log_index(), Some(100));

        // Write to new active segment
        table.apply_append(vec![test_batch(2)]).await.unwrap();
        table.record_log_index(200);

        // min_safe is min(sealed=100, active=200) = 100
        assert_eq!(table.min_safe_log_index(), Some(100));
    }

    #[tokio::test]
    async fn test_promote_clears_sealed_log_index() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // Write, seal, write more
        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(100);

        table
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();

        table.apply_append(vec![test_batch(2)]).await.unwrap();
        table.record_log_index(200);

        assert_eq!(table.min_safe_log_index(), Some(100));

        // Promote sealed segment — simulates S3 upload complete
        // Note: this will try to delete segment dir; the test doesn't
        // set up S3 so s3_manifest_version is just a number.
        table.apply_promote(1, 1).await.unwrap();

        // Sealed index cleared — only active at 200 constrains
        assert_eq!(table.min_safe_log_index(), Some(200));
    }

    #[tokio::test]
    async fn test_promote_with_no_active_writes_clears_all_constraints() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // Write and seal (no writes to new active)
        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(100);

        table
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();

        // Promote sealed — no active writes means no constraint at all
        table.apply_promote(1, 1).await.unwrap();
        assert_eq!(table.min_safe_log_index(), None);
    }

    #[tokio::test]
    async fn test_multi_table_min_safe_log_index() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        // Create two tables
        let config_a = engine.config().build_table_config("table_a", test_schema());
        let table_a = engine.create_table(config_a, None).await.unwrap();

        let config_b = engine.config().build_table_config("table_b", test_schema());
        let table_b = engine.create_table(config_b, None).await.unwrap();

        // Table A: first write at log 100
        table_a.apply_append(vec![test_batch(1)]).await.unwrap();
        table_a.record_log_index(100);

        // Table B: first write at log 500
        table_b.apply_append(vec![test_batch(1)]).await.unwrap();
        table_b.record_log_index(500);

        // Engine-level min across all tables: 100
        let min = compute_min_across_tables(&engine);
        assert_eq!(min, Some(100));

        // Seal + promote table A — its constraint is cleared
        table_a
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();
        table_a.apply_promote(1, 1).await.unwrap();

        // Now min is just table B at 500
        let min = compute_min_across_tables(&engine);
        assert_eq!(min, Some(500));

        // Seal + promote table B too
        table_b
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();
        table_b.apply_promote(1, 1).await.unwrap();

        // No constraints at all
        let min = compute_min_across_tables(&engine);
        assert_eq!(min, None);
    }

    /// Replicates LanceStateMachine::compute_min_safe_log_index()
    fn compute_min_across_tables(engine: &BisqueLance) -> Option<u64> {
        let mut min_index: Option<u64> = None;
        for table_name in engine.list_tables() {
            if let Some(table) = engine.get_table(&table_name) {
                if let Some(table_min) = table.min_safe_log_index() {
                    min_index = Some(match min_index {
                        Some(current) => current.min(table_min),
                        None => table_min,
                    });
                }
            }
        }
        min_index
    }

    #[tokio::test]
    async fn test_seal_promote_full_lifecycle() {
        // Full lifecycle: write → seal → write → promote → write → seal → promote
        // Verify log indices track correctly throughout
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // Phase 1: Write to segment 1 (active), log indices 10-20
        for i in 10..=20 {
            table.apply_append(vec![test_batch(i)]).await.unwrap();
            table.record_log_index(i as u64);
        }
        assert_eq!(table.min_safe_log_index(), Some(10));

        // Phase 2: Seal segment 1, start segment 2
        table
            .apply_seal(1, 2, crate::types::SealReason::MaxSize)
            .await
            .unwrap();
        // sealed=10, active=None
        assert_eq!(table.min_safe_log_index(), Some(10));

        // Phase 3: Write to segment 2, log indices 25-35
        for i in 25..=35 {
            table.apply_append(vec![test_batch(i)]).await.unwrap();
            table.record_log_index(i as u64);
        }
        // sealed=10, active=25 → min=10
        assert_eq!(table.min_safe_log_index(), Some(10));

        // Phase 4: Promote segment 1 to S3
        table.apply_promote(1, 1).await.unwrap();
        // sealed=None, active=25 → min=25
        assert_eq!(table.min_safe_log_index(), Some(25));

        // Phase 5: Seal segment 2, start segment 3
        table
            .apply_seal(2, 3, crate::types::SealReason::MaxAge)
            .await
            .unwrap();
        // sealed=25, active=None → min=25
        assert_eq!(table.min_safe_log_index(), Some(25));

        // Phase 6: Write to segment 3
        table.apply_append(vec![test_batch(40)]).await.unwrap();
        table.record_log_index(40);
        // sealed=25, active=40 → min=25
        assert_eq!(table.min_safe_log_index(), Some(25));

        // Phase 7: Promote segment 2
        table.apply_promote(2, 2).await.unwrap();
        // sealed=None, active=40 → min=40
        assert_eq!(table.min_safe_log_index(), Some(40));
    }

    #[tokio::test]
    async fn test_catalog_preserves_log_indices_across_restore() {
        // Verify that catalog serialization preserves first_log_index values
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // Write to active
        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(100);

        // Seal
        table
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();

        // Write to new active
        table.apply_append(vec![test_batch(2)]).await.unwrap();
        table.record_log_index(200);

        // Get catalog and verify the indices are recorded
        let catalog = table.catalog();
        assert_eq!(catalog.active_first_log_index, Some(200));
        assert_eq!(catalog.sealed_first_log_index, Some(100));

        // Simulate restore from snapshot using these catalog values
        let tmp2 = tempfile::tempdir().unwrap();
        let engine2 = test_engine(tmp2.path()).await;

        let config2 = engine2.config().build_table_config("t1", test_schema());
        let table2 = engine2
            .create_table(config2, Some(catalog.clone()))
            .await
            .unwrap();

        // Restored table should have same min_safe_log_index
        assert_eq!(table2.min_safe_log_index(), Some(100));
    }

    #[tokio::test]
    async fn test_purge_floor_with_guard_watermark_and_table_constraint() {
        // Simulate the full state machine flow:
        // 1. Table has active_first_log_index = 500
        // 2. Snapshot built at log 400 (watermark)
        // 3. Promote happens → table constraint moves to 600
        // 4. Guard watermark keeps floor at 400
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let purge_floor = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        // Write to active segment at log 500
        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(500);

        // Compute floor: min_safe=500, no guard → floor=500
        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        purge_floor.store(effective.unwrap_or(0), Ordering::Release);
        assert_eq!(purge_floor.load(Ordering::Acquire), 500);

        // Simulate get_snapshot_builder: acquire guard with watermark=400
        let _guard_handle = engine
            .snapshot_guard()
            .acquire(Duration::from_secs(60), Some(400));

        // Seal and promote — table constraint relaxes
        table
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();
        table.apply_append(vec![test_batch(2)]).await.unwrap();
        table.record_log_index(600);
        table.apply_promote(1, 1).await.unwrap();

        // Recompute floor: min_safe=600, guard watermark=400 → floor=400
        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        purge_floor.store(effective.unwrap_or(0), Ordering::Release);
        assert_eq!(
            purge_floor.load(Ordering::Acquire),
            400,
            "Guard watermark should keep floor at 400 even though table constraint is 600"
        );

        // Release guard — floor should jump to table constraint
        drop(_guard_handle);
        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        purge_floor.store(effective.unwrap_or(0), Ordering::Release);
        assert_eq!(
            purge_floor.load(Ordering::Acquire),
            600,
            "After guard release, floor should rise to table constraint"
        );
    }

    #[tokio::test]
    async fn test_purge_floor_all_promoted_no_guard() {
        // All data promoted to S3 → floor should be 0 (no constraint)
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(100);

        table
            .apply_seal(1, 2, crate::types::SealReason::MaxAge)
            .await
            .unwrap();
        table.apply_promote(1, 1).await.unwrap();

        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        assert_eq!(effective.unwrap_or(0), 0);
    }

    #[tokio::test]
    async fn test_purge_floor_concurrent_snapshots_different_watermarks() {
        // Two concurrent snapshots at different log points
        // Floor must stay at the earlier watermark
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(800);

        // Snapshot 1 at log 500
        let h1 = engine
            .snapshot_guard()
            .acquire(Duration::from_secs(60), Some(500));

        // Snapshot 2 at log 700
        let h2 = engine
            .snapshot_guard()
            .acquire(Duration::from_secs(60), Some(700));

        // Floor should be min(table=800, guard=min(500,700)=500) = 500
        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        assert_eq!(effective, Some(500));

        // Release snapshot 1 → floor rises to min(800, 700) = 700
        drop(h1);
        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        assert_eq!(effective, Some(700));

        // Release snapshot 2 → floor rises to 800 (table only)
        drop(h2);
        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        assert_eq!(effective, Some(800));
    }

    #[tokio::test]
    async fn test_drop_table_removes_constraint() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config_a = engine.config().build_table_config("a", test_schema());
        let table_a = engine.create_table(config_a, None).await.unwrap();

        let config_b = engine.config().build_table_config("b", test_schema());
        let table_b = engine.create_table(config_b, None).await.unwrap();

        table_a.apply_append(vec![test_batch(1)]).await.unwrap();
        table_a.record_log_index(100);

        table_b.apply_append(vec![test_batch(1)]).await.unwrap();
        table_b.record_log_index(500);

        assert_eq!(compute_min_across_tables(&engine), Some(100));

        // Drop table A — constraint should be only B at 500
        drop(table_a);
        engine.drop_table("a").await.unwrap();
        assert_eq!(compute_min_across_tables(&engine), Some(500));

        // Drop table B — no constraints
        drop(table_b);
        engine.drop_table("b").await.unwrap();
        assert_eq!(compute_min_across_tables(&engine), None);
    }

    #[tokio::test]
    async fn test_guard_watermark_protects_during_drop_table() {
        // Guard watermark should protect floor even when all tables are dropped
        let tmp = tempfile::tempdir().unwrap();
        let engine = test_engine(tmp.path()).await;

        let config = engine.config().build_table_config("t1", test_schema());
        let table = engine.create_table(config, None).await.unwrap();

        table.apply_append(vec![test_batch(1)]).await.unwrap();
        table.record_log_index(100);

        // Acquire guard at watermark 50
        let _handle = engine
            .snapshot_guard()
            .acquire(Duration::from_secs(60), Some(50));

        // Drop table — table constraint gone, but guard keeps floor at 50
        drop(table);
        engine.drop_table("t1").await.unwrap();

        let min_safe = compute_min_across_tables(&engine);
        let min_guard = engine.snapshot_guard().min_watermark();
        let effective = match (min_safe, min_guard) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        assert_eq!(
            effective,
            Some(50),
            "Guard watermark should keep floor at 50"
        );
    }
}
