//! Per-table storage engine — manages a single table's Lance datasets through the
//! segment lifecycle (active → sealed → deep storage).
//!
//! Each `TableEngine` instance is self-contained: it has its own datasets, write
//! mutexes, flush state, and compaction. The parent `BisqueLance` holds a registry
//! of `TableEngine` instances keyed by table name.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::cleanup::{CleanupPolicyBuilder, RemovalStats};
use lance::dataset::optimize::{
    CompactionMetrics, IgnoreRemap, RewriteResult, commit_compaction, plan_compaction,
};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexParams, IndexType};
use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};

use crate::config::TableOpenConfig;
use crate::engine::SnapshotTransferGuard;
use crate::error::{Error, Result};
use crate::ipc;
use crate::types::{
    CleanupStats, CompactionStats, FlushHandle, FlushState, SchemaVersion, SealReason,
    SegmentCatalog, SegmentId,
};
use crate::version_pins::{PinTier, VersionPinTracker};

/// Sentinel value for "no log index" in `AtomicU64` fields.
const NO_LOG_INDEX: u64 = u64::MAX;

/// Convert atomic log index value to `Option<u64>`.
#[inline]
fn atomic_to_opt(v: u64) -> Option<u64> {
    if v == NO_LOG_INDEX { None } else { Some(v) }
}

/// Convert `Option<u64>` to atomic log index value.
#[inline]
fn opt_to_atomic(v: Option<u64>) -> u64 {
    v.unwrap_or(NO_LOG_INDEX)
}

/// Storage engine for a single table. Manages local Lance datasets and tracks
/// segment state through the active → sealed → deep storage lifecycle.
///
/// # Concurrency
///
/// Datasets use `parking_lot::RwLock` for fast, non-blocking reads (clone is cheap
/// — just Arc refcount bumps) and `tokio::sync::Mutex` to serialize writers per tier.
/// Writers clone the dataset under a brief read lock, perform I/O on the clone
/// (no lock held), then briefly acquire a write lock to swap in the updated handle.
/// This means reads are never blocked by ongoing writes.
pub struct TableEngine {
    /// Table name.
    name: String,
    /// Per-table configuration.
    config: TableOpenConfig,
    catalog: RwLock<SegmentCatalog>,
    active_dataset: RwLock<Option<Dataset>>,
    active_write: AsyncMutex<()>,
    sealed_dataset: RwLock<Option<Dataset>>,
    sealed_write: AsyncMutex<()>,
    s3_dataset: RwLock<Option<Dataset>>,
    s3_write: AsyncMutex<()>,
    flush_state: RwLock<FlushState>,
    /// Tracks byte size of the active segment for seal-by-size checks.
    active_bytes: AtomicU64,
    /// When the current active segment was created.
    active_created_at: RwLock<Instant>,
    /// Schema evolution history.
    schema_history: RwLock<Vec<SchemaVersion>>,
    /// First raft log index written to the active segment (`u64::MAX` = None).
    active_first_log_index: AtomicU64,
    /// First raft log index written to the sealed segment (`u64::MAX` = None).
    sealed_first_log_index: AtomicU64,
    /// Optional version pin tracker for compaction safety with remote clients.
    version_pins: RwLock<Option<Arc<VersionPinTracker>>>,
    /// Catalog name (raft group name) for scoping version pins and metric labeling.
    catalog_name: String,
    /// Shared guard that defers file deletion while snapshot transfers are active.
    snapshot_guard: Arc<SnapshotTransferGuard>,

    // ── Pre-built metric handles (avoid per-call string clone + descriptor lookup) ──
    m_writes_bytes: metrics::Counter,
    m_writes_rows: metrics::Counter,
    m_write_latency: metrics::Histogram,
    m_deletes_rows: metrics::Counter,
    m_delete_latency: metrics::Histogram,
    m_updates_rows: metrics::Counter,
    m_flush_rows: metrics::Counter,
    m_flush_latency: metrics::Histogram,
    m_seals_max_age: metrics::Counter,
    m_seals_max_size: metrics::Counter,
    m_compact_active_latency: metrics::Histogram,
    m_compact_sealed_latency: metrics::Histogram,
}

impl TableEngine {
    /// Open or create a table engine for a single table.
    ///
    /// Creates the segments directory if needed and opens the active segment dataset.
    /// If a prior catalog is provided (from snapshot restore), uses those segment IDs;
    /// otherwise starts fresh with segment 1.
    pub async fn open(
        config: TableOpenConfig,
        catalog: Option<SegmentCatalog>,
        schema_history: Option<Vec<SchemaVersion>>,
        snapshot_guard: Arc<SnapshotTransferGuard>,
        catalog_name: String,
    ) -> Result<Self> {
        let segments_dir = config.segments_dir();
        tokio::fs::create_dir_all(&segments_dir).await?;

        let mut catalog = catalog.unwrap_or_default();
        // Ensure s3_dataset_uri is populated from config.
        if let Some(s3_uri) = &config.s3_uri {
            if catalog.s3_dataset_uri.is_empty() {
                catalog.s3_dataset_uri = s3_uri.clone();
            }
        }
        let name = config.name.clone();

        // Open or create the active segment dataset
        let active_path = config.segment_path(catalog.active_segment);
        let active_dataset = open_or_create_segment(&active_path, &config).await?;

        // Open sealed segment if one exists
        let sealed_dataset = if let Some(sealed_id) = catalog.sealed_segment {
            let sealed_path = config.segment_path(sealed_id);
            if sealed_path.exists() {
                Some(Dataset::open(sealed_path.to_str().unwrap()).await?)
            } else {
                warn!(
                    table = %name,
                    segment_id = sealed_id,
                    "Sealed segment path does not exist, clearing"
                );
                None
            }
        } else {
            None
        };

        // Open S3 dataset if configured and has existing data
        let s3_dataset = if config.has_s3() && catalog.s3_manifest_version > 0 {
            match open_s3_dataset(&config).await {
                Ok(ds) => Some(ds),
                Err(e) => {
                    warn!(table = %name, "Failed to open S3 dataset: {}, will retry on first flush", e);
                    None
                }
            }
        } else {
            None
        };

        let active_rows = active_dataset.count_rows(None).await.unwrap_or(0) as u64;

        // Initialize schema history
        let schema_history = schema_history.unwrap_or_else(|| {
            if let Some(schema) = &config.schema {
                if let Ok(ipc_bytes) = ipc::schema_to_ipc(schema) {
                    vec![SchemaVersion {
                        version: 1,
                        schema_ipc: ipc_bytes,
                        created_at_millis: chrono::Utc::now().timestamp_millis(),
                    }]
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        });

        info!(
            table = %name,
            active_segment = catalog.active_segment,
            sealed_segment = ?catalog.sealed_segment,
            s3_manifest_version = catalog.s3_manifest_version,
            s3_configured = config.has_s3(),
            active_rows,
            schema_versions = schema_history.len(),
            "TableEngine opened"
        );

        // Pre-build metric handles once — avoids per-call string clone + descriptor lookup.
        let cat = catalog_name.clone();
        let m_writes_bytes = metrics::counter!("bisque_writes_bytes_total", "catalog" => cat.clone(), "table" => name.clone());
        let m_writes_rows = metrics::counter!("bisque_writes_rows_total", "catalog" => cat.clone(), "table" => name.clone());
        let m_write_latency = metrics::histogram!("bisque_write_latency_ms", "catalog" => cat.clone(), "table" => name.clone());
        let m_deletes_rows = metrics::counter!("bisque_deletes_rows_total", "catalog" => cat.clone(), "table" => name.clone());
        let m_delete_latency = metrics::histogram!("bisque_delete_latency_ms", "catalog" => cat.clone(), "table" => name.clone());
        let m_updates_rows = metrics::counter!("bisque_updates_rows_total", "catalog" => cat.clone(), "table" => name.clone());
        let m_flush_rows = metrics::counter!("bisque_flush_rows_total", "catalog" => cat.clone(), "table" => name.clone());
        let m_flush_latency = metrics::histogram!("bisque_flush_latency_ms", "catalog" => cat.clone(), "table" => name.clone());
        let m_seals_max_age = metrics::counter!("bisque_seals_total", "catalog" => cat.clone(), "table" => name.clone(), "reason" => "MaxAge");
        let m_seals_max_size = metrics::counter!("bisque_seals_total", "catalog" => cat.clone(), "table" => name.clone(), "reason" => "MaxSize");
        let m_compact_active_latency = metrics::histogram!(
            "bisque_compaction_latency_ms", "catalog" => cat.clone(), "table" => name.clone(), "tier" => "active"
        );
        let m_compact_sealed_latency = metrics::histogram!(
            "bisque_compaction_latency_ms", "catalog" => cat, "table" => name.clone(), "tier" => "sealed"
        );

        Ok(Self {
            name,
            config,
            active_first_log_index: AtomicU64::new(opt_to_atomic(catalog.active_first_log_index)),
            sealed_first_log_index: AtomicU64::new(opt_to_atomic(catalog.sealed_first_log_index)),
            catalog: RwLock::new(catalog),
            active_dataset: RwLock::new(Some(active_dataset)),
            active_write: AsyncMutex::new(()),
            sealed_dataset: RwLock::new(sealed_dataset),
            sealed_write: AsyncMutex::new(()),
            s3_dataset: RwLock::new(s3_dataset),
            s3_write: AsyncMutex::new(()),
            flush_state: RwLock::new(FlushState::Idle),
            active_bytes: AtomicU64::new(0),
            active_created_at: RwLock::new(Instant::now()),
            schema_history: RwLock::new(schema_history),
            version_pins: RwLock::new(None),
            catalog_name,
            snapshot_guard,
            m_writes_bytes,
            m_writes_rows,
            m_write_latency,
            m_deletes_rows,
            m_delete_latency,
            m_updates_rows,
            m_flush_rows,
            m_flush_latency,
            m_seals_max_age,
            m_seals_max_size,
            m_compact_active_latency,
            m_compact_sealed_latency,
        })
    }

    /// Get the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the table config.
    pub fn config(&self) -> &TableOpenConfig {
        &self.config
    }

    /// Set the version pin tracker for compaction safety with remote clients.
    ///
    /// When set, `cleanup_s3()` will refuse to delete old versions if any
    /// remote client has them pinned.
    pub fn set_version_pins(&self, pins: Arc<VersionPinTracker>) {
        *self.version_pins.write() = Some(pins);
    }

    /// Get the catalog name used for scoping version pins and metric labeling.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    // =========================================================================
    // Log Index Tracking (for purge floor)
    // =========================================================================

    /// Record the raft log index for an append to the active segment.
    /// Only updates if this is the first write to the current active segment.
    /// Returns `true` if the value was set (first write), `false` if already set.
    pub fn record_log_index(&self, log_index: u64) -> bool {
        self.active_first_log_index
            .compare_exchange(NO_LOG_INDEX, log_index, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Get the minimum log index that constrains purging for this table.
    /// Returns `None` if no hot/warm data needs protection.
    pub fn min_safe_log_index(&self) -> Option<u64> {
        let active = atomic_to_opt(self.active_first_log_index.load(Ordering::Acquire));
        let sealed = atomic_to_opt(self.sealed_first_log_index.load(Ordering::Acquire));
        match (active, sealed) {
            (Some(a), Some(s)) => Some(a.min(s)),
            (Some(a), None) => Some(a),
            (None, Some(s)) => Some(s),
            (None, None) => None,
        }
    }

    // =========================================================================
    // Write Path (called by Raft state machine apply)
    // =========================================================================

    /// Append RecordBatches to the active local segment.
    ///
    /// Called on ALL nodes when an `AppendRecords` log is applied.
    pub async fn apply_append(&self, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let append_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        // Check for schema evolution
        self.maybe_record_schema_change(&batches[0].schema());

        let schema = batches[0].schema();
        let reader =
            arrow::record_batch::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        // Serialize writers; clone dataset under brief read lock, then I/O with no lock.
        let _guard = self.active_write.lock().await;
        let mut ds = self
            .active_dataset
            .read()
            .clone()
            .ok_or_else(|| Error::InvalidState("no active dataset".into()))?;

        ds.append(reader, Some(WriteParams::default())).await?;

        // Brief write lock to swap in the updated handle
        *self.active_dataset.write() = Some(ds);
        drop(_guard);

        self.active_bytes.fetch_add(append_bytes, Ordering::Relaxed);
        self.m_writes_bytes.increment(append_bytes);
        self.m_writes_rows.increment(total_rows as u64);
        self.m_write_latency
            .record(start.elapsed().as_millis() as f64);
        debug!(table = %self.name, bytes = append_bytes, "Appended to active segment");
        Ok(())
    }

    // =========================================================================
    // Delete / Update Path (called by Raft state machine apply)
    // =========================================================================

    /// Delete rows matching a SQL filter predicate from all tiers.
    ///
    /// Uses Lance deletion vectors (soft delete) — no data files are rewritten.
    /// The delete is applied to every tier that currently holds data (active,
    /// sealed, S3), ensuring a consistent view across the full table.
    ///
    /// Returns the total number of rows deleted across all tiers.
    /// Uses Lance's `DeleteResult.num_deleted_rows` to avoid extra `count_rows` scans.
    pub async fn apply_delete(&self, filter: &str) -> Result<u64> {
        let start = std::time::Instant::now();

        // Delete from all three tiers in parallel — each has independent locks.
        let active_fut = async {
            let _guard = self.active_write.lock().await;
            let maybe_ds = { self.active_dataset.read().clone() };
            if let Some(mut ds) = maybe_ds {
                let result = ds
                    .delete(filter)
                    .await
                    .map_err(|e| Error::DeleteFailed(e.to_string()))?;
                let deleted = result.num_deleted_rows;
                *self.active_dataset.write() = Some(ds);
                Ok::<u64, Error>(deleted)
            } else {
                Ok(0)
            }
        };

        let sealed_fut = async {
            let _guard = self.sealed_write.lock().await;
            let maybe_ds = { self.sealed_dataset.read().clone() };
            if let Some(mut ds) = maybe_ds {
                let result = ds
                    .delete(filter)
                    .await
                    .map_err(|e| Error::DeleteFailed(e.to_string()))?;
                let deleted = result.num_deleted_rows;
                *self.sealed_dataset.write() = Some(ds);
                Ok::<u64, Error>(deleted)
            } else {
                Ok(0)
            }
        };

        let s3_fut = async {
            let _guard = self.s3_write.lock().await;
            let maybe_ds = { self.s3_dataset.read().clone() };
            if let Some(mut ds) = maybe_ds {
                let result = ds
                    .delete(filter)
                    .await
                    .map_err(|e| Error::DeleteFailed(e.to_string()))?;
                let deleted = result.num_deleted_rows;
                *self.s3_dataset.write() = Some(ds);
                Ok::<u64, Error>(deleted)
            } else {
                Ok(0)
            }
        };

        let (active_del, sealed_del, s3_del) = tokio::try_join!(active_fut, sealed_fut, s3_fut)?;
        let total_deleted = active_del + sealed_del + s3_del;

        self.m_deletes_rows.increment(total_deleted);
        self.m_delete_latency
            .record(start.elapsed().as_millis() as f64);
        debug!(table = %self.name, deleted = total_deleted, filter, "Delete applied across all tiers");
        Ok(total_deleted)
    }

    /// Update rows: soft-delete matching rows across all tiers, then append
    /// replacement data to the active segment.
    ///
    /// Returns the number of rows that were deleted (replaced).
    pub async fn apply_update(&self, filter: &str, batches: Vec<RecordBatch>) -> Result<u64> {
        let deleted = self.apply_delete(filter).await?;
        self.apply_append(batches).await?;
        self.m_updates_rows.increment(deleted);
        Ok(deleted)
    }

    /// Get the current dataset version for each tier (for catalog event emission).
    pub fn tier_versions(&self) -> (Option<u64>, Option<u64>, Option<u64>) {
        let active_v = self
            .active_dataset
            .read()
            .as_ref()
            .map(|ds| ds.version().version);
        let sealed_v = self
            .sealed_dataset
            .read()
            .as_ref()
            .map(|ds| ds.version().version);
        let s3_v = self
            .s3_dataset
            .read()
            .as_ref()
            .map(|ds| ds.version().version);
        (active_v, sealed_v, s3_v)
    }

    /// Seal the active segment and rotate to a new one.
    ///
    /// Called on ALL nodes when a `SealActiveSegment` log is applied.
    pub async fn apply_seal(
        &self,
        sealed_segment_id: SegmentId,
        new_active_segment_id: SegmentId,
        reason: SealReason,
    ) -> Result<()> {
        info!(
            table = %self.name,
            sealed = sealed_segment_id,
            new_active = new_active_segment_id,
            ?reason,
            "Sealing active segment"
        );

        // Lock order: active → sealed (always this order to prevent deadlocks)
        let _active_guard = self.active_write.lock().await;
        let _sealed_guard = self.sealed_write.lock().await;

        // Clone the current active dataset (readers still see it)
        let old_active = self
            .active_dataset
            .read()
            .clone()
            .ok_or_else(|| Error::InvalidState("no active dataset to seal".into()))?;

        // Create new active segment
        let new_path = self.config.segment_path(new_active_segment_id);
        let new_dataset = open_or_create_segment(&new_path, &self.config).await?;

        // Atomically swap both datasets (brief write locks held simultaneously)
        {
            let mut active = self.active_dataset.write();
            let mut sealed = self.sealed_dataset.write();
            *sealed = Some(old_active);
            *active = Some(new_dataset);
        }

        // Rotate first_log_index: sealed inherits active's, active resets
        {
            let active_first = self
                .active_first_log_index
                .swap(NO_LOG_INDEX, Ordering::AcqRel);
            self.sealed_first_log_index
                .store(active_first, Ordering::Release);
        }

        // Update catalog
        {
            let mut cat = self.catalog.write();
            cat.sealed_segment = Some(sealed_segment_id);
            cat.active_segment = new_active_segment_id;
        }

        // Reset active segment metrics
        self.active_bytes.store(0, Ordering::Relaxed);
        *self.active_created_at.write() = Instant::now();
        match reason {
            SealReason::MaxAge => self.m_seals_max_age.increment(1),
            SealReason::MaxSize => self.m_seals_max_size.increment(1),
        }

        Ok(())
    }

    /// Create indices on the sealed segment as specified by config.
    pub async fn create_seal_indices(&self) -> Result<()> {
        if self.config.seal_indices.is_empty() {
            return Ok(());
        }

        let _guard = self.sealed_write.lock().await;
        let mut ds = match self.sealed_dataset.read().clone() {
            Some(ds) => ds,
            None => {
                warn!(table = %self.name, "No sealed dataset to index");
                return Ok(());
            }
        };

        for spec in &self.config.seal_indices {
            let columns: Vec<&str> = spec.columns.iter().map(|s| s.as_str()).collect();
            let params = index_params_for_type(spec.index_type);

            info!(
                table = %self.name,
                columns = ?spec.columns,
                index_type = %spec.index_type,
                name = ?spec.name,
                "Creating index on sealed segment"
            );

            match ds
                .create_index(
                    &columns,
                    spec.index_type,
                    spec.name.clone(),
                    params.as_ref(),
                    true,
                )
                .await
            {
                Ok(meta) => {
                    info!(
                        table = %self.name,
                        name = %meta.name,
                        columns = ?spec.columns,
                        "Index created on sealed segment"
                    );
                }
                Err(e) => {
                    warn!(
                        table = %self.name,
                        columns = ?spec.columns,
                        index_type = %spec.index_type,
                        "Failed to create index on sealed segment: {}",
                        e
                    );
                }
            }
        }

        *self.sealed_dataset.write() = Some(ds);
        Ok(())
    }

    /// Create indices on the S3 (cold) dataset.
    ///
    /// Uses the same index specs as sealed segments (`seal_indices` from config).
    /// This allows retroactively indexing cold storage data that was promoted
    /// before an index was added to the configuration.
    pub async fn create_s3_indices(&self) -> Result<()> {
        if self.config.seal_indices.is_empty() {
            return Ok(());
        }

        if !self.config.has_s3() {
            return Err(Error::S3NotConfigured);
        }

        let _guard = self.s3_write.lock().await;
        let mut ds = match self.s3_dataset.read().clone() {
            Some(ds) => ds,
            None => {
                warn!(table = %self.name, "No S3 dataset to index");
                return Ok(());
            }
        };

        for spec in &self.config.seal_indices {
            let columns: Vec<&str> = spec.columns.iter().map(|s| s.as_str()).collect();
            let params = index_params_for_type(spec.index_type);

            info!(
                table = %self.name,
                columns = ?spec.columns,
                index_type = %spec.index_type,
                name = ?spec.name,
                "Creating index on S3 dataset"
            );

            match ds
                .create_index(
                    &columns,
                    spec.index_type,
                    spec.name.clone(),
                    params.as_ref(),
                    true,
                )
                .await
            {
                Ok(meta) => {
                    info!(
                        table = %self.name,
                        name = %meta.name,
                        columns = ?spec.columns,
                        "Index created on S3 dataset"
                    );
                }
                Err(e) => {
                    warn!(
                        table = %self.name,
                        columns = ?spec.columns,
                        index_type = %spec.index_type,
                        "Failed to create index on S3 dataset: {}",
                        e
                    );
                }
            }
        }

        // Update catalog with new S3 version
        let new_version = ds.version().version;
        {
            let mut cat = self.catalog.write();
            cat.s3_manifest_version = new_version;
        }

        *self.s3_dataset.write() = Some(ds);
        info!(table = %self.name, new_version, "S3 reindex complete");
        Ok(())
    }

    /// Apply a BeginFlush command — records that a flush is in progress.
    pub fn apply_begin_flush(&self, segment_id: SegmentId) {
        *self.flush_state.write() = FlushState::InProgress {
            segment_id,
            started_at: chrono::Utc::now().timestamp_millis(),
            fragment_paths: Vec::new(),
        };
        info!(table = %self.name, segment_id, "Flush started");
    }

    /// Apply a PromoteToDeepStorage command.
    pub async fn apply_promote(
        &self,
        segment_id: SegmentId,
        s3_manifest_version: u64,
    ) -> Result<()> {
        info!(table = %self.name, segment_id, s3_manifest_version, "Promoting to deep storage");

        // Update catalog
        {
            let mut cat = self.catalog.write();
            cat.s3_manifest_version = s3_manifest_version;
            if cat.sealed_segment == Some(segment_id) {
                cat.sealed_segment = None;
            }
        }

        // Data is now in S3 — sealed segment no longer constrains log purging
        self.sealed_first_log_index
            .store(NO_LOG_INDEX, Ordering::Release);

        // Drop the sealed dataset handle
        {
            let _guard = self.sealed_write.lock().await;
            *self.sealed_dataset.write() = None;
        }

        // Clean up local sealed segment files (deferred if snapshot transfer is active)
        let segment_path = self.config.segment_path(segment_id);
        if segment_path.exists() {
            if !self.snapshot_guard.defer_or_delete(segment_path.clone()) {
                tokio::fs::remove_dir_all(&segment_path).await?;
                debug!(table = %self.name, ?segment_path, "Removed local sealed segment");
            }
        }

        // Reset flush state
        *self.flush_state.write() = FlushState::Idle;

        Ok(())
    }

    // =========================================================================
    // S3 Flush Pipeline — Leader Only
    // =========================================================================

    /// Begin a flush of the sealed segment to S3. Returns a FlushHandle.
    pub fn begin_flush(&self) -> Result<FlushHandle> {
        if !self.config.has_s3() {
            return Err(Error::S3NotConfigured);
        }

        let flush_state = self.flush_state.read();
        if let FlushState::InProgress { segment_id, .. } = &*flush_state {
            return Err(Error::FlushInProgress(*segment_id));
        }
        drop(flush_state);

        let segment_id = self
            .catalog
            .read()
            .sealed_segment
            .ok_or(Error::NoSealedSegment)?;

        Ok(FlushHandle {
            table_name: Arc::from(self.name.as_str()),
            segment_id,
            started_at: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Execute the S3 flush: read the sealed segment and append to the S3 dataset.
    ///
    /// Returns the new S3 manifest version on success.
    pub async fn execute_flush(&self, handle: &FlushHandle) -> Result<u64> {
        let flush_start = std::time::Instant::now();
        let s3_uri = self.config.s3_uri.as_ref().ok_or(Error::S3NotConfigured)?;

        info!(
            table = %self.name,
            segment_id = handle.segment_id,
            s3_uri,
            "Executing S3 flush"
        );

        // Read all data from the sealed segment
        let batches = {
            let ds = self
                .sealed_dataset
                .read()
                .clone()
                .ok_or_else(|| Error::InvalidState("no sealed dataset for flush".into()))?;

            let stream = ds
                .scan()
                .try_into_stream()
                .await
                .map_err(|e| Error::Lance(e))?;

            stream.try_collect::<Vec<RecordBatch>>().await?
        };

        if batches.is_empty() {
            info!(table = %self.name, segment_id = handle.segment_id, "Sealed segment is empty, nothing to flush");
            let version = self.catalog.read().s3_manifest_version;
            return Ok(version);
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!(
            table = %self.name,
            segment_id = handle.segment_id,
            total_rows,
            num_batches = batches.len(),
            "Read sealed segment data, writing to S3"
        );

        let write_params = WriteParams {
            max_rows_per_file: self.config.s3_max_rows_per_file,
            max_rows_per_group: self.config.s3_max_rows_per_group,
            mode: WriteMode::Append,
            enable_v2_manifest_paths: true,
            ..Default::default()
        };

        let schema = batches[0].schema();
        let reader =
            arrow::record_batch::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        // Serialize S3 writes; clone dataset, do I/O, swap back
        let _guard = self.s3_write.lock().await;
        let current_s3 = self.s3_dataset.read().clone();

        let (new_s3, new_version) = if let Some(mut s3_ds) = current_s3 {
            s3_ds.append(reader, Some(write_params)).await?;
            let version = s3_ds.version().version;
            (s3_ds, version)
        } else {
            // First flush — create the S3 dataset
            let s3_ds = if !self.config.s3_storage_options.is_empty() {
                let builder = DatasetBuilder::from_uri(s3_uri)
                    .with_storage_options(self.config.s3_storage_options.clone());
                match builder.load().await {
                    Ok(mut ds) => {
                        ds.append(reader, Some(write_params)).await?;
                        ds
                    }
                    Err(_) => {
                        let mut params = write_params.clone();
                        params.mode = WriteMode::Create;
                        Dataset::write(reader, s3_uri.as_str(), Some(params)).await?
                    }
                }
            } else {
                match Dataset::open(s3_uri.as_str()).await {
                    Ok(mut ds) => {
                        ds.append(reader, Some(write_params)).await?;
                        ds
                    }
                    Err(_) => {
                        let mut params = write_params.clone();
                        params.mode = WriteMode::Create;
                        Dataset::write(reader, s3_uri.as_str(), Some(params)).await?
                    }
                }
            };

            let version = s3_ds.version().version;
            (s3_ds, version)
        };

        *self.s3_dataset.write() = Some(new_s3);

        self.m_flush_rows.increment(total_rows as u64);
        self.m_flush_latency
            .record(flush_start.elapsed().as_millis() as f64);
        info!(
            table = %self.name,
            segment_id = handle.segment_id,
            new_s3_version = new_version,
            "S3 flush completed"
        );

        Ok(new_version)
    }

    // =========================================================================
    // Crash Recovery — New Leader
    // =========================================================================

    /// Recover from a crashed flush.
    pub async fn recover_flush(&self) -> Result<()> {
        let flush_state = self.flush_state.read().clone();

        match flush_state {
            FlushState::Idle => {
                debug!(table = %self.name, "No flush recovery needed");
                Ok(())
            }
            FlushState::InProgress {
                segment_id,
                started_at,
                ..
            } => {
                warn!(
                    table = %self.name,
                    segment_id,
                    started_at,
                    "Recovering from incomplete flush"
                );

                if self.config.has_s3() {
                    if let Err(e) = self.cleanup_s3_internal(true).await {
                        warn!(table = %self.name, "S3 cleanup during recovery failed: {}", e);
                    }
                }

                *self.flush_state.write() = FlushState::Idle;

                info!(table = %self.name, segment_id, "Flush recovery complete, ready to re-flush");
                Ok(())
            }
        }
    }

    // =========================================================================
    // S3 Cleanup
    // =========================================================================

    /// Run cleanup on the S3 dataset.
    pub async fn cleanup_s3(&self) -> Result<CleanupStats> {
        if !self.config.has_s3() {
            return Err(Error::S3NotConfigured);
        }

        if let FlushState::InProgress { segment_id, .. } = &*self.flush_state.read() {
            return Err(Error::FlushInProgress(*segment_id));
        }

        let stats = self.cleanup_s3_internal(true).await?;
        Ok(stats)
    }

    async fn cleanup_s3_internal(&self, delete_unverified: bool) -> Result<CleanupStats> {
        // Check if any remote client has pinned versions for this table.
        // If so, skip cleanup to avoid deleting files they're still reading.
        if let Some(pins) = self.version_pins.read().as_ref() {
            let cat = &self.catalog_name;
            let has_active_pins = pins
                .min_pinned_version(&cat, &self.name, PinTier::Active)
                .is_some();
            let has_sealed_pins = pins
                .min_pinned_version(&cat, &self.name, PinTier::Sealed)
                .is_some();
            if has_active_pins || has_sealed_pins {
                debug!(
                    table = %self.name,
                    has_active_pins,
                    has_sealed_pins,
                    "Skipping S3 cleanup: remote clients have pinned versions"
                );
                return Ok(CleanupStats::default());
            }
        }

        let ds = match self.s3_dataset.read().clone() {
            Some(ds) => ds,
            None => {
                debug!(table = %self.name, "No S3 dataset open, skipping cleanup");
                return Ok(CleanupStats::default());
            }
        };

        let policy = CleanupPolicyBuilder::default()
            .delete_unverified(delete_unverified)
            .build();

        let removal_stats: RemovalStats =
            lance::dataset::cleanup::cleanup_old_versions(&ds, policy).await?;

        let stats = CleanupStats {
            versions_removed: removal_stats.old_versions as u64,
            bytes_freed: removal_stats.bytes_removed as u64,
            ..Default::default()
        };

        info!(
            table = %self.name,
            files_removed = stats.files_removed,
            versions_removed = stats.versions_removed,
            bytes_freed = stats.bytes_freed,
            "S3 cleanup completed"
        );

        Ok(stats)
    }

    // =========================================================================
    // Compaction
    // =========================================================================

    /// Compact the active segment to merge small append fragments.
    pub async fn compact_active(&self) -> Result<CompactionStats> {
        let (snapshot, options) = {
            let guard = self.active_dataset.read();
            let ds = match guard.as_ref() {
                Some(ds) => ds,
                None => {
                    debug!(table = %self.name, "No active dataset, skipping compaction");
                    return Ok(CompactionStats::default());
                }
            };

            let fragment_count = ds.get_fragments().len();
            if fragment_count < self.config.compaction_min_fragments {
                debug!(
                    table = %self.name,
                    fragment_count,
                    threshold = self.config.compaction_min_fragments,
                    "Active segment below compaction threshold, skipping"
                );
                return Ok(CompactionStats::default());
            }

            info!(table = %self.name, fragment_count, "Compacting active segment");
            (ds.clone(), self.config.compaction_options())
        };

        let compact_start = std::time::Instant::now();
        let results = plan_and_execute(&snapshot, &options).await?;
        if results.is_empty() {
            return Ok(CompactionStats::default());
        }

        let _guard = self.active_write.lock().await;
        let mut ds =
            self.active_dataset.read().clone().ok_or_else(|| {
                Error::InvalidState("no active dataset for compaction commit".into())
            })?;

        let metrics =
            commit_compaction(&mut ds, results, Arc::new(IgnoreRemap::default()), &options).await?;

        *self.active_dataset.write() = Some(ds);

        let stats = CompactionStats::from_metrics(&metrics);
        self.m_compact_active_latency
            .record(compact_start.elapsed().as_millis() as f64);
        info!(
            table = %self.name,
            fragments_removed = stats.fragments_removed,
            fragments_added = stats.fragments_added,
            "Active segment compaction complete"
        );

        Ok(stats)
    }

    /// Compact the sealed segment to merge fragments before index creation and flush.
    pub async fn compact_sealed(&self) -> Result<CompactionStats> {
        let (snapshot, options) = {
            let guard = self.sealed_dataset.read();
            let ds = match guard.as_ref() {
                Some(ds) => ds,
                None => {
                    debug!(table = %self.name, "No sealed dataset, skipping compaction");
                    return Ok(CompactionStats::default());
                }
            };

            let fragment_count = ds.get_fragments().len();
            if fragment_count < 2 {
                debug!(
                    table = %self.name,
                    fragment_count,
                    "Sealed segment has too few fragments to compact"
                );
                return Ok(CompactionStats::default());
            }

            info!(table = %self.name, fragment_count, "Compacting sealed segment");
            (ds.clone(), self.config.compaction_options())
        };

        let compact_start = std::time::Instant::now();
        let results = plan_and_execute(&snapshot, &options).await?;
        if results.is_empty() {
            return Ok(CompactionStats::default());
        }

        let _guard = self.sealed_write.lock().await;
        let mut ds =
            self.sealed_dataset.read().clone().ok_or_else(|| {
                Error::InvalidState("no sealed dataset for compaction commit".into())
            })?;

        let metrics =
            commit_compaction(&mut ds, results, Arc::new(IgnoreRemap::default()), &options).await?;

        *self.sealed_dataset.write() = Some(ds);

        let stats = CompactionStats::from_metrics(&metrics);
        self.m_compact_sealed_latency
            .record(compact_start.elapsed().as_millis() as f64);
        info!(
            table = %self.name,
            fragments_removed = stats.fragments_removed,
            fragments_added = stats.fragments_added,
            "Sealed segment compaction complete"
        );

        Ok(stats)
    }

    /// Compact S3 deep storage to merge fragments from multiple flushes.
    pub async fn compact_s3(&self) -> Result<CompactionStats> {
        if !self.config.has_s3() {
            return Err(Error::S3NotConfigured);
        }

        if let FlushState::InProgress { segment_id, .. } = &*self.flush_state.read() {
            return Err(Error::FlushInProgress(*segment_id));
        }

        let (snapshot, options) = {
            let guard = self.s3_dataset.read();
            let ds = match guard.as_ref() {
                Some(ds) => ds,
                None => {
                    debug!(table = %self.name, "No S3 dataset open, skipping compaction");
                    return Ok(CompactionStats::default());
                }
            };

            let fragment_count = ds.get_fragments().len();
            if fragment_count < self.config.compaction_min_fragments {
                debug!(
                    table = %self.name,
                    fragment_count,
                    threshold = self.config.compaction_min_fragments,
                    "S3 dataset below compaction threshold, skipping"
                );
                return Ok(CompactionStats::default());
            }

            info!(table = %self.name, fragment_count, "Compacting S3 deep storage");
            (ds.clone(), self.config.compaction_options())
        };

        let results = plan_and_execute(&snapshot, &options).await?;
        if results.is_empty() {
            return Ok(CompactionStats::default());
        }

        let _guard = self.s3_write.lock().await;
        let mut ds = self
            .s3_dataset
            .read()
            .clone()
            .ok_or_else(|| Error::InvalidState("no S3 dataset for compaction commit".into()))?;

        let metrics =
            commit_compaction(&mut ds, results, Arc::new(IgnoreRemap::default()), &options).await?;

        // Update catalog with new S3 version
        let new_version = ds.version().version;
        {
            let mut cat = self.catalog.write();
            cat.s3_manifest_version = new_version;
        }

        *self.s3_dataset.write() = Some(ds);

        let stats = CompactionStats::from_metrics(&metrics);
        info!(
            table = %self.name,
            fragments_removed = stats.fragments_removed,
            fragments_added = stats.fragments_added,
            new_s3_version = new_version,
            "S3 compaction complete"
        );

        drop(_guard);

        if let Err(e) = self.cleanup_s3_internal(true).await {
            warn!(table = %self.name, "S3 version cleanup after compaction failed: {}", e);
        }

        Ok(stats)
    }

    // =========================================================================
    // Query Helpers
    // =========================================================================

    /// Check if the active segment should be sealed.
    pub fn should_seal(&self) -> Option<SealReason> {
        // Never seal an empty segment — no point rotating if nothing was written.
        let size = self.active_bytes.load(Ordering::Relaxed);
        if size == 0 {
            return None;
        }

        let age = self.active_created_at.read().elapsed();
        if age >= self.config.seal_max_age {
            return Some(SealReason::MaxAge);
        }

        if size >= self.config.seal_max_size {
            return Some(SealReason::MaxSize);
        }

        None
    }

    /// Get a snapshot of the current catalog state.
    pub fn catalog(&self) -> SegmentCatalog {
        let mut cat = self.catalog.read().clone();
        cat.active_first_log_index =
            atomic_to_opt(self.active_first_log_index.load(Ordering::Acquire));
        cat.sealed_first_log_index =
            atomic_to_opt(self.sealed_first_log_index.load(Ordering::Acquire));
        cat
    }

    /// Get the current flush state.
    pub fn flush_state(&self) -> FlushState {
        self.flush_state.read().clone()
    }

    /// Get the next segment ID (one past the highest known segment).
    pub fn next_segment_id(&self) -> SegmentId {
        let cat = self.catalog.read();
        let max = cat.active_segment.max(cat.sealed_segment.unwrap_or(0));
        max + 1
    }

    /// Check if a flush is currently in progress.
    pub fn is_flush_in_progress(&self) -> bool {
        matches!(&*self.flush_state.read(), FlushState::InProgress { .. })
    }

    /// Get a snapshot (clone) of the active dataset for querying.
    pub async fn active_dataset_snapshot(&self) -> Option<Dataset> {
        self.active_dataset.read().clone()
    }

    /// Get a snapshot (clone) of the sealed dataset for querying.
    pub async fn sealed_dataset_snapshot(&self) -> Option<Dataset> {
        self.sealed_dataset.read().clone()
    }

    /// Get a snapshot (clone) of the S3 dataset for querying.
    pub async fn s3_dataset_snapshot(&self) -> Option<Dataset> {
        self.s3_dataset.read().clone()
    }

    /// Get the schema from the active dataset.
    pub async fn schema(&self) -> Option<arrow_schema::SchemaRef> {
        let guard = self.active_dataset.read();
        guard
            .as_ref()
            .map(|ds| Arc::new(arrow_schema::Schema::from(ds.schema())))
    }

    // =========================================================================
    // Schema Evolution
    // =========================================================================

    /// Get the schema version history for this table.
    pub fn schema_history(&self) -> Vec<SchemaVersion> {
        self.schema_history.read().clone()
    }

    /// Get the current (latest) schema version.
    pub fn current_schema_version(&self) -> Option<SchemaVersion> {
        self.schema_history.read().last().cloned()
    }

    /// Record a schema change if the incoming schema differs from the current one.
    fn maybe_record_schema_change(&self, incoming_schema: &arrow_schema::SchemaRef) {
        let history = self.schema_history.read();
        if let Some(current) = history.last() {
            // Compare by decoding the stored IPC schema
            if let Ok(stored_schema) = ipc::schema_from_ipc(&current.schema_ipc) {
                if stored_schema == **incoming_schema {
                    return; // No change
                }
            }
        }
        drop(history);

        // Schema changed — record new version
        if let Ok(ipc_bytes) = ipc::schema_to_ipc(incoming_schema) {
            let mut history = self.schema_history.write();
            let next_version = history.last().map(|v| v.version + 1).unwrap_or(1);
            info!(
                table = %self.name,
                version = next_version,
                fields = incoming_schema.fields().len(),
                "Schema evolution detected, recording new version"
            );
            history.push(SchemaVersion {
                version: next_version,
                schema_ipc: ipc_bytes,
                created_at_millis: chrono::Utc::now().timestamp_millis(),
            });
        }
    }

    /// Gracefully shutdown the table engine.
    pub async fn shutdown(&self) -> Result<()> {
        info!(table = %self.name, "Shutting down TableEngine");
        *self.active_dataset.write() = None;
        *self.sealed_dataset.write() = None;
        *self.s3_dataset.write() = None;
        Ok(())
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Open an existing segment or create a new empty one.
async fn open_or_create_segment(path: &Path, config: &TableOpenConfig) -> Result<Dataset> {
    let uri = path.to_str().unwrap();

    if path.exists() {
        debug!(?path, "Opening existing segment");
        Ok(Dataset::open(uri).await?)
    } else {
        debug!(?path, "Creating new empty segment");

        let schema = config.schema.clone().unwrap_or_else(|| {
            use arrow_schema::{DataType, Field, Schema};
            std::sync::Arc::new(Schema::new(vec![Field::new(
                "_placeholder",
                DataType::Null,
                true,
            )]))
        });

        let reader = arrow::record_batch::RecordBatchIterator::new(
            std::iter::empty::<std::result::Result<RecordBatch, arrow::error::ArrowError>>(),
            schema,
        );

        let ds = Dataset::write(reader, uri, Some(WriteParams::default())).await?;
        Ok(ds)
    }
}

/// Return default index params for a given `IndexType`.
fn index_params_for_type(index_type: IndexType) -> Box<dyn IndexParams> {
    match index_type {
        IndexType::Inverted | IndexType::NGram => Box::new(InvertedIndexParams::default()),
        IndexType::BTree
        | IndexType::RTree
        | IndexType::Scalar
        | IndexType::Bitmap
        | IndexType::LabelList => Box::new(ScalarIndexParams::default()),
        IndexType::IvfFlat
        | IndexType::IvfSq
        | IndexType::IvfPq
        | IndexType::IvfHnswSq
        | IndexType::IvfHnswPq
        | IndexType::IvfHnswFlat
        | IndexType::IvfRq
        | IndexType::Vector => {
            use lance::index::vector::VectorIndexParams;
            use lance_index::vector::ivf::builder::IvfBuildParams;
            let ivf = IvfBuildParams::new(256);
            let sq = lance_index::vector::sq::builder::SQBuildParams::default();
            Box::new(VectorIndexParams::with_ivf_hnsw_sq_params(
                lance_linalg::distance::DistanceType::L2,
                ivf,
                Default::default(),
                sq,
            ))
        }
        _ => Box::new(ScalarIndexParams::default()),
    }
}

/// Plan and execute compaction tasks without holding any dataset locks.
async fn plan_and_execute(
    dataset: &Dataset,
    options: &lance::dataset::optimize::CompactionOptions,
) -> Result<Vec<RewriteResult>> {
    let plan = plan_compaction(dataset, options).await?;
    if plan.num_tasks() == 0 {
        return Ok(Vec::new());
    }

    let mut results = Vec::with_capacity(plan.num_tasks());
    for task in plan.compaction_tasks() {
        let result = task.execute(dataset).await?;
        results.push(result);
    }

    Ok(results)
}

impl CompactionStats {
    fn from_metrics(m: &CompactionMetrics) -> Self {
        Self {
            fragments_removed: m.fragments_removed as u64,
            fragments_added: m.fragments_added as u64,
            files_removed: m.files_removed as u64,
            files_added: m.files_added as u64,
        }
    }
}

/// Open an S3 dataset using the configured URI and storage options.
async fn open_s3_dataset(config: &TableOpenConfig) -> Result<Dataset> {
    let s3_uri = config.s3_uri.as_ref().ok_or(Error::S3NotConfigured)?;

    let ds = if !config.s3_storage_options.is_empty() {
        DatasetBuilder::from_uri(s3_uri)
            .with_storage_options(config.s3_storage_options.clone())
            .load()
            .await?
    } else {
        Dataset::open(s3_uri.as_str()).await?
    };

    info!(s3_uri, version = ds.version().version, "Opened S3 dataset");
    Ok(ds)
}
