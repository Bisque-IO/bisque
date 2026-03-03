//! BisqueLance storage engine — manages local Lance datasets through the
//! segment lifecycle (active → sealed → deep storage).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::cleanup::{CleanupPolicyBuilder, RemovalStats};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance_index::{DatasetIndexExt, IndexParams, IndexType};
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;
use tracing::{debug, info, warn};

use crate::config::BisqueLanceConfig;
use crate::error::{Error, Result};
use crate::types::{CleanupStats, FlushHandle, FlushState, SealReason, SegmentCatalog, SegmentId};

/// The core storage engine. Manages local Lance datasets and tracks segment state.
///
/// Intended to be wrapped by `LanceStateMachine` which drives it via Raft log application.
pub struct BisqueLance {
    config: BisqueLanceConfig,
    catalog: RwLock<SegmentCatalog>,
    /// Uses tokio async RwLock because Lance Dataset operations are async.
    active_dataset: AsyncRwLock<Option<Dataset>>,
    sealed_dataset: AsyncRwLock<Option<Dataset>>,
    /// S3 deep storage dataset (opened lazily on first flush or when catalog has S3 data).
    s3_dataset: AsyncRwLock<Option<Dataset>>,
    flush_state: RwLock<FlushState>,
    /// Tracks byte size of the active segment for seal-by-size checks.
    active_bytes: AtomicU64,
    /// When the current active segment was created.
    active_created_at: RwLock<Instant>,
}

impl BisqueLance {
    /// Open or create the storage engine.
    ///
    /// Creates the segments directory if needed and opens the active segment dataset.
    /// If a prior catalog is provided (from snapshot restore), uses those segment IDs;
    /// otherwise starts fresh with segment 1.
    pub async fn open(config: BisqueLanceConfig, catalog: Option<SegmentCatalog>) -> Result<Self> {
        let segments_dir = config.segments_dir();
        tokio::fs::create_dir_all(&segments_dir).await?;

        let catalog = catalog.unwrap_or_default();

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
                    warn!("Failed to open S3 dataset: {}, will retry on first flush", e);
                    None
                }
            }
        } else {
            None
        };

        let active_rows = active_dataset
            .count_rows(None)
            .await
            .unwrap_or(0) as u64;

        info!(
            active_segment = catalog.active_segment,
            sealed_segment = ?catalog.sealed_segment,
            s3_manifest_version = catalog.s3_manifest_version,
            s3_configured = config.has_s3(),
            active_rows,
            "BisqueLance engine opened"
        );

        Ok(Self {
            config,
            catalog: RwLock::new(catalog),
            active_dataset: AsyncRwLock::new(Some(active_dataset)),
            sealed_dataset: AsyncRwLock::new(sealed_dataset),
            s3_dataset: AsyncRwLock::new(s3_dataset),
            flush_state: RwLock::new(FlushState::Idle),
            active_bytes: AtomicU64::new(0),
            active_created_at: RwLock::new(Instant::now()),
        })
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

        let append_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        let schema = batches[0].schema();
        let reader = arrow::record_batch::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        );

        let mut ds_guard = self.active_dataset.write().await;
        let ds = ds_guard
            .as_mut()
            .ok_or_else(|| Error::InvalidState("no active dataset".into()))?;

        ds.append(reader, Some(WriteParams::default())).await?;
        drop(ds_guard);

        self.active_bytes.fetch_add(append_bytes, Ordering::Relaxed);
        debug!(bytes = append_bytes, "Appended to active segment");
        Ok(())
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
            sealed = sealed_segment_id,
            new_active = new_active_segment_id,
            ?reason,
            "Sealing active segment"
        );

        // Take the current active dataset
        let old_active = self.active_dataset.write().await.take();

        // Move it to the sealed slot (dropping any previous sealed dataset)
        *self.sealed_dataset.write().await = Some(
            old_active
                .ok_or_else(|| Error::InvalidState("no active dataset to seal".into()))?,
        );

        // Create new active segment
        let new_path = self.config.segment_path(new_active_segment_id);
        let new_dataset = open_or_create_segment(&new_path, &self.config).await?;
        *self.active_dataset.write().await = Some(new_dataset);

        // Update catalog
        {
            let mut cat = self.catalog.write();
            cat.sealed_segment = Some(sealed_segment_id);
            cat.active_segment = new_active_segment_id;
        }

        // Reset active segment metrics
        self.active_bytes.store(0, Ordering::Relaxed);
        *self.active_created_at.write() = Instant::now();

        Ok(())
    }

    /// Create indices on the sealed segment as specified by config.
    ///
    /// Called after sealing. Builds FTS, vector, and scalar indices on the
    /// sealed dataset so they're available for queries before the data is
    /// flushed to S3.
    pub async fn create_seal_indices(&self) -> Result<()> {
        if self.config.seal_indices.is_empty() {
            return Ok(());
        }

        let mut ds_guard = self.sealed_dataset.write().await;
        let ds = match ds_guard.as_mut() {
            Some(ds) => ds,
            None => {
                warn!("No sealed dataset to index");
                return Ok(());
            }
        };

        for spec in &self.config.seal_indices {
            let columns: Vec<&str> = spec.columns.iter().map(|s| s.as_str()).collect();
            let params = index_params_for_type(spec.index_type);

            info!(
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
                    true, // replace if exists
                )
                .await
            {
                Ok(meta) => {
                    info!(
                        name = %meta.name,
                        columns = ?spec.columns,
                        "Index created on sealed segment"
                    );
                }
                Err(e) => {
                    warn!(
                        columns = ?spec.columns,
                        index_type = %spec.index_type,
                        "Failed to create index on sealed segment: {}",
                        e
                    );
                    // Non-fatal: indices are an optimization, not required for correctness
                }
            }
        }

        Ok(())
    }

    /// Apply a BeginFlush command — records that a flush is in progress.
    pub fn apply_begin_flush(&self, segment_id: SegmentId) {
        *self.flush_state.write() = FlushState::InProgress {
            segment_id,
            started_at: chrono::Utc::now().timestamp_millis(),
            fragment_paths: Vec::new(),
        };
        info!(segment_id, "Flush started");
    }

    /// Apply a PromoteToDeepStorage command.
    ///
    /// Updates the S3 manifest version, drops the sealed dataset, and removes
    /// the local sealed segment files.
    pub async fn apply_promote(
        &self,
        segment_id: SegmentId,
        s3_manifest_version: u64,
    ) -> Result<()> {
        info!(segment_id, s3_manifest_version, "Promoting to deep storage");

        // Update catalog
        {
            let mut cat = self.catalog.write();
            cat.s3_manifest_version = s3_manifest_version;
            if cat.sealed_segment == Some(segment_id) {
                cat.sealed_segment = None;
            }
        }

        // Drop the sealed dataset handle
        *self.sealed_dataset.write().await = None;

        // Clean up local sealed segment files
        let segment_path = self.config.segment_path(segment_id);
        if segment_path.exists() {
            tokio::fs::remove_dir_all(&segment_path).await?;
            debug!(?segment_path, "Removed local sealed segment");
        }

        // Reset flush state
        *self.flush_state.write() = FlushState::Idle;

        Ok(())
    }

    // =========================================================================
    // S3 Flush Pipeline (Phase 2) — Leader Only
    // =========================================================================

    /// Begin a flush of the sealed segment to S3. Returns a FlushHandle.
    ///
    /// **Leader only.** The leader should:
    /// 1. Call `begin_flush()` to get a handle
    /// 2. Propose a `BeginFlush` Raft entry (replicated to all nodes)
    /// 3. Call `execute_flush(&handle)` to perform the S3 upload
    /// 4. On success, propose a `PromoteToDeepStorage` Raft entry
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
            segment_id,
            started_at: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Execute the S3 flush: read the sealed segment and append to the S3 dataset.
    ///
    /// **Leader only.** Returns the new S3 manifest version on success.
    /// The caller should then propose a `PromoteToDeepStorage` Raft entry.
    pub async fn execute_flush(&self, handle: &FlushHandle) -> Result<u64> {
        let s3_uri = self
            .config
            .s3_uri
            .as_ref()
            .ok_or(Error::S3NotConfigured)?;

        info!(
            segment_id = handle.segment_id,
            s3_uri,
            "Executing S3 flush"
        );

        // Read all data from the sealed segment
        let batches = {
            let ds_guard = self.sealed_dataset.read().await;
            let ds = ds_guard
                .as_ref()
                .ok_or_else(|| Error::InvalidState("no sealed dataset for flush".into()))?;

            let stream = ds
                .scan()
                .try_into_stream()
                .await
                .map_err(|e| Error::Lance(e))?;

            stream.try_collect::<Vec<RecordBatch>>().await?
        };

        if batches.is_empty() {
            info!(segment_id = handle.segment_id, "Sealed segment is empty, nothing to flush");
            // Return current version — no new data was written
            let version = self.catalog.read().s3_manifest_version;
            return Ok(version);
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!(
            segment_id = handle.segment_id,
            total_rows,
            num_batches = batches.len(),
            "Read sealed segment data, writing to S3"
        );

        // Build write params for S3
        let write_params = WriteParams {
            max_rows_per_file: self.config.s3_max_rows_per_file,
            max_rows_per_group: self.config.s3_max_rows_per_group,
            mode: WriteMode::Append,
            enable_v2_manifest_paths: true,
            ..Default::default()
        };

        let schema = batches[0].schema();
        let reader = arrow::record_batch::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        );

        // Open or create the S3 dataset and append
        let mut s3_guard = self.s3_dataset.write().await;

        let new_version = if let Some(ref mut s3_ds) = *s3_guard {
            // Append to existing S3 dataset
            s3_ds.append(reader, Some(write_params)).await?;
            s3_ds.version().version
        } else {
            // First flush — create the S3 dataset
            let s3_ds = if !self.config.s3_storage_options.is_empty() {
                let builder = DatasetBuilder::from_uri(s3_uri)
                    .with_storage_options(self.config.s3_storage_options.clone());
                // Try to load existing dataset
                match builder.load().await {
                    Ok(mut ds) => {
                        ds.append(reader, Some(write_params)).await?;
                        ds
                    }
                    Err(_) => {
                        // Dataset doesn't exist yet, create it
                        let mut params = write_params.clone();
                        params.mode = WriteMode::Create;
                        Dataset::write(reader, s3_uri.as_str(), Some(params)).await?
                    }
                }
            } else {
                // No special storage options
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
            *s3_guard = Some(s3_ds);
            version
        };

        info!(
            segment_id = handle.segment_id,
            new_s3_version = new_version,
            "S3 flush completed"
        );

        Ok(new_version)
    }

    // =========================================================================
    // Crash Recovery (Phase 2) — New Leader
    // =========================================================================

    /// Recover from a crashed flush. Called by a new leader on startup.
    ///
    /// If a flush was in progress when the old leader crashed:
    /// 1. The sealed segment is still intact on local NVMe
    /// 2. Any S3 fragments written by the old leader are orphans (no manifest points to them)
    /// 3. We clean up orphans and reset flush state so a new flush can be initiated
    pub async fn recover_flush(&self) -> Result<()> {
        let flush_state = self.flush_state.read().clone();

        match flush_state {
            FlushState::Idle => {
                debug!("No flush recovery needed");
                Ok(())
            }
            FlushState::InProgress {
                segment_id,
                started_at,
                ..
            } => {
                warn!(
                    segment_id,
                    started_at,
                    "Recovering from incomplete flush"
                );

                // Clean up any orphaned S3 fragments from the failed flush.
                // Since the manifest was never committed, the fragments are unreferenced.
                if self.config.has_s3() {
                    if let Err(e) = self.cleanup_s3_internal(true).await {
                        warn!("S3 cleanup during recovery failed: {}", e);
                        // Non-fatal — orphans will be cleaned up on the next successful cleanup
                    }
                }

                // Reset flush state so a new flush can be initiated
                *self.flush_state.write() = FlushState::Idle;

                info!(segment_id, "Flush recovery complete, ready to re-flush");
                Ok(())
            }
        }
    }

    // =========================================================================
    // S3 Cleanup (Phase 2)
    // =========================================================================

    /// Run cleanup on the S3 dataset to remove old versions and orphaned files.
    ///
    /// **Only safe to call when no flush is in progress** (verified via flush state).
    pub async fn cleanup_s3(&self) -> Result<CleanupStats> {
        if !self.config.has_s3() {
            return Err(Error::S3NotConfigured);
        }

        // Verify no flush in progress
        if let FlushState::InProgress { segment_id, .. } = &*self.flush_state.read() {
            return Err(Error::FlushInProgress(*segment_id));
        }

        let stats = self.cleanup_s3_internal(true).await?;
        Ok(stats)
    }

    /// Internal S3 cleanup implementation.
    ///
    /// `delete_unverified`: if true, removes orphaned files not referenced by any manifest.
    /// Safe when no concurrent writes are happening (no flush in progress).
    async fn cleanup_s3_internal(&self, delete_unverified: bool) -> Result<CleanupStats> {
        let s3_guard = self.s3_dataset.read().await;
        let ds = match s3_guard.as_ref() {
            Some(ds) => ds,
            None => {
                debug!("No S3 dataset open, skipping cleanup");
                return Ok(CleanupStats::default());
            }
        };

        let policy = CleanupPolicyBuilder::default()
            .delete_unverified(delete_unverified)
            .build();

        let removal_stats: RemovalStats =
            lance::dataset::cleanup::cleanup_old_versions(ds, policy).await?;

        let stats = CleanupStats {
            versions_removed: removal_stats.old_versions as u64,
            bytes_freed: removal_stats.bytes_removed as u64,
            ..Default::default()
        };

        info!(
            files_removed = stats.files_removed,
            versions_removed = stats.versions_removed,
            bytes_freed = stats.bytes_freed,
            "S3 cleanup completed"
        );

        Ok(stats)
    }

    // =========================================================================
    // Query Helpers
    // =========================================================================

    /// Check if the active segment should be sealed.
    pub fn should_seal(&self) -> Option<SealReason> {
        let age = self.active_created_at.read().elapsed();
        if age >= self.config.seal_max_age {
            return Some(SealReason::MaxAge);
        }

        let size = self.active_bytes.load(Ordering::Relaxed);
        if size >= self.config.seal_max_size {
            return Some(SealReason::MaxSize);
        }

        None
    }

    /// Get a snapshot of the current catalog state.
    pub fn catalog(&self) -> SegmentCatalog {
        self.catalog.read().clone()
    }

    /// Get the current flush state.
    pub fn flush_state(&self) -> FlushState {
        self.flush_state.read().clone()
    }

    /// Get the next segment ID (one past the highest known segment).
    pub fn next_segment_id(&self) -> SegmentId {
        let cat = self.catalog.read();
        let max = cat
            .active_segment
            .max(cat.sealed_segment.unwrap_or(0));
        max + 1
    }

    /// Check if a flush is currently in progress.
    pub fn is_flush_in_progress(&self) -> bool {
        matches!(&*self.flush_state.read(), FlushState::InProgress { .. })
    }

    /// Get a snapshot (clone) of the active dataset for querying.
    pub async fn active_dataset_snapshot(&self) -> Option<Dataset> {
        self.active_dataset.read().await.clone()
    }

    /// Get a snapshot (clone) of the sealed dataset for querying.
    pub async fn sealed_dataset_snapshot(&self) -> Option<Dataset> {
        self.sealed_dataset.read().await.clone()
    }

    /// Get a snapshot (clone) of the S3 dataset for querying.
    pub async fn s3_dataset_snapshot(&self) -> Option<Dataset> {
        self.s3_dataset.read().await.clone()
    }

    /// Get the schema from the active dataset.
    pub async fn schema(&self) -> Option<arrow_schema::SchemaRef> {
        let guard = self.active_dataset.read().await;
        guard.as_ref().map(|ds| {
            Arc::new(arrow_schema::Schema::from(ds.schema()))
        })
    }

    /// Get the config.
    pub fn config(&self) -> &BisqueLanceConfig {
        &self.config
    }

    /// Gracefully shutdown the engine.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down BisqueLance engine");
        *self.active_dataset.write().await = None;
        *self.sealed_dataset.write().await = None;
        *self.s3_dataset.write().await = None;
        Ok(())
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Open an existing segment or create a new empty one.
async fn open_or_create_segment(path: &Path, config: &BisqueLanceConfig) -> Result<Dataset> {
    let uri = path.to_str().unwrap();

    if path.exists() {
        debug!(?path, "Opening existing segment");
        Ok(Dataset::open(uri).await?)
    } else {
        debug!(?path, "Creating new empty segment");

        let schema = config
            .schema
            .clone()
            .unwrap_or_else(|| {
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
///
/// For scalar/BTree/Bitmap/Inverted types, uses the appropriate Lance params.
/// For vector types, uses `VectorIndexParams` with sensible defaults.
fn index_params_for_type(index_type: IndexType) -> Box<dyn IndexParams> {
    match index_type {
        IndexType::Inverted | IndexType::NGram => {
            Box::new(InvertedIndexParams::default())
        }
        IndexType::BTree | IndexType::Scalar | IndexType::Bitmap | IndexType::LabelList => {
            Box::new(ScalarIndexParams::default())
        }
        // Vector index types — use lance's VectorIndexParams
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
            // Default: IVF-HNSW-SQ with 256 partitions, L2 distance
            let ivf = IvfBuildParams::new(256);
            let sq = lance_index::vector::sq::builder::SQBuildParams::default();
            Box::new(VectorIndexParams::with_ivf_hnsw_sq_params(
                lance_linalg::distance::DistanceType::L2,
                ivf,
                Default::default(), // HnswBuildParams
                sq,
            ))
        }
        _ => {
            // Fallback to scalar params for unknown types
            Box::new(ScalarIndexParams::default())
        }
    }
}

/// Open an S3 dataset using the configured URI and storage options.
async fn open_s3_dataset(config: &BisqueLanceConfig) -> Result<Dataset> {
    let s3_uri = config
        .s3_uri
        .as_ref()
        .ok_or(Error::S3NotConfigured)?;

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
