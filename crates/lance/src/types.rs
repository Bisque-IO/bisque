use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::TimeUnit;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Unique identifier for a segment. Monotonically increasing.
pub type SegmentId = u64;

/// Reason a segment was sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SealReason {
    /// Active segment exceeded the maximum age threshold.
    MaxAge,
    /// Active segment exceeded the maximum size threshold.
    MaxSize,
}

impl fmt::Display for SealReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SealReason::MaxAge => write!(f, "MaxAge"),
            SealReason::MaxSize => write!(f, "MaxSize"),
        }
    }
}

/// Tracks which segments are active, sealed, and the S3 deep storage version for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentCatalog {
    /// Currently active segment accepting writes.
    pub active_segment: SegmentId,
    /// Most recently sealed segment (at most one).
    pub sealed_segment: Option<SegmentId>,
    /// Current S3 deep storage manifest version.
    pub s3_manifest_version: u64,
    /// S3 dataset URI.
    pub s3_dataset_uri: String,
    /// First raft log index written to the active segment.
    /// `None` if no data has been written to the active segment yet.
    #[serde(default)]
    pub active_first_log_index: Option<u64>,
    /// First raft log index written to the sealed segment.
    /// `None` if no sealed segment or it has no data.
    #[serde(default)]
    pub sealed_first_log_index: Option<u64>,
}

impl Default for SegmentCatalog {
    fn default() -> Self {
        Self {
            active_segment: 1,
            sealed_segment: None,
            s3_manifest_version: 0,
            s3_dataset_uri: String::new(),
            active_first_log_index: None,
            sealed_first_log_index: None,
        }
    }
}

/// Tracks the state of a flush operation (sealed segment → S3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlushState {
    /// No flush in progress.
    Idle,
    /// Leader is flushing sealed segment to S3.
    InProgress {
        /// The segment being flushed.
        segment_id: SegmentId,
        /// When the flush started (millis since epoch).
        started_at: i64,
        /// S3 keys of fragment files written so far (for orphan cleanup).
        fragment_paths: Vec<String>,
    },
}

impl Default for FlushState {
    fn default() -> Self {
        FlushState::Idle
    }
}

/// 128-bit transaction identifier for multi-table transactions.
///
/// Generated client-side via `uuid::Uuid::new_v4()`. Correlates all chunks
/// belonging to the same transaction across Raft log entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxnId(pub u128);

impl TxnId {
    /// Create a new random transaction ID.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().as_u128())
    }
}

impl Default for TxnId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// Individual operation within a multi-table transaction chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnOp {
    /// Append IPC-encoded RecordBatches to a table.
    Append { table: Arc<str>, data: Bytes },
    /// Soft-delete rows matching a SQL filter predicate.
    Delete { table: Arc<str>, filter: String },
    /// Delete matching rows then append replacement data.
    Update {
        table: Arc<str>,
        filter: String,
        data: Bytes,
    },
    /// Create a new table with the given IPC-encoded schema.
    CreateTable { table: Arc<str>, schema_ipc: Bytes },
    /// Drop a table and all its data.
    DropTable { table: Arc<str> },
}

impl TxnOp {
    /// Returns the table name this operation targets.
    pub fn table_name(&self) -> &str {
        match self {
            TxnOp::Append { table, .. }
            | TxnOp::Delete { table, .. }
            | TxnOp::Update { table, .. }
            | TxnOp::CreateTable { table, .. }
            | TxnOp::DropTable { table } => table,
        }
    }

    /// Approximate byte size of this operation's payload (for memory tracking).
    pub fn payload_bytes(&self) -> usize {
        match self {
            TxnOp::Append { data, .. } => data.len(),
            TxnOp::Delete { filter, .. } => filter.len(),
            TxnOp::Update { filter, data, .. } => filter.len() + data.len(),
            TxnOp::CreateTable { schema_ipc, .. } => schema_ipc.len(),
            TxnOp::DropTable { .. } => 0,
        }
    }
}

/// Raft log entry command type for bisque-lance.
///
/// Each variant is replicated via Raft and applied to all nodes' state machines.
/// All variants include a `table_name` for multi-table routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LanceCommand {
    /// Create a new table with the given schema (Arrow IPC-encoded).
    CreateTable {
        table_name: Arc<str>,
        schema_ipc: Bytes,
    },

    /// Drop a table and remove all its data.
    DropTable { table_name: Arc<str> },

    /// Replicate raw data to all nodes.
    /// Every node appends this data to its local active Lance dataset.
    /// `data` contains Arrow IPC-encoded RecordBatches.
    AppendRecords { table_name: Arc<str>, data: Bytes },

    /// Seal the current active segment and create a new one.
    SealActiveSegment {
        table_name: Arc<str>,
        sealed_segment_id: SegmentId,
        new_active_segment_id: SegmentId,
        reason: SealReason,
    },

    /// Mark the start of a flush operation (leader only writes to S3).
    BeginFlush {
        table_name: Arc<str>,
        segment_id: SegmentId,
    },

    /// Promote sealed segment data to S3 deep storage.
    /// Applied after successful S3 manifest commit.
    PromoteToDeepStorage {
        table_name: Arc<str>,
        segment_id: SegmentId,
        s3_manifest_version: u64,
    },

    /// Register a new client session for version pinning (replicated cluster-wide).
    RegisterSession { session_id: u64 },

    /// Pin a dataset version so compaction doesn't delete it.
    PinVersion {
        session_id: u64,
        table_name: Arc<str>,
        tier: Arc<str>,
        version: u64,
    },

    /// Release a previously pinned version.
    UnpinVersion {
        session_id: u64,
        table_name: Arc<str>,
        tier: Arc<str>,
        version: u64,
    },

    /// Expire/remove a client session, releasing all its pins.
    ExpireSession { session_id: u64 },

    /// Delete rows matching a SQL filter predicate from all tiers.
    /// Uses Lance deletion vectors (soft delete) — no data rewrite.
    DeleteRecords {
        table_name: Arc<str>,
        /// SQL filter predicate (e.g. "id = 5", "ts < '2024-01-01'").
        filter: String,
    },

    /// Update rows: soft-delete matching rows across all tiers, then append
    /// replacement data to the active segment.
    UpdateRecords {
        table_name: Arc<str>,
        /// SQL filter predicate for rows to replace.
        filter: String,
        /// IPC-encoded replacement RecordBatches.
        data: Bytes,
    },

    /// Accumulate a chunk of multi-table transaction data.
    /// Inert on apply — the state machine buffers the ops without writing to Lance.
    TxnChunk {
        txn_id: TxnId,
        seq: u32,
        ops: Vec<TxnOp>,
    },

    /// Commit a multi-table transaction. Triggers reassembly of all chunks
    /// and atomic apply across all affected tables.
    TxnCommit { txn_id: TxnId, total_chunks: u32 },

    /// Abort a multi-table transaction. Discards buffered chunks.
    TxnAbort { txn_id: TxnId },
}

impl fmt::Display for LanceCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LanceCommand::CreateTable {
                table_name,
                schema_ipc,
            } => {
                write!(
                    f,
                    "CreateTable(table={}, schema={} bytes)",
                    table_name,
                    schema_ipc.len()
                )
            }
            LanceCommand::DropTable { table_name } => {
                write!(f, "DropTable(table={})", table_name)
            }
            LanceCommand::AppendRecords { table_name, data } => {
                write!(
                    f,
                    "AppendRecords(table={}, {} bytes)",
                    table_name,
                    data.len()
                )
            }
            LanceCommand::SealActiveSegment {
                table_name,
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => write!(
                f,
                "SealActiveSegment(table={}, sealed={}, new={}, reason={})",
                table_name, sealed_segment_id, new_active_segment_id, reason
            ),
            LanceCommand::BeginFlush {
                table_name,
                segment_id,
            } => {
                write!(
                    f,
                    "BeginFlush(table={}, segment={})",
                    table_name, segment_id
                )
            }
            LanceCommand::PromoteToDeepStorage {
                table_name,
                segment_id,
                s3_manifest_version,
            } => write!(
                f,
                "PromoteToDeepStorage(table={}, segment={}, version={})",
                table_name, segment_id, s3_manifest_version
            ),
            LanceCommand::RegisterSession { session_id } => {
                write!(f, "RegisterSession(session={})", session_id)
            }
            LanceCommand::PinVersion {
                session_id,
                table_name,
                tier,
                version,
            } => write!(
                f,
                "PinVersion(session={}, table={}, tier={}, version={})",
                session_id, table_name, tier, version
            ),
            LanceCommand::UnpinVersion {
                session_id,
                table_name,
                tier,
                version,
            } => write!(
                f,
                "UnpinVersion(session={}, table={}, tier={}, version={})",
                session_id, table_name, tier, version
            ),
            LanceCommand::ExpireSession { session_id } => {
                write!(f, "ExpireSession(session={})", session_id)
            }
            LanceCommand::DeleteRecords { table_name, filter } => {
                write!(f, "DeleteRecords(table={}, filter={})", table_name, filter)
            }
            LanceCommand::UpdateRecords {
                table_name,
                filter,
                data,
            } => {
                write!(
                    f,
                    "UpdateRecords(table={}, filter={}, {} bytes)",
                    table_name,
                    filter,
                    data.len()
                )
            }
            LanceCommand::TxnChunk { txn_id, seq, ops } => {
                write!(
                    f,
                    "TxnChunk(txn={}, seq={}, {} ops)",
                    txn_id,
                    seq,
                    ops.len()
                )
            }
            LanceCommand::TxnCommit {
                txn_id,
                total_chunks,
            } => {
                write!(
                    f,
                    "TxnCommit(txn={}, total_chunks={})",
                    txn_id, total_chunks
                )
            }
            LanceCommand::TxnAbort { txn_id } => {
                write!(f, "TxnAbort(txn={})", txn_id)
            }
        }
    }
}

/// Response type for Raft client_write calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LanceResponse {
    /// Operation succeeded.
    Ok,
    /// Operation failed.
    Error(String),
    /// Operation succeeded, returning the number of affected rows.
    RowsAffected(u64),
}

/// Result of a successful write operation.
///
/// Contains the Raft log index at which the write was committed. Clients
/// can pass this index to query APIs for read-after-write consistency —
/// the query handler will wait until the Lance materialization watermark
/// reaches this index before executing.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Raft log index at which the write was committed.
    pub log_index: u64,
    /// The response from the state machine.
    pub response: LanceResponse,
}

impl fmt::Display for LanceResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LanceResponse::Ok => write!(f, "OK"),
            LanceResponse::Error(e) => write!(f, "Error: {}", e),
            LanceResponse::RowsAffected(n) => write!(f, "RowsAffected({})", n),
        }
    }
}

/// Handle returned by `begin_flush()` to track a flush operation.
///
/// The leader uses this to execute the S3 flush and then propose
/// a `PromoteToDeepStorage` Raft entry on success.
#[derive(Debug, Clone)]
pub struct FlushHandle {
    /// The table this flush belongs to.
    pub table_name: Arc<str>,
    /// The segment being flushed.
    pub segment_id: SegmentId,
    /// Timestamp when flush began (millis since epoch).
    pub started_at: i64,
}

/// Statistics from an S3 cleanup operation.
#[derive(Debug, Clone, Default)]
pub struct CleanupStats {
    /// Number of old data files removed.
    pub files_removed: u64,
    /// Number of old manifest versions removed.
    pub versions_removed: u64,
    /// Total bytes freed.
    pub bytes_freed: u64,
}

/// Statistics from a compaction operation.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of fragments removed by compaction.
    pub fragments_removed: u64,
    /// Number of new fragments created by compaction.
    pub fragments_added: u64,
    /// Number of individual files removed.
    pub files_removed: u64,
    /// Number of individual files added.
    pub files_added: u64,
}

/// A recorded schema version for schema evolution tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Monotonically increasing version number (1-based).
    pub version: u64,
    /// Arrow IPC-encoded schema bytes.
    #[serde(with = "bisque_protocol::catalog_events::serde_bytes_as_vec")]
    pub schema_ipc: Bytes,
    /// When this schema version was recorded (millis since epoch).
    pub created_at_millis: i64,
}

/// Per-table snapshot data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSnapshot {
    pub catalog: SegmentCatalog,
    pub flush_state: FlushState,
    pub schema_history: Vec<SchemaVersion>,
}

/// Catalog-level metadata stored alongside per-group Raft state.
///
/// Tracks catalog-wide configuration such as whether OpenTelemetry
/// ingestion tables have been enabled.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogMeta {
    /// Whether OTel tables have been created in this catalog.
    #[serde(default)]
    pub otel_enabled: bool,
    /// Unix timestamp (seconds) when OTel was enabled.
    #[serde(default)]
    pub otel_enabled_at: Option<u64>,
    /// Identity of the user who enabled OTel (e.g. "tenant:42" or email).
    #[serde(default)]
    pub otel_enabled_by: Option<String>,
}

// =============================================================================
// Multi-Table Transaction Types
// =============================================================================

/// A pending multi-table transaction being accumulated via `TxnChunk` commands.
pub struct PendingTxn {
    /// Chunks indexed by sequence number.
    pub chunks: BTreeMap<u32, Vec<TxnOp>>,
    /// Raft log index of the first chunk (for GC).
    pub first_log_index: u64,
    /// Total buffered payload bytes across all chunks.
    pub total_bytes: usize,
}

/// Staging area for in-flight multi-table transactions.
#[derive(Default)]
pub struct PendingTxns {
    /// Map from transaction ID to its accumulated chunks.
    pub txns: HashMap<TxnId, PendingTxn>,
    /// Total bytes across all pending transactions (for global memory cap).
    pub total_bytes: usize,
}

impl PendingTxns {
    /// Insert a chunk into a pending transaction. Returns an error message
    /// if memory limits are exceeded.
    pub fn insert_chunk(
        &mut self,
        txn_id: TxnId,
        seq: u32,
        ops: Vec<TxnOp>,
        log_index: u64,
        config: &TxnConfig,
    ) -> Result<(), String> {
        let chunk_bytes: usize = ops.iter().map(|op| op.payload_bytes()).sum();

        // Check global cap.
        if self.total_bytes + chunk_bytes > config.max_total_pending_bytes {
            return Err(format!(
                "transaction {} rejected: global pending bytes ({} + {}) exceeds limit {}",
                txn_id, self.total_bytes, chunk_bytes, config.max_total_pending_bytes
            ));
        }

        let pending = self.txns.entry(txn_id).or_insert_with(|| PendingTxn {
            chunks: BTreeMap::new(),
            first_log_index: log_index,
            total_bytes: 0,
        });

        // Check per-txn cap.
        if pending.total_bytes + chunk_bytes > config.max_txn_bytes {
            return Err(format!(
                "transaction {} rejected: txn bytes ({} + {}) exceeds limit {}",
                txn_id, pending.total_bytes, chunk_bytes, config.max_txn_bytes
            ));
        }

        // If overwriting an existing chunk at this seq, subtract old bytes first.
        if let Some(old_ops) = pending.chunks.insert(seq, ops) {
            let old_bytes: usize = old_ops.iter().map(|op| op.payload_bytes()).sum();
            pending.total_bytes = pending.total_bytes.saturating_sub(old_bytes);
            self.total_bytes = self.total_bytes.saturating_sub(old_bytes);
        }
        pending.total_bytes += chunk_bytes;
        self.total_bytes += chunk_bytes;
        Ok(())
    }

    /// Remove and return a pending transaction.
    pub fn remove(&mut self, txn_id: &TxnId) -> Option<PendingTxn> {
        if let Some(pending) = self.txns.remove(txn_id) {
            self.total_bytes = self.total_bytes.saturating_sub(pending.total_bytes);
            Some(pending)
        } else {
            None
        }
    }

    /// Garbage-collect abandoned transactions whose first chunk is older than
    /// `max_log_age` entries before `current_log_index`.
    pub fn gc(&mut self, current_log_index: u64, max_log_age: u64) -> usize {
        let before = self.txns.len();
        self.txns.retain(|_, pending| {
            current_log_index.saturating_sub(pending.first_log_index) <= max_log_age
        });
        let evicted = before - self.txns.len();
        if evicted > 0 {
            // Recalculate total_bytes after eviction.
            self.total_bytes = self.txns.values().map(|p| p.total_bytes).sum();
        }
        evicted
    }

    /// Clear all pending transactions (e.g., on snapshot install).
    pub fn clear(&mut self) {
        self.txns.clear();
        self.total_bytes = 0;
    }
}

/// Configuration for multi-table transaction handling.
#[derive(Debug, Clone)]
pub struct TxnConfig {
    /// Maximum payload bytes per individual transaction (default: 256 MB).
    pub max_txn_bytes: usize,
    /// Maximum total payload bytes across all pending transactions (default: 1 GB).
    pub max_total_pending_bytes: usize,
    /// GC timeout: evict pending transactions whose first chunk is this many
    /// log entries behind the current apply index (default: 100,000).
    pub gc_log_index_timeout: u64,
    /// How often (in applied log entries) to run GC (default: 1,000).
    pub gc_check_interval: u64,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self {
            max_txn_bytes: 256 * 1024 * 1024,
            max_total_pending_bytes: 1024 * 1024 * 1024,
            gc_log_index_timeout: 100_000,
            gc_check_interval: 1_000,
        }
    }
}

/// Snapshot payload for the state machine.
/// Contains metadata for all tables — actual data lives in Lance datasets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub tables: HashMap<String, PersistedTableEntry>,
    /// Minimum raft log index that must be retained.
    /// Computed as the minimum `first_log_index` across all tables' active and
    /// sealed segments. `None` means all data is in S3 (or no tables exist).
    #[serde(default)]
    pub min_safe_log_index: Option<u64>,
    /// File manifest for fresh node recovery.
    /// Lists every Lance segment file that must be transferred to restore
    /// hot/warm data on a fresh node.
    #[serde(default)]
    pub file_manifest: Vec<SnapshotFileEntry>,
    /// Address of the leader's segment sync server (host:port).
    /// Fresh nodes connect here to stream segment files after receiving
    /// the metadata snapshot.
    #[serde(default)]
    pub sync_addr: Option<String>,
    /// Catalog-level metadata (OTel config, etc.).
    #[serde(default)]
    pub catalog_meta: Option<CatalogMeta>,
}

/// Entry in the snapshot file manifest describing a single file to transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFileEntry {
    /// Relative path from `local_data_dir`
    /// (e.g., `"tables/my_table/segments/1.lance/data/0.lance"`)
    pub relative_path: String,
    /// File size in bytes.
    pub size: u64,
}

// =============================================================================
// Persisted Configuration Types
// =============================================================================

/// Serializable mirror of [`IndexSpec`](crate::config::IndexSpec).
///
/// `IndexType` from lance_index does not implement `Serialize`, so we store it
/// as a string (e.g. `"Inverted"`, `"BTree"`, `"IvfHnswSq"`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedIndexSpec {
    pub columns: Vec<String>,
    /// String representation of the `IndexType` variant.
    pub index_type: String,
    pub name: Option<String>,
}

/// Serializable descriptor for reconstructing a [`WriteProcessor`](crate::WriteProcessor).
///
/// Each variant captures the constructor arguments needed to rebuild the
/// concrete processor at recovery time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProcessorDescriptor {
    Counter {
        key_columns: Vec<String>,
        value_column: String,
        timestamp_column: Option<String>,
        timestamp_resolution_ms: i64,
        timestamp_unit: String,
        #[serde(default)]
        start_time_column: Option<String>,
    },
    Gauge {
        key_columns: Vec<String>,
        value_column: String,
        timestamp_column: Option<String>,
        timestamp_unit: String,
        #[serde(default)]
        start_time_column: Option<String>,
    },
    Histogram {
        key_columns: Vec<String>,
        boundaries_column: String,
        bucket_counts_column: String,
        sum_column: String,
        count_column: String,
        timestamp_column: Option<String>,
        timestamp_unit: String,
        #[serde(default)]
        start_time_column: Option<String>,
        #[serde(default)]
        min_column: Option<String>,
        #[serde(default)]
        max_column: Option<String>,
    },
    /// Specialized OTEL sum (counter) aggregator (sums values, truncates timestamps).
    OtelSum { timestamp_resolution_ms: i64 },
    /// Specialized OTEL gauge processor (last-write-wins).
    OtelGauge,
    /// Specialized OTEL histogram processor (merges buckets).
    OtelHistogram,
    /// Specialized OTEL exponential histogram aggregator (merges exp buckets).
    OtelExpHistogram,
}

impl ProcessorDescriptor {
    /// Reconstruct the concrete [`WriteProcessor`](crate::WriteProcessor) from
    /// this descriptor.
    pub fn into_processor(&self) -> Arc<dyn crate::write_processor::WriteProcessor> {
        match self {
            ProcessorDescriptor::Counter {
                key_columns,
                value_column,
                timestamp_column,
                timestamp_resolution_ms,
                timestamp_unit,
                start_time_column,
            } => {
                let mut agg = crate::processors::CounterAggregator::new(
                    key_columns.clone(),
                    value_column.clone(),
                );
                if let Some(ts_col) = timestamp_column {
                    agg = agg.with_timestamp(ts_col.clone(), *timestamp_resolution_ms);
                }
                agg = agg.with_timestamp_unit(parse_time_unit(timestamp_unit));
                if let Some(st_col) = start_time_column {
                    agg = agg.with_start_time(st_col.clone());
                }
                Arc::new(agg)
            }
            ProcessorDescriptor::Gauge {
                key_columns,
                value_column,
                timestamp_column,
                timestamp_unit,
                start_time_column,
            } => {
                let mut agg = crate::processors::GaugeAggregator::new(
                    key_columns.clone(),
                    value_column.clone(),
                );
                if let Some(ts_col) = timestamp_column {
                    agg = agg.with_timestamp(ts_col.clone());
                }
                agg = agg.with_timestamp_unit(parse_time_unit(timestamp_unit));
                if let Some(st_col) = start_time_column {
                    agg = agg.with_start_time(st_col.clone());
                }
                Arc::new(agg)
            }
            ProcessorDescriptor::Histogram {
                key_columns,
                boundaries_column,
                bucket_counts_column,
                sum_column,
                count_column,
                timestamp_column,
                timestamp_unit,
                start_time_column,
                min_column,
                max_column,
            } => {
                let mut agg = crate::processors::HistogramAggregator::new(key_columns.clone())
                    .with_column_names(
                        boundaries_column.clone(),
                        bucket_counts_column.clone(),
                        sum_column.clone(),
                        count_column.clone(),
                    );
                if let Some(ts_col) = timestamp_column {
                    agg = agg.with_timestamp(ts_col.clone());
                }
                agg = agg.with_timestamp_unit(parse_time_unit(timestamp_unit));
                if let Some(st_col) = start_time_column {
                    agg = agg.with_start_time(st_col.clone());
                }
                if let Some(min_col) = min_column {
                    agg = agg.with_min_column(min_col.clone());
                }
                if let Some(max_col) = max_column {
                    agg = agg.with_max_column(max_col.clone());
                }
                Arc::new(agg)
            }
            ProcessorDescriptor::OtelSum {
                timestamp_resolution_ms,
            } => Arc::new(crate::otel::processors::OtelSumAggregator::new(
                *timestamp_resolution_ms,
            )),
            ProcessorDescriptor::OtelGauge => {
                Arc::new(crate::otel::processors::OtelGaugeProcessor::new())
            }
            ProcessorDescriptor::OtelHistogram => {
                Arc::new(crate::otel::processors::OtelHistogramProcessor::new())
            }
            ProcessorDescriptor::OtelExpHistogram => {
                Arc::new(crate::otel::processors::OtelExpHistogramAggregator::new())
            }
        }
    }
}

/// Serializable batcher configuration (without `dyn WriteProcessor`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedBatcherConfig {
    pub linger_ms: u64,
    pub max_batch_bytes: usize,
    pub channel_capacity: usize,
}

/// Full per-table configuration that can be persisted to MDBX.
///
/// Does NOT include:
/// - `s3_storage_options` — contains credentials, supplied from environment
/// - `table_data_dir` — derived from engine config at runtime
/// - `schema` — stored separately in `schema_history`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedTableConfig {
    pub seal_indices: Vec<PersistedIndexSpec>,
    pub s3_uri: Option<String>,
    pub s3_max_rows_per_file: usize,
    pub s3_max_rows_per_group: usize,
    pub seal_max_age_ms: u64,
    pub seal_max_size: u64,
    pub compaction_target_rows_per_fragment: usize,
    pub compaction_materialize_deletions: bool,
    pub compaction_deletion_threshold: f32,
    pub compaction_min_fragments: usize,
    pub batcher: Option<PersistedBatcherConfig>,
    pub processor: Option<ProcessorDescriptor>,
}

/// Combined configuration + state per table — what's stored in MDBX.
///
/// Replaces `TableSnapshot` as the system of record. Contains everything
/// needed to fully recover a `TableEngine` instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedTableEntry {
    pub config: PersistedTableConfig,
    pub catalog: SegmentCatalog,
    pub flush_state: FlushState,
    pub schema_history: Vec<SchemaVersion>,
}

impl PersistedTableEntry {
    /// Extract a `TableSnapshot` view (for backward compatibility).
    pub fn to_snapshot(&self) -> TableSnapshot {
        TableSnapshot {
            catalog: self.catalog.clone(),
            flush_state: self.flush_state.clone(),
            schema_history: self.schema_history.clone(),
        }
    }

    /// Create from a `TableSnapshot` with default config (for migration).
    pub fn from_snapshot_with_defaults(snapshot: TableSnapshot) -> Self {
        Self {
            config: PersistedTableConfig::default(),
            catalog: snapshot.catalog,
            flush_state: snapshot.flush_state,
            schema_history: snapshot.schema_history,
        }
    }
}

impl Default for PersistedTableConfig {
    fn default() -> Self {
        Self {
            seal_indices: Vec::new(),
            s3_uri: None,
            s3_max_rows_per_file: 5_000_000,
            s3_max_rows_per_group: 50_000,
            seal_max_age_ms: 60_000,
            seal_max_size: 1024 * 1024 * 1024,
            compaction_target_rows_per_fragment: 1_048_576,
            compaction_materialize_deletions: true,
            compaction_deletion_threshold: 0.1,
            compaction_min_fragments: 4,
            batcher: None,
            processor: None,
        }
    }
}

// =============================================================================
// TimeUnit Helpers
// =============================================================================

/// Parse an Arrow `TimeUnit` from its string representation.
pub fn parse_time_unit(s: &str) -> TimeUnit {
    match s {
        "Second" => TimeUnit::Second,
        "Millisecond" => TimeUnit::Millisecond,
        "Microsecond" => TimeUnit::Microsecond,
        "Nanosecond" => TimeUnit::Nanosecond,
        _ => TimeUnit::Millisecond, // safe default
    }
}

/// Convert an Arrow `TimeUnit` to its string representation.
pub fn time_unit_to_string(unit: TimeUnit) -> String {
    match unit {
        TimeUnit::Second => "Second".to_string(),
        TimeUnit::Millisecond => "Millisecond".to_string(),
        TimeUnit::Microsecond => "Microsecond".to_string(),
        TimeUnit::Nanosecond => "Nanosecond".to_string(),
    }
}

/// Convert a `Duration` to milliseconds (saturating at u64::MAX).
pub fn duration_to_ms(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // 1. SealReason Display
    // =========================================================================

    #[test]
    fn seal_reason_display_max_age() {
        assert_eq!(SealReason::MaxAge.to_string(), "MaxAge");
    }

    #[test]
    fn seal_reason_display_max_size() {
        assert_eq!(SealReason::MaxSize.to_string(), "MaxSize");
    }

    // =========================================================================
    // 2. SealReason serialization roundtrip
    // =========================================================================

    #[test]
    fn seal_reason_serde_roundtrip() {
        for reason in [SealReason::MaxAge, SealReason::MaxSize] {
            let json = serde_json::to_string(&reason).unwrap();
            let back: SealReason = serde_json::from_str(&json).unwrap();
            assert_eq!(reason, back);
        }
    }

    // =========================================================================
    // 3. SegmentCatalog default values
    // =========================================================================

    #[test]
    fn segment_catalog_default_values() {
        let cat = SegmentCatalog::default();
        assert_eq!(cat.active_segment, 1);
        assert_eq!(cat.sealed_segment, None);
        assert_eq!(cat.s3_manifest_version, 0);
        assert_eq!(cat.s3_dataset_uri, "");
        assert_eq!(cat.active_first_log_index, None);
        assert_eq!(cat.sealed_first_log_index, None);
    }

    // =========================================================================
    // 4. SegmentCatalog serialization roundtrip
    // =========================================================================

    #[test]
    fn segment_catalog_serde_roundtrip_default() {
        let cat = SegmentCatalog::default();
        let json = serde_json::to_string(&cat).unwrap();
        let back: SegmentCatalog = serde_json::from_str(&json).unwrap();
        assert_eq!(back.active_segment, cat.active_segment);
        assert_eq!(back.sealed_segment, cat.sealed_segment);
        assert_eq!(back.s3_manifest_version, cat.s3_manifest_version);
        assert_eq!(back.s3_dataset_uri, cat.s3_dataset_uri);
        assert_eq!(back.active_first_log_index, cat.active_first_log_index);
        assert_eq!(back.sealed_first_log_index, cat.sealed_first_log_index);
    }

    #[test]
    fn segment_catalog_serde_roundtrip_populated() {
        let cat = SegmentCatalog {
            active_segment: 5,
            sealed_segment: Some(4),
            s3_manifest_version: 10,
            s3_dataset_uri: "s3://bucket/path".to_string(),
            active_first_log_index: Some(100),
            sealed_first_log_index: Some(50),
        };
        let json = serde_json::to_string(&cat).unwrap();
        let back: SegmentCatalog = serde_json::from_str(&json).unwrap();
        assert_eq!(back.active_segment, 5);
        assert_eq!(back.sealed_segment, Some(4));
        assert_eq!(back.s3_manifest_version, 10);
        assert_eq!(back.s3_dataset_uri, "s3://bucket/path");
        assert_eq!(back.active_first_log_index, Some(100));
        assert_eq!(back.sealed_first_log_index, Some(50));
    }

    #[test]
    fn segment_catalog_deserialize_missing_optional_fields() {
        // Fields with #[serde(default)] should default to None when absent.
        let json = r#"{
            "active_segment": 1,
            "sealed_segment": null,
            "s3_manifest_version": 0,
            "s3_dataset_uri": ""
        }"#;
        let cat: SegmentCatalog = serde_json::from_str(json).unwrap();
        assert_eq!(cat.active_first_log_index, None);
        assert_eq!(cat.sealed_first_log_index, None);
    }

    // =========================================================================
    // 5. FlushState default is Idle
    // =========================================================================

    #[test]
    fn flush_state_default_is_idle() {
        let state = FlushState::default();
        assert!(matches!(state, FlushState::Idle));
    }

    // =========================================================================
    // 6. FlushState::InProgress serialization roundtrip
    // =========================================================================

    #[test]
    fn flush_state_idle_serde_roundtrip() {
        let state = FlushState::Idle;
        let json = serde_json::to_string(&state).unwrap();
        let back: FlushState = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, FlushState::Idle));
    }

    #[test]
    fn flush_state_in_progress_serde_roundtrip() {
        let state = FlushState::InProgress {
            segment_id: 42,
            started_at: 1700000000000,
            fragment_paths: vec!["frag/0.lance".to_string(), "frag/1.lance".to_string()],
        };
        let json = serde_json::to_string(&state).unwrap();
        let back: FlushState = serde_json::from_str(&json).unwrap();
        match back {
            FlushState::InProgress {
                segment_id,
                started_at,
                fragment_paths,
            } => {
                assert_eq!(segment_id, 42);
                assert_eq!(started_at, 1700000000000);
                assert_eq!(fragment_paths.len(), 2);
                assert_eq!(fragment_paths[0], "frag/0.lance");
                assert_eq!(fragment_paths[1], "frag/1.lance");
            }
            _ => panic!("expected InProgress"),
        }
    }

    // =========================================================================
    // 7. LanceCommand Display for each variant
    // =========================================================================

    #[test]
    fn lance_command_display_create_table() {
        let cmd = LanceCommand::CreateTable {
            table_name: "t1".into(),
            schema_ipc: Bytes::from(vec![0u8; 64]),
        };
        assert_eq!(cmd.to_string(), "CreateTable(table=t1, schema=64 bytes)");
    }

    #[test]
    fn lance_command_display_drop_table() {
        let cmd = LanceCommand::DropTable {
            table_name: "t1".into(),
        };
        assert_eq!(cmd.to_string(), "DropTable(table=t1)");
    }

    #[test]
    fn lance_command_display_append_records() {
        let cmd = LanceCommand::AppendRecords {
            table_name: "t1".into(),
            data: Bytes::from(vec![0u8; 128]),
        };
        assert_eq!(cmd.to_string(), "AppendRecords(table=t1, 128 bytes)");
    }

    #[test]
    fn lance_command_display_seal_active_segment() {
        let cmd = LanceCommand::SealActiveSegment {
            table_name: "t1".into(),
            sealed_segment_id: 2,
            new_active_segment_id: 3,
            reason: SealReason::MaxAge,
        };
        assert_eq!(
            cmd.to_string(),
            "SealActiveSegment(table=t1, sealed=2, new=3, reason=MaxAge)"
        );
    }

    #[test]
    fn lance_command_display_begin_flush() {
        let cmd = LanceCommand::BeginFlush {
            table_name: "t1".into(),
            segment_id: 7,
        };
        assert_eq!(cmd.to_string(), "BeginFlush(table=t1, segment=7)");
    }

    #[test]
    fn lance_command_display_promote_to_deep_storage() {
        let cmd = LanceCommand::PromoteToDeepStorage {
            table_name: "t1".into(),
            segment_id: 7,
            s3_manifest_version: 3,
        };
        assert_eq!(
            cmd.to_string(),
            "PromoteToDeepStorage(table=t1, segment=7, version=3)"
        );
    }

    #[test]
    fn lance_command_display_register_session() {
        let cmd = LanceCommand::RegisterSession { session_id: 99 };
        assert_eq!(cmd.to_string(), "RegisterSession(session=99)");
    }

    #[test]
    fn lance_command_display_pin_version() {
        let cmd = LanceCommand::PinVersion {
            session_id: 1,
            table_name: "t1".into(),
            tier: "hot".into(),
            version: 5,
        };
        assert_eq!(
            cmd.to_string(),
            "PinVersion(session=1, table=t1, tier=hot, version=5)"
        );
    }

    #[test]
    fn lance_command_display_unpin_version() {
        let cmd = LanceCommand::UnpinVersion {
            session_id: 1,
            table_name: "t1".into(),
            tier: "cold".into(),
            version: 3,
        };
        assert_eq!(
            cmd.to_string(),
            "UnpinVersion(session=1, table=t1, tier=cold, version=3)"
        );
    }

    #[test]
    fn lance_command_display_expire_session() {
        let cmd = LanceCommand::ExpireSession { session_id: 42 };
        assert_eq!(cmd.to_string(), "ExpireSession(session=42)");
    }

    // =========================================================================
    // 8. LanceCommand serialization roundtrip for each variant
    // =========================================================================

    fn assert_lance_command_roundtrip(cmd: &LanceCommand) {
        let json = serde_json::to_string(cmd).unwrap();
        let back: LanceCommand = serde_json::from_str(&json).unwrap();
        // Compare via Debug representation since LanceCommand does not derive PartialEq.
        assert_eq!(format!("{:?}", cmd), format!("{:?}", back));
    }

    #[test]
    fn lance_command_serde_roundtrip_create_table() {
        assert_lance_command_roundtrip(&LanceCommand::CreateTable {
            table_name: "t1".into(),
            schema_ipc: Bytes::from(vec![1, 2, 3]),
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_drop_table() {
        assert_lance_command_roundtrip(&LanceCommand::DropTable {
            table_name: "t1".into(),
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_append_records() {
        assert_lance_command_roundtrip(&LanceCommand::AppendRecords {
            table_name: "t1".into(),
            data: Bytes::from(vec![10, 20, 30]),
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_seal_active_segment() {
        assert_lance_command_roundtrip(&LanceCommand::SealActiveSegment {
            table_name: "t1".into(),
            sealed_segment_id: 2,
            new_active_segment_id: 3,
            reason: SealReason::MaxSize,
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_begin_flush() {
        assert_lance_command_roundtrip(&LanceCommand::BeginFlush {
            table_name: "t1".into(),
            segment_id: 5,
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_promote_to_deep_storage() {
        assert_lance_command_roundtrip(&LanceCommand::PromoteToDeepStorage {
            table_name: "t1".into(),
            segment_id: 5,
            s3_manifest_version: 10,
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_register_session() {
        assert_lance_command_roundtrip(&LanceCommand::RegisterSession { session_id: 77 });
    }

    #[test]
    fn lance_command_serde_roundtrip_pin_version() {
        assert_lance_command_roundtrip(&LanceCommand::PinVersion {
            session_id: 1,
            table_name: "t1".into(),
            tier: "hot".into(),
            version: 5,
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_unpin_version() {
        assert_lance_command_roundtrip(&LanceCommand::UnpinVersion {
            session_id: 1,
            table_name: "t1".into(),
            tier: "cold".into(),
            version: 3,
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_expire_session() {
        assert_lance_command_roundtrip(&LanceCommand::ExpireSession { session_id: 42 });
    }

    #[test]
    fn lance_command_display_delete_records() {
        let cmd = LanceCommand::DeleteRecords {
            table_name: "events".into(),
            filter: "id > 100".to_string(),
        };
        assert_eq!(
            cmd.to_string(),
            "DeleteRecords(table=events, filter=id > 100)"
        );
    }

    #[test]
    fn lance_command_display_update_records() {
        let cmd = LanceCommand::UpdateRecords {
            table_name: "metrics".into(),
            filter: "ts < '2024-01-01'".to_string(),
            data: Bytes::from_static(&[10, 20, 30, 40, 50]),
        };
        assert_eq!(
            cmd.to_string(),
            "UpdateRecords(table=metrics, filter=ts < '2024-01-01', 5 bytes)"
        );
    }

    #[test]
    fn lance_command_serde_roundtrip_delete_records() {
        assert_lance_command_roundtrip(&LanceCommand::DeleteRecords {
            table_name: "events".into(),
            filter: "id > 100 AND status = 'inactive'".to_string(),
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_update_records() {
        assert_lance_command_roundtrip(&LanceCommand::UpdateRecords {
            table_name: "metrics".into(),
            filter: "ts < '2024-01-01'".to_string(),
            data: Bytes::from(vec![10, 20, 30, 40, 50]),
        });
    }

    // =========================================================================
    // 9. LanceResponse Display
    // =========================================================================

    #[test]
    fn lance_response_display_ok() {
        assert_eq!(LanceResponse::Ok.to_string(), "OK");
    }

    #[test]
    fn lance_response_display_error() {
        let resp = LanceResponse::Error("something went wrong".to_string());
        assert_eq!(resp.to_string(), "Error: something went wrong");
    }

    // =========================================================================
    // 10. LanceResponse serialization roundtrip
    // =========================================================================

    #[test]
    fn lance_response_serde_roundtrip_ok() {
        let resp = LanceResponse::Ok;
        let json = serde_json::to_string(&resp).unwrap();
        let back: LanceResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{:?}", resp), format!("{:?}", back));
    }

    #[test]
    fn lance_response_serde_roundtrip_error() {
        let resp = LanceResponse::Error("fail".to_string());
        let json = serde_json::to_string(&resp).unwrap();
        let back: LanceResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{:?}", resp), format!("{:?}", back));
    }

    #[test]
    fn lance_response_display_rows_affected() {
        let resp = LanceResponse::RowsAffected(42);
        assert_eq!(resp.to_string(), "RowsAffected(42)");
    }

    #[test]
    fn lance_response_display_rows_affected_zero() {
        let resp = LanceResponse::RowsAffected(0);
        assert_eq!(resp.to_string(), "RowsAffected(0)");
    }

    #[test]
    fn lance_response_serde_roundtrip_rows_affected() {
        let resp = LanceResponse::RowsAffected(1234);
        let json = serde_json::to_string(&resp).unwrap();
        let back: LanceResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{:?}", resp), format!("{:?}", back));
    }

    // =========================================================================
    // 11. PersistedTableConfig default values
    // =========================================================================

    #[test]
    fn persisted_table_config_default_values() {
        let cfg = PersistedTableConfig::default();
        assert!(cfg.seal_indices.is_empty());
        assert_eq!(cfg.s3_uri, None);
        assert_eq!(cfg.s3_max_rows_per_file, 5_000_000);
        assert_eq!(cfg.s3_max_rows_per_group, 50_000);
        assert_eq!(cfg.seal_max_age_ms, 60_000);
        assert_eq!(cfg.seal_max_size, 1024 * 1024 * 1024);
        assert_eq!(cfg.compaction_target_rows_per_fragment, 1_048_576);
        assert!(cfg.compaction_materialize_deletions);
        assert!((cfg.compaction_deletion_threshold - 0.1).abs() < f32::EPSILON);
        assert_eq!(cfg.compaction_min_fragments, 4);
        assert_eq!(cfg.batcher, None);
        assert_eq!(cfg.processor, None);
    }

    // =========================================================================
    // 12. PersistedTableEntry to_snapshot()
    // =========================================================================

    #[test]
    fn persisted_table_entry_to_snapshot() {
        let catalog = SegmentCatalog {
            active_segment: 3,
            sealed_segment: Some(2),
            s3_manifest_version: 7,
            s3_dataset_uri: "s3://test".to_string(),
            active_first_log_index: Some(50),
            sealed_first_log_index: Some(20),
        };
        let schema_ver = SchemaVersion {
            version: 1,
            schema_ipc: Bytes::from_static(&[0, 1, 2]),
            created_at_millis: 1700000000000,
        };
        let entry = PersistedTableEntry {
            config: PersistedTableConfig::default(),
            catalog: catalog.clone(),
            flush_state: FlushState::Idle,
            schema_history: vec![schema_ver.clone()],
        };

        let snap = entry.to_snapshot();
        assert_eq!(snap.catalog.active_segment, 3);
        assert_eq!(snap.catalog.sealed_segment, Some(2));
        assert_eq!(snap.catalog.s3_manifest_version, 7);
        assert!(matches!(snap.flush_state, FlushState::Idle));
        assert_eq!(snap.schema_history.len(), 1);
        assert_eq!(snap.schema_history[0].version, 1);
        assert_eq!(
            snap.schema_history[0].schema_ipc,
            Bytes::from_static(&[0, 1, 2])
        );
    }

    // =========================================================================
    // 13. PersistedTableEntry from_snapshot_with_defaults()
    // =========================================================================

    #[test]
    fn persisted_table_entry_from_snapshot_with_defaults() {
        let snapshot = TableSnapshot {
            catalog: SegmentCatalog {
                active_segment: 10,
                sealed_segment: None,
                s3_manifest_version: 5,
                s3_dataset_uri: "s3://bucket".to_string(),
                active_first_log_index: None,
                sealed_first_log_index: None,
            },
            flush_state: FlushState::Idle,
            schema_history: vec![],
        };

        let entry = PersistedTableEntry::from_snapshot_with_defaults(snapshot);
        // Config should be the default.
        assert_eq!(entry.config, PersistedTableConfig::default());
        // Catalog and flush_state should come from the snapshot.
        assert_eq!(entry.catalog.active_segment, 10);
        assert_eq!(entry.catalog.s3_manifest_version, 5);
        assert!(matches!(entry.flush_state, FlushState::Idle));
        assert!(entry.schema_history.is_empty());
    }

    // =========================================================================
    // 14. SnapshotData serialization roundtrip
    // =========================================================================

    #[test]
    fn snapshot_data_serde_roundtrip() {
        let mut tables = HashMap::new();
        tables.insert(
            "my_table".to_string(),
            PersistedTableEntry {
                config: PersistedTableConfig::default(),
                catalog: SegmentCatalog::default(),
                flush_state: FlushState::Idle,
                schema_history: vec![],
            },
        );

        let snap = SnapshotData {
            tables,
            min_safe_log_index: Some(42),
            file_manifest: vec![SnapshotFileEntry {
                relative_path: "tables/t/segments/1.lance/data/0.lance".to_string(),
                size: 4096,
            }],
            sync_addr: Some("127.0.0.1:9090".to_string()),
            catalog_meta: None,
        };

        let json = serde_json::to_string(&snap).unwrap();
        let back: SnapshotData = serde_json::from_str(&json).unwrap();

        assert_eq!(back.tables.len(), 1);
        assert!(back.tables.contains_key("my_table"));
        assert_eq!(back.min_safe_log_index, Some(42));
        assert_eq!(back.file_manifest.len(), 1);
        assert_eq!(
            back.file_manifest[0].relative_path,
            "tables/t/segments/1.lance/data/0.lance"
        );
        assert_eq!(back.file_manifest[0].size, 4096);
        assert_eq!(back.sync_addr, Some("127.0.0.1:9090".to_string()));
    }

    #[test]
    fn snapshot_data_deserialize_missing_optional_fields() {
        // file_manifest, sync_addr, and min_safe_log_index have #[serde(default)].
        let json = r#"{"tables":{}}"#;
        let snap: SnapshotData = serde_json::from_str(json).unwrap();
        assert!(snap.tables.is_empty());
        assert_eq!(snap.min_safe_log_index, None);
        assert!(snap.file_manifest.is_empty());
        assert_eq!(snap.sync_addr, None);
    }

    // =========================================================================
    // 15. SnapshotFileEntry serialization roundtrip
    // =========================================================================

    #[test]
    fn snapshot_file_entry_serde_roundtrip() {
        let entry = SnapshotFileEntry {
            relative_path: "segments/3.lance/data/1.lance".to_string(),
            size: 1_048_576,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: SnapshotFileEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.relative_path, entry.relative_path);
        assert_eq!(back.size, entry.size);
    }

    // =========================================================================
    // 16. parse_time_unit for all variants + unknown default
    // =========================================================================

    #[test]
    fn parse_time_unit_second() {
        assert_eq!(parse_time_unit("Second"), TimeUnit::Second);
    }

    #[test]
    fn parse_time_unit_millisecond() {
        assert_eq!(parse_time_unit("Millisecond"), TimeUnit::Millisecond);
    }

    #[test]
    fn parse_time_unit_microsecond() {
        assert_eq!(parse_time_unit("Microsecond"), TimeUnit::Microsecond);
    }

    #[test]
    fn parse_time_unit_nanosecond() {
        assert_eq!(parse_time_unit("Nanosecond"), TimeUnit::Nanosecond);
    }

    #[test]
    fn parse_time_unit_unknown_defaults_to_millisecond() {
        assert_eq!(parse_time_unit("bogus"), TimeUnit::Millisecond);
        assert_eq!(parse_time_unit(""), TimeUnit::Millisecond);
        assert_eq!(parse_time_unit("seconds"), TimeUnit::Millisecond);
    }

    // =========================================================================
    // 17. time_unit_to_string roundtrip for all variants
    // =========================================================================

    #[test]
    fn time_unit_to_string_roundtrip() {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let s = time_unit_to_string(unit);
            let back = parse_time_unit(&s);
            assert_eq!(unit, back, "roundtrip failed for {:?}", unit);
        }
    }

    #[test]
    fn time_unit_to_string_values() {
        assert_eq!(time_unit_to_string(TimeUnit::Second), "Second");
        assert_eq!(time_unit_to_string(TimeUnit::Millisecond), "Millisecond");
        assert_eq!(time_unit_to_string(TimeUnit::Microsecond), "Microsecond");
        assert_eq!(time_unit_to_string(TimeUnit::Nanosecond), "Nanosecond");
    }

    // =========================================================================
    // 18. duration_to_ms basic conversion
    // =========================================================================

    #[test]
    fn duration_to_ms_basic() {
        assert_eq!(duration_to_ms(Duration::from_millis(0)), 0);
        assert_eq!(duration_to_ms(Duration::from_millis(500)), 500);
        assert_eq!(duration_to_ms(Duration::from_secs(1)), 1_000);
        assert_eq!(duration_to_ms(Duration::from_secs(60)), 60_000);
    }

    #[test]
    fn duration_to_ms_sub_millisecond_truncates() {
        // 999 microseconds is less than 1 millisecond -> truncated to 0.
        assert_eq!(duration_to_ms(Duration::from_micros(999)), 0);
        // 1500 microseconds -> 1 millisecond.
        assert_eq!(duration_to_ms(Duration::from_micros(1500)), 1);
    }

    #[test]
    fn duration_to_ms_large_value_saturates() {
        // A very large duration should saturate at u64::MAX.
        let huge = Duration::from_secs(u64::MAX);
        let ms = duration_to_ms(huge);
        assert_eq!(ms, u64::MAX);
    }

    // =========================================================================
    // 19. WriteResult construction and field access
    // =========================================================================

    #[test]
    fn test_write_result_with_ok_response() {
        let wr = WriteResult {
            log_index: 1,
            response: LanceResponse::Ok,
        };
        assert_eq!(wr.log_index, 1);
        assert!(matches!(wr.response, LanceResponse::Ok));
    }

    #[test]
    fn test_write_result_with_rows_affected() {
        let wr = WriteResult {
            log_index: 99,
            response: LanceResponse::RowsAffected(42),
        };
        assert_eq!(wr.log_index, 99);
        match &wr.response {
            LanceResponse::RowsAffected(n) => assert_eq!(*n, 42),
            other => panic!("expected RowsAffected(42), got {:?}", other),
        }
    }

    #[test]
    fn test_write_result_with_error_response() {
        let wr = WriteResult {
            log_index: 7,
            response: LanceResponse::Error("something failed".to_string()),
        };
        assert_eq!(wr.log_index, 7);
        match &wr.response {
            LanceResponse::Error(msg) => assert_eq!(msg, "something failed"),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn test_write_result_debug_format() {
        let wr = WriteResult {
            log_index: 55,
            response: LanceResponse::RowsAffected(10),
        };
        let dbg = format!("{:?}", wr);
        assert!(
            dbg.contains("log_index: 55"),
            "Debug output should contain log_index: {:?}",
            dbg
        );
        assert!(
            dbg.contains("RowsAffected(10)"),
            "Debug output should contain RowsAffected(10): {:?}",
            dbg
        );
    }

    #[test]
    fn test_write_result_display() {
        // WriteResult derives Debug; verify the debug representation
        // includes both the log_index and response fields.
        let wr = WriteResult {
            log_index: 123,
            response: LanceResponse::Ok,
        };
        let dbg = format!("{:?}", wr);
        assert!(
            dbg.contains("123"),
            "Debug output should contain log_index value: {:?}",
            dbg
        );
        assert!(
            dbg.contains("Ok"),
            "Debug output should contain response variant: {:?}",
            dbg
        );
    }

    // =========================================================================
    // 20. TxnId
    // =========================================================================

    #[test]
    fn txn_id_new_is_unique() {
        let a = TxnId::new();
        let b = TxnId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn txn_id_display() {
        let id = TxnId(0x0123456789abcdef_fedcba9876543210);
        let s = format!("{}", id);
        assert_eq!(s, "0123456789abcdeffedcba9876543210");
    }

    #[test]
    fn txn_id_default_is_random() {
        let a = TxnId::default();
        let b = TxnId::default();
        assert_ne!(a, b);
    }

    #[test]
    fn txn_id_hash_works() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let id = TxnId::new();
        set.insert(id);
        assert!(set.contains(&id));
    }

    #[test]
    fn txn_id_serde_roundtrip() {
        let id = TxnId::new();
        let json = serde_json::to_string(&id).unwrap();
        let back: TxnId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    // =========================================================================
    // 21. TxnOp helpers
    // =========================================================================

    #[test]
    fn txn_op_table_name_all_variants() {
        let ops = vec![
            TxnOp::Append {
                table: "a".into(),
                data: Bytes::new(),
            },
            TxnOp::Delete {
                table: "b".into(),
                filter: String::new(),
            },
            TxnOp::Update {
                table: "c".into(),
                filter: String::new(),
                data: Bytes::new(),
            },
            TxnOp::CreateTable {
                table: "d".into(),
                schema_ipc: Bytes::new(),
            },
            TxnOp::DropTable { table: "e".into() },
        ];
        let names: Vec<&str> = ops.iter().map(|op| op.table_name()).collect();
        assert_eq!(names, vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn txn_op_payload_bytes_all_variants() {
        assert_eq!(
            TxnOp::Append {
                table: "t".into(),
                data: Bytes::from_static(&[1, 2, 3]),
            }
            .payload_bytes(),
            3
        );
        assert_eq!(
            TxnOp::Delete {
                table: "t".into(),
                filter: "id > 0".to_string(),
            }
            .payload_bytes(),
            6
        );
        assert_eq!(
            TxnOp::Update {
                table: "t".into(),
                filter: "x=1".to_string(),
                data: Bytes::from_static(&[1, 2]),
            }
            .payload_bytes(),
            5 // 3 (filter) + 2 (data)
        );
        assert_eq!(
            TxnOp::CreateTable {
                table: "t".into(),
                schema_ipc: Bytes::from_static(&[0xDE, 0xAD]),
            }
            .payload_bytes(),
            2
        );
        assert_eq!(TxnOp::DropTable { table: "t".into() }.payload_bytes(), 0);
    }

    // =========================================================================
    // 22. PendingTxns
    // =========================================================================

    fn small_txn_config() -> TxnConfig {
        TxnConfig {
            max_txn_bytes: 1024,
            max_total_pending_bytes: 2048,
            gc_log_index_timeout: 100,
            gc_check_interval: 10,
        }
    }

    #[test]
    fn pending_txns_insert_and_remove() {
        let mut pt = PendingTxns::default();
        let config = small_txn_config();
        let id = TxnId::new();

        let ops = vec![TxnOp::Append {
            table: "t".into(),
            data: Bytes::from(vec![0u8; 100]),
        }];

        pt.insert_chunk(id, 0, ops, 1, &config).unwrap();
        assert_eq!(pt.txns.len(), 1);
        assert_eq!(pt.total_bytes, 100);

        let pending = pt.remove(&id).unwrap();
        assert_eq!(pending.chunks.len(), 1);
        assert_eq!(pending.total_bytes, 100);
        assert_eq!(pending.first_log_index, 1);

        assert_eq!(pt.txns.len(), 0);
        assert_eq!(pt.total_bytes, 0);
    }

    #[test]
    fn pending_txns_multiple_chunks_same_txn() {
        let mut pt = PendingTxns::default();
        let config = small_txn_config();
        let id = TxnId::new();

        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 50]),
            }],
            10,
            &config,
        )
        .unwrap();

        pt.insert_chunk(
            id,
            1,
            vec![TxnOp::Delete {
                table: "t".into(),
                filter: "id > 0".to_string(),
            }],
            11,
            &config,
        )
        .unwrap();

        assert_eq!(pt.txns.len(), 1);
        let pending = pt.txns.get(&id).unwrap();
        assert_eq!(pending.chunks.len(), 2);
        assert_eq!(pending.first_log_index, 10); // first chunk's log index
        assert_eq!(pending.total_bytes, 56); // 50 + 6
        assert_eq!(pt.total_bytes, 56);
    }

    #[test]
    fn pending_txns_per_txn_cap() {
        let mut pt = PendingTxns::default();
        let config = TxnConfig {
            max_txn_bytes: 100,
            max_total_pending_bytes: 10_000,
            ..small_txn_config()
        };
        let id = TxnId::new();

        // First chunk fits.
        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 80]),
            }],
            1,
            &config,
        )
        .unwrap();

        // Second chunk exceeds per-txn limit.
        let err = pt
            .insert_chunk(
                id,
                1,
                vec![TxnOp::Append {
                    table: "t".into(),
                    data: Bytes::from(vec![0u8; 30]),
                }],
                2,
                &config,
            )
            .unwrap_err();

        assert!(err.contains("txn bytes"));
        assert!(err.contains("exceeds limit"));
    }

    #[test]
    fn pending_txns_global_cap() {
        let mut pt = PendingTxns::default();
        let config = TxnConfig {
            max_txn_bytes: 10_000,
            max_total_pending_bytes: 150,
            ..small_txn_config()
        };

        let id1 = TxnId::new();
        let id2 = TxnId::new();

        pt.insert_chunk(
            id1,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 100]),
            }],
            1,
            &config,
        )
        .unwrap();

        // Second txn exceeds global limit.
        let err = pt
            .insert_chunk(
                id2,
                0,
                vec![TxnOp::Append {
                    table: "t".into(),
                    data: Bytes::from(vec![0u8; 60]),
                }],
                2,
                &config,
            )
            .unwrap_err();

        assert!(err.contains("global pending bytes"));
        assert!(err.contains("exceeds limit"));
    }

    #[test]
    fn pending_txns_remove_nonexistent_returns_none() {
        let mut pt = PendingTxns::default();
        assert!(pt.remove(&TxnId::new()).is_none());
    }

    #[test]
    fn pending_txns_gc_evicts_old_transactions() {
        let mut pt = PendingTxns::default();
        let config = small_txn_config();
        let id_old = TxnId::new();
        let id_new = TxnId::new();

        // Old txn at log index 10.
        pt.insert_chunk(
            id_old,
            0,
            vec![TxnOp::DropTable { table: "t".into() }],
            10,
            &config,
        )
        .unwrap();

        // New txn at log index 200.
        pt.insert_chunk(
            id_new,
            0,
            vec![TxnOp::DropTable { table: "t".into() }],
            200,
            &config,
        )
        .unwrap();

        assert_eq!(pt.txns.len(), 2);

        // GC at log index 250 with timeout 100: old (250-10=240 > 100) evicted.
        let evicted = pt.gc(250, 100);
        assert_eq!(evicted, 1);
        assert_eq!(pt.txns.len(), 1);
        assert!(pt.txns.contains_key(&id_new));
        assert!(!pt.txns.contains_key(&id_old));
    }

    #[test]
    fn pending_txns_gc_keeps_recent() {
        let mut pt = PendingTxns::default();
        let config = small_txn_config();
        let id = TxnId::new();

        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::DropTable { table: "t".into() }],
            100,
            &config,
        )
        .unwrap();

        // GC at log index 150 with timeout 100: (150-100=50 <= 100) kept.
        let evicted = pt.gc(150, 100);
        assert_eq!(evicted, 0);
        assert_eq!(pt.txns.len(), 1);
    }

    #[test]
    fn pending_txns_gc_recalculates_total_bytes() {
        let mut pt = PendingTxns::default();
        let config = small_txn_config();

        let id1 = TxnId::new();
        let id2 = TxnId::new();

        pt.insert_chunk(
            id1,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 100]),
            }],
            10,
            &config,
        )
        .unwrap();

        pt.insert_chunk(
            id2,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 200]),
            }],
            500,
            &config,
        )
        .unwrap();

        assert_eq!(pt.total_bytes, 300);

        // Evict id1 (log 10), keep id2 (log 500).
        pt.gc(600, 100);
        assert_eq!(pt.total_bytes, 200);
    }

    #[test]
    fn pending_txns_clear() {
        let mut pt = PendingTxns::default();
        let config = small_txn_config();

        for _ in 0..5 {
            pt.insert_chunk(
                TxnId::new(),
                0,
                vec![TxnOp::Append {
                    table: "t".into(),
                    data: Bytes::from(vec![0u8; 50]),
                }],
                1,
                &config,
            )
            .unwrap();
        }

        assert_eq!(pt.txns.len(), 5);
        assert_eq!(pt.total_bytes, 250);

        pt.clear();
        assert_eq!(pt.txns.len(), 0);
        assert_eq!(pt.total_bytes, 0);
    }

    // =========================================================================
    // 22b. PendingTxns: chunk overwrite corrects memory accounting
    // =========================================================================

    #[test]
    fn pending_txns_overwrite_chunk_preserves_memory_accounting() {
        let mut pt = PendingTxns::default();
        let config = small_txn_config();
        let id = TxnId::new();

        // Insert chunk seq=0 with 100 bytes.
        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 100]),
            }],
            1,
            &config,
        )
        .unwrap();
        assert_eq!(pt.total_bytes, 100);
        assert_eq!(pt.txns[&id].total_bytes, 100);

        // Overwrite chunk seq=0 with 150 bytes.
        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 150]),
            }],
            2,
            &config,
        )
        .unwrap();
        // Should be 150, not 250 (old 100 subtracted).
        assert_eq!(pt.total_bytes, 150);
        assert_eq!(pt.txns[&id].total_bytes, 150);
        assert_eq!(pt.txns[&id].chunks.len(), 1);
    }

    #[test]
    fn pending_txns_overwrite_within_per_txn_cap() {
        let config = TxnConfig {
            max_txn_bytes: 500,
            max_total_pending_bytes: 10_000,
            ..small_txn_config()
        };
        let mut pt = PendingTxns::default();
        let id = TxnId::new();

        // Insert chunk seq=0 with 100 bytes.
        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 100]),
            }],
            1,
            &config,
        )
        .unwrap();
        assert_eq!(pt.txns[&id].total_bytes, 100);

        // Overwrite seq=0 with 200 bytes. Cap check: 100 + 200 = 300 <= 500.
        // After overwrite, net bytes = 200 (old 100 subtracted).
        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 200]),
            }],
            2,
            &config,
        )
        .unwrap();
        assert_eq!(pt.txns[&id].total_bytes, 200);
        assert_eq!(pt.total_bytes, 200);
    }

    #[test]
    fn pending_txns_overwrite_rejected_by_per_txn_cap() {
        // Cap check is conservative: old_bytes + new_bytes checked before subtraction.
        let config = TxnConfig {
            max_txn_bytes: 200,
            max_total_pending_bytes: 10_000,
            ..small_txn_config()
        };
        let mut pt = PendingTxns::default();
        let id = TxnId::new();

        pt.insert_chunk(
            id,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 100]),
            }],
            1,
            &config,
        )
        .unwrap();

        // Overwrite with 150 bytes. Cap check: 100 + 150 = 250 > 200.
        // Rejected because cap check is pre-subtraction.
        let err = pt
            .insert_chunk(
                id,
                0,
                vec![TxnOp::Append {
                    table: "t".into(),
                    data: Bytes::from(vec![0u8; 150]),
                }],
                2,
                &config,
            )
            .unwrap_err();
        assert!(err.contains("txn bytes"));
        // Original chunk should still be intact.
        assert_eq!(pt.txns[&id].total_bytes, 100);
        assert_eq!(pt.total_bytes, 100);
    }

    #[test]
    fn pending_txns_overwrite_global_cap_check_uses_net_bytes() {
        let config = TxnConfig {
            max_txn_bytes: 10_000,
            max_total_pending_bytes: 300,
            ..small_txn_config()
        };
        let mut pt = PendingTxns::default();

        // Txn1: 200 bytes.
        let id1 = TxnId::new();
        pt.insert_chunk(
            id1,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 200]),
            }],
            1,
            &config,
        )
        .unwrap();

        // Txn2: 50 bytes. Total = 250, under 300 cap.
        let id2 = TxnId::new();
        pt.insert_chunk(
            id2,
            0,
            vec![TxnOp::Append {
                table: "t".into(),
                data: Bytes::from(vec![0u8; 50]),
            }],
            2,
            &config,
        )
        .unwrap();
        assert_eq!(pt.total_bytes, 250);

        // Txn2: overwrite with 100 bytes. Net change: +50.
        // New total would be 200 + 100 = 300 (exactly at limit).
        // The check is `total_bytes + chunk_bytes > cap` which is
        // 250 + 100 = 350 > 300, so it should be rejected BEFORE the
        // overwrite subtraction. This is the current behavior.
        let err = pt
            .insert_chunk(
                id2,
                0,
                vec![TxnOp::Append {
                    table: "t".into(),
                    data: Bytes::from(vec![0u8; 100]),
                }],
                3,
                &config,
            )
            .unwrap_err();
        assert!(err.contains("global pending bytes"));
    }

    // =========================================================================
    // 23. TxnConfig defaults
    // =========================================================================

    #[test]
    fn txn_config_defaults() {
        let cfg = TxnConfig::default();
        assert_eq!(cfg.max_txn_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.max_total_pending_bytes, 1024 * 1024 * 1024);
        assert_eq!(cfg.gc_log_index_timeout, 100_000);
        assert_eq!(cfg.gc_check_interval, 1_000);
    }

    // =========================================================================
    // 24. LanceCommand Display for transaction variants
    // =========================================================================

    #[test]
    fn lance_command_display_txn_chunk() {
        let cmd = LanceCommand::TxnChunk {
            txn_id: TxnId(0x0123456789abcdef_fedcba9876543210),
            seq: 3,
            ops: vec![
                TxnOp::Append {
                    table: "t".into(),
                    data: Bytes::new(),
                },
                TxnOp::Delete {
                    table: "t".into(),
                    filter: String::new(),
                },
            ],
        };
        let s = cmd.to_string();
        assert!(s.contains("TxnChunk"));
        assert!(s.contains("seq=3"));
        assert!(s.contains("2 ops"));
    }

    #[test]
    fn lance_command_display_txn_commit() {
        let cmd = LanceCommand::TxnCommit {
            txn_id: TxnId(42),
            total_chunks: 5,
        };
        let s = cmd.to_string();
        assert!(s.contains("TxnCommit"));
        assert!(s.contains("total_chunks=5"));
    }

    #[test]
    fn lance_command_display_txn_abort() {
        let cmd = LanceCommand::TxnAbort { txn_id: TxnId(42) };
        let s = cmd.to_string();
        assert!(s.contains("TxnAbort"));
    }

    #[test]
    fn lance_command_serde_roundtrip_txn_chunk() {
        assert_lance_command_roundtrip(&LanceCommand::TxnChunk {
            txn_id: TxnId::new(),
            seq: 7,
            ops: vec![
                TxnOp::Append {
                    table: "orders".into(),
                    data: Bytes::from_static(&[1, 2, 3]),
                },
                TxnOp::Delete {
                    table: "inventory".into(),
                    filter: "product_id = 42".to_string(),
                },
            ],
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_txn_commit() {
        assert_lance_command_roundtrip(&LanceCommand::TxnCommit {
            txn_id: TxnId::new(),
            total_chunks: 3,
        });
    }

    #[test]
    fn lance_command_serde_roundtrip_txn_abort() {
        assert_lance_command_roundtrip(&LanceCommand::TxnAbort {
            txn_id: TxnId::new(),
        });
    }
}
