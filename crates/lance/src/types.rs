use std::collections::HashMap;
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

/// Raft log entry command type for bisque-lance.
///
/// Each variant is replicated via Raft and applied to all nodes' state machines.
/// All variants include a `table_name` for multi-table routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LanceCommand {
    /// Create a new table with the given schema (Arrow IPC-encoded).
    CreateTable {
        table_name: String,
        schema_ipc: Bytes,
    },

    /// Drop a table and remove all its data.
    DropTable { table_name: String },

    /// Replicate raw data to all nodes.
    /// Every node appends this data to its local active Lance dataset.
    /// `data` contains Arrow IPC-encoded RecordBatches.
    AppendRecords { table_name: String, data: Bytes },

    /// Seal the current active segment and create a new one.
    SealActiveSegment {
        table_name: String,
        sealed_segment_id: SegmentId,
        new_active_segment_id: SegmentId,
        reason: SealReason,
    },

    /// Mark the start of a flush operation (leader only writes to S3).
    BeginFlush {
        table_name: String,
        segment_id: SegmentId,
    },

    /// Promote sealed segment data to S3 deep storage.
    /// Applied after successful S3 manifest commit.
    PromoteToDeepStorage {
        table_name: String,
        segment_id: SegmentId,
        s3_manifest_version: u64,
    },

    /// Register a new client session for version pinning (replicated cluster-wide).
    RegisterSession { session_id: u64 },

    /// Pin a dataset version so compaction doesn't delete it.
    PinVersion {
        session_id: u64,
        table_name: String,
        tier: String,
        version: u64,
    },

    /// Release a previously pinned version.
    UnpinVersion {
        session_id: u64,
        table_name: String,
        tier: String,
        version: u64,
    },

    /// Expire/remove a client session, releasing all its pins.
    ExpireSession { session_id: u64 },
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
}

impl fmt::Display for LanceResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LanceResponse::Ok => write!(f, "OK"),
            LanceResponse::Error(e) => write!(f, "Error: {}", e),
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
    pub table_name: String,
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
    pub schema_ipc: Vec<u8>,
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
