use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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
}

impl Default for SegmentCatalog {
    fn default() -> Self {
        Self {
            active_segment: 1,
            sealed_segment: None,
            s3_manifest_version: 0,
            s3_dataset_uri: String::new(),
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
        schema_ipc: Vec<u8>,
    },

    /// Drop a table and remove all its data.
    DropTable { table_name: String },

    /// Replicate raw data to all nodes.
    /// Every node appends this data to its local active Lance dataset.
    /// `data` contains Arrow IPC-encoded RecordBatches.
    AppendRecords {
        table_name: String,
        data: Vec<u8>,
    },

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
}

impl fmt::Display for LanceCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LanceCommand::CreateTable { table_name, schema_ipc } => {
                write!(f, "CreateTable(table={}, schema={} bytes)", table_name, schema_ipc.len())
            }
            LanceCommand::DropTable { table_name } => {
                write!(f, "DropTable(table={})", table_name)
            }
            LanceCommand::AppendRecords { table_name, data } => {
                write!(f, "AppendRecords(table={}, {} bytes)", table_name, data.len())
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
            LanceCommand::BeginFlush { table_name, segment_id } => {
                write!(f, "BeginFlush(table={}, segment={})", table_name, segment_id)
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
    pub tables: HashMap<String, TableSnapshot>,
}
