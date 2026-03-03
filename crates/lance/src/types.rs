use serde::{Deserialize, Serialize};
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

/// Tracks which segments are active, sealed, and the S3 deep storage version.
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
/// `AppendRecords` carries Arrow IPC-encoded bytes to avoid custom Arrow serde.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LanceCommand {
    /// Replicate raw data to all nodes.
    /// Every node appends this data to its local active Lance dataset.
    /// `data` contains Arrow IPC-encoded RecordBatches.
    AppendRecords { data: Vec<u8> },

    /// Seal the current active segment and create a new one.
    SealActiveSegment {
        sealed_segment_id: SegmentId,
        new_active_segment_id: SegmentId,
        reason: SealReason,
    },

    /// Mark the start of a flush operation (leader only writes to S3).
    BeginFlush { segment_id: SegmentId },

    /// Promote sealed segment data to S3 deep storage.
    /// Applied after successful S3 manifest commit.
    PromoteToDeepStorage {
        segment_id: SegmentId,
        s3_manifest_version: u64,
    },
}

impl fmt::Display for LanceCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LanceCommand::AppendRecords { data } => {
                write!(f, "AppendRecords({} bytes)", data.len())
            }
            LanceCommand::SealActiveSegment {
                sealed_segment_id,
                new_active_segment_id,
                reason,
            } => write!(
                f,
                "SealActiveSegment(sealed={}, new={}, reason={})",
                sealed_segment_id, new_active_segment_id, reason
            ),
            LanceCommand::BeginFlush { segment_id } => {
                write!(f, "BeginFlush(segment={})", segment_id)
            }
            LanceCommand::PromoteToDeepStorage {
                segment_id,
                s3_manifest_version,
            } => write!(
                f,
                "PromoteToDeepStorage(segment={}, version={})",
                segment_id, s3_manifest_version
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

/// Snapshot payload for the state machine.
/// Contains only metadata — actual data lives in Lance datasets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub catalog: SegmentCatalog,
    pub flush_state: FlushState,
}
