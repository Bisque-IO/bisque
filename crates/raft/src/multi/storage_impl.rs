//! Unified Log Storage for Multi-Raft
//!
//! Provides per-group sharded, segmented storage with:
//! - One dedicated shard per raft group (1:1 mapping)
//! - Dynamic shard creation on first access
//! - Segmented log files (default 64MB per segment)
//! - No mutex contention - writes serialized through channels per shard
//! - CRC64-NVME checksums for crash recovery
//!
//! Each raft group has independent lifecycle (compaction, replication).
//!
//! ## Log Entry Format
//!
//! Each log record has the following format:
//! ```text
//! +----------+----------+----------+-------------+----------+
//! | len (4B) | type (1B)| group_id | payload     | crc64    |
//! |  u32 LE  |   u8     |  u64 LE  | (variable)  | (8B) LE  |
//! +----------+----------+----------+-------------+----------+
//! ```
//!
//! ## Record Types
//!
//! - **Raft records**: Vote, Entry, Truncate, Purge
//!
//! ## Segment File Naming
//!
//! Segments are named: `group-{group_id}-{segment_id}.log`
//! Each group has its own directory: `{base_dir}/{group_id}/`
//!
//! ## Recovery Protocol
//!
//! On startup/first access to a group:
//! 1. Lists all segment files for the group
//! 2. Sorts by segment_id to process in order
//! 3. Reads each segment, validating CRC64 for each record
//! 4. Stops at first invalid record (partial write from crash)
//! 5. Truncates the file at that point
//! Rebuilds in-memory state from valid records

use crate::multi::codec::{Decode, FromCodec};
use crate::multi::codec::{
    Encode, Entry as CodecEntry, LogId as CodecLogId, RawBytes, ToCodec, Vote as CodecVote,
};
use congee::U64Congee;
use congee::epoch;
use crc64fast_nvme::Digest;
use crossfire::{AsyncRx, MAsyncTx, TryRecvError, mpsc::Array};
use dashmap::DashMap;
use openraft::{
    LogId, LogState, RaftTypeConfig,
    storage::{IOFlushed, RaftLogReader, RaftLogStorage},
};
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::manifest_mdbx::{ManifestManager, SegmentMeta};
use super::segment_footer::{SegmentFooter, SegmentFooterTracker, read_footer_from_segment};
use parking_lot::RwLock;
use tokio::sync::oneshot;

/// Log record types for Raft storage
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Vote = 0x01,
    Entry = 0x02,
    Truncate = 0x03,
    Purge = 0x04,
}

impl TryFrom<u8> for RecordType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(RecordType::Vote),
            0x02 => Ok(RecordType::Entry),
            0x03 => Ok(RecordType::Truncate),
            0x04 => Ok(RecordType::Purge),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid record type: {:#x}", value),
            )),
        }
    }
}

/// Length field size: 4 bytes
pub(crate) const LENGTH_SIZE: usize = 4;
/// CRC64 size: 8 bytes
pub(crate) const CRC64_SIZE: usize = 8;
/// Group ID size: 3 bytes (u24 little-endian)
pub(crate) const GROUP_ID_SIZE: usize = 3;
/// Minimum record size: len(4) + type(1) + group_id(3) + crc(8) = 16 bytes
#[allow(dead_code)]
pub(crate) const MIN_RECORD_SIZE: usize = LENGTH_SIZE + 1 + GROUP_ID_SIZE + CRC64_SIZE;
/// Default segment size: 1MB
pub const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024;
/// Default streaming chunk size: 1MB (should be aligned and large enough for efficiency)
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Header size: len(4) + type(1) + group_id(3) = 8 bytes (64-bit aligned)
pub(crate) const HEADER_SIZE: usize = LENGTH_SIZE + 1 + GROUP_ID_SIZE;

/// Default maximum record size: 1MB
pub const DEFAULT_MAX_RECORD_SIZE: u64 = 1024 * 1024;

/// Write a u24 little-endian value to the buffer at the given offset.
#[inline]
pub(crate) fn write_u24_le(buf: &mut [u8], offset: usize, value: u64) {
    debug_assert!(value <= 0xFF_FFFF, "group_id exceeds u24 range");
    buf[offset] = value as u8;
    buf[offset + 1] = (value >> 8) as u8;
    buf[offset + 2] = (value >> 16) as u8;
}

/// Read a u24 little-endian value from the buffer at the given offset.
#[inline]
pub(crate) fn read_u24_le(buf: &[u8], offset: usize) -> u64 {
    let lo = buf[offset] as u64;
    let mid = (buf[offset + 1] as u64) << 8;
    let high = (buf[offset + 2] as u64) << 16;
    lo | mid | high
}

async fn read_exact_at_reuse(
    file: &mut File,
    offset: u64,
    len: usize,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    if len == 0 {
        buf.clear();
        return Ok(());
    }

    buf.resize(len, 0);
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.read_exact(&mut buf[..len]).await?;
    Ok(())
}

async fn open_segment_file(
    path: &Path,
    read: bool,
    write: bool,
    create: bool,
    truncate: bool,
) -> io::Result<File> {
    OpenOptions::new()
        .read(read)
        .write(write)
        .create(create)
        .truncate(truncate)
        .open(path)
        .await
}

// Avoid allocating for zero padding during segment tail alignment.
static ZERO_4K: [u8; 4096] = [0u8; 4096];

#[inline]
fn dio_align() -> u64 {
    #[cfg(any(target_os = "linux", windows))]
    {
        4096
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        1
    }
}

#[inline]
fn align_up_u64(v: u64, align: u64) -> u64 {
    if align <= 1 {
        return v;
    }
    ((v + align - 1) / align) * align
}

/// Encodes a log record with CRC64 checksum into the provided buffer.
/// Format: [len: u32][type: u8][group_id: u64][payload...][crc64: u64]
///
/// The buffer must have at least `HEADER_SIZE + payload.len() + CRC64_SIZE` capacity.
/// Returns the total number of bytes written.
fn encode_record_into(
    buf: &mut Vec<u8>,
    record_type: RecordType,
    group_id: u64,
    payload: &[u8],
) -> usize {
    buf.clear();
    append_record_into(buf, record_type, group_id, payload)
}

/// Appends a log record to `buf` (does NOT clear first).
/// Returns the number of bytes appended.
pub(crate) fn append_record_into(
    buf: &mut Vec<u8>,
    record_type: RecordType,
    group_id: u64,
    payload: &[u8],
) -> usize {
    // Total record size (excluding the len field itself)
    let record_len = 1 + GROUP_ID_SIZE + payload.len() + CRC64_SIZE; // type + group_id + payload + crc
    let total_size = LENGTH_SIZE + record_len;

    buf.reserve(total_size);

    // Build header: [len][type][group_id]
    let mut header: [u8; HEADER_SIZE] = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&(record_len as u32).to_le_bytes());
    header[4] = record_type as u8;
    write_u24_le(&mut header, 5, group_id);

    // Compute CRC64 incrementally over [type + group_id + payload]
    let mut digest = Digest::new();
    digest.write(&header[LENGTH_SIZE..]); // type + group_id (4 bytes)
    digest.write(payload);
    let crc = digest.sum64();

    // Append everything to buffer
    buf.extend_from_slice(&header);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&crc.to_le_bytes());

    total_size
}

/// Encodes a log record with CRC64 checksum (allocating version for tests/convenience)
/// Format: [len: u32][type: u8][group_id: u64][payload...][crc64: u64]
#[cfg(test)]
fn encode_record(record_type: RecordType, group_id: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_record_into(&mut buf, record_type, group_id, payload);
    buf
}

/// A parsed record from the log
#[derive(Debug)]
pub struct ParsedRecord<'a> {
    pub record_type: RecordType,
    pub group_id: u64,
    pub payload: &'a [u8],
}

/// Validates a record's CRC64 checksum
/// Input is the data AFTER the length field (type + group_id + payload + crc)
/// Returns (record_type, group_id, payload) if valid
pub(crate) fn validate_record(data: &[u8], max_record_size: usize) -> io::Result<ParsedRecord<'_>> {
    // Minimum: type(1) + group_id(3) + crc(8) = 12 bytes
    if data.len() < 1 + GROUP_ID_SIZE + CRC64_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record too short",
        ));
    }

    if data.len() > max_record_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "record exceeds max_record_size: {} > {}",
                data.len(),
                max_record_size
            ),
        ));
    }

    let payload_end = data.len() - CRC64_SIZE;
    let checksummed_data = &data[..payload_end];
    let stored_crc = u64::from_le_bytes(data[payload_end..].try_into().unwrap());

    // Verify CRC
    let mut digest = Digest::new();
    digest.write(checksummed_data);
    let computed_crc = digest.sum64();

    if computed_crc != stored_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "CRC mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            ),
        ));
    }

    // Parse header
    let record_type = RecordType::try_from(data[0])?;
    let group_id = read_u24_le(data, 1);
    let payload = &data[1 + GROUP_ID_SIZE..payload_end];

    Ok(ParsedRecord {
        record_type,
        group_id,
        payload,
    })
}

/// Configuration for per-group log storage.
/// Uses Arc<PathBuf> for cheap cloning of config across groups.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Base directory for all raft data
    pub base_dir: Arc<PathBuf>,
    /// Optional manifest directory. If None, manifest is stored in base_dir/.raft_manifest
    /// Use this if base_dir is on a filesystem that doesn't support Direct I/O (e.g., tmpfs)
    pub manifest_dir: Option<Arc<PathBuf>>,
    /// Maximum segment size in bytes. When exceeded, a new segment is created.
    /// Default: 1MB, minimum: MIN_RECORD_SIZE as u64
    pub segment_size: u64,
    /// Maximum size for individual records. None means no limit (but constrained by segment_size).
    /// Default: 1MB, validated to be <= segment_size
    pub max_record_size: Option<u64>,
    /// Interval for fsync per group. If None, fsync after every write.
    /// If Some(duration), fsync on interval only if dirty.
    pub fsync_interval: Option<Duration>,
    /// Maximum entries to keep in memory cache per group
    pub max_cache_entries_per_group: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            base_dir: Arc::new(PathBuf::from("./raft-data")),
            manifest_dir: None,
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_record_size: Some(DEFAULT_MAX_RECORD_SIZE),
            fsync_interval: Some(Duration::from_millis(100)),
            max_cache_entries_per_group: 10000,
        }
    }
}

impl StorageConfig {
    /// Create a new StorageConfig with the given base directory
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: Arc::new(base_dir.into()),
            ..Default::default()
        }
    }

    /// Set the segment size
    /// Validates that segment_size >= MIN_RECORD_SIZE
    pub fn with_segment_size(mut self, size: u64) -> Self {
        assert!(
            size >= MIN_RECORD_SIZE as u64,
            "segment_size must be at least MIN_RECORD_SIZE ({})",
            MIN_RECORD_SIZE
        );

        // Ensure segment_size is aligned for Direct I/O (must be multiple of 512 or 4096)
        let aligned_size = align_up_u64(size, dio_align());
        if aligned_size != size {
            tracing::warn!(
                "segment_size {} is not aligned to {} bytes for Direct I/O, rounding up to {}",
                size,
                dio_align(),
                aligned_size
            );
        }
        self.segment_size = aligned_size;

        // Validate max_record_size if set
        if let Some(max_record) = self.max_record_size {
            assert!(
                max_record <= aligned_size,
                "max_record_size ({}) must be <= segment_size ({})",
                max_record,
                aligned_size
            );
        }
        self
    }

    /// Set the maximum record size
    /// None means no explicit limit (but still constrained by segment_size)
    /// Validates that max_record_size <= segment_size
    pub fn with_max_record_size(mut self, max_size: impl Into<Option<u64>>) -> Self {
        let max_size_opt = max_size.into();
        if let Some(max_size) = max_size_opt {
            assert!(
                max_size <= self.segment_size,
                "max_record_size ({}) must be <= segment_size ({})",
                max_size,
                self.segment_size
            );
            assert!(
                max_size >= MIN_RECORD_SIZE as u64,
                "max_record_size must be at least MIN_RECORD_SIZE ({})",
                MIN_RECORD_SIZE
            );
        }
        self.max_record_size = max_size_opt;
        self
    }

    /// Set the fsync interval
    pub fn with_fsync_interval(mut self, interval: Option<Duration>) -> Self {
        self.fsync_interval = interval;
        self
    }

    /// Set the maximum cache entries per group
    pub fn with_max_cache_entries(mut self, max_entries: usize) -> Self {
        self.max_cache_entries_per_group = max_entries;
        self
    }

    /// Set the manifest directory. Use this if the base_dir is on a filesystem
    /// that doesn't support Direct I/O (e.g., tmpfs). If not set, manifest is
    /// stored in base_dir/.raft_manifest
    pub fn with_manifest_dir(mut self, manifest_dir: impl AsRef<Path>) -> Self {
        self.manifest_dir = Some(Arc::new(manifest_dir.as_ref().to_path_buf()));
        self
    }
}

/// Legacy config alias - maps to new StorageConfig
/// The `num_shards` field is ignored (each group gets its own shard)
#[derive(Debug, Clone)]
pub struct ShardedStorageConfig {
    pub base_dir: PathBuf,
    /// Ignored - each group gets its own shard now
    pub num_shards: usize,
    pub segment_size: u64,
    pub max_record_size: Option<u64>,
    pub fsync_interval: Option<Duration>,
    pub max_cache_entries_per_group: usize,
}

impl Default for ShardedStorageConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("./raft-data"),
            num_shards: 8, // Default value, not used
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_record_size: Some(DEFAULT_MAX_RECORD_SIZE),
            fsync_interval: Some(Duration::from_millis(100)),
            max_cache_entries_per_group: 10000,
        }
    }
}

impl From<ShardedStorageConfig> for StorageConfig {
    fn from(sharded: ShardedStorageConfig) -> Self {
        Self {
            base_dir: Arc::new(sharded.base_dir),
            manifest_dir: None,
            segment_size: sharded.segment_size,
            max_record_size: sharded.max_record_size,
            fsync_interval: sharded.fsync_interval,
            max_cache_entries_per_group: sharded.max_cache_entries_per_group,
        }
    }
}

// Keep the old config as an alias for compatibility
pub type MultiplexedStorageConfig = ShardedStorageConfig;

/// Per-group state within the storage
struct GroupState<C: RaftTypeConfig> {
    /// In-memory log cache: index -> entry
    cache: DashMap<u64, Arc<C::Entry>>,
    /// On-disk log index: entry index -> segment location.
    /// Implemented with congee (u64 -> slot) + side vector for locations.
    log_index: LogIndex,
    /// Current vote for this group (atomic)
    vote: AtomicVote,
    /// Log state
    first_index: AtomicU64,
    last_index: AtomicU64,
    /// Lower bound (inclusive) of the cache window.
    /// Entries with index < cache_low may be evicted from `cache`.
    cache_low: AtomicU64,
    last_log_id: AtomicLogId,
    /// Last purged log id (for log compaction tracking)
    last_purged_log_id: AtomicLogId,
    /// The shard for this group
    shard: Arc<ShardState<C>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LogLocation {
    pub(crate) segment_id: u64,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

pub(crate) struct LogIndex {
    map: U64Congee<usize>,
    locations: RwLock<Vec<LogLocation>>,
    free: RwLock<Vec<usize>>,
}

impl LogIndex {
    pub(crate) fn new() -> Self {
        Self {
            map: U64Congee::<usize>::new(),
            locations: RwLock::new(Vec::new()),
            free: RwLock::new(Vec::new()),
        }
    }

    pub(crate) fn get(&self, key: u64) -> Option<LogLocation> {
        let guard = epoch::pin();
        let slot = self.map.get(key, &guard)?;
        let locs = self.locations.read();
        locs.get(slot).copied()
    }

    pub(crate) fn insert(&self, key: u64, loc: LogLocation) -> io::Result<()> {
        let guard = epoch::pin();

        let slot = if let Some(slot) = self.free.write().pop() {
            let mut locs = self.locations.write();
            if slot >= locs.len() {
                locs.resize(
                    slot + 1,
                    LogLocation {
                        segment_id: 0,
                        offset: 0,
                        len: 0,
                    },
                );
            }
            locs[slot] = loc;
            slot
        } else {
            let mut locs = self.locations.write();
            let slot = locs.len();
            locs.push(loc);
            slot
        };

        self.map
            .insert(key, slot, &guard)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    #[cfg(test)]
    fn remove(&self, key: u64) {
        let guard = epoch::pin();
        if let Some(slot) = self.map.remove(key, &guard) {
            self.free.write().push(slot);
        }
    }

    pub(crate) fn truncate_from(&self, first_removed: u64) {
        // Remove keys in [first_removed, u64::MAX] in batches.
        let guard = epoch::pin();
        let mut buf: Vec<([u8; 8], usize)> = vec![([0u8; 8], 0usize); 256];

        loop {
            let n = self
                .map
                .range(first_removed, u64::MAX, &mut buf[..], &guard);
            if n == 0 {
                break;
            }

            for i in 0..n {
                let key = u64::from_be_bytes(buf[i].0);
                if key < first_removed {
                    continue;
                }
                if let Some(slot) = self.map.remove(key, &guard) {
                    self.free.write().push(slot);
                }
            }
        }
    }

    pub(crate) fn purge_to(&self, last_removed: u64) {
        // Remove keys in [0, last_removed] in batches.
        let guard = epoch::pin();
        let mut buf: Vec<([u8; 8], usize)> = vec![([0u8; 8], 0usize); 256];

        let end = last_removed.saturating_add(1);
        loop {
            let n = self.map.range(0, end, &mut buf[..], &guard);
            if n == 0 {
                break;
            }

            for i in 0..n {
                let key = u64::from_be_bytes(buf[i].0);
                if key > last_removed {
                    continue;
                }
                if let Some(slot) = self.map.remove(key, &guard) {
                    self.free.write().push(slot);
                }
            }
        }
    }
}

/// Owned parsed record to avoid lifetime issues
#[derive(Debug)]
pub(crate) struct OwnedParsedRecord {
    pub record_type: RecordType,
    pub group_id: u64,
    pub payload: Vec<u8>,
}

/// Iterator for reading records from a segment file.
/// Reads aligned chunks and yields parsed records with proper boundary handling.
struct RecordIterator<'a> {
    file: &'a mut File,
    offset: u64,
    end_offset: u64,
    chunk_size: usize,
    buffer: Vec<u8>,
    carry: Vec<u8>,
    position: usize,
}

impl<'a> RecordIterator<'a> {
    /// Creates a new RecordIterator
    pub fn new(file: &'a mut File, start_offset: u64, end_offset: u64, chunk_size: usize) -> Self {
        Self {
            file,
            offset: start_offset,
            end_offset,
            chunk_size,
            buffer: Vec::new(),
            carry: Vec::new(),
            position: 0,
        }
    }

    /// Reads the next chunk from the file, handling alignment requirements
    pub async fn read_chunk(&mut self) -> io::Result<bool> {
        if self.offset >= self.end_offset {
            return Ok(false);
        }

        // Calculate aligned read range
        let remaining = (self.end_offset - self.offset) as usize;
        let to_read = remaining.min(self.chunk_size);

        // Read the chunk with alignment safety
        read_exact_at_reuse(self.file, self.offset, to_read, &mut self.buffer).await?;

        // Prepend any carried-over data from previous chunk
        if !self.carry.is_empty() {
            self.carry.extend_from_slice(&self.buffer);
            std::mem::swap(&mut self.carry, &mut self.buffer);
            self.carry.clear();
        }

        self.position = 0;
        self.offset += to_read as u64;
        Ok(true)
    }

    /// Returns the next record from the current chunk, or None if more data is needed
    pub async fn next_record(&mut self) -> io::Result<Option<(u64, OwnedParsedRecord)>> {
        loop {
            if self.position + LENGTH_SIZE > self.buffer.len() {
                // Not enough data for length prefix
                return Ok(None);
            }

            // Read length prefix
            let len_bytes: [u8; LENGTH_SIZE] = self.buffer
                [self.position..self.position + LENGTH_SIZE]
                .try_into()
                .unwrap();
            let total_len = u32::from_le_bytes(len_bytes) as usize;

            // Validate length using configurable max_record_size (fallback to chunk_size if not configured)
            if total_len < MIN_RECORD_SIZE - LENGTH_SIZE || total_len > self.chunk_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid record length: {}", total_len),
                ));
            }

            let record_start = self.position + LENGTH_SIZE;
            let record_end = record_start + total_len;

            if record_end > self.buffer.len() {
                // Record spans chunk boundary - carry over to next read
                self.carry = self.buffer[self.position..].to_vec();
                self.position = self.buffer.len(); // Mark buffer as consumed

                if !self.read_chunk().await? {
                    return Ok(None);
                }
                continue;
            }

            // Parse the record
            let record_data = &self.buffer[record_start..record_end];
            let record_offset = self.offset - self.buffer.len() as u64 + self.position as u64;

            let parsed = validate_record(record_data, self.chunk_size)?;

            self.position = record_end;

            // Convert to owned record to avoid lifetime issues
            let owned_record = OwnedParsedRecord {
                record_type: parsed.record_type,
                group_id: parsed.group_id,
                payload: record_data[1 + GROUP_ID_SIZE..record_data.len() - CRC64_SIZE].to_vec(),
            };

            return Ok(Some((record_offset, owned_record)));
        }
    }
}

struct RecoveredGroup<C: RaftTypeConfig> {
    vote: Option<openraft::impls::Vote<C>>,
    last_log_id: Option<LogId<C>>,
    last_purged_log_id: Option<LogId<C>>,
    first_index: u64,
    last_index: u64,
    log_index: LogIndex,
    cache: Vec<(u64, Arc<C::Entry>)>,
}

async fn replay_group_from_manifest<C>(
    group_dir: &PathBuf,
    group_id: u64,
    manifest: &[(u64, u64)],
    max_cache_entries: usize,
) -> io::Result<RecoveredGroup<C>>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            Vote = openraft::impls::Vote<C>,
            Node = openraft::impls::BasicNode,
            Entry = openraft::impls::Entry<C>,
        >,
    C::D: FromCodec<RawBytes>,
{
    let mut vote: Option<openraft::impls::Vote<C>> = None;
    let mut last_purged_log_id: Option<LogId<C>> = None;

    let log_index = LogIndex::new();
    let mut last_index: u64 = 0;
    let mut first_index: u64 = 0;
    let mut last_log_id_hint: Option<LogId<C>> = None;
    let mut cache: Vec<(u64, Arc<C::Entry>)> = Vec::new();
    let mut cache_low: u64 = 0;

    let mut read_buf: Vec<u8> = Vec::with_capacity(4096);

    let max_record_size = manifest
        .get(0)
        .and_then(|m| m.1.checked_sub(1).map(|i| i as usize))
        .unwrap_or(DEFAULT_CHUNK_SIZE);

    for (segment_id, valid_bytes) in manifest.iter().copied() {
        if valid_bytes == 0 {
            continue;
        }

        let path = SegmentedLog::segment_path(group_dir, group_id, segment_id);
        let mut file = open_segment_file(&path, true, false, false, false).await?;

        let mut record_iter = RecordIterator::new(&mut file, 0u64, valid_bytes, DEFAULT_CHUNK_SIZE);

        record_iter.read_chunk().await?;

        while let Some((record_offset, parsed)) = record_iter.next_record().await? {
            if parsed.group_id != group_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "group_id mismatch in segment {}: expected {}, got {}",
                        segment_id, group_id, parsed.group_id
                    ),
                ));
            }

            match parsed.record_type {
                RecordType::Vote => {
                    let codec_vote = CodecVote::decode_from_slice(&parsed.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    vote = Some(openraft::impls::Vote::<C>::from_codec(codec_vote));
                }
                RecordType::Entry => {
                    let codec_entry = CodecEntry::<RawBytes>::decode_from_slice(&parsed.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    let entry: openraft::impls::Entry<C> =
                        openraft::impls::Entry::<C>::from_codec(codec_entry);
                    let log_id = entry.log_id;
                    let index = log_id.index;

                    log_index.insert(
                        index,
                        LogLocation {
                            segment_id,
                            offset: record_offset,
                            len: (LENGTH_SIZE
                                + 1
                                + GROUP_ID_SIZE
                                + parsed.payload.len()
                                + CRC64_SIZE) as u32,
                        },
                    )?;

                    last_index = last_index.max(index);
                    last_log_id_hint = Some(log_id);

                    if max_cache_entries > 0 {
                        let keep = max_cache_entries as u64;
                        cache_low = last_index
                            .saturating_sub(keep.saturating_sub(1))
                            .max(first_index);
                        if index >= cache_low {
                            cache.push((index, Arc::new(entry)));
                            cache.retain(|(i, _)| *i >= cache_low);
                        }
                    }
                }
                RecordType::Truncate => {
                    let (truncate_index, truncate_log_id) =
                        match CodecLogId::decode_from_slice(&parsed.payload) {
                            Ok(codec_lid) => {
                                let lid = LogId::<C>::from_codec(codec_lid);
                                (lid.index, Some(lid))
                            }
                            Err(_) if parsed.payload.len() == 8 => {
                                let idx = u64::from_le_bytes(parsed.payload.try_into().unwrap());
                                (idx, None)
                            }
                            Err(e) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    e.to_string(),
                                ));
                            }
                        };

                    let split_key = truncate_index.saturating_add(1);
                    log_index.truncate_from(split_key);
                    cache.retain(|(i, _)| *i <= truncate_index);
                    last_index = truncate_index;
                    if let Some(lid) = truncate_log_id {
                        last_log_id_hint = Some(lid);
                    } else {
                        last_log_id_hint = None;
                    }
                }
                RecordType::Purge => {
                    let codec_lid = CodecLogId::decode_from_slice(&parsed.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    let lid = LogId::<C>::from_codec(codec_lid);
                    let purge_index = lid.index;
                    last_purged_log_id = Some(lid);

                    log_index.purge_to(purge_index);
                    first_index = purge_index.saturating_add(1);
                    cache.retain(|(i, _)| *i >= first_index);
                }
            }
        }
    }

    // Determine first index from purge marker (if present).
    if let Some(ref p) = last_purged_log_id {
        first_index = first_index.max(p.index.saturating_add(1));
    }

    // If we don't have a reliable last_log_id hint (e.g. old truncate format),
    // try to read the last entry's LogId from disk using the index map.
    let last_log_id = if let Some(lid) = last_log_id_hint {
        Some(lid)
    } else if last_index > 0 {
        if let Some(loc) = log_index.get(last_index) {
            let path = SegmentedLog::segment_path(group_dir, group_id, loc.segment_id);
            let mut file = open_segment_file(&path, true, false, false, false).await?;
            read_exact_at_reuse(&mut file, loc.offset, loc.len as usize, &mut read_buf).await?;
            let buf = &read_buf[..loc.len as usize];
            let parsed = validate_record(&buf[LENGTH_SIZE..loc.len as usize], max_record_size)?;
            if parsed.record_type == RecordType::Entry {
                let codec_entry = CodecEntry::<RawBytes>::decode_from_slice(parsed.payload)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                let entry: openraft::impls::Entry<C> =
                    openraft::impls::Entry::<C>::from_codec(codec_entry);
                Some(entry.log_id)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok(RecoveredGroup {
        vote,
        last_log_id,
        last_purged_log_id,
        first_index,
        last_index,
        log_index,
        cache,
    })
}

impl<C: RaftTypeConfig + 'static> GroupState<C> {
    async fn new(
        group_id: u64,
        config: &StorageConfig,
        manifest: Arc<ManifestManager>,
    ) -> io::Result<Self>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: FromCodec<RawBytes>,
    {
        let group_dir = config.base_dir.join(format!("{}", group_id));

        // Recover segments (truncate partial writes) and build a replay manifest.
        let (segmented_log, replay_manifest) = SegmentedLog::recover(
            group_dir.clone(),
            group_id,
            config.segment_size,
            manifest.clone(),
            manifest.sender(),
        )
        .await?;

        // Replay records to rebuild in-memory state and build an on-disk index.
        let recovered = replay_group_from_manifest::<C>(
            &group_dir,
            group_id,
            &replay_manifest,
            config.max_cache_entries_per_group,
        )
        .await?;

        let shard = ShardState::spawn(segmented_log, config.fsync_interval);

        let vote = AtomicVote::new();
        vote.store::<C>(recovered.vote.as_ref());

        let last_log_id = AtomicLogId::new();
        last_log_id.store::<C>(recovered.last_log_id.as_ref());

        let last_purged_log_id = AtomicLogId::new();
        last_purged_log_id.store::<C>(recovered.last_purged_log_id.as_ref());

        let cache = DashMap::new();
        for (idx, entry) in recovered.cache {
            cache.insert(idx, entry);
        }

        let cache_low = if config.max_cache_entries_per_group == 0 {
            recovered.last_index.saturating_add(1)
        } else {
            let keep = config.max_cache_entries_per_group as u64;
            recovered
                .last_index
                .saturating_sub(keep.saturating_sub(1))
                .max(recovered.first_index)
        };

        Ok(Self {
            cache,
            log_index: recovered.log_index,
            vote,
            first_index: AtomicU64::new(recovered.first_index),
            last_index: AtomicU64::new(recovered.last_index),
            cache_low: AtomicU64::new(cache_low),
            last_log_id,
            last_purged_log_id,
            shard: Arc::new(shard),
        })
    }
}

/// Atomic storage for Vote.
///
/// Uses a SeqLock-style approach: a sequence number protects the two u64 values.
/// Writers increment the sequence before and after writing (odd = write in progress).
/// Readers retry if they see an odd sequence or if the sequence changed during read.
/// This provides lock-free reads with consistent snapshots.
pub(crate) struct AtomicVote {
    /// Sequence number: odd = write in progress, even = stable
    seq: AtomicU64,
    /// term (64 bits)
    high: AtomicU64,
    /// [node_id: 62 bits][committed: 1 bit][valid: 1 bit]
    low: AtomicU64,
}

impl AtomicVote {
    const VALID_BIT: u64 = 1;
    const COMMITTED_BIT: u64 = 2;
    const NODE_ID_SHIFT: u32 = 2;

    pub(crate) fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            high: AtomicU64::new(0),
            low: AtomicU64::new(0),
        }
    }

    pub(crate) fn store<C>(&self, vote: Option<&openraft::impls::Vote<C>>)
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            >,
    {
        // Increment sequence to odd (write in progress)
        let seq = self.seq.fetch_add(1, Ordering::Release);
        debug_assert!(seq % 2 == 0, "concurrent writes to AtomicVote");

        match vote {
            Some(v) => {
                let high = v.leader_id.term;
                let low = (v.leader_id.node_id << Self::NODE_ID_SHIFT)
                    | if v.committed { Self::COMMITTED_BIT } else { 0 }
                    | Self::VALID_BIT;
                self.high.store(high, Ordering::Relaxed);
                self.low.store(low, Ordering::Relaxed);
            }
            None => {
                self.high.store(0, Ordering::Relaxed);
                self.low.store(0, Ordering::Relaxed);
            }
        }

        // Increment sequence to even (write complete)
        self.seq.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn load<C>(&self) -> Option<openraft::impls::Vote<C>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            >,
    {
        loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 % 2 != 0 {
                // Write in progress, spin
                std::hint::spin_loop();
                continue;
            }

            let high = self.high.load(Ordering::Relaxed);
            let low = self.low.load(Ordering::Relaxed);

            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 != seq2 {
                // Sequence changed during read, retry
                std::hint::spin_loop();
                continue;
            }

            // Consistent read
            if (low & Self::VALID_BIT) == 0 {
                return None;
            }

            let term = high;
            let node_id = low >> Self::NODE_ID_SHIFT;
            let committed = (low & Self::COMMITTED_BIT) != 0;

            return Some(openraft::impls::Vote {
                leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id },
                committed,
            });
        }
    }
}

/// Atomic storage for LogId.
///
/// Uses a SeqLock-style approach: a sequence number protects the two u64 values.
/// Writers increment the sequence before and after writing (odd = write in progress).
/// Readers retry if they see an odd sequence or if the sequence changed during read.
/// This provides lock-free reads with consistent snapshots.
pub(crate) struct AtomicLogId {
    /// Sequence number: odd = write in progress, even = stable
    seq: AtomicU64,
    /// [term: 40 bits][node_id: 24 bits]
    high: AtomicU64,
    /// [index: 63 bits][valid: 1 bit]
    low: AtomicU64,
}

impl AtomicLogId {
    const VALID_BIT: u64 = 1;
    const INDEX_SHIFT: u32 = 1;
    const TERM_SHIFT: u32 = 24;
    const MAX_TERM: u64 = (1u64 << (64 - Self::TERM_SHIFT)) - 1;
    const MAX_NODE_ID: u64 = 0xFF_FFFF;

    pub(crate) fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            high: AtomicU64::new(0),
            low: AtomicU64::new(0),
        }
    }

    pub(crate) fn store<C>(&self, log_id: Option<&LogId<C>>)
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            >,
    {
        // Increment sequence to odd (write in progress)
        let seq = self.seq.fetch_add(1, Ordering::Release);
        debug_assert!(seq % 2 == 0, "concurrent writes to AtomicLogId");

        match log_id {
            Some(lid) => {
                // Check bounds in both debug and release builds to prevent silent corruption
                assert!(
                    lid.leader_id.term <= Self::MAX_TERM,
                    "term {} exceeds maximum {} for packed AtomicLogId",
                    lid.leader_id.term,
                    Self::MAX_TERM
                );
                assert!(
                    lid.leader_id.node_id <= Self::MAX_NODE_ID,
                    "node_id {} exceeds maximum {} for packed AtomicLogId",
                    lid.leader_id.node_id,
                    Self::MAX_NODE_ID
                );
                let high = (lid.leader_id.term << Self::TERM_SHIFT)
                    | (lid.leader_id.node_id & Self::MAX_NODE_ID);
                let low = (lid.index << Self::INDEX_SHIFT) | Self::VALID_BIT;
                self.high.store(high, Ordering::Relaxed);
                self.low.store(low, Ordering::Relaxed);
            }
            None => {
                self.high.store(0, Ordering::Relaxed);
                self.low.store(0, Ordering::Relaxed);
            }
        }

        // Increment sequence to even (write complete)
        self.seq.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn load<C>(&self) -> Option<LogId<C>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            >,
    {
        loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 % 2 != 0 {
                // Write in progress, spin
                std::hint::spin_loop();
                continue;
            }

            let high = self.high.load(Ordering::Relaxed);
            let low = self.low.load(Ordering::Relaxed);

            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 != seq2 {
                // Sequence changed during read, retry
                std::hint::spin_loop();
                continue;
            }

            // Consistent read
            if (low & Self::VALID_BIT) == 0 {
                return None;
            }

            let term = high >> Self::TERM_SHIFT;
            let node_id = high & Self::MAX_NODE_ID;
            let index = low >> Self::INDEX_SHIFT;

            return Some(LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id },
                index,
            });
        }
    }
}

enum WriteRequest<C: RaftTypeConfig> {
    Write {
        data: Vec<u8>,
        entry_index_range: Option<(u64, u64)>,
        record_infos: Vec<RecordInfo>,
        callbacks: Callbacks<C>,
        sync_immediately: bool,
        respond_to: oneshot::Sender<(io::Result<()>, Vec<u8>, Option<WritePlacement>)>,
    },
    Shutdown,
}

// SAFETY: WriteRequest is a plain data enum with no self-referential fields.
// Auto-Unpin fails only because RaftTypeConfig associated types lack Unpin bounds.
impl<C: RaftTypeConfig> Unpin for WriteRequest<C> {}

/// Callback payload optimized for the common cases (0 or 1 callback).
pub(crate) enum Callbacks<C: RaftTypeConfig> {
    None,
    One(IOFlushed<C>),
}

impl<C: RaftTypeConfig> Callbacks<C> {
    #[inline]
    pub(crate) fn extend_into(self, dst: &mut Vec<IOFlushed<C>>) {
        match self {
            Self::None => {}
            Self::One(cb) => dst.push(cb),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ShardError {
    pub(crate) kind: io::ErrorKind,
    pub(crate) message: String,
}

impl ShardError {
    pub(crate) fn to_io_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

#[derive(Debug, Clone, Copy)]
struct WritePlacement {
    segment_id: u64,
    offset: u64,
}

/// Information about a single record within a write batch, used for footer tracking.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordInfo {
    /// Type of the record
    pub(crate) record_type: RecordType,
    /// For Entry records: the log entry index
    pub(crate) entry_index: Option<u64>,
}

struct SegmentedLog {
    group_dir: PathBuf,
    group_id: u64,
    segment_size: u64,
    current_segment_id: u64,
    current_file: File,
    current_size: u64,
    min_entry_index: Option<u64>,
    max_entry_index: Option<u64>,
    min_ts: Option<u64>,
    max_ts: Option<u64>,
    sealed_update: Option<SegmentMeta>,
    manifest_tx: MAsyncTx<Array<SegmentMeta>>,
    footer_tracker: SegmentFooterTracker,
}

impl SegmentedLog {
    /// Create or recover a segmented log for a group (async version)
    async fn recover(
        group_dir: PathBuf,
        group_id: u64,
        segment_size: u64,
        manifest: Arc<ManifestManager>,
        manifest_tx: MAsyncTx<Array<SegmentMeta>>,
    ) -> io::Result<(Self, Vec<(u64, u64)>)> {
        tokio::fs::create_dir_all(&group_dir).await?;

        // Find existing segments
        let segments = Self::list_segments_async(&group_dir, group_id).await?;

        let (current_segment_id, current_file, current_size, replay_manifest) =
            if segments.is_empty() {
                // No existing segments - create first one
                let segment_id = 0u64;
                let path = Self::segment_path(&group_dir, group_id, segment_id);

                // Ensure segment_size is properly aligned for Direct I/O
                let aligned_segment_size = align_up_u64(segment_size, dio_align());
                if aligned_segment_size != segment_size {
                    tracing::warn!(
                        "segment_size {} is not aligned to {} bytes, using aligned size {}",
                        segment_size,
                        dio_align(),
                        aligned_segment_size
                    );
                }

                // Open file initially without Direct I/O for setup operations on problematic filesystems
                let file = open_segment_file(&path, true, true, true, true).await?;
                // Preallocate segment to full size for alignment and performance
                file.set_len(aligned_segment_size).await?;
                drop(file); // Close the file

                // Reopen with Direct I/O enabled if supported
                let file = open_segment_file(&path, true, true, false, false).await?;
                (segment_id, file, 0, vec![(segment_id, 0)])
            } else {
                // Use MDBX segment metadata to avoid scanning all segments.
                // We only validate the *tail* segment; earlier segments are assumed sealed.
                let meta_map: std::collections::HashMap<u64, SegmentMeta> = {
                    let manifest = manifest.clone();
                    tokio::task::spawn_blocking(move || manifest.read_group_segments(group_id))
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??
                };

                let mut manifest_vec: Vec<(u64, u64)> = Vec::with_capacity(segments.len());

                let mut last_segment_id = 0u64;
                let mut last_valid_bytes = 0u64;

                for (i, segment_id) in segments.iter().enumerate() {
                    let path = Self::segment_path(&group_dir, group_id, *segment_id);
                    let file_len = tokio::fs::metadata(&path).await?.len();

                    // Only validate the last segment's tail; earlier segments use manifest metadata if present.
                    let is_last = i + 1 == segments.len();
                    let (valid_bytes, file_len) = if is_last {
                        Self::recover_segment_async(&path, segment_size).await?
                    } else if let Some(m) = meta_map.get(segment_id) {
                        (m.valid_bytes.min(file_len), file_len)
                    } else {
                        // Fallback: if metadata missing, validate this segment too.
                        Self::recover_segment_async(&path, segment_size).await?
                    };

                    // Truncate this segment if it contains a partial/corrupt tail.
                    if valid_bytes < file_len {
                        // Truncation does not benefit from direct I/O and may fail with `EINVAL` on
                        // some kernels when the fd is opened with `O_DIRECT`. Do it on a buffered fd.
                        let desired_len = align_up_u64(valid_bytes, dio_align());

                        // For truncation operations, always use a file opened without Direct I/O
                        // to avoid alignment restrictions
                        {
                            let trunc_file = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .open(&path)
                                .await?;

                            if file_len != desired_len {
                                trunc_file.set_len(desired_len).await?;
                            }

                            if desired_len > valid_bytes {
                                let pad = (desired_len - valid_bytes) as usize;
                                let mut trunc_file_w = trunc_file.try_clone().await?;
                                trunc_file_w
                                    .seek(std::io::SeekFrom::Start(valid_bytes))
                                    .await?;
                                trunc_file_w.write_all(&ZERO_4K[..pad]).await?;
                            }
                        }

                        // Remove any later segments; they are after the first invalid record.
                        for later_id in segments.iter().skip(i + 1) {
                            let later_path = Self::segment_path(&group_dir, group_id, *later_id);
                            // Best-effort: we cannot unlink without the `unlinkat` feature.
                            // Leaving later segments is safe because recovery will stop at the first
                            // invalid tail again and ignore later segments.
                            let _ = later_path;
                        }
                    }

                    manifest_vec.push((*segment_id, valid_bytes));
                    last_segment_id = *segment_id;
                    last_valid_bytes = valid_bytes;

                    if valid_bytes < file_len {
                        break;
                    }
                }

                // Open the last segment for appending.
                let path = Self::segment_path(&group_dir, group_id, last_segment_id);
                // Ensure the last segment has an aligned length so subsequent O_DIRECT RMW reads work.
                {
                    let desired_len = align_up_u64(last_valid_bytes, dio_align());
                    let tmp = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)
                        .await?;
                    let current_len = tmp.metadata().await?.len();
                    if current_len != desired_len {
                        tmp.set_len(desired_len).await?;
                    }
                    if desired_len > last_valid_bytes {
                        let pad = (desired_len - last_valid_bytes) as usize;
                        let mut tmp_w = tmp.try_clone().await?;
                        tmp_w
                            .seek(std::io::SeekFrom::Start(last_valid_bytes))
                            .await?;
                        tmp_w.write_all(&ZERO_4K[..pad]).await?;
                    }
                }

                let file = open_segment_file(&path, true, true, false, false).await?;

                (last_segment_id, file, last_valid_bytes, manifest_vec)
            };

        Ok((
            Self {
                group_dir,
                group_id,
                segment_size,
                current_segment_id,
                current_file,
                current_size,
                min_entry_index: None,
                max_entry_index: None,
                min_ts: None,
                max_ts: None,
                sealed_update: None,
                manifest_tx,
                footer_tracker: SegmentFooterTracker::new(group_id, current_segment_id),
            },
            replay_manifest,
        ))
    }

    /// List segment IDs for a group, sorted by ID (async version)
    async fn list_segments_async(group_dir: &PathBuf, group_id: u64) -> io::Result<Vec<u64>> {
        let prefix = format!("group-{}-", group_id);
        let suffix = ".log";

        let mut segments = Vec::new();

        if let Ok(mut entries) = tokio::fs::read_dir(group_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                if name_str.starts_with(&prefix) && name_str.ends_with(&suffix) {
                    // Extract segment ID from filename
                    let id_str = &name_str[prefix.len()..name_str.len() - suffix.len()];
                    if let Ok(segment_id) = id_str.parse::<u64>() {
                        segments.push(segment_id);
                    }
                }
            }
        }

        segments.sort();
        Ok(segments)
    }

    /// Get the path for a segment file
    fn segment_path(group_dir: &PathBuf, group_id: u64, segment_id: u64) -> PathBuf {
        group_dir.join(format!("group-{}-{}.log", group_id, segment_id))
    }

    /// Recover a segment file using chunked reads, validating all records.
    /// Returns (valid_bytes, file_len).
    async fn recover_segment_async(path: &PathBuf, _max_record_size: u64) -> io::Result<(u64, u64)> {
        let mut file = open_segment_file(path.as_path(), true, false, false, false).await?;
        let file_len = file.metadata().await?.len();

        if file_len == 0 {
            return Ok((0, 0));
        }

        let mut iter = RecordIterator::new(&mut file, 0, file_len, DEFAULT_CHUNK_SIZE);
        if !iter.read_chunk().await? {
            return Ok((0, file_len));
        }

        let mut last_valid_end = 0u64;
        loop {
            match iter.next_record().await {
                Ok(Some((record_offset, parsed))) => {
                    let record_total = LENGTH_SIZE
                        + 1
                        + GROUP_ID_SIZE
                        + parsed.payload.len()
                        + CRC64_SIZE;
                    last_valid_end = record_offset + record_total as u64;
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!("Recovery stopped at invalid record: {}", e);
                    break;
                }
            }
        }

        tracing::info!(
            "Recovered segment {:?}: valid_bytes={}, file_len={}",
            path,
            last_valid_end,
            file_len
        );
        Ok((last_valid_end, file_len))
    }

    async fn write(
        &mut self,
        data: Vec<u8>,
        index_range: Option<(u64, u64)>,
        record_infos: &[RecordInfo],
    ) -> io::Result<(Vec<u8>, WritePlacement)> {
        let data_len = data.len() as u64;

        let mut base_offset = self.current_size;
        if self.current_size + data_len > self.segment_size {
            self.rotate().await?;
            base_offset = 0;
        }

        if let Some((min_i, max_i)) = index_range {
            self.min_entry_index =
                Some(self.min_entry_index.map(|v| v.min(min_i)).unwrap_or(min_i));
            self.max_entry_index =
                Some(self.max_entry_index.map(|v| v.max(max_i)).unwrap_or(max_i));
        }

        for info in record_infos {
            match info.record_type {
                RecordType::Vote => self.footer_tracker.record_vote(),
                RecordType::Entry => {
                    if let Some(entry_index) = info.entry_index {
                        self.footer_tracker.record_entry(entry_index);
                    }
                }
                RecordType::Truncate => self.footer_tracker.record_truncate(),
                RecordType::Purge => self.footer_tracker.record_purge(),
            }
        }

        self.current_file
            .seek(std::io::SeekFrom::Start(base_offset))
            .await?;
        self.current_file.write_all(&data).await?;

        let segment_id = self.current_segment_id;
        self.current_size += data_len;
        Ok((
            data,
            WritePlacement {
                segment_id,
                offset: base_offset,
            },
        ))
    }

    async fn rotate(&mut self) -> io::Result<()> {
        let footer = std::mem::replace(
            &mut self.footer_tracker,
            SegmentFooterTracker::new(self.group_id, self.current_segment_id + 1),
        )
        .build(self.current_size);

        let footer_bytes = footer.encode();
        let footer_len = footer_bytes.len() as u64;

        if self.current_size + footer_len <= self.segment_size {
            self.current_file
                .seek(std::io::SeekFrom::Start(self.current_size))
                .await?;
            self.current_file.write_all(&footer_bytes).await?;
            self.current_size += footer_len;
        } else {
            tracing::warn!(
                "segment {} full, footer truncated (data_size={}, footer_size={}, segment_size={})",
                self.current_segment_id,
                self.current_size,
                footer_len,
                self.segment_size
            );
        }

        self.current_file.sync_all().await?;

        self.sealed_update = Some(SegmentMeta {
            group_id: self.group_id,
            segment_id: self.current_segment_id,
            valid_bytes: self.current_size,
            min_index: self.min_entry_index,
            max_index: self.max_entry_index,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            sealed: true,
            record_type_flags: self.footer_tracker.record_type_flags,
        });

        self.current_segment_id += 1;
        let path = Self::segment_path(&self.group_dir, self.group_id, self.current_segment_id);

        self.current_file = open_segment_file(&path, true, true, true, true).await?;
        self.current_size = 0;
        self.min_entry_index = None;
        self.max_entry_index = None;
        self.min_ts = None;
        self.max_ts = None;

        Ok(())
    }

    /// Sync the current segment
    async fn sync(&self) -> io::Result<()> {
        self.current_file.sync_all().await
    }

    fn emit_manifest_updates(&mut self) {
        // Emit sealed segment update if present.
        if let Some(sealed) = self.sealed_update.take() {
            let _ = self.manifest_tx.try_send(sealed);
        }
        // Emit current segment state.
        let current = SegmentMeta {
            group_id: self.group_id,
            segment_id: self.current_segment_id,
            valid_bytes: self.current_size,
            min_index: self.min_entry_index,
            max_index: self.max_entry_index,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            sealed: false,
            record_type_flags: self.footer_tracker.record_type_flags,
        };
        let _ = self.manifest_tx.try_send(current);
    }
}

/// Per-group shard state
struct ShardState<C: RaftTypeConfig> {
    /// Channel to send write requests to the shard writer task
    write_tx: MAsyncTx<Array<WriteRequest<C>>>,
    /// Whether the shard is running
    #[allow(dead_code)]
    running: Arc<AtomicBool>,
    /// Sticky failure state: once set, all new writes fail fast.
    failed: Arc<std::sync::OnceLock<ShardError>>,
    /// Base directory for this group's segments (used for reads/recovery helpers).
    group_dir: PathBuf,
    /// Group ID for this shard.
    #[allow(dead_code)]
    group_id: u64,
}

impl<C: RaftTypeConfig + 'static> ShardState<C> {
    /// Spawn a new shard writer task using a recovered `SegmentedLog`.
    fn spawn(segmented_log: SegmentedLog, fsync_interval: Option<Duration>) -> Self {
        let group_dir = segmented_log.group_dir.clone();
        let group_id = segmented_log.group_id;

        let (write_tx, write_rx) = crossfire::mpsc::bounded_async::<WriteRequest<C>>(256);

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let failed = Arc::new(std::sync::OnceLock::new());
        let failed_clone = failed.clone();

        // Spawn the writer task
        tokio::spawn(async move {
            Self::writer_loop(
                segmented_log,
                write_rx,
                fsync_interval,
                running_clone,
                failed_clone,
            )
            .await;
        });

        Self {
            write_tx,
            running,
            failed,
            group_dir,
            group_id,
        }
    }

    /// Complete pending callbacks and sync responders with the given result.
    fn complete_pending(
        result: &io::Result<()>,
        pending_callbacks: &mut Vec<IOFlushed<C>>,
        pending_sync_responders: &mut Vec<(
            oneshot::Sender<(io::Result<()>, Vec<u8>, Option<WritePlacement>)>,
            Vec<u8>,
            Option<WritePlacement>,
        )>,
    ) {
        for cb in pending_callbacks.drain(..) {
            let r = match result {
                Ok(()) => Ok(()),
                Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
            };
            cb.io_completed(r);
        }
        for (tx, mut buf, placement) in pending_sync_responders.drain(..) {
            buf.clear();
            let r = match result {
                Ok(()) => Ok(()),
                Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
            };
            let _ = tx.send((r, buf, placement));
        }
    }

    /// Writer task loop - owns the segmented log, no mutex needed
    async fn writer_loop(
        mut log: SegmentedLog,
        write_rx: AsyncRx<Array<WriteRequest<C>>>,
        fsync_interval: Option<Duration>,
        running: Arc<AtomicBool>,
        failed: Arc<std::sync::OnceLock<ShardError>>,
    ) {
        let mut pending_callbacks: Vec<IOFlushed<C>> = Vec::new();
        let mut pending_sync_responders: Vec<(
            oneshot::Sender<(io::Result<()>, Vec<u8>, Option<WritePlacement>)>,
            Vec<u8>,
            Option<WritePlacement>,
        )> = Vec::new();

        let fail_and_exit = |err: io::Error, pending_callbacks: &mut Vec<IOFlushed<C>>| {
            let shard_err = ShardError {
                kind: err.kind(),
                message: err.to_string(),
            };
            let _ = failed.set(shard_err.clone());
            for cb in pending_callbacks.drain(..) {
                cb.io_completed(Err(shard_err.to_io_error()));
            }
            running.store(false, Ordering::Release);
        };

        loop {
            // Only wake when there is work. No periodic timer while idle.
            let mut next_req = match write_rx.recv().await {
                Ok(req) => Some(req),
                Err(_) => {
                    running.store(false, Ordering::Release);
                    return;
                }
            };

            // For fsync_interval=Some(_): we use an "immediate fsync per batch" strategy:
            // - first write triggers an fsync right after we drain the current queue
            // - writes arriving while fsync is in progress will queue and be included in the next fsync
            //
            // For fsync_interval=None: fsync after every write (legacy semantics).
            let mut wrote_any = false;
            let mut shutdown_after_batch = false;

            while let Some(request) = next_req.take() {
                match request {
                    WriteRequest::Write {
                        mut data,
                        entry_index_range,
                        record_infos,
                        callbacks,
                        sync_immediately,
                        respond_to,
                    } => {
                        let mut placement: Option<WritePlacement> = None;
                        if let Some(err) = failed.get() {
                            let _ = respond_to.send((Err(err.to_io_error()), data, None));
                            callbacks.extend_into(&mut pending_callbacks);
                            for cb in pending_callbacks.drain(..) {
                                cb.io_completed(Err(err.to_io_error()));
                            }
                        } else {
                            tracing::trace!(
                                "writer_loop: received write request ({} bytes), sync_immediately={}",
                                data.len(),
                                sync_immediately
                            );

                            if !data.is_empty() {
                                let write_result =
                                    log.write(data, entry_index_range, &record_infos).await;
                                match write_result {
                                    Ok((returned, pl)) => {
                                        data = returned;
                                        placement = Some(pl);
                                    }
                                    Err(e) => {
                                        let err_result: io::Result<()> =
                                            Err(io::Error::new(e.kind(), e.to_string()));
                                        let _ = respond_to.send((
                                            Err(io::Error::new(e.kind(), e.to_string())),
                                            Vec::new(),
                                            None,
                                        ));
                                        Self::complete_pending(
                                            &err_result,
                                            &mut pending_callbacks,
                                            &mut pending_sync_responders,
                                        );
                                        fail_and_exit(e, &mut pending_callbacks);
                                        return;
                                    }
                                };
                                wrote_any = true;
                                tracing::trace!("writer_loop: write complete");
                            }

                            callbacks.extend_into(&mut pending_callbacks);

                            if fsync_interval.is_none() {
                                // Legacy semantics: fsync after every write (or when explicitly requested).
                                // Important: callbacks must not get stranded even if this request
                                // contains no bytes (e.g. append([]) with an IOFlushed callback).
                                if sync_immediately || wrote_any || !pending_callbacks.is_empty() {
                                    let sync_result = log.sync().await;
                                    let (reply, cb_result) = match sync_result {
                                        Ok(_) => (Ok(()), Ok(())),
                                        Err(e) => {
                                            let err = io::Error::new(e.kind(), e.to_string());
                                            (Err(err), Err(io::Error::new(e.kind(), e.to_string())))
                                        }
                                    };

                                    data.clear();
                                    let _ = respond_to.send((reply, data, placement));

                                    // Complete pending callbacks for this fsync boundary.
                                    for cb in pending_callbacks.drain(..) {
                                        cb.io_completed(
                                            cb_result.as_ref().map(|_| ()).map_err(|e| {
                                                io::Error::new(e.kind(), e.to_string())
                                            }),
                                        );
                                    }

                                    match cb_result {
                                        Ok(_) => {
                                            tracing::trace!("writer_loop: sync complete");
                                            log.emit_manifest_updates();
                                        }
                                        Err(e) => {
                                            fail_and_exit(e, &mut pending_callbacks);
                                            return;
                                        }
                                    }
                                } else {
                                    data.clear();
                                    let _ = respond_to.send((Ok(()), data, placement));
                                }
                            } else {
                                // fsync_interval=Some(_): reply immediately unless the caller requires fsync.
                                data.clear();
                                if sync_immediately {
                                    pending_sync_responders.push((respond_to, data, placement));
                                } else {
                                    let _ = respond_to.send((Ok(()), data, placement));
                                }
                            }
                        }
                    }
                    WriteRequest::Shutdown => {
                        shutdown_after_batch = true;
                    }
                }

                next_req = match write_rx.try_recv() {
                    Ok(r) => Some(r),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        running.store(false, Ordering::Release);
                        return;
                    }
                };
            }

            // Immediate fsync per batch when configured with an interval.
            if fsync_interval.is_some() && wrote_any {
                let result = log.sync().await;
                Self::complete_pending(
                    &result,
                    &mut pending_callbacks,
                    &mut pending_sync_responders,
                );
                match result {
                    Ok(_) => log.emit_manifest_updates(),
                    Err(e) => {
                        fail_and_exit(e, &mut pending_callbacks);
                        return;
                    }
                }
            }

            // If no writes occurred in this batch, there is nothing to flush; complete any queued
            // callbacks / sync-responders immediately. This avoids a hang for append([]) and
            // matches the "only when a write occurs" policy.
            if fsync_interval.is_some() && !wrote_any {
                Self::complete_pending(
                    &Ok(()),
                    &mut pending_callbacks,
                    &mut pending_sync_responders,
                );
            }

            if shutdown_after_batch {
                Self::complete_pending(
                    &Ok(()),
                    &mut pending_callbacks,
                    &mut pending_sync_responders,
                );
                running.store(false, Ordering::Release);
                return;
            }
        }
    }

    async fn write(
        &self,
        data: Vec<u8>,
        entry_index_range: Option<(u64, u64)>,
        record_infos: Vec<RecordInfo>,
        callbacks: Callbacks<C>,
        sync_immediately: bool,
    ) -> io::Result<(Vec<u8>, Option<WritePlacement>)> {
        if let Some(err) = self.failed.get() {
            return Err(err.to_io_error());
        }

        let (tx, rx) = oneshot::channel::<(io::Result<()>, Vec<u8>, Option<WritePlacement>)>();

        tracing::trace!(
            "ShardState::write: sending {} bytes, sync_immediately={}",
            data.len(),
            sync_immediately
        );

        self.write_tx
            .send(WriteRequest::Write {
                data,
                entry_index_range,
                record_infos,
                callbacks,
                sync_immediately,
                respond_to: tx,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "shard writer task closed"))?;

        let (res, buf, placement) = rx.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "shard writer task dropped response",
            )
        })?;
        res.map(|_| (buf, placement))
    }

    /// Shutdown the shard (fire-and-forget, spawns async send)
    fn shutdown(&self) {
        let tx = self.write_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(WriteRequest::Shutdown).await;
        });
    }
}

/// Per-group log storage with dedicated shard per group.
///
/// Each group has its own directory with segmented log files.
/// Groups are created dynamically on first access.
/// This eliminates mutex contention and enables independent lifecycle per group.
///
/// Segment rotation:
/// - When a segment exceeds `segment_size`, a new segment is created
/// - Segments are named: `group-{group_id}-{segment_id}.log`
///
/// Crash recovery:
/// - On startup/first access, the group recovers by scanning all segments
/// - Records are validated using CRC64-NVME checksums
/// - Partial/corrupt records at the tail are discarded
pub struct PerGroupLogStorage<C: RaftTypeConfig> {
    config: StorageConfig,
    manifest: Arc<ManifestManager>,
    /// Per-group state index: maps group_id -> slot in `group_states`
    groups: GroupIndex<C>,
    /// Serializes group creation to avoid duplicate recovery work.
    creation_lock: tokio::sync::Mutex<()>,
}

/// Maximum number of raft groups supported (4K)
pub(crate) const MAX_GROUPS: usize = 4096;

/// Lock-free group index using AtomicPtr slab for O(1) access.
///
/// This provides ~2.3x faster reads than DashMap by using direct array indexing
/// instead of hashing. Each slot holds an Arc pointer stored atomically.
///
/// Safety invariants:
/// 1. Arc pointers are stable (the allocation doesn't move)
/// 2. We increment the refcount before storing and decrement on removal
/// 3. AtomicPtr provides lock-free concurrent access
struct GroupIndex<C: RaftTypeConfig> {
    /// Fixed-size array of AtomicPtr slots for O(1) access
    slots: Box<[AtomicPtr<GroupState<C>>]>,
    /// Number of active groups (for iteration optimization)
    count: AtomicUsize,
}

impl<C: RaftTypeConfig> GroupIndex<C> {
    fn new() -> Self {
        let mut slots = Vec::with_capacity(MAX_GROUPS);
        for _ in 0..MAX_GROUPS {
            slots.push(AtomicPtr::new(ptr::null_mut()));
        }
        Self {
            slots: slots.into_boxed_slice(),
            count: AtomicUsize::new(0),
        }
    }

    /// Get a group state by group_id (lock-free, O(1))
    #[inline]
    fn get(&self, group_id: u64) -> Option<Arc<GroupState<C>>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return None;
        }

        let ptr = self.slots[idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }

        // Safety: ptr was created from Arc::into_raw and remains valid while stored in the slot.
        // We increment the strong count and create a new Arc handle without disturbing the original.
        unsafe {
            Arc::increment_strong_count(ptr);
            Some(Arc::from_raw(ptr))
        }
    }

    /// Insert a group state. Returns Ok(()) if inserted, or the existing state if slot occupied.
    fn insert(&self, group_id: u64, state: Arc<GroupState<C>>) -> Result<(), Arc<GroupState<C>>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return Err(state); // Out of bounds
        }

        let new_ptr = Arc::into_raw(state) as *mut GroupState<C>;

        // Use CAS to ensure we only insert if slot is empty
        match self.slots[idx].compare_exchange(
            ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(_existing) => {
                // Slot already occupied - return the state we tried to insert
                let state = unsafe { Arc::from_raw(new_ptr) };
                Err(state)
            }
        }
    }

    /// Remove a group state, returning it if it existed
    fn remove(&self, group_id: u64) -> Option<Arc<GroupState<C>>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return None;
        }

        let ptr = self.slots[idx].swap(ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            None
        } else {
            self.count.fetch_sub(1, Ordering::Relaxed);
            // Safety: We own this pointer now (removed from slot)
            Some(unsafe { Arc::from_raw(ptr) })
        }
    }

    /// Iterate over all group states (for stop/shutdown)
    fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64, &Arc<GroupState<C>>),
    {
        for (idx, slot) in self.slots.iter().enumerate() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                // Safety: ptr is valid while stored in slot
                unsafe {
                    let arc = Arc::from_raw(ptr);
                    f(idx as u64, &arc);
                    // Don't drop - put it back
                    let _ = Arc::into_raw(arc);
                }
            }
        }
    }

    /// Get all group IDs
    fn group_ids(&self) -> Vec<u64> {
        let mut result = Vec::with_capacity(self.count.load(Ordering::Relaxed));
        for (idx, slot) in self.slots.iter().enumerate() {
            if !slot.load(Ordering::Acquire).is_null() {
                result.push(idx as u64);
            }
        }
        result
    }

    /// Get the number of groups
    #[inline]
    fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl<C: RaftTypeConfig> Drop for GroupIndex<C> {
    fn drop(&mut self) {
        // Clean up all remaining Arc pointers
        for slot in self.slots.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                // Safety: We own this pointer, drop the Arc
                unsafe {
                    let _ = Arc::from_raw(ptr);
                }
            }
        }
    }
}

// Type aliases for compatibility
pub type ShardedLogStorage<C> = PerGroupLogStorage<C>;
pub type MultiplexedLogStorage<C> = PerGroupLogStorage<C>;

impl<C: RaftTypeConfig + 'static> PerGroupLogStorage<C> {
    /// Create a new per-group storage instance.
    /// Groups are created dynamically on first access.
    pub async fn new(config: impl Into<StorageConfig>) -> io::Result<Self> {
        let config = config.into();
        tokio::fs::create_dir_all(&*config.base_dir).await?;

        let base = (*config.base_dir).clone();
        let manifest_dir = config
            .manifest_dir
            .as_ref()
            .map(|p| (**p).clone())
            .unwrap_or(base);
        let manifest = tokio::task::spawn_blocking(move || {
            // Try to open MDBX manifest, but fall back to no-op if it fails
            // This can happen on filesystems that don't support Direct I/O like tmpfs
            let result = match ManifestManager::open(&manifest_dir) {
                Ok(manifest) => Ok(manifest),
                Err(e) if e.raw_os_error() == Some(22) || e.kind() == io::ErrorKind::InvalidInput => {
                    tracing::warn!(
                        "MDBX manifest not supported on this filesystem ({}), using in-memory fallback",
                        e
                    );
                    Ok(ManifestManager::open_in_memory())
                }
                Err(e) => Err(e),
            };
            result
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        Ok(Self {
            config,
            manifest: Arc::new(manifest),
            groups: GroupIndex::new(),
            creation_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Stop all groups
    pub fn stop(&self) {
        self.groups.for_each(|_group_id, state| {
            state.shard.shutdown();
        });
    }

    /// Get or create state for a group (creates shard on first access).
    /// Uses a creation lock to prevent duplicate recovery work when multiple
    /// tasks concurrently access the same group for the first time.
    async fn get_or_create_group(&self, group_id: u64) -> io::Result<Arc<GroupState<C>>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: FromCodec<RawBytes>,
    {
        // Fast path: group already exists (lock-free lookup)
        if let Some(state) = self.groups.get(group_id) {
            return Ok(state);
        }

        // Slow path: serialize creation to avoid duplicate recovery work.
        let _guard = self.creation_lock.lock().await;

        // Double-check after acquiring lock.
        if let Some(state) = self.groups.get(group_id) {
            return Ok(state);
        }

        let state = Arc::new(GroupState::new(group_id, &self.config, self.manifest.clone()).await?);

        match self.groups.insert(group_id, state.clone()) {
            Ok(()) => Ok(state),
            Err(_) => {
                // Should not happen under the creation lock, but handle gracefully.
                state.shard.shutdown();
                Ok(self
                    .groups
                    .get(group_id)
                    .expect("group must exist after failed insert"))
            }
        }
    }

    /// Get a log storage handle for a specific group
    pub async fn get_log_storage(&self, group_id: u64) -> io::Result<GroupLogStorage<C>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: FromCodec<RawBytes>,
    {
        let group_state = self.get_or_create_group(group_id).await?;
        Ok(GroupLogStorage {
            group_id,
            config: self.config.clone(),
            state: group_state,
            encode_buf: Vec::new(),
            payload_buf: Vec::new(),
            disk_read_buf: Vec::with_capacity(4096),
            index_updates_buf: Vec::with_capacity(256),
            record_infos_buf: Vec::with_capacity(256),
            cached_read_file: None,
        })
    }

    /// Remove a group from this storage (e.g., when group is deleted)
    pub fn remove_group(&self, group_id: u64) {
        if let Some(state) = self.groups.remove(group_id) {
            state.shard.shutdown();
        }
    }

    /// Get the list of active group IDs
    pub fn group_ids(&self) -> Vec<u64> {
        self.groups.group_ids()
    }

    /// Get the number of active shards (groups).
    /// Note: With per-group sharding, this returns the number of active groups.
    /// This method is provided for backwards compatibility.
    pub fn num_shards(&self) -> usize {
        self.groups.len()
    }
}

/// Trait for multiplexed storage that can provide per-group log storage
pub trait MultiplexedStorage<C: RaftTypeConfig> {
    type GroupLogStorage: RaftLogStorage<C>;

    fn get_log_storage(
        &self,
        group_id: u64,
    ) -> impl std::future::Future<Output = Self::GroupLogStorage> + Send;
    fn remove_group(&self, group_id: u64);
    fn group_ids(&self) -> Vec<u64>;
}

impl<C> MultiplexedStorage<C> for PerGroupLogStorage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            Vote = openraft::impls::Vote<C>,
            Node = openraft::impls::BasicNode,
            Entry = openraft::impls::Entry<C>,
        > + 'static,
    C::Entry: Send + Sync + Clone + 'static,
    C::Vote: ToCodec<CodecVote>,
    C::D: ToCodec<RawBytes> + FromCodec<RawBytes>,
{
    type GroupLogStorage = GroupLogStorage<C>;

    async fn get_log_storage(&self, group_id: u64) -> Self::GroupLogStorage {
        // Panics if group creation fails - in practice this should be fallible
        PerGroupLogStorage::get_log_storage(self, group_id)
            .await
            .expect("Failed to create group storage")
    }

    fn remove_group(&self, group_id: u64) {
        PerGroupLogStorage::remove_group(self, group_id)
    }

    fn group_ids(&self) -> Vec<u64> {
        PerGroupLogStorage::group_ids(self)
    }
}

// Implement MultiRaftLogStorage trait for use with MultiRaftManager
impl<C> crate::multi::storage::MultiRaftLogStorage<C> for PerGroupLogStorage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            Vote = openraft::impls::Vote<C>,
            Node = openraft::impls::BasicNode,
            Entry = openraft::impls::Entry<C>,
        > + 'static,
    C::Entry: Send + Sync + Clone + 'static,
    C::Vote: ToCodec<CodecVote>,
    C::D: ToCodec<RawBytes> + FromCodec<RawBytes>,
{
    type GroupLogStorage = GroupLogStorage<C>;

    async fn get_log_storage(&self, group_id: u64) -> Self::GroupLogStorage {
        // Panics if group creation fails - in practice this should be fallible
        PerGroupLogStorage::get_log_storage(self, group_id)
            .await
            .expect("Failed to create group storage")
    }

    fn remove_group(&self, group_id: u64) {
        PerGroupLogStorage::remove_group(self, group_id)
    }

    fn group_ids(&self) -> Vec<u64> {
        PerGroupLogStorage::group_ids(self)
    }
}

/// Log storage handle for a specific group within a PerGroupLogStorage.
///
/// Each group has its own dedicated shard (1:1 mapping).
/// Votes and log entries are written to the group's shard.
pub struct GroupLogStorage<C: RaftTypeConfig> {
    group_id: u64,
    config: StorageConfig,
    state: Arc<GroupState<C>>,
    encode_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    disk_read_buf: Vec<u8>,
    index_updates_buf: Vec<(u64, u64, u32)>,
    record_infos_buf: Vec<RecordInfo>,
    /// Cached file handle for consecutive reads from the same segment.
    cached_read_file: Option<(u64, File)>,
}

impl<C: RaftTypeConfig> Clone for GroupLogStorage<C> {
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id,
            config: self.config.clone(),
            state: self.state.clone(),
            encode_buf: Vec::new(),
            payload_buf: Vec::new(),
            disk_read_buf: Vec::new(),
            index_updates_buf: Vec::new(),
            record_infos_buf: Vec::new(),
            cached_read_file: None,
        }
    }
}

impl<C: RaftTypeConfig> GroupLogStorage<C> {
    async fn send_encoded(
        &mut self,
        entry_index_range: Option<(u64, u64)>,
        callbacks: Callbacks<C>,
        sync_immediately: bool,
    ) -> Result<Option<WritePlacement>, io::Error> {
        let data = std::mem::take(&mut self.encode_buf);
        let record_infos = std::mem::take(&mut self.record_infos_buf);
        let (buf, placement) = self
            .state
            .shard
            .write(
                data,
                entry_index_range,
                record_infos,
                callbacks,
                sync_immediately,
            )
            .await?;
        self.encode_buf = buf;
        Ok(placement)
    }

    /// Get the group ID
    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    async fn read_entry_from_disk(&mut self, index: u64) -> io::Result<Option<C::Entry>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: FromCodec<RawBytes>,
    {
        let loc = self.state.log_index.get(index);

        let Some(loc) = loc else {
            return Ok(None);
        };

        // Reuse cached file handle if reading from the same segment.
        let mut file = match self.cached_read_file.take() {
            Some((seg_id, f)) if seg_id == loc.segment_id => f,
            _ => {
                let path = SegmentedLog::segment_path(
                    &self.state.shard.group_dir,
                    self.group_id,
                    loc.segment_id,
                );
                open_segment_file(&path, true, false, false, false).await?
            }
        };
        read_exact_at_reuse(&mut file, loc.offset, loc.len as usize, &mut self.disk_read_buf).await?;
        self.cached_read_file = Some((loc.segment_id, file));
        let buf = &self.disk_read_buf[..loc.len as usize];

        if buf.len() < LENGTH_SIZE {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short record"));
        }

        let record_len = u32::from_le_bytes(buf[..LENGTH_SIZE].try_into().unwrap()) as usize;
        if record_len + LENGTH_SIZE != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record length mismatch",
            ));
        }

        let max_record_size = self
            .config
            .max_record_size
            .unwrap_or(self.config.segment_size);
        let parsed = validate_record(&buf[LENGTH_SIZE..], max_record_size as usize)?;
        if parsed.group_id != self.group_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "group_id mismatch",
            ));
        }
        if parsed.record_type != RecordType::Entry {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected Entry record",
            ));
        }

        let codec_entry = CodecEntry::<RawBytes>::decode_from_slice(parsed.payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let entry: openraft::impls::Entry<C> = openraft::impls::Entry::<C>::from_codec(codec_entry);
        Ok(Some(entry))
    }

    fn cache_window_start(&self) -> u64 {
        let max = self.config.max_cache_entries_per_group;
        if max == 0 {
            return self
                .state
                .last_index
                .load(Ordering::Relaxed)
                .saturating_add(1);
        }

        let last = self.state.last_index.load(Ordering::Relaxed);
        let first = self.state.first_index.load(Ordering::Relaxed);
        let keep = max as u64;
        last.saturating_sub(keep.saturating_sub(1)).max(first)
    }

    fn enforce_cache_window(&self) {
        let start = self.cache_window_start();
        let prev_low = self.state.cache_low.load(Ordering::Relaxed);

        if prev_low < start {
            // Evict all entries below the new window start in one pass.
            self.state.cache.retain(|k, _| *k >= start);
        }

        self.state.cache_low.store(start, Ordering::Relaxed);
    }

    async fn compact_purged_segments(&self, purged_before: u64) -> io::Result<()> {
        // Conservative compaction: only delete segments that contain ONLY Entry records
        // and whose max entry index is < purged_before. This avoids deleting vote/purge/truncate history.
        if purged_before == 0 {
            return Ok(());
        }

        let segments =
            SegmentedLog::list_segments_async(&self.state.shard.group_dir, self.group_id).await?;

        // Never delete the current segment (writer may be appending to it).
        let current_segment_id = segments.last().copied().unwrap_or(0);

        for segment_id in segments {
            if segment_id >= current_segment_id {
                continue;
            }

            let path =
                SegmentedLog::segment_path(&self.state.shard.group_dir, self.group_id, segment_id);
            let file = match open_segment_file(&path, true, false, false, false).await {
                Ok(f) => f,
                Err(_) => continue,
            };

            let file_len = file.metadata().await?.len();
            if file_len == 0 {
                // Empty old segment: safe to delete.
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::warn!(
                        group_id = self.group_id,
                        segment_id,
                        error = %e,
                        "failed to delete empty segment during compaction"
                    );
                }
                continue;
            }

            // Use the segment footer for O(1) compaction decisions instead of scanning all records.
            let should_delete = match read_footer_from_segment(&file, file_len).await {
                Ok(Some(footer))
                    if footer.group_id == self.group_id
                        && footer.record_type_flags.has_only_entries() =>
                {
                    // Footer confirms: this segment contains only Entry records.
                    match footer.max_entry_index {
                        Some(max_idx) => max_idx < purged_before,
                        None => true, // No entries at all: safe to delete
                    }
                }
                Ok(Some(footer)) if footer.group_id == self.group_id => {
                    // Segment contains non-entry records (vote/truncate/purge): keep it.
                    false
                }
                _ => {
                    // No footer or group_id mismatch: skip (don't scan, not worth the I/O).
                    false
                }
            };

            if should_delete {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::warn!(
                        group_id = self.group_id,
                        segment_id,
                        purged_before,
                        error = %e,
                        "failed to delete purged segment during compaction"
                    );
                } else {
                    tracing::debug!(
                        group_id = self.group_id,
                        segment_id,
                        "deleted purged segment"
                    );
                }
            }
        }

        Ok(())
    }
}

impl<C> RaftLogReader<C> for GroupLogStorage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            Vote = openraft::impls::Vote<C>,
            Node = openraft::impls::BasicNode,
            Entry = openraft::impls::Entry<C>,
        >,
    C::Entry: Clone + 'static,
    C::D: FromCodec<RawBytes>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        use std::ops::Bound;

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => u64::MAX,
        };

        // Pre-size the vec based on expected range (cap at reasonable size)
        let expected_len = end.saturating_sub(start).min(1024) as usize;
        let mut entries = Vec::with_capacity(expected_len);

        for idx in start..end {
            if let Some(entry) = self.state.cache.get(&idx) {
                entries.push(entry.value().as_ref().clone());
                continue;
            }

            if let Some(entry) = self.read_entry_from_disk(idx).await? {
                if idx >= self.cache_window_start() {
                    self.state.cache.insert(idx, Arc::new(entry.clone()));
                    self.enforce_cache_window();
                }
                entries.push(entry);
            } else {
                break;
            }
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<C::Vote>, io::Error> {
        Ok(self.state.vote.load())
    }
}

impl<C> RaftLogStorage<C> for GroupLogStorage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            Vote = openraft::impls::Vote<C>,
            Node = openraft::impls::BasicNode,
            Entry = openraft::impls::Entry<C>,
        >,
    C::Entry: Send + Sync + Clone + 'static,
    C::Vote: ToCodec<CodecVote>,
    C::D: ToCodec<RawBytes> + FromCodec<RawBytes>,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let last_log_id = self.state.last_log_id.load();
        let last_purged_log_id = self.state.last_purged_log_id.load();
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &C::Vote) -> Result<(), io::Error> {
        // Store atomically in memory
        self.state.vote.store(Some(vote));

        // Persist to log file - encode into payload buffer, then build record
        let codec_vote = vote.to_codec();
        self.payload_buf.clear();
        codec_vote
            .encode_into(&mut self.payload_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        encode_record_into(
            &mut self.encode_buf,
            RecordType::Vote,
            self.group_id,
            &self.payload_buf,
        );
        self.record_infos_buf.push(RecordInfo {
            record_type: RecordType::Vote,
            entry_index: None,
        });
        let _ = self.send_encoded(None, Callbacks::None, true).await?;

        Ok(())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + Send,
    {
        use openraft::entry::RaftEntry;

        let mut last_log_id = None;

        self.encode_buf.clear();
        self.index_updates_buf.clear();
        self.record_infos_buf.clear();
        for entry in entries {
            let log_id = entry.log_id();
            let index = log_id.index;

            let codec_entry: CodecEntry<RawBytes> = entry.to_codec();
            self.payload_buf.clear();
            codec_entry
                .encode_into(&mut self.payload_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            let record_start = self.encode_buf.len() as u64;
            let record_len = append_record_into(
                &mut self.encode_buf,
                RecordType::Entry,
                self.group_id,
                &self.payload_buf,
            );
            self.index_updates_buf
                .push((index, record_start, record_len as u32));
            self.record_infos_buf.push(RecordInfo {
                record_type: RecordType::Entry,
                entry_index: Some(index),
            });

            self.state.cache.insert(index, Arc::new(entry));

            self.state.last_index.fetch_max(index, Ordering::Relaxed);
            last_log_id = Some(log_id);
        }

        // Update last log id
        if let Some(ref lid) = last_log_id {
            self.state.last_log_id.store(Some(lid));
        }

        // Send the entire batch as a single write request and attach the callback.
        // The callback is completed only after the next durable fsync that covers this write.
        let entry_index_range = self
            .index_updates_buf
            .first()
            .zip(self.index_updates_buf.last())
            .map(|(a, b)| (a.0, b.0));

        let placement = self
            .send_encoded(entry_index_range, Callbacks::One(callback), false)
            .await?;

        if let Some(pl) = placement {
            for (entry_index, record_start, record_len) in self.index_updates_buf.drain(..) {
                self.state.log_index.insert(
                    entry_index,
                    LogLocation {
                        segment_id: pl.segment_id,
                        offset: pl.offset + record_start,
                        len: record_len,
                    },
                )?;
            }
        }
        self.enforce_cache_window();

        Ok(())
    }

    async fn truncate_after(&mut self, after: Option<LogId<C>>) -> Result<(), io::Error> {
        let index = match after {
            Some(ref log_id) => log_id.index,
            None => 0,
        };

        // Remove entries after the truncation point from cache
        let last = self.state.last_index.load(Ordering::Relaxed);
        for idx in (index + 1)..=last {
            self.state.cache.remove(&idx);
        }
        self.state.log_index.truncate_from(index.saturating_add(1));

        // Update last index
        self.state.last_index.store(index, Ordering::Relaxed);
        if let Some(ref log_id) = after {
            self.state.last_log_id.store(Some(log_id));
        } else {
            self.state.last_log_id.store::<C>(None);
        }

        if let Some(log_id) = after {
            let codec_log_id: CodecLogId = log_id.to_codec();
            self.payload_buf.clear();
            codec_log_id
                .encode_into(&mut self.payload_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            encode_record_into(
                &mut self.encode_buf,
                RecordType::Truncate,
                self.group_id,
                &self.payload_buf,
            );
            self.record_infos_buf.push(RecordInfo {
                record_type: RecordType::Truncate,
                entry_index: None,
            });
            let _ = self.send_encoded(None, Callbacks::None, true).await?;
        }
        self.enforce_cache_window();

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C>) -> Result<(), io::Error> {
        let index = log_id.index;

        // Remove entries up to and including the purge point from cache
        let first = self.state.first_index.load(Ordering::Relaxed);
        for idx in first..=index {
            self.state.cache.remove(&idx);
        }
        self.state.log_index.purge_to(index);

        // Update first index and last_purged_log_id
        self.state.first_index.store(index + 1, Ordering::Relaxed);
        self.state.last_purged_log_id.store(Some(&log_id));

        let codec_log_id: CodecLogId = log_id.to_codec();
        self.payload_buf.clear();
        codec_log_id
            .encode_into(&mut self.payload_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        encode_record_into(
            &mut self.encode_buf,
            RecordType::Purge,
            self.group_id,
            &self.payload_buf,
        );
        self.record_infos_buf.push(RecordInfo {
            record_type: RecordType::Purge,
            entry_index: None,
        });
        let _ = self.send_encoded(None, Callbacks::None, true).await?;
        self.enforce_cache_window();
        // Best-effort: compact fully-purged segments.
        let _ = self.compact_purged_segments(index.saturating_add(1)).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::RaftTypeConfig;
    use openraft::type_config::async_runtime::Oneshot;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestData(Vec<u8>);

    impl std::fmt::Display for TestData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestData(len={})", self.0.len())
        }
    }

    impl ToCodec<RawBytes> for TestData {
        fn to_codec(&self) -> RawBytes {
            RawBytes(self.0.clone())
        }
    }

    impl FromCodec<RawBytes> for TestData {
        fn from_codec(codec: RawBytes) -> Self {
            Self(codec.0)
        }
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert_eq!(config.segment_size, DEFAULT_SEGMENT_SIZE);
        assert_eq!(config.fsync_interval, Some(Duration::from_millis(100)));
        assert_eq!(config.max_cache_entries_per_group, 10000);
    }

    #[test]
    fn test_sharded_config_conversion() {
        let sharded = ShardedStorageConfig {
            base_dir: PathBuf::from("/tmp/test"),
            num_shards: 16, // Should be ignored
            segment_size: 32 * 1024 * 1024,
            max_record_size: Some(2 * 1024 * 1024),
            fsync_interval: None,
            max_cache_entries_per_group: 5000,
        };

        let config: StorageConfig = sharded.into();
        assert_eq!(*config.base_dir, PathBuf::from("/tmp/test"));
        assert_eq!(config.segment_size, 32 * 1024 * 1024);
        assert_eq!(config.fsync_interval, None);
        assert_eq!(config.max_cache_entries_per_group, 5000);
    }

    #[test]
    fn test_encode_decode_record() {
        let record_type = RecordType::Entry;
        let group_id = 42u64;
        let payload = b"test payload data";

        let encoded = encode_record(record_type, group_id, payload);

        // Verify length field
        let len = u32::from_le_bytes(encoded[..4].try_into().unwrap());
        assert_eq!(len as usize, encoded.len() - LENGTH_SIZE);

        // Validate the record
        let record_data = &encoded[LENGTH_SIZE..];
        let parsed = validate_record(record_data, DEFAULT_MAX_RECORD_SIZE as usize)
            .expect("should validate");

        assert_eq!(parsed.record_type, RecordType::Entry);
        assert_eq!(parsed.group_id, 42);
        assert_eq!(parsed.payload, payload);
    }

    #[test]
    fn test_record_crc_corruption() {
        let encoded = encode_record(RecordType::Vote, 1, b"vote data");

        // Corrupt a byte in the payload
        let mut corrupted = encoded.clone();
        corrupted[10] ^= 0xFF;

        let record_data = &corrupted[LENGTH_SIZE..];
        assert!(validate_record(record_data, DEFAULT_MAX_RECORD_SIZE as usize).is_err());
    }

    #[test]
    fn test_atomic_vote() {
        let vote = AtomicVote::new();

        let loaded: Option<
            openraft::impls::Vote<crate::multi::type_config::ManiacRaftTypeConfig<String, String>>,
        > = vote.load();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_atomic_log_id() {
        let log_id = AtomicLogId::new();

        let loaded: Option<LogId<crate::multi::type_config::ManiacRaftTypeConfig<String, String>>> =
            log_id.load();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_fsync_none_callback_completes() {
        // With fsync_interval=None, callbacks must complete successfully after a durable fsync.
        use crate::multi::type_config::ManiacRaftTypeConfig;
        type C = ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let cfg = StorageConfig::new(dir.path()).with_fsync_interval(None);
            let storage = PerGroupLogStorage::<C>::new(cfg).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel::<
                Result<(), io::Error>,
            >();
            let cb = IOFlushed::<C>::signal(tx);

            group
                .append(Vec::<openraft::impls::Entry<C>>::new(), cb)
                .await
                .unwrap();

            assert!(rx.await.unwrap().is_ok());
        });
    }

    #[test]
    fn test_recovery_replays_vote_and_entries() {
        use crate::multi::type_config::ManiacRaftTypeConfig;
        type C = ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            // Tests often use tmpfs which may not support MDBX properly
            // The manifest will automatically fall back to in-memory mode
            let cfg = StorageConfig::new(dir.path())
                .with_fsync_interval(None)
                .with_max_cache_entries(1);

            // Write some state.
            {
                let storage = PerGroupLogStorage::<C>::new(cfg.clone()).await.unwrap();
                let mut group = storage.get_log_storage(42).await.unwrap();

                let vote = openraft::impls::Vote::<C> {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: 3,
                        node_id: 7,
                    },
                    committed: true,
                };
                group.save_vote(&vote).await.unwrap();

                let entry = openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 3,
                            node_id: 7,
                        },
                        index: 1,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(b"hello".to_vec())),
                };

                let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel::<
                    Result<(), io::Error>,
                >();
                let cb = IOFlushed::<C>::signal(tx);
                group.append(vec![entry], cb).await.unwrap();
                let _ = rx.await;

                // Ensure writer task releases file handles before reopening.
                storage.stop();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // Re-open and verify recovery.
            {
                let storage = PerGroupLogStorage::<C>::new(cfg).await.unwrap();
                let mut group = storage.get_log_storage(42).await.unwrap();

                let recovered_vote = group.read_vote().await.unwrap().unwrap();
                assert_eq!(recovered_vote.leader_id.term, 3);
                assert_eq!(recovered_vote.leader_id.node_id, 7);
                assert!(recovered_vote.committed);

                let entries: Vec<openraft::impls::Entry<ManiacRaftTypeConfig<TestData, ()>>> =
                    group.try_get_log_entries(1u64..=1u64).await.unwrap();
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].log_id.index, 1);
            }
        });
    }

    #[test]
    fn test_record_validation_edge_cases() {
        // Test minimum valid record size
        let min_record = encode_record(RecordType::Vote, 1, &[]);
        let record_data = &min_record[LENGTH_SIZE..];
        let parsed = validate_record(record_data, DEFAULT_MAX_RECORD_SIZE as usize)
            .expect("should validate");
        assert_eq!(parsed.payload.len(), 0);

        // Test record too short (missing CRC)
        let mut short = vec![0u8; 16]; // type(1) + group_id(3) + payload(0) = 4, but we need 8 more for CRC
        short[0] = RecordType::Vote as u8;
        assert!(validate_record(&short, DEFAULT_MAX_RECORD_SIZE as usize).is_err());

        // Test record with invalid type
        let mut invalid_type = encode_record(RecordType::Vote, 1, b"data");
        invalid_type[LENGTH_SIZE] = 0xFF; // Invalid record type
        let record_data = &invalid_type[LENGTH_SIZE..];
        assert!(validate_record(record_data, DEFAULT_MAX_RECORD_SIZE as usize).is_err());
    }

    #[test]
    fn test_record_type_conversion() {
        // Test valid conversions
        assert_eq!(RecordType::try_from(0x01).unwrap(), RecordType::Vote);
        assert_eq!(RecordType::try_from(0x02).unwrap(), RecordType::Entry);
        assert_eq!(RecordType::try_from(0x03).unwrap(), RecordType::Truncate);
        assert_eq!(RecordType::try_from(0x04).unwrap(), RecordType::Purge);

        // Test invalid conversion
        assert!(RecordType::try_from(0x99).is_err());
    }

    #[test]
    fn test_encode_record_various_sizes() {
        // Test small payload
        let small = encode_record(RecordType::Entry, 1, b"a");
        assert!(validate_record(&small[LENGTH_SIZE..], DEFAULT_MAX_RECORD_SIZE as usize).is_ok());

        // Test large payload
        let large_payload = vec![0x42u8; 10000];
        let large = encode_record(RecordType::Entry, 1, &large_payload);
        let parsed =
            validate_record(&large[LENGTH_SIZE..], DEFAULT_MAX_RECORD_SIZE as usize).unwrap();
        assert_eq!(parsed.payload, large_payload.as_slice());

        // Test zero-length payload
        let empty = encode_record(RecordType::Entry, 1, &[]);
        let parsed =
            validate_record(&empty[LENGTH_SIZE..], DEFAULT_MAX_RECORD_SIZE as usize).unwrap();
        assert_eq!(parsed.payload.len(), 0);
    }

    #[test]
    fn test_crc_calculation_consistency() {
        let payload = b"test data for CRC";
        let encoded1 = encode_record(RecordType::Entry, 42, payload);
        let encoded2 = encode_record(RecordType::Entry, 42, payload);

        // Same input should produce same CRC
        assert_eq!(encoded1, encoded2);

        // Different group_id should produce different CRC
        let encoded3 = encode_record(RecordType::Entry, 43, payload);
        assert_ne!(encoded1, encoded3);

        // Different record type should produce different CRC
        let encoded4 = encode_record(RecordType::Vote, 42, payload);
        assert_ne!(encoded1, encoded4);
    }

    // =========================================================================
    // LogIndex Tests (internal type)
    // =========================================================================

    #[test]
    fn test_log_index_basic_operations() {
        let index = LogIndex::new();

        // Insert and get
        let loc = LogLocation {
            segment_id: 0,
            offset: 100,
            len: 50,
        };
        index.insert(1, loc).unwrap();

        let retrieved = index.get(1);
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.segment_id, 0);
        assert_eq!(retrieved.offset, 100);
        assert_eq!(retrieved.len, 50);

        // Non-existent key
        assert!(index.get(999).is_none());

        // Remove
        index.remove(1);
        assert!(index.get(1).is_none());
    }

    #[test]
    fn test_log_index_multiple_entries() {
        let index = LogIndex::new();

        // Insert multiple entries
        for i in 0..100 {
            let loc = LogLocation {
                segment_id: i / 10,
                offset: i as u64 * 100,
                len: 50,
            };
            index.insert(i, loc).unwrap();
        }

        // Verify all can be retrieved
        for i in 0..100 {
            let retrieved = index.get(i).expect("entry should exist");
            assert_eq!(retrieved.segment_id, i / 10);
            assert_eq!(retrieved.offset, i as u64 * 100);
        }
    }

    #[test]
    fn test_log_index_truncate_from() {
        let index = LogIndex::new();

        // Insert entries 0-9
        for i in 0..10 {
            let loc = LogLocation {
                segment_id: 0,
                offset: i as u64 * 100,
                len: 50,
            };
            index.insert(i, loc).unwrap();
        }

        // Truncate from index 5 (removes 5-9)
        index.truncate_from(5);

        // Verify 0-4 still exist
        for i in 0..5 {
            assert!(index.get(i).is_some(), "index {} should exist", i);
        }

        // Verify 5-9 are gone
        for i in 5..10 {
            assert!(index.get(i).is_none(), "index {} should be removed", i);
        }
    }

    #[test]
    fn test_log_index_purge_to() {
        let index = LogIndex::new();

        // Insert entries 0-9
        for i in 0..10 {
            let loc = LogLocation {
                segment_id: 0,
                offset: i as u64 * 100,
                len: 50,
            };
            index.insert(i, loc).unwrap();
        }

        // Purge to index 4 (removes 0-4)
        index.purge_to(4);

        // Verify 0-4 are gone
        for i in 0..5 {
            assert!(index.get(i).is_none(), "index {} should be removed", i);
        }

        // Verify 5-9 still exist
        for i in 5..10 {
            assert!(index.get(i).is_some(), "index {} should exist", i);
        }
    }

    #[test]
    fn test_log_index_slot_reuse() {
        let index = LogIndex::new();

        // Insert and remove to populate free list
        index
            .insert(
                1,
                LogLocation {
                    segment_id: 0,
                    offset: 0,
                    len: 50,
                },
            )
            .unwrap();
        index.remove(1);

        // Insert again - should reuse slot
        index
            .insert(
                2,
                LogLocation {
                    segment_id: 1,
                    offset: 100,
                    len: 60,
                },
            )
            .unwrap();

        let retrieved = index.get(2).expect("entry should exist");
        assert_eq!(retrieved.segment_id, 1);
        assert_eq!(retrieved.offset, 100);
        assert_eq!(retrieved.len, 60);
    }

    // =========================================================================
    // AtomicVote Extended Tests
    // =========================================================================

    #[test]
    fn test_atomic_vote_store_load_with_data() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;
        let vote = AtomicVote::new();

        // Initially None
        let loaded: Option<openraft::impls::Vote<C>> = vote.load();
        assert!(loaded.is_none());

        // Store a vote
        let v = openraft::impls::Vote::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 5,
                node_id: 42,
            },
            committed: true,
        };
        vote.store(Some(&v));

        // Load it back
        let loaded: openraft::impls::Vote<C> = vote.load().expect("vote should exist");
        assert_eq!(loaded.leader_id.term, 5);
        assert_eq!(loaded.leader_id.node_id, 42);
        assert!(loaded.committed);
    }

    #[test]
    fn test_atomic_vote_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        let vote = Arc::new(AtomicVote::new());
        let v = openraft::impls::Vote::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 10,
                node_id: 5,
            },
            committed: true,
        };
        vote.store(Some(&v));

        let mut handles = vec![];
        for _ in 0..10 {
            let vote_clone = vote.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let loaded: openraft::impls::Vote<C> =
                        vote_clone.load().expect("vote should exist");
                    assert_eq!(loaded.leader_id.term, 10);
                    assert_eq!(loaded.leader_id.node_id, 5);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    // =========================================================================
    // AtomicLogId Extended Tests
    // =========================================================================

    #[test]
    fn test_atomic_log_id_boundary_values() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;
        let log_id = AtomicLogId::new();

        // Test index 0
        let lid: LogId<C> = LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 1,
                node_id: 1,
            },
            index: 0,
        };
        log_id.store(Some(&lid));
        let loaded: LogId<C> = log_id.load().unwrap();
        assert_eq!(loaded.index, 0);

        // Test large index
        let lid: LogId<C> = LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 1,
                node_id: 1,
            },
            index: u64::MAX >> 1, // 63-bit max
        };
        log_id.store(Some(&lid));
        let loaded: LogId<C> = log_id.load().unwrap();
        assert_eq!(loaded.index, u64::MAX >> 1);
    }

    #[test]
    fn test_atomic_log_id_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        let log_id = Arc::new(AtomicLogId::new());
        let lid: LogId<C> = LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 7,
                node_id: 3,
            },
            index: 500,
        };
        log_id.store(Some(&lid));

        let mut handles = vec![];
        for _ in 0..10 {
            let lid_clone = log_id.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let loaded: LogId<C> = lid_clone.load().expect("log_id should exist");
                    assert_eq!(loaded.leader_id.term, 7);
                    assert_eq!(loaded.index, 500);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    // =========================================================================
    // GroupIndex Tests
    // =========================================================================

    #[test]
    fn test_group_index_basic() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;
        let index: GroupIndex<C> = GroupIndex::new();

        assert_eq!(index.len(), 0);
        assert!(index.group_ids().is_empty());
    }

    #[test]
    fn test_group_index_bounds() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;
        let index: GroupIndex<C> = GroupIndex::new();

        // Out of bounds access should return None
        assert!(index.get(MAX_GROUPS as u64 + 1).is_none());
        assert!(index.get(u64::MAX).is_none());
    }

    // =========================================================================
    // StorageConfig Extended Tests
    // =========================================================================

    #[test]
    fn test_storage_config_builder() {
        let dir = TempDir::new().unwrap();

        let config = StorageConfig::new(dir.path())
            .with_segment_size(32 * 1024 * 1024)
            .with_max_record_size(512 * 1024)
            .with_fsync_interval(None)
            .with_max_cache_entries(5000);

        assert_eq!(config.segment_size, 32 * 1024 * 1024);
        assert_eq!(config.max_record_size, Some(512 * 1024));
        assert_eq!(config.fsync_interval, None);
        assert_eq!(config.max_cache_entries_per_group, 5000);
    }

    #[test]
    fn test_storage_config_segment_size_alignment() {
        let dir = TempDir::new().unwrap();

        // Non-aligned segment size should be rounded up
        // Set max_record_size FIRST so segment_size validation passes
        let config = StorageConfig::new(dir.path())
            .with_max_record_size(512) // Must be set before segment_size
            .with_segment_size(1000); // Not aligned to 4096

        // Should be aligned to at least 4096
        assert!(config.segment_size >= 4096);
        assert_eq!(config.segment_size % 4096, 0);
    }

    // =========================================================================
    // PerGroupLogStorage Integration Tests
    // =========================================================================

    #[test]
    fn test_per_group_storage_multiple_groups_isolation() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path())
                .with_fsync_interval(None)
                .with_max_cache_entries(100);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();

            // Create two groups
            let mut group1 = storage.get_log_storage(1).await.unwrap();
            let mut group2 = storage.get_log_storage(2).await.unwrap();

            // Write to group 1
            let v1 = openraft::impls::Vote::<C> {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 10,
                },
                committed: true,
            };
            group1.save_vote(&v1).await.unwrap();

            let e1 = openraft::impls::Entry::<C> {
                log_id: LogId {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: 1,
                        node_id: 10,
                    },
                    index: 1,
                },
                payload: openraft::EntryPayload::Normal(TestData(b"group1-data".to_vec())),
            };
            let (tx1, rx1) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
            group1
                .append(vec![e1], IOFlushed::signal(tx1))
                .await
                .unwrap();
            let _ = rx1.await;

            // Write to group 2 with different vote
            let v2 = openraft::impls::Vote::<C> {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 2,
                    node_id: 20,
                },
                committed: false,
            };
            group2.save_vote(&v2).await.unwrap();

            let e2 = openraft::impls::Entry::<C> {
                log_id: LogId {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: 2,
                        node_id: 20,
                    },
                    index: 1,
                },
                payload: openraft::EntryPayload::Normal(TestData(b"group2-data".to_vec())),
            };
            let (tx2, rx2) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
            group2
                .append(vec![e2], IOFlushed::signal(tx2))
                .await
                .unwrap();
            let _ = rx2.await;

            // Verify isolation
            let vote1 = group1.read_vote().await.unwrap().unwrap();
            assert_eq!(vote1.leader_id.term, 1);
            assert_eq!(vote1.leader_id.node_id, 10);

            let vote2 = group2.read_vote().await.unwrap().unwrap();
            assert_eq!(vote2.leader_id.term, 2);
            assert_eq!(vote2.leader_id.node_id, 20);

            // Verify entries are isolated
            let entries1 = group1.try_get_log_entries(1u64..=1u64).await.unwrap();
            assert_eq!(entries1.len(), 1);

            let entries2 = group2.try_get_log_entries(1u64..=1u64).await.unwrap();
            assert_eq!(entries2.len(), 1);

            storage.stop();
        });
    }

    #[test]
    fn test_per_group_storage_group_lifecycle() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();

            // Initially no groups
            assert!(storage.group_ids().is_empty());
            assert_eq!(storage.num_shards(), 0);

            // Add a group
            let mut group = storage.get_log_storage(42).await.unwrap();
            assert_eq!(storage.group_ids(), vec![42]);
            assert_eq!(storage.num_shards(), 1);

            // Write to group
            let v = openraft::impls::Vote::<C> {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            };
            group.save_vote(&v).await.unwrap();

            // Remove group
            storage.remove_group(42);
            assert!(storage.group_ids().is_empty());
            assert_eq!(storage.num_shards(), 0);

            storage.stop();
        });
    }

    // =========================================================================
    // GroupLogStorage Extended Tests
    // =========================================================================

    #[test]
    fn test_group_log_storage_append_and_read() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Append multiple entries
            for i in 1..=5 {
                let e = openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: i,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(
                        format!("entry-{}", i).into_bytes(),
                    )),
                };
                let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
                group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
                let _ = rx.await;
            }

            // Read entries back
            let entries = group.try_get_log_entries(1u64..=5u64).await.unwrap();
            assert_eq!(entries.len(), 5);

            for (i, entry) in entries.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            // Read subset
            let subset = group.try_get_log_entries(2u64..=4u64).await.unwrap();
            assert_eq!(subset.len(), 3);
            assert_eq!(subset[0].log_id.index, 2);
            assert_eq!(subset[2].log_id.index, 4);

            storage.stop();
        });
    }

    #[test]
    fn test_group_log_storage_get_log_state() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Initial state
            let state = group.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());
            assert!(state.last_purged_log_id.is_none());

            // Append entries
            for i in 1..=3 {
                let e = openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: i,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(b"data".to_vec())),
                };
                let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
                group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
                let _ = rx.await;
            }

            // Check updated state
            let state = group.get_log_state().await.unwrap();
            let last = state.last_log_id.expect("should have last_log_id");
            assert_eq!(last.index, 3);

            storage.stop();
        });
    }

    #[test]
    fn test_group_log_storage_truncate() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Append entries
            for i in 1..=5 {
                let e = openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: i,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(b"data".to_vec())),
                };
                let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
                group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
                let _ = rx.await;
            }

            // Truncate after index 3 (removes 4 and 5)
            let truncate_point = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 3,
            };
            group.truncate_after(Some(truncate_point)).await.unwrap();

            // Verify entries 1-3 remain
            let entries = group.try_get_log_entries(1u64..=3u64).await.unwrap();
            assert_eq!(entries.len(), 3);

            // Verify entries 4-5 are gone
            let entries = group.try_get_log_entries(4u64..=5u64).await.unwrap();
            assert_eq!(entries.len(), 0);

            // Verify state updated
            let state = group.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 3);

            storage.stop();
        });
    }

    #[test]
    fn test_group_log_storage_purge() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Append entries
            for i in 1..=5 {
                let e = openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: i,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(b"data".to_vec())),
                };
                let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
                group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
                let _ = rx.await;
            }

            // Purge up to index 2 (removes 1 and 2)
            let purge_point = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 2,
            };
            group.purge(purge_point).await.unwrap();

            // Verify entries 1-2 are gone
            let entries = group.try_get_log_entries(1u64..=2u64).await.unwrap();
            assert_eq!(entries.len(), 0);

            // Verify entries 3-5 remain
            let entries = group.try_get_log_entries(3u64..=5u64).await.unwrap();
            assert_eq!(entries.len(), 3);

            // Verify state updated
            let state = group.get_log_state().await.unwrap();
            let purged = state.last_purged_log_id.unwrap();
            assert_eq!(purged.index, 2);

            storage.stop();
        });
    }

    #[test]
    fn test_group_log_storage_cache_window() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path())
                .with_fsync_interval(None)
                .with_max_cache_entries(3); // Small cache for testing

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Append 5 entries
            for i in 1..=5 {
                let e = openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: i,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(b"data".to_vec())),
                };
                let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
                group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
                let _ = rx.await;
            }

            // Cache should only contain recent entries (3-5 with window size 3)
            assert!(group.state.cache.get(&3).is_some());
            assert!(group.state.cache.get(&4).is_some());
            assert!(group.state.cache.get(&5).is_some());

            // Entries 1-2 should be evicted from cache
            assert!(group.state.cache.get(&1).is_none());
            assert!(group.state.cache.get(&2).is_none());

            // But they should still be readable from disk
            let entries = group.try_get_log_entries(1u64..=5u64).await.unwrap();
            assert_eq!(entries.len(), 5);

            storage.stop();
        });
    }

    #[test]
    fn test_group_log_storage_batch_append() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Append a batch of entries
            let entries: Vec<_> = (1..=10)
                .map(|i| openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: i,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(
                        format!("batch-{}", i).into_bytes(),
                    )),
                })
                .collect();

            let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
            group.append(entries, IOFlushed::signal(tx)).await.unwrap();
            let _ = rx.await;

            // Verify all entries written
            let read = group.try_get_log_entries(1u64..=10u64).await.unwrap();
            assert_eq!(read.len(), 10);

            storage.stop();
        });
    }

    // =========================================================================
    // Segment Rotation Test
    // =========================================================================

    #[test]
    fn test_segment_rotation() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            // Use small segment size to trigger rotation
            // Set max_record_size FIRST so segment_size validation passes
            let config = StorageConfig::new(dir.path())
                .with_max_record_size(4096) // Must be set before segment_size
                .with_segment_size(8192) // Small segment (at least 2 pages)
                .with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Write enough entries to trigger rotation
            for i in 1..=50 {
                let data = vec![0x42u8; 200]; // 200 bytes each
                let e = openraft::impls::Entry::<C> {
                    log_id: LogId {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index: i,
                    },
                    payload: openraft::EntryPayload::Normal(TestData(data)),
                };
                let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
                group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
                let _ = rx.await;
            }

            // Verify all entries readable
            let entries = group.try_get_log_entries(1u64..=50u64).await.unwrap();
            assert_eq!(entries.len(), 50);

            // Check that multiple segments exist
            let segments = SegmentedLog::list_segments_async(&group.state.shard.group_dir, 1)
                .await
                .unwrap();
            assert!(segments.len() > 1, "Should have multiple segments");

            storage.stop();
        });
    }

    // =========================================================================
    // Record Format Extended Tests
    // =========================================================================

    #[test]
    fn test_append_record_into() {
        let mut buf = Vec::new();

        // First record
        let len1 = append_record_into(&mut buf, RecordType::Entry, 1, b"first");
        assert!(len1 > 0);

        // Second record (appended)
        let len2 = append_record_into(&mut buf, RecordType::Vote, 2, b"second");
        assert!(len2 > 0);

        // Buffer should contain both records
        assert_eq!(buf.len(), len1 + len2);
    }

    #[test]
    fn test_write_u24_le_roundtrip() {
        for value in [0, 1, 0xFF, 0xFFFF, 0xFFFFFF] {
            let mut buf = [0u8; 3];
            write_u24_le(&mut buf, 0, value);
            let read = read_u24_le(&buf, 0);
            assert_eq!(read, value);
        }
    }

    #[test]
    fn test_write_u24_le_max_value() {
        // Test the maximum valid u24 value (does not overflow)
        let mut buf = [0u8; 3];
        write_u24_le(&mut buf, 0, 0xFFFFFF);
        let read = read_u24_le(&buf, 0);
        assert_eq!(read, 0xFFFFFF);
    }

    // =========================================================================
    // Callbacks Test
    // =========================================================================

    #[test]
    fn test_callbacks_extend_into() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        // Test None extends nothing
        let mut dst = Vec::new();
        let c: Callbacks<C> = Callbacks::None;
        c.extend_into(&mut dst);
        assert!(dst.is_empty());

        // Test One extends one callback
        let (tx, _rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
        let c = Callbacks::One(IOFlushed::<C>::signal(tx));
        c.extend_into(&mut dst);
        assert_eq!(dst.len(), 1);
    }

    // =========================================================================
    // Clone Independence Test
    // =========================================================================

    #[test]
    fn test_group_log_storage_clone_independent() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group1 = storage.get_log_storage(1).await.unwrap();
            let mut group2 = group1.clone();

            // Write via group1
            let v = openraft::impls::Vote::<C> {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            };
            group1.save_vote(&v).await.unwrap();

            // Read via group2 - should see the same state
            let vote = group2.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 1);

            storage.stop();
        });
    }

    // =========================================================================
    // Concurrent Access Test
    // =========================================================================

    #[test]
    fn test_concurrent_group_access() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            use std::collections::HashSet;
            use std::sync::Arc;

            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = Arc::new(PerGroupLogStorage::<C>::new(config).await.unwrap());

            let mut handles = vec![];

            // Spawn multiple tasks accessing different groups concurrently
            for group_id in 0..10u64 {
                let storage = storage.clone();
                let handle = tokio::spawn(async move {
                    let mut group = storage.get_log_storage(group_id).await.unwrap();

                    // Write vote
                    let v = openraft::impls::Vote::<C> {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: 1,
                            node_id: group_id,
                        },
                        committed: true,
                    };
                    group.save_vote(&v).await.unwrap();

                    // Write entries
                    for i in 1..=5 {
                        let e = openraft::impls::Entry::<C> {
                            log_id: LogId {
                                leader_id: openraft::impls::leader_id_adv::LeaderId {
                                    term: 1,
                                    node_id: group_id,
                                },
                                index: i,
                            },
                            payload: openraft::EntryPayload::Normal(TestData(
                                format!("group-{}-entry-{}", group_id, i).into_bytes(),
                            )),
                        };
                        let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
                        group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
                        let _ = rx.await;
                    }

                    // Verify
                    let vote = group.read_vote().await.unwrap().unwrap();
                    assert_eq!(vote.leader_id.node_id, group_id);

                    let entries = group.try_get_log_entries(1u64..=5u64).await.unwrap();
                    assert_eq!(entries.len(), 5);
                });
                handles.push(handle);
            }

            // Wait for all tasks
            for handle in handles {
                handle.await.unwrap();
            }

            // Verify all groups exist
            let group_ids: HashSet<_> = storage.group_ids().into_iter().collect();
            for id in 0..10u64 {
                assert!(group_ids.contains(&id), "group {} should exist", id);
            }

            storage.stop();
        });
    }

    // =========================================================================
    // Boundary Condition Tests
    // =========================================================================

    #[test]
    fn test_large_entry() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path())
                .with_fsync_interval(None)
                .with_max_record_size(512 * 1024); // 512KB max

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut group = storage.get_log_storage(1).await.unwrap();

            // Write a large entry (100KB)
            let large_data = vec![0x55u8; 100 * 1024];
            let e = openraft::impls::Entry::<C> {
                log_id: LogId {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: 1,
                },
                payload: openraft::EntryPayload::Normal(TestData(large_data.clone())),
            };
            let (tx, rx) = <<C as RaftTypeConfig>::AsyncRuntime as openraft::type_config::async_runtime::AsyncRuntime>::Oneshot::channel();
            group.append(vec![e], IOFlushed::signal(tx)).await.unwrap();
            let _ = rx.await;

            // Read it back
            let entries = group.try_get_log_entries(1u64..=1u64).await.unwrap();
            assert_eq!(entries.len(), 1);

            if let openraft::EntryPayload::Normal(data) = &entries[0].payload {
                assert_eq!(data.0.len(), 100 * 1024);
            } else {
                panic!("Expected Normal payload");
            }

            storage.stop();
        });
    }

    #[test]
    fn test_zero_group_id() {
        type C = crate::multi::type_config::ManiacRaftTypeConfig<TestData, ()>;

        crate::multi::test_support::run_async(async {
            let dir = TempDir::new().unwrap();
            let config = StorageConfig::new(dir.path()).with_fsync_interval(None);

            let storage = PerGroupLogStorage::<C>::new(config).await.unwrap();

            // Group ID 0 should work
            let mut group = storage.get_log_storage(0).await.unwrap();

            let v = openraft::impls::Vote::<C> {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            };
            group.save_vote(&v).await.unwrap();

            let vote = group.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 1);

            storage.stop();
        });
    }
}
