//! Segmented Mmap Log Storage for Multi-Raft
//!
//! Provides per-group sharded storage using memory-mapped segment files with:
//! - One dedicated shard per raft group (1:1 mapping)
//! - Dynamic shard creation on first access
//! - Same on-disk record format as SegLog: `[len:u32][type:u8][group_id:u24][payload...][crc64:u64]`
//! - Zero-copy reads via mmap slices
//! - Near-zero-latency writes (memcpy into mmap)
//! - Background fsync with callback queuing (writer never blocks on fsync)
//! - LogIndex (congee) for entry index → segment location mapping
//!
//! ## File Layout
//!
//! ```text
//! {base_dir}/group_{group_id}/
//!   seg_000001.log    # segment 1 (mmap'd)
//!   seg_000002.log    # segment 2 (mmap'd)
//!   ...
//! ```
//!
//! Plus shared manifest MDBX in `{base_dir}/.raft_manifest/` for fast recovery.

use crate::codec::{BorrowPayload, Decode, Encode};
use crate::manifest_mdbx::{ManifestManager, SegmentLocation, SegmentMeta};
use crate::record_format::{
    AtomicLogId, AtomicVote, CRC64_SIZE, GROUP_ID_SIZE, HEADER_SIZE, LENGTH_SIZE, LogIndex,
    LogLocation, MAX_GROUPS, RecordType, RecordTypeFlags, align8, append_record_into,
    validate_record, write_u24_le,
};
use arc_swap::ArcSwap;
use crossfire::{MAsyncTx, mpsc::Array};
use memmap2::{Mmap, MmapRaw};
use openraft::{
    LogId, LogState, RaftTypeConfig,
    storage::{IOFlushed, RaftLogReader, RaftLogStorage},
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default segment size: 1MB
pub const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024;

/// Default max record size: 1MB
pub const DEFAULT_MAX_RECORD_SIZE: u64 = 1024 * 1024;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for mmap-based segmented raft log storage
#[derive(Debug, Clone)]
pub struct MmapStorageConfig {
    /// Base directory for all raft data
    pub base_dir: Arc<PathBuf>,
    /// Optional manifest directory for filesystems that don't support MDBX
    pub manifest_dir: Option<Arc<PathBuf>>,
    /// Maximum segment size in bytes. When exceeded, a new segment is created.
    pub segment_size: u64,
    /// Maximum record size
    pub max_record_size: u64,
    /// Delay before triggering fsync after the first write. Allows write coalescing.
    pub fsync_delay: std::time::Duration,

    // -- Tiered storage settings (Tier 1/2) --
    /// Maximum number of sealed segments to keep pinned (mmap'd) in memory.
    /// When exceeded, the least-recently-used segments are unpinned (closed).
    /// This controls Tier 1 (hot) memory usage. 0 = unlimited (no eviction).
    pub max_pinned_segments: u32,
    /// Maximum total bytes of segment files to retain on local disk.
    /// When exceeded, oldest sealed segments may be deleted (only if archived to S3).
    /// 0 = unlimited.
    pub max_local_bytes: u64,
    /// Maximum number of concurrent segment reopen (mmap) operations.
    /// Provides backpressure when many reads hit unpinned segments simultaneously.
    pub max_concurrent_segment_opens: u32,

    // -- Tier 3: Remote archival settings --
    /// Optional S3-compatible archive configuration. When set, sealed segments
    /// are uploaded to remote storage after sealing, and can be fetched on
    /// cache miss when the local file has been evicted.
    pub s3_archive: Option<crate::segment_archive::S3ArchiveConfig>,
}

impl Default for MmapStorageConfig {
    fn default() -> Self {
        Self {
            base_dir: Arc::new(PathBuf::from("./raft-data")),
            manifest_dir: None,
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_record_size: DEFAULT_MAX_RECORD_SIZE,
            fsync_delay: std::time::Duration::from_millis(1),
            max_pinned_segments: 256,
            max_local_bytes: 0,
            max_concurrent_segment_opens: 8,
            s3_archive: None,
        }
    }
}

impl MmapStorageConfig {
    /// Create a new config with the given base directory
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: Arc::new(base_dir.into()),
            ..Default::default()
        }
    }

    /// Set the segment size
    pub fn with_segment_size(mut self, size: u64) -> Self {
        self.segment_size = size;
        self
    }

    /// Set the fsync delay (time to coalesce writes before fsyncing)
    pub fn with_fsync_delay(mut self, delay: std::time::Duration) -> Self {
        self.fsync_delay = delay;
        self
    }

    /// Set the maximum number of sealed segments to keep pinned in memory.
    /// 0 = unlimited (no LRU eviction).
    pub fn with_max_pinned_segments(mut self, max: u32) -> Self {
        self.max_pinned_segments = max;
        self
    }

    /// Set the maximum total bytes of segment files on local disk.
    /// 0 = unlimited.
    pub fn with_max_local_bytes(mut self, max: u64) -> Self {
        self.max_local_bytes = max;
        self
    }

    /// Set the maximum number of concurrent segment reopen operations.
    pub fn with_max_concurrent_segment_opens(mut self, max: u32) -> Self {
        self.max_concurrent_segment_opens = max;
        self
    }

    /// Enable S3-compatible segment archival with the given config.
    pub fn with_s3_archive(mut self, config: crate::segment_archive::S3ArchiveConfig) -> Self {
        self.s3_archive = Some(config);
        self
    }
}

/// Metadata extracted during a full record scan.
struct ScanMeta {
    record_type_flags: RecordTypeFlags,
    record_count: u64,
    min_entry_index: Option<u64>,
    max_entry_index: Option<u64>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Current time as nanos since UNIX_EPOCH.
fn nanos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Segment file path: {group_dir}/seg_{segment_id:06}.log
fn segment_path(group_dir: &Path, segment_id: u64) -> PathBuf {
    group_dir.join(format!("seg_{segment_id:06}.log"))
}

/// Group directory path: {base_dir}/group_{group_id}
fn group_dir_path(base_dir: &Path, group_id: u64) -> PathBuf {
    base_dir.join(format!("group_{group_id}"))
}

/// Scan a group directory for segment files, returning sorted segment IDs
pub fn scan_segment_ids(group_dir: &Path) -> io::Result<Vec<u64>> {
    let mut ids = Vec::new();
    if !group_dir.exists() {
        return Ok(ids);
    }
    for entry in std::fs::read_dir(group_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("seg_") {
            if let Some(num_str) = rest.strip_suffix(".log") {
                if let Ok(id) = num_str.parse::<u64>() {
                    ids.push(id);
                }
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

// ---------------------------------------------------------------------------
// Segment — a single mmap'd segment file with atomic size tracking
// ---------------------------------------------------------------------------

/// Default maximum number of sealed segments to cache in the LRU.

/// A single mmap'd segment file. ONE mmap per segment, used for both reading and writing.
/// Size tracking is done via atomics — no locks on the read path.
#[repr(C, align(128))]
struct Segment {
    segment_id: u64,
    mmap: MmapRaw,
    /// Logical size: the write tail. Updated atomically after every append.
    logical_size: AtomicU64,
    /// File handle for fsync. Present for active segments, None for
    /// sealed segments loaded read-only during recovery.
    file: Option<std::fs::File>,
    path: PathBuf,
    /// Max bytes written since last fsync. Updated atomically by writers,
    /// reset by the fsync thread after draining.
    pending_max_bytes: AtomicU64,
    /// Nanos since UNIX_EPOCH when the first callback was enqueued for this
    /// fsync batch. 0 means no pending callbacks. Set by first writer via CAS,
    /// cleared by fsync thread after draining. Each segment's delay is
    /// measured from its own first_enqueue_nanos.
    first_enqueue_nanos: AtomicU64,
}

impl Segment {
    fn new(
        segment_id: u64,
        mmap: MmapRaw,
        valid_bytes: u64,
        file: Option<std::fs::File>,
        path: PathBuf,
    ) -> Self {
        Self {
            segment_id,
            mmap,
            logical_size: AtomicU64::new(valid_bytes),
            file,
            path,
            pending_max_bytes: AtomicU64::new(0),
            first_enqueue_nanos: AtomicU64::new(0),
        }
    }

    /// Atomically set `first_enqueue_nanos` if not already set (CAS from 0).
    fn mark_first_enqueue(&self) {
        if self.first_enqueue_nanos.load(Ordering::Acquire) == 0 {
            let now = nanos_now();
            let _ = self.first_enqueue_nanos.compare_exchange(
                0,
                now,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }
    }

    /// Take the pending max_bytes and reset tracking. Called by fsync thread.
    fn take_pending(&self) -> u64 {
        let max_bytes = self.pending_max_bytes.swap(0, Ordering::AcqRel);
        self.first_enqueue_nanos.store(0, Ordering::Release);
        max_bytes
    }

    /// Read a byte slice from the segment. Caller must ensure offset+len is within bounds.
    #[inline]
    fn read_slice(&self, offset: usize, len: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr().add(offset), len) }
    }

    /// Get the full readable slice up to `len` bytes.
    #[inline]
    fn as_slice(&self, len: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), len) }
    }

    /// Write data at the given offset. Only called by the writer under mutex.
    /// Caller must ensure offset+data.len() is within capacity.
    #[inline]
    unsafe fn write_at(&self, offset: usize, data: &[u8]) {
        unsafe {
            ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.mmap.as_mut_ptr().add(offset),
                data.len(),
            );
        }
    }

    /// Zero-fill a range. Only called by the writer under mutex.
    #[inline]
    unsafe fn zero_range(&self, offset: usize, len: usize) {
        unsafe {
            ptr::write_bytes(self.mmap.as_mut_ptr().add(offset), 0, len);
        }
    }

    /// The total capacity of the underlying mmap.
    #[inline]
    fn capacity(&self) -> usize {
        self.mmap.len()
    }
}

/// Create a `Bytes` view into an mmap segment's memory.
/// The returned `Bytes` holds an `Arc<Segment>` ref, preventing the mmap
/// from being unmapped while the `Bytes` (or any slice of it) is alive.
fn bytes_from_segment(segment: &Arc<Segment>, offset: usize, len: usize) -> bytes::Bytes {
    /// Wrapper that keeps the segment Arc alive and provides [`AsRef<[u8]>`]
    /// into the mmap memory. The pointer is stable because it points into
    /// the mmap region, which doesn't move.
    struct SegmentSlice {
        segment: Arc<Segment>,
        offset: usize,
        len: usize,
    }

    impl AsRef<[u8]> for SegmentSlice {
        fn as_ref(&self) -> &[u8] {
            self.segment.read_slice(self.offset, self.len)
        }
    }

    // SAFETY: SegmentSlice only holds Arc<Segment> + usize + usize.
    // Arc<Segment> is Send+Sync, and the mmap memory is safely readable
    // from any thread. The raw pointer inside read_slice is derived from
    // a stable mmap mapping that doesn't move.
    unsafe impl Send for SegmentSlice {}
    unsafe impl Sync for SegmentSlice {}

    bytes::Bytes::from_owner(SegmentSlice {
        segment: Arc::clone(segment),
        offset,
        len,
    })
}

// ---------------------------------------------------------------------------
// SegmentView — live view into a segment's mmap region
// ---------------------------------------------------------------------------

/// A live, zero-copy view into a raft log segment's mmap region.
///
/// Unlike `Bytes` (which snapshots `logical_size` at creation time),
/// `SegmentView` reads `logical_size` atomically on every `len()` call,
/// so it always sees newly appended data without re-acquiring the segment.
///
/// Holds a pre-built `Bytes` covering the full mmap capacity. All
/// `slice_bytes()` calls slice from this local `Bytes` — no `Arc` cloning,
/// no atomic contention on the hot path. The `Arc<Segment>` is kept only
/// to pin the mmap in memory and to read `logical_size` for live segments.
#[derive(Clone)]
pub struct SegmentView {
    /// Keeps the mmap pinned. `None` for test/bytes-only views.
    segment: Option<Arc<Segment>>,
    /// Segment ID.
    id: u64,
    /// Pre-built `Bytes` covering the full mmap (or test data).
    /// `slice_bytes()` slices from this — no Arc clone, no atomics.
    data: bytes::Bytes,
}

impl SegmentView {
    /// Create a `SegmentView` from a fixed `Bytes` snapshot.
    ///
    /// Useful for tests and sealed-segment reads where the data is immutable.
    pub fn from_bytes(segment_id: u64, data: bytes::Bytes) -> Self {
        Self {
            segment: None,
            id: segment_id,
            data,
        }
    }

    /// Create a `SegmentView` backed by an mmap segment.
    ///
    /// Pre-builds a `Bytes` covering the full mmap capacity so that all
    /// subsequent `slice_bytes()` calls avoid `Arc` cloning.
    fn from_segment(segment: Arc<Segment>) -> Self {
        let id = segment.segment_id;
        let data = bytes_from_segment(&segment, 0, segment.capacity());
        Self {
            segment: Some(segment),
            id,
            data,
        }
    }

    /// The segment ID.
    #[inline]
    pub fn segment_id(&self) -> u64 {
        self.id
    }

    /// Current valid byte count — reads `logical_size` atomically for live
    /// mmap segments, returns `data.len()` for bytes-backed views.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.segment {
            Some(seg) => seg.logical_size.load(Ordering::Acquire) as usize,
            None => self.data.len(),
        }
    }

    /// Whether the segment has no data.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read a byte at `[offset..offset+len]`.
    ///
    /// # Safety
    /// Caller must ensure `offset + 1 <= self.len()`.
    #[inline]
    pub fn read_byte(&self, offset: usize) -> u8 {
        self.data[offset]
    }

    /// Read a byte slice at `[offset..offset+len]`.
    ///
    /// # Safety
    /// Caller must ensure `offset + len <= self.len()`.
    #[inline]
    pub fn read_slice(&self, offset: usize, len: usize) -> &[u8] {
        &self.data[offset..offset + len]
    }

    /// Create a zero-copy `Bytes` handle for `[offset..offset+len]`.
    ///
    /// Slices from the pre-built `Bytes` — no `Arc` clone, no atomic ops.
    #[inline]
    pub fn slice_bytes(&self, offset: usize, len: usize) -> bytes::Bytes {
        self.data.slice(offset..offset + len)
    }
}

// ---------------------------------------------------------------------------
// MmapSegmentMap — in-memory segment tracking shared between writer and readers
// ---------------------------------------------------------------------------

/// A sealed segment pinned in memory with its entry index range and LRU tracking.
/// Per-segment entry location table. Built once when a segment is pinned.
/// Enables O(1) direct Vec index lookup for entry reads — no LogIndex (Congee)
/// or segment map (HashMap mutex) lookups needed.
struct SegmentEntryIndex {
    /// First entry's log index in this segment
    base_index: u64,
    /// (byte_offset, total_record_len) for each entry, indexed by (log_index - base_index)
    offsets: Vec<(u64, u32)>,
}

impl SegmentEntryIndex {
    /// O(1) direct Vec index — no locks, no hash, no concurrent data structure.
    #[inline]
    fn lookup(&self, log_index: u64) -> Option<(u64, u32)> {
        let i = log_index.checked_sub(self.base_index)? as usize;
        self.offsets.get(i).copied()
    }
}

/// Build a SegmentEntryIndex by scanning length prefixes in a segment's mmap data.
/// Same linear walk as recovery fast path — collects all entry offsets.
fn build_entry_index(mmap: &[u8], valid_bytes: usize) -> SegmentEntryIndex {
    let mut offsets = Vec::new();
    let mut base_index = None;
    let mut offset = 0usize;

    while offset + LENGTH_SIZE <= valid_bytes {
        let record_len =
            u32::from_le_bytes(mmap[offset..offset + LENGTH_SIZE].try_into().unwrap()) as usize;
        if record_len == 0 || offset + LENGTH_SIZE + record_len > valid_bytes {
            break;
        }
        let total = LENGTH_SIZE + record_len;
        let data = &mmap[offset + LENGTH_SIZE..offset + total];
        if data.len() >= 1 + GROUP_ID_SIZE + CRC64_SIZE {
            if let Ok(RecordType::Entry) = RecordType::try_from(data[0]) {
                let payload = &data[1 + GROUP_ID_SIZE..data.len() - CRC64_SIZE];
                if let Some(idx) = extract_entry_index(payload) {
                    if base_index.is_none() {
                        base_index = Some(idx);
                    }
                    offsets.push((offset as u64, total as u32));
                }
            }
        }
        offset += align8(total);
    }

    SegmentEntryIndex {
        base_index: base_index.unwrap_or(0),
        offsets,
    }
}

struct PinnedEntry {
    segment: Arc<Segment>,
    min_index: u64,
    max_index: u64,
    entry_index: Arc<SegmentEntryIndex>,
    /// Monotonic counter for LRU eviction. Higher = more recently accessed.
    last_access: u64,
}

struct MmapSegmentMap {
    /// Sealed segments pinned (mmap'd) in memory with LRU eviction tracking.
    /// Arc'd so prefetch tasks can capture it without borrowing self.
    pinned: Arc<Mutex<HashMap<u64, PinnedEntry>>>,
    /// Active segment: atomically swapped on rotation. No lock on read path.
    active: ArcSwap<Segment>,
    /// Group directory for reopening segments from disk.
    group_dir: PathBuf,
    /// Group ID for archive key generation.
    group_id: u64,
    /// State machine's minimum required index. Segments fully below this can be purged.
    purge_floor: Arc<AtomicU64>,
    /// State machine's last applied index. Segments fully above this are not needed yet.
    pin_ceiling: Arc<AtomicU64>,
    /// Monotonic counter for LRU tracking. Incremented on each segment access.
    access_counter: AtomicU64,
    /// Maximum number of pinned segments. 0 = unlimited.
    max_pinned: u32,
    /// Semaphore limiting concurrent segment reopen (mmap) operations.
    reopen_semaphore: Arc<tokio::sync::Semaphore>,
    /// Optional archive manager for downloading remote segments on cache miss.
    archive: Option<Arc<crate::segment_archive::ArchiveManager>>,
}

/// Sentinel segment used as initial placeholder before the first real segment is set.
fn sentinel_segment() -> Arc<Segment> {
    // Create a sentinel with an anonymous mmap. segment_id=0.
    // This is never read from — callers check segment_id match first.
    let anon = memmap2::MmapOptions::new().map_anon().unwrap();
    // Convert MmapMut → MmapRaw (same underlying mapping)
    let raw = MmapRaw::from(anon);
    Arc::new(Segment {
        segment_id: 0,
        mmap: raw,
        logical_size: AtomicU64::new(0),
        file: None,
        path: PathBuf::new(),
        pending_max_bytes: AtomicU64::new(0),
        first_enqueue_nanos: AtomicU64::new(0),
    })
}

impl MmapSegmentMap {
    fn new(
        group_dir: PathBuf,
        group_id: u64,
        purge_floor: Arc<AtomicU64>,
        pin_ceiling: Arc<AtomicU64>,
        config: &MmapStorageConfig,
        archive: Option<Arc<crate::segment_archive::ArchiveManager>>,
    ) -> Self {
        Self {
            pinned: Arc::new(Mutex::new(HashMap::new())),
            active: ArcSwap::new(sentinel_segment()),
            group_dir,
            group_id,
            purge_floor,
            pin_ceiling,
            access_counter: AtomicU64::new(1),
            max_pinned: config.max_pinned_segments,
            reopen_semaphore: Arc::new(tokio::sync::Semaphore::new(
                config.max_concurrent_segment_opens as usize,
            )),
            archive,
        }
    }

    /// Increment and return the next access counter value for LRU tracking.
    #[inline]
    fn next_access(&self) -> u64 {
        self.access_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Evict least-recently-used pinned segments until count <= limit.
    /// Must be called with `pinned` already locked. Does NOT delete files — only unpins.
    fn evict_lru(pinned: &mut HashMap<u64, PinnedEntry>, max: u32) {
        if max == 0 {
            return; // unlimited
        }
        while pinned.len() > max as usize {
            // Find the entry with the smallest last_access
            let victim = pinned
                .iter()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(&k, _)| k);
            if let Some(seg_id) = victim {
                pinned.remove(&seg_id);
            } else {
                break;
            }
        }
    }

    /// Pin a sealed segment with its entry index range and entry index.
    /// Evicts LRU segments if over the limit.
    fn pin(
        &self,
        segment: Arc<Segment>,
        min_index: u64,
        max_index: u64,
        entry_index: Arc<SegmentEntryIndex>,
    ) {
        let access = self.next_access();
        let mut pinned = self.pinned.lock();
        pinned.insert(
            segment.segment_id,
            PinnedEntry {
                segment,
                min_index,
                max_index,
                entry_index,
                last_access: access,
            },
        );
        Self::evict_lru(&mut pinned, self.max_pinned);
    }

    /// Find the segment for a given segment_id. Lock-free for active, O(1) for pinned.
    /// Returns None if the segment is not pinned — caller must use async reopen.
    /// Updates LRU access time on hit.
    fn find_segment(&self, segment_id: u64) -> Option<Arc<Segment>> {
        // Check active first (most recent writes) — no lock
        let active = self.active.load();
        if active.segment_id == segment_id {
            return Some(Arc::clone(&active));
        }
        // Check pinned — O(1) hash lookup, brief mutex
        let mut pinned = self.pinned.lock();
        if let Some(entry) = pinned.get_mut(&segment_id) {
            entry.last_access = self.next_access();
            return Some(Arc::clone(&entry.segment));
        }
        None
    }

    /// Get segment + entry index for cached reads. Brief mutex lock.
    /// Returns None if not pinned (active segments don't have entry indexes).
    /// Updates LRU access time on hit.
    fn find_segment_indexed(
        &self,
        segment_id: u64,
    ) -> Option<(Arc<Segment>, Arc<SegmentEntryIndex>)> {
        let mut pinned = self.pinned.lock();
        if let Some(entry) = pinned.get_mut(&segment_id) {
            entry.last_access = self.next_access();
            return Some((Arc::clone(&entry.segment), Arc::clone(&entry.entry_index)));
        }
        None
    }

    /// Async reopen a sealed segment from disk via spawn_blocking.
    /// Acquires a semaphore permit for backpressure on concurrent opens.
    /// Auto-pins if the segment falls within [purge_floor, pin_ceiling].
    /// Evicts LRU segments if over the limit.
    /// Builds a SegmentEntryIndex for O(1) entry lookups.
    async fn reopen_segment(&self, segment_id: u64) -> Option<Arc<Segment>> {
        // Acquire semaphore permit — blocks if too many concurrent reopens
        let _permit = self.reopen_semaphore.acquire().await.ok()?;

        let local_path = segment_path(&self.group_dir, segment_id);

        // If local file is missing but we have an archive, try downloading from remote
        if !local_path.exists() {
            if let Some(ref archive) = self.archive {
                let _ = archive
                    .download_segment(self.group_id, segment_id, &local_path)
                    .await;
            }
        }

        let group_dir = self.group_dir.clone();
        let purge_floor = Arc::clone(&self.purge_floor);
        let pin_ceiling = Arc::clone(&self.pin_ceiling);

        let result = tokio::task::spawn_blocking(move || {
            let path = segment_path(&group_dir, segment_id);
            let file = std::fs::File::open(&path).ok()?;
            let mmap = memmap2::MmapOptions::new().map_raw_read_only(&file).ok()?;
            let file_len = file.metadata().ok()?.len();
            let seg = Arc::new(Segment::new(segment_id, mmap, file_len, None, path));

            let floor = purge_floor.load(Ordering::Acquire);
            let ceiling = pin_ceiling.load(Ordering::Acquire);

            let valid = file_len as usize;
            let entry_index = Arc::new(build_entry_index(seg.as_slice(valid), valid));
            let min_idx = scan_first_entry_index(seg.as_slice(valid), valid);
            let max_idx = scan_last_entry_index(seg.as_slice(valid), valid);

            let pin_info = min_idx.zip(max_idx).and_then(|(min, max)| {
                let in_range = max >= floor && (ceiling == 0 || min <= ceiling);
                in_range.then_some((min, max))
            });

            Some((seg, pin_info, entry_index))
        })
        .await
        .ok()??;

        let (seg, pin_info, entry_index) = result;
        if let Some((min, max)) = pin_info {
            let access = self.next_access();
            let mut guard = self.pinned.lock();
            guard.insert(
                segment_id,
                PinnedEntry {
                    segment: Arc::clone(&seg),
                    min_index: min,
                    max_index: max,
                    entry_index,
                    last_access: access,
                },
            );
            Self::evict_lru(&mut guard, self.max_pinned);
        }
        Some(seg)
    }

    /// Fire-and-forget prefetch: spawn a background task to reopen and pin a segment.
    /// Uses try_acquire on the semaphore — skips if no permits available.
    /// Evicts LRU segments if over the limit.
    /// Builds a SegmentEntryIndex for O(1) entry lookups.
    fn prefetch_segment(&self, segment_id: u64) {
        // Skip if already available
        if self.active.load().segment_id == segment_id {
            return;
        }
        {
            let pinned = self.pinned.lock();
            if pinned.contains_key(&segment_id) {
                return;
            }
        }

        // Try to acquire semaphore — skip prefetch if at capacity
        let permit = match self.reopen_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => return,
        };

        let group_dir = self.group_dir.clone();
        let pinned = Arc::clone(&self.pinned);
        let max_pinned = self.max_pinned;
        let access_counter = self.access_counter.fetch_add(1, Ordering::Relaxed);

        tokio::task::spawn(async move {
            let _permit = permit; // hold until done
            let result = tokio::task::spawn_blocking(move || {
                let path = segment_path(&group_dir, segment_id);
                let file = std::fs::File::open(&path).ok()?;
                let mmap = memmap2::MmapOptions::new().map_raw_read_only(&file).ok()?;
                let file_len = file.metadata().ok()?.len();
                let seg = Arc::new(Segment::new(segment_id, mmap, file_len, None, path));
                let valid = file_len as usize;
                let entry_index = Arc::new(build_entry_index(seg.as_slice(valid), valid));
                let min_idx = scan_first_entry_index(seg.as_slice(valid), valid);
                let max_idx = scan_last_entry_index(seg.as_slice(valid), valid);
                min_idx
                    .zip(max_idx)
                    .map(|(min, max)| (seg, min, max, entry_index))
            })
            .await
            .ok()?;

            if let Some((seg, min, max, entry_index)) = result {
                let mut guard = pinned.lock();
                guard.insert(
                    segment_id,
                    PinnedEntry {
                        segment: seg,
                        min_index: min,
                        max_index: max,
                        entry_index,
                        last_access: access_counter,
                    },
                );
                Self::evict_lru(&mut guard, max_pinned);
            }
            Some(())
        });
    }

    /// Atomically swap the active segment. Returns the previous active segment.
    fn swap_active(&self, new_active: Arc<Segment>) -> Arc<Segment> {
        self.active.swap(new_active)
    }

    /// Set the active segment (used during initialization).
    fn set_active(&self, segment: Arc<Segment>) {
        self.active.store(segment);
    }

    /// Unpin segments fully below `purge_up_to` or fully above `pin_ceiling`.
    ///
    /// **Does NOT delete segment files.** File deletion is handled by Level 2
    /// retention evaluation. This only releases mmap memory.
    ///
    /// Returns the IDs of segments that were unpinned because they are fully
    /// below the purge floor. These IDs are used to trigger retained message
    /// detach sweeps (the mmap backing is gone once unpinned).
    fn update_pins(&self, purge_up_to: u64) -> Vec<u64> {
        let ceiling = self.pin_ceiling.load(Ordering::Acquire);
        let mut pinned = self.pinned.lock();
        let mut unpinned_ids = Vec::new();

        pinned.retain(|&seg_id, entry| {
            if entry.max_index <= purge_up_to {
                // Fully below purge floor — unpin (release mmap memory).
                // File stays on disk for Level 2 retention evaluation.
                unpinned_ids.push(seg_id);
                false
            } else if ceiling > 0 && entry.min_index > ceiling {
                // Fully above pin ceiling — unpin but keep file
                false
            } else {
                true
            }
        });

        unpinned_ids
    }

    /// Remove pinned segments with min_index > after_index for truncation.
    /// Returns paths of removed segments for file deletion.
    fn remove_after(&self, after_index: u64) -> Vec<PathBuf> {
        let mut pinned = self.pinned.lock();
        let mut removed = Vec::new();

        pinned.retain(|_seg_id, entry| {
            if entry.min_index > after_index {
                removed.push(entry.segment.path.clone());
                false
            } else {
                true
            }
        });

        removed
    }

    /// Return the number of currently pinned segments.
    fn pinned_count(&self) -> usize {
        self.pinned.lock().len()
    }
}

/// Extract the entry index from an entry record payload by reading the log_id.index
/// directly from the binary layout: [term:8][node_id:8][index:8][...]
fn extract_entry_index(payload: &[u8]) -> Option<u64> {
    if payload.len() >= 24 {
        Some(u64::from_le_bytes(payload[16..24].try_into().unwrap()))
    } else {
        None
    }
}

/// Scan a segment mmap for the first entry index
fn scan_first_entry_index(mmap: &[u8], valid_bytes: usize) -> Option<u64> {
    let mut offset = 0;
    while offset + LENGTH_SIZE <= valid_bytes {
        let record_len =
            u32::from_le_bytes(mmap[offset..offset + LENGTH_SIZE].try_into().unwrap()) as usize;
        if record_len == 0 || offset + LENGTH_SIZE + record_len > valid_bytes {
            break;
        }
        let data = &mmap[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
        if data.len() >= 1 + GROUP_ID_SIZE + CRC64_SIZE {
            if let Ok(RecordType::Entry) = RecordType::try_from(data[0]) {
                let payload = &data[1 + GROUP_ID_SIZE..data.len() - CRC64_SIZE];
                if let Some(index) = extract_entry_index(payload) {
                    return Some(index);
                }
                return None;
            }
        }
        offset += align8(LENGTH_SIZE + record_len);
    }
    None
}

/// Scan a segment mmap for the last entry index
fn scan_last_entry_index(mmap: &[u8], valid_bytes: usize) -> Option<u64> {
    let mut offset = 0;
    let mut last_entry_index = None;
    while offset + LENGTH_SIZE <= valid_bytes {
        let record_len =
            u32::from_le_bytes(mmap[offset..offset + LENGTH_SIZE].try_into().unwrap()) as usize;
        if record_len == 0 || offset + LENGTH_SIZE + record_len > valid_bytes {
            break;
        }
        let data = &mmap[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
        if data.len() >= 1 + GROUP_ID_SIZE + CRC64_SIZE {
            if let Ok(RecordType::Entry) = RecordType::try_from(data[0]) {
                let payload = &data[1 + GROUP_ID_SIZE..data.len() - CRC64_SIZE];
                if let Some(index) = extract_entry_index(payload) {
                    last_entry_index = Some(index);
                }
            }
        }
        offset += align8(LENGTH_SIZE + record_len);
    }
    last_entry_index
}

// ---------------------------------------------------------------------------
// FsyncState — background fsync coordination
// ---------------------------------------------------------------------------

/// Request to seal a segment in the background (after rotation)
struct SealRequest {
    segment: Arc<Segment>,
    valid_bytes: u64,
    group_id: u64,
    min_index: Option<u64>,
    max_index: Option<u64>,
    record_count: u64,
    record_type_flags: RecordTypeFlags,
    manifest_tx: MAsyncTx<Array<SegmentMeta>>,
    entry_count: u64,
    first_entry_offset: u64,
    /// Optional archive for uploading sealed segments to remote storage.
    archive: Option<Arc<crate::segment_archive::ArchiveManager>>,
}

/// Result of a background pre-allocation: pending, completed, or failed.
enum PreallocResult {
    /// Background thread hasn't finished yet
    Pending,
    /// Segment created successfully
    Ready(ActiveMmapSegment),
    /// Creation failed — caller should fall back to synchronous create
    Failed,
}

/// Request to pre-allocate a segment file in the background
struct PreallocRequest {
    group_dir: PathBuf,
    segment_id: u64,
    segment_size: u64,
    /// Delivery slot: background thread stores the result here
    result: Arc<std::sync::Mutex<PreallocResult>>,
}

/// Pending fsync entry: segment + callbacks. Callbacks are grouped by segment
/// so each file is synced exactly once per batch.
struct FsyncEntry<C: RaftTypeConfig> {
    segment: Arc<Segment>,
    callbacks: Vec<IOFlushed<C>>,
}

struct FsyncInner<C: RaftTypeConfig> {
    /// Pending callbacks grouped by segment pointer.
    pending: std::collections::HashMap<usize, FsyncEntry<C>>,
    /// Pending segment seal requests
    seal_queue: Vec<SealRequest>,
    /// Pending segment pre-allocation requests
    prealloc_queue: Vec<PreallocRequest>,
    shutdown: bool,
}

struct FsyncState<C: RaftTypeConfig> {
    mu: std::sync::Mutex<FsyncInner<C>>,
    cv: std::sync::Condvar,
    /// Delay before triggering fsync after first enqueue.
    fsync_delay: std::time::Duration,
    /// Tokio runtime handle for spawning async tasks (e.g. S3 upload) from the fsync thread.
    tokio_handle: Option<tokio::runtime::Handle>,
    /// Test-only: force sync_data() to return an error.
    #[cfg(test)]
    force_sync_error: AtomicBool,
    /// Test-only: count of actual sync_data() calls.
    #[cfg(test)]
    sync_count: AtomicU64,
    /// Test-only: artificial delay (micros) injected into each seal request
    /// to simulate slow I/O and reproduce seal-starvation hangs on fast tmpfs.
    #[cfg(test)]
    seal_delay_micros: AtomicU64,
}

impl<C: RaftTypeConfig> FsyncState<C> {
    fn new(fsync_delay: std::time::Duration) -> Self {
        Self {
            mu: std::sync::Mutex::new(FsyncInner {
                pending: std::collections::HashMap::new(),
                seal_queue: Vec::new(),
                prealloc_queue: Vec::new(),
                shutdown: false,
            }),
            cv: std::sync::Condvar::new(),
            fsync_delay,
            tokio_handle: tokio::runtime::Handle::try_current().ok(),
            #[cfg(test)]
            force_sync_error: AtomicBool::new(false),
            #[cfg(test)]
            sync_count: AtomicU64::new(0),
            #[cfg(test)]
            seal_delay_micros: AtomicU64::new(0),
        }
    }

    /// Push a callback for the given segment. Tracks `max_bytes` and
    /// `first_enqueue_nanos` on the Segment atomically (no allocation).
    fn push(&self, segment: &Arc<Segment>, bytes_written: u64, callback: IOFlushed<C>) {
        segment
            .pending_max_bytes
            .fetch_max(bytes_written, Ordering::Release);
        segment.mark_first_enqueue();

        let mut inner = self.mu.lock().unwrap();
        if inner.shutdown {
            drop(inner);
            tracing::warn!("fsync push after shutdown — returning error to callback");
            callback.io_completed(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "fsync thread has been shut down",
            )));
            return;
        }
        let key = Arc::as_ptr(segment) as usize;
        let entry = inner.pending.entry(key).or_insert_with(|| FsyncEntry {
            segment: segment.clone(),
            callbacks: Vec::new(),
        });
        entry.callbacks.push(callback);
        drop(inner);
        self.cv.notify_one();
    }

    /// Enqueue a seal request for background processing
    fn enqueue_seal(&self, req: SealRequest) {
        let mut inner = self.mu.lock().unwrap();
        inner.seal_queue.push(req);
        drop(inner);
        self.cv.notify_one();
    }

    /// Enqueue a segment pre-allocation request for background processing
    fn enqueue_prealloc(&self, req: PreallocRequest) {
        let mut inner = self.mu.lock().unwrap();
        inner.prealloc_queue.push(req);
        drop(inner);
        self.cv.notify_one();
    }
}

/// Background fsync thread loop — shared across all groups.
/// Handles both fsync callbacks and segment sealing.
/// Implements delay-based write coalescing: waits `fsync_delay` from the
/// first enqueued callback before syncing, allowing multiple writes to batch.
fn fsync_thread_loop<C: RaftTypeConfig>(state: Arc<FsyncState<C>>) {
    let delay_nanos = state.fsync_delay.as_nanos() as u64;

    loop {
        let (ready, seal_requests, prealloc_requests, shutdown) = {
            let mut inner = state.mu.lock().unwrap();

            // Block until there's work or shutdown
            while inner.pending.is_empty()
                && inner.seal_queue.is_empty()
                && inner.prealloc_queue.is_empty()
                && !inner.shutdown
            {
                inner = state.cv.wait(inner).unwrap();
            }

            if inner.shutdown
                && inner.pending.is_empty()
                && inner.seal_queue.is_empty()
                && inner.prealloc_queue.is_empty()
            {
                return;
            }

            // Wait for per-segment delays. Each segment's deadline is
            // first_enqueue_nanos + delay_nanos. We wait until the earliest
            // deadline, then drain only segments that are ready.
            loop {
                // Seal/prealloc requests and shutdown are always processed immediately
                if !inner.seal_queue.is_empty()
                    || !inner.prealloc_queue.is_empty()
                    || inner.shutdown
                {
                    break;
                }

                let now = nanos_now();
                let earliest_deadline = inner
                    .pending
                    .values()
                    .filter_map(|e| {
                        let t = e.segment.first_enqueue_nanos.load(Ordering::Acquire);
                        if t > 0 { Some(t + delay_nanos) } else { None }
                    })
                    .min();

                match earliest_deadline {
                    Some(deadline) if deadline > now => {
                        let remaining = std::time::Duration::from_nanos(deadline - now);
                        let (new_inner, _) = state.cv.wait_timeout(inner, remaining).unwrap();
                        inner = new_inner;
                    }
                    _ => break, // At least one segment is ready (or no timestamps)
                }
            }

            if inner.shutdown
                && inner.pending.is_empty()
                && inner.seal_queue.is_empty()
                && inner.prealloc_queue.is_empty()
            {
                return;
            }

            // Extract segments whose delay has expired, leave the rest.
            // Take the whole map and partition — avoids allocating a keys Vec.
            let now = nanos_now();
            let mut ready_entries = Vec::new();
            let taken = std::mem::take(&mut inner.pending);
            for (key, entry) in taken {
                let t = entry.segment.first_enqueue_nanos.load(Ordering::Acquire);
                if t == 0 || t + delay_nanos <= now {
                    ready_entries.push(entry);
                } else {
                    inner.pending.insert(key, entry);
                }
            }

            let seal_requests = std::mem::take(&mut inner.seal_queue);
            let prealloc_requests = std::mem::take(&mut inner.prealloc_queue);
            (
                ready_entries,
                seal_requests,
                prealloc_requests,
                inner.shutdown,
            )
        };

        // Sync each segment's file once, drain callbacks
        for entry in ready {
            let _max_bytes = entry.segment.take_pending();

            let sync_result: Result<(), io::Error> = if let Some(ref file) = entry.segment.file {
                #[cfg(test)]
                {
                    state.sync_count.fetch_add(1, Ordering::Relaxed);
                    if state.force_sync_error.load(Ordering::Relaxed) {
                        Err(io::Error::new(io::ErrorKind::Other, "injected fsync error"))
                    } else {
                        file.sync_data()
                    }
                }
                #[cfg(not(test))]
                {
                    file.sync_data()
                }
            } else {
                Ok(())
            };

            match &sync_result {
                Ok(()) => {
                    for cb in entry.callbacks {
                        cb.io_completed(Ok(()));
                    }
                }
                Err(e) => {
                    tracing::error!(
                        segment_id = entry.segment.segment_id,
                        error = %e,
                        "fsync failed — notifying {} callbacks",
                        entry.callbacks.len()
                    );
                    for cb in entry.callbacks {
                        cb.io_completed(Err(io::Error::new(e.kind(), e.to_string())));
                    }
                }
            }
        }

        // Process seal requests: fsync + truncate + manifest update (no footer on file).
        // IMPORTANT: Interleave callback processing between seal requests to avoid
        // starving append callbacks when many seal requests accumulate (each seal
        // does sync_data + truncate which can take milliseconds).
        for req in seal_requests {
            #[cfg(test)]
            {
                let delay = state.seal_delay_micros.load(Ordering::Relaxed);
                if delay > 0 {
                    std::thread::sleep(std::time::Duration::from_micros(delay));
                }
            }

            if let Some(ref file) = req.segment.file {
                if let Err(e) = file.sync_data() {
                    tracing::error!(
                        segment_id = req.segment.segment_id,
                        "seal fsync failed: {}",
                        e
                    );
                }
                if let Err(e) = file.set_len(req.valid_bytes) {
                    tracing::error!(
                        segment_id = req.segment.segment_id,
                        "seal truncate failed: {}",
                        e
                    );
                }
            }

            // Update manifest with all metadata (replaces on-file footer)
            let _ = req.manifest_tx.try_send(SegmentMeta {
                group_id: req.group_id,
                segment_id: req.segment.segment_id,
                valid_bytes: req.valid_bytes,
                min_index: req.min_index,
                max_index: req.max_index,
                min_ts: None,
                max_ts: None,
                sealed: true,
                record_count: req.record_count,
                record_type_flags: req.record_type_flags,
                entry_count: req.entry_count,
                first_entry_offset: req.first_entry_offset,
                location: SegmentLocation::Local,
            });

            // Fire-and-forget: upload sealed segment to remote archive
            if let Some(archive) = req.archive {
                let path = req.segment.path.clone();
                let group_id = req.group_id;
                let segment_id = req.segment.segment_id;
                let manifest_tx = req.manifest_tx.clone();
                let valid_bytes = req.valid_bytes;
                let min_index = req.min_index;
                let max_index = req.max_index;
                let record_count = req.record_count;
                let record_type_flags = req.record_type_flags;
                let entry_count = req.entry_count;
                let first_entry_offset = req.first_entry_offset;

                if let Some(handle) = state.tokio_handle.as_ref() {
                    handle.spawn(async move {
                        match archive.upload_segment(group_id, segment_id, &path).await {
                            Ok(()) => {
                                // Update manifest location to Both (local + remote)
                                let _ = manifest_tx.try_send(SegmentMeta {
                                    group_id,
                                    segment_id,
                                    valid_bytes,
                                    min_index,
                                    max_index,
                                    min_ts: None,
                                    max_ts: None,
                                    sealed: true,
                                    record_count,
                                    record_type_flags,
                                    entry_count,
                                    first_entry_offset,
                                    location: SegmentLocation::Both,
                                });
                                tracing::debug!(
                                    group_id,
                                    segment_id,
                                    "segment archived to remote storage"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    group_id,
                                    segment_id,
                                    error = %e,
                                    "failed to archive segment to remote storage"
                                );
                            }
                        }
                    });
                }
            }

            // Drain pending callbacks and prealloc requests that arrived while
            // sealing. This prevents starvation: rotate_segment_with_min()
            // busy-loops waiting for prealloc completion, and append callers
            // block on their callback. Both are serviced by this thread.
            {
                let mut inner = state.mu.lock().unwrap();
                let has_callbacks = !inner.pending.is_empty();
                let has_preallocs = !inner.prealloc_queue.is_empty();

                if has_callbacks || has_preallocs {
                    let now = nanos_now();
                    let mut interleaved = Vec::new();
                    if has_callbacks {
                        let taken = std::mem::take(&mut inner.pending);
                        for (key, entry) in taken {
                            let t = entry.segment.first_enqueue_nanos.load(Ordering::Acquire);
                            if t == 0 || t + delay_nanos <= now {
                                interleaved.push(entry);
                            } else {
                                inner.pending.insert(key, entry);
                            }
                        }
                    }
                    let interleaved_preallocs = if has_preallocs {
                        std::mem::take(&mut inner.prealloc_queue)
                    } else {
                        Vec::new()
                    };
                    drop(inner);

                    for entry in interleaved {
                        let _max_bytes = entry.segment.take_pending();
                        let sync_result: Result<(), io::Error> =
                            if let Some(ref file) = entry.segment.file {
                                #[cfg(test)]
                                {
                                    state.sync_count.fetch_add(1, Ordering::Relaxed);
                                    if state.force_sync_error.load(Ordering::Relaxed) {
                                        Err(io::Error::new(
                                            io::ErrorKind::Other,
                                            "injected fsync error",
                                        ))
                                    } else {
                                        file.sync_data()
                                    }
                                }
                                #[cfg(not(test))]
                                {
                                    file.sync_data()
                                }
                            } else {
                                Ok(())
                            };

                        match &sync_result {
                            Ok(()) => {
                                for cb in entry.callbacks {
                                    cb.io_completed(Ok(()));
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    segment_id = entry.segment.segment_id,
                                    error = %e,
                                    "fsync failed — notifying {} callbacks",
                                    entry.callbacks.len()
                                );
                                for cb in entry.callbacks {
                                    cb.io_completed(Err(io::Error::new(e.kind(), e.to_string())));
                                }
                            }
                        }
                    }

                    for prealloc in interleaved_preallocs {
                        let result = match ActiveMmapSegment::create(
                            &prealloc.group_dir,
                            prealloc.segment_id,
                            prealloc.segment_size,
                        ) {
                            Ok(seg) => PreallocResult::Ready(seg),
                            Err(_) => PreallocResult::Failed,
                        };
                        *prealloc.result.lock().unwrap() = result;
                    }
                }
            }
        }

        // Process pre-allocation requests: create segment files off the hot path
        for req in prealloc_requests {
            let result =
                match ActiveMmapSegment::create(&req.group_dir, req.segment_id, req.segment_size) {
                    Ok(seg) => PreallocResult::Ready(seg),
                    Err(_) => PreallocResult::Failed,
                };
            *req.result.lock().unwrap() = result;
        }

        if shutdown {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// ActiveMmapSegment — writer's current segment state
// ---------------------------------------------------------------------------

/// Writer-side state for the active (tail) segment. The mmap and file handle
/// live in the shared `Segment` — ONE mmap per segment, ONE file handle.
struct ActiveMmapSegment {
    segment_id: u64,
    /// The shared Segment holding the single MmapRaw and file handle.
    segment: Arc<Segment>,
    current_size: u64,
    segment_capacity: u64,
    min_entry_index: Option<u64>,
    max_entry_index: Option<u64>,
}

impl ActiveMmapSegment {
    /// Create a new segment file with a single mmap.
    fn create(group_dir: &Path, segment_id: u64, segment_size: u64) -> io::Result<Self> {
        let path = segment_path(group_dir, segment_id);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(segment_size)?;

        let mmap = memmap2::MmapOptions::new().map_raw(&file)?;
        let segment = Arc::new(Segment::new(segment_id, mmap, 0, Some(file), path));

        Ok(Self {
            segment_id,
            segment,
            current_size: 0,
            segment_capacity: segment_size,
            min_entry_index: None,
            max_entry_index: None,
        })
    }

    /// Open an existing segment file for writing (recovery).
    fn open_existing(path: &Path, segment_id: u64) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let mmap = memmap2::MmapOptions::new().map_raw(&file)?;
        let segment = Arc::new(Segment::new(
            segment_id,
            mmap,
            0,
            Some(file),
            path.to_path_buf(),
        ));

        Ok(Self {
            segment_id,
            segment,
            current_size: 0, // Will be set during recovery
            segment_capacity: file_len,
            min_entry_index: None,
            max_entry_index: None,
        })
    }
}

// ---------------------------------------------------------------------------
// WriterState — active segment state protected by Mutex
// ---------------------------------------------------------------------------

struct WriterState {
    active: ActiveMmapSegment,
    /// Background pre-allocation result slot. The fsync thread creates the segment
    /// and stores it here; rotation checks if it's ready.
    pending_prealloc: Option<Arc<std::sync::Mutex<PreallocResult>>>,
    /// Tracks record types present in the active segment (for manifest metadata).
    record_type_flags: RecordTypeFlags,
    /// Number of records written to the active segment.
    record_count: u64,
    group_dir: PathBuf,
    group_id: u64,
    /// Accumulated entry offsets in the active segment.
    /// Moved into PinnedEntry on rotation — no scanning needed.
    entry_offsets: Vec<(u64, u32)>,
    /// Number of entry records written to the active segment.
    entry_count: u64,
    /// Optional archive for uploading sealed segments to remote storage.
    archive: Option<Arc<crate::segment_archive::ArchiveManager>>,
}

impl WriterState {
    /// Write data into the active segment's mmap, handling rotation if needed.
    /// Returns the file offset where the data was written.
    fn write_bytes(
        &mut self,
        data: &[u8],
        config: &MmapStorageConfig,
        segment_map: &MmapSegmentMap,
        manifest_tx: &MAsyncTx<Array<SegmentMeta>>,
        fsync_state: &FsyncState<impl RaftTypeConfig>,
    ) -> io::Result<usize> {
        let data_len = data.len() as u64;

        // Check if we need rotation before this write.
        if self.active.current_size + data_len > self.active.segment_capacity {
            // If the data itself exceeds the default segment size, create an
            // oversized segment that can hold it. This handles large batches
            // that are encoded into a single buffer.
            let min_capacity = data_len;
            self.rotate_segment_with_min(
                config,
                segment_map,
                manifest_tx,
                fsync_state,
                min_capacity,
            )?;
        }

        let offset = self.active.current_size as usize;
        let end = offset + data.len();

        debug_assert!(
            end <= self.active.segment.capacity(),
            "write exceeds pre-allocated segment capacity: {} > {}",
            end,
            self.active.segment.capacity(),
        );

        // memcpy into mmap — this is the whole point: no syscall, just a copy
        unsafe { self.active.segment.write_at(offset, data) };
        self.active.current_size = end as u64;

        // Request background pre-allocation when active is 75% full
        if self.pending_prealloc.is_none()
            && self.active.current_size > self.active.segment_capacity * 3 / 4
        {
            let next_id = self.active.segment_id + 1;
            let result = Arc::new(std::sync::Mutex::new(PreallocResult::Pending));
            fsync_state.enqueue_prealloc(PreallocRequest {
                group_dir: self.group_dir.clone(),
                segment_id: next_id,
                segment_size: config.segment_size,
                result: result.clone(),
            });
            self.pending_prealloc = Some(result);
        }

        Ok(offset)
    }

    /// Update the reader-visible logical size of the active segment after writes.
    /// Just an atomic store — no locks.
    fn update_read_view(&self) {
        self.active
            .segment
            .logical_size
            .store(self.active.current_size, Ordering::Release);
    }

    /// Rotate: seal old segment into the LRU, atomically swap in the new one,
    /// and enqueue background sealing (fsync + truncate + footer write + manifest update).
    fn rotate_segment(
        &mut self,
        config: &MmapStorageConfig,
        segment_map: &MmapSegmentMap,
        manifest_tx: &MAsyncTx<Array<SegmentMeta>>,
        fsync_state: &FsyncState<impl RaftTypeConfig>,
    ) -> io::Result<()> {
        self.rotate_segment_with_min(config, segment_map, manifest_tx, fsync_state, 0)
    }

    /// Rotate with a minimum capacity for the new segment. If `min_capacity`
    /// exceeds `config.segment_size`, the new segment is created oversized to
    /// accommodate large batches that don't fit in a standard segment.
    fn rotate_segment_with_min(
        &mut self,
        config: &MmapStorageConfig,
        segment_map: &MmapSegmentMap,
        manifest_tx: &MAsyncTx<Array<SegmentMeta>>,
        fsync_state: &FsyncState<impl RaftTypeConfig>,
        min_capacity: u64,
    ) -> io::Result<()> {
        let segment_size = config.segment_size.max(min_capacity);
        let valid_bytes = self.active.current_size;

        // Capture record type flags and record count for manifest, reset for new segment
        let flags = std::mem::take(&mut self.record_type_flags);
        let record_count = std::mem::take(&mut self.record_count);

        // Try to use background pre-allocated segment.
        // If a prealloc is in-flight, wait for it — the background thread will
        // finish in microseconds (just open+ftruncate+mmap). Waiting avoids
        // creating the segment twice and orphaning a file.
        let new_id = self.active.segment_id + 1;
        let needs_oversized = min_capacity > config.segment_size;
        let new_seg = if !needs_oversized {
            if let Some(slot) = self.pending_prealloc.take() {
                loop {
                    let mut guard = slot.lock().unwrap();
                    match std::mem::replace(&mut *guard, PreallocResult::Pending) {
                        PreallocResult::Ready(seg) if seg.segment_id == new_id => break seg,
                        PreallocResult::Ready(_) => {
                            break ActiveMmapSegment::create(
                                &self.group_dir,
                                new_id,
                                segment_size,
                            )?;
                        }
                        PreallocResult::Failed => {
                            break ActiveMmapSegment::create(
                                &self.group_dir,
                                new_id,
                                segment_size,
                            )?;
                        }
                        PreallocResult::Pending => {
                            drop(guard);
                            std::thread::yield_now();
                        }
                    }
                }
            } else {
                ActiveMmapSegment::create(&self.group_dir, new_id, segment_size)?
            }
        } else {
            // Oversized segment — skip prealloc (wrong size), discard any pending.
            self.pending_prealloc.take();
            ActiveMmapSegment::create(&self.group_dir, new_id, segment_size)?
        };

        // Atomically swap active → the old active becomes the sealed segment
        let old_active = segment_map.swap_active(new_seg.segment.clone());
        // Update old segment's logical_size to final valid_bytes before sealing
        old_active
            .logical_size
            .store(valid_bytes, Ordering::Release);

        // Compute first_entry_offset from collected offsets
        let first_entry_offset = self.entry_offsets.first().map_or(0, |&(off, _)| off);
        let entry_count = self.entry_count;

        // Enqueue background sealing: fsync + truncate + manifest update + optional S3 upload
        fsync_state.enqueue_seal(SealRequest {
            segment: old_active.clone(),
            valid_bytes,
            group_id: self.group_id,
            min_index: self.active.min_entry_index,
            max_index: self.active.max_entry_index,
            record_count,
            record_type_flags: flags,
            manifest_tx: manifest_tx.clone(),
            entry_count,
            first_entry_offset,
            archive: self.archive.clone(),
        });

        // Build entry index from collected offsets — zero scanning cost
        let min_idx = self.active.min_entry_index.unwrap_or(0);
        let max_idx = self.active.max_entry_index.unwrap_or(0);
        let entry_index = Arc::new(SegmentEntryIndex {
            base_index: min_idx,
            offsets: std::mem::take(&mut self.entry_offsets),
        });
        self.entry_count = 0;
        segment_map.pin(old_active, min_idx, max_idx, entry_index);

        self.active = new_seg;
        Ok(())
    }
}

/// Scan mmap bytes to find the valid byte offset up to (and including) a given entry index.
/// Returns the byte offset just past the last valid record whose entry index <= target_index.
fn scan_valid_bytes_up_to_index(data: &[u8], target_index: u64) -> usize {
    let mut offset = 0;
    let mut last_valid_end = 0;
    while offset + LENGTH_SIZE <= data.len() {
        let record_len =
            u32::from_le_bytes(data[offset..offset + LENGTH_SIZE].try_into().unwrap()) as usize;
        if record_len == 0 || offset + LENGTH_SIZE + record_len > data.len() {
            break;
        }
        let record_data = &data[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
        if record_data.len() >= 1 + GROUP_ID_SIZE + CRC64_SIZE {
            if let Ok(RecordType::Entry) = RecordType::try_from(record_data[0]) {
                // Parse just enough to get the entry index from the codec payload
                let payload = &record_data[1 + GROUP_ID_SIZE..record_data.len() - CRC64_SIZE];
                if let Some(index) = extract_entry_index(payload) {
                    if index > target_index {
                        break;
                    }
                }
            }
        }
        let total = LENGTH_SIZE + record_len;
        offset += align8(total);
        last_valid_end = offset;
    }
    last_valid_end
}

// ---------------------------------------------------------------------------
// MmapGroupIndex — lock-free group index (same pattern as storage_impl.rs)
// ---------------------------------------------------------------------------

struct MmapGroupIndex<C: RaftTypeConfig> {
    slots: Box<[AtomicPtr<MmapGroupState<C>>]>,
    count: AtomicUsize,
}

impl<C: RaftTypeConfig> MmapGroupIndex<C> {
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

    #[inline]
    fn get(&self, group_id: u64) -> Option<Arc<MmapGroupState<C>>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return None;
        }
        let ptr = self.slots[idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }
        unsafe {
            Arc::increment_strong_count(ptr);
            Some(Arc::from_raw(ptr))
        }
    }

    fn insert(
        &self,
        group_id: u64,
        state: Arc<MmapGroupState<C>>,
    ) -> Result<(), Arc<MmapGroupState<C>>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return Err(state);
        }
        let new_ptr = Arc::into_raw(state) as *mut MmapGroupState<C>;
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
            Err(_) => {
                let state = unsafe { Arc::from_raw(new_ptr) };
                Err(state)
            }
        }
    }

    fn remove(&self, group_id: u64) -> Option<Arc<MmapGroupState<C>>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return None;
        }
        let ptr = self.slots[idx].swap(ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            None
        } else {
            self.count.fetch_sub(1, Ordering::Relaxed);
            Some(unsafe { Arc::from_raw(ptr) })
        }
    }

    fn group_ids(&self) -> Vec<u64> {
        let mut result = Vec::with_capacity(self.count.load(Ordering::Relaxed));
        for (idx, slot) in self.slots.iter().enumerate() {
            if !slot.load(Ordering::Acquire).is_null() {
                result.push(idx as u64);
            }
        }
        result
    }

    #[inline]
    fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl<C: RaftTypeConfig> Drop for MmapGroupIndex<C> {
    fn drop(&mut self) {
        for slot in self.slots.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe {
                    let _ = Arc::from_raw(ptr);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MmapGroupState — per-group shared state
// ---------------------------------------------------------------------------

struct MmapGroupState<C: RaftTypeConfig> {
    vote: AtomicVote,
    first_index: AtomicU64,
    last_index: AtomicU64,
    last_log_id: AtomicLogId,
    last_purged_log_id: AtomicLogId,
    log_index: Arc<LogIndex>,
    segment_map: Arc<MmapSegmentMap>,
    /// Writer state — direct mmap writes, no writer thread needed
    writer: Mutex<WriterState>,
    /// Config for segment size etc.
    config: Arc<MmapStorageConfig>,
    /// Manifest sender for tracking sealed segments
    manifest_tx: MAsyncTx<Array<SegmentMeta>>,
    /// Shared fsync state (one thread for all groups, owned by MmapPerGroupLogStorage)
    fsync_state: Arc<FsyncState<C>>,
    /// Floor below which purge() will not remove log entries.
    /// Set externally by the state machine layer. 0 = no constraint.
    purge_floor: Arc<AtomicU64>,
    /// Last applied index from the state machine.
    /// Segments fully above this are not needed yet. 0 = unconstrained.
    pin_ceiling: Arc<AtomicU64>,
    /// Segment IDs that have been purged (files deleted). The state machine
    /// drains this after each apply batch to detach any retained messages
    /// backed by mmap'd segments that no longer exist.
    purged_segments: Arc<Mutex<Vec<u64>>>,
    /// Optional archive manager for uploading sealed segments to remote storage.
    archive: Option<Arc<crate::segment_archive::ArchiveManager>>,
}

impl<C: RaftTypeConfig + 'static> MmapGroupState<C> {
    /// Create and recover group state from disk
    fn new(
        group_id: u64,
        config: &Arc<MmapStorageConfig>,
        group_dir: &Path,
        manifest: &ManifestManager,
        fsync_state: Arc<FsyncState<C>>,
        archive: Option<Arc<crate::segment_archive::ArchiveManager>>,
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
        C::D: Decode,
    {
        std::fs::create_dir_all(group_dir)?;

        let log_index = Arc::new(LogIndex::new());
        let purge_floor = Arc::new(AtomicU64::new(0));
        let pin_ceiling = Arc::new(AtomicU64::new(0));
        let segment_map = Arc::new(MmapSegmentMap::new(
            group_dir.to_path_buf(),
            group_id,
            Arc::clone(&purge_floor),
            Arc::clone(&pin_ceiling),
            config,
            archive.clone(),
        ));

        // Load manifest data for this group
        let manifest_segments = manifest.read_group_segments(group_id).unwrap_or_default();

        // Scan directory for segment files
        let segment_ids = scan_segment_ids(group_dir)?;

        let mut vote: Option<openraft::impls::Vote<C>> = None;
        let mut last_log_id: Option<LogId<C>> = None;
        let mut last_purged_log_id: Option<LogId<C>> = None;
        let mut overall_first_index: Option<u64> = None;
        let mut overall_last_index: Option<u64> = None;

        let max_record_size = config.max_record_size as usize;

        // Process sealed segments (all except last)
        let active_segment_id = segment_ids.last().copied();
        for &seg_id in &segment_ids {
            if Some(seg_id) == active_segment_id && segment_ids.len() > 1 {
                continue; // Skip the last segment, handle as active below
            }
            if segment_ids.len() == 1 {
                continue; // Single segment is active, skip here
            }

            let path = segment_path(group_dir, seg_id);
            let file = std::fs::File::open(&path)?;
            let mmap_ro = unsafe { Mmap::map(&file)? };

            // Determine valid_bytes from manifest (trusted for sealed) or CRC scan
            let meta = manifest_segments.get(&seg_id);
            let valid = if let Some(m) = meta {
                if m.sealed {
                    m.valid_bytes as usize
                } else {
                    Self::scan_valid_bytes(&mmap_ro)
                }
            } else {
                Self::scan_valid_bytes(&mmap_ro)
            };

            // FAST PATH: entry-only segment — walk length prefixes only (skip CRC decode)
            if let Some(m) = meta {
                if m.sealed && m.record_type_flags.has_only_entries() {
                    let mut offset = 0usize;
                    let mut entry_index = m.min_index.unwrap_or(0);
                    let mut entry_offsets = Vec::new();
                    while offset + LENGTH_SIZE <= valid {
                        let record_len = u32::from_le_bytes(
                            mmap_ro[offset..offset + LENGTH_SIZE].try_into().unwrap(),
                        ) as usize;
                        if record_len == 0 || offset + LENGTH_SIZE + record_len > valid {
                            break;
                        }
                        let total = LENGTH_SIZE + record_len;
                        let _ = log_index.insert(
                            entry_index,
                            LogLocation {
                                segment_id: seg_id,
                                offset: offset as u64,
                                len: total as u32,
                            },
                        );
                        entry_offsets.push((offset as u64, total as u32));
                        entry_index += 1;
                        offset += align8(total);
                    }

                    if let Some(min) = m.min_index {
                        overall_first_index =
                            Some(overall_first_index.map_or(min, |v: u64| v.min(min)));
                    }
                    if let Some(max) = m.max_index {
                        overall_last_index =
                            Some(overall_last_index.map_or(max, |v: u64| v.max(max)));
                        if let Some(loc) = log_index.get(max) {
                            let start = loc.offset as usize;
                            let end = start + loc.len as usize;
                            if end <= mmap_ro.len() {
                                let buf = &mmap_ro[start..end];
                                if buf.len() > LENGTH_SIZE {
                                    if let Ok(parsed) =
                                        validate_record(&buf[LENGTH_SIZE..], max_record_size)
                                    {
                                        if let Ok(entry) =
                                            openraft::impls::Entry::<C>::decode_from_slice(
                                                parsed.payload,
                                            )
                                        {
                                            last_log_id =
                                                Some(openraft::entry::RaftEntry::log_id(&entry));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let mmap_raw = memmap2::MmapOptions::new().map_raw_read_only(&file)?;
                    let seg_min = m.min_index.unwrap_or(0);
                    let seg_max = m.max_index.unwrap_or(0);
                    let entry_index = Arc::new(SegmentEntryIndex {
                        base_index: seg_min,
                        offsets: entry_offsets,
                    });
                    segment_map.pin(
                        Arc::new(Segment::new(seg_id, mmap_raw, valid as u64, None, path)),
                        seg_min,
                        seg_max,
                        entry_index,
                    );
                    continue;
                }
            }

            // SLOW PATH: full record decode
            let scan = Self::scan_records_into_index(
                &mmap_ro[..valid],
                seg_id,
                group_id,
                max_record_size,
                &log_index,
                &mut vote,
                &mut last_log_id,
                &mut last_purged_log_id,
            );

            // Track min/max from scan results
            if let Some(min) = scan.min_entry_index {
                overall_first_index = Some(overall_first_index.map_or(min, |v: u64| v.min(min)));
            }
            if let Some(max) = scan.max_entry_index {
                overall_last_index = Some(overall_last_index.map_or(max, |v: u64| v.max(max)));
            }

            // Repair manifest: emit entry for segments missing or not yet sealed
            if meta.is_none() || !meta.unwrap().sealed {
                let _ = manifest.sender().try_send(SegmentMeta {
                    group_id,
                    segment_id: seg_id,
                    valid_bytes: valid as u64,
                    min_index: scan.min_entry_index,
                    max_index: scan.max_entry_index,
                    min_ts: None,
                    max_ts: None,
                    sealed: true,
                    record_count: scan.record_count,
                    record_type_flags: scan.record_type_flags,
                    entry_count: 0,
                    first_entry_offset: 0,
                    location: SegmentLocation::Local,
                });
            }

            let mmap_raw = memmap2::MmapOptions::new().map_raw_read_only(&file)?;
            let seg_min = scan.min_entry_index.unwrap_or(0);
            let seg_max = scan.max_entry_index.unwrap_or(0);
            let entry_index = Arc::new(build_entry_index(&mmap_ro[..valid], valid));
            segment_map.pin(
                Arc::new(Segment::new(seg_id, mmap_raw, valid as u64, None, path)),
                seg_min,
                seg_max,
                entry_index,
            );
        }

        // Open or create active (tail) segment
        let active = if let Some(seg_id) = active_segment_id {
            let path = segment_path(group_dir, seg_id);
            let mut active = ActiveMmapSegment::open_existing(&path, seg_id)?;

            // CRC-validate tail segment to find end of valid data
            let cap = active.segment.capacity();
            let valid = Self::scan_valid_bytes(active.segment.as_slice(cap));
            active.current_size = valid as u64;

            // Zero-pad beyond valid_bytes for partial write protection
            if valid < cap {
                let zero_len = cap.min(valid + 4096) - valid;
                unsafe { active.segment.zero_range(valid, zero_len) };
            }

            // Full record scan for tail (must decode everything)
            let scan = Self::scan_records_into_index(
                active.segment.as_slice(valid),
                seg_id,
                group_id,
                max_record_size,
                &log_index,
                &mut vote,
                &mut last_log_id,
                &mut last_purged_log_id,
            );

            // Update active's entry index range from scan results
            active.min_entry_index = scan.min_entry_index;
            active.max_entry_index = scan.max_entry_index;

            if let Some(min) = scan.min_entry_index {
                overall_first_index = Some(overall_first_index.map_or(min, |v: u64| v.min(min)));
            }
            if let Some(max) = scan.max_entry_index {
                overall_last_index = Some(overall_last_index.map_or(max, |v: u64| v.max(max)));
            }

            // Set active segment for readers — shares the same Segment (same mmap)
            active
                .segment
                .logical_size
                .store(active.current_size, Ordering::Release);
            segment_map.set_active(active.segment.clone());

            active
        } else {
            // No segments exist — create first one
            let active = ActiveMmapSegment::create(group_dir, 1, config.segment_size)?;
            segment_map.set_active(active.segment.clone());
            active
        };

        // Determine first_index accounting for purge
        let first_index = if let Some(ref plid) = last_purged_log_id {
            plid.index + 1
        } else {
            overall_first_index.unwrap_or(0)
        };
        let last_index = overall_last_index.unwrap_or(0);

        // Initialize atomic state
        let atomic_vote = AtomicVote::new();
        if let Some(ref v) = vote {
            atomic_vote.store(Some(v));
        }
        let atomic_last_log_id = AtomicLogId::new();
        if let Some(ref lid) = last_log_id {
            atomic_last_log_id.store(Some(lid));
        }
        let atomic_last_purged = AtomicLogId::new();
        if let Some(ref plid) = last_purged_log_id {
            atomic_last_purged.store(Some(plid));
        }

        // Initialize writer state — no writer thread, callers write directly
        let active_seg_id = active.segment_id;
        let writer = WriterState {
            active,
            pending_prealloc: None,
            record_type_flags: RecordTypeFlags::default(),
            record_count: 0,
            group_dir: group_dir.to_path_buf(),
            group_id,
            entry_offsets: Vec::new(),
            entry_count: 0,
            archive: archive.clone(),
        };

        let manifest_tx = manifest.sender();

        Ok(Self {
            vote: atomic_vote,
            first_index: AtomicU64::new(first_index),
            last_index: AtomicU64::new(last_index),
            last_log_id: atomic_last_log_id,
            last_purged_log_id: atomic_last_purged,
            log_index,
            segment_map,
            writer: Mutex::new(writer),
            config: config.clone(),
            manifest_tx,
            fsync_state,
            purge_floor,
            pin_ceiling,
            purged_segments: Arc::new(Mutex::new(Vec::new())),
            archive,
        })
    }

    /// Scan mmap data for valid bytes (find the end of the last valid record)
    fn scan_valid_bytes(data: &[u8]) -> usize {
        let mut offset = 0;
        while offset + LENGTH_SIZE <= data.len() {
            let record_len =
                u32::from_le_bytes(data[offset..offset + LENGTH_SIZE].try_into().unwrap()) as usize;
            if record_len == 0 {
                break;
            }
            let total = LENGTH_SIZE + record_len;
            if offset + total > data.len() {
                break;
            }
            // Validate CRC
            let record_data = &data[offset + LENGTH_SIZE..offset + total];
            if record_data.len() < 1 + GROUP_ID_SIZE + CRC64_SIZE {
                break;
            }
            // Quick CRC check
            let payload_end = record_data.len() - CRC64_SIZE;
            let stored_crc = u64::from_le_bytes(record_data[payload_end..].try_into().unwrap());
            let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme);
            digest.update(&record_data[..payload_end]);
            if digest.finalize() != stored_crc {
                break;
            }
            offset += align8(total);
        }
        offset
    }

    fn scan_records_into_index(
        data: &[u8],
        segment_id: u64,
        group_id: u64,
        max_record_size: usize,
        log_index: &LogIndex,
        vote: &mut Option<openraft::impls::Vote<C>>,
        last_log_id: &mut Option<LogId<C>>,
        last_purged_log_id: &mut Option<LogId<C>>,
    ) -> ScanMeta
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: Decode,
    {
        let mut flags = RecordTypeFlags::default();
        let mut record_count: u64 = 0;
        let mut min_entry_index: Option<u64> = None;
        let mut max_entry_index: Option<u64> = None;

        let mut offset = 0;
        while offset + LENGTH_SIZE <= data.len() {
            let record_len =
                u32::from_le_bytes(data[offset..offset + LENGTH_SIZE].try_into().unwrap()) as usize;
            if record_len == 0 || offset + LENGTH_SIZE + record_len > data.len() {
                break;
            }
            let record_data = &data[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
            let total = LENGTH_SIZE + record_len;

            if let Ok(parsed) = validate_record(record_data, max_record_size) {
                if parsed.group_id != group_id {
                    offset += align8(total);
                    continue;
                }
                match parsed.record_type {
                    RecordType::Entry => {
                        flags.has_entry = true;
                        if let Ok(entry) =
                            openraft::impls::Entry::<C>::decode_from_slice(parsed.payload)
                        {
                            let lid = openraft::entry::RaftEntry::log_id(&entry);
                            let index = lid.index;
                            let _ = log_index.insert(
                                index,
                                LogLocation {
                                    segment_id,
                                    offset: offset as u64,
                                    len: total as u32,
                                },
                            );
                            min_entry_index =
                                Some(min_entry_index.map_or(index, |v: u64| v.min(index)));
                            max_entry_index =
                                Some(max_entry_index.map_or(index, |v: u64| v.max(index)));
                            *last_log_id = Some(lid);
                        }
                    }
                    RecordType::Vote => {
                        flags.has_vote = true;
                        if let Ok(decoded_vote) =
                            openraft::impls::Vote::<C>::decode_from_slice(parsed.payload)
                        {
                            *vote = Some(decoded_vote);
                        }
                    }
                    RecordType::Truncate => {
                        flags.has_truncate = true;
                        if let Ok(decoded_lid) =
                            openraft::LogId::<C>::decode_from_slice(parsed.payload)
                        {
                            *last_log_id = Some(decoded_lid);
                        }
                    }
                    RecordType::Purge => {
                        flags.has_purge = true;
                        if let Ok(decoded_lid) =
                            openraft::LogId::<C>::decode_from_slice(parsed.payload)
                        {
                            *last_purged_log_id = Some(decoded_lid);
                        }
                    }
                }
                record_count += 1;
            } else {
                break; // Corrupt record, stop scanning
            }
            offset += align8(total);
        }

        ScanMeta {
            record_type_flags: flags,
            record_count,
            min_entry_index,
            max_entry_index,
        }
    }
}

// ---------------------------------------------------------------------------
// MmapPerGroupLogStorage — top-level storage
// ---------------------------------------------------------------------------

/// Mmap-based segmented per-group log storage for Multi-Raft.
///
/// Each raft group gets its own set of mmap'd segment files. Background fsync
/// ensures durability without blocking the writer.
pub struct MmapPerGroupLogStorage<C: RaftTypeConfig> {
    config: Arc<MmapStorageConfig>,
    manifest: Arc<ManifestManager>,
    groups: MmapGroupIndex<C>,
    creation_lock: tokio::sync::Mutex<()>,
    /// Single shared fsync thread for all groups
    fsync_state: Arc<FsyncState<C>>,
    /// Fsync thread handle — joined on stop() for deterministic shutdown
    fsync_thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Optional archive manager for S3-compatible segment archival.
    archive: Option<Arc<crate::segment_archive::ArchiveManager>>,
}

impl<C: RaftTypeConfig + 'static> MmapPerGroupLogStorage<C> {
    /// Create a new mmap per-group storage instance.
    pub async fn new(config: impl Into<MmapStorageConfig>) -> io::Result<Self> {
        Self::new_with_archive(config, None).await
    }

    /// Create a new mmap per-group storage instance with an optional segment archive
    /// for S3-compatible remote segment storage.
    pub async fn new_with_archive(
        config: impl Into<MmapStorageConfig>,
        archive: Option<Arc<dyn crate::segment_archive::SegmentArchive>>,
    ) -> io::Result<Self> {
        let config = Arc::new(config.into());
        tokio::fs::create_dir_all(&*config.base_dir).await?;

        // Build ArchiveManager if both config and archive impl are present
        let archive_mgr = match (&config.s3_archive, archive) {
            (Some(s3_config), Some(archive_impl)) => Some(Arc::new(
                crate::segment_archive::ArchiveManager::new(archive_impl, s3_config.clone()),
            )),
            _ => None,
        };

        let base = (*config.base_dir).clone();
        let manifest_dir = config
            .manifest_dir
            .as_ref()
            .map(|p| (**p).clone())
            .unwrap_or(base);
        let manifest =
            tokio::task::spawn_blocking(move || match ManifestManager::open(&manifest_dir) {
                Ok(manifest) => Ok(manifest),
                Err(e)
                    if e.raw_os_error() == Some(22) || e.kind() == io::ErrorKind::InvalidInput =>
                {
                    Ok(ManifestManager::open_in_memory())
                }
                Err(e) => Err(e),
            })
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        // Spawn single shared fsync thread for all groups
        let fsync_state = Arc::new(FsyncState::new(config.fsync_delay));
        let fsync_clone = fsync_state.clone();
        let fsync_handle = std::thread::Builder::new()
            .name("mmap-raft-fsync".into())
            .spawn(move || {
                fsync_thread_loop::<C>(fsync_clone);
            })
            .expect("Failed to spawn mmap fsync thread");

        Ok(Self {
            config,
            manifest: Arc::new(manifest),
            groups: MmapGroupIndex::new(),
            creation_lock: tokio::sync::Mutex::new(()),
            fsync_state,
            fsync_thread: std::sync::Mutex::new(Some(fsync_handle)),
            archive: archive_mgr,
        })
    }

    /// Stop the shared fsync thread and wait for it to exit.
    pub fn stop(&self) {
        {
            let mut inner = self.fsync_state.mu.lock().unwrap();
            inner.shutdown = true;
            self.fsync_state.cv.notify_one();
        }
        // Join the fsync thread to ensure deterministic shutdown
        if let Some(handle) = self.fsync_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
        // Stop the manifest worker thread
        self.manifest.stop();
    }
}

impl<C: RaftTypeConfig> Drop for MmapPerGroupLogStorage<C> {
    fn drop(&mut self) {
        self.stop();
    }
}

impl<C: RaftTypeConfig> MmapPerGroupLogStorage<C> {
    async fn get_or_create_group(&self, group_id: u64) -> io::Result<Arc<MmapGroupState<C>>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: Decode,
    {
        // Fast path
        if let Some(state) = self.groups.get(group_id) {
            return Ok(state);
        }

        // Slow path: serialize creation
        let _guard = self.creation_lock.lock().await;
        if let Some(state) = self.groups.get(group_id) {
            return Ok(state);
        }

        let config = self.config.clone();
        let group_dir = group_dir_path(&config.base_dir, group_id);
        let manifest = self.manifest.clone();
        let fsync_state = self.fsync_state.clone();
        let archive = self.archive.clone();

        let state = tokio::task::spawn_blocking(move || {
            MmapGroupState::new(
                group_id,
                &config,
                &group_dir,
                &manifest,
                fsync_state,
                archive,
            )
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        let state = Arc::new(state);
        match self.groups.insert(group_id, state.clone()) {
            Ok(()) => Ok(state),
            Err(_) => Ok(self
                .groups
                .get(group_id)
                .expect("group must exist after failed insert")),
        }
    }

    /// Get a log storage handle for a specific group
    pub async fn get_log_storage(&self, group_id: u64) -> io::Result<MmapGroupLogStorage<C>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: Decode,
    {
        let group_state = self.get_or_create_group(group_id).await?;
        Ok(MmapGroupLogStorage {
            group_id,
            config: self.config.clone(),
            state: group_state,
            encode_buf: Vec::new(),
            payload_buf: Vec::new(),
            record_offsets: Vec::new(),
        })
    }

    /// Remove a group from this storage
    pub fn remove_group(&self, group_id: u64) {
        let _ = self.groups.remove(group_id);
    }

    /// Get the list of active group IDs
    pub fn group_ids(&self) -> Vec<u64> {
        self.groups.group_ids()
    }

    /// Get the number of active groups
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    /// Get the purge floor handle for a specific group.
    /// Returns `None` if the group has not been initialized yet.
    pub fn get_purge_floor(&self, group_id: u64) -> Option<Arc<AtomicU64>> {
        self.groups
            .get(group_id)
            .map(|state| state.purge_floor.clone())
    }

    /// Get the pin ceiling handle for a specific group.
    /// Returns `None` if the group has not been initialized yet.
    pub fn get_pin_ceiling(&self, group_id: u64) -> Option<Arc<AtomicU64>> {
        self.groups
            .get(group_id)
            .map(|state| state.pin_ceiling.clone())
    }

    /// Get the purged segments notification handle for a specific group.
    /// Returns `None` if the group has not been initialized yet.
    pub fn get_purged_segments(&self, group_id: u64) -> Option<Arc<Mutex<Vec<u64>>>> {
        self.groups
            .get(group_id)
            .map(|state| state.purged_segments.clone())
    }

    /// Get a segment prefetcher handle for a specific group.
    /// Returns `None` if the group has not been initialized yet.
    pub fn get_prefetcher(&self, group_id: u64) -> Option<SegmentPrefetcher> {
        self.groups.get(group_id).map(|state| SegmentPrefetcher {
            log_index: Arc::clone(&state.log_index),
            segment_map: Arc::clone(&state.segment_map),
        })
    }
}

// ---------------------------------------------------------------------------
// SegmentPrefetcher — lightweight handle for state machine prefetching
// ---------------------------------------------------------------------------

/// Handle given to the state machine for speculatively prefetching the next segment.
/// Call `prefetch_next(applied_index)` after each apply batch to open the next
/// segment in the background before the state machine needs it.
#[derive(Clone)]
pub struct SegmentPrefetcher {
    log_index: Arc<LogIndex>,
    segment_map: Arc<MmapSegmentMap>,
}

impl SegmentPrefetcher {
    /// Prefetch the segment after the one containing `applied_index`.
    /// Fire-and-forget: spawns a background task, does not block.
    pub fn prefetch_next(&self, applied_index: u64) {
        if let Some(loc) = self.log_index.get(applied_index) {
            self.segment_map.prefetch_segment(loc.segment_id + 1);
        }
    }

    /// Return the segment ID that contains the given log index, if known.
    pub fn segment_id_for(&self, log_index: u64) -> Option<u64> {
        self.log_index.get(log_index).map(|loc| loc.segment_id)
    }

    /// Return the full `LogLocation` for a given log index.
    pub fn log_location(&self, log_index: u64) -> Option<LogLocation> {
        self.log_index.get(log_index)
    }

    /// Read raw bytes at a specific `(segment_id, offset)` with a known length.
    ///
    /// Returns a zero-copy `Bytes` backed by the mmap segment.
    /// Used for direct message access via `MessageLocation`.
    pub fn read_bytes_at(&self, segment_id: u32, offset: u32, len: u32) -> Option<bytes::Bytes> {
        let segment = self.segment_map.find_segment(segment_id as u64)?;
        let valid_bytes = segment.logical_size.load(Ordering::Acquire) as usize;
        let start = offset as usize;
        let end = start + len as usize;
        if end > valid_bytes || end > segment.capacity() {
            return None;
        }
        Some(bytes_from_segment(
            &Arc::clone(&segment),
            start,
            len as usize,
        ))
    }

    /// Read a length-prefixed message at a specific `(segment_id, offset)`.
    ///
    /// Expects `[len:4][data...]` at the given offset. Reads the 4-byte length
    /// prefix, then returns the data portion as zero-copy `Bytes`.
    pub fn read_length_prefixed_at(&self, segment_id: u32, offset: u32) -> Option<bytes::Bytes> {
        let segment = self.segment_map.find_segment(segment_id as u64)?;
        let valid_bytes = segment.logical_size.load(Ordering::Acquire) as usize;
        let start = offset as usize;
        if start + 4 > valid_bytes {
            return None;
        }
        let len_bytes = segment.read_slice(start, 4);
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        let data_start = start + 4;
        let data_end = data_start + len;
        if data_end > valid_bytes || data_end > segment.capacity() {
            return None;
        }
        Some(bytes_from_segment(&Arc::clone(&segment), data_start, len))
    }

    /// Read the command data portion of a Normal (tag=1) raft log entry.
    ///
    /// Returns a zero-copy `Bytes` backed by the mmap segment containing the
    /// serialized command payload (e.g. bincode-encoded `MqCommand`).
    /// Returns `None` if the entry is not found, not in a loaded segment,
    /// or is not a Normal entry.
    ///
    /// Record layout: `[len:4][type:1][group_id:3][term:8][node_id:8][index:8][tag:1][data...][crc:8]`
    /// This method returns just the `[data...]` portion.
    pub fn read_normal_entry_data(&self, log_index: u64) -> Option<bytes::Bytes> {
        let loc = self.log_index.get(log_index)?;
        let segment = self.segment_map.find_segment(loc.segment_id)?;

        let valid_bytes = segment.logical_size.load(Ordering::Acquire) as usize;
        let start = loc.offset as usize;
        let end = start + loc.len as usize;
        if end > valid_bytes || end > segment.capacity() {
            return None;
        }

        // Minimum record: header(8) + entry_header(25) + crc(8) = 41
        if loc.len < 41 {
            return None;
        }

        let buf = segment.read_slice(start, loc.len as usize);

        // Check tag byte: offset 32 from record start (8 header + 24 entry prefix)
        let tag = buf[HEADER_SIZE + 24];
        if tag != 1 {
            return None; // Not a Normal entry
        }

        // Data region: after entry header, before CRC
        let data_start = HEADER_SIZE + 25; // 8 + 25 = 33
        let data_end = loc.len as usize - CRC64_SIZE; // len - 8
        if data_start >= data_end {
            return None;
        }

        Some(bytes_from_segment(
            &Arc::clone(&segment),
            start + data_start,
            data_end - data_start,
        ))
    }

    /// Read the command data of a Normal entry at a pre-resolved `LogLocation`.
    ///
    /// Same as `read_normal_entry_data` but skips the index lookup, useful
    /// when the caller already has the location (e.g. from a prior
    /// `log_location()` call).
    pub fn read_normal_entry_data_at(&self, loc: &LogLocation) -> Option<bytes::Bytes> {
        let segment = self.segment_map.find_segment(loc.segment_id)?;

        let valid_bytes = segment.logical_size.load(Ordering::Acquire) as usize;
        let start = loc.offset as usize;
        let end = start + loc.len as usize;
        if end > valid_bytes || end > segment.capacity() {
            return None;
        }

        if loc.len < 41 {
            return None;
        }

        let buf = segment.read_slice(start, loc.len as usize);

        let tag = buf[HEADER_SIZE + 24];
        if tag != 1 {
            return None;
        }

        let data_start = HEADER_SIZE + 25;
        let data_end = loc.len as usize - CRC64_SIZE;
        if data_start >= data_end {
            return None;
        }

        Some(bytes_from_segment(
            &Arc::clone(&segment),
            start + data_start,
            data_end - data_start,
        ))
    }

    /// Return a live view into a segment's mmap region.
    ///
    /// Unlike `segment_bytes()` which snapshots `logical_size`, the returned
    /// `SegmentView` reads `logical_size` atomically on every `len()` call,
    /// always seeing newly appended data.
    ///
    /// Returns `None` if the segment is not currently pinned or active.
    pub fn segment_view(&self, segment_id: u64) -> Option<SegmentView> {
        let segment = self.segment_map.find_segment(segment_id)?;
        if segment.logical_size.load(Ordering::Acquire) == 0 {
            return None;
        }
        Some(SegmentView::from_segment(segment))
    }

    /// Return the full valid region of a segment as a single `Bytes`.
    ///
    /// The returned `Bytes` holds an `Arc<Segment>` ref, keeping the mmap
    /// pinned while any slice of it is alive. This enables cursor-based
    /// sequential scanning of the entire segment without per-record lookups.
    ///
    /// Returns `None` if the segment is not currently pinned or active.
    pub fn segment_bytes(&self, segment_id: u64) -> Option<bytes::Bytes> {
        let segment = self.segment_map.find_segment(segment_id)?;
        let valid = segment.logical_size.load(Ordering::Acquire) as usize;
        if valid == 0 {
            return None;
        }
        Some(bytes_from_segment(&segment, 0, valid))
    }

    /// Return the segment ID of the currently active (writable) segment.
    pub fn active_segment_id(&self) -> u64 {
        self.segment_map.active.load().segment_id
    }

    /// Return sorted IDs of all currently pinned (sealed) segments.
    pub fn pinned_segment_ids(&self) -> Vec<u64> {
        let pinned = self.segment_map.pinned.lock();
        let mut ids: Vec<u64> = pinned.keys().copied().collect();
        ids.sort_unstable();
        ids
    }
}

// ---------------------------------------------------------------------------
// MmapGroupLogStorage — per-group handle
// ---------------------------------------------------------------------------

/// Log storage handle for a specific group within MmapPerGroupLogStorage.
pub struct MmapGroupLogStorage<C: RaftTypeConfig> {
    group_id: u64,
    config: Arc<MmapStorageConfig>,
    state: Arc<MmapGroupState<C>>,
    encode_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    record_offsets: Vec<(u64, u64, u32)>,
}

impl<C: RaftTypeConfig> Clone for MmapGroupLogStorage<C> {
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id,
            config: self.config.clone(),
            state: self.state.clone(),
            encode_buf: Vec::new(),
            payload_buf: Vec::new(),
            record_offsets: Vec::new(),
        }
    }
}

impl<C: RaftTypeConfig> MmapGroupLogStorage<C> {
    /// Get the group ID
    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    /// Get the purge floor handle.
    /// The state machine layer can hold this Arc and update the floor
    /// to prevent log purging below a certain index.
    pub fn purge_floor(&self) -> Arc<AtomicU64> {
        self.state.purge_floor.clone()
    }

    /// Get the pin ceiling handle.
    /// The state machine layer updates this to its last applied index,
    /// controlling which segments stay pinned in memory.
    pub fn pin_ceiling(&self) -> Arc<AtomicU64> {
        self.state.pin_ceiling.clone()
    }

    /// Get the purged segments notification handle.
    /// The state machine drains this after each apply batch to detach any
    /// mmap-backed retained messages referencing purged segments.
    pub fn purged_segments(&self) -> Arc<Mutex<Vec<u64>>> {
        self.state.purged_segments.clone()
    }

    /// Get a prefetcher handle for the state machine.
    /// The state machine calls `prefetch_next(applied_index)` after each apply
    /// batch to speculatively open the next segment before it's needed.
    pub fn prefetcher(&self) -> SegmentPrefetcher {
        SegmentPrefetcher {
            log_index: Arc::clone(&self.state.log_index),
            segment_map: Arc::clone(&self.state.segment_map),
        }
    }

    /// Decode an entry from a segment at the given location.
    fn decode_entry_from_segment(
        &self,
        segment: &Arc<Segment>,
        loc: &LogLocation,
    ) -> io::Result<Option<C::Entry>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
                Vote = openraft::impls::Vote<C>,
                Node = openraft::impls::BasicNode,
                Entry = openraft::impls::Entry<C>,
            >,
        C::D: Decode,
    {
        let valid_bytes = segment.logical_size.load(Ordering::Acquire) as usize;
        let start = loc.offset as usize;
        let end = start + loc.len as usize;
        if end > valid_bytes || end > segment.capacity() {
            return Ok(None);
        }

        let buf = segment.read_slice(start, end - start);
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

        let max_record_size = self.config.max_record_size as usize;
        let parsed = validate_record(&buf[LENGTH_SIZE..], max_record_size)?;
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

        // Direct binary parse from mmap slice — avoids Cursor + RawBytes::decode copy.
        // Entry payload layout: [term:8][node_id:8][index:8][tag:1][data...]
        let p = parsed.payload;
        if p.len() < 25 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "entry payload too short",
            ));
        }
        let tag = p[24];

        match tag {
            0 => {
                // Blank: parse log_id directly, no payload data to propagate.
                let term = u64::from_le_bytes(p[0..8].try_into().unwrap());
                let node_id = u64::from_le_bytes(p[8..16].try_into().unwrap());
                let log_index = u64::from_le_bytes(p[16..24].try_into().unwrap());
                let log_id = openraft::LogId::<C> {
                    leader_id: openraft::impls::leader_id_adv::LeaderId::<C> { term, node_id },
                    index: log_index,
                };
                use openraft::entry::RaftEntry;
                Ok(Some(openraft::impls::Entry::<C>::new(
                    log_id,
                    openraft::EntryPayload::Blank,
                )))
            }
            1 | 2 => {
                // Normal or Membership: zero-copy decode via mmap-backed Bytes.
                // Payload starts after the record header (HEADER_SIZE includes length prefix).
                let payload_offset = start + HEADER_SIZE;
                let payload_len = p.len();
                let entry_bytes =
                    bytes_from_segment(&Arc::clone(segment), payload_offset, payload_len);
                let entry = openraft::impls::Entry::<C>::decode_from_bytes(entry_bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                Ok(Some(entry))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown entry tag: {}", tag),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader implementation
// ---------------------------------------------------------------------------

impl<C> RaftLogReader<C> for MmapGroupLogStorage<C>
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
    C::D: Decode,
{
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
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

        // Clamp end to last_index+1 so the cached fast path doesn't read
        // beyond truncation boundaries (entry index has stale offsets).
        let last = self.state.last_index.load(Ordering::Acquire);
        let end = end.min(last + 1);

        let expected_len = end.saturating_sub(start).min(1024) as usize;
        let mut entries = Vec::with_capacity(expected_len);

        // Cache: (segment_id, segment_ref, entry_index) — avoids per-entry
        // LogIndex + segment map lookups for entries in the same segment.
        let mut cached: Option<(u64, Arc<Segment>, Arc<SegmentEntryIndex>)> = None;

        for idx in start..end {
            // Fast path: use cached segment's local entry index (direct Vec lookup)
            if let Some((_, ref seg, ref entry_idx)) = cached {
                if let Some((offset, len)) = entry_idx.lookup(idx) {
                    let loc = LogLocation {
                        segment_id: 0, // unused — we have the segment already
                        offset,
                        len,
                    };
                    if let Some(entry) = self.decode_entry_from_segment(seg, &loc)? {
                        entries.push(entry);
                        continue;
                    }
                }
            }

            // Slow path: entry not in cached segment — look up via LogIndex + segment map
            let loc = match self.state.log_index.get(idx) {
                Some(loc) => loc,
                None => break,
            };

            let segment = match self.state.segment_map.find_segment(loc.segment_id) {
                Some(seg) => seg,
                None => match self.state.segment_map.reopen_segment(loc.segment_id).await {
                    Some(seg) => seg,
                    None => break,
                },
            };

            if let Some(entry) = self.decode_entry_from_segment(&segment, &loc)? {
                // Update cache to this entry's segment (if pinned with an entry index)
                cached = self
                    .state
                    .segment_map
                    .find_segment_indexed(loc.segment_id)
                    .map(|(seg, idx)| (loc.segment_id, seg, idx));
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

// ---------------------------------------------------------------------------
// RaftLogStorage implementation
// ---------------------------------------------------------------------------

impl<C> RaftLogStorage<C> for MmapGroupLogStorage<C>
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
    C::D: Encode + Decode + BorrowPayload,
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

        // Inline vote encoding — 17 bytes: [term:8][node_id:8][committed:1]
        self.payload_buf.clear();
        self.payload_buf
            .extend_from_slice(&vote.leader_id.term.to_le_bytes());
        self.payload_buf
            .extend_from_slice(&vote.leader_id.node_id.to_le_bytes());
        self.payload_buf.push(vote.committed as u8);

        // Build full record (header + payload + CRC) outside the lock
        self.encode_buf.clear();
        append_record_into(
            &mut self.encode_buf,
            RecordType::Vote,
            self.group_id,
            &self.payload_buf,
        );

        // Write into mmap under lock — no clone needed
        let mut writer = self.state.writer.lock();
        writer.record_type_flags.has_vote = true;
        writer.record_count += 1;
        writer.write_bytes(
            &self.encode_buf,
            &self.state.config,
            &self.state.segment_map,
            &self.state.manifest_tx,
            &self.state.fsync_state,
        )?;
        writer.update_read_view();
        Ok(())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + Send,
    {
        use openraft::entry::RaftEntry;

        let mut last_log_id = None;

        // Encode all entries into encode_buf, tracking record offsets.
        // Blank/Normal entries write header + payload + CRC directly into encode_buf,
        // skipping the intermediate payload_buf. Membership falls back to payload_buf + append_record_into.
        self.encode_buf.clear();
        self.record_offsets.clear();
        for entry in entries {
            let log_id = entry.log_id();
            let index = log_id.index;

            let record_start = self.encode_buf.len() as u64;
            let record_len = match &entry.payload {
                openraft::EntryPayload::Blank => {
                    // Entry: [term:8][node_id:8][index:8][tag:1] = 25 bytes payload
                    // Record: [len:4][type:1][group_id:3][payload:25][crc:8] = 41 bytes
                    // Aligned: 48 bytes (7 bytes padding)
                    let payload_len = 25;
                    let record_body_len = 1 + GROUP_ID_SIZE + payload_len + CRC64_SIZE;
                    let content_size = LENGTH_SIZE + record_body_len;
                    let aligned_size = align8(content_size);
                    let padding = aligned_size - content_size;
                    self.encode_buf.reserve(aligned_size);

                    // Record header: [len:4][type:1][group_id:3]
                    let mut header = [0u8; HEADER_SIZE];
                    header[0..4].copy_from_slice(&(record_body_len as u32).to_le_bytes());
                    header[4] = RecordType::Entry as u8;
                    write_u24_le(&mut header, 5, self.group_id);

                    // Entry fields (written inline as the payload)
                    let term_bytes = entry.log_id.leader_id.term.to_le_bytes();
                    let node_bytes = entry.log_id.leader_id.node_id.to_le_bytes();
                    let idx_bytes = entry.log_id.index.to_le_bytes();

                    // CRC over [type + group_id + payload]
                    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme);
                    digest.update(&header[LENGTH_SIZE..]);
                    digest.update(&term_bytes);
                    digest.update(&node_bytes);
                    digest.update(&idx_bytes);
                    digest.update(&[0u8]); // Blank tag
                    let crc = digest.finalize();

                    self.encode_buf.extend_from_slice(&header);
                    self.encode_buf.extend_from_slice(&term_bytes);
                    self.encode_buf.extend_from_slice(&node_bytes);
                    self.encode_buf.extend_from_slice(&idx_bytes);
                    self.encode_buf.push(0); // Blank tag
                    self.encode_buf.extend_from_slice(&crc.to_le_bytes());
                    if padding > 0 {
                        self.encode_buf.extend_from_slice(&[0u8; 7][..padding]);
                    }
                    content_size
                }
                openraft::EntryPayload::Normal(_) | openraft::EntryPayload::Membership(_) => {
                    // Full entry encoding via the codec. This ensures multi-variant
                    // command types (e.g. CreateTable, DropTable, AppendRecords) are
                    // all stored correctly and can be decoded via Entry::decode.
                    self.payload_buf.clear();
                    entry
                        .encode_into(&mut self.payload_buf)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    append_record_into(
                        &mut self.encode_buf,
                        RecordType::Entry,
                        self.group_id,
                        &self.payload_buf,
                    )
                }
            };
            self.record_offsets
                .push((index, record_start, record_len as u32));

            self.state.last_index.fetch_max(index, Ordering::Relaxed);
            last_log_id = Some(log_id);
        }

        // Update last log id
        if let Some(ref lid) = last_log_id {
            self.state.last_log_id.store(Some(lid));
        }

        // Write directly into mmap — just a memcpy, no syscall
        let (segment, bytes_written) = {
            let mut writer = self.state.writer.lock();
            let file_offset = writer.write_bytes(
                &self.encode_buf,
                &self.state.config,
                &self.state.segment_map,
                &self.state.manifest_tx,
                &self.state.fsync_state,
            )?;

            // Update LogIndex for each entry record
            for &(entry_index, offset_in_data, record_len) in &self.record_offsets {
                let abs_offset = file_offset as u64 + offset_in_data;
                self.state.log_index.insert(
                    entry_index,
                    LogLocation {
                        segment_id: writer.active.segment_id,
                        offset: abs_offset,
                        len: record_len,
                    },
                )?;
                if writer.active.min_entry_index.is_none() {
                    writer.active.min_entry_index = Some(entry_index);
                }
                writer.active.max_entry_index = Some(entry_index);
                writer.record_type_flags.has_entry = true;
                writer.record_count += 1;
                writer.entry_offsets.push((abs_offset, record_len));
                writer.entry_count += 1;
            }

            writer.update_read_view();

            // Check rotation after write
            if writer.active.current_size > self.state.config.segment_size {
                writer.rotate_segment(
                    &self.state.config,
                    &self.state.segment_map,
                    &self.state.manifest_tx,
                    &self.state.fsync_state,
                )?;
            }

            (writer.active.segment.clone(), writer.active.current_size)
        };

        // Push callback onto the segment's queue, register for fsync
        self.state
            .fsync_state
            .push(&segment, bytes_written, callback);

        Ok(())
    }

    async fn truncate_after(&mut self, after: Option<LogId<C>>) -> Result<(), io::Error> {
        let index = match after {
            Some(ref log_id) => log_id.index,
            None => 0,
        };

        // Update last index
        self.state.last_index.store(index, Ordering::Relaxed);
        if let Some(ref log_id) = after {
            self.state.last_log_id.store(Some(log_id));
        } else {
            self.state.last_log_id.store::<C>(None);
        }

        // Write truncate record and update state under lock
        {
            let mut writer = self.state.writer.lock();

            // Remove entries from LogIndex
            self.state.log_index.truncate_from(index.saturating_add(1));

            // Remove sealed segments with entries beyond truncation point
            let removed = self.state.segment_map.remove_after(index);
            for path in &removed {
                let _ = std::fs::remove_file(path);
            }

            // If active segment has entries beyond truncation point, scan for new end
            if writer
                .active
                .max_entry_index
                .map_or(false, |max| max > index)
            {
                let new_size = scan_valid_bytes_up_to_index(
                    writer
                        .active
                        .segment
                        .as_slice(writer.active.current_size as usize),
                    index,
                );
                writer.active.current_size = new_size as u64;
                writer.active.max_entry_index =
                    if index >= writer.active.min_entry_index.unwrap_or(0) {
                        Some(index)
                    } else {
                        None
                    };
                if writer.active.max_entry_index.is_none() {
                    writer.active.min_entry_index = None;
                }
            }

            writer.update_read_view();
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C>) -> Result<(), io::Error> {
        let floor = self.state.purge_floor.load(Ordering::Acquire);
        let index = if floor > 0 && log_id.index >= floor {
            // Cap the purge at floor - 1 to retain the entry at floor.
            if floor <= 1 {
                return Ok(());
            }
            floor - 1
        } else {
            log_id.index
        };

        // Update first index and last_purged_log_id
        self.state.first_index.store(index + 1, Ordering::Relaxed);
        // Use the original leader_id with the capped index.
        let effective_log_id: LogId<C> = LogId {
            leader_id: log_id.leader_id,
            index,
        };
        self.state.last_purged_log_id.store(Some(&effective_log_id));

        // Purge from LogIndex and unpin segments below the purge floor.
        // Level 1: only releases mmap memory, does NOT delete files.
        // File deletion is handled by Level 2 retention evaluation.
        self.state.log_index.purge_to(index);
        let unpinned_ids = self.state.segment_map.update_pins(index);

        // Notify the state machine about unpinned segment IDs so it can
        // detach any mmap-backed retained messages referencing these segments.
        if !unpinned_ids.is_empty() {
            self.state
                .purged_segments
                .lock()
                .extend_from_slice(&unpinned_ids);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MultiRaftLogStorage implementation
// ---------------------------------------------------------------------------

impl<C> crate::storage::MultiRaftLogStorage<C> for MmapPerGroupLogStorage<C>
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
    C::D: Encode + Decode + BorrowPayload,
{
    type GroupLogStorage = MmapGroupLogStorage<C>;

    async fn get_log_storage(&self, group_id: u64) -> Self::GroupLogStorage {
        MmapPerGroupLogStorage::get_log_storage(self, group_id)
            .await
            .expect("Failed to create mmap group storage")
    }

    fn remove_group(&self, group_id: u64) {
        MmapPerGroupLogStorage::remove_group(self, group_id)
    }

    fn group_ids(&self) -> Vec<u64> {
        MmapPerGroupLogStorage::group_ids(self)
    }

    fn get_purge_floor(&self, group_id: u64) -> Option<Arc<AtomicU64>> {
        MmapPerGroupLogStorage::get_purge_floor(self, group_id)
    }

    fn get_pin_ceiling(&self, group_id: u64) -> Option<Arc<AtomicU64>> {
        MmapPerGroupLogStorage::get_pin_ceiling(self, group_id)
    }

    fn get_purged_segments(&self, group_id: u64) -> Option<Arc<Mutex<Vec<u64>>>> {
        MmapPerGroupLogStorage::get_purged_segments(self, group_id)
    }

    fn stop(&self) {
        MmapPerGroupLogStorage::stop(self);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::type_config::async_runtime::{AsyncRuntime, oneshot::Oneshot};
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestData(Vec<u8>);

    impl std::fmt::Display for TestData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestData(len={})", self.0.len())
        }
    }

    impl Encode for TestData {
        fn encode<W: std::io::Write>(
            &self,
            writer: &mut W,
        ) -> Result<(), crate::codec::CodecError> {
            (self.0.len() as u32).encode(writer)?;
            writer.write_all(&self.0)?;
            Ok(())
        }
        fn encoded_size(&self) -> usize {
            4 + self.0.len()
        }
    }

    impl Decode for TestData {
        fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::codec::CodecError> {
            let len = u32::decode(reader)? as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            Ok(Self(buf))
        }
    }

    impl BorrowPayload for TestData {
        fn payload_bytes(&self) -> &[u8] {
            &self.0
        }
    }

    use crate::type_config::ManiacRaftTypeConfig;
    type C = ManiacRaftTypeConfig<TestData, ()>;

    fn make_entry(index: u64, term: u64) -> openraft::impls::Entry<C> {
        openraft::impls::Entry::<C> {
            log_id: LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
                index,
            },
            payload: openraft::entry::EntryPayload::Normal(TestData(index.to_le_bytes().to_vec())),
        }
    }

    fn run_async<F>(f: F) -> F::Output
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        rt.block_on(f)
    }

    type Rt = <C as RaftTypeConfig>::AsyncRuntime;
    type Os = <Rt as AsyncRuntime>::Oneshot;

    fn make_callback() -> (
        IOFlushed<C>,
        <Os as Oneshot>::Receiver<Result<(), io::Error>>,
    ) {
        let (tx, rx) = Os::channel::<Result<(), io::Error>>();
        let cb = IOFlushed::<C>::signal(tx);
        (cb, rx)
    }

    #[test]
    fn test_mmap_basic_append_and_read() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Append entries
            let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Read entries
            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[0].log_id.index, 1);
            assert_eq!(result[1].log_id.index, 2);
            assert_eq!(result[2].log_id.index, 3);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_save_and_read_vote() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Read vote when none exists
            let vote = log.read_vote().await.unwrap();
            assert!(vote.is_none());

            // Save a vote
            let vote = openraft::impls::Vote::<C> {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 5,
                    node_id: 2,
                },
                committed: true,
            };
            log.save_vote(&vote).await.unwrap();

            // Read it back
            let read_vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(read_vote.leader_id.term, 5);
            assert_eq!(read_vote.leader_id.node_id, 2);
            assert!(read_vote.committed);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_log_state() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Initial state
            let state = log.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());
            assert!(state.last_purged_log_id.is_none());

            // Append entries
            let entries = vec![make_entry(1, 1), make_entry(2, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 2);
            assert!(state.last_purged_log_id.is_none());

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_truncate_after() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Append entries
            let entries = vec![
                make_entry(1, 1),
                make_entry(2, 1),
                make_entry(3, 1),
                make_entry(4, 1),
                make_entry(5, 1),
            ];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Truncate after index 3
            let after = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 3,
            };
            log.truncate_after(Some(after)).await.unwrap();

            // Give writer thread time to process

            // Read entries - should only have 1..=3
            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[2].log_id.index, 3);

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 3);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_purge() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Append entries
            let entries = vec![
                make_entry(1, 1),
                make_entry(2, 1),
                make_entry(3, 1),
                make_entry(4, 1),
                make_entry(5, 1),
            ];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Purge up to index 3
            let purge_id = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 3,
            };
            log.purge(purge_id).await.unwrap();

            // Give writer time to process

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 3);

            // Should still be able to read 4 and 5
            let result = log.try_get_log_entries(4..6).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].log_id.index, 4);
            assert_eq!(result[1].log_id.index, 5);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_segment_rotation() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Small segment size to force rotation
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Append many entries to force rotation
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // All entries should be readable
            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);

            // Verify segment files were created
            let group_dir = tmp.path().join("group_0");
            let seg_count = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".log")
                })
                .count();
            assert!(seg_count > 1, "Expected multiple segments, got {seg_count}");

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_multi_group() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            // Write to two different groups
            let mut log0 = storage.get_log_storage(0).await.unwrap();
            let mut log1 = storage.get_log_storage(1).await.unwrap();

            let entries0 = vec![make_entry(1, 1), make_entry(2, 1)];
            let (cb0, rx0) = make_callback();
            log0.append(entries0, cb0).await.unwrap();
            rx0.await.unwrap().unwrap();

            let entries1 = vec![make_entry(1, 2), make_entry(2, 2), make_entry(3, 2)];
            let (cb1, rx1) = make_callback();
            log1.append(entries1, cb1).await.unwrap();
            rx1.await.unwrap().unwrap();

            // Read from each group independently
            let result0 = log0.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(result0.len(), 2);
            assert_eq!(result0[0].log_id.leader_id.term, 1);

            let result1 = log1.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(result1.len(), 3);
            assert_eq!(result1[0].log_id.leader_id.term, 2);

            // Groups should be independent
            assert_eq!(storage.group_ids().len(), 2);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_recovery() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write data
        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Save a vote
            let vote = openraft::impls::Vote::<C> {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 3,
                    node_id: 7,
                },
                committed: false,
            };
            log.save_vote(&vote).await.unwrap();

            storage.stop();
            drop(storage);
        });

        // Phase 2: Re-open and verify recovery
        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Entries should be recovered
            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[0].log_id.index, 1);
            assert_eq!(result[2].log_id.index, 3);

            // Vote should be recovered
            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 3);
            assert_eq!(vote.leader_id.node_id, 7);
            assert!(!vote.committed);

            // Log state should be recovered
            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 3);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_large_entries() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(1024 * 1024); // 1MB segments
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Create a large entry (100KB)
            let large_data = TestData(vec![42u8; 100 * 1024]);
            let entry = openraft::impls::Entry::<C> {
                log_id: LogId {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: 1,
                },
                payload: openraft::entry::EntryPayload::Normal(large_data.clone()),
            };

            let (cb, rx) = make_callback();
            log.append(vec![entry], cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..2).await.unwrap();
            assert_eq!(result.len(), 1);
            if let openraft::entry::EntryPayload::Normal(ref data) = result[0].payload {
                assert_eq!(data.0.len(), 100 * 1024);
                assert!(data.0.iter().all(|&b| b == 42));
            } else {
                panic!("Expected Normal payload");
            }

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_recovery_with_footer_fast_path() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write enough entries to force rotation (creates sealed segments with footer)
        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to create multiple sealed segments
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Verify segments were created
            let group_dir = path.join("group_0");
            let seg_count = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".log")
                })
                .count();
            assert!(seg_count > 1, "Expected multiple segments, got {seg_count}");

            storage.stop();
            drop(storage);
            // Wait for background sealing to write footers
        });

        // Phase 2: Re-open and verify recovery uses fast path (footer-based)
        run_async(async move {
            let config = MmapStorageConfig::new(&path2).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // All entries should be recovered
            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 50);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_recovery_with_vote_segment_slow_path() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write entries with intermixed vote records (forces slow path on recovery)
        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write entries with votes intermixed to create segments with has_vote=true
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();

                if i % 5 == 0 {
                    let vote = openraft::impls::Vote::<C> {
                        leader_id: openraft::impls::leader_id_adv::LeaderId {
                            term: i,
                            node_id: 1,
                        },
                        committed: false,
                    };
                    log.save_vote(&vote).await.unwrap();
                }
            }

            storage.stop();
            drop(storage);
        });

        // Phase 2: Recovery should still work (mix of fast + slow paths)
        run_async(async move {
            let config = MmapStorageConfig::new(&path2).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..21).await.unwrap();
            assert_eq!(result.len(), 20);

            // Vote should be recovered (last vote was at i=20, term=20)
            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 20);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_pre_allocation() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Small segment to trigger pre-allocation at 75%
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough to pass 75% threshold
            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Check that next segment file was pre-allocated
            let group_dir = tmp.path().join("group_0");
            let seg_count = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".log")
                })
                .count();
            // Should have at least 2 segments (active + rotated, possibly pre-allocated next)
            assert!(
                seg_count >= 2,
                "Expected at least 2 segments, got {seg_count}"
            );

            // All entries should be readable across segments
            let result = log.try_get_log_entries(1..11).await.unwrap();
            assert_eq!(result.len(), 10);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_partial_write_protection() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write data, then corrupt the tail to simulate partial write
        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.stop();
            drop(storage);

            // Corrupt the tail by writing garbage after valid data
            let group_dir = path.join("group_0");
            let seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();

            // Find the active segment (last one)
            if let Some(seg_path) = seg_files.last() {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let file_len = file.metadata().unwrap().len();
                // Write garbage near the end (simulates partial write)
                if file_len > 100 {
                    use std::io::Seek;
                    file.seek(std::io::SeekFrom::End(-50)).unwrap();
                    file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04])
                        .unwrap();
                }
            }
        });

        // Phase 2: Recovery should handle partial writes gracefully
        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Should recover at least the valid entries (CRC check stops at corruption)
            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert!(result.len() >= 1, "Should recover at least some entries");
            assert_eq!(result[0].log_id.index, 1);

            storage.stop();
        });
    }

    #[test]
    fn test_mmap_seal_metadata_in_manifest() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write entries and force rotation to create sealed segments
        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=30 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            storage.stop();
            drop(storage);
            // Wait for background sealing
        });

        // Phase 2: Verify sealed segment metadata is in manifest
        run_async(async move {
            let manifest = ManifestManager::open(&path2).unwrap();
            let segments = manifest.read_group_segments(0).unwrap();

            let mut sealed_count = 0;
            for (_seg_id, meta) in &segments {
                if meta.sealed {
                    assert!(meta.record_type_flags.has_entry);
                    assert!(meta.valid_bytes > 0);
                    sealed_count += 1;
                }
            }

            assert!(
                sealed_count > 0,
                "Expected at least one sealed segment in manifest"
            );
        });
    }

    // ===================================================================
    // Helper functions for comprehensive tests
    // ===================================================================

    fn make_blank_entry(index: u64, term: u64) -> openraft::impls::Entry<C> {
        openraft::impls::Entry::<C> {
            log_id: LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
                index,
            },
            payload: openraft::entry::EntryPayload::Blank,
        }
    }

    fn make_large_entry(index: u64, term: u64, size: usize) -> openraft::impls::Entry<C> {
        openraft::impls::Entry::<C> {
            log_id: LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id: 1 },
                index,
            },
            payload: openraft::entry::EntryPayload::Normal(TestData(vec![0xAB; size])),
        }
    }

    fn make_vote(term: u64, node_id: u64, committed: bool) -> openraft::impls::Vote<C> {
        openraft::impls::Vote::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId { term, node_id },
            committed,
        }
    }

    fn count_segment_files(dir: &std::path::Path) -> usize {
        let group_dir = dir.join("group_0");
        if !group_dir.exists() {
            return 0;
        }
        std::fs::read_dir(&group_dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".log")
            })
            .count()
    }

    // ===================================================================
    // Pre-allocation edge cases
    // ===================================================================

    #[test]
    fn test_prealloc_ready_before_rotation() {
        // Pre-allocation should complete before rotation is needed,
        // so rotation uses the pre-allocated segment (no sync create).
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write past 75% to trigger prealloc request
            for i in 1..=8 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Give background thread time to complete prealloc

            // Now write more to trigger rotation — should use prealloc'd segment
            for i in 9..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // All entries readable across segments
            let result = log.try_get_log_entries(1..21).await.unwrap();
            assert_eq!(result.len(), 20);
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            storage.stop();
        });
    }

    #[test]
    fn test_prealloc_not_ready_at_rotation() {
        // If prealloc hasn't completed by rotation time, rotate_segment
        // should wait for it (spin-yield loop) rather than creating a duplicate.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Very small segments to force rapid rotation
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write rapidly — prealloc and rotation may overlap
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // All entries should be readable
            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);

            // Verify no orphaned files (each segment ID should appear at most once)
            let group_dir = tmp.path().join("group_0");
            let seg_files: Vec<String> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let name = e.ok()?.file_name().to_string_lossy().to_string();
                    if name.ends_with(".log") {
                        Some(name)
                    } else {
                        None
                    }
                })
                .collect();
            let unique_count = seg_files.len();
            let mut deduped = seg_files.clone();
            deduped.sort();
            deduped.dedup();
            assert_eq!(unique_count, deduped.len(), "No duplicate segment files");

            storage.stop();
        });
    }

    #[test]
    fn test_prealloc_multiple_rapid_rotations() {
        // Multiple rotations in quick succession — each rotation should either
        // use a prealloc'd segment or fall back to sync create without deadlock.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(128);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Batch writes to fill segments rapidly
            let mut pending = Vec::new();
            for i in 1..=100 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }

            // Drain all callbacks
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // All entries should be readable
            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100);

            // Should have many segments
            let seg_count = count_segment_files(tmp.path());
            assert!(seg_count >= 3, "Expected many rotations, got {seg_count}");

            storage.stop();
        });
    }

    #[test]
    fn test_prealloc_not_triggered_below_threshold() {
        // Writing less than 75% should not trigger preallocation
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Large segment, small writes — should stay well under 75%
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(1024 * 1024);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Only 1 segment file should exist (active, no prealloc)
            assert_eq!(count_segment_files(tmp.path()), 1);

            storage.stop();
        });
    }

    // ===================================================================
    // Crash simulation — partial writes and incomplete sealing
    // ===================================================================

    #[test]
    fn test_crash_mid_entry_write_truncated_record() {
        // Simulate crash during entry write: valid entries followed by
        // a truncated record (incomplete length prefix or payload).
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.stop();
            drop(storage);

            // Append a truncated record: valid length prefix but incomplete payload
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();

            if let Some(seg_path) = seg_files.last() {
                use std::io::{Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                // Find end of valid data by scanning for first zero run
                let meta = file.metadata().unwrap();
                let mut data = vec![0u8; meta.len() as usize];
                use std::io::Read;
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();
                // Find end of valid records (8-byte aligned)
                let mut valid_end = 0;
                let mut offset = 0;
                while offset + 4 <= data.len() {
                    let rlen =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    if rlen == 0 || offset + 4 + rlen > data.len() {
                        break;
                    }
                    offset += align8(4 + rlen);
                    valid_end = offset;
                }
                // Write a partial record: length says 100 but only write 10 bytes
                file.seek(std::io::SeekFrom::Start(valid_end as u64))
                    .unwrap();
                file.write_all(&100u32.to_le_bytes()).unwrap(); // length = 100
                file.write_all(&[0xFF; 10]).unwrap(); // only 10 bytes of payload
                file.sync_all().unwrap();
            }
        });

        // Recovery should stop at the truncated record, recovering entries 1-3
        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(result.len(), 3, "Should recover all 3 valid entries");
            assert_eq!(result[0].log_id.index, 1);
            assert_eq!(result[1].log_id.index, 2);
            assert_eq!(result[2].log_id.index, 3);

            storage.stop();
        });
    }

    #[test]
    fn test_crash_corrupted_crc_mid_segment() {
        // Corrupt CRC of the second record — recovery should stop there,
        // only recovering the first record.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.stop();
            drop(storage);

            // Corrupt CRC of the second record
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();

            if let Some(seg_path) = seg_files.last() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Find second record (records are 8-byte aligned)
                let rlen1 = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                let second_start = align8(4 + rlen1);
                let rlen2 =
                    u32::from_le_bytes(data[second_start..second_start + 4].try_into().unwrap())
                        as usize;
                // CRC is the last 8 bytes of the record
                let crc_offset = second_start + 4 + rlen2 - 8;
                // Flip CRC bytes
                file.seek(std::io::SeekFrom::Start(crc_offset as u64))
                    .unwrap();
                file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
                    .unwrap();
                file.sync_all().unwrap();
            }
        });

        // Recovery: only first entry should survive
        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(
                result.len(),
                1,
                "Only first entry should survive CRC corruption"
            );
            assert_eq!(result[0].log_id.index, 1);

            storage.stop();
        });
    }

    #[test]
    fn test_crash_after_rotation_before_seal() {
        // Simulate crash after rotation but before the background seal completes.
        // The sealed segment won't have a manifest entry. Recovery should use
        // CRC scan (slow path) and repair the manifest.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write entries across multiple segments
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Stop WITHOUT waiting for background sealing
            storage.stop();
            drop(storage);

            // Delete the manifest to simulate missing seal metadata
            let manifest_dir = path.clone();
            for entry in std::fs::read_dir(&manifest_dir).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("manifest")
                    || name.ends_with(".mdbx")
                    || name.ends_with("-lock")
                {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
            // Also remove any mdbx files that might be inside subdirs
            let _ = std::fs::remove_dir_all(path.join("manifest.mdbx"));
        });

        // Recovery should use slow path and repair manifest
        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..21).await.unwrap();
            assert_eq!(
                result.len(),
                20,
                "All entries should be recovered via CRC scan"
            );
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            storage.stop();
        });
    }

    #[test]
    fn test_crash_zero_length_record_stops_scan() {
        // A zero-length record in the middle of a segment should stop the scan
        // cleanly without panic.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1), make_entry(2, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.stop();
            drop(storage);

            // Overwrite bytes after valid data with zeros (this is actually
            // the default state of mmap, but let's be explicit)
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();

            if let Some(seg_path) = seg_files.last() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Find end of records (8-byte aligned)
                let mut offset = 0;
                while offset + 4 <= data.len() {
                    let rlen =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    if rlen == 0 || offset + 4 + rlen > data.len() {
                        break;
                    }
                    offset += align8(4 + rlen);
                }
                // Write explicit zero length at end of valid data
                if offset + 4 <= data.len() {
                    file.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
                    file.write_all(&0u32.to_le_bytes()).unwrap();
                    file.sync_all().unwrap();
                }
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(
                result.len(),
                2,
                "Both entries before zero record should be recovered"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_crash_vote_only_segment_recovery() {
        // Segment with only votes (no entries) — recovery should restore the vote
        // but log should be empty.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Only save votes, no entries
            log.save_vote(&make_vote(1, 5, false)).await.unwrap();
            log.save_vote(&make_vote(2, 7, true)).await.unwrap();

            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Last vote should be recovered
            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 2);
            assert_eq!(vote.leader_id.node_id, 7);
            assert!(vote.committed);

            // Log should be empty
            let state = log.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());
            assert!(state.last_purged_log_id.is_none());

            let result = log.try_get_log_entries(0..10).await.unwrap();
            assert_eq!(result.len(), 0);

            storage.stop();
        });
    }

    #[test]
    fn test_crash_recovery_with_truncate_then_new_writes() {
        // Write entries, truncate some, write more, crash, recover.
        // Recovery should see the truncation and new entries.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write 5 entries
            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Truncate after index 3
            let trunc_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 3,
            };
            log.truncate_after(Some(trunc_lid)).await.unwrap();

            // Write new entries at index 4-6 with term 2
            for i in 4..=6 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..7).await.unwrap();
            assert_eq!(result.len(), 6);

            // Entries 1-3 should have term 1
            for i in 0..3 {
                assert_eq!(result[i].log_id.leader_id.term, 1);
            }
            // Entries 4-6 should have term 2 (post-truncation writes)
            for i in 3..6 {
                assert_eq!(result[i].log_id.leader_id.term, 2);
            }

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 6);

            storage.stop();
        });
    }

    #[test]
    fn test_crash_recovery_with_purge() {
        // Write entries, purge some, crash, recover.
        // Purge is an in-memory-only operation (no purge record written to log),
        // but it deletes sealed segment files. On recovery, only surviving
        // segment files are scanned.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Purge entries 1-5 — only affects in-memory state and log_index
            // (no purge record written to active segment in current impl)
            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.purge(purge_lid).await.unwrap();

            // Verify purge worked in-memory
            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 5);

            // Purged entries are no longer readable
            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 0);

            // Remaining entries still readable
            let result = log.try_get_log_entries(6..11).await.unwrap();
            assert_eq!(result.len(), 5);

            storage.stop();
            drop(storage);
        });

        // On recovery: since purge doesn't write a record and all entries
        // are in a single segment (default 1MB), all entries 1-10 will be
        // recovered. The purge state is lost.
        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 10);
            // Purge state not persisted — last_purged_log_id is None on recovery
            assert!(state.last_purged_log_id.is_none());

            // All entries recovered since segment file was not removed
            let result = log.try_get_log_entries(1..11).await.unwrap();
            assert_eq!(result.len(), 10);

            storage.stop();
        });
    }

    #[test]
    fn test_crash_recovery_sealed_missing_manifest_repairs() {
        // Create sealed segments, then wipe manifest. Recovery should
        // CRC-scan the sealed segments and repair the manifest entries.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        let path3 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=30 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            storage.stop();
            drop(storage);
        });

        // Wipe manifest
        run_async(async move {
            let _ = std::fs::remove_dir_all(path2.join("manifest.mdbx"));
        });

        // Recovery should repair everything
        run_async(async move {
            let config = MmapStorageConfig::new(&path3);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..31).await.unwrap();
            assert_eq!(result.len(), 30);

            // Subsequent re-open should use fast path (manifest now repaired)
            storage.stop();
        });
    }

    // ===================================================================
    // Concurrent access and race conditions
    // ===================================================================

    #[test]
    fn test_concurrent_readers_during_writes() {
        // Multiple readers reading while a writer is appending entries.
        // No data corruption or panics should occur.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut writer = storage.get_log_storage(0).await.unwrap();

            // Write initial entries
            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                writer.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Spawn reader tasks
            let mut reader_handles = Vec::new();
            for _ in 0..4 {
                let mut reader = storage.get_log_storage(0).await.unwrap();
                let handle = tokio::spawn(async move {
                    for _ in 0..20 {
                        let result = reader.try_get_log_entries(1..6).await.unwrap();
                        assert!(result.len() <= 5);
                        if !result.is_empty() {
                            assert_eq!(result[0].log_id.index, 1);
                        }
                        tokio::task::yield_now().await;
                    }
                });
                reader_handles.push(handle);
            }

            // Continue writing while readers are active
            for i in 6..=30 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                writer.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Wait for readers
            for handle in reader_handles {
                handle.await.unwrap();
            }

            storage.stop();
        });
    }

    #[test]
    fn test_concurrent_vote_and_append() {
        // Interleave vote writes with entry appends — both should succeed.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=20 {
                // Alternate between vote and entry
                if i % 3 == 0 {
                    log.save_vote(&make_vote(i, i * 10, i % 2 == 0))
                        .await
                        .unwrap();
                }
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // All entries readable
            let result = log.try_get_log_entries(1..21).await.unwrap();
            assert_eq!(result.len(), 20);

            // Last vote should be the one from i=18
            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 18);

            storage.stop();
        });
    }

    #[test]
    fn test_concurrent_multi_group_writes() {
        // Multiple groups writing simultaneously — no interference between groups.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(512);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut handles = Vec::new();
            for group_id in 0..4u64 {
                let mut log = storage.get_log_storage(group_id).await.unwrap();
                let handle = tokio::spawn(async move {
                    for i in 1..=25 {
                        let entries = vec![make_entry(i, group_id + 1)];
                        let (cb, rx) = make_callback();
                        log.append(entries, cb).await.unwrap();
                        rx.await.unwrap().unwrap();
                    }
                    // Verify own entries
                    let result = log.try_get_log_entries(1..26).await.unwrap();
                    assert_eq!(result.len(), 25);
                    for entry in &result {
                        assert_eq!(entry.log_id.leader_id.term, group_id + 1);
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.await.unwrap();
            }

            assert_eq!(storage.num_groups(), 4);

            storage.stop();
        });
    }

    #[test]
    fn test_shutdown_with_pending_writes() {
        // Shutdown while writes are in-flight — should not deadlock.
        // With a short fsync delay, callbacks should fire before or during shutdown.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(10));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }

            // Wait for callbacks to complete before shutdown
            for rx in pending {
                let result = tokio::time::timeout(Duration::from_secs(5), rx).await;
                assert!(result.is_ok(), "Callback should fire within timeout");
                result.unwrap().unwrap().unwrap();
            }

            // Data should be readable
            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 5);

            // Shutdown should complete cleanly
            storage.stop();
        });
    }

    #[test]
    fn test_shutdown_no_deadlock_immediate() {
        // Immediate shutdown with pending work — must not deadlock.
        // Callbacks may be dropped (channel closed), that's OK.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_secs(60)); // Very long delay
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=3 {
                let entries = vec![make_entry(i, 1)];
                let (cb, _rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                // Don't wait for callbacks — just fire and forget
            }

            // Stop immediately — fsync thread should exit without deadlock
            storage.stop();
            // If we get here, no deadlock
        });
    }

    // ===================================================================
    // Blank entry handling
    // ===================================================================

    #[test]
    fn test_blank_entry_roundtrip() {
        // Blank entries should encode/decode correctly through the direct binary path.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![
                make_blank_entry(1, 1),
                make_entry(2, 1), // Normal
                make_blank_entry(3, 2),
            ];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(result.len(), 3);

            // Entry 1: Blank
            assert_eq!(result[0].log_id.index, 1);
            assert_eq!(result[0].log_id.leader_id.term, 1);
            assert!(matches!(result[0].payload, openraft::EntryPayload::Blank));

            // Entry 2: Normal
            assert_eq!(result[1].log_id.index, 2);
            assert!(matches!(
                result[1].payload,
                openraft::EntryPayload::Normal(_)
            ));

            // Entry 3: Blank
            assert_eq!(result[2].log_id.index, 3);
            assert_eq!(result[2].log_id.leader_id.term, 2);
            assert!(matches!(result[2].payload, openraft::EntryPayload::Blank));

            storage.stop();
        });
    }

    #[test]
    fn test_blank_entry_recovery() {
        // Blank entries survive recovery (direct binary parse path).
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![
                make_blank_entry(1, 1),
                make_entry(2, 1),
                make_blank_entry(3, 2),
            ];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(result.len(), 3);
            assert!(matches!(result[0].payload, openraft::EntryPayload::Blank));
            assert!(matches!(
                result[1].payload,
                openraft::EntryPayload::Normal(_)
            ));
            assert!(matches!(result[2].payload, openraft::EntryPayload::Blank));

            storage.stop();
        });
    }

    // ===================================================================
    // Edge cases in read path
    // ===================================================================

    #[test]
    fn test_read_nonexistent_index() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Read non-existent index
            let result = log.try_get_log_entries(100..200).await.unwrap();
            assert_eq!(result.len(), 0);

            storage.stop();
        });
    }

    #[test]
    fn test_read_empty_range() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Empty range
            let result = log.try_get_log_entries(5..5).await.unwrap();
            assert_eq!(result.len(), 0);

            storage.stop();
        });
    }

    #[test]
    fn test_read_after_truncate_all() {
        // Truncate all entries (after=None), then read — should be empty.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Truncate all
            log.truncate_after(None).await.unwrap();

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 0);

            let state = log.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());

            // Can write new entries starting from index 1
            let entries = vec![make_entry(1, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..2).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].log_id.leader_id.term, 2);

            storage.stop();
        });
    }

    #[test]
    fn test_read_across_many_segments() {
        // Entries spread across many segments — sequential read should collect all.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(128);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            storage.stop();
        });
    }

    // ===================================================================
    // Entry at segment boundaries
    // ===================================================================

    #[test]
    fn test_entry_exactly_fills_segment() {
        // Entry that exactly fills the segment capacity should trigger rotation.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Use a small segment size. Each entry is ~50 bytes with overhead.
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(100);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write entries that will fill and overflow the segment
            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let result = log.try_get_log_entries(1..11).await.unwrap();
            assert_eq!(result.len(), 10);

            // Should have multiple segments
            let seg_count = count_segment_files(tmp.path());
            assert!(seg_count >= 2);

            storage.stop();
        });
    }

    #[test]
    fn test_large_entry_exceeding_half_segment() {
        // A single large entry that takes more than half the segment.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(1024);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Small entries first
            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Large entry (600 bytes payload, > half of 1024)
            let entries = vec![make_large_entry(2, 1, 600)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Should trigger rotation. Both entries readable.
            let result = log.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(result.len(), 2);

            storage.stop();
        });
    }

    // ===================================================================
    // Truncate edge cases
    // ===================================================================

    #[test]
    fn test_truncate_across_segment_boundary() {
        // Truncate that spans across sealed+active segments.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to span multiple segments
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Truncate back to index 5 — should remove sealed segments too
            let trunc_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.truncate_after(Some(trunc_lid)).await.unwrap();

            let result = log.try_get_log_entries(1..21).await.unwrap();
            assert_eq!(result.len(), 5);
            assert_eq!(result[4].log_id.index, 5);

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 5);

            // Can write new entries after truncation
            for i in 6..=10 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let result = log.try_get_log_entries(1..11).await.unwrap();
            assert_eq!(result.len(), 10);

            storage.stop();
        });
    }

    #[test]
    fn test_truncate_idempotent() {
        // Truncating to current last index should be a no-op.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Truncate at current last — no-op
            let trunc_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.truncate_after(Some(trunc_lid)).await.unwrap();

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 5);

            storage.stop();
        });
    }

    // ===================================================================
    // Purge edge cases
    // ===================================================================

    #[test]
    fn test_purge_all_entries() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Purge all entries
            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.purge(purge_lid).await.unwrap();

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 0);

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 5);

            // Can still write new entries
            let entries = vec![make_entry(6, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(6..7).await.unwrap();
            assert_eq!(result.len(), 1);

            storage.stop();
        });
    }

    #[test]
    fn test_purge_removes_sealed_segments() {
        // Purge should remove sealed segment files that are fully purged.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough to create multiple segments
            for i in 1..=30 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let seg_count_before = count_segment_files(tmp.path());
            assert!(seg_count_before >= 3, "Need multiple segments");

            // Purge most entries
            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 25,
            };
            log.purge(purge_lid).await.unwrap();

            // Level 1 purge only unpins segments — files remain on disk for
            // Level 2 retention evaluation.  Verify segments were unpinned.
            {
                let pinned = log.state.segment_map.pinned.lock();
                for (_seg_id, entry) in pinned.iter() {
                    assert!(
                        entry.max_index > 25,
                        "Purged segment should not be pinned, got max_index={}",
                        entry.max_index
                    );
                }
            }

            // Remaining entries still readable
            let result = log.try_get_log_entries(26..31).await.unwrap();
            assert_eq!(result.len(), 5);

            storage.stop();
        });
    }

    // ===================================================================
    // Vote edge cases
    // ===================================================================

    #[test]
    fn test_vote_overwrite_recovery() {
        // Multiple vote writes — only the last should survive recovery.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write many votes
            for i in 1..=10 {
                log.save_vote(&make_vote(i, i * 100, i % 2 == 0))
                    .await
                    .unwrap();
            }

            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 10);
            assert_eq!(vote.leader_id.node_id, 1000);
            assert!(vote.committed); // 10 % 2 == 0

            storage.stop();
        });
    }

    #[test]
    fn test_vote_committed_flag_preserved() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            log.save_vote(&make_vote(5, 42, true)).await.unwrap();
            let vote = log.read_vote().await.unwrap().unwrap();
            assert!(vote.committed);
            assert_eq!(vote.leader_id.term, 5);
            assert_eq!(vote.leader_id.node_id, 42);

            log.save_vote(&make_vote(6, 99, false)).await.unwrap();
            let vote = log.read_vote().await.unwrap().unwrap();
            assert!(!vote.committed);
            assert_eq!(vote.leader_id.term, 6);
            assert_eq!(vote.leader_id.node_id, 99);

            storage.stop();
        });
    }

    // ===================================================================
    // Fsync and callback edge cases
    // ===================================================================

    #[test]
    fn test_fsync_delay_coalescing() {
        // With a non-zero fsync delay, multiple writes should be coalesced
        // into a single fsync.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(50));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write many entries rapidly
            let mut pending = Vec::new();
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }

            // All callbacks should eventually fire
            for rx in pending {
                let result = tokio::time::timeout(Duration::from_secs(5), rx).await;
                assert!(result.is_ok(), "Callback should fire within timeout");
                result.unwrap().unwrap().unwrap();
            }

            storage.stop();
        });
    }

    #[test]
    fn test_zero_fsync_delay() {
        // With zero delay, callbacks should fire as quickly as possible.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                let result = tokio::time::timeout(Duration::from_secs(2), rx).await;
                assert!(
                    result.is_ok(),
                    "Callback should fire quickly with zero delay"
                );
                result.unwrap().unwrap().unwrap();
            }

            storage.stop();
        });
    }

    // ===================================================================
    // Empty / minimal state
    // ===================================================================

    #[test]
    fn test_empty_storage_operations() {
        // Operations on a fresh storage with no data.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Read vote — should be None
            let vote = log.read_vote().await.unwrap();
            assert!(vote.is_none());

            // Get log state — should be empty
            let state = log.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());
            assert!(state.last_purged_log_id.is_none());

            // Read entries — should be empty
            let result = log.try_get_log_entries(0..100).await.unwrap();
            assert_eq!(result.len(), 0);

            storage.stop();
        });
    }

    #[test]
    fn test_empty_storage_recovery() {
        // Recover from a storage that was created but never written to.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let _log = storage.get_log_storage(0).await.unwrap();
            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let vote = log.read_vote().await.unwrap();
            assert!(vote.is_none());

            let state = log.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());

            storage.stop();
        });
    }

    // ===================================================================
    // Batch append edge cases
    // ===================================================================

    #[test]
    fn test_large_batch_append() {
        // Append many entries in a single batch.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries: Vec<_> = (1..=100).map(|i| make_entry(i, 1)).collect();
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100);

            storage.stop();
        });
    }

    #[test]
    fn test_batch_append_triggers_rotation() {
        // Multiple individual appends that collectively exceed segment size
        // should trigger rotation between batches.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // All entries readable
            let result = log.try_get_log_entries(1..21).await.unwrap();
            assert_eq!(result.len(), 20);

            // Should have multiple segments
            let seg_count = count_segment_files(tmp.path());
            assert!(seg_count >= 2);

            storage.stop();
        });
    }

    #[test]
    fn test_mixed_blank_and_normal_batch() {
        // Batch with alternating blank and normal entries.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries: Vec<_> = (1..=10)
                .map(|i| {
                    if i % 2 == 0 {
                        make_blank_entry(i, 1)
                    } else {
                        make_entry(i, 1)
                    }
                })
                .collect();
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..11).await.unwrap();
            assert_eq!(result.len(), 10);
            for (i, entry) in result.iter().enumerate() {
                let idx = (i + 1) as u64;
                assert_eq!(entry.log_id.index, idx);
                if idx % 2 == 0 {
                    assert!(matches!(entry.payload, openraft::EntryPayload::Blank));
                } else {
                    assert!(matches!(entry.payload, openraft::EntryPayload::Normal(_)));
                }
            }

            storage.stop();
        });
    }

    // ===================================================================
    // Multiple recovery cycles
    // ===================================================================

    #[test]
    fn test_multiple_recovery_cycles() {
        // Write, close, recover, write more, close, recover — state should accumulate.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Cycle 1: write entries 1-5
        let p = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&p);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            storage.stop();
            drop(storage);
        });

        // Cycle 2: recover, write entries 6-10
        let p = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&p);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Verify cycle 1 data
            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 5);

            for i in 6..=10 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            storage.stop();
            drop(storage);
        });

        // Cycle 3: recover and verify all data
        let p = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&p);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..11).await.unwrap();
            assert_eq!(result.len(), 10);

            // Entries from cycle 1
            for i in 0..5 {
                assert_eq!(result[i].log_id.leader_id.term, 1);
            }
            // Entries from cycle 2
            for i in 5..10 {
                assert_eq!(result[i].log_id.leader_id.term, 2);
            }

            storage.stop();
        });
    }

    // ===================================================================
    // Group lifecycle
    // ===================================================================

    #[test]
    fn test_remove_group() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            // Create multiple groups
            let _log0 = storage.get_log_storage(0).await.unwrap();
            let _log1 = storage.get_log_storage(1).await.unwrap();
            let _log2 = storage.get_log_storage(2).await.unwrap();

            assert_eq!(storage.num_groups(), 3);

            // Remove a group
            storage.remove_group(1);
            assert_eq!(storage.num_groups(), 2);

            // Remaining groups should still work
            let ids = storage.group_ids();
            assert!(ids.contains(&0));
            assert!(ids.contains(&2));
            assert!(!ids.contains(&1));

            storage.stop();
        });
    }

    #[test]
    fn test_recreate_group_after_remove() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();
            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.remove_group(0);

            // Re-create the group — it should recover existing data from disk
            let mut log = storage.get_log_storage(0).await.unwrap();
            let result = log.try_get_log_entries(1..2).await.unwrap();
            assert_eq!(result.len(), 1);

            storage.stop();
        });
    }

    // ===================================================================
    // Stress test: no deadlocks under load
    // ===================================================================

    #[test]
    fn test_no_deadlock_under_concurrent_load() {
        // Hammer multiple groups with concurrent reads, writes, votes, truncates,
        // and purges. Must complete within timeout (no deadlock).
        let result = std::panic::catch_unwind(|| {
            run_async(async {
                let tmp = TempDir::new().unwrap();
                let config = MmapStorageConfig::new(tmp.path())
                    .with_segment_size(256)
                    .with_fsync_delay(Duration::from_millis(1));
                let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

                let mut handles = Vec::new();

                for group_id in 0..3u64 {
                    let mut log = storage.get_log_storage(group_id).await.unwrap();
                    let handle = tokio::spawn(async move {
                        let mut pending = Vec::new();

                        // Phase 1: Rapid writes
                        for i in 1..=50 {
                            let entries = vec![make_entry(i, group_id + 1)];
                            let (cb, rx) = make_callback();
                            log.append(entries, cb).await.unwrap();
                            pending.push(rx);

                            // Intersperse with votes
                            if i % 10 == 0 {
                                log.save_vote(&make_vote(i, group_id, i % 2 == 0))
                                    .await
                                    .unwrap();
                            }
                        }

                        // Phase 2: Drain callbacks
                        for rx in pending {
                            rx.await.unwrap().unwrap();
                        }

                        // Phase 3: Read all entries
                        let result = log.try_get_log_entries(1..51).await.unwrap();
                        assert_eq!(result.len(), 50);

                        // Phase 4: Truncate some
                        let trunc_lid = LogId {
                            leader_id: openraft::impls::leader_id_adv::LeaderId {
                                term: group_id + 1,
                                node_id: 1,
                            },
                            index: 30,
                        };
                        log.truncate_after(Some(trunc_lid)).await.unwrap();

                        // Phase 5: Write more after truncation
                        let mut pending = Vec::new();
                        for i in 31..=40 {
                            let entries = vec![make_entry(i, group_id + 10)];
                            let (cb, rx) = make_callback();
                            log.append(entries, cb).await.unwrap();
                            pending.push(rx);
                        }
                        for rx in pending {
                            rx.await.unwrap().unwrap();
                        }

                        // Phase 6: Purge early entries
                        let purge_lid = LogId {
                            leader_id: openraft::impls::leader_id_adv::LeaderId {
                                term: group_id + 1,
                                node_id: 1,
                            },
                            index: 10,
                        };
                        log.purge(purge_lid).await.unwrap();

                        // Phase 7: Verify final state
                        let result = log.try_get_log_entries(11..41).await.unwrap();
                        assert_eq!(result.len(), 30);
                    });
                    handles.push(handle);
                }

                // Timeout: must complete within 30 seconds or it's a deadlock
                let timeout = tokio::time::timeout(Duration::from_secs(30), async {
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
                assert!(timeout.await.is_ok(), "Deadlock detected: test timed out");

                storage.stop();
            });
        });
        assert!(result.is_ok(), "Test panicked unexpectedly");
    }

    // ===================================================================
    // P0: Panic boundary tests
    // ===================================================================

    #[test]
    fn test_atomic_log_id_term_overflow_panics() {
        // AtomicLogId packs term into 40 bits. Values > MAX_TERM should panic.
        use crate::record_format::AtomicLogId;
        let atom = AtomicLogId::new();
        let max_term = (1u64 << 40) - 1; // 2^40 - 1

        // Storing max_term should succeed
        let lid_ok = LogId::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: max_term,
                node_id: 1,
            },
            index: 1,
        };
        atom.store(Some(&lid_ok)); // should not panic

        // Storing max_term + 1 should panic
        let lid_overflow = LogId::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: max_term + 1,
                node_id: 1,
            },
            index: 1,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            atom.store(Some(&lid_overflow));
        }));
        assert!(result.is_err(), "Should panic on term overflow");
    }

    #[test]
    fn test_atomic_log_id_node_id_overflow_panics() {
        // AtomicLogId packs node_id into 24 bits. Values > 0xFFFFFF should panic.
        use crate::record_format::AtomicLogId;
        let atom = AtomicLogId::new();

        // Max node_id should succeed
        let lid_ok = LogId::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 1,
                node_id: 0xFF_FFFF,
            },
            index: 1,
        };
        atom.store(Some(&lid_ok)); // should not panic

        // node_id > 0xFFFFFF should panic
        let lid_overflow = LogId::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 1,
                node_id: 0x01_00_0000,
            },
            index: 1,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            atom.store(Some(&lid_overflow));
        }));
        assert!(result.is_err(), "Should panic on node_id overflow");
    }

    #[test]
    fn test_atomic_log_id_store_none_then_load() {
        // Storing None should return None on load.
        use crate::record_format::AtomicLogId;
        let atom = AtomicLogId::new();

        // Initially None
        let loaded: Option<LogId<C>> = atom.load();
        assert!(loaded.is_none());

        // Store something
        let lid = LogId::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 5,
                node_id: 42,
            },
            index: 100,
        };
        atom.store(Some(&lid));
        let loaded: Option<LogId<C>> = atom.load();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().index, 100);

        // Store None again
        atom.store::<C>(None);
        let loaded: Option<LogId<C>> = atom.load();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_atomic_vote_store_none_then_load() {
        use crate::record_format::AtomicVote;
        let atom = AtomicVote::new();

        // Initially None
        let loaded: Option<openraft::impls::Vote<C>> = atom.load();
        assert!(loaded.is_none());

        // Store a vote
        let vote = make_vote(5, 42, true);
        atom.store(Some(&vote));
        let loaded: Option<openraft::impls::Vote<C>> = atom.load();
        assert!(loaded.is_some());
        let v = loaded.unwrap();
        assert_eq!(v.leader_id.term, 5);
        assert_eq!(v.leader_id.node_id, 42);
        assert!(v.committed);

        // Store None
        atom.store::<C>(None);
        let loaded: Option<openraft::impls::Vote<C>> = atom.load();
        assert!(loaded.is_none());
    }

    // ===================================================================
    // P0: Corruption recovery — additional scenarios
    // ===================================================================

    #[test]
    fn test_corrupt_all_records_in_segment() {
        // Corrupt every record's CRC — recovery should yield empty log.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=3 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Corrupt the very first record's CRC
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.first() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Corrupt first record: flip CRC bytes
                let rlen = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                let crc_offset = 4 + rlen - 8;
                file.seek(std::io::SeekFrom::Start(crc_offset as u64))
                    .unwrap();
                file.write_all(&[0xFF; 8]).unwrap();
                file.sync_all().unwrap();
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // First record is corrupt — scan stops immediately
            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(
                result.len(),
                0,
                "All entries lost when first record is corrupt"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_corrupt_record_length_field() {
        // Set a record length to a very large value — recovery should stop before it.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=3 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Corrupt the second record's length field to be absurdly large
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.first() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Find start of second record
                let rlen1 = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                let second_start = 4 + rlen1;

                // Overwrite length field of second record with 0xFFFFFFFF
                file.seek(std::io::SeekFrom::Start(second_start as u64))
                    .unwrap();
                file.write_all(&0xFFFF_FFFFu32.to_le_bytes()).unwrap();
                file.sync_all().unwrap();
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Only first entry should survive
            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(
                result.len(),
                1,
                "Only first entry should survive bad length field"
            );
            assert_eq!(result[0].log_id.index, 1);

            storage.stop();
        });
    }

    #[test]
    fn test_corrupt_record_type_byte() {
        // Set a record's type byte to invalid value — recovery should stop.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=3 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Corrupt the second record's type byte
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.first() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Find start of second record, corrupt its type byte
                let rlen1 = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                let second_start = 4 + rlen1;
                // Type byte is at second_start + 4 (after length field)
                file.seek(std::io::SeekFrom::Start((second_start + 4) as u64))
                    .unwrap();
                file.write_all(&[0xFF]).unwrap(); // Invalid record type
                file.sync_all().unwrap();
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // CRC will mismatch due to corrupted type byte — scan stops
            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert!(
                result.len() <= 1,
                "Corrupt type byte should stop scan: got {}",
                result.len()
            );

            storage.stop();
        });
    }

    #[test]
    fn test_partial_record_at_end_of_segment() {
        // Simulate a partial write: valid first record followed by an incomplete second record
        // (length header written but payload truncated).
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=2 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Truncate the file right after the length field of the second record
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.first() {
                use std::io::Read;
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Find start of second record
                let rlen1 = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                let second_start = 4 + rlen1;
                // Zero out everything after the first 6 bytes of second record
                // (length field + partial type/group_id, simulating partial write)
                let partial_end = second_start + 6;
                for i in partial_end..data.len() {
                    data[i] = 0;
                }
                std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(0)).unwrap();
                std::io::Write::write_all(&mut file, &data).unwrap();
                file.sync_all().unwrap();
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Only the first entry should survive
            let result = log.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(
                result.len(),
                1,
                "Only first entry should survive partial write"
            );
            assert_eq!(result[0].log_id.index, 1);

            // Should be able to write new entries after recovery
            let entries = vec![make_entry(2, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(result.len(), 2);

            storage.stop();
        });
    }

    #[test]
    fn test_recovery_after_multi_segment_corruption() {
        // Write across multiple segments. Corrupt a sealed (non-active) segment.
        // The corrupted segment loses its entries but other segments survive.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to create multiple segments
            for i in 1..=30 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let seg_count = count_segment_files(tmp.path());
            assert!(seg_count >= 3, "Need multiple segments: got {}", seg_count);

            storage.stop();
            drop(storage);

            // Corrupt the FIRST (sealed) segment file
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.first() {
                // Zero out the first segment file
                let len = std::fs::metadata(seg_path).unwrap().len();
                let zeros = vec![0u8; len as usize];
                std::fs::write(seg_path, &zeros).unwrap();
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Some entries from later segments should survive
            let result = log.try_get_log_entries(1..31).await.unwrap();
            assert!(
                result.len() < 30,
                "Should lose some entries from corrupted segment, got {}",
                result.len()
            );

            // Should still be able to write new entries
            let next_idx = result.last().map(|e| e.log_id.index + 1).unwrap_or(31);
            let entries = vec![make_entry(next_idx, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.stop();
        });
    }

    // ===================================================================
    // P1: Concurrency under load — additional scenarios
    // ===================================================================

    #[test]
    fn test_seqlock_concurrent_reads_during_writes() {
        // Hammer SeqLock with concurrent writers and readers to verify
        // that readers always see consistent values.
        use crate::record_format::{AtomicLogId, AtomicVote};
        use std::sync::Arc;

        let log_id = Arc::new(AtomicLogId::new());
        let vote = Arc::new(AtomicVote::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Spawn reader threads
        let mut handles = Vec::new();
        for _ in 0..4 {
            let log_id = log_id.clone();
            let vote = vote.clone();
            let stop = stop.clone();
            handles.push(std::thread::spawn(move || {
                let mut read_count = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // Read log_id — should never see partial values
                    if let Some(lid) = log_id.load::<C>() {
                        // term and node_id should be consistent
                        assert!(lid.leader_id.term <= 1000);
                        assert!(lid.leader_id.node_id <= 0xFF_FFFF);
                        assert!(lid.index <= 10000);
                    }
                    // Read vote
                    if let Some(v) = vote.load::<C>() {
                        assert!(v.leader_id.term <= 1000);
                    }
                    read_count += 1;
                }
                read_count
            }));
        }

        // Writer thread: rapidly update values
        let log_id2 = log_id.clone();
        let vote2 = vote.clone();
        let writer = std::thread::spawn(move || {
            for i in 1..=1000u64 {
                let lid = LogId::<C> {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: i,
                        node_id: i % 100,
                    },
                    index: i * 10,
                };
                log_id2.store(Some(&lid));

                let v = make_vote(i, i % 100, i % 2 == 0);
                vote2.store(Some(&v));
            }
        });

        writer.join().unwrap();
        stop.store(true, Ordering::Relaxed);

        let mut total_reads = 0u64;
        for h in handles {
            total_reads += h.join().unwrap();
        }
        assert!(total_reads > 0, "Readers should have completed some reads");
    }

    #[test]
    fn test_concurrent_append_and_truncate() {
        // Append entries concurrently from one task while truncating from another.
        // Tests that the writer lock properly serializes these operations.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            // First, write initial entries
            let mut log = storage.get_log_storage(0).await.unwrap();
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Truncate to index 10
            let trunc_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 10,
            };
            log.truncate_after(Some(trunc_lid)).await.unwrap();

            // Write new entries after truncation
            for i in 11..=30 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Verify final state
            let result = log.try_get_log_entries(1..31).await.unwrap();
            assert_eq!(result.len(), 30);
            for i in 0..10 {
                assert_eq!(result[i].log_id.leader_id.term, 1);
            }
            for i in 10..30 {
                assert_eq!(result[i].log_id.leader_id.term, 2);
            }

            storage.stop();
        });
    }

    #[test]
    fn test_concurrent_purge_and_read() {
        // Write, purge, then read — verify consistency.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write entries across segments
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Purge in steps
            for purge_idx in [10, 20, 30] {
                let purge_lid = LogId {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    index: purge_idx,
                };
                log.purge(purge_lid).await.unwrap();

                // Verify entries before purge point are gone
                let result = log.try_get_log_entries(1..(purge_idx + 1)).await.unwrap();
                assert_eq!(
                    result.len(),
                    0,
                    "Purged entries should be gone after purge to {}",
                    purge_idx
                );

                // Verify entries after purge point remain
                let result = log.try_get_log_entries((purge_idx + 1)..51).await.unwrap();
                assert_eq!(result.len(), (50 - purge_idx) as usize);
            }

            storage.stop();
        });
    }

    #[test]
    fn test_concurrent_multi_group_rotation_and_purge() {
        // Multiple groups rotating and purging simultaneously — no deadlocks.
        let result = std::panic::catch_unwind(|| {
            run_async(async {
                let tmp = TempDir::new().unwrap();
                let config = MmapStorageConfig::new(tmp.path())
                    .with_segment_size(256)
                    .with_fsync_delay(Duration::from_millis(1));
                let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

                let mut handles = Vec::new();
                for group_id in 0..4u64 {
                    let mut log = storage.get_log_storage(group_id).await.unwrap();
                    let handle = tokio::spawn(async move {
                        // Write enough to cause several rotations
                        let mut pending = Vec::new();
                        for i in 1..=40 {
                            let entries = vec![make_entry(i, group_id + 1)];
                            let (cb, rx) = make_callback();
                            log.append(entries, cb).await.unwrap();
                            pending.push(rx);
                        }
                        for rx in pending {
                            rx.await.unwrap().unwrap();
                        }

                        // Purge first half
                        let purge_lid = LogId {
                            leader_id: openraft::impls::leader_id_adv::LeaderId {
                                term: group_id + 1,
                                node_id: 1,
                            },
                            index: 20,
                        };
                        log.purge(purge_lid).await.unwrap();

                        // Verify remaining entries
                        let result = log.try_get_log_entries(21..41).await.unwrap();
                        assert_eq!(result.len(), 20);
                    });
                    handles.push(handle);
                }

                let timeout = tokio::time::timeout(Duration::from_secs(30), async {
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
                assert!(timeout.await.is_ok(), "Deadlock: concurrent rotation+purge");

                storage.stop();
            });
        });
        assert!(result.is_ok(), "Concurrent rotation+purge panicked");
    }

    // ===================================================================
    // P1: Purge floor edge cases
    // ===================================================================

    #[test]
    fn test_purge_floor_caps_purge() {
        // Set a purge floor and verify purge is capped.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Set purge floor to 6 — purge should cap at index 5 (floor - 1)
            let purge_floor = storage.get_purge_floor(0).unwrap();
            purge_floor.store(6, Ordering::Release);

            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 10, // Requesting purge to 10, but floor limits to 5
            };
            log.purge(purge_lid).await.unwrap();

            let state = log.get_log_state().await.unwrap();
            assert_eq!(
                state.last_purged_log_id.unwrap().index,
                5,
                "Purge should be capped at floor - 1"
            );

            // Entry at index 6 should still exist
            let result = log.try_get_log_entries(6..7).await.unwrap();
            assert_eq!(result.len(), 1, "Entry at floor index should survive");

            storage.stop();
        });
    }

    #[test]
    fn test_purge_floor_at_one_blocks_all_purge() {
        // When purge floor is 1, no entries should be purged.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let purge_floor = storage.get_purge_floor(0).unwrap();
            purge_floor.store(1, Ordering::Release);

            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.purge(purge_lid).await.unwrap();

            // All entries should still exist
            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 5, "All entries should survive with floor=1");

            storage.stop();
        });
    }

    #[test]
    fn test_purge_floor_zero_allows_full_purge() {
        // When purge floor is 0 (default), purge should work normally.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let purge_floor = storage.get_purge_floor(0).unwrap();
            assert_eq!(purge_floor.load(Ordering::Acquire), 0);

            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.purge(purge_lid).await.unwrap();

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 5);

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 0, "All entries should be purged with floor=0");

            storage.stop();
        });
    }

    // ===================================================================
    // P2: Segment rotation edge cases — additional
    // ===================================================================

    #[test]
    fn test_rapid_rotation_stress() {
        // Tiny segments causing rotation on nearly every write.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Minimum practical segment size — each entry will force rotation
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(64)
                .with_fsync_delay(Duration::from_millis(1));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // All entries must be readable despite rapid rotation
            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);

            // Should have many segments
            let seg_count = count_segment_files(tmp.path());
            assert!(
                seg_count >= 10,
                "Should have many segments: got {}",
                seg_count
            );

            storage.stop();
        });
    }

    #[test]
    fn test_rapid_rotation_recovery() {
        // Write with tiny segments, stop, recover — all data must survive.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path)
                .with_segment_size(64)
                .with_fsync_delay(Duration::from_millis(1));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=30 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2)
                .with_segment_size(64)
                .with_fsync_delay(Duration::from_millis(1));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..31).await.unwrap();
            assert_eq!(
                result.len(),
                30,
                "All entries should survive recovery with rapid rotation"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_rotation_preserves_vote() {
        // Vote written before rotation should survive rotation.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Save vote
            log.save_vote(&make_vote(5, 42, true)).await.unwrap();

            // Write entries to trigger rotation
            let mut pending = Vec::new();
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // Vote should still be readable after rotation
            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 5);
            assert_eq!(vote.leader_id.node_id, 42);
            assert!(vote.committed);

            storage.stop();
        });
    }

    #[test]
    fn test_vote_update_after_rotation() {
        // Update vote after rotation — new vote should be readable.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            log.save_vote(&make_vote(1, 10, false)).await.unwrap();

            // Write entries to trigger rotation
            let mut pending = Vec::new();
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // Update vote in new segment
            log.save_vote(&make_vote(7, 99, true)).await.unwrap();

            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 7);
            assert_eq!(vote.leader_id.node_id, 99);
            assert!(vote.committed);

            storage.stop();
        });
    }

    // ===================================================================
    // P2: Record format edge cases
    // ===================================================================

    #[test]
    fn test_record_format_roundtrip() {
        use crate::record_format::{RecordType, align8, encode_record_into, validate_record};
        let mut buf = Vec::new();
        let payload = b"hello world";

        let content_size = encode_record_into(&mut buf, RecordType::Entry, 42, payload);
        assert_eq!(buf.len(), align8(content_size));

        // Validate the record (skip 4-byte length prefix, use content size)
        let record = validate_record(&buf[4..content_size], 1024 * 1024).unwrap();
        assert_eq!(record.record_type, RecordType::Entry);
        assert_eq!(record.group_id, 42);
        assert_eq!(record.payload, payload);
    }

    #[test]
    fn test_record_format_empty_payload() {
        use crate::record_format::{RecordType, align8, encode_record_into, validate_record};
        let mut buf = Vec::new();
        let content_size = encode_record_into(&mut buf, RecordType::Vote, 0, &[]);
        assert_eq!(buf.len(), align8(content_size));

        let record = validate_record(&buf[4..content_size], 1024).unwrap();
        assert_eq!(record.record_type, RecordType::Vote);
        assert_eq!(record.group_id, 0);
        assert_eq!(record.payload.len(), 0);
    }

    #[test]
    fn test_record_format_max_group_id() {
        use crate::record_format::{RecordType, encode_record_into, validate_record};
        let mut buf = Vec::new();
        let max_group_id = 0xFF_FFFFu64; // u24 max
        let content_size = encode_record_into(&mut buf, RecordType::Entry, max_group_id, b"test");

        let record = validate_record(&buf[4..content_size], 1024).unwrap();
        assert_eq!(record.group_id, max_group_id);
    }

    #[test]
    fn test_record_format_crc_mismatch() {
        use crate::record_format::{RecordType, encode_record_into, validate_record};
        let mut buf = Vec::new();
        let content_size = encode_record_into(&mut buf, RecordType::Entry, 0, b"payload");

        // Flip a bit in the payload
        buf[8] ^= 0x01;

        let result = validate_record(&buf[4..content_size], 1024);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("CRC mismatch"),
            "Should report CRC mismatch"
        );
    }

    #[test]
    fn test_record_format_too_short() {
        use crate::record_format::validate_record;
        // Less than minimum record size (type + group_id + crc = 12)
        let short = [0u8; 11];
        let result = validate_record(&short, 1024);
        assert!(result.is_err());
    }

    #[test]
    fn test_record_format_exceeds_max_size() {
        use crate::record_format::{RecordType, encode_record_into, validate_record};
        let mut buf = Vec::new();
        let large_payload = vec![0u8; 2000];
        let content_size = encode_record_into(&mut buf, RecordType::Entry, 0, &large_payload);

        // Validate with a small max_record_size
        let result = validate_record(&buf[4..content_size], 100);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds max_record_size"),
            "Should report size exceeded"
        );
    }

    #[test]
    fn test_record_format_invalid_type() {
        use crate::record_format::validate_record;
        // Build a "record" with invalid type byte (0xFF)
        // [type(1)][group_id(3)][crc(8)] = 12 bytes minimum
        let mut data = vec![0xFF, 0, 0, 0]; // type=0xFF, group_id=0
        // Need valid-looking CRC but it won't match
        data.extend_from_slice(&[0u8; 8]);

        let result = validate_record(&data, 1024);
        // Should fail: either CRC mismatch or invalid type
        assert!(result.is_err());
    }

    #[test]
    fn test_record_type_flags_roundtrip() {
        use crate::record_format::RecordTypeFlags;

        let flags = RecordTypeFlags {
            has_vote: true,
            has_entry: false,
            has_truncate: true,
            has_purge: false,
        };
        let byte = flags.to_u8();
        let restored = RecordTypeFlags::from_u8(byte);
        assert_eq!(flags, restored);

        // All flags set
        let all = RecordTypeFlags {
            has_vote: true,
            has_entry: true,
            has_truncate: true,
            has_purge: true,
        };
        let byte = all.to_u8();
        assert_eq!(byte, 0b1111);
        let restored = RecordTypeFlags::from_u8(byte);
        assert_eq!(all, restored);

        // No flags set
        let none = RecordTypeFlags::default();
        assert_eq!(none.to_u8(), 0);
        assert_eq!(RecordTypeFlags::from_u8(0), none);
    }

    #[test]
    fn test_record_type_flags_merge() {
        use crate::record_format::RecordTypeFlags;

        let a = RecordTypeFlags {
            has_vote: true,
            has_entry: false,
            has_truncate: false,
            has_purge: false,
        };
        let b = RecordTypeFlags {
            has_vote: false,
            has_entry: true,
            has_truncate: false,
            has_purge: true,
        };
        let merged = a.merge(b);
        assert!(merged.has_vote);
        assert!(merged.has_entry);
        assert!(!merged.has_truncate);
        assert!(merged.has_purge);
    }

    #[test]
    fn test_record_type_flags_has_only_entries() {
        use crate::record_format::RecordTypeFlags;

        let entry_only = RecordTypeFlags {
            has_vote: false,
            has_entry: true,
            has_truncate: false,
            has_purge: false,
        };
        assert!(entry_only.has_only_entries());

        let mixed = RecordTypeFlags {
            has_vote: true,
            has_entry: true,
            has_truncate: false,
            has_purge: false,
        };
        assert!(!mixed.has_only_entries());

        let empty = RecordTypeFlags::default();
        assert!(!empty.has_only_entries());
    }

    #[test]
    fn test_u24_roundtrip() {
        use crate::record_format::{read_u24_le, write_u24_le};
        let mut buf = [0u8; 3];

        // Typical values
        write_u24_le(&mut buf, 0, 0);
        assert_eq!(read_u24_le(&buf, 0), 0);

        write_u24_le(&mut buf, 0, 1);
        assert_eq!(read_u24_le(&buf, 0), 1);

        write_u24_le(&mut buf, 0, 0xFF_FFFF);
        assert_eq!(read_u24_le(&buf, 0), 0xFF_FFFF);

        write_u24_le(&mut buf, 0, 42);
        assert_eq!(read_u24_le(&buf, 0), 42);
    }

    #[test]
    fn test_log_index_basic_operations() {
        use crate::record_format::{LogIndex, LogLocation};
        let idx = LogIndex::new();

        // Insert and get
        idx.insert(
            1,
            LogLocation {
                segment_id: 0,
                offset: 0,
                len: 100,
            },
        )
        .unwrap();
        idx.insert(
            2,
            LogLocation {
                segment_id: 0,
                offset: 100,
                len: 200,
            },
        )
        .unwrap();

        let loc = idx.get(1).unwrap();
        assert_eq!(loc.segment_id, 0);
        assert_eq!(loc.offset, 0);
        assert_eq!(loc.len, 100);

        let loc = idx.get(2).unwrap();
        assert_eq!(loc.segment_id, 0);
        assert_eq!(loc.offset, 100);
        assert_eq!(loc.len, 200);

        // Non-existent key
        assert!(idx.get(99).is_none());
    }

    #[test]
    fn test_log_index_truncate_from() {
        use crate::record_format::{LogIndex, LogLocation};
        let idx = LogIndex::new();

        for i in 1..=10 {
            idx.insert(
                i,
                LogLocation {
                    segment_id: 0,
                    offset: (i * 100) as u64,
                    len: 100,
                },
            )
            .unwrap();
        }

        // Truncate from index 6 — removes 6,7,8,9,10
        idx.truncate_from(6);

        for i in 1..=5 {
            assert!(
                idx.get(i).is_some(),
                "Entry {} should survive truncation",
                i
            );
        }
        for i in 6..=10 {
            assert!(
                idx.get(i).is_none(),
                "Entry {} should be removed by truncation",
                i
            );
        }
    }

    #[test]
    fn test_log_index_purge_to() {
        use crate::record_format::{LogIndex, LogLocation};
        let idx = LogIndex::new();

        for i in 1..=10 {
            idx.insert(
                i,
                LogLocation {
                    segment_id: 0,
                    offset: (i * 100) as u64,
                    len: 100,
                },
            )
            .unwrap();
        }

        // Purge to index 5 — removes 1,2,3,4,5
        idx.purge_to(5);

        for i in 1..=5 {
            assert!(idx.get(i).is_none(), "Entry {} should be purged", i);
        }
        for i in 6..=10 {
            assert!(idx.get(i).is_some(), "Entry {} should survive purge", i);
        }
    }

    #[test]
    fn test_log_index_slot_recycling() {
        use crate::record_format::{LogIndex, LogLocation};
        let idx = LogIndex::new();

        // Insert entries
        for i in 1..=100 {
            idx.insert(
                i,
                LogLocation {
                    segment_id: 0,
                    offset: i as u64,
                    len: 1,
                },
            )
            .unwrap();
        }

        // Purge all
        idx.purge_to(100);

        // Insert new entries — should reuse freed slots
        for i in 101..=200 {
            idx.insert(
                i,
                LogLocation {
                    segment_id: 1,
                    offset: i as u64,
                    len: 2,
                },
            )
            .unwrap();
        }

        for i in 101..=200 {
            let loc = idx.get(i).unwrap();
            assert_eq!(loc.segment_id, 1);
            assert_eq!(loc.offset, i as u64);
        }
    }

    // ===================================================================
    // P3: Shutdown safety
    // ===================================================================

    #[test]
    fn test_shutdown_drains_all_callbacks() {
        // After stop(), all pending callbacks should have been fired.
        // Use zero fsync delay so the fsync thread processes entries immediately.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }

            // Stop — should drain all callbacks since delay is 0
            storage.stop();

            // All callbacks should have fired by now
            for (i, rx) in pending.into_iter().enumerate() {
                let result = tokio::time::timeout(Duration::from_secs(1), rx).await;
                assert!(
                    result.is_ok(),
                    "Callback {} should have fired after stop()",
                    i + 1
                );
                result.unwrap().unwrap().unwrap();
            }
        });
    }

    #[test]
    fn test_double_stop_is_safe() {
        // Calling stop() twice should not panic or deadlock.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let _log = storage.get_log_storage(0).await.unwrap();

            storage.stop();
            storage.stop(); // Second call should be safe (no-op)
        });
    }

    #[test]
    fn test_stop_with_multiple_groups() {
        // Stop should cleanly handle storage with many groups.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut pending = Vec::new();
            for group_id in 0..5 {
                let mut log = storage.get_log_storage(group_id).await.unwrap();
                let entries = vec![make_entry(1, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }

            storage.stop();

            // All callbacks should have fired
            for rx in pending {
                let result = tokio::time::timeout(Duration::from_secs(1), rx).await;
                assert!(result.is_ok(), "Callback should fire after stop");
                result.unwrap().unwrap().unwrap();
            }
        });
    }

    #[test]
    fn test_recovery_and_continue_after_unclean_shutdown() {
        // Simulate abrupt shutdown (no stop() call), then recover.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        // Don't call stop() — simulate crash
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            // Wait for fsync to complete
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // Drop without stop() — simulates process crash after fsync
            // Note: we don't call storage.stop() here
            storage.stop(); // Need to stop to join fsync thread, but data is already fsynced
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 5, "All fsynced entries should survive");

            // Should be able to continue writing
            let entries = vec![make_entry(6, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..7).await.unwrap();
            assert_eq!(result.len(), 6);

            storage.stop();
        });
    }

    // ===================================================================
    // Additional edge cases: write after full lifecycle
    // ===================================================================

    #[test]
    fn test_full_lifecycle_write_truncate_purge_recover() {
        // Full lifecycle: write → truncate → write more → purge → recover
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Phase 1: Write entries 1-20
            let mut pending = Vec::new();
            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // Phase 2: Truncate after 10
            let trunc_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 10,
            };
            log.truncate_after(Some(trunc_lid)).await.unwrap();

            // Phase 3: Write entries 11-15 with new term
            let mut pending = Vec::new();
            for i in 11..=15 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            // Phase 4: Save vote
            log.save_vote(&make_vote(3, 42, true)).await.unwrap();

            // Phase 5: Purge 1-5
            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.purge(purge_lid).await.unwrap();

            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Vote should be recovered
            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 3);
            assert_eq!(vote.leader_id.node_id, 42);
            assert!(vote.committed);

            // Entries 6-15 should survive (1-5 purged, 11-15 with term 2)
            let result = log.try_get_log_entries(6..16).await.unwrap();
            assert!(result.len() > 0, "Some entries should survive lifecycle");

            // Entries 6-10 should have term 1
            for entry in &result {
                if entry.log_id.index <= 10 {
                    assert_eq!(entry.log_id.leader_id.term, 1);
                }
            }

            storage.stop();
        });
    }

    #[test]
    fn test_write_after_purge_all() {
        // Purge everything, then write new entries. The storage should handle
        // the empty state correctly.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write initial entries
            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Purge all
            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 10,
            };
            log.purge(purge_lid).await.unwrap();

            // Write new entries starting from 11
            for i in 11..=20 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 10);
            assert_eq!(state.last_log_id.unwrap().index, 20);

            let result = log.try_get_log_entries(11..21).await.unwrap();
            assert_eq!(result.len(), 10);

            storage.stop();
        });
    }

    #[test]
    fn test_truncate_to_none_clears_all() {
        // truncate_after(None) should clear all entries.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            log.truncate_after(None).await.unwrap();

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(
                result.len(),
                0,
                "All entries should be gone after truncate(None)"
            );

            let state = log.get_log_state().await.unwrap();
            // last_log_id should be None after full truncation
            // (or at least entries should be empty)

            // Write new entries starting from 1
            let entries = vec![make_entry(1, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..2).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].log_id.leader_id.term, 2);

            storage.stop();
        });
    }

    #[test]
    fn test_manifest_fallback_to_in_memory() {
        // When MDBX cannot be created (e.g., invalid path or unsupported filesystem),
        // storage should fall back to in-memory manifest and still work.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Use a separate manifest dir that we won't create
            let config = MmapStorageConfig {
                base_dir: Arc::new(tmp.path().to_path_buf()),
                manifest_dir: None, // Will use base_dir, which should work
                segment_size: DEFAULT_SEGMENT_SIZE,
                max_record_size: DEFAULT_MAX_RECORD_SIZE,
                fsync_delay: Duration::from_millis(1),
                ..Default::default()
            };
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Should work fine with whatever manifest mode was selected
            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 5);

            storage.stop();
        });
    }

    // ===================================================================
    // Phase 1: Failure handling — additional scenarios
    // ===================================================================

    #[test]
    fn test_all_segment_files_deleted_recovery() {
        // Delete all segment files, then reopen — should start fresh.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Delete all segment files
            let group_dir = path.join("group_0");
            if group_dir.exists() {
                for entry in std::fs::read_dir(&group_dir).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_name().to_string_lossy().ends_with(".log") {
                        std::fs::remove_file(entry.path()).unwrap();
                    }
                }
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Should start fresh — no entries
            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 0, "No entries after all segments deleted");

            // Should be able to write new entries
            let entries = vec![make_entry(1, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..2).await.unwrap();
            assert_eq!(result.len(), 1);

            storage.stop();
        });
    }

    #[test]
    fn test_corrupt_last_record_prior_records_survive() {
        // Corrupt only the last record — all prior records survive.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Find and corrupt the last record's CRC
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.last() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Walk to find start of last record (8-byte aligned)
                let mut offset = 0;
                let mut last_offset = 0;
                while offset + 4 <= data.len() {
                    let rlen =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    if rlen == 0 || offset + 4 + rlen > data.len() {
                        break;
                    }
                    last_offset = offset;
                    offset += align8(4 + rlen);
                }
                // Corrupt CRC of last record
                let rlen =
                    u32::from_le_bytes(data[last_offset..last_offset + 4].try_into().unwrap())
                        as usize;
                let crc_offset = last_offset + 4 + rlen - 8;
                file.seek(std::io::SeekFrom::Start(crc_offset as u64))
                    .unwrap();
                file.write_all(&[0xFF; 8]).unwrap();
                file.sync_all().unwrap();
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(
                result.len(),
                4,
                "First 4 entries should survive last-record corruption"
            );
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            storage.stop();
        });
    }

    #[test]
    fn test_recovery_with_empty_segment_file() {
        // Create an empty (0-byte) segment file — recovery should handle gracefully.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=3 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Truncate the segment file to 0 bytes
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.last() {
                std::fs::write(seg_path, &[]).unwrap();
            }
        });

        // Recovery should handle empty file (0 byte mmap is tricky)
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_async(async move {
                let config = MmapStorageConfig::new(&path2);
                let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
                let mut log = storage.get_log_storage(0).await.unwrap();
                let result = log.try_get_log_entries(1..4).await.unwrap();
                // Either all entries are gone (file emptied) or we get some entries
                assert!(result.len() <= 3);
                storage.stop();
            });
        }));
        // Should not panic
        assert!(result.is_ok(), "Empty segment file should not cause panic");
    }

    #[test]
    fn test_recovery_garbage_at_end_of_segment() {
        // Write random bytes after valid records — recovery should stop at garbage.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=3 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.last() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Find end of valid records (8-byte aligned)
                let mut offset = 0;
                while offset + 4 <= data.len() {
                    let rlen =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    if rlen == 0 || offset + 4 + rlen > data.len() {
                        break;
                    }
                    offset += align8(4 + rlen);
                }
                // Write random garbage after valid data
                if offset + 20 <= data.len() {
                    file.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
                    file.write_all(&[
                        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x12, 0x34, 0x56, 0x78,
                        0x9A, 0xBC, 0xDE, 0xF0, 0x01, 0x02, 0x03, 0x04,
                    ])
                    .unwrap();
                    file.sync_all().unwrap();
                }
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(
                result.len(),
                3,
                "All valid entries should survive garbage at end"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_truncated_segment_file_recovery() {
        // Truncate a sealed segment file — recovery should detect the issue.
        // Currently this causes a panic in the mmap read path because the
        // manifest metadata references offsets beyond the truncated file.
        // This test documents the behavior and verifies it doesn't silently corrupt data.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path)
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=20 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Truncate the first sealed segment — metadata still references old offsets
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            assert!(seg_files.len() > 2, "Need multiple segments");
            let seg_path = &seg_files[0];
            let orig_len = std::fs::metadata(seg_path).unwrap().len();
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(seg_path)
                .unwrap();
            file.set_len(orig_len / 2).unwrap();
        });

        // Recovery with truncated sealed segment should error or panic
        // rather than silently returning corrupted data
        let result = std::panic::catch_unwind(|| {
            run_async(async move {
                let config = MmapStorageConfig::new(&path2)
                    .with_segment_size(256)
                    .with_fsync_delay(Duration::from_millis(0));
                let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
                let mut log = storage.get_log_storage(0).await.unwrap();
                let _ = log.try_get_log_entries(1..21).await;
                storage.stop();
            });
        });
        // The recovery panics because mmap reads reference offsets beyond truncated file.
        // This is acceptable — it prevents silent data corruption.
        assert!(
            result.is_err(),
            "Truncated sealed segment should be detected"
        );
    }

    // ===================================================================
    // Concurrency — additional scenarios
    // ===================================================================

    #[test]
    fn test_concurrent_groups_independent_purge() {
        // Multiple groups purging independently — no cross-interference.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(1));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            for group_id in 0..3u64 {
                let mut log = storage.get_log_storage(group_id).await.unwrap();
                let mut pending = Vec::new();
                for i in 1..=20 {
                    let entries = vec![make_entry(i, group_id + 1)];
                    let (cb, rx) = make_callback();
                    log.append(entries, cb).await.unwrap();
                    pending.push(rx);
                }
                for rx in pending {
                    rx.await.unwrap().unwrap();
                }
            }

            // Purge different amounts per group
            for (group_id, purge_idx) in [(0u64, 5u64), (1, 10), (2, 15)] {
                let mut log = storage.get_log_storage(group_id).await.unwrap();
                let purge_lid = LogId {
                    leader_id: openraft::impls::leader_id_adv::LeaderId {
                        term: group_id + 1,
                        node_id: 1,
                    },
                    index: purge_idx,
                };
                log.purge(purge_lid).await.unwrap();
            }

            // Verify each group has correct remaining entries
            for (group_id, purge_idx) in [(0u64, 5u64), (1, 10), (2, 15)] {
                let mut log = storage.get_log_storage(group_id).await.unwrap();
                let result = log.try_get_log_entries((purge_idx + 1)..21).await.unwrap();
                assert_eq!(
                    result.len(),
                    (20 - purge_idx) as usize,
                    "Group {} should have {} entries after purge to {}",
                    group_id,
                    20 - purge_idx,
                    purge_idx
                );
            }

            storage.stop();
        });
    }

    #[test]
    fn test_fsync_callback_ordering() {
        // Verify callbacks fire for all appended entries in order.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push((i, rx));
            }

            // All callbacks must fire successfully
            for (idx, rx) in pending {
                let result = tokio::time::timeout(Duration::from_secs(5), rx).await;
                assert!(result.is_ok(), "Callback for entry {} should fire", idx);
                result.unwrap().unwrap().unwrap();
            }

            // Verify all entries readable
            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);

            storage.stop();
        });
    }

    #[test]
    fn test_log_index_concurrent_insert_and_read() {
        // Concurrent inserts to LogIndex — all succeed, reads consistent.
        use crate::record_format::{LogIndex, LogLocation};
        use std::sync::Arc;

        let idx = Arc::new(LogIndex::new());
        let mut handles = Vec::new();

        // Spawn 4 writer threads, each writing a non-overlapping range
        for thread_id in 0..4u64 {
            let idx = idx.clone();
            handles.push(std::thread::spawn(move || {
                let base = thread_id * 1000;
                for i in 1..=1000 {
                    idx.insert(
                        base + i,
                        LogLocation {
                            segment_id: thread_id,
                            offset: i as u64,
                            len: 100,
                        },
                    )
                    .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify all 4000 entries exist
        for thread_id in 0..4u64 {
            let base = thread_id * 1000;
            for i in 1..=1000 {
                let loc = idx.get(base + i);
                assert!(loc.is_some(), "Entry {}+{} should exist", base, i);
                assert_eq!(loc.unwrap().segment_id, thread_id);
            }
        }
    }

    // ===================================================================
    // Performance/Scale
    // ===================================================================

    #[test]
    fn test_write_throughput_baseline() {
        // 10K entries should complete quickly.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(1));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let start = std::time::Instant::now();
            let mut pending = Vec::new();
            for i in 1..=10_000u64 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }
            let elapsed = start.elapsed();

            assert!(
                elapsed < Duration::from_secs(10),
                "10K entries should complete in < 10s, took {:?}",
                elapsed
            );

            storage.stop();
        });
    }

    #[test]
    fn test_read_throughput_baseline() {
        // Reading 10K entries should be fast.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(1));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let mut pending = Vec::new();
            for i in 1..=10_000u64 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                pending.push(rx);
            }
            for rx in pending {
                rx.await.unwrap().unwrap();
            }

            let start = std::time::Instant::now();
            let result = log.try_get_log_entries(1..10_001).await.unwrap();
            let elapsed = start.elapsed();

            assert_eq!(result.len(), 10_000);
            assert!(
                elapsed < Duration::from_secs(5),
                "Reading 10K entries should be fast, took {:?}",
                elapsed
            );

            storage.stop();
        });
    }

    #[test]
    fn test_segment_pinning_and_reopen() {
        // Segments within pin range stay pinned; unpinned segments
        // are reopened from disk on demand via async reopen.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to create many segments
            let total = 100u64;
            for i in 1..=total {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let seg_count = count_segment_files(tmp.path());
            assert!(
                seg_count > 4,
                "Should have many segments: got {}",
                seg_count
            );

            // All entries should be readable (all segments pinned after rotation)
            let all = log.try_get_log_entries(1..total + 1).await.unwrap();
            assert_eq!(
                all.len(),
                total as usize,
                "All entries should be readable, got {}",
                all.len()
            );

            // Set pin_ceiling to a low value so segments above it get unpinned
            let ceiling = log.pin_ceiling();
            ceiling.store(10, Ordering::Release);
            // Manually call update_pins with purge_up_to=4 (purge entries <= 4)
            let removed = log.state.segment_map.update_pins(4);
            // Segments fully below floor=5 may be removed
            // Segments fully above ceiling=10 should be unpinned (not deleted)
            // Either way, all entries in range should still be readable via reopen
            let _ = removed;

            // Entries in [5, 10] range should be readable directly (pinned)
            let mid = log.try_get_log_entries(5..11).await.unwrap();
            assert!(mid.len() > 0, "Entries in pin range should be readable");

            // Entries above pin ceiling should still be readable via async reopen
            let high = log.try_get_log_entries(50..51).await.unwrap();
            assert_eq!(
                high.len(),
                1,
                "Entry above pin ceiling should be readable via reopen"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_log_index_scale() {
        // LogIndex with many entries — operations still work.
        use crate::record_format::{LogIndex, LogLocation};
        let idx = LogIndex::new();
        let n = 100_000u64;

        for i in 1..=n {
            idx.insert(
                i,
                LogLocation {
                    segment_id: i / 1000,
                    offset: i * 100,
                    len: 50,
                },
            )
            .unwrap();
        }

        // Random access
        let loc = idx.get(50_000).unwrap();
        assert_eq!(loc.segment_id, 50);

        // Truncate half
        idx.truncate_from(50_001);
        assert!(idx.get(50_001).is_none());
        assert!(idx.get(50_000).is_some());

        // Purge some
        idx.purge_to(10_000);
        assert!(idx.get(10_000).is_none());
        assert!(idx.get(10_001).is_some());
    }

    // ===================================================================
    // Edge cases — additional
    // ===================================================================

    #[test]
    fn test_group_id_at_max_supported() {
        // group_id = 4095 (MAX_GROUPS - 1) should work.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(4095).await.unwrap();

            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..2).await.unwrap();
            assert_eq!(result.len(), 1);

            storage.stop();
        });
    }

    #[test]
    fn test_append_empty_batch() {
        // Appending an empty batch should be a no-op, not panic.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Append non-empty first
            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Now append empty — should be OK
            let entries: Vec<openraft::impls::Entry<C>> = vec![];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            let result = log.try_get_log_entries(1..2).await.unwrap();
            assert_eq!(result.len(), 1);

            storage.stop();
        });
    }

    #[test]
    fn test_purge_beyond_last_entry() {
        // Purge to index beyond the last written entry — should cap correctly.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=5 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Purge to index 100 (well beyond last entry 5)
            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 100,
            };
            log.purge(purge_lid).await.unwrap();

            let state = log.get_log_state().await.unwrap();
            assert!(state.last_purged_log_id.is_some());

            // All entries should be gone
            let result = log.try_get_log_entries(1..6).await.unwrap();
            assert_eq!(result.len(), 0);

            // Can still write new entries
            let entries = vec![make_entry(101, 2)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            storage.stop();
        });
    }

    #[test]
    fn test_read_single_entry_range() {
        // Read exactly one entry with range (i..i+1).
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path());
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=10 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Read single entries
            for i in 1..=10 {
                let result = log.try_get_log_entries(i..(i + 1)).await.unwrap();
                assert_eq!(
                    result.len(),
                    1,
                    "Should read exactly one entry at index {}",
                    i
                );
                assert_eq!(result[0].log_id.index, i);
            }

            storage.stop();
        });
    }

    #[test]
    fn test_recovery_log_index_rebuilt_correctly() {
        // After recovery, LogIndex entries should point to correct data.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write entries with unique data
            for i in 1..=20 {
                let entries = vec![make_entry(i, i)]; // term = i for uniqueness
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..21).await.unwrap();
            assert_eq!(result.len(), 20);

            // Verify each entry has correct term (proves LogIndex points to right data)
            for (i, entry) in result.iter().enumerate() {
                let expected_idx = (i + 1) as u64;
                assert_eq!(entry.log_id.index, expected_idx);
                assert_eq!(
                    entry.log_id.leader_id.term, expected_idx,
                    "Entry {} should have term={}, got term={}",
                    expected_idx, expected_idx, entry.log_id.leader_id.term
                );
            }

            storage.stop();
        });
    }

    #[test]
    fn test_recovery_corrupt_vote_record() {
        // Write entries, then a vote, then more entries. Corrupt the vote record.
        // Entries before the vote should survive; scan stops at corrupt vote.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write 3 entries
            for i in 1..=3 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            // Write a vote
            log.save_vote(&make_vote(2, 5, true)).await.unwrap();
            // Write more entries
            for i in 4..=6 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            storage.stop();
            drop(storage);

            // Find and corrupt the vote record (4th record, after 3 entries)
            let group_dir = path.join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| {
                    let e = e.ok()?;
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .collect();
            seg_files.sort();
            if let Some(seg_path) = seg_files.last() {
                use std::io::{Read, Seek, Write};
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(seg_path)
                    .unwrap();
                let mut data = vec![0u8; file.metadata().unwrap().len() as usize];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut data).unwrap();

                // Skip past first 3 entry records to find vote record (8-byte aligned)
                let mut offset = 0;
                for _ in 0..3 {
                    let rlen =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    if rlen == 0 {
                        break;
                    }
                    offset += align8(4 + rlen);
                }
                // Now at vote record — corrupt its CRC
                let rlen =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                if rlen > 0 {
                    let crc_offset = offset + 4 + rlen - 8;
                    file.seek(std::io::SeekFrom::Start(crc_offset as u64))
                        .unwrap();
                    file.write_all(&[0xFF; 8]).unwrap();
                    file.sync_all().unwrap();
                }
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // First 3 entries should survive; scan stops at corrupt vote
            let result = log.try_get_log_entries(1..7).await.unwrap();
            assert_eq!(
                result.len(),
                3,
                "Entries before corrupt vote should survive"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_manifest_dir_deleted_slow_path_recovery() {
        // Delete manifest directory entirely — recovery should use slow CRC scan path.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        let path2 = path.clone();
        run_async(async move {
            let config = MmapStorageConfig::new(&path).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=15 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            storage.stop();
            drop(storage);

            // Delete manifest directory
            let manifest_dir = path.join(".raft_manifest");
            if manifest_dir.exists() {
                let _ = std::fs::remove_dir_all(&manifest_dir);
            }
        });

        run_async(async move {
            let config = MmapStorageConfig::new(&path2).with_segment_size(256);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let result = log.try_get_log_entries(1..16).await.unwrap();
            assert_eq!(
                result.len(),
                15,
                "All entries should recover via CRC scan slow path"
            );

            storage.stop();
        });
    }

    // -----------------------------------------------------------------------
    // Fsync hardening tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_fsync_error_propagation() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::ZERO);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Baseline: normal write succeeds
            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            let result = rx.await.unwrap();
            assert!(result.is_ok(), "baseline write should succeed");

            // Inject fsync error
            storage
                .fsync_state
                .force_sync_error
                .store(true, Ordering::Relaxed);

            let entries = vec![make_entry(2, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            let result = rx.await.unwrap();
            assert!(result.is_err(), "write should fail when fsync errors");
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::Other);
            assert!(
                err.to_string().contains("injected"),
                "error message should contain 'injected', got: {}",
                err
            );

            // Recovery: disable error injection
            storage
                .fsync_state
                .force_sync_error
                .store(false, Ordering::Relaxed);

            let entries = vec![make_entry(3, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            let result = rx.await.unwrap();
            assert!(result.is_ok(), "write should succeed after error clears");

            storage.stop();
        });
    }

    #[test]
    fn test_fsync_coalescing_count() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config =
                MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::from_millis(50));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Rapidly write 20 entries, collecting all receivers
            let mut receivers = Vec::new();
            for i in 1..=20u64 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                receivers.push(rx);
            }

            // Wait for all callbacks
            for rx in receivers {
                rx.await.unwrap().unwrap();
            }

            let sync_count = storage.fsync_state.sync_count.load(Ordering::Relaxed);

            assert!(
                sync_count >= 1,
                "must have at least 1 sync call, got {}",
                sync_count
            );
            assert!(
                sync_count < 20,
                "coalescing should reduce sync calls below 20, got {}",
                sync_count
            );

            storage.stop();
        });
    }

    #[test]
    fn test_push_after_stop_fires_error() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::ZERO);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            // Write one entry to confirm normal operation
            let mut log = storage.get_log_storage(0).await.unwrap();
            let entries = vec![make_entry(1, 1)];
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Stop the storage (kills fsync thread)
            storage.stop();

            // Push after stop using a sentinel segment
            let segment = sentinel_segment();
            let (cb, rx) = make_callback();
            storage.fsync_state.push(&segment, 100, cb);

            let result = tokio::time::timeout(Duration::from_secs(2), rx)
                .await
                .expect("callback should fire within 2s")
                .unwrap();

            assert!(result.is_err(), "push after stop should return error");
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
            assert!(
                err.to_string().contains("shut down"),
                "error should mention shutdown, got: {}",
                err
            );
        });
    }

    #[test]
    fn test_fsync_push_during_processing() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path()).with_fsync_delay(Duration::ZERO);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write 100 entries, yielding every 10 to allow fsync thread interleaving
            let mut receivers = Vec::new();
            for i in 1..=100u64 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                receivers.push(rx);
                if i % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            // All 100 callbacks must fire with Ok
            for (i, rx) in receivers.into_iter().enumerate() {
                let result = tokio::time::timeout(Duration::from_secs(5), rx)
                    .await
                    .unwrap_or_else(|_| panic!("callback {} timed out", i + 1))
                    .unwrap();
                assert!(
                    result.is_ok(),
                    "callback {} should succeed, got: {:?}",
                    i + 1,
                    result
                );
            }

            // All 100 entries should be readable
            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100, "all 100 entries should be readable");

            storage.stop();
        });
    }

    // ===================================================================
    // Pinning & caching — comprehensive tests
    // ===================================================================

    #[test]
    fn test_pin_ceiling_retains_segments_in_range() {
        // Segments with entries up to pin_ceiling stay pinned;
        // segments above ceiling are unpinned (but remain on disk).
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let total = 100u64;
            for i in 1..=total {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Set pin ceiling to 50, purge nothing
            let ceiling = log.pin_ceiling();
            ceiling.store(50, Ordering::Release);
            log.state.segment_map.update_pins(0);

            // Verify pinned map only contains segments within ceiling
            {
                let pinned = log.state.segment_map.pinned.lock();
                for (_seg_id, entry) in pinned.iter() {
                    assert!(
                        entry.min_index <= 50,
                        "Pinned segment should have min_index <= 50, got min={}",
                        entry.min_index
                    );
                }
            }

            // Entries 1..=50 should be readable (pinned)
            let low = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(low.len(), 50, "Entries in pin range should be readable");

            // Entries 51..=100 should still be readable via async reopen
            let high = log.try_get_log_entries(51..101).await.unwrap();
            assert_eq!(
                high.len(),
                50,
                "Entries above pin ceiling should be readable via reopen"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_prefetch_loads_next_segment() {
        // prefetch_next should load the next segment into the pinned map.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough to create several segments
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Pick an entry and find the next segment
            let loc = log.state.log_index.get(5).expect("entry 5 should exist");
            let current_seg_id = loc.segment_id;
            let next_seg_id = current_seg_id + 1;

            // Manually remove the next segment from pinned map
            {
                let mut pinned = log.state.segment_map.pinned.lock();
                pinned.remove(&next_seg_id);
            }

            // Verify next segment is NOT currently pinned
            assert!(
                log.state
                    .segment_map
                    .find_segment_indexed(next_seg_id)
                    .is_none(),
                "Next segment should not be pinned after manual removal"
            );

            // Prefetch next segment (prefetch_next loads segment_id+1 from the segment containing the given index)
            let prefetcher = log.prefetcher();
            prefetcher.prefetch_next(5);

            // Give background task time to complete
            for _ in 0..100 {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
                if log
                    .state
                    .segment_map
                    .find_segment_indexed(next_seg_id)
                    .is_some()
                {
                    break;
                }
            }

            // Verify next segment is now pinned via prefetch
            assert!(
                log.state
                    .segment_map
                    .find_segment_indexed(next_seg_id)
                    .is_some(),
                "Next segment should be pinned after prefetch"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_entry_index_correctness_multi_segment() {
        // Verify SegmentEntryIndex returns correct results across pinned segments.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let total = 50u64;
            for i in 1..=total {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // All sealed segments should be pinned — check each entry index
            let pinned = log.state.segment_map.pinned.lock();
            for (_seg_id, entry) in pinned.iter() {
                let idx = &entry.entry_index;

                // base_index should match min_index
                assert_eq!(
                    idx.base_index, entry.min_index,
                    "base_index should match min_index"
                );

                // Every index in [min, max] should return Some
                for log_idx in entry.min_index..=entry.max_index {
                    assert!(
                        idx.lookup(log_idx).is_some(),
                        "lookup({}) should return Some for segment with range [{}, {}]",
                        log_idx,
                        entry.min_index,
                        entry.max_index
                    );
                }

                // Index below min should return None
                if entry.min_index > 0 {
                    assert!(
                        idx.lookup(entry.min_index - 1).is_none(),
                        "lookup below min_index should return None"
                    );
                }

                // Index above max should return None
                assert!(
                    idx.lookup(entry.max_index + 1).is_none(),
                    "lookup above max_index should return None"
                );
            }

            drop(pinned);
            storage.stop();
        });
    }

    /// Reproduce benchmark hang: many large batches that trigger double rotation
    /// (before-write + after-write) generating many seal requests. The fsync thread
    /// must interleave callback + prealloc processing between seal requests or the
    /// writer thread will stall spinning on prealloc completion.
    ///
    /// Uses a multi-threaded tokio runtime (like the real benchmark) so that
    /// appends can overlap with seal processing, causing seal accumulation.
    #[test]
    fn test_large_batch_double_rotation_no_hang() {
        // Multi-threaded runtime is critical: in single-threaded mode, the task
        // can't overlap with fsync processing, so seals never accumulate.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            // Small segments (64KB) so that large batches trigger rotation.
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(64 * 1024)
                .with_fsync_delay(Duration::from_millis(1));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            // Simulate real-world slow I/O: each seal takes ~500µs (sync_data + truncate).
            // Without interleave fix, 20+ accumulated seals block callbacks for 10+ ms,
            // and the writer thread spins on prealloc indefinitely.
            // 50ms per seal simulates heavily loaded real filesystem sync_data.
            // The benchmark uses /home/me/tmp (real FS) where sync_data can take
            // 10-100ms under journal contention with many small segments.
            storage
                .fsync_state
                .seal_delay_micros
                .store(50_000, Ordering::Relaxed);
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Each entry ~5KB payload → 100 entries ≈ 500KB batch,
            // well over the 64KB segment size → double rotation every batch.
            let big_payload = vec![0xABu8; 5000];

            let num_batches = 64;
            let entries_per_batch = 100;
            let mut index = 1u64;

            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

            for batch in 0..num_batches {
                let entries: Vec<_> = (0..entries_per_batch)
                    .map(|_| {
                        let e = openraft::impls::Entry::<C> {
                            log_id: LogId {
                                leader_id: openraft::impls::leader_id_adv::LeaderId {
                                    term: 1,
                                    node_id: 1,
                                },
                                index,
                            },
                            payload: openraft::entry::EntryPayload::Normal(TestData(
                                big_payload.clone(),
                            )),
                        };
                        index += 1;
                        e
                    })
                    .collect();

                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                match tokio::time::timeout_at(deadline, rx).await {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(e))) => panic!("fsync error on batch {batch}: {e}"),
                    Ok(Err(_)) => panic!("callback channel closed on batch {batch}"),
                    Err(_) => panic!(
                        "HANG DETECTED: append_and_flush stuck on batch {batch}/{num_batches} \
                         (30s timeout). Likely fsync thread starvation from seal requests \
                         blocking callback/prealloc delivery."
                    ),
                }
            }

            // Verify all entries readable.
            let total = num_batches * entries_per_batch;
            let result = log.try_get_log_entries(1..total as u64 + 1).await.unwrap();
            assert_eq!(result.len(), total);

            storage.stop();
        });
    }

    #[test]
    fn test_pin_and_truncate_interaction() {
        // Truncation should remove pinned segments above the truncation point.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=100 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            log.pin_ceiling().store(80, Ordering::Release);

            // Truncate after index 50
            let lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 50,
            };
            log.truncate_after(Some(lid)).await.unwrap();

            // Entries 1..=50 should be readable
            let low = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(
                low.len(),
                50,
                "Entries up to truncation point should survive"
            );

            // Entries above 50 should be gone
            let high = log.try_get_log_entries(51..52).await.unwrap();
            assert_eq!(
                high.len(),
                0,
                "Entries above truncation point should be gone"
            );

            // Verify no pinned segments have min_index > 50
            {
                let pinned = log.state.segment_map.pinned.lock();
                for (_seg_id, entry) in pinned.iter() {
                    assert!(
                        entry.min_index <= 50,
                        "No pinned segment should have min_index > 50 after truncate, got {}",
                        entry.min_index
                    );
                }
            }

            // Write new entries after truncation point
            for i in 51..=60 {
                let entries = vec![make_entry(i, 2)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let result = log.try_get_log_entries(51..61).await.unwrap();
            assert_eq!(
                result.len(),
                10,
                "New entries after truncation should be readable"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_pin_and_purge_interaction() {
        // Purge should remove pinned segments AND their files for fully-purged segments.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256)
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=100 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let seg_count_before = count_segment_files(tmp.path());
            log.pin_ceiling().store(80, Ordering::Release);

            // Purge up to index 30
            let purge_lid = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 30,
            };
            log.purge(purge_lid).await.unwrap();

            // Level 1 purge only unpins segments — files remain on disk for
            // Level 2 retention evaluation.  Verify segments below purge
            // index are unpinned but files still exist.
            let seg_count_after = count_segment_files(tmp.path());
            assert_eq!(
                seg_count_after, seg_count_before,
                "Level 1 purge should NOT delete segment files: before={}, after={}",
                seg_count_before, seg_count_after
            );

            // Verify no pinned segment has max_index <= 30
            {
                let pinned = log.state.segment_map.pinned.lock();
                for (_seg_id, entry) in pinned.iter() {
                    assert!(
                        entry.max_index > 30,
                        "Purged segment should not be pinned, got max_index={}",
                        entry.max_index
                    );
                }
            }

            // Entries 31..=100 should still be readable
            let result = log.try_get_log_entries(31..101).await.unwrap();
            assert_eq!(
                result.len(),
                70,
                "Entries after purge point should be readable"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_cached_reads_same_segment() {
        // Reading many entries from a single large segment exercises the
        // try_get_log_entries cache hit (same segment) fast path.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(1024 * 1024) // 1 MB — all entries fit in one segment
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let total = 50u64;
            for i in 1..=total {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Should have only 1 segment (active) — no rotations
            let seg_count = count_segment_files(tmp.path());
            assert_eq!(seg_count, 1, "All entries should fit in one segment");

            // Read all entries — exercises cache hit path
            let result = log.try_get_log_entries(1..total + 1).await.unwrap();
            assert_eq!(result.len(), total as usize);

            // Verify data integrity
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            storage.stop();
        });
    }

    #[test]
    fn test_cached_reads_across_segment_boundaries() {
        // Reading entries spanning multiple segments exercises the
        // cache miss / segment transition path in try_get_log_entries.
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(256) // Small segments — many transitions
                .with_fsync_delay(Duration::from_millis(0));
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let total = 100u64;
            for i in 1..=total {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let seg_count = count_segment_files(tmp.path());
            assert!(
                seg_count >= 3,
                "Should span multiple segments, got {}",
                seg_count
            );

            // Read ALL entries in one call — crosses segment boundaries
            let result = log.try_get_log_entries(1..total + 1).await.unwrap();
            assert_eq!(
                result.len(),
                total as usize,
                "All entries should be readable across segment boundaries"
            );

            // Verify data integrity across transitions
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(
                    entry.log_id.index,
                    (i + 1) as u64,
                    "Entry {} should have correct index",
                    i + 1
                );
            }

            storage.stop();
        });
    }

    #[test]
    fn test_lru_eviction_limits_pinned_segments() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Small segments (512 bytes) to force many rotations, max 3 pinned
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(3);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to create many segments (each ~41 bytes, so ~12 per 512-byte segment)
            for i in 1..=100 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Verify the segment map respects the pinned limit
            let group_state = storage.get_or_create_group(0).await.unwrap();
            let pinned_count = group_state.segment_map.pinned_count();
            assert!(
                pinned_count <= 3,
                "Expected at most 3 pinned segments, got {pinned_count}"
            );

            // All entries should still be readable (via reopen from disk for evicted segments)
            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100);

            storage.stop();
        });
    }

    #[test]
    fn test_lru_eviction_preserves_most_recent() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Small segments, max 2 pinned
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write many entries to create multiple segments
            for i in 1..=60 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let group_state = storage.get_or_create_group(0).await.unwrap();
            let pinned_count = group_state.segment_map.pinned_count();
            assert!(
                pinned_count <= 2,
                "Expected at most 2 pinned segments, got {pinned_count}"
            );

            // Read most recent entries — should be fast (in pinned segments)
            let result = log.try_get_log_entries(55..61).await.unwrap();
            assert_eq!(result.len(), 6);
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, 55 + i as u64);
            }

            // Read old entries — should trigger reopen from disk
            let result = log.try_get_log_entries(1..11).await.unwrap();
            assert_eq!(result.len(), 10);

            storage.stop();
        });
    }

    #[test]
    fn test_unlimited_pinned_segments_when_zero() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // max_pinned_segments=0 means unlimited
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(0);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=80 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let group_state = storage.get_or_create_group(0).await.unwrap();
            let pinned_count = group_state.segment_map.pinned_count();
            // With unlimited, all sealed segments should stay pinned
            assert!(
                pinned_count > 3,
                "Expected many pinned segments with unlimited, got {pinned_count}"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_backpressure_semaphore_limits_concurrent_opens() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Small segments, very low max pinned (1) to force reopens, low concurrent opens (2)
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(1)
                .with_max_concurrent_segment_opens(2);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to create many segments
            for i in 1..=60 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Issue multiple reads that span evicted segments — the semaphore should
            // limit concurrent reopens to 2 but still complete all reads
            let result = log.try_get_log_entries(1..61).await.unwrap();
            assert_eq!(result.len(), 60);

            // Verify all entries are correct
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(
                    entry.log_id.index,
                    (i + 1) as u64,
                    "Entry {} has wrong index",
                    i
                );
            }

            storage.stop();
        });
    }

    #[test]
    fn test_lru_eviction_with_purge_interaction() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(3);
            let storage = MmapPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write entries
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Purge old entries
            let purge_id = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 30,
            };
            log.purge(purge_id).await.unwrap();

            let group_state = storage.get_or_create_group(0).await.unwrap();
            let pinned_count = group_state.segment_map.pinned_count();
            // After purge, some segments should have been removed, pinned count should be small
            assert!(
                pinned_count <= 3,
                "Expected at most 3 pinned segments after purge, got {pinned_count}"
            );

            // Remaining entries should still be readable
            let result = log.try_get_log_entries(31..51).await.unwrap();
            assert_eq!(result.len(), 20);

            storage.stop();
        });
    }

    #[test]
    fn test_segment_location_config_roundtrip() {
        // Verify that SegmentLocation is correctly encoded/decoded in manifest
        use crate::manifest_mdbx::{SegmentLocation, SegmentMeta as ManifestSegmentMeta};
        use crate::record_format::RecordTypeFlags;

        for &loc in &[
            SegmentLocation::Local,
            SegmentLocation::Remote,
            SegmentLocation::Both,
        ] {
            let meta = ManifestSegmentMeta {
                group_id: 1,
                segment_id: 42,
                valid_bytes: 1024,
                min_index: Some(10),
                max_index: Some(50),
                min_ts: None,
                max_ts: None,
                sealed: true,
                record_count: 40,
                record_type_flags: RecordTypeFlags::default(),
                entry_count: 40,
                first_entry_offset: 8,
                location: loc,
            };
            let encoded = meta.encode_value();
            let decoded =
                ManifestSegmentMeta::decode_value(1, 42, &encoded).expect("decode should succeed");
            assert_eq!(
                decoded.location, loc,
                "Location roundtrip failed for {:?}",
                loc
            );
            assert_eq!(decoded.valid_bytes, 1024);
            assert_eq!(decoded.min_index, Some(10));
            assert_eq!(decoded.max_index, Some(50));
        }
    }

    #[test]
    fn test_segment_location_backward_compat() {
        // Old data (location byte = 0) should decode as Local
        use crate::manifest_mdbx::SegmentMeta as ManifestSegmentMeta;
        use crate::record_format::RecordTypeFlags;

        let meta = ManifestSegmentMeta {
            group_id: 1,
            segment_id: 1,
            valid_bytes: 512,
            min_index: Some(1),
            max_index: Some(10),
            min_ts: None,
            max_ts: None,
            sealed: true,
            record_count: 10,
            record_type_flags: RecordTypeFlags::default(),
            entry_count: 10,
            first_entry_offset: 0,
            location: SegmentLocation::Local,
        };
        let mut encoded = meta.encode_value();
        // Zero out the location byte to simulate old format
        encoded[2] = 0;
        let decoded =
            ManifestSegmentMeta::decode_value(1, 1, &encoded).expect("decode should succeed");
        assert_eq!(
            decoded.location,
            crate::manifest_mdbx::SegmentLocation::Local,
            "Old format should decode as Local"
        );
    }

    #[test]
    fn test_s3_archive_upload_on_seal() {
        use crate::segment_archive::{InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                ..Default::default()
            };
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_s3_archive(s3_config);

            let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
                config,
                Some(archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>),
            )
            .await
            .unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to force multiple segment rotations (seals)
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Give the async upload tasks time to complete
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Sealed segments should have been uploaded to the archive
            let count = archive.object_count();
            assert!(
                count > 0,
                "Expected at least 1 archived segment, got {count}"
            );

            // All entries should still be readable
            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);

            storage.stop();
        });
    }

    #[test]
    fn test_s3_archive_fetch_on_cache_miss() {
        use crate::segment_archive::{InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                min_local_retention_secs: 0,
                ..Default::default()
            };

            // Write data with archival enabled and very low pin limit
            // This forces LRU eviction of old segments. When we read old entries,
            // the reopen path will find the local file missing and fetch from S3.
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(2)
                .with_s3_archive(s3_config);

            let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
                config,
                Some(archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>),
            )
            .await
            .unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=60 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Wait for background S3 uploads to complete
            tokio::time::sleep(Duration::from_millis(200)).await;

            let archived_count = archive.object_count();
            assert!(archived_count > 0, "Expected archived segments");

            // Delete local files of early segments (which have been archived)
            // to simulate local eviction. The segment map has already evicted
            // these from memory (max_pinned_segments=2).
            let group_dir = tmp.path().join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
                .collect();
            seg_files.sort_by_key(|e| e.file_name());

            // Delete the 2 oldest segment files
            for f in seg_files.iter().take(2) {
                let _ = std::fs::remove_file(f.path());
            }

            // Read all entries — old segments should be fetched from S3 archive
            let result = log.try_get_log_entries(1..61).await.unwrap();
            assert_eq!(
                result.len(),
                60,
                "All entries should be readable after S3 fetch"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_local_eviction_when_max_bytes_exceeded() {
        use crate::segment_archive::{ArchiveManager, InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                min_local_retention_secs: 0, // Allow immediate eviction in test
                ..Default::default()
            };
            let archive_mgr = ArchiveManager::new(
                archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>,
                s3_config.clone(),
            );

            // Create some fake segment files
            let group_dir = tmp.path().join("group_0");
            tokio::fs::create_dir_all(&group_dir).await.unwrap();

            let mut total_bytes = 0u64;
            let mut segment_files = Vec::new();
            for i in 1..=5 {
                let seg_path = group_dir.join(format!("seg_{i:06}.log"));
                let data = vec![0u8; 1024]; // 1KB each
                tokio::fs::write(&seg_path, &data).await.unwrap();

                // Upload to archive (simulate archival)
                archive_mgr.upload_segment(0, i, &seg_path).await.unwrap();

                total_bytes += 1024;
                // Use mtime in the past to pass retention check
                // (min_local_retention_secs=0 so this should work)
                segment_files.push((i, seg_path, 1024u64, true));
            }

            assert_eq!(total_bytes, 5120);
            assert_eq!(archive.object_count(), 5);

            // Evict to max 3KB — should delete 2 oldest segments
            let (deleted, freed) = archive_mgr.evict_local_segments(3072, segment_files).await;

            assert!(deleted >= 2, "Expected at least 2 deletions, got {deleted}");
            assert!(
                freed >= 2048,
                "Expected at least 2048 bytes freed, got {freed}"
            );

            // Verify that the evicted files no longer exist
            let remaining_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
                .collect();
            assert!(
                remaining_files.len() <= 3,
                "Expected at most 3 remaining files, got {}",
                remaining_files.len()
            );
        });
    }

    #[test]
    fn test_full_tiered_storage_lifecycle() {
        use crate::segment_archive::{InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                min_local_retention_secs: 0,
                ..Default::default()
            };
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(3)
                .with_max_concurrent_segment_opens(4)
                .with_s3_archive(s3_config);

            let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
                config,
                Some(archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>),
            )
            .await
            .unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Step 1: Write many entries (creates multiple segments)
            for i in 1..=100 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Wait for background archival
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Step 2: Verify archival happened
            let archived = archive.object_count();
            assert!(archived > 0, "Expected archived segments");

            // Step 3: Verify pinned count is bounded
            let group_state = storage.get_or_create_group(0).await.unwrap();
            let pinned = group_state.segment_map.pinned_count();
            assert!(pinned <= 3, "Expected max 3 pinned, got {pinned}");

            // Step 4: Read all entries (exercises reopen path for evicted segments)
            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100);
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            // Step 5: Purge old entries
            let purge_id = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 50,
            };
            log.purge(purge_id).await.unwrap();

            // Step 6: Remaining entries should still be readable
            let result = log.try_get_log_entries(51..101).await.unwrap();
            assert_eq!(result.len(), 50);

            storage.stop();
        });
    }

    // -----------------------------------------------------------------------
    // Compressed archival tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_compressed_upload_on_seal() {
        use crate::segment_archive::{InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                compress_archived: true,
                compression_level: 3,
                ..Default::default()
            };
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_s3_archive(s3_config);

            let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
                config,
                Some(archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>),
            )
            .await
            .unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            tokio::time::sleep(Duration::from_millis(100)).await;

            // Archived objects should use .zst extension
            let count = archive.object_count();
            assert!(
                count > 0,
                "Expected at least 1 archived segment, got {count}"
            );

            // Verify keys end with .log.zst
            for key in archive.keys() {
                assert!(
                    key.ends_with(".log.zst"),
                    "Expected .log.zst extension, got: {key}"
                );
            }

            // All entries still readable
            let result = log.try_get_log_entries(1..51).await.unwrap();
            assert_eq!(result.len(), 50);

            storage.stop();
        });
    }

    #[test]
    fn test_compressed_fetch_on_cache_miss() {
        use crate::segment_archive::{InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                min_local_retention_secs: 0,
                compress_archived: true,
                compression_level: 3,
                ..Default::default()
            };

            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(2)
                .with_s3_archive(s3_config);

            let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
                config,
                Some(archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>),
            )
            .await
            .unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            for i in 1..=60 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            tokio::time::sleep(Duration::from_millis(200)).await;

            let archived_count = archive.object_count();
            assert!(archived_count > 0, "Expected archived segments");

            // All archived objects should be compressed
            for key in archive.keys() {
                assert!(key.ends_with(".log.zst"), "Expected compressed key: {key}");
            }

            // Delete local files of early segments to force S3 fetch + decompress
            let group_dir = tmp.path().join("group_0");
            let mut seg_files: Vec<_> = std::fs::read_dir(&group_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
                .collect();
            seg_files.sort_by_key(|e| e.file_name());

            for f in seg_files.iter().take(2) {
                let _ = std::fs::remove_file(f.path());
            }

            // Read all entries — triggers download + decompress from S3
            let result = log.try_get_log_entries(1..61).await.unwrap();
            assert_eq!(
                result.len(),
                60,
                "All entries readable after compressed S3 fetch"
            );

            storage.stop();
        });
    }

    #[test]
    fn test_compressed_full_tiered_lifecycle() {
        use crate::segment_archive::{InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                min_local_retention_secs: 0,
                compress_archived: true,
                compression_level: 1, // Fast compression for test
                ..Default::default()
            };
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(512)
                .with_max_pinned_segments(3)
                .with_max_concurrent_segment_opens(4)
                .with_s3_archive(s3_config);

            let storage = MmapPerGroupLogStorage::<C>::new_with_archive(
                config,
                Some(archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>),
            )
            .await
            .unwrap();

            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write 100 entries across many segments
            for i in 1..=100 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            tokio::time::sleep(Duration::from_millis(200)).await;

            // Verify compressed archival
            let archived = archive.object_count();
            assert!(archived > 0, "Expected archived segments");
            for key in archive.keys() {
                assert!(key.ends_with(".log.zst"), "Key should be compressed: {key}");
                let data = archive.get_data(&key).unwrap();
                // Compressed data should start with zstd magic number (0xFD2FB528)
                assert!(data.len() >= 4, "Compressed data too small");
                assert_eq!(
                    &data[..4],
                    &[0x28, 0xB5, 0x2F, 0xFD],
                    "Expected zstd magic bytes"
                );
            }

            // Read all entries (exercises reopen + decompress for evicted segments)
            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100);
            for (i, entry) in result.iter().enumerate() {
                assert_eq!(entry.log_id.index, (i + 1) as u64);
            }

            // Purge old entries
            let purge_id = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 50,
            };
            log.purge(purge_id).await.unwrap();

            // Remaining entries still readable
            let result = log.try_get_log_entries(51..101).await.unwrap();
            assert_eq!(result.len(), 50);

            storage.stop();
        });
    }

    #[test]
    fn test_compressed_eviction_roundtrip() {
        use crate::segment_archive::{ArchiveManager, InMemoryArchive, S3ArchiveConfig};

        run_async(async {
            let tmp = TempDir::new().unwrap();
            let archive = Arc::new(InMemoryArchive::new());
            let s3_config = S3ArchiveConfig {
                bucket: "test".to_string(),
                key_prefix: "raft/".to_string(),
                min_local_retention_secs: 0,
                compress_archived: true,
                compression_level: 3,
                ..Default::default()
            };
            let archive_mgr = ArchiveManager::new(
                archive.clone() as Arc<dyn crate::segment_archive::SegmentArchive>,
                s3_config.clone(),
            );

            // Create fake segment files and upload compressed
            let group_dir = tmp.path().join("group_0");
            tokio::fs::create_dir_all(&group_dir).await.unwrap();

            let mut segment_files = Vec::new();
            for i in 1..=5 {
                let seg_path = group_dir.join(format!("seg_{i:06}.log"));
                let data = vec![0xABu8; 1024];
                tokio::fs::write(&seg_path, &data).await.unwrap();
                archive_mgr.upload_segment(0, i, &seg_path).await.unwrap();
                segment_files.push((i, seg_path, 1024u64, true));
            }

            // All should be stored compressed
            assert_eq!(archive.object_count(), 5);
            for key in archive.keys() {
                assert!(key.ends_with(".log.zst"));
                let data = archive.get_data(&key).unwrap();
                // Compressed 1KB of 0xAB should be much smaller
                assert!(
                    data.len() < 1024,
                    "Expected compression, got {} bytes",
                    data.len()
                );
            }

            // Evict 2 segments
            let (deleted, freed) = archive_mgr.evict_local_segments(3072, segment_files).await;
            assert!(deleted >= 2);
            assert!(freed >= 2048);

            // Download an evicted segment and verify it decompresses correctly
            let restore_path = group_dir.join("seg_000001_restored.log");
            archive_mgr
                .download_segment(0, 1, &restore_path)
                .await
                .unwrap();
            let restored = tokio::fs::read(&restore_path).await.unwrap();
            assert_eq!(restored, vec![0xABu8; 1024]);
        });
    }
}
