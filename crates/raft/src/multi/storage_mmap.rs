//! Segmented Mmap Log Storage for Multi-Raft
//!
//! Provides per-group sharded storage using memory-mapped segment files with:
//! - One dedicated shard per raft group (1:1 mapping)
//! - Dynamic shard creation on first access
//! - Same on-disk record format as SegLog: `[len:u32][type:u8][group_id:u24][payload...][crc64:u64]`
//! - Zero-copy reads via mmap slices
//! - Near-zero-latency writes (memcpy into MmapMut)
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

use crate::multi::codec::{
    BorrowPayload, Decode, Encode, Entry as CodecEntry, LogId as CodecLogId, RawBytes, ToCodec,
    Vote as CodecVote, FromCodec,
};
use crate::multi::manifest_mdbx::{ManifestManager, SegmentMeta};
use crate::multi::segment_footer::{
    SegmentFooter, SegmentFooterTracker,
    FOOTER_HEADER_SIZE, FOOTER_MAGIC, FOOTER_TRAILER_SIZE,
};
use crate::multi::storage_impl::{
    AtomicLogId, AtomicVote, LogIndex, LogLocation, MAX_GROUPS, MultiplexedStorage,
    RecordType, append_record_into, validate_record,
    LENGTH_SIZE, CRC64_SIZE, GROUP_ID_SIZE,
};
use crossfire::{MAsyncTx, mpsc::Array};
use dashmap::DashMap;
use memmap2::{Mmap, MmapMut};
use openraft::{
    LogId, LogState, RaftTypeConfig,
    storage::{IOFlushed, RaftLogReader, RaftLogStorage},
};
use parking_lot::Mutex;
use std::io;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

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
    /// Maximum entries to keep in memory cache per group
    pub max_cache_entries_per_group: usize,
    /// Maximum record size
    pub max_record_size: u64,
}

impl Default for MmapStorageConfig {
    fn default() -> Self {
        Self {
            base_dir: Arc::new(PathBuf::from("./raft-data")),
            manifest_dir: None,
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_cache_entries_per_group: 10000,
            max_record_size: DEFAULT_MAX_RECORD_SIZE,
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

    /// Set the maximum cache entries per group
    pub fn with_max_cache_entries(mut self, max_entries: usize) -> Self {
        self.max_cache_entries_per_group = max_entries;
        self
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Segment file path: {group_dir}/seg_{segment_id:06}.log
fn segment_path(group_dir: &Path, segment_id: u64) -> PathBuf {
    group_dir.join(format!("seg_{segment_id:06}.log"))
}

/// Group directory path: {base_dir}/group_{group_id}
fn group_dir_path(base_dir: &Path, group_id: u64) -> PathBuf {
    base_dir.join(format!("group_{group_id}"))
}

/// Scan a group directory for segment files, returning sorted segment IDs
fn scan_segment_ids(group_dir: &Path) -> io::Result<Vec<u64>> {
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
// MmapSegmentMap — in-memory segment tracking shared between writer and readers
// ---------------------------------------------------------------------------

struct SealedSegment {
    segment_id: u64,
    mmap: Arc<Mmap>,
    valid_bytes: u64,
    path: PathBuf,
}

struct ActiveReadView {
    segment_id: u64,
    mmap: Arc<Mmap>,
    valid_bytes: u64,
}

struct MmapSegmentMap {
    sealed: parking_lot::RwLock<Vec<SealedSegment>>,
    active: parking_lot::RwLock<Option<ActiveReadView>>,
}

impl MmapSegmentMap {
    fn new() -> Self {
        Self {
            sealed: parking_lot::RwLock::new(Vec::new()),
            active: parking_lot::RwLock::new(None),
        }
    }

    /// Find the segment for a given log index using LogLocation info
    fn find_mmap(&self, segment_id: u64) -> Option<(Arc<Mmap>, u64)> {
        // Check active first (most recent writes)
        {
            let active = self.active.read();
            if let Some(ref av) = *active {
                if av.segment_id == segment_id {
                    return Some((av.mmap.clone(), av.valid_bytes));
                }
            }
        }
        // Check sealed segments
        {
            let sealed = self.sealed.read();
            for seg in sealed.iter().rev() {
                if seg.segment_id == segment_id {
                    return Some((seg.mmap.clone(), seg.valid_bytes));
                }
            }
        }
        None
    }

    /// Add a sealed segment
    fn add_sealed(&self, entry: SealedSegment) {
        let mut guard = self.sealed.write();
        let pos = guard.partition_point(|s| s.segment_id < entry.segment_id);
        guard.insert(pos, entry);
    }

    /// Update the active read view
    fn update_active(&self, view: ActiveReadView) {
        let mut guard = self.active.write();
        *guard = Some(view);
    }

    /// Remove sealed segments where their entire range is purged.
    /// Returns paths of removed segments for file deletion.
    fn remove_purged_segments(&self, purge_index: u64, log_index: &LogIndex) -> Vec<PathBuf> {
        let mut guard = self.sealed.write();
        let mut removed = Vec::new();
        guard.retain(|seg| {
            // Check if any entries in this segment are still needed
            // A segment can be removed if all its entries have indices <= purge_index
            // We check by seeing if the segment has entries beyond purge_index
            // by looking up the last entry's index via the segment's valid_bytes range
            // Simple approach: scan the segment mmap for the last entry index
            let last_idx = scan_last_entry_index(&seg.mmap, seg.valid_bytes as usize);
            if let Some(last) = last_idx {
                if last <= purge_index {
                    removed.push(seg.path.clone());
                    return false;
                }
            }
            true
        });
        let _ = log_index; // LogIndex cleanup done separately
        removed
    }

    /// Remove sealed segments with first_index > after_index for truncation
    fn remove_after(&self, after_index: u64) -> Vec<PathBuf> {
        let mut guard = self.sealed.write();
        let mut removed = Vec::new();
        guard.retain(|seg| {
            let first_idx = scan_first_entry_index(&seg.mmap, seg.valid_bytes as usize);
            if let Some(first) = first_idx {
                if first > after_index {
                    removed.push(seg.path.clone());
                    return false;
                }
            }
            true
        });
        removed
    }

}

/// Scan a segment mmap for the first entry index
fn scan_first_entry_index(mmap: &[u8], valid_bytes: usize) -> Option<u64> {
    let mut offset = 0;
    while offset + LENGTH_SIZE <= valid_bytes {
        let record_len = u32::from_le_bytes(
            mmap[offset..offset + LENGTH_SIZE].try_into().unwrap(),
        ) as usize;
        if record_len == 0 || offset + LENGTH_SIZE + record_len > valid_bytes {
            break;
        }
        let data = &mmap[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
        if data.len() >= 1 + GROUP_ID_SIZE + CRC64_SIZE {
            if let Ok(RecordType::Entry) = RecordType::try_from(data[0]) {
                // This is an entry record — extract the index from the codec payload
                let payload = &data[1 + GROUP_ID_SIZE..data.len() - CRC64_SIZE];
                if let Ok(codec_entry) = CodecLogId::decode_from_slice(payload) {
                    return Some(codec_entry.index);
                }
                // Alternatively, the log index stores the index, but we don't have it here
                // For now, return None and use LogIndex for this purpose
                return None;
            }
        }
        offset += LENGTH_SIZE + record_len;
    }
    None
}

/// Scan a segment mmap for the last entry index
fn scan_last_entry_index(mmap: &[u8], valid_bytes: usize) -> Option<u64> {
    let mut offset = 0;
    let mut last_entry_index = None;
    while offset + LENGTH_SIZE <= valid_bytes {
        let record_len = u32::from_le_bytes(
            mmap[offset..offset + LENGTH_SIZE].try_into().unwrap(),
        ) as usize;
        if record_len == 0 || offset + LENGTH_SIZE + record_len > valid_bytes {
            break;
        }
        let data = &mmap[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
        if data.len() >= 1 + GROUP_ID_SIZE + CRC64_SIZE {
            if let Ok(RecordType::Entry) = RecordType::try_from(data[0]) {
                let payload = &data[1 + GROUP_ID_SIZE..data.len() - CRC64_SIZE];
                if let Ok(codec_entry) = CodecLogId::decode_from_slice(payload) {
                    last_entry_index = Some(codec_entry.index);
                }
            }
        }
        offset += LENGTH_SIZE + record_len;
    }
    last_entry_index
}

// ---------------------------------------------------------------------------
// FsyncState — background fsync coordination
// ---------------------------------------------------------------------------

/// Request to seal a segment in the background (after rotation)
struct SealRequest {
    file: std::fs::File,
    valid_bytes: u64,
    footer_data: Vec<u8>,
    segment_id: u64,
    group_id: u64,
    min_index: Option<u64>,
    max_index: Option<u64>,
    manifest_tx: MAsyncTx<Array<SegmentMeta>>,
}

struct FsyncInner<C: RaftTypeConfig> {
    /// Pending (file, callback) pairs from all groups
    entries: Vec<(Arc<std::fs::File>, IOFlushed<C>)>,
    /// Pending segment seal requests
    seal_queue: Vec<SealRequest>,
    shutdown: bool,
}

struct FsyncState<C: RaftTypeConfig> {
    mu: std::sync::Mutex<FsyncInner<C>>,
    cv: std::sync::Condvar,
}

impl<C: RaftTypeConfig> FsyncState<C> {
    fn new() -> Self {
        Self {
            mu: std::sync::Mutex::new(FsyncInner {
                entries: Vec::new(),
                seal_queue: Vec::new(),
                shutdown: false,
            }),
            cv: std::sync::Condvar::new(),
        }
    }

    /// Push a callback with its associated file handle
    fn push(&self, file: Arc<std::fs::File>, callback: IOFlushed<C>) {
        let mut inner = self.mu.lock().unwrap();
        inner.entries.push((file, callback));
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
}

/// Background fsync thread loop — shared across all groups.
/// Handles both fsync callbacks and segment sealing.
fn fsync_thread_loop<C: RaftTypeConfig>(state: Arc<FsyncState<C>>) {
    loop {
        let (entries, seal_requests, shutdown) = {
            let mut inner = state.mu.lock().unwrap();
            while inner.entries.is_empty() && inner.seal_queue.is_empty() && !inner.shutdown {
                inner = state.cv.wait(inner).unwrap();
            }

            if inner.shutdown && inner.entries.is_empty() && inner.seal_queue.is_empty() {
                return;
            }

            let entries = std::mem::take(&mut inner.entries);
            let seal_requests = std::mem::take(&mut inner.seal_queue);
            (entries, seal_requests, inner.shutdown)
        };

        // Sync each unique callback file once (deduplicate by Arc pointer)
        let mut synced = std::collections::HashSet::new();
        for (file, _) in &entries {
            let ptr = Arc::as_ptr(file) as usize;
            if synced.insert(ptr) {
                let _ = file.sync_data();
            }
        }

        // Fire all callbacks
        for (_, cb) in entries {
            cb.io_completed(Ok(()));
        }

        // Process seal requests: fsync + write footer + truncate + manifest update
        for req in seal_requests {
            // fsync the segment data
            let _ = req.file.sync_data();

            // Write footer at the end of valid data, then truncate
            let final_size = req.valid_bytes + req.footer_data.len() as u64;

            // Use pwrite to write footer at valid_bytes offset
            {
                use std::os::unix::fs::FileExt;
                let _ = req.file.write_at(&req.footer_data, req.valid_bytes);
            }

            // Truncate to valid_bytes + footer
            let _ = req.file.set_len(final_size);
            let _ = req.file.sync_data();

            // Update manifest
            let _ = req.manifest_tx.try_send(SegmentMeta {
                group_id: req.group_id,
                segment_id: req.segment_id,
                valid_bytes: req.valid_bytes,
                min_index: req.min_index,
                max_index: req.max_index,
                min_ts: None,
                max_ts: None,
                sealed: true,
            });
        }

        if shutdown {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// ActiveMmapSegment — writer's current segment state
// ---------------------------------------------------------------------------

struct ActiveMmapSegment {
    segment_id: u64,
    mmap: MmapMut,
    file: std::fs::File,
    current_size: u64,
    segment_capacity: u64,
    path: PathBuf,
    min_entry_index: Option<u64>,
    max_entry_index: Option<u64>,
}

impl ActiveMmapSegment {
    /// Create a new segment file with mmap
    fn create(group_dir: &Path, segment_id: u64, segment_size: u64) -> io::Result<Self> {
        let path = segment_path(group_dir, segment_id);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(segment_size)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            segment_id,
            mmap,
            file,
            current_size: 0,
            segment_capacity: segment_size,
            path,
            min_entry_index: None,
            max_entry_index: None,
        })
    }

    /// Open an existing segment file for writing (recovery)
    fn open_existing(path: &Path, segment_id: u64) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            segment_id,
            mmap,
            file,
            current_size: 0, // Will be set during recovery
            segment_capacity: file_len,
            path: path.to_path_buf(),
            min_entry_index: None,
            max_entry_index: None,
        })
    }

    /// Create a read-only Mmap from the same file
    fn create_reader_mmap(&self) -> io::Result<Arc<Mmap>> {
        let mmap = unsafe { Mmap::map(&self.file)? };
        Ok(Arc::new(mmap))
    }
}

// ---------------------------------------------------------------------------
// WriterState — active segment state protected by Mutex
// ---------------------------------------------------------------------------

struct WriterState {
    active: ActiveMmapSegment,
    /// Pre-allocated next segment — used on rotation to avoid blocking
    next_segment: Option<ActiveMmapSegment>,
    /// Tracks record types and entry ranges for the active segment's footer
    footer_tracker: SegmentFooterTracker,
    group_dir: PathBuf,
    group_id: u64,
    /// Dup'd file handle for the active segment — shared with fsync thread via Arc
    fsync_file: Arc<std::fs::File>,
    /// Buffer for encoding vote records (reused to avoid allocation)
    vote_buf: Vec<u8>,
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
        // Check if we need rotation before this write
        if self.active.current_size + data.len() as u64 > self.active.segment_capacity {
            self.rotate_segment(config, segment_map, manifest_tx, fsync_state)?;
        }

        let offset = self.active.current_size as usize;
        let end = offset + data.len();

        // Extend file if needed (shouldn't normally happen with pre-allocated segments)
        if end > self.active.mmap.len() {
            let new_size = (end as u64).max(self.active.segment_capacity * 2);
            self.active.file.set_len(new_size)?;
            self.active.segment_capacity = new_size;
            self.active.mmap = unsafe { MmapMut::map_mut(&self.active.file)? };
        }

        // memcpy into mmap — this is the whole point: no syscall, just a copy
        self.active.mmap[offset..end].copy_from_slice(data);
        self.active.current_size = end as u64;

        // Pre-allocate next segment when active is 75% full
        if self.next_segment.is_none()
            && self.active.current_size > self.active.segment_capacity * 3 / 4
        {
            let next_id = self.active.segment_id + 1;
            self.next_segment =
                Some(ActiveMmapSegment::create(&self.group_dir, next_id, config.segment_size)?);
        }

        Ok(offset)
    }

    /// Update the reader-visible active read view after writes
    fn update_read_view(&self, segment_map: &MmapSegmentMap) {
        if let Ok(reader_mmap) = self.active.create_reader_mmap() {
            segment_map.update_active(ActiveReadView {
                segment_id: self.active.segment_id,
                mmap: reader_mmap,
                valid_bytes: self.active.current_size,
            });
        }
    }

    /// Rotate: immediately make old segment available as sealed, enqueue background
    /// sealing (fsync + truncate + footer write + manifest update), swap to pre-allocated segment.
    fn rotate_segment(
        &mut self,
        config: &MmapStorageConfig,
        segment_map: &MmapSegmentMap,
        manifest_tx: &MAsyncTx<Array<SegmentMeta>>,
        fsync_state: &FsyncState<impl RaftTypeConfig>,
    ) -> io::Result<()> {
        let valid_bytes = self.active.current_size;

        // Create sealed mmap immediately (shares pages with MmapMut, no I/O)
        let sealed_mmap = unsafe { Mmap::map(&self.active.file)? };

        // Add to sealed list right away — readers can find it immediately
        segment_map.add_sealed(SealedSegment {
            segment_id: self.active.segment_id,
            mmap: Arc::new(sealed_mmap),
            valid_bytes,
            path: self.active.path.clone(),
        });

        // Build footer from tracker
        let old_tracker = std::mem::replace(
            &mut self.footer_tracker,
            SegmentFooterTracker::new(self.group_id, self.active.segment_id + 1),
        );
        let footer = old_tracker.build(valid_bytes);
        let footer_data = footer.encode();

        // Enqueue background sealing: fsync + truncate + footer write + manifest update
        fsync_state.enqueue_seal(SealRequest {
            file: self.active.file.try_clone()?,
            valid_bytes,
            footer_data,
            segment_id: self.active.segment_id,
            group_id: self.group_id,
            min_index: self.active.min_entry_index,
            max_index: self.active.max_entry_index,
            manifest_tx: manifest_tx.clone(),
        });

        // Use pre-allocated segment if available, otherwise create synchronously
        let new_id = self.active.segment_id + 1;
        let new_seg = match self.next_segment.take() {
            Some(seg) if seg.segment_id == new_id => seg,
            _ => ActiveMmapSegment::create(&self.group_dir, new_id, config.segment_size)?,
        };

        // Update fsync file handle to new segment
        self.fsync_file = Arc::new(new_seg.file.try_clone()?);

        // Update active read view
        if let Ok(reader_mmap) = new_seg.create_reader_mmap() {
            segment_map.update_active(ActiveReadView {
                segment_id: new_seg.segment_id,
                mmap: reader_mmap,
                valid_bytes: 0,
            });
        }

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
        let record_len = u32::from_le_bytes(
            data[offset..offset + LENGTH_SIZE].try_into().unwrap(),
        ) as usize;
        if record_len == 0 || offset + LENGTH_SIZE + record_len > data.len() {
            break;
        }
        let record_data = &data[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
        if record_data.len() >= 1 + GROUP_ID_SIZE + CRC64_SIZE {
            if let Ok(RecordType::Entry) = RecordType::try_from(record_data[0]) {
                // Parse just enough to get the entry index from the codec payload
                let payload = &record_data[1 + GROUP_ID_SIZE..record_data.len() - CRC64_SIZE];
                if let Ok(log_id) = CodecLogId::decode_from_slice(payload) {
                    if log_id.index > target_index {
                        break;
                    }
                }
            }
        }
        let total = LENGTH_SIZE + record_len;
        offset += total;
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
    cache: DashMap<u64, Arc<C::Entry>>,
    vote: AtomicVote,
    first_index: AtomicU64,
    last_index: AtomicU64,
    cache_low: AtomicU64,
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
}

impl<C: RaftTypeConfig + 'static> MmapGroupState<C> {
    /// Create and recover group state from disk
    fn new(
        group_id: u64,
        config: &Arc<MmapStorageConfig>,
        group_dir: &Path,
        manifest: &ManifestManager,
        fsync_state: Arc<FsyncState<C>>,
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
        std::fs::create_dir_all(group_dir)?;

        let log_index = Arc::new(LogIndex::new());
        let segment_map = Arc::new(MmapSegmentMap::new());

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
            let mmap = unsafe { Mmap::map(&file)? };

            // Try to read footer from sealed segment for fast recovery
            let footer = Self::read_footer_from_mmap(&mmap);

            if let Some(ref ft) = footer {
                if ft.record_type_flags.has_only_entries() {
                    // FAST PATH: entry-only segment with footer — walk length prefixes only
                    let valid = ft.valid_bytes as usize;
                    let mut offset = 0usize;
                    let mut entry_index = ft.min_entry_index.unwrap_or(0);
                    while offset + LENGTH_SIZE <= valid {
                        let record_len = u32::from_le_bytes(
                            mmap[offset..offset + LENGTH_SIZE].try_into().unwrap(),
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
                        entry_index += 1;
                        offset += total;
                    }

                    if let Some(min) = ft.min_entry_index {
                        overall_first_index = Some(
                            overall_first_index.map_or(min, |v: u64| v.min(min)),
                        );
                    }
                    if let Some(max) = ft.max_entry_index {
                        overall_last_index = Some(
                            overall_last_index.map_or(max, |v: u64| v.max(max)),
                        );
                        // Update last_log_id — we need to decode the last entry for its log_id
                        // Walk to last record and decode just that one
                        if let Some(loc) = log_index.get(max) {
                            let start = loc.offset as usize;
                            let end = start + loc.len as usize;
                            if end <= mmap.len() {
                                let buf = &mmap[start..end];
                                if buf.len() > LENGTH_SIZE {
                                    if let Ok(parsed) = validate_record(&buf[LENGTH_SIZE..], max_record_size) {
                                        if let Ok(codec_entry) = CodecEntry::<RawBytes>::decode_from_slice(parsed.payload) {
                                            let entry: openraft::impls::Entry<C> =
                                                openraft::impls::Entry::<C>::from_codec(codec_entry);
                                            last_log_id = Some(openraft::entry::RaftEntry::log_id(&entry));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    segment_map.add_sealed(SealedSegment {
                        segment_id: seg_id,
                        mmap: Arc::new(mmap),
                        valid_bytes: ft.valid_bytes,
                        path,
                    });
                    continue;
                }
            }

            // SLOW PATH: full record decode
            // Use footer.valid_bytes if available, manifest if available, else CRC scan
            let valid = if let Some(ref ft) = footer {
                ft.valid_bytes as usize
            } else if let Some(meta) = manifest_segments.get(&seg_id) {
                if meta.sealed {
                    meta.valid_bytes as usize
                } else {
                    Self::scan_valid_bytes(&mmap)
                }
            } else {
                Self::scan_valid_bytes(&mmap)
            };

            Self::scan_records_into_index(
                &mmap[..valid],
                seg_id,
                group_id,
                max_record_size,
                &log_index,
                &mut vote,
                &mut last_log_id,
                &mut last_purged_log_id,
            );

            // Track min/max from manifest or footer
            if let Some(ref ft) = footer {
                if let Some(min) = ft.min_entry_index {
                    overall_first_index = Some(overall_first_index.map_or(min, |v: u64| v.min(min)));
                }
                if let Some(max) = ft.max_entry_index {
                    overall_last_index = Some(overall_last_index.map_or(max, |v: u64| v.max(max)));
                }
            } else if let Some(meta) = manifest_segments.get(&seg_id) {
                if let Some(min) = meta.min_index {
                    overall_first_index = Some(overall_first_index.map_or(min, |v: u64| v.min(min)));
                }
                if let Some(max) = meta.max_index {
                    overall_last_index = Some(overall_last_index.map_or(max, |v: u64| v.max(max)));
                }
            }

            segment_map.add_sealed(SealedSegment {
                segment_id: seg_id,
                mmap: Arc::new(mmap),
                valid_bytes: valid as u64,
                path,
            });
        }

        // Open or create active (tail) segment
        let active = if let Some(seg_id) = active_segment_id {
            let path = segment_path(group_dir, seg_id);
            let mut active = ActiveMmapSegment::open_existing(&path, seg_id)?;

            // CRC-validate tail segment to find end of valid data
            let valid = Self::scan_valid_bytes(&active.mmap);
            active.current_size = valid as u64;

            // Zero-pad beyond valid_bytes for partial write protection
            if valid < active.mmap.len() {
                let zero_end = active.mmap.len().min(valid + 4096);
                for byte in &mut active.mmap[valid..zero_end] {
                    *byte = 0;
                }
            }

            // Full record scan for tail (must decode everything)
            Self::scan_records_into_index(
                &active.mmap[..valid],
                seg_id,
                group_id,
                max_record_size,
                &log_index,
                &mut vote,
                &mut last_log_id,
                &mut last_purged_log_id,
            );

            // Update active's entry index range
            active.min_entry_index = scan_first_entry_index(&active.mmap, valid);
            active.max_entry_index = scan_last_entry_index(&active.mmap, valid);

            if let Some(min) = active.min_entry_index {
                overall_first_index = Some(overall_first_index.map_or(min, |v: u64| v.min(min)));
            }
            if let Some(max) = active.max_entry_index {
                overall_last_index = Some(overall_last_index.map_or(max, |v: u64| v.max(max)));
            }

            // Set active read view
            if let Ok(reader_mmap) = active.create_reader_mmap() {
                segment_map.update_active(ActiveReadView {
                    segment_id: active.segment_id,
                    mmap: reader_mmap,
                    valid_bytes: active.current_size,
                });
            }

            active
        } else {
            // No segments exist — create first one
            let active = ActiveMmapSegment::create(group_dir, 1, config.segment_size)?;
            if let Ok(reader_mmap) = active.create_reader_mmap() {
                segment_map.update_active(ActiveReadView {
                    segment_id: active.segment_id,
                    mmap: reader_mmap,
                    valid_bytes: 0,
                });
            }
            active
        };

        // Determine first_index accounting for purge
        let first_index = if let Some(ref plid) = last_purged_log_id {
            plid.index + 1
        } else {
            overall_first_index.unwrap_or(0)
        };
        let last_index = overall_last_index.unwrap_or(0);

        // Populate cache from recent entries
        let cache: DashMap<u64, Arc<C::Entry>> = DashMap::new();
        let max_cache = config.max_cache_entries_per_group as u64;
        if max_cache > 0 && last_index > 0 {
            let cache_start = last_index
                .saturating_sub(max_cache.saturating_sub(1))
                .max(first_index);
            for idx in cache_start..=last_index {
                if let Some(loc) = log_index.get(idx) {
                    if let Some((mmap, valid_bytes)) = segment_map.find_mmap(loc.segment_id) {
                        let start = loc.offset as usize;
                        let end = start + loc.len as usize;
                        if end <= valid_bytes as usize && end <= mmap.len() {
                            let buf = &mmap[start..end];
                            if buf.len() >= LENGTH_SIZE {
                                let record_data = &buf[LENGTH_SIZE..];
                                if let Ok(parsed) = validate_record(record_data, max_record_size) {
                                    if parsed.record_type == RecordType::Entry {
                                        if let Ok(codec_entry) =
                                            CodecEntry::<RawBytes>::decode_from_slice(parsed.payload)
                                        {
                                            let entry: openraft::impls::Entry<C> =
                                                openraft::impls::Entry::<C>::from_codec(codec_entry);
                                            cache.insert(idx, Arc::new(entry));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

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
        let fsync_file = Arc::new(active.file.try_clone()?);
        let active_seg_id = active.segment_id;
        let writer = WriterState {
            active,
            next_segment: None,
            footer_tracker: SegmentFooterTracker::new(group_id, active_seg_id),
            group_dir: group_dir.to_path_buf(),
            group_id,
            fsync_file,
            vote_buf: Vec::new(),
        };

        let manifest_tx = manifest.sender();

        Ok(Self {
            cache,
            vote: atomic_vote,
            first_index: AtomicU64::new(first_index),
            last_index: AtomicU64::new(last_index),
            cache_low: AtomicU64::new(0),
            last_log_id: atomic_last_log_id,
            last_purged_log_id: atomic_last_purged,
            log_index,
            segment_map,
            writer: Mutex::new(writer),
            config: config.clone(),
            manifest_tx,
            fsync_state,
        })
    }

    /// Scan mmap data for valid bytes (find the end of the last valid record)
    fn scan_valid_bytes(data: &[u8]) -> usize {
        let mut offset = 0;
        while offset + LENGTH_SIZE <= data.len() {
            let record_len = u32::from_le_bytes(
                data[offset..offset + LENGTH_SIZE].try_into().unwrap(),
            ) as usize;
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
            let stored_crc =
                u64::from_le_bytes(record_data[payload_end..].try_into().unwrap());
            let mut digest = crc64fast_nvme::Digest::new();
            digest.write(&record_data[..payload_end]);
            if digest.sum64() != stored_crc {
                break;
            }
            offset += total;
        }
        offset
    }

    /// Scan records in mmap data and populate LogIndex + extract state
    fn scan_records_into_index(
        data: &[u8],
        segment_id: u64,
        group_id: u64,
        max_record_size: usize,
        log_index: &LogIndex,
        vote: &mut Option<openraft::impls::Vote<C>>,
        last_log_id: &mut Option<LogId<C>>,
        last_purged_log_id: &mut Option<LogId<C>>,
    ) where
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
        use crate::multi::codec::Vote as CodecVote;

        let mut offset = 0;
        while offset + LENGTH_SIZE <= data.len() {
            let record_len = u32::from_le_bytes(
                data[offset..offset + LENGTH_SIZE].try_into().unwrap(),
            ) as usize;
            if record_len == 0 || offset + LENGTH_SIZE + record_len > data.len() {
                break;
            }
            let record_data = &data[offset + LENGTH_SIZE..offset + LENGTH_SIZE + record_len];
            let total = LENGTH_SIZE + record_len;

            if let Ok(parsed) = validate_record(record_data, max_record_size) {
                if parsed.group_id != group_id {
                    offset += total;
                    continue;
                }
                match parsed.record_type {
                    RecordType::Entry => {
                        // Decode just enough to get the log_id
                        if let Ok(codec_entry) =
                            CodecEntry::<RawBytes>::decode_from_slice(parsed.payload)
                        {
                            let entry: openraft::impls::Entry<C> =
                                openraft::impls::Entry::<C>::from_codec(codec_entry);
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
                            *last_log_id = Some(lid);
                        }
                    }
                    RecordType::Vote => {
                        if let Ok(codec_vote) =
                            CodecVote::decode_from_slice(parsed.payload)
                        {
                            *vote = Some(openraft::impls::Vote::<C> {
                                leader_id: openraft::impls::leader_id_adv::LeaderId {
                                    term: codec_vote.leader_id.term,
                                    node_id: codec_vote.leader_id.node_id,
                                },
                                committed: codec_vote.committed,
                            });
                        }
                    }
                    RecordType::Truncate => {
                        if let Ok(codec_lid) = CodecLogId::decode_from_slice(parsed.payload) {
                            *last_log_id = Some(LogId {
                                leader_id: openraft::impls::leader_id_adv::LeaderId {
                                    term: codec_lid.leader_id.term,
                                    node_id: codec_lid.leader_id.node_id,
                                },
                                index: codec_lid.index,
                            });
                        }
                    }
                    RecordType::Purge => {
                        if let Ok(codec_lid) = CodecLogId::decode_from_slice(parsed.payload) {
                            *last_purged_log_id = Some(LogId {
                                leader_id: openraft::impls::leader_id_adv::LeaderId {
                                    term: codec_lid.leader_id.term,
                                    node_id: codec_lid.leader_id.node_id,
                                },
                                index: codec_lid.index,
                            });
                        }
                    }
                }
            } else {
                break; // Corrupt record, stop scanning
            }
            offset += total;
        }
    }

    /// Read a SegmentFooter from the end of an mmap'd segment file.
    /// Returns None if no valid footer is found.
    fn read_footer_from_mmap(mmap: &[u8]) -> Option<SegmentFooter> {
        let footer_total = FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE;
        if mmap.len() < footer_total {
            return None;
        }
        let footer_start = mmap.len() - footer_total;
        let magic_offset = footer_start + FOOTER_HEADER_SIZE;
        if magic_offset + 4 > mmap.len() {
            return None;
        }
        let magic = u32::from_le_bytes(
            mmap[magic_offset..magic_offset + 4].try_into().ok()?,
        );
        if magic != FOOTER_MAGIC {
            return None;
        }
        SegmentFooter::decode(&mmap[footer_start..]).ok()
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
}

impl<C: RaftTypeConfig + 'static> MmapPerGroupLogStorage<C> {
    /// Create a new mmap per-group storage instance.
    pub async fn new(config: impl Into<MmapStorageConfig>) -> io::Result<Self> {
        let config = Arc::new(config.into());
        tokio::fs::create_dir_all(&*config.base_dir).await?;

        let base = (*config.base_dir).clone();
        let manifest_dir = config
            .manifest_dir
            .as_ref()
            .map(|p| (**p).clone())
            .unwrap_or(base);
        let manifest = tokio::task::spawn_blocking(move || {
            match ManifestManager::open(&manifest_dir) {
                Ok(manifest) => Ok(manifest),
                Err(e) if e.raw_os_error() == Some(22) || e.kind() == io::ErrorKind::InvalidInput => {
                    Ok(ManifestManager::open_in_memory())
                }
                Err(e) => Err(e),
            }
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        // Spawn single shared fsync thread for all groups
        let fsync_state = Arc::new(FsyncState::new());
        let fsync_clone = fsync_state.clone();
        std::thread::Builder::new()
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
        })
    }

    /// Stop all groups and the shared fsync thread
    pub fn stop(&self) {
        let mut inner = self.fsync_state.mu.lock().unwrap();
        inner.shutdown = true;
        self.fsync_state.cv.notify_one();
    }

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
        C::D: FromCodec<RawBytes>,
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

        let state = tokio::task::spawn_blocking(move || {
            MmapGroupState::new(group_id, &config, &group_dir, &manifest, fsync_state)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        let state = Arc::new(state);
        match self.groups.insert(group_id, state.clone()) {
            Ok(()) => Ok(state),
            Err(_) => {
                Ok(self
                    .groups
                    .get(group_id)
                    .expect("group must exist after failed insert"))
            }
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
        C::D: FromCodec<RawBytes>,
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
            self.state.cache.retain(|k, _| *k >= start);
        }
        self.state.cache_low.store(start, Ordering::Relaxed);
    }

    /// Read an entry from mmap by index (cache miss path)
    fn read_entry_from_mmap(&self, index: u64) -> io::Result<Option<C::Entry>>
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
        let loc = match self.state.log_index.get(index) {
            Some(loc) => loc,
            None => return Ok(None),
        };

        let (mmap, valid_bytes) = match self.state.segment_map.find_mmap(loc.segment_id) {
            Some(pair) => pair,
            None => return Ok(None),
        };

        let start = loc.offset as usize;
        let end = start + loc.len as usize;
        if end > valid_bytes as usize || end > mmap.len() {
            return Ok(None);
        }

        let buf = &mmap[start..end];
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

        let codec_entry = CodecEntry::<RawBytes>::decode_from_slice(parsed.payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let entry: openraft::impls::Entry<C> =
            openraft::impls::Entry::<C>::from_codec(codec_entry);
        Ok(Some(entry))
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
    C::D: FromCodec<RawBytes>,
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

        let expected_len = end.saturating_sub(start).min(1024) as usize;
        let mut entries = Vec::with_capacity(expected_len);

        for idx in start..end {
            // Fast path: cache hit
            if let Some(entry) = self.state.cache.get(&idx) {
                entries.push(entry.value().as_ref().clone());
                continue;
            }

            // Slow path: mmap read
            if let Some(entry) = self.read_entry_from_mmap(idx)? {
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
    C::Vote: ToCodec<CodecVote>,
    C::D: ToCodec<RawBytes> + FromCodec<RawBytes> + BorrowPayload,
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

        // Encode vote payload
        let codec_vote = vote.to_codec();
        self.payload_buf.clear();
        codec_vote
            .encode_into(&mut self.payload_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        // Write directly into mmap under lock
        let mut writer = self.state.writer.lock();
        writer.vote_buf.clear();
        append_record_into(
            &mut writer.vote_buf,
            RecordType::Vote,
            self.group_id,
            &self.payload_buf,
        );
        let data = writer.vote_buf.clone();
        writer.footer_tracker.record_vote();
        writer.write_bytes(
            &data,
            &self.state.config,
            &self.state.segment_map,
            &self.state.manifest_tx,
            &self.state.fsync_state,
        )?;
        writer.update_read_view(&self.state.segment_map);
        Ok(())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + Send,
    {
        use openraft::entry::RaftEntry;

        let mut last_log_id = None;

        // Encode all entries into encode_buf, tracking record offsets
        self.encode_buf.clear();
        self.record_offsets.clear();
        for entry in entries {
            let log_id = entry.log_id();
            let index = log_id.index;

            // Encode entry directly into payload_buf, avoiding to_codec() clone
            // for Blank/Normal entries. Only Membership falls back to codec path.
            self.payload_buf.clear();
            match &entry.payload {
                openraft::EntryPayload::Blank => {
                    self.payload_buf.reserve(25);
                    self.payload_buf
                        .extend_from_slice(&entry.log_id.leader_id.term.to_le_bytes());
                    self.payload_buf
                        .extend_from_slice(&entry.log_id.leader_id.node_id.to_le_bytes());
                    self.payload_buf
                        .extend_from_slice(&entry.log_id.index.to_le_bytes());
                    self.payload_buf.push(0); // Blank
                }
                openraft::EntryPayload::Normal(data) => {
                    let bytes = data.payload_bytes();
                    self.payload_buf.reserve(29 + bytes.len());
                    self.payload_buf
                        .extend_from_slice(&entry.log_id.leader_id.term.to_le_bytes());
                    self.payload_buf
                        .extend_from_slice(&entry.log_id.leader_id.node_id.to_le_bytes());
                    self.payload_buf
                        .extend_from_slice(&entry.log_id.index.to_le_bytes());
                    self.payload_buf.push(1); // Normal
                    self.payload_buf
                        .extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    self.payload_buf.extend_from_slice(bytes);
                }
                openraft::EntryPayload::Membership(_) => {
                    // Rare: fall back to codec path for membership entries
                    let codec_entry: CodecEntry<RawBytes> = entry.to_codec();
                    codec_entry.encode_into(&mut self.payload_buf).map_err(
                        |e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()),
                    )?;
                }
            }

            let record_start = self.encode_buf.len() as u64;
            let record_len = append_record_into(
                &mut self.encode_buf,
                RecordType::Entry,
                self.group_id,
                &self.payload_buf,
            );
            self.record_offsets
                .push((index, record_start, record_len as u32));

            self.state.cache.insert(index, Arc::new(entry));
            self.state.last_index.fetch_max(index, Ordering::Relaxed);
            last_log_id = Some(log_id);
        }

        // Update last log id
        if let Some(ref lid) = last_log_id {
            self.state.last_log_id.store(Some(lid));
        }

        // Write directly into mmap — just a memcpy, no syscall
        let fsync_file = {
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
                writer.footer_tracker.record_entry(entry_index);
            }

            writer.update_read_view(&self.state.segment_map);

            // Check rotation after write
            if writer.active.current_size > self.state.config.segment_size {
                writer.rotate_segment(
                    &self.state.config,
                    &self.state.segment_map,
                    &self.state.manifest_tx,
                    &self.state.fsync_state,
                )?;
            }

            // Clone the fsync file handle before dropping the lock
            writer.fsync_file.clone()
        };

        // Push callback with its file to the shared fsync thread
        self.state.fsync_state.push(fsync_file, callback);

        self.enforce_cache_window();
        Ok(())
    }

    async fn truncate_after(&mut self, after: Option<LogId<C>>) -> Result<(), io::Error> {
        let index = match after {
            Some(ref log_id) => log_id.index,
            None => 0,
        };

        // Remove entries after truncation point from cache
        let last = self.state.last_index.load(Ordering::Relaxed);
        for idx in (index + 1)..=last {
            self.state.cache.remove(&idx);
        }

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
            if writer.active.max_entry_index.map_or(false, |max| max > index) {
                let new_size = scan_valid_bytes_up_to_index(
                    &writer.active.mmap[..writer.active.current_size as usize],
                    index,
                );
                writer.active.current_size = new_size as u64;
                writer.active.max_entry_index = if index >= writer.active.min_entry_index.unwrap_or(0) {
                    Some(index)
                } else {
                    None
                };
                if writer.active.max_entry_index.is_none() {
                    writer.active.min_entry_index = None;
                }
            }

            writer.update_read_view(&self.state.segment_map);
        }

        self.enforce_cache_window();
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C>) -> Result<(), io::Error> {
        let index = log_id.index;

        // Remove entries up to and including purge point from cache
        let first = self.state.first_index.load(Ordering::Relaxed);
        for idx in first..=index {
            self.state.cache.remove(&idx);
        }

        // Update first index and last_purged_log_id
        self.state.first_index.store(index + 1, Ordering::Relaxed);
        self.state.last_purged_log_id.store(Some(&log_id));

        // Purge from LogIndex and remove old segments
        self.state.log_index.purge_to(index);
        let removed = self.state.segment_map.remove_purged_segments(index, &self.state.log_index);
        for path in &removed {
            let _ = std::fs::remove_file(path);
        }

        self.enforce_cache_window();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MultiplexedStorage + MultiRaftLogStorage implementations
// ---------------------------------------------------------------------------

impl<C> MultiplexedStorage<C> for MmapPerGroupLogStorage<C>
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
    C::D: ToCodec<RawBytes> + FromCodec<RawBytes> + BorrowPayload,
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
}

impl<C> crate::multi::storage::MultiRaftLogStorage<C> for MmapPerGroupLogStorage<C>
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
    C::D: ToCodec<RawBytes> + FromCodec<RawBytes> + BorrowPayload,
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::type_config::async_runtime::{AsyncRuntime, oneshot::Oneshot};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;
    use std::time::Duration;

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
        fn from_codec(raw: RawBytes) -> Self {
            Self(raw.0)
        }
    }

    impl BorrowPayload for TestData {
        fn payload_bytes(&self) -> &[u8] {
            &self.0
        }
    }

    use crate::multi::type_config::ManiacRaftTypeConfig;
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
            tokio::time::sleep(Duration::from_millis(50)).await;

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
            tokio::time::sleep(Duration::from_millis(50)).await;

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
            tokio::time::sleep(Duration::from_millis(200)).await;
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
            let config = MmapStorageConfig::new(tmp.path())
                .with_segment_size(1024 * 1024); // 1MB segments
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
            tokio::time::sleep(Duration::from_millis(500)).await;
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
            tokio::time::sleep(Duration::from_millis(500)).await;
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
            assert!(seg_count >= 2, "Expected at least 2 segments, got {seg_count}");

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
            tokio::time::sleep(Duration::from_millis(200)).await;

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
                    file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04]).unwrap();
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
    fn test_mmap_footer_roundtrip_via_seal() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write entries and force rotation to create sealed segments with footers
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
            tokio::time::sleep(Duration::from_millis(500)).await;
        });

        // Phase 2: Verify sealed segments have footers
        run_async(async move {
            let group_dir = path2.join("group_0");
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

            let mut footers_found = 0;
            // Check all sealed segments (all except last)
            for seg_path in &seg_files[..seg_files.len().saturating_sub(1)] {
                let data = std::fs::read(seg_path).unwrap();
                let footer_total = FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE;
                if data.len() >= footer_total {
                    let footer_start = data.len() - footer_total;
                    let magic_offset = footer_start + FOOTER_HEADER_SIZE;
                    if magic_offset + 4 <= data.len() {
                        let magic = u32::from_le_bytes(
                            data[magic_offset..magic_offset + 4].try_into().unwrap(),
                        );
                        if magic == FOOTER_MAGIC {
                            let footer = SegmentFooter::decode(&data[footer_start..]).unwrap();
                            assert!(footer.record_type_flags.has_entry);
                            assert!(footer.valid_bytes > 0);
                            assert!(footer.record_count > 0);
                            footers_found += 1;
                        }
                    }
                }
            }

            assert!(footers_found > 0, "Expected at least one sealed segment with footer");
        });
    }
}
