//! Segmented MDBX Log Storage for Multi-Raft
//!
//! Provides per-group sharded storage using segmented MDBX databases with:
//! - One dedicated shard per raft group (1:1 mapping)
//! - Dynamic shard creation on first access
//! - Segmented MDBX databases (size-based rotation)
//! - No mutex contention - writes serialized through channels per shard
//! - MDBX built-in ACID transactions and data integrity
//!
//! Each raft group has independent lifecycle (compaction, replication).
//!
//! ## File Layout
//!
//! ```text
//! {base_dir}/{group_id}/
//!   meta.mdb           # group metadata (vote, last_log_id, last_purged_log_id)
//!   meta.mdb-lock      # MDBX lock file
//!   seg-000001.mdb     # entry segment 1
//!   seg-000001.mdb-lock
//!   seg-000002.mdb     # entry segment 2
//!   ...
//! ```
//!
//! ## Key Design
//!
//! - Each segment is an independent MDBX database with a single `entries` table
//! - Keys: u64 log index as big-endian 8 bytes (B-tree ordered)
//! - Values: codec-encoded entry bytes
//! - Purge: delete entire segment files (instant space reclaim)
//! - Recovery: scan segment files, read first/last keys from each

use crate::multi::codec::{Decode, FromCodec};
use crate::multi::codec::{
    Encode, Entry as CodecEntry, LogId as CodecLogId, RawBytes, ToCodec, Vote as CodecVote,
};
use crate::multi::storage_impl::{
    AtomicLogId, AtomicVote, Callbacks, MAX_GROUPS, MultiplexedStorage, ShardError,
};
use crossfire::{MAsyncTx, Rx, TryRecvError, mpsc::Array};
use dashmap::DashMap;
use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode, Table, TableFlags,
    WriteFlags,
};
use openraft::{
    LogId, LogState, RaftTypeConfig,
    storage::{IOFlushed, RaftLogReader, RaftLogStorage},
};
use parking_lot::RwLock;
use std::borrow::Cow;
use std::io;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Meta table key tags
const META_KEY_VOTE: &[u8] = &[0x01];
const META_KEY_LAST_LOG_ID: &[u8] = &[0x02];
const META_KEY_LAST_PURGED_LOG_ID: &[u8] = &[0x03];

/// MDBX table names
const ENTRIES_TABLE: &str = "entries";
const META_STATE_TABLE: &str = "state";

/// Default segment size: 1MB (same as storage_impl)
pub const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for MDBX-based segmented raft log storage
#[derive(Debug, Clone)]
pub struct MdbxStorageConfig {
    /// Base directory for all raft data
    pub base_dir: Arc<PathBuf>,
    /// Maximum segment size in bytes. When exceeded, a new segment is created.
    pub segment_size: u64,
    /// Maximum MDBX map size per segment database
    pub max_db_size: usize,
    /// MDBX growth step
    pub growth_step: usize,
    /// Interval for fsync per group. If None, fsync after every write (Durable).
    /// If Some(duration), use SafeNoSync + periodic explicit sync.
    pub fsync_interval: Option<Duration>,
    /// Maximum entries to keep in memory cache per group
    pub max_cache_entries_per_group: usize,
}

impl Default for MdbxStorageConfig {
    fn default() -> Self {
        Self {
            base_dir: Arc::new(PathBuf::from("./raft-data")),
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_db_size: 256 * 1024 * 1024, // 256 MB per segment
            growth_step: 64 * 1024,         // 64 KB
            fsync_interval: Some(Duration::from_millis(100)),
            max_cache_entries_per_group: 10000,
        }
    }
}

impl MdbxStorageConfig {
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

    fn sync_mode(&self) -> SyncMode {
        if self.fsync_interval.is_none() {
            SyncMode::UtterlyNoSync
        } else {
            SyncMode::UtterlyNoSync
        }
    }
}

// ---------------------------------------------------------------------------
// MDBX Helpers
// ---------------------------------------------------------------------------

/// Open an MDBX database at the given path with no_sub_dir mode
fn open_mdbx(
    path: &Path,
    max_size: usize,
    growth_step: usize,
    sync_mode: SyncMode,
    read_only: bool,
) -> io::Result<Database<NoWriteMap>> {
    let mut opts = DatabaseOptions::default();
    opts.max_tables = Some(4);
    opts.no_sub_dir = true;

    if read_only {
        opts.mode = Mode::ReadOnly;
    } else {
        opts.mode = Mode::ReadWrite(ReadWriteOptions {
            min_size: Some(64 * 1024),
            max_size: Some(max_size as isize),
            growth_step: Some(growth_step as isize),
            shrink_threshold: Some(256 * 1024),
            sync_mode,
            ..Default::default()
        });
    }

    Database::<NoWriteMap>::open_with_options(path, opts)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("MDBX open error: {e}")))
}

/// Reconstruct a Table handle from a cached DBI (same pattern as manifest_mdbx.rs)
unsafe fn table_from_dbi<'txn>(dbi: u32) -> Table<'txn> {
    #[repr(C)]
    struct RawTable<'txn> {
        dbi: u32,
        _marker: std::marker::PhantomData<&'txn ()>,
    }
    let raw = RawTable {
        dbi,
        _marker: std::marker::PhantomData,
    };
    unsafe { std::mem::transmute(raw) }
}

/// Encode a u64 log index as big-endian 8 bytes for MDBX key
#[inline]
fn index_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

/// Segment file path: {group_dir}/seg-{segment_id:06}.mdb
fn segment_path(group_dir: &Path, segment_id: u64) -> PathBuf {
    group_dir.join(format!("seg-{segment_id:06}.mdb"))
}

/// Meta database path: {group_dir}/meta.mdb
fn meta_path(group_dir: &Path) -> PathBuf {
    group_dir.join("meta.mdb")
}

/// Group directory path: {base_dir}/{group_id}
fn group_dir_path(base_dir: &Path, group_id: u64) -> PathBuf {
    base_dir.join(group_id.to_string())
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
        if let Some(rest) = name.strip_prefix("seg-") {
            if let Some(num_str) = rest.strip_suffix(".mdb") {
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
// SegmentMap — in-memory segment tracking shared between writer and readers
// ---------------------------------------------------------------------------

struct SegmentEntry {
    segment_id: u64,
    first_index: u64,
    last_index: u64,
    db: Arc<Database<NoWriteMap>>,
    path: PathBuf,
}

struct SegmentMap {
    inner: RwLock<Vec<SegmentEntry>>,
}

impl SegmentMap {
    fn new() -> Self {
        Self {
            inner: RwLock::new(Vec::new()),
        }
    }

    /// Find the database for a given log index (binary search by first_index)
    fn find_db(&self, index: u64) -> Option<Arc<Database<NoWriteMap>>> {
        let guard = self.inner.read();
        if guard.is_empty() {
            return None;
        }
        // Find the last segment where first_index <= index
        let pos = guard.partition_point(|s| s.first_index <= index);
        if pos == 0 {
            return None;
        }
        let seg = &guard[pos - 1];
        if index <= seg.last_index {
            Some(seg.db.clone())
        } else {
            None
        }
    }

    /// Add a new sealed segment entry
    fn add_sealed(&self, entry: SegmentEntry) {
        let mut guard = self.inner.write();
        // Insert in sorted order by segment_id
        let pos = guard.partition_point(|s| s.segment_id < entry.segment_id);
        guard.insert(pos, entry);
    }

    /// Update the last_index for the latest segment (the active segment)
    fn update_last_index(&self, segment_id: u64, last_index: u64) {
        let mut guard = self.inner.write();
        if let Some(seg) = guard.iter_mut().rfind(|s| s.segment_id == segment_id) {
            seg.last_index = last_index;
        }
    }

    /// Remove all segments where last_index <= purge_index.
    /// Returns the paths of removed segments for file deletion.
    fn remove_purged(&self, purge_index: u64, active_segment_id: u64) -> Vec<PathBuf> {
        let mut guard = self.inner.write();
        let mut removed = Vec::new();
        guard.retain(|s| {
            if s.segment_id != active_segment_id && s.last_index <= purge_index {
                removed.push(s.path.clone());
                false
            } else {
                true
            }
        });
        removed
    }

    /// Remove segments with first_index > after_index and return their paths.
    /// Also returns the segment_id of the segment containing after_index (if any).
    fn remove_after(
        &self,
        after_index: u64,
        active_segment_id: u64,
    ) -> (Vec<(PathBuf, Arc<Database<NoWriteMap>>)>, Option<u64>) {
        let mut guard = self.inner.write();
        let mut removed = Vec::new();
        let mut containing_segment_id = None;

        // Find segment containing after_index
        let pos = guard.partition_point(|s| s.first_index <= after_index);
        if pos > 0 {
            let seg = &mut guard[pos - 1];
            if after_index <= seg.last_index {
                seg.last_index = after_index;
                containing_segment_id = Some(seg.segment_id);
            }
        }

        // Remove all segments after the containing one
        guard.retain(|s| {
            if s.first_index > after_index && s.segment_id != active_segment_id {
                removed.push((s.path.clone(), s.db.clone()));
                false
            } else {
                true
            }
        });

        (removed, containing_segment_id)
    }

    /// Get all segment entries (for recovery/debug)
    fn segments(&self) -> Vec<(u64, u64, u64)> {
        let guard = self.inner.read();
        guard
            .iter()
            .map(|s| (s.segment_id, s.first_index, s.last_index))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// ActiveSegment — writer's current segment state
// ---------------------------------------------------------------------------

struct ActiveSegment {
    segment_id: u64,
    db: Arc<Database<NoWriteMap>>,
    entries_dbi: u32,
    data_size: u64,
    first_index: Option<u64>,
    last_index: Option<u64>,
    path: PathBuf,
}

impl ActiveSegment {
    /// Create a new MDBX segment file
    fn create(group_dir: &Path, segment_id: u64, config: &MdbxStorageConfig) -> io::Result<Self> {
        let path = segment_path(group_dir, segment_id);
        let db = open_mdbx(
            &path,
            config.max_db_size,
            config.growth_step,
            config.sync_mode(),
            false,
        )?;

        // Create the entries table
        let txn = db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("begin_rw_txn: {e}")))?;
        let table = txn
            .create_table(Some(ENTRIES_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("create_table: {e}")))?;
        let dbi = table.dbi();
        drop(table);
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("commit: {e}")))?;

        let db = Arc::new(db);
        Ok(Self {
            segment_id,
            db,
            entries_dbi: dbi,
            data_size: 0,
            first_index: None,
            last_index: None,
            path,
        })
    }

    /// Open an existing segment for writing (recovery)
    fn open_existing(
        group_dir: &Path,
        segment_id: u64,
        config: &MdbxStorageConfig,
    ) -> io::Result<Self> {
        let path = segment_path(group_dir, segment_id);
        let db = open_mdbx(
            &path,
            config.max_db_size,
            config.growth_step,
            config.sync_mode(),
            false,
        )?;

        // Open the entries table
        let txn = db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("begin_rw_txn: {e}")))?;
        let table = txn
            .create_table(Some(ENTRIES_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("create_table: {e}")))?;
        let dbi = table.dbi();
        drop(table);

        // Find existing first/last keys and data size
        let table = unsafe { table_from_dbi(dbi) };
        let mut cursor = txn
            .cursor(&table)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("cursor: {e}")))?;

        let first_index = cursor
            .first::<Cow<[u8]>, Cow<[u8]>>()
            .ok()
            .flatten()
            .map(|(k, _)| u64::from_be_bytes(k.as_ref().try_into().unwrap_or([0; 8])));

        let (last_index, data_size) = if first_index.is_some() {
            let mut size = 0u64;
            let mut last = first_index;
            // Reset cursor to first
            let _ = cursor.first::<Cow<[u8]>, Cow<[u8]>>();
            loop {
                match cursor.next::<Cow<[u8]>, Cow<[u8]>>() {
                    Ok(Some((k, v))) => {
                        size += v.len() as u64 + 8; // value + key overhead
                        last = Some(u64::from_be_bytes(k.as_ref().try_into().unwrap_or([0; 8])));
                    }
                    _ => break,
                }
            }
            // Add first entry size
            if let Ok(Some((_, v))) = {
                let _ = cursor.first::<Cow<[u8]>, Cow<[u8]>>();
                cursor.first::<Cow<[u8]>, Cow<[u8]>>()
            } {
                size += v.len() as u64 + 8;
            }
            (last, size)
        } else {
            (None, 0)
        };

        drop(cursor);
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("commit: {e}")))?;

        let db = Arc::new(db);
        Ok(Self {
            segment_id,
            db,
            entries_dbi: dbi,
            data_size,
            first_index,
            last_index,
            path,
        })
    }
}

// ---------------------------------------------------------------------------
// MetaDb — per-group metadata database
// ---------------------------------------------------------------------------

struct MetaDb {
    db: Arc<Database<NoWriteMap>>,
    state_dbi: u32,
}

impl MetaDb {
    fn open(group_dir: &Path, config: &MdbxStorageConfig) -> io::Result<Self> {
        let path = meta_path(group_dir);
        let db = open_mdbx(
            &path,
            32 * 1024 * 1024, // 32 MB max for meta
            64 * 1024,
            config.sync_mode(),
            false,
        )?;

        let txn = db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("begin_rw_txn: {e}")))?;
        let table = txn
            .create_table(Some(META_STATE_TABLE), TableFlags::default())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("create_table: {e}")))?;
        let dbi = table.dbi();
        drop(table);
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("commit: {e}")))?;

        Ok(Self {
            db: Arc::new(db),
            state_dbi: dbi,
        })
    }

    fn read_meta(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("begin_ro_txn: {e}")))?;
        let table = unsafe { table_from_dbi::<'_>(self.state_dbi) };
        match txn.get::<Cow<[u8]>>(&table, key) {
            Ok(Some(data)) => Ok(Some(data.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("meta read: {e}"),
            )),
        }
    }

    fn write_meta(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("begin_rw_txn: {e}")))?;
        let table = unsafe { table_from_dbi::<'_>(self.state_dbi) };
        txn.put(&table, key, value, WriteFlags::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("meta write: {e}")))?;
        txn.commit()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("meta commit: {e}")))?;
        Ok(())
    }

    fn read_vote<C>(&self) -> io::Result<Option<openraft::impls::Vote<C>>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            >,
    {
        let data = self.read_meta(META_KEY_VOTE)?;
        match data {
            None => Ok(None),
            Some(bytes) => {
                let codec_vote = CodecVote::decode_from_slice(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                let vote = openraft::impls::Vote::<C>::from_codec(codec_vote);
                Ok(Some(vote))
            }
        }
    }

    fn read_log_id<C>(&self, key: &[u8]) -> io::Result<Option<LogId<C>>>
    where
        C: RaftTypeConfig<
                NodeId = u64,
                Term = u64,
                LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            >,
    {
        let data = self.read_meta(key)?;
        match data {
            None => Ok(None),
            Some(bytes) => {
                let codec_lid = CodecLogId::decode_from_slice(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                let lid = LogId::<C>::from_codec(codec_lid);
                Ok(Some(lid))
            }
        }
    }

    fn db(&self) -> &Arc<Database<NoWriteMap>> {
        &self.db
    }

    fn dbi(&self) -> u32 {
        self.state_dbi
    }
}

// ---------------------------------------------------------------------------
// WriteOp / WriteRequest — sent to writer thread
// ---------------------------------------------------------------------------

enum MdbxWriteOp {
    PutEntry { index: u64, data: Vec<u8> },
    SaveVote { data: Vec<u8> },
    SaveLastLogId { data: Vec<u8> },
    SavePurgedLogId { data: Vec<u8> },
    TruncateAfter { after_index: u64 },
    PurgeTo { purge_index: u64 },
}

struct MdbxWriteRequest<C: RaftTypeConfig> {
    ops: Vec<MdbxWriteOp>,
    callbacks: Callbacks<C>,
}

// SAFETY: MdbxWriteRequest is a plain data struct with no self-referential fields.
impl<C: RaftTypeConfig> Unpin for MdbxWriteRequest<C> {}

// ---------------------------------------------------------------------------
// MdbxGroupIndex — lock-free group lookup (duplicate of GroupIndex pattern)
// ---------------------------------------------------------------------------

struct MdbxGroupIndex<C: RaftTypeConfig> {
    slots: Box<[AtomicPtr<MdbxGroupState<C>>]>,
    count: AtomicUsize,
}

impl<C: RaftTypeConfig> MdbxGroupIndex<C> {
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
    fn get(&self, group_id: u64) -> Option<Arc<MdbxGroupState<C>>> {
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
        state: Arc<MdbxGroupState<C>>,
    ) -> Result<(), Arc<MdbxGroupState<C>>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return Err(state);
        }
        let new_ptr = Arc::into_raw(state) as *mut MdbxGroupState<C>;
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

    fn remove(&self, group_id: u64) -> Option<Arc<MdbxGroupState<C>>> {
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

    fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64, &Arc<MdbxGroupState<C>>),
    {
        for (idx, slot) in self.slots.iter().enumerate() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe {
                    let arc = Arc::from_raw(ptr);
                    f(idx as u64, &arc);
                    let _ = Arc::into_raw(arc);
                }
            }
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

impl<C: RaftTypeConfig> Drop for MdbxGroupIndex<C> {
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
// MdbxShardState — per-group shard (channel + shared state)
// ---------------------------------------------------------------------------

struct MdbxShardState<C: RaftTypeConfig> {
    write_tx: MAsyncTx<Array<MdbxWriteRequest<C>>>,
    running: Arc<AtomicBool>,
    failed: Arc<std::sync::OnceLock<ShardError>>,
    group_id: u64,
}

impl<C: RaftTypeConfig> MdbxShardState<C> {
    fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
        // Drop the sender channel to wake the writer thread
        // The writer thread will exit when it detects the channel is closed
    }

    fn check_failed(&self) -> io::Result<()> {
        if let Some(err) = self.failed.get() {
            Err(err.to_io_error())
        } else {
            Ok(())
        }
    }
}

impl<C: RaftTypeConfig + 'static> MdbxShardState<C> {
    fn spawn(
        group_id: u64,
        group_dir: PathBuf,
        config: Arc<MdbxStorageConfig>,
        meta_db: Arc<MetaDb>,
        segment_map: Arc<SegmentMap>,
        active_segment: ActiveSegment,
    ) -> Self {
        let (write_tx, write_rx) =
            crossfire::mpsc::bounded_async_blocking::<MdbxWriteRequest<C>>(256);
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let failed = Arc::new(std::sync::OnceLock::new());
        let failed_clone = failed.clone();

        std::thread::Builder::new()
            .name(format!("mdbx-raft-writer-{group_id}"))
            .spawn(move || {
                Self::writer_loop(
                    group_dir,
                    config,
                    meta_db,
                    segment_map,
                    active_segment,
                    write_rx,
                    running_clone,
                    failed_clone,
                );
            })
            .expect("Failed to spawn MDBX writer thread");

        Self {
            write_tx,
            running,
            failed,
            group_id,
        }
    }

    fn writer_loop(
        group_dir: PathBuf,
        config: Arc<MdbxStorageConfig>,
        meta_db: Arc<MetaDb>,
        segment_map: Arc<SegmentMap>,
        mut active: ActiveSegment,
        rx: Rx<Array<MdbxWriteRequest<C>>>,
        running: Arc<AtomicBool>,
        failed: Arc<std::sync::OnceLock<ShardError>>,
    ) {
        let mut pending_callbacks: Vec<IOFlushed<C>> = Vec::new();
        let mut last_sync = std::time::Instant::now();

        let fail_and_exit = |err: io::Error,
                             pending_callbacks: &mut Vec<IOFlushed<C>>,
                             failed: &std::sync::OnceLock<ShardError>| {
            let shard_err = ShardError {
                kind: err.kind(),
                message: err.to_string(),
            };
            let _ = failed.set(shard_err);
            for cb in pending_callbacks.drain(..) {
                cb.io_completed(Err(io::Error::new(err.kind(), err.to_string())));
            }
        };

        loop {
            // Block for first request
            let first = match rx.recv() {
                Ok(req) => req,
                Err(_) => return, // Channel closed
            };

            let mut batch = vec![first];

            // Drain without blocking to collect more requests
            for _ in 0..255 {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            // Begin RW transaction on active segment
            let txn = match active.db.begin_rw_txn() {
                Ok(txn) => txn,
                Err(e) => {
                    fail_and_exit(
                        io::Error::new(io::ErrorKind::Other, format!("begin_rw_txn: {e}")),
                        &mut pending_callbacks,
                        &failed,
                    );
                    return;
                }
            };
            let entries_table = unsafe { table_from_dbi::<'_>(active.entries_dbi) };

            let mut meta_ops: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut needs_truncate: Option<u64> = None;
            let mut needs_purge: Option<u64> = None;

            for request in &mut batch {
                for op in request.ops.drain(..) {
                    match op {
                        MdbxWriteOp::PutEntry { index, data } => {
                            let key = index_key(index);
                            if let Err(e) =
                                txn.put(&entries_table, &key, &data, WriteFlags::empty())
                            {
                                drop(entries_table);
                                drop(txn);
                                fail_and_exit(
                                    io::Error::new(io::ErrorKind::Other, format!("put entry: {e}")),
                                    &mut pending_callbacks,
                                    &failed,
                                );
                                return;
                            }
                            active.data_size += data.len() as u64 + 8;
                            if active.first_index.is_none() {
                                active.first_index = Some(index);
                            }
                            active.last_index = Some(index);
                        }
                        MdbxWriteOp::SaveVote { data } => {
                            meta_ops.push((META_KEY_VOTE.to_vec(), data));
                        }
                        MdbxWriteOp::SaveLastLogId { data } => {
                            meta_ops.push((META_KEY_LAST_LOG_ID.to_vec(), data));
                        }
                        MdbxWriteOp::SavePurgedLogId { data } => {
                            meta_ops.push((META_KEY_LAST_PURGED_LOG_ID.to_vec(), data));
                        }
                        MdbxWriteOp::TruncateAfter { after_index } => {
                            needs_truncate = Some(after_index);
                        }
                        MdbxWriteOp::PurgeTo { purge_index } => {
                            needs_purge = Some(purge_index);
                        }
                    }
                }
                std::mem::replace(&mut request.callbacks, Callbacks::None)
                    .extend_into(&mut pending_callbacks);
            }

            // Handle truncate within active segment
            if let Some(after_index) = needs_truncate {
                // Delete entries > after_index using cursor
                let mut cursor = match txn.cursor(&entries_table) {
                    Ok(c) => c,
                    Err(e) => {
                        drop(entries_table);
                        drop(txn);
                        fail_and_exit(
                            io::Error::new(io::ErrorKind::Other, format!("truncate cursor: {e}")),
                            &mut pending_callbacks,
                            &failed,
                        );
                        return;
                    }
                };
                let start_key = index_key(after_index.saturating_add(1));
                // Position cursor at the first key >= start_key and delete forward
                if cursor.set_range::<Cow<[u8]>, Cow<[u8]>>(&start_key).is_ok() {
                    loop {
                        if cursor.del(WriteFlags::empty()).is_err() {
                            break;
                        }
                        match cursor.next::<Cow<[u8]>, Cow<[u8]>>() {
                            Ok(Some(_)) => continue,
                            _ => break,
                        }
                    }
                }
                drop(cursor);
                active.last_index = if active.first_index.map_or(false, |fi| after_index >= fi) {
                    Some(after_index)
                } else {
                    None
                };

                // Also remove newer segments from the segment map
                let (removed_segs, _) = segment_map.remove_after(after_index, active.segment_id);
                for (path, _db) in &removed_segs {
                    let _ = std::fs::remove_file(path);
                    // Also remove lock file
                    let lock_path = path.with_extension("mdb-lock");
                    let _ = std::fs::remove_file(&lock_path);
                }
            }

            // Commit entries transaction
            drop(entries_table);
            if let Err(e) = txn.commit() {
                fail_and_exit(
                    io::Error::new(io::ErrorKind::Other, format!("commit entries: {e}")),
                    &mut pending_callbacks,
                    &failed,
                );
                return;
            }

            // Update segment map with latest last_index
            if let Some(li) = active.last_index {
                segment_map.update_last_index(active.segment_id, li);
            }

            // Write meta operations in a separate transaction
            if !meta_ops.is_empty() {
                let meta_txn = match meta_db.db().begin_rw_txn() {
                    Ok(txn) => txn,
                    Err(e) => {
                        fail_and_exit(
                            io::Error::new(io::ErrorKind::Other, format!("begin meta txn: {e}")),
                            &mut pending_callbacks,
                            &failed,
                        );
                        return;
                    }
                };
                let meta_table = unsafe { table_from_dbi::<'_>(meta_db.dbi()) };
                for (key, value) in &meta_ops {
                    if let Err(e) = meta_txn.put(
                        &meta_table,
                        key.as_slice(),
                        value.as_slice(),
                        WriteFlags::empty(),
                    ) {
                        drop(meta_table);
                        drop(meta_txn);
                        fail_and_exit(
                            io::Error::new(io::ErrorKind::Other, format!("put meta: {e}")),
                            &mut pending_callbacks,
                            &failed,
                        );
                        return;
                    }
                }
                drop(meta_table);
                if let Err(e) = meta_txn.commit() {
                    fail_and_exit(
                        io::Error::new(io::ErrorKind::Other, format!("commit meta: {e}")),
                        &mut pending_callbacks,
                        &failed,
                    );
                    return;
                }
            }

            // Handle purge (delete old segment files)
            if let Some(purge_index) = needs_purge {
                let removed_paths = segment_map.remove_purged(purge_index, active.segment_id);
                for path in &removed_paths {
                    let _ = std::fs::remove_file(path);
                    let lock_path = path.with_extension("mdb-lock");
                    let _ = std::fs::remove_file(&lock_path);
                }
            }

            // Periodic sync if configured
            if let Some(interval) = config.fsync_interval {
                if last_sync.elapsed() >= interval {
                    let _ = active.db.sync(true);
                    let _ = meta_db.db().sync(true);
                    last_sync = std::time::Instant::now();
                }
            }

            // Fire callbacks
            for cb in pending_callbacks.drain(..) {
                cb.io_completed(Ok(()));
            }

            // Check if rotation needed
            if active.data_size > config.segment_size {
                // Seal current segment into segment map
                if let (Some(fi), Some(li)) = (active.first_index, active.last_index) {
                    segment_map.add_sealed(SegmentEntry {
                        segment_id: active.segment_id,
                        first_index: fi,
                        last_index: li,
                        db: active.db.clone(),
                        path: active.path.clone(),
                    });
                }

                // Create new segment
                let new_id = active.segment_id + 1;
                match ActiveSegment::create(&group_dir, new_id, &config) {
                    Ok(new_seg) => {
                        // Add the new (empty) segment to segment map so reads can find it
                        active = new_seg;
                    }
                    Err(e) => {
                        fail_and_exit(
                            io::Error::new(io::ErrorKind::Other, format!("rotate segment: {e}")),
                            &mut pending_callbacks,
                            &failed,
                        );
                        return;
                    }
                }
            }

            if !running.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MdbxGroupState — per-group shared state
// ---------------------------------------------------------------------------

struct MdbxGroupState<C: RaftTypeConfig> {
    cache: DashMap<u64, Arc<C::Entry>>,
    vote: AtomicVote,
    first_index: AtomicU64,
    last_index: AtomicU64,
    cache_low: AtomicU64,
    last_log_id: AtomicLogId,
    last_purged_log_id: AtomicLogId,
    segment_map: Arc<SegmentMap>,
    meta_db: Arc<MetaDb>,
    shard: Arc<MdbxShardState<C>>,
}

impl<C: RaftTypeConfig + 'static> MdbxGroupState<C> {
    /// Create and recover group state from disk
    fn new(group_id: u64, config: &Arc<MdbxStorageConfig>, group_dir: &Path) -> io::Result<Self>
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

        // Open meta database
        let meta_db = Arc::new(MetaDb::open(group_dir, config)?);

        // Read persisted state
        let vote: Option<openraft::impls::Vote<C>> = meta_db.read_vote()?;
        let last_log_id: Option<LogId<C>> = meta_db.read_log_id(META_KEY_LAST_LOG_ID)?;
        let last_purged_log_id: Option<LogId<C>> =
            meta_db.read_log_id(META_KEY_LAST_PURGED_LOG_ID)?;

        // Scan for segment files
        let segment_ids = scan_segment_ids(group_dir)?;
        let segment_map = Arc::new(SegmentMap::new());

        let mut overall_first_index: Option<u64> = None;
        let mut overall_last_index: Option<u64> = None;

        // Open sealed segments (all except the last)
        let active_segment_id = segment_ids.last().copied();
        for &seg_id in &segment_ids {
            if Some(seg_id) == active_segment_id {
                continue; // Skip active segment, handle below
            }
            let path = segment_path(group_dir, seg_id);
            let db = open_mdbx(
                &path,
                config.max_db_size,
                config.growth_step,
                config.sync_mode(),
                false,
            )?;

            // Read first/last keys
            let txn = db
                .begin_ro_txn()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("ro_txn: {e}")))?;
            // Open or create table (ensure it exists)
            let rw_txn = db
                .begin_rw_txn()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("rw_txn: {e}")))?;
            let tbl = rw_txn
                .create_table(Some(ENTRIES_TABLE), TableFlags::default())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("create_table: {e}")))?;
            let dbi = tbl.dbi();
            drop(tbl);
            rw_txn
                .commit()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("commit: {e}")))?;

            drop(txn);
            let txn = db
                .begin_ro_txn()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("ro_txn: {e}")))?;
            let table = unsafe { table_from_dbi(dbi) };
            let mut cursor = txn
                .cursor(&table)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("cursor: {e}")))?;

            let first = cursor
                .first::<Cow<[u8]>, Cow<[u8]>>()
                .ok()
                .flatten()
                .map(|(k, _)| u64::from_be_bytes(k.as_ref().try_into().unwrap_or([0; 8])));
            let last = cursor
                .last::<Cow<[u8]>, Cow<[u8]>>()
                .ok()
                .flatten()
                .map(|(k, _)| u64::from_be_bytes(k.as_ref().try_into().unwrap_or([0; 8])));

            drop(cursor);
            drop(table);
            drop(txn);

            if let (Some(fi), Some(li)) = (first, last) {
                if overall_first_index.is_none() || fi < overall_first_index.unwrap() {
                    overall_first_index = Some(fi);
                }
                if overall_last_index.is_none() || li > overall_last_index.unwrap() {
                    overall_last_index = Some(li);
                }
                segment_map.add_sealed(SegmentEntry {
                    segment_id: seg_id,
                    first_index: fi,
                    last_index: li,
                    db: Arc::new(db),
                    path,
                });
            }
        }

        // Open or create active segment
        let active = if let Some(seg_id) = active_segment_id {
            let active = ActiveSegment::open_existing(group_dir, seg_id, config)?;
            if let (Some(fi), Some(li)) = (active.first_index, active.last_index) {
                if overall_first_index.is_none() || fi < overall_first_index.unwrap() {
                    overall_first_index = Some(fi);
                }
                if overall_last_index.is_none() || li > overall_last_index.unwrap() {
                    overall_last_index = Some(li);
                }
            }
            // Also register active segment in segment map for reads
            if let (Some(fi), Some(li)) = (active.first_index, active.last_index) {
                segment_map.add_sealed(SegmentEntry {
                    segment_id: active.segment_id,
                    first_index: fi,
                    last_index: li,
                    db: active.db.clone(),
                    path: active.path.clone(),
                });
            } else if active.first_index.is_none() {
                // Empty active segment - still register it for potential future reads
                // It will be updated when entries are written
            }
            active
        } else {
            // No segments exist, create the first one
            ActiveSegment::create(group_dir, 1, config)?
        };

        // Determine first_index accounting for purge
        let first_index = if let Some(ref plid) = last_purged_log_id {
            plid.index + 1
        } else {
            overall_first_index.unwrap_or(0)
        };

        let last_index = overall_last_index.unwrap_or(0);

        // Populate cache from last segment's entries
        let cache: DashMap<u64, Arc<C::Entry>> = DashMap::new();
        let max_cache = config.max_cache_entries_per_group as u64;
        if max_cache > 0 && last_index > 0 {
            let cache_start = last_index
                .saturating_sub(max_cache.saturating_sub(1))
                .max(first_index);
            // Read from the active segment (or last sealed) to populate cache
            if let Ok(txn) = active.db.begin_ro_txn() {
                let table = unsafe { table_from_dbi::<'_>(active.entries_dbi) };
                let start_key = index_key(cache_start);
                if let Ok(mut cursor) = txn.cursor(&table) {
                    if cursor.set_range::<Cow<[u8]>, Cow<[u8]>>(&start_key).is_ok() {
                        // Read the entry at set_range position
                        if let Ok(Some((k, v))) =
                            cursor.set_range::<Cow<[u8]>, Cow<[u8]>>(&start_key)
                        {
                            let idx = u64::from_be_bytes(k.as_ref().try_into().unwrap_or([0; 8]));
                            if idx >= first_index {
                                if let Ok(codec_entry) =
                                    CodecEntry::<RawBytes>::decode_from_slice(v.as_ref())
                                {
                                    let entry: openraft::impls::Entry<C> =
                                        openraft::impls::Entry::<C>::from_codec(codec_entry);
                                    cache.insert(idx, Arc::new(entry));
                                }
                            }
                        }
                        loop {
                            match cursor.next::<Cow<[u8]>, Cow<[u8]>>() {
                                Ok(Some((k, v))) => {
                                    let idx =
                                        u64::from_be_bytes(k.as_ref().try_into().unwrap_or([0; 8]));
                                    if idx >= first_index {
                                        if let Ok(codec_entry) =
                                            CodecEntry::<RawBytes>::decode_from_slice(v.as_ref())
                                        {
                                            let entry: openraft::impls::Entry<C> =
                                                openraft::impls::Entry::<C>::from_codec(
                                                    codec_entry,
                                                );
                                            cache.insert(idx, Arc::new(entry));
                                        }
                                    }
                                }
                                _ => break,
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

        // Spawn writer thread
        let shard = Arc::new(MdbxShardState::spawn(
            group_id,
            group_dir.to_path_buf(),
            config.clone(),
            meta_db.clone(),
            segment_map.clone(),
            active,
        ));

        Ok(Self {
            cache,
            vote: atomic_vote,
            first_index: AtomicU64::new(first_index),
            last_index: AtomicU64::new(last_index),
            cache_low: AtomicU64::new(0),
            last_log_id: atomic_last_log_id,
            last_purged_log_id: atomic_last_purged,
            segment_map,
            meta_db,
            shard,
        })
    }
}

// ---------------------------------------------------------------------------
// MdbxPerGroupLogStorage — top-level storage
// ---------------------------------------------------------------------------

/// MDBX-based segmented per-group log storage for Multi-Raft.
///
/// Each raft group gets its own set of segmented MDBX databases. Segments are
/// rotated based on size and old segments are deleted entirely on purge.
pub struct MdbxPerGroupLogStorage<C: RaftTypeConfig> {
    config: Arc<MdbxStorageConfig>,
    groups: MdbxGroupIndex<C>,
    creation_lock: tokio::sync::Mutex<()>,
}

impl<C: RaftTypeConfig + 'static> MdbxPerGroupLogStorage<C> {
    /// Create a new MDBX per-group storage instance.
    pub async fn new(config: impl Into<MdbxStorageConfig>) -> io::Result<Self> {
        let config = Arc::new(config.into());
        tokio::fs::create_dir_all(&*config.base_dir).await?;

        Ok(Self {
            config,
            groups: MdbxGroupIndex::new(),
            creation_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Stop all groups
    pub fn stop(&self) {
        self.groups.for_each(|_group_id, state| {
            state.shard.shutdown();
        });
    }

    async fn get_or_create_group(&self, group_id: u64) -> io::Result<Arc<MdbxGroupState<C>>>
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

        let state =
            tokio::task::spawn_blocking(move || MdbxGroupState::new(group_id, &config, &group_dir))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        let state = Arc::new(state);
        match self.groups.insert(group_id, state.clone()) {
            Ok(()) => Ok(state),
            Err(_) => {
                state.shard.shutdown();
                Ok(self
                    .groups
                    .get(group_id)
                    .expect("group must exist after failed insert"))
            }
        }
    }

    /// Get a log storage handle for a specific group
    pub async fn get_log_storage(&self, group_id: u64) -> io::Result<MdbxGroupLogStorage<C>>
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
        Ok(MdbxGroupLogStorage {
            group_id,
            config: self.config.clone(),
            state: group_state,
            encode_buf: Vec::new(),
        })
    }

    /// Remove a group from this storage
    pub fn remove_group(&self, group_id: u64) {
        if let Some(state) = self.groups.remove(group_id) {
            state.shard.shutdown();
        }
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
// MdbxGroupLogStorage — per-group handle
// ---------------------------------------------------------------------------

/// Log storage handle for a specific group within MdbxPerGroupLogStorage.
pub struct MdbxGroupLogStorage<C: RaftTypeConfig> {
    group_id: u64,
    config: Arc<MdbxStorageConfig>,
    state: Arc<MdbxGroupState<C>>,
    encode_buf: Vec<u8>,
}

impl<C: RaftTypeConfig> Clone for MdbxGroupLogStorage<C> {
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id,
            config: self.config.clone(),
            state: self.state.clone(),
            encode_buf: Vec::new(),
        }
    }
}

impl<C: RaftTypeConfig> MdbxGroupLogStorage<C> {
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

    /// Read an entry from MDBX by index (cache miss path)
    fn read_entry_from_mdbx(&self, index: u64) -> io::Result<Option<C::Entry>>
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
        let db = match self.state.segment_map.find_db(index) {
            Some(db) => db,
            None => return Ok(None),
        };

        let txn = db
            .begin_ro_txn()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("ro_txn: {e}")))?;

        // We need the entries table DBI. Since we can't easily get it from a sealed
        // segment without opening it for write, we open/create the table.
        // For RO transactions, we need to open the existing table.
        let table = txn
            .open_table(Some(ENTRIES_TABLE))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("open_table: {e}")))?;

        let key = index_key(index);
        match txn.get::<Cow<[u8]>>(&table, &key) {
            Ok(Some(data)) => {
                let codec_entry = CodecEntry::<RawBytes>::decode_from_slice(data.as_ref())
                    .map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("decode: {e}"))
                    })?;
                let entry: openraft::impls::Entry<C> =
                    openraft::impls::Entry::<C>::from_codec(codec_entry);
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("get entry: {e}"),
            )),
        }
    }

    /// Send a write request to the writer thread and wait for completion
    async fn send_write(&self, ops: Vec<MdbxWriteOp>, callbacks: Callbacks<C>) -> io::Result<()> {
        self.state.shard.check_failed()?;

        let request = MdbxWriteRequest { ops, callbacks };
        self.state.shard.write_tx.send(request).await.map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "MDBX writer thread disconnected")
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader implementation
// ---------------------------------------------------------------------------

impl<C> RaftLogReader<C> for MdbxGroupLogStorage<C>
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

            // Slow path: MDBX read
            if let Some(entry) = self.read_entry_from_mdbx(idx)? {
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

impl<C> RaftLogStorage<C> for MdbxGroupLogStorage<C>
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

        // Encode vote
        let codec_vote = vote.to_codec();
        self.encode_buf.clear();
        codec_vote
            .encode_into(&mut self.encode_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let ops = vec![MdbxWriteOp::SaveVote {
            data: self.encode_buf.clone(),
        }];
        self.send_write(ops, Callbacks::None).await?;
        Ok(())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + Send,
    {
        use openraft::entry::RaftEntry;

        let mut ops = Vec::new();
        let mut last_log_id = None;

        for entry in entries {
            let log_id = entry.log_id();
            let index = log_id.index;

            let codec_entry: CodecEntry<RawBytes> = entry.to_codec();
            self.encode_buf.clear();
            codec_entry
                .encode_into(&mut self.encode_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            ops.push(MdbxWriteOp::PutEntry {
                index,
                data: self.encode_buf.clone(),
            });

            self.state.cache.insert(index, Arc::new(entry));
            self.state.last_index.fetch_max(index, Ordering::Relaxed);
            last_log_id = Some(log_id);
        }

        // Update last log id
        if let Some(ref lid) = last_log_id {
            self.state.last_log_id.store(Some(lid));

            // Also persist last_log_id
            let codec_lid: CodecLogId = lid.to_codec();
            self.encode_buf.clear();
            codec_lid
                .encode_into(&mut self.encode_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            ops.push(MdbxWriteOp::SaveLastLogId {
                data: self.encode_buf.clone(),
            });
        }

        self.send_write(ops, Callbacks::One(callback)).await?;
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

        // Update last index
        self.state.last_index.store(index, Ordering::Relaxed);
        if let Some(ref log_id) = after {
            self.state.last_log_id.store(Some(log_id));
        } else {
            self.state.last_log_id.store::<C>(None);
        }

        let mut ops = vec![MdbxWriteOp::TruncateAfter { after_index: index }];

        // Persist updated last_log_id
        if let Some(log_id) = after {
            let codec_lid: CodecLogId = log_id.to_codec();
            self.encode_buf.clear();
            codec_lid
                .encode_into(&mut self.encode_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            ops.push(MdbxWriteOp::SaveLastLogId {
                data: self.encode_buf.clone(),
            });
        }

        self.send_write(ops, Callbacks::None).await?;
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

        // Encode purged log id
        let codec_lid: CodecLogId = log_id.to_codec();
        self.encode_buf.clear();
        codec_lid
            .encode_into(&mut self.encode_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let ops = vec![
            MdbxWriteOp::SavePurgedLogId {
                data: self.encode_buf.clone(),
            },
            MdbxWriteOp::PurgeTo { purge_index: index },
        ];

        self.send_write(ops, Callbacks::None).await?;
        self.enforce_cache_window();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MultiplexedStorage + MultiRaftLogStorage implementations
// ---------------------------------------------------------------------------

impl<C> MultiplexedStorage<C> for MdbxPerGroupLogStorage<C>
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
    type GroupLogStorage = MdbxGroupLogStorage<C>;

    async fn get_log_storage(&self, group_id: u64) -> Self::GroupLogStorage {
        MdbxPerGroupLogStorage::get_log_storage(self, group_id)
            .await
            .expect("Failed to create MDBX group storage")
    }

    fn remove_group(&self, group_id: u64) {
        MdbxPerGroupLogStorage::remove_group(self, group_id)
    }

    fn group_ids(&self) -> Vec<u64> {
        MdbxPerGroupLogStorage::group_ids(self)
    }
}

impl<C> crate::multi::storage::MultiRaftLogStorage<C> for MdbxPerGroupLogStorage<C>
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
    type GroupLogStorage = MdbxGroupLogStorage<C>;

    async fn get_log_storage(&self, group_id: u64) -> Self::GroupLogStorage {
        MdbxPerGroupLogStorage::get_log_storage(self, group_id)
            .await
            .expect("Failed to create MDBX group storage")
    }

    fn remove_group(&self, group_id: u64) {
        MdbxPerGroupLogStorage::remove_group(self, group_id)
    }

    fn group_ids(&self) -> Vec<u64> {
        MdbxPerGroupLogStorage::group_ids(self)
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

    // Helper to create IOFlushed callback using openraft's oneshot
    fn make_callback() -> (
        IOFlushed<C>,
        <Os as Oneshot>::Receiver<Result<(), io::Error>>,
    ) {
        let (tx, rx) = Os::channel::<Result<(), io::Error>>();
        let cb = IOFlushed::<C>::signal(tx);
        (cb, rx)
    }

    #[test]
    fn test_mdbx_basic_append_and_read() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();

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
    fn test_mdbx_save_and_read_vote() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();

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
    fn test_mdbx_log_state() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();

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
    fn test_mdbx_truncate_after() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();

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
    fn test_mdbx_purge() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();

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

            // Give writer thread time to process
            tokio::time::sleep(Duration::from_millis(50)).await;

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 3);

            // Should still be able to read entries 4 and 5
            let result = log.try_get_log_entries(4..6).await.unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].log_id.index, 4);

            storage.stop();
        });
    }

    #[test]
    fn test_mdbx_group_isolation() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();

            let mut log0 = storage.get_log_storage(0).await.unwrap();
            let mut log1 = storage.get_log_storage(1).await.unwrap();

            // Append different entries to each group
            let entries0 = vec![make_entry(1, 1)];
            let (cb0, rx0) = make_callback();
            log0.append(entries0, cb0).await.unwrap();
            rx0.await.unwrap().unwrap();

            let entries1 = vec![make_entry(1, 2), make_entry(2, 2)];
            let (cb1, rx1) = make_callback();
            log1.append(entries1, cb1).await.unwrap();
            rx1.await.unwrap().unwrap();

            // Each group should only see its own entries
            let result0 = log0.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(result0.len(), 1);
            assert_eq!(result0[0].log_id.leader_id.term, 1);

            let result1 = log1.try_get_log_entries(1..3).await.unwrap();
            assert_eq!(result1.len(), 2);
            assert_eq!(result1[0].log_id.leader_id.term, 2);

            storage.stop();
        });
    }

    #[test]
    fn test_mdbx_recovery() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write data
        let path1 = path.clone();
        run_async(async move {
            let config = MdbxStorageConfig::new(&path1);
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
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

            // Give writer time to flush
            tokio::time::sleep(Duration::from_millis(100)).await;
            storage.stop();
        });

        // Phase 2: Recover and verify
        let path2 = path.clone();
        run_async(async move {
            let config = MdbxStorageConfig::new(&path2);
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Verify entries survived recovery
            let result = log.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[0].log_id.index, 1);
            assert_eq!(result[2].log_id.index, 3);

            // Verify vote survived
            let vote = log.read_vote().await.unwrap().unwrap();
            assert_eq!(vote.leader_id.term, 3);
            assert_eq!(vote.leader_id.node_id, 7);

            // Verify log state
            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 3);

            storage.stop();
        });
    }

    #[test]
    fn test_mdbx_segment_rotation() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Small segment size to force rotation
            let config = MdbxStorageConfig::new(tmp.path()).with_segment_size(512);
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write enough entries to force rotation
            for i in 1..=100 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Verify all entries readable
            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100);
            assert_eq!(result[0].log_id.index, 1);
            assert_eq!(result[99].log_id.index, 100);

            // Verify multiple segments were created
            let group_dir = group_dir_path(tmp.path(), 0);
            let seg_ids = scan_segment_ids(&group_dir).unwrap();
            assert!(
                seg_ids.len() > 1,
                "Expected multiple segments, got {}",
                seg_ids.len()
            );

            storage.stop();
        });
    }

    #[test]
    fn test_mdbx_purge_deletes_segments() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            // Small segment size to force multiple segments
            let config = MdbxStorageConfig::new(tmp.path()).with_segment_size(256);
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write entries across multiple segments
            for i in 1..=50 {
                let entries = vec![make_entry(i, 1)];
                let (cb, rx) = make_callback();
                log.append(entries, cb).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            let group_dir = group_dir_path(tmp.path(), 0);
            let seg_count_before = scan_segment_ids(&group_dir).unwrap().len();
            assert!(seg_count_before > 1);

            // Purge first 40 entries
            let purge_id = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 40,
            };
            log.purge(purge_id).await.unwrap();

            // Give writer time to process
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Some segments should have been deleted
            let seg_count_after = scan_segment_ids(&group_dir).unwrap().len();
            assert!(
                seg_count_after < seg_count_before,
                "Expected segment deletion: before={}, after={}",
                seg_count_before,
                seg_count_after
            );

            // Remaining entries should still be readable
            let result = log.try_get_log_entries(41..51).await.unwrap();
            assert_eq!(result.len(), 10);
            assert_eq!(result[0].log_id.index, 41);

            storage.stop();
        });
    }

    #[test]
    fn test_mdbx_large_entry() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Write a large entry (TestData is small, but this verifies the path)
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
    fn test_mdbx_concurrent_groups() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = Arc::new(MdbxPerGroupLogStorage::<C>::new(config).await.unwrap());

            let mut handles = Vec::new();
            for group_id in 0..4u64 {
                let storage = storage.clone();
                handles.push(tokio::spawn(async move {
                    let mut log = storage.get_log_storage(group_id).await.unwrap();
                    for i in 1..=20 {
                        let entries = vec![make_entry(i, group_id + 1)];
                        let (cb, rx) = make_callback();
                        log.append(entries, cb).await.unwrap();
                        rx.await.unwrap().unwrap();
                    }
                    // Verify
                    let result = log.try_get_log_entries(1..21).await.unwrap();
                    assert_eq!(result.len(), 20);
                    for (i, entry) in result.iter().enumerate() {
                        assert_eq!(entry.log_id.index, i as u64 + 1);
                        assert_eq!(entry.log_id.leader_id.term, group_id + 1);
                    }
                }));
            }

            for h in handles {
                h.await.unwrap();
            }

            assert_eq!(storage.num_groups(), 4);
            storage.stop();
        });
    }

    #[test]
    fn test_mdbx_batch_append() {
        run_async(async {
            let tmp = TempDir::new().unwrap();
            let config = MdbxStorageConfig::new(tmp.path());
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            // Append a batch of entries
            let entries: Vec<_> = (1..=100).map(|i| make_entry(i, 1)).collect();
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Verify all entries
            let result = log.try_get_log_entries(1..101).await.unwrap();
            assert_eq!(result.len(), 100);

            storage.stop();
        });
    }

    #[test]
    fn test_mdbx_recovery_with_purge() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: Write and purge
        let path1 = path.clone();
        run_async(async move {
            let config = MdbxStorageConfig::new(&path1);
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let entries: Vec<_> = (1..=10).map(|i| make_entry(i, 1)).collect();
            let (cb, rx) = make_callback();
            log.append(entries, cb).await.unwrap();
            rx.await.unwrap().unwrap();

            // Purge first 5
            let purge_id = LogId {
                leader_id: openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                index: 5,
            };
            log.purge(purge_id).await.unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;
            storage.stop();
        });

        // Phase 2: Recover and verify purge state
        let path2 = path.clone();
        run_async(async move {
            let config = MdbxStorageConfig::new(&path2);
            let storage = MdbxPerGroupLogStorage::<C>::new(config).await.unwrap();
            let mut log = storage.get_log_storage(0).await.unwrap();

            let state = log.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 5);
            assert_eq!(state.last_log_id.unwrap().index, 10);

            // Should still read entries 6..=10
            let result = log.try_get_log_entries(6..11).await.unwrap();
            assert_eq!(result.len(), 5);
            assert_eq!(result[0].log_id.index, 6);

            storage.stop();
        });
    }
}
