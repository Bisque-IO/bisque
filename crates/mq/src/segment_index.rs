//! Segment index (.sidx) — per-segment entity-level index.
//!
//! Two concrete types share a common read-only trait:
//!
//! * [`SegmentIndexBuilder`] — append-optimised in-memory builder used during a
//!   segment's active lifetime.  Uses a lock-free [`BuilderMap`] so readers
//!   never block the single writer.
//!
//! * [`FrozenSegmentIndex`] — zero-deserialisation read-only index backed by a
//!   flat byte buffer (mmap or `Vec<u8>`).  Contains an open-addressing hash
//!   table for O(1) entity lookup.
//!
//! Both implement [`SegmentIndex`] so callers can iterate records for a given
//! `(entity_type, entity_id)` without caring which representation they hold.
//!
//! [`SegmentIndexMap`] is a concurrent `segment_id → SegmentIndexBuilder` map
//! that lives outside the engine's `RwLock`, allowing readers to query the
//! tail segment's index without blocking on applies.
//!
//! ## On-disk layout (v2)
//!
//! ```text
//! [Header: 16 bytes]
//!   magic:            [u8; 4]     b"SIDX"
//!   version:          u32 LE      2
//!   segment_id:       u32 LE
//!   bucket_count:     u32 LE      power-of-2
//!
//! [Hash table: bucket_count × 24 bytes]
//!   entity_type:      u8          0xFF = empty
//!   _pad:             [u8; 3]
//!   entity_id:        u64 LE
//!   records_offset:   u32 LE      byte offset from file start
//!   records_count:    u32 LE
//!
//! [Records region: N × 8 bytes]
//!   record_offset:    u32 LE      byte offset in the raft segment
//!   record_len:       u32 LE
//! ```

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::types::MqCommand;

// ---- Constants ----

pub const SIDX_MAGIC: [u8; 4] = *b"SIDX";
pub const SIDX_VERSION: u32 = 2;
pub const SIDX_HEADER_SIZE: usize = 16;
pub const SIDX_BUCKET_SIZE: usize = 24;
pub const SIDX_RECORD_SIZE: usize = 8;

pub const ENTITY_TOPIC: u8 = 0;
pub const ENTITY_QUEUE: u8 = 1;
pub const ENTITY_ACTOR_NS: u8 = 2;

const BUCKET_EMPTY: u8 = 0xFF;

// ---- Record location ----

/// A single record location within a raft segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct RecordLoc {
    pub offset: u32,
    pub len: u32,
}

// ---- Trait ----

/// Read-only access to a segment index, shared by builder and frozen types.
pub trait SegmentIndex: Send + Sync {
    fn segment_id(&self) -> u32;
    fn is_empty(&self) -> bool;
    fn entry_count(&self) -> usize;

    /// Iterate record locations for a given `(entity_type, entity_id)`.
    fn find_entity(&self, entity_type: u8, entity_id: u64) -> Vec<RecordLoc>;
}

// ============================================================================
// AppendVec — lock-free, single-writer / multi-reader, chunked append-only vec
// ============================================================================

const CHUNK_BITS: u32 = 6;
const CHUNK_SIZE: usize = 1 << CHUNK_BITS; // 64 entries per chunk
const CHUNK_MASK: usize = CHUNK_SIZE - 1;
const MAX_CHUNKS: usize = 32; // up to 2048 records per entity per segment

type Chunk = [RecordLoc; CHUNK_SIZE];

/// Lock-free append-only vec using fixed-size heap chunks and atomic length.
///
/// # Safety contract
///
/// *Single writer*: only one thread may call [`push`] at a time.
///
/// *Multiple readers*: any number of threads may call [`iter`] concurrently
/// with the single writer — they will see a consistent prefix of entries up
/// to the length observed at their `Acquire` load.
pub struct AppendVec {
    chunks: [AtomicPtr<Chunk>; MAX_CHUNKS],
    len: AtomicU32,
}

// SAFETY: The atomic operations + single-writer contract ensure safe sharing.
unsafe impl Send for AppendVec {}
unsafe impl Sync for AppendVec {}

const NULL_CHUNK: AtomicPtr<Chunk> = AtomicPtr::new(std::ptr::null_mut());

impl AppendVec {
    pub fn new() -> Self {
        Self {
            chunks: [NULL_CHUNK; MAX_CHUNKS],
            len: AtomicU32::new(0),
        }
    }

    /// Number of records visible to readers.
    #[inline]
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append a record.  **Single-writer only.**
    ///
    /// # Panics
    ///
    /// Panics if the vec exceeds `MAX_CHUNKS * CHUNK_SIZE` entries.
    #[inline]
    pub fn push(&self, rec: RecordLoc) {
        // Relaxed is fine — only one writer, so we always see our own stores.
        let cur = self.len.load(Ordering::Relaxed) as usize;
        let chunk_idx = cur >> CHUNK_BITS;
        let slot = cur & CHUNK_MASK;

        assert!(chunk_idx < MAX_CHUNKS, "AppendVec overflow");

        // Allocate chunk on first use.
        let mut ptr = self.chunks[chunk_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            // zeroed is fine — RecordLoc is Copy and we only read up to `len`.
            let boxed = Box::new(unsafe { std::mem::zeroed::<Chunk>() });
            ptr = Box::into_raw(boxed);
            self.chunks[chunk_idx].store(ptr, Ordering::Release);
        }

        // Write the slot.
        unsafe {
            (*ptr)[slot] = rec;
        }

        // Publish the new length — Release pairs with readers' Acquire.
        self.len.store((cur + 1) as u32, Ordering::Release);
    }

    /// Iterate all entries visible at the time of the call.
    #[inline]
    pub fn iter(&self) -> AppendVecIter<'_> {
        let len = self.len.load(Ordering::Acquire) as usize;
        AppendVecIter {
            vec: self,
            pos: 0,
            len,
        }
    }
}

impl Drop for AppendVec {
    fn drop(&mut self) {
        for chunk_ptr in &mut self.chunks {
            let ptr = *chunk_ptr.get_mut();
            if !ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
        }
    }
}

/// Iterator over an [`AppendVec`] snapshot.
pub struct AppendVecIter<'a> {
    vec: &'a AppendVec,
    pos: usize,
    len: usize,
}

impl<'a> Iterator for AppendVecIter<'a> {
    type Item = RecordLoc;

    #[inline]
    fn next(&mut self) -> Option<RecordLoc> {
        if self.pos >= self.len {
            return None;
        }
        let chunk_idx = self.pos >> CHUNK_BITS;
        let slot = self.pos & CHUNK_MASK;
        self.pos += 1;

        let ptr = self.vec.chunks[chunk_idx].load(Ordering::Acquire);
        debug_assert!(!ptr.is_null());
        Some(unsafe { (*ptr)[slot] })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.len - self.pos;
        (rem, Some(rem))
    }
}

impl<'a> ExactSizeIterator for AppendVecIter<'a> {}

// ============================================================================
// BuilderMap — lock-free, single-writer / multi-reader open-addressing hash map
// ============================================================================

/// Minimum number of buckets (always power of 2).
const MIN_BUCKETS: usize = 16;
/// Resize when load factor exceeds 70%.
const LOAD_FACTOR_NUM: usize = 7;
const LOAD_FACTOR_DEN: usize = 10;

/// Empty sentinel for packed keys.
const KEY_EMPTY: u64 = 0;

/// Pack `(entity_type, entity_id)` into a single `u64`.
///
/// Uses `(entity_type + 1)` in the top byte to ensure the packed value is
/// never zero (the empty sentinel), even when `entity_id == 0`.
#[inline]
fn pack_key(entity_type: u8, entity_id: u64) -> u64 {
    debug_assert!(entity_id <= 0x00FF_FFFF_FFFF_FFFF);
    ((entity_type as u64 + 1) << 56) | (entity_id & 0x00FF_FFFF_FFFF_FFFF)
}

/// Unpack a packed key back to `(entity_type, entity_id)`.
#[inline]
fn unpack_key(packed: u64) -> (u8, u64) {
    let entity_type = ((packed >> 56) - 1) as u8;
    let entity_id = packed & 0x00FF_FFFF_FFFF_FFFF;
    (entity_type, entity_id)
}

/// A single bucket in the hash table.
struct Bucket {
    /// Packed `(entity_type, entity_id)`.  `KEY_EMPTY` (0) means unoccupied.
    key: AtomicU64,
    /// Pointer to a heap-allocated `AppendVec`.  Non-null once the bucket is
    /// occupied — stored with `Release` *before* the key is published.
    records: AtomicPtr<AppendVec>,
}

/// An open-addressing hash table with power-of-2 bucket count.
struct Table {
    buckets: Box<[Bucket]>,
    mask: usize,
}

impl Table {
    fn new(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        let buckets: Vec<Bucket> = (0..capacity)
            .map(|_| Bucket {
                key: AtomicU64::new(KEY_EMPTY),
                records: AtomicPtr::new(std::ptr::null_mut()),
            })
            .collect();
        Self {
            buckets: buckets.into_boxed_slice(),
            mask: capacity - 1,
        }
    }

    fn capacity(&self) -> usize {
        self.buckets.len()
    }
}

/// Lock-free, single-writer / multi-reader open-addressing hash map.
///
/// Maps `(entity_type, entity_id)` → `AppendVec` of record locations.
///
/// # Safety contract
///
/// *Single writer*: only one thread may call [`insert_or_append`] at a time.
///
/// *Multiple readers*: any number of threads may call [`find`] concurrently
/// with the single writer.
///
/// On resize, the old table is retired to an internal list (kept alive for
/// concurrent readers that may still be probing it).  All retired tables and
/// `AppendVec` allocations are freed on drop.
pub struct BuilderMap {
    table: AtomicPtr<Table>,
    /// Retired tables kept alive for concurrent readers.
    /// Only the writer appends (single-writer guarantee); Drop frees them.
    retired: UnsafeCell<Vec<Box<Table>>>,
    /// Number of distinct entities (occupied buckets in the current table).
    distinct: AtomicU32,
    /// Total number of records across all entities.
    total: AtomicU32,
}

// SAFETY: Single-writer + atomic ops ensure safe sharing.
unsafe impl Send for BuilderMap {}
unsafe impl Sync for BuilderMap {}

impl BuilderMap {
    pub fn new(initial_capacity: usize) -> Self {
        let cap = initial_capacity.max(MIN_BUCKETS).next_power_of_two();
        let table = Box::new(Table::new(cap));
        Self {
            table: AtomicPtr::new(Box::into_raw(table)),
            retired: UnsafeCell::new(Vec::new()),
            distinct: AtomicU32::new(0),
            total: AtomicU32::new(0),
        }
    }

    /// Insert a record for the given entity.  **Single-writer only.**
    ///
    /// If the entity already exists, appends to its `AppendVec`.
    /// If it's new, allocates an `AppendVec`, inserts it, and may resize.
    pub fn insert_or_append(&self, entity_type: u8, entity_id: u64, rec: RecordLoc) {
        let packed = pack_key(entity_type, entity_id);
        let table = unsafe { &*self.table.load(Ordering::Acquire) };

        // Probe for existing entry or empty slot.
        let hash = entity_hash(entity_type, entity_id);
        let mut idx = hash & table.mask;
        loop {
            let bucket = &table.buckets[idx];
            let k = bucket.key.load(Ordering::Acquire);

            if k == packed {
                // Existing entity — append to its AppendVec.
                let vec_ptr = bucket.records.load(Ordering::Acquire);
                debug_assert!(!vec_ptr.is_null());
                unsafe { (*vec_ptr).push(rec) };
                self.total.fetch_add(1, Ordering::Relaxed);
                return;
            }

            if k == KEY_EMPTY {
                // Empty slot — insert new entity.
                break;
            }

            idx = (idx + 1) & table.mask;
        }

        // Check if we need to resize before inserting.
        let distinct = self.distinct.load(Ordering::Relaxed) as usize + 1;
        if distinct * LOAD_FACTOR_DEN > table.capacity() * LOAD_FACTOR_NUM {
            self.resize(table.capacity() * 2);
            // After resize, re-probe in the new table.
            self.insert_into_current(packed, rec);
            return;
        }

        // Insert into the empty slot we found.
        let bucket = &table.buckets[idx];

        // Allocate AppendVec and push the first record.
        let vec = Box::new(AppendVec::new());
        vec.push(rec);
        let vec_ptr = Box::into_raw(vec);

        // CRITICAL ORDERING: store records pointer (Release) BEFORE key.
        // Readers load key first (Acquire) — if they see a non-empty key,
        // the records pointer is guaranteed visible.
        bucket.records.store(vec_ptr, Ordering::Release);
        bucket.key.store(packed, Ordering::Release);

        self.distinct.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    /// Insert into the current table after a resize.  Single-writer only.
    fn insert_into_current(&self, packed: u64, rec: RecordLoc) {
        let table = unsafe { &*self.table.load(Ordering::Acquire) };
        let (entity_type, entity_id) = unpack_key(packed);
        let hash = entity_hash(entity_type, entity_id);
        let mut idx = hash & table.mask;

        loop {
            let bucket = &table.buckets[idx];
            let k = bucket.key.load(Ordering::Relaxed); // single writer

            if k == packed {
                // Was rehashed from old table — just append.
                let vec_ptr = bucket.records.load(Ordering::Relaxed);
                unsafe { (*vec_ptr).push(rec) };
                self.total.fetch_add(1, Ordering::Relaxed);
                return;
            }

            if k == KEY_EMPTY {
                // New entry in the resized table.
                let vec = Box::new(AppendVec::new());
                vec.push(rec);
                let vec_ptr = Box::into_raw(vec);
                bucket.records.store(vec_ptr, Ordering::Release);
                bucket.key.store(packed, Ordering::Release);
                self.distinct.fetch_add(1, Ordering::Relaxed);
                self.total.fetch_add(1, Ordering::Relaxed);
                return;
            }

            idx = (idx + 1) & table.mask;
        }
    }

    /// Resize the hash table.  Single-writer only.
    ///
    /// Copies all occupied entries from the old table to a new larger table.
    /// The old table is retired (kept alive for concurrent readers).
    fn resize(&self, new_capacity: usize) {
        let old_table = unsafe { &*self.table.load(Ordering::Relaxed) };
        let new_table = Box::new(Table::new(new_capacity));

        // Rehash all occupied entries from old → new.
        for bucket in old_table.buckets.iter() {
            let k = bucket.key.load(Ordering::Relaxed);
            if k == KEY_EMPTY {
                continue;
            }
            let vec_ptr = bucket.records.load(Ordering::Relaxed);

            // Find empty slot in new table.
            let (et, eid) = unpack_key(k);
            let hash = entity_hash(et, eid);
            let mut idx = hash & new_table.mask;
            loop {
                let new_bucket = &new_table.buckets[idx];
                if new_bucket.key.load(Ordering::Relaxed) == KEY_EMPTY {
                    // Copy the pointer — AppendVec is heap-stable, survives resize.
                    new_bucket.records.store(vec_ptr, Ordering::Relaxed);
                    new_bucket.key.store(k, Ordering::Relaxed);
                    break;
                }
                idx = (idx + 1) & new_table.mask;
            }
        }

        // Swap in new table with Release so readers see the new data.
        let new_ptr = Box::into_raw(new_table);
        let old_ptr = self.table.swap(new_ptr, Ordering::Release);

        // Retire old table — keep alive for concurrent readers.
        let retired = unsafe { &mut *self.retired.get() };
        retired.push(unsafe { Box::from_raw(old_ptr) });
    }

    /// Look up an entity's records.  Safe for concurrent readers.
    pub fn find(&self, entity_type: u8, entity_id: u64) -> Option<&AppendVec> {
        let packed = pack_key(entity_type, entity_id);
        let table = unsafe { &*self.table.load(Ordering::Acquire) };
        let hash = entity_hash(entity_type, entity_id);
        let mut idx = hash & table.mask;

        loop {
            let bucket = &table.buckets[idx];
            let k = bucket.key.load(Ordering::Acquire);

            if k == KEY_EMPTY {
                return None;
            }

            if k == packed {
                let vec_ptr = bucket.records.load(Ordering::Acquire);
                debug_assert!(!vec_ptr.is_null());
                // SAFETY: AppendVec is heap-allocated and lives until BuilderMap
                // is dropped.  Concurrent readers only call `iter()` on it.
                return Some(unsafe { &*vec_ptr });
            }

            idx = (idx + 1) & table.mask;
        }
    }

    /// Number of distinct entities.
    #[inline]
    pub fn distinct_count(&self) -> u32 {
        self.distinct.load(Ordering::Relaxed)
    }

    /// Total record count across all entities.
    #[inline]
    pub fn total_count(&self) -> u32 {
        self.total.load(Ordering::Relaxed)
    }

    /// Iterate all occupied entries.  Returns `(packed_key, &AppendVec)`.
    ///
    /// Snapshot: reads entries visible at the time of the call.
    pub fn iter(&self) -> BuilderMapIter<'_> {
        let table = unsafe { &*self.table.load(Ordering::Acquire) };
        BuilderMapIter { table, pos: 0 }
    }
}

impl Drop for BuilderMap {
    fn drop(&mut self) {
        // Free the current table's AppendVecs.
        let table = unsafe { &mut *(*self.table.get_mut()) };
        for bucket in table.buckets.iter_mut() {
            let k = *bucket.key.get_mut();
            if k != KEY_EMPTY {
                let ptr = *bucket.records.get_mut();
                if !ptr.is_null() {
                    unsafe { drop(Box::from_raw(ptr)) };
                }
            }
        }
        // Free the current table.
        unsafe { drop(Box::from_raw(*self.table.get_mut())) };

        // Free retired tables (their AppendVec pointers are stale copies
        // that were moved to the current table during resize — don't
        // double-free them).
        // The `Vec<Box<Table>>` drop handles freeing the table allocations.
        // Bucket contents in retired tables have dangling pointers but
        // we never dereference them — the Box<Table> drop just frees the
        // bucket array memory.
    }
}

/// Iterator over occupied entries in a [`BuilderMap`].
pub struct BuilderMapIter<'a> {
    table: &'a Table,
    pos: usize,
}

impl<'a> Iterator for BuilderMapIter<'a> {
    type Item = (u64, &'a AppendVec);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.table.buckets.len() {
            let bucket = &self.table.buckets[self.pos];
            self.pos += 1;
            let k = bucket.key.load(Ordering::Acquire);
            if k != KEY_EMPTY {
                let vec_ptr = bucket.records.load(Ordering::Acquire);
                debug_assert!(!vec_ptr.is_null());
                return Some((k, unsafe { &*vec_ptr }));
            }
        }
        None
    }
}

// ============================================================================
// SegmentIndexBuilder — in-memory, append-optimised
// ============================================================================

/// Per-segment index builder.  Entries are appended during the segment's active
/// lifetime via [`add_entry`].  When the segment is sealed, call [`serialize`]
/// to produce the on-disk [`FrozenSegmentIndex`] format.
///
/// Uses a lock-free [`BuilderMap`] internally — the single writer (state
/// machine apply) never blocks concurrent readers.
pub struct SegmentIndexBuilder {
    segment_id: u32,
    map: BuilderMap,
}

impl SegmentIndexBuilder {
    pub fn new(segment_id: u32) -> Self {
        Self::with_capacity(segment_id, MIN_BUCKETS)
    }

    /// Create a builder with a hint for the expected number of distinct entities.
    pub fn with_capacity(segment_id: u32, entity_hint: usize) -> Self {
        // Size for ~70% load factor.
        let cap = ((entity_hint * LOAD_FACTOR_DEN / LOAD_FACTOR_NUM) + 1)
            .max(MIN_BUCKETS)
            .next_power_of_two();
        Self {
            segment_id,
            map: BuilderMap::new(cap),
        }
    }

    /// Append a record location for the given entity.  **Single-writer only.**
    #[inline]
    pub fn add_entry(&self, entity_type: u8, entity_id: u64, record_offset: u32, record_len: u32) {
        self.map.insert_or_append(
            entity_type,
            entity_id,
            RecordLoc {
                offset: record_offset,
                len: record_len,
            },
        );
    }

    /// Serialize to the on-disk v2 format.
    ///
    /// The returned `Vec<u8>` can be written directly to a `.sidx` file or
    /// wrapped in a [`FrozenSegmentIndex`].
    pub fn serialize(&self) -> Vec<u8> {
        // Collect all entities and their records.
        let mut entities: Vec<((u8, u64), Vec<RecordLoc>)> = self
            .map
            .iter()
            .map(|(packed, vec)| {
                let key = unpack_key(packed);
                let recs: Vec<RecordLoc> = vec.iter().collect();
                (key, recs)
            })
            .collect();

        // Sort entities by (entity_type, entity_id) for deterministic output.
        entities.sort_unstable_by_key(|&(key, _)| key);

        // Sort records within each entity by offset.
        for (_, recs) in &mut entities {
            recs.sort_unstable_by_key(|r| r.offset);
        }

        let distinct_entities = entities.len();
        let total_records: usize = entities.iter().map(|(_, r)| r.len()).sum();

        // Bucket count: next power of 2 ≥ ceil(distinct_entities / 0.7).
        let min_buckets = ((distinct_entities as f64 / 0.7).ceil() as usize).max(4);
        let bucket_count = min_buckets.next_power_of_two();

        let hash_table_size = bucket_count * SIDX_BUCKET_SIZE;
        let records_region_offset = SIDX_HEADER_SIZE + hash_table_size;
        let total_size = records_region_offset + total_records * SIDX_RECORD_SIZE;

        let mut buf = vec![0u8; total_size];

        // --- Header ---
        buf[0..4].copy_from_slice(&SIDX_MAGIC);
        buf[4..8].copy_from_slice(&SIDX_VERSION.to_le_bytes());
        buf[8..12].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[12..16].copy_from_slice(&(bucket_count as u32).to_le_bytes());

        // Mark all buckets empty.
        for i in 0..bucket_count {
            buf[SIDX_HEADER_SIZE + i * SIDX_BUCKET_SIZE] = BUCKET_EMPTY;
        }

        // --- Write records and hash table ---
        let mask = bucket_count - 1;
        let mut records_cursor = records_region_offset;

        for ((entity_type, entity_id), recs) in &entities {
            // Write records for this entity.
            let rec_offset = records_cursor;
            let rec_count = recs.len();
            for rec in recs {
                buf[records_cursor..records_cursor + 4].copy_from_slice(&rec.offset.to_le_bytes());
                buf[records_cursor + 4..records_cursor + 8].copy_from_slice(&rec.len.to_le_bytes());
                records_cursor += SIDX_RECORD_SIZE;
            }

            // Insert into hash table with linear probing.
            let hash = entity_hash(*entity_type, *entity_id);
            let mut bucket = hash & mask;
            loop {
                let b_off = SIDX_HEADER_SIZE + bucket * SIDX_BUCKET_SIZE;
                if buf[b_off] == BUCKET_EMPTY {
                    buf[b_off] = *entity_type;
                    // pad bytes already zero
                    buf[b_off + 4..b_off + 12].copy_from_slice(&entity_id.to_le_bytes());
                    buf[b_off + 12..b_off + 16].copy_from_slice(&(rec_offset as u32).to_le_bytes());
                    buf[b_off + 16..b_off + 20].copy_from_slice(&(rec_count as u32).to_le_bytes());
                    // bytes 20..24 are reserved padding (already zero)
                    break;
                }
                bucket = (bucket + 1) & mask;
            }
        }

        debug_assert_eq!(records_cursor, total_size);
        buf
    }

    /// Serialize and write to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let buf = self.serialize();
        writer.write_all(&buf)
    }

    /// Extract per-entity summaries: `(entity_type, entity_id, record_count, total_bytes)`.
    ///
    /// Used to populate the MDBX segment_ranges multimap table when a segment
    /// is sealed.
    pub fn entity_summaries(&self) -> Vec<(u8, u64, u64, u64)> {
        self.map
            .iter()
            .map(|(packed, vec)| {
                let (entity_type, entity_id) = unpack_key(packed);
                let records: Vec<RecordLoc> = vec.iter().collect();
                let record_count = records.len() as u64;
                let total_bytes: u64 = records.iter().map(|r| r.len as u64).sum();
                (entity_type, entity_id, record_count, total_bytes)
            })
            .collect()
    }
}

impl SegmentIndex for SegmentIndexBuilder {
    fn segment_id(&self) -> u32 {
        self.segment_id
    }

    fn is_empty(&self) -> bool {
        self.map.total_count() == 0
    }

    fn entry_count(&self) -> usize {
        self.map.total_count() as usize
    }

    fn find_entity(&self, entity_type: u8, entity_id: u64) -> Vec<RecordLoc> {
        match self.map.find(entity_type, entity_id) {
            Some(vec) => vec.iter().collect(),
            None => Vec::new(),
        }
    }
}

// ============================================================================
// SegmentIndexMap — concurrent segment_id → SegmentIndexBuilder map
// ============================================================================

/// Concurrent map of `segment_id → SegmentIndexBuilder`.
///
/// Lives outside the engine's `RwLock` so readers can query the tail segment's
/// index without blocking on applies.
///
/// The writer (state machine apply) calls [`add_entry`] and [`take_sealed`].
/// Readers call [`find_entity`] or [`get_builder`].
///
/// Internally uses a `Mutex` only for structural changes (insert new segment,
/// remove sealed segments).  Per-entity appends within a segment go through
/// the lock-free `BuilderMap` and never touch the mutex.
pub struct SegmentIndexMap {
    /// Current builders keyed by segment_id.
    ///
    /// The Mutex is only held briefly for:
    /// - Inserting a new segment (first write to a segment)
    /// - Draining sealed segments
    /// - Clearing on snapshot restore
    ///
    /// Record appends within a segment do NOT hold this mutex.
    segments: Mutex<HashMap<u32, Arc<SegmentIndexBuilder>>>,
}

impl SegmentIndexMap {
    pub fn new() -> Self {
        Self {
            segments: Mutex::new(HashMap::new()),
        }
    }

    /// Add a record to the segment index.  If the segment's builder doesn't
    /// exist yet, creates one.
    ///
    /// The mutex is only held if a new segment needs to be inserted.
    /// Appends to existing segments are lock-free.
    pub fn add_entry(
        &self,
        seg_id: u32,
        entity_type: u8,
        entity_id: u64,
        record_offset: u32,
        record_len: u32,
    ) {
        // Fast path: try to get existing builder without locking.
        // We clone the Arc so we can release the mutex before appending.
        let builder = {
            let segments = self.segments.lock();
            segments.get(&seg_id).cloned()
        };

        if let Some(b) = builder {
            b.add_entry(entity_type, entity_id, record_offset, record_len);
            return;
        }

        // Slow path: need to insert a new builder.
        let b = {
            let mut segments = self.segments.lock();
            let b = segments
                .entry(seg_id)
                .or_insert_with(|| Arc::new(SegmentIndexBuilder::new(seg_id)));
            Arc::clone(b)
        };
        b.add_entry(entity_type, entity_id, record_offset, record_len);
    }

    /// Track which entities a command touches in the per-segment index.
    ///
    /// Called from the state machine after `apply_command`.  The `record_location`
    /// is `(segment_id, record_byte_offset)` from the raft storage layer.
    pub fn track_command(&self, cmd: &MqCommand, record_location: (u32, u32)) {
        let (seg_id, rec_offset) = record_location;
        let cmd_len = cmd.as_bytes().len() as u32;

        match cmd.tag() {
            MqCommand::TAG_PUBLISH => {
                let topic_id = cmd.field_u64(8);
                self.add_entry(seg_id, ENTITY_TOPIC, topic_id, rec_offset, cmd_len);
            }
            MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
                let exchange_id = cmd.field_u64(8);
                self.add_entry(seg_id, ENTITY_TOPIC, exchange_id, rec_offset, cmd_len);
            }
            MqCommand::TAG_BATCH => {
                let batch = cmd.as_batch();
                for sub_cmd in batch.commands() {
                    match sub_cmd.tag() {
                        MqCommand::TAG_PUBLISH => {
                            let topic_id = sub_cmd.field_u64(8);
                            self.add_entry(seg_id, ENTITY_TOPIC, topic_id, rec_offset, cmd_len);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    /// Drain all segment builders for segments other than `current_seg_id`.
    ///
    /// Returns the sealed builders for writing to `.sidx` files.
    pub fn take_sealed(&self, current_seg_id: Option<u32>) -> Vec<(u32, Arc<SegmentIndexBuilder>)> {
        let mut segments = self.segments.lock();
        let keys_to_drain: Vec<u32> = segments
            .keys()
            .copied()
            .filter(|&seg_id| Some(seg_id) != current_seg_id)
            .collect();

        if keys_to_drain.is_empty() {
            return Vec::new();
        }

        let mut sealed = Vec::with_capacity(keys_to_drain.len());
        for seg_id in keys_to_drain {
            if let Some(idx) = segments.remove(&seg_id) {
                if !idx.is_empty() {
                    sealed.push((seg_id, idx));
                }
            }
        }
        sealed
    }

    /// Clear all builders (e.g. on snapshot restore).
    pub fn clear(&self) {
        let mut segments = self.segments.lock();
        segments.clear();
    }

    /// Get a builder for a specific segment (for readers).
    pub fn get_builder(&self, seg_id: u32) -> Option<Arc<SegmentIndexBuilder>> {
        let segments = self.segments.lock();
        segments.get(&seg_id).cloned()
    }

    /// Check if any builders exist.
    pub fn is_empty(&self) -> bool {
        let segments = self.segments.lock();
        segments.is_empty()
    }

    /// Number of active segment builders.
    pub fn len(&self) -> usize {
        let segments = self.segments.lock();
        segments.len()
    }
}

// ============================================================================
// FrozenSegmentIndex — zero-deserialisation, read-only
// ============================================================================

/// Read-only segment index backed by a flat byte buffer.
///
/// The buffer can be an mmap'd file or an owned `Vec<u8>` wrapped in `Bytes`.
/// Lookups are O(1) amortised via the embedded open-addressing hash table.
/// No deserialization — all reads are direct byte arithmetic.
pub struct FrozenSegmentIndex {
    data: Bytes,
}

impl FrozenSegmentIndex {
    /// Wrap existing bytes.  Returns `None` on invalid/truncated data.
    pub fn from_bytes(data: Bytes) -> Option<Self> {
        if data.len() < SIDX_HEADER_SIZE {
            return None;
        }
        if &data[0..4] != &SIDX_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != SIDX_VERSION {
            return None;
        }
        let bucket_count = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
        if !bucket_count.is_power_of_two() {
            return None;
        }
        let min_size = SIDX_HEADER_SIZE + bucket_count * SIDX_BUCKET_SIZE;
        if data.len() < min_size {
            return None;
        }
        Some(Self { data })
    }

    /// Wrap a `Vec<u8>` (e.g. read from disk).
    pub fn from_vec(data: Vec<u8>) -> Option<Self> {
        Self::from_bytes(Bytes::from(data))
    }

    #[inline]
    fn bucket_count(&self) -> usize {
        u32::from_le_bytes(self.data[12..16].try_into().unwrap()) as usize
    }

    /// Look up the records region for a given entity.
    /// Returns `(records_byte_offset, records_count)` or `None`.
    #[inline]
    fn lookup(&self, entity_type: u8, entity_id: u64) -> Option<(usize, usize)> {
        let bucket_count = self.bucket_count();
        let mask = bucket_count - 1;
        let hash = entity_hash(entity_type, entity_id);
        let mut bucket = hash & mask;

        loop {
            let b_off = SIDX_HEADER_SIZE + bucket * SIDX_BUCKET_SIZE;
            let b_type = self.data[b_off];

            if b_type == BUCKET_EMPTY {
                return None;
            }

            if b_type == entity_type {
                let b_id = u64::from_le_bytes(self.data[b_off + 4..b_off + 12].try_into().unwrap());
                if b_id == entity_id {
                    let rec_off =
                        u32::from_le_bytes(self.data[b_off + 12..b_off + 16].try_into().unwrap())
                            as usize;
                    let rec_count =
                        u32::from_le_bytes(self.data[b_off + 16..b_off + 20].try_into().unwrap())
                            as usize;
                    return Some((rec_off, rec_count));
                }
            }

            bucket = (bucket + 1) & mask;
        }
    }

    /// Total number of records across all entities.
    fn count_all_records(&self) -> usize {
        let bucket_count = self.bucket_count();
        let mut total = 0usize;
        for i in 0..bucket_count {
            let b_off = SIDX_HEADER_SIZE + i * SIDX_BUCKET_SIZE;
            if self.data[b_off] != BUCKET_EMPTY {
                let rec_count =
                    u32::from_le_bytes(self.data[b_off + 16..b_off + 20].try_into().unwrap())
                        as usize;
                total += rec_count;
            }
        }
        total
    }
}

impl SegmentIndex for FrozenSegmentIndex {
    fn segment_id(&self) -> u32 {
        u32::from_le_bytes(self.data[8..12].try_into().unwrap())
    }

    fn is_empty(&self) -> bool {
        let bucket_count = self.bucket_count();
        for i in 0..bucket_count {
            let b_off = SIDX_HEADER_SIZE + i * SIDX_BUCKET_SIZE;
            if self.data[b_off] != BUCKET_EMPTY {
                return false;
            }
        }
        true
    }

    fn entry_count(&self) -> usize {
        self.count_all_records()
    }

    fn find_entity(&self, entity_type: u8, entity_id: u64) -> Vec<RecordLoc> {
        let (rec_off, rec_count) = match self.lookup(entity_type, entity_id) {
            Some(v) => v,
            None => return Vec::new(),
        };

        let mut out = Vec::with_capacity(rec_count);
        for i in 0..rec_count {
            let r = rec_off + i * SIDX_RECORD_SIZE;
            let offset = u32::from_le_bytes(self.data[r..r + 4].try_into().unwrap());
            let len = u32::from_le_bytes(self.data[r + 4..r + 8].try_into().unwrap());
            out.push(RecordLoc { offset, len });
        }
        out
    }
}

// ---- Hash function ----

/// FNV-1a–inspired hash for `(entity_type, entity_id)`.
#[inline]
fn entity_hash(entity_type: u8, entity_id: u64) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    h ^= entity_type as u64;
    h = h.wrapping_mul(0x100000001b3);
    h ^= entity_id;
    h = h.wrapping_mul(0x100000001b3);
    h as usize
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- AppendVec tests --

    #[test]
    fn test_append_vec_basic() {
        let v = AppendVec::new();
        assert!(v.is_empty());

        v.push(RecordLoc {
            offset: 10,
            len: 20,
        });
        v.push(RecordLoc {
            offset: 30,
            len: 40,
        });
        assert_eq!(v.len(), 2);

        let items: Vec<RecordLoc> = v.iter().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0],
            RecordLoc {
                offset: 10,
                len: 20
            }
        );
        assert_eq!(
            items[1],
            RecordLoc {
                offset: 30,
                len: 40
            }
        );
    }

    #[test]
    fn test_append_vec_crosses_chunk_boundary() {
        let v = AppendVec::new();
        for i in 0..(CHUNK_SIZE as u32 + 10) {
            v.push(RecordLoc {
                offset: i * 100,
                len: 50,
            });
        }
        assert_eq!(v.len(), CHUNK_SIZE as u32 + 10);

        let items: Vec<RecordLoc> = v.iter().collect();
        assert_eq!(items.len(), CHUNK_SIZE + 10);
        assert_eq!(items[0].offset, 0);
        assert_eq!(items[CHUNK_SIZE].offset, CHUNK_SIZE as u32 * 100);
    }

    #[test]
    fn test_append_vec_exact_size_iter() {
        let v = AppendVec::new();
        for i in 0..5u32 {
            v.push(RecordLoc { offset: i, len: i });
        }
        let iter = v.iter();
        assert_eq!(iter.len(), 5);
    }

    // -- BuilderMap tests --

    #[test]
    fn test_builder_map_basic() {
        let m = BuilderMap::new(MIN_BUCKETS);
        assert_eq!(m.distinct_count(), 0);
        assert_eq!(m.total_count(), 0);

        m.insert_or_append(
            ENTITY_TOPIC,
            1,
            RecordLoc {
                offset: 100,
                len: 50,
            },
        );
        m.insert_or_append(
            ENTITY_TOPIC,
            1,
            RecordLoc {
                offset: 200,
                len: 60,
            },
        );
        assert_eq!(m.distinct_count(), 1);
        assert_eq!(m.total_count(), 2);

        let vec = m.find(ENTITY_TOPIC, 1).unwrap();
        let recs: Vec<RecordLoc> = vec.iter().collect();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].offset, 100);
        assert_eq!(recs[1].offset, 200);

        assert!(m.find(ENTITY_TOPIC, 999).is_none());
        assert!(m.find(ENTITY_QUEUE, 1).is_none());
    }

    #[test]
    fn test_builder_map_resize() {
        let m = BuilderMap::new(4); // start small to force resize
        for i in 0..20u64 {
            m.insert_or_append(
                ENTITY_TOPIC,
                i,
                RecordLoc {
                    offset: i as u32 * 100,
                    len: 50,
                },
            );
        }
        assert_eq!(m.distinct_count(), 20);
        assert_eq!(m.total_count(), 20);

        // All entries still findable after resize
        for i in 0..20u64 {
            let vec = m.find(ENTITY_TOPIC, i).unwrap();
            let recs: Vec<RecordLoc> = vec.iter().collect();
            assert_eq!(recs.len(), 1);
            assert_eq!(recs[0].offset, i as u32 * 100);
        }
    }

    #[test]
    fn test_builder_map_mixed_types() {
        let m = BuilderMap::new(MIN_BUCKETS);
        m.insert_or_append(
            ENTITY_TOPIC,
            1,
            RecordLoc {
                offset: 100,
                len: 50,
            },
        );
        m.insert_or_append(
            ENTITY_QUEUE,
            1,
            RecordLoc {
                offset: 200,
                len: 60,
            },
        );
        m.insert_or_append(
            ENTITY_ACTOR_NS,
            1,
            RecordLoc {
                offset: 300,
                len: 70,
            },
        );

        assert_eq!(m.distinct_count(), 3);

        assert_eq!(
            m.find(ENTITY_TOPIC, 1)
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .offset,
            100
        );
        assert_eq!(
            m.find(ENTITY_QUEUE, 1)
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .offset,
            200
        );
        assert_eq!(
            m.find(ENTITY_ACTOR_NS, 1)
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .offset,
            300
        );
    }

    #[test]
    fn test_builder_map_entity_id_zero() {
        let m = BuilderMap::new(MIN_BUCKETS);
        m.insert_or_append(
            ENTITY_TOPIC,
            0,
            RecordLoc {
                offset: 42,
                len: 10,
            },
        );
        let vec = m.find(ENTITY_TOPIC, 0).unwrap();
        assert_eq!(vec.iter().next().unwrap().offset, 42);
    }

    #[test]
    fn test_builder_map_iter() {
        let m = BuilderMap::new(MIN_BUCKETS);
        m.insert_or_append(
            ENTITY_TOPIC,
            1,
            RecordLoc {
                offset: 100,
                len: 50,
            },
        );
        m.insert_or_append(
            ENTITY_QUEUE,
            2,
            RecordLoc {
                offset: 200,
                len: 60,
            },
        );

        let entries: Vec<_> = m.iter().collect();
        assert_eq!(entries.len(), 2);
    }

    // -- Builder tests --

    #[test]
    fn test_builder_basic() {
        let b = SegmentIndexBuilder::new(42);
        assert!(b.is_empty());
        assert_eq!(b.segment_id(), 42);

        b.add_entry(ENTITY_TOPIC, 1, 100, 200);
        b.add_entry(ENTITY_TOPIC, 1, 300, 150);
        assert_eq!(b.entry_count(), 2);

        let recs = b.find_entity(ENTITY_TOPIC, 1);
        assert_eq!(recs.len(), 2);
        assert_eq!(
            recs[0],
            RecordLoc {
                offset: 100,
                len: 200
            }
        );
        assert_eq!(
            recs[1],
            RecordLoc {
                offset: 300,
                len: 150
            }
        );

        // Non-existent
        assert!(b.find_entity(ENTITY_TOPIC, 999).is_empty());
        assert!(b.find_entity(ENTITY_QUEUE, 1).is_empty());
    }

    #[test]
    fn test_builder_multiple_entities() {
        let b = SegmentIndexBuilder::new(1);
        b.add_entry(ENTITY_TOPIC, 1, 100, 50);
        b.add_entry(ENTITY_QUEUE, 2, 200, 60);
        b.add_entry(ENTITY_ACTOR_NS, 3, 300, 70);
        b.add_entry(ENTITY_TOPIC, 1, 400, 80);

        assert_eq!(b.entry_count(), 4);

        let t1 = b.find_entity(ENTITY_TOPIC, 1);
        assert_eq!(t1.len(), 2);

        let q2 = b.find_entity(ENTITY_QUEUE, 2);
        assert_eq!(q2.len(), 1);
        assert_eq!(q2[0].offset, 200);

        let a3 = b.find_entity(ENTITY_ACTOR_NS, 3);
        assert_eq!(a3.len(), 1);
        assert_eq!(a3[0].offset, 300);
    }

    // -- SegmentIndexMap tests --

    #[test]
    fn test_index_map_basic() {
        let m = SegmentIndexMap::new();
        assert!(m.is_empty());

        m.add_entry(1, ENTITY_TOPIC, 10, 100, 50);
        m.add_entry(1, ENTITY_TOPIC, 10, 200, 60);
        m.add_entry(2, ENTITY_QUEUE, 20, 300, 70);

        assert_eq!(m.len(), 2);

        let b1 = m.get_builder(1).unwrap();
        assert_eq!(b1.entry_count(), 2);

        let b2 = m.get_builder(2).unwrap();
        assert_eq!(b2.entry_count(), 1);
    }

    #[test]
    fn test_index_map_take_sealed() {
        let m = SegmentIndexMap::new();
        m.add_entry(1, ENTITY_TOPIC, 1, 100, 50);
        m.add_entry(2, ENTITY_TOPIC, 1, 200, 60);
        m.add_entry(3, ENTITY_TOPIC, 1, 300, 70);

        let sealed = m.take_sealed(Some(3));
        assert_eq!(sealed.len(), 2);
        let seg_ids: Vec<u32> = sealed.iter().map(|(id, _)| *id).collect();
        assert!(seg_ids.contains(&1));
        assert!(seg_ids.contains(&2));

        assert_eq!(m.len(), 1);
        assert!(m.get_builder(3).is_some());
    }

    #[test]
    fn test_index_map_clear() {
        let m = SegmentIndexMap::new();
        m.add_entry(1, ENTITY_TOPIC, 1, 100, 50);
        assert!(!m.is_empty());
        m.clear();
        assert!(m.is_empty());
    }

    // -- Frozen roundtrip tests --

    #[test]
    fn test_roundtrip_empty() {
        let b = SegmentIndexBuilder::new(42);
        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();

        assert_eq!(frozen.segment_id(), 42);
        assert!(frozen.is_empty());
        assert_eq!(frozen.entry_count(), 0);
    }

    #[test]
    fn test_roundtrip_single_entity() {
        let b = SegmentIndexBuilder::new(1);
        b.add_entry(ENTITY_TOPIC, 100, 64, 128);
        b.add_entry(ENTITY_TOPIC, 100, 256, 64);

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();

        assert_eq!(frozen.segment_id(), 1);
        assert_eq!(frozen.entry_count(), 2);

        let recs = frozen.find_entity(ENTITY_TOPIC, 100);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].offset, 64);
        assert_eq!(recs[1].offset, 256);

        // Non-existent entity
        assert!(frozen.find_entity(ENTITY_TOPIC, 999).is_empty());
        assert!(frozen.find_entity(ENTITY_QUEUE, 100).is_empty());
    }

    #[test]
    fn test_roundtrip_multiple_entities() {
        let b = SegmentIndexBuilder::new(7);
        b.add_entry(ENTITY_QUEUE, 5, 1000, 50);
        b.add_entry(ENTITY_TOPIC, 1, 100, 200);
        b.add_entry(ENTITY_TOPIC, 1, 400, 150);
        b.add_entry(ENTITY_TOPIC, 3, 600, 80);
        b.add_entry(ENTITY_ACTOR_NS, 10, 2000, 100);
        b.add_entry(ENTITY_QUEUE, 5, 1100, 60);

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(frozen.entry_count(), 6);

        // Topic 1
        let t1 = frozen.find_entity(ENTITY_TOPIC, 1);
        assert_eq!(t1.len(), 2);
        assert_eq!(t1[0].offset, 100);
        assert_eq!(t1[1].offset, 400);

        // Topic 3
        let t3 = frozen.find_entity(ENTITY_TOPIC, 3);
        assert_eq!(t3.len(), 1);
        assert_eq!(t3[0].offset, 600);

        // Queue 5
        let q5 = frozen.find_entity(ENTITY_QUEUE, 5);
        assert_eq!(q5.len(), 2);
        assert_eq!(q5[0].offset, 1000);
        assert_eq!(q5[1].offset, 1100);

        // Actor NS 10
        let a10 = frozen.find_entity(ENTITY_ACTOR_NS, 10);
        assert_eq!(a10.len(), 1);
    }

    #[test]
    fn test_roundtrip_sorted_records() {
        let b = SegmentIndexBuilder::new(1);
        for i in 0..5u32 {
            b.add_entry(ENTITY_TOPIC, 1, i * 100, 50);
        }

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();

        let recs = frozen.find_entity(ENTITY_TOPIC, 1);
        let offsets: Vec<u32> = recs.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![0, 100, 200, 300, 400]);
    }

    // -- Boundary value tests --

    #[test]
    fn test_segment_id_zero() {
        let b = SegmentIndexBuilder::new(0);
        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(frozen.segment_id(), 0);
    }

    #[test]
    fn test_segment_id_max() {
        let b = SegmentIndexBuilder::new(u32::MAX);
        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(frozen.segment_id(), u32::MAX);
    }

    #[test]
    fn test_max_values() {
        let b = SegmentIndexBuilder::new(1);
        let max_entity_id: u64 = 0x00FF_FFFF_FFFF_FFFF;
        b.add_entry(ENTITY_TOPIC, max_entity_id, u32::MAX, u32::MAX);

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();

        let recs = frozen.find_entity(ENTITY_TOPIC, max_entity_id);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].offset, u32::MAX);
        assert_eq!(recs[0].len, u32::MAX);
    }

    #[test]
    fn test_zero_offset_and_len() {
        let b = SegmentIndexBuilder::new(1);
        b.add_entry(ENTITY_QUEUE, 0, 0, 0);

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();

        let recs = frozen.find_entity(ENTITY_QUEUE, 0);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].offset, 0);
        assert_eq!(recs[0].len, 0);
    }

    // -- Large dataset tests --

    #[test]
    fn test_many_entities() {
        let b = SegmentIndexBuilder::new(1);
        for i in 0..200u64 {
            b.add_entry(ENTITY_TOPIC, i, (i as u32) * 100, 50);
        }

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(frozen.entry_count(), 200);

        let first = frozen.find_entity(ENTITY_TOPIC, 0);
        assert_eq!(first[0].offset, 0);

        let mid = frozen.find_entity(ENTITY_TOPIC, 100);
        assert_eq!(mid[0].offset, 10000);

        let last = frozen.find_entity(ENTITY_TOPIC, 199);
        assert_eq!(last[0].offset, 19900);

        assert!(frozen.find_entity(ENTITY_TOPIC, 200).is_empty());
    }

    #[test]
    fn test_many_entries_per_entity() {
        let b = SegmentIndexBuilder::new(1);
        let entry_count = 2000u32;

        for i in 0..entry_count {
            b.add_entry(ENTITY_TOPIC, 42, i * 64, 60);
        }

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(frozen.entry_count(), entry_count as usize);

        let recs = frozen.find_entity(ENTITY_TOPIC, 42);
        assert_eq!(recs.len(), entry_count as usize);
        assert_eq!(recs[0].offset, 0);
        assert_eq!(recs[1000].offset, 64000);
        assert_eq!(recs[1999].offset, 1999 * 64);
    }

    #[test]
    fn test_many_entities_many_entries() {
        let b = SegmentIndexBuilder::new(99);
        let num_entities = 50;
        let entries_per = 100;

        for eid in 0..num_entities {
            for j in 0..entries_per {
                let offset = (eid * entries_per + j) as u32 * 128;
                b.add_entry(ENTITY_TOPIC, eid as u64, offset, 120);
            }
        }

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(frozen.entry_count(), num_entities * entries_per);

        for eid in 0..num_entities {
            let recs = frozen.find_entity(ENTITY_TOPIC, eid as u64);
            assert_eq!(recs.len(), entries_per);
            let offsets: Vec<u32> = recs.iter().map(|r| r.offset).collect();
            for w in offsets.windows(2) {
                assert!(w[0] < w[1], "records must be sorted by offset");
            }
        }
    }

    // -- Mixed entity type tests --

    #[test]
    fn test_mixed_entity_types() {
        let b = SegmentIndexBuilder::new(1);
        b.add_entry(ENTITY_ACTOR_NS, 1, 300, 50);
        b.add_entry(ENTITY_TOPIC, 1, 100, 50);
        b.add_entry(ENTITY_QUEUE, 1, 200, 50);

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(frozen.entry_count(), 3);

        let t = frozen.find_entity(ENTITY_TOPIC, 1);
        assert_eq!(t[0].offset, 100);

        let q = frozen.find_entity(ENTITY_QUEUE, 1);
        assert_eq!(q[0].offset, 200);

        let a = frozen.find_entity(ENTITY_ACTOR_NS, 1);
        assert_eq!(a[0].offset, 300);
    }

    // -- File I/O tests --

    #[test]
    fn test_write_to_roundtrip() {
        let b = SegmentIndexBuilder::new(5);
        b.add_entry(ENTITY_TOPIC, 1, 0, 100);
        b.add_entry(ENTITY_QUEUE, 2, 200, 50);

        let mut file_buf = Vec::new();
        b.write_to(&mut file_buf).unwrap();

        let frozen = FrozenSegmentIndex::from_vec(file_buf).unwrap();
        assert_eq!(frozen.segment_id(), 5);
        assert_eq!(frozen.entry_count(), 2);
    }

    #[test]
    fn test_file_persistence_roundtrip() {
        let dir = std::env::temp_dir().join("bisque-sidx-test-v3");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_segment.sidx");

        let b = SegmentIndexBuilder::new(42);
        for i in 0..50u32 {
            b.add_entry(ENTITY_TOPIC, (i % 10) as u64, i * 256, 200);
        }
        for i in 0..30u32 {
            b.add_entry(ENTITY_QUEUE, (i % 5) as u64, 50000 + i * 128, 100);
        }

        {
            let mut file = std::fs::File::create(&path).unwrap();
            b.write_to(&mut file).unwrap();
        }

        let data = std::fs::read(&path).unwrap();
        let frozen = FrozenSegmentIndex::from_vec(data).unwrap();
        assert_eq!(frozen.segment_id(), 42);
        assert_eq!(frozen.entry_count(), 80);

        let t0 = frozen.find_entity(ENTITY_TOPIC, 0);
        assert_eq!(t0.len(), 5);
        let q3 = frozen.find_entity(ENTITY_QUEUE, 3);
        assert_eq!(q3.len(), 6);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    // -- Corruption / truncation tests --

    #[test]
    fn test_invalid_data() {
        assert!(FrozenSegmentIndex::from_vec(vec![]).is_none());
        assert!(FrozenSegmentIndex::from_vec(vec![0u8; 15]).is_none());
        assert!(FrozenSegmentIndex::from_vec(b"NOPE____________".to_vec()).is_none());

        // Wrong version
        let mut bad = vec![0u8; 16];
        bad[0..4].copy_from_slice(b"SIDX");
        bad[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert!(FrozenSegmentIndex::from_vec(bad).is_none());
    }

    #[test]
    fn test_truncated_hash_table() {
        let b = SegmentIndexBuilder::new(1);
        b.add_entry(ENTITY_TOPIC, 1, 0, 10);
        let buf = b.serialize();

        // Truncate in the middle of the hash table
        let truncated = buf[..SIDX_HEADER_SIZE + 10].to_vec();
        assert!(FrozenSegmentIndex::from_vec(truncated).is_none());
    }

    #[test]
    fn test_non_power_of_two_bucket_count() {
        let mut bad = vec![0u8; 256];
        bad[0..4].copy_from_slice(b"SIDX");
        bad[4..8].copy_from_slice(&SIDX_VERSION.to_le_bytes());
        bad[8..12].copy_from_slice(&1u32.to_le_bytes());
        bad[12..16].copy_from_slice(&5u32.to_le_bytes()); // 5 is not power of 2
        assert!(FrozenSegmentIndex::from_vec(bad).is_none());
    }

    // -- Serialize is deterministic --

    #[test]
    fn test_serialize_deterministic() {
        let b = SegmentIndexBuilder::new(1);
        b.add_entry(ENTITY_TOPIC, 1, 0, 100);
        b.add_entry(ENTITY_QUEUE, 2, 200, 50);

        let buf1 = b.serialize();
        let buf2 = b.serialize();
        assert_eq!(buf1, buf2);
    }

    // -- Trait consistency --

    #[test]
    fn test_builder_and_frozen_agree() {
        let b = SegmentIndexBuilder::new(7);
        b.add_entry(ENTITY_TOPIC, 1, 100, 50);
        b.add_entry(ENTITY_TOPIC, 1, 200, 60);
        b.add_entry(ENTITY_QUEUE, 5, 300, 70);
        b.add_entry(ENTITY_ACTOR_NS, 10, 400, 80);

        let buf = b.serialize();
        let frozen = FrozenSegmentIndex::from_vec(buf).unwrap();

        // Both should return the same results via the trait
        fn check(idx: &dyn SegmentIndex) {
            assert_eq!(idx.segment_id(), 7);
            assert!(!idx.is_empty());
            assert_eq!(idx.entry_count(), 4);

            let t1 = idx.find_entity(ENTITY_TOPIC, 1);
            assert_eq!(t1.len(), 2);
            assert_eq!(t1[0].offset, 100);
            assert_eq!(t1[1].offset, 200);

            let q5 = idx.find_entity(ENTITY_QUEUE, 5);
            assert_eq!(q5.len(), 1);
            assert_eq!(q5[0].offset, 300);

            let a10 = idx.find_entity(ENTITY_ACTOR_NS, 10);
            assert_eq!(a10.len(), 1);
            assert_eq!(a10[0].offset, 400);

            assert!(idx.find_entity(ENTITY_TOPIC, 999).is_empty());
        }

        check(&b);
        check(&frozen);
    }

    // -- RecordLoc --

    #[test]
    fn test_record_loc_eq() {
        let a = RecordLoc {
            offset: 100,
            len: 200,
        };
        let b = RecordLoc {
            offset: 100,
            len: 200,
        };
        let c = RecordLoc {
            offset: 100,
            len: 201,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_record_loc_debug() {
        let r = RecordLoc {
            offset: 42,
            len: 99,
        };
        let dbg = format!("{:?}", r);
        assert!(dbg.contains("42"));
        assert!(dbg.contains("99"));
    }

    // -- pack/unpack key --

    #[test]
    fn test_pack_unpack_roundtrip() {
        for &et in &[ENTITY_TOPIC, ENTITY_QUEUE, ENTITY_ACTOR_NS] {
            for &eid in &[0u64, 1, 42, u64::MAX >> 8] {
                let packed = pack_key(et, eid);
                assert_ne!(packed, KEY_EMPTY);
                let (rt, rid) = unpack_key(packed);
                assert_eq!(rt, et);
                assert_eq!(rid, eid);
            }
        }
    }

    // ========================================================================
    // Concurrency tests
    // ========================================================================

    use std::sync::{Arc, Barrier};
    use std::thread;

    /// Single writer pushes, multiple readers concurrently iterate.
    /// Readers must always see a consistent prefix (no torn reads, no gaps).
    #[test]
    fn test_append_vec_concurrent_readers() {
        let vec = Arc::new(AppendVec::new());
        let count = 1024u32;
        let num_readers = 4;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let vec = Arc::clone(&vec);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut max_seen = 0u32;
                    // Keep reading until we've seen all entries.
                    loop {
                        let snapshot: Vec<RecordLoc> = vec.iter().collect();
                        let n = snapshot.len() as u32;
                        assert!(
                            n >= max_seen,
                            "length must be monotonically increasing: saw {n} after {max_seen}"
                        );
                        max_seen = n;

                        // Verify prefix is a contiguous sequence 0,1,2,...
                        for (i, rec) in snapshot.iter().enumerate() {
                            assert_eq!(
                                rec.offset, i as u32,
                                "entry {i} has wrong offset {}, expected {i}",
                                rec.offset
                            );
                            assert_eq!(rec.len, i as u32 + 1000);
                        }

                        if max_seen >= count {
                            break;
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        // Writer
        barrier.wait();
        for i in 0..count {
            vec.push(RecordLoc {
                offset: i,
                len: i + 1000,
            });
        }

        for h in readers {
            h.join().unwrap();
        }
    }

    /// Single writer inserts many entities; concurrent readers look them up.
    /// Readers must never see a non-empty key with a null/corrupt AppendVec.
    #[test]
    fn test_builder_map_concurrent_find() {
        let map = Arc::new(BuilderMap::new(MIN_BUCKETS));
        let num_entities = 200u64;
        let records_per = 20u32;
        let num_readers = 4;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let map = Arc::clone(&map);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    loop {
                        let total = map.total_count();
                        // Probe random entities
                        for eid in 0..num_entities {
                            if let Some(vec) = map.find(ENTITY_TOPIC, eid) {
                                let entries: Vec<RecordLoc> = vec.iter().collect();
                                let n = entries.len();
                                assert!(n > 0, "found entity but AppendVec empty");
                                assert!(
                                    n <= records_per as usize,
                                    "too many records: {n} > {records_per}"
                                );
                                // Verify entries are contiguous values for this entity.
                                for (j, rec) in entries.iter().enumerate() {
                                    assert_eq!(
                                        rec.offset,
                                        eid as u32 * 10000 + j as u32,
                                        "corrupt record for entity {eid} at position {j}"
                                    );
                                }
                            }
                        }
                        if total >= num_entities as u32 * records_per {
                            // Final verification: all entities must be findable.
                            for eid in 0..num_entities {
                                let vec = map
                                    .find(ENTITY_TOPIC, eid)
                                    .unwrap_or_else(|| panic!("entity {eid} missing at end"));
                                assert_eq!(vec.len(), records_per);
                            }
                            break;
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        // Writer
        barrier.wait();
        for round in 0..records_per {
            for eid in 0..num_entities {
                map.insert_or_append(
                    ENTITY_TOPIC,
                    eid,
                    RecordLoc {
                        offset: eid as u32 * 10000 + round,
                        len: 100,
                    },
                );
            }
        }

        for h in readers {
            h.join().unwrap();
        }
    }

    /// BuilderMap iter() during concurrent inserts must never yield corrupt data.
    #[test]
    fn test_builder_map_concurrent_iter() {
        let map = Arc::new(BuilderMap::new(MIN_BUCKETS));
        let num_entities = 100u64;
        let num_readers = 4;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let map = Arc::clone(&map);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    loop {
                        let distinct = map.distinct_count();
                        // Iterate all entries — must be valid.
                        let entries: Vec<_> = map.iter().collect();
                        for (packed_key, vec) in &entries {
                            assert_ne!(*packed_key, KEY_EMPTY, "iter yielded empty key");
                            let (et, _eid) = unpack_key(*packed_key);
                            assert_eq!(et, ENTITY_TOPIC, "unexpected entity type");
                            let n = vec.len();
                            assert!(n > 0, "occupied bucket has empty AppendVec");
                        }
                        if distinct >= num_entities as u32 {
                            break;
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        // Writer
        barrier.wait();
        for eid in 0..num_entities {
            map.insert_or_append(
                ENTITY_TOPIC,
                eid,
                RecordLoc {
                    offset: eid as u32,
                    len: 50,
                },
            );
        }

        for h in readers {
            h.join().unwrap();
        }
    }

    /// BuilderMap resize under concurrent readers.
    /// Start with MIN_BUCKETS (16) and insert enough entities to force
    /// multiple resizes.  Readers must never see corrupt state.
    #[test]
    fn test_builder_map_resize_under_readers() {
        let map = Arc::new(BuilderMap::new(MIN_BUCKETS));
        // 16 buckets * 70% = 11 before first resize, so 100 entities → many resizes.
        let num_entities = 100u64;
        let num_readers = 6;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let map = Arc::clone(&map);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut found_at_end = false;
                    loop {
                        let distinct = map.distinct_count();
                        // Random lookups
                        for eid in 0..num_entities {
                            if let Some(vec) = map.find(ENTITY_TOPIC, eid) {
                                let n = vec.len();
                                assert!(n >= 1 && n <= 3);
                            }
                        }
                        // Iter scan
                        for (packed_key, vec) in map.iter() {
                            assert_ne!(packed_key, KEY_EMPTY);
                            assert!(vec.len() > 0);
                        }
                        if distinct >= num_entities as u32 && !found_at_end {
                            found_at_end = true;
                            // Verify all entities present.
                            for eid in 0..num_entities {
                                assert!(
                                    map.find(ENTITY_TOPIC, eid).is_some(),
                                    "entity {eid} missing after all inserts"
                                );
                            }
                            break;
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        // Writer: insert entities, then append more records to cause
        // interleaved new-entity inserts and existing-entity appends.
        barrier.wait();
        for eid in 0..num_entities {
            map.insert_or_append(
                ENTITY_TOPIC,
                eid,
                RecordLoc {
                    offset: eid as u32,
                    len: 10,
                },
            );
        }
        // Second pass: append to existing entities.
        for eid in 0..num_entities {
            map.insert_or_append(
                ENTITY_TOPIC,
                eid,
                RecordLoc {
                    offset: eid as u32 + 10000,
                    len: 10,
                },
            );
        }
        // Third pass.
        for eid in 0..num_entities {
            map.insert_or_append(
                ENTITY_TOPIC,
                eid,
                RecordLoc {
                    offset: eid as u32 + 20000,
                    len: 10,
                },
            );
        }

        for h in readers {
            h.join().unwrap();
        }

        // Final verification.
        assert_eq!(map.distinct_count(), num_entities as u32);
        assert_eq!(map.total_count(), num_entities as u32 * 3);
        for eid in 0..num_entities {
            let vec = map.find(ENTITY_TOPIC, eid).unwrap();
            assert_eq!(vec.len(), 3);
        }
    }

    /// SegmentIndexBuilder: single writer, concurrent readers via the trait.
    #[test]
    fn test_builder_concurrent_find_entity() {
        let builder = Arc::new(SegmentIndexBuilder::new(1));
        let num_topics = 50u64;
        let num_queues = 50u64;
        let records_per = 40u32;
        let num_readers = 4;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let builder = Arc::clone(&builder);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    loop {
                        let total = builder.entry_count();
                        // Spot-check entities via the trait method.
                        for tid in 0..num_topics {
                            let recs = builder.find_entity(ENTITY_TOPIC, tid);
                            let n = recs.len();
                            assert!(
                                n <= records_per as usize,
                                "topic {tid}: too many records {n}"
                            );
                            // Records must have correct offsets.
                            for (j, rec) in recs.iter().enumerate() {
                                assert_eq!(
                                    rec.offset,
                                    tid as u32 * 1000 + j as u32,
                                    "topic {tid} record {j} corrupt"
                                );
                            }
                        }
                        for qid in 0..num_queues {
                            let recs = builder.find_entity(ENTITY_QUEUE, qid);
                            for (j, rec) in recs.iter().enumerate() {
                                assert_eq!(
                                    rec.offset,
                                    qid as u32 * 2000 + j as u32,
                                    "queue {qid} record {j} corrupt"
                                );
                            }
                        }
                        if total >= (num_topics + num_queues) as usize * records_per as usize {
                            break;
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        // Writer
        barrier.wait();
        for round in 0..records_per {
            for tid in 0..num_topics {
                builder.add_entry(ENTITY_TOPIC, tid, tid as u32 * 1000 + round, 64);
            }
            for qid in 0..num_queues {
                builder.add_entry(ENTITY_QUEUE, qid, qid as u32 * 2000 + round, 64);
            }
        }

        for h in readers {
            h.join().unwrap();
        }

        // Verify final state.
        assert_eq!(
            builder.entry_count(),
            (num_topics + num_queues) as usize * records_per as usize
        );
    }

    /// SegmentIndexMap: concurrent track_command (writer) + get_builder/find_entity (readers).
    #[test]
    fn test_segment_index_map_concurrent() {
        let sidx = Arc::new(SegmentIndexMap::new());
        let num_segments = 4u32;
        let records_per_segment = 200u32;
        let num_readers = 4;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let sidx = Arc::clone(&sidx);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    loop {
                        let total_len = sidx.len();
                        // Read from any segment that exists.
                        for seg in 1..=num_segments {
                            if let Some(builder) = sidx.get_builder(seg) {
                                let count = builder.entry_count();
                                assert!(count <= records_per_segment as usize);
                                // Spot-check: records for topic 1 in this segment.
                                let recs = builder.find_entity(ENTITY_TOPIC, 1);
                                for rec in &recs {
                                    // Offset encodes segment, so must match.
                                    assert_eq!(rec.offset / 10000, seg, "record in wrong segment");
                                }
                            }
                        }
                        // Check total records across all segments.
                        let mut total_records = 0usize;
                        for seg in 1..=num_segments {
                            if let Some(b) = sidx.get_builder(seg) {
                                total_records += b.entry_count();
                            }
                        }
                        if total_records >= num_segments as usize * records_per_segment as usize {
                            break;
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        // Writer: add entries across multiple segments.
        barrier.wait();
        for round in 0..records_per_segment {
            for seg in 1..=num_segments {
                sidx.add_entry(seg, ENTITY_TOPIC, 1, seg * 10000 + round, 64);
            }
        }

        for h in readers {
            h.join().unwrap();
        }

        // Final: each segment should have exactly records_per_segment entries.
        for seg in 1..=num_segments {
            let b = sidx.get_builder(seg).unwrap();
            let recs = b.find_entity(ENTITY_TOPIC, 1);
            assert_eq!(recs.len(), records_per_segment as usize);
        }
    }

    /// SegmentIndexMap: concurrent take_sealed while readers hold Arc refs.
    /// Readers that obtained a builder Arc before take_sealed must still be
    /// able to read from it without corruption.
    #[test]
    fn test_segment_index_map_take_sealed_concurrent() {
        let sidx = Arc::new(SegmentIndexMap::new());

        // Populate 3 segments.
        for seg in 1..=3u32 {
            for i in 0..100u32 {
                sidx.add_entry(seg, ENTITY_TOPIC, 1, seg * 10000 + i, 64);
            }
        }

        let num_readers = 4;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        // Readers grab builder Arcs before seal.
        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let sidx = Arc::clone(&sidx);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    // Grab refs to all 3 builders before the writer seals them.
                    let builders: Vec<_> = (1..=3u32)
                        .filter_map(|seg| sidx.get_builder(seg).map(|b| (seg, b)))
                        .collect();
                    barrier.wait();

                    // Even after take_sealed removes them from the map,
                    // our Arcs keep them alive.
                    for (seg, builder) in &builders {
                        let recs = builder.find_entity(ENTITY_TOPIC, 1);
                        assert_eq!(
                            recs.len(),
                            100,
                            "segment {seg} should still have 100 records"
                        );
                        for (i, rec) in recs.iter().enumerate() {
                            assert_eq!(rec.offset, seg * 10000 + i as u32);
                        }
                    }
                })
            })
            .collect();

        barrier.wait();
        // Seal segments 1 and 2 (current=3).
        let sealed = sidx.take_sealed(Some(3));
        assert_eq!(sealed.len(), 2);

        for h in readers {
            h.join().unwrap();
        }

        // Segment 3 is still in the map.
        assert!(sidx.get_builder(3).is_some());
        assert!(sidx.get_builder(1).is_none());
        assert!(sidx.get_builder(2).is_none());
    }

    /// Stress test: heavy concurrent reads during continuous writes that
    /// trigger multiple BuilderMap resizes. Verifies no data loss or corruption.
    #[test]
    fn test_builder_map_stress() {
        let map = Arc::new(BuilderMap::new(MIN_BUCKETS));
        let num_entities = 500u64;
        let num_readers = 8;
        let barrier = Arc::new(Barrier::new(num_readers + 1));

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let map = Arc::clone(&map);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut iterations = 0u64;
                    loop {
                        let distinct = map.distinct_count();
                        // Mixed: find + iter
                        if iterations % 3 == 0 {
                            // Iterate all — every yielded entry must be valid.
                            for (key, vec) in map.iter() {
                                assert_ne!(key, KEY_EMPTY);
                                let n = vec.len();
                                assert!(n >= 1);
                            }
                        } else {
                            // Point lookups
                            let eid = iterations % num_entities;
                            if let Some(vec) = map.find(ENTITY_TOPIC, eid) {
                                assert!(vec.len() >= 1);
                            }
                        }
                        iterations += 1;
                        if distinct >= num_entities as u32 {
                            break;
                        }
                        thread::yield_now();
                    }
                })
            })
            .collect();

        barrier.wait();
        for eid in 0..num_entities {
            map.insert_or_append(
                ENTITY_TOPIC,
                eid,
                RecordLoc {
                    offset: eid as u32,
                    len: 64,
                },
            );
        }

        for h in readers {
            h.join().unwrap();
        }

        assert_eq!(map.distinct_count(), num_entities as u32);
        for eid in 0..num_entities {
            assert!(map.find(ENTITY_TOPIC, eid).is_some());
        }
    }

    /// Verify that serialize() during concurrent writes produces a valid
    /// frozen index (no panics, no corrupt data). The frozen index may contain
    /// a subset of the written records since serialize snapshots atomically.
    #[test]
    fn test_builder_serialize_during_writes() {
        let builder = Arc::new(SegmentIndexBuilder::new(1));
        let num_entities = 50u64;
        let records_per = 100u32;
        let barrier = Arc::new(Barrier::new(2));

        let writer_builder = Arc::clone(&builder);
        let writer_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            writer_barrier.wait();
            for round in 0..records_per {
                for eid in 0..num_entities {
                    writer_builder.add_entry(ENTITY_TOPIC, eid, eid as u32 * 1000 + round, 64);
                }
            }
        });

        barrier.wait();
        // Serialize multiple times while the writer is active.
        let mut snapshots = Vec::new();
        for _ in 0..10 {
            let buf = builder.serialize();
            snapshots.push(buf);
            thread::yield_now();
        }

        writer.join().unwrap();

        // Each snapshot must be a valid frozen index.
        for (i, buf) in snapshots.iter().enumerate() {
            let frozen = FrozenSegmentIndex::from_vec(buf.clone())
                .unwrap_or_else(|| panic!("snapshot {i} produced invalid frozen index"));
            assert_eq!(frozen.segment_id(), 1);
            // Each entity's records must have valid offsets.
            for eid in 0..num_entities {
                let recs = frozen.find_entity(ENTITY_TOPIC, eid);
                for rec in &recs {
                    assert_eq!(
                        rec.offset / 1000,
                        eid as u32,
                        "snapshot {i}: record for entity {eid} has wrong offset {}",
                        rec.offset
                    );
                }
            }
        }

        // Final serialize must have all data.
        let final_buf = builder.serialize();
        let frozen = FrozenSegmentIndex::from_vec(final_buf).unwrap();
        assert_eq!(
            frozen.entry_count(),
            num_entities as usize * records_per as usize
        );
    }
}
