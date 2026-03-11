//! Shared binary record format for raft log storage.
//!
//! Defines the on-disk record layout, CRC64 checksums, record types,
//! and shared primitives (atomic vote/log-id, congee-based log index)
//! used by the mmap storage backend.
//!
//! ## Log Record Format
//!
//! ```text
//! +----------+----------+----------+-------------+----------+
//! | len (4B) | type (1B)| group_id | payload     | crc64    |
//! |  u32 LE  |   u8     | u24 LE   | (variable)  | (8B) LE  |
//! +----------+----------+----------+-------------+----------+
//! ```

use congee::U64Congee;
use congee::epoch;
use crc64fast_nvme::Digest;
use openraft::{LogId, RaftTypeConfig};
use parking_lot::RwLock;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Record type flags (per-segment metadata for fast-path recovery)
// ---------------------------------------------------------------------------

/// Tracks which record types are present in a segment.
/// Used for fast-path recovery: entry-only segments can skip full CRC decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecordTypeFlags {
    pub has_vote: bool,
    pub has_entry: bool,
    pub has_truncate: bool,
    pub has_purge: bool,
}

impl RecordTypeFlags {
    #[inline]
    pub fn has_only_entries(&self) -> bool {
        self.has_entry && !self.has_vote && !self.has_truncate && !self.has_purge
    }

    pub fn to_u8(&self) -> u8 {
        let mut flags = 0u8;
        if self.has_vote {
            flags |= 1 << 0;
        }
        if self.has_entry {
            flags |= 1 << 1;
        }
        if self.has_truncate {
            flags |= 1 << 2;
        }
        if self.has_purge {
            flags |= 1 << 3;
        }
        flags
    }

    pub fn from_u8(flags: u8) -> Self {
        Self {
            has_vote: (flags & (1 << 0)) != 0,
            has_entry: (flags & (1 << 1)) != 0,
            has_truncate: (flags & (1 << 2)) != 0,
            has_purge: (flags & (1 << 3)) != 0,
        }
    }

    /// Merge two flags — union of all record types.
    pub fn merge(self, other: Self) -> Self {
        Self {
            has_vote: self.has_vote || other.has_vote,
            has_entry: self.has_entry || other.has_entry,
            has_truncate: self.has_truncate || other.has_truncate,
            has_purge: self.has_purge || other.has_purge,
        }
    }
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Record format constants
// ---------------------------------------------------------------------------

/// Length field size: 4 bytes
pub const LENGTH_SIZE: usize = 4;
/// CRC64 size: 8 bytes
pub const CRC64_SIZE: usize = 8;
/// Group ID size: 3 bytes (u24 little-endian)
pub const GROUP_ID_SIZE: usize = 3;
/// Minimum record size: len(4) + type(1) + group_id(3) + crc(8) = 16 bytes
pub const MIN_RECORD_SIZE: usize = LENGTH_SIZE + 1 + GROUP_ID_SIZE + CRC64_SIZE;
/// Header size: len(4) + type(1) + group_id(3) = 8 bytes (64-bit aligned)
pub const HEADER_SIZE: usize = LENGTH_SIZE + 1 + GROUP_ID_SIZE;

/// Maximum number of raft groups supported (4K)
pub const MAX_GROUPS: usize = 4096;

// ---------------------------------------------------------------------------
// u24 helpers
// ---------------------------------------------------------------------------

/// Write a u24 little-endian value to the buffer at the given offset.
#[inline]
pub fn write_u24_le(buf: &mut [u8], offset: usize, value: u64) {
    debug_assert!(value <= 0xFF_FFFF, "group_id exceeds u24 range");
    buf[offset] = value as u8;
    buf[offset + 1] = (value >> 8) as u8;
    buf[offset + 2] = (value >> 16) as u8;
}

/// Read a u24 little-endian value from the buffer at the given offset.
#[inline]
pub fn read_u24_le(buf: &[u8], offset: usize) -> u64 {
    let lo = buf[offset] as u64;
    let mid = (buf[offset + 1] as u64) << 8;
    let high = (buf[offset + 2] as u64) << 16;
    lo | mid | high
}

// ---------------------------------------------------------------------------
// Record encoding / validation
// ---------------------------------------------------------------------------

/// Encodes a log record with CRC64 checksum into the provided buffer (clears first).
/// Returns the total number of bytes written.
#[cfg(test)]
pub fn encode_record_into(
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
pub fn append_record_into(
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

/// A parsed record from the log
#[derive(Debug)]
pub struct ParsedRecord<'a> {
    pub record_type: RecordType,
    pub group_id: u64,
    pub payload: &'a [u8],
}

/// Validates a record's CRC64 checksum.
/// Input is the data AFTER the length field (type + group_id + payload + crc).
/// Returns (record_type, group_id, payload) if valid.
pub fn validate_record(data: &[u8], max_record_size: usize) -> io::Result<ParsedRecord<'_>> {
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

// ---------------------------------------------------------------------------
// Log index (congee-based)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct LogLocation {
    pub segment_id: u64,
    pub offset: u64,
    pub len: u32,
}

pub struct LogIndex {
    map: U64Congee<usize>,
    locations: RwLock<Vec<LogLocation>>,
    free: RwLock<Vec<usize>>,
}

impl LogIndex {
    pub fn new() -> Self {
        Self {
            map: U64Congee::<usize>::new(),
            locations: RwLock::new(Vec::new()),
            free: RwLock::new(Vec::new()),
        }
    }

    pub fn get(&self, key: u64) -> Option<LogLocation> {
        let guard = epoch::pin();
        let slot = self.map.get(key, &guard)?;
        let locs = self.locations.read();
        locs.get(slot).copied()
    }

    pub fn insert(&self, key: u64, loc: LogLocation) -> io::Result<()> {
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

    pub fn truncate_from(&self, first_removed: u64) {
        // Remove keys in [first_removed, u64::MAX] in batches.
        // Collect freed slots locally, then push all at once to reduce lock acquisitions.
        let guard = epoch::pin();
        let mut buf: Vec<([u8; 8], usize)> = vec![([0u8; 8], 0usize); 256];
        let mut freed_slots = Vec::new();

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
                    freed_slots.push(slot);
                }
            }
        }

        if !freed_slots.is_empty() {
            self.free.write().extend(freed_slots);
        }
    }

    pub fn purge_to(&self, last_removed: u64) {
        // Remove keys in [0, last_removed] in batches.
        // Collect freed slots locally, then push all at once to reduce lock acquisitions.
        let guard = epoch::pin();
        let mut buf: Vec<([u8; 8], usize)> = vec![([0u8; 8], 0usize); 256];
        let mut freed_slots = Vec::new();

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
                    freed_slots.push(slot);
                }
            }
        }

        if !freed_slots.is_empty() {
            self.free.write().extend(freed_slots);
        }
    }
}

// ---------------------------------------------------------------------------
// Atomic Vote (SeqLock)
// ---------------------------------------------------------------------------

/// Atomic storage for Vote.
///
/// Uses a SeqLock-style approach: a sequence number protects the two u64 values.
/// Writers increment the sequence before and after writing (odd = write in progress).
/// Readers retry if they see an odd sequence or if the sequence changed during read.
/// This provides lock-free reads with consistent snapshots.
pub struct AtomicVote {
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

    pub fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            high: AtomicU64::new(0),
            low: AtomicU64::new(0),
        }
    }

    pub fn store<C>(&self, vote: Option<&openraft::impls::Vote<C>>)
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

    pub fn load<C>(&self) -> Option<openraft::impls::Vote<C>>
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

// ---------------------------------------------------------------------------
// Atomic LogId (SeqLock)
// ---------------------------------------------------------------------------

/// Atomic storage for LogId.
///
/// Uses a SeqLock-style approach: a sequence number protects the two u64 values.
/// Writers increment the sequence before and after writing (odd = write in progress).
/// Readers retry if they see an odd sequence or if the sequence changed during read.
/// This provides lock-free reads with consistent snapshots.
pub struct AtomicLogId {
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

    pub fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            high: AtomicU64::new(0),
            low: AtomicU64::new(0),
        }
    }

    pub fn store<C>(&self, log_id: Option<&LogId<C>>)
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

    pub fn load<C>(&self) -> Option<LogId<C>>
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
