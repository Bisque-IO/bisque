//! Segment Footer for Fast Recovery
//!
//! Footer structure written at the end of each sealed segment containing metadata
//! about record types, entry ranges, and record counts.
//!
//! ## Footer Format (at end of segment file)
//!
//! ```text
//! +------------------+------------------+------------------+
//! | Footer Header    | Footer Magic     | Footer Length    |
//! | (64 bytes)       | (4 bytes)        | (4 bytes)        |
//! +------------------+------------------+------------------+
//! ```
//!
//! ## Recovery Process
//!
//! 1. Seek to file_len - 8 bytes
//! 2. Read footer_length (4 bytes) and magic (4 bytes)
//! 3. If magic matches, read and validate footer header
//! 4. Use metadata to optimize recovery (skip segments with only entries)
//!
//! If magic doesn't match or validation fails, fall back to full scan.

use crc64fast_nvme::Digest;
use std::io;

pub const FOOTER_MAGIC: u32 = 0x54464152; // "RAFT" in little-endian
pub const FOOTER_VERSION: u8 = 1;
pub const FOOTER_HEADER_SIZE: usize = 64;
pub const FOOTER_TRAILER_SIZE: usize = 8;

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

    #[inline]
    pub fn has_raft_metadata(&self) -> bool {
        self.has_vote || self.has_truncate || self.has_purge
    }

    pub(crate) fn to_u8(&self) -> u8 {
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

    pub(crate) fn from_u8(flags: u8) -> Self {
        Self {
            has_vote: (flags & (1 << 0)) != 0,
            has_entry: (flags & (1 << 1)) != 0,
            has_truncate: (flags & (1 << 2)) != 0,
            has_purge: (flags & (1 << 3)) != 0,
        }
    }

    /// Merge two flags — union of all record types.
    pub(crate) fn merge(self, other: Self) -> Self {
        Self {
            has_vote: self.has_vote || other.has_vote,
            has_entry: self.has_entry || other.has_entry,
            has_truncate: self.has_truncate || other.has_truncate,
            has_purge: self.has_purge || other.has_purge,
        }
    }
}

/// Layout (64 bytes):
/// [0]     version (u8)
/// [1]     record_type_flags (u8)
/// [2]     has_min_index (u8)
/// [3]     has_max_index (u8)
/// [4-7]   reserved
/// [8-15]  group_id (u64 LE)
/// [16-23] segment_id (u64 LE)
/// [24-31] valid_bytes (u64 LE)
/// [32-39] min_entry_index (u64 LE)
/// [40-47] max_entry_index (u64 LE)
/// [48-55] record_count (u64 LE)
/// [56-63] header_checksum (u64 LE)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFooter {
    pub version: u8,
    pub record_type_flags: RecordTypeFlags,
    pub group_id: u64,
    pub segment_id: u64,
    pub valid_bytes: u64,
    pub min_entry_index: Option<u64>,
    pub max_entry_index: Option<u64>,
    pub record_count: u64,
}

impl SegmentFooter {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE];

        buf[0] = self.version;
        buf[1] = self.record_type_flags.to_u8();
        buf[2] = if self.min_entry_index.is_some() { 1 } else { 0 };
        buf[3] = if self.max_entry_index.is_some() { 1 } else { 0 };

        buf[8..16].copy_from_slice(&self.group_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.valid_bytes.to_le_bytes());
        buf[32..40].copy_from_slice(&self.min_entry_index.unwrap_or(0).to_le_bytes());
        buf[40..48].copy_from_slice(&self.max_entry_index.unwrap_or(0).to_le_bytes());
        buf[48..56].copy_from_slice(&self.record_count.to_le_bytes());

        let mut digest = Digest::new();
        digest.write(&buf[0..56]);
        let checksum = digest.sum64();
        buf[56..64].copy_from_slice(&checksum.to_le_bytes());

        buf[64..68].copy_from_slice(&FOOTER_MAGIC.to_le_bytes());
        let total_len = (FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE) as u32;
        buf[68..72].copy_from_slice(&total_len.to_le_bytes());

        buf
    }

    pub fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "footer data too short",
            ));
        }

        let trailer_start = data.len() - FOOTER_TRAILER_SIZE;
        let magic = u32::from_le_bytes(data[trailer_start..trailer_start + 4].try_into().unwrap());
        let stored_len = u32::from_le_bytes(
            data[trailer_start + 4..trailer_start + 8]
                .try_into()
                .unwrap(),
        ) as usize;

        if magic != FOOTER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid footer magic: {:#x}, expected {:#x}",
                    magic, FOOTER_MAGIC
                ),
            ));
        }

        if stored_len != data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "footer length mismatch: stored {}, actual {}",
                    stored_len,
                    data.len()
                ),
            ));
        }

        let stored_checksum = u64::from_le_bytes(data[56..64].try_into().unwrap());
        let mut digest = Digest::new();
        digest.write(&data[0..56]);
        let computed_checksum = digest.sum64();

        if stored_checksum != computed_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "footer checksum mismatch: stored {:#x}, computed {:#x}",
                    stored_checksum, computed_checksum
                ),
            ));
        }

        let version = data[0];
        if version != FOOTER_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported footer version: {}", version),
            ));
        }

        let record_type_flags = RecordTypeFlags::from_u8(data[1]);
        let has_min_index = data[2] != 0;
        let has_max_index = data[3] != 0;

        let group_id = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let segment_id = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let valid_bytes = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let min_entry_raw = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let max_entry_raw = u64::from_le_bytes(data[40..48].try_into().unwrap());
        let record_count = u64::from_le_bytes(data[48..56].try_into().unwrap());

        Ok(Self {
            version,
            record_type_flags,
            group_id,
            segment_id,
            valid_bytes,
            min_entry_index: if has_min_index {
                Some(min_entry_raw)
            } else {
                None
            },
            max_entry_index: if has_max_index {
                Some(max_entry_raw)
            } else {
                None
            },
            record_count,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SegmentFooterTracker {
    pub group_id: u64,
    pub segment_id: u64,
    pub record_type_flags: RecordTypeFlags,
    pub min_entry_index: Option<u64>,
    pub max_entry_index: Option<u64>,
    pub record_count: u64,
}

impl SegmentFooterTracker {
    pub fn new(group_id: u64, segment_id: u64) -> Self {
        Self {
            group_id,
            segment_id,
            record_type_flags: RecordTypeFlags::default(),
            min_entry_index: None,
            max_entry_index: None,
            record_count: 0,
        }
    }

    pub fn record_vote(&mut self) {
        self.record_type_flags.has_vote = true;
        self.record_count += 1;
    }

    pub fn record_truncate(&mut self) {
        self.record_type_flags.has_truncate = true;
        self.record_count += 1;
    }

    pub fn record_purge(&mut self) {
        self.record_type_flags.has_purge = true;
        self.record_count += 1;
    }

    pub fn record_entry(&mut self, entry_index: u64) {
        self.record_type_flags.has_entry = true;
        self.record_count += 1;

        self.min_entry_index = Some(
            self.min_entry_index
                .map(|v| v.min(entry_index))
                .unwrap_or(entry_index),
        );
        self.max_entry_index = Some(
            self.max_entry_index
                .map(|v| v.max(entry_index))
                .unwrap_or(entry_index),
        );
    }

    pub fn build(self, valid_bytes: u64) -> SegmentFooter {
        SegmentFooter {
            version: FOOTER_VERSION,
            record_type_flags: self.record_type_flags,
            group_id: self.group_id,
            segment_id: self.segment_id,
            valid_bytes,
            min_entry_index: self.min_entry_index,
            max_entry_index: self.max_entry_index,
            record_count: self.record_count,
        }
    }
}

pub async fn read_footer_from_segment(
    file: &tokio::fs::File,
    _file_len: u64,
) -> io::Result<Option<SegmentFooter>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let file_len = file.metadata().await?.len();
    if file_len < (FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE) as u64 {
        return Ok(None);
    }

    let footer_len = (FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE) as u64;
    let footer_offset = file_len - footer_len;
    let mut footer_buf = vec![0u8; footer_len as usize];
    let mut f = file.try_clone().await?;
    f.seek(std::io::SeekFrom::Start(footer_offset)).await?;
    f.read_exact(&mut footer_buf).await?;

    let magic_offset = FOOTER_HEADER_SIZE;
    let magic = u32::from_le_bytes(
        footer_buf[magic_offset..magic_offset + 4]
            .try_into()
            .unwrap(),
    );
    if magic != FOOTER_MAGIC {
        return Ok(None);
    }

    let footer = SegmentFooter::decode(&footer_buf)?;
    Ok(Some(footer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_type_flags_roundtrip() {
        let flags = RecordTypeFlags {
            has_vote: true,
            has_entry: true,
            has_truncate: false,
            has_purge: true,
        };

        let encoded = flags.to_u8();
        let decoded = RecordTypeFlags::from_u8(encoded);
        assert_eq!(flags, decoded);
    }

    #[test]
    fn test_record_type_flags_has_only_entries() {
        let entries_only = RecordTypeFlags {
            has_entry: true,
            ..Default::default()
        };
        assert!(entries_only.has_only_entries());

        let with_vote = RecordTypeFlags {
            has_entry: true,
            has_vote: true,
            ..Default::default()
        };
        assert!(!with_vote.has_only_entries());
    }

    #[test]
    fn test_footer_roundtrip() {
        let footer = SegmentFooter {
            version: FOOTER_VERSION,
            record_type_flags: RecordTypeFlags {
                has_entry: true,
                has_vote: true,
                ..Default::default()
            },
            group_id: 42,
            segment_id: 7,
            valid_bytes: 1024 * 1024,
            min_entry_index: Some(100),
            max_entry_index: Some(500),
            record_count: 401,
        };

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE);

        let decoded = SegmentFooter::decode(&encoded).unwrap();
        assert_eq!(footer, decoded);
    }

    #[test]
    fn test_footer_no_entries() {
        let footer = SegmentFooter {
            version: FOOTER_VERSION,
            record_type_flags: RecordTypeFlags {
                has_vote: true,
                ..Default::default()
            },
            group_id: 1,
            segment_id: 0,
            valid_bytes: 128,
            min_entry_index: None,
            max_entry_index: None,
            record_count: 1,
        };

        let encoded = footer.encode();
        let decoded = SegmentFooter::decode(&encoded).unwrap();
        assert_eq!(footer, decoded);
        assert!(decoded.min_entry_index.is_none());
        assert!(decoded.max_entry_index.is_none());
    }

    #[test]
    fn test_footer_checksum_validation() {
        let footer = SegmentFooter {
            version: FOOTER_VERSION,
            record_type_flags: RecordTypeFlags::default(),
            group_id: 1,
            segment_id: 0,
            valid_bytes: 0,
            min_entry_index: None,
            max_entry_index: None,
            record_count: 0,
        };

        let mut encoded = footer.encode();
        encoded[10] ^= 0xFF; // Corrupt a byte

        let result = SegmentFooter::decode(&encoded);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }

    #[test]
    fn test_footer_invalid_magic() {
        let mut data = vec![0u8; FOOTER_HEADER_SIZE + FOOTER_TRAILER_SIZE];
        data[FOOTER_HEADER_SIZE..FOOTER_HEADER_SIZE + 4]
            .copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        let len_bytes = (data.len() as u32).to_le_bytes();
        data[FOOTER_HEADER_SIZE + 4..].copy_from_slice(&len_bytes);

        let result = SegmentFooter::decode(&data);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid footer magic")
        );
    }

    #[test]
    fn test_footer_tracker() {
        let mut tracker = SegmentFooterTracker::new(42, 0);

        tracker.record_vote();
        tracker.record_entry(100);
        tracker.record_entry(101);
        tracker.record_entry(102);

        let footer = tracker.build(1024);

        assert!(footer.record_type_flags.has_vote);
        assert!(footer.record_type_flags.has_entry);
        assert!(!footer.record_type_flags.has_truncate);
        assert_eq!(footer.min_entry_index, Some(100));
        assert_eq!(footer.max_entry_index, Some(102));
        assert_eq!(footer.record_count, 4);
        assert_eq!(footer.valid_bytes, 1024);
    }
}
