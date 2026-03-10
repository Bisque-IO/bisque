//! Cursor-based sequential reader for raft log segments.
//!
//! Treats each raft segment as a flat list of `MqCommand` records,
//! scanning sequentially through the mmap bytes. The raft record framing
//! (`[len:4][type:1][group_id:3]...[crc:8]`) is parsed only to find
//! record boundaries and extract the command payload — the raft metadata
//! (term, node_id, log_index) is ignored by the reader.
//!
//! ## Record Layout
//!
//! ```text
//! Raft record:    [len:4][type:1][group_id:3][payload...][crc:8]
//! Entry payload:  [term:8][node_id:8][index:8][tag:1][data...]
//! ```
//!
//! For Normal entries (tag=1), `data` is the serialized `MqCommand` bytes.
//! The cursor extracts just the `data` portion as a zero-copy `Bytes` slice.

use bisque_raft::record_format::{CRC64_SIZE, HEADER_SIZE, LENGTH_SIZE};
use bytes::Bytes;

use crate::types::MqCommand;

/// Raft entry header: term(8) + node_id(8) + index(8) + tag(1) = 25 bytes.
const ENTRY_HEADER_SIZE: usize = 25;

/// Minimum raft record size that can contain a Normal entry:
/// header(8) + entry_header(25) + crc(8) = 41 bytes.
const MIN_ENTRY_RECORD_SIZE: usize = HEADER_SIZE + ENTRY_HEADER_SIZE + CRC64_SIZE;

/// A record yielded by the cursor.
pub struct SegmentRecord {
    /// The raft log index of this entry.
    pub log_index: u64,
    /// The MqCommand extracted from this entry (zero-copy slice of segment mmap).
    pub command: MqCommand,
}

/// Sequential cursor over MqCommand records in a single raft log segment.
///
/// Pins the segment mmap via a `Bytes` handle. Advances through records
/// by reading length prefixes — no index lookups needed.
///
/// When a `TAG_BATCH` command is encountered, the cursor explodes it into
/// individual sub-commands, yielding each one separately with the same
/// `log_index` as the batch entry.
pub struct MqSegmentCursor {
    /// The full segment bytes (holds Arc<Segment> alive via Bytes ownership).
    data: Bytes,
    /// Current byte offset within the segment.
    offset: usize,
    /// Segment ID for diagnostics / prefetching.
    segment_id: u64,
    /// When exploding a TAG_BATCH, holds the batch payload bytes.
    batch_data: Option<Bytes>,
    /// Current byte offset within `batch_data` (after tag+count header).
    batch_offset: usize,
    /// Remaining sub-commands in the current batch.
    batch_remaining: u32,
    /// The log_index of the batch currently being exploded.
    batch_log_index: u64,
}

impl MqSegmentCursor {
    /// Create a cursor pinning the given segment.
    ///
    /// `data` must be the full valid region of the segment (from
    /// `SegmentPrefetcher::segment_bytes`). The cursor starts at offset 0.
    pub fn new(segment_id: u64, data: Bytes) -> Self {
        Self {
            data,
            offset: 0,
            segment_id,
            batch_data: None,
            batch_offset: 0,
            batch_remaining: 0,
            batch_log_index: 0,
        }
    }

    /// The segment ID this cursor is scanning.
    #[inline]
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Current byte offset within the segment.
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Whether the cursor has reached the end of valid data.
    #[inline]
    pub fn is_exhausted(&self) -> bool {
        self.offset + LENGTH_SIZE > self.data.len()
    }

    /// Reset the cursor to the beginning of the segment.
    #[inline]
    pub fn rewind(&mut self) {
        self.offset = 0;
        self.batch_data = None;
        self.batch_remaining = 0;
    }

    /// Advance to the next record, returning the MqCommand if it's a Normal entry.
    ///
    /// Skips non-Entry records (Vote, Truncate, Purge) and non-Normal entries
    /// (Membership, Blank). When a `TAG_BATCH` command is encountered, the
    /// batch is exploded and each sub-command is yielded individually (all
    /// sharing the same `log_index`).
    ///
    /// Returns `None` when the segment is exhausted.
    pub fn next_record(&mut self) -> Option<SegmentRecord> {
        // If we're in the middle of exploding a batch, yield the next sub-command.
        // Batch format: [TAG_BATCH:1][count:4][len1:4][cmd1][len2:4][cmd2]...
        if let Some(data) = &self.batch_data {
            if self.batch_remaining > 0 {
                let off = self.batch_offset;
                let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                let cmd = MqCommand::from_bytes(data.slice(off + 4..off + 4 + len));
                self.batch_offset = off + 4 + len;
                self.batch_remaining -= 1;
                return Some(SegmentRecord {
                    log_index: self.batch_log_index,
                    command: cmd,
                });
            }
            // Batch exhausted.
            self.batch_data = None;
        }

        loop {
            if self.offset + LENGTH_SIZE > self.data.len() {
                return None;
            }

            let record_len = u32::from_le_bytes(
                self.data[self.offset..self.offset + LENGTH_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;

            // Zero length or overrun = end of valid data.
            let total = LENGTH_SIZE + record_len;
            if record_len == 0 || self.offset + total > self.data.len() {
                // Park at end so is_exhausted() returns true.
                self.offset = self.data.len();
                return None;
            }

            let record_start = self.offset;
            self.offset += total;

            // Minimum size check: must fit raft header + entry header + CRC.
            if total < MIN_ENTRY_RECORD_SIZE {
                continue;
            }

            // Check record type: must be Entry (0x02).
            let record_type = self.data[record_start + LENGTH_SIZE];
            if record_type != 0x02 {
                continue;
            }

            // Check entry tag: must be Normal (1).
            // Entry tag is at: record_start + HEADER_SIZE + 24 (after term+node_id+index).
            let entry_tag = self.data[record_start + HEADER_SIZE + 24];
            if entry_tag != 1 {
                continue;
            }

            // Extract log_index from the entry header.
            let index_offset = record_start + HEADER_SIZE + 16; // after term(8) + node_id(8)
            let log_index = u64::from_le_bytes(
                self.data[index_offset..index_offset + 8]
                    .try_into()
                    .unwrap(),
            );

            // Extract MqCommand data: after entry header, before CRC.
            let data_start = record_start + HEADER_SIZE + ENTRY_HEADER_SIZE;
            let data_end = record_start + total - CRC64_SIZE;
            if data_start >= data_end {
                continue;
            }

            let cmd_bytes = self.data.slice(data_start..data_end);

            // Explode TAG_BATCH: store the bytes and re-enter to yield first sub-command.
            // Layout: [TAG_BATCH:1][count:4][len1:4][cmd1][len2:4][cmd2]...
            if cmd_bytes[0] == MqCommand::TAG_BATCH && cmd_bytes.len() >= 5 {
                let count = u32::from_le_bytes(cmd_bytes[1..5].try_into().unwrap());
                if count > 0 {
                    self.batch_data = Some(cmd_bytes);
                    self.batch_offset = 5; // skip tag(1) + count(4)
                    self.batch_remaining = count;
                    self.batch_log_index = log_index;
                    // Re-enter to yield first sub-command from the batch path above.
                    return self.next_record();
                }
                // Empty batch — skip.
                continue;
            }

            return Some(SegmentRecord {
                log_index,
                command: MqCommand::from_bytes(cmd_bytes),
            });
        }
    }

    /// Scan for the next record matching a specific entity.
    ///
    /// Filters by MqCommand tag and entity_id. For Publish commands the
    /// entity_id is at bytes `[1..9]` (topic_id); for Enqueue it's also
    /// at `[1..9]` (queue_id). Returns `None` when the segment is exhausted.
    ///
    /// `match_tag` should be one of the `MqCommand::TAG_*` constants.
    pub fn next_record_for_entity(
        &mut self,
        match_tag: u8,
        entity_id: u64,
    ) -> Option<SegmentRecord> {
        loop {
            let rec = self.next_record()?;
            let cmd = &rec.command;
            if cmd.tag() == match_tag && cmd.buf.len() >= 9 {
                let id = u64::from_le_bytes(cmd.buf[1..9].try_into().unwrap());
                if id == entity_id {
                    return Some(rec);
                }
            }
        }
    }

    /// Collect all remaining records into a Vec.
    pub fn collect_all(&mut self) -> Vec<SegmentRecord> {
        let mut out = Vec::new();
        while let Some(rec) = self.next_record() {
            out.push(rec);
        }
        out
    }

    /// Collect remaining records matching a specific entity.
    pub fn collect_for_entity(&mut self, match_tag: u8, entity_id: u64) -> Vec<SegmentRecord> {
        let mut out = Vec::new();
        while let Some(rec) = self.next_record_for_entity(match_tag, entity_id) {
            out.push(rec);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// MqSegmentScanner — multi-segment sequential scanner
// ---------------------------------------------------------------------------

/// Multi-segment sequential scanner backed by a `SegmentPrefetcher`.
///
/// Walks through segments in order, creating a cursor for each one.
/// Automatically advances to the next segment when the current one
/// is exhausted. Triggers read-ahead prefetching for the next segment.
pub struct MqSegmentScanner {
    prefetcher: bisque_raft::SegmentPrefetcher,
    /// Current cursor (None if not yet started or fully exhausted).
    cursor: Option<MqSegmentCursor>,
    /// Next segment ID to try when the current cursor is exhausted.
    next_segment_id: u64,
    /// Segment ID of the last segment we know exists (active segment).
    /// We stop scanning past this.
    active_segment_id: u64,
}

impl MqSegmentScanner {
    /// Create a scanner starting from `start_segment_id`.
    pub fn new(prefetcher: bisque_raft::SegmentPrefetcher, start_segment_id: u64) -> Self {
        let active_segment_id = prefetcher.active_segment_id();
        Self {
            prefetcher,
            cursor: None,
            next_segment_id: start_segment_id,
            active_segment_id,
        }
    }

    /// Create a scanner that starts from the earliest available segment.
    pub fn from_start(prefetcher: bisque_raft::SegmentPrefetcher) -> Self {
        // Start from segment 1 (segment 0 is the sentinel).
        Self::new(prefetcher, 1)
    }

    /// The current segment ID being scanned, if any.
    pub fn current_segment_id(&self) -> Option<u64> {
        self.cursor.as_ref().map(|c| c.segment_id())
    }

    /// Advance to the next record across segments.
    ///
    /// When the current segment is exhausted, automatically opens the next
    /// segment and continues scanning. Returns `None` when all segments
    /// are exhausted.
    pub fn next_record(&mut self) -> Option<SegmentRecord> {
        loop {
            // Try the current cursor first.
            if let Some(cursor) = &mut self.cursor {
                if let Some(rec) = cursor.next_record() {
                    return Some(rec);
                }
                // Current segment exhausted — fall through to advance.
            }

            // Try to open the next segment.
            if !self.advance_segment() {
                return None;
            }
        }
    }

    /// Advance to the next record matching a specific entity across segments.
    pub fn next_record_for_entity(
        &mut self,
        match_tag: u8,
        entity_id: u64,
    ) -> Option<SegmentRecord> {
        loop {
            if let Some(cursor) = &mut self.cursor {
                if let Some(rec) = cursor.next_record_for_entity(match_tag, entity_id) {
                    return Some(rec);
                }
            }

            if !self.advance_segment() {
                return None;
            }
        }
    }

    /// Try to advance to the next available segment.
    /// Returns true if a new cursor was created, false if no more segments.
    fn advance_segment(&mut self) -> bool {
        // Refresh active segment ID in case it changed.
        self.active_segment_id = self.prefetcher.active_segment_id();

        while self.next_segment_id <= self.active_segment_id {
            let seg_id = self.next_segment_id;
            self.next_segment_id += 1;

            if let Some(data) = self.prefetcher.segment_bytes(seg_id) {
                // Prefetch the segment after this one.
                if self.next_segment_id <= self.active_segment_id {
                    self.prefetcher.prefetch_next(0); // trigger background load
                }
                self.cursor = Some(MqSegmentCursor::new(seg_id, data));
                return true;
            }
            // Segment not pinned — skip it (may have been evicted or purged).
        }

        self.cursor = None;
        false
    }
}

// ---------------------------------------------------------------------------
// Point-read helpers — bridge SegmentPrefetcher to MqCommand message extraction
// ---------------------------------------------------------------------------

/// Read the MqCommand at the given raft log index.
///
/// Returns `None` if the entry is not found, not a Normal entry, or has been purged.
#[inline]
pub fn read_command(
    prefetcher: &bisque_raft::SegmentPrefetcher,
    log_index: u64,
) -> Option<MqCommand> {
    let data = prefetcher.read_normal_entry_data(log_index)?;
    if data.is_empty() {
        return None;
    }
    Some(MqCommand::from_bytes(data))
}

/// Read a single flat message at the given raft log index.
///
/// Extracts the first message from a Publish or PublishToExchange command.
/// Returns `None` if the entry is not a publish command or has been purged.
#[inline]
pub fn read_message_at(
    prefetcher: &bisque_raft::SegmentPrefetcher,
    log_index: u64,
) -> Option<Bytes> {
    let cmd = read_command(prefetcher, log_index)?;
    cmd.publish_messages()?.next()
}

/// Read all flat messages at the given raft log index into `out`.
///
/// Clears `out` before appending. Returns an empty vec if the entry is not
/// a publish command or has been purged.
pub fn read_messages_at_into(
    prefetcher: &bisque_raft::SegmentPrefetcher,
    log_index: u64,
    out: &mut Vec<Bytes>,
) {
    out.clear();
    if let Some(cmd) = read_command(prefetcher, log_index) {
        if let Some(msgs) = cmd.publish_messages() {
            out.extend(msgs);
        }
    }
}

/// Read the latest message from a topic using metadata + prefetcher.
///
/// `latest` is `(log_index, msg_pos)` from `MqMetadata::get_topic_latest()`.
pub fn read_latest_topic_message(
    prefetcher: &bisque_raft::SegmentPrefetcher,
    topic_id: u64,
    latest: (u64, usize),
) -> Option<Bytes> {
    let (log_index, msg_pos) = latest;
    let cmd = read_command(prefetcher, log_index)?;
    cmd.publish_messages_for_topic(topic_id)?.nth(msg_pos)
}

// ---------------------------------------------------------------------------
// MqReader — bundled prefetcher + metadata for protocol adapters
// ---------------------------------------------------------------------------

/// Bundles a `SegmentPrefetcher` and `MqMetadata` into a single handle.
///
/// Protocol adapters (MQTT, SQS, Kafka) receive this instead of the old
/// `Arc<dyn MqLogReader>`. Provides point-reads via the prefetcher and
/// metadata queries via the lock-free snapshot.
pub struct MqReader {
    pub prefetcher: bisque_raft::SegmentPrefetcher,
    pub metadata: std::sync::Arc<crate::metadata::MqMetadata>,
}

impl MqReader {
    pub fn new(
        prefetcher: bisque_raft::SegmentPrefetcher,
        metadata: std::sync::Arc<crate::metadata::MqMetadata>,
    ) -> Self {
        Self {
            prefetcher,
            metadata,
        }
    }

    /// Read a single flat message at the given raft log index.
    #[inline]
    pub fn read_message_at(&self, log_index: u64) -> Option<Bytes> {
        read_message_at(&self.prefetcher, log_index)
    }

    /// Read all flat messages at the given raft log index into `out`.
    pub fn read_messages_at_into(&self, log_index: u64, out: &mut Vec<Bytes>) {
        read_messages_at_into(&self.prefetcher, log_index, out)
    }

    /// List topics matching a name prefix.
    pub fn list_topics_with_prefix(&self, prefix: &str) -> Vec<(Bytes, u64)> {
        self.metadata.list_topics_with_prefix(prefix)
    }

    /// Read the latest message from a topic (for retained messages).
    pub fn read_latest_topic_message(&self, topic_id: u64) -> Option<Bytes> {
        let latest = self.metadata.get_topic_latest(topic_id)?;
        read_latest_topic_message(&self.prefetcher, topic_id, latest)
    }

    /// Create a segment cursor for the given segment.
    pub fn segment_cursor(&self, segment_id: u64) -> Option<MqSegmentCursor> {
        let data = self.prefetcher.segment_bytes(segment_id)?;
        Some(MqSegmentCursor::new(segment_id, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bisque_raft::record_format::{RecordType, append_record_into};

    /// Build a fake segment with Normal raft entries containing MqCommand data.
    fn build_test_segment(commands: &[MqCommand]) -> Bytes {
        let mut buf = Vec::new();
        for (i, cmd) in commands.iter().enumerate() {
            // Build entry payload: [term:8][node_id:8][index:8][tag:1][data...]
            let mut payload = Vec::new();
            let term: u64 = 1;
            let node_id: u64 = 1;
            let index: u64 = (i + 1) as u64;
            let tag: u8 = 1; // Normal
            payload.extend_from_slice(&term.to_le_bytes());
            payload.extend_from_slice(&node_id.to_le_bytes());
            payload.extend_from_slice(&index.to_le_bytes());
            payload.push(tag);
            payload.extend_from_slice(cmd.as_bytes());

            append_record_into(&mut buf, RecordType::Entry, 0, &payload);
        }
        Bytes::from(buf)
    }

    /// Build a fake segment with mixed record types (including non-entry records).
    fn build_mixed_segment(commands: &[MqCommand]) -> Bytes {
        let mut buf = Vec::new();
        // Vote record first.
        let vote_payload = [0u8; 16];
        append_record_into(&mut buf, RecordType::Vote, 0, &vote_payload);

        for (i, cmd) in commands.iter().enumerate() {
            let mut payload = Vec::new();
            let term: u64 = 1;
            let node_id: u64 = 1;
            let index: u64 = (i + 1) as u64;
            let tag: u8 = 1;
            payload.extend_from_slice(&term.to_le_bytes());
            payload.extend_from_slice(&node_id.to_le_bytes());
            payload.extend_from_slice(&index.to_le_bytes());
            payload.push(tag);
            payload.extend_from_slice(cmd.as_bytes());
            append_record_into(&mut buf, RecordType::Entry, 0, &payload);

            // Interleave a Truncate record (should be skipped).
            if i % 2 == 0 {
                let trunc_payload = 0u64.to_le_bytes();
                append_record_into(&mut buf, RecordType::Truncate, 0, &trunc_payload);
            }
        }
        Bytes::from(buf)
    }

    #[test]
    fn cursor_empty_segment() {
        let data = Bytes::new();
        let mut cursor = MqSegmentCursor::new(1, data);
        assert!(cursor.is_exhausted());
        assert!(cursor.next_record().is_none());
    }

    #[test]
    fn cursor_scan_all_records() {
        let cmds = vec![
            MqCommand::publish(10, &[Bytes::from_static(b"hello")]),
            MqCommand::publish(20, &[Bytes::from_static(b"world")]),
            MqCommand::enqueue(30, &[Bytes::from_static(b"msg")], &[]),
        ];
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        let rec1 = cursor.next_record().expect("should get record 1");
        assert_eq!(rec1.log_index, 1);
        assert_eq!(rec1.command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(rec1.command.as_publish().topic_id(), 10);

        let rec2 = cursor.next_record().expect("should get record 2");
        assert_eq!(rec2.log_index, 2);
        assert_eq!(rec2.command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(rec2.command.as_publish().topic_id(), 20);

        let rec3 = cursor.next_record().expect("should get record 3");
        assert_eq!(rec3.log_index, 3);
        assert_eq!(rec3.command.tag(), MqCommand::TAG_ENQUEUE);
        assert_eq!(rec3.command.as_enqueue().queue_id(), 30);

        assert!(cursor.next_record().is_none());
        assert!(cursor.is_exhausted());
    }

    #[test]
    fn cursor_skips_non_entry_records() {
        let cmds = vec![
            MqCommand::publish(1, &[Bytes::from_static(b"a")]),
            MqCommand::publish(2, &[Bytes::from_static(b"b")]),
            MqCommand::publish(3, &[Bytes::from_static(b"c")]),
        ];
        let data = build_mixed_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        let records = cursor.collect_all();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].command.as_publish().topic_id(), 1);
        assert_eq!(records[1].command.as_publish().topic_id(), 2);
        assert_eq!(records[2].command.as_publish().topic_id(), 3);
    }

    #[test]
    fn cursor_filter_by_entity() {
        let cmds = vec![
            MqCommand::publish(10, &[Bytes::from_static(b"a")]),
            MqCommand::publish(20, &[Bytes::from_static(b"b")]),
            MqCommand::publish(10, &[Bytes::from_static(b"c")]),
            MqCommand::enqueue(10, &[Bytes::from_static(b"d")], &[]),
            MqCommand::publish(10, &[Bytes::from_static(b"e")]),
        ];
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        // Filter for Publish to topic 10.
        let records = cursor.collect_for_entity(MqCommand::TAG_PUBLISH, 10);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].log_index, 1);
        assert_eq!(records[1].log_index, 3);
        assert_eq!(records[2].log_index, 5);

        // Rewind and filter for Enqueue to queue 10.
        cursor.rewind();
        let records = cursor.collect_for_entity(MqCommand::TAG_ENQUEUE, 10);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].log_index, 4);
    }

    #[test]
    fn cursor_rewind() {
        let cmds = vec![MqCommand::publish(1, &[Bytes::from_static(b"x")])];
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        assert!(cursor.next_record().is_some());
        assert!(cursor.next_record().is_none());

        cursor.rewind();
        assert!(!cursor.is_exhausted());
        assert!(cursor.next_record().is_some());
    }

    #[test]
    fn cursor_zero_copy_messages() {
        let cmds = vec![MqCommand::publish(
            42,
            &[Bytes::from_static(b"hello"), Bytes::from_static(b"world")],
        )];
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        let rec = cursor.next_record().unwrap();
        let msgs: Vec<Bytes> = rec.command.publish_messages().unwrap().collect();
        assert_eq!(msgs.len(), 2);
        assert_eq!(&msgs[0][..], b"hello");
        assert_eq!(&msgs[1][..], b"world");
    }

    #[test]
    fn cursor_membership_entry_skipped() {
        // Build a segment with a Membership entry (tag=2, not Normal).
        let mut buf = Vec::new();
        let mut payload = Vec::new();
        let term: u64 = 1;
        let node_id: u64 = 1;
        let index: u64 = 1;
        let tag: u8 = 2; // Membership, not Normal
        payload.extend_from_slice(&term.to_le_bytes());
        payload.extend_from_slice(&node_id.to_le_bytes());
        payload.extend_from_slice(&index.to_le_bytes());
        payload.push(tag);
        payload.extend_from_slice(&[0u8; 32]); // dummy membership data
        append_record_into(&mut buf, RecordType::Entry, 0, &payload);

        // Then a Normal entry.
        let cmd = MqCommand::publish(1, &[Bytes::from_static(b"ok")]);
        let mut payload2 = Vec::new();
        payload2.extend_from_slice(&1u64.to_le_bytes());
        payload2.extend_from_slice(&1u64.to_le_bytes());
        payload2.extend_from_slice(&2u64.to_le_bytes());
        payload2.push(1); // Normal
        payload2.extend_from_slice(cmd.as_bytes());
        append_record_into(&mut buf, RecordType::Entry, 0, &payload2);

        let data = Bytes::from(buf);
        let mut cursor = MqSegmentCursor::new(1, data);

        // Should skip the Membership entry and return only the Normal one.
        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.log_index, 2);
        assert_eq!(rec.command.tag(), MqCommand::TAG_PUBLISH);
        assert!(cursor.next_record().is_none());
    }

    #[test]
    fn cursor_large_segment() {
        // 1000 records to verify sequential scanning at scale.
        let cmds: Vec<MqCommand> = (0..1000)
            .map(|i| MqCommand::publish(i % 10, &[Bytes::from(format!("msg-{i}"))]))
            .collect();
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        let all = cursor.collect_all();
        assert_eq!(all.len(), 1000);
        for (i, rec) in all.iter().enumerate() {
            assert_eq!(rec.log_index, (i + 1) as u64);
            assert_eq!(rec.command.as_publish().topic_id(), (i % 10) as u64);
        }

        // Filter for topic 5.
        cursor.rewind();
        let filtered = cursor.collect_for_entity(MqCommand::TAG_PUBLISH, 5);
        assert_eq!(filtered.len(), 100);
    }

    #[test]
    fn cursor_explodes_batch() {
        // A batch containing 3 sub-commands should yield 3 individual records.
        let sub1 = MqCommand::publish(10, &[Bytes::from_static(b"a")]);
        let sub2 = MqCommand::publish(20, &[Bytes::from_static(b"b")]);
        let sub3 = MqCommand::enqueue(30, &[Bytes::from_static(b"c")], &[]);
        let batch = MqCommand::batch(&[sub1, sub2, sub3]);

        let cmds = vec![
            MqCommand::publish(1, &[Bytes::from_static(b"before")]),
            batch,
            MqCommand::publish(2, &[Bytes::from_static(b"after")]),
        ];
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        // Record 1: standalone publish.
        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.log_index, 1);
        assert_eq!(rec.command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(rec.command.as_publish().topic_id(), 1);

        // Records 2-4: exploded batch sub-commands (all share log_index=2).
        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.log_index, 2);
        assert_eq!(rec.command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(rec.command.as_publish().topic_id(), 10);

        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.log_index, 2);
        assert_eq!(rec.command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(rec.command.as_publish().topic_id(), 20);

        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.log_index, 2);
        assert_eq!(rec.command.tag(), MqCommand::TAG_ENQUEUE);
        assert_eq!(rec.command.as_enqueue().queue_id(), 30);

        // Record 5: standalone publish after batch.
        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.log_index, 3);
        assert_eq!(rec.command.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(rec.command.as_publish().topic_id(), 2);

        assert!(cursor.next_record().is_none());
    }

    #[test]
    fn cursor_entity_filter_inside_batch() {
        // Entity filtering should find sub-commands inside batches.
        let sub1 = MqCommand::publish(10, &[Bytes::from_static(b"x")]);
        let sub2 = MqCommand::publish(20, &[Bytes::from_static(b"y")]);
        let sub3 = MqCommand::publish(10, &[Bytes::from_static(b"z")]);
        let batch = MqCommand::batch(&[sub1, sub2, sub3]);

        let cmds = vec![
            MqCommand::publish(10, &[Bytes::from_static(b"solo")]),
            batch,
        ];
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        let records = cursor.collect_for_entity(MqCommand::TAG_PUBLISH, 10);
        // 1 standalone + 2 from batch (sub1, sub3) = 3 total.
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].log_index, 1);
        assert_eq!(records[1].log_index, 2);
        assert_eq!(records[2].log_index, 2);
    }

    #[test]
    fn cursor_consecutive_batches() {
        // Two batches back-to-back: ensure state resets between them.
        let batch1 = MqCommand::batch(&[
            MqCommand::publish(1, &[Bytes::from_static(b"a")]),
            MqCommand::publish(2, &[Bytes::from_static(b"b")]),
        ]);
        let batch2 = MqCommand::batch(&[MqCommand::publish(3, &[Bytes::from_static(b"c")])]);
        let data = build_test_segment(&[batch1, batch2]);
        let mut cursor = MqSegmentCursor::new(1, data);

        let all = cursor.collect_all();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].command.as_publish().topic_id(), 1);
        assert_eq!(all[0].log_index, 1);
        assert_eq!(all[1].command.as_publish().topic_id(), 2);
        assert_eq!(all[1].log_index, 1);
        assert_eq!(all[2].command.as_publish().topic_id(), 3);
        assert_eq!(all[2].log_index, 2);
    }

    #[test]
    fn cursor_empty_batch_skipped() {
        let empty_batch = MqCommand::batch(&[]);
        let cmds = vec![
            empty_batch,
            MqCommand::publish(1, &[Bytes::from_static(b"after")]),
        ];
        let data = build_test_segment(&cmds);
        let mut cursor = MqSegmentCursor::new(1, data);

        let all = cursor.collect_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].log_index, 2);
        assert_eq!(all[0].command.as_publish().topic_id(), 1);
    }

    #[test]
    fn cursor_truncated_segment() {
        // Build a valid segment then truncate it mid-record.
        let cmds = vec![
            MqCommand::publish(1, &[Bytes::from_static(b"ok")]),
            MqCommand::publish(2, &[Bytes::from_static(b"truncated")]),
        ];
        let full = build_test_segment(&cmds);
        // Truncate to remove the second record partially.
        let truncated = full.slice(..full.len() - 5);
        let mut cursor = MqSegmentCursor::new(1, truncated);

        // First record should succeed.
        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.command.as_publish().topic_id(), 1);
        // Second record is truncated — cursor should stop.
        assert!(cursor.next_record().is_none());
    }

    #[test]
    fn cursor_rewind_mid_batch() {
        // Rewind while in the middle of exploding a batch.
        let batch = MqCommand::batch(&[
            MqCommand::publish(1, &[Bytes::from_static(b"a")]),
            MqCommand::publish(2, &[Bytes::from_static(b"b")]),
            MqCommand::publish(3, &[Bytes::from_static(b"c")]),
        ]);
        let data = build_test_segment(&[batch]);
        let mut cursor = MqSegmentCursor::new(1, data);

        // Read first sub-command from batch.
        let rec = cursor.next_record().unwrap();
        assert_eq!(rec.command.as_publish().topic_id(), 1);

        // Rewind — should reset batch state.
        cursor.rewind();
        let all = cursor.collect_all();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].command.as_publish().topic_id(), 1);
        assert_eq!(all[1].command.as_publish().topic_id(), 2);
        assert_eq!(all[2].command.as_publish().topic_id(), 3);
    }

    #[test]
    fn cursor_batch_with_mixed_command_types() {
        // Batch containing different command types; entity filter picks the right ones.
        let batch = MqCommand::batch(&[
            MqCommand::publish(10, &[Bytes::from_static(b"p1")]),
            MqCommand::enqueue(10, &[Bytes::from_static(b"e1")], &[]),
            MqCommand::publish(10, &[Bytes::from_static(b"p2")]),
            MqCommand::enqueue(20, &[Bytes::from_static(b"e2")], &[]),
        ]);
        let data = build_test_segment(&[batch]);
        let mut cursor = MqSegmentCursor::new(1, data);

        // Filter for publishes to topic 10.
        let pubs = cursor.collect_for_entity(MqCommand::TAG_PUBLISH, 10);
        assert_eq!(pubs.len(), 2);

        // Rewind and filter for enqueue to queue 10.
        cursor.rewind();
        let enqueues = cursor.collect_for_entity(MqCommand::TAG_ENQUEUE, 10);
        assert_eq!(enqueues.len(), 1);

        // Rewind and filter for enqueue to queue 20.
        cursor.rewind();
        let enqueues = cursor.collect_for_entity(MqCommand::TAG_ENQUEUE, 20);
        assert_eq!(enqueues.len(), 1);
    }

    #[test]
    fn cursor_only_length_prefix_no_data() {
        // Segment with just a 4-byte zero length prefix — should stop immediately.
        let data = Bytes::from_static(&[0, 0, 0, 0]);
        let mut cursor = MqSegmentCursor::new(1, data);
        assert!(cursor.next_record().is_none());
        assert!(cursor.is_exhausted());
    }
}
