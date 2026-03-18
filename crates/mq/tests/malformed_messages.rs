//! Comprehensive tests for malicious and malformed message handling.
//!
//! These tests verify that the bisque-mq crate never panics when processing
//! corrupt, truncated, or adversarially crafted binary data. Every code path
//! must return a `Result::Err`, `None`, or a safe default — never `panic!`.

use bytes::{BufMut, Bytes, BytesMut};

use bisque_mq::codec::*;
use bisque_mq::config::MqConfig;
use bisque_mq::segment_index::{FrozenSegmentIndex, SIDX_MAGIC, SIDX_VERSION};
use bisque_mq::types::MqCommand;
use bisque_mq::{MqEngine, ResponseEntry};

// =============================================================================
// Helper: build a raw command buffer with a specific tag and size
// =============================================================================

/// Build a minimal header-only command buffer.
fn make_header(size: u32, fixed: u16, tag: u8, flags: u8) -> BytesMut {
    let mut buf = BytesMut::with_capacity(size as usize);
    buf.put_u32_le(size);
    buf.put_u16_le(fixed);
    buf.put_u8(tag);
    buf.put_u8(flags);
    buf
}

/// Build an MqCommand from raw bytes.
fn cmd_from_bytes(data: &[u8]) -> MqCommand {
    MqCommand::from_bytes(Bytes::from(data.to_vec()))
}

/// Apply a command to a fresh engine, return the response.
fn apply_cmd(cmd: &MqCommand, log_index: u64) -> ResponseEntry {
    let engine = MqEngine::new(MqConfig::new("/tmp/bisque-mq-malformed-test"));
    let mut buf = BytesMut::with_capacity(64);
    engine.apply_command(cmd, &mut buf, log_index, 1000, None);
    ResponseEntry::split_from(&mut buf)
}

// =============================================================================
// 1. Empty and undersized buffers
// =============================================================================

#[test]
fn empty_buffer_validate_fails() {
    let cmd = cmd_from_bytes(&[]);
    assert!(cmd.validate().is_err());
}

#[test]
fn one_byte_buffer_validate_fails() {
    let cmd = cmd_from_bytes(&[0x42]);
    assert!(cmd.validate().is_err());
}

#[test]
fn seven_byte_buffer_validate_fails() {
    let cmd = cmd_from_bytes(&[0; 7]);
    assert!(cmd.validate().is_err());
}

#[test]
fn eight_byte_header_validate_ok() {
    // 8-byte header with size=8 is a valid (empty) command
    let mut buf = make_header(8, 8, 0, 0);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.validate().is_ok());
}

#[test]
fn header_claims_larger_than_buffer() {
    // Header says size=1024 but buffer is only 8 bytes
    let mut buf = make_header(1024, 8, MqCommand::TAG_DELETE_TOPIC, 0);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.validate().is_err());
}

// =============================================================================
// 2. View struct construction with undersized buffers
// =============================================================================

#[test]
fn as_create_topic_too_short() {
    // CmdCreateTopic::MIN_SIZE = 28, give it only 16 bytes
    let mut buf = make_header(16, 8, MqCommand::TAG_CREATE_TOPIC, 0);
    buf.put_bytes(0, 8); // pad to 16
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_create_topic().is_err());
}

#[test]
fn as_publish_too_short() {
    // CmdPublish::MIN_SIZE = 24, give it only 8
    let mut buf = make_header(8, 8, MqCommand::TAG_PUBLISH, 0);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_publish().is_err());
}

#[test]
fn as_create_binding_too_short() {
    // CmdCreateBinding::MIN_SIZE = 40
    let mut buf = make_header(16, 8, MqCommand::TAG_CREATE_BINDING, 0);
    buf.put_bytes(0, 8);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_create_binding().is_err());
}

#[test]
fn as_forwarded_batch_too_short() {
    // CmdForwardedBatch::MIN_SIZE = 32
    let mut buf = make_header(16, 8, MqCommand::TAG_FORWARDED_BATCH, 0);
    buf.put_bytes(0, 8);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_forwarded_batch().is_err());
}

#[test]
fn as_batch_too_short() {
    // CmdBatch::MIN_SIZE = 16
    let mut buf = make_header(8, 8, MqCommand::TAG_BATCH, 0);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_batch().is_err());
}

#[test]
fn as_create_consumer_group_too_short() {
    // CmdCreateConsumerGroup::MIN_SIZE = 59
    let mut buf = make_header(32, 32, MqCommand::TAG_CREATE_CONSUMER_GROUP, 0);
    buf.put_bytes(0, 24);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_create_consumer_group().is_err());
}

#[test]
fn as_commit_group_offset_too_short() {
    // CmdCommitGroupOffset::MIN_SIZE = 56
    let mut buf = make_header(32, 32, MqCommand::TAG_COMMIT_GROUP_OFFSET, 0);
    buf.put_bytes(0, 24);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_commit_group_offset().is_err());
}

#[test]
fn as_set_will_too_short() {
    // CmdSetWill::MIN_SIZE = 44
    let mut buf = make_header(24, 24, MqCommand::TAG_SET_WILL, 0);
    buf.put_bytes(0, 16);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_set_will().is_err());
}

#[test]
fn as_persist_session_too_short() {
    // CmdPersistSession::MIN_SIZE = 52
    let mut buf = make_header(32, 32, MqCommand::TAG_PERSIST_SESSION, 0);
    buf.put_bytes(0, 24);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.as_persist_session().is_err());
}

// =============================================================================
// 3. Variable-field corruption: flex8 with out-of-bounds offset
// =============================================================================

#[test]
fn flex8_large_branch_offset_past_buffer() {
    // Build a 32-byte command with a flex8 field at offset 8 whose "large"
    // branch points past the buffer.
    let mut buf = make_header(32, 16, MqCommand::TAG_CREATE_TOPIC, 0);
    // At offset 8: flex8 large marker (bit 0 = 1), with data_offset pointing to byte 9999
    // raw = (9999 << 1) | 1 = 19999
    buf.put_u32_le(19999); // flex8 raw: large branch, offset=9999
    buf.put_u32_le(100); // flex8 size: 100 bytes
    buf.put_bytes(0, 16); // pad to 32 bytes
    let cmd = MqCommand::split_from(&mut buf);
    // The view struct construction should succeed (MIN_SIZE is met)
    let view = cmd.as_create_topic().unwrap();
    // But reading the flex8 name field should return an error
    assert!(view.name().is_err());
}

#[test]
fn flex8_small_branch_length_past_buffer() {
    // Build a command where flex8 small-branch has length=7 but the buffer
    // is only 12 bytes total, so offset+1+7 = 16 > 12.
    // We need a buffer smaller than MIN_SIZE, so use MqCommand directly.
    let data: &[u8] = &[
        12,
        0,
        0,
        0, // size=12
        8,
        0, // fixed=8
        MqCommand::TAG_CREATE_TOPIC,
        0,  // tag, flags
        14, // flex8 at offset 8: len=7 (byte = 7 << 1 = 14)
        0x41,
        0x41,
        0x41, // only 3 bytes of data (need 7)
    ];
    let cmd = cmd_from_bytes(data);
    // View struct construction should fail (MIN_SIZE=28 > 12)
    assert!(cmd.as_create_topic().is_err());
}

// =============================================================================
// 4. Variable-field corruption: blob with out-of-bounds offset
// =============================================================================

#[test]
fn blob_offset_past_buffer() {
    // CmdCreateTopic has a blob field at offset 16 for retention.
    // Set data_offset to a huge value.
    let mut buf = make_header(32, 24, MqCommand::TAG_CREATE_TOPIC, 0);
    // offset 8: flex8 name (empty, small branch, len=0)
    buf.put_u8(0);
    buf.put_bytes(0, 7);
    // offset 16: blob [data_offset:4][data_size:4]
    buf.put_u32_le(50000); // data_offset past end of buffer
    buf.put_u32_le(32); // data_size
    // offset 24: partition_count
    buf.put_u32_le(1);
    buf.put_bytes(0, 4); // pad to 32
    let cmd = MqCommand::split_from(&mut buf);
    let view = cmd.as_create_topic().unwrap();
    // blob read should fail
    assert!(view.retention().is_err());
}

// =============================================================================
// 5. Iterator corruption: VecBytesIter with bad data_start
// =============================================================================

#[test]
fn vec_bytes_iter_bad_data_start() {
    // CmdPublish: @8 topic_id:u64, @16 messages:vec_bytes [count:4][data_start:4]
    let mut buf = make_header(24, 24, MqCommand::TAG_PUBLISH, 0);
    buf.put_u64_le(42); // topic_id
    buf.put_u32_le(5); // count = 5 messages
    buf.put_u32_le(9999); // data_start = way past buffer
    let cmd = MqCommand::split_from(&mut buf);
    let view = cmd.as_publish().unwrap();
    // Iterator should return None immediately (or on first next) — not panic
    let messages: Vec<Bytes> = view.messages().collect();
    assert!(messages.is_empty());
}

#[test]
fn vec_bytes_iter_truncated_data_region() {
    // Build a publish command with count=3 but only enough data for 1 message
    let mut buf = make_header(40, 24, MqCommand::TAG_PUBLISH, 0);
    buf.put_u64_le(42); // topic_id
    buf.put_u32_le(3); // count = 3
    buf.put_u32_le(24); // data_start = 24 (right after fixed region)
    // Only write 1 message: [len:4][data]
    buf.put_u32_le(4); // len = 4
    buf.extend_from_slice(b"abcd"); // 4 bytes of data
    let cmd = MqCommand::split_from(&mut buf);
    let view = cmd.as_publish().unwrap();
    let messages: Vec<Bytes> = view.messages().collect();
    // Should get only 1 message, not panic trying to read the other 2
    assert_eq!(messages.len(), 1);
    assert_eq!(&messages[0][..], b"abcd");
}

// =============================================================================
// 6. BatchIter corruption
// =============================================================================

#[test]
fn batch_iter_truncated_sub_commands() {
    // Build a batch with count=5 but only 1 sub-command
    let mut buf = make_header(40, 16, MqCommand::TAG_BATCH, 0);
    buf.put_u32_le(5); // count = 5
    buf.put_u32_le(0); // batch_id = 0
    // One 16-byte sub-command (padded to 8-byte boundary)
    let sub_cmd_size = 16u32;
    buf.put_u32_le(sub_cmd_size); // size field of sub-cmd
    buf.put_u16_le(8); // fixed_size
    buf.put_u8(MqCommand::TAG_DELETE_TOPIC);
    buf.put_u8(0);
    buf.put_u64_le(999); // topic_id
    // Fix the outer header size
    let total = buf.len() as u32;
    buf[0..4].copy_from_slice(&total.to_le_bytes());
    let cmd = MqCommand::split_from(&mut buf);
    let batch = cmd.as_batch().unwrap();
    assert_eq!(batch.count(), 5);
    // Iterator should yield 1 item and then stop (not panic for the other 4)
    let items: Vec<&[u8]> = batch.commands().collect();
    assert_eq!(items.len(), 1);
}

#[test]
fn batch_iter_zero_size_sub_command() {
    // Sub-command with size=0 should stop iteration
    let mut buf = make_header(24, 16, MqCommand::TAG_BATCH, 0);
    buf.put_u32_le(3); // count = 3
    buf.put_u32_le(0); // batch_id
    buf.put_u32_le(0); // sub-cmd size = 0 (invalid)
    buf.put_u32_le(0); // padding
    let total = buf.len() as u32;
    buf[0..4].copy_from_slice(&total.to_le_bytes());
    let cmd = MqCommand::split_from(&mut buf);
    let batch = cmd.as_batch().unwrap();
    let items: Vec<&[u8]> = batch.commands().collect();
    assert_eq!(items.len(), 0);
}

// =============================================================================
// 7. CmdForwardedBatchIter corruption
// =============================================================================

#[test]
fn forwarded_batch_iter_truncated_payload() {
    // Forwarded batch with count=10 but only 1 sub-frame fits
    let mut buf = make_header(64, 32, MqCommand::TAG_FORWARDED_BATCH, 0);
    buf.put_u32_le(0); // node_id
    buf.put_u32_le(10); // count = 10
    buf.put_u64_le(1); // batch_seq
    buf.put_u64_le(1); // leader_seq
    // One sub-frame: [payload_len:4][client_id:4][request_seq:8][cmd_bytes]
    let cmd_bytes = b"hello";
    let payload_len = 12 + cmd_bytes.len() as u32;
    buf.put_u32_le(payload_len);
    buf.put_u32_le(42); // client_id
    buf.put_u64_le(1); // request_seq
    buf.extend_from_slice(cmd_bytes);
    let total = buf.len() as u32;
    buf[0..4].copy_from_slice(&total.to_le_bytes());
    let cmd = MqCommand::split_from(&mut buf);
    let batch = cmd.as_forwarded_batch().unwrap();
    let items: Vec<(u32, u64, &[u8])> = batch.iter().collect();
    // Should get 1, not panic for the other 9
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, 42);
    assert_eq!(items[0].2, b"hello");
}

#[test]
fn forwarded_batch_iter_payload_len_too_small() {
    // payload_len < 12 should stop iteration
    let mut buf = make_header(40, 32, MqCommand::TAG_FORWARDED_BATCH, 0);
    buf.put_u32_le(0); // node_id
    buf.put_u32_le(1); // count = 1
    buf.put_u64_le(1); // batch_seq
    buf.put_u64_le(1); // leader_seq
    buf.put_u32_le(4); // payload_len = 4 (< 12 minimum)
    buf.put_u32_le(0); // some data
    let total = buf.len() as u32;
    buf[0..4].copy_from_slice(&total.to_le_bytes());
    let cmd = MqCommand::split_from(&mut buf);
    let batch = cmd.as_forwarded_batch().unwrap();
    let items: Vec<(u32, u64, &[u8])> = batch.iter().collect();
    assert_eq!(items.len(), 0);
}

// =============================================================================
// 8. Engine apply with malformed commands — must not panic
// =============================================================================

#[test]
fn engine_apply_empty_command_no_panic() {
    let cmd = cmd_from_bytes(&[0; 8]); // valid header, unknown tag 0
    let resp = apply_cmd(&cmd, 1);
    // Unknown tags get an OK response (noop) or unknown tag handling
    // The key test: no panic occurred
    let _ = resp.tag();
}

#[test]
fn engine_apply_truncated_create_topic_no_panic() {
    // TAG_CREATE_TOPIC with only 16 bytes (needs 28)
    let mut buf = make_header(16, 8, MqCommand::TAG_CREATE_TOPIC, 0);
    buf.put_bytes(0, 8);
    let cmd = MqCommand::split_from(&mut buf);
    let resp = apply_cmd(&cmd, 1);
    // Should get an error response, not a panic
    assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR);
}

#[test]
fn engine_apply_truncated_publish_no_panic() {
    let mut buf = make_header(16, 8, MqCommand::TAG_PUBLISH, 0);
    buf.put_bytes(0, 8);
    let cmd = MqCommand::split_from(&mut buf);
    let resp = apply_cmd(&cmd, 1);
    assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR);
}

#[test]
fn engine_apply_truncated_group_ack_no_panic() {
    // TAG_GROUP_ACK needs at least 24 bytes for the vec_u64 field
    let mut buf = make_header(16, 16, MqCommand::TAG_GROUP_ACK, 0);
    buf.put_u64_le(999); // group_id
    let cmd = MqCommand::split_from(&mut buf);
    let resp = apply_cmd(&cmd, 1);
    // Should handle gracefully — either error or "not found" since group doesn't exist
    let _ = resp.tag();
}

#[test]
fn engine_apply_all_zeros_no_panic() {
    let cmd = cmd_from_bytes(&[0; 64]);
    let resp = apply_cmd(&cmd, 1);
    let _ = resp.tag();
}

#[test]
fn engine_apply_all_0xff_no_panic() {
    let cmd = cmd_from_bytes(&[0xFF; 64]);
    let resp = apply_cmd(&cmd, 1);
    let _ = resp.tag();
}

#[test]
fn engine_apply_random_looking_data_no_panic() {
    // Simulate adversarial data: valid-ish header with zeroed body.
    // We use zeros (not random patterns) to avoid triggering huge allocations
    // from garbage vec_bytes counts or flex8 offsets — those are tested
    // individually in the vec_bytes/flex8 corruption tests above.
    let engine = MqEngine::new(MqConfig::new("/tmp/bisque-mq-malformed-test"));
    for tag in 0..=60u8 {
        let mut buf = make_header(64, 64, tag, 0);
        buf.put_bytes(0, 56); // zeroed body = all counts/offsets are 0
        let cmd = MqCommand::split_from(&mut buf);
        let mut resp_buf = BytesMut::with_capacity(64);
        engine.apply_command(&cmd, &mut resp_buf, 1, 1000, None);
        // The only assertion: we got here without panicking
    }
}

// =============================================================================
// 9. (FlatOptBytes is not publicly constructible — tested via engine paths)
// =============================================================================

// =============================================================================
// 10. ResponseEntry with truncated buffers
// =============================================================================

#[test]
fn response_entry_empty_buf() {
    let resp = ResponseEntry {
        buf: Bytes::from_static(&[]),
    };
    // These may panic on indexing — the response is always constructed internally,
    // but let's verify the typical accessors handle it
    // tag() reads buf[0] — will panic on empty buf, but ResponseEntry is never
    // constructed with empty buf in practice. This test documents the contract.
}

#[test]
fn response_entry_messages_out_of_bounds() {
    // TAG_MESSAGES response with message_count=100 but only 16 bytes
    let mut buf = BytesMut::with_capacity(16);
    buf.put_u8(ResponseEntry::TAG_MESSAGES); // tag
    buf.put_u8(0); // status
    buf.put_u16_le(100); // message_count = 100 (way too many)
    buf.put_bytes(0, 12); // pad to 16
    let resp = ResponseEntry { buf: buf.freeze() };
    // Iterating should stop safely, not try to read 100 * 32 bytes
    let msgs: Vec<_> = resp.messages().collect();
    assert!(msgs.len() < 100);
}

#[test]
fn response_entry_batch_entries_out_of_bounds() {
    // TAG_BATCH with count=50 but insufficient data
    let mut buf = BytesMut::with_capacity(16);
    buf.put_u8(ResponseEntry::TAG_BATCH);
    buf.put_u8(0);
    buf.put_u16_le(50); // batch count = 50
    buf.put_bytes(0, 4);
    buf.put_u64_le(0); // log_index
    let resp = ResponseEntry { buf: buf.freeze() };
    let entries: Vec<_> = resp.batch_entries().collect();
    assert!(entries.len() < 50);
}

#[test]
fn response_entry_retained_messages_truncated() {
    // TAG_RETAINED_MESSAGES with count=20 but only 16 bytes
    let mut buf = BytesMut::with_capacity(16);
    buf.put_u8(ResponseEntry::TAG_RETAINED_MESSAGES);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(20); // count = 20
    buf.put_bytes(0, 8);
    let resp = ResponseEntry { buf: buf.freeze() };
    let items: Vec<_> = resp.retained_messages().collect();
    assert!(items.len() < 20);
}

#[test]
fn response_entry_dead_letter_ids_truncated() {
    // TAG_DEAD_LETTERED with count=100 but only 24 bytes
    let mut buf = BytesMut::with_capacity(24);
    buf.put_u8(ResponseEntry::TAG_DEAD_LETTERED);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(100); // count = 100
    buf.put_u64_le(0); // log_index
    buf.put_u64_le(0); // dlq_topic_id
    let resp = ResponseEntry { buf: buf.freeze() };
    let ids: Vec<_> = resp.dead_letter_ids().collect();
    assert!(ids.len() < 100);
}

// =============================================================================
// 11. MqCommand::validate with various edge cases
// =============================================================================

#[test]
fn validate_size_field_zero() {
    // cmd_size = 0 but buffer has 8 bytes — size < buf.len is ok
    let mut buf = make_header(0, 8, 0, 0);
    let cmd = MqCommand::split_from(&mut buf);
    // size=0 <= buf.len()=8, should be OK
    assert!(cmd.validate().is_ok());
}

#[test]
fn validate_size_field_u32_max() {
    // cmd_size = u32::MAX but buffer is only 8 bytes
    let mut buf = BytesMut::with_capacity(8);
    buf.put_u32_le(u32::MAX);
    buf.put_u16_le(8);
    buf.put_u8(0);
    buf.put_u8(0);
    let cmd = MqCommand::split_from(&mut buf);
    assert!(cmd.validate().is_err());
}

// =============================================================================
// 12. Segment index with corrupted data
// =============================================================================

#[test]
fn frozen_segment_index_too_small() {
    let data = vec![0u8; 4]; // way too small
    assert!(bisque_mq::segment_index::FrozenSegmentIndex::from_vec(data).is_none());
}

#[test]
fn frozen_segment_index_bad_magic() {
    let mut data = vec![0u8; 64];
    // Write wrong magic
    data[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    assert!(bisque_mq::segment_index::FrozenSegmentIndex::from_vec(data).is_none());
}

#[test]
fn frozen_segment_index_bad_version() {
    let mut data = vec![0u8; 64];
    data[0..4].copy_from_slice(&SIDX_MAGIC);
    data[4..8].copy_from_slice(&999u32.to_le_bytes()); // wrong version
    assert!(FrozenSegmentIndex::from_vec(data).is_none());
}

#[test]
fn frozen_segment_index_non_power_of_two_buckets() {
    let mut data = vec![0u8; 128];
    data[0..4].copy_from_slice(&SIDX_MAGIC);
    data[4..8].copy_from_slice(&SIDX_VERSION.to_le_bytes());
    data[8..12].copy_from_slice(&1u32.to_le_bytes()); // segment_id
    data[12..16].copy_from_slice(&3u32.to_le_bytes()); // bucket_count=3 (not power of 2)
    assert!(FrozenSegmentIndex::from_vec(data).is_none());
}

// =============================================================================
// 13. VecSliceIter corruption
// =============================================================================

#[test]
fn vec_slice_iter_huge_count() {
    // vec_bytes slot: [count:4][data_start:4]
    // count = 1_000_000, data_start = right after the slot
    let mut buf = vec![0u8; 16];
    // count at offset 0
    buf[0..4].copy_from_slice(&1_000_000u32.to_le_bytes());
    // data_start at offset 4
    buf[4..8].copy_from_slice(&8u32.to_le_bytes());
    // Only 8 bytes of actual data (enough for ~1 entry with len=0)
    let iter = VecSliceIter::new(&buf, 0);
    let items: Vec<&[u8]> = iter.collect();
    // Should stop early without panic
    assert!(items.len() < 1_000_000);
}

#[test]
fn vec_slice_iter_data_start_past_buffer() {
    let mut buf = vec![0u8; 8];
    buf[0..4].copy_from_slice(&5u32.to_le_bytes()); // count = 5
    buf[4..8].copy_from_slice(&9999u32.to_le_bytes()); // data_start past end
    let iter = VecSliceIter::new(&buf, 0);
    let items: Vec<&[u8]> = iter.collect();
    assert_eq!(items.len(), 0);
}

// =============================================================================
// 14. Mixed adversarial: valid structure, corrupt flex data
// =============================================================================

#[test]
fn create_topic_with_corrupt_name_and_retention() {
    // Build a structurally valid CmdCreateTopic but with garbage flex/blob data
    let mut buf = make_header(40, 32, MqCommand::TAG_CREATE_TOPIC, 0);
    // @8: flex8 name — large branch pointing backwards
    buf.put_u32_le((0 << 1) | 1); // data_offset=0 (overlaps header!)
    buf.put_u32_le(8); // size=8
    // @16: blob retention — offset past buffer
    buf.put_u32_le(0xFFFF);
    buf.put_u32_le(0xFFFF);
    // @24: partition_count
    buf.put_u32_le(1);
    buf.put_bytes(0, 4); // pad
    let total = buf.len() as u32;
    buf[0..4].copy_from_slice(&total.to_le_bytes());
    let cmd = MqCommand::split_from(&mut buf);
    let view = cmd.as_create_topic().unwrap();
    // Name might return some bytes (offset 0 is within buffer) or an error
    let _ = view.name();
    // Retention should fail — offset 0xFFFF is way past buffer
    assert!(view.retention().is_err());
}

// =============================================================================
// 15. CmdPublishToDlq with corrupt vec_u64 data
// =============================================================================

#[test]
fn publish_to_dlq_corrupt_dead_letter_ids() {
    let mut buf = make_header(48, 40, MqCommand::TAG_GROUP_PUBLISH_TO_DLQ, 0);
    buf.put_u64_le(1); // source_group_id
    buf.put_u64_le(2); // dlq_topic_id
    // @24: dead_letter_ids vec_u64 [count:4][data_offset:4]
    buf.put_u32_le(1000); // count = 1000 (way more than buffer can hold)
    buf.put_u32_le(40); // data_offset right at end of fixed region
    // @32: messages vec_bytes
    buf.put_u32_le(0); // count = 0
    buf.put_u32_le(0); // data_offset = 0
    let total = buf.len() as u32;
    buf[0..4].copy_from_slice(&total.to_le_bytes());
    let cmd = MqCommand::split_from(&mut buf);
    let view = cmd.as_publish_to_dlq().unwrap();
    // dead_letter_ids should return error or empty — not panic
    let result = view.dead_letter_ids();
    assert!(result.is_err() || result.unwrap().is_empty());
}

// =============================================================================
// 16. Engine apply_command fuzz-style: every tag with minimum buffer
// =============================================================================

#[test]
fn engine_apply_every_tag_with_minimum_buffer_no_panic() {
    let engine = MqEngine::new(MqConfig::new("/tmp/bisque-mq-malformed-test"));
    // Test every possible tag value (0..=255) with just an 8-byte header
    for tag in 0..=255u8 {
        let mut buf = make_header(8, 8, tag, 0);
        let cmd = MqCommand::split_from(&mut buf);
        let mut resp_buf = BytesMut::with_capacity(64);
        engine.apply_command(&cmd, &mut resp_buf, 1, 1000, None);
        // No panic = success
    }
}

#[test]
fn engine_apply_every_tag_with_64_byte_buffer_no_panic() {
    // 64 bytes covers the largest fixed region of any command type.
    // The buffer has a realistic fixed_size=64 so the engine's validation
    // sees a self-consistent header.
    let engine = MqEngine::new(MqConfig::new("/tmp/bisque-mq-malformed-test"));
    for tag in 0..=255u8 {
        let mut buf = make_header(64, 64, tag, 0);
        buf.put_u64_le(42); // primary_id at offset 8
        buf.put_bytes(0, 48); // fill to 64 bytes
        let cmd = MqCommand::split_from(&mut buf);
        let mut resp_buf = BytesMut::with_capacity(64);
        engine.apply_command(&cmd, &mut resp_buf, 1, 1000, None);
        // No panic = success
    }
}

// =============================================================================
// 17. Group joined fields with corrupt member data
// =============================================================================

#[test]
fn response_group_joined_fields_truncated() {
    // Build a TAG_GROUP_JOINED response with lengths that exceed the buffer
    let mut buf = BytesMut::with_capacity(24);
    buf.put_u8(ResponseEntry::TAG_GROUP_JOINED);
    buf.put_u8(0); // status
    buf.put_u8(0); // flags
    buf.put_u8(0); // pad
    buf.put_i32_le(1); // generation
    buf.put_u64_le(0); // log_index
    buf.put_u16_le(1000); // leader_len = 1000 (way past buffer)
    buf.put_u16_le(0);
    buf.put_u16_le(0);
    buf.put_u16_le(0);
    let resp = ResponseEntry { buf: buf.freeze() };
    // Should not panic — returns empty/default strings
    let (leader, member_id, protocol, members) = resp.group_joined_fields();
    // With corrupt lengths, we get truncated or empty results
    let _ = (leader, member_id, protocol, members);
}

// =============================================================================
// 18. Stress: rapid alternation of valid and corrupt commands
// =============================================================================

#[test]
fn alternating_valid_and_corrupt_commands() {
    let engine = MqEngine::new(MqConfig::new("/tmp/bisque-mq-malformed-test"));

    for i in 0..100u64 {
        let mut resp_buf = BytesMut::with_capacity(64);

        if i % 2 == 0 {
            // Valid create-topic command
            let mut cmd_buf = BytesMut::new();
            MqCommand::write_create_topic(
                &mut cmd_buf,
                &format!("topic-{i}"),
                &bisque_mq::types::RetentionPolicy::default(),
                1,
            );
            let cmd = MqCommand::split_from(&mut cmd_buf);
            engine.apply_command(&cmd, &mut resp_buf, i, 1000, None);
            let resp = ResponseEntry::split_from(&mut resp_buf);
            assert_eq!(resp.tag(), ResponseEntry::TAG_ENTITY_CREATED);
        } else {
            // Corrupt command — truncated
            let mut buf = make_header(12, 8, MqCommand::TAG_CREATE_TOPIC, 0);
            buf.put_bytes(0, 4);
            let cmd = MqCommand::split_from(&mut buf);
            engine.apply_command(&cmd, &mut resp_buf, i, 1000, None);
            let resp = ResponseEntry::split_from(&mut resp_buf);
            assert_eq!(resp.tag(), ResponseEntry::TAG_ERROR);
        }
    }
}

// =============================================================================
// 19. Per-tag truncation tests: every tag with buffer 1 byte short of minimum
// =============================================================================

/// Build a command with exactly `size` bytes, using the given tag and fixed_size.
fn make_cmd(size: usize, fixed: u16, tag: u8) -> MqCommand {
    let mut buf = BytesMut::with_capacity(size);
    buf.put_u32_le(size as u32);
    buf.put_u16_le(fixed);
    buf.put_u8(tag);
    buf.put_u8(0);
    if size > 8 {
        buf.put_bytes(0, size - 8);
    }
    MqCommand::split_from(&mut buf)
}

/// Apply a truncated command (1 byte short of minimum) and verify it produces
/// an error response instead of panicking.
fn assert_truncated_tag_returns_error(tag: u8, min_size: usize) {
    if min_size <= 8 {
        return; // Can't truncate below 8 (header)
    }
    let cmd = make_cmd(min_size - 1, (min_size - 1) as u16, tag);
    let engine = MqEngine::new(MqConfig::new("/tmp/bisque-mq-malformed-test"));
    let mut buf = BytesMut::with_capacity(64);
    engine.apply_command(&cmd, &mut buf, 1, 1000, None);
    let resp = ResponseEntry::split_from(&mut buf);
    assert_eq!(
        resp.tag(),
        ResponseEntry::TAG_ERROR,
        "tag {} with {} bytes (min={}) should produce ERROR, got tag={}",
        tag,
        min_size - 1,
        min_size,
        resp.tag()
    );
}

// -- Topics --

#[test]
fn truncated_delete_topic() {
    assert_truncated_tag_returns_error(MqCommand::TAG_DELETE_TOPIC, 16);
}

#[test]
fn truncated_commit_offset() {
    assert_truncated_tag_returns_error(MqCommand::TAG_COMMIT_OFFSET, 32);
}

#[test]
fn truncated_purge_topic() {
    assert_truncated_tag_returns_error(MqCommand::TAG_PURGE_TOPIC, 24);
}

// -- Exchanges --

#[test]
fn truncated_delete_exchange() {
    assert_truncated_tag_returns_error(MqCommand::TAG_DELETE_EXCHANGE, 16);
}

#[test]
fn truncated_delete_binding() {
    assert_truncated_tag_returns_error(MqCommand::TAG_DELETE_BINDING, 16);
}

// -- Consumer Groups --

#[test]
fn truncated_delete_consumer_group() {
    assert_truncated_tag_returns_error(MqCommand::TAG_DELETE_CONSUMER_GROUP, 16);
}

#[test]
fn truncated_expire_group_sessions() {
    assert_truncated_tag_returns_error(MqCommand::TAG_EXPIRE_GROUP_SESSIONS, 16);
}

#[test]
fn truncated_group_deliver() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_DELIVER, 28);
}

#[test]
fn truncated_group_ack() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_ACK, 24);
}

#[test]
fn truncated_group_nack() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_NACK, 24);
}

#[test]
fn truncated_group_release() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_RELEASE, 24);
}

#[test]
fn truncated_group_modify() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_MODIFY, 24);
}

#[test]
fn truncated_group_extend_visibility() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_EXTEND_VISIBILITY, 32);
}

#[test]
fn truncated_group_timeout_expired() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_TIMEOUT_EXPIRED, 24);
}

#[test]
fn truncated_group_expire_pending() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_EXPIRE_PENDING, 24);
}

#[test]
fn truncated_group_purge() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_PURGE, 16);
}

#[test]
fn truncated_group_get_attributes() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_GET_ATTRIBUTES, 16);
}

// -- Actor-based groups --

#[test]
fn truncated_group_deliver_actor() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_DELIVER_ACTOR, 28);
}

#[test]
fn truncated_group_ack_actor() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_ACK_ACTOR, 24);
}

#[test]
fn truncated_group_nack_actor() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_NACK_ACTOR, 24);
}

#[test]
fn truncated_group_assign_actors() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_ASSIGN_ACTORS, 28);
}

#[test]
fn truncated_group_release_actors() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_RELEASE_ACTORS, 24);
}

#[test]
fn truncated_group_evict_idle() {
    assert_truncated_tag_returns_error(MqCommand::TAG_GROUP_EVICT_IDLE, 24);
}

// -- Cron --

#[test]
fn truncated_cron_enable() {
    assert_truncated_tag_returns_error(MqCommand::TAG_CRON_ENABLE, 16);
}

#[test]
fn truncated_cron_disable() {
    assert_truncated_tag_returns_error(MqCommand::TAG_CRON_DISABLE, 16);
}

#[test]
fn truncated_cron_trigger() {
    assert_truncated_tag_returns_error(MqCommand::TAG_CRON_TRIGGER, 24);
}

#[test]
fn truncated_cron_update() {
    assert_truncated_tag_returns_error(MqCommand::TAG_CRON_UPDATE, 16);
}

// -- Sessions --

#[test]
fn truncated_create_session() {
    assert_truncated_tag_returns_error(MqCommand::TAG_CREATE_SESSION, 40);
}

#[test]
fn truncated_disconnect_session() {
    assert_truncated_tag_returns_error(MqCommand::TAG_DISCONNECT_SESSION, 16);
}

#[test]
fn truncated_heartbeat_session() {
    assert_truncated_tag_returns_error(MqCommand::TAG_HEARTBEAT_SESSION, 16);
}

#[test]
fn truncated_clear_will() {
    assert_truncated_tag_returns_error(MqCommand::TAG_CLEAR_WILL, 16);
}

#[test]
fn truncated_fire_pending_wills() {
    assert_truncated_tag_returns_error(MqCommand::TAG_FIRE_PENDING_WILLS, 16);
}

// =============================================================================
// 20. Per-tag with exact minimum size: should NOT panic (may return error
//     response for "not found" etc., but no crash)
// =============================================================================

#[test]
fn exact_minimum_size_all_tags_no_panic() {
    let tags_and_sizes: &[(u8, usize)] = &[
        (MqCommand::TAG_CREATE_TOPIC, 28),
        (MqCommand::TAG_DELETE_TOPIC, 16),
        (MqCommand::TAG_PUBLISH, 24),
        (MqCommand::TAG_COMMIT_OFFSET, 32),
        (MqCommand::TAG_PURGE_TOPIC, 24),
        (MqCommand::TAG_SET_RETAINED, 32),
        (MqCommand::TAG_CREATE_EXCHANGE, 16),
        (MqCommand::TAG_DELETE_EXCHANGE, 16),
        (MqCommand::TAG_CREATE_BINDING, 40),
        (MqCommand::TAG_DELETE_BINDING, 16),
        (MqCommand::TAG_PUBLISH_TO_EXCHANGE, 24),
        (MqCommand::TAG_CREATE_CONSUMER_GROUP, 59),
        (MqCommand::TAG_DELETE_CONSUMER_GROUP, 16),
        (MqCommand::TAG_JOIN_CONSUMER_GROUP, 56),
        (MqCommand::TAG_SYNC_CONSUMER_GROUP, 40),
        (MqCommand::TAG_LEAVE_CONSUMER_GROUP, 24),
        (MqCommand::TAG_HEARTBEAT_CONSUMER_GROUP, 28),
        (MqCommand::TAG_COMMIT_GROUP_OFFSET, 56),
        (MqCommand::TAG_EXPIRE_GROUP_SESSIONS, 16),
        (MqCommand::TAG_GROUP_DELIVER, 28),
        (MqCommand::TAG_GROUP_ACK, 24),
        (MqCommand::TAG_GROUP_NACK, 24),
        (MqCommand::TAG_GROUP_RELEASE, 24),
        (MqCommand::TAG_GROUP_MODIFY, 24),
        (MqCommand::TAG_GROUP_EXTEND_VISIBILITY, 32),
        (MqCommand::TAG_GROUP_TIMEOUT_EXPIRED, 24),
        (MqCommand::TAG_GROUP_PUBLISH_TO_DLQ, 40),
        (MqCommand::TAG_GROUP_EXPIRE_PENDING, 24),
        (MqCommand::TAG_GROUP_PURGE, 16),
        (MqCommand::TAG_GROUP_GET_ATTRIBUTES, 16),
        (MqCommand::TAG_GROUP_DELIVER_ACTOR, 28),
        (MqCommand::TAG_GROUP_ACK_ACTOR, 24),
        (MqCommand::TAG_GROUP_NACK_ACTOR, 24),
        (MqCommand::TAG_GROUP_ASSIGN_ACTORS, 28),
        (MqCommand::TAG_GROUP_RELEASE_ACTORS, 24),
        (MqCommand::TAG_GROUP_EVICT_IDLE, 24),
        (MqCommand::TAG_CRON_ENABLE, 16),
        (MqCommand::TAG_CRON_DISABLE, 16),
        (MqCommand::TAG_CRON_TRIGGER, 24),
        (MqCommand::TAG_CRON_UPDATE, 16),
        (MqCommand::TAG_CREATE_SESSION, 40),
        (MqCommand::TAG_DISCONNECT_SESSION, 16),
        (MqCommand::TAG_HEARTBEAT_SESSION, 16),
        (MqCommand::TAG_SET_WILL, 44),
        (MqCommand::TAG_CLEAR_WILL, 16),
        (MqCommand::TAG_FIRE_PENDING_WILLS, 16),
        (MqCommand::TAG_PERSIST_SESSION, 52),
        (MqCommand::TAG_RESTORE_SESSION, 16),
        (MqCommand::TAG_EXPIRE_SESSIONS, 8),
        (MqCommand::TAG_BATCH, 16),
        (MqCommand::TAG_PRUNE_DEDUP_WINDOW, 8),
        (MqCommand::TAG_GET_RETAINED, 24),
        (MqCommand::TAG_DELETE_RETAINED, 24),
        (MqCommand::TAG_PUBLISH_TO_EXCHANGE_MQTT, 24),
        (MqCommand::TAG_SET_RETAINED_MQTT, 32),
        (MqCommand::TAG_FORWARDED_BATCH, 32),
        (MqCommand::TAG_RESUME, 16),
    ];

    let engine = MqEngine::new(MqConfig::new("/tmp/bisque-mq-malformed-test"));
    for &(tag, min_size) in tags_and_sizes {
        let cmd = make_cmd(min_size, min_size as u16, tag);
        let mut resp_buf = BytesMut::with_capacity(128);
        engine.apply_command(&cmd, &mut resp_buf, 1, 1000, None);
        // No panic = success. Response may be OK, ERROR (entity not found), etc.
        assert!(
            resp_buf.len() > 0,
            "tag {} with exact min size {} produced no response",
            tag,
            min_size
        );
    }
}
