//! Test module to isolate and verify the record format implementation.
//! Tests the binary format: [len(4)][type(1)][group_id(3)][payload][crc64(8)]

use super::*;
use crate::multi::type_config::ManiacRaftTypeConfig;
use std::io;
use tempfile::TempDir;

#[derive(Debug, Clone)]
struct TestData(pub Vec<u8>);

impl openraft::RaftTypeConfig for TestData {
    type D = TestData;
    type R = ();
    type NodeId = u64;
    type Term = u64;
}

type TestConfig = ManiacRaftTypeConfig<TestData, ()>;

fn setup_test_dir() -> TempDir {
    TempDir::new().unwrap()
}

#[test]
fn test_record_format_basic() {
    // Test basic record encoding and decoding
    let mut buf = Vec::new();
    let group_id: u64 = 42;
    let payload: &[u8] = b"test payload";

    // Encode record
    let total_size = encode_record_into(&mut buf, RecordType::Entry, group_id, payload);

    // Verify minimum size
    assert!(total_size >= MIN_RECORD_SIZE);
    assert_eq!(buf.len(), total_size);

    // Decode record (skip length prefix)
    let record_data = &buf[LENGTH_SIZE..];
    let parsed = validate_record(record_data).unwrap();

    assert_eq!(parsed.record_type, RecordType::Entry);
    assert_eq!(parsed.group_id, group_id);
    assert_eq!(parsed.payload, payload);
}

#[test]
fn test_record_format_empty_payload() {
    // Test record with empty payload
    let mut buf = Vec::new();
    let group_id: u64 = 1;

    let total_size = encode_record_into(&mut buf, RecordType::Vote, group_id, &[]);

    // Should be exactly minimum size
    assert_eq!(total_size, MIN_RECORD_SIZE);
    assert_eq!(buf.len(), MIN_RECORD_SIZE);

    // Verify can be decoded
    let record_data = &buf[LENGTH_SIZE..];
    let parsed = validate_record(record_data).unwrap();

    assert_eq!(parsed.record_type, RecordType::Vote);
    assert_eq!(parsed.group_id, group_id);
    assert_eq!(parsed.payload.len(), 0);
}

#[test]
fn test_record_format_max_group_id() {
    // Test maximum 24-bit group_id
    let mut buf = Vec::new();
    let max_group_id: u64 = 0xFF_FFFF; // 24-bit max
    let payload = b"test";

    encode_record_into(&mut buf, RecordType::Purge, max_group_id, payload);

    let record_data = &buf[LENGTH_SIZE..];
    let parsed = validate_record(record_data).unwrap();

    assert_eq!(parsed.group_id, max_group_id);
}

#[test]
fn test_record_format_large_payload() {
    // Test record with large payload (10KB)
    let mut buf = Vec::new();
    let group_id: u64 = 123;
    let payload = vec![0x42u8; 10 * 1024];

    let total_size = encode_record_into(&mut buf, RecordType::Entry, group_id, &payload);

    // Verify size includes payload
    let expected_size = LENGTH_SIZE + 1 + GROUP_ID_SIZE + payload.len() + CRC64_SIZE;
    assert_eq!(total_size, expected_size);

    // Verify round-trip
    let record_data = &buf[LENGTH_SIZE..];
    let parsed = validate_record(record_data).unwrap();

    assert_eq!(parsed.group_id, group_id);
    assert_eq!(parsed.payload, &payload[..]);
}

#[test]
fn test_record_format_crc_corruption() {
    // Test CRC validation catches corruption
    let mut buf = Vec::new();
    let group_id: u64 = 5;
    let payload = b"corruption test";

    encode_record_into(&mut buf, RecordType::Truncate, group_id, payload);

    // Corrupt a byte in the payload
    let mut corrupted = buf.clone();
    let payload_start = LENGTH_SIZE + 1 + GROUP_ID_SIZE;
    corrupted[payload_start + 5] ^= 0xFF;

    let record_data = &corrupted[LENGTH_SIZE..];
    let result = validate_record(record_data);

    assert!(result.is_err(), "CRC should detect corruption");
}

#[test]
fn test_record_format_all_types() {
    // Test encoding/decoding all record types
    let test_cases = [
        (RecordType::Vote, 1u64, b"votedata".as_slice()),
        (RecordType::Entry, 2, b"entrydata"),
        (RecordType::Truncate, 3, b"truncatedata"),
        (RecordType::Purge, 4, b"purgedata"),
        (RecordType::WalFrame, 5, b"waldata"),
        (RecordType::Changeset, 6, b"changesetdata"),
        (RecordType::Checkpoint, 7, b"checkpointdata"),
        (RecordType::WalMetadata, 8, b"metadata"),
    ];

    for (record_type, group_id, payload) in test_cases {
        let mut buf = Vec::new();
        encode_record_into(&mut buf, record_type, group_id, payload);

        let record_data = &buf[LENGTH_SIZE..];
        let parsed = validate_record(record_data).unwrap();

        assert_eq!(
            parsed.record_type, record_type,
            "type mismatch for {:?}",
            record_type
        );
        assert_eq!(
            parsed.group_id, group_id,
            "group_id mismatch for {:?}",
            record_type
        );
        assert_eq!(
            parsed.payload, payload,
            "payload mismatch for {:?}",
            record_type
        );
    }
}

#[test]
fn test_record_format_length_prefix() {
    // Verify length prefix is correct
    let mut buf = Vec::new();
    let group_id: u64 = 99;
    let payload = b"verify length prefix";

    let total_size = encode_record_into(&mut buf, RecordType::Entry, group_id, payload);

    // Read length prefix
    let len_bytes: [u8; 4] = buf[0..4].try_into().unwrap();
    let record_len = u32::from_le_bytes(len_bytes) as usize;

    // Calculate expected length (type + group_id + payload + crc)
    let expected_len = 1 + GROUP_ID_SIZE + payload.len() + CRC64_SIZE;
    assert_eq!(record_len, expected_len);

    // Total buffer size should be length + length_prefix
    assert_eq!(buf.len(), LENGTH_SIZE + record_len);
}

#[tokio::test]
async fn test_record_format_on_disk() {
    // Test writing and reading records from an actual file
    let dir = setup_test_dir();
    let path = dir.path().join("test.log");

    // Open file for writing
    let file = open_segment_file(&path, false, true, true, true)
        .await
        .unwrap();

    // Write several records
    let records = vec![
        (RecordType::Vote, 1u64, b"vote1".as_slice()),
        (RecordType::Entry, 2, b"entry1"),
        (RecordType::Vote, 3, b"vote2"),
        (RecordType::Entry, 4, b"entry2"),
    ];

    let mut write_buf = Vec::new();
    let mut dio_scratch = AlignedBuf::default();

    for (record_type, group_id, payload) in &records {
        encode_record_into(&mut write_buf, *record_type, *group_id, payload);
    }

    // Write all at once
    file.write_all_at(&write_buf, 0).await.unwrap();

    // Now read back using RecordIterator
    let file = open_segment_file(&path, true, false, false, false)
        .await
        .unwrap();
    let mut iterator = RecordIterator::new(
        &file,
        0,
        write_buf.len() as u64,
        DEFAULT_CHUNK_SIZE,
        &mut dio_scratch,
    );

    iterator.read_chunk().await.unwrap();

    let mut read_count = 0;
    while let Some((offset, parsed)) = iterator.next_record().await.unwrap() {
        assert_eq!(
            offset,
            (LENGTH_SIZE + 1 + GROUP_ID_SIZE + records[read_count].2.len() + CRC64_SIZE) as u64
                * read_count as u64
        );
        assert_eq!(parsed.record_type, records[read_count].0);
        assert_eq!(parsed.group_id, records[read_count].1);
        assert_eq!(parsed.payload, records[read_count].2);
        read_count += 1;
    }

    assert_eq!(read_count, records.len(), "Should read all records");
}

#[test]
fn test_group_id_24bit_limits() {
    // Test that group_id is properly truncated to 24 bits
    let test_cases = vec![
        (0u64, 0u64),           // min value
        (1, 1),                 // small value
        (0xFF, 0xFF),           // 8-bit max
        (0xFFFF, 0xFFFF),       // 16-bit max
        (0xFFFFFF, 0xFFFFFF),   // 24-bit max
        (0x1FFFFFF, 0xFFFFFF),  // exceeds 24-bit (should truncate)
        (0xFFFFFFFF, 0xFFFFFF), // way exceeds 24-bit
    ];

    for (input, expected) in test_cases {
        // Test write_u24_le and read_u24_le
        let mut buf = [0u8; 3];
        write_u24_le(&mut buf, 0, input);
        let output = read_u24_le(&buf, 0);
        assert_eq!(output, expected, "Failed for input: {}", input);
    }
}

#[tokio::test]
async fn test_record_boundary_conditions() {
    // Test record exactly at MIN_RECORD_SIZE
    let dir = setup_test_dir();
    let path = dir.path().join("boundary_test.log");

    let file = open_segment_file(&path, false, true, true, true)
        .await
        .unwrap();

    // Record with empty payload = exactly MIN_RECORD_SIZE
    let mut buf = Vec::new();
    encode_record_into(&mut buf, RecordType::Vote, 1, &[]);
    assert_eq!(buf.len(), MIN_RECORD_SIZE);

    file.write_all_at(&buf, 0).await.unwrap();

    // Read it back
    let record_data = &buf[LENGTH_SIZE..];
    let parsed = validate_record(record_data).unwrap();
    assert_eq!(parsed.record_type, RecordType::Vote);
    assert_eq!(parsed.group_id, 1);
    assert_eq!(parsed.payload.len(), 0);
}
