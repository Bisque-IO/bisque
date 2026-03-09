//! Arrow IPC encoding/decoding helpers.
//!
//! RecordBatches are serialized to IPC format before being placed into Raft log entries.
//! This keeps the Raft layer schema-agnostic and enables zero-copy-friendly payloads.

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use bytes::Bytes;

use crate::error::{Error, Result};

/// Encode one or more RecordBatches into Arrow IPC streaming format bytes.
///
/// Returns `Bytes` directly — callers on the hot path always need `Bytes`,
/// and this avoids an intermediate `Vec<u8>` → `Bytes` conversion.
/// Pre-allocates based on array memory size to minimize reallocs.
pub fn encode_record_batches(batches: &[RecordBatch]) -> Result<Bytes> {
    if batches.is_empty() {
        return Ok(Bytes::new());
    }

    let schema = batches[0].schema();
    // Pre-allocate with a size hint from array memory sizes.
    let size_hint: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    let mut buf = Vec::with_capacity(size_hint);

    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    Ok(Bytes::from(buf))
}

/// Encode RecordBatches into a WebSocket binary frame with a header prefix.
///
/// Frame format: `[header bytes] ++ [Arrow IPC stream bytes]`
/// Single allocation — the header is written first, then IPC data is appended
/// directly into the same buffer. Returns `Vec<u8>` to avoid a copy when the
/// caller needs an owned `Vec` (e.g. for `fastwebsockets::Payload::Owned`).
pub fn encode_record_batches_framed(header: &[u8], batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(header.to_vec());
    }

    let schema = batches[0].schema();
    let size_hint: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    let mut buf = Vec::with_capacity(header.len() + size_hint);
    buf.extend_from_slice(header);

    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    Ok(buf)
}

/// Decode Arrow IPC streaming format bytes into RecordBatches.
pub fn decode_record_batches(data: &[u8]) -> Result<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let reader =
        StreamReader::try_new(std::io::Cursor::new(data), None).map_err(|e| Error::Arrow(e))?;

    let batches: Result<Vec<_>> = reader.map(|r| r.map_err(Error::Arrow)).collect();
    batches
}

/// Decode Arrow IPC streaming format bytes, extending into an existing Vec.
///
/// Avoids allocating a second `Vec` when the caller already has one to fill.
pub fn decode_record_batches_into(data: &[u8], out: &mut Vec<RecordBatch>) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    let reader =
        StreamReader::try_new(std::io::Cursor::new(data), None).map_err(|e| Error::Arrow(e))?;

    for batch_result in reader {
        out.push(batch_result?);
    }

    Ok(())
}

/// Encode an Arrow Schema to IPC streaming format bytes.
///
/// Uses the IPC stream format with zero batches — the schema is embedded
/// in the stream header.
pub fn schema_to_ipc(schema: &Schema) -> Result<Bytes> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.finish()?;
    }
    Ok(Bytes::from(buf))
}

/// Decode an Arrow Schema from IPC streaming format bytes.
///
/// Reads the schema from the IPC stream header (ignores any batches).
pub fn schema_from_ipc(data: &[u8]) -> Result<Schema> {
    let reader =
        StreamReader::try_new(std::io::Cursor::new(data), None).map_err(|e| Error::Arrow(e))?;
    Ok((*reader.schema()).clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids: Vec<i32> = (0..n as i32).collect();
        let names: Vec<String> = (0..n).map(|i| format!("row_{}", i)).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn roundtrip_single_batch() {
        let batch = make_test_batch(10);
        let encoded = encode_record_batches(&[batch.clone()]).unwrap();
        let decoded = decode_record_batches(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], batch);
    }

    #[test]
    fn roundtrip_multiple_batches() {
        let batch1 = make_test_batch(5);
        let batch2 = make_test_batch(3);
        let encoded = encode_record_batches(&[batch1.clone(), batch2.clone()]).unwrap();
        let decoded = decode_record_batches(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], batch1);
        assert_eq!(decoded[1], batch2);
    }

    #[test]
    fn empty_roundtrip() {
        let encoded = encode_record_batches(&[]).unwrap();
        assert!(encoded.is_empty());
        let decoded = decode_record_batches(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn ws_binary_frame_roundtrip() {
        // Simulates the WebSocket binary frame protocol:
        // [4-byte request_id (big-endian)] ++ [Arrow IPC stream bytes]
        let batch = make_test_batch(5);
        let request_id: u32 = 42;

        // Encode (server-side)
        let ipc_bytes = encode_record_batches(&[batch.clone()]).unwrap();
        let mut frame = Vec::with_capacity(4 + ipc_bytes.len());
        frame.extend_from_slice(&request_id.to_be_bytes());
        frame.extend_from_slice(&ipc_bytes);

        // Decode (client-side)
        assert!(frame.len() > 4);
        let decoded_request_id = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
        assert_eq!(decoded_request_id, request_id);

        let ipc_payload = &frame[4..];
        let decoded = decode_record_batches(ipc_payload).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], batch);
    }

    #[test]
    fn encode_framed_empty_batches() {
        let header = 42u32.to_be_bytes();
        let result = encode_record_batches_framed(&header, &[]).unwrap();
        assert_eq!(result, header);
    }

    #[test]
    fn encode_framed_roundtrip() {
        let header = 0xCAFEu32.to_be_bytes();
        let batch = make_test_batch(5);
        let frame = encode_record_batches_framed(&header, &[batch.clone()]).unwrap();

        // Header preserved
        assert_eq!(&frame[..4], &header);
        // IPC payload decodes correctly
        let decoded = decode_record_batches(&frame[4..]).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], batch);
    }

    #[test]
    fn ws_binary_frame_multiple_batches() {
        // Multiple batches in a single IPC stream frame
        let batch1 = make_test_batch(3);
        let batch2 = make_test_batch(7);
        let request_id: u32 = 0xDEAD_BEEF;

        let ipc_bytes = encode_record_batches(&[batch1.clone(), batch2.clone()]).unwrap();
        let mut frame = Vec::with_capacity(4 + ipc_bytes.len());
        frame.extend_from_slice(&request_id.to_be_bytes());
        frame.extend_from_slice(&ipc_bytes);

        let decoded_request_id = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
        assert_eq!(decoded_request_id, 0xDEAD_BEEF);

        let decoded = decode_record_batches(&frame[4..]).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], batch1);
        assert_eq!(decoded[1], batch2);
    }

    #[test]
    fn schema_roundtrip() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);
        let encoded = schema_to_ipc(&schema).unwrap();
        let decoded = schema_from_ipc(&encoded).unwrap();
        assert_eq!(schema, decoded);
    }
}
