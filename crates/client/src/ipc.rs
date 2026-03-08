//! Arrow IPC encoding/decoding helpers for the bisque client.

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use bytes::Bytes;

/// Encode an Arrow Schema to IPC streaming format bytes.
///
/// Uses the IPC stream format with zero batches — the schema is embedded
/// in the stream header.
pub fn schema_to_ipc(schema: &Schema) -> Result<Bytes, arrow::error::ArrowError> {
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
pub fn schema_from_ipc(data: &[u8]) -> Result<Schema, arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    Ok((*reader.schema()).clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

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
