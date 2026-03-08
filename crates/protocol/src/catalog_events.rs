//! Catalog event types shared between server and client.
//!
//! [`CatalogEvent`] and [`CatalogEventKind`] describe mutations to the catalog
//! (table create/drop, segment seal, version bump, promote to deep storage).
//! These types are serialized to JSON for the WebSocket protocol and to bincode
//! for MDBX persistence.

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Serde bridge: serialize `Bytes` as `Vec<u8>`, deserialize `Vec<u8>` into `Bytes`.
/// This keeps JSON wire-format identical to the previous `Vec<u8>` field while
/// making `Clone` O(1) via refcount instead of O(n) memcpy.
pub mod serde_bytes_as_vec {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        bytes.as_ref().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        let v: Vec<u8> = Vec::deserialize(d)?;
        Ok(Bytes::from(v))
    }
}

/// A sequenced catalog mutation event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEvent {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// The catalog (raft group) this event belongs to.
    #[serde(default)]
    pub catalog: Arc<str>,
    /// The mutation that occurred.
    pub event: CatalogEventKind,
}

/// The kind of catalog mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogEventKind {
    /// A new table was created.
    TableCreated {
        table: Arc<str>,
        /// Arrow IPC-encoded schema bytes (zero-copy clone via Bytes refcount).
        #[serde(with = "serde_bytes_as_vec")]
        schema_ipc: Bytes,
    },
    /// A table was dropped.
    TableDropped { table: Arc<str> },
    /// The active segment's Lance dataset version changed (new writes materialized).
    ActiveVersionBumped { table: Arc<str>, version: u64 },
    /// The active segment was sealed and a new active segment was created.
    SegmentSealed {
        table: Arc<str>,
        active_version: u64,
        sealed_version: u64,
    },
    /// A sealed segment was promoted to S3 deep storage.
    SegmentPromoted {
        table: Arc<str>,
        s3_manifest_version: u64,
    },
    /// Data was mutated (delete or update) across one or more tiers.
    DataMutated {
        table: Arc<str>,
        /// New active dataset version (if active was mutated).
        #[serde(skip_serializing_if = "Option::is_none")]
        active_version: Option<u64>,
        /// New sealed dataset version (if sealed was mutated).
        #[serde(skip_serializing_if = "Option::is_none")]
        sealed_version: Option<u64>,
        /// New S3 dataset version (if S3 was mutated).
        #[serde(skip_serializing_if = "Option::is_none")]
        s3_version: Option<u64>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_table_created() {
        let kind = CatalogEventKind::TableCreated {
            table: "users".into(),
            schema_ipc: Bytes::from_static(&[10, 20, 30]),
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::TableCreated { table, schema_ipc } => {
                assert_eq!(&*table, "users");
                assert_eq!(&schema_ipc[..], &[10, 20, 30]);
            }
            other => panic!("expected TableCreated, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_table_dropped() {
        let kind = CatalogEventKind::TableDropped {
            table: "orders".into(),
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::TableDropped { table } => assert_eq!(&*table, "orders"),
            other => panic!("expected TableDropped, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_catalog_event() {
        let event = CatalogEvent {
            seq: 42,
            catalog: "production".into(),
            event: CatalogEventKind::SegmentSealed {
                table: "logs".into(),
                active_version: 10,
                sealed_version: 9,
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        let decoded: CatalogEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.seq, 42);
        assert_eq!(&*decoded.catalog, "production");
        match decoded.event {
            CatalogEventKind::SegmentSealed {
                table,
                active_version,
                sealed_version,
            } => {
                assert_eq!(&*table, "logs");
                assert_eq!(active_version, 10);
                assert_eq!(sealed_version, 9);
            }
            other => panic!("expected SegmentSealed, got {:?}", other),
        }
    }

    #[test]
    fn serde_catalog_event_default_catalog_field() {
        let json = r#"{"seq":1,"event":{"type":"table_dropped","table":"t"}}"#;
        let decoded: CatalogEvent = serde_json::from_str(json).unwrap();
        assert_eq!(&*decoded.catalog, "");
        assert_eq!(decoded.seq, 1);
    }

    #[test]
    fn serde_data_mutated_skip_serializing_none() {
        let kind = CatalogEventKind::DataMutated {
            table: "t".into(),
            active_version: Some(1),
            sealed_version: None,
            s3_version: None,
        };
        let json = serde_json::to_string(&kind).unwrap();
        assert!(json.contains("\"active_version\":1"));
        assert!(!json.contains("sealed_version"));
        assert!(!json.contains("s3_version"));
    }
}
