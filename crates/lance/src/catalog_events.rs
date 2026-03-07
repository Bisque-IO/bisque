//! Catalog event bus for real-time notification of table metadata changes.
//!
//! [`CatalogEventBus`] broadcasts sequenced [`CatalogEvent`]s to subscribers
//! (typically WebSocket connections). Events are emitted by the Raft state machine
//! after each successful mutation that changes the catalog (table create/drop,
//! segment seal, version bump, promote to deep storage).
//!
//! The bus uses `tokio::sync::broadcast` with a bounded buffer (1024 events).
//! Lagged subscribers receive `RecvError::Lagged`, signaling them to fetch a
//! full catalog snapshot instead of incremental replay.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// A sequenced catalog mutation event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEvent {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// The catalog (raft group) this event belongs to.
    #[serde(default)]
    pub catalog: String,
    /// The mutation that occurred.
    pub event: CatalogEventKind,
}

/// The kind of catalog mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogEventKind {
    /// A new table was created.
    TableCreated {
        table: String,
        /// Arrow IPC-encoded schema bytes.
        schema_ipc: Vec<u8>,
    },
    /// A table was dropped.
    TableDropped { table: String },
    /// The active segment's Lance dataset version changed (new writes materialized).
    ActiveVersionBumped { table: String, version: u64 },
    /// The active segment was sealed and a new active segment was created.
    SegmentSealed {
        table: String,
        active_version: u64,
        sealed_version: u64,
    },
    /// A sealed segment was promoted to S3 deep storage.
    SegmentPromoted {
        table: String,
        s3_manifest_version: u64,
    },
    /// Data was mutated (delete or update) across one or more tiers.
    DataMutated {
        table: String,
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

/// Broadcasts catalog events to subscribers with monotonic sequencing.
pub struct CatalogEventBus {
    tx: broadcast::Sender<CatalogEvent>,
    seq: AtomicU64,
}

impl CatalogEventBus {
    /// Create a new event bus with the given initial sequence number.
    ///
    /// The sequence should be initialized from the MDBX WAL's latest entry
    /// on startup, or 0 if the WAL is empty.
    pub fn new(initial_seq: u64) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            tx,
            seq: AtomicU64::new(initial_seq),
        }
    }

    /// Subscribe to catalog events.
    ///
    /// Returns a receiver that will get all future events. If the receiver
    /// falls behind by more than 1024 events, it will receive `Lagged`.
    pub fn subscribe(&self) -> broadcast::Receiver<CatalogEvent> {
        self.tx.subscribe()
    }

    /// Publish a catalog event, assigning it the next sequence number.
    ///
    /// Returns the published event (with its assigned sequence number).
    /// Returns `None` if there are no active subscribers.
    pub fn publish(&self, catalog: String, kind: CatalogEventKind) -> CatalogEvent {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let event = CatalogEvent {
            seq,
            catalog,
            event: kind,
        };
        // Ignore send error — it just means no subscribers are connected.
        let _ = self.tx.send(event.clone());
        event
    }

    /// Get the current (latest) sequence number.
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_created(name: &str) -> CatalogEventKind {
        CatalogEventKind::TableCreated {
            table: name.to_string(),
            schema_ipc: vec![1, 2, 3],
        }
    }

    // ---------------------------------------------------------------
    // 1. new() with initial_seq=0 starts at seq 0
    // ---------------------------------------------------------------
    #[test]
    fn new_with_initial_seq_zero() {
        let bus = CatalogEventBus::new(0);
        assert_eq!(bus.current_seq(), 0);
    }

    // ---------------------------------------------------------------
    // 2. new() with initial_seq=100 starts at seq 100
    // ---------------------------------------------------------------
    #[test]
    fn new_with_initial_seq_100() {
        let bus = CatalogEventBus::new(100);
        assert_eq!(bus.current_seq(), 100);
    }

    // ---------------------------------------------------------------
    // 3. publish() increments sequence monotonically
    // ---------------------------------------------------------------
    #[test]
    fn publish_increments_sequence_monotonically() {
        let bus = CatalogEventBus::new(0);
        let e1 = bus.publish("c".into(), table_created("t1"));
        let e2 = bus.publish("c".into(), table_created("t2"));
        let e3 = bus.publish("c".into(), table_created("t3"));
        assert_eq!(e1.seq, 1);
        assert_eq!(e2.seq, 2);
        assert_eq!(e3.seq, 3);
        assert!(e1.seq < e2.seq && e2.seq < e3.seq);
    }

    // ---------------------------------------------------------------
    // 4. publish() returns event with correct seq, catalog, and kind
    // ---------------------------------------------------------------
    #[test]
    fn publish_returns_correct_event() {
        let bus = CatalogEventBus::new(10);
        let event = bus.publish(
            "my_catalog".into(),
            CatalogEventKind::TableDropped {
                table: "users".into(),
            },
        );
        assert_eq!(event.seq, 11);
        assert_eq!(event.catalog, "my_catalog");
        match &event.event {
            CatalogEventKind::TableDropped { table } => assert_eq!(table, "users"),
            other => panic!("expected TableDropped, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // 5. subscribe() receives published events
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn subscriber_receives_published_events() {
        let bus = CatalogEventBus::new(0);
        let mut rx = bus.subscribe();

        bus.publish("cat".into(), table_created("t1"));
        bus.publish("cat".into(), table_created("t2"));

        let e1 = rx.recv().await.unwrap();
        let e2 = rx.recv().await.unwrap();
        assert_eq!(e1.seq, 1);
        assert_eq!(e2.seq, 2);
    }

    // ---------------------------------------------------------------
    // 6. multiple subscribers all receive the same event
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn multiple_subscribers_receive_same_event() {
        let bus = CatalogEventBus::new(0);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        let mut rx3 = bus.subscribe();

        bus.publish("cat".into(), table_created("t"));

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        let e3 = rx3.recv().await.unwrap();

        assert_eq!(e1.seq, e2.seq);
        assert_eq!(e2.seq, e3.seq);
        assert_eq!(e1.seq, 1);
        assert_eq!(e1.catalog, "cat");
    }

    // ---------------------------------------------------------------
    // 7. subscriber created after publish does not receive past events
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn late_subscriber_misses_past_events() {
        let bus = CatalogEventBus::new(0);
        bus.publish("cat".into(), table_created("before"));

        let mut rx = bus.subscribe();
        bus.publish("cat".into(), table_created("after"));

        let event = rx.recv().await.unwrap();
        assert_eq!(event.seq, 2);
        match &event.event {
            CatalogEventKind::TableCreated { table, .. } => assert_eq!(table, "after"),
            other => panic!("expected TableCreated, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // 8. publish with no subscribers does not panic
    // ---------------------------------------------------------------
    #[test]
    fn publish_with_no_subscribers_does_not_panic() {
        let bus = CatalogEventBus::new(0);
        let event = bus.publish("cat".into(), table_created("t"));
        assert_eq!(event.seq, 1);
    }

    // ---------------------------------------------------------------
    // 9. current_seq() reflects the latest published seq
    // ---------------------------------------------------------------
    #[test]
    fn current_seq_reflects_latest() {
        let bus = CatalogEventBus::new(5);
        assert_eq!(bus.current_seq(), 5);

        bus.publish("c".into(), table_created("t1"));
        assert_eq!(bus.current_seq(), 6);

        bus.publish("c".into(), table_created("t2"));
        assert_eq!(bus.current_seq(), 7);

        bus.publish("c".into(), table_created("t3"));
        assert_eq!(bus.current_seq(), 8);
    }

    // ---------------------------------------------------------------
    // 10. high-volume sequential publishes maintain ordering
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn high_volume_publishes_maintain_ordering() {
        let bus = CatalogEventBus::new(0);
        let mut rx = bus.subscribe();

        let count = 500;
        for i in 0..count {
            bus.publish(
                "cat".into(),
                CatalogEventKind::ActiveVersionBumped {
                    table: format!("t{}", i),
                    version: i as u64,
                },
            );
        }

        let mut prev_seq = 0u64;
        for _ in 0..count {
            let event = rx.recv().await.unwrap();
            assert!(
                event.seq > prev_seq,
                "seq {} should be > {}",
                event.seq,
                prev_seq
            );
            prev_seq = event.seq;
        }
        assert_eq!(prev_seq, count as u64);
        assert_eq!(bus.current_seq(), count as u64);
    }

    // ---------------------------------------------------------------
    // 11. CatalogEventKind serde roundtrip for each variant
    // ---------------------------------------------------------------
    #[test]
    fn serde_roundtrip_table_created() {
        let kind = CatalogEventKind::TableCreated {
            table: "users".into(),
            schema_ipc: vec![10, 20, 30],
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::TableCreated { table, schema_ipc } => {
                assert_eq!(table, "users");
                assert_eq!(schema_ipc, vec![10, 20, 30]);
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
            CatalogEventKind::TableDropped { table } => assert_eq!(table, "orders"),
            other => panic!("expected TableDropped, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_active_version_bumped() {
        let kind = CatalogEventKind::ActiveVersionBumped {
            table: "metrics".into(),
            version: 42,
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::ActiveVersionBumped { table, version } => {
                assert_eq!(table, "metrics");
                assert_eq!(version, 42);
            }
            other => panic!("expected ActiveVersionBumped, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_segment_sealed() {
        let kind = CatalogEventKind::SegmentSealed {
            table: "logs".into(),
            active_version: 5,
            sealed_version: 4,
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::SegmentSealed {
                table,
                active_version,
                sealed_version,
            } => {
                assert_eq!(table, "logs");
                assert_eq!(active_version, 5);
                assert_eq!(sealed_version, 4);
            }
            other => panic!("expected SegmentSealed, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_segment_promoted() {
        let kind = CatalogEventKind::SegmentPromoted {
            table: "events".into(),
            s3_manifest_version: 99,
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::SegmentPromoted {
                table,
                s3_manifest_version,
            } => {
                assert_eq!(table, "events");
                assert_eq!(s3_manifest_version, 99);
            }
            other => panic!("expected SegmentPromoted, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // 12. CatalogEvent serde roundtrip
    // ---------------------------------------------------------------
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
        assert_eq!(decoded.catalog, "production");
        match decoded.event {
            CatalogEventKind::SegmentSealed {
                table,
                active_version,
                sealed_version,
            } => {
                assert_eq!(table, "logs");
                assert_eq!(active_version, 10);
                assert_eq!(sealed_version, 9);
            }
            other => panic!("expected SegmentSealed, got {:?}", other),
        }
    }

    #[test]
    fn serde_catalog_event_default_catalog_field() {
        // The `catalog` field has `#[serde(default)]`, so omitting it should
        // deserialize to an empty string.
        let json = r#"{"seq":1,"event":{"type":"table_dropped","table":"t"}}"#;
        let decoded: CatalogEvent = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.catalog, "");
        assert_eq!(decoded.seq, 1);
    }

    // ---------------------------------------------------------------
    // 13. DataMutated serde roundtrips
    // ---------------------------------------------------------------

    #[test]
    fn serde_roundtrip_data_mutated_all_versions() {
        let kind = CatalogEventKind::DataMutated {
            table: "metrics".into(),
            active_version: Some(10),
            sealed_version: Some(7),
            s3_version: Some(3),
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::DataMutated {
                table,
                active_version,
                sealed_version,
                s3_version,
            } => {
                assert_eq!(table, "metrics");
                assert_eq!(active_version, Some(10));
                assert_eq!(sealed_version, Some(7));
                assert_eq!(s3_version, Some(3));
            }
            other => panic!("expected DataMutated, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_data_mutated_active_only() {
        let kind = CatalogEventKind::DataMutated {
            table: "logs".into(),
            active_version: Some(5),
            sealed_version: None,
            s3_version: None,
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::DataMutated {
                table,
                active_version,
                sealed_version,
                s3_version,
            } => {
                assert_eq!(table, "logs");
                assert_eq!(active_version, Some(5));
                assert_eq!(sealed_version, None);
                assert_eq!(s3_version, None);
            }
            other => panic!("expected DataMutated, got {:?}", other),
        }
    }

    #[test]
    fn serde_roundtrip_data_mutated_none_versions() {
        let kind = CatalogEventKind::DataMutated {
            table: "events".into(),
            active_version: None,
            sealed_version: None,
            s3_version: None,
        };
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: CatalogEventKind = serde_json::from_str(&json).unwrap();
        match decoded {
            CatalogEventKind::DataMutated {
                table,
                active_version,
                sealed_version,
                s3_version,
            } => {
                assert_eq!(table, "events");
                assert_eq!(active_version, None);
                assert_eq!(sealed_version, None);
                assert_eq!(s3_version, None);
            }
            other => panic!("expected DataMutated, got {:?}", other),
        }
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
        // The JSON should contain active_version but NOT sealed_version or s3_version.
        assert!(
            json.contains("\"active_version\":1"),
            "expected active_version in JSON: {}",
            json
        );
        assert!(
            !json.contains("sealed_version"),
            "sealed_version should be omitted from JSON: {}",
            json
        );
        assert!(
            !json.contains("s3_version"),
            "s3_version should be omitted from JSON: {}",
            json
        );
    }
}
