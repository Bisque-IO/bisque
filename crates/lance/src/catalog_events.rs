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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast;

// Re-export types from bisque-protocol so existing `use crate::catalog_events::*` still works.
pub use bisque_protocol::catalog_events::{CatalogEvent, CatalogEventKind, serde_bytes_as_vec};

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
    pub fn publish(&self, catalog: Arc<str>, kind: CatalogEventKind) -> CatalogEvent {
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
    use bytes::Bytes;

    fn table_created(name: &str) -> CatalogEventKind {
        CatalogEventKind::TableCreated {
            table: Arc::from(name),
            schema_ipc: Bytes::from_static(&[1, 2, 3]),
        }
    }

    #[test]
    fn new_with_initial_seq_zero() {
        let bus = CatalogEventBus::new(0);
        assert_eq!(bus.current_seq(), 0);
    }

    #[test]
    fn new_with_initial_seq_100() {
        let bus = CatalogEventBus::new(100);
        assert_eq!(bus.current_seq(), 100);
    }

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
        assert_eq!(&*event.catalog, "my_catalog");
        match &event.event {
            CatalogEventKind::TableDropped { table } => assert_eq!(&**table, "users"),
            other => panic!("expected TableDropped, got {:?}", other),
        }
    }

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
        assert_eq!(&*e1.catalog, "cat");
    }

    #[tokio::test]
    async fn late_subscriber_misses_past_events() {
        let bus = CatalogEventBus::new(0);
        bus.publish("cat".into(), table_created("before"));

        let mut rx = bus.subscribe();
        bus.publish("cat".into(), table_created("after"));

        let event = rx.recv().await.unwrap();
        assert_eq!(event.seq, 2);
        match &event.event {
            CatalogEventKind::TableCreated { table, .. } => assert_eq!(&**table, "after"),
            other => panic!("expected TableCreated, got {:?}", other),
        }
    }

    #[test]
    fn publish_with_no_subscribers_does_not_panic() {
        let bus = CatalogEventBus::new(0);
        let event = bus.publish("cat".into(), table_created("t"));
        assert_eq!(event.seq, 1);
    }

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

    #[tokio::test]
    async fn high_volume_publishes_maintain_ordering() {
        let bus = CatalogEventBus::new(0);
        let mut rx = bus.subscribe();

        let count = 500;
        for i in 0..count {
            bus.publish(
                "cat".into(),
                CatalogEventKind::ActiveVersionBumped {
                    table: Arc::from(format!("t{}", i).as_str()),
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
}
