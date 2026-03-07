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
