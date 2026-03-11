use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::types::Subscription;

/// Persisted consumer metadata (snapshot / MDBX).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMeta {
    pub consumer_id: u64,
    pub group_name: String,
    pub connected_at: u64,
    pub last_heartbeat_at: u64,
    pub subscriptions: HashSet<Subscription>,
    #[serde(default)]
    pub assigned_jobs: HashSet<u64>,
}

impl ConsumerMeta {
    pub fn new(
        consumer_id: u64,
        group_name: String,
        connected_at: u64,
        subscriptions: HashSet<Subscription>,
    ) -> Self {
        Self {
            consumer_id,
            group_name,
            connected_at,
            last_heartbeat_at: connected_at,
            subscriptions,
            assigned_jobs: HashSet::new(),
        }
    }
}

/// Lock-free in-memory consumer state.
///
/// Immutable identity fields are stored directly. Mutable state uses atomics
/// and `RwLock` for concurrent readers with single-writer (Raft apply path).
pub struct ConsumerState {
    // -- Immutable identity (set once at creation) --
    pub meta: ConsumerMeta,

    // -- Mutable state --
    last_heartbeat_at: AtomicU64,
    assigned_jobs: RwLock<HashSet<u64>>,

    /// entity_id → list of in-flight message_ids
    pub in_flight: DashMap<u64, Vec<u64>>,
}

impl ConsumerState {
    pub fn new(meta: ConsumerMeta) -> Self {
        let last_heartbeat_at = AtomicU64::new(meta.last_heartbeat_at);
        let assigned_jobs = RwLock::new(meta.assigned_jobs.clone());
        Self {
            meta,
            last_heartbeat_at,
            assigned_jobs,
            in_flight: DashMap::new(),
        }
    }

    #[inline]
    pub fn last_heartbeat_at(&self) -> u64 {
        self.last_heartbeat_at.load(Ordering::Relaxed)
    }

    pub fn assigned_jobs(&self) -> parking_lot::RwLockReadGuard<'_, HashSet<u64>> {
        self.assigned_jobs.read()
    }

    pub fn heartbeat(&self, at: u64) {
        self.last_heartbeat_at.store(at, Ordering::Relaxed);
    }

    pub fn is_dead(&self, current_time: u64, timeout_ms: u64) -> bool {
        current_time > self.last_heartbeat_at() + timeout_ms
    }

    pub fn add_assigned_job(&self, job_id: u64) {
        self.assigned_jobs.write().insert(job_id);
    }

    pub fn remove_assigned_job(&self, job_id: u64) {
        self.assigned_jobs.write().remove(&job_id);
    }

    /// Flush mutable state back to a cloned `ConsumerMeta` for snapshot/persistence.
    pub fn snapshot_meta(&self) -> ConsumerMeta {
        let mut m = self.meta.clone();
        m.last_heartbeat_at = self.last_heartbeat_at();
        m.assigned_jobs = self.assigned_jobs.read().clone();
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EntityType, Subscription};

    fn make_consumer(id: u64, connected_at: u64) -> ConsumerState {
        let meta = ConsumerMeta::new(
            id,
            "test-group".to_string(),
            connected_at,
            [Subscription {
                entity_type: EntityType::Queue,
                entity_id: 1,
            }]
            .into_iter()
            .collect(),
        );
        ConsumerState::new(meta)
    }

    #[test]
    fn test_consumer_meta_defaults() {
        let meta = ConsumerMeta::new(42, "workers".to_string(), 1000, HashSet::new());
        assert_eq!(meta.consumer_id, 42);
        assert_eq!(meta.group_name, "workers");
        assert_eq!(meta.connected_at, 1000);
        assert_eq!(meta.last_heartbeat_at, 1000);
        assert!(meta.assigned_jobs.is_empty());
    }

    #[test]
    fn test_heartbeat() {
        let consumer = make_consumer(1, 1000);
        assert_eq!(consumer.last_heartbeat_at(), 1000);

        consumer.heartbeat(5000);
        assert_eq!(consumer.last_heartbeat_at(), 5000);
    }

    #[test]
    fn test_is_dead() {
        let consumer = make_consumer(1, 1000);
        let timeout_ms = 30_000;

        // Not dead yet
        assert!(!consumer.is_dead(1000, timeout_ms));
        assert!(!consumer.is_dead(30_999, timeout_ms));
        assert!(!consumer.is_dead(31_000, timeout_ms));

        // Dead after timeout
        assert!(consumer.is_dead(31_001, timeout_ms));
    }

    #[test]
    fn test_is_dead_after_heartbeat() {
        let consumer = make_consumer(1, 1000);
        consumer.heartbeat(10_000);

        // Deadline resets to heartbeat time
        assert!(!consumer.is_dead(39_000, 30_000));
        assert!(!consumer.is_dead(40_000, 30_000));
        assert!(consumer.is_dead(40_001, 30_000));
    }

    #[test]
    fn test_consumer_serde_roundtrip() {
        let meta = ConsumerMeta::new(
            42,
            "workers".to_string(),
            1000,
            [Subscription {
                entity_type: EntityType::Topic,
                entity_id: 5,
            }]
            .into_iter()
            .collect(),
        );
        let bytes = bincode::serde::encode_to_vec(&meta, bincode::config::standard()).unwrap();
        let (decoded, _): (ConsumerMeta, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.consumer_id, 42);
        assert_eq!(decoded.group_name, "workers");
        assert_eq!(decoded.subscriptions.len(), 1);
        assert!(decoded.subscriptions.iter().any(|s| s.entity_id == 5));
    }

    #[test]
    fn test_snapshot_meta_roundtrip() {
        let consumer = make_consumer(1, 1000);
        consumer.heartbeat(5000);
        consumer.add_assigned_job(42);

        let snap = consumer.snapshot_meta();
        assert_eq!(snap.consumer_id, 1);
        assert_eq!(snap.last_heartbeat_at, 5000);
        assert!(snap.assigned_jobs.contains(&42));
    }

    #[test]
    fn test_assigned_jobs() {
        let consumer = make_consumer(1, 1000);
        assert!(consumer.assigned_jobs().is_empty());

        consumer.add_assigned_job(10);
        consumer.add_assigned_job(20);
        assert_eq!(consumer.assigned_jobs().len(), 2);

        consumer.remove_assigned_job(10);
        assert_eq!(consumer.assigned_jobs().len(), 1);
        assert!(consumer.assigned_jobs().contains(&20));
    }
}
