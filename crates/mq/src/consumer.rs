use std::collections::HashSet;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::types::Subscription;

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

/// In-memory consumer state.
pub struct ConsumerState {
    pub meta: ConsumerMeta,
    /// entity_id → list of in-flight message_ids
    pub in_flight: DashMap<u64, Vec<u64>>,
}

impl ConsumerState {
    pub fn new(meta: ConsumerMeta) -> Self {
        Self {
            meta,
            in_flight: DashMap::new(),
        }
    }

    pub fn heartbeat(&mut self, at: u64) {
        self.meta.last_heartbeat_at = at;
    }

    pub fn is_dead(&self, current_time: u64, timeout_ms: u64) -> bool {
        current_time > self.meta.last_heartbeat_at + timeout_ms
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
        let mut consumer = make_consumer(1, 1000);
        assert_eq!(consumer.meta.last_heartbeat_at, 1000);

        consumer.heartbeat(5000);
        assert_eq!(consumer.meta.last_heartbeat_at, 5000);
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
        let mut consumer = make_consumer(1, 1000);
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
}
