use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::config::ActorConfig;
use crate::types::{SegmentRange, name_hash};

// =============================================================================
// Actor Namespace Metadata (persisted to MDBX)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorNamespaceMeta {
    pub namespace_id: u64,
    pub name: String,
    pub created_at: u64,
    pub config: ActorConfig,
    pub active_actor_count: u64,
    #[serde(default)]
    pub segment_index: Vec<SegmentRange>,
    #[serde(default)]
    pub name_hash: u64,
}

impl ActorNamespaceMeta {
    pub fn new(namespace_id: u64, name: String, created_at: u64, config: ActorConfig) -> Self {
        let hash = name_hash(&name);
        Self {
            namespace_id,
            name,
            created_at,
            config,
            active_actor_count: 0,
            segment_index: Vec::new(),
            name_hash: hash,
        }
    }

    /// Ensure name_hash is populated (for deserialized data that may have default 0).
    pub fn ensure_name_hash(&mut self) {
        if self.name_hash == 0 {
            self.name_hash = name_hash(&self.name);
        }
    }
}

// =============================================================================
// Per-Actor State (persisted to MDBX for active actors)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorState {
    pub namespace_id: u64,
    pub actor_id: Bytes,
    #[serde(default)]
    pub assigned_consumer_id: Option<u64>,
    pub pending_count: u32,
    pub head_index: u64,
    pub tail_index: u64,
    #[serde(default)]
    pub in_flight_index: Option<u64>,
    pub last_activity_at: u64,
    #[serde(default)]
    pub attempts: u32,
}

// =============================================================================
// In-memory Actor Namespace State
// =============================================================================

pub struct ActorNamespaceState {
    pub meta: ActorNamespaceMeta,
    /// actor_id → ActorInMemory
    pub actors: HashMap<Bytes, ActorInMemory>,
    /// consumer_id → set of assigned actor_ids
    pub consumer_assignments: HashMap<u64, HashSet<Bytes>>,

    /// Cached min required index. `None` = dirty, needs recompute.
    cached_min_required: Option<Option<u64>>,

    // Pre-initialized metrics
    m_send_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_evict_count: metrics::Counter,
}

pub struct ActorInMemory {
    pub state: ActorState,
    /// Ordered mailbox of raft log indexes for pending messages.
    pub mailbox: VecDeque<u64>,
    /// Per-message reply_to topic name, keyed by log_index.
    /// Only populated for messages that have a reply_to field.
    pub reply_to_map: HashMap<u64, String>,
}

impl ActorNamespaceState {
    pub fn new(meta: ActorNamespaceMeta) -> Self {
        let labels = [("namespace", meta.name.clone())];
        let m_send_count = metrics::counter!("mq.actor.send.count", &labels);
        let m_deliver_count = metrics::counter!("mq.actor.deliver.count", &labels);
        let m_ack_count = metrics::counter!("mq.actor.ack.count", &labels);
        let m_evict_count = metrics::counter!("mq.actor.evict.count", &labels);

        Self {
            meta,
            actors: HashMap::new(),
            consumer_assignments: HashMap::new(),
            cached_min_required: Some(None),
            m_send_count,
            m_deliver_count,
            m_ack_count,
            m_evict_count,
        }
    }

    pub fn apply_send(
        &mut self,
        actor_id: &Bytes,
        log_index: u64,
        current_time: u64,
        reply_to: Option<String>,
    ) -> Result<(), crate::types::MqError> {
        let actor = self.actors.entry(actor_id.clone()).or_insert_with(|| {
            self.meta.active_actor_count += 1;
            ActorInMemory {
                state: ActorState {
                    namespace_id: self.meta.namespace_id,
                    actor_id: actor_id.clone(),
                    assigned_consumer_id: None,
                    pending_count: 0,
                    head_index: 0,
                    tail_index: 0,
                    in_flight_index: None,
                    last_activity_at: current_time,
                    attempts: 0,
                },
                mailbox: VecDeque::new(),
                reply_to_map: HashMap::new(),
            }
        });

        if actor.state.pending_count >= self.meta.config.max_mailbox_depth {
            return Err(crate::types::MqError::MailboxFull {
                pending: actor.state.pending_count as u32,
            });
        }

        actor.mailbox.push_back(log_index);
        if let Some(rt) = reply_to {
            actor.reply_to_map.insert(log_index, rt);
        }
        actor.state.pending_count += 1;
        actor.state.head_index = log_index;
        if actor.state.tail_index == 0 {
            actor.state.tail_index = log_index;
        }
        actor.state.last_activity_at = current_time;

        // Update cached min: new message can only lower the floor
        if let Some(ref mut cached) = self.cached_min_required {
            let new_min = actor
                .state
                .in_flight_index
                .unwrap_or(actor.state.tail_index);
            *cached = Some(cached.map_or(new_min, |m| m.min(new_min)));
        }

        self.m_send_count.increment(1);
        Ok(())
    }

    pub fn apply_deliver(&mut self, actor_id: &Bytes, consumer_id: u64) -> Option<u64> {
        let actor = self.actors.get_mut(actor_id)?;

        // Must be assigned to this consumer
        if actor.state.assigned_consumer_id != Some(consumer_id) {
            return None;
        }

        // Already has an in-flight message
        if actor.state.in_flight_index.is_some() {
            return None;
        }

        let msg_index = actor.mailbox.pop_front()?;
        actor.state.in_flight_index = Some(msg_index);
        actor.state.pending_count = actor.state.pending_count.saturating_sub(1);
        actor.state.attempts += 1;

        self.m_deliver_count.increment(1);
        Some(msg_index)
    }

    /// ACK a message and return its `reply_to` topic name (if any).
    pub fn apply_ack(&mut self, actor_id: &Bytes, message_id: u64) -> Option<String> {
        if let Some(actor) = self.actors.get_mut(actor_id) {
            if actor.state.in_flight_index == Some(message_id) {
                actor.state.in_flight_index = None;
                actor.state.attempts = 0;
                let reply_to = actor.reply_to_map.remove(&message_id);
                // Update tail to next pending if any
                if let Some(&next) = actor.mailbox.front() {
                    actor.state.tail_index = next;
                }
                // Invalidate if this message could have been the global min
                if let Some(Some(cached_min)) = self.cached_min_required {
                    if message_id <= cached_min {
                        self.cached_min_required = None;
                    }
                }
                self.m_ack_count.increment(1);
                return reply_to;
            }
        }
        None
    }

    pub fn apply_nack(&mut self, actor_id: &Bytes, message_id: u64) {
        if let Some(actor) = self.actors.get_mut(actor_id) {
            if actor.state.in_flight_index == Some(message_id) {
                // Put back at front of mailbox
                actor.mailbox.push_front(message_id);
                actor.state.in_flight_index = None;
                actor.state.pending_count += 1;
            }
        }
    }

    pub fn apply_assign(&mut self, consumer_id: u64, actor_ids: &[Bytes]) {
        let set = self.consumer_assignments.entry(consumer_id).or_default();

        for actor_id in actor_ids {
            if let Some(actor) = self.actors.get_mut(actor_id) {
                actor.state.assigned_consumer_id = Some(consumer_id);
                set.insert(actor_id.clone());
            }
        }
    }

    pub fn apply_release(&mut self, consumer_id: u64) {
        if let Some(actor_ids) = self.consumer_assignments.remove(&consumer_id) {
            for actor_id in &actor_ids {
                if let Some(actor) = self.actors.get_mut(actor_id) {
                    actor.state.assigned_consumer_id = None;
                    // Return in-flight message to mailbox
                    if let Some(msg_id) = actor.state.in_flight_index.take() {
                        actor.mailbox.push_front(msg_id);
                        actor.state.pending_count += 1;
                    }
                }
            }
        }
    }

    pub fn apply_evict_idle(&mut self, before_timestamp: u64) -> usize {
        let before = self.actors.len();
        self.actors.retain(|_actor_id, actor| {
            let keep = actor.state.pending_count > 0
                || actor.state.in_flight_index.is_some()
                || actor.state.last_activity_at >= before_timestamp;
            if !keep {
                self.meta.active_actor_count = self.meta.active_actor_count.saturating_sub(1);
            }
            keep
        });
        let count = before - self.actors.len();
        if count > 0 {
            self.cached_min_required = None;
        }
        self.m_evict_count.increment(count as u64);
        count
    }

    /// Get unassigned actors that have pending messages.
    pub fn unassigned_actors_with_messages(&self) -> Vec<&Bytes> {
        self.actors
            .iter()
            .filter(|(_, a)| {
                a.state.assigned_consumer_id.is_none()
                    && (a.state.pending_count > 0 || a.state.in_flight_index.is_some())
            })
            .map(|(id, _)| id)
            .collect()
    }

    /// Returns the minimum log index required by this namespace (for purge floor).
    /// Uses a cache that is maintained incrementally by send/deliver/ack/nack/release/evict.
    pub fn min_required_index(&mut self) -> Option<u64> {
        if let Some(cached) = self.cached_min_required {
            return cached;
        }
        let mut min: Option<u64> = None;
        for actor in self.actors.values() {
            if actor.state.pending_count > 0 || actor.state.in_flight_index.is_some() {
                let actor_min = actor
                    .state
                    .in_flight_index
                    .unwrap_or(actor.state.tail_index);
                min = Some(min.map_or(actor_min, |m: u64| m.min(actor_min)));
            }
        }
        self.cached_min_required = Some(min);
        min
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ActorConfig;

    fn make_ns(name: &str) -> ActorNamespaceState {
        let meta = ActorNamespaceMeta::new(1, name.to_string(), 1000, ActorConfig::default());
        ActorNamespaceState::new(meta)
    }

    fn actor_id(name: &str) -> Bytes {
        Bytes::from(name.to_string())
    }

    #[test]
    fn test_send_creates_actor() {
        let mut ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        assert_eq!(ns.actors.len(), 1);
        assert_eq!(ns.meta.active_actor_count, 1);

        let actor = &ns.actors[&aid];
        assert_eq!(actor.state.pending_count, 1);
        assert_eq!(actor.state.head_index, 10);
        assert_eq!(actor.state.tail_index, 10);
        assert_eq!(actor.mailbox.len(), 1);
    }

    #[test]
    fn test_send_multiple_messages() {
        let mut ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 11, 1001, None).unwrap();
        ns.apply_send(&aid, 12, 1002, None).unwrap();

        let actor = &ns.actors[&aid];
        assert_eq!(actor.state.pending_count, 3);
        assert_eq!(actor.state.head_index, 12);
        assert_eq!(actor.state.tail_index, 10);
        assert_eq!(actor.mailbox.len(), 3);
    }

    #[test]
    fn test_send_mailbox_full() {
        let config = ActorConfig {
            max_mailbox_depth: 2,
            ..Default::default()
        };
        let meta = ActorNamespaceMeta::new(1, "test".to_string(), 1000, config);
        let mut ns = ActorNamespaceState::new(meta);
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 11, 1001, None).unwrap();
        let result = ns.apply_send(&aid, 12, 1002, None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::types::MqError::MailboxFull { .. }
        ));
    }

    #[test]
    fn test_deliver_requires_assignment() {
        let mut ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        // No assignment yet
        assert!(ns.apply_deliver(&aid, 100).is_none());

        // Assign and deliver
        ns.apply_assign(100, &[aid.clone()]);
        let msg = ns.apply_deliver(&aid, 100);
        assert_eq!(msg, Some(10));
    }

    #[test]
    fn test_deliver_serialized() {
        let mut ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 11, 1001, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);

        // First deliver
        let msg1 = ns.apply_deliver(&aid, 100);
        assert_eq!(msg1, Some(10));

        // Second deliver blocked (in-flight exists)
        let msg2 = ns.apply_deliver(&aid, 100);
        assert!(msg2.is_none());
    }

    #[test]
    fn test_ack_clears_in_flight() {
        let mut ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 11, 1001, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);

        ns.apply_deliver(&aid, 100);
        ns.apply_ack(&aid, 10);

        let actor = &ns.actors[&aid];
        assert!(actor.state.in_flight_index.is_none());
        assert_eq!(actor.state.attempts, 0);

        // Can now deliver next
        let msg = ns.apply_deliver(&aid, 100);
        assert_eq!(msg, Some(11));
    }

    #[test]
    fn test_nack_returns_to_front() {
        let mut ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);
        ns.apply_deliver(&aid, 100);

        ns.apply_nack(&aid, 10);
        let actor = &ns.actors[&aid];
        assert!(actor.state.in_flight_index.is_none());
        assert_eq!(actor.state.pending_count, 1);
        assert_eq!(actor.mailbox.front(), Some(&10));
    }

    #[test]
    fn test_assign_and_release() {
        let mut ns = make_ns("test");
        let a1 = actor_id("user-1");
        let a2 = actor_id("user-2");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_send(&a2, 11, 1001, None).unwrap();

        ns.apply_assign(100, &[a1.clone(), a2.clone()]);
        assert_eq!(ns.consumer_assignments[&100].len(), 2);
        assert_eq!(ns.actors[&a1].state.assigned_consumer_id, Some(100));

        ns.apply_release(100);
        assert!(!ns.consumer_assignments.contains_key(&100));
        assert!(ns.actors[&a1].state.assigned_consumer_id.is_none());
    }

    #[test]
    fn test_release_returns_in_flight() {
        let mut ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);
        ns.apply_deliver(&aid, 100);

        // In-flight message exists
        assert_eq!(ns.actors[&aid].state.in_flight_index, Some(10));

        ns.apply_release(100);
        let actor = &ns.actors[&aid];
        assert!(actor.state.in_flight_index.is_none());
        assert_eq!(actor.state.pending_count, 1);
        assert_eq!(actor.mailbox.front(), Some(&10));
    }

    #[test]
    fn test_evict_idle() {
        let mut ns = make_ns("test");
        let a1 = actor_id("idle");
        let a2 = actor_id("active");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_assign(100, &[a1.clone()]);
        ns.apply_deliver(&a1, 100);
        ns.apply_ack(&a1, 10); // now idle, last_activity_at=1000

        ns.apply_send(&a2, 11, 5000, None).unwrap(); // active, has pending

        let evicted = ns.apply_evict_idle(3000); // evict actors idle before 3000
        assert_eq!(evicted, 1);
        assert!(!ns.actors.contains_key(&a1));
        assert!(ns.actors.contains_key(&a2)); // not evicted (has pending)
    }

    #[test]
    fn test_unassigned_actors_with_messages() {
        let mut ns = make_ns("test");
        let a1 = actor_id("unassigned");
        let a2 = actor_id("assigned");
        let _a3 = actor_id("empty");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_send(&a2, 11, 1000, None).unwrap();
        // a3 not created (no messages)

        ns.apply_assign(100, &[a2.clone()]);

        let unassigned = ns.unassigned_actors_with_messages();
        assert_eq!(unassigned.len(), 1);
        assert!(unassigned.contains(&&a1));
    }

    #[test]
    fn test_min_required_index() {
        let mut ns = make_ns("test");
        assert!(ns.min_required_index().is_none());

        let a1 = actor_id("a1");
        let a2 = actor_id("a2");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_send(&a2, 20, 1000, None).unwrap();

        assert_eq!(ns.min_required_index(), Some(10));
    }

    #[test]
    fn test_min_required_index_with_in_flight() {
        let mut ns = make_ns("test");
        let aid = actor_id("a1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 20, 1001, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);
        ns.apply_deliver(&aid, 100); // msg 10 is now in-flight

        assert_eq!(ns.min_required_index(), Some(10));
    }

    #[test]
    fn test_multiple_actors_different_consumers() {
        let mut ns = make_ns("test");
        let a1 = actor_id("a1");
        let a2 = actor_id("a2");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_send(&a2, 11, 1000, None).unwrap();

        ns.apply_assign(100, &[a1.clone()]);
        ns.apply_assign(200, &[a2.clone()]);

        assert_eq!(ns.apply_deliver(&a1, 100), Some(10));
        assert_eq!(ns.apply_deliver(&a2, 200), Some(11));

        // Cannot deliver a1 with consumer 200
        assert!(ns.apply_deliver(&a1, 200).is_none());
    }
}
