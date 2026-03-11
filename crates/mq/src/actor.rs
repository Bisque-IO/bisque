use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::config::ActorConfig;
use crate::types::name_hash;

/// Sentinel: cached min needs full recompute.
const MIN_DIRTY: u64 = u64::MAX;
/// Sentinel: computed result is None (no actors with pending messages).
const MIN_NONE: u64 = u64::MAX - 1;
/// Sentinel for Option<u64> atomics: 0 means None.
const NONE_U64: u64 = 0;

#[inline]
fn opt_to_atomic(v: Option<u64>) -> u64 {
    v.unwrap_or(NONE_U64)
}

#[inline]
fn atomic_to_opt(v: u64) -> Option<u64> {
    if v == NONE_U64 { None } else { Some(v) }
}

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
// In-memory Actor State (lock-free atomics + mutex mailbox)
// =============================================================================

/// Lock-free per-actor instance.
///
/// Immutable identity fields (`namespace_id`, `actor_id`) are plain values.
/// Mutable scalar fields use atomics for concurrent readers with single-writer
/// updates (Raft apply path). The mailbox uses `parking_lot::Mutex` since
/// `VecDeque` cannot be made atomic.
pub struct ActorInMemory {
    // -- Immutable identity --
    pub namespace_id: u64,
    pub actor_id: Bytes,

    // -- Mutable state (atomics) --
    pub(crate) assigned_consumer_id: AtomicU64, // 0 = None
    pub(crate) pending_count: AtomicU32,
    pub(crate) head_index: AtomicU64,
    pub(crate) tail_index: AtomicU64,
    pub(crate) in_flight_index: AtomicU64, // 0 = None
    pub(crate) last_activity_at: AtomicU64,
    pub(crate) attempts: AtomicU32,

    // -- Complex mutable state --
    pub(crate) mailbox: Mutex<VecDeque<u64>>,
    /// Per-message reply_to topic name bytes, keyed by log_index.
    /// Only populated for messages that have a reply_to field.
    /// Stored as zero-copy `Bytes` to avoid String allocation.
    pub reply_to_map: DashMap<u64, Bytes>,
}

impl ActorInMemory {
    /// Create a new actor from immutable identity fields.
    pub fn new(namespace_id: u64, actor_id: Bytes, current_time: u64) -> Self {
        Self {
            namespace_id,
            actor_id,
            assigned_consumer_id: AtomicU64::new(NONE_U64),
            pending_count: AtomicU32::new(0),
            head_index: AtomicU64::new(0),
            tail_index: AtomicU64::new(0),
            in_flight_index: AtomicU64::new(NONE_U64),
            last_activity_at: AtomicU64::new(current_time),
            attempts: AtomicU32::new(0),
            mailbox: Mutex::new(VecDeque::new()),
            reply_to_map: DashMap::new(),
        }
    }

    /// Restore from a persisted `ActorState` (snapshot install / recovery).
    /// The mailbox is empty — it will be rebuilt from the raft log.
    pub fn from_state(state: ActorState) -> Self {
        Self {
            namespace_id: state.namespace_id,
            actor_id: state.actor_id,
            assigned_consumer_id: AtomicU64::new(opt_to_atomic(state.assigned_consumer_id)),
            pending_count: AtomicU32::new(state.pending_count),
            head_index: AtomicU64::new(state.head_index),
            tail_index: AtomicU64::new(state.tail_index),
            in_flight_index: AtomicU64::new(opt_to_atomic(state.in_flight_index)),
            last_activity_at: AtomicU64::new(state.last_activity_at),
            attempts: AtomicU32::new(state.attempts),
            mailbox: Mutex::new(VecDeque::new()),
            reply_to_map: DashMap::new(),
        }
    }

    // -- Atomic accessors --

    #[inline]
    pub fn assigned_consumer_id(&self) -> Option<u64> {
        atomic_to_opt(self.assigned_consumer_id.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn pending_count(&self) -> u32 {
        self.pending_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn head_index(&self) -> u64 {
        self.head_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn tail_index(&self) -> u64 {
        self.tail_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn in_flight_index(&self) -> Option<u64> {
        atomic_to_opt(self.in_flight_index.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn last_activity_at(&self) -> u64 {
        self.last_activity_at.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn attempts(&self) -> u32 {
        self.attempts.load(Ordering::Relaxed)
    }

    /// Flush atomics back to a serializable `ActorState` for snapshot/persistence.
    pub fn snapshot_state(&self) -> ActorState {
        ActorState {
            namespace_id: self.namespace_id,
            actor_id: self.actor_id.clone(),
            assigned_consumer_id: self.assigned_consumer_id(),
            pending_count: self.pending_count(),
            head_index: self.head_index(),
            tail_index: self.tail_index(),
            in_flight_index: self.in_flight_index(),
            last_activity_at: self.last_activity_at(),
            attempts: self.attempts(),
        }
    }
}

// =============================================================================
// In-memory Actor Namespace State
// =============================================================================

/// Lock-free actor namespace state.
///
/// All inner collections use `DashMap` for concurrent readers and single writer.
/// Methods take `&self` — the Raft apply path (single writer) and leader tasks
/// (readers) can operate on different actors without contention.
pub struct ActorNamespaceState {
    pub meta: ActorNamespaceMeta,
    /// actor_id → ActorInMemory (DashMap for per-actor concurrent access)
    pub actors: DashMap<Bytes, ActorInMemory>,
    /// consumer_id → set of assigned actor_ids
    pub consumer_assignments: DashMap<u64, HashSet<Bytes>>,
    /// Live actor count (atomic for interior mutability).
    /// Synced to `meta.active_actor_count` during snapshot.
    active_count: AtomicU64,
    /// Cached min required index.
    /// `MIN_DIRTY` = needs recompute, `MIN_NONE` = no required, else = cached value.
    cached_min_required: AtomicU64,

    // Pre-initialized metrics
    m_send_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_evict_count: metrics::Counter,
}

impl ActorNamespaceState {
    pub fn new(meta: ActorNamespaceMeta, catalog_name: &str) -> Self {
        let labels = [
            ("catalog", catalog_name.to_owned()),
            ("namespace", meta.name.clone()),
        ];
        let m_send_count = metrics::counter!("mq.actor.send.count", &labels);
        let m_deliver_count = metrics::counter!("mq.actor.deliver.count", &labels);
        let m_ack_count = metrics::counter!("mq.actor.ack.count", &labels);
        let m_evict_count = metrics::counter!("mq.actor.evict.count", &labels);

        Self {
            meta,
            actors: DashMap::new(),
            consumer_assignments: DashMap::new(),
            active_count: AtomicU64::new(0),
            cached_min_required: AtomicU64::new(MIN_NONE),
            m_send_count,
            m_deliver_count,
            m_ack_count,
            m_evict_count,
        }
    }

    /// Current live actor count.
    #[inline]
    pub fn active_count(&self) -> u64 {
        self.active_count.load(Ordering::Relaxed)
    }

    pub fn apply_send(
        &self,
        actor_id: &Bytes,
        log_index: u64,
        current_time: u64,
        reply_to: Option<Bytes>,
    ) -> Result<(), crate::types::MqError> {
        let namespace_id = self.meta.namespace_id;
        let max_depth = self.meta.config.max_mailbox_depth;

        let actor = self.actors.entry(actor_id.clone()).or_insert_with(|| {
            self.active_count.fetch_add(1, Ordering::Relaxed);
            ActorInMemory::new(namespace_id, actor_id.clone(), current_time)
        });

        let pending = actor.pending_count();
        if pending >= max_depth {
            return Err(crate::types::MqError::MailboxFull {
                pending: pending as u32,
            });
        }

        actor.mailbox.lock().push_back(log_index);
        if let Some(rt) = reply_to {
            actor.reply_to_map.insert(log_index, rt);
        }
        actor.pending_count.fetch_add(1, Ordering::Relaxed);
        actor.head_index.store(log_index, Ordering::Relaxed);
        if actor.tail_index() == 0 {
            actor.tail_index.store(log_index, Ordering::Relaxed);
        }
        actor
            .last_activity_at
            .store(current_time, Ordering::Relaxed);

        // Update cached min: new message can only lower the floor
        let cached = self.cached_min_required.load(Ordering::Relaxed);
        if cached != MIN_DIRTY {
            let in_flight = actor.in_flight_index();
            let new_min = in_flight.unwrap_or(actor.tail_index());
            if cached == MIN_NONE {
                self.cached_min_required.store(new_min, Ordering::Relaxed);
            } else {
                self.cached_min_required
                    .store(cached.min(new_min), Ordering::Relaxed);
            }
        }

        self.m_send_count.increment(1);
        Ok(())
    }

    pub fn apply_deliver(&self, actor_id: &Bytes, consumer_id: u64) -> Option<u64> {
        let actor = self.actors.get(actor_id)?;

        // Must be assigned to this consumer
        if actor.assigned_consumer_id() != Some(consumer_id) {
            return None;
        }

        // Already has an in-flight message
        if actor.in_flight_index().is_some() {
            return None;
        }

        let msg_index = actor.mailbox.lock().pop_front()?;
        actor.in_flight_index.store(msg_index, Ordering::Relaxed);
        let prev = actor.pending_count.load(Ordering::Relaxed);
        actor
            .pending_count
            .store(prev.saturating_sub(1), Ordering::Relaxed);
        actor.attempts.fetch_add(1, Ordering::Relaxed);

        self.m_deliver_count.increment(1);
        Some(msg_index)
    }

    /// ACK a message and return its `reply_to` topic name bytes (if any).
    pub fn apply_ack(&self, actor_id: &Bytes, message_id: u64) -> Option<Bytes> {
        if let Some(actor) = self.actors.get(actor_id) {
            if actor.in_flight_index() == Some(message_id) {
                actor.in_flight_index.store(NONE_U64, Ordering::Relaxed);
                actor.attempts.store(0, Ordering::Relaxed);
                let reply_to = actor.reply_to_map.remove(&message_id).map(|(_, v)| v);
                // Update tail to next pending if any
                if let Some(&next) = actor.mailbox.lock().front() {
                    actor.tail_index.store(next, Ordering::Relaxed);
                }
                // Invalidate if this message could have been the global min
                let cached = self.cached_min_required.load(Ordering::Relaxed);
                if cached != MIN_DIRTY && cached != MIN_NONE && message_id <= cached {
                    self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
                }
                self.m_ack_count.increment(1);
                return reply_to;
            }
        }
        None
    }

    pub fn apply_nack(&self, actor_id: &Bytes, message_id: u64) {
        if let Some(actor) = self.actors.get(actor_id) {
            if actor.in_flight_index() == Some(message_id) {
                actor.mailbox.lock().push_front(message_id);
                actor.in_flight_index.store(NONE_U64, Ordering::Relaxed);
                actor.pending_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn apply_assign(&self, consumer_id: u64, actor_ids: &[Bytes]) {
        let mut set = self.consumer_assignments.entry(consumer_id).or_default();

        for actor_id in actor_ids {
            if let Some(actor) = self.actors.get(actor_id) {
                actor
                    .assigned_consumer_id
                    .store(consumer_id, Ordering::Relaxed);
                set.insert(actor_id.clone());
            }
        }
    }

    pub fn apply_release(&self, consumer_id: u64) {
        if let Some((_, actor_ids)) = self.consumer_assignments.remove(&consumer_id) {
            for actor_id in &actor_ids {
                if let Some(actor) = self.actors.get(actor_id) {
                    actor
                        .assigned_consumer_id
                        .store(NONE_U64, Ordering::Relaxed);
                    // Return in-flight message to mailbox
                    let in_flight = actor.in_flight_index();
                    if let Some(msg_id) = in_flight {
                        actor.in_flight_index.store(NONE_U64, Ordering::Relaxed);
                        actor.mailbox.lock().push_front(msg_id);
                        actor.pending_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    pub fn apply_evict_idle(&self, before_timestamp: u64) -> usize {
        let before = self.actors.len();
        self.actors.retain(|_actor_id, actor| {
            let keep = actor.pending_count() > 0
                || actor.in_flight_index().is_some()
                || actor.last_activity_at() >= before_timestamp;
            if !keep {
                self.active_count.fetch_sub(1, Ordering::Relaxed);
            }
            keep
        });
        let count = before - self.actors.len();
        if count > 0 {
            self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
        }
        self.m_evict_count.increment(count as u64);
        count
    }

    /// Get unassigned actors that have pending messages.
    /// Returns owned `Bytes` keys (DashMap guards don't expose long-lived refs).
    pub fn unassigned_actors_with_messages(&self) -> Vec<Bytes> {
        self.actors
            .iter()
            .filter(|entry| {
                let a = entry.value();
                a.assigned_consumer_id().is_none()
                    && (a.pending_count() > 0 || a.in_flight_index().is_some())
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Returns the minimum log index required by this namespace (for purge floor).
    /// Uses an atomic cache maintained incrementally by send/ack/evict.
    pub fn min_required_index(&self) -> Option<u64> {
        let cached = self.cached_min_required.load(Ordering::Relaxed);
        if cached == MIN_NONE {
            return None;
        }
        if cached != MIN_DIRTY {
            return Some(cached);
        }
        // Recompute
        let mut min: Option<u64> = None;
        for entry in self.actors.iter() {
            let actor = entry.value();
            if actor.pending_count() > 0 || actor.in_flight_index().is_some() {
                let actor_min = actor.in_flight_index().unwrap_or(actor.tail_index());
                min = Some(min.map_or(actor_min, |m: u64| m.min(actor_min)));
            }
        }
        match min {
            Some(v) => {
                self.cached_min_required.store(v, Ordering::Relaxed);
                Some(v)
            }
            None => {
                self.cached_min_required.store(MIN_NONE, Ordering::Relaxed);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ActorConfig;

    fn make_ns(name: &str) -> ActorNamespaceState {
        let meta = ActorNamespaceMeta::new(1, name.to_string(), 1000, ActorConfig::default());
        ActorNamespaceState::new(meta, "test")
    }

    fn actor_id(name: &str) -> Bytes {
        Bytes::from(name.to_string())
    }

    #[test]
    fn test_send_creates_actor() {
        let ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        assert_eq!(ns.actors.len(), 1);
        assert_eq!(ns.active_count(), 1);

        let actor = ns.actors.get(&aid).unwrap();
        assert_eq!(actor.pending_count(), 1);
        assert_eq!(actor.head_index(), 10);
        assert_eq!(actor.tail_index(), 10);
        assert_eq!(actor.mailbox.lock().len(), 1);
    }

    #[test]
    fn test_send_multiple_messages() {
        let ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 11, 1001, None).unwrap();
        ns.apply_send(&aid, 12, 1002, None).unwrap();

        let actor = ns.actors.get(&aid).unwrap();
        assert_eq!(actor.pending_count(), 3);
        assert_eq!(actor.head_index(), 12);
        assert_eq!(actor.tail_index(), 10);
        assert_eq!(actor.mailbox.lock().len(), 3);
    }

    #[test]
    fn test_send_mailbox_full() {
        let config = ActorConfig {
            max_mailbox_depth: 2,
            ..Default::default()
        };
        let meta = ActorNamespaceMeta::new(1, "test".to_string(), 1000, config);
        let ns = ActorNamespaceState::new(meta, "test");
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
        let ns = make_ns("test");
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
        let ns = make_ns("test");
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
        let ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 11, 1001, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);

        ns.apply_deliver(&aid, 100);
        ns.apply_ack(&aid, 10);

        let actor = ns.actors.get(&aid).unwrap();
        assert!(actor.in_flight_index().is_none());
        assert_eq!(actor.attempts(), 0);
        drop(actor);

        // Can now deliver next
        let msg = ns.apply_deliver(&aid, 100);
        assert_eq!(msg, Some(11));
    }

    #[test]
    fn test_nack_returns_to_front() {
        let ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);
        ns.apply_deliver(&aid, 100);

        ns.apply_nack(&aid, 10);
        let actor = ns.actors.get(&aid).unwrap();
        assert!(actor.in_flight_index().is_none());
        assert_eq!(actor.pending_count(), 1);
        assert_eq!(actor.mailbox.lock().front().copied(), Some(10));
    }

    #[test]
    fn test_assign_and_release() {
        let ns = make_ns("test");
        let a1 = actor_id("user-1");
        let a2 = actor_id("user-2");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_send(&a2, 11, 1001, None).unwrap();

        ns.apply_assign(100, &[a1.clone(), a2.clone()]);
        assert_eq!(ns.consumer_assignments.get(&100).unwrap().len(), 2);
        assert_eq!(
            ns.actors.get(&a1).unwrap().assigned_consumer_id(),
            Some(100)
        );

        ns.apply_release(100);
        assert!(!ns.consumer_assignments.contains_key(&100));
        assert!(ns.actors.get(&a1).unwrap().assigned_consumer_id().is_none());
    }

    #[test]
    fn test_release_returns_in_flight() {
        let ns = make_ns("test");
        let aid = actor_id("user-1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);
        ns.apply_deliver(&aid, 100);

        // In-flight message exists
        assert_eq!(ns.actors.get(&aid).unwrap().in_flight_index(), Some(10));

        ns.apply_release(100);
        let actor = ns.actors.get(&aid).unwrap();
        assert!(actor.in_flight_index().is_none());
        assert_eq!(actor.pending_count(), 1);
        assert_eq!(actor.mailbox.lock().front().copied(), Some(10));
    }

    #[test]
    fn test_evict_idle() {
        let ns = make_ns("test");
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
        let ns = make_ns("test");
        let a1 = actor_id("unassigned");
        let a2 = actor_id("assigned");
        let _a3 = actor_id("empty");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_send(&a2, 11, 1000, None).unwrap();
        // a3 not created (no messages)

        ns.apply_assign(100, &[a2.clone()]);

        let unassigned = ns.unassigned_actors_with_messages();
        assert_eq!(unassigned.len(), 1);
        assert!(unassigned.contains(&a1));
    }

    #[test]
    fn test_min_required_index() {
        let ns = make_ns("test");
        assert!(ns.min_required_index().is_none());

        let a1 = actor_id("a1");
        let a2 = actor_id("a2");

        ns.apply_send(&a1, 10, 1000, None).unwrap();
        ns.apply_send(&a2, 20, 1000, None).unwrap();

        assert_eq!(ns.min_required_index(), Some(10));
    }

    #[test]
    fn test_min_required_index_with_in_flight() {
        let ns = make_ns("test");
        let aid = actor_id("a1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 20, 1001, None).unwrap();
        ns.apply_assign(100, &[aid.clone()]);
        ns.apply_deliver(&aid, 100); // msg 10 is now in-flight

        assert_eq!(ns.min_required_index(), Some(10));
    }

    #[test]
    fn test_multiple_actors_different_consumers() {
        let ns = make_ns("test");
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

    #[test]
    fn test_snapshot_state_roundtrip() {
        let ns = make_ns("test");
        let aid = actor_id("a1");

        ns.apply_send(&aid, 10, 1000, None).unwrap();
        ns.apply_send(&aid, 20, 2000, None).unwrap();
        ns.apply_assign(42, &[aid.clone()]);
        ns.apply_deliver(&aid, 42);

        let actor = ns.actors.get(&aid).unwrap();
        let snap = actor.snapshot_state();
        assert_eq!(snap.namespace_id, 1);
        assert_eq!(snap.actor_id, aid);
        assert_eq!(snap.assigned_consumer_id, Some(42));
        assert_eq!(snap.pending_count, 1);
        assert_eq!(snap.head_index, 20);
        assert_eq!(snap.in_flight_index, Some(10));
        assert_eq!(snap.last_activity_at, 2000);
        assert_eq!(snap.attempts, 1);
    }
}
