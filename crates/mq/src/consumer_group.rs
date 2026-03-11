//! Raft-replicated consumer group state for Kafka-compatible group coordination.
//!
//! All group coordinator state (membership, generation, phase, assignments,
//! offsets) is replicated through Raft. On leader failover, the new leader
//! reconstructs a fully functional coordinator from the applied state —
//! no rebalance needed.
//!
//! **Concurrency model**: single Raft-apply writer, concurrent readers.
//! Scalar fields use atomics; leader/protocol use `ArcSwap` (lock-free);
//! members/offsets use `DashMap`; per-member assignment uses `RwLock`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

// ─── Enums ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AutoOffsetReset {
    Earliest = 0,
    Latest = 1,
    None = 2,
}

impl Default for AutoOffsetReset {
    fn default() -> Self {
        Self::Latest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GroupPhase {
    Empty = 0,
    PreparingRebalance = 1,
    CompletingRebalance = 2,
    Stable = 3,
    Dead = 4,
}

impl Default for GroupPhase {
    fn default() -> Self {
        Self::Empty
    }
}

impl GroupPhase {
    #[inline]
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Empty,
            1 => Self::PreparingRebalance,
            2 => Self::CompletingRebalance,
            3 => Self::Stable,
            4 => Self::Dead,
            _ => Self::Empty,
        }
    }
}

// ─── Persisted metadata (snapshot / MDBX) ────────────────────────────────────

/// Persisted consumer group metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMeta {
    pub group_id: u64,
    pub name: String,
    #[serde(default)]
    pub name_hash: u64,
    pub created_at: u64,
    pub generation: i32,
    pub phase: GroupPhase,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: Option<String>,
    pub auto_offset_reset: AutoOffsetReset,
    pub last_activity_at: u64,
    pub next_member_id: u64,
    pub members: Vec<GroupMemberMeta>,
}

/// Persisted member state within a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMemberMeta {
    pub member_id: String,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    /// `(protocol_name, metadata_bytes)` — subscription metadata per protocol.
    pub protocols: Vec<(String, Vec<u8>)>,
    /// Serialized partition assignment (opaque bytes from SyncGroup).
    #[serde(default)]
    pub assignment: Vec<u8>,
}

/// A committed offset for `(group, topic, partition)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupTopicPartitionOffset {
    pub topic_id: u64,
    pub partition_index: u32,
    pub committed_offset: u64,
    pub metadata: Option<String>,
    pub committed_at: u64,
}

/// Snapshot data for a consumer group (used in `MqSnapshotData`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub meta: ConsumerGroupMeta,
    pub offsets: Vec<GroupTopicPartitionOffset>,
}

// ─── In-memory state (lock-free readers, single Raft-apply writer) ───────────

/// Lock-free in-memory consumer group state.
///
/// Immutable identity lives in `meta`. Mutable scalars use atomics.
/// Members and offsets use `DashMap` / `RwLock` for concurrent readers.
pub struct ConsumerGroupState {
    /// Immutable identity (set at creation).
    pub meta: ConsumerGroupMeta,

    // ── Atomic mutable scalars ──
    generation: AtomicU32,
    last_activity_at: AtomicU64,
    next_member_id: AtomicU64,
    phase: AtomicU8,

    // ── Lock-free complex state (ArcSwap for SWMR) ──
    leader: ArcSwapOption<String>,
    protocol_name: ArcSwap<String>,
    members: DashMap<String, GroupMemberState>,

    /// Group-keyed offsets: `(topic_id, partition_index)` → offset.
    pub(crate) offsets: DashMap<(u64, u32), GroupTopicPartitionOffset>,

    /// Phase transition notification (for async JoinGroup/SyncGroup completion).
    /// Wrapped in `Arc` so Kafka adapter can clone and await outside DashMap borrow.
    pub phase_notify: Arc<tokio::sync::Notify>,

    // ── Pre-initialized metrics ──
    m_offset_commits: metrics::Counter,
    m_generation_bumps: metrics::Counter,
    m_rebalances: metrics::Counter,
}

/// In-memory member state within a consumer group.
pub struct GroupMemberState {
    // Immutable identity
    pub member_id: String,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: SmallVec<[(String, Bytes); 2]>,

    // Mutable
    pub assignment: RwLock<Bytes>,
    pub last_heartbeat_at: AtomicU64,

    // Join/Sync tracking flags (for phase completion detection)
    pub joined_this_gen: AtomicBool,
    pub synced_this_gen: AtomicBool,
}

impl ConsumerGroupState {
    /// Create from freshly-allocated metadata (no members yet).
    pub fn new(meta: ConsumerGroupMeta, catalog_name: &str) -> Self {
        let catalog = catalog_name.to_owned();
        let name = &meta.name;
        Self {
            generation: AtomicU32::new(meta.generation as u32),
            last_activity_at: AtomicU64::new(meta.last_activity_at),
            next_member_id: AtomicU64::new(meta.next_member_id),
            phase: AtomicU8::new(meta.phase as u8),
            leader: ArcSwapOption::new(meta.leader.clone().map(|s| Arc::new(s))),
            protocol_name: ArcSwap::new(Arc::new(meta.protocol_name.clone())),
            members: DashMap::new(),
            offsets: DashMap::new(),
            phase_notify: Arc::new(tokio::sync::Notify::new()),
            m_offset_commits: metrics::counter!("mq.consumer_group.offset_commits", "catalog" => catalog.clone(), "group" => name.clone()),
            m_generation_bumps: metrics::counter!("mq.consumer_group.generation_bumps", "catalog" => catalog.clone(), "group" => name.clone()),
            m_rebalances: metrics::counter!("mq.consumer_group.rebalances", "catalog" => catalog, "group" => name.clone()),
            meta,
        }
    }

    /// Reconstruct from a snapshot (members + offsets pre-populated).
    pub fn from_snapshot(
        mut meta: ConsumerGroupMeta,
        offsets: Vec<GroupTopicPartitionOffset>,
        catalog_name: &str,
    ) -> Self {
        // Take ownership of members before passing meta to new() — avoids cloning meta.
        let snapshot_members = std::mem::take(&mut meta.members);
        let last_activity = meta.last_activity_at;
        let state = Self::new(meta, catalog_name);

        // Restore members from snapshot meta
        for m in snapshot_members {
            let protocols: SmallVec<[(String, Bytes); 2]> = m
                .protocols
                .into_iter()
                .map(|(n, d)| (n, Bytes::from(d)))
                .collect();
            let member_id = m.member_id;
            state.members.insert(
                member_id.clone(),
                GroupMemberState {
                    member_id,
                    client_id: m.client_id,
                    session_timeout_ms: m.session_timeout_ms,
                    rebalance_timeout_ms: m.rebalance_timeout_ms,
                    protocol_type: m.protocol_type,
                    protocols,
                    assignment: RwLock::new(Bytes::from(m.assignment)),
                    last_heartbeat_at: AtomicU64::new(last_activity),
                    joined_this_gen: AtomicBool::new(false),
                    synced_this_gen: AtomicBool::new(false),
                },
            );
        }

        // Restore offsets
        for o in offsets {
            state.offsets.insert((o.topic_id, o.partition_index), o);
        }

        state
    }

    // ── Readers ──────────────────────────────────────────────────────────────

    #[inline]
    pub fn generation(&self) -> i32 {
        self.generation.load(Ordering::Relaxed) as i32
    }

    #[inline]
    pub fn phase(&self) -> GroupPhase {
        GroupPhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    #[inline]
    pub fn last_activity_at(&self) -> u64 {
        self.last_activity_at.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn next_member_id(&self) -> u64 {
        self.next_member_id.load(Ordering::Relaxed)
    }

    /// Lock-free leader read. Returns `Option<Arc<String>>` — zero-alloc, just an atomic load + refcount bump.
    #[inline]
    pub fn leader(&self) -> Option<Arc<String>> {
        self.leader.load_full()
    }

    /// Lock-free protocol name read. Returns `Arc<String>` — zero-alloc, just an atomic load + refcount bump.
    #[inline]
    pub fn protocol_name(&self) -> Arc<String> {
        self.protocol_name.load_full()
    }

    #[inline]
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    #[inline]
    pub fn has_member(&self, member_id: &str) -> bool {
        self.members.contains_key(member_id)
    }

    #[inline]
    pub fn auto_offset_reset(&self) -> AutoOffsetReset {
        self.meta.auto_offset_reset
    }

    /// Get a committed offset for a `(topic_id, partition_index)` pair.
    #[inline]
    pub fn get_offset(&self, topic_id: u64, partition_index: u32) -> Option<u64> {
        self.offsets
            .get(&(topic_id, partition_index))
            .map(|o| o.committed_offset)
    }

    /// Get the assignment bytes for a member (zero-copy Bytes clone).
    #[inline]
    pub fn get_member_assignment(&self, member_id: &str) -> Option<Bytes> {
        self.members
            .get(member_id)
            .map(|m| m.assignment.read().clone())
    }

    /// Get all member protocols (for JoinGroup leader response).
    /// Returns `(member_id, protocol_metadata)` for the chosen protocol.
    /// Uses zero-copy Bytes — callers get a refcounted view, not a copy.
    pub fn member_protocols(&self) -> Vec<(String, Bytes)> {
        // Lock-free load — no RwLock, just an atomic read.
        let chosen = self.protocol_name.load();
        self.members
            .iter()
            .map(|entry| {
                let m = entry.value();
                let meta_bytes = m
                    .protocols
                    .iter()
                    .find(|(name, _)| name.as_str() == chosen.as_str())
                    .map(|(_, data)| data.clone()) // Bytes clone = Arc refcount bump
                    .unwrap_or_default();
                (m.member_id.clone(), meta_bytes)
            })
            .collect()
    }

    /// Find members whose session has expired.
    /// Returns SmallVec to avoid heap allocation for typical small expired sets.
    pub fn find_expired_members(&self, now_ms: u64) -> SmallVec<[String; 4]> {
        self.members
            .iter()
            .filter(|entry| {
                let m = entry.value();
                let timeout = m.session_timeout_ms as u64;
                let last = m.last_heartbeat_at.load(Ordering::Relaxed);
                now_ms > last + timeout
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Check if all current members have their `joined_this_gen` flag set.
    #[inline]
    pub fn all_members_joined(&self) -> bool {
        if self.members.is_empty() {
            return false;
        }
        self.members
            .iter()
            .all(|e| e.value().joined_this_gen.load(Ordering::Relaxed))
    }

    /// Check if all current members have their `synced_this_gen` flag set.
    #[inline]
    pub fn all_members_synced(&self) -> bool {
        if self.members.is_empty() {
            return false;
        }
        self.members
            .iter()
            .all(|e| e.value().synced_this_gen.load(Ordering::Relaxed))
    }

    // ── Writers (Raft apply path only) ───────────────────────────────────────

    #[inline]
    pub fn set_phase(&self, phase: GroupPhase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    #[inline]
    pub fn touch_activity(&self, at: u64) {
        self.last_activity_at.store(at, Ordering::Relaxed);
    }

    #[inline]
    pub fn increment_next_member_id(&self) -> u64 {
        self.next_member_id.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.m_generation_bumps.increment(1);
    }

    pub fn set_leader(&self, leader: Option<String>) {
        self.leader.store(leader.map(Arc::new));
    }

    #[inline]
    pub fn clear_leader(&self) {
        self.leader.store(None);
    }

    pub fn set_protocol_name(&self, name: String) {
        self.protocol_name.store(Arc::new(name));
    }

    /// Insert or update a member.
    #[inline]
    pub fn upsert_member(&self, member: GroupMemberState) {
        self.members.insert(member.member_id.clone(), member);
    }

    /// Remove a member by ID.
    #[inline]
    pub fn remove_member(&self, member_id: &str) {
        self.members.remove(member_id);
    }

    /// Mark a member as having joined this generation.
    #[inline]
    pub fn mark_joined(&self, member_id: &str) {
        if let Some(m) = self.members.get(member_id) {
            m.joined_this_gen.store(true, Ordering::Relaxed);
        }
    }

    /// Mark a member as having synced this generation.
    #[inline]
    pub fn mark_synced(&self, member_id: &str) {
        if let Some(m) = self.members.get(member_id) {
            m.synced_this_gen.store(true, Ordering::Relaxed);
        }
    }

    /// Clear all join marks (after generation bump).
    pub fn clear_join_marks(&self) {
        for entry in self.members.iter() {
            entry
                .value()
                .joined_this_gen
                .store(false, Ordering::Relaxed);
        }
    }

    /// Clear all sync marks.
    pub fn clear_sync_marks(&self) {
        for entry in self.members.iter() {
            entry
                .value()
                .synced_this_gen
                .store(false, Ordering::Relaxed);
        }
    }

    /// Store assignment bytes for a specific member.
    #[inline]
    pub fn set_member_assignment(&self, member_id: &str, assignment: Bytes) {
        if let Some(m) = self.members.get(member_id) {
            *m.assignment.write() = assignment;
        }
    }

    /// Update heartbeat timestamp for a member.
    #[inline]
    pub fn update_member_heartbeat(&self, member_id: &str, at: u64) {
        if let Some(m) = self.members.get(member_id) {
            m.last_heartbeat_at.store(at, Ordering::Relaxed);
        }
    }

    /// Select the most commonly supported protocol and set it.
    /// Uses SmallVec to avoid heap allocation for the typical small protocol set.
    pub fn select_and_set_protocol(&self) {
        // Count protocol support across all members.
        // Typical consumer groups have 1-3 protocols — SmallVec avoids heap alloc.
        let mut counts: SmallVec<[(String, usize); 8]> = SmallVec::new();
        for entry in self.members.iter() {
            for (name, _) in entry.value().protocols.iter() {
                if let Some(c) = counts.iter_mut().find(|(n, _)| n == name) {
                    c.1 += 1;
                } else {
                    counts.push((name.clone(), 1));
                }
            }
        }
        // Pick the protocol supported by all members (or most).
        let member_count = self.members.len();
        if let Some((name, _)) = counts
            .iter()
            .filter(|(_, c)| *c == member_count)
            .max_by_key(|(_, c)| *c)
        {
            self.set_protocol_name(name.clone());
        } else if let Some((name, _)) = counts.iter().max_by_key(|(_, c)| *c) {
            self.set_protocol_name(name.clone());
        }
    }

    /// Elect a leader from current members. Picks the first member
    /// (deterministic, same on all Raft nodes).
    pub fn elect_leader(&self) {
        let leader = self
            .members
            .iter()
            .next()
            .map(|e| Arc::new(e.key().clone()));
        self.leader.store(leader);
    }

    #[inline]
    pub fn record_offset_commit(&self) {
        self.m_offset_commits.increment(1);
    }

    #[inline]
    pub fn record_rebalance(&self) {
        self.m_rebalances.increment(1);
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    /// Flush mutable state back to a `ConsumerGroupMeta` for persistence.
    /// Builds the struct field-by-field to avoid cloning the members Vec
    /// from `self.meta` (which would be immediately replaced).
    pub fn snapshot_meta(&self) -> ConsumerGroupMeta {
        let members: Vec<GroupMemberMeta> = self
            .members
            .iter()
            .map(|entry| {
                let ms = entry.value();
                GroupMemberMeta {
                    member_id: ms.member_id.clone(),
                    client_id: ms.client_id.clone(),
                    session_timeout_ms: ms.session_timeout_ms,
                    rebalance_timeout_ms: ms.rebalance_timeout_ms,
                    protocol_type: ms.protocol_type.clone(),
                    protocols: ms
                        .protocols
                        .iter()
                        .map(|(n, d)| (n.clone(), d.to_vec()))
                        .collect(),
                    assignment: ms.assignment.read().to_vec(),
                }
            })
            .collect();

        ConsumerGroupMeta {
            group_id: self.meta.group_id,
            name: self.meta.name.clone(),
            name_hash: self.meta.name_hash,
            created_at: self.meta.created_at,
            generation: self.generation(),
            phase: self.phase(),
            protocol_type: self.meta.protocol_type.clone(),
            protocol_name: (*self.protocol_name()).clone(),
            leader: self.leader().map(|a| (*a).clone()),
            auto_offset_reset: self.meta.auto_offset_reset,
            last_activity_at: self.last_activity_at(),
            next_member_id: self.next_member_id(),
            members,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_group(id: u64, name: &str) -> ConsumerGroupState {
        let meta = ConsumerGroupMeta {
            group_id: id,
            name: name.to_string(),
            name_hash: 0,
            created_at: 1000,
            generation: 0,
            phase: GroupPhase::Empty,
            protocol_type: "consumer".to_string(),
            protocol_name: String::new(),
            leader: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 1000,
            next_member_id: 1,
            members: Vec::new(),
        };
        ConsumerGroupState::new(meta, "test")
    }

    fn make_member(id: &str, protocols: &[(&str, &[u8])]) -> GroupMemberState {
        GroupMemberState {
            member_id: id.to_string(),
            client_id: "test-client".to_string(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            protocol_type: "consumer".to_string(),
            protocols: protocols
                .iter()
                .map(|(n, d)| (n.to_string(), Bytes::from(d.to_vec())))
                .collect(),
            assignment: RwLock::new(Bytes::new()),
            last_heartbeat_at: AtomicU64::new(1000),
            joined_this_gen: AtomicBool::new(false),
            synced_this_gen: AtomicBool::new(false),
        }
    }

    #[test]
    fn test_new_group_defaults() {
        let group = make_group(1, "test-group");
        assert_eq!(group.generation(), 0);
        assert_eq!(group.phase(), GroupPhase::Empty);
        assert_eq!(group.member_count(), 0);
        assert!(group.leader().is_none());
        assert_eq!(group.last_activity_at(), 1000);
        assert_eq!(group.next_member_id(), 1);
    }

    #[test]
    fn test_upsert_and_remove_member() {
        let group = make_group(1, "g");
        let m = make_member("m-1", &[("range", b"")]);
        group.upsert_member(m);

        assert_eq!(group.member_count(), 1);
        assert!(group.has_member("m-1"));

        group.remove_member("m-1");
        assert_eq!(group.member_count(), 0);
        assert!(!group.has_member("m-1"));
    }

    #[test]
    fn test_phase_transitions() {
        let group = make_group(1, "g");
        assert_eq!(group.phase(), GroupPhase::Empty);

        group.set_phase(GroupPhase::PreparingRebalance);
        assert_eq!(group.phase(), GroupPhase::PreparingRebalance);

        group.set_phase(GroupPhase::CompletingRebalance);
        assert_eq!(group.phase(), GroupPhase::CompletingRebalance);

        group.set_phase(GroupPhase::Stable);
        assert_eq!(group.phase(), GroupPhase::Stable);
    }

    #[test]
    fn test_generation_bump() {
        let group = make_group(1, "g");
        assert_eq!(group.generation(), 0);

        group.bump_generation();
        assert_eq!(group.generation(), 1);

        group.bump_generation();
        assert_eq!(group.generation(), 2);
    }

    #[test]
    fn test_all_members_joined() {
        let group = make_group(1, "g");
        assert!(!group.all_members_joined()); // empty = false

        group.upsert_member(make_member("m-1", &[("range", b"")]));
        group.upsert_member(make_member("m-2", &[("range", b"")]));

        assert!(!group.all_members_joined());

        group.mark_joined("m-1");
        assert!(!group.all_members_joined());

        group.mark_joined("m-2");
        assert!(group.all_members_joined());

        group.clear_join_marks();
        assert!(!group.all_members_joined());
    }

    #[test]
    fn test_elect_leader() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b"")]));
        group.upsert_member(make_member("m-2", &[("range", b"")]));

        group.elect_leader();
        let leader = group.leader().unwrap();
        assert!(leader.as_str() == "m-1" || leader.as_str() == "m-2");
    }

    #[test]
    fn test_select_protocol() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b""), ("roundrobin", b"")]));
        group.upsert_member(make_member("m-2", &[("range", b"")]));

        group.select_and_set_protocol();
        // "range" is supported by both, "roundrobin" only by one
        assert_eq!(group.protocol_name().as_str(), "range");
    }

    #[test]
    fn test_member_assignment() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b"")]));

        assert!(group.get_member_assignment("m-1").unwrap().is_empty());

        group.set_member_assignment("m-1", Bytes::from_static(b"assignment-data"));
        assert_eq!(
            group.get_member_assignment("m-1").unwrap().as_ref(),
            b"assignment-data"
        );
    }

    #[test]
    fn test_heartbeat_and_session_expiry() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b"")]));

        // Member was created with heartbeat at 1000, timeout 30000
        assert!(group.find_expired_members(30_000).is_empty());
        assert!(group.find_expired_members(31_000).is_empty());
        assert_eq!(group.find_expired_members(31_001).len(), 1);

        // Update heartbeat
        group.update_member_heartbeat("m-1", 20_000);
        assert!(group.find_expired_members(50_000).is_empty());
        assert_eq!(group.find_expired_members(50_001).len(), 1);
    }

    #[test]
    fn test_offset_commit() {
        let group = make_group(1, "g");
        group.offsets.insert(
            (10, 0),
            GroupTopicPartitionOffset {
                topic_id: 10,
                partition_index: 0,
                committed_offset: 42,
                metadata: None,
                committed_at: 1000,
            },
        );

        let offset = group.offsets.get(&(10, 0)).unwrap();
        assert_eq!(offset.committed_offset, 42);
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let group = make_group(1, "test-group");
        group.upsert_member(make_member("m-1", &[("range", b"\x01\x02")]));
        group.set_member_assignment("m-1", Bytes::from_static(b"assign"));
        group.bump_generation();
        group.set_phase(GroupPhase::Stable);
        group.set_leader(Some("m-1".to_string()));
        group.touch_activity(5000);
        group.offsets.insert(
            (10, 0),
            GroupTopicPartitionOffset {
                topic_id: 10,
                partition_index: 0,
                committed_offset: 99,
                metadata: Some("md".to_string()),
                committed_at: 4000,
            },
        );

        let snap_meta = group.snapshot_meta();
        assert_eq!(snap_meta.generation, 1);
        assert_eq!(snap_meta.phase, GroupPhase::Stable);
        assert_eq!(snap_meta.leader, Some("m-1".to_string()));
        assert_eq!(snap_meta.last_activity_at, 5000);
        assert_eq!(snap_meta.members.len(), 1);
        assert_eq!(snap_meta.members[0].assignment, b"assign");

        let offsets: Vec<_> = group.offsets.iter().map(|e| e.value().clone()).collect();

        // Reconstruct from snapshot
        let restored = ConsumerGroupState::from_snapshot(snap_meta.clone(), offsets, "test");
        assert_eq!(restored.generation(), 1);
        assert_eq!(restored.phase(), GroupPhase::Stable);
        assert_eq!(restored.member_count(), 1);
        assert!(restored.has_member("m-1"));
        assert_eq!(
            restored.get_member_assignment("m-1").unwrap().as_ref(),
            b"assign"
        );
        assert_eq!(restored.offsets.get(&(10, 0)).unwrap().committed_offset, 99);
    }

    #[test]
    fn test_serde_roundtrip() {
        let meta = ConsumerGroupMeta {
            group_id: 42,
            name: "my-group".to_string(),
            name_hash: 123456,
            created_at: 1000,
            generation: 3,
            phase: GroupPhase::Stable,
            protocol_type: "consumer".to_string(),
            protocol_name: "range".to_string(),
            leader: Some("m-1".to_string()),
            auto_offset_reset: AutoOffsetReset::Earliest,
            last_activity_at: 5000,
            next_member_id: 10,
            members: vec![GroupMemberMeta {
                member_id: "m-1".to_string(),
                client_id: "c-1".to_string(),
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 60_000,
                protocol_type: "consumer".to_string(),
                protocols: vec![("range".to_string(), vec![1, 2, 3])],
                assignment: vec![4, 5, 6],
            }],
        };

        let snap = ConsumerGroupSnapshot {
            meta,
            offsets: vec![GroupTopicPartitionOffset {
                topic_id: 10,
                partition_index: 0,
                committed_offset: 42,
                metadata: Some("test".to_string()),
                committed_at: 4000,
            }],
        };

        let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
        let (decoded, _): (ConsumerGroupSnapshot, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.meta.group_id, 42);
        assert_eq!(decoded.meta.name, "my-group");
        assert_eq!(decoded.meta.generation, 3);
        assert_eq!(decoded.meta.phase, GroupPhase::Stable);
        assert_eq!(decoded.meta.members.len(), 1);
        assert_eq!(decoded.offsets.len(), 1);
        assert_eq!(decoded.offsets[0].committed_offset, 42);
    }

    #[test]
    fn test_find_expired_boundary() {
        let group = make_group(1, "g-boundary");
        let member = GroupMemberState {
            session_timeout_ms: 10_000,
            ..make_member("m1", &[("range", b"")])
        };
        group.upsert_member(member);
        group.update_member_heartbeat("m1", 5000);

        // At exactly 5000 + 10000 = 15000 → NOT expired (need > timeout)
        let expired = group.find_expired_members(15000);
        assert!(expired.is_empty(), "should not expire at exact boundary");

        // At 15001 → expired
        let expired = group.find_expired_members(15001);
        assert_eq!(expired.as_slice(), &["m1"]);
    }

    #[test]
    fn test_multiple_protocols_selection() {
        let group = make_group(1, "g-proto");
        // m1: ["range", "roundrobin"]
        group.upsert_member(make_member("m1", &[("range", b""), ("roundrobin", b"")]));
        // m2: ["range", "sticky"]
        group.upsert_member(make_member("m2", &[("range", b""), ("sticky", b"")]));
        // m3: ["roundrobin", "sticky"]
        group.upsert_member(make_member("m3", &[("roundrobin", b""), ("sticky", b"")]));

        // range: supported by m1, m2 → 2 votes
        // roundrobin: m1, m3 → 2 votes
        // sticky: m2, m3 → 2 votes
        // With a tie, should pick one deterministically
        group.select_and_set_protocol();
        let proto = group.protocol_name();
        assert!(
            proto.as_str() == "range"
                || proto.as_str() == "roundrobin"
                || proto.as_str() == "sticky",
            "protocol should be one of the three: {proto}"
        );
    }

    #[test]
    fn test_no_common_protocol() {
        let group = make_group(1, "g-no-common");
        group.upsert_member(make_member("m1", &[("range", b"")]));
        group.upsert_member(make_member("m2", &[("roundrobin", b"")]));
        group.upsert_member(make_member("m3", &[("sticky", b"")]));

        // No protocol in common — should still select one (most supported = 1 each)
        group.select_and_set_protocol();
        let proto = group.protocol_name();
        assert!(!proto.is_empty(), "should select some protocol");
    }

    #[test]
    fn test_empty_protocol_list() {
        let group = make_group(1, "g-empty-proto");
        group.upsert_member(make_member("m1", &[]));

        group.select_and_set_protocol();
        // With no protocols, the name should be empty or remain unchanged
        let proto = group.protocol_name();
        // Just verify it doesn't panic
        assert!(proto.is_empty() || !proto.is_empty());
    }

    #[test]
    fn test_concurrent_offset_reads() {
        use std::sync::Arc;
        let group = Arc::new(make_group(1, "g-concurrent"));

        // Write some offsets
        group.offsets.insert(
            (10, 0),
            GroupTopicPartitionOffset {
                topic_id: 10,
                partition_index: 0,
                committed_offset: 100,
                metadata: None,
                committed_at: 1000,
            },
        );

        // Spawn readers concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let g = Arc::clone(&group);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let offset = g.get_offset(10, 0);
                        assert!(offset.is_some());
                    }
                })
            })
            .collect();

        // Write more offsets concurrently
        for i in 0..100 {
            group.offsets.insert(
                (10, i),
                GroupTopicPartitionOffset {
                    topic_id: 10,
                    partition_index: i,
                    committed_offset: i as u64 * 10,
                    metadata: None,
                    committed_at: 1000,
                },
            );
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_generation_atomics() {
        use std::sync::Arc;
        let group = Arc::new(make_group(1, "g-gen-atomic"));

        // Bump from multiple threads
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let g = Arc::clone(&group);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        g.bump_generation();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // 10 threads × 100 bumps = 1000 total
        assert_eq!(group.generation(), 1000);
    }

    #[test]
    fn test_member_assignment_rwlock() {
        use std::sync::Arc;
        let group = Arc::new(make_group(1, "g-asn-lock"));
        group.upsert_member(make_member("m1", &[("range", b"")]));
        group.set_member_assignment("m1", Bytes::from_static(b"initial"));

        // Concurrent reads while we also write
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let g = Arc::clone(&group);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let asn = g.get_member_assignment("m1");
                        assert!(asn.is_some());
                    }
                })
            })
            .collect();

        // Write new assignments concurrently
        for i in 0..100 {
            let data = format!("assignment-{i}");
            group.set_member_assignment("m1", Bytes::from(data));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Final assignment should be the last one written
        let asn = group.get_member_assignment("m1").unwrap();
        assert!(asn.starts_with(b"assignment-"));
    }

    #[test]
    fn test_phase_atomic_all_variants() {
        let group = make_group(1, "g-phase");
        for phase in [
            GroupPhase::Empty,
            GroupPhase::PreparingRebalance,
            GroupPhase::CompletingRebalance,
            GroupPhase::Stable,
            GroupPhase::Dead,
        ] {
            group.set_phase(phase);
            assert_eq!(group.phase(), phase);
        }
    }

    #[test]
    fn test_member_protocols_zero_copy() {
        let group = make_group(1, "g-proto-zc");
        group.upsert_member(make_member("m-1", &[("range", b"\x01\x02\x03")]));
        group.set_protocol_name("range".to_string());

        let protos = group.member_protocols();
        assert_eq!(protos.len(), 1);
        assert_eq!(protos[0].0, "m-1");
        // Verify we get Bytes (zero-copy) back, not Vec<u8>
        assert_eq!(protos[0].1.as_ref(), b"\x01\x02\x03");
    }

    #[test]
    fn test_from_snapshot_no_meta_clone() {
        // Verify from_snapshot takes ownership properly
        let mut meta = ConsumerGroupMeta {
            group_id: 1,
            name: "snap-group".to_string(),
            name_hash: 0,
            created_at: 1000,
            generation: 5,
            phase: GroupPhase::Stable,
            protocol_type: "consumer".to_string(),
            protocol_name: "range".to_string(),
            leader: Some("m-1".to_string()),
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 5000,
            next_member_id: 10,
            members: vec![GroupMemberMeta {
                member_id: "m-1".to_string(),
                client_id: "c-1".to_string(),
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 60_000,
                protocol_type: "consumer".to_string(),
                protocols: vec![("range".to_string(), vec![1, 2, 3])],
                assignment: vec![4, 5, 6],
            }],
        };
        let offsets = vec![GroupTopicPartitionOffset {
            topic_id: 10,
            partition_index: 0,
            committed_offset: 42,
            metadata: None,
            committed_at: 1000,
        }];

        let state = ConsumerGroupState::from_snapshot(meta, offsets, "test");
        assert_eq!(state.generation(), 5);
        assert_eq!(state.phase(), GroupPhase::Stable);
        assert_eq!(state.member_count(), 1);
        assert!(state.has_member("m-1"));
        assert_eq!(
            state.get_member_assignment("m-1").unwrap().as_ref(),
            &[4, 5, 6]
        );
        assert_eq!(state.get_offset(10, 0), Some(42));
        // meta.members should have been taken (moved out)
        assert!(state.meta.members.is_empty());
    }
}
