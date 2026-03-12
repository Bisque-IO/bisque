//! Unified session management — replaces ConsumerState + ProducerMeta.
//!
//! A session represents a connected client with an optional will message,
//! keep-alive tracking, and subscription state. Sessions support persistent
//! semantics (MQTT 5.0 clean start = false), client takeover on duplicate
//! client_id, and protocol-agnostic will lifecycle.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::types::{SessionSubscription, WillConfig};

// =============================================================================
// Session Metadata (persisted to MDBX / snapshot)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMeta {
    pub session_id: u64,
    pub client_id: String,
    /// CRC64-NVME hash of client_id for O(1) lookup.
    #[serde(default)]
    pub client_id_hash: u64,
    pub created_at: u64,
    pub connected_at: u64,
    pub last_activity_at: u64,
    /// Keep-alive interval in milliseconds. 0 = disabled.
    #[serde(default)]
    pub keep_alive_ms: u64,
    /// Session expiry interval in milliseconds. 0 = session expires on disconnect.
    #[serde(default)]
    pub session_expiry_ms: u64,
    /// Will message to publish on unexpected disconnect.
    #[serde(default)]
    pub will: Option<WillConfig>,
    /// Subscriptions held by this session.
    #[serde(default)]
    pub subscriptions: HashSet<SessionSubscription>,
    /// Optional producer name (for dedup / named producers).
    #[serde(default)]
    pub producer_name: Option<String>,
}

impl SessionMeta {
    pub fn new(
        session_id: u64,
        client_id: String,
        connected_at: u64,
        keep_alive_ms: u64,
        session_expiry_ms: u64,
    ) -> Self {
        let client_id_hash = crate::types::name_hash(&client_id);
        Self {
            session_id,
            client_id,
            client_id_hash,
            created_at: connected_at,
            connected_at,
            last_activity_at: connected_at,
            keep_alive_ms,
            session_expiry_ms,
            will: None,
            subscriptions: HashSet::new(),
            producer_name: None,
        }
    }

    /// Ensure client_id_hash is populated (for deserialized data).
    pub fn ensure_hash(&mut self) {
        if self.client_id_hash == 0 {
            self.client_id_hash = crate::types::name_hash(&self.client_id);
        }
    }
}

// =============================================================================
// In-memory Session State
// =============================================================================

/// Lock-free in-memory session state.
///
/// Immutable identity fields are stored directly in `meta`. Mutable state uses
/// atomics and `RwLock` for concurrent readers with single-writer (Raft apply).
pub struct SessionState {
    pub meta: SessionMeta,

    // -- Mutable state --
    last_activity_at: AtomicU64,
    will: RwLock<Option<WillConfig>>,
    subscriptions: RwLock<HashSet<SessionSubscription>>,
}

impl SessionState {
    pub fn new(meta: SessionMeta) -> Self {
        let last_activity_at = AtomicU64::new(meta.last_activity_at);
        let will = RwLock::new(meta.will.clone());
        let subscriptions = RwLock::new(meta.subscriptions.clone());
        Self {
            meta,
            last_activity_at,
            will,
            subscriptions,
        }
    }

    #[inline]
    pub fn last_activity_at(&self) -> u64 {
        self.last_activity_at.load(Ordering::Relaxed)
    }

    pub fn heartbeat(&self, at: u64) {
        self.last_activity_at.store(at, Ordering::Relaxed);
    }

    /// Check if the session's keep-alive has expired.
    /// Uses 1.5× multiplier per MQTT 5.0 spec §3.1.2.10.
    pub fn is_expired(&self, current_time: u64) -> bool {
        let keep_alive = self.meta.keep_alive_ms;
        if keep_alive == 0 {
            return false; // keep-alive disabled
        }
        let deadline = self.last_activity_at() + (keep_alive * 3 / 2);
        current_time > deadline
    }

    pub fn set_will(&self, will: WillConfig) {
        *self.will.write() = Some(will);
    }

    pub fn clear_will(&self) {
        *self.will.write() = None;
    }

    pub fn take_will(&self) -> Option<WillConfig> {
        self.will.write().take()
    }

    pub fn has_will(&self) -> bool {
        self.will.read().is_some()
    }

    pub fn subscriptions(&self) -> parking_lot::RwLockReadGuard<'_, HashSet<SessionSubscription>> {
        self.subscriptions.read()
    }

    pub fn add_subscription(&self, sub: SessionSubscription) {
        self.subscriptions.write().insert(sub);
    }

    pub fn remove_subscription(&self, sub: &SessionSubscription) {
        self.subscriptions.write().remove(sub);
    }

    /// Flush mutable state back to a cloned `SessionMeta` for snapshot/persistence.
    pub fn snapshot_meta(&self) -> SessionMeta {
        let mut m = self.meta.clone();
        m.last_activity_at = self.last_activity_at();
        m.will = self.will.read().clone();
        m.subscriptions = self.subscriptions.read().clone();
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_session(id: u64, connected_at: u64) -> SessionState {
        let meta = SessionMeta::new(id, format!("client-{id}"), connected_at, 30_000, 0);
        SessionState::new(meta)
    }

    #[test]
    fn test_session_meta_defaults() {
        let meta = SessionMeta::new(42, "test-client".to_string(), 1000, 30_000, 0);
        assert_eq!(meta.session_id, 42);
        assert_eq!(meta.client_id, "test-client");
        assert_eq!(meta.connected_at, 1000);
        assert_eq!(meta.last_activity_at, 1000);
        assert_eq!(meta.keep_alive_ms, 30_000);
        assert!(meta.will.is_none());
        assert!(meta.subscriptions.is_empty());
    }

    #[test]
    fn test_heartbeat() {
        let session = make_session(1, 1000);
        assert_eq!(session.last_activity_at(), 1000);
        session.heartbeat(5000);
        assert_eq!(session.last_activity_at(), 5000);
    }

    #[test]
    fn test_is_expired() {
        let session = make_session(1, 1000);
        // keep_alive = 30_000, deadline = 1000 + 45_000 = 46_000 (1.5x)
        assert!(!session.is_expired(45_000));
        assert!(!session.is_expired(46_000));
        assert!(session.is_expired(46_001));
    }

    #[test]
    fn test_is_expired_disabled() {
        let meta = SessionMeta::new(1, "client".to_string(), 1000, 0, 0);
        let session = SessionState::new(meta);
        assert!(!session.is_expired(999_999_999)); // never expires
    }

    #[test]
    fn test_will_lifecycle() {
        let session = make_session(1, 1000);
        assert!(!session.has_will());

        let will = WillConfig {
            topic_id: 42,
            payload: Bytes::from_static(b"goodbye"),
            delay_ms: 0,
            retained: false,
        };
        session.set_will(will);
        assert!(session.has_will());

        let taken = session.take_will();
        assert!(taken.is_some());
        assert!(!session.has_will());
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let session = make_session(1, 1000);
        session.heartbeat(5000);
        session.set_will(WillConfig {
            topic_id: 10,
            payload: Bytes::from_static(b"will"),
            delay_ms: 5000,
            retained: true,
        });

        let snap = session.snapshot_meta();
        assert_eq!(snap.last_activity_at, 5000);
        assert!(snap.will.is_some());
        assert_eq!(snap.will.as_ref().unwrap().topic_id, 10);
    }
}
