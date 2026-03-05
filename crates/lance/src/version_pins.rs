//! Server-side version pin tracker for compaction safety.
//!
//! Remote `BisqueClient` sessions pin the Lance dataset versions they are actively
//! reading. The server must not delete data files for any pinned version during
//! compaction cleanup. Each WebSocket session has a lease that expires if heartbeats
//! stop, preventing zombie pins from permanently blocking cleanup.
//!
//! # Protocol
//!
//! Clients send JSON messages over the WebSocket:
//! - `{"type":"pin","table":"events","tier":"active","version":42}` — pin a version
//! - `{"type":"unpin","table":"events","tier":"active","version":42}` — release a pin
//! - `{"type":"heartbeat"}` — renew the session lease
//!
//! The server reaps expired sessions (no heartbeat within `lease_timeout`).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::{debug, info};

/// Identifies a pinned dataset version.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PinKey {
    pub table: String,
    pub tier: PinTier,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum PinTier {
    Active,
    Sealed,
}

impl PinTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            PinTier::Active => "active",
            PinTier::Sealed => "sealed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "active" => Some(PinTier::Active),
            "sealed" => Some(PinTier::Sealed),
            _ => None,
        }
    }
}

/// Unique session identifier for a WebSocket connection.
pub type SessionId = u64;

/// Per-session state.
struct SessionState {
    pins: HashSet<PinKey>,
    last_heartbeat: Instant,
}

/// Tracks version pins across all WebSocket sessions.
///
/// Thread-safe via interior `Mutex`. Operations are fast (no I/O).
pub struct VersionPinTracker {
    sessions: Mutex<HashMap<SessionId, SessionState>>,
    /// Aggregated pin counts: (table, tier, version) → count.
    pin_counts: Mutex<HashMap<PinKey, usize>>,
    next_session_id: AtomicU64,
    lease_timeout: Duration,
}

impl VersionPinTracker {
    /// Create a new tracker with the given lease timeout.
    ///
    /// Sessions that don't heartbeat within this duration are reaped.
    pub fn new(lease_timeout: Duration) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            pin_counts: Mutex::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            lease_timeout,
        }
    }

    /// Register a new session and return its ID.
    pub fn create_session(&self) -> SessionId {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let state = SessionState {
            pins: HashSet::new(),
            last_heartbeat: Instant::now(),
        };
        self.sessions.lock().insert(id, state);
        debug!(session_id = id, "Version pin session created");
        id
    }

    /// Remove a session and release all its pins.
    pub fn remove_session(&self, session_id: SessionId) {
        let pins = {
            let mut sessions = self.sessions.lock();
            match sessions.remove(&session_id) {
                Some(state) => state.pins,
                None => return,
            }
        };

        if !pins.is_empty() {
            let mut counts = self.pin_counts.lock();
            for pin in &pins {
                if let Some(count) = counts.get_mut(pin) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        counts.remove(pin);
                    }
                }
            }
            debug!(
                session_id,
                pins_released = pins.len(),
                "Session removed, pins released"
            );
        }
    }

    /// Renew a session's lease.
    pub fn heartbeat(&self, session_id: SessionId) {
        if let Some(state) = self.sessions.lock().get_mut(&session_id) {
            state.last_heartbeat = Instant::now();
        }
    }

    /// Pin a version for a session.
    pub fn pin(&self, session_id: SessionId, key: PinKey) {
        let inserted = {
            let mut sessions = self.sessions.lock();
            match sessions.get_mut(&session_id) {
                Some(state) => {
                    state.last_heartbeat = Instant::now();
                    state.pins.insert(key.clone())
                }
                None => return,
            }
        };

        if inserted {
            let mut counts = self.pin_counts.lock();
            *counts.entry(key).or_insert(0) += 1;
        }
    }

    /// Unpin a version for a session.
    pub fn unpin(&self, session_id: SessionId, key: PinKey) {
        let removed = {
            let mut sessions = self.sessions.lock();
            match sessions.get_mut(&session_id) {
                Some(state) => state.pins.remove(&key),
                None => return,
            }
        };

        if removed {
            let mut counts = self.pin_counts.lock();
            if let Some(count) = counts.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&key);
                }
            }
        }
    }

    /// Get the minimum pinned version for a table+tier, or `None` if nothing is pinned.
    ///
    /// Compaction cleanup must not delete versions >= this value.
    pub fn min_pinned_version(&self, table: &str, tier: PinTier) -> Option<u64> {
        let counts = self.pin_counts.lock();
        counts
            .keys()
            .filter(|k| k.table == table && k.tier == tier)
            .map(|k| k.version)
            .min()
    }

    /// Reap expired sessions. Returns the number of sessions reaped.
    ///
    /// Should be called periodically (e.g. every 10s) from a background task.
    pub fn reap_expired(&self) -> usize {
        let now = Instant::now();
        let expired: Vec<SessionId> = {
            let sessions = self.sessions.lock();
            sessions
                .iter()
                .filter(|(_, state)| now.duration_since(state.last_heartbeat) > self.lease_timeout)
                .map(|(&id, _)| id)
                .collect()
        };

        let count = expired.len();
        for id in expired {
            info!(session_id = id, "Reaping expired version pin session");
            self.remove_session(id);
        }
        count
    }

    /// Get the number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Get the total number of active pins.
    pub fn pin_count(&self) -> usize {
        self.pin_counts.lock().values().sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pin_unpin() {
        let tracker = VersionPinTracker::new(Duration::from_secs(30));
        let s1 = tracker.create_session();
        let s2 = tracker.create_session();

        let key = PinKey {
            table: "events".into(),
            tier: PinTier::Active,
            version: 5,
        };

        tracker.pin(s1, key.clone());
        tracker.pin(s2, key.clone());
        assert_eq!(
            tracker.min_pinned_version("events", PinTier::Active),
            Some(5)
        );

        tracker.unpin(s1, key.clone());
        assert_eq!(
            tracker.min_pinned_version("events", PinTier::Active),
            Some(5)
        );

        tracker.unpin(s2, key.clone());
        assert_eq!(tracker.min_pinned_version("events", PinTier::Active), None);
    }

    #[test]
    fn test_min_pinned_version() {
        let tracker = VersionPinTracker::new(Duration::from_secs(30));
        let s1 = tracker.create_session();
        let s2 = tracker.create_session();

        tracker.pin(
            s1,
            PinKey {
                table: "events".into(),
                tier: PinTier::Active,
                version: 10,
            },
        );
        tracker.pin(
            s2,
            PinKey {
                table: "events".into(),
                tier: PinTier::Active,
                version: 5,
            },
        );

        assert_eq!(
            tracker.min_pinned_version("events", PinTier::Active),
            Some(5)
        );
        assert_eq!(tracker.min_pinned_version("events", PinTier::Sealed), None);
        assert_eq!(tracker.min_pinned_version("other", PinTier::Active), None);
    }

    #[test]
    fn test_session_removal_releases_pins() {
        let tracker = VersionPinTracker::new(Duration::from_secs(30));
        let s1 = tracker.create_session();

        tracker.pin(
            s1,
            PinKey {
                table: "t".into(),
                tier: PinTier::Active,
                version: 1,
            },
        );
        tracker.pin(
            s1,
            PinKey {
                table: "t".into(),
                tier: PinTier::Sealed,
                version: 2,
            },
        );

        assert_eq!(tracker.pin_count(), 2);
        tracker.remove_session(s1);
        assert_eq!(tracker.pin_count(), 0);
        assert_eq!(tracker.session_count(), 0);
    }

    #[test]
    fn test_reap_expired() {
        let tracker = VersionPinTracker::new(Duration::from_millis(10));
        let s1 = tracker.create_session();
        tracker.pin(
            s1,
            PinKey {
                table: "t".into(),
                tier: PinTier::Active,
                version: 1,
            },
        );

        std::thread::sleep(Duration::from_millis(20));
        let reaped = tracker.reap_expired();
        assert_eq!(reaped, 1);
        assert_eq!(tracker.session_count(), 0);
        assert_eq!(tracker.pin_count(), 0);
    }

    #[test]
    fn test_heartbeat_prevents_reap() {
        let tracker = VersionPinTracker::new(Duration::from_millis(50));
        let s1 = tracker.create_session();
        tracker.pin(
            s1,
            PinKey {
                table: "t".into(),
                tier: PinTier::Active,
                version: 1,
            },
        );

        std::thread::sleep(Duration::from_millis(30));
        tracker.heartbeat(s1);
        std::thread::sleep(Duration::from_millis(30));

        let reaped = tracker.reap_expired();
        assert_eq!(reaped, 0);
        assert_eq!(tracker.session_count(), 1);
    }
}
