use std::sync::Arc;

use smallvec::SmallVec;
use tokio::sync::Notify;

/// Push-based consumer group delivery notifier.
///
/// Protocol adapters create a single `Arc<Notify>` per connection and register
/// it for each consumer group they subscribe to via `watch()`. When the engine
/// enqueues a message, `notify()` wakes all connections watching that group.
///
/// Uses `tokio::sync::Notify` with coalescing `notify_waiters()` semantics,
/// matching the `HighWaterMark` pattern from `async_apply.rs`. Multiple
/// `notify()` calls before a connection wakes coalesce into a single wakeup.
///
/// This is a local (non-raft-replicated) mechanism; each node maintains
/// its own notifier for its local protocol adapter connections.
pub struct GroupNotifier {
    /// group_id → list of per-connection Notify handles.
    watchers: papaya::HashMap<u64, SmallVec<[Arc<Notify>; 4]>>,
}

impl GroupNotifier {
    pub fn new() -> Self {
        Self {
            watchers: papaya::HashMap::new(),
        }
    }

    /// Register a connection-level `Notify` for the given group.
    ///
    /// The caller owns the `Arc<Notify>` and awaits `.notified()` in its
    /// connection loop. When the engine calls `notify(group_id)`, all
    /// registered Notifys are woken.
    pub fn watch(&self, group_id: u64, notify: &Arc<Notify>) {
        let guard = self.watchers.pin();
        let mut list = guard.get(&group_id).cloned().unwrap_or_default();
        list.push(Arc::clone(notify));
        guard.insert(group_id, list);
    }

    /// Notify all watchers for the given group that a message is available.
    ///
    /// Iterates the registered Notify handles by reference (no clone).
    /// Uses `notify_waiters()` for coalescing semantics.
    #[inline]
    pub fn notify(&self, group_id: u64) {
        let guard = self.watchers.pin();
        if let Some(list) = guard.get(&group_id) {
            for n in list.iter() {
                n.notify_waiters();
            }
        }
    }

    /// Remove all watchers for the given group.
    pub fn unwatch(&self, group_id: u64) {
        self.watchers.pin().remove(&group_id);
    }
}

impl Default for GroupNotifier {
    fn default() -> Self {
        Self::new()
    }
}
