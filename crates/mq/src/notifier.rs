use dashmap::DashMap;
use smallvec::SmallVec;
use tokio::sync::mpsc;

/// Push-based consumer group delivery notifier.
///
/// Protocol adapters register interest in specific consumer groups via `watch()`.
/// When the engine enqueues a message, it calls `notify()` to wake all
/// watchers immediately — eliminating polling interval.
///
/// This is a local (non-raft-replicated) mechanism; each node maintains
/// its own notifier for its local protocol adapter connections.
pub struct GroupNotifier {
    watchers: DashMap<u64, SmallVec<[mpsc::UnboundedSender<()>; 4]>>,
}

impl GroupNotifier {
    pub fn new() -> Self {
        Self {
            watchers: DashMap::new(),
        }
    }

    /// Register a watcher for the given group. Returns a receiver that
    /// will be signalled each time a message is available.
    pub fn watch(&self, group_id: u64) -> mpsc::UnboundedReceiver<()> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.watchers.entry(group_id).or_default().push(tx);
        rx
    }

    /// Notify all watchers for the given group that a message is available.
    /// Dead senders (whose receivers have been dropped) are cleaned up.
    pub fn notify(&self, group_id: u64) {
        if let Some(mut entry) = self.watchers.get_mut(&group_id) {
            entry.retain(|tx| tx.send(()).is_ok());
        }
    }

    /// Remove all watchers for the given group.
    pub fn unwatch(&self, group_id: u64) {
        self.watchers.remove(&group_id);
    }
}

impl Default for GroupNotifier {
    fn default() -> Self {
        Self::new()
    }
}
