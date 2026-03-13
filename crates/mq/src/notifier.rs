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
    watchers: papaya::HashMap<u64, SmallVec<[mpsc::UnboundedSender<()>; 4]>>,
}

impl GroupNotifier {
    pub fn new() -> Self {
        Self {
            watchers: papaya::HashMap::new(),
        }
    }

    /// Register a watcher for the given group. Returns a receiver that
    /// will be signalled each time a message is available.
    pub fn watch(&self, group_id: u64) -> mpsc::UnboundedReceiver<()> {
        let (tx, rx) = mpsc::unbounded_channel();
        let guard = self.watchers.pin();
        let mut senders = guard.get(&group_id).cloned().unwrap_or_default();
        senders.push(tx);
        guard.insert(group_id, senders);
        rx
    }

    /// Notify all watchers for the given group that a message is available.
    /// Dead senders (whose receivers have been dropped) are cleaned up.
    pub fn notify(&self, group_id: u64) {
        let guard = self.watchers.pin();
        if let Some(senders) = guard.get(&group_id).cloned() {
            let mut senders = senders;
            senders.retain(|tx| tx.send(()).is_ok());
            guard.insert(group_id, senders);
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
