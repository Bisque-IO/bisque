use dashmap::DashMap;
use smallvec::SmallVec;
use tokio::sync::mpsc;

/// Push-based queue delivery notifier.
///
/// Protocol adapters register interest in specific queues via `watch()`.
/// When the engine enqueues a message, it calls `notify()` to wake all
/// watchers immediately — eliminating the 50ms polling interval.
///
/// This is a local (non-raft-replicated) mechanism; each node maintains
/// its own notifier for its local protocol adapter connections.
pub struct QueueNotifier {
    watchers: DashMap<u64, SmallVec<[mpsc::UnboundedSender<()>; 4]>>,
}

impl QueueNotifier {
    pub fn new() -> Self {
        Self {
            watchers: DashMap::new(),
        }
    }

    /// Register a watcher for the given queue. Returns a receiver that
    /// will be signalled each time a message is enqueued.
    pub fn watch(&self, queue_id: u64) -> mpsc::UnboundedReceiver<()> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.watchers.entry(queue_id).or_default().push(tx);
        rx
    }

    /// Notify all watchers for the given queue that a message is available.
    /// Dead senders (whose receivers have been dropped) are cleaned up.
    pub fn notify(&self, queue_id: u64) {
        if let Some(mut entry) = self.watchers.get_mut(&queue_id) {
            entry.retain(|tx| tx.send(()).is_ok());
        }
    }

    /// Remove all watchers for the given queue.
    pub fn unwatch(&self, queue_id: u64) {
        self.watchers.remove(&queue_id);
    }
}

impl Default for QueueNotifier {
    fn default() -> Self {
        Self::new()
    }
}
