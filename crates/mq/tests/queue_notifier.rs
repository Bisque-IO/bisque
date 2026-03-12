//! Tests for the QueueNotifier (Optimization 1: Push-Based Queue Delivery).
//!
//! The QueueNotifier is a local (non-raft-replicated) mechanism that notifies
//! protocol adapters when new messages are enqueued to a queue, eliminating
//! the need for polling-based delivery.

// These tests are for when the QueueNotifier module is implemented.
// They use tokio::test since the notifier uses tokio channels.

#[cfg(test)]
mod tests {
    // Note: Import will be: use bisque_mq::notifier::QueueNotifier;
    // For now, these tests document the expected behavior.

    use tokio::sync::mpsc;

    // Placeholder for QueueNotifier — replace with actual import when implemented
    struct QueueNotifier {
        watchers: dashmap::DashMap<u64, Vec<mpsc::UnboundedSender<()>>>,
    }

    impl QueueNotifier {
        fn new() -> Self {
            Self {
                watchers: dashmap::DashMap::new(),
            }
        }

        fn watch(&self, queue_id: u64) -> mpsc::UnboundedReceiver<()> {
            let (tx, rx) = mpsc::unbounded_channel();
            self.watchers.entry(queue_id).or_default().push(tx);
            rx
        }

        fn notify(&self, queue_id: u64) {
            if let Some(mut entry) = self.watchers.get_mut(&queue_id) {
                entry.retain(|tx| tx.send(()).is_ok());
            }
        }

        fn unwatch(&self, queue_id: u64) {
            self.watchers.remove(&queue_id);
        }
    }

    #[tokio::test]
    async fn test_watch_notify_basic() {
        let notifier = QueueNotifier::new();
        let mut rx = notifier.watch(1);

        notifier.notify(1);

        // Should receive notification
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;
        assert!(result.is_ok(), "should receive notification");
        assert!(result.unwrap().is_some(), "channel should not be closed");
    }

    #[tokio::test]
    async fn test_multiple_watchers() {
        let notifier = QueueNotifier::new();
        let mut rx1 = notifier.watch(1);
        let mut rx2 = notifier.watch(1);
        let mut rx3 = notifier.watch(1);

        notifier.notify(1);

        // All 3 should receive
        let timeout = std::time::Duration::from_millis(100);
        assert!(tokio::time::timeout(timeout, rx1.recv()).await.is_ok());
        assert!(tokio::time::timeout(timeout, rx2.recv()).await.is_ok());
        assert!(tokio::time::timeout(timeout, rx3.recv()).await.is_ok());
    }

    #[tokio::test]
    async fn test_unwatch() {
        let notifier = QueueNotifier::new();
        let mut rx = notifier.watch(1);

        notifier.unwatch(1);
        notifier.notify(1);

        // Should NOT receive notification (channel sender dropped by unwatch)
        let result = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
        // Either timeout or channel closed
        match result {
            Err(_) => {}   // timeout — expected if sender was dropped
            Ok(None) => {} // channel closed — also expected
            Ok(Some(())) => panic!("should not receive notification after unwatch"),
        }
    }

    #[tokio::test]
    async fn test_dead_sender_cleanup() {
        let notifier = QueueNotifier::new();
        let rx1 = notifier.watch(1);
        let mut rx2 = notifier.watch(1);

        // Drop rx1 — its sender should be cleaned up on next notify
        drop(rx1);

        notifier.notify(1);

        // rx2 should still receive
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), rx2.recv()).await;
        assert!(result.is_ok());

        // After cleanup, only 1 sender should remain
        assert_eq!(
            notifier.watchers.get(&1).map(|e| e.len()).unwrap_or(0),
            1,
            "dead sender should be cleaned up"
        );
    }

    #[tokio::test]
    async fn test_notify_no_watchers() {
        let notifier = QueueNotifier::new();
        // Should not panic
        notifier.notify(999);
    }

    #[tokio::test]
    async fn test_cross_queue_isolation() {
        let notifier = QueueNotifier::new();
        let mut rx1 = notifier.watch(1);

        // Notify queue 2 — should NOT wake queue 1 watcher
        notifier.notify(2);

        let result = tokio::time::timeout(std::time::Duration::from_millis(50), rx1.recv()).await;
        assert!(
            result.is_err(),
            "queue 1 watcher should NOT be notified by queue 2 event"
        );
    }

    #[tokio::test]
    async fn test_multiple_notify_not_coalesced() {
        let notifier = QueueNotifier::new();
        let mut rx = notifier.watch(1);

        // Send 5 notifications
        for _ in 0..5 {
            notifier.notify(1);
        }

        // All 5 should be receivable (unbounded channel, not coalesced)
        let mut count = 0;
        let timeout = std::time::Duration::from_millis(100);
        while tokio::time::timeout(timeout, rx.recv()).await.is_ok() {
            count += 1;
            if count >= 5 {
                break;
            }
        }
        assert_eq!(count, 5, "all 5 notifications should be received");
    }

    #[tokio::test]
    async fn test_watch_after_notify() {
        let notifier = QueueNotifier::new();

        // Notify first
        notifier.notify(1);

        // Then watch — should NOT receive retroactive notification
        let mut rx = notifier.watch(1);

        let result = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
        assert!(
            result.is_err(),
            "should not receive retroactive notification"
        );
    }

    #[tokio::test]
    async fn test_watch_multiple_queues() {
        let notifier = QueueNotifier::new();
        let mut rx1 = notifier.watch(1);
        let mut rx2 = notifier.watch(2);
        let mut rx3 = notifier.watch(3);

        // Notify only queue 2
        notifier.notify(2);

        let timeout = std::time::Duration::from_millis(50);

        // Only rx2 should fire
        assert!(
            tokio::time::timeout(timeout, rx2.recv()).await.is_ok(),
            "queue 2 watcher should fire"
        );
        assert!(
            tokio::time::timeout(timeout, rx1.recv()).await.is_err(),
            "queue 1 watcher should NOT fire"
        );
        assert!(
            tokio::time::timeout(timeout, rx3.recv()).await.is_err(),
            "queue 3 watcher should NOT fire"
        );
    }

    #[tokio::test]
    async fn test_unwatch_one_queue_doesnt_affect_others() {
        let notifier = QueueNotifier::new();
        let _rx1 = notifier.watch(1);
        let mut rx2 = notifier.watch(2);

        notifier.unwatch(1);
        notifier.notify(2);

        let timeout = std::time::Duration::from_millis(100);
        assert!(
            tokio::time::timeout(timeout, rx2.recv()).await.is_ok(),
            "queue 2 watcher should still work after unwatch(1)"
        );
    }
}
