use std::collections::{HashSet, VecDeque};

/// Fair round-robin delivery scheduler.
///
/// Maintains a ring of ready subscription IDs. Each call to `next_ready()`
/// pops the front of the ring and pushes it to the back, ensuring every
/// ready subscription gets one turn before any gets a second.
pub struct DeliveryScheduler {
    ready_ring: VecDeque<u32>,
    in_ring: HashSet<u32>,
}

impl DeliveryScheduler {
    pub fn new() -> Self {
        Self {
            ready_ring: VecDeque::new(),
            in_ring: HashSet::new(),
        }
    }

    /// Mark a subscription as ready for delivery.
    /// No-op if already in the ring.
    pub fn mark_ready(&mut self, sub_id: u32) {
        if self.in_ring.insert(sub_id) {
            self.ready_ring.push_back(sub_id);
        }
    }

    /// Mark a subscription as not ready (remove from ring).
    pub fn mark_not_ready(&mut self, sub_id: u32) {
        if self.in_ring.remove(&sub_id) {
            self.ready_ring.retain(|&id| id != sub_id);
        }
    }

    /// Remove a subscription entirely (e.g. on unsubscribe).
    pub fn remove(&mut self, sub_id: u32) {
        self.mark_not_ready(sub_id);
    }

    /// Get the next ready subscription ID (round-robin).
    ///
    /// Pops from front, pushes to back. Returns None if no subs are ready.
    pub fn next_ready(&mut self) -> Option<u32> {
        if let Some(sub_id) = self.ready_ring.pop_front() {
            self.ready_ring.push_back(sub_id);
            Some(sub_id)
        } else {
            None
        }
    }

    /// Peek at the next ready subscription without rotating.
    pub fn peek_ready(&self) -> Option<u32> {
        self.ready_ring.front().copied()
    }

    /// Returns true if any subscriptions are ready.
    pub fn has_ready(&self) -> bool {
        !self.ready_ring.is_empty()
    }

    /// Number of ready subscriptions.
    pub fn ready_count(&self) -> usize {
        self.ready_ring.len()
    }
}

impl Default for DeliveryScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_scheduler() {
        let mut sched = DeliveryScheduler::new();
        assert!(!sched.has_ready());
        assert_eq!(sched.ready_count(), 0);
        assert_eq!(sched.next_ready(), None);
        assert_eq!(sched.peek_ready(), None);
    }

    #[test]
    fn test_single_subscription() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        assert!(sched.has_ready());
        assert_eq!(sched.ready_count(), 1);
        assert_eq!(sched.peek_ready(), Some(1));
        assert_eq!(sched.next_ready(), Some(1));
        // Should still be in ring (pushed to back)
        assert_eq!(sched.next_ready(), Some(1));
    }

    #[test]
    fn test_round_robin_fairness() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.mark_ready(3);
        assert_eq!(sched.ready_count(), 3);

        // First round
        assert_eq!(sched.next_ready(), Some(1));
        assert_eq!(sched.next_ready(), Some(2));
        assert_eq!(sched.next_ready(), Some(3));
        // Second round — same order
        assert_eq!(sched.next_ready(), Some(1));
        assert_eq!(sched.next_ready(), Some(2));
        assert_eq!(sched.next_ready(), Some(3));
    }

    #[test]
    fn test_mark_ready_idempotent() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(1);
        sched.mark_ready(1);
        assert_eq!(sched.ready_count(), 1);
    }

    #[test]
    fn test_mark_not_ready() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.mark_ready(3);

        sched.mark_not_ready(2);
        assert_eq!(sched.ready_count(), 2);
        assert_eq!(sched.next_ready(), Some(1));
        assert_eq!(sched.next_ready(), Some(3));
        assert_eq!(sched.next_ready(), Some(1));
    }

    #[test]
    fn test_mark_not_ready_unknown() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_not_ready(999); // no-op
        assert_eq!(sched.ready_count(), 0);
    }

    #[test]
    fn test_remove() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.remove(1);
        assert_eq!(sched.ready_count(), 1);
        assert_eq!(sched.next_ready(), Some(2));
    }

    #[test]
    fn test_remove_and_re_add() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.remove(1);
        sched.mark_ready(1); // re-add to back
        // Order: 2, 1
        assert_eq!(sched.next_ready(), Some(2));
        assert_eq!(sched.next_ready(), Some(1));
    }

    #[test]
    fn test_drain_all() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.remove(1);
        sched.remove(2);
        assert!(!sched.has_ready());
        assert_eq!(sched.next_ready(), None);
    }

    #[test]
    fn test_fairness_with_dynamic_adds() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        assert_eq!(sched.next_ready(), Some(1)); // 1 goes to back
        sched.mark_ready(3); // 3 added to back
        // Ring: [2, 1, 3]
        assert_eq!(sched.next_ready(), Some(2));
        assert_eq!(sched.next_ready(), Some(1));
        assert_eq!(sched.next_ready(), Some(3));
    }

    #[test]
    fn test_many_subscriptions() {
        let mut sched = DeliveryScheduler::new();
        for i in 0..100 {
            sched.mark_ready(i);
        }
        assert_eq!(sched.ready_count(), 100);
        // First 100 calls should return 0..99 in order
        for i in 0..100 {
            assert_eq!(sched.next_ready(), Some(i));
        }
        // Next call wraps around
        assert_eq!(sched.next_ready(), Some(0));
    }

    // -- Additional edge case tests --

    #[test]
    fn test_mark_not_ready_then_re_ready() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.mark_ready(3);
        // Remove middle, add back
        sched.mark_not_ready(2);
        sched.mark_ready(2);
        // Order: 1, 3, 2
        assert_eq!(sched.next_ready(), Some(1));
        assert_eq!(sched.next_ready(), Some(3));
        assert_eq!(sched.next_ready(), Some(2));
    }

    #[test]
    fn test_remove_only_entry() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.remove(1);
        assert!(!sched.has_ready());
        assert_eq!(sched.ready_count(), 0);
        assert_eq!(sched.next_ready(), None);
        assert_eq!(sched.peek_ready(), None);
    }

    #[test]
    fn test_mark_not_ready_preserves_others_order() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(10);
        sched.mark_ready(20);
        sched.mark_ready(30);
        sched.mark_ready(40);

        // Remove 20 and 30
        sched.mark_not_ready(20);
        sched.mark_not_ready(30);

        assert_eq!(sched.ready_count(), 2);
        assert_eq!(sched.next_ready(), Some(10));
        assert_eq!(sched.next_ready(), Some(40));
        assert_eq!(sched.next_ready(), Some(10));
    }

    #[test]
    fn test_default_impl() {
        let sched = DeliveryScheduler::default();
        assert!(!sched.has_ready());
        assert_eq!(sched.ready_count(), 0);
    }

    #[test]
    fn test_rapid_add_remove_cycle() {
        let mut sched = DeliveryScheduler::new();
        for i in 0..50 {
            sched.mark_ready(i);
        }
        for i in 0..50 {
            sched.remove(i);
        }
        assert_eq!(sched.ready_count(), 0);
        assert!(!sched.has_ready());

        // Re-add all
        for i in 0..50 {
            sched.mark_ready(i);
        }
        assert_eq!(sched.ready_count(), 50);
        assert_eq!(sched.next_ready(), Some(0));
    }

    #[test]
    fn test_next_ready_rotates_single() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(42);
        // Calling next_ready repeatedly on single entry should always return it
        for _ in 0..10 {
            assert_eq!(sched.next_ready(), Some(42));
        }
        assert_eq!(sched.ready_count(), 1);
    }

    #[test]
    fn test_peek_does_not_rotate() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        assert_eq!(sched.peek_ready(), Some(1));
        assert_eq!(sched.peek_ready(), Some(1));
        // next_ready rotates
        assert_eq!(sched.next_ready(), Some(1));
        assert_eq!(sched.peek_ready(), Some(2));
    }

    #[test]
    fn test_remove_front_of_ring() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.mark_ready(3);
        sched.remove(1); // remove front
        assert_eq!(sched.next_ready(), Some(2));
        assert_eq!(sched.next_ready(), Some(3));
    }

    #[test]
    fn test_remove_back_of_ring() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.mark_ready(3);
        sched.remove(3); // remove back
        assert_eq!(sched.next_ready(), Some(1));
        assert_eq!(sched.next_ready(), Some(2));
        assert_eq!(sched.next_ready(), Some(1));
    }

    #[test]
    fn test_interleaved_next_and_mark_not_ready() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.mark_ready(3);

        assert_eq!(sched.next_ready(), Some(1)); // ring: [2, 3, 1]
        sched.mark_not_ready(1); // ring: [2, 3]
        assert_eq!(sched.next_ready(), Some(2)); // ring: [3, 2]
        sched.mark_not_ready(3); // ring: [2]
        assert_eq!(sched.next_ready(), Some(2));
        sched.mark_not_ready(2);
        assert_eq!(sched.next_ready(), None);
    }
}
