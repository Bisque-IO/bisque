use std::collections::{HashMap, VecDeque};

/// Fair round-robin delivery scheduler with O(1) mark_not_ready.
///
/// Uses generation counters for lazy removal: `mark_not_ready` bumps the
/// generation for a subscription (O(1)), and stale ring entries are silently
/// discarded when `next_ready` encounters them. This avoids the O(n)
/// `VecDeque::retain` scan that an eager-removal approach would require.
pub struct DeliveryScheduler {
    /// Ring of (sub_id, generation) pairs. May contain stale entries whose
    /// generation no longer matches `active_seq`.
    ring: VecDeque<(u32, u32)>,
    /// Current active generation per sub_id. `0` means the sub is inactive
    /// (was marked not-ready or removed). A non-zero value is the generation
    /// that was assigned when the sub was last marked ready.
    active_seq: HashMap<u32, u32>,
    /// Monotonically increasing generation counter.
    seq: u32,
    /// Number of currently active (ready) subscriptions.
    active_count: usize,
}

impl DeliveryScheduler {
    pub fn new() -> Self {
        Self {
            ring: VecDeque::new(),
            active_seq: HashMap::new(),
            seq: 0,
            active_count: 0,
        }
    }

    /// Mark a subscription as ready for delivery.
    /// No-op if already active in the ring.
    #[inline]
    pub fn mark_ready(&mut self, sub_id: u32) {
        let entry = self.active_seq.entry(sub_id).or_insert(0);
        if *entry == 0 {
            // Not currently active — assign a new generation and enqueue.
            self.seq += 1;
            *entry = self.seq;
            self.ring.push_back((sub_id, self.seq));
            self.active_count += 1;
        }
        // Already active: idempotent no-op.
    }

    /// Mark a subscription as not ready. O(1) — just invalidates the
    /// generation; stale ring entries are cleaned up lazily by `next_ready`.
    #[inline]
    pub fn mark_not_ready(&mut self, sub_id: u32) {
        if let Some(s) = self.active_seq.get_mut(&sub_id) {
            if *s != 0 {
                *s = 0; // invalidate — ring entry becomes stale
                self.active_count -= 1;
            }
        }
    }

    /// Remove a subscription entirely (e.g. on unsubscribe).
    #[inline]
    pub fn remove(&mut self, sub_id: u32) {
        if let Some(s) = self.active_seq.remove(&sub_id) {
            if s != 0 {
                self.active_count -= 1;
            }
        }
        // Stale ring entries cleaned up lazily by next_ready.
    }

    /// Get the next ready subscription ID (round-robin).
    ///
    /// Pops from front, skipping stale entries, pushes the live entry to back.
    /// Returns None if no subscriptions are ready.
    #[inline]
    pub fn next_ready(&mut self) -> Option<u32> {
        loop {
            let (sub_id, s) = self.ring.pop_front()?;
            let current = self.active_seq.get(&sub_id).copied().unwrap_or(0);
            if current == s {
                // Still active with the same generation.
                self.ring.push_back((sub_id, s));
                return Some(sub_id);
            }
            // Stale entry — discard silently.
        }
    }

    /// Peek at the next ready subscription without rotating.
    ///
    /// Scans past any stale entries at the front of the ring (removing them)
    /// to find the first live entry.
    pub fn peek_ready(&mut self) -> Option<u32> {
        // Drain stale entries from the front.
        while let Some(&(sub_id, s)) = self.ring.front() {
            let current = self.active_seq.get(&sub_id).copied().unwrap_or(0);
            if current == s {
                return Some(sub_id);
            }
            self.ring.pop_front(); // stale
        }
        None
    }

    /// Returns true if any subscriptions are ready.
    #[inline]
    pub fn has_ready(&self) -> bool {
        self.active_count > 0
    }

    /// Number of ready subscriptions.
    #[inline]
    pub fn ready_count(&self) -> usize {
        self.active_count
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
        sched.mark_not_ready(1); // ring: [2, 3] (1 stale, cleaned lazily)
        assert_eq!(sched.next_ready(), Some(2)); // ring: [3, 1_stale, 2]
        sched.mark_not_ready(3); // ring: [2] (3 stale)
        assert_eq!(sched.next_ready(), Some(2));
        sched.mark_not_ready(2);
        assert_eq!(sched.next_ready(), None);
    }

    #[test]
    fn test_mark_not_ready_then_ready_no_duplicate_turns() {
        let mut sched = DeliveryScheduler::new();
        sched.mark_ready(1);
        sched.mark_ready(2);
        sched.mark_ready(3);

        // Remove and re-add 2 — should get exactly one turn per cycle
        sched.mark_not_ready(2);
        sched.mark_ready(2);

        // Full cycle: each sub gets exactly one turn
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(sched.next_ready().unwrap());
        }
        seen.sort();
        assert_eq!(seen, vec![1, 2, 3]);

        // Second cycle: same subs, each once
        seen.clear();
        for _ in 0..3 {
            seen.push(sched.next_ready().unwrap());
        }
        seen.sort();
        assert_eq!(seen, vec![1, 2, 3]);
    }

    #[test]
    fn test_stale_entries_cleaned_by_next_ready() {
        let mut sched = DeliveryScheduler::new();
        // Add 5 subs, remove 3 of them
        for i in 0..5 {
            sched.mark_ready(i);
        }
        sched.mark_not_ready(1);
        sched.mark_not_ready(3);
        sched.mark_not_ready(4);

        // Only 0 and 2 should be returned
        assert_eq!(sched.ready_count(), 2);
        assert_eq!(sched.next_ready(), Some(0));
        assert_eq!(sched.next_ready(), Some(2));
        assert_eq!(sched.next_ready(), Some(0));
    }
}
