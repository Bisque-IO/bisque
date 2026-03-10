use std::sync::atomic::Ordering;

use crate::engine::MqEngine;

impl MqEngine {
    /// Full recompute of the minimum raft log index that must be retained.
    ///
    /// This scans all entities — O(n). Use `compute_purge_floor()` for the cached O(1) path.
    fn recompute_purge_floor(&self) -> u64 {
        let mut min_index = u64::MAX;

        // Topics use dense per-topic offsets and direct segment addressing
        // (MessageLocation), so their tail_index / consumer offsets are NOT
        // raft log indexes and must not participate in the raft purge floor.
        // Segment retention for topic data is handled separately via segment
        // reference tracking.

        for entry in self.meta.queues.iter() {
            if let Some(q_min) = entry.value().min_required_index() {
                min_index = min_index.min(q_min);
            }
        }

        for entry in self.meta.actor_namespaces.iter() {
            if let Some(a_min) = entry.value().min_required_index() {
                min_index = min_index.min(a_min);
            }
        }

        if min_index == u64::MAX { 0 } else { min_index }
    }

    /// Return the cached purge floor. Recomputes only if dirty.
    pub fn compute_purge_floor(&mut self) -> u64 {
        if self.meta.purge_floor_dirty.load(Ordering::Relaxed) {
            let floor = self.recompute_purge_floor();
            self.meta.cached_purge_floor.store(floor, Ordering::Relaxed);
            self.meta.purge_floor_dirty.store(false, Ordering::Relaxed);
            floor
        } else {
            self.meta.cached_purge_floor.load(Ordering::Relaxed)
        }
    }

    /// O(1) — a message was added at `log_index`. The floor can only decrease.
    #[inline]
    pub(crate) fn on_message_added(&self, log_index: u64) {
        if self.meta.purge_floor_dirty.load(Ordering::Relaxed) {
            return; // will recompute anyway
        }
        let cached = self.meta.cached_purge_floor.load(Ordering::Relaxed);
        if cached == 0 {
            self.meta
                .cached_purge_floor
                .store(log_index, Ordering::Relaxed);
        } else {
            self.meta
                .cached_purge_floor
                .store(cached.min(log_index), Ordering::Relaxed);
        }
    }

    /// A message at `removed_index` was removed. Recompute only if it could
    /// have been the global minimum.
    #[inline]
    pub(crate) fn on_message_removed(&self, removed_index: u64) {
        if self.meta.purge_floor_dirty.load(Ordering::Relaxed) {
            return;
        }
        let cached = self.meta.cached_purge_floor.load(Ordering::Relaxed);
        if removed_index <= cached {
            self.meta.purge_floor_dirty.store(true, Ordering::Relaxed);
        }
        // If removed_index > cached, the floor hasn't changed.
    }

    /// An entity with messages was deleted — must recompute.
    #[inline]
    pub(crate) fn mark_purge_floor_dirty(&self) {
        self.meta.purge_floor_dirty.store(true, Ordering::Relaxed);
    }
}
