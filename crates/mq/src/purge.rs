use crate::engine::MqEngine;

impl MqEngine {
    /// Full recompute of the minimum raft log index that must be retained.
    ///
    /// This scans all entities — O(n). Use `purge_floor()` for the cached O(1) path.
    fn recompute_purge_floor(&mut self) -> u64 {
        let mut min_index = u64::MAX;

        // Topics use dense per-topic offsets and direct segment addressing
        // (MessageLocation), so their tail_index / consumer offsets are NOT
        // raft log indexes and must not participate in the raft purge floor.
        // Segment retention for topic data is handled separately via segment
        // reference tracking.

        for queue in self.queues.values_mut() {
            if let Some(q_min) = queue.min_required_index() {
                min_index = min_index.min(q_min);
            }
        }

        for ns in self.actor_namespaces.values_mut() {
            if let Some(a_min) = ns.min_required_index() {
                min_index = min_index.min(a_min);
            }
        }

        if min_index == u64::MAX { 0 } else { min_index }
    }

    /// Return the cached purge floor. Recomputes only if dirty.
    pub fn compute_purge_floor(&mut self) -> u64 {
        if self.purge_floor_dirty {
            self.cached_purge_floor = self.recompute_purge_floor();
            self.purge_floor_dirty = false;
        }
        self.cached_purge_floor
    }

    /// O(1) — a message was added at `log_index`. The floor can only decrease.
    #[inline]
    pub(crate) fn on_message_added(&mut self, log_index: u64) {
        if self.purge_floor_dirty {
            return; // will recompute anyway
        }
        if self.cached_purge_floor == 0 {
            self.cached_purge_floor = log_index;
        } else {
            self.cached_purge_floor = self.cached_purge_floor.min(log_index);
        }
    }

    /// A message at `removed_index` was removed. Recompute only if it could
    /// have been the global minimum.
    #[inline]
    pub(crate) fn on_message_removed(&mut self, removed_index: u64) {
        if self.purge_floor_dirty {
            return;
        }
        if removed_index <= self.cached_purge_floor {
            self.purge_floor_dirty = true;
        }
        // If removed_index > cached, the floor hasn't changed.
    }

    /// An entity with messages was deleted — must recompute.
    #[inline]
    pub(crate) fn mark_purge_floor_dirty(&mut self) {
        self.purge_floor_dirty = true;
    }
}
