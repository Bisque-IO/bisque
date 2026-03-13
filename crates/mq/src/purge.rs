use std::sync::atomic::Ordering;

use crate::consumer_group::VariantState;
use crate::engine::MqEngine;

impl MqEngine {
    /// Full recompute of the minimum raft log index that must be retained.
    ///
    /// This scans all entities — O(n). Use `compute_purge_floor()` for the cached O(1) path.
    fn recompute_purge_floor(&self) -> u64 {
        let mut min_index = u64::MAX;
        let meta = self.metadata();

        let topics_guard = meta.topics.pin();
        for (_, topic) in topics_guard.iter() {
            let t_min = topic.min_required_index();
            if t_min > 0 {
                min_index = min_index.min(t_min);
            }
        }

        let cg_guard = meta.consumer_groups.pin();
        for (_, cg) in cg_guard.iter() {
            let cg = cg;
            let cg_min = match &cg.variant_state {
                VariantState::Offset => None,
                VariantState::Ack(s) => s.min_required_index(),
                VariantState::Actor(s) => s.min_required_index(),
            };
            if let Some(m) = cg_min {
                min_index = min_index.min(m);
            }
        }

        if min_index == u64::MAX { 0 } else { min_index }
    }

    /// Return the cached purge floor. Recomputes only if dirty.
    pub fn compute_purge_floor(&self) -> u64 {
        let meta = self.metadata();
        if meta.purge_floor_dirty.load(Ordering::Relaxed) {
            let floor = self.recompute_purge_floor();
            meta.cached_purge_floor.store(floor, Ordering::Relaxed);
            meta.purge_floor_dirty.store(false, Ordering::Relaxed);
            floor
        } else {
            meta.cached_purge_floor.load(Ordering::Relaxed)
        }
    }

    /// O(1) — a message was added at `log_index`. The floor can only decrease.
    #[inline]
    pub(crate) fn on_message_added(&self, log_index: u64) {
        let meta = self.metadata();
        if meta.purge_floor_dirty.load(Ordering::Relaxed) {
            return; // will recompute anyway
        }
        let cached = meta.cached_purge_floor.load(Ordering::Relaxed);
        if cached == 0 {
            meta.cached_purge_floor.store(log_index, Ordering::Relaxed);
        } else {
            meta.cached_purge_floor
                .store(cached.min(log_index), Ordering::Relaxed);
        }
    }

    /// A message at `removed_index` was removed. Recompute only if it could
    /// have been the global minimum.
    #[inline]
    pub(crate) fn on_message_removed(&self, removed_index: u64) {
        let meta = self.metadata();
        if meta.purge_floor_dirty.load(Ordering::Relaxed) {
            return;
        }
        let cached = meta.cached_purge_floor.load(Ordering::Relaxed);
        if removed_index <= cached {
            meta.purge_floor_dirty.store(true, Ordering::Relaxed);
        }
        // If removed_index > cached, the floor hasn't changed.
    }

    /// An entity with messages was deleted — must recompute.
    #[inline]
    pub(crate) fn mark_purge_floor_dirty(&self) {
        self.metadata()
            .purge_floor_dirty
            .store(true, Ordering::Relaxed);
    }
}
