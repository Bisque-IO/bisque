# Heap Epoch Design Notes

## Current Architecture
- `Collector`: usize index into global `COLLECTORS` array, owns reclaim flag + watermark cache + skip counter
- `Epoch`: per-tree epoch counter + collector_id + heap + dealloc fn
- `EpochGuard`: pins thread slot, restores on drop, triggers try_reclaim on outermost unpin
- `CollectorPin`: per-thread per-collector state with epoch AtomicU64 + garbage VecDeque
- Thread slots: global `THREADS` LazyLock with `UnsafeCell`-based thread-local index caching

## Key Optimizations Applied
1. UnsafeCell thread-local (replaced RefCell) — 16-59% improvement at high thread counts
2. Watermark caching — skip drain pass if watermark unchanged
3. Back-off skip counter — writer advance skips reclaim N times after no-progress, reader unpin resets
4. Lazy epoch advance — epoch bumps on first retire after reclaim clears needs_reclaim, not per publish
5. RetiredBag stores no Heap/dealloc — passed through reclaim path from Epoch
6. VecDeque replaced BinaryHeap — O(1) push/pop, bags arrive in epoch order
7. Removed pending buffer / defer / flush_pending — dead code after WriteGuard-only writes
8. Publish is one atomic store (published_root) — no seq.fetch_add, no epoch.advance

## Next Steps: SPSC Integration
- Replace `Mutex<VecDeque<RetiredBag>>` with lock-free SPSC per CollectorPin
- Writer (retire_inner) pushes to sender — own thread, zero contention
- Reclaimer (try_reclaim) pops from receiver — single winner via needs_reclaim CAS
- Issue: popped-but-not-safe bags need local buffer in reclaimer (can't push back to SPSC)
- Simulation shows 2.2x speedup on both reads and writes at 1W/1R
- Segfault in simulation: likely SPSC receiver state or lost bags in reclaim loop
