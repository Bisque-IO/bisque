//! Runtime-configured SPSC (single-producer / single-consumer) bounded queue.
//!
//! This module provides a variant of the segmented SPSC queue that uses runtime
//! configuration instead of const generics. This is useful when:
//!
//! - Queue sizes need to be determined at runtime
//! - Configuration comes from user input or config files
//! - You want to avoid monomorphization bloat from many queue configurations
//!
//! # Trade-offs vs Const Generic Version
//!
//! | Aspect | `Spsc<T, P, NUM_SEGS_P2, S>` | `DynSpsc<T, S>` |
//! |--------|------------------------------|-----------------|
//! | Configuration | Compile-time | Runtime |
//! | Performance | Optimal (constants inlined) | Slightly slower (runtime loads) |
//! | Binary size | Larger (monomorphization) | Smaller |
//! | Flexibility | Fixed at compile time | Configurable |
//!
//! # Example
//!
//! ```ignore
//! use maniac::spsc::dynamic::{DynSpsc, DynSender, DynReceiver};
//!
//! // Create queue with 64 items/segment, 256 segments
//! let config = DynSpscConfig::new(6, 8); // P=6, NUM_SEGS_P2=8
//! let (sender, receiver) = DynSpsc::<u64, NoOpSignal>::new_with_config(config, NoOpSignal);
//!
//! sender.try_push(42).unwrap();
//! assert_eq!(receiver.try_pop(), Some(42));
//! ```

use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::sync::Arc;

use super::{PopError, PushError};
use crate::CachePadded;

use super::SignalSchedule;

pub mod unbounded;

/// Configuration for a dynamically-sized SPSC queue.
///
/// This struct holds the runtime-determined parameters that would
/// normally be const generics in the static version.
#[derive(Clone, Copy, Debug)]
pub struct DynSpscConfig {
    /// log2(segment size) - each segment holds `2^p` items
    pub p: usize,
    /// log2(directory size) - directory has `2^num_segs_p2` slots
    pub num_segs_p2: usize,
    /// Maximum number of segments to keep in the sealed pool
    pub max_pooled_segments: usize,
    pub seg_mask: usize,
    pub dir_mask: usize,
    pub capacity: usize,
}

impl DynSpscConfig {
    /// Creates a new configuration with default max_pooled_segments (0).
    ///
    /// # Arguments
    ///
    /// * `p` - log2(segment_size). Example: p=8 → 256 items/segment
    /// * `num_segs_p2` - log2(directory_size). Example: num_segs_p2=10 → 1024 segments
    ///
    /// # Panics
    ///
    /// Panics if `p` or `num_segs_p2` would cause overflow (sum > 63).
    #[inline]
    pub fn new(p: usize, num_segs_p2: usize) -> Self {
        assert!(
            p + num_segs_p2 <= 63,
            "p + num_segs_p2 must be <= 63 to avoid overflow"
        );
        let p = p.max(1);
        let num_segs_p2 = num_segs_p2.max(1);
        let seg_mask = (1usize << p) - 1;
        let dir_mask = (1usize << num_segs_p2) - 1;
        let capacity = (1usize << p) * (1usize << num_segs_p2) - 1;
        Self {
            p,
            num_segs_p2,
            max_pooled_segments: 0,
            seg_mask,
            dir_mask,
            capacity,
        }
    }

    /// Creates a new configuration with custom max_pooled_segments.
    #[inline]
    pub fn with_max_pooled(p: usize, num_segs_p2: usize, max_pooled_segments: usize) -> Self {
        assert!(
            p + num_segs_p2 <= 63,
            "p + num_segs_p2 must be <= 63 to avoid overflow"
        );
        let p = p.max(1);
        let num_segs_p2 = num_segs_p2.max(1);
        let seg_mask = (1usize << p) - 1;
        let dir_mask = (1usize << num_segs_p2) - 1;
        let capacity = (1usize << p) * (1usize << num_segs_p2) - 1;
        Self {
            p,
            num_segs_p2,
            max_pooled_segments,
            seg_mask,
            dir_mask,
            capacity,
        }
    }

    #[inline]
    pub fn with_capacity(
        page_size: usize,
        num_segments: usize,
        max_pooled_segments: usize,
    ) -> Self {
        let page_size = page_size.min(16).next_power_of_two();
        let num_segments = num_segments.min(1).next_power_of_two();
        let max_pooled_segments = max_pooled_segments.min(num_segments);
        let p = page_size.trailing_zeros() as usize;
        let num_segs_p2 = num_segments.trailing_zeros() as usize;
        Self {
            p,
            num_segs_p2,
            max_pooled_segments,
            seg_mask: page_size - 1,
            dir_mask: num_segments - 1,
            capacity: page_size * num_segments - 1,
        }
    }

    /// Items per segment (2^p)
    #[inline]
    pub fn seg_size(&self) -> usize {
        self.seg_mask + 1
    }

    /// Number of directory slots (2^num_segs_p2)
    #[inline]
    pub fn num_segs(&self) -> usize {
        self.dir_mask + 1
    }

    /// Mask to extract offset within a segment
    #[inline]
    pub fn seg_mask(&self) -> usize {
        self.seg_mask
    }

    /// Mask to extract segment index from directory
    #[inline]
    pub fn dir_mask(&self) -> usize {
        self.dir_mask
    }

    /// Maximum capacity of the queue
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Bit mask for the close flag in the head position (bit 63).
const CLOSED_CHANNEL_MASK: u64 = 1u64 << 63;

/// Bit mask covering all position bits (excludes close bit).
const RIGHT_MASK: u64 = !CLOSED_CHANNEL_MASK;

/// Cache-line aligned producer state.
struct ProducerState {
    head: AtomicU64,
    tail_cache: UnsafeCell<u64>,
    sealable_lo: AtomicU64,
    fresh_allocations: AtomicU64,
    pool_reuses: AtomicU64,
    total_allocated_segments: AtomicU64,
}

unsafe impl Send for ProducerState {}
unsafe impl Sync for ProducerState {}

impl ProducerState {
    fn new() -> Self {
        Self {
            head: AtomicU64::new(0),
            tail_cache: UnsafeCell::new(0),
            sealable_lo: AtomicU64::new(0),
            fresh_allocations: AtomicU64::new(0),
            pool_reuses: AtomicU64::new(0),
            total_allocated_segments: AtomicU64::new(0),
        }
    }
}

/// Cache-line aligned consumer state.
struct ConsumerState {
    tail: AtomicU64,
    head_cache: UnsafeCell<u64>,
    sealable_hi: AtomicU64,
}

unsafe impl Send for ConsumerState {}
unsafe impl Sync for ConsumerState {}

impl ConsumerState {
    fn new() -> Self {
        Self {
            tail: AtomicU64::new(0),
            head_cache: UnsafeCell::new(0),
            sealable_hi: AtomicU64::new(0),
        }
    }
}

/// Producer half of a dynamically-configured SPSC queue.
pub struct Sender<T, S: SignalSchedule> {
    queue: Arc<DynSpsc<T, S>>,
}

impl<T, S: SignalSchedule> Sender<T, S> {
    /// Returns the number of items currently in the queue.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns `true` if the queue contains no items.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns `true` if the queue is at maximum capacity.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    /// Returns the capacity of the queue.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Attempts to enqueue a single item.
    #[inline(always)]
    pub fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        self.queue.try_push(item)
    }

    /// Attempts to enqueue multiple items from a slice.
    #[inline(always)]
    pub fn try_push_n(&self, src: &[T]) -> Result<usize, PushError<()>> {
        self.queue.try_push_n(src)
    }

    /// Returns the number of fresh segment allocations (pool misses).
    #[inline(always)]
    pub fn fresh_allocations(&self) -> u64 {
        self.queue.fresh_allocations()
    }

    /// Returns the number of segment pool reuses (pool hits).
    #[inline(always)]
    pub fn pool_reuses(&self) -> u64 {
        self.queue.pool_reuses()
    }

    /// Returns the segment pool reuse rate as a percentage (0-100%).
    #[inline(always)]
    pub fn pool_reuse_rate(&self) -> f64 {
        self.queue.pool_reuse_rate()
    }

    /// Resets allocation statistics to zero.
    #[inline(always)]
    pub fn reset_allocation_stats(&self) {
        self.queue.reset_allocation_stats();
    }

    /// Returns the current number of allocated segments.
    #[inline(always)]
    pub fn allocated_segments(&self) -> u64 {
        self.queue.allocated_segments()
    }

    /// Calculates the current memory usage in bytes.
    #[inline(always)]
    pub fn allocated_memory_bytes(&self) -> usize {
        self.queue.allocated_memory_bytes()
    }

    /// Closes the underlying queue.
    #[inline(always)]
    pub(crate) fn close_channel(&self) {
        unsafe {
            self.queue.close();
        }
    }
}

impl<T, S: SignalSchedule> Drop for Sender<T, S> {
    fn drop(&mut self) {
        unsafe {
            self.queue.close();
        }
    }
}

/// Consumer half of a dynamically-configured SPSC queue.
pub struct Receiver<T, S: SignalSchedule> {
    queue: Arc<DynSpsc<T, S>>,
}

impl<T, S: SignalSchedule> Receiver<T, S> {
    /// Returns the number of items currently in the queue.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns `true` if the queue contains no items.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns `true` if the queue is at maximum capacity.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    /// Returns the capacity of the queue.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Checks if the queue has been closed.
    #[inline(always)]
    pub fn is_closed(&self) -> bool {
        self.queue.is_closed()
    }

    /// Attempts to dequeue a single item.
    #[inline(always)]
    pub fn try_pop(&self) -> Option<T> {
        self.queue.try_pop()
    }

    /// Attempts to dequeue multiple items into a slice.
    #[inline(always)]
    pub fn try_pop_n(&self, dst: &mut [T]) -> Result<usize, PopError> {
        self.queue.try_pop_n(dst)
    }

    /// Zero-copy consumption: processes items in-place via callback.
    #[inline(always)]
    pub fn consume_in_place<F>(&self, max: usize, f: F) -> usize
    where
        F: FnMut(&[T]) -> usize,
    {
        self.queue.consume_in_place(max, f)
    }

    /// Returns the number of fresh segment allocations.
    #[inline(always)]
    pub fn fresh_allocations(&self) -> u64 {
        self.queue.fresh_allocations()
    }

    /// Returns the number of segment pool reuses.
    #[inline(always)]
    pub fn pool_reuses(&self) -> u64 {
        self.queue.pool_reuses()
    }

    /// Returns the segment pool reuse rate.
    #[inline(always)]
    pub fn pool_reuse_rate(&self) -> f64 {
        self.queue.pool_reuse_rate()
    }

    /// Resets allocation statistics.
    #[inline(always)]
    pub fn reset_allocation_stats(&self) {
        self.queue.reset_allocation_stats();
    }

    /// Returns the current number of allocated segments.
    #[inline(always)]
    pub fn allocated_segments(&self) -> u64 {
        self.queue.allocated_segments()
    }

    /// Calculates the current memory usage in bytes.
    #[inline(always)]
    pub fn allocated_memory_bytes(&self) -> usize {
        self.queue.allocated_memory_bytes()
    }

    /// Attempts to deallocate excess segments.
    #[inline(always)]
    pub fn try_deallocate_excess(&self) -> u64 {
        self.queue.try_deallocate_excess()
    }

    /// Deallocates sealed pool segments down to a target size.
    #[inline(always)]
    pub fn deallocate_to(&self, target_pool_size: usize) -> u64 {
        self.queue.deallocate_to(target_pool_size)
    }
}

/// A dynamically-configured bounded SPSC queue.
///
/// This is the runtime-configured variant of `Spsc<T, P, NUM_SEGS_P2, S>`.
/// Configuration parameters are stored at runtime instead of being const generics.
pub struct DynSpsc<T, S: SignalSchedule> {
    /// Runtime configuration
    config: DynSpscConfig,
    /// Segment directory
    segs: Box<[AtomicPtr<MaybeUninit<T>>]>,
    /// Producer state
    producer: CachePadded<ProducerState>,
    /// Consumer state
    consumer: CachePadded<ConsumerState>,
    /// Signal scheduler
    signal: CachePadded<S>,
}

impl<T, S: SignalSchedule> DynSpsc<T, S> {
    /// Creates a new dynamically-configured SPSC queue.
    ///
    /// # Arguments
    ///
    /// * `config` - Runtime configuration for segment size and directory size
    /// * `signal` - Signal scheduler for executor integration
    pub fn new_with_config(config: DynSpscConfig, signal: S) -> (Sender<T, S>, Receiver<T, S>) {
        let queue = Arc::new(unsafe { Self::new_unsafe(config, signal) });
        (
            Sender {
                queue: Arc::clone(&queue),
            },
            Receiver {
                queue: Arc::clone(&queue),
            },
        )
    }

    /// Creates a new queue (unsafe, internal use).
    ///
    /// # Safety
    ///
    /// Callers must ensure the returned value is wrapped in Arc and
    /// only one Sender and one Receiver are created.
    pub unsafe fn new_unsafe(config: DynSpscConfig, signal: S) -> Self {
        let num_segs = config.num_segs();
        let mut v = Vec::with_capacity(num_segs);
        for _ in 0..num_segs {
            v.push(AtomicPtr::new(ptr::null_mut()));
        }

        Self {
            config,
            segs: v.into_boxed_slice(),
            producer: CachePadded::new(ProducerState::new()),
            consumer: CachePadded::new(ConsumerState::new()),
            signal: CachePadded::new(signal),
        }
    }

    /// Returns the queue capacity.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.config.capacity()
    }

    /// Returns the segment size.
    #[inline(always)]
    pub fn seg_size(&self) -> usize {
        self.config.seg_size()
    }

    /// Returns the number of segments.
    #[inline(always)]
    pub fn num_segs(&self) -> usize {
        self.config.num_segs()
    }

    // ──────────────────────────────────────────────────────────────────────────────
    // Signal Integration API
    // ──────────────────────────────────────────────────────────────────────────────

    #[inline(always)]
    pub fn schedule(&self) {
        let _ = self.signal.schedule();
    }

    #[inline(always)]
    pub(crate) fn mark(&self) {
        self.signal.mark();
    }

    #[inline(always)]
    pub(crate) fn unmark(&self) {
        self.signal.unmark();
    }

    #[inline(always)]
    pub(crate) fn unmark_and_schedule(&self) {
        self.signal.unmark_and_schedule();
    }

    // ──────────────────────────────────────────────────────────────────────────────
    // Inspection API
    // ──────────────────────────────────────────────────────────────────────────────

    #[inline(always)]
    pub fn len(&self) -> usize {
        let h = self.producer.head.load(Ordering::Acquire);
        let t = self.consumer.tail.load(Ordering::Acquire);
        let h_pos = h & RIGHT_MASK;
        h_pos.saturating_sub(t) as usize
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    // ──────────────────────────────────────────────────────────────────────────────
    // Close API
    // ──────────────────────────────────────────────────────────────────────────────

    #[inline(always)]
    pub fn is_closed(&self) -> bool {
        self.producer.head.load(Ordering::Relaxed) & CLOSED_CHANNEL_MASK != 0
    }

    #[inline(always)]
    pub unsafe fn close(&self) {
        self.producer
            .head
            .fetch_or(CLOSED_CHANNEL_MASK, Ordering::Relaxed);
        self.schedule();
    }

    // ──────────────────────────────────────────────────────────────────────────────
    // Producer API
    // ──────────────────────────────────────────────────────────────────────────────

    #[inline(always)]
    pub fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        self.try_push_n(core::slice::from_ref(&item))
            .map(|_| ())
            .map_err(|err| match err {
                PushError::Full(()) => PushError::Full(item),
                PushError::Closed(()) => PushError::Closed(item),
            })
    }

    pub fn try_push_n(&self, src: &[T]) -> Result<usize, PushError<()>> {
        let h = self.producer.head.load(Ordering::Acquire);

        if h & CLOSED_CHANNEL_MASK != 0 {
            return Err(PushError::Closed(()));
        }

        let mut t = unsafe { *self.producer.tail_cache.get() };
        let cap = self.capacity();
        let mut free = cap - (h - t) as usize;

        if free == 0 {
            t = self.consumer.tail.load(Ordering::Acquire);
            unsafe {
                *self.producer.tail_cache.get() = t;
            }
            free = cap - (h - t) as usize;
            if free == 0 {
                return Err(PushError::Full(()));
            }
        }

        let p = self.config.p;
        let seg_mask = self.config.seg_mask();
        let dir_mask = self.config.dir_mask();
        let seg_size = self.config.seg_size();

        let mut n = src.len().min(free);
        let mut i = h;
        let mut copied = 0usize;

        while n > 0 {
            let seg_idx = ((i >> p) as usize) & dir_mask;
            let off = (i as usize) & seg_mask;
            let base = self.ensure_segment_for(seg_idx);
            let can = (seg_size - off).min(n);

            unsafe {
                let dst_ptr = base.add(off) as *mut T as *mut core::ffi::c_void;
                let src_ptr = src.as_ptr().add(copied) as *const core::ffi::c_void;
                ptr::copy_nonoverlapping(
                    src_ptr as *const u8,
                    dst_ptr as *mut u8,
                    can * core::mem::size_of::<T>(),
                );
            }

            i += can as u64;
            copied += can;
            n -= can;

            if ((i as usize) & seg_mask) == 0 {
                let _ = self.segs[((i >> p) as usize) & dir_mask].load(Ordering::Relaxed);
            }
        }

        self.producer.head.store(i, Ordering::Release);
        let _ = self.signal.schedule();
        Ok(copied)
    }

    // ──────────────────────────────────────────────────────────────────────────────
    // Consumer API
    // ──────────────────────────────────────────────────────────────────────────────

    #[inline(always)]
    pub fn try_pop(&self) -> Option<T> {
        let mut tmp: T = unsafe { core::mem::zeroed() };
        match self.try_pop_n(core::slice::from_mut(&mut tmp)) {
            Ok(1) => Some(tmp),
            _ => None,
        }
    }

    pub fn try_pop_n(&self, dst: &mut [T]) -> Result<usize, PopError> {
        let t = self.consumer.tail.load(Ordering::Relaxed);

        let mut h = unsafe { *self.consumer.head_cache.get() };
        let h_pos = h & RIGHT_MASK;
        let mut avail = h_pos.saturating_sub(t) as usize;

        if avail == 0 {
            h = self.producer.head.load(Ordering::Acquire);
            unsafe {
                *self.consumer.head_cache.get() = h;
            }
            let h_pos = h & RIGHT_MASK;
            avail = h_pos.saturating_sub(t) as usize;
            if avail == 0 {
                if h & CLOSED_CHANNEL_MASK != 0 {
                    return Err(PopError::Closed);
                }
                return Err(PopError::Empty);
            }
        }

        let p = self.config.p;
        let seg_mask = self.config.seg_mask();
        let dir_mask = self.config.dir_mask();
        let seg_size = self.config.seg_size();

        let mut n = dst.len().min(avail);
        let mut i = t;
        let mut copied = 0usize;

        while n > 0 {
            let seg_idx = ((i >> p) as usize) & dir_mask;
            let off = (i as usize) & seg_mask;

            let base = self.segs[seg_idx].load(Ordering::Acquire);
            debug_assert!(!base.is_null());

            let can = (seg_size - off).min(n);
            unsafe {
                let src_ptr = (base.add(off) as *const MaybeUninit<T>) as *const T;
                let dst_ptr = dst.as_mut_ptr().add(copied);
                ptr::copy_nonoverlapping(src_ptr, dst_ptr, can);
            }

            i += can as u64;
            copied += can;
            n -= can;

            if ((i as usize) & seg_mask) == 0 && i > 0 {
                let s_done = (i >> p).wrapping_sub(1);
                self.seal_after(s_done);
            }
        }

        self.consumer.tail.store(i, Ordering::Release);
        Ok(copied)
    }

    pub fn consume_in_place<F>(&self, max: usize, mut f: F) -> usize
    where
        F: FnMut(&[T]) -> usize,
    {
        if max == 0 {
            return 0;
        }

        let mut t = self.consumer.tail.load(Ordering::Relaxed);

        let mut h = unsafe { *self.consumer.head_cache.get() };
        let h_pos = h & RIGHT_MASK;
        let mut avail = h_pos.saturating_sub(t) as usize;

        if avail == 0 {
            h = self.producer.head.load(Ordering::Acquire);
            unsafe {
                *self.consumer.head_cache.get() = h;
            }
            let h_pos = h & RIGHT_MASK;
            avail = h_pos.saturating_sub(t) as usize;
            if avail == 0 {
                return 0;
            }
        }

        let p = self.config.p;
        let seg_mask = self.config.seg_mask();
        let dir_mask = self.config.dir_mask();
        let seg_size = self.config.seg_size();

        let mut remaining = max.min(avail);
        let mut total_consumed = 0usize;

        while remaining > 0 {
            let seg_idx = ((t >> p) as usize) & dir_mask;
            let off = (t as usize) & seg_mask;

            let base = self.segs[seg_idx].load(Ordering::Acquire);

            let in_seg = seg_size - off;
            let n = remaining.min(in_seg);

            let slice = unsafe {
                let ptr = (base.add(off) as *const MaybeUninit<T>) as *const T;
                core::slice::from_raw_parts(ptr, n)
            };

            let took = f(slice).min(n);
            if took == 0 {
                break;
            }

            t += took as u64;
            total_consumed += took;
            remaining -= took;

            if ((t as usize) & seg_mask) == 0 && t > 0 {
                let s_done = (t >> p).wrapping_sub(1);
                self.seal_after(s_done);
            }

            if took < n {
                break;
            }
        }

        self.consumer.tail.store(t, Ordering::Release);
        total_consumed
    }

    // ──────────────────────────────────────────────────────────────────────────────
    // Metrics API
    // ──────────────────────────────────────────────────────────────────────────────

    #[inline(always)]
    pub fn fresh_allocations(&self) -> u64 {
        self.producer.fresh_allocations.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn pool_reuses(&self) -> u64 {
        self.producer.pool_reuses.load(Ordering::Relaxed)
    }

    pub fn pool_reuse_rate(&self) -> f64 {
        let fresh = self.fresh_allocations();
        let reused = self.pool_reuses();
        let total = fresh + reused;

        if total == 0 {
            0.0
        } else {
            (reused as f64 / total as f64) * 100.0
        }
    }

    pub fn reset_allocation_stats(&self) {
        self.producer.fresh_allocations.store(0, Ordering::Relaxed);
        self.producer.pool_reuses.store(0, Ordering::Relaxed);
    }

    pub fn allocated_segments(&self) -> u64 {
        self.producer
            .total_allocated_segments
            .load(Ordering::Relaxed)
    }

    pub fn try_deallocate_excess(&self) -> u64 {
        if self.config.max_pooled_segments == usize::MAX {
            return 0;
        }
        self.try_deallocate_pool_to(self.config.max_pooled_segments)
    }

    pub fn deallocate_to(&self, target_pool_size: usize) -> u64 {
        self.try_deallocate_pool_to(target_pool_size)
    }

    pub fn allocated_memory_bytes(&self) -> usize {
        let segments = self.allocated_segments() as usize;
        segments * self.config.seg_size() * core::mem::size_of::<T>()
    }

    // ──────────────────────────────────────────────────────────────────────────────
    // Internal Implementation
    // ──────────────────────────────────────────────────────────────────────────────

    #[inline(always)]
    fn seal_after(&self, s_done: u64) {
        let new_hi = s_done + 1;
        self.consumer.sealable_hi.store(new_hi, Ordering::Release);

        if self.config.max_pooled_segments != usize::MAX {
            self.try_deallocate_pool_to(self.config.max_pooled_segments);
        }
    }

    fn try_deallocate_pool_to(&self, target_pool_size: usize) -> u64 {
        let mut hi = self.consumer.sealable_hi.load(Ordering::Acquire);
        let lo = self.producer.sealable_lo.load(Ordering::Acquire);
        let pool_size = hi.saturating_sub(lo);

        if pool_size <= target_pool_size as u64 {
            return 0;
        }

        let head = self.producer.head.load(Ordering::Acquire) & RIGHT_MASK;
        let tail = self.consumer.tail.load(Ordering::Acquire);

        if tail != head {
            return 0;
        }

        let dir_mask = self.config.dir_mask();
        let seg_size = self.config.seg_size();
        let num_segs = self.config.num_segs();

        let mut total_allocated = 0u64;
        for i in 0..num_segs {
            let seg_ptr = self.segs[i].load(Ordering::Acquire);
            if !seg_ptr.is_null() {
                total_allocated += 1;
            }
        }

        if total_allocated <= target_pool_size as u64 {
            return 0;
        }

        let need_to_dealloc = total_allocated - target_pool_size as u64;
        let mut deallocated_count = 0u64;
        let mut current_lo = lo;

        while current_lo < hi && deallocated_count < need_to_dealloc {
            let dir_idx = (current_lo as usize) & dir_mask;

            match self.producer.sealable_lo.compare_exchange(
                current_lo,
                current_lo + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let seg_ptr = self.segs[dir_idx].swap(ptr::null_mut(), Ordering::AcqRel);

                    if !seg_ptr.is_null() {
                        unsafe {
                            free_segment::<T>(seg_ptr, seg_size);
                        }
                        deallocated_count += 1;
                    }

                    current_lo += 1;
                }
                Err(new_lo_val) => {
                    current_lo = new_lo_val;
                    let new_hi = self.consumer.sealable_hi.load(Ordering::Acquire);
                    if new_hi < hi {
                        hi = new_hi;
                    }
                }
            }
        }

        if deallocated_count > 0 {
            self.producer
                .total_allocated_segments
                .fetch_sub(deallocated_count, Ordering::Relaxed);
        }

        deallocated_count
    }

    fn ensure_segment_for(&self, seg_idx: usize) -> *mut MaybeUninit<T> {
        let p = self.segs[seg_idx].load(Ordering::Acquire);
        if !p.is_null() {
            return p;
        }

        let mut hi = self.consumer.sealable_hi.load(Ordering::Acquire);
        let mut lo = self.producer.sealable_lo.load(Ordering::Acquire);

        let dir_mask = self.config.dir_mask();
        let seg_size = self.config.seg_size();
        let num_segs = self.config.num_segs();

        let should_deallocate = if self.config.max_pooled_segments < num_segs {
            let pool_size = hi.saturating_sub(lo);
            pool_size > self.config.max_pooled_segments as u64
        } else {
            false
        };

        while lo < hi {
            let src_idx = (lo as usize) & dir_mask;

            match self.producer.sealable_lo.compare_exchange(
                lo,
                lo + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let q = self.segs[src_idx].swap(ptr::null_mut(), Ordering::AcqRel);

                    if q.is_null() {
                        let newp = unsafe { alloc_segment::<T>(seg_size) };
                        self.segs[seg_idx].store(newp, Ordering::Release);
                        self.producer
                            .fresh_allocations
                            .fetch_add(1, Ordering::Relaxed);
                        self.producer
                            .total_allocated_segments
                            .fetch_add(1, Ordering::Relaxed);
                        return newp;
                    } else if should_deallocate {
                        let new_hi = self.consumer.sealable_hi.load(Ordering::Acquire);
                        let new_lo = self.producer.sealable_lo.load(Ordering::Acquire);
                        let new_pool_size = new_hi.saturating_sub(new_lo);

                        if new_pool_size <= self.config.max_pooled_segments as u64 {
                            self.segs[seg_idx].store(q, Ordering::Release);
                            self.producer.pool_reuses.fetch_add(1, Ordering::Relaxed);
                            return q;
                        }

                        unsafe {
                            free_segment::<T>(q, seg_size);
                        }
                        self.producer
                            .total_allocated_segments
                            .fetch_sub(1, Ordering::Relaxed);

                        lo = new_lo;
                        hi = new_hi;
                    } else {
                        self.segs[seg_idx].store(q, Ordering::Release);
                        self.producer.pool_reuses.fetch_add(1, Ordering::Relaxed);
                        return q;
                    }
                }
                Err(new_lo) => {
                    lo = new_lo;
                    let new_hi = self.consumer.sealable_hi.load(Ordering::Acquire);
                    if new_hi > hi {
                        hi = new_hi;
                    }
                }
            }
        }

        let newp = unsafe { alloc_segment::<T>(seg_size) };
        self.segs[seg_idx].store(newp, Ordering::Release);
        self.producer
            .fresh_allocations
            .fetch_add(1, Ordering::Relaxed);
        self.producer
            .total_allocated_segments
            .fetch_add(1, Ordering::Relaxed);
        newp
    }
}

impl<T, S: SignalSchedule> Drop for DynSpsc<T, S> {
    fn drop(&mut self) {
        let seg_size = self.config.seg_size();
        for slot in self.segs.iter() {
            let p = slot.load(Ordering::Relaxed);
            if !p.is_null() {
                unsafe { free_segment::<T>(p, seg_size) };
            }
        }
    }
}

unsafe fn alloc_segment<T>(seg_size: usize) -> *mut MaybeUninit<T> {
    unsafe {
        let elem = core::mem::size_of::<MaybeUninit<T>>();
        let align = core::mem::align_of::<MaybeUninit<T>>();
        let layout = Layout::from_size_align_unchecked(elem * seg_size, align);
        let p = alloc_zeroed(layout) as *mut MaybeUninit<T>;
        if p.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        p
    }
}

unsafe fn free_segment<T>(ptr_base: *mut MaybeUninit<T>, seg_size: usize) {
    unsafe {
        let elem = core::mem::size_of::<MaybeUninit<T>>();
        let align = core::mem::align_of::<MaybeUninit<T>>();
        let layout = Layout::from_size_align_unchecked(elem * seg_size, align);
        dealloc(ptr_base as *mut u8, layout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::spsc::NoOpSignal;

    fn test_config() -> DynSpscConfig {
        DynSpscConfig::new(6, 8) // 64 items/segment, 256 segments
    }

    #[test]
    fn basic_push_pop() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);
        for i in 0..200u64 {
            sender.try_push(i).unwrap();
        }
        for i in 0..200u64 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
        assert!(receiver.try_pop().is_none());
    }

    #[test]
    fn zero_copy_consume_in_place() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);
        let n = 64 * 3 + 5;
        for i in 0..n as u64 {
            sender.try_push(i).unwrap();
        }

        let mut seen = 0u64;
        while seen < n as u64 {
            let mut local_seen = seen;
            let took = receiver.consume_in_place(50, |chunk: &[u64]| {
                for (k, &v) in chunk.iter().enumerate() {
                    assert_eq!(v, local_seen + k as u64, "Mismatch at local offset {}", k);
                }
                local_seen += chunk.len() as u64;
                chunk.len()
            });
            assert!(took > 0);
            seen += took as u64;
        }
    }

    #[test]
    fn test_config_values() {
        let config = test_config();
        assert_eq!(config.seg_size(), 64);
        assert_eq!(config.num_segs(), 256);
        assert_eq!(config.seg_mask(), 63);
        assert_eq!(config.dir_mask(), 255);
        assert_eq!(config.capacity(), 64 * 256 - 1);
    }

    #[test]
    fn test_new_queue_is_empty() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);
        assert!(sender.is_empty());
        assert!(receiver.is_empty());
        assert!(!sender.is_full());
        assert_eq!(sender.len(), 0);
    }

    #[test]
    fn test_single_item_operations() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);
        assert!(sender.is_empty());

        assert!(sender.try_push(42).is_ok());
        assert_eq!(sender.len(), 1);
        assert!(!sender.is_empty());

        assert_eq!(receiver.try_pop(), Some(42));
        assert_eq!(receiver.len(), 0);
        assert!(receiver.is_empty());
    }

    #[test]
    fn test_push_pop_order() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);
        let items = [1, 2, 3, 4, 5];

        for &item in &items {
            sender.try_push(item).unwrap();
        }

        for &expected in &items {
            assert_eq!(receiver.try_pop(), Some(expected));
        }
    }

    #[test]
    fn test_try_push_n() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);
        let items = [10, 20, 30, 40, 50];

        let pushed = sender.try_push_n(&items).unwrap();
        assert_eq!(pushed, items.len());
        assert_eq!(sender.len(), items.len());

        for &expected in items.iter() {
            assert_eq!(receiver.try_pop(), Some(expected));
        }
    }

    #[test]
    fn test_try_pop_n() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);
        let items = [1, 2, 3, 4, 5, 6, 7, 8];

        for &item in &items {
            sender.try_push(item).unwrap();
        }

        let mut buffer = [0u64; 5];
        let popped = receiver.try_pop_n(&mut buffer).unwrap();
        assert_eq!(popped, 5);
        assert_eq!(&buffer[..5], &[1, 2, 3, 4, 5]);

        let mut buffer2 = [0u64; 8];
        let popped2 = receiver.try_pop_n(&mut buffer2).unwrap();
        assert_eq!(popped2, 3);
        assert_eq!(&buffer2[..3], &[6, 7, 8]);
    }

    #[test]
    fn test_operations_on_empty_queue() {
        let (_, receiver) = DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);

        assert_eq!(receiver.try_pop(), None);

        let mut buffer = [0u64; 5];
        assert!(receiver.try_pop_n(&mut buffer).is_err());

        let consumed = receiver.consume_in_place(10, |_| 0);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn test_operations_on_full_queue() {
        let config = test_config();
        let (sender, receiver) = DynSpsc::<u64, NoOpSignal>::new_with_config(config, NoOpSignal);

        let capacity = config.capacity();
        for i in 0..capacity as u64 {
            sender.try_push(i).unwrap();
        }

        assert!(sender.is_full());
        assert_eq!(sender.len(), capacity);

        assert!(sender.try_push(999).is_err());

        let items = [1000, 1001, 1002];
        assert!(sender.try_push_n(&items).is_err());

        assert_eq!(receiver.try_pop(), Some(0));
        assert!(!sender.is_full());
        assert_eq!(sender.len(), capacity - 1);
    }

    #[test]
    fn test_segment_boundaries() {
        let config = test_config();
        let (sender, receiver) = DynSpsc::<u64, NoOpSignal>::new_with_config(config, NoOpSignal);
        let seg_size = config.seg_size();

        for i in 0..seg_size as u64 {
            sender.try_push(i).unwrap();
        }

        sender.try_push(seg_size as u64).unwrap();

        for i in 0..=seg_size as u64 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        assert!(receiver.is_empty());
    }

    #[test]
    fn test_multiple_segments() {
        let config = test_config();
        let (sender, receiver) = DynSpsc::<u64, NoOpSignal>::new_with_config(config, NoOpSignal);
        let seg_size = config.seg_size();
        let num_segments = 3;
        let total_items = seg_size * num_segments;

        for i in 0..total_items as u64 {
            sender.try_push(i).unwrap();
        }

        for i in 0..total_items as u64 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        assert!(receiver.is_empty());
    }

    #[test]
    fn test_queue_reuse_after_drain() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);

        for i in 0..1000 {
            sender.try_push(i as u64).unwrap();
        }
        for i in 0..1000 {
            assert_eq!(receiver.try_pop(), Some(i as u64));
        }

        assert!(receiver.is_empty());

        for i in 0..500 {
            sender.try_push((i + 1000) as u64).unwrap();
        }
        for i in 0..500 {
            assert_eq!(receiver.try_pop(), Some((i + 1000) as u64));
        }

        assert!(receiver.is_empty());
    }

    #[test]
    fn test_different_types() {
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        struct TestStruct {
            a: u32,
            b: i64,
        }

        let config = DynSpscConfig::new(5, 6);
        let (sender, receiver) =
            DynSpsc::<TestStruct, NoOpSignal>::new_with_config(config, NoOpSignal);

        let item1 = TestStruct { a: 1, b: -1 };
        let item2 = TestStruct { a: 2, b: -2 };

        sender.try_push(item1).unwrap();
        sender.try_push(item2).unwrap();

        assert_eq!(receiver.try_pop(), Some(item1));
        assert_eq!(receiver.try_pop(), Some(item2));
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_close_semantics() {
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(test_config(), NoOpSignal);

        sender.try_push(1).unwrap();
        sender.try_push(2).unwrap();

        drop(sender); // Closes the queue

        assert!(receiver.is_closed());
        assert_eq!(receiver.try_pop(), Some(1));
        assert_eq!(receiver.try_pop(), Some(2));
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_different_configurations() {
        // Small queue
        let small_config = DynSpscConfig::new(4, 2); // 16 items/seg, 4 segs = 63 capacity
        let (sender, receiver) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(small_config, NoOpSignal);
        assert_eq!(sender.capacity(), 63);

        for i in 0..63u64 {
            sender.try_push(i).unwrap();
        }
        assert!(sender.is_full());

        for i in 0..63u64 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Large queue
        let large_config = DynSpscConfig::new(10, 4); // 1024 items/seg, 16 segs = 16383 capacity
        let (sender2, receiver2) =
            DynSpsc::<u64, NoOpSignal>::new_with_config(large_config, NoOpSignal);
        assert_eq!(sender2.capacity(), 16383);

        for i in 0..1000u64 {
            sender2.try_push(i).unwrap();
        }
        for i in 0..1000u64 {
            assert_eq!(receiver2.try_pop(), Some(i));
        }
    }
}
