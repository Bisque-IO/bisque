//! StripedArc - A striped Arc that distributes reference counting across cache lines.
//!
//! Uses epoch-based reclamation to defer expensive cleanup checks.
//!
//! Memory Ordering:
//! - `increment()`: Release - ensures all prior accesses to T happen before increment
//! - `decrement()`: AcqRel - ensures all prior increments are visible when count crosses zero
//! - `load()`: Acquire - ensures we see all prior updates to the counter
//! - `is_closed()`: Relaxed - close flag doesn't need ordering with other operations
//! - `close()`: Release - ensures close is visible to subsequent operations
//! - SeqCst fence before total() - ensures all decrements across stripes are visible
//! - CAS on cleanup_claimed - prevents double-free by atomically claiming cleanup

use crate::padded::CachePadded;

use std::ops::Deref;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

struct Counter {
    value: AtomicI64,
}

impl Counter {
    const CLOSED_MASK: i64 = 1 << 63;
    const VALUE_MASK: i64 = !Self::CLOSED_MASK;
    const BIAS: i64 = 1 << 62; // 2^62, allows storing signed 63-bit values

    fn new() -> Self {
        // Initialize biased to represent actual value of 0
        Counter {
            value: AtomicI64::new(Self::BIAS),
        }
    }

    #[inline(always)]
    pub(crate) fn load(&self) -> i64 {
        let bits = self.value.load(Ordering::Acquire);
        let biased = bits & Self::VALUE_MASK;
        // Remove bias to get actual signed value (supports negative numbers)
        biased.wrapping_sub(Self::BIAS)
    }

    #[inline(always)]
    pub(crate) fn is_closed(&self) -> bool {
        (self.value.load(Ordering::Relaxed) & Self::CLOSED_MASK) != 0
    }

    #[inline(always)]
    pub(crate) fn increment(&self) {
        // Increment biased value (closed bit in bit 63 is preserved)
        self.value.fetch_add(1, Ordering::Release);
    }

    #[inline(always)]
    pub(crate) fn decrement(&self) -> bool {
        // Decrement biased value and return whether closed bit was set BEFORE decrement
        let prev = self.value.fetch_sub(1, Ordering::AcqRel);
        (prev & Self::CLOSED_MASK) != 0
    }

    #[inline(always)]
    pub(crate) fn close(&self) {
        self.value.fetch_or(Self::CLOSED_MASK, Ordering::Release);
    }
}

/// Striped reference count with epoch-based reclamation.
///
/// Reduces contention by distributing increments/decrements across multiple cache lines.
/// Uses signed integers so individual stripes can go negative as long as the total
/// sum remains correct.
///
/// Correctness: Uses AcqRel ordering on decrement to ensure zero-crossing detection
/// sees all prior increments. Uses SeqCst fences before total() to ensure global
/// ordering across stripes. Uses CAS-based cleanup claim to prevent double-free.
pub(crate) struct StripedRefCount {
    counters: Box<[CachePadded<Counter>]>,
    mask: usize,
    /// Flag to atomically claim cleanup ownership (prevents double-free)
    cleanup_claimed: AtomicU64,
}

impl StripedRefCount {
    fn new() -> Self {
        let num_stripes = std::thread::available_parallelism()
            .map(|n| (n.get() * 8).next_power_of_two())
            .unwrap_or(32);
        let mask = num_stripes - 1;

        let counters: Vec<_> = (0..num_stripes)
            .map(|_| CachePadded::new(Counter::new()))
            .collect();

        Self {
            counters: counters.into_boxed_slice(),
            mask,
            cleanup_claimed: AtomicU64::new(0),
        }
    }

    // Increment refcount using the thread stripe index
    #[inline(always)]
    pub(crate) fn increment(&self) {
        let idx = thread_stripe() & self.mask;
        self.counters[idx].increment();
    }

    /// Increment refcount using the given stripe index
    #[inline(always)]
    pub(crate) fn increment_by_stripe(&self, stripe: usize) {
        let idx = stripe & self.mask;
        self.counters[idx].increment();
    }

    /// Decrement refcount using the thread stripe index.
    /// This is a fast operation - does NOT check if total is zero.
    #[inline(always)]
    pub(crate) fn decrement(&self) {
        let idx = thread_stripe() & self.mask;
        self.counters[idx].decrement();
    }

    // Decrement refcount using the given stripe index.
    /// This is a fast operation - does NOT check if total is zero.
    #[inline(always)]
    pub(crate) fn decrement_by_stripe(&self, stripe: usize) {
        let idx = stripe & self.mask;
        self.counters[idx].decrement();
    }

    /// Get total refcount (sum of all stripes)
    #[inline]
    fn total(&self) -> i64 {
        self.counters.iter().map(|c| c.load()).sum()
    }
}

/// A striped Arc that distributes reference counting across cache lines.
/// Uses signed counters so individual stripes can go negative. Clones and
/// drops of clones are single op atomic operations with zero or near zero
/// contention. Counts can be unbalanced, but the total count is always
/// non-negative.
///
/// The first StripedArc is created with a refcount of 1 and flagged as master.
/// When the master is dropped, decrements become expensive because they need
/// to calculate the total count to determine if the pointer should be dropped.
///
/// WARNING: StripedArc is not compact and should not be used for short lived
/// objects. It is intended for long running types like an async runtime.
pub struct StripedArc<T> {
    ptr: *const StripedArcInner<T>,
    master: bool,
}

pub(crate) struct StripedArcInner<T> {
    pub(crate) refcount: StripedRefCount,
    pub(crate) data: T,
}

impl<T> StripedArc<T> {
    /// Create a new StripedArc with initial refcount of 1
    pub fn new(data: T) -> Self {
        let inner = Box::new(StripedArcInner {
            refcount: StripedRefCount::new(),
            data,
        });

        // Start with refcount of 1 in stripe 0
        inner.refcount.counters[thread_stripe() & inner.refcount.mask].increment();

        Self {
            ptr: Box::into_raw(inner),
            master: true,
        }
    }

    #[inline]
    fn inner(&self) -> &StripedArcInner<T> {
        // Safety: ptr is always valid while StripedArc exists
        unsafe { &*self.ptr }
    }

    /// Get raw pointer to the inner structure (for waker data)
    #[inline]
    pub(crate) fn inner_ptr(&self) -> *const StripedArcInner<T> {
        self.ptr
    }

    /// Get a raw pointer to the data.
    #[inline]
    pub fn as_ptr(this: &Self) -> *const T {
        &this.inner().data as *const T
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner().refcount.counters[0].is_closed()
    }
}

impl<T> Clone for StripedArc<T> {
    #[inline]
    fn clone(&self) -> Self {
        // Use thread ID for stripe selection on normal clones
        let stripe = thread_stripe();
        self.inner().refcount.increment_by_stripe(stripe);
        Self {
            ptr: self.ptr,
            master: false,
        }
    }
}

impl<T> Drop for StripedArc<T> {
    fn drop(&mut self) {
        let stripe = thread_stripe();
        let refcount = &self.inner().refcount;

        if self.master {
            // Mark all partitions as closed.
            for counter in refcount.counters.iter() {
                counter.close();
            }

            refcount.counters[stripe & refcount.mask].decrement();

            // SeqCst fence to ensure all decrements across all stripes are visible
            std::sync::atomic::fence(Ordering::SeqCst);

            let total = refcount.total();
            assert!(total >= 0);

            if total == 0 {
                // Atomically claim cleanup to prevent double-free
                if refcount
                    .cleanup_claimed
                    .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    // Cleanup the striped arc
                    unsafe {
                        drop(Box::from_raw(self.ptr as *mut StripedArcInner<T>));
                    }
                }
            }
        } else {
            // Decrement the reference count for the current stripe and determine if closed.
            if refcount.counters[stripe & refcount.mask].decrement() {
                for counter in refcount.counters.iter() {
                    // maybe a race with master dropping?
                    if !counter.is_closed() {
                        // No biggie, just nudge it along.
                        counter.close();
                    }
                }

                // SeqCst fence to ensure all decrements across all stripes are visible
                std::sync::atomic::fence(Ordering::SeqCst);

                let total = refcount.total();
                assert!(total >= 0);

                if total == 0 {
                    // Atomically claim cleanup to prevent double-free
                    if refcount
                        .cleanup_claimed
                        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        // Cleanup the striped arc
                        unsafe {
                            drop(Box::from_raw(self.ptr as *mut StripedArcInner<T>));
                        }
                    }
                }
            }
        }
    }
}

impl<T> Deref for StripedArc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner().data
    }
}

// Safety: StripedArc is Send/Sync if T is Send/Sync
unsafe impl<T: Send + Sync> Send for StripedArc<T> {}
unsafe impl<T: Send + Sync> Sync for StripedArc<T> {}

impl<T: std::fmt::Debug> std::fmt::Debug for StripedArc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

/// Get a stripe index for the current thread.
///
/// IMPORTANT: The returned value must be masked with `StripedRefCount::mask`
/// before accessing `StripedRefCount::counters`. This is because the hash
/// may produce values larger than the number of available stripes.
#[inline]
pub(crate) fn thread_stripe() -> usize {
    use std::hash::{Hash, Hasher};
    // Use thread-local cached stripe
    thread_local! {
        static STRIPE: usize = {
            let id = std::thread::current().id();
            let mut hasher = std::hash::DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish() as usize
        };
    }
    STRIPE.with(|s| *s)
}
