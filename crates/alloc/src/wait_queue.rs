//! Lock-free async wait queue for memory pressure notification.
//!
//! Zero allocation at wait time — all waiter slots are pre-allocated
//! from the arena at `Heap` construction time.
//!
//! When all slots are exhausted (too many concurrent waiters), callers
//! get an immediate error rather than silently spinning.

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bisque_mimalloc_sys as ffi;

// =========================================================================
// WakerSlot
// =========================================================================

const SLOT_EMPTY: usize = 0;
const SLOT_REGISTERED: usize = 1;
const SLOT_NOTIFIED: usize = 2;

#[repr(C)]
struct WakerSlot {
    waker: UnsafeCell<Option<Waker>>,
    state: AtomicUsize,
}

unsafe impl Send for WakerSlot {}
unsafe impl Sync for WakerSlot {}

impl WakerSlot {
    fn new() -> Self {
        Self {
            waker: UnsafeCell::new(None),
            state: AtomicUsize::new(SLOT_EMPTY),
        }
    }

    /// Try to claim this slot. Returns true if successfully claimed.
    #[inline]
    fn try_claim(&self) -> bool {
        self.state
            .compare_exchange(
                SLOT_EMPTY,
                SLOT_REGISTERED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Store/update a waker in a claimed slot.
    #[inline]
    fn store_waker(&self, waker: &Waker) {
        unsafe { *self.waker.get() = Some(waker.clone()) };
    }

    /// Wake the waiter and mark slot as notified (slot stays claimed).
    /// Returns true if a waker was present and woken.
    fn wake(&self) -> bool {
        // Transition REGISTERED → NOTIFIED
        if self
            .state
            .compare_exchange(
                SLOT_REGISTERED,
                SLOT_NOTIFIED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            if let Some(waker) = unsafe { (*self.waker.get()).take() } {
                waker.wake();
                return true;
            }
        }
        false
    }

    /// Reset slot from NOTIFIED back to REGISTERED for re-waiting.
    #[inline]
    #[allow(dead_code)]
    fn reset_for_reuse(&self) {
        self.state.store(SLOT_REGISTERED, Ordering::Release);
    }

    /// Release the slot entirely (back to EMPTY).
    fn release(&self) {
        unsafe { (*self.waker.get()).take() };
        self.state.store(SLOT_EMPTY, Ordering::Release);
    }
}

// =========================================================================
// WaitQueue
// =========================================================================

/// Async wait queue with pre-allocated waiter slots.
///
/// Slots are allocated from a `mi_heap_t` at construction time. When
/// all slots are exhausted, new waiters get an error rather than
/// silently degrading.
pub struct WaitQueue {
    /// Generation counter. Bumped on every notify. Waiters snapshot
    /// this to detect notifications that arrive between condition
    /// check and registration.
    generation: AtomicU64,
    /// Pre-allocated waiter slots.
    slots: *mut WakerSlot,
    /// Number of slots.
    slot_count: usize,
    /// Number of actively claimed slots (for `has_waiters`).
    active_count: AtomicUsize,
}

unsafe impl Send for WaitQueue {}
unsafe impl Sync for WaitQueue {}

impl WaitQueue {
    /// Allocate a wait queue with `slot_count` waiter slots from `mi_heap`.
    ///
    /// # Safety
    /// `mi_heap` must be a valid mimalloc heap pointer.
    pub(crate) unsafe fn new_in(slot_count: usize, mi_heap: *mut ffi::mi_heap_t) -> Self {
        let layout = std::alloc::Layout::array::<WakerSlot>(slot_count).unwrap();
        let raw = unsafe { ffi::mi_heap_malloc_aligned(mi_heap, layout.size(), layout.align()) };
        assert!(!raw.is_null(), "failed to allocate WaitQueue slots");
        let slots = raw as *mut WakerSlot;
        for i in 0..slot_count {
            unsafe { std::ptr::write(slots.add(i), WakerSlot::new()) };
        }
        Self {
            generation: AtomicU64::new(0),
            slots,
            slot_count,
            active_count: AtomicUsize::new(0),
        }
    }

    /// Create a wait queue with inline storage (for testing).
    /// Slots are NOT heap-allocated — caller must ensure lifetime.
    #[cfg(test)]
    pub fn new_inline(slot_count: usize) -> (Self, Vec<WakerSlot>) {
        let mut storage: Vec<WakerSlot> = (0..slot_count).map(|_| WakerSlot::new()).collect();
        let queue = Self {
            generation: AtomicU64::new(0),
            slots: storage.as_mut_ptr(),
            slot_count,
            active_count: AtomicUsize::new(0),
        };
        (queue, storage)
    }

    /// Try to acquire a waiter slot and return a future. Returns `None`
    /// if all slots are exhausted (too many concurrent waiters).
    ///
    /// The returned `WaitGuard` holds the slot for the duration of the
    /// wait. Call `guard.wait().await` to suspend until notified. After
    /// wakeup, the caller retries their operation. If it fails again,
    /// call `guard.wait().await` again — the same slot is reused.
    ///
    /// The slot is released when the `WaitGuard` is dropped.
    pub fn try_acquire(&self) -> Option<WaitGuard<'_>> {
        for i in 0..self.slot_count {
            let slot = unsafe { &*self.slots.add(i) };
            if slot.try_claim() {
                self.active_count.fetch_add(1, Ordering::Relaxed);
                return Some(WaitGuard {
                    queue: self,
                    slot_idx: i,
                    last_gen: self.generation.load(Ordering::Acquire),
                });
            }
        }
        None // all slots full
    }

    /// Wake one waiting task.
    pub fn notify_one(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        for i in 0..self.slot_count {
            let slot = unsafe { &*self.slots.add(i) };
            if slot.wake() {
                return;
            }
        }
    }

    /// Wake all waiting tasks.
    pub fn notify_all(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        for i in 0..self.slot_count {
            let slot = unsafe { &*self.slots.add(i) };
            slot.wake();
        }
    }

    /// Returns true if any tasks are currently waiting.
    #[inline]
    pub fn has_waiters(&self) -> bool {
        self.active_count.load(Ordering::Relaxed) > 0
    }

    /// Number of slots.
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.slot_count
    }

    /// Current generation (for diagnostics).
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

// WaitQueue slots are freed by mi_heap_destroy (bulk arena teardown).
// No individual drop needed.

// =========================================================================
// WaitGuard — holds a slot, can wait multiple times
// =========================================================================

/// A claimed waiter slot. Holds the slot until dropped.
///
/// Call [`wait()`](WaitGuard::wait) to get a future that suspends
/// until notified. After wakeup, retry the operation. If it fails,
/// call `wait()` again — the same slot is reused. No re-acquisition.
pub struct WaitGuard<'a> {
    queue: &'a WaitQueue,
    slot_idx: usize,
    /// Generation last seen by this guard. Initialized to current gen
    /// on acquire. Updated to current gen on each wait() call.
    last_gen: u64,
}

impl<'a> WaitGuard<'a> {
    /// Return a future that resolves when this slot is notified.
    ///
    /// Can be called multiple times — each call prepares for another
    /// wait cycle. If the slot was already notified (from a previous
    /// cycle), the future resolves immediately on first poll.
    pub fn wait(&mut self) -> WaitFuture<'_> {
        // Use the guard's last_gen as the snapshot — this detects
        // notifications that happened since the guard was acquired
        // or since the last wait() completed.
        let snapshot = self.last_gen;

        let slot = unsafe { &*self.queue.slots.add(self.slot_idx) };
        // Reset NOTIFIED → REGISTERED for re-waiting.
        let _ = slot.state.compare_exchange(
            SLOT_NOTIFIED,
            SLOT_REGISTERED,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );

        WaitFuture {
            queue: self.queue,
            slot_idx: self.slot_idx,
            snapshot,
            last_gen: &mut self.last_gen,
            registered: false,
        }
    }
}

impl<'a> Drop for WaitGuard<'a> {
    fn drop(&mut self) {
        let slot = unsafe { &*self.queue.slots.add(self.slot_idx) };
        slot.release();
        self.queue.active_count.fetch_sub(1, Ordering::Relaxed);
    }
}

// =========================================================================
// WaitFuture — stack-pinned, zero allocation
// =========================================================================

/// Future returned by [`WaitGuard::wait()`]. Stack-pinned, zero allocation.
pub struct WaitFuture<'a> {
    queue: &'a WaitQueue,
    slot_idx: usize,
    snapshot: u64,
    last_gen: &'a mut u64,
    registered: bool,
}

impl<'a> Future for WaitFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let slot = unsafe { &*this.queue.slots.add(this.slot_idx) };

        // Check if we were notified (state transitioned to NOTIFIED).
        if slot.state.load(Ordering::Acquire) == SLOT_NOTIFIED {
            // Update guard's last_gen so next wait() detects NEW notifications.
            *this.last_gen = this.queue.generation.load(Ordering::Acquire);
            return Poll::Ready(());
        }

        // Check if generation advanced since our snapshot.
        let current = this.queue.generation.load(Ordering::Acquire);
        if current != this.snapshot {
            *this.last_gen = current;
            return Poll::Ready(());
        }

        // Register/update our waker.
        slot.store_waker(cx.waker());
        slot.state.store(SLOT_REGISTERED, Ordering::Release);
        this.registered = true;

        // Double-check after registering — prevents lost wakeup.
        if slot.state.load(Ordering::Acquire) == SLOT_NOTIFIED {
            *this.last_gen = this.queue.generation.load(Ordering::Acquire);
            return Poll::Ready(());
        }
        let current = this.queue.generation.load(Ordering::Acquire);
        if current != this.snapshot {
            *this.last_gen = current;
            return Poll::Ready(());
        }

        Poll::Pending
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_queue(slots: usize) -> (WaitQueue, Vec<WakerSlot>) {
        WaitQueue::new_inline(slots)
    }

    #[test]
    fn basic_creation() {
        let (q, _s) = make_queue(16);
        assert!(!q.has_waiters());
        assert_eq!(q.slot_count(), 16);
        assert_eq!(q.generation(), 0);
    }

    #[test]
    fn notify_no_waiters() {
        let (q, _s) = make_queue(8);
        q.notify_one();
        assert_eq!(q.generation(), 1);
        q.notify_all();
        assert_eq!(q.generation(), 2);
    }

    #[test]
    fn try_acquire_exhaustion() {
        let (q, _s) = make_queue(2);
        let g1 = q.try_acquire();
        assert!(g1.is_some());
        let g2 = q.try_acquire();
        assert!(g2.is_some());
        // All slots taken.
        assert!(q.try_acquire().is_none());
        // Drop one → slot freed.
        drop(g1);
        assert!(q.try_acquire().is_some());
    }

    #[test]
    fn guard_drop_releases_slot() {
        let (q, _s) = make_queue(1);
        assert!(!q.has_waiters());
        {
            let _g = q.try_acquire().unwrap();
            assert!(q.has_waiters());
        }
        assert!(!q.has_waiters());
    }

    #[tokio::test]
    async fn wait_and_notify_one() {
        // Use Arc to share between tasks. We need the storage to live long enough.
        struct Shared {
            queue: WaitQueue,
            _storage: Vec<WakerSlot>,
        }
        unsafe impl Send for Shared {}
        unsafe impl Sync for Shared {}

        let (queue, storage) = make_queue(8);
        let shared = Arc::new(Shared {
            queue,
            _storage: storage,
        });
        let s2 = Arc::clone(&shared);

        let handle = tokio::spawn(async move {
            let mut guard = s2.queue.try_acquire().unwrap();
            guard.wait().await;
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(shared.queue.has_waiters());

        shared.queue.notify_one();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn wait_notify_retry_wait() {
        // Simulate: wait → notified → retry fails → wait again → notified → success
        struct Shared {
            queue: WaitQueue,
            _storage: Vec<WakerSlot>,
            value: AtomicUsize,
        }
        unsafe impl Send for Shared {}
        unsafe impl Sync for Shared {}

        let (queue, storage) = make_queue(8);
        let shared = Arc::new(Shared {
            queue,
            _storage: storage,
            value: AtomicUsize::new(0),
        });
        let s2 = Arc::clone(&shared);

        let handle = tokio::spawn(async move {
            let mut guard = s2.queue.try_acquire().unwrap();
            loop {
                guard.wait().await;
                let v = s2.value.load(Ordering::Acquire);
                if v >= 2 {
                    return v;
                }
                // Retry — not ready yet, wait again with same slot.
            }
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // First notify — value is 1, waiter retries.
        shared.value.store(1, Ordering::Release);
        shared.queue.notify_one();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Second notify — value is 2, waiter succeeds.
        shared.value.store(2, Ordering::Release);
        shared.queue.notify_one();

        let result = handle.await.unwrap();
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn notify_all_wakes_everyone() {
        struct Shared {
            queue: WaitQueue,
            _storage: Vec<WakerSlot>,
        }
        unsafe impl Send for Shared {}
        unsafe impl Sync for Shared {}

        let (queue, storage) = make_queue(16);
        let shared = Arc::new(Shared {
            queue,
            _storage: storage,
        });

        let mut handles = Vec::new();
        for _ in 0..4 {
            let s = Arc::clone(&shared);
            handles.push(tokio::spawn(async move {
                let mut guard = s.queue.try_acquire().unwrap();
                guard.wait().await;
            }));
        }

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        shared.queue.notify_all();

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn generation_prevents_lost_wakeup() {
        // Guard is acquired (last_gen=0). Then notify bumps gen to 1.
        // wait() uses last_gen=0 as snapshot, sees current gen=1, returns immediately.
        let (q, _s) = make_queue(4);
        let mut guard = q.try_acquire().unwrap();
        q.notify_one(); // bumps generation 0→1
        guard.wait().await; // snapshot=0, current=1 → immediate return
    }

    #[tokio::test]
    async fn multiple_notify_one_wakes_sequentially() {
        struct Shared {
            queue: WaitQueue,
            _storage: Vec<WakerSlot>,
        }
        unsafe impl Send for Shared {}
        unsafe impl Sync for Shared {}

        let (queue, storage) = make_queue(16);
        let shared = Arc::new(Shared {
            queue,
            _storage: storage,
        });
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..3 {
            let s = Arc::clone(&shared);
            let c = Arc::clone(&counter);
            handles.push(tokio::spawn(async move {
                let mut guard = s.queue.try_acquire().unwrap();
                guard.wait().await;
                c.fetch_add(1, Ordering::Relaxed);
            }));
        }

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Wake one at a time.
        shared.queue.notify_one();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        shared.queue.notify_one();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(counter.load(Ordering::Relaxed), 2);

        shared.queue.notify_one();
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }
}
