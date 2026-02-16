//! An efficient async condition variable for lock-free algorithms, a.k.a.
//! "eventcount".
//!
//! [Eventcount][eventcount]-like primitives are useful to make some operations
//! on a lock-free structure blocking, for instance to transform bounded queues
//! into bounded channels. Such a primitive allows an interested task to block
//! until a predicate is satisfied by checking the predicate each time it
//! receives a notification.
//!
//! ## Optimizations
//!
//! This implementation includes several performance optimizations:
//!
//! 1. **Inline notifier storage**: The `Notifier` is stored inline in the
//!    `WaitUntil` future, eliminating heap allocations in the hot path.
//!
//! 2. **Lock-free empty check**: `notify_one`/`notify_all` return immediately
//!    with just an atomic load when no waiters are present (common case).
//!
//! 3. **Vec-based FIFO wait list**: Uses a Vec with O(1) swap-remove for good
//!    cache locality and FIFO-ish notification order.
//!
//! 4. **Batched wakeups**: Wakers are collected and invoked outside the
//!    lock to minimize lock hold time.
//!
//! 5. **Optimized mutex**: Uses `parking_lot::Mutex` for smaller size
//!    and better performance than std::sync::Mutex.
//!
//! [eventcount]:
//!     https://www.1024cores.net/home/lock-free-algorithms/eventcounts
//!
//! # Examples
//!
//! Wait until a non-zero value has been sent asynchronously.
//!
//! ```ignore
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use std::sync::Arc;
//! use std::thread;
//!
//! use futures_executor::block_on;
//!
//! use async_event::Event;
//!
//!
//! let value = Arc::new(AtomicUsize::new(0));
//! let event = Arc::new(Event::new());
//!
//! // Set a non-zero value concurrently.
//! thread::spawn({
//!     let value = value.clone();
//!     let event = event.clone();
//!
//!     move || {
//!         // A relaxed store is sufficient here: `Event::notify*` methods insert
//!         // atomic fences to warrant adequate synchronization.
//!         value.store(42, Ordering::Relaxed);
//!         event.notify_one();
//!     }
//! });
//!
//! // Wait until the value is set.
//! block_on(async move {
//!     let v = event
//!         .wait_until(|| {
//!             // A relaxed load is sufficient here: `Event::wait_until` inserts
//!             // atomic fences to warrant adequate synchronization.
//!             let v = value.load(Ordering::Relaxed);
//!             if v != 0 { Some(v) } else { None }
//!         })
//!         .await;
//!
//!      assert_eq!(v, 42);
//! });
//! ```
#![warn(missing_docs, missing_debug_implementations, unreachable_pub)]

mod loom_exports;

use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use loom_exports::sync::atomic::{self, AtomicBool};
use parking_lot::Mutex;
use pin_project_lite::pin_project;

/// Maximum number of wakers to batch before waking them outside the lock.
const MAX_BATCH_WAKEUPS: usize = 16;

/// Sentinel value indicating the notifier is not in the wait list.
const NOT_IN_LIST: usize = usize::MAX;

/// An object that can receive or send notifications.
#[derive(Debug)]
pub struct Event {
    wait_set: WaitSet,
}

impl Event {
    /// Creates a new event.
    pub const fn new() -> Self {
        Self {
            wait_set: WaitSet::new(),
        }
    }

    /// Notify a number of awaiting events that the predicate should be checked.
    ///
    /// If fewer events than requested are currently awaiting, then all awaiting
    /// events are notified.
    #[inline(always)]
    pub fn notify(&self, n: usize) {
        // This fence synchronizes with the other fence in `WaitUntil::poll` and
        // ensures that either the `poll` method will successfully check the
        // predicate set before this call, or the notifier inserted by `poll`
        // will be visible in the wait list when calling `WaitSet::notify` (or
        // both).
        atomic::fence(Ordering::SeqCst);

        // LOCK-FREE FAST PATH: Check if empty without any locking
        // This is the common case - notify when no one is waiting
        if self.wait_set.is_empty.load(Ordering::Acquire) {
            return;
        }

        // Slow path: there are waiters, need to take lock
        // Safety: all notifiers in the wait set are guaranteed to be alive
        // since the `WaitUntil` drop handler ensures that notifiers are removed
        // from the wait set before they are deallocated.
        unsafe {
            self.wait_set.notify(n);
        }
    }

    /// Notify one awaiting event (if any) that the predicate should be checked.
    #[inline(always)]
    pub fn notify_one(&self) {
        self.notify(1);
    }

    /// Notify all awaiting events that the predicate should be checked.
    #[inline(always)]
    pub fn notify_all(&self) {
        self.notify(usize::MAX);
    }

    /// Returns a future that can be `await`ed until the provided predicate is
    /// satisfied.
    pub fn wait_until<F, T>(&self, predicate: F) -> WaitUntil<'_, F, T>
    where
        F: FnMut() -> Option<T>,
    {
        WaitUntil::new(&self.wait_set, predicate)
    }

    /// Returns a future that can be `await`ed until the provided predicate is
    /// satisfied or until the provided future completes.
    ///
    /// The deadline is specified as a `Future` that is expected to resolve to
    /// `()` after some duration, such as a `tokio::time::Sleep` future.
    pub fn wait_until_or_timeout<F, T, D>(
        &self,
        predicate: F,
        deadline: D,
    ) -> WaitUntilOrTimeout<'_, F, T, D>
    where
        F: FnMut() -> Option<T>,
        D: Future<Output = ()>,
    {
        WaitUntilOrTimeout::new(&self.wait_set, predicate, deadline)
    }
}

impl Default for Event {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for Event {}
unsafe impl Sync for Event {}

/// A waker wrapper that can be inserted in a Vec-based wait list.
///
/// This structure is designed to be stored inline in the `WaitUntil` future
/// to avoid heap allocations. Each notifier tracks its index in the wait list
/// for O(1) removal via swap-remove.
#[repr(C)]
struct Notifier {
    /// The current waker, if any. Protected by the WaitSet mutex.
    waker: UnsafeCell<Option<Waker>>,
    /// Index in the wait list Vec, or NOT_IN_LIST if not in list.
    index: UnsafeCell<usize>,
}

impl Notifier {
    /// Creates a new Notifier without any registered waker.
    const fn new() -> Self {
        Self {
            waker: UnsafeCell::new(None),
            index: UnsafeCell::new(NOT_IN_LIST),
        }
    }

    /// Stores the specified waker if it differs from the cached waker.
    ///
    /// # Safety
    ///
    /// The caller must have exclusive access (not in wait set, or holding lock).
    #[inline]
    unsafe fn set_waker(&self, waker: &Waker) {
        let waker_ptr = self.waker.get();
        let current = unsafe { &*waker_ptr };
        if match current {
            Some(w) => !w.will_wake(waker),
            None => true,
        } {
            unsafe { *waker_ptr = Some(waker.clone()) };
        }
    }

    /// Clones the waker if present.
    ///
    /// # Safety
    ///
    /// The caller must hold the WaitSet lock.
    #[inline]
    unsafe fn clone_waker(&self) -> Option<Waker> {
        unsafe { (*self.waker.get()).clone() }
    }

    /// Gets the current index.
    #[inline]
    unsafe fn get_index(&self) -> usize {
        unsafe { *self.index.get() }
    }

    /// Sets the index.
    #[inline]
    unsafe fn set_index(&self, index: usize) {
        unsafe { *self.index.get() = index };
    }

    /// Returns true if the notifier is in the wait list.
    #[inline]
    fn is_in_list(&self) -> bool {
        unsafe { *self.index.get() != NOT_IN_LIST }
    }
}

impl std::fmt::Debug for Notifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notifier")
            .field("index", &unsafe { *self.index.get() })
            .finish()
    }
}

unsafe impl Send for Notifier {}
unsafe impl Sync for Notifier {}

/// A future that can be `await`ed until a predicate is satisfied.
///
/// This future stores the notifier inline to avoid heap allocations.
/// It must be pinned before polling because the notifier's address is
/// used in the wait list.
pub struct WaitUntil<'a, F: FnMut() -> Option<T>, T> {
    /// The notifier stored inline. This MUST remain at a stable address
    /// once the future is pinned, as pointers to it are stored in the wait set.
    notifier: Notifier,
    /// Current state of the future.
    state: WaitUntilState,
    /// The predicate to check.
    predicate: F,
    /// Reference to the wait set.
    wait_set: &'a WaitSet,
    /// Marker to make this type !Unpin, ensuring it stays pinned.
    _pin: PhantomPinned,
}

impl<'a, F: FnMut() -> Option<T>, T> WaitUntil<'a, F, T> {
    /// Creates a future associated with the specified event sink that can be
    /// `await`ed until the specified predicate is satisfied.
    fn new(wait_set: &'a WaitSet, predicate: F) -> Self {
        Self {
            notifier: Notifier::new(),
            state: WaitUntilState::Idle,
            predicate,
            wait_set,
            _pin: PhantomPinned,
        }
    }
}

impl<F: FnMut() -> Option<T>, T> Drop for WaitUntil<'_, F, T> {
    fn drop(&mut self) {
        if self.state == WaitUntilState::Polled {
            // If we are in the `Polled` state, it means that the future was
            // cancelled and its notifier may still be in the wait set.
            // We need to cancel it so another waiter can be notified.
            let notifier_ptr = NonNull::from(&self.notifier);
            unsafe {
                self.wait_set.cancel(notifier_ptr);
            }
        }
    }
}

impl<F: FnMut() -> Option<T>, T> std::fmt::Debug for WaitUntil<'_, F, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitUntil")
            .field("state", &self.state)
            .field("notifier", &self.notifier)
            .finish()
    }
}

unsafe impl<F: (FnMut() -> Option<T>) + Send, T: Send> Send for WaitUntil<'_, F, T> {}

impl<'a, F: FnMut() -> Option<T>, T> Future for WaitUntil<'a, F, T> {
    type Output = T;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: we're not moving out of self, just getting mutable references
        // to fields. The notifier address remains stable.
        let this = unsafe { self.get_unchecked_mut() };

        debug_assert!(this.state != WaitUntilState::Completed);

        // Get a stable pointer to our inline notifier
        let notifier_ptr = NonNull::from(&this.notifier);

        // Remove the notifier if it is in the wait set. In most cases this will
        // be a cheap no-op because, unless the wake-up is spurious, the
        // notifier was already removed from the wait set.
        if this.state == WaitUntilState::Polled {
            unsafe { this.wait_set.remove_relaxed(notifier_ptr) };
        }

        // Fast path: check predicate before any synchronization.
        if let Some(v) = (this.predicate)() {
            this.state = WaitUntilState::Completed;
            return Poll::Ready(v);
        }

        // Set or update the waker.
        let waker = cx.waker();
        unsafe { this.notifier.set_waker(waker) };

        // Insert into the wait set.
        unsafe { this.wait_set.insert(notifier_ptr) };

        // This fence synchronizes with the other fence in `Event::notify`.
        atomic::fence(Ordering::SeqCst);

        if let Some(v) = (this.predicate)() {
            // We need to cancel and not merely remove the notifier from the
            // wait set so that another event sink can be notified.
            unsafe {
                this.wait_set.cancel(notifier_ptr);
            }

            this.state = WaitUntilState::Completed;
            return Poll::Ready(v);
        }

        this.state = WaitUntilState::Polled;
        Poll::Pending
    }
}

/// State of the `WaitUntil` future.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitUntilState {
    /// Initial state, never polled.
    Idle,
    /// Has been polled and is waiting.
    Polled,
    /// Completed successfully.
    Completed,
}

pin_project! {
    /// A future that can be `await`ed until a predicate is satisfied or until a
    /// deadline elapses.
    pub struct WaitUntilOrTimeout<'a, F: FnMut() -> Option<T>, T, D: Future<Output = ()>> {
        #[pin]
        wait_until: WaitUntil<'a, F, T>,
        #[pin]
        deadline: D,
    }
}

impl<'a, F, T, D> WaitUntilOrTimeout<'a, F, T, D>
where
    F: FnMut() -> Option<T>,
    D: Future<Output = ()>,
{
    /// Creates a future associated with the specified event sink that can be
    /// `await`ed until the specified predicate is satisfied, or until the
    /// specified timeout future completes.
    fn new(wait_set: &'a WaitSet, predicate: F, deadline: D) -> Self {
        Self {
            wait_until: WaitUntil::new(wait_set, predicate),
            deadline,
        }
    }
}

impl<F: FnMut() -> Option<T>, T, D: Future<Output = ()>> std::fmt::Debug
    for WaitUntilOrTimeout<'_, F, T, D>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitUntilOrTimeout").finish_non_exhaustive()
    }
}

impl<'a, F, T, D> Future for WaitUntilOrTimeout<'a, F, T, D>
where
    F: FnMut() -> Option<T>,
    D: Future<Output = ()>,
{
    type Output = Option<T>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Poll::Ready(value) = this.wait_until.poll(cx) {
            Poll::Ready(Some(value))
        } else if this.deadline.poll(cx).is_ready() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// A set of notifiers using a Vec with O(1) swap-remove.
///
/// The Vec stores pointers to inline notifiers. Each notifier tracks its
/// own index, which is updated during swap-remove operations. This gives
/// us O(1) removal while maintaining good cache locality.
struct WaitSet {
    /// Mutex protecting the wait list.
    waiters: Mutex<Vec<NonNull<Notifier>>>,
    /// Lock-free emptiness check. This is the key optimization:
    /// notify_one/notify_all can return immediately with just an atomic load
    /// when no waiters are present (the common case).
    is_empty: AtomicBool,
}

impl WaitSet {
    /// Creates a new empty wait set.
    const fn new() -> Self {
        Self {
            waiters: Mutex::new(Vec::new()),
            is_empty: AtomicBool::new(true),
        }
    }

    /// Inserts a notifier into the wait set.
    ///
    /// # Safety
    ///
    /// The notifier must remain valid until it is removed from the wait set.
    /// The notifier should not already be in the wait set.
    #[inline]
    unsafe fn insert(&self, notifier: NonNull<Notifier>) {
        let mut waiters = self.waiters.lock();

        debug_assert!(!unsafe { notifier.as_ref().is_in_list() });

        // Record the index where we're inserting
        let index = waiters.len();
        unsafe { notifier.as_ref().set_index(index) };

        // Add to the Vec
        waiters.push(notifier);

        // Update emptiness flag
        self.is_empty.store(false, Ordering::Release);
    }

    /// Remove the specified notifier if it is still in the wait set.
    ///
    /// Uses a fast-path check without locking when possible.
    ///
    /// # Safety
    ///
    /// The notifier must be valid.
    #[inline]
    unsafe fn remove_relaxed(&self, notifier: NonNull<Notifier>) {
        // Fast path: check if already removed (no lock needed)
        if !unsafe { notifier.as_ref().is_in_list() } {
            return;
        }

        unsafe { self.remove(notifier) };
    }

    /// Remove the specified notifier from the wait set using swap-remove.
    ///
    /// # Safety
    ///
    /// The notifier must be valid.
    unsafe fn remove(&self, notifier: NonNull<Notifier>) {
        let mut waiters = self.waiters.lock();

        let notifier_ref = unsafe { notifier.as_ref() };

        // Check again if still in list (may have been removed between check and lock)
        let index = unsafe { notifier_ref.get_index() };
        if index == NOT_IN_LIST {
            return;
        }

        // Swap-remove: move the last element to this position
        let last_index = waiters.len() - 1;
        if index != last_index {
            // Update the moved notifier's index
            let moved = waiters[last_index];
            unsafe { moved.as_ref().set_index(index) };
            waiters[index] = moved;
        }
        waiters.pop();

        // Mark as removed
        unsafe { notifier_ref.set_index(NOT_IN_LIST) };

        // Update emptiness flag
        if waiters.is_empty() {
            self.is_empty.store(true, Ordering::Release);
        }
    }

    /// Remove the specified notifier if still in wait set, or notify another.
    ///
    /// # Safety
    ///
    /// The notifier and all notifiers in the wait set must be valid.
    unsafe fn cancel(&self, notifier: NonNull<Notifier>) {
        let mut waiters = self.waiters.lock();

        let notifier_ref = unsafe { notifier.as_ref() };
        let index = unsafe { notifier_ref.get_index() };

        if index != NOT_IN_LIST {
            // Still in list, remove it using swap-remove
            let last_index = waiters.len() - 1;
            if index != last_index {
                let moved = waiters[last_index];
                unsafe { moved.as_ref().set_index(index) };
                waiters[index] = moved;
            }
            waiters.pop();

            unsafe { notifier_ref.set_index(NOT_IN_LIST) };

            if waiters.is_empty() {
                self.is_empty.store(true, Ordering::Release);
            }
        } else if !waiters.is_empty() {
            // We were already notified (removed by notify), pass notification to another waiter
            // Pop from the front for FIFO behavior
            let other = waiters[0];

            // Swap-remove from front
            let last_index = waiters.len() - 1;
            if last_index > 0 {
                let moved = waiters[last_index];
                unsafe { moved.as_ref().set_index(0) };
                waiters[0] = moved;
            }
            waiters.pop();

            // Clone waker before marking as removed
            let waker = unsafe { other.as_ref().clone_waker() };

            // Mark as removed
            unsafe { other.as_ref().set_index(NOT_IN_LIST) };

            if waiters.is_empty() {
                self.is_empty.store(true, Ordering::Release);
            }

            // Release lock before waking
            drop(waiters);

            // Wake the other waiter
            if let Some(w) = waker {
                w.wake();
            }
        }
    }

    /// Send notifications to up to `count` waiters.
    ///
    /// Note: The caller has already checked is_empty, so we know there are waiters.
    ///
    /// # Safety
    ///
    /// All notifiers in the wait set must be valid.
    unsafe fn notify(&self, count: usize) {
        // Collect wakers to wake outside the lock
        let mut wakers_to_wake: [MaybeUninit<Waker>; MAX_BATCH_WAKEUPS] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let mut waker_count = 0usize;
        let mut remaining = count;

        {
            let mut waiters = self.waiters.lock();

            while remaining > 0 && !waiters.is_empty() {
                // Pop from front for FIFO behavior using swap-remove
                let notifier = waiters[0];

                let last_index = waiters.len() - 1;
                if last_index > 0 {
                    let moved = waiters[last_index];
                    unsafe { moved.as_ref().set_index(0) };
                    waiters[0] = moved;
                }
                waiters.pop();

                let notifier_ref = unsafe { notifier.as_ref() };

                // Clone the waker while we have access
                if let Some(waker) = unsafe { notifier_ref.clone_waker() } {
                    if waker_count < MAX_BATCH_WAKEUPS {
                        wakers_to_wake[waker_count].write(waker);
                        waker_count += 1;
                    } else {
                        // Batch is full, wake immediately
                        waker.wake();
                    }
                }

                // Mark as removed
                unsafe { notifier_ref.set_index(NOT_IN_LIST) };

                remaining -= 1;
            }

            // Update emptiness flag
            if waiters.is_empty() {
                self.is_empty.store(true, Ordering::Release);
            }
        }

        // Wake all collected wakers outside the lock
        for i in 0..waker_count {
            let waker = unsafe { wakers_to_wake[i].assume_init_read() };
            waker.wake();
        }
    }
}

impl std::fmt::Debug for WaitSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitSet")
            .field("is_empty", &self.is_empty.load(Ordering::Relaxed))
            .finish()
    }
}

impl Default for WaitSet {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: WaitSet uses proper synchronization (atomics + mutex)
unsafe impl Send for WaitSet {}
unsafe impl Sync for WaitSet {}

/// Non-loom tests.
#[cfg(all(test, not(async_event_loom)))]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;

    use crate::future::block_on;

    #[test]
    fn smoke() {
        static SIGNAL: AtomicBool = AtomicBool::new(false);

        let event = Arc::new(Event::new());

        let th_recv = {
            let event = event.clone();
            thread::spawn(move || {
                block_on(async move {
                    event
                        .wait_until(|| {
                            if SIGNAL.load(Ordering::Relaxed) {
                                Some(())
                            } else {
                                None
                            }
                        })
                        .await;

                    assert!(SIGNAL.load(Ordering::Relaxed));
                })
            })
        };

        SIGNAL.store(true, Ordering::Relaxed);
        event.notify_one();

        th_recv.join().unwrap();
    }

    #[test]
    fn predicate_already_satisfied() {
        static SIGNAL: AtomicBool = AtomicBool::new(true);

        let event = Event::new();

        block_on(async {
            let result = event
                .wait_until(|| {
                    if SIGNAL.load(Ordering::Relaxed) {
                        Some(42)
                    } else {
                        None
                    }
                })
                .await;

            assert_eq!(result, 42);
        });
    }

    #[test]
    fn one_to_many() {
        const NUM_RECV: usize = 4;

        static SIGNAL: AtomicBool = AtomicBool::new(false);

        let event = Arc::new(Event::new());
        let barrier = Arc::new(std::sync::Barrier::new(NUM_RECV + 1));

        let th_recv: Vec<_> = (0..NUM_RECV)
            .map(|_| {
                let event = event.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    block_on(async move {
                        barrier.wait();
                        event
                            .wait_until(|| {
                                if SIGNAL.load(Ordering::Relaxed) {
                                    Some(())
                                } else {
                                    None
                                }
                            })
                            .await;

                        assert!(SIGNAL.load(Ordering::Relaxed));
                    })
                })
            })
            .collect();

        barrier.wait();

        thread::sleep(Duration::from_millis(10));
        SIGNAL.store(true, Ordering::Relaxed);
        event.notify_all();

        for th in th_recv {
            th.join().unwrap();
        }
    }

    #[test]
    fn many_to_many() {
        const NUM_SEND: usize = 4;
        const NUM_RECV: usize = 4;
        const MSG_PER_SENDER: usize = 10;

        let received = Arc::new(AtomicUsize::new(0));
        let sent = Arc::new(AtomicUsize::new(0));
        let event = Arc::new(Event::new());

        let th_recv: Vec<_> = (0..NUM_RECV)
            .map(|_| {
                let received = received.clone();
                let sent = sent.clone();
                let event = event.clone();
                thread::spawn(move || {
                    block_on(async move {
                        loop {
                            let r = event
                                .wait_until(|| {
                                    let r = received.load(Ordering::Relaxed);
                                    let s = sent.load(Ordering::Relaxed);

                                    if r < s {
                                        Some(r)
                                    } else if s >= NUM_SEND * MSG_PER_SENDER {
                                        Some(usize::MAX)
                                    } else {
                                        None
                                    }
                                })
                                .await;

                            if r == usize::MAX {
                                break;
                            }

                            received.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                })
            })
            .collect();

        let th_send: Vec<_> = (0..NUM_SEND)
            .map(|_| {
                let sent = sent.clone();
                let event = event.clone();
                thread::spawn(move || {
                    for _ in 0..MSG_PER_SENDER {
                        sent.fetch_add(1, Ordering::Relaxed);
                        event.notify_one();
                        thread::sleep(Duration::from_micros(100));
                    }
                })
            })
            .collect();

        for th in th_send {
            th.join().unwrap();
        }
        // Notify all receivers that we're done.
        event.notify_all();

        for th in th_recv {
            th.join().unwrap();
        }

        assert_eq!(received.load(Ordering::Relaxed), NUM_SEND * MSG_PER_SENDER);
    }

    #[test]
    fn cancellation() {
        // Test that when a waiter is cancelled, another waiter is notified.
        // This tests the cancel() method's forwarding behavior.
        let event = Arc::new(Event::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(3));

        // Waiter 1: will be cancelled after being notified
        let event1 = event.clone();
        let counter1 = counter.clone();
        let completed1 = completed.clone();
        let barrier1 = barrier.clone();
        let th1 = thread::spawn(move || {
            block_on(async move {
                // Create a future that will never satisfy the predicate
                let fut = event1.wait_until(|| {
                    // This waiter looks for counter == 999 which never happens
                    let c = counter1.load(Ordering::Relaxed);
                    if c == 999 { Some(c) } else { None }
                });
                let mut fut = std::pin::pin!(fut);

                // Poll once to register
                let waker = Waker::noop();
                let mut cx = Context::from_waker(&waker);
                let _ = fut.as_mut().poll(&mut cx);

                barrier1.wait(); // Signal we're registered

                // Wait a bit then drop the future (cancel)
                thread::sleep(Duration::from_millis(30));
                drop(fut);
                completed1.fetch_add(1, Ordering::Relaxed);
            })
        });

        // Waiter 2: should eventually complete when waiter 1 cancels
        let event2 = event.clone();
        let counter2 = counter.clone();
        let completed2 = completed.clone();
        let barrier2 = barrier.clone();
        let th2 = thread::spawn(move || {
            block_on(async move {
                barrier2.wait(); // Wait for waiter 1 to register first

                event2
                    .wait_until(|| {
                        let c = counter2.load(Ordering::Relaxed);
                        if c > 0 { Some(c) } else { None }
                    })
                    .await;

                completed2.fetch_add(1, Ordering::Relaxed);
            })
        });

        barrier.wait(); // Wait for waiter 1 to be registered

        // Set signal and notify once
        counter.store(1, Ordering::Relaxed);
        event.notify_one();

        // Waiter 1 gets notified but predicate fails, then drops (cancel)
        // Cancel should forward notification to waiter 2

        th1.join().unwrap();
        th2.join().unwrap();

        // Both should have completed
        assert_eq!(completed.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn notify_all() {
        const NUM_WAITERS: usize = 5;
        static SIGNAL: AtomicBool = AtomicBool::new(false);

        let event = Arc::new(Event::new());
        let woken = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..NUM_WAITERS)
            .map(|_| {
                let event = event.clone();
                let woken = woken.clone();
                thread::spawn(move || {
                    block_on(async move {
                        event
                            .wait_until(|| {
                                if SIGNAL.load(Ordering::Relaxed) {
                                    Some(())
                                } else {
                                    None
                                }
                            })
                            .await;
                        woken.fetch_add(1, Ordering::Relaxed);
                    })
                })
            })
            .collect();

        // Wait for all to be waiting.
        thread::sleep(Duration::from_millis(20));

        // Notify all at once.
        SIGNAL.store(true, Ordering::Relaxed);
        event.notify_all();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(woken.load(Ordering::Relaxed), NUM_WAITERS);
    }

    #[test]
    fn rapid_notify_before_wait() {
        // Test notifying before anyone is waiting.
        let event = Event::new();

        // These should be no-ops.
        event.notify_one();
        event.notify_all();
        event.notify(100);

        // Now do a normal wait/notify cycle.
        static SIGNAL: AtomicBool = AtomicBool::new(false);

        let event = Arc::new(Event::new());
        let event2 = event.clone();

        let th = thread::spawn(move || {
            block_on(async move {
                event2
                    .wait_until(|| {
                        if SIGNAL.load(Ordering::Relaxed) {
                            Some(42)
                        } else {
                            None
                        }
                    })
                    .await
            })
        });

        thread::sleep(Duration::from_millis(10));
        SIGNAL.store(true, Ordering::Relaxed);
        event.notify_one();

        assert_eq!(th.join().unwrap(), 42);
    }

    #[test]
    fn test_timeout() {
        let event = Event::new();
        static SIGNAL: AtomicBool = AtomicBool::new(false);

        block_on(async {
            let deadline = async {
                std::thread::sleep(Duration::from_millis(10));
            };

            let result = event
                .wait_until_or_timeout(
                    || {
                        if SIGNAL.load(Ordering::Relaxed) {
                            Some(42)
                        } else {
                            None
                        }
                    },
                    deadline,
                )
                .await;

            // Should timeout since SIGNAL is never set.
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_inline_notifier_no_allocation() {
        // This test verifies that the notifier is stored inline.
        // We can't directly measure allocations, but we can verify
        // the size is reasonable.
        let size = std::mem::size_of::<WaitUntil<'_, fn() -> Option<()>, ()>>();
        // Should be small-ish (notifier + state + predicate pointer + wait_set reference)
        assert!(size < 128, "WaitUntil is too large: {} bytes", size);
    }

    #[test]
    fn stress_test_concurrent_notify_and_wait() {
        const NUM_WAITERS: usize = 8;
        const NUM_NOTIFIERS: usize = 4;
        const ITERATIONS: usize = 100;

        for _ in 0..ITERATIONS {
            let counter = Arc::new(AtomicUsize::new(0));
            let event = Arc::new(Event::new());
            let completed = Arc::new(AtomicUsize::new(0));

            let waiter_handles: Vec<_> = (0..NUM_WAITERS)
                .map(|_| {
                    let event = event.clone();
                    let counter = counter.clone();
                    let completed = completed.clone();

                    thread::spawn(move || {
                        block_on(async move {
                            event
                                .wait_until(|| {
                                    let c = counter.load(Ordering::Relaxed);
                                    if c > 0 { Some(c) } else { None }
                                })
                                .await;
                            completed.fetch_add(1, Ordering::Relaxed);
                        })
                    })
                })
                .collect();

            thread::sleep(Duration::from_millis(5));

            let notifier_handles: Vec<_> = (0..NUM_NOTIFIERS)
                .map(|_| {
                    let event = event.clone();
                    let counter = counter.clone();
                    thread::spawn(move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                        event.notify_all();
                    })
                })
                .collect();

            for h in notifier_handles {
                h.join().unwrap();
            }
            for h in waiter_handles {
                h.join().unwrap();
            }

            assert_eq!(completed.load(Ordering::Relaxed), NUM_WAITERS);
        }
    }

    #[test]
    fn test_spurious_wakeup_handling() {
        let event = Arc::new(Event::new());
        let counter = Arc::new(AtomicUsize::new(0));

        let event2 = event.clone();
        let counter2 = counter.clone();

        let handle = thread::spawn(move || {
            block_on(async move {
                event2
                    .wait_until(|| {
                        let c = counter2.load(Ordering::Relaxed);
                        if c >= 3 { Some(c) } else { None }
                    })
                    .await
            })
        });

        thread::sleep(Duration::from_millis(10));

        // Simulate spurious wakeups by notifying without satisfying predicate.
        counter.store(1, Ordering::Relaxed);
        event.notify_one();
        thread::sleep(Duration::from_millis(5));

        counter.store(2, Ordering::Relaxed);
        event.notify_one();
        thread::sleep(Duration::from_millis(5));

        // Now satisfy the predicate.
        counter.store(3, Ordering::Relaxed);
        event.notify_one();

        let result = handle.join().unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn test_predicate_side_effects() {
        let event = Arc::new(Event::new());
        let call_count = Arc::new(AtomicUsize::new(0));

        let event2 = event.clone();
        let call_count2 = call_count.clone();

        let handle = thread::spawn(move || {
            block_on(async move {
                event2
                    .wait_until(|| {
                        let count = call_count2.fetch_add(1, Ordering::Relaxed);
                        if count >= 2 { Some(count) } else { None }
                    })
                    .await
            })
        });

        thread::sleep(Duration::from_millis(10));
        event.notify_one();
        thread::sleep(Duration::from_millis(5));
        event.notify_one();

        let result = handle.join().unwrap();
        assert!(result >= 2);
    }

    #[test]
    fn test_multiple_events_independent() {
        let event1 = Arc::new(Event::new());
        let event2 = Arc::new(Event::new());
        static SIGNAL1: AtomicBool = AtomicBool::new(false);
        static SIGNAL2: AtomicBool = AtomicBool::new(false);

        let e1 = event1.clone();
        let h1 = thread::spawn(move || {
            block_on(async move {
                e1.wait_until(|| {
                    if SIGNAL1.load(Ordering::Relaxed) {
                        Some(1)
                    } else {
                        None
                    }
                })
                .await
            })
        });

        let e2 = event2.clone();
        let h2 = thread::spawn(move || {
            block_on(async move {
                e2.wait_until(|| {
                    if SIGNAL2.load(Ordering::Relaxed) {
                        Some(2)
                    } else {
                        None
                    }
                })
                .await
            })
        });

        thread::sleep(Duration::from_millis(10));

        // Notify event2 first.
        SIGNAL2.store(true, Ordering::Relaxed);
        event2.notify_one();

        assert_eq!(h2.join().unwrap(), 2);

        // h1 should still be waiting.
        thread::sleep(Duration::from_millis(5));

        // Now notify event1.
        SIGNAL1.store(true, Ordering::Relaxed);
        event1.notify_one();

        assert_eq!(h1.join().unwrap(), 1);
    }

    #[test]
    fn test_notify_more_than_waiters() {
        let event = Arc::new(Event::new());
        static SIGNAL: AtomicBool = AtomicBool::new(false);

        let e = event.clone();
        let h = thread::spawn(move || {
            block_on(async move {
                e.wait_until(|| {
                    if SIGNAL.load(Ordering::Relaxed) {
                        Some(42)
                    } else {
                        None
                    }
                })
                .await
            })
        });

        thread::sleep(Duration::from_millis(10));

        SIGNAL.store(true, Ordering::Relaxed);
        // Notify way more than there are waiters.
        event.notify(1000);

        assert_eq!(h.join().unwrap(), 42);
    }

    #[test]
    fn test_value_propagation() {
        let event = Arc::new(Event::new());
        let values = Arc::new(std::sync::Mutex::new(Vec::new()));

        let e = event.clone();
        let v = values.clone();
        let h = thread::spawn(move || {
            block_on(async move {
                e.wait_until(|| {
                    let vals = v.lock().unwrap();
                    if vals.len() >= 3 {
                        Some(vals.iter().sum::<i32>())
                    } else {
                        None
                    }
                })
                .await
            })
        });

        thread::sleep(Duration::from_millis(10));

        values.lock().unwrap().push(1);
        event.notify_one();
        thread::sleep(Duration::from_millis(5));

        values.lock().unwrap().push(2);
        event.notify_one();
        thread::sleep(Duration::from_millis(5));

        values.lock().unwrap().push(3);
        event.notify_one();

        let result = h.join().unwrap();
        assert_eq!(result, 6); // 1 + 2 + 3
    }

    #[test]
    fn test_repeated_wait_same_event() {
        let event = Arc::new(Event::new());

        for expected in 1..=5 {
            let counter = Arc::new(AtomicUsize::new(0));

            let e = event.clone();
            let c = counter.clone();
            let handle = thread::spawn(move || {
                block_on(async move {
                    e.wait_until(|| {
                        let val = c.load(Ordering::Relaxed);
                        if val >= expected { Some(val) } else { None }
                    })
                    .await
                })
            });

            thread::sleep(Duration::from_millis(10));

            counter.store(expected, Ordering::Relaxed);
            event.notify_one();

            let result = handle.join().unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_swap_remove_correctness() {
        // Test that swap-remove doesn't corrupt the wait list when waiters
        // wake in unpredictable order based on their predicates
        let event = Arc::new(Event::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        const NUM_WAITERS: usize = 10;

        let handles: Vec<_> = (0..NUM_WAITERS)
            .map(|i| {
                let event = event.clone();
                let counter = counter.clone();
                let completed = completed.clone();
                thread::spawn(move || {
                    block_on(async move {
                        event
                            .wait_until(|| {
                                let c = counter.load(Ordering::Relaxed);
                                if c > i { Some(i) } else { None }
                            })
                            .await;
                        completed.fetch_add(1, Ordering::Relaxed);
                    })
                })
            })
            .collect();

        thread::sleep(Duration::from_millis(20));

        // Use notify_all since waiter conditions depend on counter value,
        // not on which waiter gets notified. Each increment satisfies one more waiter.
        for i in 1..=NUM_WAITERS {
            counter.store(i, Ordering::Relaxed);
            event.notify_all();
            // Wait for the i-th waiter to complete
            while completed.load(Ordering::Relaxed) < i {
                thread::yield_now();
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_middle_removal() {
        // Test removing from the middle of the wait list
        let event = Arc::new(Event::new());

        // Create 3 waiters
        let barrier = Arc::new(std::sync::Barrier::new(4));

        let event1 = event.clone();
        let barrier1 = barrier.clone();
        let h1 = thread::spawn(move || {
            block_on(async move {
                let fut = event1.wait_until(|| None::<()>);
                let mut fut = std::pin::pin!(fut);

                let waker = Waker::noop();
                let mut cx = Context::from_waker(&waker);
                let _ = fut.as_mut().poll(&mut cx);

                barrier1.wait();
                // Keep waiting
                barrier1.wait();
            })
        });

        let event2 = event.clone();
        let barrier2 = barrier.clone();
        let h2 = thread::spawn(move || {
            block_on(async move {
                let fut = event2.wait_until(|| None::<()>);
                let mut fut = std::pin::pin!(fut);

                let waker = Waker::noop();
                let mut cx = Context::from_waker(&waker);
                let _ = fut.as_mut().poll(&mut cx);

                barrier2.wait();
                // This one will be cancelled
                barrier2.wait();
                // Future dropped here
            })
        });

        let event3 = event.clone();
        let barrier3 = barrier.clone();
        let h3 = thread::spawn(move || {
            block_on(async move {
                let fut = event3.wait_until(|| None::<()>);
                let mut fut = std::pin::pin!(fut);

                let waker = Waker::noop();
                let mut cx = Context::from_waker(&waker);
                let _ = fut.as_mut().poll(&mut cx);

                barrier3.wait();
                // Keep waiting
                barrier3.wait();
            })
        });

        // Wait for all to be registered
        barrier.wait();

        // Let h2 cancel (its future will be dropped)
        barrier.wait();

        // Give time for cleanup
        thread::sleep(Duration::from_millis(10));

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    }
}
