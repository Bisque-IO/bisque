//! A simplified atomic waker optimized for the single-registrant, multi-notifier pattern.
//!
//! This is a stripped-down version of `DiatomicWaker` that removes support for:
//! - `unregister()` - not needed for IO completion patterns
//! - `wait_until()` - futures handle their own polling
//!
//! The simplification allows for fewer atomic operations in the common case.

use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::Waker;
use std::cell::UnsafeCell;

// State bits:
// - INDEX [bit 0]: which slot contains the current waker (0 or 1)
// - UPDATE [bit 1]: a new waker is pending in the alternate slot
// - REGISTERED [bit 2]: a waker is registered and waiting for notification

const INDEX: usize = 0b0001;
const UPDATE: usize = 0b0010;
const REGISTERED: usize = 0b0100;

/// A lightweight atomic waker for single-registrant, multi-notifier patterns.
///
/// Optimized for IO completion scenarios where:
/// - One future registers/updates a waker
/// - One or more IO threads may call notify()
/// - The waker is typically the same across multiple registrations
///
/// # Safety
///
/// `register()` must only be called from a single thread (the future's polling thread).
/// `notify()` may be called from any thread concurrently.
#[derive(Debug)]
pub struct SimpleWaker {
    state: AtomicUsize,
    wakers: [UnsafeCell<Option<Waker>>; 2],
}

// SAFETY: The state machine ensures proper synchronization between the single
// registrant and multiple notifiers.
unsafe impl Send for SimpleWaker {}
unsafe impl Sync for SimpleWaker {}

impl SimpleWaker {
    /// Creates a new `SimpleWaker` with no registered waker.
    #[inline]
    pub const fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            wakers: [UnsafeCell::new(None), UnsafeCell::new(None)],
        }
    }

    /// Registers a waker to be notified.
    ///
    /// If the same waker is already registered, this is a fast no-op.
    /// If a different waker is provided, it will be stored and made active.
    ///
    /// # Safety
    ///
    /// Must only be called from a single thread. Cannot be called concurrently
    /// with itself.
    #[inline]
    pub unsafe fn register(&self, waker: &Waker) {
        let state = self.state.load(Ordering::Acquire);

        // Fast path: check if same waker is already registered
        if state & REGISTERED != 0 {
            let idx = self.current_waker_index(state);
            if self.will_wake(idx, waker) {
                return;
            }
        }

        // Check if waker matches (even if not registered)
        let idx = self.current_waker_index(state);
        if self.will_wake(idx, waker) {
            // Same waker, just set REGISTERED
            self.state.fetch_or(REGISTERED, Ordering::Release);
            return;
        }

        // Different waker - need to store it in the alternate slot
        let alt_idx = idx ^ 1;

        // Store the new waker in the alternate slot
        // SAFETY: We're the only writer (single registrant), and notifiers only
        // read from the INDEX slot. The alternate slot is exclusively ours.
        unsafe {
            *self.wakers[alt_idx].get() = Some(waker.clone());
        }

        // Publish the update: set UPDATE and REGISTERED
        // The notifier will see UPDATE and switch to the new waker
        self.state.fetch_or(UPDATE | REGISTERED, Ordering::Release);
    }

    /// Notifies the registered waker, if any.
    ///
    /// This is safe to call from multiple threads concurrently.
    /// If no waker is registered, this is a no-op.
    ///
    /// Uses a single atomic operation in the success path.
    #[inline]
    pub fn notify(&self) {
        let mut state = self.state.load(Ordering::Acquire);

        // Fast path: no one waiting
        if state & REGISTERED == 0 {
            return;
        }

        // Try to clear REGISTERED atomically (and consume UPDATE if present)
        // Only one notifier will succeed - the winner calls wake_by_ref()
        loop {
            if state & REGISTERED == 0 {
                // Someone else already handled it
                return;
            }

            // Calculate new state: clear REGISTERED
            // If UPDATE is set, also clear UPDATE and flip INDEX to point to new waker
            let new_state = if state & UPDATE != 0 {
                // Consume the update: clear UPDATE, clear REGISTERED, flip INDEX
                (state & !UPDATE & !REGISTERED) ^ INDEX
            } else {
                // Just clear REGISTERED
                state & !REGISTERED
            };

            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // We won the race - we're the only one who will wake
                    // The new INDEX points to the waker we should use
                    let idx = new_state & INDEX;

                    // SAFETY: Only one notifier reaches here per registration.
                    // The registrant only writes to the alternate slot (1 - INDEX),
                    // never to the current INDEX slot. So this read is safe.
                    unsafe {
                        if let Some(w) = &*self.wakers[idx].get() {
                            w.wake_by_ref();
                        }
                    }
                    return;
                }
                Err(s) => {
                    state = s;
                    core::hint::spin_loop();
                }
            }
        }
    }

    /// Clears the REGISTERED flag without dropping the cached waker.
    ///
    /// # Safety
    ///
    /// Must only be called from the registrant thread.
    #[inline]
    pub unsafe fn unregister(&self) {
        self.state.fetch_and(!REGISTERED, Ordering::Release);
    }

    /// Returns true if a waker is currently registered.
    #[inline]
    pub fn is_registered(&self) -> bool {
        self.state.load(Ordering::Relaxed) & REGISTERED != 0
    }

    #[inline]
    fn current_waker_index(&self, state: usize) -> usize {
        if state & UPDATE != 0 {
            // New waker is in alternate slot
            (state & INDEX) ^ 1
        } else {
            state & INDEX
        }
    }

    #[inline]
    fn will_wake(&self, idx: usize, waker: &Waker) -> bool {
        // SAFETY: Only called from register(), which is single-threaded
        unsafe {
            match &*self.wakers[idx].get() {
                Some(w) => w.will_wake(waker),
                None => false,
            }
        }
    }
}

impl Default for SimpleWaker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::task::{RawWaker, RawWakerVTable};

    static WAKE_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn test_waker() -> Waker {
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        fn wake(_: *const ()) {
            WAKE_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(_: *const ()) {
            WAKE_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        fn drop(_: *const ()) {}

        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    #[test]
    fn test_register_notify() {
        let sw = SimpleWaker::new();
        let waker = test_waker();
        WAKE_COUNT.store(0, Ordering::Relaxed);

        unsafe { sw.register(&waker) };
        assert!(sw.is_registered());

        sw.notify();
        assert_eq!(WAKE_COUNT.load(Ordering::Relaxed), 1);
        assert!(!sw.is_registered());
    }

    #[test]
    fn test_notify_no_waker() {
        let sw = SimpleWaker::new();
        sw.notify(); // Should not panic
    }

    #[test]
    fn test_same_waker_fast_path() {
        let sw = SimpleWaker::new();
        let waker = test_waker();

        unsafe {
            sw.register(&waker);
            sw.register(&waker); // Should be fast path
            sw.register(&waker); // Should be fast path
        }

        assert!(sw.is_registered());
    }

    #[test]
    fn test_concurrent_notify() {
        let sw = Arc::new(SimpleWaker::new());
        let waker = test_waker();
        WAKE_COUNT.store(0, Ordering::Relaxed);

        unsafe { sw.register(&waker) };

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let sw = Arc::clone(&sw);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        sw.notify();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // At least one wake should have happened
        assert!(WAKE_COUNT.load(Ordering::Relaxed) >= 1);
    }
}
