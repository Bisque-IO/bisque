//! A minimal atomic waker for the single-registrant, multi-notifier pattern.
//!
//! This is the simplest possible waker: just a waker slot with no state machine.
//! Since register is single-threaded, we only need to check `will_wake()` to avoid cloning.
//!
//! Every `notify()` calls `wake_by_ref()` if a waker is present. This is fine for IO
//! completion where typically only one completion happens per registration.

use core::cell::UnsafeCell;
use core::task::Waker;

/// A minimal atomic waker for single-registrant, multi-notifier patterns.
///
/// Key properties:
/// - `register()`: just a `will_wake()` check (pointer comparison), clone only if different
/// - `notify()`: just read and wake
/// - No atomic state machine, no generation counter
///
/// Tradeoff: Every `notify()` calls `wake_by_ref()`. For IO completion where you
/// typically get one completion per registration, this is optimal.
///
/// # Safety
///
/// `register()` must only be called from a single thread (the future's polling thread).
/// `notify()` may be called from any thread concurrently.
pub struct GenWaker {
    /// Single waker slot
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: waker is only written by single registrant thread
// notify() only reads and calls wake_by_ref() which is safe to call concurrently
unsafe impl Send for GenWaker {}
unsafe impl Sync for GenWaker {}

impl GenWaker {
    /// Creates a new `GenWaker` with no registered waker.
    #[inline]
    pub const fn new() -> Self {
        Self {
            waker: UnsafeCell::new(None),
        }
    }

    /// Registers a waker to be notified.
    ///
    /// Only clones the waker if it's different from the currently stored one.
    ///
    /// # Safety
    ///
    /// Must only be called from a single thread.
    #[inline]
    pub unsafe fn register(&self, waker: &Waker) {
        // SAFETY: Only called from single registrant thread
        unsafe {
            let slot = &mut *self.waker.get();
            match slot {
                Some(w) if w.will_wake(waker) => {} // same waker, nothing to do
                _ => *slot = Some(waker.clone()),
            }
        }
    }

    /// Notifies the registered waker, if any.
    ///
    /// Every call will invoke `wake_by_ref()` if a waker is present.
    #[inline]
    pub fn notify(&self) {
        // SAFETY: The registrant only writes to waker slot, we only read.
        // wake_by_ref() is safe to call from any thread.
        unsafe {
            if let Some(w) = &*self.waker.get() {
                w.wake_by_ref();
            }
        }
    }

    /// Clears the registered waker.
    ///
    /// # Safety
    ///
    /// Must only be called from the registrant thread.
    #[inline]
    pub unsafe fn unregister(&self) {
        // SAFETY: Only called from single registrant thread
        unsafe { *self.waker.get() = None };
    }

    /// Returns true if a waker is registered.
    #[inline]
    pub fn is_registered(&self) -> bool {
        // SAFETY: Just reading, and the value is either Some or None
        unsafe { (*self.waker.get()).is_some() }
    }
}

impl Default for GenWaker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GenWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenWaker").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{RawWaker, RawWakerVTable};

    fn counting_waker(counter: &'static AtomicUsize) -> Waker {
        fn clone(data: *const ()) -> RawWaker {
            RawWaker::new(data, &VTABLE)
        }
        fn wake(data: *const ()) {
            let counter = unsafe { &*(data as *const AtomicUsize) };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(data: *const ()) {
            let counter = unsafe { &*(data as *const AtomicUsize) };
            counter.fetch_add(1, Ordering::Relaxed);
        }
        fn drop(_: *const ()) {}

        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

        unsafe {
            Waker::from_raw(RawWaker::new(
                counter as *const AtomicUsize as *const (),
                &VTABLE,
            ))
        }
    }

    #[test]
    fn test_register_notify() {
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        COUNT.store(0, Ordering::Relaxed);

        let gw = GenWaker::new();
        let waker = counting_waker(&COUNT);

        unsafe { gw.register(&waker) };
        assert!(gw.is_registered());

        gw.notify();
        assert_eq!(COUNT.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_register_same_waker() {
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        let gw = GenWaker::new();
        let waker = counting_waker(&COUNT);

        unsafe {
            gw.register(&waker);
            // These should all skip cloning (will_wake returns true)
            gw.register(&waker);
            gw.register(&waker);
            gw.register(&waker);
        }

        assert!(gw.is_registered());
    }

    #[test]
    fn test_multiple_notify() {
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        COUNT.store(0, Ordering::Relaxed);

        let gw = GenWaker::new();
        let waker = counting_waker(&COUNT);

        unsafe { gw.register(&waker) };

        // Multiple notifies - each one wakes
        gw.notify();
        gw.notify();
        gw.notify();

        assert_eq!(COUNT.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_concurrent_notify() {
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        COUNT.store(0, Ordering::Relaxed);

        let gw = Arc::new(GenWaker::new());
        let waker = counting_waker(&COUNT);

        unsafe { gw.register(&waker) };

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let gw = Arc::clone(&gw);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        gw.notify();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // All notifies call wake (4 threads * 1000 = 4000)
        assert_eq!(COUNT.load(Ordering::Relaxed), 4000);
    }

    #[test]
    fn test_notify_no_waker() {
        let gw = GenWaker::new();
        // Should not panic
        gw.notify();
        gw.notify();
    }

    #[test]
    fn test_unregister() {
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        let gw = GenWaker::new();
        let waker = counting_waker(&COUNT);

        unsafe { gw.register(&waker) };
        assert!(gw.is_registered());

        unsafe { gw.unregister() };
        assert!(!gw.is_registered());
    }
}
