//! WaitableState - Blocking/waiting infrastructure for contract groups

use crate::util::CachePadded;
use parking_lot::{Condvar, Mutex};
use std::cell::UnsafeCell;
use std::f64::MAX_10_EXP;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};

/// Function pointer type for spawn callback.
/// Called with user data when work is available but no threads are waiting.
pub type SpawnCallbackFn = fn(*const ());

/// Semaphore for blocking thread pools.
pub(crate) struct Semaphore {
    /// Pending work count
    count: CachePadded<AtomicI64>,
    /// Number of threads waiting
    waiters: CachePadded<AtomicU64>,
    mutex: Mutex<()>,
    condvar: Condvar,
    /// Current number of active threads (for dynamic thread pools)
    active_threads: AtomicUsize,
    /// Maximum number of threads allowed (0 = no limit/no callback)
    max_threads: UnsafeCell<usize>,
    /// Callback function for spawning new threads.
    /// Set once before threads start, never modified after.
    spawn_callback: UnsafeCell<Option<SpawnCallbackFn>>,
    /// User data passed to spawn callback.
    spawn_callback_data: UnsafeCell<*const ()>,
}

// Safety: spawn_callback/max_threads are set once before threads start and read-only after
unsafe impl Send for Semaphore {}
unsafe impl Sync for Semaphore {}

impl Semaphore {
    pub(crate) fn new() -> Self {
        Self {
            count: CachePadded::new(AtomicI64::new(0)),
            waiters: CachePadded::new(AtomicU64::new(0)),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
            active_threads: AtomicUsize::new(0),
            max_threads: UnsafeCell::new(0),
            spawn_callback: UnsafeCell::new(None),
            spawn_callback_data: UnsafeCell::new(std::ptr::null()),
        }
    }

    /// Set the spawn callback function and thread limits.
    ///
    /// # Safety
    /// Must be called before any threads access this semaphore.
    /// The caller must ensure that `data` remains valid for the lifetime of this semaphore.
    #[inline]
    pub(crate) unsafe fn set_spawn_callback(
        &self,
        callback: SpawnCallbackFn,
        data: *const (),
        max_threads: usize,
    ) {
        unsafe {
            *self.spawn_callback.get() = Some(callback);
            *self.spawn_callback_data.get() = data;
            *self.max_threads.get() = max_threads;
        }
    }

    /// Increment active thread count. Returns the previous count.
    #[inline]
    pub(crate) fn add_thread(&self) -> usize {
        self.active_threads.fetch_add(1, Ordering::AcqRel)
    }

    /// Decrement active thread count. Returns the previous count.
    #[inline]
    pub(crate) fn remove_thread(&self) -> usize {
        self.active_threads.fetch_sub(1, Ordering::AcqRel)
    }

    /// Get current active thread count.
    #[inline]
    pub(crate) fn active_threads(&self) -> usize {
        self.active_threads.load(Ordering::Acquire)
    }

    /// Get current waiter count (idle threads).
    #[inline]
    pub(crate) fn waiters(&self) -> u64 {
        self.waiters.load(Ordering::Acquire)
    }

    /// Release (signal) - increment count and wake one waiter if any.
    /// For dynamic pools, spawns a new thread if pending work exceeds idle capacity.
    #[inline]
    pub(crate) fn release(&self) {
        let waiters = self.waiters.load(Ordering::Acquire);

        if waiters > 0 {
            self.count.fetch_add(1, Ordering::AcqRel);
            // Static pool mode - just wake a waiter
            self.condvar.notify_one();
        }

        // Safety: spawn_callback is set once before threads start
        // let callback = unsafe { *self.spawn_callback.get() };

        // if let Some(cb) = callback {
        //     // Dynamic pool mode
        //     let active_threads = self.active_threads.load(Ordering::Acquire);
        //     let max_threads = unsafe { *self.max_threads.get() };

        //     if waiters > 0 {
        //         // Wake an idle thread
        //         self.count.fetch_add(1, Ordering::AcqRel);
        //         self.condvar.notify_one();
        //     }

        //     // Also try to spawn if below max (handles burst of work)
        //     if active_threads < max_threads {
        //         self.try_spawn_thread(cb);
        //     }
        // } else if waiters > 0 {
        //     // Static pool mode - just wake a waiter
        //     self.count.fetch_add(1, Ordering::AcqRel);
        //     self.condvar.notify_one();
        // }
        // No waiters and no spawn callback (or at max threads) - work tracked by signal trees
    }

    /// Try to spawn a new thread if below max
    #[inline]
    fn try_spawn_thread(&self, cb: SpawnCallbackFn) {
        let max = unsafe { *self.max_threads.get() };
        let mut active = self.active_threads.load(Ordering::Acquire);
        while active < max {
            match self.active_threads.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let data = unsafe { *self.spawn_callback_data.get() };
                    cb(data);
                    return;
                }
                Err(current) => {
                    active = current;
                }
            }
        }
    }

    /// Release all waiters (for shutdown)
    #[inline]
    pub(crate) fn release_all(&self) {
        self.count.fetch_add(
            1024 + self.waiters.load(Ordering::Acquire) as i64,
            Ordering::AcqRel,
        );
        self.condvar.notify_all();
    }

    /// Try to acquire without blocking - returns true if acquired
    #[inline]
    pub(crate) fn try_acquire(&self) -> bool {
        let prev = self.count.fetch_sub(1, Ordering::AcqRel);
        if prev > 0 {
            true
        } else {
            // Went negative, restore
            self.count.fetch_add(1, Ordering::AcqRel);
            false
        }
    }

    /// Acquire - blocks until count > 0 or stopped
    /// Returns true if acquired, false if stopped
    #[inline]
    pub(crate) fn acquire(&self, stopped: &AtomicBool) -> bool {
        // Fast path
        if self.try_acquire() {
            return true;
        }

        // Register as waiter
        self.waiters.fetch_add(1, Ordering::AcqRel);

        let mut guard = self.mutex.lock();

        loop {
            if self.try_acquire() {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                return true;
            }

            if stopped.load(Ordering::Acquire) {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                return false;
            }

            self.condvar.wait(&mut guard);
        }
    }

    /// Acquire with timeout - returns true if acquired
    #[inline]
    pub(crate) fn acquire_timeout(
        &self,
        stopped: &AtomicBool,
        duration: std::time::Duration,
    ) -> bool {
        // Fast path
        if self.try_acquire() {
            return true;
        }
        // Register as waiter
        self.waiters.fetch_add(1, Ordering::AcqRel);

        let deadline = std::time::Instant::now() + duration;
        let mut guard = self.mutex.lock();

        loop {
            if self.try_acquire() {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                return true;
            }

            if stopped.load(Ordering::Relaxed) {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                return false;
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                self.waiters.fetch_sub(1, Ordering::AcqRel);
                return false;
            }

            self.condvar.wait_for(&mut guard, remaining);
        }
    }
}
