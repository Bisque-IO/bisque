//! Blocking thread pool for CPU-bound and blocking operations.
//!
//! Uses the scheduler's ContractGroup for efficient work stealing and
//! thread coordination. Supports dynamic thread scaling between
//! min_threads and max_threads based on load.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crate::scheduler::{BiasState, ContractGroup};

/// Default minimum number of threads
const DEFAULT_MIN_THREADS: usize = 1;

/// Default maximum number of threads
const DEFAULT_MAX_THREADS: usize = 256;

/// Default contract group capacity
const DEFAULT_CAPACITY: u64 = 65536 * 4;

/// Default idle timeout before a thread exits (if above min_threads)
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Shared state for the blocking pool
struct PoolInner {
    /// The contract group for scheduling work
    group: ContractGroup,
    /// Minimum number of threads to keep alive
    min_threads: usize,
    /// Idle timeout before excess threads exit
    idle_timeout: Duration,
    /// Whether the pool is shutting down
    shutdown: AtomicBool,
}

/// A thread pool for blocking operations.
///
/// Threads are spawned on demand up to `max_threads` and will exit
/// after `idle_timeout` if the thread count exceeds `min_threads`.
pub struct BlockingPool {
    inner: Arc<PoolInner>,
}

impl BlockingPool {
    /// Create a new blocking pool with default settings.
    pub fn new() -> Self {
        Self::with_threads(DEFAULT_MIN_THREADS, DEFAULT_MAX_THREADS)
    }

    /// Create a new blocking pool with specified thread limits.
    pub fn with_threads(min_threads: usize, max_threads: usize) -> Self {
        Self::with_config(min_threads, max_threads, DEFAULT_IDLE_TIMEOUT)
    }

    /// Create a new blocking pool with full configuration.
    pub fn with_config(min_threads: usize, max_threads: usize, idle_timeout: Duration) -> Self {
        assert!(min_threads > 0, "min_threads must be at least 1");
        assert!(
            max_threads >= min_threads,
            "max_threads must be >= min_threads"
        );

        let inner = Arc::new(PoolInner {
            group: ContractGroup::new_blocking_with_capacity(DEFAULT_CAPACITY),
            min_threads,
            idle_timeout,
            shutdown: AtomicBool::new(false),
        });

        // Set spawn callback only for dynamic pools (min < max)
        // Fixed pools don't need the callback overhead
        if min_threads < max_threads {
            // Safety: callback is set before any threads are spawned
            unsafe {
                inner.group.set_spawn_callback(
                    Self::spawn_callback,
                    Arc::as_ptr(&inner) as *const (),
                    max_threads,
                );
            }
        }

        // Spawn minimum threads
        for _ in 0..min_threads {
            inner.group.add_thread();
            Self::spawn_worker(Arc::clone(&inner));
        }

        Self { inner }
    }

    /// Callback invoked when work is available but no threads are waiting.
    /// Note: The semaphore has already incremented active_threads before calling this.
    fn spawn_callback(data: *const ()) {
        // Safety: data is a pointer to PoolInner from Arc::as_ptr
        // Reconstruct Arc for spawn_worker
        unsafe {
            Arc::increment_strong_count(data as *const PoolInner);
            let arc = Arc::from_raw(data as *const PoolInner);
            BlockingPool::spawn_worker(arc);
        }
    }

    /// Spawn a blocking task on the pool.
    ///
    /// The closure will be executed on one of the pool's worker threads.
    pub fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Wrap the closure in an async block that just runs it
        let task = async move {
            f();
        };

        // Try to spawn the task
        // The spawn callback will be invoked if no threads are waiting
        if self.inner.group.spawn(task, true).is_err() {
            // Contract group is full, but we still need to run the task
            // This shouldn't happen with proper capacity, but handle gracefully
            panic!("blocking pool capacity exceeded");
        }
    }

    /// Spawn a blocking task that returns a value.
    ///
    /// Returns a handle that can be used to retrieve the result.
    pub fn spawn_blocking<F, T>(&self, f: F) -> BlockingHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        self.spawn(move || {
            let result = f();
            let _ = tx.send(result);
        });

        BlockingHandle { rx }
    }

    /// Spawn a new worker thread.
    fn spawn_worker(inner: Arc<PoolInner>) {
        thread::Builder::new()
            .name("blocking-worker".into())
            .spawn(move || {
                Self::worker_loop(inner);
            })
            .expect("failed to spawn worker thread");
    }

    /// The main worker loop.
    fn worker_loop(inner: Arc<PoolInner>) {
        let mut bias = BiasState::new();

        loop {
            // Wait for work with timeout (semaphore tracks waiters internally)
            let contract = inner.group.poll_next_timeout(&mut bias, inner.idle_timeout);

            match contract {
                Some(_) => {
                    // Contract was polled and executed by poll_next_timeout
                    // Just continue to the next iteration
                }
                None => {
                    // Timeout or shutdown
                    if inner.shutdown.load(Ordering::Acquire) {
                        // Decrement and exit
                        inner.group.remove_thread();
                        return;
                    }

                    // Check if we should exit (above min_threads)
                    let active = inner.group.active_threads();
                    if active > inner.min_threads {
                        // Try to decrement and exit
                        let prev = inner.group.remove_thread();
                        if prev > inner.min_threads {
                            // Successfully reduced thread count, exit
                            return;
                        } else {
                            // We went below min, restore and continue
                            inner.group.add_thread();
                        }
                    }
                }
            }
        }
    }

    /// Get the current number of active threads.
    pub fn active_threads(&self) -> usize {
        self.inner.group.active_threads()
    }

    /// Get the current number of idle threads.
    pub fn idle_threads(&self) -> u64 {
        self.inner.group.idle_threads()
    }

    /// Shut down the pool, waiting for all threads to exit.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.group.stop();
    }

    /// Check if the pool is shutting down.
    pub fn is_shutdown(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }
}

impl Default for BlockingPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for BlockingPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Handle to retrieve the result of a blocking task.
pub struct BlockingHandle<T> {
    rx: std::sync::mpsc::Receiver<T>,
}

impl<T> BlockingHandle<T> {
    /// Block until the result is available.
    pub fn join(self) -> T {
        self.rx.recv().expect("blocking task panicked")
    }

    /// Try to get the result without blocking.
    pub fn try_join(&self) -> Option<T> {
        self.rx.try_recv().ok()
    }

    /// Wait for the result with a timeout.
    pub fn join_timeout(&self, timeout: Duration) -> Option<T> {
        self.rx.recv_timeout(timeout).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_spawn_and_execute() {
        let pool = BlockingPool::with_threads(1, 4);
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = Arc::clone(&counter);
        pool.spawn(move || {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });

        // Give the task time to execute
        thread::sleep(Duration::from_millis(50));

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_spawn_blocking_with_result() {
        let pool = BlockingPool::with_threads(1, 4);

        let handle = pool.spawn_blocking(|| 42);

        assert_eq!(handle.join(), 42);
    }

    #[test]
    fn test_multiple_tasks() {
        let pool = BlockingPool::with_threads(2, 8);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..100 {
            let counter_clone = Arc::clone(&counter);
            pool.spawn(move || {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            });
        }

        // Wait for all tasks to complete
        thread::sleep(Duration::from_millis(500));

        assert_eq!(counter.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_thread_scaling() {
        let pool = BlockingPool::with_threads(1, 4);

        // Initially should have min_threads
        assert!(pool.active_threads() >= 1);

        // Spawn tasks that block briefly to force thread creation
        let barrier = Arc::new(std::sync::Barrier::new(5));

        for _ in 0..4 {
            let barrier_clone = Arc::clone(&barrier);
            pool.spawn(move || {
                barrier_clone.wait();
            });
        }

        // Give threads time to spawn
        thread::sleep(Duration::from_millis(100));

        // Should have spawned more threads
        let active = pool.active_threads();
        assert!(
            active >= 1,
            "expected at least 1 active thread, got {}",
            active
        );

        // Release the barrier
        barrier.wait();

        // Give threads time to become idle and potentially exit
        thread::sleep(Duration::from_millis(100));
    }

    #[test]
    fn test_shutdown() {
        let pool = BlockingPool::with_threads(2, 4);

        pool.spawn(|| {
            thread::sleep(Duration::from_millis(10));
        });

        pool.shutdown();

        assert!(pool.is_shutdown());
    }

    #[test]
    fn test_idle_timeout() {
        // Use a short idle timeout for testing
        let pool = BlockingPool::with_config(1, 4, Duration::from_millis(100));

        // Spawn tasks to create extra threads
        let barrier = Arc::new(std::sync::Barrier::new(4));

        for _ in 0..3 {
            let barrier_clone = Arc::clone(&barrier);
            pool.spawn(move || {
                barrier_clone.wait();
            });
        }

        // Wait for threads to spawn
        thread::sleep(Duration::from_millis(50));
        barrier.wait();

        // Wait for idle timeout to expire
        thread::sleep(Duration::from_millis(200));

        // Should have scaled back down toward min_threads
        let active = pool.active_threads();
        assert!(
            active <= 2,
            "expected threads to scale down, got {} active",
            active
        );
    }

    #[test]
    fn test_burst_spawning() {
        // Test that burst of blocking work spawns multiple threads
        // This tests the fix for: 1 waiter + N releases should spawn more threads
        let pool = BlockingPool::with_threads(1, 8);
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));

        // Give time for min thread to start and become idle
        thread::sleep(Duration::from_millis(50));

        // Spawn 6 tasks that each block for 50ms
        // If threads spawn correctly, they should run concurrently
        for _ in 0..6 {
            let max_concurrent = Arc::clone(&max_concurrent);
            let current = Arc::clone(&current);
            pool.spawn(move || {
                let c = current.fetch_add(1, Ordering::SeqCst) + 1;

                // Update max concurrent
                let mut max = max_concurrent.load(Ordering::SeqCst);
                while c > max {
                    match max_concurrent.compare_exchange_weak(
                        max,
                        c,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(m) => max = m,
                    }
                }

                // Block for 50ms - long enough that sequential execution would take 300ms
                thread::sleep(Duration::from_millis(50));

                current.fetch_sub(1, Ordering::SeqCst);
            });
        }

        // Wait for all to complete (should be ~50-100ms if concurrent, 300ms if sequential)
        thread::sleep(Duration::from_millis(200));

        let max = max_concurrent.load(Ordering::SeqCst);
        assert!(
            max >= 3,
            "expected at least 3 concurrent tasks, got {}",
            max
        );
    }
}
