//! IO thread pool implementation.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use crate::ptr::StripedArc;
use crate::util::random;

use super::contract::{IoContractGroup, IoContractGroupConfig, IoContractGroupInner};
use super::handle::IoHandle;
use super::io_thread::spawn_io_thread;

/// Builder for configuring an [`IoPool`].
pub struct IoPoolBuilder {
    num_threads: usize,
    contract_capacity: usize,
    /// Force use of poll-based driver instead of io_uring on Linux.
    force_poll: bool,
    /// Force linear scanning instead of summary-based selection.
    force_linear_scan: bool,
}

impl Default for IoPoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IoPoolBuilder {
    pub fn new() -> Self {
        Self {
            num_threads: 1,
            contract_capacity: 4096,
            force_poll: false,
            force_linear_scan: false,
        }
    }

    /// Set the number of IO threads.
    pub fn num_threads(mut self, n: usize) -> Self {
        self.num_threads = n.max(1);
        self
    }

    /// Set the capacity of each thread's contract group.
    pub fn contract_capacity(mut self, capacity: usize) -> Self {
        self.contract_capacity = capacity.max(64);
        self
    }

    /// Force use of poll-based driver instead of io_uring on Linux.
    ///
    /// This can be useful for:
    /// - Debugging and comparing performance between io_uring and poll
    /// - Running on older kernels that don't support io_uring
    /// - Working around io_uring bugs or limitations
    ///
    /// On non-Linux platforms, this option has no effect as poll is always used.
    #[cfg(target_os = "linux")]
    pub fn force_poll(mut self, force: bool) -> Self {
        self.force_poll = force;
        self
    }

    /// Force linear scanning instead of summary-based selection.
    ///
    /// This can be useful for small contract groups or debugging.
    pub fn force_linear_scan(mut self, force: bool) -> Self {
        self.force_linear_scan = force;
        self
    }

    /// Build the IO pool.
    pub fn build(self) -> io::Result<IoPool> {
        let mut contract_groups = Vec::with_capacity(self.num_threads);
        let mut threads = Vec::with_capacity(self.num_threads);
        let shutdown = Arc::new(AtomicBool::new(false));

        for i in 0..self.num_threads {
            let group = IoContractGroup::with_config(IoContractGroupConfig {
                capacity: self.contract_capacity as u64,
                force_linear_scan: self.force_linear_scan,
            });
            contract_groups.push(StripedArc::clone(group.inner()));
            threads.push(spawn_io_thread(
                StripedArc::clone(group.inner()),
                Arc::clone(&shutdown),
                i,
                self.force_poll,
            ));
        }

        Ok(IoPool {
            inner: StripedArc::new(IoPoolInner {
                contract_groups,
                num_threads: self.num_threads,
                shutdown,
            }),
            threads,
        })
    }
}

/// Shared state for the IO pool.
pub struct IoPoolInner {
    pub(crate) contract_groups: Vec<StripedArc<IoContractGroupInner>>,
    pub(crate) num_threads: usize,
    pub(crate) shutdown: Arc<AtomicBool>,
}

impl std::fmt::Debug for IoPoolInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoPoolInner")
            .field("num_threads", &self.num_threads)
            .field("shutdown", &self.shutdown.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl IoPoolInner {
    /// Get the contract group for a specific thread.
    #[inline]
    pub fn contract_group(&self, thread_idx: usize) -> &StripedArc<IoContractGroupInner> {
        &self.contract_groups[thread_idx % self.num_threads]
    }
}

/// A pool of IO threads, each with its own proactor.
pub struct IoPool {
    inner: StripedArc<IoPoolInner>,
    threads: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for IoPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoPool")
            .field("num_threads", &self.inner.num_threads)
            .finish_non_exhaustive()
    }
}

impl IoPool {
    /// Create a new IO pool with default settings.
    pub fn new() -> io::Result<Self> {
        IoPoolBuilder::new().build()
    }

    /// Create a builder for configuring the pool.
    pub fn builder() -> IoPoolBuilder {
        IoPoolBuilder::new()
    }

    /// Get the number of IO threads.
    pub fn num_threads(&self) -> usize {
        self.inner.num_threads
    }

    /// Open a file descriptor with random thread assignment.
    pub fn open(&self, fd: i32) -> IoHandle {
        let thread_idx = (random::next_u64() as usize) % self.inner.num_threads;
        IoHandle::new(fd, thread_idx, StripedArc::clone(&self.inner))
    }

    /// Open a file descriptor with a specific thread assignment.
    pub fn open_on_thread(&self, fd: i32, thread_idx: usize) -> IoHandle {
        let idx = thread_idx % self.inner.num_threads;
        IoHandle::new(fd, idx, StripedArc::clone(&self.inner))
    }

    /// Shutdown the pool, waiting for all threads to finish.
    pub fn shutdown(self) {
        self.inner.shutdown.store(true, Ordering::Release);
        for handle in self.threads {
            let _ = handle.join();
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}
