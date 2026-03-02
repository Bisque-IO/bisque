//! Test support utilities for multi-raft tests
//!
//! Provides helpers to run async tests inside Tokio runtime and common fixtures.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use tempfile::TempDir;

/// Run an async future inside a Tokio runtime
///
/// This creates a Tokio runtime and runs the future within it.
pub fn run_async<F>(f: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    rt.block_on(f)
}

/// Port allocator for tests to avoid conflicts
pub struct PortAllocator {
    next: AtomicU16,
}

impl PortAllocator {
    pub fn new(start: u16) -> Self {
        Self {
            next: AtomicU16::new(start),
        }
    }

    pub fn next(&self) -> u16 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for PortAllocator {
    fn default() -> Self {
        Self::new(50000)
    }
}

/// Temporary directory helper for storage tests
pub struct TestTempDir {
    inner: TempDir,
}

impl TestTempDir {
    pub fn new() -> Self {
        Self {
            inner: tempfile::tempdir().expect("Failed to create temp directory"),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.inner.path().to_path_buf()
    }
}

impl Default for TestTempDir {
    fn default() -> Self {
        Self::new()
    }
}
