//! Example demonstrating the IO thread pool with per-thread proactor.
//!
//! This example shows:
//! 1. Creating an IoPool with multiple IO threads
//! 2. File operations: read, write, sync
//! 3. Concurrent operations from multiple threads
//! 4. Zero-allocation IO via ring buffers with owned buffer pattern

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use maniac::driver::{BufResult, IoPool, IoPoolBuilder};

/// Simple block_on implementation for running futures without a full runtime.
fn block_on<F: Future>(mut fut: F) -> F::Output {
    fn noop_clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);

    let mut fut = unsafe { Pin::new_unchecked(&mut fut) };

    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => std::hint::spin_loop(),
        }
    }
}

#[cfg(unix)]
fn main() -> io::Result<()> {
    println!("=== IO Pool Example ===\n");

    // Create pool with 2 IO threads
    let pool = IoPoolBuilder::new()
        .num_threads(2)
        .contract_capacity(256)
        .build()?;

    println!("Pool created with {} IO threads\n", pool.num_threads());

    // ========================================================================
    // File Operations
    // ========================================================================
    println!("--- File Operations ---");

    let temp_path = "/tmp/io_pool_test.txt";
    let test_content = b"Hello from IO Pool! This is test data.";

    // Create and write to a file using standard IO first
    {
        let mut file = File::create(temp_path)?;
        file.write_all(test_content)?;
    }

    // Open file and read using IO pool
    let file = OpenOptions::new().read(true).open(temp_path)?;
    let handle = pool.open(file.as_raw_fd());
    println!(
        "File opened on IO thread {} (fd={})",
        handle.thread_idx(),
        handle.fd()
    );

    // Async read - buffer ownership is taken and returned via BufResult
    let buf = vec![0u8; 64];
    let future = handle.read_at(0, buf);
    let BufResult(result, buf) = block_on(future);
    match result {
        Ok(n) => {
            println!("  read_at: {} bytes read", n);
            println!("  content: {:?}", String::from_utf8_lossy(&buf[..n]));
        }
        Err(e) => println!("  read_at error: {}", e),
    }

    // Open file for writing
    let file_w = OpenOptions::new().write(true).open(temp_path)?;
    let handle_w = pool.open(file_w.as_raw_fd());

    // Async write - buffer ownership is taken and returned
    let write_buf = b"Overwritten data!".to_vec();
    let future = handle_w.write_at(0, write_buf);
    let BufResult(result, _write_buf) = block_on(future);
    match result {
        Ok(n) => println!("  write_at: {} bytes written", n),
        Err(e) => println!("  write_at error: {}", e),
    }

    // Sync to disk
    let future = handle_w.sync(false);
    match block_on(future) {
        Ok(_) => println!("  sync: success"),
        Err(e) => println!("  sync error: {}", e),
    }

    // Verify the write
    {
        let mut file = File::open(temp_path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        println!("  after write: {:?}", content);
    }

    // ========================================================================
    // Concurrent Operations
    // ========================================================================
    println!("\n--- Concurrent Operations ---");

    // Create test file again with fresh data
    {
        let mut file = File::create(temp_path)?;
        file.write_all(b"Concurrent test data for multiple readers.")?;
    }

    let pool: Arc<IoPool> = Arc::new(pool);

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let pool: Arc<IoPool> = Arc::clone(&pool);
            let path = temp_path.to_string();
            thread::spawn(move || {
                let file = OpenOptions::new().read(true).open(&path).unwrap();
                let io_handle = pool.open(file.as_raw_fd());

                let buf = vec![0u8; 16];
                let future = io_handle.read_at(0, buf);
                let BufResult(result, buf) = block_on(future);
                match result {
                    Ok(n) => println!(
                        "  Worker {} read {} bytes via IO thread {}: {:?}",
                        i,
                        n,
                        io_handle.thread_idx(),
                        String::from_utf8_lossy(&buf[..n])
                    ),
                    Err(e) => println!("  Worker {} error: {}", i, e),
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // ========================================================================
    // Cleanup
    // ========================================================================
    println!("\n--- Cleanup ---");

    std::fs::remove_file(temp_path).ok();

    match Arc::try_unwrap(pool) {
        Ok(pool) => {
            pool.shutdown();
            println!("  Pool shutdown complete");
        }
        Err(_) => {
            println!("  Warning: pool still has references");
        }
    }

    println!("\n=== Example Complete ===");
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    println!("This example currently only runs on Unix platforms.");
    println!("Windows support is available but requires different fd handling.");
}
