//! IO thread implementation with per-thread driver.
//!
//! Each IO thread has its own driver (io-uring on Linux by default, mio elsewhere)
//! and consumes operations from the IoContractGroup.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::ptr::StripedArc;

use super::contract::IoContractGroupInner;
use super::poll::PollDriver;

#[cfg(target_os = "linux")]
use super::uring::UringDriver;

/// The driver type used by an IO thread.
#[cfg(target_os = "linux")]
enum Driver {
    Uring(UringDriver),
    Poll(PollDriver),
}

#[cfg(not(target_os = "linux"))]
enum Driver {
    Poll(PollDriver),
}

/// An IO thread that runs its own driver and consumes from the contract group.
pub(crate) struct IoThread {
    driver: Driver,
    contract_group: StripedArc<IoContractGroupInner>,
    shutdown: Arc<AtomicBool>,
    /// Bias for fair work selection
    bias: u64,
}

impl IoThread {
    #[cfg(target_os = "linux")]
    pub fn new(
        contract_group: StripedArc<IoContractGroupInner>,
        shutdown: Arc<AtomicBool>,
        force_poll: bool,
    ) -> io::Result<Self> {
        let driver = if force_poll {
            Driver::Poll(PollDriver::new(1024, StripedArc::clone(&contract_group))?)
        } else {
            Driver::Uring(UringDriver::new(1024, StripedArc::clone(&contract_group))?)
        };

        Ok(Self {
            driver,
            contract_group,
            shutdown,
            bias: 0,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn new(
        contract_group: StripedArc<IoContractGroupInner>,
        shutdown: Arc<AtomicBool>,
        _force_poll: bool,
    ) -> io::Result<Self> {
        let driver = Driver::Poll(PollDriver::new(1024, StripedArc::clone(&contract_group))?);

        Ok(Self {
            driver,
            contract_group,
            shutdown,
            bias: 0,
        })
    }

    pub fn run(&mut self) {
        while !self.shutdown.load(Ordering::Relaxed) {
            let mut count = 0;
            // Consume and submit operations from the contract group first
            // This must happen before polling to ensure we process new submissions
            while let Some(contract) = self.contract_group.pop(&mut self.bias) {
                let id = contract.id();
                let result = match &mut self.driver {
                    #[cfg(target_os = "linux")]
                    Driver::Uring(uring) => uring.submit(contract),
                    Driver::Poll(poll) => poll.submit(contract),
                };

                if let Err(e) = result {
                    eprintln!("submit error: {}", e);
                    // Signal error to completion
                    self.contract_group.complete(id, Err(e));
                }

                count += 1;
                if count > 50000 {
                    break;
                }
            }

            // Poll driver for completions
            match &mut self.driver {
                #[cfg(target_os = "linux")]
                Driver::Uring(uring) => {
                    // Check for completions without blocking
                    let _ = uring.poll();
                    std::thread::sleep(Duration::from_micros(100));
                }
                Driver::Poll(poll) => {
                    // With level-triggered mode and a waker registered, poll() will
                    // be interrupted when new contracts are pushed, so we can use
                    // longer timeouts safely.
                    match poll.poll(Some(Duration::from_millis(100))) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(e) => eprintln!("driver error: {}", e),
                    }
                }
            }
        }
    }
}

pub(crate) fn spawn_io_thread(
    contract_group: StripedArc<IoContractGroupInner>,
    shutdown: Arc<AtomicBool>,
    thread_idx: usize,
    force_poll: bool,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("io-thread-{}", thread_idx))
        .spawn(
            move || match IoThread::new(contract_group, shutdown, force_poll) {
                Ok(mut thread) => thread.run(),
                Err(e) => {
                    eprintln!("Failed to create IO thread {}: {}", thread_idx, e);
                }
            },
        )
        .expect("failed to spawn IO thread")
}
