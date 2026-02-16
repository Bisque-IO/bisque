//! IO thread pool with per-thread proactor.
//!
//! This module provides an IO thread pool where each thread has its own
//! proactor for completion-based async IO. Operations are submitted
//! via contract groups and completed asynchronously.
//!
//! # Contract-Based Design
//! All per-operation allocations are eliminated using a signal tree-based
//! contract pool:
//! - Producers claim available slots via atomic bitmap operations
//! - Consumers find ready work via hierarchical summary bits
//! - Single consumer model (IO driver thread)
//! - No blocking - wake mechanism handled by drivers via eventfd/poller
//!
//! # Platform Support
//! - Linux: io_uring for true async IO
//! - Windows/macOS/BSD: mio-based polling with IO thread syscalls
//!
//! # Example
//! ```ignore
//! use maniac::driver::IoPool;
//!
//! let pool = IoPool::new()?;
//! let handle = pool.open(fd);
//!
//! // Async read - buffer ownership transferred to future
//! let buf = vec![0u8; 4096];
//! let (result, buf) = handle.read_at(0, buf).await;
//! ```

#[cfg(feature = "io-driver")]
pub mod asyncify;
#[cfg(feature = "io-driver")]
mod contract;
#[cfg(feature = "io-driver")]
mod handle;
#[cfg(feature = "io-driver")]
mod io_thread;
#[cfg(feature = "io-driver")]
mod pool;

// Platform-specific drivers
#[cfg(feature = "io-driver")]
mod poll;
#[cfg(all(feature = "io-driver", target_os = "linux"))]
mod uring;

#[cfg(feature = "io-driver")]
pub mod sys_slice;

#[cfg(feature = "io-driver")]
pub use crate::buf::{BufResult, IoBuf, IoBufMut};
#[cfg(feature = "io-driver")]
pub use contract::{
    ContractId, IoContract, IoContractGroup, IoContractGroupConfig, IoContractGroupInner, IoResult,
    Op, OpType, PollWaker,
};
#[cfg(feature = "io-driver")]
pub use handle::{
    AcceptFuture, AcceptResult, AddrStorage, ConnectFuture, IoHandle, ReadFuture, RecvFromFuture,
    RecvFromResult, SendToFuture, SyncFuture, WriteFuture,
};
#[cfg(feature = "io-driver")]
pub use pool::{IoPool, IoPoolBuilder};
