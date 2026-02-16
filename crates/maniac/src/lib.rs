//! Work Contract - A lock-free task scheduling library
//!
//! This is a Rust port of the C++ work_contract library, providing:
//! - Signal trees for efficient lock-free signaling
//! - Work contracts for task scheduling with automatic rescheduling
//! - Future contracts for async task scheduling

pub mod blocking;
pub mod buf;
pub mod fs;
pub mod future;
pub mod generator;
pub mod io;

// Re-export BufResult at crate root for buf_try macro
pub use buf::BufResult;
pub mod ptr;
pub mod scheduler;
pub mod signal_tree;
pub mod util;
pub mod work_contract;

pub mod driver;
#[cfg(feature = "io-driver")]
pub mod net;

pub use scheduler::{ContractGroup, FutureContract, PanicHandler};
pub use signal_tree::{INVALID_SIGNAL_INDEX, SignalIndex, SignalTree};
pub use work_contract::{
    BlockingWorkContractGroup, NonBlockingWorkContractGroup, SynchronizationMode, ThisContract,
    WorkContract, WorkContractGroup,
};
