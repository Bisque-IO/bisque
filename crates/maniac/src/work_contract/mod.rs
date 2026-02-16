//! Work Contract - Lock-free task scheduling
//!
//! Work contracts provide a way to schedule and execute tasks with
//! automatic rescheduling support. Contracts can be scheduled, released,
//! and executed by worker threads.

mod contract;
mod group;
mod this_contract;

pub use contract::{InitialState, WorkContract};
pub use group::{BlockingWorkContractGroup, NonBlockingWorkContractGroup, WorkContractGroup};
pub use this_contract::ThisContract;

/// Synchronization mode for work contract groups
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronizationMode {
    /// Non-blocking mode - workers spin when no work is available
    NonBlocking,
    /// Blocking mode - workers wait on condition variable when no work is available
    Blocking,
}

impl Default for SynchronizationMode {
    fn default() -> Self {
        SynchronizationMode::NonBlocking
    }
}

/// Work contract ID type
pub type WorkContractId = u64;

/// Invalid contract ID
pub const INVALID_CONTRACT_ID: WorkContractId = !0u64;
