//! Future Contract - Async task scheduling
//!
//! An async variant of work contracts that polls futures instead of
//! calling synchronous work functions. Optimized for async runtimes.

mod contract;
mod error;
mod group;
mod join;
pub mod signal;
mod waitable;

pub use contract::{FutureContract, JoinHandle, TypedFutureContract};
pub use error::{PanicHandler, SpawnError};
pub use group::{ContractGroup, ContractGroupInner};
pub use join::JoinSlot;
pub use waitable::SpawnCallbackFn;

use crate::ptr::StripedArc;

/// Contract ID - packed representation with slot index and generation.
///
/// The generation is used to detect stale wakers that reference
/// a slot that has been reused for a different contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct ContractId {
    /// Slot index within the contract group
    pub index: u32,
    /// Generation counter for ABA detection (62 bits used)
    pub generation: u64,
}

impl ContractId {
    /// Create a new contract ID with the given index and generation
    #[inline]
    pub const fn new(index: u32, generation: u64) -> Self {
        Self { index, generation }
    }

    /// Create an invalid contract ID
    #[inline]
    pub const fn invalid() -> Self {
        Self {
            index: u32::MAX,
            generation: u64::MAX,
        }
    }

    /// Check if this is an invalid contract ID
    #[inline]
    pub const fn is_invalid(self) -> bool {
        self.index == u32::MAX
    }
}

impl Default for ContractId {
    fn default() -> Self {
        Self::invalid()
    }
}

/// Invalid contract ID constant
pub const INVALID_CONTRACT_ID: ContractId = ContractId::invalid();

/// Bias state for fair contract selection across threads.
/// Each polling thread should maintain its own BiasState instance.
/// Uses bias flags that get advanced to cycle through different selections.
#[derive(Clone)]
pub struct BiasState {
    pub thread_id: u64,
    /// Bias flags - high bits consumed at each tree level to prefer left/right
    pub flags: u64,
    /// Hint from previous selection - records which branches had work
    pub hint: u64,
    pub cycle: u64,
    /// Summary bias for tree selection
    pub summary_bias: u64,
    pub summary_hint: u64,
    /// Counter for periodic randomness injection
    pub poll_count: u64,
    /// Counter for round-robin tree allocation (starts random, increments)
    pub alloc_counter: u64,
    pub hints: Box<[u64]>,
    pub contract_group: Option<ContractGroup>,
}

/// How often to inject fresh randomness (every N polls)
const RANDOMNESS_INJECTION_INTERVAL: u64 = 64;

impl Drop for BiasState {
    fn drop(&mut self) {
        if let Some(group) = self.contract_group.take() {
            group.drop_bias_state(self);
        }
    }
}

impl BiasState {
    /// Create a new bias state with randomized initial values.
    /// Each thread gets different starting points to reduce contention.
    #[inline]
    pub fn new() -> Self {
        let mut hints = Vec::with_capacity(65536);
        for _ in 0..65536 {
            hints.push(0);
        }
        Self {
            thread_id: 0,
            // flags: crate::util::random::next_u64(),
            // hint: crate::util::random::next_u64(),
            // summary_bias: crate::util::random::next_u64(),
            flags: 0,
            hint: 0,
            summary_bias: 0,
            summary_hint: crate::util::random::next_u64(),
            cycle: 0,
            poll_count: 0,
            alloc_counter: 0,
            // alloc_counter: crate::util::random::next_u64(),
            hints: hints.into_boxed_slice(),
            contract_group: None,
        }
    }

    /// Inject fresh randomness periodically to prevent long-term bias patterns.
    /// Call this after each successful poll.
    #[inline]
    pub fn maybe_inject_randomness(&mut self) {
        self.poll_count = self.poll_count.wrapping_add(1);
        if self.poll_count & (RANDOMNESS_INJECTION_INTERVAL - 1) == 0 {
            // Every 64 polls, XOR in fresh randomness
            // let fresh = crate::util::random::next_u64();
            // self.summary_bias ^= fresh;
            // self.flags ^= fresh >> 32;
            // self.hint ^= fresh >> 32;

            // crate::util::random::rapidrng_fast(&mut self.summary_bias);
            // crate::util::random::rapidrng_fast(&mut self.flags);
            // crate::util::random::rapidrng_fast(&mut self.hint);

            // self.summary_bias = crate::util::random::next_u64();
            // self.hint = crate::util::random::next_u64();
            // self.flags = crate::util::random::next_u64();
        } else {
            // self.summary_bias += 1;
            // self.flags += 1;
            // self.hint = self.hint.rotate_left(1);
        }
    }
}
