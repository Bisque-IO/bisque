//! ThisContract - Thread-local access to the currently executing contract
//!
//! Provides static methods to schedule or release the current contract
//! from within the contract's work function.

use super::WorkContractId;
use std::cell::Cell;

#[derive(Clone, Copy)]
struct ThisContractData {
    id: WorkContractId,
    group: *const (),
    schedule_fn: fn(WorkContractId, *const ()),
    release_fn: fn(WorkContractId, *const ()),
}

thread_local! {
    static CURRENT_CONTRACT: Cell<Option<ThisContractData>> = const { Cell::new(None) };
}

/// Provides access to the currently executing contract
///
/// This struct is created when a contract's work function is invoked
/// and provides static methods to schedule or release the current contract.
pub struct ThisContract {
    prev: Option<ThisContractData>,
}

impl ThisContract {
    /// Create a new ThisContract context
    #[doc(hidden)]
    #[inline(always)]
    pub fn new(
        id: WorkContractId,
        group: *const (),
        schedule_fn: fn(WorkContractId, *const ()),
        release_fn: fn(WorkContractId, *const ()),
    ) -> Self {
        let prev = CURRENT_CONTRACT.with(|c| c.get());
        CURRENT_CONTRACT.with(|c| {
            c.set(Some(ThisContractData {
                id,
                group,
                schedule_fn,
                release_fn,
            }))
        });
        Self { prev }
    }

    /// Schedule the current contract for execution
    #[inline(always)]
    pub fn schedule() {
        CURRENT_CONTRACT.with(|c| {
            if let Some(data) = c.get() {
                (data.schedule_fn)(data.id, data.group);
            }
        });
    }

    /// Release the current contract
    #[inline(always)]
    pub fn release() {
        CURRENT_CONTRACT.with(|c| {
            if let Some(data) = c.get() {
                (data.release_fn)(data.id, data.group);
            }
        });
    }

    /// Get the ID of the current contract
    #[inline(always)]
    pub fn get_id() -> Option<WorkContractId> {
        CURRENT_CONTRACT.with(|c| c.get().map(|d| d.id))
    }
}

impl Drop for ThisContract {
    #[inline(always)]
    fn drop(&mut self) {
        CURRENT_CONTRACT.with(|c| c.set(self.prev));
    }
}
