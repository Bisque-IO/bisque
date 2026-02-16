//! WorkContract - A handle to a scheduled task
//!
//! A work contract represents a task that can be scheduled for execution.
//! Contracts can be rescheduled, released, and moved but not copied.

use super::group::WorkContractGroupOps;
use super::WorkContractId;
use std::sync::{Arc, Weak};

/// Initial state of a work contract
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InitialState {
    /// Contract is not scheduled upon creation
    #[default]
    Unscheduled,
    /// Contract is scheduled for execution upon creation
    Scheduled,
}

/// A handle to a work contract
///
/// Work contracts are movable but not clonable. When dropped, the contract
/// is released (its release function will be called).
pub struct WorkContract {
    pub(crate) owner: Option<*const dyn WorkContractGroupOps>,
    pub(crate) release_token: Option<Arc<ReleaseToken>>,
    pub(crate) id: WorkContractId,
}

// Safety: WorkContract can be sent between threads
// The raw pointer is only used to call schedule/release on the owner
unsafe impl Send for WorkContract {}

impl WorkContract {
    /// Create a new invalid/empty work contract
    pub(crate) fn new() -> Self {
        Self {
            owner: None,
            release_token: None,
            id: 0,
        }
    }

    /// Create a work contract with the given parameters
    pub(crate) fn with_params(
        owner: *const dyn WorkContractGroupOps,
        release_token: Arc<ReleaseToken>,
        id: WorkContractId,
        initial_state: InitialState,
    ) -> Self {
        let contract = Self {
            owner: Some(owner),
            release_token: Some(release_token),
            id,
        };
        if initial_state == InitialState::Scheduled {
            contract.schedule();
        }
        contract
    }

    /// Schedule this contract for execution
    pub fn schedule(&self) {
        if let Some(owner) = self.owner {
            unsafe {
                (*owner).schedule(self.id);
            }
        }
    }

    /// Release this contract
    ///
    /// Returns true if the contract was successfully released,
    /// false if it was already released or invalid.
    pub fn release(&mut self) -> bool {
        if let Some(token) = self.release_token.take() {
            if token.schedule_release(self.id) {
                self.owner = None;
                return true;
            }
        }
        false
    }

    /// Deschedule this contract (not yet scheduled for next execution)
    ///
    /// This prevents the contract from being executed until it is scheduled again.
    pub fn deschedule(&self) -> bool {
        // In the C++ version this just clears the schedule flag
        // For now we don't implement this
        false
    }

    /// Check if this contract is valid
    pub fn is_valid(&self) -> bool {
        if let Some(ref token) = self.release_token {
            token.is_valid()
        } else {
            false
        }
    }

    /// Get the contract ID
    #[allow(dead_code)]
    pub(crate) fn get_id(&self) -> WorkContractId {
        self.id
    }
}

impl Default for WorkContract {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WorkContract {
    fn drop(&mut self) {
        self.release();
    }
}

/// Release token that tracks whether a contract group is still valid
pub struct ReleaseToken {
    group: std::sync::RwLock<Option<Weak<dyn WorkContractGroupOps>>>,
}

impl ReleaseToken {
    pub fn new(group: Weak<dyn WorkContractGroupOps>) -> Self {
        Self {
            group: std::sync::RwLock::new(Some(group)),
        }
    }

    pub fn schedule_release(&self, contract_id: WorkContractId) -> bool {
        let guard = self.group.read().unwrap();
        if let Some(ref weak) = *guard {
            if let Some(group) = weak.upgrade() {
                group.release(contract_id);
                return true;
            }
        }
        false
    }

    pub fn orphan(&self) {
        let mut guard = self.group.write().unwrap();
        *guard = None;
    }

    pub fn is_valid(&self) -> bool {
        let guard = self.group.read().unwrap();
        if let Some(ref weak) = *guard {
            weak.strong_count() > 0
        } else {
            false
        }
    }
}
