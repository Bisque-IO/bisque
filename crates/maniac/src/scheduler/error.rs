//! Error types for future contracts

use super::ContractId;

/// Error returned when spawning a future fails
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    /// No available contract slots - the group is at capacity
    NoCapacity,
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnError::NoCapacity => write!(f, "no available contract slots"),
        }
    }
}

impl std::error::Error for SpawnError {}

/// Handler for panics that occur during future polling
pub type PanicHandler = Box<dyn Fn(ContractId, Box<dyn std::any::Any + Send>) + Send + Sync>;

/// Default panic handler that resumes the panic
pub(crate) fn default_panic_handler(_id: ContractId, e: Box<dyn std::any::Any + Send>) {
    std::panic::resume_unwind(e);
}
