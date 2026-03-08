//! Storage traits for Multi-Raft
//!
//! The storage layer is designed to multiplex multiple raft groups:
//! - A single LogStorage instance can handle entries for multiple groups
//! - Multiple LogStorage instances can be used to shard groups
//! - State machines are external - each group provides its own

use openraft::OptionalSend;
use openraft::OptionalSync;
use openraft::RaftTypeConfig;
use openraft::storage::RaftLogStorage;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Trait for a storage backend that provides log storage for multiple Raft groups.
///
/// Each storage instance can handle multiple groups. The system can have
/// multiple storage instances to shard groups across different backends.
///
/// State machines are NOT part of this trait - they are provided externally
/// per group since different groups may have completely different state machines.
pub trait MultiRaftLogStorage<C>: OptionalSend + OptionalSync + 'static
where
    C: RaftTypeConfig,
{
    /// The log storage type for individual groups
    type GroupLogStorage: RaftLogStorage<C>;

    /// Get the log storage for a specific group (async version).
    ///
    /// This returns a handle to the log storage for a single group.
    /// Multiple calls with the same group_id should return handles
    /// that share the same underlying state.
    ///
    /// The first call for a new group_id may perform recovery operations
    /// using async I/O.
    fn get_log_storage(&self, group_id: u64) -> impl Future<Output = Self::GroupLogStorage> + Send;

    /// Remove a group from this storage.
    ///
    /// Called when a group is deleted or moved to another storage.
    fn remove_group(&self, group_id: u64);

    /// Get list of all group IDs currently in this storage.
    fn group_ids(&self) -> Vec<u64>;

    /// Get the purge floor handle for a specific group.
    ///
    /// The state machine layer can hold this `Arc<AtomicU64>` and update it
    /// to prevent log purging below a certain index. Returns `None` if the
    /// group has not been initialized yet.
    fn get_purge_floor(&self, group_id: u64) -> Option<Arc<AtomicU64>>;

    /// Stop background threads and release resources.
    ///
    /// Implementations with background threads (fsync, manifest workers)
    /// should override this to join those threads and close databases.
    /// Default is a no-op.
    fn stop(&self) {}
}
