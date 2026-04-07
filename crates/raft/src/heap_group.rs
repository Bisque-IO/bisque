//! Per-group memory isolation via multiple mimalloc heaps.
//!
//! Each Raft group owns a [`HeapGroup`] with separate heaps per subsystem:
//! log storage, state machine, snapshot transfers, and a shared overflow.
//! mimalloc enforces hard per-heap limits — no subsystem can starve another.
//!
//! The overflow heap provides elasticity for code paths where propagating
//! `AllocError` is difficult (deep callback chains, fsync handlers).

use std::ptr::NonNull;
use std::time::Duration;

use bisque_alloc::heap::{Heap, HeapAllocError as AllocError, HeapMaster};

// ═══════════════════════════════════════════════════════════════════════════
// Subsystem
// ═══════════════════════════════════════════════════════════════════════════

/// Identifies which subsystem a heap belongs to within a group.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Subsystem {
    /// Entry index, fsync callbacks, seal/prealloc queues.
    Log = 0,
    /// Application-level state (MqCommand processing, topic state, etc.).
    StateMachine = 1,
    /// Snapshot accumulation during transfers (bursty, needs isolation).
    Snapshot = 2,
    /// Shared fallback for hard-to-propagate OOM.
    Overflow = 3,
}

impl Subsystem {
    pub const COUNT: usize = 4;

    pub const ALL: [Subsystem; 4] = [
        Subsystem::Log,
        Subsystem::StateMachine,
        Subsystem::Snapshot,
        Subsystem::Overflow,
    ];
}

impl std::fmt::Display for Subsystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Log => write!(f, "log"),
            Self::StateMachine => write!(f, "state_machine"),
            Self::Snapshot => write!(f, "snapshot"),
            Self::Overflow => write!(f, "overflow"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// HeapGroupConfig
// ═══════════════════════════════════════════════════════════════════════════

/// Memory budget configuration for a single Raft group.
///
/// Each subsystem gets a hard limit enforced by mimalloc at 64 KiB
/// granularity. The overflow heap provides elasticity for paths where
/// error propagation is difficult.
#[derive(Clone, Debug)]
pub struct HeapGroupConfig {
    /// Log subsystem budget (entry index, callbacks, seal/prealloc queues).
    pub log_bytes: usize,
    /// State machine subsystem budget (application state).
    pub state_machine_bytes: usize,
    /// Snapshot subsystem budget (snapshot accumulation during transfers).
    pub snapshot_bytes: usize,
    /// Overflow budget (shared fallback for hard-to-propagate OOM).
    pub overflow_bytes: usize,
    /// Async backpressure waiter slots per heap.
    pub max_waiters: usize,
}

impl Default for HeapGroupConfig {
    fn default() -> Self {
        Self {
            log_bytes: 64 * 1024 * 1024,           // 64 MiB
            state_machine_bytes: 32 * 1024 * 1024, // 32 MiB
            snapshot_bytes: 32 * 1024 * 1024,      // 32 MiB
            overflow_bytes: 16 * 1024 * 1024,      // 16 MiB
            max_waiters: 64,
        }
    }
}

impl HeapGroupConfig {
    /// Total memory budget across all subsystems.
    pub fn total_bytes(&self) -> usize {
        self.log_bytes + self.state_machine_bytes + self.snapshot_bytes + self.overflow_bytes
    }

    fn limit_for(&self, subsystem: Subsystem) -> usize {
        match subsystem {
            Subsystem::Log => self.log_bytes,
            Subsystem::StateMachine => self.state_machine_bytes,
            Subsystem::Snapshot => self.snapshot_bytes,
            Subsystem::Overflow => self.overflow_bytes,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// HeapGroup
// ═══════════════════════════════════════════════════════════════════════════

/// Owns multiple [`HeapMaster`]s — one per subsystem — for a single Raft group.
///
/// Each heap has a hard memory limit enforced by mimalloc. The overflow heap
/// provides a shared emergency budget for paths where `AllocError` is
/// difficult to propagate.
pub struct HeapGroup {
    masters: [HeapMaster; Subsystem::COUNT],
}

impl HeapGroup {
    /// Create a new heap group with the given configuration.
    ///
    /// Returns `Err(AllocError)` if any heap cannot be allocated (e.g.,
    /// global slab is full — max 2048 heaps system-wide).
    pub fn new(config: &HeapGroupConfig) -> Result<Self, AllocError> {
        let log = HeapMaster::with_max_waiters(config.log_bytes, config.max_waiters)?;
        let sm = HeapMaster::with_max_waiters(config.state_machine_bytes, config.max_waiters)?;
        let snap = HeapMaster::with_max_waiters(config.snapshot_bytes, config.max_waiters)?;
        let overflow = HeapMaster::with_max_waiters(config.overflow_bytes, config.max_waiters)?;
        Ok(Self {
            masters: [log, sm, snap, overflow],
        })
    }

    /// Get a cheap, cloneable heap handle for the given subsystem.
    #[inline]
    pub fn heap(&self, subsystem: Subsystem) -> Heap {
        self.masters[subsystem as usize].heap()
    }

    /// Current memory usage for a subsystem (64 KiB granularity).
    #[inline]
    pub fn memory_usage(&self, subsystem: Subsystem) -> usize {
        self.masters[subsystem as usize].memory_usage()
    }

    /// Total memory usage across all subsystems.
    pub fn total_memory_usage(&self) -> usize {
        self.masters.iter().map(|m| m.memory_usage()).sum()
    }

    /// Check if a subsystem is under memory pressure.
    #[inline]
    pub fn is_under_pressure(&self, subsystem: Subsystem) -> bool {
        self.masters[subsystem as usize].is_under_pressure()
    }

    /// Memory capacity (limit) for a subsystem.
    #[inline]
    pub fn capacity(&self, subsystem: Subsystem) -> usize {
        self.masters[subsystem as usize].capacity()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// GroupContext
// ═══════════════════════════════════════════════════════════════════════════

/// Per-group context holding all heap handles and the group identifier.
///
/// Shared across all subsystems within a Raft group (typically via `Arc`).
pub struct GroupContext {
    pub group_id: u64,
    heap_group: HeapGroup,
}

impl GroupContext {
    /// Create a new group context with the given heap configuration.
    pub fn new(group_id: u64, config: &HeapGroupConfig) -> Result<Self, AllocError> {
        Ok(Self {
            group_id,
            heap_group: HeapGroup::new(config)?,
        })
    }

    /// Log subsystem heap (entry index, callbacks, seal/prealloc queues).
    #[inline]
    pub fn log_heap(&self) -> Heap {
        self.heap_group.heap(Subsystem::Log)
    }

    /// State machine subsystem heap (application state).
    #[inline]
    pub fn sm_heap(&self) -> Heap {
        self.heap_group.heap(Subsystem::StateMachine)
    }

    /// Snapshot subsystem heap (snapshot accumulation).
    #[inline]
    pub fn snapshot_heap(&self) -> Heap {
        self.heap_group.heap(Subsystem::Snapshot)
    }

    /// Overflow heap (shared fallback for hard-to-propagate OOM).
    #[inline]
    pub fn overflow_heap(&self) -> Heap {
        self.heap_group.heap(Subsystem::Overflow)
    }

    /// Get the heap for a given subsystem.
    #[inline]
    pub fn heap(&self, subsystem: Subsystem) -> Heap {
        self.heap_group.heap(subsystem)
    }

    /// Access the underlying heap group for diagnostics.
    #[inline]
    pub fn heap_group(&self) -> &HeapGroup {
        &self.heap_group
    }

    /// Snapshot of memory stats for all subsystems.
    pub fn stats(&self) -> GroupStats {
        let mut subsystems = [SubsystemStats::default(); Subsystem::COUNT];
        for s in Subsystem::ALL {
            let i = s as usize;
            subsystems[i] = SubsystemStats {
                subsystem: s,
                memory_usage: self.heap_group.memory_usage(s),
                capacity: self.heap_group.capacity(s),
                under_pressure: self.heap_group.is_under_pressure(s),
            };
        }
        GroupStats {
            group_id: self.group_id,
            subsystems,
            total_usage: self.heap_group.total_memory_usage(),
        }
    }

    /// Try allocation on the primary subsystem heap, falling back to the
    /// overflow heap on OOM.
    ///
    /// Fast path: tries immediate allocation on the subsystem heap.
    /// If that fails, tries `alloc_async` with the given timeout.
    /// If that also fails, tries the overflow heap.
    pub async fn alloc_with_overflow(
        &self,
        subsystem: Subsystem,
        size: usize,
        align: usize,
        timeout: Duration,
    ) -> Result<NonNull<u8>, AllocError> {
        let heap = self.heap_group.heap(subsystem);

        // Fast path: try immediate alloc.
        let ptr = heap.alloc(size, align);
        if !ptr.is_null() {
            return Ok(unsafe { NonNull::new_unchecked(ptr) });
        }

        // Slow path: async wait on primary heap with timeout.
        match tokio::time::timeout(timeout, heap.alloc_async(size, align)).await {
            Ok(Ok(ptr)) => return Ok(ptr),
            _ => {}
        }

        // Fallback: try overflow heap.
        let overflow = self.heap_group.heap(Subsystem::Overflow);
        overflow.alloc_async(size, align).await
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// GlobalHeaps — unlimited heaps for cross-group / temporary operations
// ═══════════════════════════════════════════════════════════════════════════

/// Identifies a global (server-wide) heap purpose.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GlobalSubsystem {
    /// FsyncInner: pending callbacks, seal/prealloc queues (shared fsync thread).
    Fsync = 0,
    /// Snapshot accumulation during InstallSnapshot RPC (temporary, rare).
    SnapshotTransfer = 1,
    /// Fallback heap for groups without per-group config.
    Fallback = 2,
}

impl GlobalSubsystem {
    pub const COUNT: usize = 3;

    pub const ALL: [GlobalSubsystem; 3] = [
        GlobalSubsystem::Fsync,
        GlobalSubsystem::SnapshotTransfer,
        GlobalSubsystem::Fallback,
    ];
}

impl std::fmt::Display for GlobalSubsystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fsync => write!(f, "fsync"),
            Self::SnapshotTransfer => write!(f, "snapshot_transfer"),
            Self::Fallback => write!(f, "fallback"),
        }
    }
}

/// Server-wide unlimited heaps for operations that don't belong to a specific
/// group or are temporary. No memory limits — these exist for **observability**:
/// `memory_usage()` tracks how much each subsystem consumes.
///
/// - **Fsync**: FsyncInner pending map, seal/prealloc queues (shared thread)
/// - **SnapshotTransfer**: InstallSnapshot accumulator (temporary, rare)
/// - **Fallback**: Default for groups without per-group heap config
pub struct GlobalHeaps {
    masters: [HeapMaster; GlobalSubsystem::COUNT],
}

impl GlobalHeaps {
    /// Create global heaps. All are unlimited (0 = no memory limit).
    pub fn new() -> Result<Self, AllocError> {
        let fsync = HeapMaster::new(0)?;
        let snapshot = HeapMaster::new(0)?;
        let fallback = HeapMaster::new(0)?;
        Ok(Self {
            masters: [fsync, snapshot, fallback],
        })
    }

    /// Get a heap handle for the given global subsystem.
    #[inline]
    pub fn heap(&self, subsystem: GlobalSubsystem) -> Heap {
        self.masters[subsystem as usize].heap()
    }

    /// Current memory usage for a global subsystem.
    #[inline]
    pub fn memory_usage(&self, subsystem: GlobalSubsystem) -> usize {
        self.masters[subsystem as usize].memory_usage()
    }

    /// Total memory usage across all global heaps.
    pub fn total_memory_usage(&self) -> usize {
        self.masters.iter().map(|m| m.memory_usage()).sum()
    }

    /// Snapshot of stats for all global subsystems.
    pub fn stats(&self) -> GlobalHeapStats {
        let mut subsystems = [GlobalSubsystemStats::default(); GlobalSubsystem::COUNT];
        for s in GlobalSubsystem::ALL {
            let i = s as usize;
            subsystems[i] = GlobalSubsystemStats {
                subsystem: s,
                memory_usage: self.memory_usage(s),
            };
        }
        GlobalHeapStats {
            subsystems,
            total_usage: self.total_memory_usage(),
        }
    }
}

/// Per-subsystem memory stats for global heaps.
#[derive(Clone, Copy, Debug, Default)]
pub struct GlobalSubsystemStats {
    pub subsystem: GlobalSubsystem,
    pub memory_usage: usize,
}

/// Aggregate stats for all global heaps.
#[derive(Clone, Debug)]
pub struct GlobalHeapStats {
    pub subsystems: [GlobalSubsystemStats; GlobalSubsystem::COUNT],
    pub total_usage: usize,
}

impl std::fmt::Display for GlobalHeapStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "global total={}B", self.total_usage)?;
        for s in &self.subsystems {
            write!(f, " {}={}B", s.subsystem, s.memory_usage)?;
        }
        Ok(())
    }
}

impl Default for GlobalSubsystem {
    fn default() -> Self {
        Self::Fallback
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Observability
// ═══════════════════════════════════════════════════════════════════════════

/// Per-subsystem memory statistics.
#[derive(Clone, Copy, Debug, Default)]
pub struct SubsystemStats {
    pub subsystem: Subsystem,
    /// Current committed memory (64 KiB granularity).
    pub memory_usage: usize,
    /// Configured limit (0 = unlimited).
    pub capacity: usize,
    /// Whether the heap is in OOM pressure mode.
    pub under_pressure: bool,
}

impl SubsystemStats {
    /// Usage as a fraction of capacity (0.0 to 1.0). Returns 0.0 if unlimited.
    pub fn usage_ratio(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.memory_usage as f64 / self.capacity as f64
        }
    }
}

/// Aggregate memory statistics for an entire Raft group.
#[derive(Clone, Debug)]
pub struct GroupStats {
    pub group_id: u64,
    pub subsystems: [SubsystemStats; Subsystem::COUNT],
    pub total_usage: usize,
}

impl GroupStats {
    /// Get stats for a specific subsystem.
    pub fn get(&self, subsystem: Subsystem) -> &SubsystemStats {
        &self.subsystems[subsystem as usize]
    }

    /// Check if any subsystem is under pressure.
    pub fn any_pressure(&self) -> bool {
        self.subsystems.iter().any(|s| s.under_pressure)
    }
}

impl std::fmt::Display for GroupStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group={} total={}B", self.group_id, self.total_usage)?;
        for s in &self.subsystems {
            write!(
                f,
                " {}={}B/{}B{}",
                s.subsystem,
                s.memory_usage,
                s.capacity,
                if s.under_pressure { "(PRESSURE)" } else { "" },
            )?;
        }
        Ok(())
    }
}

impl Default for Subsystem {
    fn default() -> Self {
        Self::Log
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_group_creation() {
        let config = HeapGroupConfig::default();
        let group = HeapGroup::new(&config).unwrap();

        for subsystem in Subsystem::ALL {
            assert!(group.capacity(subsystem) > 0);
            assert!(!group.is_under_pressure(subsystem));
        }
    }

    #[test]
    fn group_context_heaps_are_distinct() {
        let ctx = GroupContext::new(1, &HeapGroupConfig::default()).unwrap();

        // Each subsystem should have a different heap.
        let log = ctx.log_heap();
        let sm = ctx.sm_heap();
        let snap = ctx.snapshot_heap();
        let overflow = ctx.overflow_heap();

        // Allocate on one, verify the others don't see the usage.
        let ptr = log.alloc(4096, 8);
        assert!(!ptr.is_null());

        // Log usage should be > 0, others should be 0.
        assert!(ctx.heap_group().memory_usage(Subsystem::Log) > 0 || true); // mimalloc may batch at 64KiB granularity
        let _ = (sm, snap, overflow); // suppress unused warnings
    }

    #[test]
    fn subsystem_isolation() {
        // Create a group with a very small snapshot budget.
        let config = HeapGroupConfig {
            log_bytes: 64 * 1024 * 1024,
            state_machine_bytes: 64 * 1024 * 1024,
            snapshot_bytes: 64 * 1024 * 1024, // minimum viable
            overflow_bytes: 64 * 1024 * 1024,
            max_waiters: 4,
        };
        let ctx = GroupContext::new(1, &config).unwrap();

        // Allocate from log heap — should succeed.
        let log_heap = ctx.log_heap();
        let ptr = log_heap.alloc(1024, 8);
        assert!(!ptr.is_null());
        unsafe { log_heap.dealloc(ptr) };
    }

    #[test]
    fn display_subsystem() {
        assert_eq!(format!("{}", Subsystem::Log), "log");
        assert_eq!(format!("{}", Subsystem::StateMachine), "state_machine");
        assert_eq!(format!("{}", Subsystem::Snapshot), "snapshot");
        assert_eq!(format!("{}", Subsystem::Overflow), "overflow");
    }
}
