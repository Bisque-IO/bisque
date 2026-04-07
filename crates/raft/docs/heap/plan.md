Plan: Integrate bisque-alloc with bisque-raft for Per-Group Memory Isolation

## Context

bisque-raft currently uses `std::Vec`, `bytes::BytesMut`, `std::collections::HashMap`, and the global allocator for all memory. A single Raft group that accumulates large snapshots, builds large entry indices, or queues many fsync callbacks can consume unbounded memory and OOM the entire process — taking down all other groups.

bisque-alloc provides per-heap memory limits (mimalloc-backed), async backpressure (`alloc_async` with pre-allocated waiter slots), and fallible collections. The goal is to give each Raft group a **set of isolated heaps** so no single subsystem within a group — and no single group within the cluster — can starve the rest.

The transport TCP layer stays on the global allocator (infallible). It feeds into the raft log, which is heap-bounded, so backpressure propagates naturally via async flow control.

---

## Architecture: Multi-Heap Group Isolation

### HeapGroup — Multiple Real Heaps Per Raft Group

Each Raft group owns a `HeapGroup` with **separate mimalloc HeapMasters** per subsystem:

```
GroupId=1 → HeapGroup {
    log:           HeapMaster(log_limit),       // entry index, callbacks, seal/prealloc queues
    state_machine: HeapMaster(sm_limit),        // application state (MqCommand, topics, etc.)
    snapshot:      HeapMaster(snap_limit),       // snapshot accumulation during transfers
    overflow:      HeapMaster(overflow_limit),   // shared fallback for hard-to-propagate OOM
}
```

**Why real heaps, not soft limits:** mimalloc enforces hard per-heap limits at 64 KiB granularity with zero overshoot. Application-level soft limits would require double-accounting on every allocation. Real heaps give us hardware-enforced isolation with zero hot-path overhead.

**The overflow heap:** When a subsystem allocation fails and error propagation is difficult (e.g., deep in a callback chain), the code falls back to the overflow heap. This provides elasticity without compromising subsystem isolation. The overflow heap has its own limit — it's the group's "emergency budget," not unlimited.

**Slab capacity:** With 4 heaps/group and 2048 slab slots → 512 max groups. This is sufficient for most deployments. The slab size (`FixedSlab<HeapData, N>`) is a const generic and can be increased later if needed.

### Backpressure Strategy

- **Default path:** `heap.alloc_async(size, align).await` with a timeout — waits for memory to free up
- **Fast-fail paths:** Snapshot install can reject chunks immediately when snapshot heap is full (leader retries)
- **Overflow fallback:** For places where `Result<_, AllocError>` is difficult to propagate, try overflow heap before failing
- **Propagation chain:** Heap full → alloc_async blocks → async task stalls → upstream queue fills → TCP backpressure → sender slows down

---

## Phase 0A: Fill bisque-alloc Collection API Gaps

### Files to modify:
- `crates/alloc/src/vec.rs`
- `crates/alloc/src/collections/hash_map.rs`

### Vec gaps:
1. **`drain(range)` → `Drain<'_, T>` iterator** — infallible (no allocation), needed by rpc_server.rs and storage_mmap.rs
2. **`sort_unstable()` / `sort_unstable_by()`** — delegate to slice methods, infallible
3. **`remove(index) → T`** — shift elements left, infallible, needed by various cleanup paths
4. **`insert(index, val) → Result<(), AllocError>`** — shift elements right, may grow
5. **`extend_from_iter(iter) → Result<(), AllocError>`** — fallible extend from iterator (non-Copy types)

### HashMap gaps:
1. **`entry(key) → Entry<'_, K, V>`** — expose hashbrown's Entry API with try_reserve before insert. Needed by FsyncInner (storage_mmap.rs:1184)
2. **`drain() → Drain<'_, K, V>`** — infallible drain iterator
3. **`into_iter()`** — consuming IntoIterator impl

### Already present (no action needed):
- `SmallVec<T, N>` with heap spill — already in vec.rs:452-793
- `Vec::retain`, `Vec::append`, `Vec::swap_remove`, `Vec::truncate`, `Vec::clear`
- `HashMap::retain`, `HashMap::get_mut`, `HashMap::remove`, `HashMap::iter_mut`
- `HeapArc<T>` (fallible Arc), `HeapBox<T>` (fallible Box)
- `Bytes` / `BytesMut` with heap-backed allocation

---

## Phase 0B: Consolidate ART + Build ShardedFixedArt

### Goal
Replace `DashMap` and `papaya::HashMap` usage in bisque-raft with a heap-backed, concurrent, ordered ART that supports generic keys via `ArtKey<K>`. This gives us a concurrent map that participates in per-group memory budgets natively.

### Step 1: Consolidate `fixed/` and `fixed2/`

The old `fixed/` lacks the `ArtKey<K>` trait and is less capable. Replace it:

- **Remove** `crates/alloc/src/collections/art/fixed/` (old, `[u8; K]`-only, no ArtKey trait)
- **Move** `crates/alloc/src/collections/art/fixed2/` → `crates/alloc/src/collections/art/fixed/`
- **Update** `crates/alloc/src/collections/art/mod.rs` — `pub mod fixed;` now points to the ArtKey-based implementation
- **Re-export** `ArtKey` trait from `crates/alloc/src/collections/mod.rs` and `crates/alloc/src/lib.rs`

After this, `FixedArt<K, V>` with `ArtKey<K>` is the canonical fixed-key ART.

### Step 2: Create `ShardedFixedArt<K, V>` in `fixed/`

New file: `crates/alloc/src/collections/art/fixed/sharded.rs`

Combines ShardedArt's DashMap-like inline-operation model with FixedArt's generic keys:

```rust
/// Concurrent ART with fixed-size keys, inline operations, and per-shard garbage.
///
/// Unlike FixedArt (which uses WriteGuard + explicit publish), ShardedFixedArt
/// auto-publishes after each operation — behaving like DashMap but ordered,
/// heap-backed, and epoch-safe.
pub struct ShardedFixedArt<const K: usize, V> {
    published_root: AtomicUsize,
    batch_root: UnsafeCell<usize>,
    shard_locks: Box<[Mutex<crate::Vec<usize>>; 256]>,  // per-shard garbage
    generation: UnsafeCell<u64>,
    epoch: Epoch,
    heap: Heap,
    _marker: PhantomData<V>,
}
```

**Key API surface (DashMap-compatible):**

```rust
impl<const K: usize, V> ShardedFixedArt<K, V> {
    pub fn new(collector: &Collector, heap: &Heap) -> Self;
    pub fn heap(&self) -> &Heap;

    // Read (epoch-pinned)
    pub fn read(&self) -> ShardedReadGuard<'_, K, V>;
    pub fn get(&self, key: &[u8; K]) -> Option<&V>;     // pin + lookup
    pub fn find<Q: ArtKey<K>>(&self, key: Q) -> Option<&V>;

    // Write (inline, auto-publish per-op is optional — publish() for batch)
    pub fn put(&self, key: impl ArtKey<K>, val: V) -> Result<Option<V>, AllocError>;
    pub fn delete(&self, key: impl ArtKey<K>) -> Result<Option<V>, AllocError>;

    // Compute (DashMap-style atomic read-modify-write)
    pub fn compute_if_present<Q: ArtKey<K>>(
        &self, key: Q, f: impl FnOnce(&V) -> Option<V>,
    ) -> Result<Option<(V, Option<V>)>, AllocError>;

    pub fn compute_or_insert<Q: ArtKey<K>>(
        &self, key: Q, f: impl FnOnce() -> V,
    ) -> Result<&V, AllocError>;

    pub fn compare_exchange<Q: ArtKey<K>>(
        &self, key: Q, old: &V, new: Option<V>,
    ) -> Result<Option<V>, Option<V>>
    where V: PartialEq;

    // Batch publish (swap roots, seal garbage)
    pub fn publish(&self) -> Result<(), AllocError>;
}
```

**Design decisions:**
- **Per-shard garbage** (`Mutex<Vec<usize>>`) instead of WriteGuard — enables inline operations without holding a guard across multiple ops
- **`V` is not required to be `Copy`** (unlike old ShardedArt) — values stored by ownership in `LeafNode<K, V>`, moved via `ptr::read`
- **Reuses `fixed2`'s node types** (`LeafNode<K, V>`, `Node4/16/48/256`), COW module, and epoch dealloc — only the top-level concurrency model differs
- **`ArtKey<K>` throughout** — `u32`, `u64`, `u128`, `i32`, `i64`, `i128`, `[u8; K]` all work zero-cost

### Step 3: Type aliases for common use cases

```rust
// In crates/alloc/src/collections/art/fixed/sharded.rs
pub type ShardedU32Art<V> = ShardedFixedArt<4, V>;
pub type ShardedU64Art<V> = ShardedFixedArt<8, V>;
pub type ShardedU128Art<V> = ShardedFixedArt<16, V>;
```

### Step 4: Export from lib.rs

```rust
// In crates/alloc/src/lib.rs
pub use collections::art::fixed::{
    FixedArt, ShardedFixedArt, ArtKey,
    U32Art, U64Art, U128Art,
    ShardedU32Art, ShardedU64Art, ShardedU128Art,
};
```

### What this replaces in bisque-raft:
- `DashMap<u64, T>` → `ShardedU64Art<T>` (concurrent, ordered, heap-backed)
- The old usize-only `ShardedArt<V>` in `sharded.rs` can be deprecated or kept as a thin wrapper

---

## Phase 1: HeapGroup + GroupContext Scaffolding

### New file: `crates/raft/src/heap_group.rs`

```rust
/// Memory budget configuration for a single Raft group.
pub struct HeapGroupConfig {
    pub log_bytes: usize,           // e.g. 64 MiB
    pub state_machine_bytes: usize, // e.g. 32 MiB
    pub snapshot_bytes: usize,      // e.g. 32 MiB
    pub overflow_bytes: usize,      // e.g. 16 MiB
    pub max_waiters: usize,         // async backpressure slots per heap (default 64)
}

/// Identifies which subsystem a heap belongs to.
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum Subsystem {
    Log = 0,
    StateMachine = 1,
    Snapshot = 2,
    Overflow = 3,
}

/// Owns multiple HeapMasters — one per subsystem — for a single Raft group.
pub struct HeapGroup {
    masters: [HeapMaster; 4],       // indexed by Subsystem
}

impl HeapGroup {
    pub fn new(config: &HeapGroupConfig) -> Result<Self, AllocError> { ... }
    pub fn heap(&self, subsystem: Subsystem) -> Heap { ... }
    pub fn memory_usage(&self, subsystem: Subsystem) -> usize { ... }
    pub fn total_memory_usage(&self) -> usize { ... }
    pub fn is_under_pressure(&self, subsystem: Subsystem) -> bool { ... }
}

/// Per-group context shared across all subsystems. Wraps HeapGroup in Arc.
pub struct GroupContext {
    pub group_id: u64,
    heap_group: HeapGroup,
}

impl GroupContext {
    pub fn log_heap(&self) -> Heap { self.heap_group.heap(Subsystem::Log) }
    pub fn sm_heap(&self) -> Heap { self.heap_group.heap(Subsystem::StateMachine) }
    pub fn snapshot_heap(&self) -> Heap { self.heap_group.heap(Subsystem::Snapshot) }
    pub fn overflow_heap(&self) -> Heap { self.heap_group.heap(Subsystem::Overflow) }
}
```

### Modify: `crates/raft/Cargo.toml`
- Add `bisque-alloc = { workspace = true }` dependency

### Modify: `crates/raft/src/lib.rs`
- Add `pub mod heap_group;`

---

## Phase 2: Wire GroupContext into Storage

### File: `crates/raft/src/storage_mmap.rs`

1. Add `heap_group_config: Option<HeapGroupConfig>` to `MmapStorageConfig`
2. Add `ctx: Option<Arc<GroupContext>>` to `MmapGroupState`
3. Create `GroupContext` during `MmapGroupState` initialization when config provides limits
4. Expose `pub fn group_context(&self) -> Option<&Arc<GroupContext>>` on `MmapGroupLogStorage`
5. Thread `ctx` through to `WriterState` and `FsyncState`

No collection migrations yet — just plumbing the handles through.

---

## Phase 3: Migrate Storage Hot Spots

### File: `crates/raft/src/storage_mmap.rs`

Migrate in order of risk (lowest first). Each migration:
- Swaps `std::vec::Vec<T>` → `bisque_alloc::Vec<T>` (or `HashMap`)
- Threads the log heap through via `ctx.log_heap()`
- Makes `push()`/`insert()` calls return `Result` and propagates errors

### Targets:
1. **`SegmentEntryIndex::offsets: Vec<(u64, u32)>`** (line 572) → `bisque_alloc::Vec`. Built once per segment pin, then read-only. Low risk.
2. **`purged_segments: Arc<Mutex<Vec<u64>>>`** (line 2120) → `bisque_alloc::Vec<u64>`. Append-only, drained periodically.
3. **`FsyncEntry::callbacks: Vec<IOFlushed<C>>`** (line 1083) → `bisque_alloc::Vec`. Grouped by segment.
4. **`FsyncInner::pending: HashMap<usize, FsyncEntry<C>>`** (line 1088) → `bisque_alloc::HashMap`. Requires entry() API from Phase 0.
5. **`seal_queue: Vec<SealRequest>` / `prealloc_queue: Vec<PreallocRequest>`** (lines 1090, 1092) → `bisque_alloc::Vec`.

---

## Phase 4: Migrate Snapshot Accumulation

### File: `crates/raft/src/rpc_server.rs`

1. `SnapshotAccumulator::data: Vec<u8>` (line ~53) → `bisque_alloc::Vec<u8>` using `ctx.snapshot_heap()`
2. `SnapshotTransferManager` looks up `GroupContext` for target group when creating accumulators
3. `append_chunk()` becomes fallible — returns `Result<bool, AllocError>`
4. When snapshot heap is full, reject `InstallSnapshot` RPC with backpressure signal (leader retries with backoff)

---

## Phase 5: Overflow Fallback Pattern

Establish a utility for the "try primary, fall back to overflow" pattern:

```rust
impl GroupContext {
    /// Try allocation on subsystem heap, fall back to overflow on OOM.
    pub async fn alloc_with_overflow(
        &self,
        subsystem: Subsystem,
        size: usize,
        align: usize,
        timeout: Duration,
    ) -> Result<NonNull<u8>, AllocError> {
        let heap = self.heap_group.heap(subsystem);
        // Fast path: try immediate alloc
        if let Ok(ptr) = heap.try_alloc(size, align) {
            return Ok(ptr);
        }
        // Try async wait on primary heap
        match tokio::time::timeout(timeout, heap.alloc_async(size, align)).await {
            Ok(Ok(ptr)) => Ok(ptr),
            _ => {
                // Fall back to overflow heap
                self.overflow_heap()
                    .alloc_async(size, align)
                    .await
            }
        }
    }
}
```

Wire this into the storage paths where error propagation is difficult (deep callback chains, fsync completion handlers).

---

## Phase 6: Observability

- Export per-subsystem metrics: `heap_group_memory_usage_bytes{group_id, subsystem}`
- Export pressure events: `heap_group_pressure_total{group_id, subsystem}`
- Add `GroupContext::stats() → GroupStats` for dashboard/admin endpoints
- Log warnings when overflow heap is used (indicates subsystem budget is too tight)

---

## What Stays on Global Allocator

| Component | Reason |
|-----------|--------|
| Transport TCP encode buffers | Per-thread, not per-group. Infallible by design. |
| Transport TCP read buffers | Per-connection `BytesMut`. Backpressure comes from downstream heap bounds. |
| `crossfire` channels | Pre-allocated bounded arrays. Already have item-count backpressure. |
| `FuturesUnordered` | Tokio runtime internals. Bounded by `max_concurrent_requests`. |
| OpenRaft internals | Uses `std::vec::Vec` via serde. Not practical to change without forking. |

## What Migrates to Heap-Backed ART

| Current | Replacement | Benefit |
|---------|-------------|---------|
| `DashMap<u64, T>` in manager/network | `ShardedU64Art<T>` on group heap | Per-group memory isolation, ordered, no rehash storms |
| `HashMap<usize, FsyncEntry>` in storage | `bisque_alloc::HashMap` on log heap | Memory-bounded fsync tracking |
| `congee` ART index in storage | `FixedArt` / `ShardedFixedArt` on log heap | Unified ART implementation, heap-backed |

---

## Verification Plan

1. **Unit tests in bisque-alloc:** Test new `drain()`, `entry()`, `sort_unstable()` APIs
2. **ShardedFixedArt tests:** Insert/get/delete with `u64`, `u128`, `[u8; 32]` keys; non-Copy values (String); compute_if_present; compare_exchange; concurrent read/write from multiple threads; publish snapshot consistency
3. **ShardedFixedArt memory isolation:** Create tree on a small heap (e.g. 4 MiB), insert until heap full, verify `AllocError` returned (not panic/OOM)
4. **Unit test HeapGroup:** Create group, allocate from each subsystem, verify isolation (one heap full doesn't block others)
5. **Integration test:** Configure a group with small heap limits, write entries until log heap fills, verify `alloc_async` backpressure activates
6. **Snapshot pressure test:** Send oversized snapshot chunks, verify rejection when snapshot heap is full
7. **Overflow test:** Trigger OOM on primary heap, verify fallback to overflow succeeds
8. **Existing raft tests:** Run full test suite to verify no regressions (`cargo test -p bisque-raft`)
9. **Multi-group isolation test:** Run 2+ groups with tight budgets, verify one group under pressure doesn't affect the other
10. **DashMap→ShardedFixedArt migration test:** Verify manager group routing still works correctly after DashMap replacement
