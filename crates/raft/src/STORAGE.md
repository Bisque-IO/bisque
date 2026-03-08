## `storage_impl.rs` design: Per-group segmented log storage

This document describes the **current** design implemented in `crates/raft/src/multi/storage_impl.rs`.

It focuses on:
- **On-disk format** and crash-safety.
- **Per-group sharding** and durability callback semantics.
- **Recovery** and the **disk-backed read** path.
- **Indexing** (u64-keyed, `congee`-backed with free-list).
- **Caching**, compaction, and known trade-offs.

---

### Goals

- **Per Raft group isolation**: each group has independent lifecycle and no cross-group locking.
- **High write throughput**: group appends are batched and serialized through a per-group writer task.
- **Durable flush semantics**: `IOFlushed` callbacks are completed only after the corresponding writes are **fsync’d**.
- **Crash recovery**: tail corruption/partial writes are detected and truncated; state is rebuilt by replay.
- **Disk-backed reads**: reads work after restart and after cache eviction.

---

### Storage layout

- **Base directory**: `StorageConfig.base_dir`.
- **Per-group directory**: `{base_dir}/{group_id}/`.
- **Segment naming**: `group-{group_id}-{segment_id}.log`.
  - `segment_id` increases monotonically per group.

---

### Record format (per segment)

Each record is:

```
[len: u32 LE][type: u8][group_id: u24 LE][payload...][crc64: u64 LE]
```

- `len` is the number of bytes **after** the `len` field (i.e., `type + group_id + payload + crc64`).
- `group_id` is encoded as 3 bytes in little-endian format (maximum value: 16,777,215).
- `crc64` is computed over `[type + group_id + payload]` using **crc64fast-nvme** (excludes the length prefix).
- Minimum record size: 16 bytes (4 + 1 + 3 + 8).
- **Header alignment**: The 8-byte header `[len + type + group_id]` is now 64-bit aligned, improving memory access efficiency.

#### Record types

- Raft:
  - `Vote = 0x01`
  - `Entry = 0x02`
  - `Truncate = 0x03`
  - `Purge = 0x04`

---

### Sharding model and concurrency

#### Per-group sharding
### Sharding model and concurrency

#### Per-group sharding

- There is a **1:1 mapping** between a Raft group and a shard/writer task.
- `PerGroupLogStorage` maintains a `GroupIndex<C>` with **fixed-size atomic slots** (constant `MAX_GROUPS` array), not a `DashMap`.
- `GroupIndex` provides **O(1) lock-free access** using `AtomicPtr<GroupState<C>>` slots indexed by `group_id`.
- `get_log_storage(group_id)` returns a `GroupLogStorage` handle for that group.

**Note**: `group_id` is stored as `u64` in memory but encoded as **u24** (3 bytes) on disk, supporting up to 16,777,215 groups.

#### Writer serialization (no mutex on the hot path)

- Each group spawns a single **writer task** (`ShardState::writer_loop`) that owns the group's `SegmentedLog`.
- All writes are sent through a bounded `crossfire` mpsc channel (`256` capacity).
- The writer loop **opportunistically drains** queued requests (`try_recv()`) to reduce wakeups and batch writes.
- **Sticky failure**: on write/sync error, the shard enters a failed state; all pending callbacks are failed and future writes fail-fast.

---

### Durability semantics (`IOFlushed`)

OpenRaft supplies an `IOFlushed<C>` callback to `append()`. This implementation provides **durable** semantics:

- A callback is only completed after the writer performs a durable `sync_all()` that covers the corresponding write.
- Writes and callbacks are propagated through the writer channel as a single request:
  - `WriteRequest::Write { data, callbacks, sync_immediately, respond_to, entry_index_range }`

#### Fsync policy

Configured by `StorageConfig.fsync_interval`:

- **`None` (fully durable)**:
  - Every write implies an immediate `sync_all()`.
  - Callback completion happens after that sync.
  - **No batching**: each write fsyncs individually.

- **`Some(interval)` (group-commit/batch)**:
  - Writes are appended immediately to the segment.
  - `sync_all()` happens **once per batch** after draining the queue.
  - Callbacks are queued and completed together after the fsync.
  - Enables higher throughput by amortizing fsync cost across multiple writes.

#### Error propagation

- The writer maintains a **sticky failure** state (`ShardState.failed: OnceLock<ShardError>`).
- On write/sync error:
  - The shard enters failed state.
  - Pending callbacks are completed with an error.
  - Future writes fail-fast via `self.failed.get()` check.

---

### Segmentation

- Segment rotation: `current_size + data_len > segment_size`.
- **Segment metadata** tracked per segment:
  - `min_entry_index`, `max_entry_index` (best-effort, updated on writes)
  - `min_ts`, `max_ts` (unix nanos, for temporal queries/compaction)
  - Helps make **compaction decisions O(#segments)** without scanning content.
- Rotation fsyncs the old segment and opens a new segment file.
- `WritePlacement { segment_id, offset }` returned to caller to build disk index.

---

### In-memory state per group (`GroupState`)

`GroupState` maintains:

- **Vote**: stored in `AtomicVote` (packed atomics with seqlock).
- **Log state**:
  - `first_index`, `last_index` (`AtomicU64`)
  - `last_log_id`, `last_purged_log_id` (`AtomicLogId`)
- **Cache**: `DashMap<u64, Arc<Entry>>` storing a window of recent entries.
  - `cache_low` tracks the inclusive lower bound of the cache window.
  - Entries `< cache_low` are evicted to bound memory.
- **Disk index**: `LogIndex` maps entry index → on-disk location (`LogLocation`).
- **Shard handle**: `Arc<ShardState<C>>` for sending writes to writer task.

**Note**: The `group_id` field in `AtomicVote` and `AtomicLogId` packing is encoded as `u64` for simplicity, which is larger than the **u24** disk format but ensures clean atomic operations.

#### Atomic packing details

##### `AtomicVote` (64-bit packing)

```
high: [term: 64 bits]
low:  [node_id: 62 bits][committed: 1][valid: 1]
```

- **node_id limit**: `0x3FFFFFFFFFFFFFFF` (62 bits, enforced with `assert!`).

##### `AtomicLogId` (128-bit packing with seqlock)

```
high: [term: 40 bits][node_id: 24 bits]
low:  [index: 63 bits][valid: 1 bit]
```

- **Packing constraints** (enforced with `assert!`):
  - `node_id <= 0xFF_FFFF` (24 bits)
  - `term <= (1 << 40) - 1` (40 bits)
- **Seqlock**: odd seq = write in progress; even seq = stable; readers spin/retry on change.

---

### Disk index design (`congee` + free-list)

The implementation replaces `BTreeMap<u64, LogLocation>` with a `congee`-backed structure:

#### Why a wrapper is needed

`congee::U64Congee<V>` stores values as `usize` (via `V: From<usize> + Into<usize>`), which is not suitable for storing `LogLocation` directly.

`LogIndex` structure:
- `U64Congee<usize>`: maps `u64` entry index → **slot index**
- `Vec<LogLocation>`: stores the actual `{segment_id, offset, len}`
- `free: Vec<usize>`: free-list for reusing slots on deletes

#### Operations

- `get(index) -> Option<LogLocation>`: O(log n) concurrent map lookup + vector indexing
- `insert(index, LogLocation) -> io::Result<()>`: allocates or reuses slot, updates map
- `remove(index)`: removes from map, pushes slot to free-list
- `truncate_from(first_removed)`: batch removes range `[first_removed, u64::MAX]`
- `purge_to(last_removed)`: batch removes range `[0, last_removed]`

Both range operations use `U64Congee::range()` in batches (256 keys per batch) to avoid long critical sections.

---

### Read path (cache + disk)

`try_get_log_entries(range)` returns a contiguous set of entries starting at `range.start`.

For each index:

1. Try `cache` (DashMap lookup).
2. On miss: consult `LogIndex` and read the record from disk asynchronously.
3. Decode the entry payload.
4. If within cache window, insert into cache.
5. If any index is missing, stop and return partial results.

#### Async I/O with alignment fallbacks

Reads use Maniac's async filesystem layer (io-uring on Linux, poll-based on other platforms). The implementation handles direct-I/O alignment constraints with a three-tier fallback strategy:

1. **Primary path**: `file.read_exact_at()` with aligned buffers (DIO when supported)
2. **Fallback for EINVAL**: `read_exact_at_unaligned_into()` for unaligned access
3. **Last resort**: `maniac::blocking::unblock_fread_at()` on a thread pool for problematic configurations

This approach provides **portable, correct async I/O** while maximizing performance where alignment constraints don't apply.

---

### Cache policy (bounded memory)

Configured by `StorageConfig.max_cache_entries_per_group` (default: `0` = cache disabled).

- Cache is treated as a **hot window** near `last_index`.
- The start of the window is computed as:
  ```rust
  cache_window_start = max(first_index, last_index - (max_cache_entries - 1))
  ```
- `cache_low` is updated after append/truncate/purge and after disk-miss insertions.
- `enforce_cache_window()` evicts entries `< cache_window_start`.
- Keeps memory bounded while preserving correctness via disk-backed reads.

---

### Crash recovery

Recovery happens at group creation (`GroupState::new` → `SegmentedLog::recover` → `replay_group_from_manifest`).

#### Phase 1: segment validation and tail truncation

- List segments and process in ascending `segment_id`.
- For each segment, scan records and verify CRC.
- Stop at the first invalid record (partial write, corruption) → treat everything after as truncated.
- Compute `valid_bytes` per segment → build replay manifest of `(segment_id, valid_bytes)` pairs.

#### Phase 2: replay

Iterate records in order from replay manifest:

- `Vote`: restore in-memory vote (`AtomicVote`)
- `Entry`: 
  - Decode entry, get `log_id.index`
  - Insert into `LogIndex` (mapping index → location)
  - Optionally populate cache if within window
  - Update `first_index`, `last_index`, `last_log_id`
- `Truncate`:
  - New format: decode full `LogId`, remove indices > `log_id.index`
  - Backward compat: old payload was just `u64` index
  - Update `last_index` and truncate cache/index
- `Purge`:
  - Decode `LogId`, remove indices ≤ `log_id.index`
  - Update `first_index`, `last_purged_log_id`
  - Remove purged entries from cache/index

LibSQL record types (`WalFrame`, `Changeset`, `Checkpoint`, `WalMetadata`) are **ignored** for Raft state recovery.

Result: rebuilt in-memory state plus disk index sufficient to serve reads even if cache is empty.

---

### Compaction

Triggered on `purge()` (best-effort, conservative).

**Compactor rule** (preserve Raft metadata history):
- Only delete **old segments** (never the current segment writer is appending to).
- Only delete if segment contains **only Entry records**.
- Only delete if its **maximum entry index < purge boundary**.

This avoids deleting segments containing `Vote`/`Truncate`/`Purge` markers.

**Implementation**:
- Scans each candidate segment sequentially to verify "only entries" condition.
- Decodes each `Entry` record to find its `log_id.index`.
- Deletes segment via `maniac::fs::remove_file` if all conditions met.

**Future optimization**: Persist per-segment metadata (min/max entry index, presence of non-entry records) to avoid scanning during compaction.

---

### Configuration (`StorageConfig`)

- `base_dir`: directory root for all groups.
- `segment_size`: rotate threshold in bytes (default: 64MB).
- `fsync_interval`:
  - `None`: fsync after every write (fully durable, lower throughput).
  - `Some(Duration)`: group-commit per batch (higher throughput, still durable).
- `max_cache_entries_per_group`: cache window size per group (default: 0 = disabled).

---

### Traits and type aliases

```rust
pub type MultiplexedLogStorage = PerGroupLogStorage<C>;
pub type ShardedLogStorage = PerGroupLogStorage<C>;

// Implements both OpenRaft's MultiRaftLogStorage and custom MultiplexedStorage
trait MultiplexedStorage {
    type GroupLogStorage: RaftLogStorage<C>;
    async fn get_log_storage(&self, group_id: u64) -> Result<Self::GroupLogStorage, io::Error>;
    fn remove_group(&self, group_id: u64) -> Option<Self::GroupLogStorage>;
    fn group_ids(&self) -> Vec<u64>;
}
```

---

### Limitations / trade-offs
### Limitations / trade-offs

- **Async I/O with three-tier fallback on read/recovery**: Uses `maniac::fs::File::read_exact_at()` (async direct I/O) as primary path, with fallbacks for unaligned access and thread-pool blocking I/O when needed. This provides portability while maintaining async semantics.
- **LogIndex free-list**: slots are reused; the side `locations` vector can grow until reused.
- **Truncate backward-compatibility**: older logs may have `Truncate` records with just an index (u64). Recovery attempts to handle both formats.
- **Concurrent group initialization**: best-effort double-check; concurrent `get_or_create_group` can do duplicate recovery work, but only one result is stored in `GroupIndex`.
- **Compaction scans segments**: currently scans candidate segments sequentially to verify "only entries" condition; this is O(total bytes purged) but conservative and safe.
- **u24 group_id limit**: Maximum of 16,777,215 groups, which is sufficient for most deployments but worth monitoring for extremely large multi-tenant scenarios.

---

### Future work

**Segment footer index for fast startup**: Currently, recovery requires scanning every valid record in every segment, which can take minutes for large logs (>10GB). We can write a compact B+tree or sorted index at the end of each segment during rotation that maps entry index ranges to file offsets. On startup, we'd only need to read the footer (last few KB) of each segment, rebuild the `LogIndex` in O(#segments) instead of O(#records), and skip the full replay. This would dramatically reduce cold-start times from minutes to seconds. The footer should include checksums and be written atomically with the segment seal operation.

**Per-segment metadata for O(#segments) compaction**: The current compaction strategy scans every record in candidate segments to verify they contain "only entries" and to find min/max index values. This is O(bytes purged) and can cause latency spikes during large purges. By maintaining a small metadata block (16-32 bytes) at the start or end of each segment with fields like: `min_entry_index`, `max_entry_index`, `has_non_entry_records` (bitmap), `checksum`, we could make compaction decisions in O(#segments) time without scanning content. This metadata would be built incrementally during writes and sealed during segment rotation.

**Exactly-once group initialization**: The current `get_or_create_group` uses a best-effort double-check pattern that can still race: two concurrent calls for the same group_id will both start recovery, but only one result is stored in `GroupIndex`. While correct (no corruption), this wastes CPU and I/O. A stronger guarantee would use a per-group mutex or a "recovery-in-progress" flag stored in the `GroupIndex` slot itself. The slot could temporarily hold a `Recovering` state that other tasks await, ensuring only one recovery runs while others wait idempotently. This requires careful async-aware locking to avoid blocking the executor.

**Async read path with alignment safety**: The current read path uses blocking std::fs I/O to avoid alignment issues with Maniac's async file operations. However, this sacrifices throughput and integration with the async runtime. Future work should investigate: 1) Using `maniac::fs::File` with explicit alignment padding (read aligned, then slice), or 2) Adding a small thread pool for blocking reads that still integrates with the async completion system. The ideal solution would maintain the O_DIRECT alignment while being fully async.

**Streaming record read API for bulk operations**: Recovery and compaction currently read records individually (length prefix + record body), causing numerous small I/O requests. For large logs with millions of records, this overhead becomes significant. A streaming API could:

1. **Read large aligned chunks** (e.g., 1MB) in a single async I/O operation during sequential scans
2. **Parse multiple records** from the buffer in-memory, handling length-prefixed framing
3. **Handle record splits** across chunk boundaries by carrying over partial records
4. **Return an async Stream<Item = Result<ParsedRecord, Error>>** for efficient processing

This would reduce system calls by 100-1000x during recovery, dramatically improving startup times and compaction performance. The API would be exposed as `SegmentedLog::stream_records(offset, buffer_size) -> impl Stream` and used during recovery instead of individual record reads. Buffer alignment requirements would be handled internally, making it both fast and safe.

**Metrics and observability**: The storage layer currently lacks operational visibility. We should expose prometheus-style metrics including: `raft_storage_cache_hits_total`/`_misses_total` (with group_id label), `raft_storage_fsync_duration_seconds` (histogram), `raft_storage_segment_size_bytes` (gauge), `raft_storage_compaction_deleted_segments_total`, and `raft_storage_recovery_duration_seconds`. These should use low-overhead atomics and be optional via feature flag. This would help operators tune `max_cache_entries_per_group`, detect slow fsync devices, and monitor compaction efficiency.

**u32 group_id extension**: The current u24 encoding supports 16,777,215 groups, which is sufficient for most multi-tenant deployments. However, if we approach this limit (e.g., SaaS platforms with millions of tenants), we could extend to u32 (4 bytes) while maintaining 64-bit alignment by using a 12-byte header: `[len: u32][type: u8][group_id: u32][padding: 3][crc...]`. This would still be 64-bit aligned and support 4.2 billion groups. The change would be backward-incompatible but could be managed via a format version marker in the segment filename or a new record type. For now, we monitor usage and document the limit clearly.
