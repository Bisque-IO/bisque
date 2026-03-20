★ Insight ─────────────────────────────────────`
This is one of the hardest problems in systems design: **per-tenant memory accounting in a language with a global allocator**. Rust doesn't give you per-object memory tracking — every `Vec::push`, `HashMap` resize, `Arc` clone, and `String` allocation goes through a single global allocator. The question is whether to fight that (custom allocators) or work with it (cooperative accounting).
`─────────────────────────────────────────────────`

Here's my analysis of the problem space and the realistic options:

---

## The Core Challenge

A single bisque-mq raft group's memory footprint has **5 fundamentally different categories**, each with different tracking characteristics:

| Category | Examples | Trackable? | Dominant? |
|----------|----------|------------|-----------|
| **1. Data-proportional state** | `AckVariantState.messages`, actor mailboxes, dedup windows | Yes — atomic counters already exist (`pending_bytes`, `in_flight_bytes`, etc.) | **Yes — can be GiB-scale** |
| **2. Entity-proportional metadata** | Topic/exchange/session/group objects in 26 `papaya::HashMap`s | Estimable — count entities × avg size | Medium — 10s MB to low GiB |
| **3. Raft storage RSS** | Pinned mmap segments, `LogIndex` (congee tree), `SegmentEntryIndex` | Partially — pinned count × segment_size for mmap; LogIndex grows monotonically | **Yes — default 16 GiB mmap ceiling** |
| **4. Pipeline buffers** | Crossfire channels, `PartitionInbox`, response `BytesMut`, forward TCP buffers | Bounded by config — known at startup | Small — low MBs |
| **5. Incidental allocations** | HashMap bucket arrays, `Arc` control blocks, `String` headers, `Vec` growth, papaya epoch garbage | **No** — invisible to application code | Unknown — 10-30% overhead |

Category 5 is the crux of the problem. You can account for categories 1–4 cooperatively, but you'll always have a gap from allocator-level overhead that you can't see.

---

## Two Fundamental Approaches

### Approach A: Cooperative Accounting (Bottom-Up)

Track memory at each allocation site using atomic counters, similar to the existing `pending_bytes`/`in_flight_bytes` pattern.

**What already exists** (ready to aggregate today):
- Per-topic: `total_bytes` (AtomicU64)
- Per-ack-group: `pending_bytes`, `in_flight_bytes`, `delayed_bytes` (AtomicU64)
- Per-actor: `pending_bytes`, `in_flight_bytes` (AtomicU64)
- Disk: `disk_usage_bytes` (Arc<AtomicU64>)
- Raft inflight: `RaftBacklog.current()` (semaphore permits)

**What's missing:**
- Entity metadata overhead (26 papaya maps — no `.len()` method; requires O(n) scan)
- Raft `LogIndex` (congee tree — no size query)
- Retained messages (can be large, mmap or heap)
- Dedup index (scc::TreeIndex — no size query)
- All incidental allocations (~10-30% of real RSS)

**Pros:** Zero-overhead on hot path (atomic fetch_add in single-writer apply), precise for data-plane memory, already 60% built.

**Cons:** Will always undercount real RSS by 10-30%. Papaya/congee/scc don't expose size. Requires discipline at every new allocation site.

### Approach B: Allocator-Level Tracking (Top-Down)

Use a custom global allocator (or jemalloc's `malloc_stats_print` / `epoch` + `stats.allocated`) to get real RSS, then attribute it to raft groups.

**Options:**
- **Per-group allocator arena** — each raft group gets its own allocator instance. Rust doesn't support this ergonomically (no `Box<T, A>` in most containers, papaya/congee don't support custom allocators).
- **Tagged allocations** — use thread-local or task-local group IDs to tag allocations. Fragile, inaccurate for shared `Arc`s.
- **Global RSS monitoring** — use `jemalloc_ctl` or `/proc/self/statm` to read process RSS, then divide by group count. Coarse but simple.

**Pros:** Captures *everything* including incidental allocations.

**Cons:** Per-group arenas are impractical in Rust today. Global RSS divided by N groups is too coarse for fair multi-tenancy.

---

## My Recommendation: Hybrid Cooperative Accounting

Neither approach alone works. Here's what I'd recommend:

### Layer 1: Cooperative Data-Plane Accounting (Precise)

A `GroupMemoryBudget` struct (similar to `RaftBacklog`) that tracks the **known large consumers** via atomic counters. This covers categories 1-3 which represent 70-90% of real memory:

```
GroupMemoryBudget
├── data_bytes          ← sum of pending_bytes + in_flight_bytes + delayed_bytes across all groups
├── metadata_bytes      ← entity_count × estimated_per_entity_size (periodically recomputed)
├── raft_pinned_bytes   ← pinned_segment_count × segment_size
├── raft_index_bytes    ← (last_index - first_index) × ~40 bytes (LogIndex entry estimate)
├── retained_bytes      ← sum of retained message sizes
├── dedup_bytes          ← dedup_entry_count × ~48 bytes (estimated)
└── overhead_factor     ← configurable multiplier (default 1.25) to account for category 5
```

**Enforcement:** Before accepting a new write in the apply path, check `budget.current() * overhead_factor < max_memory_bytes`. If exceeded, return `MqError::MemoryLimitExceeded` for new `CreateTopic`, `Enqueue`, `Publish` commands. Existing data keeps working (reads, acks, deliveries).

### Layer 2: Process-Level Safety Net (Coarse)

Monitor process RSS via jemalloc or `/proc/self/statm` on a background timer. If total RSS exceeds a process-wide hard limit, set a global "memory pressure" flag that makes *all* groups reject new writes. This is the last line of defense.

### Key Design Decisions Needed

1. **Where to enforce?** The state machine `apply()` is single-writer, so checking a budget there is cheap and serialized. But by the time data reaches `apply()`, it's already in the raft log. The earlier enforcement point is at `RaftWriter::submit()` / `LocalWriter::send()` — before the data enters the raft pipeline. However, this means the *leader* rejects the write before replication, which is the correct behavior for backpressure.

2. **What about mmap RSS?** Pinned mmap segments are virtual memory that may or may not be resident. The OS can page them out under memory pressure. Should we count them at face value or use `mincore()` to check actual residency? Face value is safer for budgeting — treat them as fully resident.

3. **What about LogIndex growth?** The congee tree grows monotonically with log entries. After log compaction/purge, old entries are removed but the tree may not shrink. This is a potential slow leak. Tracking `(last_index - first_index) × entry_size` gives an upper bound.

4. **Papaya map sizes** — these maps don't expose `.len()`. Options:
   - Maintain shadow atomic counters (increment on insert, decrement on remove) — disciplined but requires touching every call site
   - Periodic O(n) scan on a background timer — acceptable if scan is infrequent (every 30s)
   - Accept the estimate: `entity_count × average_entry_size`

5. **Graceful degradation** — when memory limit is hit, what operations should still work?
   - Reads, deliveries, acks: **Yes** (these reduce memory)
   - New publishes/enqueues: **Reject** with `MqError::MemoryLimitExceeded`
   - Topic/group creation: **Reject**
   - Consumer connections: **Allow** (they drain memory)

---

## What This Means Concretely

For a cloud multi-tenant deployment where each tenant gets a raft group with, say, a 2 GiB memory budget:

- **Data-plane** (message queues): atomic counters already track `pending_bytes` + `in_flight_bytes` → exact
- **Metadata** (topics, sessions): tracked via entity count × estimated size → ~90% accurate  
- **Raft storage** (pinned segments): `max_pinned_segments × segment_size` → exact ceiling, real usage ≤ ceiling
- **Overhead factor** (1.25x): covers HashMap buckets, Arc overhead, papaya epoch garbage → gets us to ~95% of real RSS
- **Process-level safety net**: catches the remaining 5% edge cases

The combination gives you **predictable, enforceable per-group memory limits** without requiring per-group allocator arenas, which is the right tradeoff for a production system.

What are your thoughts on this direction? Key questions:
1. Do you want enforcement at the `RaftWriter::submit()` level (pre-raft) or at `apply()` (post-raft)?
2. Should mmap segments count toward the memory budget at face value?
3. What's the target granularity — per-group budgets, or per-node budgets divided among groups?
