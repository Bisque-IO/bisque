# Two-Level Segment Purge Architecture

## Overview

Segment lifecycle in bisque-mq has two distinct levels of purging with
fundamentally different responsibilities:

| Level | Name | Purpose | Deletes files? |
|-------|------|---------|---------------|
| 1 | **Unpin** | Release mmap memory for segments no longer needed by active operations | **No** |
| 2 | **Retention** | Delete segment files from local disk (and optionally archive) based on per-entity retention policies | **Yes** |

The invariant: **Level 1 NEVER deletes segment files. Level 2 is the only
path that removes files from disk.**

---

## Level 1: Unpin (Active Data Floor)

### What it does

Level 1 determines the minimum raft log index still required by any active
consumer, ack state, or actor state. Segments fully below this floor are
**unpinned** — their mmap is closed and memory is freed — but their files
remain on disk.

### Current implementation

- `MqEngine::compute_purge_floor()` → scans all topics and consumer groups
  to find the minimum referenced log index.
- `MqStateMachine::update_purge_floor()` → writes the floor to the raft
  storage's `purge_floor: Arc<AtomicU64>`.
- OpenRaft calls `MmapLogStorage::purge()` → calls
  `MmapSegmentMap::update_pins(purge_up_to)`.

### Fix required

`update_pins()` currently both unpins AND deletes segment files. After the
fix:

```
update_pins(purge_up_to):
  for each pinned segment:
    if segment.max_index <= purge_up_to:
      unpin (remove from pinned map)        ← keep this
      collect path for deletion             ← REMOVE this
      push to purged_segments               ← REMOVE this (move to Level 2)
    if segment.min_index > pin_ceiling:
      unpin but keep file                   ← unchanged
```

The method returns only the unpinned segment IDs (for retained message
detach sweeps). File deletion moves entirely to Level 2.

### Retained message sweep

When segments are unpinned, retained messages backed by those segments must
be detached (copied to heap) before the mmap mapping becomes invalid. This
sweep is triggered by the `purged_segments` notification:

- **Sequential path**: state machine sweeps all exchanges and topics.
- **Async apply path**: each partition worker sweeps its owned entities
  (`entity_id % num_partitions == partition_id`), then the state machine
  drains the purged list.

This sweep happens at Level 1 (unpin time), NOT at Level 2 (deletion time),
because the mmap backing disappears at unpin.

---

## Level 2: Retention-Based Deletion

### Purpose

Evaluate per-entity retention policies to determine which unpinned segments
can be permanently deleted from local disk. A segment is eligible for
deletion only when **every entity with data in that segment** has satisfied
its retention policy for that data.

### Retention policies

Per-topic `RetentionPolicy` (already defined in `types.rs`):

```rust
pub struct RetentionPolicy {
    pub max_age_secs: Option<u64>,    // delete data older than N seconds
    pub max_bytes: Option<u64>,       // keep at most N bytes per topic
    pub max_messages: Option<u64>,    // keep at most N messages per topic
}
```

Default (all `None`) = retain forever = segments are never deleted by
retention.

### Data flow

```
┌─────────────────┐
│  Sealed segment  │  .sidx written with entity summaries
│  (segment_id=5)  │
└────────┬─────────┘
         │
         ▼
┌─────────────────┐
│  Level 1 Unpin   │  mmap closed, memory freed
│  purge_floor ≥ 5 │  retained messages detached
└────────┬─────────┘
         │
         ▼
┌─────────────────┐
│  Level 2 Eval    │  periodic retention check
│  RetentionEval   │  "can segment 5 be deleted?"
└────────┬─────────┘
         │
    ┌────┴────┐
    │ All     │ No ──► keep file, check again later
    │ entities│
    │ satisfied?
    └────┬────┘
         │ Yes
         ▼
┌─────────────────┐
│  Archive check   │  if archive configured:
│  (optional)      │  ensure segment uploaded before delete
└────────┬─────────┘
         │
         ▼
┌─────────────────┐
│  Delete file     │  std::fs::remove_file
└──────────────────┘
```

### Segment deletion eligibility

A sealed segment is eligible for deletion when ALL of these hold:

1. **Unpinned**: the segment is below the Level 1 purge floor (no active
   consumers reference it).

2. **Age-based retention**: the segment file's mtime is older than the
   configured `max_age_secs`. (Per-entity byte/message retention tracking
   via MDBX has been removed.)

3. **Archived** (if archive configured): the segment has been uploaded to
   remote storage. Checked via `ArchiveManager`.

4. **Local retention cooldown**: if archive configured, the segment must
   have been sealed for at least `min_local_retention_secs` to prevent
   thrashing.

### Core struct: `RetentionEvaluator`

Lives in `crates/mq/src/retention.rs`:

```rust
pub struct RetentionEvaluator {
    /// Raft group directory — segment files live here.
    group_dir: PathBuf,
    /// Group ID (retained for metrics labels).
    group_id: u64,
    /// Engine metadata — reads topic retention policies.
    metadata: Arc<MqMetadata>,
    /// Level 1 purge floor — segments below this are unpinned.
    purge_floor: Arc<AtomicU64>,
    /// Active segment ID — never delete the active (being written) segment.
    active_segment_id: AtomicU64,
    /// Guard against concurrent evaluations.
    evaluating: AtomicBool,
    /// Segment IDs that have been deleted (avoid re-scanning).
    deleted_segments: Mutex<HashSet<u64>>,
    /// Last evaluation time for rate limiting.
    last_eval: Mutex<Option<Instant>>,
    /// Pre-initialized metrics.
    m_segments_deleted: metrics::Counter,
    m_bytes_deleted: metrics::Counter,
    m_eval_count: metrics::Counter,
}
```

Note: The evaluator no longer depends on MDBX or per-entity segment
tracking. Segment deletion eligibility is determined by the purge floor
and age-based retention via file mtime. There is no `MqManifestManager`.

### Evaluation algorithm

```
fn evaluate(&self) -> Vec<u64>:
  floor = purge_floor.load()
  active = active_segment_id.load()
  candidates = scan_segment_ids(group_dir)
                 .filter(|id| id < active)
                 .filter(|id| !deleted_segments.contains(id))

  for each candidate segment_id:
    // Check 1: is it below the unpin floor?
    if segment_max_index > floor:
      skip (still pinned)

    // Check 2: age-based retention via file mtime
    // If the segment file's mtime is older than the retention age,
    // the segment is eligible for deletion.

    // All checks passed — delete file and record deletion
    deletable.push(segment_id)

  return deletable
```

Note: The old algorithm performed per-entity segment tracking via MDBX
(`manifest.read_all_entities_for_segment`) to check per-entity retention
policies (max_bytes, max_messages). This has been simplified. The
evaluator now uses the purge floor (which already accounts for active
consumers) plus age-based retention via file mtime. Per-entity byte/message
counting is no longer performed.

### How retention policies map to segment decisions

**max_age_secs**: Uses the segment file's mtime as a proxy. If a segment
file was last modified more than `max_age_secs` ago, the age policy is
satisfied. This is zero-cost and avoids any need for per-entity metadata.

**max_bytes / max_messages**: Per-entity byte and message counting across
segments has been removed. The purge floor already ensures no active
consumers reference the segment. Age-based retention via mtime is the
primary deletion criterion.

### Lifecycle integration

The `RetentionEvaluator` runs periodically, NOT on every apply:

- **Trigger**: after every Nth apply batch, or on a timer (e.g., every 30s).
- **Location**: `MqStateMachine::apply()` — after updating the purge floor,
  optionally run a retention evaluation cycle.
- **Execution**: on `spawn_blocking` to avoid blocking the apply path.
- **Deletion**: `std::fs::remove_file` on the blocking pool.

### Configuration

```rust
pub struct RetentionConfig {
    /// How often to evaluate retention (in apply batches). 0 = disabled.
    pub eval_interval_batches: u64,
    /// Minimum time between evaluations (seconds).
    pub eval_interval_secs: u64,
}
```

---

## Implementation Plan

### Step 1: Fix Level 1 (`update_pins` stops deleting)

**File**: `crates/raft/src/storage_mmap.rs`

- `update_pins()` → rename return type to just `Vec<u64>` (unpinned segment
  IDs). Remove path collection. Remove file deletion from the caller in
  `purge()`.
- The caller (`MmapLogStorage::purge()`) still pushes unpinned IDs to
  `purged_segments` for retained message detach sweeps.
- Remove `std::fs::remove_file` calls from `purge()`.

### Step 2: Create `RetentionEvaluator`

**File**: `crates/mq/src/retention.rs`

- `RetentionEvaluator::new(...)` — constructor
- `RetentionEvaluator::evaluate() -> Vec<u64>` — returns deletable segment IDs
- `RetentionEvaluator::delete_segments(ids: &[u64])` — deletes files
- Uses purge floor + file mtime for deletion eligibility (no MDBX dependency)

### Step 3: Wire into state machine

**File**: `crates/mq/src/state_machine.rs`

- Add `retention_evaluator: Option<RetentionEvaluator>` field
- After `update_purge_floor()`, check if retention eval is due
- Spawn blocking task for evaluation + deletion

### Step 4: *(Removed — no manifest helper needed)*

The old plan required an MDBX-backed `read_entities_for_segment()` helper
in `crates/mq/src/manifest.rs`. This is no longer needed because bisque-mq
no longer has a manifest system. Recovery uses Raft snapshots + log replay.

---

## Edge Cases

1. **Topic deleted while segment exists**: the segment still has data for
   that topic, but the topic's retention policy is gone. Treat deleted
   topics as "retention satisfied" — their data can be purged.

2. **Empty retention policy**: `None/None/None` means "retain forever".
   Segments containing data for such topics are never deleted by retention.
   Only manual purge or topic deletion unblocks them.

3. **All-default cluster**: if no topics set retention policies, Level 2
   never deletes anything. Segments accumulate until retention policies are
   configured or topics are deleted.

4. **Mixed retention on same segment**: segment 5 has data for topic A
   (max_age=1h) and topic B (retain forever). Segment 5 is never deleted
   until topic B is deleted or its policy changes.

5. **Archive upload failure**: segment stays on local disk. Retry on next
   evaluation cycle.

6. **Concurrent evaluation**: at most one evaluation runs at a time
   (guarded by AtomicBool or Mutex).

7. **Segment still mmap'd by readers**: Level 1 unpins before Level 2
   deletes. By the time Level 2 runs, no mmaps reference the segment. The
   file can be safely deleted.
