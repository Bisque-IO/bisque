Performance Analysis: bisque-mq Hot Paths



---

### 5. HIGH — `HashSet<u32>` for `seen_ids` in `ClientPartition` (async_apply.rs:248, 271, 315)

Every drain cycle in `ClientPartition::run` accumulates matched `client_id`s into a `HashSet`, then iterates it to fire `is_done=true` callbacks, then clears it:
```rust
self.seen_ids.insert(client_id);  // per matched frame
// ...
for &id in &self.seen_ids { ... }  // end of drain
self.seen_ids.clear();
```

`HashSet::insert` computes a hash per entry. For typical workloads (few clients per partition), a `SmallVec<[u32; 8]>` with dedup-on-read is far cheaper:
```rust
seen_ids: SmallVec<[u32; 8]>,

// Insert (accept duplicates):
self.seen_ids.push(client_id);

// Signal (dedup inline):
self.seen_ids.sort_unstable();
self.seen_ids.dedup();
for &id in &self.seen_ids { ... }
self.seen_ids.clear();
```
**Impact:** Eliminates hash overhead per matched frame. Inline SmallVec avoids heap allocation for ≤8 concurrent clients per partition (which covers nearly all real deployments).

---

### 6. HIGH — `sweep_purged_retained`: Vec clone under lock + O(n) linear scan (async_apply.rs:719–743)

```rust
let purged_ids: Vec<u64> = {
    let guard = purged.lock();  // contended with state machine
    guard.clone()               // heap allocation under lock
};
// Later, for every exchange/topic in the system:
if purged_ids.contains(&seg_id) { ... }  // O(n) per entity
```

For `k` purged segments and `m` exchanges/topics, this is O(k·m) work per sweep.

**Fix:** Convert to `HashSet<u64>` for O(1) lookup, and minimize lock hold time:
```rust
let purged_set: HashSet<u64> = {
    let guard = purged.lock();
    if guard.is_empty() { return; }
    guard.iter().copied().collect()
};
// ...
if purged_set.contains(&seg_id) { ... }  // O(1)
```
**Impact:** Dramatically reduces sweep cost when many exchanges/topics exist or multiple segments are purged simultaneously.

---

### 9. MEDIUM — Per-partition `papaya::HashMap::pin()` in `ClientIdTable::take` (async_apply.rs:479)

Every `take()` call (once per applied command) acquires a papaya epoch guard via `self.entries.pin()`. The epoch guard acquisition itself is an `Acquire` load + thread-local epoch bump — not free.

**Fix:** Cache the pin guard for the duration of `process_range` by passing it as a parameter:
```rust
// ClientIdTable gains:
pub fn pin_guard(&self) -> papaya::Guard<'_> {
    self.entries.pin()
}
pub fn take_with_guard<'g>(&self, log_index: u64, guard: &'g papaya::Guard<'_>) -> Option<u32> {
    guard.remove(&log_index).copied()
}

// In process_range():
let cid_guard = self.client_id_table.pin_guard(); // once per range
// ... in apply loop:
if let Some(client_id) = self.client_id_table.take_with_guard(rec.log_index, &cid_guard) { ... }
```
**Impact:** Reduces epoch guard acquisitions from O(n) to O(1) per `process_range` call.

---

### 10. LOW — `PendingRequest` enum variants heap-allocate `String` (async_apply.rs:1030–1033)

```rust
pub enum PendingRequest {
    ProducePartition { topic: String, partition: i32 },  // heap alloc per request
    CreateEntity { name: String },                        // heap alloc per request
    // ...
}
```

Every Kafka produce or entity-creation request allocates a `String`. These are per-connection, per-request allocations on the adapter path.

**Fix:** Use `Arc<str>` (shared from the parsed topic name) or `SmallString` for names typically ≤64 bytes. If the topic name is already available as `Bytes` from the command decode, store that instead.

---

### Priority Summary

| # | Location | Type | Severity | Fix |
|---|----------|------|----------|-----|
| 1 | `route_response` L402–411 | 2× alloc + memcpy per command | **CRITICAL** | Single `BytesMut`, direct encode |
| 2 | `ClientIdTable::take` L478–485 | Double hash lookup per command | **CRITICAL** | `remove()` returns value |
| 3 | `flush_response_buf` L566–574 | ArcSwap guard held across await | **HIGH** | Snapshot + drop guard before await |
| 4 | `response_buf: HashMap` L507 | HashMap overhead for ≤4 entries | **HIGH** | `SmallVec<[(u32, BytesMut); 4]>` |
| 5 | `seen_ids: HashSet` L248/271 | Hash overhead per matched frame | **HIGH** | `SmallVec<[u32; 8]>` |
| 6 | `sweep_purged_retained` L719–743 | Vec clone + O(n·m) scan | **HIGH** | `HashSet<u64>` outside lock |
| 7 | `ResponseWriter::write_inner` L393 | `notify_one()` per write | **MEDIUM** | `reader_waiting: AtomicBool` gate |
| 8 | `new_or_recycle` L158–160 | Mutex always acquired | **MEDIUM** | Atomic count fast-path |
| 9 | `ClientIdTable::pin()` L479 | Epoch pin per apply | **MEDIUM** | Cache guard per `process_range` |
| 10 | `PendingRequest` variants L1030 | String alloc per request | **LOW** | `Arc<str>` or `Bytes` |

The two **CRITICAL** items (#1 and #2) are the most impactful because they fire on every single applied command that has a waiting client — which is essentially every command in a live system. Items #3–#6 are on the forwarded-batch and sweep paths which are common in multi-node deployments.
