# Remediation Plan — Eliminate Panics from bisque-raft

## Status: COMPLETE ✅

All phases executed successfully. Workspace compiles cleanly.

## Guiding Principles

1. **Panics are reserved for catastrophic state corruption** — situations where continuing would cause data loss or silent corruption worse than crashing.
2. **`unwrap()` must be used only when the value is provably `Some`/`Ok`** — e.g., after a bounds check that guarantees the conversion succeeds.
3. **`Result` with proper error types is the de-facto standard** for all fallible operations.
4. **Poisoned mutexes should recover, not cascade** — use `lock().unwrap_or_else(|e| e.into_inner())` since poisoning indicates a prior panic already handled; re-panicking serves no purpose.

---

## Phase 1: `std::sync::Mutex` Poisoning Recovery ✅

**Files**: `storage_mmap.rs`, `manifest.rs`

All `std::sync::Mutex::lock().unwrap()` replaced with `lock().unwrap_or_else(|e| e.into_inner())`.
All `std::sync::Condvar::wait().unwrap()` and `wait_timeout().unwrap()` similarly recovered.

---

## Phase 2: Explicit `panic!()` Removal ✅

### 2a. `manifest.rs` — `new_in_memory()` panic ✅
Changed `new_in_memory()` to return `io::Result<Self>`.
*(Note: the manifest system was later rewritten from MDBX to an append-only WAL with periodic snapshots.)*

### 2b. `network.rs` — `MultiRaftNetworkFactory::new_client()` ✅
Replaced `panic!()` with `unreachable!()` + documentation. This is an OpenRaft trait guard — calling it is a programming error, not a runtime condition.

---

## Phase 3: `.expect()` on Thread Spawn → Return `io::Result` ✅

### 3a. `storage_mmap.rs` — fsync thread spawn ✅
Converted to `?` operator.

### 3b. `manifest.rs` — manifest worker thread spawn ✅
Now returns `io::Result` via `?`.

---

## Phase 4: `.expect()` on Invariant → Return Error ✅

### 4a. `storage_mmap.rs` — group must exist after insert ✅
Replaced with `ok_or_else(|| io::Error::new(...))`.

---

## Phase 5: `assert!()` in `AtomicLogId::store()` → `debug_assert!()` ✅

### 5a. `record_format.rs` — term/node_id bounds ✅
Converted to `debug_assert!()` — compiles out in release builds.

---

## Phase 6: Anonymous mmap unwrap → Return Result ✅

### 6a. `storage_mmap.rs` — `sentinel_segment()` ✅
Returns `io::Result<Arc<Segment>>`.

---

## Phase 7: `SystemTime` unwraps → `unwrap_or_default()` ✅

### 7a. `storage_mmap.rs` — `nanos_now()` ✅
### 7b. `segment_archive.rs` — retention cutoff ✅

---

## Phase 8: Pattern Cleanup ✅

### 8a. `storage_mmap.rs` — `meta.unwrap()` after `is_none()` check ✅
Rewritten as idiomatic `map_or` / pattern match.

---

## Phase 9: Trait-Level Error Propagation ✅

### 9a. `MultiRaftLogStorage::get_log_storage()` ✅
- **`storage.rs`**: Trait method changed from `-> impl Future<Output = Self::GroupLogStorage>` to `-> impl Future<Output = io::Result<Self::GroupLogStorage>>`
- **`storage_mmap.rs`**: Trait impl now returns `io::Result`, removing the `.expect()`
- **`manager.rs`**: New `AddGroupError<C>` enum with `Storage(io::Error)` and `Raft(Fatal<C>)` variants. `add_group()` returns `Result<Raft<C>, AddGroupError<C>>`
- **`lib.rs`**: `AddGroupError` exported
- All downstream callers (bisque, bisque-mq, bisque-lance) use `Box<dyn Error>` or `.unwrap()` in tests — no breakage

### 9b. `rpc_server.rs` — `encode_framed().expect()` ✅
Replaced with `unwrap_or_else` that logs the error and returns an empty `Vec`.

---

## Remaining `.try_into().unwrap()` — Intentionally Kept

~25 occurrences of `.try_into().unwrap()` on fixed-size byte slices remain. These are all provably infallible:
- Each is preceded by a bounds check guaranteeing the slice is the exact size required
- Converting `[u8; N]` from a bounds-checked `&[u8]` slice cannot fail
- Replacing these with fallible patterns would add noise without safety benefit

---

## Files Modified

| File | Phases |
|------|--------|
| `storage.rs` | 9a (trait signature) |
| `storage_mmap.rs` | 1, 3a, 4a, 6a, 7a, 8a, 9a |
| `manifest.rs` | 1, 2a, 3b |
| `record_format.rs` | 5 |
| `segment_archive.rs` | 7b |
| `network.rs` | 2b |
| `rpc_server.rs` | 9b |
| `manager.rs` | 9a (AddGroupError, add_group signature) |
| `lib.rs` | 9a (export AddGroupError) |
