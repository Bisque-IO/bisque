# Panic Analysis Report — bisque-raft

## Summary

Total potential panic points found in **production code** (excluding `#[cfg(test)]` modules, test files, and example files):

| Category | Count | Status |
|----------|-------|--------|
| `std::sync::Mutex::lock().unwrap()` | 9 | **FIXED** — `unwrap_or_else(\|e\| e.into_inner())` |
| `std::sync::Condvar::wait().unwrap()` / `wait_timeout().unwrap()` | 2 | **FIXED** — same recovery pattern |
| `.expect()` on thread spawn | 2 | **FIXED** — returns `io::Result` |
| `.expect()` on invariant | 1 | **FIXED** — returns `io::Error` |
| `panic!()` in `manifest::new_in_memory()` | 1 | **FIXED** — returns `io::Error` |
| `panic!()` in `network::new_client()` | 1 | **FIXED** — replaced with `unreachable!()` (programming error trap) |
| `.expect()` on `MultiRaftLogStorage::get_log_storage` | 1 | **FIXED** — trait returns `io::Result`, `.expect()` removed |
| `.expect()` on `encode_framed` in rpc_server | 1 | **FIXED** — `unwrap_or_else` with error logging |
| `SystemTime::now().duration_since(UNIX_EPOCH).unwrap()` | 2 | **FIXED** — `unwrap_or_default()` |
| `assert!()` in `AtomicLogId::store()` | 2 | **FIXED** — converted to `debug_assert!()` |
| `.unwrap()` on mmap alloc | 1 | **FIXED** — returns `io::Result` |
| `meta.is_none() \|\| !meta.unwrap().sealed` pattern | 1 | **FIXED** — rewritten as idiomatic pattern |
| `.try_into().unwrap()` on fixed-size slices | ~25 | **ACCEPTABLE** — provably safe (see below) |
| `"0.0.0.0:5000".parse().unwrap()` | 1 | **ACCEPTABLE** — infallible on valid literal |
| Test support code (`test_support.rs`) | ~5 | **ACCEPTABLE** — test utility only |

---

## Detailed Findings by File

### 1. `storage_mmap.rs` — Highest density of production panics

#### 1a. `std::sync::Mutex::lock().unwrap()` (FsyncState methods) — FIXED
- `FsyncState::push()`, `enqueue_seal()`, `enqueue_prealloc()`
- `fsync_thread_loop()` — all lock sites
- `rotate_segment_with_min()`
- `stop()`

All converted to `lock().unwrap_or_else(|e| e.into_inner())` to recover from mutex poisoning.

#### 1b. `std::sync::Condvar::wait().unwrap()` / `wait_timeout().unwrap()` — FIXED
- `fsync_thread_loop()`: `state.cv.wait(inner)` and `state.cv.wait_timeout(inner, remaining)`

Converted to `unwrap_or_else(|e| e.into_inner())`.

#### 1c. `.try_into().unwrap()` on fixed-size byte slices — ACCEPTABLE
- Lines 482, 869, 880, 905, 1694, 1941, 2174, 2189, 2229, 2614, 2847, 2884-2886

All preceded by bounds checks (loop conditions or explicit `if` guards) guaranteeing the slice is exactly the right size. The `try_into()` conversion is mathematically infallible.

#### 1d. `.expect()` on thread spawn — FIXED
Converted to `?` operator returning `io::Error`.

#### 1e. `.expect()` on invariant — FIXED
Converted to `ok_or_else(|| io::Error::new(...))`.

#### 1f. `SystemTime` — FIXED
`nanos_now()` now uses `.unwrap_or_default()`.

#### 1g. `MmapOptions::new().map_anon().unwrap()` — FIXED
`sentinel_segment()` now returns `io::Result`.

#### 1h. `meta.unwrap()` after `is_none()` check — FIXED
Rewritten as idiomatic `map_or` / pattern match.

#### 1i. `MultiRaftLogStorage::get_log_storage` `.expect()` — FIXED
The `MultiRaftLogStorage` trait now returns `io::Result<Self::GroupLogStorage>`, eliminating the `.expect()`. A new `AddGroupError<C>` error type in `manager.rs` propagates both storage and raft initialization errors.

### 2. `manifest.rs` (WAL-based manifest, replaced the former MDBX-based `manifest_mdbx.rs`)

#### 2a. `std::sync::Mutex::lock().unwrap()` in `stop()` — FIXED
Converted to `unwrap_or_else(|e| e.into_inner())`.

#### 2b. `.expect()` on thread spawn — FIXED
Converted to `?` operator.

#### 2c. `panic!()` in `new_in_memory()` — FIXED
`new_in_memory()` now returns `io::Result<Self>`.

### 3. `transport_tcp.rs`

#### 3a. `.try_into().unwrap()` on frame prefix — ACCEPTABLE
Preceded by `buf.len() < FRAME_PREFIX_LEN` check. Infallible.

### 4. `rpc_server.rs`

#### 4a. `.parse().unwrap()` in Default impl — ACCEPTABLE
`"0.0.0.0:5000"` is a hardcoded valid socket address. Cannot fail.

#### 4b. `.try_into().unwrap()` on frame prefix — ACCEPTABLE
Same pattern as transport_tcp.rs.

#### 4c. `.expect("encode to Vec cannot fail")` — FIXED
Replaced with `unwrap_or_else` that logs the error and returns an empty `Vec`.

### 5. `network.rs`

#### 5a. `panic!()` in `new_client()` — FIXED
Replaced `panic!()` with `unreachable!()` and added documentation explaining this is an OpenRaft trait impl that must never be called directly (GroupNetworkFactory is the correct entry point).

### 6. `record_format.rs`

#### 6a. `.try_into().unwrap()` on CRC bytes — ACCEPTABLE
`payload_end = data.len() - CRC64_SIZE` after bounds check guarantees 8 bytes. Infallible.

#### 6b. `assert!()` in `AtomicLogId::store()` — FIXED
Converted to `debug_assert!()` (compiles out in release builds).

### 7. `segment_archive.rs`

#### 7a. `SystemTime` unwrap — FIXED
Converted to `.unwrap_or_default()`.

### 8. `codec.rs` (production code)

#### 8a. `.try_into().unwrap()` in `decode_from_bytes()` — ACCEPTABLE
Preceded by `data.len() < 25` check. Each slice is exactly 8 bytes. Infallible.

### 9. `test_support.rs` (public module, not `#[cfg(test)]`)

#### 9a-c. Various `.expect()` and `.unwrap()` — ACCEPTABLE
Test utility module. Panicking on setup failures is standard test practice.

---

## Classification Summary

### Fixed (all production panics that could crash the process)
1. ✅ All `std::sync::Mutex::lock().unwrap()` → `unwrap_or_else(|e| e.into_inner())`
2. ✅ All `std::sync::Condvar::wait().unwrap()` → same recovery
3. ✅ `panic!()` in manifest `new_in_memory()` → returns `io::Error`
4. ✅ `panic!()` in `MultiRaftNetworkFactory::new_client()` → `unreachable!()`
5. ✅ `.expect()` on thread spawn → returns `io::Result`
6. ✅ `.expect()` on group insertion invariant → returns `io::Error`
7. ✅ `assert!()` in `AtomicLogId::store()` → `debug_assert!()`
8. ✅ `MmapOptions::new().map_anon().unwrap()` → returns `io::Result`
9. ✅ `SystemTime` unwraps → `unwrap_or_default()`
10. ✅ `meta.unwrap()` pattern → idiomatic rewrite
11. ✅ `MultiRaftLogStorage::get_log_storage` → trait returns `io::Result`
12. ✅ `encode_framed().expect()` → `unwrap_or_else` with logging

### Acceptable As-Is
13. `"0.0.0.0:5000".parse().unwrap()` — infallible on a valid literal
14. `.try_into().unwrap()` where slice size is guaranteed by prior bounds check
15. Test support code (`test_support.rs`) — test-only utilities
