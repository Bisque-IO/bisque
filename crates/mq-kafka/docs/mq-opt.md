# Kafka Protocol Native Performance Optimization Plan

## Executive Summary

This document provides a comprehensive analysis of the bisque-mq-kafka protocol adapter's
performance characteristics and a phased implementation plan for achieving extremely efficient
native Kafka support. The optimizations target the produce and fetch hot paths, eliminating
unnecessary allocations, data copies, and intermediate data structures.

**Estimated hot-path improvement:** 40-60% reduction in per-message allocations and copies.

---

## Current Architecture Analysis

### Data Flow: Produce Path

```
Kafka Client → TCP → Frame Decode (zero-copy) → RecordBatch Decode
  → Vec<Record> allocation (1 per record)
  → FlatMessageBuilder per record (1 BytesMut alloc + 1 copy per record)
  → MqCommand::publish() (1 Vec<u8> alloc + 1 copy of ALL messages)
  → Write Batcher merge (potential full re-encode on merge)
  → Raft commit → apply_publish() (zero-copy, atomics only)
```

**Current cost per produce batch (N messages, S total bytes):**
- Allocations: O(N) FlatMessage buffers + O(1) MqCommand buffer + O(1) merge buffer
- Copies: O(S) FlatMessage encoding + O(S) MqCommand encoding + O(S) merge re-encoding
- **Total: ~2-3× total message bytes copied**

### Data Flow: Fetch Path

```
Raft Log (mmap) → KafkaLogReader.read_topic_messages()
  → Vec<FetchedMessage> allocation (1 per message + Vec<(Bytes,Bytes)> headers each)
  → build_record_batch() constructs RecordBatch struct
    → Vec<Record> allocation (1 per message + Vec<RecordHeader> per message)
    → Bytes::clone() for key/value/headers (refcount bumps, not heap copies)
  → encode_record_batch() into BytesMut
    → O(S) copy of all key/value/header bytes
    → O(S) CRC32 scan
  → Freeze to Bytes → response encode
```

**Current cost per fetch batch (N messages, S total bytes):**
- Allocations: O(N) FetchedMessage + O(N) Record + O(N) Vec<RecordHeader>
- Copies: O(S) encode into BytesMut + O(S) CRC scan
- **Total: ~2× total message bytes touched + O(N) struct allocations**

### Key Bottlenecks Identified

| # | Location | Issue | Impact |
|---|----------|-------|--------|
| 1 | `produce_records()` | Per-record FlatMessageBuilder allocation | O(N) allocs |
| 2 | `MqCommand::publish()` | Unsized Vec<u8> buffer, copies all messages | O(S) copy |
| 3 | `merge_pending()` | Full re-encode of merged batches | O(S) extra copy |
| 4 | `build_record_batch()` | Intermediate FetchedMessage→Record→RecordBatch | O(N) struct allocs |
| 5 | `encode_record_batch()` | Full payload copy + CRC scan (2 passes) | O(2S) |
| 6 | `data_notify` | Global notification wakes ALL fetch waiters | Thundering herd |
| 7 | `KafkaLogReader` | Only trait+mock exists; no production impl | Blocking |

---

## Implementation Plan

### Phase 1: Direct FlatMessage-to-RecordBatch Encoding [FETCH PATH]
**Files:** `codec.rs`, `handler.rs`
**Impact:** Eliminates FetchedMessage/Record/RecordBatch intermediate structs on fetch

Add `encode_record_batch_from_flat()` that encodes directly from `FlatMessage` bytes
into a Kafka record batch, avoiding all intermediate struct allocations:

- [x] Add `encode_record_batch_from_flat(messages: &[(u64, Bytes)], base_offset: i64, buf: &mut BytesMut)` to codec.rs
- [x] `messages` is `&[(offset, flat_message_bytes)]` — zero-copy from mmap
- [x] Read FlatMessage metadata (timestamp, key, value, headers) inline during encoding
- [x] Write varint-encoded records directly into BytesMut (single pass)
- [x] Compute CRC32 over the encoded batch
- [x] Update `build_record_batch()` in handler.rs to use new function
- [x] Add `build_record_batch_from_flat()` method to KafkaHandler
- [x] Add unit tests for the new encode function

**Allocation savings:** Eliminates Vec<FetchedMessage>, Vec<Record>, Vec<RecordHeader>

### Phase 2: Batch-Optimized Produce Path [PRODUCE PATH]
**Files:** `handler.rs`, `codec.rs`
**Impact:** Single allocation for all FlatMessages in a batch

- [x] Add `build_flat_messages_batch()` helper that pre-computes total size
- [x] Calculate exact buffer size for all messages before allocating
- [x] Build all FlatMessages into a single pre-sized Vec<Bytes>
- [x] Add `decode_record_batch_to_flat()` that goes directly from Kafka records to FlatMessage bytes
- [x] Unit tests for batch produce optimization

**Allocation savings:** Single pre-sized allocation instead of N individual allocations

### Phase 3: Pre-sized MqCommand Buffers and Merge Optimization
**Files:** `codec.rs` (mq crate), `write_batcher.rs`
**Impact:** Eliminates buffer growth reallocs and merge re-encoding

- [x] Add `publish_presized()` constructor that pre-computes exact buffer size
- [x] Scan messages once for total size, allocate exactly, encode once
- [x] Add `publish_append()` for merge: appends messages to existing command without full re-encode
- [x] Update `merge_pending()` to use append-based merging
- [x] Unit tests for pre-sized buffers and append merge

**Allocation savings:** Zero reallocs in MqCommand construction, no re-encode on merge

### Phase 4: Per-Topic Fetch Notification
**Files:** `handler.rs`, `server.rs`
**Impact:** Eliminates thundering herd on fetch long-polling

- [x] Replace global `data_notify: Arc<Notify>` with `TopicNotifier` map
- [x] `TopicNotifier`: `DashMap<u64, Arc<Notify>>` keyed by topic_id
- [x] Produce path notifies only affected topic(s)
- [x] Fetch long-poll subscribes to specific topics being fetched
- [x] Fallback to global notify for unknown topics (partition map refresh)
- [x] Unit tests for per-topic notification

**Latency improvement:** Fetch waiters only wake when their topics have new data

### Phase 5: Connection-Level Write Buffer Optimization
**Files:** `connection.rs`, `codec.rs`
**Impact:** Reduces write buffer reallocs and copies

- [x] Pre-size write_buf based on estimated response size
- [x] Add `response_size_hint()` to estimate response encoding size
- [x] Reserve capacity before encoding response
- [x] Track average response size per connection for adaptive sizing
- [x] Unit tests for buffer sizing

### Phase 6: KafkaLogReader Native Implementation
**Files:** `handler.rs`
**Impact:** Enables production fetch path with direct FlatMessage access

- [x] Add `read_topic_flat_messages()` method to KafkaLogReader trait
- [x] Returns `Vec<(u64, Bytes)>` — (offset, flat_message_bytes) pairs
- [x] Zero-copy from mmap-backed raft log segments
- [x] Update `do_fetch()` to prefer flat message path when available
- [x] Add default trait implementation that falls back to read_topic_messages()
- [x] Unit tests for flat message reading

### Phase 7: Comprehensive Testing and Validation
- [x] Unit tests for all new codec functions
- [x] Round-trip tests: produce → store → fetch with verification
- [x] Edge cases: empty batches, single message, max batch sizes
- [x] Header preservation tests (key, value, headers round-trip)
- [x] Compression compatibility tests
- [x] Build and full test suite passes

---

## Detailed Allocation Analysis (Before vs After)

### Produce Path (per batch of N messages, S total bytes)

| Step | Before | After |
|------|--------|-------|
| RecordBatch decode | Vec<Record>(N) | Vec<Record>(N) (unchanged) |
| FlatMessage build | N × BytesMut(msg_size) | 1 × Vec(S) pre-sized |
| MqCommand encode | Vec<u8>(growing) | Vec<u8>(exact S) |
| Batcher merge | Full re-encode O(S) | Append O(new_msgs) |
| **Total allocs** | **N+2 (+1 on merge)** | **3 (no merge realloc)** |
| **Total copies** | **~3S** | **~2S** |

### Fetch Path (per batch of N messages, S total bytes)

| Step | Before | After |
|------|--------|-------|
| Log read | Vec<FetchedMessage>(N) | Vec<(u64,Bytes)>(N) zero-copy |
| Record structs | Vec<Record>(N) + N×Vec<RecordHeader> | None (direct encode) |
| Encode to wire | BytesMut(growing) + O(S) copy | BytesMut(est_S) + O(S) copy |
| CRC scan | O(S) scan | O(S) scan (unavoidable) |
| **Total allocs** | **3N + 2** | **2** |
| **Total copies** | **~2S** | **~S + CRC scan** |

---

## Performance Metrics to Track

- `kafka.produce.batch_encode_ns` — Time to encode produce batch to FlatMessages
- `kafka.fetch.batch_encode_ns` — Time to encode fetch response record batches
- `kafka.produce.bytes_per_batch` — Average bytes per produce batch
- `kafka.fetch.bytes_per_batch` — Average bytes per fetch batch
- `kafka.batcher.merge_count` — Number of publish merges (append vs re-encode)
- `kafka.fetch.notify_wakeups` — Per-topic vs global notification wakeups

---

## Implementation Status Tracking

| Phase | Status | Tests |
|-------|--------|-------|
| Phase 1: Direct FlatMessage→RecordBatch | ✅ Complete | ✅ 11 tests |
| Phase 2: Batch Produce Optimization | ✅ Complete | ✅ 4 tests |
| Phase 3: Pre-sized Buffers + Merge | ✅ Complete | ✅ 4 tests |
| Phase 4: Per-Topic Notification | ✅ Complete | ✅ 3 tests |
| Phase 5: Write Buffer Optimization | ✅ Complete | ✅ 2 tests |
| Phase 6: KafkaLogReader Native | ✅ Complete | ✅ 3 tests |
| Phase 7: Testing & Validation | ✅ Complete | ✅ All 644 tests pass |
