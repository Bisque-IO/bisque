# Bisque-MQ Panic Analysis Report

## Overview

This report catalogs every potential panic scenario in the `bisque-mq` crate's **production source code** (`crates/mq/src/`). Test and example code panics are noted separately as they have lower impact but should still be addressed for consistency.

The dominant pattern is `try_into().unwrap()` for byte-to-integer conversion during binary protocol decoding. While the `.try_into()` from a correctly-sized slice is infallible, the **slice indexing** (`buf[offset..offset + N]`) that precedes it will panic if the buffer is too short. Malformed network input, corrupted Raft log entries, or truncated mmap segments can all produce undersized buffers, making these panics reachable in production.

---

## 1. Production Code: `types.rs` (~20 unwraps)

### Pattern: `buf_field_*` free functions and `MqCommand::field_*` methods

These are the foundational byte-reading primitives used throughout the crate.

| Line(s) | Function | Pattern |
|---------|----------|---------|
| 677 | `buf_field_u64()` | `u64::from_le_bytes(buf[offset..offset+8].try_into().unwrap())` |
| 683 | `buf_field_u32()` | `u32::from_le_bytes(buf[offset..offset+4].try_into().unwrap())` |
| 689, 693 | `buf_field_vec_u64()` | Two unwraps reading count + data_offset |
| 713-716 | `MqCommand::cmd_size()` | `self.buf[OFF_SIZE..OFF_SIZE+4].try_into().unwrap()` |
| 723-727 | `MqCommand::fixed_size()` | `self.buf[OFF_FIXED..OFF_FIXED+2].try_into().unwrap()` |
| 743 | `MqCommand::field_u64()` | `self.buf[offset..offset+8].try_into().unwrap()` |
| 749 | `MqCommand::field_u32()` | `self.buf[offset..offset+4].try_into().unwrap()` |
| 767, 770 | `MqCommand::field_flex8()` | Large-blob branch: two unwraps for offset + size |
| 794, 797 | `MqCommand::field_opt_flex8_bytes()` | Same large-blob pattern |
| 814, 819 | `MqCommand::field_vec_u64()` | count + data_offset unwraps |
| 829 | `MqCommand::field_vec_bytes_count()` | Single unwrap |
| 836-846 | `MqCommand::field_vec_bytes_get()` | Loop with unwraps on each iteration |

**Risk**: HIGH - these are called on every command decode. A malformed buffer from network or corrupted log will panic the process.

---

## 2. Production Code: `codec.rs` (~70 unwraps)

### Pattern: View struct accessor methods

Every view struct (`CmdPublish`, `CmdEnqueue`, `CmdCreateTopic`, `CmdDisposition`, `CmdGroupJoin`, `CmdGroupLeave`, `CmdGroupHeartbeat`, `CmdProducePartition`, `CmdActorDispatch`, `CmdForwardedBatch`, `CmdForwardedBatchIter`, `CmdExchangePublish`, `CmdExchangePublishIter`, `CmdSetTopicProps`, `CmdPartitionRangeOp`, `CmdPartitionRangeOpIter`) uses the same pattern:

```rust
pub fn some_field(&self) -> u64 {
    u64::from_le_bytes(self.buf[OFFSET..OFFSET + 8].try_into().unwrap())
}
```

Key locations (non-exhaustive, representative lines):
- **CmdPublish** (lines 654-688): `topic_id()`, `base_offset()`, `dedup_key()`, `timestamp()`, `entry_count()`, `entries()`
- **CmdDisposition** (lines 695-721): `topic_id()`, `msg_len()`, `group_id()`, `session_id()`, `consumer_id()`
- **CmdGroupJoin** (lines 736-839): `entry_count()`, `entries()`, `topic_id()`, `generation()`, `member iteration`
- **CmdGroupLeave/Heartbeat** (lines 854-883): Various field accessors
- **CmdProducePartition** (lines 2384-2510): `count()`, `topic_id()`, `partition_id()`, `entries()`
- **CmdActorDispatch** (lines 2506-2556): `group_id()`, `actor_id()`, `routing_keys()`
- **CmdForwardedBatch** (lines 2573-2579): `node_id()`, `count()`
- **CmdForwardedBatchIter** (lines 3065-3177): Iterator with 3 unwraps per iteration
- **CmdExchangePublish** (lines 2688-2723): `exchange_id()`, `error_code()`, `timestamp()`, etc.
- **CmdSetTopicProps** (lines 2741-2773): `topic_id()`, `ttl()`, `max_queue_size()`, `partition_table()`
- **CmdPartitionRangeOp** (lines 2798-2818): `topic_id()`, `error_code()`, `partition_table()`
- **PublishMessageIter** (lines 3238-3262): Iterator with unwrap per entry
- **FlatMqCommand** accessor (line 914): `log_index` from flat buffer
- **decode_u64s_at** (line 144): Loop with unwrap per u64

Also:
- Line 290: `value.encode(&mut tmp).unwrap()` - bincode encode (infallible for valid data but could panic on OOM)
- Lines 2331-2348: `read_flex8_raw()` and `read_flex8_raw_at()` - flex8 decoding helpers with unwraps

**Risk**: HIGH - same as types.rs. Every command accessor can panic on malformed input.

---

## 3. Production Code: `forward.rs` (~25 unwraps in production paths)

### Pattern A: Protocol frame parsing

| Line(s) | Context | Pattern |
|---------|---------|---------|
| 435 | `parse_client_frame()` | `buf[..4].try_into().unwrap()` - guarded by `buf.len() < 4` check above |
| 1186 | Acceptor batch loop | `buf[scan..scan+4].try_into().unwrap()` - guarded by `scan + 4 <= buf.len()` |
| 1204, 1210 | Slab sub-frame parse | `slab[pos..pos+4]` / `slab[pos+4..pos+8]` - guarded by `pos + 8 <= complete` |
| 1391 | Follower handshake | `handshake_buf[..4].try_into().unwrap()` - guarded by `len != 4` check |
| 1416-1420 | Batch header parse | 4 unwraps for seq/group/len/ack - guarded by `read_buf.len() >= BATCH_HEADER_LEN` |
| 1442 | Sub-payload counting | `payload[sub_pos..sub_pos+4]` - guarded by `sub_pos + 4 <= payload.len()` |
| 1613 | Test handler handshake | Same pattern as 1391 |
| 1628-1630 | Test handler batch header | Same pattern as 1416-1420 |
| 1642 | Test handler sub-payload | Same pattern as 1442 |

### Pattern B: ForwardedBatchIter

| Line(s) | Context | Pattern |
|---------|---------|---------|
| 1515 | `ForwardedBatchIter::next()` | `self.buf[self.pos..self.pos+4]` - guarded by `self.pos + 4 > self.buf.len()` |
| 1518, 1520 | Iterator fields | `client_id` and `request_seq` - guarded by `payload_len < 12` check |

**Risk**: MEDIUM - most are properly guarded by bounds checks before the unwrap. The `try_into().unwrap()` is technically infallible given the guards, but the guards and unwraps are separated, making it fragile to refactoring. Should still be converted for defense-in-depth.

---

## 4. Production Code: `segment_index.rs` (~18 unwraps + 2 test panics)

### Pattern: FrozenSegmentIndex binary parsing

| Line(s) | Function | Pattern |
|---------|----------|---------|
| 918 | `from_bytes()` | Version read - guarded by `data.len() < SIDX_HEADER_SIZE` |
| 922 | `from_bytes()` | Bucket count - same guard |
| 940 | `bucket_count()` | `self.data[12..16]` - no explicit guard (relies on from_bytes validation) |
| 961, 964, 967 | `lookup()` | Bucket fields - relies on from_bytes min_size validation |
| 985 | `count_all_records()` | rec_count in loop - same reliance |
| 996 | `segment_id()` | `self.data[8..12]` - relies on header size validation |
| 1023, 1024 | `find_entity()` | Record offset/len - relies on bucket validation |

### Test panics

| Line | Context | Code |
|------|---------|------|
| 1896 | Concurrent stress test | `unwrap_or_else(\|\| panic!("entity {eid} missing at end"))` |
| 2380 | Snapshot validity test | `unwrap_or_else(\|\| panic!("snapshot {i} produced invalid frozen index"))` |

**Risk**: MEDIUM - `from_bytes()` validates header size and bucket count, so the internal methods are "structurally safe" after construction. However, the validation doesn't verify that record offsets/counts stored in bucket data actually fall within `data.len()`. A corrupted index file could still cause panics in `find_entity()` if `rec_off + rec_count * SIDX_RECORD_SIZE > data.len()`.

---

## 5. Production Code: `manifest.rs` (~9 unwraps)

### Pattern: MDBX key/value byte parsing

| Line(s) | Function | Pattern |
|---------|----------|---------|
| 430 | `read_snapshot_data()` | `bytes[..8].try_into().unwrap()` - guarded by `bytes.len() == 8` |
| 525 | `read_structural_state()` | Same pattern, same guard |
| 534 | `read_structural_state()` | Same pattern |
| 597 | Entity key parsing | `key[1..9].try_into().unwrap()` - key is exactly 9 bytes (guarded) |
| 992 | Segment range lookup | `val[..8].try_into().unwrap()` - relies on MDBX value size |
| 1065 | Entity iteration | `key[1..9].try_into().unwrap()` - guarded by `key.len() != 9` |
| 1097-1099 | `decode_segment_range_value()` | 3 unwraps for segment_id, record_count, total_bytes - no explicit guard |

**Risk**: LOW-MEDIUM - most are guarded by explicit length checks. Line 1097-1099 in `decode_segment_range_value()` has no explicit guard and relies on MDBX storing correctly-sized values.

---

## 6. Production Code: `cursor.rs` (~8 unwraps)

### Pattern: Segment record parsing in MqSegmentCursor

| Line(s) | Function | Pattern |
|---------|----------|---------|
| 125 | `next_record()` batch sub-cmd | `data[off..off+4].try_into().unwrap()` - relies on batch_data validity |
| 145-149 | `next_record()` record_len | `view.read_slice(...).try_into().unwrap()` - guarded by offset+LENGTH_SIZE check |
| 190 | `next_record()` log_index | `view.read_slice(index_offset, 8).try_into().unwrap()` - guarded by total size |
| 204 | `next_record()` batch count | `cmd_bytes[8..12].try_into().unwrap()` - guarded by `cmd_bytes.len() >= 16` |
| 242 | `next_record_for_entity()` | `cmd.buf[8..16].try_into().unwrap()` - guarded by `cmd.buf.len() >= 16` |
| 282-286 | `next_record_raw()` record_len | Same as 145-149 |
| 317 | `next_record_raw()` log_index | Same as 190 |
| 456 | `MqSegmentScanner` entity scan | `rec.command.buf[8..16].try_into().unwrap()` - guarded by len >= 16 |

**Risk**: MEDIUM - most are guarded by preceding bounds checks, but a corrupted segment could still trigger panics in the batch sub-command parsing path (line 125).

---

## 7. Production Code: `async_apply.rs` (~5 production unwraps)

| Line | Context | Pattern | Risk |
|------|---------|---------|------|
| 1575 | Worker scan loop | `scanner.as_mut().unwrap()` | LOW - set to Some 3 lines above, structurally guaranteed |
| 1691 | Worker segment_id | `scan.current_segment_id().unwrap()` | MEDIUM - could be None if segment exhausted mid-loop |
| 1224 | Response frame parse | `buf[pos..pos+4].try_into().unwrap()` | MEDIUM - in response entry iteration |
| 1239 | Response client_id | `buf[payload_start..payload_start+4].try_into().unwrap()` | MEDIUM |

Plus ~30 unwraps in response entry view methods (lines 654-883) which delegate to types.rs/codec.rs patterns.

---

## 8. Production Code: `state_machine.rs` (1 expect)

| Line | Context | Pattern | Risk |
|------|---------|---------|------|
| 102 | `init_async_apply()` | `.expect("init_async_apply requires a prefetcher")` | MEDIUM - initialization failure shouldn't crash the process |
| 103 | `init_async_apply()` | `.unwrap_or(0)` on `last_applied.map(...)` | SAFE - unwrap_or is not a panic |

---

## 9. Production Code: `engine.rs` (1 unreachable)

| Line | Context | Pattern | Risk |
|------|---------|---------|------|
| 1963 | Batch ACK dispatch | `_ => unreachable!()` | LOW - match arms cover TAG_GROUP_ACK/NACK/RELEASE only; caller pre-filters. But a corrupted batch could reach this. |

---

## 10. Production Code: `write_batcher.rs` (1 unwrap)

| Line | Context | Pattern | Risk |
|------|---------|---------|------|
| 589 | `dispatch_response()` | `callers.into_iter().next().unwrap()` | LOW - guarded by `callers.len() == 1` check on line 588 |

---

## 11. Test Code Summary

Test files (`crates/mq/tests/`, `#[cfg(test)]` modules) contain ~200+ unwraps. These are acceptable in test code but should prefer `.expect("descriptive message")` over bare `.unwrap()` for debuggability. Key files:

- `consumer_group.rs` tests: ~100 unwraps (mostly HashMap lookups in assertions)
- `write_batcher.rs` tests: ~30 unwraps
- `forward.rs` tests: ~20 unwraps
- `async_apply.rs` tests: ~15 unwraps + 4 `panic!()` in match arms
- `engine.rs` tests: ~15 unwraps
- `manifest.rs` tests: ~80 unwraps
- Integration test files: ~50 unwraps total

---

## 12. Example/Benchmark Code Summary

Example files contain ~40 unwraps for CLI parsing, setup, and assertions. These are acceptable for benchmarks but ideally should use `anyhow` or similar for better error messages:

- `forward_bench.rs`: ~15 unwraps
- `bench.rs`: ~20 unwraps
- `parallel_apply_bench.rs`: ~6 unwraps + 1 `panic!()`
- `dedup_bench.rs`: ~4 unwraps

---

## Summary Statistics

| Category | Unwrap Count | Panic/Unreachable | Total |
|----------|-------------|-------------------|-------|
| Production code (types.rs) | ~20 | 0 | ~20 |
| Production code (codec.rs) | ~70 | 0 | ~70 |
| Production code (forward.rs) | ~25 | 0 | ~25 |
| Production code (segment_index.rs) | ~18 | 0 (2 in tests) | ~18 |
| Production code (manifest.rs) | ~9 | 0 | ~9 |
| Production code (cursor.rs) | ~8 | 0 | ~8 |
| Production code (async_apply.rs) | ~5 | 0 | ~5 |
| Production code (other) | ~3 | 1 unreachable | ~4 |
| **Production Total** | **~158** | **~1** | **~159** |
| Test code | ~200+ | ~6 | ~206+ |
| Example code | ~40 | ~1 | ~41 |
| **Grand Total** | **~398+** | **~8** | **~406+** |
