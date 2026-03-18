# Bisque-MQ Panic Remediation Plan

Based on the analysis in [report.md](./report.md), this plan eliminates ~159 production panic points while preserving zero-copy performance characteristics.

## Design Principles

1. **`Result` by default** - All fallible operations return `Result<T, DecodeError>` or `io::Result<T>`
2. **Unwrap only with proof** - `.unwrap()` is acceptable only when the invariant is provably upheld within the same scope (e.g., `len == 1` check then `.next().unwrap()`)
3. **Validate at boundaries** - Validate buffer sizes at construction/entry points; internal methods can rely on validated state
4. **No performance regression** - Safe helpers must be `#[inline]` and compile to the same bounds-check-then-read pattern

## Error Type

```rust
/// Error returned when decoding a binary buffer fails.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DecodeError {
    #[error("buffer too short: need {need} bytes at offset {offset}, have {have}")]
    BufferTooShort { need: usize, offset: usize, have: usize },

    #[error("invalid data: {0}")]
    Invalid(String),
}
```

This lives in `types.rs` alongside `MqCommand` and is re-exported from `lib.rs`.

---

## Phase 1: Safe byte-reading helpers (`types.rs`)

Create `#[inline]` helper functions that return `Result<T, DecodeError>`:

```rust
#[inline]
pub fn read_u16_le(buf: &[u8], offset: usize) -> Result<u16, DecodeError> { ... }

#[inline]
pub fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32, DecodeError> { ... }

#[inline]
pub fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64, DecodeError> { ... }

#[inline]
pub fn read_i32_le(buf: &[u8], offset: usize) -> Result<i32, DecodeError> { ... }
```

Each checks `offset + N <= buf.len()` before slicing. The compiler will elide the check when it can prove bounds.

### Changes to `types.rs`

- `buf_field_u64()` -> returns `Result<u64, DecodeError>`
- `buf_field_u32()` -> returns `Result<u32, DecodeError>`
- `buf_field_vec_u64()` -> returns `Result<DecodeU64s, DecodeError>`
- `MqCommand::cmd_size()` -> returns `Result<u32, DecodeError>`
- `MqCommand::fixed_size()` -> returns `Result<u16, DecodeError>`
- `MqCommand::field_u64()` -> returns `Result<u64, DecodeError>`
- `MqCommand::field_u32()` -> returns `Result<u32, DecodeError>`
- `MqCommand::field_flex8()` -> returns `Result<&[u8], DecodeError>`
- `MqCommand::field_flex8_str()` -> returns `Result<&str, DecodeError>`
- `MqCommand::field_opt_flex8_bytes()` -> returns `Result<Option<Bytes>, DecodeError>`
- `MqCommand::field_vec_u64()` -> returns `Result<DecodeU64s, DecodeError>`
- `MqCommand::field_vec_bytes_count()` -> returns `Result<u32, DecodeError>`
- `MqCommand::field_vec_bytes_get()` -> returns `Result<&[u8], DecodeError>`
- `MqCommand::field_vec_bytes_get_bytes()` -> returns `Result<Bytes, DecodeError>`

**Estimated impact**: ~20 unwraps eliminated.

---

## Phase 2: Codec view structs (`codec.rs`)

All view struct methods that read fields will propagate `Result<T, DecodeError>` using the safe helpers from Phase 1.

### Approach

Each accessor method like:
```rust
pub fn topic_id(&self) -> u64 {
    u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
}
```

Becomes:
```rust
pub fn topic_id(&self) -> Result<u64, DecodeError> {
    read_u64_le(&self.buf, 8)
}
```

### View structs to update:
- `CmdPublish` - 8 methods
- `CmdDisposition` - 6 methods
- `CmdGroupJoin` - 8 methods + iterator
- `CmdGroupLeave` - 3 methods
- `CmdGroupHeartbeat` - 4 methods + iterator
- `CmdProducePartition` - 5 methods
- `CmdActorDispatch` - 4 methods
- `CmdForwardedBatch` - 2 methods
- `CmdForwardedBatchIter` - iterator (3 reads per step)
- `CmdExchangePublish` - 6 methods
- `CmdExchangePublishIter` - iterator
- `CmdSetTopicProps` - 6 methods
- `CmdPartitionRangeOp` - 4 methods
- `CmdPartitionRangeOpIter` - iterator
- `PublishMessageIter` - iterator
- `FlatMqCommand` accessor - 1 method
- `decode_u64s_at` - loop body
- `read_flex8_raw` / `read_flex8_raw_at` - 2 functions

### Call-site updates

All callers of these accessors throughout the crate must be updated to handle `Result`. This primarily affects:
- `engine.rs` - command application logic (use `?` propagation)
- `state_machine.rs` - command classification
- `async_apply.rs` - worker command processing
- `cursor.rs` - record/batch parsing
- `forward.rs` - forwarded batch iteration

**Estimated impact**: ~70 unwraps eliminated.

---

## Phase 3: Forward protocol parsing (`forward.rs`)

### Approach

Most unwraps in forward.rs are already guarded by bounds checks. Convert the pattern:
```rust
// Before
if buf.len() < 4 { return None; }
let client_id = u32::from_le_bytes(buf[..4].try_into().unwrap());

// After
if buf.len() < 4 { return None; }
let client_id = read_u32_le(buf, 0).ok()?;  // or use map_err for io::Error
```

For the batch header parsing in acceptor loops, use a helper that returns `io::Result`:
```rust
fn parse_batch_header(buf: &[u8]) -> io::Result<BatchHeader> { ... }
```

### Specific changes:
- `parse_client_frame()` - return `Option<(u32, &[u8])>` (already does, just remove unwrap)
- Acceptor frame loop - use safe helpers with `?`
- `ForwardedBatchIter::next()` - use safe helpers, return `None` on decode failure
- Follower handshake parsing - use safe helpers with `?`
- Batch header parsing (both production and test paths) - use safe helpers

**Estimated impact**: ~25 unwraps eliminated.

---

## Phase 4: Segment index (`segment_index.rs`)

### Approach

`FrozenSegmentIndex::from_bytes()` already validates header size. Strengthen it to also validate that bucket data references are in-bounds. For internal methods, use the safe read helpers.

### Specific changes:
- `from_bytes()` - use `read_u32_le` for version and bucket_count reads
- `bucket_count()` - use `read_u32_le`, return `Result`
- `lookup()` - use safe helpers, return `Option` (already does via wrapper)
- `count_all_records()` - use safe helpers
- `segment_id()` - use `read_u32_le`
- `find_entity()` - use safe helpers, add bounds check for record region
- Test panics (lines 1896, 2380) - leave as-is (test code, `.expect()` is fine)

**Estimated impact**: ~18 unwraps eliminated.

---

## Phase 5: Manifest parsing (`manifest.rs`)

### Approach

Most manifest reads are guarded. Add explicit bounds checks to `decode_segment_range_value()` and use safe helpers.

### Specific changes:
- `decode_segment_range_value()` - add `val.len() >= 24` guard, use safe helpers
- NEXT_ID / STRUCTURAL_FLOOR reads - use safe helpers (already guarded by `bytes.len() == 8`)
- Entity key parsing - use safe helpers (already guarded by `key.len() != 9`)
- Segment range lookup - use safe helpers with bounds check

**Estimated impact**: ~9 unwraps eliminated.

---

## Phase 6: Cursor parsing (`cursor.rs`)

### Approach

Use safe helpers in `MqSegmentCursor::next_record()` and `next_record_raw()`. For batch sub-command parsing, add bounds validation.

### Specific changes:
- `next_record()` record_len - use `read_u32_le` with error → return `None`
- `next_record()` log_index - use `read_u64_le`
- `next_record()` batch count - use `read_u32_le`
- `next_record()` batch sub-cmd size - add `off + 4 <= data.len()` check
- `next_record_raw()` - same pattern as `next_record()`
- `next_record_for_entity()` - use `read_u64_le` for entity_id extraction
- `MqSegmentScanner` entity scan - same

**Estimated impact**: ~8 unwraps eliminated.

---

## Phase 7: Async apply and remaining files

### `async_apply.rs`
- Line 1575: `scanner.as_mut().unwrap()` - This is structurally guaranteed (set 3 lines above). Keep but add a comment: `// SAFETY: set to Some in the block above`.
- Line 1691: `scan.current_segment_id().unwrap()` - convert to `.ok_or()` with error logging and `continue`.
- Response entry parsing (lines 1224, 1239) - use safe helpers.

### `state_machine.rs`
- Line 102: `.expect("init_async_apply requires a prefetcher")` - change to return `Result<(), DecodeError>` or similar.

### `engine.rs`
- Line 1963: `_ => unreachable!()` - replace with error response and `tracing::error!` log.

### `write_batcher.rs`
- Line 589: `callers.into_iter().next().unwrap()` - structurally safe (guarded by `len() == 1`). Add comment documenting the invariant.

**Estimated impact**: ~7 unwraps/panics eliminated or documented.

---

## Execution Order

The phases must be executed in order because later phases depend on earlier ones:

1. Phase 1 (types.rs) - creates the error type and safe helpers that everything else uses
2. Phase 2 (codec.rs) - converts view struct accessors; many callers use these
3. Phases 3-7 (forward, segment_index, manifest, cursor, async_apply, etc.) - can proceed in any order as they consume the helpers

## Out of Scope

- **Test code**: Test unwraps are acceptable. No changes planned for `#[cfg(test)]` modules or `tests/` directory.
- **Example/benchmark code**: Unwraps in `examples/` are acceptable for benchmarks.
- **Upstream crate changes**: No changes to `bisque-raft` or other crates.
