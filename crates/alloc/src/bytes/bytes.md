# Bisque Bytes and BytesMut Design Document

## 1. Overview

`Bytes` and `BytesMut` are high-performance byte buffer types that serve as
the foundation for zero-copy I/O in bisque. Both are exactly **32 bytes** on
the stack, use tag-based dispatch (no vtable), and store small payloads inline
with no heap allocation.

**Bytes** is an immutable, reference-counted, cloneable byte buffer.
Three modes: inline (up to 30 bytes), heap (shared, arena-backed), and static
(`&'static [u8]` with no-op clone/drop).

**BytesMut** is a mutable, exclusively-owned, growable byte buffer.
Two modes: inline (up to 22 bytes, always carrying a `Heap` handle for future
promotion), and heap (refcounted, arena-backed, same `HeapRepr` as `Bytes`).

### Design goals

- **Zero-copy protocol parsing**: `split_to` on `BytesMut` returns an
  immutable `Bytes` view sharing the same heap allocation -- no memcpy for
  parsed frames.
- **No vtable dispatch**: mode is a tag byte at offset 31, not a function
  pointer table. Clone, drop, and freeze are inlined match arms.
- **Small buffer optimization**: most protocol headers, small messages, and
  metadata fit inline with zero heap allocation.
- **Per-heap allocation**: every heap-backed buffer is allocated from a
  mimalloc arena via a `Heap` handle, enabling thread-local allocation and
  correct cross-thread deallocation.
- **Sole-owner fast drop**: avoids `lock xadd` when refcount is 1.
- **Zero-cost freeze**: `BytesMut::freeze()` is `transmute` for the common
  case (heap buffer > 30 bytes).


## 2. Memory Layout

All representations occupy exactly 32 bytes. The tag byte at offset 31
discriminates the mode.

### 2.1 Bytes

```
Inline (tag=0, data up to 30 bytes):

 byte:  0                              30   31
       +-------------------------------+----+----+
       |         data[0..30]           |len | 0  |
       +-------------------------------+----+----+
                   30 bytes              1B   1B

Heap (tag=1, shared HeapRepr):

 byte:  0        8     12    16       24    31
       +--------+-----+-----+--------+-----+----+
       |ptr: *u8|len  |cap  |hdr: *H |pad:7| 1  |
       | 8B     | u32 | u32 | 8B     |     |    |
       +--------+-----+-----+--------+-----+----+

Static (tag=2):

 byte:  0        8     12              24    31
       +--------+-----+---------------+-----+----+
       |ptr: *u8|len  |   pad: 15B          | 2  |
       | 8B     | u32 |                     |    |
       +--------+-----+---------------+-----+----+
       header is null, cap is 0. No refcount, no-op clone/drop.
```

### 2.2 BytesMut

```
Inline (tag=0, data up to 22 bytes):

 byte:  0         8                    30   31
       +---------+---------------------+----+----+
       |  Heap   |     data[0..22]     |len | 0  |
       |  (8B)   |       22 bytes      | 1B | 1B |
       +---------+---------------------+----+----+

       The Heap handle (8 bytes) is always present so that promotion
       to heap mode never requires the caller to pass a heap reference.

Heap (tag=1, identical HeapRepr -- freeze = transmute):

 byte:  0        8     12    16       24    31
       +--------+-----+-----+--------+-----+----+
       |ptr: *u8|len  |cap  |hdr: *H |pad:7| 1  |
       | 8B     | u32 | u32 | 8B     |     |    |
       +--------+-----+-----+--------+-----+----+
```

Key point: `Bytes` heap mode and `BytesMut` heap mode use the **exact same
`HeapRepr` struct** at identical field offsets. This is what enables zero-cost
`freeze()` and `try_mut()`.

### 2.3 Shared Heap Header

Every heap allocation is prefixed with a 16-byte `Header`:

```
       +------------+----------+-------------------+
       | ref_cnt    | cap      | heap_data         |
       | AtomicU32  | u32      | *const HeapData   |
       | (4 bytes)  | (4 bytes)| (8 bytes)         |
       +------------+----------+-------------------+
       |         data bytes follow here ...        |
       +-------------------------------------------+
       offset 0     4          8                   16
```

- `ref_cnt`: atomic reference count with FROZEN_BIT at bit 31.
- `cap`: the allocated capacity in bytes (data region only, excludes header).
- `heap_data`: raw pointer to the owning `HeapData` (mimalloc heap). This is a
  raw pointer, NOT a TLRC-refcounted handle, avoiding 2 TLRC bracket
  operations (6 atomic stores + 2 TLS lookups) per alloc/dealloc cycle.

Data follows the header immediately at `header_ptr + 16`. The `ptr` field in
`HeapRepr` always points into this data region (and may be advanced past the
header base by `split_to`).


## 3. Refcounting

### 3.1 The FROZEN_BIT

`FROZEN_BIT = 1 << 31` is set in `ref_cnt` at allocation time:

```rust
ref_cnt: AtomicU32::new(1 | FROZEN_BIT)
```

All reads of the actual count use `COUNT_MASK = !FROZEN_BIT` to strip bit 31:

```rust
fn count(&self) -> u32 {
    self.ref_cnt.load(Ordering::Acquire) & COUNT_MASK
}
```

The FROZEN_BIT has no behavioral effect during normal operation -- it is always
set. Its purpose is compatibility with `try_mut()`, which clears it via
`store(1)` to mark the buffer as exclusively owned and mutable again. This
avoids needing a separate flag field.

### 3.2 Sole-owner fast-path drop

Dropping a heap-backed `Bytes` or `BytesMut` checks the refcount:

```rust
// Fast path: sole owner -- plain load avoids expensive `lock xadd`.
if h.ref_cnt.load(Ordering::Acquire) & COUNT_MASK == 1 {
    Heap::dealloc_raw(h.heap_data, header as *mut u8);
    return;
}
// Shared path: atomic decrement.
if h.ref_cnt.fetch_sub(1, Ordering::Release) & COUNT_MASK == 1 {
    fence(Ordering::Acquire);
    Heap::dealloc_raw(h.heap_data, header as *mut u8);
}
```

When the count is 1, no other thread holds a reference. A plain
`load(Acquire)` suffices -- on x86, all loads have acquire semantics at the
hardware level. This avoids the expensive `lock xadd` instruction
(`fetch_sub`), saving approximately 5ns per drop in the common case.

The shared path uses `fetch_sub` with `Release` ordering. If the decremented
value reaches 1, this thread is the last owner and deallocates after an
`Acquire` fence to synchronize with prior `Release` stores from other threads.

### 3.3 Clone

- **Inline/Static**: 32-byte memcpy. No atomics.
- **Heap**: `fetch_add(1, Relaxed)` on `ref_cnt`, then 32-byte memcpy of the
  stack representation.

BytesMut clone always allocates a fresh buffer and copies data (deep clone),
since BytesMut must be exclusively owned.


## 4. Zero-Copy split_to / split_off

### 4.1 The sliding window design

`BytesMut::split_to(at)` is the core operation for zero-copy protocol parsing.
For heap-backed buffers it works as follows:

```
Before split_to(at):

  Header
  +------+------+-----------+
  |refcnt| cap  | heap_data |
  +------+------+-----------+
  |  data data data data data data ...  |
  ^                                     ^
  |<-------- BytesMut [ptr..ptr+len) --------->|
  |<-------- capacity --------------------------->|

After split_to(at):

  Header (refcount incremented to 2)
  +------+------+-----------+
  |refcnt| cap  | heap_data |
  +------+------+-----------+
  |  data data data data data data ...  |
  ^              ^                      ^
  |<-- Bytes -->|<--- BytesMut -------->|
  [ptr..ptr+at)  [ptr+at..ptr+at+remaining)
                 cap reduced by `at`
```

1. Increment the header refcount (`fetch_add(1, Relaxed)`).
2. Create a `Bytes` pointing at `[ptr..ptr+at)` with `cap=0`.
3. Advance BytesMut's `ptr` by `at`, decrement both `len` and `cap` by `at`.

### 4.2 The structural invariant

The key insight that eliminates COW checks on the write path:

> **Bytes views occupy memory BEFORE BytesMut's current `ptr`. BytesMut writes
> only to memory AFTER `ptr+len` (up to `ptr+cap`). These regions never
> overlap.**

Because `split_to` advances `ptr`, the returned `Bytes` view covers memory
that BytesMut will never touch again. BytesMut's writable region
`[ptr..ptr+cap)` starts after all previously split-off `Bytes` views. This
means:

- No COW check is needed before writing to BytesMut.
- No data copying is needed when splitting.
- The only cost is one atomic increment per split.

### 4.3 split_off

`split_off(at)` returns `[at..len)` as `Bytes`, leaving `[0..at)` in
BytesMut. The critical detail: **cap is clamped to `at`**:

```rust
self.repr.heap.cap = at as u32;
```

This prevents BytesMut from writing into the tail region that the returned
`Bytes` now owns. Any subsequent `extend_from_slice` that exceeds the clamped
cap will trigger `try_grow`, which allocates a fresh buffer.

### 4.4 try_grow

When BytesMut needs more capacity than available:

- **Sole owner** (refcount == 1): `mi_heap_realloc` in place. The header,
  refcount, and data pointer are updated. This is the fast path.
- **Shared** (refcount > 1, from prior `split_to`/`split_off`): allocates a
  fresh buffer via `mi_heap_malloc`, copies only the live data (`len` bytes,
  not the entire old allocation), decrements the old refcount, and points
  BytesMut at the new allocation. This is the cold path (`grow_shared`).


## 5. Freeze / try_mut

### 5.1 freeze (BytesMut -> Bytes)

```rust
pub fn freeze(self) -> Bytes {
    if self.repr.tag() == TAG_HEAP {
        let len = unsafe { self.repr.heap.len as usize };
        if len > INLINE_CAP {
            return unsafe { std::mem::transmute(self) };
        }
    }
    self.freeze_cold()
}
```

**Hot path** (heap mode, len > 30): `transmute`. Both types share identical
`HeapRepr` layout at the same field offsets. The FROZEN_BIT is already set at
allocation time. Zero copies, zero atomics, zero branches beyond the tag
check.

**Cold path** (`freeze_cold`, marked `#[cold] #[inline(never)]`):

- Empty buffer: returns `Bytes::new()`.
- Inline BytesMut: copies up to 22 bytes into a 30-byte `InlineRepr` (wider
  inline capacity for Bytes).
- Heap with len <= 30: promotes to inline Bytes (copies data, releases the
  heap allocation if sole owner, otherwise decrements refcount).

### 5.2 try_mut (Bytes -> BytesMut)

```rust
pub fn try_mut(self) -> Result<BytesMut, Self> {
    if self.repr.tag() != TAG_HEAP { return Err(self); }
    // ... check refcount == 1 ...
    h.ref_cnt.store(1, Ordering::Release);  // clear FROZEN_BIT
    // 32-byte memcpy reinterpret
    // Restore cap from header
    Ok(bm)
}
```

Succeeds only for heap-backed `Bytes` with refcount == 1. Clears FROZEN_BIT
via `store(1)`, copies the 32-byte stack repr, restores `cap` from the header
(since Bytes may have zeroed it during slice/split operations), and forgets the
original Bytes.


## 6. Hot/Cold Path Splitting

The implementation aggressively separates fast paths from cold paths using
`#[cold]` and `#[inline(never)]` attributes. This is a deliberate I-cache
optimization: the fast path stays small enough to be inlined at the call site,
while cold paths are outlined into separate functions that are rarely called.

### extend_from_slice

```
extend_from_slice(data)
  |
  +-- Fast: tag==HEAP && len+data_len <= cap
  |     -> memcpy, update len, return
  |
  +-- Slow: extend_from_slice_slow()  [#[cold] #[inline(never)]]
        |
        +-- Inline, fits in 22B -> memcpy into inline data
        +-- Inline, needs promotion -> promote_to_heap(), then memcpy
        +-- Heap, needs growth -> try_grow(), then memcpy
```

### freeze

```
freeze(self)
  |
  +-- Hot: tag==HEAP && len > 30 -> transmute (zero-cost)
  |
  +-- Cold: freeze_cold()  [#[cold] #[inline(never)]]
        +-- empty -> Bytes::new()
        +-- inline -> copy to InlineRepr
        +-- heap, small -> promote to inline, release alloc
```

### Drop (Bytes)

```
drop(&mut self)
  |
  +-- tag != HEAP -> return (no-op for inline/static)
  |
  +-- count == 1 -> dealloc directly (plain load, no atomic RMW)
  |
  +-- count > 1 -> fetch_sub, dealloc if last
```


## 7. Design Differences from the bytes Crate

| Aspect                  | bisque Bytes/BytesMut             | bytes crate                      |
|-------------------------|-----------------------------------|----------------------------------|
| **Stack size**          | 32 bytes (both types)             | 32 bytes (both types)            |
| **Inline storage**      | Bytes: 30B, BytesMut: 22B         | None                             |
| **Dispatch mechanism**  | Tag byte at offset 31             | Vtable (function pointer table)  |
| **Allocation**          | Per-heap via mimalloc arena        | Global allocator                 |
|                         | (Header prefix in same alloc)     | (separate Arc-like alloc)        |
| **Freeze**              | transmute for heap (zero-cost)    | Vtable pointer setup             |
| **split_to (heap)**     | Zero-copy, shared alloc           | Zero-copy, shared alloc          |
| **COW on write**        | None -- structural invariant      | Runtime refcount check           |
|                         | (ptr always past split views)     | (may trigger COW copy)           |
| **Drop (sole owner)**   | Plain load (no lock xadd)         | Vtable indirect call + atomic    |
| **Drop (shared)**       | fetch_sub + conditional dealloc   | Vtable indirect call + atomic    |
| **Clone (inline)**      | 32-byte memcpy, no atomics        | N/A (no inline mode)             |
| **Clone (heap)**        | fetch_add + 32-byte memcpy        | fetch_add + memcpy               |
| **Clone (static)**      | 32-byte memcpy, no atomics        | Vtable copy, no atomics          |
| **Static mode**         | tag=2, ptr+len, no-op drop        | Vtable with static promoter fn   |
| **Refcount location**   | Header prefix in data allocation  | Separate Arc-like allocation     |
| **BytesMut Heap handle**| Embedded in inline repr (8B)      | N/A (uses global allocator)      |
| **Capacity growth**     | next_power_of_two, min 64         | Varies by strategy               |
| **Max inline**          | Bytes: 30B, BytesMut: 22B         | 0B (always heap or static)       |


## 8. Protocol Parsing Pattern

The recommended usage pattern for network protocol parsing with bisque:

```rust
use bisque_alloc::{Heap, HeapMaster, bytes::{Bytes, BytesMut}};

fn parse_frames(
    buf: &mut BytesMut,
    frames: &mut Vec<Bytes>,
) {
    loop {
        if buf.len() < 4 {
            break; // need more data
        }

        // Read frame length from first 4 bytes (without consuming).
        let frame_len = u32::from_be_bytes(
            [buf[0], buf[1], buf[2], buf[3]]
        ) as usize;
        let total = 4 + frame_len;

        if buf.len() < total {
            break; // incomplete frame, wait for more data
        }

        // Zero-copy split: returns Bytes sharing the same allocation.
        // BytesMut's ptr advances past the frame -- no memcpy,
        // no COW check needed on subsequent writes.
        let frame = buf.split_to(total);

        // frame is now an immutable Bytes. It can be sent to other
        // threads, cloned cheaply (atomic increment), and dropped
        // independently. BytesMut continues writing to the remaining
        // region without interference.
        frames.push(frame);
    }
}

// Setup:
fn example() {
    let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
    let heap = master.new_heap();
    let mut buf = BytesMut::with_capacity(4096, &heap).unwrap();

    // Simulate receiving data from the network.
    buf.extend_from_slice(&1024u32.to_be_bytes()).unwrap();
    buf.extend_from_slice(&vec![0u8; 1024]).unwrap();

    let mut frames = Vec::new();
    parse_frames(&mut buf, &mut frames);

    // frames[0] is a Bytes view into the original allocation.
    // buf still has capacity for more data without reallocation.
    // When all Bytes views and the BytesMut are dropped, the
    // allocation is freed to the originating mimalloc heap.
}
```

### What happens under the hood

1. `BytesMut::with_capacity(4096, &heap)` allocates a single contiguous block:
   16-byte Header + 4096 bytes of data, from the mimalloc arena.

2. `extend_from_slice` appends data. The hot path (heap mode, enough capacity)
   is a single `memcpy` + length update -- no branches beyond the tag check.

3. `split_to(total)` increments the header refcount (one `fetch_add`), creates
   a `Bytes` pointing at the consumed region, and advances BytesMut's pointer.
   Total cost: one atomic increment + stack struct initialization.

4. BytesMut continues appending to the region after the split point. No COW
   check is needed because the structural invariant guarantees non-overlapping
   regions.

5. When `Bytes` is dropped and it is the sole remaining reference, it
   deallocates with a plain `load` (no `lock xadd`). When shared, it uses
   `fetch_sub`.


## 9. Performance Characteristics

### Faster than bytes crate

| Operation                  | Why faster                                            |
|----------------------------|-------------------------------------------------------|
| Small buffer alloc/drop    | Inline storage (30B/22B) avoids heap entirely         |
| Clone (small data)         | 32-byte memcpy, zero atomics (vs always-atomic clone) |
| Drop (sole owner)          | Plain load vs lock xadd (~5ns savings on x86)         |
| Drop (inline/static)       | No-op / immediate return (no vtable indirection)      |
| freeze (heap, >30B)        | transmute (vs vtable pointer setup + field shuffle)    |
| extend_from_slice (hot)    | Direct memcpy, no vtable call, no COW check           |
| Write after split_to       | No COW check needed (structural invariant)            |
| Allocation                 | mimalloc arena, single alloc (header+data contiguous) |
| Dispatch                   | Tag byte match (1 cmp) vs vtable load + indirect call |

### Comparable to bytes crate

| Operation                  | Notes                                                 |
|----------------------------|-------------------------------------------------------|
| split_to / split_off       | Both are zero-copy with one atomic increment          |
| Clone (large heap data)    | Both do fetch_add + memcpy                            |
| Shared drop                | Both do fetch_sub + conditional dealloc               |

### Tradeoffs

| Aspect                     | Tradeoff                                              |
|----------------------------|-------------------------------------------------------|
| BytesMut inline capacity   | 22B vs 30B for Bytes (8B used by Heap handle)         |
| Max buffer size            | u32 len/cap limits to ~4GB (vs usize in bytes crate)  |
| HeapMaster lifetime        | HeapMaster must outlive all derived Bytes/BytesMut     |
| No custom vtable extension | Fixed set of modes (inline/heap/static) only           |
| freeze (small heap data)   | Cold path copies + may dealloc (bytes crate: uniform)  |
