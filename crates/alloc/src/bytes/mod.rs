//! High-performance byte buffers with per-heap allocation.
//!
//! Two types, both 32 bytes on the stack, with tag-based inline storage:
//!
//! - [`Bytes`] — immutable, reference-counted, cloneable. Inline up to 30 bytes.
//! - [`BytesMut`] — mutable, exclusively-owned, growable. Inline up to 22 bytes.
//!
//! # Design
//!
//! - **Single allocation**: header (refcount + capacity + heap pointer) and data
//!   are contiguous in one heap allocation. No separate `Box<Shared>`.
//! - **No vtable dispatch**: mode is a tag byte, not a function pointer table.
//!   Clone/drop are inlined match arms, not indirect calls.
//! - **Small buffer optimization**: both types store small payloads inline —
//!   `Bytes` up to 30 bytes, `BytesMut` up to 22 bytes — zero heap allocation.
//! - **Static mode** (`Bytes` only): `&'static [u8]` references are stored as
//!   a pointer — no refcount, no allocation, no-op clone/drop.
//! - **Zero-cost freeze**: `BytesMut::freeze()` is a `transmute` for heap buffers
//!   (both types share identical `HeapRepr` layout). Cold paths (inline, empty,
//!   heap→inline promotion) are outlined to keep the hot path tiny.
//! - **Sole-owner fast drop**: `Bytes::Drop` uses a plain `load(Acquire)` instead
//!   of expensive `lock xadd` when refcount == 1, saving ~5ns per drop.
//! - **Hot/cold path splitting**: `extend_from_slice`, `freeze`, and `Drop` split
//!   fast paths (heap mode, enough capacity) from cold paths (inline, growth,
//!   shared refcount) to minimize instruction cache pressure.
//! - **Heap attribution**: every heap-backed buffer carries its [`Heap`] handle
//!   in the allocation header. Memory is freed to the correct arena on drop.
//!
//! # Representation
//!
//! Both types use a 32-byte stack union with a tag at byte 31. `Bytes` and
//! `BytesMut` share the same `HeapRepr` for heap mode, enabling zero-cost
//! `freeze()`/`try_mut()` conversions.
//!
//! ```text
//! ── Bytes ──────────────────────────────────────────────
//!
//! Inline (≤ 30 bytes, tag=0):
//! ┌──────────────────────────┬─────┬─────┐
//! │       data[0..30]        │ len │ tag │
//! └──────────────────────────┴─────┴─────┘
//!  0                        30    31
//!
//! Heap (tag=1) — shared with BytesMut:
//! ┌──────────┬──────┬──────┬──────────┬───────┬─────┐
//! │ ptr: *u8 │len:32│cap:32│ hdr: *H  │ pad:7 │ tag │
//! └──────────┴──────┴──────┴──────────┴───────┴─────┘
//!  0          8      12     16         24     31
//!
//! Static (tag=2):
//! ┌──────────┬──────┬──────┬───────────────┬─────┐
//! │ ptr: *u8 │len:32│      │   pad: 15     │ tag │
//! └──────────┴──────┴──────┴───────────────┴─────┘
//!
//! ── BytesMut ───────────────────────────────────────────
//!
//! Inline (≤ 22 bytes, tag=0):
//! ┌──────────────┬──────────────────┬─────┬─────┐
//! │  heap: Heap  │   data[0..22]    │ len │ tag │
//! └──────────────┴──────────────────┴─────┴─────┘
//!  0              8                 30    31
//!
//! Heap (tag=1) — same HeapRepr as Bytes (freeze = transmute):
//! ┌──────────┬──────┬──────┬──────────┬───────┬─────┐
//! │ ptr: *u8 │len:32│cap:32│ hdr: *H  │ pad:7 │ tag │
//! └──────────┴──────┴──────┴──────────┴───────┴─────┘
//!
//! ── Shared heap header ─────────────────────────────────
//!
//!   Header (16 bytes, at start of heap allocation):
//!   ┌────────────┬──────────┬───────────────────┐
//!   │ref_cnt: u32│ cap: u32 │ heap_data: *const │
//!   └────────────┴──────────┴───────────────────┘
//!   data follows immediately at header + 16
//! ```
//!
//! `BytesMut` trades 8 bytes of inline capacity (30 → 22) to always carry a
//! [`Heap`] handle in inline mode, ensuring promotion to heap-backed storage
//! is always possible without requiring the caller to pass a heap reference.

mod inner;

pub use inner::{Bytes, BytesMut};
