//! High-performance byte buffers with per-heap allocation.
//!
//! Two types sharing the same underlying representation:
//!
//! - [`Bytes`] — immutable, reference-counted, cloneable. 32 bytes on the stack.
//! - [`BytesMut`] — mutable, exclusively-owned, growable. 40 bytes on the stack.
//!
//! # Design
//!
//! Addresses all shortcomings of the `bytes` crate:
//!
//! - **Single allocation**: header (refcount + capacity + heap handle) and data
//!   are contiguous in one heap allocation. No separate `Box<Shared>`.
//! - **No vtable dispatch**: mode is a tag byte, not a function pointer table.
//!   Clone/drop are inlined match arms, not indirect calls.
//! - **Small buffer optimization**: payloads ≤ 30 bytes are stored inline
//!   in the `Bytes` struct itself — zero heap allocation, clone is memcpy.
//! - **Static mode**: `&'static [u8]` references are stored as a pointer —
//!   no refcount, no allocation, no-op clone/drop.
//! - **Frozen bit**: `BytesMut::freeze()` is a bit flip, not a type conversion
//!   with allocation. `Bytes::try_mut()` reverses it if refcount == 1.
//! - **Heap attribution**: every heap-backed buffer carries its [`Heap`] handle
//!   in the allocation header. Memory is freed to the correct arena on drop.
//!
//! # Representation
//!
//! Both types use a 32-byte stack representation with a tag at byte 31:
//!
//! ```text
//! Inline (≤ 30 bytes):
//! ┌──────────────────────────┬─────┬─────┐
//! │       data[0..30]        │ len │ tag │
//! └──────────────────────────┴─────┴─────┘
//!  0                        30    31
//!
//! Heap-allocated:
//! ┌──────────┬──────────┬──────────┬───────┬─────┐
//! │ ptr: *u8 │ len: u64 │ hdr: *H  │ pad:7 │ tag │
//! └──────────┴──────────┴──────────┴───────┴─────┘
//!  0          8          16         24     31
//!
//!   Header (16 bytes, at start of heap allocation):
//!   ┌────────────┬──────────┬──────────────┐
//!   │ref_cnt: u32│ cap: u32 │ heap: Heap:8 │
//!   └────────────┴──────────┴──────────────┘
//!   data follows immediately at header + 16
//!
//! Static:
//! ┌──────────┬──────────┬───────────────┬─────┐
//! │ ptr: *u8 │ len: u64 │   pad: 15     │ tag │
//! └──────────┴──────────┴───────────────┴─────┘
//! ```

mod inner;

pub use inner::{Bytes, BytesMut};
