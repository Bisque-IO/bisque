//! `bisque-alloc` — Memory-limited heap allocation for multi-tenant workloads.
//!
//! [`HeapMaster`] owns a mimalloc heap with an optional per-heap memory limit.
//! [`Heap`] is a cheap, cloneable handle. Keep the master alive as long as any
//! `Heap` clones exist.
//!
//! # Architecture
//!
//! ```text
//! HeapMaster::new(256 MiB)
//! ├── mi_heap_new_ex(allow_destroy) — create mimalloc heap
//! ├── mi_heap_set_memory_limit()    — per-heap limit (CAS-enforced, zero overshoot)
//! ├── HeapMaster::heap() → Heap     — cheap cloneable handle
//! ├── impl allocator_api2::Allocator— hashbrown HashMap/HashSet support
//! └── Drop: mi_heap_destroy         — bulk-free all pages and segments
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use bisque_alloc::{HeapMaster, Heap};
//! use bisque_alloc::collections;
//!
//! let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
//! let heap = master.heap();
//!
//! let mut v = collections::Vec::new(&heap);
//! v.extend_from_slice(&[1, 2, 3]).unwrap();
//!
//! let mut m = collections::HashMap::<String, u64>::new(&heap);
//! m.try_insert("key".into(), 42).unwrap();
//! ```

pub mod arc;
pub mod boxed;
pub mod bytes;
pub mod collections;
pub mod epoch;
pub mod heap;
pub mod padded;
pub mod slab;
pub mod string;
pub mod striped;
pub mod tlrc;
pub mod vec;
pub mod wait_queue;

pub use arc::HeapArc;
pub use boxed::HeapBox;
pub use bytes::{Bytes, BytesMut};
pub use collections::art::fixed::{
    ArtKey, FixedArt, FixedReadGuard, FixedWriteGuard, U32Art, U64Art, U128Art,
    sharded::{ShardedFixedArt, ShardedU32Art, ShardedU64Art, ShardedU128Art},
};
pub use collections::{HashMap, HashSet, art::Art};
pub use epoch::heap::{Collector as HeapCollector, Epoch, EpochGuard as HeapEpochGuard};
pub use epoch::{EpochBox, EpochRef};
pub use heap::{Heap, HeapMaster};
pub use padded::CachePadded;
pub use string::String;
pub use tlrc::{Tlrc, TlrcRef};
pub use vec::Vec;

pub type HeapBytes = bytes::Bytes;
pub type HeapBytesMut = bytes::BytesMut;
pub type HeapString = string::String;
pub type HeapVec = vec::Vec<u8>;

/// Re-export the mimalloc global allocator for use as `#[global_allocator]`.
pub use bisque_mimalloc_sys::MiMalloc;
