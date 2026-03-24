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

pub mod bytes;
pub mod collections;
pub mod heap;
mod os_mmap;
pub mod padded;
pub mod ptr;
pub mod striped;
pub mod wait_queue;

pub use heap::{Heap, HeapMaster};
pub use ptr::{EpochBox, EpochRef, HeapArc, HeapBox, Tlrc, TlrcRef};

/// Alias for [`collections::Vec`] (heap-backed byte buffer).
pub type HeapVec = collections::Vec;

/// Re-export the mimalloc global allocator for use as `#[global_allocator]`.
pub use bisque_mimalloc_sys::MiMalloc;
