//! `bisque-alloc` — Fixed-size, mmap-backed heap allocation for multi-tenant workloads.
//!
//! Each [`Heap`] owns a fixed-size `mmap` region registered as an exclusive
//! mimalloc arena. Allocations routed through the heap are confined to that
//! region — when it's full, allocations fail with `AllocError` instead of
//! growing unbounded.
//!
//! # Architecture
//!
//! ```text
//! Heap::new(256 MiB)
//! ├── os_mmap::alloc_pages()        — mmap/VirtualAlloc fixed region
//! ├── mi_manage_os_memory_ex()      — register as exclusive mimalloc arena
//! ├── mi_heap_new_ex(allow_destroy) — heap pinned to that arena
//! ├── impl allocator_api2::Allocator— hashbrown HashMap/HashSet support
//! └── Drop: mi_heap_destroy → mi_arena_unload → munmap/VirtualFree
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use bisque_alloc::Heap;
//! use bisque_alloc::collections;
//!
//! let heap = Heap::new(64 * 1024 * 1024).unwrap(); // 64 MiB arena
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
pub mod heap;
mod os_mmap;
pub mod padded;
pub mod striped;
pub mod wait_queue;

pub use arc::Arc;
pub use boxed::Box;
pub use heap::Heap;

/// Alias for [`collections::Vec`] (heap-backed byte buffer).
pub type HeapVec = collections::Vec;

/// Re-export the mimalloc global allocator for use as `#[global_allocator]`.
pub use bisque_mimalloc_sys::MiMalloc;
