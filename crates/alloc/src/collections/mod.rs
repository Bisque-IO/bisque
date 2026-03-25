//! Heap-backed collections with fallible allocation.
//!
//! Every mutating operation returns `Result<_, AllocError>` instead of
//! panicking on OOM. All memory is allocated from a [`Heap`](crate::Heap).

pub mod art;
pub mod congee;
mod hash_map;
mod hash_set;
pub mod rart;

pub use hash_map::HashMap;
pub use hash_set::HashSet;
