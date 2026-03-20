//! Heap-backed collections with fallible allocation.
//!
//! Every mutating operation returns `Result<_, AllocError>` instead of
//! panicking on OOM. All memory is allocated from a [`Heap`](crate::Heap).

mod btree_map;
mod hash_map;
mod hash_set;
mod vec;

pub use btree_map::BTreeMap;
pub use hash_map::HashMap;
pub use hash_set::HashSet;
pub use vec::Vec;
