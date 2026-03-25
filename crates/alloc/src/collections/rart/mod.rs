//! # RART - Adaptive Radix Tree with heap-backed allocation
//!
//! A high-performance, memory-efficient implementation of Adaptive Radix Trees (ART)
//! using [`Heap`](crate::Heap)-backed allocation with fallible operations.
//!
//! All internal node allocations go through the heap, and every mutating
//! operation returns `Result<_, AllocError>` instead of panicking on OOM.

mod node;
pub mod published_tree;

#[doc(hidden)]
pub mod mapping;
#[doc(hidden)]
pub mod utils;

pub mod cow_tree;
pub mod iter;
pub mod keys;
pub mod partials;
pub mod range;
pub mod stats;
pub mod tree;

pub use keys::{KeyTrait, array_key::ArrayKey, vector_key::VectorKey};
pub use partials::Partial;
pub use tree::AdaptiveRadixTree;
