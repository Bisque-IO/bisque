//! ART epoch — re-exports from `crate::epoch::heap` with ART-specific dealloc.

pub use crate::epoch::heap::{Collector, Epoch, EpochGuard};

use crate::Heap;

use super::node::dealloc_node;

/// ART-specific dealloc function for epoch retirement.
///
/// # Safety
///
/// `ptr` must be a valid ART node pointer (leaf or internal).
pub unsafe fn art_dealloc(heap: &Heap, ptr: usize) {
    dealloc_node(heap, ptr);
}

/// Create an ART epoch bound to a collector.
pub fn new_art_epoch(collector: &Collector, heap: &Heap) -> Epoch {
    Epoch::new(collector, heap, art_dealloc)
}
