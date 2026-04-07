//! ART epoch — re-exports from `crate::epoch::heap` with ART-specific dealloc.

pub use crate::epoch::heap::{Collector, DeallocFn, Epoch, EpochGuard};

use crate::Heap;
use crate::heap::HeapData;

use super::node::dealloc_node;

/// ART-specific dealloc function for epoch retirement.
///
/// # Safety
///
/// `ptr` must be a valid ART node pointer (leaf or internal).
pub unsafe fn art_dealloc(heap_data: *const HeapData, ptr: usize) {
    dealloc_node_raw(heap_data, ptr);
}

/// Dealloc an ART node using raw HeapData pointer (no Heap clone needed).
unsafe fn dealloc_node_raw(heap_data: *const HeapData, p: usize) {
    use super::node::{NULL_CHILD, is_leaf, leaf_ptr};
    if p == NULL_CHILD {
        return;
    }
    if is_leaf(p) {
        Heap::dealloc_raw(heap_data, leaf_ptr(p) as *mut u8);
    } else {
        Heap::dealloc_raw(heap_data, p as *mut u8);
    }
}

/// Create an ART epoch bound to a collector.
pub fn new_art_epoch(collector: &Collector) -> Epoch {
    Epoch::new(collector, art_dealloc)
}
