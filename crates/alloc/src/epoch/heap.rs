//! Heap-epoch reclamation built on the global seize/Hyaline collector.
//!
//! Uses the global seize collector for correctness (Hyaline protocol).
//! `Collector` holds a `Heap` clone to guarantee the heap outlives all
//! retired entries — seize's reclaim callbacks use `heap_data` raw pointers.

use crate::Heap;
use crate::heap::HeapData;

// ═══════════════════════════════════════════════════════════════════════════

/// Deallocation function for epoch-retired pointers.
pub type DeallocFn = unsafe fn(*const HeapData, usize);

// ═══════════════════════════════════════════════════════════════════════════

/// Batch of retired pointers sharing a single Heap clone.
/// Keeps HeapData alive until seize reclaims this batch.
struct RetireBatch {
    heap: Heap,
    dealloc: DeallocFn,
    ptrs: Box<[usize]>,
}

unsafe impl Send for RetireBatch {}

unsafe fn reclaim_batch(
    batch_ptr: *mut RetireBatch,
    _: &crate::epoch::collector::Collector,
) {
    let batch = unsafe { Box::from_raw(batch_ptr) };
    let heap_data = batch.heap.data() as *const HeapData;
    for &ptr in batch.ptrs.iter() {
        unsafe { (batch.dealloc)(heap_data, ptr) };
    }
    // batch.heap drops here — Heap refcount decremented
}

// ═══════════════════════════════════════════════════════════════════════════
// Collector — holds a Heap clone to keep HeapData alive
// ═══════════════════════════════════════════════════════════════════════════

pub struct Collector {
    heap: Heap,
}

impl Collector {
    pub fn new(heap: &Heap) -> Self {
        // Touch the global seize collector to ensure it's initialized.
        let _ = crate::epoch::collector();
        Self {
            heap: heap.clone(),
        }
    }

    #[inline]
    pub fn id(&self) -> usize {
        0
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }
}

impl std::fmt::Debug for Collector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Collector").finish()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Epoch
// ═══════════════════════════════════════════════════════════════════════════

pub struct Epoch {
    dealloc: DeallocFn,
    heap: Heap,
}

pub struct EpochGuard<'a> {
    _guard: crate::epoch::LocalGuard<'a>,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Epoch {
    pub fn new(collector: &Collector, dealloc: DeallocFn) -> Self {
        Self {
            dealloc,
            heap: collector.heap.clone(),
        }
    }

    #[inline]
    pub fn pin(&self) -> EpochGuard<'_> {
        let guard = crate::epoch::collector().enter();
        EpochGuard {
            _guard: guard,
            _not_send: std::marker::PhantomData,
        }
    }

    pub fn advance(&self) {
        use crate::epoch::Guard;
        let guard = crate::epoch::collector().enter();
        guard.flush();
    }

    pub fn seal(&self, ptrs: crate::Vec<usize>) {
        self.seal_slice(ptrs.as_slice());
    }

    pub fn seal_slice(&self, ptrs: &[usize]) {
        if ptrs.is_empty() {
            return;
        }
        let batch = Box::into_raw(Box::new(RetireBatch {
            heap: self.heap.clone(),
            dealloc: self.dealloc,
            ptrs: ptrs.into(),
        }));
        unsafe { crate::epoch::collector().retire(batch, reclaim_batch) };
    }

    pub fn retire(&self, ptrs: crate::Vec<usize>) {
        self.seal(ptrs);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// EpochGuard
// ═══════════════════════════════════════════════════════════════════════════

impl<'a> EpochGuard<'a> {
    #[inline]
    pub fn epoch(&self) -> u64 {
        1
    }
}
