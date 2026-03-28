//! Fuzz target for HeapMaster / Heap lifecycle and allocation.
//!
//! Exercises: alloc/dealloc, realloc, zeroed alloc, clone handles,
//! cross-thread alloc and free on a SHARED heap, memory pressure,
//! heap churn, and drop ordering.
//!
//! The heap is shared across the main thread and spawned worker threads,
//! exercising mimalloc 3.x first-class heap multi-thread safety.
//!
//! Run: cd crates/alloc/fuzz && cargo +nightly fuzz run fuzz_heap

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bisque_alloc::{Heap, HeapMaster};
use libfuzzer_sys::fuzz_target;

/// Maximum number of live allocations.
const MAX_ALLOCS: usize = 64;
/// Maximum number of operations per fuzz run.
const MAX_OPS: usize = 256;
/// Maximum single allocation size (keep small to avoid OOM).
const MAX_ALLOC_SIZE: usize = 4096;
/// Heap memory limit — small enough to exercise pressure paths.
const HEAP_LIMIT: usize = 2 * 1024 * 1024; // 2 MiB
/// Maximum spawned threads.
const MAX_THREADS: usize = 4;

#[derive(Debug, Arbitrary)]
enum Op {
    /// Allocate `size` bytes with default alignment.
    Alloc { size: u16 },
    /// Allocate zeroed bytes and verify they are zero.
    AllocZeroed { size: u16 },
    /// Free a random allocation.
    Free { idx: u8 },
    /// Realloc a random allocation to a new size.
    Realloc { idx: u8, new_size: u16 },
    /// Clone the Heap handle (exercises TlrcRef clone).
    CloneHeap,
    /// Drop a cloned Heap handle.
    DropClonedHeap { idx: u8 },
    /// Free an allocation on a different thread (cross-thread dealloc).
    CrossThreadFree { idx: u8 },
    /// Allocate on a different thread, return pointer to main thread.
    CrossThreadAlloc { size: u16 },
    /// Spawn a worker that does N alloc/free cycles on the shared heap.
    CrossThreadChurn { count: u8 },
    /// Spawn a worker that allocates N blocks, keeps them alive, then exits.
    /// The worker's theap is deleted — pages must transfer to a sibling theap.
    /// Main thread later frees the blocks (cross-theap free after transfer).
    WorkerAllocAndExit { count: u8 },
    /// Write a pattern to a random allocation and verify it.
    WriteAndVerify { idx: u8, val: u8 },
    /// Free all allocations at once.
    FreeAll,
    /// Query memory stats (capacity, usage).
    QueryStats,
    /// Collect garbage (triggers mimalloc page purging).
    Collect,
}

/// A raw pointer wrapper that is Send (mimalloc pointers can be freed from any thread).
#[derive(Clone, Copy)]
struct SendPtr(*mut u8);
// SAFETY: mimalloc allocations can be freed from any thread.
unsafe impl Send for SendPtr {}

/// Tracked allocation with its size and fill byte for verification.
struct Alloc {
    ptr: SendPtr,
    size: usize,
    fill: u8,
}

impl Alloc {
    fn raw(&self) -> *mut u8 {
        self.ptr.0
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);

    let op_count: usize = match u.int_in_range(1..=MAX_OPS) {
        Ok(n) => n,
        Err(_) => return,
    };
    let mut ops = Vec::with_capacity(op_count);
    for _ in 0..op_count {
        match u.arbitrary() {
            Ok(op) => ops.push(op),
            Err(_) => break,
        }
    }
    if ops.is_empty() {
        return;
    }

    let master = match HeapMaster::new(HEAP_LIMIT) {
        Ok(m) => m,
        Err(_) => return,
    };
    let heap = master.heap();

    let mut allocs: Vec<Alloc> = Vec::new();
    let mut cloned_heaps: Vec<Heap> = Vec::new();
    let mut thread_handles: Vec<std::thread::JoinHandle<()>> = Vec::new();
    // Channel for receiving allocations made on worker threads.
    let (alloc_tx, alloc_rx) = std::sync::mpsc::channel::<Alloc>();

    for op in &ops {
        // Drain any allocations returned from worker threads.
        while let Ok(a) = alloc_rx.try_recv() {
            if allocs.len() < MAX_ALLOCS {
                allocs.push(a);
            } else {
                unsafe { heap.dealloc(a.raw()) };
            }
        }

        match op {
            Op::Alloc { size } => {
                if allocs.len() < MAX_ALLOCS {
                    let sz = (*size as usize).min(MAX_ALLOC_SIZE);
                    if sz == 0 {
                        continue;
                    }
                    let ptr = heap.alloc(sz, 8);
                    if !ptr.is_null() {
                        let fill = (sz & 0xFF) as u8;
                        unsafe { std::ptr::write_bytes(ptr, fill, sz) };
                        allocs.push(Alloc {
                            ptr: SendPtr(ptr),
                            size: sz,
                            fill,
                        });
                    }
                }
            }
            Op::AllocZeroed { size } => {
                if allocs.len() < MAX_ALLOCS {
                    let sz = (*size as usize).min(MAX_ALLOC_SIZE);
                    if sz == 0 {
                        continue;
                    }
                    let ptr = heap.alloc_zeroed(sz, 8);
                    if !ptr.is_null() {
                        let slice = unsafe { std::slice::from_raw_parts(ptr, sz) };
                        assert!(slice.iter().all(|&b| b == 0), "alloc_zeroed not zeroed");
                        allocs.push(Alloc {
                            ptr: SendPtr(ptr),
                            size: sz,
                            fill: 0,
                        });
                    }
                }
            }
            Op::Free { idx } => {
                if !allocs.is_empty() {
                    let i = *idx as usize % allocs.len();
                    let a = allocs.swap_remove(i);
                    unsafe { heap.dealloc(a.raw()) };
                }
            }
            Op::Realloc { idx, new_size } => {
                if !allocs.is_empty() {
                    let i = *idx as usize % allocs.len();
                    let new_sz = (*new_size as usize).min(MAX_ALLOC_SIZE);
                    if new_sz == 0 {
                        continue;
                    }
                    let a = &mut allocs[i];
                    let new_ptr = unsafe { heap.realloc(a.raw(), new_sz, 8) };
                    if !new_ptr.is_null() {
                        // Verify preserved prefix.
                        let preserved = a.size.min(new_sz);
                        let slice = unsafe { std::slice::from_raw_parts(new_ptr, preserved) };
                        assert!(slice.iter().all(|&b| b == a.fill), "realloc corrupted data");
                        let fill = (new_sz & 0xFF) as u8;
                        unsafe { std::ptr::write_bytes(new_ptr, fill, new_sz) };
                        a.ptr = SendPtr(new_ptr);
                        a.size = new_sz;
                        a.fill = fill;
                    }
                }
            }
            Op::CloneHeap => {
                if cloned_heaps.len() < 8 {
                    cloned_heaps.push(heap.clone());
                }
            }
            Op::DropClonedHeap { idx } => {
                if !cloned_heaps.is_empty() {
                    let i = *idx as usize % cloned_heaps.len();
                    cloned_heaps.swap_remove(i);
                }
            }
            Op::CrossThreadFree { idx } => {
                if !allocs.is_empty() && thread_handles.len() < MAX_THREADS {
                    let i = *idx as usize % allocs.len();
                    let a = allocs.swap_remove(i);
                    let h = heap.clone();
                    thread_handles.push(std::thread::spawn(move || {
                        // Verify data integrity before freeing.
                        let slice = unsafe { std::slice::from_raw_parts(a.raw(), a.size) };
                        assert!(
                            slice.iter().all(|&b| b == a.fill),
                            "cross-thread data corruption before free"
                        );
                        unsafe { h.dealloc(a.raw()) };
                    }));
                }
            }
            Op::CrossThreadAlloc { size } => {
                if thread_handles.len() < MAX_THREADS {
                    let sz = (*size as usize).min(MAX_ALLOC_SIZE);
                    if sz == 0 {
                        continue;
                    }
                    let h = heap.clone();
                    let tx = alloc_tx.clone();
                    thread_handles.push(std::thread::spawn(move || {
                        let ptr = h.alloc(sz, 8);
                        if !ptr.is_null() {
                            let fill = (sz & 0xFF) as u8;
                            unsafe { std::ptr::write_bytes(ptr, fill, sz) };
                            let _ = tx.send(Alloc {
                                ptr: SendPtr(ptr),
                                size: sz,
                                fill,
                            });
                        }
                    }));
                }
            }
            Op::CrossThreadChurn { count } => {
                if thread_handles.len() < MAX_THREADS {
                    let n = (*count as usize).min(64);
                    let h = heap.clone();
                    thread_handles.push(std::thread::spawn(move || {
                        let mut ptrs = Vec::new();
                        for i in 0..n {
                            let sz = 64 + (i % 16) * 64;
                            let p = h.alloc(sz, 8);
                            if !p.is_null() {
                                unsafe { std::ptr::write_bytes(p, 0xCC, sz) };
                                ptrs.push(p);
                            }
                        }
                        for p in ptrs {
                            unsafe { h.dealloc(p) };
                        }
                    }));
                }
            }
            Op::WorkerAllocAndExit { count } => {
                if thread_handles.len() < MAX_THREADS {
                    let n = (*count as usize).min(32);
                    let h = heap.clone();
                    let tx = alloc_tx.clone();
                    // Worker allocates blocks, sends pointers back, then EXITS.
                    // Its theap is deleted — pages transfer to a sibling theap.
                    thread_handles.push(std::thread::spawn(move || {
                        for _ in 0..n {
                            let sz = 256;
                            let p = h.alloc(sz, 8);
                            if !p.is_null() {
                                let fill = 0xDE_u8;
                                unsafe { std::ptr::write_bytes(p, fill, sz) };
                                let _ = tx.send(Alloc {
                                    ptr: SendPtr(p),
                                    size: sz,
                                    fill,
                                });
                            }
                        }
                        // Thread exits here — theap deleted, pages transferred.
                    }));
                }
            }
            Op::WriteAndVerify { idx, val } => {
                if !allocs.is_empty() {
                    let i = *idx as usize % allocs.len();
                    let a = &mut allocs[i];
                    unsafe { std::ptr::write_bytes(a.raw(), *val, a.size) };
                    a.fill = *val;
                    let slice = unsafe { std::slice::from_raw_parts(a.raw(), a.size) };
                    assert!(slice.iter().all(|&b| b == *val), "write/verify mismatch");
                }
            }
            Op::FreeAll => {
                for a in allocs.drain(..) {
                    unsafe { heap.dealloc(a.raw()) };
                }
            }
            Op::QueryStats => {
                let cap = heap.capacity();
                let usage = heap.memory_usage();
                assert!(usage <= cap || cap == 0, "usage {usage} > capacity {cap}");
            }
            Op::Collect => {
                heap.collect(true);
            }
        }
    }

    // Join all threads.
    for h in thread_handles {
        let _ = h.join();
    }

    // Drain any remaining cross-thread allocations.
    drop(alloc_tx);
    while let Ok(a) = alloc_rx.try_recv() {
        allocs.push(a);
    }

    // Free remaining allocations.
    for a in allocs.drain(..) {
        unsafe { heap.dealloc(a.raw()) };
    }

    // Drop cloned heaps before master.
    drop(cloned_heaps);
    drop(heap);
    drop(master);
});
