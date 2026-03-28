//! Fuzz target for Tlrc / TlrcRef lifecycle.
//!
//! Models a tokio-like runtime: persistent worker threads that hold multiple
//! refs, with work-stealing (refs migrate between workers) creating unbalanced
//! per-thread counts. Exercises the full Tlrc lifecycle: create, clone,
//! cross-thread send, drop ordering, orphan/stale counters, hot-path cache,
//! SCAN_EAGERLY_SENTINEL, and concurrent scan triggers.
//!
//! Run: cd crates/alloc/fuzz && cargo +nightly fuzz run fuzz_tlrc

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bisque_alloc::{Tlrc, TlrcRef};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

/// Maximum number of live TlrcRef handles on the main thread.
const MAX_REFS: usize = 32;
/// Maximum number of live TlrcRef handles per worker.
const MAX_WORKER_REFS: usize = 16;
/// Maximum number of operations per fuzz run.
const MAX_OPS: usize = 256;
/// Number of persistent worker threads (models tokio worker pool).
const NUM_WORKERS: usize = 4;

// =========================================================================
// Ops
// =========================================================================

#[derive(Debug, Arbitrary)]
enum Op {
    /// Create a TlrcRef from the master, or clone from existing refs.
    CreateRef,
    /// Clone an existing local TlrcRef.
    CloneRef { idx: u8 },
    /// Drop a local TlrcRef.
    DropRef { idx: u8 },
    /// Send a local TlrcRef to a worker (models task spawn / work-stealing).
    /// The worker holds the ref — it is NOT immediately dropped.
    SendToWorker { idx: u8, worker: u8 },
    /// Tell a worker to drop one of its refs (models task completion).
    WorkerDrop { worker: u8, idx: u8 },
    /// Tell a worker to do clone/drop loops on one of its refs
    /// (models a task polling repeatedly on the same worker).
    WorkerCloneLoop { worker: u8, idx: u8, n: u8 },
    /// Tell a worker to clone a ref and send the clone to another worker
    /// (models work-stealing: increment on worker A, decrement on worker B).
    WorkerSteal { src: u8, idx: u8, dst: u8 },
    /// Drop the master early (only once). All refs must still work.
    DropMaster,
    /// Deref all local refs and verify canary intact.
    DerefAll,
    /// Drop all local refs, then recreate some from master.
    DropAllThenRecreate { count: u8 },
}

// =========================================================================
// Canary payload
// =========================================================================

/// Multi-word canary for UAF detection. Poisoned on drop so any post-free
/// access is detectable even without ASan.
const CANARY: [u64; 4] = [
    0xDEAD_BEEF_CAFE_BABE,
    0x0123_4567_89AB_CDEF,
    0xFEDC_BA98_7654_3210,
    0xA5A5_5A5A_C3C3_3C3C,
];

struct Payload {
    canary: [u64; 4],
    drop_count: Arc<AtomicUsize>,
}

impl Payload {
    fn assert_alive(&self) {
        assert_eq!(self.canary, CANARY, "canary corrupted — possible UAF");
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.assert_alive();
        self.canary = [0; 4];
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

// =========================================================================
// Worker thread commands
// =========================================================================

enum Cmd {
    /// Hold a TlrcRef (models receiving a task / work-steal).
    Hold(TlrcRef<Payload>),
    /// Drop the ref at the given index.
    Drop(u8),
    /// Clone ref at index N times, then drop all clones.
    CloneLoop(u8, u8),
    /// Clone ref at index, send clone as Hold to the destination worker.
    StealTo(u8, mpsc::Sender<Cmd>),
    /// Shut down: drop all refs and exit.
    Shutdown,
}

/// Runs a worker loop: receives commands, manages a local vec of refs.
/// Models a tokio worker thread that persists across many tasks.
fn worker_loop(rx: mpsc::Receiver<Cmd>, live_count: Arc<AtomicUsize>) {
    let mut local: Vec<TlrcRef<Payload>> = Vec::new();
    while let Ok(cmd) = rx.recv() {
        match cmd {
            Cmd::Hold(r) => {
                r.assert_alive();
                if local.len() < MAX_WORKER_REFS {
                    local.push(r);
                    live_count.fetch_add(1, Ordering::SeqCst);
                }
                // else: ref is dropped here, live_count not incremented
            }
            Cmd::Drop(idx) => {
                if !local.is_empty() {
                    let i = idx as usize % local.len();
                    let r = local.swap_remove(i);
                    r.assert_alive();
                    live_count.fetch_sub(1, Ordering::SeqCst);
                    drop(r);
                }
            }
            Cmd::CloneLoop(idx, n) => {
                if !local.is_empty() {
                    let i = idx as usize % local.len();
                    let iterations = (n as usize) % 64;
                    for _ in 0..iterations {
                        let c = local[i].clone();
                        c.assert_alive();
                        drop(c);
                    }
                }
            }
            Cmd::StealTo(idx, dest_tx) => {
                if !local.is_empty() {
                    let i = idx as usize % local.len();
                    let cloned = local[i].clone();
                    // Send clone to destination worker as a Hold command.
                    // If the channel is closed (destination shut down),
                    // the ref is dropped here — decrement on this thread.
                    let _ = dest_tx.send(Cmd::Hold(cloned));
                }
            }
            Cmd::Shutdown => break,
        }
    }
    // Drop all remaining refs — models worker shutdown / runtime drop.
    let n = local.len();
    drop(local);
    live_count.fetch_sub(n, Ordering::SeqCst);
}

// =========================================================================
// Fuzz target
// =========================================================================

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

    let master_drops_first: bool = u.arbitrary().unwrap_or(false);
    let drop_count = Arc::new(AtomicUsize::new(0));

    let mut master: Option<Tlrc<Payload>> = Some(Tlrc::new(Payload {
        canary: CANARY,
        drop_count: drop_count.clone(),
    }));
    let mut refs: Vec<TlrcRef<Payload>> = Vec::new();

    // Persistent worker pool — models tokio runtime worker threads.
    let worker_live = Arc::new(AtomicUsize::new(0));
    let mut worker_tx: Vec<mpsc::Sender<Cmd>> = Vec::with_capacity(NUM_WORKERS);
    let mut worker_handles = Vec::with_capacity(NUM_WORKERS);
    for _ in 0..NUM_WORKERS {
        let (tx, rx) = mpsc::channel();
        let live = worker_live.clone();
        worker_handles.push(std::thread::spawn(move || worker_loop(rx, live)));
        worker_tx.push(tx);
    }

    for op in &ops {
        match op {
            Op::CreateRef => {
                if refs.len() < MAX_REFS {
                    if let Some(m) = &master {
                        refs.push(m.tlrc_ref());
                    } else if let Some(r) = refs.first() {
                        refs.push(r.clone());
                    }
                }
            }
            Op::CloneRef { idx } => {
                if !refs.is_empty() && refs.len() < MAX_REFS {
                    let i = *idx as usize % refs.len();
                    let cloned = refs[i].clone();
                    refs.push(cloned);
                }
            }
            Op::DropRef { idx } => {
                if !refs.is_empty() {
                    let i = *idx as usize % refs.len();
                    refs.swap_remove(i);
                }
            }
            Op::SendToWorker { idx, worker } => {
                if !refs.is_empty() {
                    let i = *idx as usize % refs.len();
                    let w = *worker as usize % NUM_WORKERS;
                    let r = refs.swap_remove(i);
                    // Send to worker. If send fails (shouldn't — workers alive),
                    // the ref is dropped on the main thread.
                    let _ = worker_tx[w].send(Cmd::Hold(r));
                }
            }
            Op::WorkerDrop { worker, idx } => {
                let w = *worker as usize % NUM_WORKERS;
                let _ = worker_tx[w].send(Cmd::Drop(*idx));
            }
            Op::WorkerCloneLoop { worker, idx, n } => {
                let w = *worker as usize % NUM_WORKERS;
                let _ = worker_tx[w].send(Cmd::CloneLoop(*idx, *n));
            }
            Op::WorkerSteal { src, idx, dst } => {
                let s = *src as usize % NUM_WORKERS;
                let d = *dst as usize % NUM_WORKERS;
                if s != d {
                    let _ = worker_tx[s].send(Cmd::StealTo(*idx, worker_tx[d].clone()));
                }
            }
            Op::DropMaster => {
                master.take();
            }
            Op::DerefAll => {
                if let Some(m) = &master {
                    (**m).assert_alive();
                }
                for r in &refs {
                    r.assert_alive();
                }
            }
            Op::DropAllThenRecreate { count } => {
                refs.clear();
                let n = (*count as usize).min(MAX_REFS);
                if let Some(m) = &master {
                    for _ in 0..n {
                        refs.push(m.tlrc_ref());
                    }
                }
            }
        }

        // Invariant: payload must not have been dropped while any handle
        // is alive — master, local refs, or worker-held refs.
        if master.is_some() || !refs.is_empty() || worker_live.load(Ordering::SeqCst) > 0 {
            assert_eq!(drop_count.load(Ordering::SeqCst), 0, "premature drop");
        }
    }

    // Shut down workers. Each worker drops all its accumulated refs.
    for tx in &worker_tx {
        let _ = tx.send(Cmd::Shutdown);
    }
    // Drop sender handles so workers see channel disconnect if they
    // miss the Shutdown command.
    drop(worker_tx);
    for h in worker_handles {
        let _ = h.join();
    }

    // Fuzzer-controlled drop order.
    if master_drops_first {
        drop(master);
        drop(refs);
    } else {
        drop(refs);
        drop(master);
    }

    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        1,
        "payload must be dropped exactly once"
    );
});
