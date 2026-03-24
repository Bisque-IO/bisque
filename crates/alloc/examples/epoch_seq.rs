//! "Never transition" design: per-thread counters for the ENTIRE lifecycle.
//! No refcount, no closed flag, no epoch, no transition to Arc-mode.
//!
//! Thread hot path: NO CAS, NO loop, NO fence. Just load+store on count
//! with seq=FROZEN as busy signal.
//!
//! Destruction: when master_dropped && sum of all counters == 0 → destroy.
//! The scan uses scanner-side CAS on seq for idle counters (mutual exclusion
//! with thread's bracket entry) to get a consistent per-counter read.
//!
//! After master_dropped: each decrement that brings a counter to an
//! "interesting" state (≤0) triggers a full scan. The scan CAS's each idle
//! counter's seq → reads count → restores seq. Active counters (seq=FROZEN):
//! spin until idle, then CAS.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering::*};
use std::sync::{Arc, Barrier};
use std::thread;

const FROZEN: u32 = u32::MAX;
const LOCKED: u32 = u32::MAX - 1; // scanner's CAS lock on idle counter

struct Counter {
    count: AtomicI64,
    seq: AtomicU32, // thread writes (FROZEN=busy, normal=idle). Scanner CAS's to LOCKED.
}

// =========================================================================
// Thread hot path: NO CAS, NO loop
// =========================================================================

#[inline(always)]
fn bracket(c: &Counter, delta: i64, master_dropped: &AtomicBool) -> bool {
    // Enter bracket.
    let s = c.seq.load(Relaxed); // store-forwarded: own value

    // If scanner locked our counter (LOCKED): it's reading our count.
    // Spin until scanner restores seq. (Very rare — only during Tlrc::drop scan.)
    // This is NOT on the hot path when master is alive.
    if s == LOCKED {
        while c.seq.load(Acquire) == LOCKED {
            std::hint::spin_loop();
        }
        // Retry with new seq value.
        return bracket(c, delta, master_dropped);
    }

    c.seq.store(FROZEN, Release); // busy

    // Modify count.
    let v = c.count.load(Relaxed);
    c.count.store(v + delta, Release);

    // Exit bracket.
    let next = if s + 1 >= LOCKED { 0 } else { s + 1 };
    c.seq.store(next, Release);

    // Check if we should trigger a scan.
    // Only after master_dropped AND our count is "interesting" (≤ 0).
    if master_dropped.load(Relaxed) && v + delta <= 0 {
        return true; // caller should scan
    }
    false
}

// =========================================================================
// Scanner: CAS each counter's seq to LOCKED, read count, restore seq.
// Returns the sum of all counts.
// =========================================================================

fn scan_all(counters: &[Arc<Counter>]) -> i64 {
    let mut sum: i64 = 0;
    for counter in counters {
        loop {
            let s = counter.seq.load(Acquire);
            if s == FROZEN {
                // Thread busy. Spin until idle.
                std::hint::spin_loop();
                continue;
            }
            if s == LOCKED {
                // Another scanner? Shouldn't happen. Spin.
                std::hint::spin_loop();
                continue;
            }
            // Idle. CAS to LOCKED.
            if counter
                .seq
                .compare_exchange(s, LOCKED, AcqRel, Relaxed)
                .is_ok()
            {
                // Locked. Thread's bracket entry will see LOCKED and spin.
                // Count is stable. Read it.
                let count = counter.count.load(Acquire);
                sum += count;
                // Restore seq so thread can proceed.
                counter.seq.store(s, Release);
                break;
            }
            // CAS failed: thread entered bracket between our read and CAS.
            std::hint::spin_loop();
        }
    }
    sum
}

// =========================================================================
// Try destroy: scan all counters. If sum == 0: destroy.
// Uses a destroying flag to prevent multiple threads from destroying.
// =========================================================================

fn try_destroy(counters: &[Arc<Counter>], destroying: &AtomicBool) -> bool {
    // Fast check: is someone already destroying?
    if destroying.load(Relaxed) {
        return false;
    }

    let sum = scan_all(counters);
    if sum != 0 {
        return false;
    }

    // Sum == 0. Try to claim destruction.
    // Re-scan under the destroying flag to prevent races.
    if destroying
        .compare_exchange(false, true, AcqRel, Relaxed)
        .is_err()
    {
        return false; // another thread claimed it
    }

    // Re-scan to confirm (a clone might have happened between first scan and CAS).
    let sum2 = scan_all(counters);
    if sum2 != 0 {
        destroying.store(false, Release); // release claim
        return false;
    }

    // Confirmed: sum == 0 twice with destroying=true.
    // No thread can clone while we hold destroying=true... wait, threads
    // don't check destroying. They can still clone.
    //
    // But: sum==0 means no live TlrcRefs exist. Nobody CAN clone (you need
    // an existing TlrcRef to clone). So sum stays 0. Safe to destroy.
    true
}

// =========================================================================
// Test harness
// =========================================================================

fn main() {
    const NUM_THREADS: usize = 16;
    const OPS_PER_THREAD: usize = 10_000;
    const ITERATIONS: usize = 100_000;

    for iter in 0..ITERATIONS {
        if iter % 10000 == 0 {
            eprintln!("iter {iter}");
        }

        let master_dropped = Arc::new(AtomicBool::new(false));
        let destroying = Arc::new(AtomicBool::new(false));
        let destroyed = Arc::new(AtomicBool::new(false));
        let drop_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Master counter: NUM_THREADS tlrc_ref() calls.
        let master = Arc::new(Counter {
            count: AtomicI64::new(NUM_THREADS as i64),
            seq: AtomicU32::new(0),
        });

        // Spawned counters.
        let spawned: Vec<_> = (0..NUM_THREADS)
            .map(|_| {
                Arc::new(Counter {
                    count: AtomicI64::new(0),
                    seq: AtomicU32::new(0),
                })
            })
            .collect();

        // All counters (master + spawned) for scanning.
        let all_counters: Arc<Vec<Arc<Counter>>> = Arc::new(
            std::iter::once(master.clone())
                .chain(spawned.iter().cloned())
                .collect(),
        );

        let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
        let mut handles = vec![];

        for i in 0..NUM_THREADS {
            let c = spawned[i].clone();
            let b = barrier.clone();
            let md = master_dropped.clone();
            let dy = destroying.clone();
            let dd = destroyed.clone();
            let dc = drop_count.clone();
            let ac = all_counters.clone();
            let is_dormant = i < NUM_THREADS / 4; // 2 dormant, 6 active

            handles.push(thread::spawn(move || {
                b.wait();

                if is_dormant {
                    // DORMANT: drop the moved-in ref (debt: count→-1).
                    // Then do nothing. Thread goes idle.
                    let should_scan = bracket(&c, -1, &md);
                    if should_scan && !dd.load(Relaxed) {
                        if try_destroy(&ac, &dy) {
                            dd.store(true, Release);
                            dc.fetch_add(1, SeqCst);
                        }
                    }
                    // Dormant: no more ops. Count=-1. Seq=idle.
                } else {
                    // ACTIVE: clone r (+1), tight loop, drop cloned r (-1),
                    // drop original r (-1 = debt).
                    bracket(&c, 1, &md); // r.clone()

                    for _ in 0..OPS_PER_THREAD {
                        bracket(&c, 1, &md); // clone
                        bracket(&c, -1, &md); // drop
                    }

                    let should_scan = bracket(&c, -1, &md); // drop cloned r
                    if should_scan && !dd.load(Relaxed) {
                        if try_destroy(&ac, &dy) {
                            dd.store(true, Release);
                            dc.fetch_add(1, SeqCst);
                        }
                    }

                    let should_scan = bracket(&c, -1, &md); // drop orig r (debt)
                    if should_scan && !dd.load(Relaxed) {
                        if try_destroy(&ac, &dy) {
                            dd.store(true, Release);
                            dc.fetch_add(1, SeqCst);
                        }
                    }
                }
            }));
        }

        barrier.wait();

        // Master drops: set flag, then scan.
        master_dropped.store(true, Release);

        // The master's count stays at NUM_THREADS. It was never decremented
        // because the "tlrc_ref" calls were on the master counter.
        // In the real Tlrc: the master counter holds +N.
        // After master_dropped: threads drop their refs → spawned counts go to -1.
        // Total: master(+N) + spawned(N * -1) = 0 when all refs are gone.

        // Master could also trigger a scan, but in this simulation the
        // threads trigger scans on interesting decrements.

        for h in handles {
            h.join().unwrap();
        }

        // After all threads joined: all refs are dropped. Sum should be 0.
        let final_sum = scan_all(&all_counters);
        let was_destroyed = destroyed.load(SeqCst);
        let dc_val = drop_count.load(SeqCst);

        if final_sum != 0 {
            eprintln!("FAIL iter {iter}: final_sum={final_sum} (should be 0)");
            eprintln!("  master: count={}", master.count.load(Relaxed));
            for (i, c) in spawned.iter().enumerate() {
                eprintln!("  spawned[{i}]: count={}", c.count.load(Relaxed));
            }
            std::process::exit(1);
        }

        if !was_destroyed {
            // Threads didn't trigger destruction. Master does it.
            // (In real Tlrc: Tlrc::drop would scan after joining.)
            if try_destroy(&all_counters, &destroying) {
                drop_count.fetch_add(1, SeqCst);
            }
        }

        let final_dc = drop_count.load(SeqCst);
        if final_dc != 1 {
            eprintln!("FAIL iter {iter}: drop_count={final_dc} (should be 1)");
            std::process::exit(1);
        }
    }
    eprintln!("ALL {ITERATIONS} iterations passed");
}
