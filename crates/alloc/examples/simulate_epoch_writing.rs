//! Simulate the full WriteGuard → epoch → reclaim pipeline.
//!
//! Compares:
//!   1. **Mutex+VecDeque** (current heap epoch design)
//!   2. **crossfire SPSC** (bounded lock-free channel)
//!
//! Run: cargo run --release -p bisque-alloc --example simulate-epoch-writing

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bisque_alloc::MiMalloc;
use parking_lot::Mutex;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const BATCH_SIZE: usize = 1000;
const OPS_PER_THREAD: u64 = 20_000;
const READER_OPS: u64 = 500_000;
const UNPINNED: u64 = u64::MAX;

struct GarbageBag {
    epoch: u64,
    count: usize,
}

struct Guarded<T>(UnsafeCell<T>);
unsafe impl<T> Sync for Guarded<T> {}
impl<T> Guarded<T> {
    fn new(v: T) -> Self {
        Self(UnsafeCell::new(v))
    }
    unsafe fn get(&self) -> &mut T {
        unsafe { &mut *self.0.get() }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Model 1: Mutex + VecDeque (current)
// ═══════════════════════════════════════════════════════════════════════════

struct MutexModel {
    epoch: AtomicU64,
    needs_reclaim: AtomicBool,
    garbage: Vec<Mutex<VecDeque<GarbageBag>>>,
    pin_slots: Vec<AtomicU64>,
}

impl MutexModel {
    fn new(n: usize) -> Self {
        Self {
            epoch: AtomicU64::new(1),
            needs_reclaim: AtomicBool::new(false),
            garbage: (0..n).map(|_| Mutex::new(VecDeque::new())).collect(),
            pin_slots: (0..n).map(|_| AtomicU64::new(UNPINNED)).collect(),
        }
    }
    #[inline]
    fn pin(&self, tid: usize) -> u64 {
        let e = self.epoch.load(Ordering::Acquire);
        self.pin_slots[tid].store(
            e.min(self.pin_slots[tid].load(Ordering::Relaxed)),
            Ordering::Release,
        );
        e
    }
    #[inline]
    fn unpin(&self, tid: usize) {
        self.pin_slots[tid].store(UNPINNED, Ordering::Release);
    }
    fn retire(&self, tid: usize, bag: GarbageBag) {
        self.garbage[tid].lock().push_back(bag);
        if !self.needs_reclaim.swap(true, Ordering::AcqRel) {
            self.epoch.fetch_add(1, Ordering::Release);
        }
    }
    fn try_reclaim(&self, n: usize) {
        if !self.needs_reclaim.load(Ordering::Relaxed) {
            return;
        }
        if self
            .needs_reclaim
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let mut wm = UNPINNED;
        for i in 0..n {
            let v = self.pin_slots[i].load(Ordering::Acquire);
            if v < wm {
                wm = v;
            }
        }
        let safe = if wm == UNPINNED { u64::MAX } else { wm };
        let mut rem = false;
        for i in 0..n {
            if let Some(mut g) = self.garbage[i].try_lock() {
                while let Some(b) = g.front() {
                    if b.epoch < safe {
                        black_box(g.pop_front().unwrap().count);
                    } else {
                        break;
                    }
                }
                if !g.is_empty() {
                    rem = true;
                }
            } else {
                rem = true;
            }
        }
        if rem {
            self.needs_reclaim.store(true, Ordering::Release);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Model 2: crossfire SPSC bounded
// ═══════════════════════════════════════════════════════════════════════════

type CfTx = crossfire::Tx<crossfire::spsc::Array<GarbageBag>>;
type CfRx = crossfire::Rx<crossfire::spsc::Array<GarbageBag>>;

struct CrossfireModel {
    epoch: AtomicU64,
    needs_reclaim: AtomicBool,
    senders: Vec<Guarded<CfTx>>,
    receivers: Vec<Guarded<CfRx>>,
    holdback: Vec<Guarded<VecDeque<GarbageBag>>>,
    pin_slots: Vec<AtomicU64>,
}

impl CrossfireModel {
    fn new(n: usize) -> Self {
        let mut senders = Vec::with_capacity(n);
        let mut receivers = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = crossfire::spsc::bounded_blocking(256);
            senders.push(Guarded::new(tx));
            receivers.push(Guarded::new(rx));
        }
        Self {
            epoch: AtomicU64::new(1),
            needs_reclaim: AtomicBool::new(false),
            senders,
            receivers,
            holdback: (0..n).map(|_| Guarded::new(VecDeque::new())).collect(),
            pin_slots: (0..n).map(|_| AtomicU64::new(UNPINNED)).collect(),
        }
    }
    #[inline]
    fn pin(&self, tid: usize) -> u64 {
        let e = self.epoch.load(Ordering::Acquire);
        self.pin_slots[tid].store(
            e.min(self.pin_slots[tid].load(Ordering::Relaxed)),
            Ordering::Release,
        );
        e
    }
    #[inline]
    fn unpin(&self, tid: usize) {
        self.pin_slots[tid].store(UNPINNED, Ordering::Release);
    }
    fn retire(&self, tid: usize, bag: GarbageBag) {
        let tx = unsafe { self.senders[tid].get() };
        let _ = tx.try_send(bag);
        if !self.needs_reclaim.swap(true, Ordering::AcqRel) {
            self.epoch.fetch_add(1, Ordering::Release);
        }
    }
    fn try_reclaim(&self, n: usize) {
        if !self.needs_reclaim.load(Ordering::Relaxed) {
            return;
        }
        if self
            .needs_reclaim
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let mut wm = UNPINNED;
        for i in 0..n {
            let v = self.pin_slots[i].load(Ordering::Acquire);
            if v < wm {
                wm = v;
            }
        }
        let safe = if wm == UNPINNED { u64::MAX } else { wm };
        let mut rem = false;
        for i in 0..n {
            let hb = unsafe { self.holdback[i].get() };
            let rx = unsafe { self.receivers[i].get() };
            while let Some(b) = hb.front() {
                if b.epoch < safe {
                    black_box(hb.pop_front().unwrap().count);
                } else {
                    break;
                }
            }
            while let Ok(bag) = rx.try_recv() {
                if bag.epoch < safe {
                    black_box(bag.count);
                } else {
                    hb.push_back(bag);
                    rem = true;
                    break;
                }
            }
            if !hb.is_empty() {
                rem = true;
            }
        }
        if rem {
            self.needs_reclaim.store(true, Ordering::Release);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Harness
// ═══════════════════════════════════════════════════════════════════════════

fn mops(ops: u64, elapsed: Duration) -> f64 {
    ops as f64 / elapsed.as_secs_f64() / 1e6
}
struct R {
    w: f64,
    r: f64,
}

macro_rules! bench_model {
    ($model_ty:ty, $nw:expr, $nr:expr) => {{
        let n = $nw + $nr;
        let m = Arc::new(<$model_ty>::new(n));
        let bar = Arc::new(Barrier::new(n));
        let wops = Arc::new(AtomicU64::new(0));
        let rops = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let start = Instant::now();
        std::thread::scope(|s| {
            for tid in 0..$nw {
                let m = &m;
                let bar = &bar;
                let wops = &wops;
                let done = &done;
                s.spawn(move || {
                    bar.wait();
                    for c in 0..OPS_PER_THREAD {
                        let _ = m.pin(tid);
                        m.retire(
                            tid,
                            GarbageBag {
                                epoch: m.epoch.load(Ordering::Relaxed),
                                count: BATCH_SIZE,
                            },
                        );
                        m.unpin(tid);
                        if c % 100 == 0 {
                            std::hint::spin_loop();
                        }
                    }
                    done.store(true, Ordering::Relaxed);
                    wops.fetch_add(OPS_PER_THREAD, Ordering::Relaxed);
                });
            }
            for tid in 0..$nr {
                let rtid = $nw + tid;
                let m = &m;
                let bar = &bar;
                let rops = &rops;
                let done = &done;
                s.spawn(move || {
                    bar.wait();
                    let mut c = 0u64;
                    loop {
                        let _ = m.pin(rtid);
                        black_box(0u64);
                        m.unpin(rtid);
                        m.try_reclaim(n);
                        c += 1;
                        if c >= READER_OPS && done.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    rops.fetch_add(c, Ordering::Relaxed);
                });
            }
        });
        let el = start.elapsed();
        R {
            w: mops(wops.load(Ordering::Relaxed) * BATCH_SIZE as u64, el),
            r: mops(rops.load(Ordering::Relaxed), el),
        }
    }};
}

fn main() {
    println!("=== Epoch Write Model: Mutex vs crossfire SPSC ===");
    println!(
        "  Batch: {} | W pubs: {} | R pins: {}\n",
        BATCH_SIZE, OPS_PER_THREAD, READER_OPS
    );

    let cfgs: &[(usize, usize)] = &[
        (1, 0),
        (1, 1),
        (1, 4),
        (1, 8),
        (2, 2),
        (2, 4),
        (4, 1),
        (4, 4),
    ];

    println!(
        "  {:>3}W {:>2}R {:>12} {:>12} {:>12} {:>12}",
        "", "", "Mutex W", "SPSC W", "Mutex R", "SPSC R"
    );
    println!("  {}", "─".repeat(60));

    for &(w, r) in cfgs {
        let mx = bench_model!(MutexModel, w, r);
        let cf = bench_model!(CrossfireModel, w, r);
        println!(
            "  {:>3}W {:>2}R {:>10.0} M {:>10.0} M {:>10.0} M {:>10.0} M",
            w, r, mx.w, cf.w, mx.r, cf.r
        );
    }

    println!("\n=== Done ===");
}
