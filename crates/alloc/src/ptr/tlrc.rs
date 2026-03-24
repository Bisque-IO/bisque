//! Tlrc — Thread-Local Reference Counted smart pointer.
//!
//! # "Never transition" design (from epoch_seq.rs)
//!
//! Per-thread counters for the ENTIRE lifecycle. No refcount transition.
//!
//! **Hot path (NO CAS, NO loop, NO fence):**
//!   seq.store(FROZEN) → count ±= 1 → seq.store(next).
//!   If scanner locked our counter (LOCKED): spin until released (rare).
//!
//! **Destruction:** when master_dropped && scan_all() == 0 → destroy.
//!   Scanner CAS's each idle counter's seq → LOCKED, reads count, restores.
//!   Double-scan with `destroying` flag prevents double-free.

use std::hash::{BuildHasherDefault, Hasher};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};

use parking_lot::Mutex;

// =========================================================================
// Identity hasher
// =========================================================================

#[derive(Default)]
struct IdentityHasher(u64);
impl Hasher for IdentityHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    #[inline(always)]
    fn write_usize(&mut self, i: usize) {
        self.0 = i as u64;
    }
}
type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

// =========================================================================
// TLCounter
// =========================================================================

/// Thread is modifying count. Scanner sees this and spins.
const FROZEN_SEQ: u32 = u32::MAX;
/// Scanner has locked this counter to read it. Thread spins on entry.
const LOCKED_SEQ: u32 = u32::MAX - 1;
/// Normal seq values must stay below this.
const SEQ_LIMIT: u32 = u32::MAX - 2;

struct TLCounter {
    count: AtomicI64,
    seq: AtomicU32,
    /// Set to true by TLCounterMap::Drop when the owning thread exits.
    /// try_destroy checks this: if orphaned AND tlrc_ptr==0, it frees
    /// the counter directly (no thread-local map holds a reference).
    orphaned: AtomicBool,
    tlrc_ptr: AtomicU64,
}

unsafe impl Send for TLCounter {}
unsafe impl Sync for TLCounter {}

// =========================================================================
// TLCounterMap (thread-local)
// =========================================================================

struct TLCounterMap {
    map: hashbrown::HashMap<usize, *mut TLCounter, IdentityBuildHasher>,
    cached_key: usize,
    cached_counter: *mut TLCounter,
}

impl TLCounterMap {
    #[inline(always)]
    fn get_cached(&mut self, key: usize) -> Option<*mut TLCounter> {
        if self.cached_key == key && !self.cached_counter.is_null() {
            if unsafe { (*self.cached_counter).tlrc_ptr.load(Ordering::Acquire) } != 0 {
                return Some(self.cached_counter);
            }
            self.cached_key = 0;
            self.cached_counter = std::ptr::null_mut();
        }
        None
    }

    #[inline(always)]
    fn get_or_stale(&mut self, key: usize) -> Option<*mut TLCounter> {
        if let Some(&ptr) = self.map.get(&key) {
            if unsafe { (*ptr).tlrc_ptr.load(Ordering::Acquire) } != 0 {
                self.cached_key = key;
                self.cached_counter = ptr;
                return Some(ptr);
            }
            self.map.remove(&key);
            self.cached_key = 0;
            self.cached_counter = std::ptr::null_mut();
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
        None
    }

    fn remove(&mut self, key: usize) {
        if self.cached_key == key {
            self.cached_key = 0;
            self.cached_counter = std::ptr::null_mut();
        }
        self.map.remove(&key);
    }

    fn sweep_stale(&mut self) {
        self.map.retain(|_, &mut ptr| {
            let stale = unsafe { (*ptr).tlrc_ptr.load(Ordering::Acquire) } == 0;
            if stale {
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
            !stale
        });
        self.cached_key = 0;
        self.cached_counter = std::ptr::null_mut();
    }
}

impl Drop for TLCounterMap {
    fn drop(&mut self) {
        for (_, &ptr) in self.map.iter() {
            let tlrc = unsafe { (*ptr).tlrc_ptr.load(Ordering::Acquire) };
            if tlrc == 0 {
                // Stale (Tlrc already destroyed). Safe to free.
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            } else {
                // Active or master-dropped. Mark orphaned so try_destroy
                // can free it after setting tlrc_ptr=0.
                unsafe {
                    (*ptr).orphaned.store(true, Ordering::Release);
                }
            }
        }
    }
}

thread_local! {
    static COUNTER_MAP: std::cell::UnsafeCell<TLCounterMap> =
        const { std::cell::UnsafeCell::new(TLCounterMap {
            map: hashbrown::HashMap::with_hasher(IdentityBuildHasher::new()),
            cached_key: 0,
            cached_counter: std::ptr::null_mut(),
        }) };
}

// =========================================================================
// TlrcInner
// =========================================================================

/// `tlrc_ptr` sentinel: master has dropped but inner is still alive.
/// Threads see this on bracket entry and know to trigger a scan after
/// interesting decrements. Distinguished from 0 (destroyed/stale).
const MASTER_DROPPED_SENTINEL: u64 = 1;

#[repr(C)]
pub(crate) struct TlrcInner<T> {
    counters: Mutex<Vec<*mut TLCounter>>,
    destroying: AtomicBool,
    pub(crate) data: std::mem::ManuallyDrop<T>,
}

impl<T> TlrcInner<T> {
    pub(crate) fn new(data: T) -> Self {
        Self {
            counters: Mutex::new(Vec::with_capacity(8)),
            destroying: AtomicBool::new(false),
            data: std::mem::ManuallyDrop::new(data),
        }
    }
}

unsafe impl<T: Send + Sync> Send for TlrcInner<T> {}
unsafe impl<T: Send + Sync> Sync for TlrcInner<T> {}

fn alloc_counter(key: usize, initial_count: i64, master_dropped: bool) -> *mut TLCounter {
    let tlrc_ptr = if master_dropped {
        MASTER_DROPPED_SENTINEL
    } else {
        key as u64
    };
    Box::into_raw(Box::new(TLCounter {
        count: AtomicI64::new(initial_count),
        seq: AtomicU32::new(0),
        orphaned: AtomicBool::new(false),
        tlrc_ptr: AtomicU64::new(tlrc_ptr),
    }))
}

// =========================================================================
// Scanner: CAS each counter's seq → LOCKED, read count, restore seq.
// =========================================================================

/// Scan all counters under a snapshot. CAS each idle counter's seq → LOCKED,
/// read count, restore seq. Active (FROZEN) counters: spin until idle.
fn scan_counters(snapshot: &[*mut TLCounter]) -> i64 {
    let mut sum: i64 = 0;
    for &c in snapshot.iter() {
        let counter = unsafe { &*c };
        loop {
            let s = counter.seq.load(Ordering::Acquire);
            if s == FROZEN_SEQ {
                std::hint::spin_loop();
                continue;
            }
            if s == LOCKED_SEQ {
                std::hint::spin_loop();
                continue;
            }
            if counter
                .seq
                .compare_exchange(s, LOCKED_SEQ, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                let count = counter.count.load(Ordering::Acquire);
                sum += count;
                counter.seq.store(s, Ordering::Release);
                break;
            }
            std::hint::spin_loop();
        }
    }
    sum
}

/// Try to destroy TlrcInner.
///
/// Enters an epoch guard so `inner` stays alive even if another thread's
/// try_destroy concurrently retires it. The epoch ensures the shell isn't
/// freed until we exit the guard.
///
/// Safety: `inner_ptr` must point to a valid (possibly epoch-retired but
/// not yet reclaimed) TlrcInner.
fn try_destroy<T>(inner_ptr: *const TlrcInner<T>) -> bool {
    let _guard = super::epoch::collector().enter();
    // SAFETY: Use raw pointer field access throughout — never create
    // &TlrcInner<T> because another thread's ManuallyDrop::drop creates
    // &mut to data, which conflicts with a shared borrow of the whole struct.
    let ptr = inner_ptr as *mut TlrcInner<T>;

    // Fast bail.
    if unsafe { &*std::ptr::addr_of!((*ptr).destroying) }.load(Ordering::Acquire) {
        return false;
    }

    // Lock to serialize try_destroy callers and block new registrations.
    let counters = unsafe { &*std::ptr::addr_of!((*ptr).counters) }.lock();
    if unsafe { &*std::ptr::addr_of!((*ptr).destroying) }.load(Ordering::Relaxed) {
        return false;
    }

    // Scan while holding lock. No new counters can appear.
    let sum = scan_counters(&counters);

    if sum != 0 {
        return false;
    }

    // sum==0 while holding Mutex. No new counters can appear.
    // Mark all counters stale and free orphaned ones (dead threads).
    for &c in counters.iter() {
        unsafe {
            (*c).tlrc_ptr.store(0, Ordering::Release);
            if (*c).orphaned.load(Ordering::Acquire) {
                drop(Box::from_raw(c));
            }
        }
    }

    // Set destroying BEFORE dropping lock.
    unsafe { &*std::ptr::addr_of!((*ptr).destroying) }.store(true, Ordering::Release);
    drop(counters);

    // Drop T's data synchronously (user destructor runs now).
    // Wrap in catch_unwind so the shell is always retired even if T::drop panics.
    let panic_payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        std::mem::ManuallyDrop::drop(&mut (*ptr).data);
    }))
    .err();

    // Retire the shell via epoch (always runs, even after panic).
    unsafe {
        super::epoch::collector().retire(ptr, reclaim_shell::<T>);
    }

    // Re-raise the panic after shell cleanup.
    if let Some(payload) = panic_payload {
        std::panic::resume_unwind(payload);
    }

    true
}

/// Epoch reclaim function for the TlrcInner shell.
/// By the time this runs, all threads have advanced past the retirement
/// epoch. No one is accessing the shell. Safe to dealloc.
unsafe fn reclaim_shell<T>(ptr: *mut TlrcInner<T>, _: &seize::Collector) {
    unsafe {
        // Drop the entire TlrcInner via Box. This properly drops:
        // - Mutex<Vec<...>>: frees the Vec's heap buffer
        // - AtomicBool: no-op
        // - ManuallyDrop<T>: does NOT re-run T::drop (already dropped in try_destroy)
        drop(Box::from_raw(ptr));
    }
}

// =========================================================================
// Tlrc / TlrcRef
// =========================================================================

pub struct Tlrc<T> {
    ptr: *const TlrcInner<T>,
}
pub struct TlrcRef<T> {
    ptr: *const TlrcInner<T>,
}

impl<T> Tlrc<T> {
    pub fn new(data: T) -> Self {
        Self {
            ptr: Box::into_raw(Box::new(TlrcInner::new(data))),
        }
    }

    pub unsafe fn from_raw(ptr: *mut TlrcInner<T>) -> Self {
        Self { ptr }
    }

    #[inline]
    fn inner(&self) -> &TlrcInner<T> {
        unsafe { &*self.ptr }
    }

    #[inline]
    pub fn tlrc_ref(&self) -> TlrcRef<T> {
        tlrc_increment(self.ptr);
        TlrcRef { ptr: self.ptr }
    }

    #[inline]
    pub fn as_ptr(this: &Self) -> *const T {
        &*this.inner().data as *const T
    }
}

impl<T> TlrcRef<T> {
    #[inline]
    fn inner(&self) -> &TlrcInner<T> {
        unsafe { &*self.ptr }
    }
    #[inline]
    pub fn as_ptr(this: &Self) -> *const T {
        &*this.inner().data as *const T
    }
}

impl<T> Clone for TlrcRef<T> {
    #[inline]
    fn clone(&self) -> Self {
        tlrc_increment(self.ptr);
        Self { ptr: self.ptr }
    }
}

// =========================================================================
// Tlrc::drop
// =========================================================================

impl<T> Drop for Tlrc<T> {
    fn drop(&mut self) {
        let ptr = self.ptr as *mut TlrcInner<T>;
        let key = self.ptr as usize;

        // Fast path: no refs ever created.
        {
            let counters = unsafe { &*std::ptr::addr_of!((*ptr).counters) }.lock();
            if counters.is_empty() {
                drop(counters);
                unsafe {
                    // Single owner: no concurrent access. Drop T, then free.
                    std::mem::ManuallyDrop::drop(&mut (*ptr).data);
                    // Now drop the shell (Mutex, etc.) and free allocation.
                    // ManuallyDrop won't re-drop T.
                    drop(Box::from_raw(ptr));
                }
                return;
            }
        }

        // Remove master's counter from thread-local map and mark orphaned.
        // After removal, no thread-local map holds a reference, so
        // try_destroy can free it directly (same as dead-thread orphan).
        let _ = COUNTER_MAP.try_with(|cell| {
            let tl = unsafe { &mut *cell.get() };
            if let Some(&cptr) = tl.map.get(&key) {
                unsafe {
                    (*cptr).orphaned.store(true, Ordering::Release);
                }
            }
            tl.remove(key);
        });

        // Signal master dropped by setting tlrc_ptr on ALL counters to
        // MASTER_DROPPED_SENTINEL. Threads see this on bracket entry
        // (part of the stale/cache check) — no inner access needed.
        {
            let counters = unsafe { &*std::ptr::addr_of!((*ptr).counters) }.lock();
            for &c in counters.iter() {
                unsafe {
                    (*c).tlrc_ptr
                        .store(MASTER_DROPPED_SENTINEL, Ordering::Release);
                }
            }
        }

        // Try to destroy now (all refs might already be gone).
        try_destroy(self.ptr);
    }
}

impl<T> Drop for TlrcRef<T> {
    #[inline]
    fn drop(&mut self) {
        tlrc_decrement(self.ptr);
    }
}

// =========================================================================
// Hot path: NO CAS, NO loop on the fast path.
//
// Bracket: seq=FROZEN → count±=1 → seq=next.
// If seq==LOCKED (scanner reading): spin until restored (extremely rare).
// After bracket: if master_dropped && count≤0: trigger scan.
// =========================================================================

#[inline(always)]
/// Execute a bracket: enter (seq=FROZEN), modify count, exit (seq=next).
/// Returns the new count value (for scan-trigger check).
fn do_bracket(counter: &TLCounter, delta: i64) -> i64 {
    let s = counter.seq.load(Ordering::Relaxed);

    // If scanner locked our counter: spin until restored.
    if s == LOCKED_SEQ {
        while counter.seq.load(Ordering::Acquire) == LOCKED_SEQ {
            std::hint::spin_loop();
        }
        return do_bracket(counter, delta);
    }

    // Enter bracket.
    counter.seq.store(FROZEN_SEQ, Ordering::Release);

    // Modify count.
    let c = counter.count.load(Ordering::Relaxed);
    let new_c = c + delta;
    counter.count.store(new_c, Ordering::Release);

    // Exit bracket.
    let next = if s + 1 >= SEQ_LIMIT { 0 } else { s + 1 };
    counter.seq.store(next, Ordering::Release);

    new_c
}

#[inline(always)]
fn tlrc_increment<T>(inner_ptr: *const TlrcInner<T>) {
    let key = inner_ptr as usize;

    COUNTER_MAP.with(|cell| {
        let tl = unsafe { &mut *cell.get() };
        let cptr = tl.get_cached(key).or_else(|| tl.get_or_stale(key));

        if let Some(cptr) = cptr {
            let _ = do_bracket(unsafe { &*cptr }, 1);
            // Increment never triggers destroy (count just went up).
        } else {
            // Cold path: register new counter. Enter epoch guard to
            // ensure inner stays alive during Mutex access.
            // Use raw pointer field access — avoid &TlrcInner (Stacked Borrows).
            tl.sweep_stale();
            let _guard = super::epoch::collector().enter();
            let iptr = inner_ptr as *mut TlrcInner<T>;
            let mut counters = unsafe { &*std::ptr::addr_of!((*iptr).counters) }.lock();
            if unsafe { &*std::ptr::addr_of!((*iptr).destroying) }.load(Ordering::Relaxed) {
                return;
            }
            // Check if master already dropped by inspecting existing counters.
            let md = counters.iter().any(|&c| unsafe {
                (*c).tlrc_ptr.load(Ordering::Relaxed) == MASTER_DROPPED_SENTINEL
            });
            let ptr = alloc_counter(key, 1, md);
            tl.map.insert(key, ptr);
            tl.cached_key = key;
            tl.cached_counter = ptr;
            counters.push(ptr);
        }
    });
}

#[inline(always)]
fn tlrc_decrement<T>(inner_ptr: *const TlrcInner<T>) {
    let key = inner_ptr as usize;

    let should_scan = COUNTER_MAP.with(|cell| {
        let tl = unsafe { &mut *cell.get() };
        let cptr = tl.get_cached(key).or_else(|| tl.get_or_stale(key));

        if let Some(cptr) = cptr {
            let counter = unsafe { &*cptr };
            // Check master_dropped via tlrc_ptr sentinel — NO inner access.
            // This is on the TLCounter (thread-local cache line), not inner.
            let master_dropped =
                counter.tlrc_ptr.load(Ordering::Acquire) == MASTER_DROPPED_SENTINEL;
            let new_c = do_bracket(counter, -1);

            // Trigger scan if master dropped AND our count is interesting.
            master_dropped && new_c <= 0
        } else {
            // Cold path: register new counter with -1 (cross-thread debt).
            // Enter epoch guard to ensure inner stays alive.
            // Use raw pointer field access — avoid &TlrcInner (Stacked Borrows).
            tl.sweep_stale();
            let _guard = super::epoch::collector().enter();
            let iptr = inner_ptr as *mut TlrcInner<T>;
            let mut counters = unsafe { &*std::ptr::addr_of!((*iptr).counters) }.lock();
            if unsafe { &*std::ptr::addr_of!((*iptr).destroying) }.load(Ordering::Relaxed) {
                return false;
            }
            // Check if master already dropped via existing counters' tlrc_ptr.
            let md = counters.iter().any(|&c| unsafe {
                (*c).tlrc_ptr.load(Ordering::Relaxed) == MASTER_DROPPED_SENTINEL
            });
            let ptr = alloc_counter(key, -1, md);
            tl.map.insert(key, ptr);
            tl.cached_key = key;
            tl.cached_counter = ptr;
            counters.push(ptr);
            drop(counters);
            // Trigger scan: master dropped AND we just registered debt (-1 ≤ 0).
            md
        }
    });

    if should_scan {
        // try_destroy enters its own epoch guard for safety.
        try_destroy::<T>(inner_ptr);
    }
}

// =========================================================================
// Trait impls
// =========================================================================

impl<T> Deref for Tlrc<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &*self.inner().data
    }
}
impl<T> Deref for TlrcRef<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &*self.inner().data
    }
}
unsafe impl<T: Send + Sync> Send for Tlrc<T> {}
unsafe impl<T: Send + Sync> Sync for Tlrc<T> {}
unsafe impl<T: Send + Sync> Send for TlrcRef<T> {}
unsafe impl<T: Send + Sync> Sync for TlrcRef<T> {}
impl<T: std::fmt::Debug> std::fmt::Debug for Tlrc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}
impl<T: std::fmt::Debug> std::fmt::Debug for TlrcRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Barrier};
    use std::thread;

    struct DropCounter {
        count: Arc<AtomicUsize>,
    }
    impl DropCounter {
        fn new(c: &Arc<AtomicUsize>) -> Self {
            Self { count: c.clone() }
        }
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn new_and_deref() {
        assert_eq!(*Tlrc::new(42u64), 42);
    }

    #[test]
    fn tlrc_ref_deref() {
        assert_eq!(&*Tlrc::new(String::from("hello")).tlrc_ref(), "hello");
    }

    #[test]
    fn clone_tlrc_ref() {
        let t = Tlrc::new(100u32);
        let r1 = t.tlrc_ref();
        let r2 = r1.clone();
        assert_eq!(*r1, 100);
        assert_eq!(*r2, 100);
    }

    #[test]
    fn drop_order_master_last() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r1 = t.tlrc_ref();
        let r2 = t.tlrc_ref();
        drop(r1);
        drop(r2);
        assert_eq!(d.load(Ordering::SeqCst), 0);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_order_master_first() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r1 = t.tlrc_ref();
        let r2 = r1.clone();
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 0);
        drop(r1);
        assert_eq!(d.load(Ordering::SeqCst), 0);
        drop(r2);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn debug_impl() {
        let t = Tlrc::new(42);
        assert_eq!(format!("{:?}", t), "42");
        assert_eq!(format!("{:?}", t.tlrc_ref()), "42");
    }

    #[test]
    fn as_ptr() {
        let t = Tlrc::new(99u32);
        let r = t.tlrc_ref();
        assert_eq!(Tlrc::as_ptr(&t), TlrcRef::as_ptr(&r));
    }

    #[test]
    fn exactly_one_drop_single_thread() {
        let d = Arc::new(AtomicUsize::new(0));
        {
            let t = Tlrc::new(DropCounter::new(&d));
            let _r1 = t.tlrc_ref();
            let _r2 = t.tlrc_ref();
            let _r3 = _r2.clone();
        }
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn exactly_one_drop_multi_thread() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let h: Vec<_> = (0..8)
            .map(|_| {
                let r = t.tlrc_ref();
                thread::spawn(move || {
                    let _c1 = r.clone();
                    let _c2 = r.clone();
                })
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        assert_eq!(d.load(Ordering::SeqCst), 0);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn no_leak_master_drops_after_all_refs() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let h: Vec<_> = (0..4)
            .map(|_| {
                let r = t.tlrc_ref();
                thread::spawn(move || drop(r))
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn no_leak_master_drops_before_all_refs() {
        let d = Arc::new(AtomicUsize::new(0));
        let d2 = d.clone();
        let h = thread::spawn(move || {
            let t = Tlrc::new(DropCounter::new(&d2));
            let b = Arc::new(Barrier::new(5));
            let h: Vec<_> = (0..4)
                .map(|_| {
                    let r = t.tlrc_ref();
                    let b = b.clone();
                    thread::spawn(move || {
                        let r = r.clone();
                        b.wait();
                        drop(r);
                    })
                })
                .collect();
            drop(t);
            b.wait();
            for h in h {
                h.join().unwrap();
            }
        });
        h.join().unwrap();
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn battle_high_contention() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let h: Vec<_> = (0..16)
            .map(|_| {
                let r = t.tlrc_ref();
                thread::spawn(move || {
                    for _ in 0..100_000 {
                        let c = r.clone();
                        std::hint::black_box(&*c);
                        drop(c);
                    }
                })
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn battle_cross_thread() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let h: Vec<_> = (0..8)
            .map(|_| {
                let r = t.tlrc_ref();
                thread::spawn(move || {
                    let sh: Vec<_> = (0..1_000)
                        .map(|_| {
                            let c = r.clone();
                            thread::spawn(move || {
                                std::hint::black_box(&*c);
                                drop(c);
                            })
                        })
                        .collect();
                    for h in sh {
                        h.join().unwrap();
                    }
                })
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    // === Fuzz tests ===
    struct UafSentinel {
        magic: AtomicU64,
        drops: Arc<AtomicUsize>,
    }
    impl UafSentinel {
        const ALIVE: u64 = 0xDEAD_BEEF_CAFE_BABE;
        fn new(d: &Arc<AtomicUsize>) -> Self {
            Self {
                magic: AtomicU64::new(Self::ALIVE),
                drops: d.clone(),
            }
        }
        fn assert_alive(&self) {
            assert_eq!(
                self.magic.load(Ordering::Relaxed),
                Self::ALIVE,
                "USE-AFTER-FREE"
            );
        }
    }
    impl Drop for UafSentinel {
        fn drop(&mut self) {
            assert_eq!(
                self.magic.load(Ordering::Relaxed),
                Self::ALIVE,
                "DOUBLE-FREE"
            );
            self.magic.store(0, Ordering::Relaxed);
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn fuzz_concurrent_master_drop() {
        for i in 0..50 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(17));
                let h: Vec<_> = (0..16)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            let r = r.clone();
                            b.wait();
                            for _ in 0..50_000 {
                                let c = r.clone();
                                c.assert_alive();
                                drop(c);
                            }
                            r.assert_alive();
                            drop(r);
                        })
                    })
                    .collect();
                b.wait();
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1, "iteration {i}");
        }
    }

    #[test]
    fn fuzz_cross_thread_scatter() {
        for i in 0..20 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(9));
                let h: Vec<_> = (0..8)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            let sh: Vec<_> = (0..100)
                                .map(|_| {
                                    let c = r.clone();
                                    thread::spawn(move || {
                                        c.assert_alive();
                                        drop(c);
                                    })
                                })
                                .collect();
                            r.assert_alive();
                            drop(r);
                            for h in sh {
                                h.join().unwrap();
                            }
                        })
                    })
                    .collect();
                b.wait();
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1, "iteration {i}");
        }
    }

    #[test]
    fn fuzz_rapid_lifecycle() {
        let total = Arc::new(AtomicUsize::new(0));
        for i in 0..1_000 {
            let di = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(UafSentinel::new(&di));
            let h: Vec<_> = (0..4)
                .map(|_| {
                    let r = t.tlrc_ref();
                    thread::spawn(move || {
                        for _ in 0..100 {
                            let c = r.clone();
                            c.assert_alive();
                            drop(c);
                        }
                        r.assert_alive();
                    })
                })
                .collect();
            for h in h {
                h.join().unwrap();
            }
            drop(t);
            assert_eq!(di.load(Ordering::SeqCst), 1, "lifecycle {i}");
            total.fetch_add(1, Ordering::Relaxed);
        }
        assert_eq!(total.load(Ordering::Relaxed), 1_000);
    }

    #[test]
    fn fuzz_max_contention_storm() {
        for _ in 0..20 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let running = Arc::new(AtomicBool::new(true));
                let h: Vec<_> = (0..16)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let running = running.clone();
                        thread::spawn(move || {
                            let mut n = 0u64;
                            while running.load(Ordering::Relaxed) || n < 10_000 {
                                let c = r.clone();
                                c.assert_alive();
                                drop(c);
                                n += 1;
                            }
                            r.assert_alive();
                        })
                    })
                    .collect();
                thread::sleep(std::time::Duration::from_millis(5));
                running.store(false, Ordering::Relaxed);
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn fuzz_unbalanced_debt() {
        for _ in 0..50 {
            let d = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(UafSentinel::new(&d));
            let r = t.tlrc_ref();
            let cl: Vec<_> = (0..64).map(|_| r.clone()).collect();
            drop(r);
            let h: Vec<_> = cl
                .into_iter()
                .map(|c| {
                    thread::spawn(move || {
                        c.assert_alive();
                        drop(c);
                    })
                })
                .collect();
            for h in h {
                h.join().unwrap();
            }
            drop(t);
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn fuzz_master_first_debt() {
        for _ in 0..50 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(33));
                let h: Vec<_> = (0..32)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            r.assert_alive();
                            drop(r);
                        })
                    })
                    .collect();
                drop(t);
                b.wait();
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            let drops = d.load(Ordering::SeqCst);
            assert!(
                drops == 1,
                "fuzz_master_first_debt: drops={drops} (expected 1)"
            );
        }
    }

    #[test]
    fn battle_concurrent_master_drop() {
        for iter_i in 0..10 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(DropCounter::new(&d2));
                let b = Arc::new(Barrier::new(9));
                let h: Vec<_> = (0..8)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            let r = r.clone();
                            b.wait();
                            for _ in 0..10_000 {
                                let c = r.clone();
                                std::hint::black_box(&*c);
                                drop(c);
                            }
                            drop(r);
                        })
                    })
                    .collect();
                b.wait();
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1, "iter {iter_i}");
        }
    }

    #[test]
    fn battle_many_short_lived() {
        let d = Arc::new(AtomicUsize::new(0));
        for i in 0..10_000 {
            let t = Tlrc::new(DropCounter::new(&d));
            let r = t.tlrc_ref();
            std::hint::black_box(&*r);
            drop(r);
            drop(t);
            assert_eq!(d.load(Ordering::SeqCst), i + 1, "leak at {i}");
        }
    }

    #[test]
    fn battle_many_instances() {
        let num = 100;
        let d = Arc::new(AtomicUsize::new(0));
        let owners: Vec<_> = (0..num).map(|_| Tlrc::new(DropCounter::new(&d))).collect();
        let refs: Vec<Vec<_>> = (0..8)
            .map(|_| owners.iter().map(|t| t.tlrc_ref()).collect())
            .collect();
        let h: Vec<_> = refs
            .into_iter()
            .map(|tr| {
                thread::spawn(move || {
                    for r in &tr {
                        std::hint::black_box(&**r);
                    }
                    drop(tr);
                })
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        drop(owners);
        assert_eq!(d.load(Ordering::SeqCst), num);
    }

    #[test]
    fn battle_staggered_exit() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let h: Vec<_> = (0..8u64)
            .map(|i| {
                let r = t.tlrc_ref();
                thread::spawn(move || {
                    thread::sleep(std::time::Duration::from_millis(i * 5));
                    for _ in 0..1_000 {
                        let c = r.clone();
                        std::hint::black_box(&*c);
                        drop(c);
                    }
                })
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    // =====================================================================
    // Orphan + Stale lifecycle tests
    // =====================================================================

    /// Thread exits BEFORE master drops (orphan path).
    /// TLCounterMap::Drop sets orphaned=true. Later try_destroy frees.
    #[test]
    fn orphan_thread_exits_before_master() {
        for _ in 0..100 {
            let d = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(UafSentinel::new(&d));
            // Spawn threads that drop their refs and EXIT.
            let h: Vec<_> = (0..8)
                .map(|_| {
                    let r = t.tlrc_ref();
                    thread::spawn(move || {
                        drop(r);
                        // Thread exits here. TLCounterMap::Drop marks orphaned.
                    })
                })
                .collect();
            for h in h {
                h.join().unwrap();
            }
            // Now all threads are dead. Counters are orphaned.
            // Master drops → try_destroy frees orphaned counters.
            drop(t);
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Master drops BEFORE threads exit (stale path).
    /// try_destroy sets tlrc_ptr=0. TLCounterMap::Drop frees stale counters.
    #[test]
    fn stale_master_drops_before_thread_exits() {
        for _ in 0..100 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(9));
                let h: Vec<_> = (0..8)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            // Drop ref. Counter becomes stale after try_destroy.
                            drop(r);
                            // Thread stays alive briefly. TLCounterMap::Drop will
                            // see tlrc_ptr=0 → free the counter.
                        })
                    })
                    .collect();
                drop(t); // master drops first
                b.wait(); // release threads
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Both die concurrently — thread exits while master drops.
    /// Tests the race between orphaned and stale paths.
    #[test]
    fn orphan_stale_concurrent() {
        for _ in 0..200 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(17));
                let h: Vec<_> = (0..16)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            r.assert_alive();
                            drop(r);
                            // Thread exits immediately after drop.
                            // Race: TLCounterMap::Drop vs try_destroy.
                        })
                    })
                    .collect();
                b.wait();
                drop(t); // master drops concurrently with thread exits
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Dormant threads: drop ref immediately (debt), then never touch
    /// this Tlrc again. Other threads are active. Tests scanner handling
    /// of mixed dormant + active counters.
    #[test]
    fn fuzz_dormant_mixed_active() {
        for _ in 0..100 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(17));
                let h: Vec<_> = (0..16)
                    .enumerate()
                    .map(|(i, _)| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        let is_dormant = i < 8; // half are dormant
                        thread::spawn(move || {
                            b.wait();
                            if is_dormant {
                                // Drop immediately. Debt counter (-1). No more ops.
                                drop(r);
                            } else {
                                // Active: clone/drop loop, then drop.
                                for _ in 0..10_000 {
                                    let c = r.clone();
                                    c.assert_alive();
                                    drop(c);
                                }
                                r.assert_alive();
                                drop(r);
                            }
                        })
                    })
                    .collect();
                b.wait();
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// All threads are dormant (pure debt). Master drops. Scanner must
    /// handle all-dormant case where no thread triggers a scan.
    #[test]
    fn fuzz_all_dormant() {
        for _ in 0..500 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(33));
                let h: Vec<_> = (0..32)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            // Drop immediately. Thread becomes dormant.
                            drop(r);
                        })
                    })
                    .collect();
                b.wait();
                // Wait for all threads to drop their refs.
                for h in h {
                    h.join().unwrap();
                }
                // Now drop master. All counters are dormant.
                // Tlrc::drop's try_destroy should find sum==0.
                drop(t);
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// All threads dormant, master drops FIRST (threads haven't dropped yet).
    /// Master's try_destroy finds sum!=0 (threads still hold refs).
    /// Then threads drop (debt) and trigger try_destroy.
    #[test]
    fn fuzz_all_dormant_master_first() {
        for _ in 0..200 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(17));
                let h: Vec<_> = (0..16)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            r.assert_alive();
                            drop(r);
                        })
                    })
                    .collect();
                drop(t); // master drops before barrier
                b.wait(); // release threads
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Thread exits with the counter still alive (orphan), then the Tlrc
    /// is reused at the same address. Tests that stale detection works
    /// after address reuse.
    #[test]
    fn stale_address_reuse() {
        let d = Arc::new(AtomicUsize::new(0));
        for i in 0..1_000 {
            let t = Tlrc::new(DropCounter::new(&d));
            let r = t.tlrc_ref();
            std::hint::black_box(&*r);
            drop(r);
            drop(t);
            assert_eq!(d.load(Ordering::SeqCst), i + 1, "leak at {i}");
        }
    }

    /// Massive cross-thread debt: N threads each get 1 ref created on
    /// main, moved across. Each thread creates additional clones and
    /// scatters them to sub-threads. Deep debt chains.
    #[test]
    fn fuzz_deep_debt_chains() {
        for _ in 0..20 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let h: Vec<_> = (0..8)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        thread::spawn(move || {
                            // Create sub-clones, scatter to sub-threads.
                            let sh: Vec<_> = (0..50)
                                .map(|_| {
                                    let c = r.clone();
                                    thread::spawn(move || {
                                        // Sub-sub-clones.
                                        let ssh: Vec<_> = (0..10)
                                            .map(|_| {
                                                let cc = c.clone();
                                                thread::spawn(move || {
                                                    cc.assert_alive();
                                                    drop(cc);
                                                })
                                            })
                                            .collect();
                                        c.assert_alive();
                                        drop(c);
                                        for h in ssh {
                                            h.join().unwrap();
                                        }
                                    })
                                })
                                .collect();
                            r.assert_alive();
                            drop(r);
                            for h in sh {
                                h.join().unwrap();
                            }
                        })
                    })
                    .collect();
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Rapid Tlrc creation and destruction across threads.
    /// Tests TLCounterMap sweep_stale under pressure.
    #[test]
    fn fuzz_sweep_stale_pressure() {
        let total = Arc::new(AtomicUsize::new(0));
        let h: Vec<_> = (0..4)
            .map(|_| {
                let total = total.clone();
                thread::spawn(move || {
                    for _ in 0..1_000 {
                        let d = Arc::new(AtomicUsize::new(0));
                        let t = Tlrc::new(DropCounter::new(&d));
                        let r = t.tlrc_ref();
                        let c1 = r.clone();
                        let c2 = r.clone();
                        drop(r);
                        drop(c1);
                        drop(c2);
                        drop(t);
                        assert_eq!(d.load(Ordering::SeqCst), 1);
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        assert_eq!(total.load(Ordering::Relaxed), 4_000);
    }

    /// Thread pool simulation: long-lived threads working on many
    /// short-lived Tlrc instances. Tests counter reuse and cleanup.
    #[test]
    fn fuzz_thread_pool_workload() {
        let b = Arc::new(Barrier::new(9));
        let total = Arc::new(AtomicUsize::new(0));
        let h: Vec<_> = (0..8)
            .map(|_| {
                let b = b.clone();
                let total = total.clone();
                thread::spawn(move || {
                    b.wait();
                    for _ in 0..500 {
                        let d = Arc::new(AtomicUsize::new(0));
                        let t = Tlrc::new(DropCounter::new(&d));
                        let r = t.tlrc_ref();
                        for _ in 0..50 {
                            let c = r.clone();
                            std::hint::black_box(&*c);
                            drop(c);
                        }
                        drop(r);
                        drop(t);
                        assert_eq!(d.load(Ordering::SeqCst), 1);
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        b.wait();
        for h in h {
            h.join().unwrap();
        }
        assert_eq!(total.load(Ordering::Relaxed), 4_000);
    }

    /// Single ref, no clones. Tests minimal lifecycle.
    #[test]
    fn single_ref_no_clones() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r = t.tlrc_ref();
        drop(r);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// No refs at all. Fast path in Tlrc::drop.
    #[test]
    fn no_refs_at_all() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Many refs on a single thread (no cross-thread, high count).
    #[test]
    fn many_refs_single_thread() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let refs: Vec<_> = (0..1_000).map(|_| t.tlrc_ref()).collect();
        drop(refs);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Clone chain: each thread clones from the previous thread's ref.
    #[test]
    fn clone_chain_across_threads() {
        for _ in 0..50 {
            let d = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(UafSentinel::new(&d));
            let mut r = t.tlrc_ref();
            let mut handles = vec![];
            for _ in 0..16 {
                let next = r.clone();
                let prev = r;
                handles.push(thread::spawn(move || {
                    prev.assert_alive();
                    drop(prev);
                }));
                r = next;
            }
            r.assert_alive();
            drop(r);
            drop(t);
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Stress: alternating clone and drop across two threads.
    /// One thread clones, other drops. Maximum cross-thread traffic.
    #[test]
    fn fuzz_ping_pong_cross_thread() {
        use std::sync::mpsc;
        for _ in 0..20 {
            let d = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(UafSentinel::new(&d));
            let (tx, rx) = mpsc::sync_channel::<TlrcRef<UafSentinel>>(32);
            let r = t.tlrc_ref();

            // Producer: clones on its thread, sends to consumer.
            let producer = {
                let r = r.clone();
                thread::spawn(move || {
                    for _ in 0..10_000 {
                        let c = r.clone();
                        if tx.send(c).is_err() {
                            break;
                        }
                    }
                    drop(r);
                })
            };

            // Consumer: receives and drops (cross-thread debt).
            let consumer = thread::spawn(move || {
                while let Ok(c) = rx.recv() {
                    c.assert_alive();
                    drop(c);
                }
            });

            producer.join().unwrap();
            consumer.join().unwrap();
            drop(r);
            drop(t);
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Scan trigger race: many threads bring count to 0 simultaneously
    /// after master drops. Multiple threads call try_destroy. Exactly
    /// one should win.
    #[test]
    fn fuzz_simultaneous_scan_trigger() {
        for _ in 0..200 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(65));
                // Create 64 refs, each on its own thread.
                let h: Vec<_> = (0..64)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            // All drop simultaneously.
                            r.assert_alive();
                            drop(r);
                        })
                    })
                    .collect();
                drop(t); // master drops
                b.wait(); // all threads drop at once
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    /// Scanner locked counter while thread enters bracket.
    /// Tests the LOCKED_SEQ spin path on the thread side.
    #[test]
    fn fuzz_scanner_thread_interleave() {
        for _ in 0..10 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(17));
                let h: Vec<_> = (0..16)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        let b = b.clone();
                        thread::spawn(move || {
                            b.wait();
                            // Rapid clone+drop to maximize chance of hitting
                            // LOCKED_SEQ during try_destroy's scan.
                            for _ in 0..50_000 {
                                let c = r.clone();
                                drop(c);
                            }
                            drop(r);
                        })
                    })
                    .collect();
                b.wait();
                // Master drops mid-operation. Scanner will CAS counters
                // while threads are actively bracketing.
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    // =====================================================================
    // Miri-compatible tests: small iteration counts, all state transitions.
    // Run with: cargo +nightly miri test -p bisque-alloc --lib ptr::tlrc::tests::miri_
    //
    // Detects: UAF, double-free, data races, invalid memory access,
    //          stale/orphan lifecycle bugs, scanner CAS correctness.
    // =====================================================================

    /// Miri: full lifecycle — alloc, clone, drop, destroy on one thread.
    #[test]
    fn miri_single_thread_lifecycle() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r1 = t.tlrc_ref();
        let r2 = r1.clone();
        let r3 = r2.clone();
        drop(r3);
        drop(r2);
        drop(r1);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Miri: master drops first, refs later (same thread).
    #[test]
    fn miri_master_first_same_thread() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r = t.tlrc_ref();
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 0);
        drop(r);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Miri: cross-thread drop (debt counter).
    #[test]
    fn miri_cross_thread_drop() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r = t.tlrc_ref();
        let h = thread::spawn(move || {
            std::hint::black_box(&*r);
            drop(r);
        });
        h.join().unwrap();
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Miri: master drops while 2 threads hold refs.
    #[test]
    fn miri_master_drops_threads_hold() {
        let d = Arc::new(AtomicUsize::new(0));
        let d2 = d.clone();
        let h = thread::spawn(move || {
            let t = Tlrc::new(DropCounter::new(&d2));
            let b = Arc::new(Barrier::new(3));
            let h: Vec<_> = (0..2)
                .map(|_| {
                    let r = t.tlrc_ref();
                    let b = b.clone();
                    thread::spawn(move || {
                        b.wait();
                        let c = r.clone();
                        drop(c);
                        drop(r);
                    })
                })
                .collect();
            drop(t);
            b.wait();
            for h in h {
                h.join().unwrap();
            }
        });
        h.join().unwrap();
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Miri: orphan — thread exits before master drops.
    #[test]
    fn miri_orphan_lifecycle() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r = t.tlrc_ref();
        let h = thread::spawn(move || drop(r));
        h.join().unwrap();
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Miri: stale — master drops first, then new Tlrc at possibly same addr.
    #[test]
    fn miri_stale_lifecycle() {
        let d = Arc::new(AtomicUsize::new(0));
        for i in 0..3 {
            let t = Tlrc::new(DropCounter::new(&d));
            let r = t.tlrc_ref();
            drop(r);
            drop(t);
            assert_eq!(d.load(Ordering::SeqCst), i + 1);
        }
    }

    /// Miri: cross-thread debt — clones on main, drops on spawned.
    #[test]
    fn miri_cross_thread_debt() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r = t.tlrc_ref();
        let h: Vec<_> = (0..3)
            .map(|_| {
                let c = r.clone();
                thread::spawn(move || drop(c))
            })
            .collect();
        for h in h {
            h.join().unwrap();
        }
        drop(r);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Miri: multiple Tlrc instances on same thread (tests map with multiple keys).
    #[test]
    fn miri_multiple_instances() {
        let d1 = Arc::new(AtomicUsize::new(0));
        let d2 = Arc::new(AtomicUsize::new(0));
        let t1 = Tlrc::new(DropCounter::new(&d1));
        let t2 = Tlrc::new(DropCounter::new(&d2));
        let r1 = t1.tlrc_ref();
        let r2 = t2.tlrc_ref();
        drop(r1);
        drop(r2);
        drop(t1);
        drop(t2);
        assert_eq!(d1.load(Ordering::SeqCst), 1);
        assert_eq!(d2.load(Ordering::SeqCst), 1);
    }

    /// Miri: 2 threads both trigger scan after master drops.
    #[test]
    fn miri_concurrent_scan() {
        let d = Arc::new(AtomicUsize::new(0));
        let d2 = d.clone();
        let h = thread::spawn(move || {
            let t = Tlrc::new(DropCounter::new(&d2));
            let b = Arc::new(Barrier::new(3));
            let h: Vec<_> = (0..2)
                .map(|_| {
                    let r = t.tlrc_ref();
                    let b = b.clone();
                    thread::spawn(move || {
                        b.wait();
                        drop(r);
                    })
                })
                .collect();
            drop(t);
            b.wait();
            for h in h {
                h.join().unwrap();
            }
        });
        h.join().unwrap();
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    /// Miri: scanner CAS races with thread bracket entry.
    #[test]
    fn miri_scanner_bracket_race() {
        let d = Arc::new(AtomicUsize::new(0));
        let d2 = d.clone();
        let h = thread::spawn(move || {
            let t = Tlrc::new(DropCounter::new(&d2));
            let b = Arc::new(Barrier::new(3));
            let h: Vec<_> = (0..2)
                .map(|_| {
                    let r = t.tlrc_ref();
                    let b = b.clone();
                    thread::spawn(move || {
                        b.wait();
                        for _ in 0..5 {
                            let c = r.clone();
                            drop(c);
                        }
                        drop(r);
                    })
                })
                .collect();
            b.wait();
            drop(t);
            for h in h {
                h.join().unwrap();
            }
        });
        h.join().unwrap();
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }
}
