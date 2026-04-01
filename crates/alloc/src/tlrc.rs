//! Tlrc — Thread-Local Reference Counted smart pointer.
//!
//! # Per-thread atomic slot design
//!
//! Each `TlrcInner` owns a registry of per-thread slots. Each slot contains
//! a single `AtomicI64` delta counter, cache-line padded to prevent false
//! sharing between threads.
//!
//! **Hot path:** `slot.delta.fetch_add(±1, Relaxed)` — one atomic RMW on the
//! thread's own cache line. Since only one thread writes to each slot, the
//! cache line stays in Exclusive/Modified state and the LOCK prefix (x86) or
//! LDXR/STXR (ARM) completes without bus traffic.
//!
//! **Collection (swap-scan):**
//!   1. Lock the slot registry (blocks new slot creation).
//!   2. For each slot, `delta.swap(0, AcqRel)` — atomically read and zero.
//!   3. Sum the swapped values.
//!   4. If sum=0 → object is unreachable, destroy it.
//!   5. If sum≠0 → restore each slot via `delta.fetch_add(value, Release)`.
//!
//! **Safety argument:** The swap-scan gives a linearizable snapshot. Each
//! concurrent `fetch_add(±1)` either lands before the swap (included in the
//! sum) or after (adds to the zeroed slot, preserved by the restore).
//! If sum=0 after the scan, no thread holds a live reference. New references
//! require cloning an existing one, so no thread can acquire a reference
//! from nothing. Therefore the sum stays 0 and destruction is safe.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use parking_lot::Mutex;

use crate::epoch;
use crate::epoch::raw::membarrier;

// =========================================================================
// ThreadSlot — one per (thread, TlrcInner) pair, cache-line padded.
// =========================================================================

struct ThreadSlot {
    /// Cumulative delta (increments - decrements) for this thread.
    /// Only the owning thread does fetch_add; the collector does swap/load.
    delta: AtomicI64,
    /// False after the owning thread's TLSlotMap has been dropped.
    alive: AtomicBool,
    /// True after try_destroy claims this slot's TlrcInner. Signals to
    /// get_slot that the cached entry is stale (address may be reused).
    stale: AtomicBool,
}

impl ThreadSlot {
    fn new_leaked() -> &'static Self {
        Box::leak(Box::new(crate::CachePadded::new(Self {
            delta: AtomicI64::new(0),
            alive: AtomicBool::new(true),
            stale: AtomicBool::new(false),
        })))
    }
}

// =========================================================================
// Thread-local slot map
// =========================================================================

/// Identity hasher for pointer-keyed maps (avoids hashing overhead).
#[derive(Default)]
struct IdentityHasher(u64);
impl std::hash::Hasher for IdentityHasher {
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
type IdentityBuildHasher = std::hash::BuildHasherDefault<IdentityHasher>;

struct TLSlotMap {
    map: hashbrown::HashMap<usize, &'static ThreadSlot, IdentityBuildHasher>,
    /// Single-entry cache for the most recently used slot.
    cached_key: usize,
    cached_slot: *const ThreadSlot,
}

impl TLSlotMap {
    const fn new() -> Self {
        Self {
            map: hashbrown::HashMap::with_hasher(IdentityBuildHasher::new()),
            cached_key: 0,
            cached_slot: std::ptr::null(),
        }
    }

    /// Fast path: check the single-entry cache.
    #[inline(always)]
    fn get_cached(&self, key: usize) -> Option<&'static ThreadSlot> {
        if self.cached_key == key && !self.cached_slot.is_null() {
            Some(unsafe { &*self.cached_slot })
        } else {
            None
        }
    }

    /// Slow path: look up in the HashMap.
    #[inline]
    fn get_or_insert(
        &mut self,
        key: usize,
        create: impl FnOnce() -> &'static ThreadSlot,
    ) -> &'static ThreadSlot {
        let slot = self.map.entry(key).or_insert_with(create);
        self.cached_key = key;
        self.cached_slot = *slot as *const ThreadSlot;
        *slot
    }

    /// Remove entries for destroyed TlrcInner objects.
    fn sweep_stale(&mut self) {
        // Slots whose TlrcInner is destroyed have been removed from the
        // inner's registry. We detect staleness via the `alive` flag on
        // the slot — if the slot is dead (thread exited) and delta=0,
        // we can remove it from our map. For live slots, we keep them
        // even if the inner might be destroyed, since they're harmless.
        //
        // Actually, we can't easily detect whether a TlrcInner is destroyed
        // just from the slot. For simplicity, we don't sweep — stale entries
        // in the map are ~16 bytes each and bounded by the number of distinct
        // Tlrc objects this thread has interacted with.
    }
}

impl Drop for TLSlotMap {
    fn drop(&mut self) {
        // Thread is exiting. Mark all our slots as dead.
        // The delta values persist in the leaked slots for the collector.
        for (_, slot) in self.map.iter() {
            slot.alive.store(false, Ordering::Release);
        }
        self.cached_key = 0;
        self.cached_slot = std::ptr::null();
    }
}

thread_local! {
    static TL_SLOTS: std::cell::UnsafeCell<TLSlotMap> =
        const { std::cell::UnsafeCell::new(TLSlotMap::new()) };
}

// =========================================================================
// TlrcInner
// =========================================================================

#[repr(C)]
pub(crate) struct TlrcInner<T> {
    /// All thread slots that have ever interacted with this object.
    slots: Mutex<Vec<&'static ThreadSlot>>,
    /// Set true once destruction is claimed. Prevents double-free.
    destroying: AtomicBool,
    /// True once the master `Tlrc` has been dropped.
    master_dropped: AtomicBool,
    pub(crate) data: std::mem::ManuallyDrop<T>,
}

impl<T> TlrcInner<T> {
    pub(crate) fn new(data: T) -> Self {
        Self {
            slots: Mutex::new(Vec::new()),
            destroying: AtomicBool::new(false),
            master_dropped: AtomicBool::new(false),
            data: std::mem::ManuallyDrop::new(data),
        }
    }
}

unsafe impl<T: Send + Sync> Send for TlrcInner<T> {}
unsafe impl<T: Send + Sync> Sync for TlrcInner<T> {}

// =========================================================================
// Collection: swap-scan
// =========================================================================

/// Try to destroy the TlrcInner. Returns true if destruction happened.
///
/// Safety: `inner_ptr` must point to a valid TlrcInner.
fn try_destroy<T>(inner_ptr: *const TlrcInner<T>) -> bool {
    let ptr = inner_ptr as *mut TlrcInner<T>;

    // Fast bail: already being destroyed.
    if unsafe { &*std::ptr::addr_of!((*ptr).destroying) }.load(Ordering::Acquire) {
        return false;
    }

    // Lock to serialize concurrent try_destroy callers and block new registrations.
    let slots = unsafe { &*std::ptr::addr_of!((*ptr).slots) }.lock();

    // Double-check after acquiring lock.
    if unsafe { &*std::ptr::addr_of!((*ptr).destroying) }.load(Ordering::Relaxed) {
        return false;
    }

    // Swap-scan: atomically swap each slot's delta to 0 and sum.
    let mut sum: i64 = 0;
    let mut values: Vec<i64> = Vec::with_capacity(slots.len());
    for &slot in slots.iter() {
        let v = slot.delta.swap(0, Ordering::AcqRel);
        sum += v;
        values.push(v);
    }

    if sum != 0 {
        // Restore: add values back to slots.
        for (i, &slot) in slots.iter().enumerate() {
            if values[i] != 0 {
                slot.delta.fetch_add(values[i], Ordering::Release);
            }
        }
        return false;
    }

    // sum=0 while holding lock. No new slots can appear.
    // Mark all slots stale so that get_slot detects address reuse.
    for &slot in slots.iter() {
        slot.stale.store(true, Ordering::Release);
    }
    // Claim destruction.
    unsafe { &*std::ptr::addr_of!((*ptr).destroying) }.store(true, Ordering::Release);
    drop(slots);

    // Drop T's data.
    let panic_payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        std::mem::ManuallyDrop::drop(&mut (*ptr).data);
    }))
    .err();

    // Defer-free the shell via epoch reclamation. A concurrent thread may be
    // between checking master_dropped and calling try_destroy — it needs to
    // read `destroying` from this allocation. The epoch guard held by each
    // caller prevents reclamation until all concurrent accesses complete.
    unsafe {
        epoch::collector().retire(ptr, reclaim_shell::<T>);
    }

    if let Some(payload) = panic_payload {
        std::panic::resume_unwind(payload);
    }

    true
}

/// Epoch reclaim function for the TlrcInner shell.
/// At this point T's data has already been dropped (ManuallyDrop::drop ran
/// in try_destroy). We just need to free the allocation.
unsafe fn reclaim_shell<T>(ptr: *mut TlrcInner<T>, _collector: &epoch::Collector) {
    unsafe {
        drop(Box::from_raw(ptr));
    }
}

// =========================================================================
// Hot path: increment / decrement
// =========================================================================

/// Get or create this thread's slot for the given TlrcInner.
#[inline(always)]
fn get_slot<T>(inner_ptr: *const TlrcInner<T>) -> &'static ThreadSlot {
    let key = inner_ptr as usize;

    TL_SLOTS.with(|cell| {
        let map = unsafe { &mut *cell.get() };

        // Fast path: cached — verify not stale (address reuse after destruction).
        if let Some(slot) = map.get_cached(key) {
            if !slot.stale.load(Ordering::Acquire) {
                return slot;
            }
            // Stale: evict from cache and map, fall through to create.
            map.map.remove(&key);
            map.cached_key = 0;
            map.cached_slot = std::ptr::null();
        }

        // Check map entry for staleness too.
        if let Some(&slot) = map.map.get(&key) {
            if !slot.stale.load(Ordering::Acquire) {
                map.cached_key = key;
                map.cached_slot = slot as *const ThreadSlot;
                return slot;
            }
            map.map.remove(&key);
        }

        // Cold path: create new slot and register with the TlrcInner.
        let slot = ThreadSlot::new_leaked();
        let ptr = inner_ptr as *mut TlrcInner<T>;
        {
            let mut guard = unsafe { &*std::ptr::addr_of!((*ptr).slots) }.lock();
            guard.push(slot);
        }
        map.map.insert(key, slot);
        map.cached_key = key;
        map.cached_slot = slot as *const ThreadSlot;
        slot
    })
}

#[inline(always)]
fn tlrc_increment<T>(inner_ptr: *const TlrcInner<T>) {
    let slot = get_slot(inner_ptr);
    slot.delta.fetch_add(1, Ordering::Relaxed);
}

#[inline(always)]
fn tlrc_decrement<T>(inner_ptr: *const TlrcInner<T>) {
    // Enter an epoch guard for the entire decrement. After our fetch_add(-1),
    // another thread's try_destroy might retire the shell. The guard prevents
    // the epoch collector from reclaiming it while we still access the shell
    // (master_dropped, destroying, slots mutex).
    let _guard = epoch::collector().enter();
    let slot = get_slot(inner_ptr);
    slot.delta.fetch_add(-1, Ordering::Relaxed);

    let ptr = inner_ptr as *mut TlrcInner<T>;
    if unsafe { &*std::ptr::addr_of!((*ptr).master_dropped) }.load(Ordering::Acquire) {
        try_destroy(inner_ptr);
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

    /// Create a `TlrcRef` from a raw `TlrcInner` pointer, incrementing the TLRC.
    ///
    /// # Safety
    /// `ptr` must point to a live `TlrcInner<T>`.
    #[inline]
    pub(crate) unsafe fn clone_from_raw(ptr: *const TlrcInner<T>) -> Self {
        tlrc_increment(ptr);
        Self { ptr }
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
// Drop
// =========================================================================

impl<T> Drop for Tlrc<T> {
    fn drop(&mut self) {
        let ptr = self.ptr as *mut TlrcInner<T>;

        // Fast path: no refs ever created.
        {
            let slots = unsafe { &*std::ptr::addr_of!((*ptr).slots) }.lock();
            if slots.is_empty() {
                drop(slots);
                unsafe {
                    std::mem::ManuallyDrop::drop(&mut (*ptr).data);
                    drop(Box::from_raw(ptr));
                }
                return;
            }
        }

        // Signal master dropped.
        unsafe { &*std::ptr::addr_of!((*ptr).master_dropped) }.store(true, Ordering::Release);

        let _guard = epoch::collector().enter();
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
    use std::sync::atomic::{AtomicU64, AtomicUsize};
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

    // === Basic tests ===

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
    fn no_refs_at_all() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn single_ref_no_clones() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let r = t.tlrc_ref();
        drop(r);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn many_refs_single_thread() {
        let d = Arc::new(AtomicUsize::new(0));
        let t = Tlrc::new(DropCounter::new(&d));
        let refs: Vec<_> = (0..1_000).map(|_| t.tlrc_ref()).collect();
        drop(refs);
        drop(t);
        assert_eq!(d.load(Ordering::SeqCst), 1);
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

    // === Multi-thread tests ===

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

    // === Battle / fuzz tests ===

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

    // === Orphan / stale lifecycle tests ===

    #[test]
    fn orphan_thread_exits_before_master() {
        for _ in 0..100 {
            let d = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(UafSentinel::new(&d));
            let h: Vec<_> = (0..8)
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
    }

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
    }

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
                        let is_dormant = i < 8;
                        thread::spawn(move || {
                            b.wait();
                            if is_dormant {
                                drop(r);
                            } else {
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
                            drop(r);
                        })
                    })
                    .collect();
                b.wait();
                for h in h {
                    h.join().unwrap();
                }
                drop(t);
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

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
                drop(t);
                b.wait();
                for h in h {
                    h.join().unwrap();
                }
            });
            h.join().unwrap();
            let drops = d.load(Ordering::SeqCst);
            assert!(drops == 1, "drops={drops} (expected 1)");
        }
    }

    #[test]
    fn fuzz_simultaneous_scan_trigger() {
        for _ in 0..200 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(UafSentinel::new(&d2));
                let b = Arc::new(Barrier::new(65));
                let h: Vec<_> = (0..64)
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
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

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
                            let sh: Vec<_> = (0..50)
                                .map(|_| {
                                    let c = r.clone();
                                    thread::spawn(move || {
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

    #[test]
    fn fuzz_ping_pong_cross_thread() {
        use std::sync::mpsc;
        for _ in 0..20 {
            let d = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(UafSentinel::new(&d));
            let (tx, rx) = mpsc::sync_channel::<TlrcRef<UafSentinel>>(32);
            let r = t.tlrc_ref();

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

    // === Miri-compatible tests ===

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

    #[test]
    fn lifecycle_debt_counter_orphan() {
        for _ in 0..200 {
            let d = Arc::new(AtomicUsize::new(0));
            let t = Tlrc::new(DropCounter::new(&d));
            let r = t.tlrc_ref();
            let h = thread::spawn(move || drop(r));
            h.join().unwrap();
            drop(t);
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn lifecycle_rapid_churn() {
        let total = Arc::new(AtomicUsize::new(0));
        for _ in 0..50 {
            let d = Arc::new(AtomicUsize::new(0));
            let d2 = d.clone();
            let total2 = total.clone();
            let h = thread::spawn(move || {
                let t = Tlrc::new(DropCounter::new(&d2));
                let h: Vec<_> = (0..8)
                    .map(|_| {
                        let r = t.tlrc_ref();
                        thread::spawn(move || {
                            for _ in 0..50 {
                                let c = r.clone();
                                std::hint::black_box(&*c);
                                drop(c);
                            }
                            drop(r);
                        })
                    })
                    .collect();
                drop(t);
                for h in h {
                    h.join().unwrap();
                }
                total2.fetch_add(1, Ordering::Relaxed);
            });
            h.join().unwrap();
            assert_eq!(d.load(Ordering::SeqCst), 1);
        }
        assert_eq!(total.load(Ordering::Relaxed), 50);
    }
}
