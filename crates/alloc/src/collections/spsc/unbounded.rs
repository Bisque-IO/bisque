//! # Unbounded SPSC Queue
//!
//! An unbounded single-producer single-consumer queue with global monotonic
//! head/tail counters and a linked list of flat segment nodes.
//!
//! # Architecture
//!
//! ```text
//! Global positions (monotonic u64):
//!   head: [63=close bit][62..0=producer position]   (producer writes)
//!   tail: consumer position                          (consumer writes)
//!
//! Node linked list:
//!   Node 0 ──→ Node 1 ──→ Node 2 ──→ ...
//!   [data]     [data]     [data]
//!
//! Each node: flat array of NODE_CAP items (2^(P+NUM_SEGS_P2))
//! Position mapping: node_index = pos / NODE_CAP, offset = pos & NODE_MASK
//! ```
//!
//! The producer advances `head` after writing items. The consumer advances
//! `tail` after reading. Nodes are freed lazily when the consumer crosses
//! a node boundary. Close is encoded in bit 63 of `head`.

use super::{NoOpSignal, PopError, PushError, SignalSchedule};
use crate::CachePadded;
use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

const CLOSED_BIT: u64 = 1u64 << 63;
const POS_MASK: u64 = !CLOSED_BIT;

// ═══════════════════════════════════════════════════════════════════════════
// Node — flat segment with a next pointer
// ═══════════════════════════════════════════════════════════════════════════

struct Node<T> {
    data: *mut MaybeUninit<T>,
    next: AtomicUsize, // 0 = no next, else ptr to next Node
}

impl<T> Node<T> {
    fn alloc(cap: usize) -> *mut Self {
        let data = alloc_data::<T>(cap);
        Box::into_raw(Box::new(Self {
            data,
            next: AtomicUsize::new(0),
        }))
    }
}

fn alloc_data<T>(cap: usize) -> *mut MaybeUninit<T> {
    if core::mem::size_of::<MaybeUninit<T>>() == 0 || cap == 0 {
        return ptr::null_mut();
    }
    unsafe {
        let layout = Layout::array::<MaybeUninit<T>>(cap).unwrap();
        let p = alloc_zeroed(layout) as *mut MaybeUninit<T>;
        if p.is_null() {
            handle_alloc_error(layout);
        }
        p
    }
}

unsafe fn free_data<T>(ptr: *mut MaybeUninit<T>, cap: usize) {
    if ptr.is_null() || core::mem::size_of::<MaybeUninit<T>>() == 0 || cap == 0 {
        return;
    }
    unsafe {
        let layout = Layout::array::<MaybeUninit<T>>(cap).unwrap();
        dealloc(ptr as *mut u8, layout);
    }
}

/// Free a node and its data segment.
unsafe fn free_node<T>(node: *mut Node<T>, cap: usize) {
    unsafe {
        free_data((*node).data, cap);
        drop(Box::from_raw(node));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// UnboundedSpsc
// ═══════════════════════════════════════════════════════════════════════════

pub struct UnboundedSpsc<
    T,
    const P: usize = 6,
    const NUM_SEGS_P2: usize = 8,
    S: SignalSchedule + Clone = NoOpSignal,
> {
    // ── Producer state (only producer writes) ────────────────────────────
    /// Global producer position. Bit 63 = close flag.
    prod_head: CachePadded<AtomicU64>,
    /// Producer's current write node.
    prod_node: UnsafeCell<*mut Node<T>>,

    // ── Consumer state (only consumer writes) ────────────────────────────
    /// Global consumer position.
    cons_tail: CachePadded<AtomicU64>,
    /// Consumer's cached copy of prod_head (avoids cache-line bouncing).
    cons_head_cache: UnsafeCell<u64>,
    /// Consumer's current read node.
    cons_node: UnsafeCell<*mut Node<T>>,

    // ── Shared ───────────────────────────────────────────────────────────
    node_count: AtomicUsize,
    signal: S,
}

unsafe impl<T: Send, const P: usize, const N: usize, S: SignalSchedule + Clone> Send
    for UnboundedSpsc<T, P, N, S>
{
}
unsafe impl<T: Send, const P: usize, const N: usize, S: SignalSchedule + Clone> Sync
    for UnboundedSpsc<T, P, N, S>
{
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    UnboundedSpsc<T, P, NUM_SEGS_P2, S>
{
    /// Items per node. Power of two.
    pub const NODE_CAP: usize = (1usize << P) * (1usize << NUM_SEGS_P2);
    const NODE_MASK: usize = Self::NODE_CAP - 1;

    // ── Construction ─────────────────────────────────────────────────────

    pub fn new() -> (
        UnboundedSender<T, P, NUM_SEGS_P2, S>,
        UnboundedReceiver<T, P, NUM_SEGS_P2, S>,
    )
    where
        S: Default,
    {
        Self::new_with_signal(S::default())
    }

    pub fn new_with_signal(
        signal: S,
    ) -> (
        UnboundedSender<T, P, NUM_SEGS_P2, S>,
        UnboundedReceiver<T, P, NUM_SEGS_P2, S>,
    ) {
        let first = Node::alloc(Self::NODE_CAP);
        let q = Arc::new(Self {
            prod_head: CachePadded::new(AtomicU64::new(0)),
            prod_node: UnsafeCell::new(first),
            cons_tail: CachePadded::new(AtomicU64::new(0)),
            cons_head_cache: UnsafeCell::new(0),
            cons_node: UnsafeCell::new(first),
            node_count: AtomicUsize::new(1),
            signal,
        });
        let tx = UnboundedSender { q: q.clone() };
        let rx = UnboundedReceiver { q };
        (tx, rx)
    }

    /// Create a queue without allocating the first node.
    /// The first push lazily allocates. Ideal for embedding in static
    /// registries where most slots are never used.
    pub(crate) fn new_lazy() -> Self
    where
        S: Default,
    {
        Self {
            prod_head: CachePadded::new(AtomicU64::new(0)),
            prod_node: UnsafeCell::new(ptr::null_mut()),
            cons_tail: CachePadded::new(AtomicU64::new(0)),
            cons_head_cache: UnsafeCell::new(0),
            cons_node: UnsafeCell::new(ptr::null_mut()),
            node_count: AtomicUsize::new(0),
            signal: S::default(),
        }
    }

    // ── Accessors ────────────────────────────────────────────────────────

    pub fn node_count(&self) -> usize {
        self.node_count.load(Ordering::Relaxed)
    }

    pub fn total_capacity(&self) -> usize {
        self.node_count.load(Ordering::Relaxed) * Self::NODE_CAP
    }

    pub fn len(&self) -> usize {
        let h = self.prod_head.load(Ordering::Acquire) & POS_MASK;
        let t = self.cons_tail.load(Ordering::Acquire);
        h.saturating_sub(t) as usize
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn create_receiver(self: &Arc<Self>) -> UnboundedReceiver<T, P, NUM_SEGS_P2, S> {
        UnboundedReceiver {
            q: Arc::clone(self),
        }
    }

    // ── Producer internals ───────────────────────────────────────────────

    /// Push a slice of items. Returns number pushed (== src.len() unless closed).
    /// Items are bitwise-copied; caller must not drop the originals.
    pub(crate) fn push_n(&self, src: &[T]) -> Result<usize, PushError<()>> {
        let h = self.prod_head.load(Ordering::Relaxed);
        if h & CLOSED_BIT != 0 {
            return Err(PushError::Closed(()));
        }
        let mut h_pos = (h & POS_MASK) as usize;
        let to_push = src.len();
        let mut pushed = 0usize;

        while pushed < to_push {
            let offset = h_pos & Self::NODE_MASK;

            // Need a new node? (first push on lazy queue, or crossed boundary)
            if offset == 0 {
                let needs_alloc = unsafe { (*self.prod_node.get()).is_null() } || h_pos > 0;
                if needs_alloc {
                    let new_node = Node::alloc(Self::NODE_CAP);
                    unsafe {
                        let old = *self.prod_node.get();
                        if !old.is_null() {
                            // Link old → new. Release ensures prior data writes
                            // are visible before consumer follows the pointer.
                            (*old).next.store(new_node as usize, Ordering::Release);
                        } else {
                            // First node — set consumer's node too. Safe because no
                            // consumer can run until we publish head > 0 below.
                            *self.cons_node.get() = new_node;
                        }
                        *self.prod_node.get() = new_node;
                    }
                    self.node_count.fetch_add(1, Ordering::Relaxed);
                }
            }

            let node = unsafe { *self.prod_node.get() };
            let can = (Self::NODE_CAP - offset).min(to_push - pushed);

            unsafe {
                let dst = (*node).data.add(offset) as *mut T;
                let s = (src.as_ptr()).add(pushed);
                ptr::copy_nonoverlapping(s, dst, can);
            }

            h_pos += can;
            pushed += can;
        }

        // Publish all writes at once.
        self.prod_head.store(h_pos as u64, Ordering::Release);
        self.signal.schedule();
        Ok(pushed)
    }

    // ── Consumer internals ───────────────────────────────────────────────

    /// Refresh the consumer's cached head. Returns (h_pos, is_closed).
    #[inline(always)]
    fn refresh_head_cache(&self) -> (u64, bool) {
        let h = self.prod_head.load(Ordering::Acquire);
        unsafe { *self.cons_head_cache.get() = h }
        ((h & POS_MASK), h & CLOSED_BIT != 0)
    }

    /// Returns (available_items, is_closed).
    #[inline(always)]
    fn consumer_avail(&self) -> (usize, bool) {
        let t = self.cons_tail.load(Ordering::Relaxed);
        let h = unsafe { *self.cons_head_cache.get() };
        let h_pos = h & POS_MASK;
        let avail = h_pos.saturating_sub(t) as usize;
        if avail > 0 {
            return (avail, false);
        }
        // Refresh
        let (h_pos, closed) = self.refresh_head_cache();
        let avail = h_pos.saturating_sub(t) as usize;
        (avail, closed && avail == 0)
    }

    /// Advance consumer node if tail crossed a boundary.
    /// Called BEFORE reading from the new position.
    ///
    /// # Safety: only the consumer thread may call this.
    #[inline(always)]
    unsafe fn maybe_advance_cons_node(&self, tail: u64) {
        let offset = (tail as usize) & Self::NODE_MASK;
        if offset == 0 && tail > 0 {
            let old = *self.cons_node.get();
            let next = (*old).next.load(Ordering::Acquire) as *mut Node<T>;
            debug_assert!(!next.is_null(), "consumer advanced past linked nodes");
            *self.cons_node.get() = next;
            free_node(old, Self::NODE_CAP);
            self.node_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Push a single item. Bitwise-copies and forgets the original.
    pub(crate) fn push_one(&self, item: T) -> Result<(), PushError<T>> {
        match self.push_n(core::slice::from_ref(&item)) {
            Ok(_) => {
                core::mem::forget(item);
                Ok(())
            }
            Err(PushError::Closed(())) => Err(PushError::Closed(item)),
            Err(PushError::Full(())) => Err(PushError::Full(item)),
        }
    }

    /// Pop up to `dst.len()` items. Core consumer path.
    pub(crate) fn pop_n(&self, dst: &mut [T]) -> Result<usize, PopError> {
        let (avail, closed) = self.consumer_avail();
        if avail == 0 {
            return if closed {
                Err(PopError::Closed)
            } else {
                Err(PopError::Empty)
            };
        }

        let mut t = self.cons_tail.load(Ordering::Relaxed);
        let n = dst.len().min(avail);
        let mut copied = 0usize;

        while copied < n {
            unsafe { self.maybe_advance_cons_node(t) };

            let offset = (t as usize) & Self::NODE_MASK;
            let node = unsafe { *self.cons_node.get() };
            let can = (Self::NODE_CAP - offset).min(n - copied);

            unsafe {
                let s = (*node).data.add(offset) as *const T;
                let d = dst.as_mut_ptr().add(copied);
                ptr::copy_nonoverlapping(s, d, can);
            }

            t += can as u64;
            copied += can;
        }

        self.cons_tail.store(t, Ordering::Release);
        Ok(copied)
    }

    /// Zero-copy consume: invoke `f` with slices of items in-place.
    pub(crate) fn consume_in_place_inner<F>(&self, max: usize, mut f: F) -> usize
    where
        F: FnMut(&[T]) -> usize,
    {
        if max == 0 {
            return 0;
        }
        let (avail, _closed) = self.consumer_avail();
        if avail == 0 {
            return 0;
        }

        let mut t = self.cons_tail.load(Ordering::Relaxed);
        let total = max.min(avail);
        let mut consumed = 0usize;

        while consumed < total {
            unsafe { self.maybe_advance_cons_node(t) };

            let offset = (t as usize) & Self::NODE_MASK;
            let node = unsafe { *self.cons_node.get() };
            let can = (Self::NODE_CAP - offset).min(total - consumed);

            let ate = unsafe {
                let base = (*node).data.add(offset) as *const T;
                let slice = core::slice::from_raw_parts(base, can);
                f(slice)
            };

            let ate = ate.min(can);
            t += ate as u64;
            consumed += ate;

            if ate < can {
                break; // callback consumed fewer than offered
            }
        }

        self.cons_tail.store(t, Ordering::Release);
        consumed
    }

    /// Close the channel by setting bit 63 in head.
    pub(crate) fn close(&self) {
        self.prod_head.fetch_or(CLOSED_BIT, Ordering::Release);
        self.signal.schedule();
    }
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Drop
    for UnboundedSpsc<T, P, NUM_SEGS_P2, S>
{
    fn drop(&mut self) {
        // Free the entire node list starting from the consumer's current node.
        let mut ptr = unsafe { *self.cons_node.get() };
        while !ptr.is_null() {
            unsafe {
                let next = (*ptr).next.load(Ordering::Relaxed);
                free_node(ptr, Self::NODE_CAP);
                ptr = if next != 0 {
                    next as *mut Node<T>
                } else {
                    ptr::null_mut()
                };
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Sender
// ═══════════════════════════════════════════════════════════════════════════

pub struct UnboundedSender<
    T,
    const P: usize = 6,
    const NUM_SEGS_P2: usize = 8,
    S: SignalSchedule + Clone = NoOpSignal,
> {
    q: Arc<UnboundedSpsc<T, P, NUM_SEGS_P2, S>>,
}

impl<T, const P: usize, const N: usize, S: SignalSchedule + Clone> Clone
    for UnboundedSender<T, P, N, S>
{
    fn clone(&self) -> Self {
        Self { q: self.q.clone() }
    }
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    UnboundedSender<T, P, NUM_SEGS_P2, S>
{
    pub fn unbounded_arc(&self) -> Arc<UnboundedSpsc<T, P, NUM_SEGS_P2, S>> {
        Arc::clone(&self.q)
    }

    pub fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        match self.q.push_n(core::slice::from_ref(&item)) {
            Ok(_) => {
                core::mem::forget(item);
                Ok(())
            }
            Err(PushError::Closed(())) => Err(PushError::Closed(item)),
            Err(PushError::Full(())) => Err(PushError::Full(item)),
        }
    }

    pub fn try_push_n(&self, items: &mut Vec<T>) -> Result<usize, PushError<()>> {
        if items.is_empty() {
            return Ok(0);
        }
        let n = self.q.push_n(items.as_slice())?;
        // Items were bitwise-copied into the queue. Clear without dropping.
        unsafe { items.set_len(0) };
        Ok(n)
    }

    pub fn node_count(&self) -> usize {
        self.q.node_count()
    }

    pub fn total_capacity(&self) -> usize {
        self.q.total_capacity()
    }

    pub fn len(&self) -> usize {
        self.q.len()
    }

    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }

    pub fn close_channel(&self) {
        self.q.close();
    }
}

impl<T, const P: usize, const N: usize, S: SignalSchedule + Clone> Drop
    for UnboundedSender<T, P, N, S>
{
    fn drop(&mut self) {
        self.close_channel();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Receiver
// ═══════════════════════════════════════════════════════════════════════════

pub struct UnboundedReceiver<
    T,
    const P: usize = 6,
    const NUM_SEGS_P2: usize = 8,
    S: SignalSchedule + Clone = NoOpSignal,
> {
    q: Arc<UnboundedSpsc<T, P, NUM_SEGS_P2, S>>,
}

impl<T, const P: usize, const N: usize, S: SignalSchedule + Clone> Clone
    for UnboundedReceiver<T, P, N, S>
{
    fn clone(&self) -> Self {
        Self { q: self.q.clone() }
    }
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    UnboundedReceiver<T, P, NUM_SEGS_P2, S>
{
    /// Try to pop a single item.
    ///
    /// - `Ok(item)` — dequeued
    /// - `Err(PopError::Empty)` — empty, items may arrive later
    /// - `Err(PopError::Closed)` — channel closed and fully drained
    pub fn try_pop(&self) -> Result<T, PopError> {
        let mut tmp: T = unsafe { core::mem::zeroed() };
        match self.q.pop_n(core::slice::from_mut(&mut tmp)) {
            Ok(1) => Ok(tmp),
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }

    /// Try to pop multiple items into `dst`.
    pub fn try_pop_n(&self, dst: &mut [T]) -> Result<usize, PopError> {
        self.q.pop_n(dst)
    }

    /// Zero-copy consumption via callback.
    pub fn consume_in_place<F>(&self, max: usize, f: F) -> usize
    where
        F: FnMut(&[T]) -> usize,
    {
        self.q.consume_in_place_inner(max, f)
    }

    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }

    pub fn len(&self) -> usize {
        self.q.len()
    }

    /// Best-effort closed check. Prefer `try_pop()` returning
    /// `Err(PopError::Closed)` as the authoritative signal.
    pub fn is_closed(&self) -> bool {
        self.q.prod_head.load(Ordering::Acquire) & CLOSED_BIT != 0
    }

    pub fn node_count(&self) -> usize {
        self.q.node_count()
    }

    pub fn total_capacity(&self) -> usize {
        self.q.total_capacity()
    }

    pub fn mark(&self) {
        self.q.signal.mark();
    }

    pub fn unmark(&self) {
        self.q.signal.unmark();
    }

    pub fn unmark_and_schedule(&self) {
        self.q.signal.unmark_and_schedule();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_unbounded_spsc() {
        type Q = UnboundedSpsc<u64>;
        let (tx, rx) = Q::new();

        assert!(tx.try_push(42).is_ok());
        assert_eq!(rx.try_pop(), Ok(42));
        assert!(rx.try_pop().is_err());
    }

    #[test]
    fn test_unbounded_growth() {
        // P=2, NUM_SEGS_P2=2 → NODE_CAP = 4*4 = 16
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        for i in 0..16 {
            assert!(tx.try_push(i).is_ok());
        }
        // Still in first node
        assert_eq!(tx.node_count(), 1);

        // 17th item triggers second node
        assert!(tx.try_push(16).is_ok());
        assert_eq!(tx.node_count(), 2);

        for i in 0..17 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
        assert!(rx.try_pop().is_err());
    }

    #[test]
    fn test_multiple_nodes_traversal() {
        // P=1, NUM_SEGS_P2=1 → NODE_CAP = 2*2 = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        for i in 0..10 {
            assert!(tx.try_push(i).is_ok());
        }
        assert!(tx.node_count() > 1);

        for i in 0..10 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
    }

    #[test]
    fn test_try_push_n_unbounded() {
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        let mut items: Vec<u64> = (0..10).collect();
        let pushed = tx.try_push_n(&mut items).unwrap();
        assert_eq!(pushed, 10);
        assert!(items.is_empty());

        for i in 0..10 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
    }

    #[test]
    fn test_consumer_advancement() {
        // NODE_CAP = 16
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        for i in 0..17 {
            assert!(tx.try_push(i).is_ok());
        }
        assert_eq!(tx.node_count(), 2);

        for i in 0..17 {
            assert_eq!(rx.try_pop(), Ok(i));
        }

        // After draining past boundary, old node freed
        // (node freed lazily on next pop crossing boundary)
        for i in 17..20 {
            assert!(tx.try_push(i).is_ok());
        }
        for i in 17..20 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
    }

    #[test]
    fn test_concurrent_operations() {
        type Q = UnboundedSpsc<u64>;
        let (tx, rx) = Q::new();

        let producer = {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..1000 {
                    while tx.try_push(i).is_err() {
                        thread::sleep(Duration::from_micros(1));
                    }
                }
            })
        };

        let consumer = {
            let rx = rx.clone();
            thread::spawn(move || {
                let mut received = Vec::new();
                while received.len() < 1000 {
                    if let Ok(item) = rx.try_pop() {
                        received.push(item);
                    }
                    thread::sleep(Duration::from_micros(1));
                }
                received
            })
        };

        producer.join().unwrap();
        let received = consumer.join().unwrap();
        assert_eq!(received.len(), 1000);
        for (i, &v) in received.iter().enumerate() {
            assert_eq!(v, i as u64);
        }
    }

    #[test]
    fn test_statistics_tracking() {
        // NODE_CAP = 16
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, _rx) = Q::new();

        assert_eq!(tx.node_count(), 1);
        assert_eq!(tx.total_capacity(), 16);

        for i in 0..17 {
            assert!(tx.try_push(i).is_ok());
        }
        assert_eq!(tx.node_count(), 2);
        assert_eq!(tx.total_capacity(), 32);
    }

    #[test]
    fn test_consume_in_place_unbounded() {
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        for i in 0..10 {
            assert!(tx.try_push(i).is_ok());
        }

        let sum = Arc::new(Mutex::new(0u64));
        let s = sum.clone();
        let consumed = rx.consume_in_place(10, |chunk| {
            let mut total = s.lock().unwrap();
            for &item in chunk {
                *total += item;
            }
            chunk.len()
        });

        assert_eq!(consumed, 10);
        assert_eq!(*sum.lock().unwrap(), (0..10).sum::<u64>());
    }

    #[test]
    fn test_memory_dropping_different_nodes() {
        // NODE_CAP = 16
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        for i in 0..50 {
            assert!(tx.try_push(i).is_ok());
        }
        assert!(tx.node_count() > 1);
        let initial = tx.node_count();

        // Drain past first node boundary
        for i in 0..20 {
            assert_eq!(rx.try_pop(), Ok(i));
        }

        // Push more
        for i in 50..60 {
            assert!(tx.try_push(i).is_ok());
        }

        // Continue consuming
        for i in 20..25 {
            assert_eq!(rx.try_pop(), Ok(i));
        }

        // Nodes should have been freed
        assert!(tx.node_count() < initial);
    }

    #[test]
    fn test_producer_consumer_position_divergence() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        for i in 0..30 {
            assert!(tx.try_push(i).is_ok());
        }

        for i in 0..9 {
            assert_eq!(rx.try_pop(), Ok(i));
        }

        for i in 30..33 {
            assert!(tx.try_push(i).is_ok());
        }

        for i in 9..12 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
    }

    #[test]
    fn test_channel_close_with_divergent_positions() {
        // NODE_CAP = 16
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        for i in 0..40 {
            assert!(tx.try_push(i).is_ok());
        }
        for i in 0..20 {
            assert_eq!(rx.try_pop(), Ok(i));
        }

        tx.close_channel();

        for i in 20..40 {
            assert_eq!(rx.try_pop(), Ok(i));
        }

        assert_eq!(rx.try_pop(), Err(PopError::Closed));
        assert!(rx.is_closed());
    }

    #[test]
    fn test_large_node_count_stress() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        let num_items = 1000u64;
        for i in 0..num_items {
            assert!(tx.try_push(i).is_ok());
        }
        assert!(tx.node_count() > 100);

        for i in 0..num_items {
            assert_eq!(rx.try_pop(), Ok(i));
        }
        assert!(rx.try_pop().is_err());
    }

    #[test]
    fn test_memory_pressure_with_rapid_producer_consumer() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        for i in 0..200u64 {
            assert!(tx.try_push(i).is_ok());
        }

        let mut consumed = Vec::new();
        for _ in 0..150 {
            if let Ok(item) = rx.try_pop() {
                consumed.push(item);
            }
        }
        assert_eq!(consumed.len(), 150);
        for (i, &v) in consumed.iter().enumerate() {
            assert_eq!(v, i as u64);
        }

        for i in 200..250u64 {
            assert!(tx.try_push(i).is_ok());
        }

        for _ in 0..100 {
            if let Ok(item) = rx.try_pop() {
                consumed.push(item);
            }
        }
        assert_eq!(consumed.len(), 250);
        for (i, &v) in consumed.iter().enumerate() {
            assert_eq!(v, i as u64);
        }
    }

    #[test]
    fn test_consumer_abandoning_nodes() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        for i in 0..50u64 {
            assert!(tx.try_push(i).is_ok());
        }
        for i in 0..10 {
            assert_eq!(rx.try_pop(), Ok(i));
        }

        drop(rx);

        // Sender can still push
        for i in 50..60u64 {
            assert!(tx.try_push(i).is_ok());
        }
    }

    #[test]
    fn test_interleaved_producer_consumer_stress() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();
        let mut expected = 0u64;

        for round in 0..20u64 {
            for i in 0..5 {
                assert!(tx.try_push(round * 5 + i).is_ok());
            }
            for _ in 0..3 {
                if let Ok(item) = rx.try_pop() {
                    assert_eq!(item, expected);
                    expected += 1;
                }
            }
        }

        while let Ok(item) = rx.try_pop() {
            assert_eq!(item, expected);
            expected += 1;
        }
        assert_eq!(expected, 100);
    }

    #[test]
    fn test_node_count_accuracy() {
        // NODE_CAP = 16
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        assert_eq!(tx.node_count(), 1);

        // Fill first node
        for i in 0..16u64 {
            assert_eq!(tx.node_count(), 1);
            assert!(tx.try_push(i).is_ok());
        }
        assert_eq!(tx.node_count(), 1);

        // 17th → second node
        assert!(tx.try_push(16).is_ok());
        assert_eq!(tx.node_count(), 2);

        // Fill second node (items 17..32)
        for i in 17..32u64 {
            assert!(tx.try_push(i).is_ok());
        }
        assert_eq!(tx.node_count(), 2);

        // 33rd → third node
        assert!(tx.try_push(32).is_ok());
        assert_eq!(tx.node_count(), 3);

        // Consume 10 items (still in first node)
        for i in 0..10u64 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
        assert_eq!(tx.node_count(), 3);
    }

    #[test]
    fn test_try_push_n_with_node_creation() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        let mut batch: Vec<u64> = (0..4).collect();
        let pushed = tx.try_push_n(&mut batch).unwrap();
        assert_eq!(pushed, 4);
        assert_eq!(tx.node_count(), 1);

        let mut batch2: Vec<u64> = (4..11).collect();
        let pushed = tx.try_push_n(&mut batch2).unwrap();
        assert_eq!(pushed, 7);
        assert!(tx.node_count() > 1);

        for i in 0..11u64 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
    }

    #[test]
    fn test_consume_in_place_across_nodes() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        for i in 0..10u64 {
            assert!(tx.try_push(i).is_ok());
        }

        let sum = Arc::new(Mutex::new(0u64));
        let s = sum.clone();
        let consumed = rx.consume_in_place(10, |chunk| {
            let mut total = s.lock().unwrap();
            for &item in chunk {
                *total += item;
            }
            chunk.len()
        });

        assert_eq!(consumed, 10);
        assert_eq!(*sum.lock().unwrap(), (0..10).sum::<u64>());
        assert!(rx.try_pop().is_err());
    }

    #[test]
    fn test_try_push_n_performance() {
        // NODE_CAP = 16
        type Q = UnboundedSpsc<u64, 2, 2>;
        let (tx, rx) = Q::new();

        let mut batch: Vec<u64> = (0..100).collect();
        let pushed = tx.try_push_n(&mut batch).unwrap();
        assert_eq!(pushed, 100);

        for i in 0..100u64 {
            assert_eq!(rx.try_pop(), Ok(i));
        }
        assert!(tx.node_count() >= 1);
    }

    #[test]
    fn test_close_returns_closed_error() {
        type Q = UnboundedSpsc<u64>;
        let (tx, rx) = Q::new();

        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();
        drop(tx); // close

        assert_eq!(rx.try_pop(), Ok(1));
        assert_eq!(rx.try_pop(), Ok(2));
        assert_eq!(rx.try_pop(), Err(PopError::Closed));
    }

    #[test]
    fn test_push_after_close() {
        type Q = UnboundedSpsc<u64>;
        let (tx, _rx) = Q::new();

        tx.close_channel();
        assert!(matches!(tx.try_push(42), Err(PushError::Closed(42))));
    }

    #[test]
    fn test_pop_n_across_nodes() {
        // NODE_CAP = 4
        type Q = UnboundedSpsc<u64, 1, 1>;
        let (tx, rx) = Q::new();

        for i in 0..10u64 {
            tx.try_push(i).unwrap();
        }

        let mut buf = [0u64; 10];
        let n = rx.try_pop_n(&mut buf).unwrap();
        assert_eq!(n, 10);
        for i in 0..10 {
            assert_eq!(buf[i], i as u64);
        }
    }
}
