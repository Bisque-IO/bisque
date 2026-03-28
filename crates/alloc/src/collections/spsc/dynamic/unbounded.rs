//! Runtime-configured unbounded SPSC queue.
//!
//! This module provides an unbounded variant of the dynamically-configured SPSC queue.
//! It uses a linked list of `DynSpsc` segments that grow automatically when needed.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                 Dynamic Unbounded Spsc Structure                │
//! │                                                                 │
//! │  Head (Atomic) ──→ Node 0 ──→ Node 1 ──→ Node 2 ──→ ...         │
//! │       │                 │           │           │               │
//! │       │            [DynSpsc]    [DynSpsc]   [DynSpsc]           │
//! │       │          (config-sized) (config-sized) (config-sized)   │
//! │       │                                                         │
//! │  Tail (Cached) ──→ Current Node for Producer                    │
//! │                                                                 │
//! │  Consumer automatically progresses through nodes                │
//! │  Producer creates new nodes when current tail is full           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Trade-offs vs Const Generic Unbounded Version
//!
//! | Aspect | `UnboundedSpsc<T, P, NUM_SEGS_P2, S>` | `DynUnboundedSpsc<T, S>` |
//! |--------|---------------------------------------|--------------------------|
//! | Configuration | Compile-time | Runtime |
//! | Performance | Optimal (constants inlined) | Slightly slower (runtime loads) |
//! | Binary size | Larger (monomorphization) | Smaller |
//! | Flexibility | Fixed at compile time | Configurable |
//!
//! # Example
//!
//! ```ignore
//! use maniac::spsc::dyn_unbounded::{DynUnboundedSpsc, DynUnboundedSender, DynUnboundedReceiver};
//! use maniac::spsc::dynamic::DynSpscConfig;
//! use maniac::spsc::NoOpSignal;
//!
//! // Create unbounded queue with runtime-configured segment size
//! let config = DynSpscConfig::new(6, 8); // 64 items/segment, 256 segments per node
//! let (sender, receiver) = DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(config);
//!
//! // Push items - will automatically create new nodes as needed
//! for i in 0..100000 {
//!     sender.try_push(i).unwrap();
//! }
//!
//! // Pop items
//! for i in 0..100000 {
//!     assert_eq!(receiver.try_pop(), Some(i));
//! }
//! ```

use crate::collections::spsc::dynamic::{DynSpsc, DynSpscConfig};
use crate::collections::spsc::{NoOpSignal, PopError, PushError, SignalSchedule};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Intrusive node containing a DynSpsc segment
struct Node<T, S: SignalSchedule> {
    /// The underlying DynSpsc queue for this segment
    queue: DynSpsc<T, S>,

    /// Next node in the linked list (atomic for thread-safe access)
    next: AtomicUsize,

    /// Flag indicating if this node is currently full
    is_full: AtomicUsize,

    /// Phantom data to ensure proper variance
    _phantom: PhantomData<T>,
}

impl<T, S: SignalSchedule + Clone> Node<T, S> {
    /// Create a new node with an empty DynSpsc
    fn new(config: DynSpscConfig, signal: Arc<S>) -> *mut Self {
        let node = Box::new(Self {
            queue: unsafe { DynSpsc::new_unsafe(config, (*signal).clone()) },
            next: AtomicUsize::new(0),
            is_full: AtomicUsize::new(0),
            _phantom: PhantomData,
        });
        Box::into_raw(node)
    }
}

/// Dynamic unbounded SPSC implementation
pub struct DynUnboundedSpsc<T, S: SignalSchedule + Clone = NoOpSignal> {
    /// Runtime configuration for each segment
    config: DynSpscConfig,

    /// Head of the linked list (atomic for consumer access)
    head: Arc<AtomicUsize>,

    /// Cached tail pointer for producer (avoids atomic operations in common case)
    current_tail: UnsafeCell<*mut Node<T, S>>,

    /// Total number of nodes in the linked list
    node_count: AtomicUsize,

    /// Total capacity across all nodes
    total_capacity: AtomicUsize,

    /// Signal scheduler for creating new nodes
    signal: Arc<S>,
}

unsafe impl<T, S: SignalSchedule + Clone> Send for DynUnboundedSpsc<T, S> {}
unsafe impl<T, S: SignalSchedule + Clone> Sync for DynUnboundedSpsc<T, S> {}

impl<T, S: SignalSchedule + Clone> DynUnboundedSpsc<T, S> {
    /// Create a new unbounded DynSpsc with default configuration
    pub fn new() -> (DynUnboundedSender<T, S>, DynUnboundedReceiver<T, S>)
    where
        S: Default,
    {
        Self::new_with_signal(DynSpscConfig::new(6, 8), S::default())
    }

    /// Create a new unbounded DynSpsc with custom configuration
    pub fn new_with_config(
        config: DynSpscConfig,
    ) -> (DynUnboundedSender<T, S>, DynUnboundedReceiver<T, S>)
    where
        S: Default,
    {
        Self::new_with_signal(config, S::default())
    }

    /// Create a new unbounded DynSpsc with a custom signal scheduler
    pub fn new_with_signal(
        config: DynSpscConfig,
        signal: S,
    ) -> (DynUnboundedSender<T, S>, DynUnboundedReceiver<T, S>) {
        let signal_arc = Arc::new(signal);
        // Create the first node
        let first_node = Node::new(config, signal_arc.clone());

        let unbounded = Arc::new(Self {
            config,
            head: Arc::new(AtomicUsize::new(first_node as usize)),
            current_tail: UnsafeCell::new(first_node),
            node_count: AtomicUsize::new(1),
            total_capacity: AtomicUsize::new(config.capacity()),
            signal: signal_arc,
        });

        let sender = DynUnboundedSender {
            unbounded: unbounded.clone(),
        };

        let receiver = DynUnboundedReceiver { unbounded };

        (sender, receiver)
    }

    /// Get the runtime configuration
    pub fn config(&self) -> DynSpscConfig {
        self.config
    }

    /// Get the total number of nodes in the linked list
    pub fn node_count(&self) -> usize {
        self.node_count.load(Ordering::Relaxed)
    }

    /// Get the total capacity across all nodes
    pub fn total_capacity(&self) -> usize {
        self.total_capacity.load(Ordering::Relaxed)
    }

    /// Get the current length (approximate, as it requires traversing nodes)
    pub fn len(&self) -> usize {
        let mut len = 0;
        let mut current = self.head.load(Ordering::Acquire) as *mut Node<T, S>;

        while !current.is_null() {
            unsafe {
                len += (*current).queue.len();
                current = (*current).next.load(Ordering::Acquire) as *mut Node<T, S>;
            }
        }

        len
    }

    /// Check if the queue is empty (approximate)
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head_node_ptr = self.head.load(Ordering::Acquire) as *mut Node<T, S>;
        if head_node_ptr.is_null() {
            return true;
        }

        unsafe { (*head_node_ptr).queue.is_empty() }
    }

    /// Create a receiver from an Arc<DynUnboundedSpsc>
    pub fn create_receiver(self: &Arc<Self>) -> DynUnboundedReceiver<T, S> {
        DynUnboundedReceiver {
            unbounded: Arc::clone(self),
        }
    }

    /// Close the queue
    pub fn close(&self) {
        let mut current = self.head.load(Ordering::Acquire) as *mut Node<T, S>;

        while !current.is_null() {
            unsafe {
                (*current).queue.close();
                current = (*current).next.load(Ordering::Acquire) as *mut Node<T, S>;
            }
        }
    }

    /// Internal: try to push to the current tail node
    #[inline(always)]
    fn try_push_to_current(&self, item: T) -> Result<(), PushError<T>> {
        unsafe {
            let current_tail = *self.current_tail.get();
            if current_tail.is_null() {
                return Err(PushError::Full(item));
            }

            (*current_tail).queue.try_push(item)
        }
    }

    /// Internal: create a new node and link it
    #[inline(always)]
    fn create_new_node(&self) -> *mut Node<T, S> {
        let new_node = Node::new(self.config, self.signal.clone());

        // Link the new node to the current tail
        unsafe {
            let current_tail = *self.current_tail.get();
            if !current_tail.is_null() {
                (*current_tail)
                    .next
                    .store(new_node as usize, Ordering::Release);
            }
        }

        // Update cached tail
        unsafe {
            *self.current_tail.get() = new_node;
        }

        // Update statistics
        self.node_count.fetch_add(1, Ordering::Relaxed);
        self.total_capacity
            .fetch_add(self.config.capacity(), Ordering::Relaxed);

        new_node
    }

    /// Internal: advance consumer to next node if current is empty and closed
    #[inline(always)]
    fn try_advance_consumer(&self) -> bool {
        unsafe {
            let current_head_ptr = self.head.load(Ordering::Acquire) as *mut Node<T, S>;
            if current_head_ptr.is_null() {
                return false;
            }

            let current_head = &*current_head_ptr;

            // If current head is empty and closed, advance to next
            if current_head.queue.is_empty() && current_head.queue.is_closed() {
                let next_head = current_head.next.load(Ordering::Acquire) as *mut Node<T, S>;
                if !next_head.is_null() {
                    self.head.store(next_head as usize, Ordering::Release);
                    return true;
                }
            }

            false
        }
    }
}

impl<T, S: SignalSchedule + Clone + Default> Default for DynUnboundedSpsc<T, S> {
    fn default() -> Self {
        // This is a bit awkward since we return a tuple normally
        // We'll create the internal structure directly
        let config = DynSpscConfig::new(6, 8);
        let signal = Arc::new(S::default());
        let first_node = Node::new(config, signal.clone());

        Self {
            config,
            head: Arc::new(AtomicUsize::new(first_node as usize)),
            current_tail: UnsafeCell::new(first_node),
            node_count: AtomicUsize::new(1),
            total_capacity: AtomicUsize::new(config.capacity()),
            signal,
        }
    }
}

/// Sender half of the dynamic unbounded SPSC
pub struct DynUnboundedSender<T, S: SignalSchedule + Clone = NoOpSignal> {
    unbounded: Arc<DynUnboundedSpsc<T, S>>,
}

impl<T, S: SignalSchedule + Clone> Clone for DynUnboundedSender<T, S> {
    fn clone(&self) -> Self {
        Self {
            unbounded: self.unbounded.clone(),
        }
    }
}

impl<T, S: SignalSchedule + Clone> DynUnboundedSender<T, S> {
    /// Get a clone of the Arc<DynUnboundedSpsc>
    pub fn unbounded_arc(&self) -> Arc<DynUnboundedSpsc<T, S>> {
        Arc::clone(&self.unbounded)
    }

    /// Get the runtime configuration
    pub fn config(&self) -> DynSpscConfig {
        self.unbounded.config()
    }

    /// Try to push an item to the queue
    pub fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        // Try the current tail first (fast path)
        match self.unbounded.try_push_to_current(item) {
            Ok(()) => Ok(()),
            Err(PushError::Full(item)) => {
                // Current tail is full, create a new node
                let _new_node = self.unbounded.create_new_node();

                // Try again with the new tail
                match self.unbounded.try_push_to_current(item) {
                    Ok(()) => Ok(()),
                    Err(PushError::Full(item)) => Err(PushError::Full(item)),
                    Err(PushError::Closed(item)) => Err(PushError::Closed(item)),
                }
            }
            Err(PushError::Closed(item)) => Err(PushError::Closed(item)),
        }
    }

    /// Try to push multiple items to the queue using bulk operations
    ///
    /// This method uses the underlying DynSpsc's bulk try_push_n for efficient batch operations.
    /// Items are moved out of the Vec using unsafe pointer operations to avoid Clone/Copy bounds.
    ///
    /// The Vec is drained as items are successfully pushed. On return, the Vec contains only
    /// items that were not pushed (if any).
    pub fn try_push_n(&self, items: &mut Vec<T>) -> Result<usize, PushError<()>> {
        if items.is_empty() {
            return Ok(0);
        }

        let original_len = items.len();
        let mut total_pushed = 0;

        unsafe {
            let src_ptr = items.as_ptr();

            while total_pushed < original_len {
                let current_tail = *self.unbounded.current_tail.get();
                if current_tail.is_null() {
                    // Remove the items we successfully pushed from the Vec
                    if total_pushed > 0 {
                        let remaining = original_len - total_pushed;
                        std::ptr::copy(src_ptr.add(total_pushed), items.as_mut_ptr(), remaining);
                        items.set_len(remaining);
                    }
                    return Err(PushError::Closed(()));
                }

                // Create a slice from the remaining items
                let remaining_slice = std::slice::from_raw_parts(
                    src_ptr.add(total_pushed),
                    original_len - total_pushed,
                );

                // Try to push the remaining items using bulk operation
                match (*current_tail).queue.try_push_n(remaining_slice) {
                    Ok(n) => {
                        total_pushed += n;

                        // If we pushed all remaining items, we're done
                        if total_pushed == original_len {
                            items.set_len(0);
                            return Ok(total_pushed);
                        }

                        // Current node is full, create a new one
                        let _new_node = self.unbounded.create_new_node();
                    }
                    Err(PushError::Full(())) => {
                        // Queue is full but not closed, create a new node
                        let _new_node = self.unbounded.create_new_node();
                    }
                    Err(PushError::Closed(())) => {
                        // Remove the items we successfully pushed
                        if total_pushed > 0 {
                            let remaining = original_len - total_pushed;
                            std::ptr::copy(
                                src_ptr.add(total_pushed),
                                items.as_mut_ptr(),
                                remaining,
                            );
                            items.set_len(remaining);
                        }
                        return Err(PushError::Closed(()));
                    }
                }
            }

            items.set_len(0);
            Ok(total_pushed)
        }
    }

    /// Get the number of nodes in the unbounded queue
    pub fn node_count(&self) -> usize {
        self.unbounded.node_count()
    }

    /// Get the total capacity across all nodes
    pub fn total_capacity(&self) -> usize {
        self.unbounded.total_capacity()
    }

    /// Get the current length (approximate)
    pub fn len(&self) -> usize {
        self.unbounded.len()
    }

    /// Check if the queue is empty (approximate)
    pub fn is_empty(&self) -> bool {
        self.unbounded.is_empty()
    }

    /// Close the channel for sending
    pub fn close_channel(&self) {
        unsafe {
            let current_tail = *self.unbounded.current_tail.get();
            if !current_tail.is_null() {
                (*current_tail).queue.close();
            }
        }
    }
}

impl<T, S: SignalSchedule + Clone> Drop for DynUnboundedSender<T, S> {
    fn drop(&mut self) {
        self.close_channel();
    }
}

/// Receiver half of the dynamic unbounded SPSC
pub struct DynUnboundedReceiver<T, S: SignalSchedule + Clone = NoOpSignal> {
    unbounded: Arc<DynUnboundedSpsc<T, S>>,
}

impl<T, S: SignalSchedule + Clone> Clone for DynUnboundedReceiver<T, S> {
    fn clone(&self) -> Self {
        Self {
            unbounded: self.unbounded.clone(),
        }
    }
}

impl<T, S: SignalSchedule + Clone> DynUnboundedReceiver<T, S> {
    /// Get the runtime configuration
    pub fn config(&self) -> DynSpscConfig {
        self.unbounded.config()
    }

    /// Try to pop an item from the queue
    pub fn try_pop(&self) -> Option<T> {
        loop {
            let head_node = self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, S>;
            if head_node.is_null() {
                return None;
            }

            unsafe {
                let item = (*head_node).queue.try_pop();
                if item.is_some() {
                    return item;
                }

                // If current head is empty, check if we should advance to next node
                if (*head_node).queue.is_empty() {
                    let next_head = (*head_node).next.load(Ordering::Acquire) as *mut Node<T, S>;
                    if !next_head.is_null() {
                        self.unbounded
                            .head
                            .store(next_head as usize, Ordering::Release);
                        continue;
                    } else {
                        // No more nodes, check if current node is closed
                        if (*head_node).queue.is_closed() {
                            return None;
                        } else {
                            return None;
                        }
                    }
                }

                return None;
            }
        }
    }

    /// Try to pop multiple items from the queue
    pub fn try_pop_n(&self, items: &mut [T]) -> Result<usize, PopError> {
        let mut filled = 0;
        let mut remaining_space = items;

        while !remaining_space.is_empty() {
            let head_node = self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, S>;
            if head_node.is_null() {
                break;
            }

            unsafe {
                match (*head_node).queue.try_pop_n(remaining_space) {
                    Ok(n) => {
                        filled += n;
                        remaining_space = &mut remaining_space[n..];

                        if remaining_space.is_empty()
                            || ((*head_node).queue.is_empty() && (*head_node).queue.is_closed())
                        {
                            break;
                        }
                    }
                    Err(PopError::Empty) => {
                        if (*head_node).queue.is_empty() {
                            let next_head =
                                (*head_node).next.load(Ordering::Acquire) as *mut Node<T, S>;
                            if !next_head.is_null() {
                                self.unbounded
                                    .head
                                    .store(next_head as usize, Ordering::Release);
                                continue;
                            } else {
                                if (*head_node).queue.is_closed() {
                                    break;
                                } else {
                                    break;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        if filled == 0 {
            Err(PopError::Empty)
        } else {
            Ok(filled)
        }
    }

    /// Consume items in place using a callback
    pub fn consume_in_place<F>(&self, max: usize, mut f: F) -> usize
    where
        F: FnMut(&[T]) -> usize,
    {
        let mut consumed_total = 0;
        let mut remaining = max;

        while consumed_total < max {
            let head_node = self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, S>;
            if head_node.is_null() {
                break;
            }

            unsafe {
                let to_consume = std::cmp::min(remaining, (*head_node).queue.len());
                if to_consume == 0 {
                    if (*head_node).queue.is_empty() {
                        let next_head =
                            (*head_node).next.load(Ordering::Acquire) as *mut Node<T, S>;
                        if !next_head.is_null() {
                            self.unbounded
                                .head
                                .store(next_head as usize, Ordering::Release);
                            continue;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                let consumed = (*head_node)
                    .queue
                    .consume_in_place(to_consume, |chunk| f(chunk));

                consumed_total += consumed;
                remaining -= consumed;

                if consumed < to_consume {
                    break;
                }
            }
        }

        consumed_total
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.unbounded.is_empty()
    }

    /// Get the current length (approximate)
    pub fn len(&self) -> usize {
        self.unbounded.len()
    }

    /// Check if the channel is closed
    pub fn is_closed(&self) -> bool {
        let head_node = self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, S>;
        if head_node.is_null() {
            return true;
        }

        unsafe { (*head_node).queue.is_closed() }
    }

    /// Get the number of nodes in the unbounded queue
    pub fn node_count(&self) -> usize {
        self.unbounded.node_count()
    }

    /// Get the total capacity across all nodes
    pub fn total_capacity(&self) -> usize {
        self.unbounded.total_capacity()
    }

    /// Mark the current head node as executing (for signal scheduling)
    pub fn mark(&self) {
        let head_node = self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, S>;
        if !head_node.is_null() {
            unsafe {
                (*head_node).queue.mark();
            }
        }
    }

    /// Unmark the current head node (for signal scheduling)
    pub fn unmark(&self) {
        let head_node = self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, S>;
        if !head_node.is_null() {
            unsafe {
                (*head_node).queue.unmark();
            }
        }
    }

    /// Unmark and reschedule the current head node (for signal scheduling)
    pub fn unmark_and_schedule(&self) {
        let head_node = self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, S>;
        if !head_node.is_null() {
            unsafe {
                (*head_node).queue.unmark_and_schedule();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    fn test_config() -> DynSpscConfig {
        DynSpscConfig::new(6, 8) // 64 items/segment, 256 segments = ~16k items per node
    }

    fn small_config() -> DynSpscConfig {
        DynSpscConfig::new(2, 2) // 4 items/segment, 4 segments = 15 items per node
    }

    fn tiny_config() -> DynSpscConfig {
        DynSpscConfig::new(1, 1) // 2 items/segment, 2 segments = 3 items per node
    }

    #[test]
    fn test_basic_unbounded_dyn_spsc() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(test_config());

        // Test basic push/pop
        assert!(sender.try_push(42).is_ok());
        assert_eq!(receiver.try_pop(), Some(42));
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_unbounded_growth() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(small_config());

        // Fill the first node (15 items)
        for i in 0..15 {
            assert!(sender.try_push(i).is_ok());
        }

        // Next push should create a new node
        assert!(sender.try_push(15).is_ok());

        // Verify we can pop all items
        for i in 0..16 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_multiple_nodes_traversal() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(tiny_config());

        // Fill multiple nodes (need more than 3 items to create multiple nodes)
        for i in 0..10 {
            assert!(sender.try_push(i).is_ok());
        }

        // Verify node count increased
        assert!(sender.node_count() > 1);

        // Pop all items
        for i in 0..10 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
    }

    #[test]
    fn test_try_push_n_unbounded() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(small_config());

        let mut items: Vec<u64> = (0..50).collect();
        let pushed = sender.try_push_n(&mut items).unwrap();
        assert_eq!(pushed, 50);

        // Verify all items were pushed
        for i in 0..50 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
    }

    #[test]
    fn test_try_pop_n_unbounded() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(small_config());

        // Push items that span multiple nodes
        for i in 0..50 {
            assert!(sender.try_push(i).is_ok());
        }

        // Pop in batches
        let mut buffer = [0u64; 20];
        let popped = receiver.try_pop_n(&mut buffer).unwrap();
        assert_eq!(popped, 20);
        for i in 0..20 {
            assert_eq!(buffer[i], i as u64);
        }

        let popped = receiver.try_pop_n(&mut buffer).unwrap();
        assert_eq!(popped, 20);
        for i in 0..20 {
            assert_eq!(buffer[i], (i + 20) as u64);
        }
    }

    #[test]
    fn test_consumer_advancement() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(small_config());

        // Fill first node completely (15 items)
        for i in 0..15 {
            assert!(sender.try_push(i).is_ok());
        }

        // Add one more item to trigger creation of second node
        assert!(sender.try_push(15).is_ok());

        // Verify we now have 2 nodes
        assert_eq!(sender.node_count(), 2);

        // Pop all items from first node
        for i in 0..16 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Add items to second node
        for i in 16..19 {
            assert!(sender.try_push(i).is_ok());
        }

        // Should be able to pop from second node
        for i in 16..19 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
    }

    #[test]
    fn test_concurrent_operations() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(test_config());

        // Producer thread
        let producer = {
            let sender = sender.clone();
            thread::spawn(move || {
                for i in 0..1000 {
                    while sender.try_push(i).is_err() {
                        thread::sleep(Duration::from_micros(1));
                    }
                }
            })
        };

        // Consumer thread
        let consumer = {
            let receiver = receiver.clone();
            thread::spawn(move || {
                let mut received = Vec::new();
                while received.len() < 1000 {
                    if let Some(item) = receiver.try_pop() {
                        received.push(item);
                    }
                    thread::sleep(Duration::from_micros(1));
                }
                received
            })
        };

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        // Verify all items were received
        assert_eq!(received.len(), 1000);
        for i in 0..1000 {
            assert!(received.contains(&i));
        }
    }

    #[test]
    fn test_statistics_tracking() {
        let config = small_config();
        let (sender, _receiver) = DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(config);

        assert_eq!(sender.node_count(), 1);
        assert_eq!(sender.total_capacity(), config.capacity());

        // Fill first node and add one more item
        for i in 0..16 {
            assert!(sender.try_push(i).is_ok());
        }

        assert_eq!(sender.node_count(), 2);
        assert_eq!(sender.total_capacity(), config.capacity() * 2);
    }

    #[test]
    fn test_consume_in_place_unbounded() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(small_config());

        // Add items to multiple nodes
        for i in 0..30 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consume using callback
        let sum = Arc::new(Mutex::new(0));
        let sum_clone = sum.clone();

        let consumed = receiver.consume_in_place(30, |chunk| {
            let mut total = sum_clone.lock().unwrap();
            for &item in chunk {
                *total += item;
            }
            chunk.len()
        });

        assert_eq!(consumed, 30);
        assert_eq!(*sum.lock().unwrap(), (0..30).sum::<u64>());
    }

    #[test]
    fn test_large_node_count_stress() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(tiny_config());

        // Create many nodes
        let num_items = 1000;
        for i in 0..num_items {
            assert!(sender.try_push(i).is_ok());
        }

        // Verify we have many nodes
        assert!(sender.node_count() > 100);

        // Consume all items
        for i in 0..num_items {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Should be empty now
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_interleaved_producer_consumer_stress() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(tiny_config());

        let mut expected_consumed = 0;

        // Interleaved push and pop operations
        for round in 0..20 {
            // Push a few items
            for i in 0..5 {
                let item = round * 5 + i;
                assert!(sender.try_push(item).is_ok());
            }

            // Pop a few items
            for _ in 0..3 {
                if let Some(item) = receiver.try_pop() {
                    assert_eq!(item, expected_consumed);
                    expected_consumed += 1;
                }
            }
        }

        // Consume remaining items
        while let Some(item) = receiver.try_pop() {
            assert_eq!(item, expected_consumed);
            expected_consumed += 1;
        }

        // Verify we consumed all expected items
        assert_eq!(expected_consumed, 100);
    }

    #[test]
    fn test_try_push_n_with_node_creation() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(tiny_config());

        // Fill first node completely
        let mut first_batch: Vec<u64> = (0..3).collect();
        let pushed = sender.try_push_n(&mut first_batch).unwrap();
        assert_eq!(pushed, 3);
        assert_eq!(sender.node_count(), 1);

        // Try to push more than fits in current node
        let mut second_batch: Vec<u64> = (3..10).collect();
        let pushed = sender.try_push_n(&mut second_batch).unwrap();
        assert_eq!(pushed, 7);

        // Should have created additional nodes
        assert!(sender.node_count() > 1);

        // Verify all items were pushed
        for i in 0..10 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
    }

    #[test]
    fn test_consume_in_place_across_nodes() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(tiny_config());

        // Fill multiple nodes
        for i in 0..10 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consume across node boundaries using consume_in_place
        let sum = Arc::new(Mutex::new(0));
        let sum_clone = sum.clone();

        let consumed = receiver.consume_in_place(10, |chunk| {
            let mut total = sum_clone.lock().unwrap();
            for &item in chunk {
                *total += item;
            }
            chunk.len()
        });

        assert_eq!(consumed, 10);
        assert_eq!(*sum.lock().unwrap(), (0..10).sum::<u64>());

        // Queue should be empty now
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_try_push_n_performance() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(small_config());

        // Test large batch push that spans multiple nodes
        let mut large_batch: Vec<u64> = (0..100).collect();
        let pushed = sender.try_push_n(&mut large_batch).unwrap();
        assert_eq!(pushed, 100);

        // Verify all items were pushed efficiently
        for i in 0..100 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Verify node count increased appropriately
        assert!(sender.node_count() >= 6);
    }

    #[test]
    fn test_channel_close() {
        let (sender, receiver) =
            DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(test_config());

        sender.try_push(1).unwrap();
        sender.try_push(2).unwrap();

        drop(sender);

        assert!(receiver.is_closed());
        assert_eq!(receiver.try_pop(), Some(1));
        assert_eq!(receiver.try_pop(), Some(2));
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_config_access() {
        let config = DynSpscConfig::new(5, 7);
        let (sender, receiver) = DynUnboundedSpsc::<u64, NoOpSignal>::new_with_config(config);

        assert_eq!(sender.config().p, 5);
        assert_eq!(sender.config().num_segs_p2, 7);
        assert_eq!(receiver.config().p, 5);
        assert_eq!(receiver.config().num_segs_p2, 7);
    }

    #[test]
    fn test_default_config() {
        let (sender, receiver) = DynUnboundedSpsc::<u64, NoOpSignal>::new();

        // Default config is p=6, num_segs_p2=8
        assert_eq!(sender.config().p, 6);
        assert_eq!(sender.config().num_segs_p2, 8);

        sender.try_push(42).unwrap();
        assert_eq!(receiver.try_pop(), Some(42));
    }
}
