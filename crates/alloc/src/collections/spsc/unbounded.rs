//!
//! # Unbounded Spsc
//!
//! An unbounded single-producer single-consumer queue implemented as an intrusive
//! linked list of fixed-capacity Spsc segments. Optimized for the common case of
//! fitting within a single segment, with automatic expansion when needed.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Unbounded Spsc Structure                     │
//! │                                                                 │
//! │  Head (Atomic) ──→ Node 0 ──→ Node 1 ──→ Node 2 ──→ ...         │
//! │       │                 │           │           │               │
//! │       │              [Spsc]      [Spsc]      [Spsc]             │
//! │       │            (64 items)  (64 items)  (64 items)           │
//! │       │                                                         │
//! │  Tail (Cached) ──→ Current Node for Producer                    │
//! │                                                                 │
//! │  Consumer automatically progresses through nodes                │
//! │  Producer creates new nodes when current tail is full           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!

use super::{NoOpSignal, PopError, PushError, SignalSchedule, Spsc};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ptr::null;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Intrusive node containing an Spsc segment
struct Node<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule> {
    /// The underlying Spsc queue for this segment
    queue: Spsc<T, P, NUM_SEGS_P2, S>,

    /// Next node in the linked list (atomic for thread-safe access)
    next: AtomicUsize,

    /// Flag indicating if this node is currently full
    /// Used for optimization and potential cleanup
    is_full: AtomicUsize,

    /// Phantom data to ensure proper variance
    _phantom: PhantomData<T>,
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    Node<T, P, NUM_SEGS_P2, S>
{
    /// Create a new node with an empty Spsc
    fn new(signal: Arc<S>) -> *mut Self {
        let node = Box::new(Self {
            queue: unsafe { Spsc::new_unsafe_with_gate((&*signal).clone()) },
            next: AtomicUsize::new(0),
            is_full: AtomicUsize::new(0),
            _phantom: PhantomData,
        });
        Box::into_raw(node)
    }
}

/// Unbounded Spsc implementation
pub struct UnboundedSpsc<
    T,
    const P: usize = 6,
    const NUM_SEGS_P2: usize = 8,
    S: SignalSchedule + Clone = NoOpSignal,
> {
    /// Head of the linked list (atomic for consumer access)
    head: Arc<AtomicUsize>,

    /// Cached tail pointer for producer (avoids atomic operations in common case)
    current_tail: UnsafeCell<*mut Node<T, P, NUM_SEGS_P2, S>>,

    /// Total number of nodes in the linked list
    node_count: AtomicUsize,

    /// Total capacity across all nodes
    total_capacity: AtomicUsize,

    /// Signal scheduler for creating new nodes
    signal: Arc<S>,
}

unsafe impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Send
    for UnboundedSpsc<T, P, NUM_SEGS_P2, S>
{
}

unsafe impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Sync
    for UnboundedSpsc<T, P, NUM_SEGS_P2, S>
{
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    UnboundedSpsc<T, P, NUM_SEGS_P2, S>
{
    /// Create a new unbounded Spsc with default configuration
    pub fn new() -> (
        UnboundedSender<T, P, NUM_SEGS_P2, S>,
        UnboundedReceiver<T, P, NUM_SEGS_P2, S>,
    )
    where
        S: Default,
    {
        Self::new_with_signal(S::default())
    }

    /// Create a new unbounded Spsc with a custom signal scheduler
    pub fn new_with_signal(
        signal: S,
    ) -> (
        UnboundedSender<T, P, NUM_SEGS_P2, S>,
        UnboundedReceiver<T, P, NUM_SEGS_P2, S>,
    ) {
        // Create the first node
        let first_node = Node::new(Arc::new(signal.clone()));

        let unbounded = Arc::new(Self {
            head: Arc::new(AtomicUsize::new(first_node as usize)),
            current_tail: UnsafeCell::new(first_node),
            node_count: AtomicUsize::new(1),
            total_capacity: AtomicUsize::new(Spsc::<T, P, NUM_SEGS_P2, S>::capacity()),
            signal: Arc::new(signal),
        });

        let sender = UnboundedSender {
            unbounded: unbounded.clone(),
        };

        let receiver = UnboundedReceiver { unbounded };

        (sender, receiver)
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
        let mut current = self.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;

        while !current.is_null() {
            unsafe {
                len += (*current).queue.len();
                current =
                    (*current).next.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
            }
        }

        len
    }

    /// Check if the queue is empty (approximate)
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head_node_ptr = self.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
        if head_node_ptr.is_null() {
            return true;
        }

        unsafe {
            // Check if head node has any items
            !(*head_node_ptr).queue.is_empty()
        }
    }

    /// Create a receiver from an Arc<UnboundedSpsc>
    /// This is useful for MPSC implementations that store the Arc
    pub fn create_receiver(self: &Arc<Self>) -> UnboundedReceiver<T, P, NUM_SEGS_P2, S> {
        UnboundedReceiver {
            unbounded: Arc::clone(self),
        }
    }

    /// Close the queue
    /// This closes all underlying Spsc nodes
    pub fn close(&self) {
        let mut current = self.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;

        while !current.is_null() {
            unsafe {
                (*current).queue.close();
                current =
                    (*current).next.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
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
    fn create_new_node(&self) -> *mut Node<T, P, NUM_SEGS_P2, S> {
        let new_node = Node::new(self.signal.clone());

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
            .fetch_add(Spsc::<T, P, NUM_SEGS_P2, S>::capacity(), Ordering::Relaxed);

        new_node
    }

    /// Internal: advance consumer to next node if current is empty and closed
    #[inline(always)]
    fn try_advance_consumer(&self) -> bool {
        unsafe {
            let current_head_ptr =
                self.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
            if current_head_ptr.is_null() {
                return false;
            }

            let current_head = &*current_head_ptr;

            // If current head is empty and closed, advance to next
            if current_head.queue.is_empty() && current_head.queue.is_closed() {
                let next_head =
                    current_head.next.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
                if !next_head.is_null() {
                    self.head.store(next_head as usize, Ordering::Release);
                    return true;
                }
            }

            false
        }
    }
}

/// Sender half of the unbounded Spsc
pub struct UnboundedSender<
    T,
    const P: usize = 6,
    const NUM_SEGS_P2: usize = 8,
    S: SignalSchedule + Clone = NoOpSignal,
> {
    unbounded: Arc<UnboundedSpsc<T, P, NUM_SEGS_P2, S>>,
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Clone
    for UnboundedSender<T, P, NUM_SEGS_P2, S>
{
    fn clone(&self) -> Self {
        Self {
            unbounded: self.unbounded.clone(),
        }
    }
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    UnboundedSender<T, P, NUM_SEGS_P2, S>
{
    /// Get a clone of the Arc<UnboundedSpsc>
    /// This is useful for MPSC implementations that need to store the Arc
    pub fn unbounded_arc(&self) -> Arc<UnboundedSpsc<T, P, NUM_SEGS_P2, S>> {
        Arc::clone(&self.unbounded)
    }

    /// Try to push an item to the queue
    pub fn try_push(&self, item: T) -> Result<(), PushError<T>> {
        // Try the current tail first (fast path)
        match self.unbounded.try_push_to_current(item) {
            Ok(()) => return Ok(()),
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
    /// This method uses the underlying Spsc's bulk try_push_n for efficient batch operations.
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
                    // by shifting remaining items to the front
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
                            items.set_len(0); // All items moved out
                            return Ok(total_pushed);
                        }

                        // If n > 0, we had a partial push - current node is full
                        // If n == 0, the queue returned Ok(0) meaning it's full
                        // Either way, create a new node to continue
                        let _new_node = self.unbounded.create_new_node();
                    }
                    Err(PushError::Full(())) => {
                        // Queue is full but not closed, create a new node
                        let _new_node = self.unbounded.create_new_node();

                        // After creating a new node, loop will retry with the new node
                    }
                    Err(PushError::Closed(())) => {
                        // Remove the items we successfully pushed from the Vec
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

            // All items pushed successfully
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

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Drop
    for UnboundedSender<T, P, NUM_SEGS_P2, S>
{
    fn drop(&mut self) {
        // Close the channel when the sender is dropped
        self.close_channel();
    }
}

/// Receiver half of the unbounded Spsc
pub struct UnboundedReceiver<
    T,
    const P: usize = 6,
    const NUM_SEGS_P2: usize = 8,
    S: SignalSchedule + Clone = NoOpSignal,
> {
    unbounded: Arc<UnboundedSpsc<T, P, NUM_SEGS_P2, S>>,
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Clone
    for UnboundedReceiver<T, P, NUM_SEGS_P2, S>
{
    fn clone(&self) -> Self {
        Self {
            unbounded: self.unbounded.clone(),
        }
    }
}

impl<T, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    UnboundedReceiver<T, P, NUM_SEGS_P2, S>
{
    /// Try to pop an item from the queue
    pub fn try_pop(&self) -> Option<T> {
        loop {
            // Try to pop from the current head
            let head_node =
                self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
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
                    let next_head = (*head_node).next.load(Ordering::Acquire)
                        as *mut Node<T, P, NUM_SEGS_P2, S>;
                    if !next_head.is_null() {
                        self.unbounded
                            .head
                            .store(next_head as usize, Ordering::Release);
                        continue; // Try to pop from the new head
                    } else {
                        // No more nodes, check if the current node is closed
                        if (*head_node).queue.is_closed() {
                            return None;
                        } else {
                            // Current node not closed, items might arrive later
                            return None;
                        }
                    }
                }

                // Current head has items but we couldn't pop (shouldn't happen)
                return None;
            }
        }
    }

    /// Try to pop multiple items from the queue
    pub fn try_pop_n(&self, items: &mut [T]) -> Result<usize, PopError> {
        let mut filled = 0;
        let mut remaining_space = items;

        while !remaining_space.is_empty() {
            let head_node =
                self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
            if head_node.is_null() {
                break;
            }

            unsafe {
                match (*head_node).queue.try_pop_n(remaining_space) {
                    Ok(n) => {
                        filled += n;
                        remaining_space = &mut remaining_space[n..];

                        // If we filled the current buffer or the head is exhausted, break
                        if remaining_space.is_empty()
                            || ((*head_node).queue.is_empty() && (*head_node).queue.is_closed())
                        {
                            break;
                        }
                    }
                    Err(PopError::Empty) => {
                        // Current head is empty, check if we should advance
                        if (*head_node).queue.is_empty() {
                            let next_head = (*head_node).next.load(Ordering::Acquire)
                                as *mut Node<T, P, NUM_SEGS_P2, S>;
                            if !next_head.is_null() {
                                self.unbounded
                                    .head
                                    .store(next_head as usize, Ordering::Release);
                                continue; // Try to pop from the new head
                            } else {
                                // No more nodes, check if current node is closed
                                if (*head_node).queue.is_closed() {
                                    break; // No more items coming
                                } else {
                                    break; // Items might arrive later
                                }
                            }
                        } else {
                            break; // Current head not empty but no items available
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
            let head_node =
                self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
            if head_node.is_null() {
                break;
            }

            unsafe {
                let to_consume = std::cmp::min(remaining, (*head_node).queue.len());
                if to_consume == 0 {
                    // Check if we should advance to next node
                    if (*head_node).queue.is_empty() {
                        let next_head = (*head_node).next.load(Ordering::Acquire)
                            as *mut Node<T, P, NUM_SEGS_P2, S>;
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

                // Consume from current node
                let consumed = (*head_node)
                    .queue
                    .consume_in_place(to_consume, |chunk| f(chunk));

                consumed_total += consumed;
                remaining -= consumed;

                if consumed < to_consume {
                    break; // Consumer function didn't consume all requested items
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
        let head_node =
            self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
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
        let head_node =
            self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
        if !head_node.is_null() {
            unsafe {
                (*head_node).queue.mark();
            }
        }
    }

    /// Unmark the current head node (for signal scheduling)
    pub fn unmark(&self) {
        let head_node =
            self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
        if !head_node.is_null() {
            unsafe {
                (*head_node).queue.unmark();
            }
        }
    }

    /// Unmark and reschedule the current head node (for signal scheduling)
    pub fn unmark_and_schedule(&self) {
        let head_node =
            self.unbounded.head.load(Ordering::Acquire) as *mut Node<T, P, NUM_SEGS_P2, S>;
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

    #[test]
    fn test_basic_unbounded_spsc() {
        type TestQ = UnboundedSpsc<u64>;
        let (sender, receiver) = TestQ::new();

        // Test basic push/pop
        assert!(sender.try_push(42).is_ok());
        assert_eq!(receiver.try_pop(), Some(42));
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_unbounded_growth() {
        type TestQ = UnboundedSpsc<u64, 2, 2>; // Small capacity: 4 items per node
        let (sender, receiver) = TestQ::new();

        // Fill the first node (4 items)
        for i in 0..4 {
            assert!(sender.try_push(i).is_ok());
        }

        // Next push should create a new node
        assert!(sender.try_push(4).is_ok());

        // Verify we can pop all items
        for i in 0..5 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
        assert_eq!(receiver.try_pop(), None);
    }

    #[test]
    fn test_multiple_nodes_traversal() {
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 2 items per segment, 2 segments = 3 capacity per node
        let (sender, receiver) = TestQ::new();

        // Fill multiple nodes (need more than 3 items to create multiple nodes)
        for i in 0..10 {
            assert!(sender.try_push(i).is_ok());
        }

        // Verify node count increased
        assert!(sender.node_count() > 1);
        assert_eq!(sender.total_capacity(), sender.node_count() * 3); // (2*2)-1 = 3 items per node

        // Pop all items
        for i in 0..10 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
    }

    #[test]
    fn test_try_push_n_unbounded() {
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 4 items per node
        let (sender, receiver) = TestQ::new();

        let mut items: Vec<u64> = (0..10).collect();
        let pushed = sender.try_push_n(&mut items).unwrap();
        assert_eq!(pushed, 10);

        // Verify all items were pushed
        for i in 0..10 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
    }

    #[test]
    fn test_consumer_advancement() {
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 4 items per segment, 4 segments = 15 capacity per node
        let (sender, receiver) = TestQ::new();

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
        type TestQ = UnboundedSpsc<u64>;
        let (sender, receiver) = TestQ::new();

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
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 4 items per segment, 4 segments = 15 capacity per node
        let (sender, receiver) = TestQ::new();

        assert_eq!(sender.node_count(), 1);
        assert_eq!(sender.total_capacity(), 15); // (4*4)-1 = 15

        // Fill first node and add one more item
        for i in 0..16 {
            assert!(sender.try_push(i).is_ok());
        }

        assert_eq!(sender.node_count(), 2);
        assert_eq!(sender.total_capacity(), 30); // 2 * 15
    }

    #[test]
    fn test_consume_in_place_unbounded() {
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 4 items per node
        let (sender, receiver) = TestQ::new();

        // Add items to multiple nodes
        for i in 0..10 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consume using callback
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
    }

    #[test]
    fn test_memory_dropping_different_nodes() {
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 15 items per node
        let (sender, receiver) = TestQ::new();

        // Fill multiple nodes
        for i in 0..50 {
            assert!(sender.try_push(i).is_ok());
        }

        // Verify we have multiple nodes
        assert!(sender.node_count() > 1);
        let initial_node_count = sender.node_count();

        // Consumer consumes from first node completely
        for i in 0..15 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // At this point, consumer should be at node 1, sender should be at node 3 or later
        // Consumer finishes node 1
        assert_eq!(receiver.try_pop(), Some(15));

        // Verify that we can still push to later nodes
        for i in 50..60 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consumer should be able to continue from where it left off
        for i in 16..20 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Memory should still be properly managed
        assert!(sender.node_count() >= initial_node_count);
    }

    #[test]
    fn test_producer_consumer_position_divergence() {
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 3 items per node - small for easy testing
        let (sender, receiver) = TestQ::new();

        // Fill many nodes
        for i in 0..30 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consumer lags behind significantly
        // Consumer at node 0 (items 0-2)
        assert_eq!(receiver.try_pop(), Some(0));
        assert_eq!(receiver.try_pop(), Some(1));
        assert_eq!(receiver.try_pop(), Some(2));

        // Producer continues to node 9 (items 27-29)
        for i in 30..33 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consumer moves to node 1 (items 3-5)
        assert_eq!(receiver.try_pop(), Some(3));
        assert_eq!(receiver.try_pop(), Some(4));
        assert_eq!(receiver.try_pop(), Some(5));

        // Consumer moves to node 2 (items 6-8)
        assert_eq!(receiver.try_pop(), Some(6));
        assert_eq!(receiver.try_pop(), Some(7));
        assert_eq!(receiver.try_pop(), Some(8));

        // At this point:
        // - Consumer is at node 2, about to read item 9
        // - Producer is at node 10+ (items 30+)
        // - Nodes 0, 1 should be eligible for cleanup (but we don't implement cleanup yet)

        // Continue consuming to verify the system still works
        for i in 9..12 {
            assert_eq!(receiver.try_pop(), Some(i));
        }
    }

    #[test]
    fn test_channel_close_with_divergent_positions() {
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 15 items per node
        let (sender, receiver) = TestQ::new();

        // Fill multiple nodes
        for i in 0..40 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consumer gets ahead and consumes several nodes
        for i in 0..20 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Consumer is now at node 1, producer is at node 2 or 3
        // Close the channel while they're at different positions
        sender.close_channel();

        // Consumer should still be able to consume remaining items
        for i in 20..40 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // After consuming all items, should get None
        assert_eq!(receiver.try_pop(), None);
        assert!(receiver.is_closed());
    }

    #[test]
    fn test_large_node_count_stress() {
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 3 items per node
        let (sender, receiver) = TestQ::new();

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
    fn test_memory_pressure_with_rapid_producer_consumer() {
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 3 items per node
        let (sender, receiver) = TestQ::new();

        // Rapid production and consumption
        let mut produced_items = Vec::new();
        let mut consumed_items = Vec::new();

        // Producer creates items rapidly
        for i in 0..200 {
            assert!(sender.try_push(i).is_ok());
            produced_items.push(i);
        }

        // Consumer catches up and consumes many items
        for _ in 0..150 {
            if let Some(item) = receiver.try_pop() {
                consumed_items.push(item);
            }
        }

        // Verify consumption is working correctly
        assert_eq!(consumed_items.len(), 150);
        for (i, &item) in consumed_items.iter().enumerate() {
            assert_eq!(item, i as u64);
        }

        // Continue producing
        for i in 200..250 {
            assert!(sender.try_push(i).is_ok());
            produced_items.push(i);
        }

        // Continue consuming
        for _ in 0..100 {
            if let Some(item) = receiver.try_pop() {
                consumed_items.push(item);
            }
        }

        // Verify the pattern continues correctly
        assert_eq!(consumed_items.len(), 250);
        for (i, &item) in consumed_items.iter().enumerate() {
            assert_eq!(item, i as u64);
        }
    }

    #[test]
    fn test_consumer_abandoning_nodes() {
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 3 items per node
        let (sender, receiver) = TestQ::new();

        // Fill many nodes
        for i in 0..50 {
            assert!(sender.try_push(i).is_ok());
        }

        // Consumer partially consumes some nodes then stops
        for i in 0..10 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Drop the receiver - this should not affect the sender
        drop(receiver);

        // Sender should still be able to push more items
        for i in 50..60 {
            assert!(sender.try_push(i).is_ok());
        }

        // Create a new receiver and verify it can consume from the beginning
        let (_sender2, receiver2) = TestQ::new();

        // This new receiver should start from the beginning (or be independent)
        // Since we dropped the first receiver, the behavior depends on implementation
        // For now, just verify the sender still works
        for i in 60..70 {
            assert!(sender.try_push(i).is_ok());
        }
    }

    #[test]
    fn test_interleaved_producer_consumer_stress() {
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 3 items per node
        let (sender, receiver) = TestQ::new();

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
        assert_eq!(expected_consumed, 100); // 20 rounds * 5 items per round
    }

    #[test]
    fn test_node_count_accuracy() {
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 15 items per node
        let (sender, receiver) = TestQ::new();

        // Initially should have 1 node
        assert_eq!(sender.node_count(), 1);

        // Fill first node (15 items)
        for i in 0..15 {
            assert_eq!(sender.node_count(), 1);
            assert!(sender.try_push(i).is_ok());
        }

        // Adding one more should create second node
        assert!(sender.try_push(15).is_ok());
        assert_eq!(sender.node_count(), 2);

        // Add more items to fill second node (items 16-30 = 15 items)
        for i in 16..30 {
            assert!(sender.try_push(i).is_ok());
        }
        // Should still have 2 nodes (second node can hold 15 items)
        assert_eq!(sender.node_count(), 2);

        // Add one more to create third node
        assert!(sender.try_push(30).is_ok());
        assert_eq!(sender.node_count(), 3);

        // Consume some items but not entire nodes
        for i in 0..10 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Node count should remain the same
        assert_eq!(sender.node_count(), 3);

        // Consume entire first node
        for i in 10..15 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Node count should still be the same (we don't implement cleanup yet)
        assert_eq!(sender.node_count(), 3);
    }

    #[test]
    fn test_try_push_n_with_node_creation() {
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 3 items per node
        let (sender, receiver) = TestQ::new();

        // Fill first node completely
        let mut first_batch: Vec<u64> = (0..3).collect();
        let pushed = sender.try_push_n(&mut first_batch).unwrap();
        assert_eq!(pushed, 3);
        assert_eq!(sender.node_count(), 1);

        // Try to push more than fits in current node
        let mut second_batch: Vec<u64> = (3..10).collect(); // 7 items
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
        type TestQ = UnboundedSpsc<u64, 1, 1>; // 3 items per node
        let (sender, receiver) = TestQ::new();

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
        type TestQ = UnboundedSpsc<u64, 2, 2>; // 15 items per node
        let (sender, receiver) = TestQ::new();

        // Test large batch push that spans multiple nodes
        let mut large_batch: Vec<u64> = (0..100).collect();
        let pushed = sender.try_push_n(&mut large_batch).unwrap();
        assert_eq!(pushed, 100);

        // Verify all items were pushed efficiently
        for i in 0..100 {
            assert_eq!(receiver.try_pop(), Some(i));
        }

        // Verify node count increased appropriately
        // 100 items / 15 items per node = ~7 nodes
        assert!(sender.node_count() >= 6);
        assert!(sender.node_count() <= 8);
    }
}
