use std::{
    cell::UnsafeCell,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
};

use super::{PopError, PushError};

use super::{CachePadded, SignalSchedule, Spsc};

/// Three-level segmented SPSC queue (segment → directory → region).
///
/// This queue widens the capacity envelope of [`Spsc`] by introducing an outer ring of
/// SPSC “regions”. Each region is itself a fully-featured [`Spsc`]; the producer writes
/// sequentially through the ring, switching to the next region once the current region
/// fills. The consumer drains regions in order. Regions are recycled through an intrusive
/// Treiber stack so steady-state memory stays low and deallocations can be avoided when
/// demand drops.
pub struct LayeredSpsc<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> {
    regions: Box<[AtomicPtr<Region<T, P, NUM_SEGS_P2, S>>]>,
    spare_regions: AtomicPtr<Region<T, P, NUM_SEGS_P2, S>>,
    spare_count: AtomicUsize,
    producer: CachePadded<ProducerState<T, P, NUM_SEGS_P2, S>>,
    consumer: CachePadded<ConsumerState<T, P, NUM_SEGS_P2, S>>,
    head_items: AtomicU64,
    tail_items: AtomicU64,
    producer_region_cursor: AtomicU64,
    signal: CachePadded<S>,
    max_pooled_segments: usize,
    max_pooled_regions: usize,
}

impl<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> LayeredSpsc<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>
{
    const REGION_MASK: usize = (1 << NUM_REGIONS_P2) - 1;

    /// Capacity of a single region (delegates to the inner [`Spsc`] capacity).
    pub const REGION_CAPACITY: usize = Spsc::<T, P, NUM_SEGS_P2, S>::capacity();

    /// Total capacity of this layered queue.
    pub const CAPACITY: usize = Self::REGION_CAPACITY * (1 << NUM_REGIONS_P2);

    fn region_slot(index: u64) -> usize {
        (index as usize) & Self::REGION_MASK
    }

    fn new_inner(signal: S, max_pooled_segments: usize, max_pooled_regions: usize) -> Arc<Self> {
        assert!(NUM_REGIONS_P2 > 0, "NUM_REGIONS must be positive");
        let region_count = 1 << NUM_REGIONS_P2;
        let mut regions = Vec::with_capacity(region_count);
        regions.resize_with(region_count, || AtomicPtr::new(ptr::null_mut()));

        Arc::new(Self {
            regions: regions.into_boxed_slice(),
            spare_regions: AtomicPtr::new(ptr::null_mut()),
            spare_count: AtomicUsize::new(0),
            producer: CachePadded::new(ProducerState::new()),
            consumer: CachePadded::new(ConsumerState::new()),
            head_items: AtomicU64::new(0),
            tail_items: AtomicU64::new(0),
            producer_region_cursor: AtomicU64::new(0),
            signal: CachePadded::new(signal),
            max_pooled_segments,
            max_pooled_regions,
        })
    }

    /// Creates a layered SPSC queue with the default pooling configuration.
    pub fn new(
        signal: S,
    ) -> (
        LayeredSender<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>,
        LayeredReceiver<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>,
    ) {
        Self::new_with_config(signal, 0, 1)
    }

    /// Creates a layered SPSC queue with explicit pooling configuration.
    ///
    /// * `max_pooled_segments` caps the number of segments retained by each region.
    /// * `max_pooled_regions` caps the number of whole regions kept in the outer pool.
    pub fn new_with_config(
        signal: S,
        max_pooled_segments: usize,
        max_pooled_regions: usize,
    ) -> (
        LayeredSender<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>,
        LayeredReceiver<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>,
    ) {
        let queue = Self::new_inner(signal, max_pooled_segments, max_pooled_regions);
        (
            LayeredSender {
                queue: Arc::clone(&queue),
            },
            LayeredReceiver { queue },
        )
    }

    #[inline(always)]
    fn ensure_producer_region(
        &self,
    ) -> Result<*mut Region<T, P, NUM_SEGS_P2, S>, AcquireRegionError> {
        let state = &self.producer;
        let region_idx = unsafe { *state.region_index.get() };
        let slot = Self::region_slot(region_idx);

        let mut current_ptr = unsafe { *state.active_region.get() };
        if !current_ptr.is_null() {
            return Ok(current_ptr);
        }

        if !self.regions[slot].load(Ordering::Acquire).is_null() {
            return Err(AcquireRegionError::CapacityFull);
        }

        current_ptr = self
            .take_spare_region()
            .unwrap_or_else(|| unsafe { Region::alloc(&self.signal, self.max_pooled_segments) });

        unsafe {
            (*current_ptr)
                .next
                .store(ptr::null_mut(), Ordering::Relaxed);
            *state.active_region.get() = current_ptr;
        }

        Ok(current_ptr)
    }

    #[inline(always)]
    fn publish_region(&self, region_ptr: *mut Region<T, P, NUM_SEGS_P2, S>, slot: usize) {
        if self.regions[slot].load(Ordering::Relaxed).is_null() {
            self.regions[slot].store(region_ptr, Ordering::Release);
        }
    }

    #[inline(always)]
    fn rotate_producer_region(&self) {
        let state = &self.producer;
        let region_idx = unsafe { *state.region_index.get() } + 1;

        unsafe {
            *state.region_index.get() = region_idx;
            *state.active_region.get() = ptr::null_mut();
        }

        self.producer_region_cursor
            .store(region_idx, Ordering::Release);
    }

    #[inline(always)]
    fn release_consumer_region(&self, slot: usize, region_ptr: *mut Region<T, P, NUM_SEGS_P2, S>) {
        self.regions[slot].store(ptr::null_mut(), Ordering::Release);
        unsafe {
            *self.consumer.active_region.get() = ptr::null_mut();
            *self.consumer.region_index.get() += 1;
        }
        self.recycle_region(region_ptr);
    }

    #[inline(always)]
    fn take_spare_region(&self) -> Option<*mut Region<T, P, NUM_SEGS_P2, S>> {
        loop {
            let head = self.spare_regions.load(Ordering::Acquire);
            if head.is_null() {
                return None;
            }
            let next = unsafe { (*head).next.load(Ordering::Acquire) };
            if self
                .spare_regions
                .compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.spare_count.fetch_sub(1, Ordering::Relaxed);
                unsafe { (*head).next.store(ptr::null_mut(), Ordering::Relaxed) };
                return Some(head);
            }
        }
    }

    #[inline(always)]
    fn recycle_region(&self, region_ptr: *mut Region<T, P, NUM_SEGS_P2, S>) {
        if region_ptr.is_null() {
            return;
        }

        if self.spare_count.load(Ordering::Relaxed) >= self.max_pooled_regions {
            unsafe {
                Region::free(region_ptr);
            }
            return;
        }

        loop {
            let head = self.spare_regions.load(Ordering::Acquire);
            unsafe { (*region_ptr).next.store(head, Ordering::Relaxed) }
            if self
                .spare_regions
                .compare_exchange(head, region_ptr, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.spare_count.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let head = self.head_items.load(Ordering::Acquire);
        let tail = self.tail_items.load(Ordering::Acquire);
        head.saturating_sub(tail) as usize
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        Self::CAPACITY
    }
}

impl<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> Drop for LayeredSpsc<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>
{
    fn drop(&mut self) {
        for slot in self.regions.iter() {
            let ptr = slot.load(Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe { Region::free(ptr) };
            }
        }

        let mut spare = self.spare_regions.load(Ordering::Relaxed);
        while !spare.is_null() {
            let next = unsafe { (*spare).next.load(Ordering::Relaxed) };
            unsafe { Region::free(spare) };
            spare = next;
        }

        // Producer / consumer may still hold active regions that are not present in slots
        let prod_active = unsafe { *self.producer.active_region.get() };
        if !prod_active.is_null() {
            unsafe { Region::free(prod_active) };
        }
        let cons_active = unsafe { *self.consumer.active_region.get() };
        if !cons_active.is_null() {
            unsafe { Region::free(cons_active) };
        }
    }
}

pub struct LayeredSender<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> {
    queue: Arc<LayeredSpsc<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>>,
}

pub struct LayeredReceiver<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> {
    queue: Arc<LayeredSpsc<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>>,
}

impl<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> Clone for LayeredSender<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>
{
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> LayeredSender<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>
{
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    #[inline(always)]
    pub fn try_push(&self, mut item: T) -> Result<(), PushError<T>> {
        loop {
            let region_idx = unsafe { *self.queue.producer.region_index.get() };
            let slot = LayeredSpsc::<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>::region_slot(region_idx);
            let region_ptr = match self.queue.ensure_producer_region() {
                Ok(ptr) => ptr,
                Err(AcquireRegionError::CapacityFull) => {
                    return Err(PushError::Full(item));
                }
            };
            let region = unsafe { &(*region_ptr).queue };

            match region.try_push(item) {
                Ok(()) => {
                    self.queue.publish_region(region_ptr, slot);
                    self.queue.head_items.fetch_add(1, Ordering::Release);
                    return Ok(());
                }
                Err(PushError::Full(returned)) => {
                    self.queue.publish_region(region_ptr, slot);
                    self.queue.rotate_producer_region();
                    item = returned;
                    continue;
                }
                Err(PushError::Closed(returned)) => {
                    self.queue.publish_region(region_ptr, slot);
                    return Err(PushError::Closed(returned));
                }
            }
        }
    }

    pub fn try_push_n(&self, src: &[T]) -> Result<usize, PushError<()>> {
        if src.is_empty() {
            return Ok(0);
        }

        let mut offset = 0usize;
        while offset < src.len() {
            let region_idx = unsafe { *self.queue.producer.region_index.get() };
            let slot = LayeredSpsc::<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>::region_slot(region_idx);
            let region_ptr = match self.queue.ensure_producer_region() {
                Ok(ptr) => ptr,
                Err(AcquireRegionError::CapacityFull) => {
                    return if offset == 0 {
                        Err(PushError::Full(()))
                    } else {
                        Ok(offset)
                    };
                }
            };
            let region = unsafe { &(*region_ptr).queue };
            match region.try_push_n(&src[offset..]) {
                Ok(pushed) if pushed > 0 => {
                    self.queue.publish_region(region_ptr, slot);
                    self.queue
                        .head_items
                        .fetch_add(pushed as u64, Ordering::Release);
                    offset += pushed;

                    if offset == src.len() {
                        return Ok(offset);
                    }

                    // Region may be full now; attempt to continue, rotating if necessary.
                    if region.is_full() {
                        self.queue.rotate_producer_region();
                    }
                }
                Ok(_) => {
                    self.queue.publish_region(region_ptr, slot);
                    self.queue.rotate_producer_region();
                }
                Err(PushError::Full(())) => {
                    self.queue.publish_region(region_ptr, slot);
                    self.queue.rotate_producer_region();
                    if offset == 0 {
                        return Err(PushError::Full(()));
                    } else {
                        return Ok(offset);
                    }
                }
                Err(PushError::Closed(())) => {
                    self.queue.publish_region(region_ptr, slot);
                    return Err(PushError::Closed(()));
                }
            }
        }

        Ok(offset)
    }
}

impl<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> LayeredReceiver<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>
{
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    pub fn try_pop(&self) -> Option<T> {
        loop {
            let state = &self.queue.consumer;
            let region_idx = unsafe { *state.region_index.get() };
            let slot = LayeredSpsc::<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>::region_slot(region_idx);

            let mut region_ptr = unsafe { *state.active_region.get() };
            if region_ptr.is_null() {
                region_ptr = self.queue.regions[slot].load(Ordering::Acquire);
                if region_ptr.is_null() {
                    return None;
                }
                unsafe { *state.active_region.get() = region_ptr };
            }

            let region = unsafe { &(*region_ptr).queue };
            if let Some(value) = region.try_pop() {
                self.queue.tail_items.fetch_add(1, Ordering::Release);
                return Some(value);
            }

            let producer_cursor = self.queue.producer_region_cursor.load(Ordering::Acquire);
            if producer_cursor == region_idx {
                return None;
            }

            if region.is_empty() {
                self.queue.release_consumer_region(slot, region_ptr);
                continue;
            } else {
                return None;
            }
        }
    }

    pub fn try_pop_n(&self, dst: &mut [T]) -> Result<usize, PopError> {
        if dst.is_empty() {
            return Ok(0);
        }

        let mut filled = 0usize;

        loop {
            let state = &self.queue.consumer;
            let region_idx = unsafe { *state.region_index.get() };
            let slot = LayeredSpsc::<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>::region_slot(region_idx);

            let mut region_ptr = unsafe { *state.active_region.get() };
            if region_ptr.is_null() {
                region_ptr = self.queue.regions[slot].load(Ordering::Acquire);
                if region_ptr.is_null() {
                    return if filled == 0 {
                        Err(PopError::Empty)
                    } else {
                        Ok(filled)
                    };
                }
                unsafe { *state.active_region.get() = region_ptr };
            }

            let region = unsafe { &(*region_ptr).queue };
            match region.try_pop_n(&mut dst[filled..]) {
                Ok(read) if read > 0 => {
                    self.queue
                        .tail_items
                        .fetch_add(read as u64, Ordering::Release);
                    filled += read;
                    if filled == dst.len() {
                        return Ok(filled);
                    }
                    if region.is_empty() {
                        self.queue.release_consumer_region(slot, region_ptr);
                    }
                }
                Ok(_) => {
                    let producer_cursor = self.queue.producer_region_cursor.load(Ordering::Acquire);
                    if producer_cursor == region_idx {
                        return if filled == 0 {
                            Err(PopError::Empty)
                        } else {
                            Ok(filled)
                        };
                    }
                    if region.is_empty() {
                        self.queue.release_consumer_region(slot, region_ptr);
                        continue;
                    }
                }
                Err(PopError::Empty) => {
                    let producer_cursor = self.queue.producer_region_cursor.load(Ordering::Acquire);
                    if producer_cursor == region_idx {
                        return if filled == 0 {
                            Err(PopError::Empty)
                        } else {
                            Ok(filled)
                        };
                    }
                    if region.is_empty() {
                        self.queue.release_consumer_region(slot, region_ptr);
                        continue;
                    }
                }
                Err(PopError::Timeout) => {
                    if filled == 0 {
                        return Err(PopError::Timeout);
                    } else {
                        return Ok(filled);
                    }
                }
                Err(PopError::Closed) => {
                    if filled == 0 {
                        return Err(PopError::Closed);
                    } else {
                        return Ok(filled);
                    }
                }
            }
        }
    }

    pub fn consume_in_place<F>(&self, max: usize, mut f: F) -> usize
    where
        F: FnMut(&[T]) -> usize,
    {
        if max == 0 {
            return 0;
        }

        let mut consumed_total = 0usize;

        loop {
            let state = &self.queue.consumer;
            let region_idx = unsafe { *state.region_index.get() };
            let slot = LayeredSpsc::<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>::region_slot(region_idx);

            let mut region_ptr = unsafe { *state.active_region.get() };
            if region_ptr.is_null() {
                region_ptr = self.queue.regions[slot].load(Ordering::Acquire);
                if region_ptr.is_null() {
                    return consumed_total;
                }
                unsafe { *state.active_region.get() = region_ptr };
            }

            let region = unsafe { &(*region_ptr).queue };
            let remaining = max - consumed_total;
            let consumed = region.consume_in_place(remaining, &mut f);
            if consumed == 0 {
                let producer_cursor = self.queue.producer_region_cursor.load(Ordering::Acquire);
                if producer_cursor == region_idx {
                    return consumed_total;
                }
                if region.is_empty() {
                    self.queue.release_consumer_region(slot, region_ptr);
                    continue;
                } else {
                    return consumed_total;
                }
            }

            self.queue
                .tail_items
                .fetch_add(consumed as u64, Ordering::Release);
            consumed_total += consumed;

            if consumed_total >= max {
                return consumed_total;
            }

            if region.is_empty() {
                self.queue.release_consumer_region(slot, region_ptr);
            }
        }
    }
}

impl<
    T: Copy,
    const P: usize,
    const NUM_SEGS_P2: usize,
    const NUM_REGIONS_P2: usize,
    S: SignalSchedule + Clone,
> Clone for LayeredReceiver<T, P, NUM_SEGS_P2, NUM_REGIONS_P2, S>
{
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

struct ProducerState<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> {
    region_index: UnsafeCell<u64>,
    active_region: UnsafeCell<*mut Region<T, P, NUM_SEGS_P2, S>>,
}

impl<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    ProducerState<T, P, NUM_SEGS_P2, S>
{
    const fn new() -> Self {
        Self {
            region_index: UnsafeCell::new(0),
            active_region: UnsafeCell::new(ptr::null_mut()),
        }
    }
}

unsafe impl<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Send
    for ProducerState<T, P, NUM_SEGS_P2, S>
{
}
unsafe impl<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Sync
    for ProducerState<T, P, NUM_SEGS_P2, S>
{
}

struct ConsumerState<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> {
    region_index: UnsafeCell<u64>,
    active_region: UnsafeCell<*mut Region<T, P, NUM_SEGS_P2, S>>,
}

impl<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    ConsumerState<T, P, NUM_SEGS_P2, S>
{
    const fn new() -> Self {
        Self {
            region_index: UnsafeCell::new(0),
            active_region: UnsafeCell::new(ptr::null_mut()),
        }
    }
}

unsafe impl<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Send
    for ConsumerState<T, P, NUM_SEGS_P2, S>
{
}
unsafe impl<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> Sync
    for ConsumerState<T, P, NUM_SEGS_P2, S>
{
}

struct Region<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone> {
    queue: Spsc<T, P, NUM_SEGS_P2, S>,
    next: AtomicPtr<Self>,
}

impl<T: Copy, const P: usize, const NUM_SEGS_P2: usize, S: SignalSchedule + Clone>
    Region<T, P, NUM_SEGS_P2, S>
{
    unsafe fn alloc(signal: &S, max_pooled_segments: usize) -> *mut Self {
        let queue =
            unsafe { Spsc::new_unsafe_with_gate_and_config(signal.clone(), max_pooled_segments) };
        Box::into_raw(Box::new(Self {
            queue,
            next: AtomicPtr::new(ptr::null_mut()),
        }))
    }

    unsafe fn free(ptr: *mut Self) {
        if ptr.is_null() {
            return;
        }
        drop(unsafe { Box::from_raw(ptr) });
    }
}

enum AcquireRegionError {
    CapacityFull,
}
