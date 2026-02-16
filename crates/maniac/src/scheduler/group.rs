use super::BiasState;
use super::ContractId;
use super::contract::{FutureContract, TypedFutureContract};
use super::error::{PanicHandler, SpawnError, default_panic_handler};
use super::join::{TypedTask, TypedTaskInner};
use super::waitable::Semaphore;
use crate::ptr::{StripedArc, StripedArcInner, StripedRefCount};
use crate::scheduler::signal::SignalLeaf;
use crate::signal_tree::summary::TreeSummary;
use crate::signal_tree::{INVALID_SIGNAL_INDEX, SignalTree64, SignalTreeOps};

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// Contract state - simple state machine
mod state {
    pub const IDLE: u64 = 0;
    pub const SCHEDULED: u64 = 1 << 62;
    pub const EXECUTING: u64 = 2 << 62;
    pub const STATE_MASK: u64 = 0x3 << 62;
    pub const GENERATION_MASK: u64 = !STATE_MASK;
}

/// Type alias for the boxed future - returns a type-erased result pointer
type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Waker data stored inline in each contract.
/// Uses packed ContractId (index + generation) for efficient storage.
#[repr(C)]
struct WakerData {
    /// Pointer to the StripedArcInner containing ContractGroupInner
    arc_inner: *const StripedArcInner<ContractGroupInner>,
    /// Packed contract ID containing both index and generation
    contract_id: ContractId,
}

impl WakerData {
    #[inline]
    fn group(&self) -> &ContractGroupInner {
        // Safety: arc_inner is valid while waker exists (refcount keeps it alive)
        unsafe { &(*self.arc_inner).data }
    }

    #[inline]
    fn refcount(&self) -> &StripedRefCount {
        // Safety: arc_inner is valid while waker exists
        unsafe { &(*self.arc_inner).refcount }
    }
}

// Safety: WakerData contains a pointer and ID, group outlives wakers via refcount
unsafe impl Send for WakerData {}
unsafe impl Sync for WakerData {}

const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    // clone
    |ptr| {
        let data = unsafe { &*(ptr as *const WakerData) };
        data.refcount().increment();
        RawWaker::new(ptr, &WAKER_VTABLE)
    },
    // wake (consumes waker)
    |ptr| {
        let data = unsafe { &*(ptr as *const WakerData) };
        data.group().schedule(data.contract_id);
    },
    // wake_by_ref
    |ptr| {
        let data = unsafe { &*(ptr as *const WakerData) };
        data.group().schedule(data.contract_id);
    },
    // drop
    |ptr| {
        let data = unsafe { &*(ptr as *const WakerData) };
        data.refcount().decrement();
    },
);

/// Internal contract data - cache line aligned to prevent false sharing
#[cfg_attr(target_arch = "x86_64", repr(C, align(64)))]
#[cfg_attr(
    any(
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64"
    ),
    repr(C, align(128))
)]
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "hexagon",
    ),
    repr(C, align(32))
)]
#[cfg_attr(target_arch = "m68k", repr(C, align(16)))]
#[cfg_attr(target_arch = "s390x", repr(C, align(256)))]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "hexagon",
        target_arch = "m68k",
        target_arch = "s390x",
    )),
    repr(C, align(64))
)]
pub struct Contract {
    /// Combined state (bits 62-63) and generation (bits 0-31)
    state_and_generation: AtomicU64,
    /// Embedded waker data - avoids allocation on each poll
    waker_data: WakerData,
    /// Single future to poll (None when slot is empty)
    future: UnsafeCell<Option<BoxFuture>>,
    /// Waker for join handle - stored inline for zero allocation
    join_waker: UnsafeCell<Option<Waker>>,
}

// Safety: Contract uses atomic state for synchronization
unsafe impl Send for Contract {}
unsafe impl Sync for Contract {}

impl Contract {
    fn new() -> Self {
        Self {
            state_and_generation: AtomicU64::new(state::IDLE),
            waker_data: WakerData {
                arc_inner: std::ptr::null(),
                contract_id: ContractId::invalid(),
            },
            future: UnsafeCell::new(None),
            join_waker: UnsafeCell::new(None),
        }
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.waker_data.contract_id.index as usize
    }

    #[inline]
    pub fn generation(&self) -> u64 {
        self.state_and_generation.load(Ordering::Relaxed) & state::GENERATION_MASK
    }

    #[inline(always)]
    fn init_waker(&mut self, arc_inner: *const StripedArcInner<ContractGroupInner>, index: u32) {
        self.waker_data.arc_inner = arc_inner;
        self.waker_data.contract_id.index = index;
    }

    #[inline(always)]
    unsafe fn set_waker_generation(&self, generation: u64) {
        let waker_data = &self.waker_data as *const WakerData as *mut WakerData;
        unsafe { (*waker_data).contract_id.generation = generation };
    }

    #[inline(always)]
    fn waker(&self) -> Waker {
        unsafe {
            Waker::from_raw(RawWaker::new(
                &self.waker_data as *const WakerData as *const (),
                &WAKER_VTABLE,
            ))
        }
    }

    #[inline(always)]
    unsafe fn get_future(&self) -> &mut BoxFuture {
        unsafe { (*self.future.get()).as_mut().expect("future not set") }
    }

    #[inline(always)]
    unsafe fn set_future(&self, future: BoxFuture) {
        unsafe { *self.future.get() = Some(future) };
    }

    #[inline(always)]
    unsafe fn clear_future(&self) {
        unsafe { *self.future.get() = None };
    }

    #[inline(always)]
    unsafe fn set_join_waker(&self, waker: Option<Waker>) {
        unsafe { *self.join_waker.get() = waker };
    }

    #[inline(always)]
    unsafe fn take_join_waker(&self) -> Option<Waker> {
        unsafe { (*self.join_waker.get()).take() }
    }
}

/// Default signal tree capacity
const SIGNAL_LEAF_CAPACITY: u64 = 64;
const SIGNAL_LEAF_MASK: u64 = 63;

/// Default future contract group capacity
const DEFAULT_CAPACITY: u64 = 4096;

/// Inner state of a future contract group
pub struct ContractGroupInner {
    blocking: bool,
    force_hybrid: bool,
    use_summary: bool,
    sub_tree_mask: u64,
    signal_trees: Box<[SignalLeaf]>,
    available_trees: Box<[SignalLeaf]>,
    contracts: Box<[Contract]>,
    thread_counter: AtomicU64,
    stopped: AtomicBool,
    summaries: Box<[SignalLeaf]>,
    /// Semaphore for blocking - counts summary words that are non-zero
    semaphore: Semaphore,
    panic_handler: PanicHandler,
}

impl ContractGroupInner {
    fn new(
        capacity: u64,
        blocking: bool,
        force_hybrid: bool,
        force_summary: bool,
        panic_handler: PanicHandler,
    ) -> Self {
        let sub_tree_count = crate::signal_tree::minimum_power_of_two(
            (capacity + SIGNAL_LEAF_CAPACITY - 1) / SIGNAL_LEAF_CAPACITY,
        );
        let sub_tree_mask = sub_tree_count - 1;
        let total_capacity = sub_tree_count * SIGNAL_LEAF_CAPACITY;

        let mut signal_trees = Vec::with_capacity(sub_tree_count as usize);
        let mut available_trees = Vec::with_capacity(sub_tree_count as usize);

        for _ in 0..sub_tree_count {
            signal_trees.push(SignalLeaf::new());
            let available = SignalLeaf::new();
            for i in 0..SIGNAL_LEAF_CAPACITY {
                available.set(i);
            }
            available_trees.push(available);
        }

        let summary_leaf_count = sub_tree_count / 64;
        let mut summaries = Vec::with_capacity(summary_leaf_count as usize);
        for _ in 0..summary_leaf_count {
            summaries.push(SignalLeaf::new());
        }

        let mut contracts = Vec::with_capacity(total_capacity as usize);
        for _ in 0..total_capacity {
            contracts.push(Contract::new());
        }

        Self {
            blocking,
            force_hybrid,
            use_summary: force_summary,
            sub_tree_mask,
            signal_trees: signal_trees.into_boxed_slice(),
            available_trees: available_trees.into_boxed_slice(),
            contracts: contracts.into_boxed_slice(),
            thread_counter: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            summaries: summaries.into_boxed_slice(),
            semaphore: Semaphore::new(),
            panic_handler,
        }
    }

    pub(crate) fn new_bias_state(&self) -> BiasState {
        let mut state = BiasState::new();
        state.thread_id = self.thread_counter.fetch_add(1, Ordering::Relaxed);
        state
    }

    pub(crate) fn drop_bias_state(&self, bias: &mut BiasState) {
        self.thread_counter.fetch_sub(1, Ordering::Relaxed);
    }

    fn init_wakers(arc: &StripedArc<Self>) {
        let arc_inner_ptr = arc.inner_ptr();
        for (index, contract) in arc.contracts.iter().enumerate() {
            let contract_ptr = contract as *const Contract as *mut Contract;
            unsafe {
                (*contract_ptr).init_waker(arc_inner_ptr, index as u32);
            }
        }
    }

    #[inline(always)]
    fn get_tree_and_signal_index(&self, index: u32) -> (u64, u64) {
        let index = index as u64;
        (index / SIGNAL_LEAF_CAPACITY, index & SIGNAL_LEAF_MASK)
    }

    /// Schedule a contract for polling. Checks generation to ignore stale wakers.
    #[inline]
    pub fn schedule(&self, id: ContractId) {
        let contract = &self.contracts[id.index as usize];

        let combined = contract.state_and_generation.load(Ordering::Acquire);
        let generation = combined & state::GENERATION_MASK;
        let scheduled = (combined & state::SCHEDULED) != 0;

        // Quick check: already scheduled or generation mismatch (stale waker)
        if scheduled || generation != id.generation {
            return;
        }

        let prev_combined = contract
            .state_and_generation
            .fetch_or(state::SCHEDULED, Ordering::AcqRel);

        let prev_scheduled = (prev_combined & state::SCHEDULED) != 0;
        let prev_executing = (prev_combined & state::EXECUTING) != 0;
        let prev_gen = prev_combined & state::GENERATION_MASK;

        // Set signal only if:
        // 1. We were the one who set SCHEDULED (was not already set)
        // 2. Task is not currently executing (finish_execution will handle signal)
        // 3. Generation still matches (not a stale waker)
        if !prev_scheduled && !prev_executing && prev_gen == id.generation {
            self.set_contract_signal(id.index);
        }
    }

    #[inline(always)]
    fn set_contract_signal(&self, index: u32) {
        let (tree_index, bit_index) = self.get_tree_and_signal_index(index);
        let (tree_was_empty, _) = self.signal_trees[tree_index as usize].set(bit_index);

        if tree_was_empty && self.use_summary {
            let (was_set, was_empty) =
                self.summaries[(tree_index as usize) / 64].set(tree_index & SIGNAL_LEAF_MASK);
            // if self.blocking && was_set && was_empty {
            //     // Signal semaphore when a tree transitions from empty to non-empty
            //     // self.semaphore.release();
            // }
        }

        // Wake a thread for every new signal
        if self.blocking {
            self.semaphore.release();
        }
    }

    #[inline(always)]
    fn set_contract_signal_yielded(&self, index: u32) {
        let (tree_index, bit_index) = self.get_tree_and_signal_index(index);
        let (tree_was_empty, _) = self.signal_trees[tree_index as usize].set(bit_index);

        if tree_was_empty && self.use_summary {
            let (was_set, was_empty) =
                self.summaries[(tree_index as usize) / 64].set(tree_index & SIGNAL_LEAF_MASK);
            // if self.blocking && was_set && was_empty {
            //     // Signal semaphore when a tree transitions from empty to non-empty
            //     // self.semaphore.release();
            // }
        }

        // Wake a thread to pick up the re-scheduled task
        // if self.blocking {
        //     self.semaphore.release();
        // }
    }

    pub fn get_available_contract(&self, bias: &mut BiasState) -> Option<u32> {
        let sub_tree_mask = self.sub_tree_mask;

        // Use BiasState's alloc_counter for round-robin tree selection
        let counter = bias.alloc_counter;
        if bias.alloc_counter == 0 {
            bias.alloc_counter = 1;
        }

        let hint = crate::util::random::next_u64();

        for i in 0..self.available_trees.len() * 128 {
            // let sub_tree_index = bias.alloc_counter & sub_tree_mask;
            // let sub_tree_index = bias.alloc_counter & sub_tree_mask;
            // let sub_tree_index =
            // crate::util::random::rapidrng_fast(&mut bias.alloc_counter) & sub_tree_mask;
            let sub_tree_index = counter.wrapping_add(i as u64) & sub_tree_mask;
            // let sub_tree_index = crate::util::random::next_u64() & sub_tree_mask;

            // let bit_index = self.available_trees[sub_tree_index as usize].acquire_largest();
            let (bit_index, _, _) =
                self.available_trees[sub_tree_index as usize].acquire_with_bias(hint, hint);
            if bit_index != SignalLeaf::INVALID {
                // println!(
                //     "available contract: {}  leaf_index: {}  bit_index: {}",
                //     ((sub_tree_index * 64) + bit_index) as u32,
                //     sub_tree_index,
                //     bit_index
                // );
                bias.alloc_counter += 1;
                return Some(((sub_tree_index * 64) + bit_index) as u32);
            }

            bias.alloc_counter += 1;
        }

        None
    }

    pub fn release_available_contract(&self, index: u32) {
        let (tree_index, signal_index) = self.get_tree_and_signal_index(index);
        self.available_trees[tree_index as usize].set(signal_index);
    }

    /// Transition from EXECUTING back to IDLE or SCHEDULED.
    #[inline(always)]
    fn finish_execution(&self, index: u32, contract: &Contract) {
        // Clear EXECUTING bit: EXECUTING|SCHEDULED -> SCHEDULED, EXECUTING -> IDLE
        let prev_combined = contract
            .state_and_generation
            .fetch_and(!state::EXECUTING, Ordering::AcqRel);

        let had_scheduled = (prev_combined & state::SCHEDULED) != 0;

        // If the contract was EXECUTING|SCHEDULED, it needs to be polled again.
        // Set signal so a worker can pick it up.
        if had_scheduled {
            self.set_contract_signal_yielded(index);
        }
    }

    fn wake_joiner(&self, contract: &Contract) {
        unsafe {
            if let Some(waker) = contract.take_join_waker() {
                waker.wake();
            }
        }
    }

    pub(crate) unsafe fn release_contract(&self, index: u32) {
        let contract = &self.contracts[index as usize];
        self.erase_contract(index, contract);
    }

    /// Reset contract to IDLE and increment generation.
    fn erase_contract(&self, index: u32, contract: &Contract) {
        let current = contract.state_and_generation.load(Ordering::Acquire);
        // Clear state bits and increment generation in single swap
        let new_val = (current & state::GENERATION_MASK) + 1;
        contract
            .state_and_generation
            .store(new_val, Ordering::Release);

        let (tree_index, signal_index) = self.get_tree_and_signal_index(index);

        // Clear the signal tree bit to prevent stale work from being picked up
        // self.signal_trees[tree_index as usize].clear_bit(signal_index);

        unsafe {
            contract.clear_future();
            contract.set_join_waker(None);
        }

        self.available_trees[tree_index as usize].set(signal_index);
    }

    #[inline]
    pub fn is_completed(&self, index: u32) -> bool {
        let combined = self.contracts[index as usize]
            .state_and_generation
            .load(Ordering::Acquire);
        let state = (combined >> 62) & 0x3;
        state == (state::IDLE >> 62)
    }

    /// Register a waker to be called when the contract completes.
    /// Returns true if already completed.
    pub fn register_join_waker(&self, index: u32, waker: &Waker) -> bool {
        let contract = &self.contracts[index as usize];

        let current_combined = contract.state_and_generation.load(Ordering::Acquire);
        let current_state = (current_combined >> 62) & 0x3;
        if current_state == (state::IDLE >> 62) {
            return true;
        }

        unsafe {
            contract.set_join_waker(Some(waker.clone()));
        }

        // Re-check after storing to handle completion race
        let current_combined = contract.state_and_generation.load(Ordering::Acquire);
        let current_state = (current_combined >> 62) & 0x3;
        if current_state == (state::IDLE >> 62) {
            unsafe {
                if let Some(w) = contract.take_join_waker() {
                    w.wake();
                }
            }
            return true;
        }

        false
    }

    fn process_panic(&self, id: ContractId, exception: Box<dyn std::any::Any + Send>) {
        (self.panic_handler)(id, exception);
    }

    /// Transition SCHEDULED -> EXECUTING and poll the contract.
    /// Returns true if contract was processed, false if another worker took it.
    #[inline]
    fn process_contract(&self, index: u32, contract: &Contract) -> bool {
        let mut current = contract.state_and_generation.load(Ordering::Acquire);

        // loop {
        //     // If already executing or not scheduled, another thread took it
        //     if (current & state::EXECUTING) != 0 || (current & state::SCHEDULED) == 0 {
        //         return false;
        //     }

        //     match contract.state_and_generation.compare_exchange_weak(
        //         current,
        //         new_state,
        //         Ordering::AcqRel,
        //         Ordering::Acquire,
        //     ) {
        //         Ok(_) => break,
        //         Err(actual) => current = actual,
        //     }
        // }

        if (current & state::EXECUTING) != 0 || (current & state::SCHEDULED) == 0 {
            return false;
        }

        let new_state = (current | state::EXECUTING) & !state::SCHEDULED;
        contract
            .state_and_generation
            .swap(new_state, Ordering::Release);

        self.poll_contract(index, contract);
        true
    }

    #[inline]
    fn poll_contract(&self, index: u32, contract: &Contract) {
        let waker = std::mem::ManuallyDrop::new(contract.waker());
        let mut cx = Context::from_waker(&waker);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            contract.get_future().as_mut().poll(&mut cx)
        }));

        match result {
            Ok(Poll::Ready(())) => {
                self.wake_joiner(contract);
                self.erase_contract(index, contract);
            }
            Ok(Poll::Pending) => {
                self.finish_execution(index, contract);
            }
            Err(e) => {
                self.wake_joiner(contract);
                self.erase_contract(index, contract);
                let generation =
                    contract.state_and_generation.load(Ordering::Relaxed) & state::GENERATION_MASK;
                self.process_panic(ContractId::new(index, generation), e);
            }
        }
    }

    const SUMMARY_SELECTION_THRESHOLD: usize = 16;

    #[inline]
    fn poll_next_contract_with_bias_inner(
        &self,
        bias: &mut BiasState,
        was_woken: bool,
    ) -> Option<&'_ Contract> {
        // self.poll_next_linear_scan(bias, was_woken)
        self.poll_next_linear_random_scan(bias, was_woken)
    }

    #[inline]
    fn poll_next_linear_scan(&self, bias: &mut BiasState, was_woken: bool) -> Option<&Contract> {
        // crate::util::random::rapidrng_fast(&mut bias.flags);
        // crate::util::random::rapidrng_fast(&mut bias.hint);

        // Match work_contract's execute_next_contract_with_bias pattern
        let sub_tree_mask = self.sub_tree_mask;
        let mut sub_tree_index = bias.flags / 64;

        let mut summary_value = 0u64;
        let mut summary_index = u64::MAX;
        let mut loaded_summary_index = u64::MAX;

        for _ in 0..self.signal_trees.len() {
            sub_tree_index &= sub_tree_mask;

            if self.use_summary {
                summary_index = sub_tree_index / 64;
                // if loaded_summary_index != summary_index {
                //     let summary = &self.tree_summary.words[summary_index as usize];
                //     summary_value = summary.load(Ordering::Acquire);
                //     loaded_summary_index = summary_index;
                // }

                // let summary_counters = summary.load(Ordering::Relaxed);
                if self.summaries[summary_index as usize].load() == 0 {
                    // if summary_value == 0 {
                    sub_tree_index = (summary_index + 1) * 64;
                    bias.flags = sub_tree_index * 64;
                    continue;
                }
            }

            let (signal_index, tree_is_empty, new_hint) =
                self.signal_trees[sub_tree_index as usize].acquire_with_bias(bias.flags, bias.hint);

            if signal_index != SignalLeaf::INVALID {
                let contract_id = sub_tree_index * 64 + signal_index;

                // bias advancement
                let x = new_hint ^ bias.flags;
                let b = (x & (!x).wrapping_add(1)) & 63;
                if b == 0 {
                    bias.flags = (sub_tree_index + 1) * 64;
                } else {
                    bias.flags |= b;
                    bias.flags &= !(b - 1);
                }
                bias.hint = new_hint;

                // Clear summary bit if tree became empty.
                // Important that this is done periodically to avoid excessive contention.
                // This is ok because a false positive is harmless, just temporarily more
                // scanning.
                if self.use_summary
                    && tree_is_empty
                    && crate::util::random::rapidrng_fast(&mut bias.summary_hint) & 511 == 0
                {
                    self.summaries[summary_index as usize].clear(sub_tree_index & SIGNAL_LEAF_MASK);
                    if !self.signal_trees[sub_tree_index as usize].is_empty() {
                        self.summaries[summary_index as usize]
                            .set(sub_tree_index & SIGNAL_LEAF_MASK);
                        if self.blocking {
                            self.semaphore.release();
                        }
                    }
                }

                // Chained wakeup: if we were woken and tree still has work, wake another
                if !tree_is_empty && self.blocking && was_woken {
                    self.semaphore.release();
                }

                let contract = &self.contracts[contract_id as usize];
                if self.process_contract(contract_id as u32, contract) {
                    return Some(contract);
                }
            }

            if self.use_summary
                && crate::util::random::rapidrng_fast(&mut bias.summary_hint) & 511 == 0
                && self.summaries[summary_index as usize].load() == 0
            {
                self.summaries[summary_index as usize].clear(sub_tree_index & SIGNAL_LEAF_MASK);
                if !self.signal_trees[sub_tree_index as usize].is_empty() {
                    self.summaries[summary_index as usize].set(sub_tree_index & SIGNAL_LEAF_MASK);
                    if self.blocking {
                        self.semaphore.release();
                    }
                }
            }

            sub_tree_index += 1;
            bias.flags = sub_tree_index * 64;
        }

        None
    }

    #[inline]
    fn poll_next_linear_random_scan(
        &self,
        bias: &mut BiasState,
        was_woken: bool,
    ) -> Option<&Contract> {
        // crate::util::random::rapidrng_fast(&mut bias.flags);
        // crate::util::random::rapidrng_fast(&mut bias.hint);

        // Match work_contract's execute_next_contract_with_bias pattern
        let sub_tree_mask = self.sub_tree_mask;
        let mut sub_tree_index = bias.flags / 64;

        let mut summary_value = 0u64;
        let mut summary_index = u64::MAX;
        let mut loaded_summary_index = u64::MAX;

        for _ in 0..self.signal_trees.len() {
            sub_tree_index &= sub_tree_mask;
            // sub_tree_index = crate::util::random::rapidrng_fast(&mut bias.flags) & sub_tree_mask;

            if self.use_summary {
                summary_index = sub_tree_index / 64;
                // if loaded_summary_index != summary_index {
                //     let summary = &self.tree_summary.words[summary_index as usize];
                //     summary_value = summary.load(Ordering::Acquire);
                //     loaded_summary_index = summary_index;
                // }

                // let summary_counters = summary.load(Ordering::Relaxed);
                if self.summaries[summary_index as usize].load() == 0 {
                    // if summary_value == 0 {
                    sub_tree_index = (summary_index + 1) * 64;
                    bias.flags = sub_tree_index * 64;
                    continue;
                }
            }

            let (signal_index, tree_is_empty, new_hint) =
                self.signal_trees[sub_tree_index as usize].acquire_with_bias(bias.flags, bias.hint);

            if signal_index != SignalLeaf::INVALID {
                let contract_id = sub_tree_index * 64 + signal_index;

                // bias advancement
                let x = new_hint ^ bias.flags;
                let b = (x & (!x).wrapping_add(1)) & 63;
                if b == 0 {
                    bias.flags = (sub_tree_index + 1) * 64;
                } else {
                    bias.flags |= b;
                    bias.flags &= !(b - 1);
                }
                bias.hint = new_hint;

                // Clear summary bit if tree became empty.
                // Important that this is done periodically to avoid excessive contention.
                // This is ok because a false positive is harmless, just temporarily more
                // scanning.
                if self.use_summary
                    && tree_is_empty
                    && crate::util::random::rapidrng_fast(&mut bias.summary_hint) & 511 == 0
                {
                    self.summaries[summary_index as usize].clear(sub_tree_index & SIGNAL_LEAF_MASK);
                    if !self.signal_trees[sub_tree_index as usize].is_empty() {
                        self.summaries[summary_index as usize]
                            .set(sub_tree_index & SIGNAL_LEAF_MASK);
                        if self.blocking {
                            self.semaphore.release();
                        }
                    }
                }

                // Chained wakeup: if we were woken and tree still has work, wake another
                if !tree_is_empty && self.blocking && was_woken {
                    self.semaphore.release();
                }

                let contract = &self.contracts[contract_id as usize];
                if self.process_contract(contract_id as u32, contract) {
                    return Some(contract);
                }
            } else {
                // crate::util::random::rapidrng_fast(&mut bias.flags);
                // crate::util::random::rapidrng_fast(&mut bias.hint);
            }

            if self.use_summary
                && crate::util::random::rapidrng_fast(&mut bias.summary_hint) & 511 == 0
                && self.summaries[summary_index as usize].load() == 0
            {
                self.summaries[summary_index as usize].clear(sub_tree_index & SIGNAL_LEAF_MASK);
                if !self.signal_trees[sub_tree_index as usize].is_empty() {
                    self.summaries[summary_index as usize].set(sub_tree_index & SIGNAL_LEAF_MASK);
                    if self.blocking {
                        self.semaphore.release();
                    }
                }
            }

            sub_tree_index += 1;
            bias.flags = sub_tree_index * 64;
        }

        None
    }

    #[inline]
    fn poll_next_contract_with_bias(&self, bias: &mut BiasState) -> Option<&Contract> {
        // Try to find work without blocking first
        let result = self.poll_next_contract_with_bias_inner(bias, false);
        if result.is_some() {
            return result;
        }

        if !self.blocking {
            return None;
        }

        // Block on semaphore until there's work
        for _ in 0..SIGNAL_LEAF_CAPACITY {
            if !self.semaphore.acquire(&self.stopped) {
                return None; // Stopped
            }

            // Semaphore acquired - try to find work (pass was_woken=true for chained wakeup)
            let result = self.poll_next_contract_with_bias_inner(bias, true);
            if result.is_some() {
                return result;
            }

            std::hint::spin_loop();
            // std::thread::yield_now();
            // Lost the race, loop and wait again
        }

        None
    }

    fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            if self.blocking {
                self.semaphore.release_all();
            }
        }
    }

    fn poll_next_timeout(
        &self,
        bias: &mut BiasState,
        timeout: std::time::Duration,
    ) -> Option<&Contract> {
        let result = self.poll_next_contract_with_bias_inner(bias, false);
        if result.is_some() {
            return result;
        }

        if !self.semaphore.acquire_timeout(&self.stopped, timeout) {
            return None;
        }

        self.poll_next_contract_with_bias_inner(bias, true)
    }
}

#[derive(Clone)]
pub struct ContractGroup {
    pub inner: StripedArc<ContractGroupInner>,
}

impl ContractGroup {
    pub fn with_capacity(capacity: u64) -> Self {
        Self::with_capacity_and_options(capacity, false, Box::new(default_panic_handler))
    }

    pub fn with_capacity_blocking(capacity: u64) -> Self {
        Self::with_capacity_and_options(capacity, true, Box::new(default_panic_handler))
    }

    pub fn with_capacity_and_options(
        capacity: u64,
        blocking: bool,
        panic_handler: PanicHandler,
    ) -> Self {
        Self::with_all_options(capacity, blocking, false, false, panic_handler)
    }

    pub fn with_all_options(
        capacity: u64,
        blocking: bool,
        force_hybrid: bool,
        force_summary: bool,
        panic_handler: PanicHandler,
    ) -> Self {
        let inner = StripedArc::new(ContractGroupInner::new(
            capacity,
            blocking,
            force_hybrid,
            force_summary,
            panic_handler,
        ));
        ContractGroupInner::init_wakers(&inner);
        Self { inner }
    }

    pub fn new_bias_state(&self) -> BiasState {
        self.inner.new_bias_state()
    }

    pub(crate) fn drop_bias_state(&self, bias: &mut BiasState) {
        self.inner.drop_bias_state(bias)
    }

    pub fn spawn_unscheduled<F>(&self, future: F) -> Result<FutureContract, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn(future, false)
    }

    pub fn spawn<F>(&self, future: F, scheduled: bool) -> Result<FutureContract, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        thread_local! {
            static TLS_BIAS: std::cell::RefCell<BiasState> = std::cell::RefCell::new(BiasState::new());
        }
        let index = TLS_BIAS
            .with(|cell| {
                let mut bias = cell.borrow_mut();
                self.inner.get_available_contract(&mut bias)
            })
            .ok_or(SpawnError::NoCapacity)?;
        let contract = &self.inner.contracts[index as usize];

        unsafe {
            contract.set_future(Box::pin(future));
        }

        let id = ContractId::new(
            index,
            contract.state_and_generation.load(Ordering::Relaxed) & state::GENERATION_MASK,
        );

        unsafe {
            contract.set_waker_generation(id.generation);
        }

        let fc = FutureContract::with_params(self.inner.clone(), id);

        if scheduled {
            // self.inner.poll_contract(index, contract);
            fc.schedule();
        }

        Ok(fc)
    }

    pub fn spawn_typed<F, T>(
        &self,
        future: F,
        scheduled: bool,
    ) -> Result<TypedFutureContract<F, T>, SpawnError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Single Arc allocation contains both the future and the JoinSlot.
        // The Arc is shared between the TypedTask (which executes) and
        // TypedFutureContract (which reads the result).
        let inner = Arc::new(TypedTaskInner::new(future));
        let task = TypedTask::new(Arc::clone(&inner));

        // Spawn the task
        thread_local! {
            static TLS_BIAS: std::cell::RefCell<BiasState> = std::cell::RefCell::new(BiasState::new());
        }
        let index = TLS_BIAS
            .with(|cell| {
                let mut bias = cell.borrow_mut();
                self.inner.get_available_contract(&mut bias)
            })
            .ok_or(SpawnError::NoCapacity)?;
        let contract = &self.inner.contracts[index as usize];

        unsafe {
            contract.set_future(Box::pin(task));
        }

        let id = ContractId::new(
            index,
            contract.state_and_generation.load(Ordering::Relaxed) & state::GENERATION_MASK,
        );

        unsafe {
            contract.set_waker_generation(id.generation);
        }

        let fc = FutureContract::with_params(self.inner.clone(), id);

        if scheduled {
            fc.schedule();
        }

        // The slot is kept alive by the Arc which is held by both the task
        // and the TypedFutureContract. Even after the task completes and is
        // dropped, the TypedFutureContract's Arc keeps the slot alive.
        Ok(TypedFutureContract::new(fc, inner))
    }

    #[inline]
    pub fn poll_next(&self, bias: &mut BiasState) -> Option<&Contract> {
        self.inner.poll_next_contract_with_bias(bias)
    }

    #[inline]
    pub fn poll_next_timeout(
        &self,
        bias: &mut BiasState,
        timeout: std::time::Duration,
    ) -> Option<&Contract> {
        self.inner.poll_next_timeout(bias, timeout)
    }

    pub fn stop(&self) {
        self.inner.stop();
    }

    pub fn is_stopped(&self) -> bool {
        self.inner.stopped.load(Ordering::Acquire)
    }

    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn new_blocking() -> Self {
        Self::with_capacity_blocking(DEFAULT_CAPACITY)
    }

    pub fn new_blocking_with_capacity(capacity: u64) -> Self {
        Self::with_capacity_blocking(capacity)
    }

    pub fn with_panic_handler(panic_handler: PanicHandler) -> Self {
        Self::with_capacity_and_options(DEFAULT_CAPACITY, false, panic_handler)
    }

    /// Set a callback to be invoked when work is available but no threads are waiting.
    ///
    /// # Safety
    /// Must be called before any threads start polling this group.
    /// The caller must ensure that `data` remains valid for the lifetime of this group.
    pub unsafe fn set_spawn_callback(
        &self,
        callback: super::waitable::SpawnCallbackFn,
        data: *const (),
        max_threads: usize,
    ) {
        unsafe {
            self.inner
                .semaphore
                .set_spawn_callback(callback, data, max_threads)
        };
    }

    /// Increment the active thread count. Returns the previous count.
    #[inline]
    pub fn add_thread(&self) -> usize {
        self.inner.semaphore.add_thread()
    }

    /// Decrement the active thread count. Returns the previous count.
    #[inline]
    pub fn remove_thread(&self) -> usize {
        self.inner.semaphore.remove_thread()
    }

    /// Get the current active thread count.
    #[inline]
    pub fn active_threads(&self) -> usize {
        self.inner.semaphore.active_threads()
    }

    /// Get the current number of waiting threads (idle).
    #[inline]
    pub fn idle_threads(&self) -> u64 {
        self.inner.semaphore.waiters()
    }
}

impl Default for ContractGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ContractGroup {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_create_and_poll_contract() {
        let group = ContractGroup::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = Arc::clone(&counter);
        let _contract = group.spawn(
            async move {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            },
            true,
        );

        let mut bias = BiasState::new();
        let result = group.poll_next(&mut bias);
        assert!(result.is_some());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_waker_reschedules() {
        let group = ContractGroup::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(AtomicBool::new(false));

        let counter_clone = Arc::clone(&counter);
        let ready_clone = Arc::clone(&ready);

        let _contract = group.spawn(
            std::future::poll_fn(move |cx| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                if ready_clone.load(Ordering::Relaxed) {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }),
            true,
        );

        let mut bias = BiasState::new();

        group.poll_next(&mut bias);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        group.poll_next(&mut bias);
        assert_eq!(counter.load(Ordering::Relaxed), 2);

        ready.store(true, Ordering::Relaxed);
        group.poll_next(&mut bias);
        assert_eq!(counter.load(Ordering::Relaxed), 3);

        let result = group.poll_next(&mut bias);
        assert!(result.is_none());
    }

    #[test]
    fn test_custom_panic_handler() {
        let panic_count = Arc::new(AtomicUsize::new(0));
        let panic_count_clone = Arc::clone(&panic_count);

        let group = ContractGroup::with_panic_handler(Box::new(move |_id, _e| {
            panic_count_clone.fetch_add(1, Ordering::Relaxed);
        }));

        let _contract = group
            .spawn(
                async {
                    panic!("test panic");
                },
                true,
            )
            .unwrap();

        let mut bias = BiasState::new();
        group.poll_next(&mut bias);
        assert_eq!(panic_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_blocking_group() {
        use std::thread;
        use std::time::Duration;

        let group = Arc::new(ContractGroup::new_blocking());
        let counter = Arc::new(AtomicUsize::new(0));

        let group_clone = Arc::clone(&group);
        let counter_clone = Arc::clone(&counter);
        let handle = thread::spawn(move || {
            let mut bias = BiasState::new();
            while !group_clone.is_stopped() {
                let result = group_clone.poll_next(&mut bias);
                if result.is_none() {
                    break;
                }
            }
            counter_clone.load(Ordering::Relaxed)
        });

        thread::sleep(Duration::from_millis(10));

        let counter_clone = Arc::clone(&counter);
        let _contract = group
            .spawn(
                async move {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                },
                true,
            )
            .unwrap();

        thread::sleep(Duration::from_millis(10));

        group.stop();

        let final_count = handle.join().unwrap();

        assert_eq!(final_count, 1);
    }

    #[test]
    fn test_blocking_timeout() {
        use std::time::Duration;

        let group = ContractGroup::new_blocking();
        let mut bias = BiasState::new();

        let start = std::time::Instant::now();
        let result = group.poll_next_timeout(&mut bias, Duration::from_millis(50));
        let elapsed = start.elapsed();

        assert!(result.is_none());
        assert!(elapsed >= Duration::from_millis(40));
    }

    #[test]
    fn test_join_handle() {
        use std::thread;

        let group = Arc::new(ContractGroup::new());
        let completed = Arc::new(AtomicBool::new(false));

        let completed_clone = Arc::clone(&completed);
        let contract = group
            .spawn(
                async move {
                    completed_clone.store(true, Ordering::Release);
                },
                true,
            )
            .unwrap();

        let join_handle = contract.join();

        let group_clone = Arc::clone(&group);
        let poll_handle = thread::spawn(move || {
            let mut bias = BiasState::new();
            while group_clone.poll_next(&mut bias).is_some() {}
        });

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(&waker);

        loop {
            let mut pinned = std::pin::pin!(join_handle);
            match pinned.as_mut().poll(&mut cx) {
                Poll::Ready(()) => break,
                Poll::Pending => {
                    thread::yield_now();
                }
            }
            break;
        }

        poll_handle.join().unwrap();

        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn test_join_already_completed() {
        let group = ContractGroup::new();
        let completed = Arc::new(AtomicBool::new(false));

        let completed_clone = Arc::clone(&completed);
        let contract = group
            .spawn(
                async move {
                    completed_clone.store(true, Ordering::Release);
                },
                true,
            )
            .unwrap();

        let mut bias = BiasState::new();
        group.poll_next(&mut bias);

        assert!(completed.load(Ordering::Acquire));

        let join_handle = contract.join();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(&waker);
        let pinned = std::pin::pin!(join_handle);

        match pinned.poll(&mut cx) {
            Poll::Ready(()) => {}
            Poll::Pending => panic!("Join should be ready for completed contract"),
        }
    }

    #[test]
    fn test_typed_contract() {
        let group = ContractGroup::new();

        let contract = group.spawn_typed(async { 42i32 }, true).unwrap();

        let mut bias = BiasState::new();
        group.poll_next(&mut bias);

        assert!(contract.is_completed());

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(&waker);
        let pinned = std::pin::pin!(contract);

        match pinned.poll(&mut cx) {
            Poll::Ready(value) => assert_eq!(value, 42),
            Poll::Pending => panic!("Typed contract should be ready"),
        }
    }

    #[test]
    fn test_typed_contract_with_string() {
        let group = ContractGroup::new();

        let contract = group
            .spawn_typed(async { String::from("hello world") }, true)
            .unwrap();

        let mut bias = BiasState::new();
        group.poll_next(&mut bias);

        assert!(contract.is_completed());

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(&waker);
        let pinned = std::pin::pin!(contract);

        match pinned.poll(&mut cx) {
            Poll::Ready(value) => assert_eq!(value, "hello world"),
            Poll::Pending => panic!("Typed contract should be ready"),
        }
    }

    #[test]
    fn test_typed_contract_not_yet_complete() {
        let group = ContractGroup::new();
        let ready = Arc::new(AtomicBool::new(false));

        let ready_clone = Arc::clone(&ready);
        let mut contract = group
            .spawn_typed(
                async move {
                    while !ready_clone.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    123i32
                },
                true,
            )
            .unwrap();

        assert!(!contract.is_completed());

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(&waker);

        {
            let pinned = std::pin::Pin::new(&mut contract);
            match pinned.poll(&mut cx) {
                Poll::Ready(_) => panic!("Should be pending"),
                Poll::Pending => {}
            }
        }
    }

    #[test]
    fn test_tree_summary_race_condition() {
        use std::thread;
        use std::time::Duration;

        const NUM_WORKERS: usize = 4;
        const TOTAL_CONTRACTS: usize = 1000;

        let group = Arc::new(ContractGroup::with_capacity_blocking(
            (TOTAL_CONTRACTS + 64) as u64,
        ));
        let completed_count = Arc::new(AtomicU64::new(0));

        for _ in 0..TOTAL_CONTRACTS {
            let counter = Arc::clone(&completed_count);
            let _contract = group
                .spawn(
                    async move {
                        counter.fetch_add(1, Ordering::Relaxed);
                    },
                    true,
                )
                .unwrap();
        }

        let mut worker_handles = Vec::new();
        for _ in 0..NUM_WORKERS {
            let group = Arc::clone(&group);

            let handle = thread::spawn(move || {
                let mut bias = BiasState::new();
                loop {
                    let result = group.poll_next_timeout(&mut bias, Duration::from_millis(50));
                    if result.is_none() {
                        break;
                    }
                }
            });
            worker_handles.push(handle);
        }

        for handle in worker_handles {
            handle.join().unwrap();
        }

        let final_count = completed_count.load(Ordering::Relaxed) as usize;

        assert_eq!(
            final_count, TOTAL_CONTRACTS,
            "Expected {} contracts to complete, but only {} did.",
            TOTAL_CONTRACTS, final_count
        );
    }

    #[test]
    fn test_rapid_spawn_poll_cycle() {
        use std::thread;
        use std::time::Duration;

        const NUM_THREADS: usize = 8;
        const TEST_DURATION_MS: u64 = 1000;

        // Use larger capacity like the benchmark
        let group = Arc::new(ContractGroup::with_capacity(65536));
        let stop = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for _ in 0..NUM_THREADS {
            let group = Arc::clone(&group);
            let stop = Arc::clone(&stop);
            let completed = Arc::clone(&completed);

            let handle = thread::spawn(move || {
                let mut bias = BiasState::new();
                while !stop.load(Ordering::Relaxed) {
                    // Spawn a contract
                    if let Ok(_contract) = group.spawn(async {}, true) {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                    // Poll (might get our own or someone else's)
                    group.poll_next(&mut bias);
                }
            });
            handles.push(handle);
        }

        thread::sleep(Duration::from_millis(TEST_DURATION_MS));
        stop.store(true, Ordering::Relaxed);

        for handle in handles {
            handle.join().unwrap();
        }

        println!(
            "Completed {} spawn+poll cycles",
            completed.load(Ordering::Relaxed)
        );
    }
}
