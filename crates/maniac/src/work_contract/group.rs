//! WorkContractGroup - Container for work contracts
//!
//! A work contract group manages a collection of work contracts and
//! provides methods to create contracts and execute pending work.

use super::contract::{InitialState, ReleaseToken, WorkContract};
use super::this_contract::ThisContract;
use super::{INVALID_CONTRACT_ID, WorkContractId};
use crate::signal_tree::{INVALID_SIGNAL_INDEX, SignalTree64, SignalTreeOps};

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use parking_lot::{Condvar, Mutex};

/// State flags for a contract
mod flags {
    pub const RELEASE_FLAG: u64 = 0x00000004;
    pub const EXECUTE_FLAG: u64 = 0x00000002;
    pub const SCHEDULE_FLAG: u64 = 0x00000001;
}

/// Trait for contract callbacks - work, release, and exception handling
pub trait ContractHandler: Send + Sync {
    /// Called when the contract is executed
    fn work(&self);
    /// Called when the contract is released
    fn release(&self);
    /// Called when work or release panics
    fn exception(&self, e: Box<dyn std::any::Any + Send>);
}

/// Default no-op handler that resumes panics
struct NoopHandler;

impl ContractHandler for NoopHandler {
    #[inline(always)]
    fn work(&self) {}
    #[inline(always)]
    fn release(&self) {}
    #[inline(always)]
    fn exception(&self, e: Box<dyn std::any::Any + Send>) {
        std::panic::resume_unwind(e);
    }
}

/// Handler that wraps closures
struct ClosureHandler<W, R, E>
where
    W: Fn() + Send + Sync,
    R: Fn() + Send + Sync,
    E: Fn(Box<dyn std::any::Any + Send>) + Send + Sync,
{
    work: W,
    release: R,
    exception: E,
}

impl<W, R, E> ContractHandler for ClosureHandler<W, R, E>
where
    W: Fn() + Send + Sync,
    R: Fn() + Send + Sync,
    E: Fn(Box<dyn std::any::Any + Send>) + Send + Sync,
{
    #[inline(always)]
    fn work(&self) {
        (self.work)();
    }
    #[inline(always)]
    fn release(&self) {
        (self.release)();
    }
    #[inline(always)]
    fn exception(&self, e: Box<dyn std::any::Any + Send>) {
        (self.exception)(e);
    }
}

/// Internal contract data - cache line aligned to prevent false sharing
#[repr(C, align(64))]
struct Contract {
    flags: AtomicU64,
    // Single trait object for all callbacks - reduces vtable overhead
    handler: UnsafeCell<Box<dyn ContractHandler>>,
    release_token: UnsafeCell<Option<Arc<ReleaseToken>>>,
}

// Safety: Contract uses atomic flags for synchronization
unsafe impl Send for Contract {}
unsafe impl Sync for Contract {}

impl Contract {
    fn new() -> Self {
        Self {
            flags: AtomicU64::new(0),
            handler: UnsafeCell::new(Box::new(NoopHandler)),
            release_token: UnsafeCell::new(None),
        }
    }

    /// Get handler - caller must ensure exclusive access via flags
    #[inline(always)]
    unsafe fn get_handler(&self) -> &dyn ContractHandler {
        unsafe { &**self.handler.get() }
    }

    /// Set handler - caller must ensure exclusive access
    #[inline(always)]
    unsafe fn set_handler(&self, handler: Box<dyn ContractHandler>) {
        unsafe { *self.handler.get() = handler };
    }

    /// Clear handler to no-op
    #[inline(always)]
    unsafe fn clear_handler(&self) {
        unsafe { *self.handler.get() = Box::new(NoopHandler) };
    }

    /// Get release token
    #[inline(always)]
    unsafe fn get_release_token(&self) -> &Option<Arc<ReleaseToken>> {
        unsafe { &*self.release_token.get() }
    }

    /// Set release token
    #[inline(always)]
    unsafe fn set_release_token(&self, token: Option<Arc<ReleaseToken>>) {
        unsafe { *self.release_token.get() = token };
    }
}

/// Trait for work contract group operations needed by WorkContract
pub trait WorkContractGroupOps: Send + Sync {
    fn schedule(&self, contract_id: WorkContractId);
    fn release(&self, contract_id: WorkContractId);
}

/// Default signal tree capacity
const DEFAULT_SIGNAL_TREE_CAPACITY: u64 = 64;

/// Default work contract group capacity
const DEFAULT_CAPACITY: u64 = 512;

/// Inner state of a work contract group
/// BLOCKING const generic: true for blocking mode, false for non-blocking
pub struct WorkContractGroupInner<const BLOCKING: bool> {
    sub_tree_mask: u64,
    signal_tree_capacity: u64,
    signal_trees: Vec<SignalTree64>,
    available_trees: Vec<SignalTree64>,
    contracts: Vec<Contract>,
    stopped: AtomicBool,
    next_available_tree_index: AtomicU64,
    non_zero_counter: AtomicI64,
    waitable_state: WaitableState,
}

struct WaitableState {
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl WaitableState {
    fn new() -> Self {
        Self {
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    #[inline]
    fn notify_one(&self) {
        self.condvar.notify_one();
    }

    #[inline]
    fn notify_all(&self) {
        self.condvar.notify_all();
    }

    #[inline]
    fn wait(&self, non_zero_counter: &AtomicI64, stopped: &AtomicBool) -> bool {
        // Fast path: check without lock first
        if non_zero_counter.load(Ordering::Acquire) != 0 {
            return true;
        }

        // Slow path: acquire lock and wait
        let mut guard = self.mutex.lock();

        // Re-check under lock to avoid race with notify
        if non_zero_counter.load(Ordering::Acquire) != 0 {
            return true;
        }
        if stopped.load(Ordering::Acquire) {
            return false;
        }

        self.condvar.wait_while(&mut guard, |_| {
            non_zero_counter.load(Ordering::Acquire) == 0 && !stopped.load(Ordering::Acquire)
        });
        !stopped.load(Ordering::Acquire)
    }

    #[inline]
    fn wait_for(
        &self,
        non_zero_counter: &AtomicI64,
        stopped: &AtomicBool,
        duration: std::time::Duration,
    ) -> bool {
        // Fast path
        if non_zero_counter.load(Ordering::Acquire) != 0 {
            return true;
        }

        let mut guard = self.mutex.lock();

        if non_zero_counter.load(Ordering::Acquire) != 0 {
            return true;
        }
        if stopped.load(Ordering::Acquire) {
            return false;
        }

        // Use parking_lot's wait_for with timeout
        let deadline = std::time::Instant::now() + duration;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }

            let result = self.condvar.wait_for(&mut guard, remaining);

            if non_zero_counter.load(Ordering::Acquire) != 0 {
                return true;
            }
            if stopped.load(Ordering::Acquire) {
                return false;
            }
            if result.timed_out() {
                return false;
            }
        }
    }
}

impl<const BLOCKING: bool> WorkContractGroupInner<BLOCKING> {
    fn new(capacity: u64) -> Self {
        let signal_tree_capacity = DEFAULT_SIGNAL_TREE_CAPACITY;
        let sub_tree_count = crate::signal_tree::minimum_power_of_two(
            (capacity + signal_tree_capacity - 1) / signal_tree_capacity,
        );
        let sub_tree_mask = sub_tree_count - 1;
        let total_capacity = sub_tree_count * signal_tree_capacity;

        let mut signal_trees = Vec::with_capacity(sub_tree_count as usize);
        let mut available_trees = Vec::with_capacity(sub_tree_count as usize);

        for _ in 0..sub_tree_count {
            signal_trees.push(SignalTree64::new());
            let available = SignalTree64::new();
            // Mark all slots as available
            for i in 0..signal_tree_capacity {
                available.set(i);
            }
            available_trees.push(available);
        }

        let mut contracts = Vec::with_capacity(total_capacity as usize);
        for _ in 0..total_capacity {
            contracts.push(Contract::new());
        }

        Self {
            sub_tree_mask,
            signal_tree_capacity,
            signal_trees,
            available_trees,
            contracts,
            stopped: AtomicBool::new(false),
            next_available_tree_index: AtomicU64::new(0),
            non_zero_counter: AtomicI64::new(0),
            waitable_state: WaitableState::new(),
        }
    }

    #[inline(always)]
    fn get_tree_and_signal_index(&self, contract_id: WorkContractId) -> (u64, u64) {
        (
            contract_id / self.signal_tree_capacity,
            contract_id % self.signal_tree_capacity,
        )
    }

    #[inline]
    pub fn schedule(&self, contract_id: WorkContractId) {
        let previous_flags = self.contracts[contract_id as usize]
            .flags
            .fetch_or(flags::SCHEDULE_FLAG, Ordering::AcqRel);
        let not_scheduled_nor_executing =
            (previous_flags & (flags::SCHEDULE_FLAG | flags::EXECUTE_FLAG)) == 0;
        if not_scheduled_nor_executing {
            self.set_contract_signal(contract_id);
        }
    }

    #[inline]
    pub fn release(&self, contract_id: WorkContractId) {
        let flags_to_set = flags::RELEASE_FLAG | flags::SCHEDULE_FLAG;
        let previous_flags = self.contracts[contract_id as usize]
            .flags
            .fetch_or(flags_to_set, Ordering::AcqRel);
        let not_scheduled_nor_executing =
            (previous_flags & (flags::SCHEDULE_FLAG | flags::EXECUTE_FLAG)) == 0;
        if not_scheduled_nor_executing {
            self.set_contract_signal(contract_id);
        }
    }

    #[inline]
    fn increment_non_zero_counter(&self) {
        // Increment and wake one thread per non-empty tree
        self.non_zero_counter.fetch_add(1, Ordering::AcqRel);
        self.waitable_state.notify_one();
    }

    #[inline]
    fn decrement_non_zero_counter(&self) {
        self.non_zero_counter.fetch_sub(1, Ordering::AcqRel);
    }

    #[inline(always)]
    fn set_contract_signal(&self, contract_id: WorkContractId) {
        let (tree_index, signal_index) = self.get_tree_and_signal_index(contract_id);

        let (tree_was_empty, _) = self.signal_trees[tree_index as usize].set(signal_index);

        if BLOCKING && tree_was_empty {
            self.increment_non_zero_counter();
        }
    }

    fn get_available_contract(&self) -> WorkContractId {
        for _ in 0..self.available_trees.len() {
            let sub_tree_index = self
                .next_available_tree_index
                .fetch_add(1, Ordering::Relaxed)
                & self.sub_tree_mask;

            if !self.available_trees[sub_tree_index as usize].empty() {
                let (signal_index, _) =
                    self.available_trees[sub_tree_index as usize].select_largest(0);
                if signal_index != INVALID_SIGNAL_INDEX {
                    return sub_tree_index * self.signal_tree_capacity + signal_index;
                }
            }
        }
        INVALID_CONTRACT_ID
    }

    #[inline(always)]
    fn clear_execute_flag(&self, contract_id: WorkContractId) {
        let new_flags = self.contracts[contract_id as usize]
            .flags
            .fetch_sub(flags::EXECUTE_FLAG, Ordering::AcqRel)
            - flags::EXECUTE_FLAG;

        if (new_flags & flags::SCHEDULE_FLAG) == flags::SCHEDULE_FLAG {
            self.set_contract_signal(contract_id);
        }
    }

    fn erase_contract(&self, contract_id: WorkContractId) {
        let contract = &self.contracts[contract_id as usize];

        // Safety: We have exclusive access via the EXECUTE_FLAG + RELEASE_FLAG
        unsafe {
            // Orphan the release token first
            if let Some(ref token) = *contract.get_release_token() {
                token.orphan();
            }

            // Clear handler and release token
            contract.clear_handler();
            contract.set_release_token(None);
        }

        // Mark the slot as available
        let (tree_index, signal_index) = self.get_tree_and_signal_index(contract_id);
        self.available_trees[tree_index as usize].set(signal_index);
    }

    fn process_release(&self, contract_id: WorkContractId) {
        let contract = &self.contracts[contract_id as usize];

        // Call release function with proper cleanup
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safety: We have exclusive access via EXECUTE_FLAG + RELEASE_FLAG
            unsafe {
                contract.get_handler().release();
            }
        }));

        // Always erase the contract
        self.erase_contract(contract_id);

        // Re-panic if the release function panicked
        if let Err(e) = result {
            self.process_panic(contract_id, e);
        }
    }

    fn process_panic(&self, contract_id: WorkContractId, exception: Box<dyn std::any::Any + Send>) {
        let contract = &self.contracts[contract_id as usize];

        // Safety: We have exclusive access via EXECUTE_FLAG
        // The exception handler defaults to resume_unwind if not set
        unsafe {
            contract.get_handler().exception(exception);
        }
    }

    #[inline]
    fn process_contract(&self, contract_id: WorkContractId) {
        let contract = &self.contracts[contract_id as usize];
        let flags = contract.flags.fetch_add(1, Ordering::AcqRel) + 1;

        let is_released = (flags & flags::RELEASE_FLAG) == flags::RELEASE_FLAG;
        if is_released {
            self.process_release(contract_id);
            return;
        }

        // Create auto-clear guard for execute flag
        struct AutoClearExecuteFlag<'a, const B: bool> {
            contract_id: WorkContractId,
            inner: &'a WorkContractGroupInner<B>,
        }

        impl<const B: bool> Drop for AutoClearExecuteFlag<'_, B> {
            #[inline]
            fn drop(&mut self) {
                self.inner.clear_execute_flag(self.contract_id);
            }
        }

        let _auto_clear = AutoClearExecuteFlag {
            contract_id,
            inner: self,
        };

        // Set up this_contract context
        #[inline(always)]
        fn schedule_fn<const B: bool>(id: WorkContractId, group: *const ()) {
            unsafe {
                let inner = &*(group as *const WorkContractGroupInner<B>);
                inner.schedule(id);
            }
        }

        #[inline(always)]
        fn release_fn<const B: bool>(id: WorkContractId, group: *const ()) {
            unsafe {
                let inner = &*(group as *const WorkContractGroupInner<B>);
                inner.release(id);
            }
        }

        let _this_contract = ThisContract::new(
            contract_id,
            self as *const _ as *const (),
            schedule_fn::<BLOCKING>,
            release_fn::<BLOCKING>,
        );

        // Execute the work function - no lock needed!
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safety: We have exclusive access via EXECUTE_FLAG
            unsafe {
                contract.get_handler().work();
            }
        }));

        if let Err(e) = result {
            self.process_panic(contract_id, e);
        }
    }

    #[inline]
    fn execute_next_contract_with_bias(&self, bias_flags: &mut u64, hint: &mut u64) -> u64 {
        if BLOCKING {
            if !self
                .waitable_state
                .wait(&self.non_zero_counter, &self.stopped)
            {
                return INVALID_CONTRACT_ID;
            }
        }

        let mut sub_tree_index = (*bias_flags / 64); // & self.sub_tree_mask;

        for _ in 0..self.signal_trees.len() {
            sub_tree_index &= self.sub_tree_mask;

            let (signal_index, tree_is_empty, new_hint) =
                self.signal_trees[sub_tree_index as usize].select_with_hint(*bias_flags, *hint);

            if signal_index != INVALID_SIGNAL_INDEX {
                if BLOCKING {
                    if tree_is_empty {
                        // Decrement counter since tree became empty
                        self.decrement_non_zero_counter();
                    } else {
                        // Chain wakeup: tree still has work, wake another thread
                        self.waitable_state.notify_all();
                    }
                }

                let contract_id = sub_tree_index * 64 + signal_index;

                // Update bias flags using returned hint
                let x = new_hint ^ *bias_flags;
                let b = (x & (!x).wrapping_add(1)) & 63;
                if b == 0 {
                    *bias_flags = (sub_tree_index + 1) * 64;
                } else {
                    *bias_flags |= b;
                    *bias_flags &= !(b - 1);
                }
                *hint = new_hint;

                self.process_contract(contract_id);
                return signal_index;
            }

            sub_tree_index += 1;
            *bias_flags = sub_tree_index * self.signal_tree_capacity;
        }

        INVALID_CONTRACT_ID
    }

    fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            // Orphan all release tokens
            for contract in &self.contracts {
                unsafe {
                    if let Some(ref token) = *contract.get_release_token() {
                        token.orphan();
                    }
                }
            }

            if BLOCKING {
                self.waitable_state.notify_all();
            }
        }
    }
}

impl<const BLOCKING: bool> WorkContractGroupOps for WorkContractGroupInner<BLOCKING> {
    #[inline]
    fn schedule(&self, contract_id: WorkContractId) {
        WorkContractGroupInner::schedule(self, contract_id)
    }

    #[inline]
    fn release(&self, contract_id: WorkContractId) {
        WorkContractGroupInner::release(self, contract_id)
    }
}

/// A work contract group manages a collection of work contracts
/// BLOCKING const generic: true for blocking mode, false for non-blocking
pub struct WorkContractGroup<const BLOCKING: bool> {
    inner: Arc<WorkContractGroupInner<BLOCKING>>,
}

// Thread-local bias state (combined to reduce TLS access overhead)
#[derive(Clone, Copy, Default)]
struct BiasState {
    flags: u64,
    hint: u64,
}

thread_local! {
    // static TLS_BIAS: std::cell::Cell<BiasState> = const { std::cell::Cell::new(BiasState { flags: 0, hint: 0 }) };
    static TLS_BIAS: std::cell::Cell<BiasState> = { std::cell::Cell::new(BiasState { flags: 0, hint: crate::util::random::next_u64() }) };
    // static TLS_BIAS: std::cell::Cell<BiasState> = { std::cell::Cell::new(BiasState { flags: crate::util::random::next_u64(), hint: crate::util::random::next_u64() }) };
}

impl<const BLOCKING: bool> WorkContractGroup<BLOCKING> {
    /// Create a new work contract group with the specified capacity
    pub fn with_capacity(capacity: u64) -> Self {
        Self {
            inner: Arc::new(WorkContractGroupInner::new(capacity)),
        }
    }

    /// Create a new work contract with a work function
    pub fn create_contract<F>(&self, work: F) -> WorkContract
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.create_contract_with_state(work, InitialState::Unscheduled)
    }

    /// Create a new work contract with a work function and initial state
    pub fn create_contract_with_state<F>(
        &self,
        work: F,
        initial_state: InitialState,
    ) -> WorkContract
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.create_contract_full(work, || {}, |_| {}, initial_state)
    }

    /// Create a new work contract with work and release functions
    pub fn create_contract_with_release<F, R>(&self, work: F, release: R) -> WorkContract
    where
        F: Fn() + Send + Sync + 'static,
        R: Fn() + Send + Sync + 'static,
    {
        self.create_contract_full(work, release, |_| {}, InitialState::Unscheduled)
    }

    /// Create a new work contract with all callbacks
    pub fn create_contract_full<F, R, E>(
        &self,
        work: F,
        release: R,
        exception: E,
        initial_state: InitialState,
    ) -> WorkContract
    where
        F: Fn() + Send + Sync + 'static,
        R: Fn() + Send + Sync + 'static,
        E: Fn(Box<dyn std::any::Any + Send>) + Send + Sync + 'static,
    {
        let contract_id = self.inner.get_available_contract();
        if contract_id == INVALID_CONTRACT_ID {
            return WorkContract::new();
        }

        let contract = &self.inner.contracts[contract_id as usize];

        // Safety: Contract slot is available (not in use by any thread)
        unsafe {
            contract.flags.store(0, Ordering::Release);
            contract.set_handler(Box::new(ClosureHandler {
                work,
                release,
                exception,
            }));

            // Coerce to trait object for type erasure
            let inner_dyn: Arc<dyn WorkContractGroupOps> = self.inner.clone();
            let release_token = Arc::new(ReleaseToken::new(Arc::downgrade(&inner_dyn)));
            contract.set_release_token(Some(Arc::clone(&release_token)));

            WorkContract::with_params(
                Arc::as_ptr(&inner_dyn),
                release_token,
                contract_id,
                initial_state,
            )
        }
    }

    /// Execute the next available contract
    ///
    /// Returns the signal index if a contract was executed, or INVALID_CONTRACT_ID if none available.
    #[inline]
    pub fn execute_next_contract(&self) -> u64 {
        TLS_BIAS.with(|cell| {
            let mut state = cell.get();
            let result = self
                .inner
                .execute_next_contract_with_bias(&mut state.flags, &mut state.hint);
            cell.set(state);
            result
        })
    }

    /// Execute the next available contract with explicit bias flags and hint
    ///
    /// The bias flags and hint are updated based on the selection made.
    #[inline]
    pub fn execute_next_contract_with_bias(&self, bias_flags: &mut u64, hint: &mut u64) -> u64 {
        self.inner.execute_next_contract_with_bias(bias_flags, hint)
    }

    /// Stop the work contract group
    ///
    /// After stopping, no new contracts can be created and waiting workers will be woken.
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// Check if the group has been stopped
    pub fn is_stopped(&self) -> bool {
        self.inner.stopped.load(Ordering::Acquire)
    }

    /// Create a new work contract group with default capacity
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

impl<const BLOCKING: bool> Default for WorkContractGroup<BLOCKING> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const BLOCKING: bool> Drop for WorkContractGroup<BLOCKING> {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Blocking-mode specific methods
impl WorkContractGroup<true> {
    /// Execute the next available contract, waiting up to the specified duration
    ///
    /// Only available for blocking mode groups.
    pub fn execute_next_contract_timeout(&self, timeout: std::time::Duration) -> u64 {
        if !self.inner.waitable_state.wait_for(
            &self.inner.non_zero_counter,
            &self.inner.stopped,
            timeout,
        ) {
            return INVALID_CONTRACT_ID;
        }

        // We woke from wait_for, now try to select (use the regular path)
        self.execute_next_contract()
    }
}

/// Non-blocking work contract group (workers spin when no work available)
pub type NonBlockingWorkContractGroup = WorkContractGroup<false>;

/// Blocking work contract group (workers wait on condvar when no work available)
pub type BlockingWorkContractGroup = WorkContractGroup<true>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_create_and_execute_contract() {
        let group = NonBlockingWorkContractGroup::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = Arc::clone(&counter);
        let contract = group.create_contract_with_state(
            move || {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            },
            InitialState::Scheduled,
        );

        assert!(contract.is_valid());

        // Execute the contract
        let result = group.execute_next_contract();
        assert_ne!(result, INVALID_CONTRACT_ID);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_reschedule_contract() {
        let group = NonBlockingWorkContractGroup::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = Arc::clone(&counter);
        let _contract = group.create_contract_with_state(
            move || {
                let count = counter_clone.fetch_add(1, Ordering::Relaxed);
                if count < 2 {
                    ThisContract::schedule();
                }
            },
            InitialState::Scheduled,
        );

        // Execute three times
        group.execute_next_contract();
        group.execute_next_contract();
        group.execute_next_contract();

        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_release_contract() {
        let group = NonBlockingWorkContractGroup::new();
        let released = Arc::new(AtomicBool::new(false));

        let released_clone = Arc::clone(&released);
        let mut contract = group.create_contract_with_release(
            || {},
            move || {
                released_clone.store(true, Ordering::Relaxed);
            },
        );

        assert!(contract.is_valid());
        contract.release();
        assert!(!contract.is_valid());

        // Execute to process the release
        group.execute_next_contract();

        assert!(released.load(Ordering::Relaxed));
    }
}
