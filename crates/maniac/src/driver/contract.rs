//! Contract-based IO submission using signal trees instead of MPSC queues.
//!
//! This replaces the ring.rs approach with a fixed-size contract pool where:
//! - Producers claim available slots via atomic bitmap operations
//! - Consumers find ready work via hierarchical summary bits
//! - Single consumer model (IO driver thread)
//! - No blocking - wake mechanism handled by drivers via eventfd/poller

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64, Ordering};
use std::task::Waker;
use std::{io, ptr};

use crate::ptr::StripedArc;

/// Trait for objects that can wake up an IO thread's poll loop.
pub trait PollWaker: Send + Sync {
    /// Wake the poll loop to check for new work.
    fn wake(&self);
}

// ============================================================================
// Contract state machine
// ============================================================================

/// Contract state - simple state machine:
/// FREE -> READY (has op to process) -> EXECUTING -> FREE
///                                   -> CANCELED -> FREE (with buf_drop cleanup)
mod state {
    pub const FREE: u8 = 0;
    pub const READY: u8 = 1;
    pub const EXECUTING: u8 = 2;
    pub const CANCELED: u8 = 3;
    /// Operation completed, waiting for future to read result and release slot
    pub const COMPLETE: u8 = 4;
    /// Cancel requested while IO in-flight, waiting for IO thread to acknowledge
    pub const CANCELING: u8 = 5;
}

// ============================================================================
// ContractId - index into contract pool
// ============================================================================

/// Index into the contract pool.
/// Encodes signal index in upper 16 bits, bit index in lower 16 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContractId(pub u32);

impl ContractId {
    pub const INVALID: ContractId = ContractId(u32::MAX);

    #[inline]
    pub fn is_valid(self) -> bool {
        self.0 != u32::MAX
    }

    /// Encode signal and bit indices into a ContractId.
    #[inline]
    pub fn encode(signal_idx: u32, bit_idx: u32) -> Self {
        ContractId((signal_idx << 6) | (bit_idx & 0x3F))
    }

    /// Decode signal index from ContractId (upper bits).
    #[inline]
    pub fn signal_idx(self) -> u32 {
        self.0 >> 6
    }

    /// Decode bit index from ContractId (lower 6 bits).
    #[inline]
    pub fn bit_idx(self) -> u32 {
        self.0 & 0x3F
    }
}

// ============================================================================
// Op - operation data
// ============================================================================

/// Operation data submitted to the IO driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Op {
    pub op_type: u8,
    pub flags: u8,
    pub _pad: u16,
    pub fd: i32,
    pub offset: u64,
    /// Pointer to user's buffer (owned by the future)
    pub buf_ptr: *mut u8,
    /// Length of buffer
    pub buf_len: u32,
}

// SAFETY: Op contains a raw pointer but it's only dereferenced by the IO driver
// thread which has exclusive access during EXECUTING state.
unsafe impl Send for Op {}
unsafe impl Sync for Op {}

impl Op {
    pub const fn new() -> Self {
        Op {
            op_type: 0,
            flags: 0,
            _pad: 0,
            fd: 0,
            offset: 0,
            buf_ptr: ptr::null_mut(),
            buf_len: 0,
        }
    }

    pub fn with_params(
        op_type: u8,
        flags: u8,
        fd: i32,
        offset: u64,
        buf_ptr: *mut u8,
        buf_len: u32,
    ) -> Self {
        Op {
            op_type,
            flags,
            _pad: 0,
            fd,
            offset,
            buf_ptr,
            buf_len,
        }
    }
}

impl Default for Op {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// IoResult - operation result
// ============================================================================

/// IO operation result (bytes transferred or error).
pub type IoResult = io::Result<usize>;

// ============================================================================
// IoContract - individual contract slot
// ============================================================================

/// Buffer drop function type - called when a canceled operation completes
/// to clean up the buffer that was transferred to the contract.
type BufDropFn = Box<dyn FnOnce() + Send>;

/// Individual IO contract slot.
/// Cache-line aligned to prevent false sharing between contracts.
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
pub struct IoContract {
    /// Contract ID (encodes signal_idx and bit_idx)
    id: ContractId,
    /// Current state (FREE, READY, EXECUTING, CANCELED)
    state: AtomicU8,
    /// Waker to notify when operation completes.
    /// Copied (not cloned) on push - we don't own it, so no drop.
    /// The future owns the waker and outlives the IO operation.
    waker: UnsafeCell<std::mem::MaybeUninit<Waker>>,
    /// Whether a waker is present
    has_waker: std::cell::Cell<bool>,
    /// Operation result
    result: UnsafeCell<IoResult>, // IoResult = io::Result<usize>
    /// Operation data
    op: UnsafeCell<Op>,
    /// Buffer drop function for canceled operations
    buf_drop: AtomicPtr<BufDropFn>,
}

// SAFETY: IoContract uses atomic state for synchronization.
// The UnsafeCell fields are only accessed according to the state machine.
unsafe impl Send for IoContract {}
unsafe impl Sync for IoContract {}

impl IoContract {
    fn new(id: ContractId) -> Self {
        Self {
            id,
            state: AtomicU8::new(state::FREE),
            waker: UnsafeCell::new(std::mem::MaybeUninit::uninit()),
            has_waker: std::cell::Cell::new(false),
            result: UnsafeCell::new(Ok(0)),
            op: UnsafeCell::new(Op::new()),
            buf_drop: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Get the contract ID.
    #[inline]
    pub fn id(&self) -> ContractId {
        self.id
    }

    /// Get the operation data (only valid when state is READY or EXECUTING).
    ///
    /// # Safety
    /// Caller must ensure the contract is in READY or EXECUTING state.
    #[inline]
    pub unsafe fn op(&self) -> &Op {
        unsafe { &*self.op.get() }
    }

    /// Set the waker for this contract. Called once during push().
    ///
    /// The waker is copied (not cloned) - the future owns the waker and
    /// must outlive the IO operation.
    ///
    /// # Safety
    /// Must only be called when the contract is being submitted (transitioning to READY).
    /// The future's waker must remain valid until the operation completes or is canceled.
    #[inline]
    unsafe fn set_waker(&self, waker: &Waker) {
        unsafe {
            // Copy the waker bytes without cloning (no ref count increment)
            std::ptr::copy_nonoverlapping(
                waker as *const Waker,
                (*self.waker.get()).as_mut_ptr(),
                1,
            );
            self.has_waker.set(true);
        }
    }

    /// Wake the registered waker and clear it.
    ///
    /// # Safety
    /// Must only be called during completion (state transition to COMPLETE).
    #[inline]
    unsafe fn wake_and_clear(&self) {
        if self.has_waker.get() {
            self.has_waker.set(false);
            // Wake by ref since we don't own the waker (no drop will be called)
            unsafe { (*self.waker.get()).assume_init_ref().wake_by_ref() };
        }
    }

    /// Clear the waker without waking.
    ///
    /// # Safety
    /// Must only be called during cancel or release.
    #[inline]
    unsafe fn clear_waker(&self) {
        // Just clear the flag - no drop needed since we don't own the waker
        self.has_waker.set(false);
    }

    /// Check if the contract is complete (back to FREE state).
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.state.load(Ordering::Acquire) == state::COMPLETE
    }

    /// Take the result. Only valid after completion.
    ///
    /// # Safety
    /// Caller must ensure the contract has completed.
    #[inline]
    pub unsafe fn take_result(&self) -> IoResult {
        unsafe { std::mem::replace(&mut *self.result.get(), Ok(0)) }
    }
}

// ============================================================================
// Signal - 64-bit atomic bitmap for tracking slots
// ============================================================================

/// 64-bit atomic bitmap for tracking slot availability or readiness.
/// Cache-line aligned to prevent false sharing.
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
struct Signal {
    value: AtomicU64,
}

impl Signal {
    fn new(value: u64) -> Self {
        Self {
            value: AtomicU64::new(value),
        }
    }

    /// Load the current value.
    #[inline]
    fn load(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    /// Check if empty (no bits set).
    #[inline]
    fn is_empty(&self) -> bool {
        self.value.load(Ordering::Relaxed) == 0
    }

    /// Find first set bit. Returns 64 if no bits are set.
    #[inline]
    fn find_first_set(&self) -> u32 {
        let value = self.value.load(Ordering::Acquire);
        if value == 0 {
            64
        } else {
            value.trailing_zeros()
        }
    }

    /// Find a set bit nearest to the hint position.
    #[inline]
    fn find_nearest(&self, hint: u64) -> u32 {
        let value = self.value.load(Ordering::Acquire);
        if value == 0 {
            return 64;
        }
        find_nearest_by_distance(value, hint & 63) as u32
    }

    /// Atomically set a bit. Returns true if the bit was previously unset.
    #[inline]
    fn set_bit(&self, bit: u64) -> bool {
        let mask = 1u64 << (bit & 63);
        let prev = self.value.fetch_or(mask, Ordering::AcqRel);
        (prev & mask) == 0
    }

    /// Atomically clear a bit. Returns true if the bit was previously set.
    #[inline]
    fn clear_bit(&self, bit: u64) -> bool {
        let mask = 1u64 << (bit & 63);
        let prev = self.value.fetch_and(!mask, Ordering::AcqRel);
        (prev & mask) != 0
    }

    /// Atomically test if a bit is set.
    #[inline]
    fn test_bit(&self, bit: u64) -> bool {
        let mask = 1u64 << (bit & 63);
        (self.value.load(Ordering::Relaxed) & mask) != 0
    }

    /// Try to acquire (clear) a specific bit. Returns true if successful.
    #[inline]
    fn try_acquire(&self, bit: u64) -> bool {
        self.clear_bit(bit)
    }

    /// Try to acquire any set bit. Returns the bit index or 64 if none available.
    /// Try to acquire any set bit, starting from hint for round-robin fairness.
    /// Returns (bit_index, signal_is_empty) or (64, _) if none acquired.
    #[inline]
    fn try_acquire_any(&self, hint: u64) -> (u32, bool) {
        let value = self.value.load(Ordering::Acquire);
        if value == 0 {
            return (64, true);
        }

        // Use find_nearest for round-robin fairness starting from hint
        let bit = find_nearest_by_distance(value, hint & 63) as u32;
        if bit < 64 {
            let mask = 1u64 << bit;
            // let prev = value;
            // let value = value & !mask;
            // let prev = self.value.store(value, Ordering::Release);
            let prev = self.value.fetch_and(!mask, Ordering::AcqRel);
            if (prev & mask) != 0 {
                // Successfully acquired - check if signal became empty
                let is_empty = (prev & !mask) == 0;
                (bit, is_empty)
            } else {
                // Lost race - someone else cleared this bit
                (64, prev == 0)
            }
        } else {
            (64, value == 0)
        }
    }
}

use crate::util::CachePadded;

/// Summary words for tracking which trees have work (1 bit per tree)
/// Each AtomicU64 covers 64 trees
pub struct TreeSummary {
    words: Vec<CachePadded<AtomicU64>>,
    word_count: usize,
    word_mask: usize,
}

impl TreeSummary {
    pub(crate) fn new(tree_count: usize) -> Self {
        let word_count = ((tree_count + 63) / 64).next_power_of_two();
        let mut words = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            words.push(CachePadded::new(AtomicU64::new(0)));
        }
        Self {
            words,
            word_count,
            word_mask: word_count - 1,
        }
    }

    /// Check if any tree has work
    #[inline]
    pub(crate) fn has_work(&self) -> bool {
        for word in &self.words {
            if word.load(Ordering::Acquire) != 0 {
                return true;
            }
        }
        false
    }

    /// Set bit for tree becoming non-empty.
    /// Returns (bit_was_unset, word_was_zero) - word_was_zero is true if the entire
    /// word transitioned from 0 to non-zero (useful for semaphore signaling).
    #[inline]
    pub(crate) fn set_bit(&self, tree_index: u64) -> (bool, bool) {
        let word_idx = (tree_index >> 6) as usize;
        let bit = 1u64 << (tree_index & 63);
        let prev = self.words[word_idx].fetch_or(bit, Ordering::AcqRel);
        ((prev & bit) == 0, prev == 0)
    }

    /// Clear bit for tree becoming empty.
    /// Returns true if the bit was actually cleared (was set before).
    #[inline]
    pub(crate) fn clear_bit(&self, tree_index: u64) -> bool {
        let word_idx = (tree_index >> 6) as usize;
        let bit = 1u64 << (tree_index & 63);
        let prev = self.words[word_idx].fetch_and(!bit, Ordering::AcqRel);
        (prev & bit) != 0
    }

    /// Test if a tree has the bit set (indicating it may have work)
    #[inline]
    pub(crate) fn test_bit(&self, tree_index: u64) -> bool {
        let word_idx = (tree_index >> 6) as usize;
        let bit = 1u64 << (tree_index & 63);
        (self.words[word_idx].load(Ordering::Relaxed) & bit) != 0
    }

    /// Select a tree index using random bias for fairness.
    /// Returns (tree_index, new_bias) or (u64::MAX, bias) if no trees have work.
    #[inline]
    pub(crate) fn select_with_bias(&self, mut random: u64) -> (u64, u64) {
        // Use random to pick starting word
        let start_word = ((random >> 6) as usize) & self.word_mask;

        for i in 0..self.word_count {
            let word_idx = (start_word + i) & self.word_mask;
            let word = self.words[word_idx].load(Ordering::Relaxed);
            if word == 0 {
                continue;
            }

            // Select a uniformly random set bit within word
            let bit_pos = select_random_bit(word, random);
            // let bit_pos = find_nearest_by_distance(word, random & 63);
            // let bit_pos = find_nearest_by_distance(word, 37);
            // let bit_pos = select_random_bit(word, random & 63);

            if bit_pos < 64 {
                let tree_index = (word_idx as u64) * 64 + bit_pos;
                return (tree_index, random);
            }

            // random = random.wrapping_add(1);
            // random = crate::util::random::next_u64();
            // random = crate::util::random::rapidrng_fast(&mut random);
        }

        (u64::MAX, random)
    }
}

// ============================================================================
// IoContractGroup - the main container
// ============================================================================

/// Configuration for IoContractGroup.
pub struct IoContractGroupConfig {
    /// Total capacity (will be rounded up to multiple of 64)
    pub capacity: u64,
    /// Force linear scanning instead of summary-based selection
    pub force_linear_scan: bool,
}

impl Default for IoContractGroupConfig {
    fn default() -> Self {
        Self {
            capacity: 4096,
            force_linear_scan: false,
        }
    }
}

/// Public handle to the IO contract group.
pub struct IoContractGroup {
    inner: StripedArc<IoContractGroupInner>,
}

impl IoContractGroup {
    /// Create a new contract group with default configuration.
    pub fn new() -> Self {
        Self::with_config(IoContractGroupConfig::default())
    }

    /// Create a new contract group with specified capacity.
    pub fn with_capacity(capacity: u64) -> Self {
        Self::with_config(IoContractGroupConfig {
            capacity,
            ..Default::default()
        })
    }

    /// Create a new contract group with full configuration.
    pub fn with_config(config: IoContractGroupConfig) -> Self {
        Self {
            inner: StripedArc::new(IoContractGroupInner::new(config)),
        }
    }

    /// Submit an operation. Returns the contract reference on success.
    #[inline]
    pub fn push(&self, op: Op, waker: &Waker) -> Result<ContractId, ()> {
        self.inner.push(op, waker)
    }

    /// Try to get the next ready contract for processing.
    /// Returns None if no work is available.
    #[inline]
    pub fn pop(&self, bias: &mut u64) -> Option<&IoContract> {
        self.inner.pop(bias)
    }

    /// Check if there is any work available.
    #[inline]
    pub fn has_work(&self) -> bool {
        self.inner.has_work()
    }

    /// Complete an operation and wake the waiting future.
    #[inline]
    pub fn complete(&self, id: ContractId, result: io::Result<usize>) {
        self.inner.complete(id, result)
    }

    /// Cancel an operation. If the operation is already executing,
    /// the buf_drop will be called when it completes.
    #[inline]
    pub fn cancel(&self, id: ContractId, buf_drop: Option<BufDropFn>) {
        self.inner.cancel(id, buf_drop)
    }

    /// Release a completed contract slot for reuse.
    /// This must be called after reading the result from a completed operation.
    #[inline]
    pub fn release_contract(&self, id: ContractId) {
        self.inner.release_contract(id)
    }

    /// Get a contract by ID.
    #[inline]
    pub fn get(&self, id: ContractId) -> Option<&IoContract> {
        self.inner.get(id)
    }

    /// Get the inner StripedArc for sharing.
    pub fn inner(&self) -> &StripedArc<IoContractGroupInner> {
        &self.inner
    }
}

impl Default for IoContractGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for IoContractGroup {
    fn clone(&self) -> Self {
        Self {
            inner: StripedArc::clone(&self.inner),
        }
    }
}

// ============================================================================
// IoContractGroupInner - internal implementation
// ============================================================================

/// Threshold for using summary-based selection vs linear scan.
const SUMMARY_THRESHOLD: usize = 4;

/// Inner state of the IO contract group.
pub struct IoContractGroupInner {
    /// All contracts (fixed size, pre-allocated)
    contracts: Vec<IoContract>,
    /// Number of signal words
    signal_count: usize,
    /// Mask for signal index (signal_count - 1)
    signal_mask: u64,
    /// Signals indicating ready contracts (bit SET = ready to consume)
    signals: Vec<Signal>,
    /// Available slots (bit SET = slot available for allocation)
    available: Vec<Signal>,
    /// Summary bits for fast has-work detection
    summary: TreeSummary,
    /// Force linear scanning
    force_linear_scan: bool,
    /// Optional waker to notify IO thread when work is pushed (set once, read many)
    /// Stored as raw pointer to Arc for lock-free access.
    poll_waker: AtomicPtr<std::sync::Arc<dyn PollWaker>>,
}

// SAFETY: All access is synchronized via atomic operations.
unsafe impl Send for IoContractGroupInner {}
unsafe impl Sync for IoContractGroupInner {}

impl IoContractGroupInner {
    fn new(config: IoContractGroupConfig) -> Self {
        // Round up to multiple of 64
        let signal_count =
            crate::signal_tree::minimum_power_of_two((config.capacity + 63) / 64).max(1) as usize;
        let total_capacity = signal_count * 64;

        // Initialize signals (all zeros - no work ready)
        let mut signals = Vec::with_capacity(signal_count);
        for _ in 0..signal_count {
            signals.push(Signal::new(0));
        }

        // Initialize available (all ones - all slots available)
        let mut available = Vec::with_capacity(signal_count);
        for _ in 0..signal_count {
            available.push(Signal::new(u64::MAX));
        }

        // Initialize contracts
        let mut contracts = Vec::with_capacity(total_capacity);
        for i in 0..total_capacity {
            let signal_idx = (i / 64) as u32;
            let bit_idx = (i % 64) as u32;
            contracts.push(IoContract::new(ContractId::encode(signal_idx, bit_idx)));
        }

        Self {
            contracts,
            signal_count,
            signal_mask: (signal_count - 1) as u64,
            signals,
            available,
            summary: TreeSummary::new(signal_count),
            force_linear_scan: config.force_linear_scan,
            poll_waker: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Set the poll waker that will be notified when work is pushed.
    /// The Box is leaked intentionally - the waker lives for the lifetime of the program.
    pub fn set_poll_waker(&self, waker: std::sync::Arc<dyn PollWaker>) {
        let boxed = Box::new(waker);
        let ptr = Box::into_raw(boxed);
        self.poll_waker.store(ptr, Ordering::Release);
    }

    /// Notify the poll waker if one is set.
    #[inline]
    fn notify_poll_waker(&self) {
        let ptr = self.poll_waker.load(Ordering::Acquire);
        if !ptr.is_null() {
            // SAFETY: ptr was created from Box::into_raw and is valid for program lifetime
            unsafe { &**ptr }.wake();
        }
    }

    /// Check if there is any work available.
    #[inline]
    pub fn has_work(&self) -> bool {
        if self.force_linear_scan || self.signal_count <= SUMMARY_THRESHOLD {
            // Linear scan
            for signal in &self.signals {
                if !signal.is_empty() {
                    return true;
                }
            }
            false
        } else {
            self.summary.has_work()
        }
    }

    /// Get a contract by ID.
    #[inline]
    pub fn get(&self, id: ContractId) -> Option<&IoContract> {
        if !id.is_valid() {
            return None;
        }
        let index = id.0 as usize;
        self.contracts.get(index)
    }

    /// Submit an operation.
    pub fn push(&self, op: Op, waker: &Waker) -> Result<ContractId, ()> {
        // Try to find an available slot using random start for fairness
        let start = crate::util::random::next_u64() & self.signal_mask;

        for i in 0..self.signal_count {
            let signal_idx = ((start as usize) + i) & (self.signal_count - 1);
            let available = &self.available[signal_idx];

            // Find an available slot in this signal word
            let bit = available.find_first_set();
            if bit >= 64 {
                continue;
            }

            // Try to claim the slot (clear the available bit)
            if !available.clear_bit(bit as u64) {
                // Lost the race, try next
                continue;
            }

            // Successfully claimed the slot
            let contract_idx = signal_idx * 64 + bit as usize;
            let contract = &self.contracts[contract_idx];

            // Write the operation data
            unsafe {
                *contract.op.get() = op;
            }

            // Set the waker (pinned for duration of operation)
            unsafe { contract.set_waker(waker) };

            // Transition to READY state
            contract.state.store(state::READY, Ordering::Release);

            // Set the signal bit to indicate work is ready
            let signal = &self.signals[signal_idx];
            let was_empty = signal.set_bit(bit as u64);

            // Update summary if signal transitioned from empty to non-empty
            if was_empty {
                self.summary.set_bit(signal_idx as u64);
            }

            // Notify the poll waker so the IO thread wakes up to process this work
            self.notify_poll_waker();

            return Ok(contract.id);
        }

        Err(())
    }

    /// Try to get the next ready contract.
    pub fn pop(&self, bias: &mut u64) -> Option<&IoContract> {
        // if self.force_linear_scan || self.signal_count <= SUMMARY_THRESHOLD {
        //     self.pop_linear_scan(bias)
        // } else {
        //     self.pop_with_summary(bias)
        // }

        self.pop_with_summary(bias)
        // self.pop_linear_scan(bias)
    }

    /// Pop using summary-based selection.
    fn pop_with_summary(&self, bias: &mut u64) -> Option<&IoContract> {
        // Use summary to find a signal word with work
        // bias encodes: upper bits = signal index, lower 6 bits = bit position
        let (signal_idx, _) = self.summary.select_with_bias(*bias >> 6);
        *bias = (*bias).wrapping_add(1);
        if signal_idx == u64::MAX {
            return None;
        }

        let signal = &self.signals[signal_idx as usize];

        // Try to acquire a ready slot from this signal, using lower 6 bits of bias
        let (bit, signal_is_empty) = signal.try_acquire_any(*bias);
        if bit >= 64 {
            // Lost the race, the work was taken by someone else
            // Clear summary bit if signal is empty
            if signal_is_empty {
                self.summary.clear_bit(signal_idx);
                // Re-check and restore if signal got work again
                if !signal.is_empty() {
                    self.summary.set_bit(signal_idx);
                }
            }
            return None;
        }

        // Update bias: encode selected signal and bit+1 for round-robin
        let contract_idx = (signal_idx as u64) * 64 + bit as u64;
        *bias = contract_idx.wrapping_add(1);

        // Clear summary bit if signal became empty
        if signal_is_empty {
            self.summary.clear_bit(signal_idx);
            // Re-check and restore if signal got work again
            if !signal.is_empty() {
                self.summary.set_bit(signal_idx);
            }
        }

        // Get the contract and transition to EXECUTING
        let contract_idx = (signal_idx as usize) * 64 + bit as usize;
        let contract = &self.contracts[contract_idx];

        // Transition READY -> EXECUTING
        let prev_state = contract.state.swap(state::EXECUTING, Ordering::AcqRel);

        // Handle the case where contract was canceled while we were acquiring
        if prev_state == state::CANCELED {
            // Run the buf_drop if present and release the slot
            self.finish_canceled(contract);
            return None;
        }

        debug_assert_eq!(prev_state, state::READY);
        Some(contract)
    }

    /// Complete an operation.
    pub fn complete(&self, id: ContractId, result: io::Result<usize>) {
        let Some(contract) = self.get(id) else {
            return;
        };

        // Use CAS loop to handle concurrent state transitions
        loop {
            let prev_state = contract.state.load(Ordering::Acquire);

            match prev_state {
                state::EXECUTING => {
                    // Try to transition EXECUTING -> COMPLETE
                    if contract
                        .state
                        .compare_exchange(
                            state::EXECUTING,
                            state::COMPLETE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        // State changed, retry
                        continue;
                    }

                    // Store result after successful state transition
                    unsafe {
                        *contract.result.get() = result;
                    }

                    // Wake the waiting future and clear the waker
                    // SAFETY: We just transitioned to COMPLETE, so we own the waker
                    unsafe { contract.wake_and_clear() };

                    // NOTE: Do NOT release the slot here! The future will release it
                    // after reading the result via release_contract().
                    return;
                }
                state::CANCELING => {
                    // Cancel was requested while IO was in-flight.
                    // We own cleanup - run buf_drop and release the slot.
                    self.finish_canceled(contract);
                    return;
                }
                state::CANCELED => {
                    // Canceled before we got here - run buf_drop and release
                    self.finish_canceled(contract);
                    return;
                }
                _ => {
                    // Invalid state - should not happen
                    debug_assert!(false, "complete called in invalid state: {}", prev_state);
                    return;
                }
            }
        }
    }

    /// Cancel an operation.
    pub fn cancel(&self, id: ContractId, buf_drop: Option<BufDropFn>) {
        let Some(contract) = self.get(id) else {
            return;
        };

        // Store the buf_drop function if provided
        if let Some(drop_fn) = buf_drop {
            let boxed = Box::new(drop_fn);
            let ptr = Box::into_raw(boxed);
            contract.buf_drop.store(ptr, Ordering::Release);
        }

        // Try to transition to CANCELED state
        let prev_state = contract.state.compare_exchange(
            state::READY,
            state::CANCELED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        match prev_state {
            Ok(state::READY) => {
                // Successfully canceled before execution started
                // Clear the signal bit since we're canceling
                let signal_idx = id.signal_idx() as usize;
                let bit_idx = id.bit_idx() as u64;

                let signal = &self.signals[signal_idx];
                let was_set = signal.clear_bit(bit_idx);

                // Update summary if signal became empty
                if was_set && signal.is_empty() {
                    self.summary.clear_bit(signal_idx as u64);
                    if !signal.is_empty() {
                        self.summary.set_bit(signal_idx as u64);
                    }
                }

                // Run buf_drop and release immediately
                self.finish_canceled(contract);
            }
            Err(state::EXECUTING) => {
                // Already executing - transition to CANCELING
                // Must use CAS because complete() might have already transitioned to COMPLETE
                let cas_result = contract.state.compare_exchange(
                    state::EXECUTING,
                    state::CANCELING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );

                match cas_result {
                    Ok(_) => {
                        // Successfully transitioned to CANCELING.
                        // IO thread will see CANCELING in complete(), run buf_drop, and release.
                        // We're done - do NOT wait, do NOT touch buf_drop.
                    }
                    Err(state::COMPLETE) => {
                        // complete() already finished - clean up buf_drop and release
                        let ptr = contract.buf_drop.swap(ptr::null_mut(), Ordering::AcqRel);
                        if !ptr.is_null() {
                            unsafe {
                                let drop_fn = Box::from_raw(ptr);
                                (*drop_fn)();
                            }
                        }
                        // Clear the waker
                        // SAFETY: complete() already woke, we're cleaning up
                        unsafe { contract.clear_waker() };
                        // Release the contract
                        if contract
                            .state
                            .compare_exchange(
                                state::COMPLETE,
                                state::FREE,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            self.release_slot(id);
                        }
                    }
                    _ => {
                        // Other state (shouldn't happen) - someone else is handling it
                    }
                }
            }
            Err(state::COMPLETE) | Err(state::FREE) => {
                // Already completed or released - clean up the buf_drop we stored
                // and release the contract if it's still in COMPLETE state
                let ptr = contract.buf_drop.swap(ptr::null_mut(), Ordering::AcqRel);
                if !ptr.is_null() {
                    unsafe {
                        let _ = Box::from_raw(ptr);
                    }
                }
                // If in COMPLETE state, transition to FREE and release
                if prev_state == Err(state::COMPLETE) {
                    // Clear the waker
                    // SAFETY: complete() already woke, we're cleaning up
                    unsafe { contract.clear_waker() };
                    if contract
                        .state
                        .compare_exchange(
                            state::COMPLETE,
                            state::FREE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.release_slot(id);
                    }
                }
            }
            _ => {
                // Already canceled or other state
            }
        }
    }

    pub(crate) fn try_to_complete(&self, contract: &IoContract) -> Option<IoResult> {
        if contract.state.load(Ordering::Acquire) == state::COMPLETE {
            let result = unsafe { contract.take_result() };
            // Clear waker (already woken in complete())
            // SAFETY: We're in the completion path, single-threaded access
            unsafe { contract.clear_waker() };
            contract.state.store(state::FREE, Ordering::Release);
            // Successfully transitioned to FREE, now release the slot
            self.release_slot(contract.id());
            Some(result)
        } else {
            None
        }
    }

    /// Release a completed contract after the future has read the result.
    /// This transitions from COMPLETE -> FREE and releases the slot for reuse.
    pub fn release_contract(&self, id: ContractId) {
        let Some(contract) = self.get(id) else {
            return;
        };

        // contract.state.store(state::FREE, Ordering::Release);
        // self.release_slot(id);

        // Only release if in COMPLETE state
        let prev_state = contract.state.compare_exchange(
            state::COMPLETE,
            state::FREE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if prev_state.is_ok() {
            // Successfully transitioned to FREE, now release the slot
            self.release_slot(id);
        }

        // If not in COMPLETE state, someone else already released it (e.g., via cancel)
    }

    /// Finish a canceled contract - run buf_drop and release slot.
    fn finish_canceled(&self, contract: &IoContract) {
        // Take and run the buf_drop function
        let ptr = contract.buf_drop.swap(ptr::null_mut(), Ordering::AcqRel);
        if !ptr.is_null() {
            unsafe {
                let drop_fn = Box::from_raw(ptr);
                (*drop_fn)();
            }
        }

        // Clear the waker without waking (the future is being dropped)
        // SAFETY: We're in the cancel path, no concurrent access
        unsafe { contract.clear_waker() };

        // Transition to FREE
        contract.state.store(state::FREE, Ordering::Release);

        // Release the slot
        self.release_slot(contract.id);
    }

    /// Release a slot back to the available pool.
    fn release_slot(&self, id: ContractId) {
        let signal_idx = id.signal_idx() as usize;
        let bit_idx = id.bit_idx() as u64;

        let available = &self.available[signal_idx];
        available.set_bit(bit_idx);
    }
}

// ============================================================================
// OpType - operation type enum
// ============================================================================

/// Operation types
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpType {
    // File operations
    ReadAt = 0,
    WriteAt = 1,
    Read = 2,
    Write = 3,
    ReadVectoredAt = 4,
    WriteVectoredAt = 5,
    ReadVectored = 6,
    WriteVectored = 7,
    Fsync = 8,
    Fdatasync = 9,
    Open = 10,
    Close = 11,
    Truncate = 12,
    Stat = 13,
    CreateDir = 14,
    Unlink = 15,
    Rmdir = 16,
    Rename = 17,
    Symlink = 18,
    Link = 19,
    Readlink = 20,
    Splice = 21,
    Pipe = 22,

    // Socket operations
    Accept = 50,
    Connect = 51,
    Recv = 52,
    Send = 53,
    RecvFrom = 54,
    SendTo = 55,
    RecvVectored = 56,
    SendVectored = 57,
    RecvMsg = 58,
    SendMsg = 59,
    Shutdown = 60,
    Socket = 61,
    Bind = 62,
    Listen = 63,
    GetSockOpt = 64,
    SetSockOpt = 65,

    // Utility operations
    Poll = 90,
    Cancel = 91,
    Nop = 92,

    // Windows-specific
    ConnectNamedPipe = 100,
}

impl OpType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(OpType::ReadAt),
            1 => Some(OpType::WriteAt),
            2 => Some(OpType::Read),
            3 => Some(OpType::Write),
            4 => Some(OpType::ReadVectoredAt),
            5 => Some(OpType::WriteVectoredAt),
            6 => Some(OpType::ReadVectored),
            7 => Some(OpType::WriteVectored),
            8 => Some(OpType::Fsync),
            9 => Some(OpType::Fdatasync),
            10 => Some(OpType::Open),
            11 => Some(OpType::Close),
            12 => Some(OpType::Truncate),
            13 => Some(OpType::Stat),
            14 => Some(OpType::CreateDir),
            15 => Some(OpType::Unlink),
            16 => Some(OpType::Rmdir),
            17 => Some(OpType::Rename),
            18 => Some(OpType::Symlink),
            19 => Some(OpType::Link),
            20 => Some(OpType::Readlink),
            21 => Some(OpType::Splice),
            22 => Some(OpType::Pipe),
            50 => Some(OpType::Accept),
            51 => Some(OpType::Connect),
            52 => Some(OpType::Recv),
            53 => Some(OpType::Send),
            54 => Some(OpType::RecvFrom),
            55 => Some(OpType::SendTo),
            56 => Some(OpType::RecvVectored),
            57 => Some(OpType::SendVectored),
            58 => Some(OpType::RecvMsg),
            59 => Some(OpType::SendMsg),
            60 => Some(OpType::Shutdown),
            61 => Some(OpType::Socket),
            62 => Some(OpType::Bind),
            63 => Some(OpType::Listen),
            64 => Some(OpType::GetSockOpt),
            65 => Some(OpType::SetSockOpt),
            90 => Some(OpType::Poll),
            91 => Some(OpType::Cancel),
            92 => Some(OpType::Nop),
            100 => Some(OpType::ConnectNamedPipe),
            _ => None,
        }
    }

    pub fn is_file_op(self) -> bool {
        matches!(
            self,
            OpType::ReadAt
                | OpType::WriteAt
                | OpType::Read
                | OpType::Write
                | OpType::Fsync
                | OpType::Fdatasync
                | OpType::Close
        )
    }

    pub fn is_socket_op(self) -> bool {
        matches!(
            self,
            OpType::Accept
                | OpType::Connect
                | OpType::Recv
                | OpType::Send
                | OpType::RecvFrom
                | OpType::SendTo
                | OpType::RecvVectored
                | OpType::SendVectored
                | OpType::RecvMsg
                | OpType::SendMsg
        )
    }

    pub fn is_dir_op(self) -> bool {
        matches!(
            self,
            OpType::Open
                | OpType::CreateDir
                | OpType::Unlink
                | OpType::Rmdir
                | OpType::Rename
                | OpType::Symlink
                | OpType::Link
                | OpType::Readlink
        )
    }
}

pub fn find_nearest_by_distance(value: u64, start_index: u64) -> u64 {
    if start_index >= 64 {
        return 64;
    }

    // Search forward
    let mask_forward = !((1u64 << start_index) - 1);
    let forward_bits = value & mask_forward;
    let mask_backward = (1u64 << start_index) - 1;
    let backward_bits = value & mask_backward;

    if forward_bits != 0 {
        let forward_index = forward_bits.trailing_zeros() as u64;

        if backward_bits == 0 {
            return forward_index;
        }

        let forward_dist = forward_index - start_index;
        let backward_index = 63 - backward_bits.leading_zeros() as u64;
        let backward_dist = start_index - backward_index;

        if forward_dist < backward_dist {
            forward_index
        } else {
            backward_index
        }
    } else if backward_bits != 0 {
        63 - backward_bits.leading_zeros() as u64
    } else {
        64
    }
}

/// Select a uniformly random set bit from value using random as entropy.
/// Uses rotation + trailing_zeros for O(1) uniform selection.
/// Returns the bit index (0-63) or 64 if no bits are set.
#[inline]
pub fn select_random_bit(value: u64, random: u64) -> u64 {
    if value == 0 {
        return 64;
    }

    // Rotate the value by a random amount, then find the first set bit.
    // This gives uniform distribution because each bit has equal probability
    // of being rotated into position 0.
    let rotate_amount = (random & 63) as u32;
    let rotated = value.rotate_right(rotate_amount);
    let bit_in_rotated = rotated.trailing_zeros() as u64;

    // Convert back to original bit position
    ((bit_in_rotated + rotate_amount as u64) & 63)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_contract_id_encoding() {
        let id = ContractId::encode(10, 42);
        assert_eq!(id.signal_idx(), 10);
        assert_eq!(id.bit_idx(), 42);

        let id = ContractId::encode(0, 0);
        assert_eq!(id.signal_idx(), 0);
        assert_eq!(id.bit_idx(), 0);

        let id = ContractId::encode(63, 63);
        assert_eq!(id.signal_idx(), 63);
        assert_eq!(id.bit_idx(), 63);
    }

    #[test]
    fn test_push_pop_basic() {
        let group = IoContractGroup::with_capacity(64);
        let waker = std::task::Waker::noop();

        let op = Op::with_params(OpType::Read as u8, 0, 42, 0, std::ptr::null_mut(), 1024);
        let id = group.push(op, &waker).expect("push should succeed");

        assert!(id.is_valid());
        assert!(group.has_work());

        let mut bias = 0u64;
        let contract = group.pop(&mut bias).expect("pop should return contract");

        assert_eq!(contract.id(), id);
        unsafe {
            let op = contract.op();
            assert_eq!(op.fd, 42);
            assert_eq!(op.buf_len, 1024);
        }

        // Complete the operation
        group.complete(id, Ok(512));

        // Contract should be free now
        let contract = group.get(id).unwrap();
        assert!(contract.is_complete());
    }

    #[test]
    fn test_push_until_full() {
        let group = IoContractGroup::with_capacity(64);
        let waker = std::task::Waker::noop();

        // Fill all slots
        for i in 0..64 {
            let op = Op::with_params(OpType::Read as u8, 0, i, 0, std::ptr::null_mut(), 0);
            let result = group.push(op, &waker);
            assert!(result.is_ok(), "push {} should succeed", i);
        }

        // Next push should fail
        let op = Op::with_params(OpType::Read as u8, 0, 999, 0, std::ptr::null_mut(), 0);
        let result = group.push(op, &waker);
        assert!(result.is_err(), "push should fail when full");
    }

    #[test]
    fn test_cancel_before_execution() {
        let group = IoContractGroup::with_capacity(64);
        let waker = std::task::Waker::noop();
        let drop_called = Arc::new(AtomicUsize::new(0));

        let op = Op::with_params(OpType::Read as u8, 0, 42, 0, std::ptr::null_mut(), 0);
        let id = group.push(op, &waker).expect("push should succeed");

        // Cancel with buf_drop
        let drop_called_clone = Arc::clone(&drop_called);
        group.cancel(
            id,
            Some(Box::new(move || {
                drop_called_clone.fetch_add(1, Ordering::Relaxed);
            })),
        );

        // buf_drop should have been called
        assert_eq!(drop_called.load(Ordering::Relaxed), 1);

        // Slot should be available again
        let op = Op::with_params(OpType::Read as u8, 0, 43, 0, std::ptr::null_mut(), 0);
        let result = group.push(op, &waker);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cancel_during_execution() {
        let group = IoContractGroup::with_capacity(64);
        let waker = std::task::Waker::noop();
        let drop_called = Arc::new(AtomicUsize::new(0));

        let op = Op::with_params(OpType::Read as u8, 0, 42, 0, std::ptr::null_mut(), 0);
        let id = group.push(op, &waker).expect("push should succeed");

        // Pop to start execution
        let mut bias = 0u64;
        let _contract = group.pop(&mut bias).expect("pop should succeed");

        // Cancel during execution
        let drop_called_clone = Arc::clone(&drop_called);
        group.cancel(
            id,
            Some(Box::new(move || {
                drop_called_clone.fetch_add(1, Ordering::Relaxed);
            })),
        );

        // buf_drop should NOT have been called yet
        assert_eq!(drop_called.load(Ordering::Relaxed), 0);

        // Complete the operation - this should trigger buf_drop
        group.complete(id, Ok(0));

        // Now buf_drop should have been called
        assert_eq!(drop_called.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_multiple_push_pop() {
        let group = IoContractGroup::with_capacity(256);
        let waker = std::task::Waker::noop();

        // Push multiple operations
        let mut ids = Vec::new();
        for i in 0..100 {
            let op = Op::with_params(OpType::Read as u8, 0, i, 0, std::ptr::null_mut(), 0);
            let id = group.push(op, &waker).expect("push should succeed");
            ids.push(id);
        }

        // Pop and complete all
        let mut bias = 0u64;
        let mut completed = 0;
        while let Some(contract) = group.pop(&mut bias) {
            group.complete(contract.id(), Ok(completed));
            completed += 1;
        }

        assert_eq!(completed, 100);
    }

    #[test]
    fn test_linear_scan_mode() {
        let group = IoContractGroup::with_config(IoContractGroupConfig {
            capacity: 256,
            force_linear_scan: true,
        });
        let waker = std::task::Waker::noop();

        for i in 0..50 {
            let op = Op::with_params(OpType::Read as u8, 0, i, 0, std::ptr::null_mut(), 0);
            group.push(op, &waker).expect("push should succeed");
        }

        let mut bias = 0u64;
        let mut count = 0;
        while let Some(contract) = group.pop(&mut bias) {
            group.complete(contract.id(), Ok(0));
            count += 1;
        }

        assert_eq!(count, 50);
    }
}
