//! Signal Tree Node
//!
//! A node is a 64-bit atomic value representing multiple sub-counters.
//! For leaf nodes (capacity=64), each bit is a counter.
//! For non-leaf nodes, multiple bits form each counter.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use super::{INVALID_SIGNAL_INDEX, SignalIndex, minimum_bit_count};

/// Trait for node selection strategies
pub trait Selector {
    /// Select a counter index based on bias flags and current counter values
    /// Returns (selected_index, updated_bias_hint)
    fn select(
        total_counters: u64,
        bits_per_counter: u32,
        bias_flags: u64,
        counters: u64,
        bias_hint: u64,
    ) -> (SignalIndex, u64);
}

/// Default selector that selects based on bias flags
/// When multiple counters are non-zero, prefers the one indicated by bias
pub struct DefaultSelector;

impl Selector for DefaultSelector {
    #[inline(always)]
    fn select(
        total_counters: u64,
        bits_per_counter: u32,
        bias_flags: u64,
        counters: u64,
        bias_hint: u64,
    ) -> (SignalIndex, u64) {
        default_select_iterative(
            total_counters,
            bits_per_counter,
            bias_flags,
            counters,
            bias_hint,
        )
    }
}

#[inline(always)]
fn default_select_64(
    bias_flags: u64,
    mut counters: u64,
    mut next_bias: u64,
    bias_hint: u64,
) -> (SignalIndex, u64) {
    const TOTAL_COUNTERS: u64 = 64;
    const BITS_PER_COUNTER: u64 = 1;
    const BIAS_BIT: u64 = 1 << 63;
    const COUNTERS_PER_HALF: u64 = TOTAL_COUNTERS / 2;
    const BITS_PER_HALF: u64 = COUNTERS_PER_HALF * BITS_PER_COUNTER;
    const RIGHT_BIT_MASK: u64 = (1u64 << BITS_PER_HALF) - 1;
    const LEFT_BIT_MASK: u64 = RIGHT_BIT_MASK << BITS_PER_HALF;

    let right_counters = counters & RIGHT_BIT_MASK;
    let left_counters = counters & LEFT_BIT_MASK;
    let bias_right = bias_flags & BIAS_BIT;
    let choose_right = (bias_right != 0 && right_counters != 0) || left_counters == 0;

    next_bias <<= 1;
    next_bias |= if right_counters != 0 { 1 } else { 0 };
    counters >>= if choose_right { 0 } else { BITS_PER_HALF };

    let add = if choose_right { COUNTERS_PER_HALF } else { 0 };

    let (result, new_bias) = default_select_32(bias_flags, counters & RIGHT_BIT_MASK, next_bias);

    (result + add, bias_hint | new_bias)
}

#[inline(always)]
fn default_select_32(bias_flags: u64, mut counters: u64, mut next_hint: u64) -> (SignalIndex, u64) {
    const TOTAL_COUNTERS: u64 = 32;
    const BITS_PER_COUNTER: u64 = 1;
    const BIAS_BIT: u64 = (1 << 63) / 2;
    const COUNTERS_PER_HALF: u64 = TOTAL_COUNTERS / 2;
    const BITS_PER_HALF: u64 = COUNTERS_PER_HALF * BITS_PER_COUNTER;
    const RIGHT_BIT_MASK: u64 = (1u64 << BITS_PER_HALF) - 1;
    const LEFT_BIT_MASK: u64 = RIGHT_BIT_MASK << BITS_PER_HALF;

    let right_counters = counters & RIGHT_BIT_MASK;
    let left_counters = counters & LEFT_BIT_MASK;
    let bias_right = bias_flags & BIAS_BIT;
    let choose_right = (bias_right != 0 && right_counters != 0) || left_counters == 0;

    next_hint <<= 1;
    next_hint |= if right_counters != 0 { 1 } else { 0 };
    counters >>= if choose_right { 0 } else { BITS_PER_HALF };

    let add = if choose_right { COUNTERS_PER_HALF } else { 0 };

    let (result, new_bias) = default_select_16(bias_flags, counters & RIGHT_BIT_MASK, next_hint);

    (result + add, new_bias)
}

#[inline(always)]
fn default_select_16(bias_flags: u64, mut counters: u64, mut next_bias: u64) -> (SignalIndex, u64) {
    const TOTAL_COUNTERS: u64 = 16;
    const BITS_PER_COUNTER: u64 = 1;
    const BIAS_BIT: u64 = (1 << 63) / 4;
    const COUNTERS_PER_HALF: u64 = TOTAL_COUNTERS / 2;
    const BITS_PER_HALF: u64 = COUNTERS_PER_HALF * BITS_PER_COUNTER;
    const RIGHT_BIT_MASK: u64 = (1u64 << BITS_PER_HALF) - 1;
    const LEFT_BIT_MASK: u64 = RIGHT_BIT_MASK << BITS_PER_HALF;

    let right_counters = counters & RIGHT_BIT_MASK;
    let left_counters = counters & LEFT_BIT_MASK;
    let bias_right = bias_flags & BIAS_BIT;
    let choose_right = (bias_right != 0 && right_counters != 0) || left_counters == 0;

    next_bias <<= 1;
    next_bias |= if right_counters != 0 { 1 } else { 0 };
    counters >>= if choose_right { 0 } else { BITS_PER_HALF };

    let add = if choose_right { COUNTERS_PER_HALF } else { 0 };

    let (result, new_bias) = default_select_8(bias_flags, counters & RIGHT_BIT_MASK, next_bias);

    (result + add, new_bias)
}

#[inline(always)]
fn default_select_8(bias_flags: u64, mut counters: u64, mut next_hint: u64) -> (SignalIndex, u64) {
    const TOTAL_COUNTERS: u64 = 8;
    const BITS_PER_COUNTER: u64 = 1;
    const BIAS_BIT: u64 = (1 << 63) / 8;
    const COUNTERS_PER_HALF: u64 = TOTAL_COUNTERS / 2;
    const BITS_PER_HALF: u64 = COUNTERS_PER_HALF * BITS_PER_COUNTER;
    const RIGHT_BIT_MASK: u64 = (1u64 << BITS_PER_HALF) - 1;
    const LEFT_BIT_MASK: u64 = RIGHT_BIT_MASK << BITS_PER_HALF;

    let right_counters = counters & RIGHT_BIT_MASK;
    let left_counters = counters & LEFT_BIT_MASK;
    let bias_right = bias_flags & BIAS_BIT;
    let choose_right = (bias_right != 0 && right_counters != 0) || left_counters == 0;

    next_hint <<= 1;
    next_hint |= if right_counters != 0 { 1 } else { 0 };
    counters >>= if choose_right { 0 } else { BITS_PER_HALF };

    let add = if choose_right { COUNTERS_PER_HALF } else { 0 };

    let (result, new_bias) = default_select_4(bias_flags, counters & RIGHT_BIT_MASK, next_hint);

    (result + add, new_bias)
}

#[inline(always)]
fn default_select_4(bias_flags: u64, mut counters: u64, mut next_hint: u64) -> (SignalIndex, u64) {
    const TOTAL_COUNTERS: u64 = 4;
    const BITS_PER_COUNTER: u64 = 1;
    const BIAS_BIT: u64 = (1 << 63) / 16;
    const COUNTERS_PER_HALF: u64 = TOTAL_COUNTERS / 2;
    const BITS_PER_HALF: u64 = COUNTERS_PER_HALF * BITS_PER_COUNTER;
    const RIGHT_BIT_MASK: u64 = (1u64 << BITS_PER_HALF) - 1;
    const LEFT_BIT_MASK: u64 = RIGHT_BIT_MASK << BITS_PER_HALF;

    let right_counters = counters & RIGHT_BIT_MASK;
    let left_counters = counters & LEFT_BIT_MASK;
    let bias_right = bias_flags & BIAS_BIT;
    let choose_right = (bias_right != 0 && right_counters != 0) || left_counters == 0;

    next_hint <<= 1;
    next_hint |= if right_counters != 0 { 1 } else { 0 };
    counters >>= if choose_right { 0 } else { BITS_PER_HALF };

    let add = if choose_right { COUNTERS_PER_HALF } else { 0 };

    let (result, new_bias) = default_select_2(bias_flags, counters & RIGHT_BIT_MASK, next_hint);

    (result + add, new_bias)
}

#[inline(always)]
fn default_select_2(bias_flags: u64, counters: u64, mut next_hint: u64) -> (SignalIndex, u64) {
    const TOTAL_COUNTERS: u64 = 2;
    const BITS_PER_COUNTER: u64 = 1;
    const BIAS_BIT: u64 = (1 << 63) / 32;
    const COUNTERS_PER_HALF: u64 = TOTAL_COUNTERS / 2;
    const BITS_PER_HALF: u64 = COUNTERS_PER_HALF * BITS_PER_COUNTER;
    const RIGHT_BIT_MASK: u64 = (1u64 << BITS_PER_HALF) - 1;
    const LEFT_BIT_MASK: u64 = RIGHT_BIT_MASK << BITS_PER_HALF;

    let right_counters = counters & RIGHT_BIT_MASK;
    let left_counters = counters & LEFT_BIT_MASK;
    let bias_right = bias_flags & BIAS_BIT;
    let choose_right = (bias_right != 0 && right_counters != 0) || left_counters == 0;

    next_hint <<= 1;
    next_hint |= if right_counters != 0 { 1 } else { 0 };

    let result = if choose_right { COUNTERS_PER_HALF } else { 0 };

    (result, next_hint)
}

#[inline(always)]
pub fn select_64(bias_flags: u64, counters: u64, bias_hint: u64) -> (SignalIndex, u64) {
    crate::scheduler::signal::select_with_bias(counters, bias_flags, bias_hint)
    // default_select_iterative(64, 1, bias_flags, counters, bias_hint)
}

/// Iterative implementation of default_select for better performance
#[inline(always)]
fn default_select_iterative(
    mut total_counters: u64,
    bits_per_counter: u32,
    mut bias_flags: u64,
    mut counters: u64,
    mut bias_hint: u64,
) -> (SignalIndex, u64) {
    // let (result1, next_bias1) = default_select_64(bias_flags, counters, 0, bias_hint);
    let (result1, next_bias1) =
        crate::scheduler::signal::select_with_bias(counters, bias_flags, bias_hint);
    // let mut result: u64 = 0;
    // let mut next_bias: u64 = 0;
    // let mut bias_bit: u64 = 1u64 << 63;

    // while total_counters > 1 {
    //     let counters_per_half = total_counters / 2;
    //     let bits_per_half = counters_per_half * bits_per_counter as u64;
    //     let right_bit_mask = (1u64 << bits_per_half) - 1;
    //     let left_bit_mask = right_bit_mask << bits_per_half;

    //     let right_counters = counters & right_bit_mask;
    //     let left_counters = counters & left_bit_mask;
    //     // let bias_right = (bias_flags & (1u64 << 63)) != 0;
    //     let bias_right = (bias_flags & bias_bit) != 0;
    //     let choose_right = (bias_right && right_counters != 0) || left_counters == 0;

    //     next_bias <<= 1;
    //     next_bias |= if right_counters != 0 { 1 } else { 0 };

    //     if choose_right {
    //         result += counters_per_half;
    //         counters >>= 0;
    //         // counters &= right_bit_mask;
    //     } else {
    //         counters >>= bits_per_half;
    //     }

    //     counters &= right_bit_mask;

    //     total_counters = counters_per_half;
    //     bias_flags <<= 1;
    //     bias_bit /= 2;
    // }

    // bias_hint |= next_bias;
    (result1, next_bias1)
    // (result, bias_hint)
}

/// Selector that selects the largest (most populated) counter
pub struct LargestChildSelector;

impl Selector for LargestChildSelector {
    #[inline(always)]
    fn select(
        total_counters: u64,
        bits_per_counter: u32,
        _bias_flags: u64,
        counters: u64,
        bias_hint: u64,
    ) -> (SignalIndex, u64) {
        let result = if bits_per_counter == 1 {
            // For leaf nodes, any non-zero bit works
            if counters > 0 {
                counters.leading_zeros() as u64
            } else {
                INVALID_SIGNAL_INDEX
            }
        } else {
            // Find the counter with the largest value
            let counter_mask = (1u64 << bits_per_counter) - 1;
            let mut selected = INVALID_SIGNAL_INDEX;
            let mut max_val = 0u64;
            let mut counters = counters;

            for i in 0..total_counters {
                let val = counters & counter_mask;
                if val > max_val {
                    max_val = val;
                    selected = i;
                }
                counters >>= bits_per_counter;
            }

            if selected != INVALID_SIGNAL_INDEX {
                total_counters - selected - 1
            } else {
                INVALID_SIGNAL_INDEX
            }
        };
        (result, bias_hint)
    }
}

/// Node traits computed at compile time
pub struct NodeTraits<const CAPACITY: u64, const TREE_CAPACITY: u64>;

impl<const CAPACITY: u64, const TREE_CAPACITY: u64> NodeTraits<CAPACITY, TREE_CAPACITY> {
    pub const ROOT_NODE: bool = TREE_CAPACITY == CAPACITY;
    pub const NUMBER_OF_COUNTERS: u64 = super::sub_counter_arity(CAPACITY);
    pub const COUNTER_CAPACITY: u64 = CAPACITY / Self::NUMBER_OF_COUNTERS;
    pub const BITS_PER_COUNTER: u32 = minimum_bit_count(Self::COUNTER_CAPACITY) as u32;
    pub const COUNTER_MASK: u64 = (1u64 << Self::BITS_PER_COUNTER) - 1;
    pub const IS_LEAF: bool = CAPACITY == 64;
}

/// A signal tree node
///
/// Non-leaf nodes contain multiple sub-counters packed into a 64-bit word.
/// Leaf nodes use each bit as a separate counter.
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
pub struct Node<const CAPACITY: u64, const TREE_CAPACITY: u64> {
    pub(crate) value: AtomicU64,
}

impl<const CAPACITY: u64, const TREE_CAPACITY: u64> Default for Node<CAPACITY, TREE_CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAPACITY: u64, const TREE_CAPACITY: u64> Node<CAPACITY, TREE_CAPACITY> {
    pub const CAPACITY: u64 = CAPACITY;
    pub const TREE_CAPACITY: u64 = TREE_CAPACITY;
    pub const NUMBER_OF_COUNTERS: u64 = NodeTraits::<CAPACITY, TREE_CAPACITY>::NUMBER_OF_COUNTERS;
    pub const COUNTER_CAPACITY: u64 = NodeTraits::<CAPACITY, TREE_CAPACITY>::COUNTER_CAPACITY;
    pub const BITS_PER_COUNTER: u32 = NodeTraits::<CAPACITY, TREE_CAPACITY>::BITS_PER_COUNTER;
    pub const COUNTER_MASK: u64 = NodeTraits::<CAPACITY, TREE_CAPACITY>::COUNTER_MASK;
    pub const IS_LEAF: bool = NodeTraits::<CAPACITY, TREE_CAPACITY>::IS_LEAF;
    pub const IS_ROOT: bool = NodeTraits::<CAPACITY, TREE_CAPACITY>::ROOT_NODE;

    /// Precomputed addend values for each counter index
    const ADDENDS: [u64; 64] = {
        let mut arr = [0u64; 64];
        let mut i = 0;
        while i < Self::NUMBER_OF_COUNTERS as usize && i < 64 {
            arr[i] =
                1u64 << ((Self::NUMBER_OF_COUNTERS - i as u64 - 1) * Self::BITS_PER_COUNTER as u64);
            i += 1;
        }
        arr
    };

    /// Create a new empty node
    pub const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    /// Check if the node is empty (all counters are zero)
    #[inline(always)]
    pub fn empty(&self) -> bool {
        self.value.load(Ordering::Relaxed) == 0
    }

    #[inline(always)]
    pub fn load(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    /// Load the raw value with relaxed ordering (for SIMD scanning)
    #[inline(always)]
    pub fn load_relaxed(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Load the raw value with relaxed ordering (for SIMD scanning)
    #[inline(always)]
    pub fn acquire(&self, bit_index: u64) -> (u64, bool, bool) {
        let bit = 1u64 << bit_index;
        let previous = self.value.fetch_and(!bit, Ordering::AcqRel);
        (previous, (previous & bit) == bit, previous & !bit == 0)
    }

    /// Get the addend value for a specific counter index
    #[inline(always)]
    fn addend(counter_index: u64) -> u64 {
        // Use precomputed value when possible
        if counter_index < 64 {
            Self::ADDENDS[counter_index as usize]
        } else {
            1u64 << ((Self::NUMBER_OF_COUNTERS - counter_index - 1) * Self::BITS_PER_COUNTER as u64)
        }
    }

    /// Set (increment) the counter for the given signal index
    ///
    /// Returns (tree_was_empty, set_successful)
    /// - tree_was_empty: true if this set caused the tree to transition from empty to non-empty
    /// - set_successful: true if the signal was successfully set (wasn't already set for leaf nodes)
    #[inline(always)]
    pub fn set(&self, signal_index: u64) -> (bool, bool) {
        let counter_index = signal_index / Self::COUNTER_CAPACITY;

        if Self::IS_ROOT {
            if !Self::IS_LEAF {
                // Root non-leaf node: increment counter, return if was empty
                let prev = self
                    .value
                    .fetch_add(Self::addend(counter_index), Ordering::AcqRel);
                (prev == 0, true)
            } else {
                // Root leaf node: set bit, return if was empty and if bit wasn't already set
                let bit = 0x8000_0000_0000_0000u64 >> counter_index;
                let prev = self.value.fetch_or(bit, Ordering::AcqRel);
                (prev == 0, (prev & bit) == 0)
            }
        } else {
            if !Self::IS_LEAF {
                // Non-root non-leaf node: just increment counter
                self.value
                    .fetch_add(Self::addend(counter_index), Ordering::AcqRel);
                (true, true)
            } else {
                // Non-root leaf node: set bit, return if bit wasn't already set
                let bit = 0x8000_0000_0000_0000u64 >> counter_index;
                let prev = self.value.fetch_or(bit, Ordering::AcqRel);
                (false, (prev & bit) == 0)
            }
        }
    }

    /// Clear (reset to 0) the counter for the given signal index
    ///
    /// Returns true if the counter was non-zero before clearing
    #[inline(always)]
    pub fn clear(&self, signal_index: u64) -> bool {
        let counter_index = signal_index / Self::COUNTER_CAPACITY;
        let bit = 0x8000_0000_0000_0000u64 >> counter_index;
        let prev = self.value.fetch_and(!bit, Ordering::AcqRel);
        (prev & bit) != 0
    }

    /// Select and decrement a counter
    ///
    /// Returns (counter_index, node_is_now_empty, updated_bias_hint)
    /// Returns (INVALID_SIGNAL_INDEX, false, bias_hint) if node is empty
    #[inline(always)]
    pub fn select<S: Selector>(&self, bias_flags: u64, bias_hint: u64) -> (SignalIndex, bool, u64) {
        let mut expected = self.value.load(Ordering::Acquire);

        while expected != 0 {
            let (counter_index, new_hint) = S::select(
                // 64,
                Self::NUMBER_OF_COUNTERS,
                Self::BITS_PER_COUNTER,
                bias_flags,
                expected,
                bias_hint,
            );

            if !Self::IS_LEAF {
                // Non-leaf node: decrement the counter
                let desired = expected - Self::addend(counter_index);
                match self.value.compare_exchange_weak(
                    expected,
                    desired,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return (counter_index, desired == 0, new_hint),
                    Err(e) => expected = e,
                }
            } else {
                // Leaf node: clear the bit
                let bit = 0x8000_0000_0000_0000u64 >> counter_index;
                expected = self.value.fetch_and(!bit, Ordering::AcqRel);
                if (expected & bit) == bit {
                    return (counter_index, expected == bit, new_hint);
                } else {
                    // crate::util::random::rapidrng_fast(&mut bias_hint);
                    // return (INVALID_SIGNAL_INDEX, false, bias_hint);
                }
            }
        }

        (INVALID_SIGNAL_INDEX, false, bias_hint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_set_select() {
        // Leaf node with capacity 64, also root
        let node: Node<64, 64> = Node::new();

        assert!(node.empty());

        // Set signal 0
        let (was_empty, success) = node.set(0);
        assert!(was_empty);
        assert!(success);
        assert!(!node.empty());

        // Set signal 0 again - should fail (already set)
        let (was_empty, success) = node.set(0);
        assert!(!was_empty);
        assert!(!success);

        // Set signal 1
        let (was_empty, success) = node.set(1);
        assert!(!was_empty);
        assert!(success);

        // Select should return one of the set signals
        let (idx, is_empty, _) = node.select::<DefaultSelector>(0, 0);
        assert!(idx == 0 || idx == 1);
        assert!(!is_empty);

        // Select again
        let (idx2, is_empty, _) = node.select::<DefaultSelector>(0, 0);
        assert!(idx2 == 0 || idx2 == 1);
        assert!(idx != idx2);
        assert!(is_empty);

        assert!(node.empty());
    }

    #[test]
    fn test_non_leaf_node() {
        // Non-leaf node with capacity 512, root
        let node: Node<512, 512> = Node::new();

        assert!(node.empty());

        // Set signal 0
        let (was_empty, success) = node.set(0);
        assert!(was_empty);
        assert!(success);

        // Set same counter again - should succeed for non-leaf (incrementing)
        let (_, success) = node.set(0);
        assert!(success);

        // Select twice to decrement counter
        let (idx, is_empty, _) = node.select::<DefaultSelector>(0, 0);
        assert_eq!(idx, 0);
        assert!(!is_empty);

        let (idx, is_empty, _) = node.select::<DefaultSelector>(0, 0);
        assert_eq!(idx, 0);
        assert!(is_empty);
    }
}
