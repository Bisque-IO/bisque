//! Signal Tree - A lock-free hierarchical signaling structure
//!
//! The signal tree provides O(log N) set and select operations for managing
//! a large number of signals efficiently using atomic operations.

mod level;
mod node;
pub mod summary;
pub mod tree;

pub use tree::{
    SignalTree, SignalTree64, SignalTree512, SignalTree2048, SignalTree8192, SignalTree32768,
    SignalTree131072, SignalTree1048576, SignalTreeOps,
};

pub use summary::TreeSummary;

pub use node::select_64;

/// Type alias for signal indices
pub type SignalIndex = u64;

/// Invalid signal index constant (equivalent to ~0ull in C++)
pub const INVALID_SIGNAL_INDEX: SignalIndex = !0u64;

/// Calculate the minimum number of bits needed to represent a value
#[inline]
pub const fn minimum_bit_count(value: u64) -> u32 {
    if value == 0 {
        0
    } else {
        64 - value.leading_zeros()
    }
}

/// Calculate the minimum power of two >= value
#[inline]
pub const fn minimum_power_of_two(value: u64) -> u64 {
    if value <= 1 {
        1
    } else {
        1u64 << minimum_bit_count(value - 1)
    }
}

/// Check if a value is a power of two
#[inline]
pub const fn is_power_of_two(value: u64) -> bool {
    value.count_ones() == 1
}

/// Calculate the sub-counter arity for a given counter capacity
/// This determines how many sub-counters fit in a 64-bit word
pub const fn sub_counter_arity(counter_capacity: u64) -> u64 {
    sub_counter_arity_impl(counter_capacity, 64)
}

const fn sub_counter_arity_impl(counter_capacity: u64, n: u64) -> u64 {
    if n == 0 {
        return 0;
    }
    let bits_per_counter = 64 - (counter_capacity / n).leading_zeros() as u64;
    let bits_required = bits_per_counter * n;
    if bits_required <= 64 {
        n
    } else {
        sub_counter_arity_impl(counter_capacity, n / 2)
    }
}

/// Valid signal tree sizes
pub const VALID_TREE_SIZES: [u64; 16] = [
    64,       // 2^6
    64 << 3,  // 2^9  = 512
    64 << 5,  // 2^11 = 2048
    64 << 7,  // 2^13 = 8192
    64 << 9,  // 2^15 = 32768
    64 << 11, // 2^17 = 131072
    64 << 12, // 2^18 = 262144
    64 << 13, // 2^19 = 524288
    64 << 14, // 2^20 = 1048576
    64 << 15, // 2^21 = 2097152
    64 << 16, // 2^22 = 4194304
    64 << 17, // 2^23 = 8388608
    64 << 18, // 2^24 = 16777216
    64 << 19, // 2^25 = 33554432
    64 << 20, // 2^26 = 67108864
    64 << 21, // 2^27 = 134217728
];

/// Select the appropriate tree size for a requested capacity
pub const fn select_tree_size(requested: u64) -> u64 {
    let mut i = 0;
    while i < VALID_TREE_SIZES.len() {
        if VALID_TREE_SIZES[i] >= requested {
            return VALID_TREE_SIZES[i];
        }
        i += 1;
    }
    // Fallback for very large sizes
    1u64 << (64 - requested.leading_zeros())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimum_bit_count() {
        assert_eq!(minimum_bit_count(0), 0);
        assert_eq!(minimum_bit_count(1), 1);
        assert_eq!(minimum_bit_count(2), 2);
        assert_eq!(minimum_bit_count(3), 2);
        assert_eq!(minimum_bit_count(4), 3);
        assert_eq!(minimum_bit_count(64), 7);
    }

    #[test]
    fn test_minimum_power_of_two() {
        assert_eq!(minimum_power_of_two(1), 1);
        assert_eq!(minimum_power_of_two(2), 2);
        assert_eq!(minimum_power_of_two(3), 4);
        assert_eq!(minimum_power_of_two(5), 8);
        assert_eq!(minimum_power_of_two(64), 64);
        assert_eq!(minimum_power_of_two(65), 128);
    }

    #[test]
    fn test_is_power_of_two() {
        assert!(is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(!is_power_of_two(3));
        assert!(is_power_of_two(4));
        assert!(is_power_of_two(64));
        assert!(!is_power_of_two(65));
    }

    #[test]
    fn test_sub_counter_arity() {
        assert_eq!(sub_counter_arity(64), 64);
        assert_eq!(sub_counter_arity(512), 8);
        assert_eq!(sub_counter_arity(2048), 4);
    }

    #[test]
    fn test_select_tree_size() {
        assert_eq!(select_tree_size(1), 64);
        assert_eq!(select_tree_size(64), 64);
        assert_eq!(select_tree_size(65), 512);
        assert_eq!(select_tree_size(512), 512);
        assert_eq!(select_tree_size(1000), 2048);
    }
}
