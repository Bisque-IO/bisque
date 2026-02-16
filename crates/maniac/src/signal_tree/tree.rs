//! Signal Tree - The main tree structure
//!
//! A signal tree provides efficient lock-free set/select operations
//! for managing a large number of signals.

use std::sync::atomic::Ordering;

use super::level::*;
use super::node::{DefaultSelector, LargestChildSelector};
#[allow(unused_imports)]
use super::{INVALID_SIGNAL_INDEX, SignalIndex, minimum_bit_count};

/// Trait for signal tree operations
pub trait SignalTreeOps {
    /// The capacity of this tree
    const CAPACITY: u64;

    /// Check if the tree is empty
    fn empty(&self) -> bool;

    /// Set a signal
    /// Returns (tree_was_empty, set_successful)
    fn set(&self, signal_index: SignalIndex) -> (bool, bool);

    /// Select a signal using the default selector
    /// Returns (signal_index, tree_is_now_empty)
    fn select(&self, bias: u64) -> (SignalIndex, bool);

    /// Select a signal using the default selector with hint
    /// Returns (signal_index, tree_is_now_empty, new_hint)
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64);

    /// Select a signal using the largest child selector
    /// Returns (signal_index, tree_is_now_empty)
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool);
}

/// Signal tree with capacity 64
/// Flattened structure - directly uses an AtomicU64 since there's always exactly 1 node.
// #[cfg_attr(target_arch = "x86_64", repr(C, align(64)))]
// #[cfg_attr(
//     any(
//         target_arch = "aarch64",
//         target_arch = "arm64ec",
//         target_arch = "powerpc64"
//     ),
//     repr(C, align(128))
// )]
// #[cfg_attr(
//     not(any(
//         target_arch = "x86_64",
//         target_arch = "aarch64",
//         target_arch = "arm64ec",
//         target_arch = "powerpc64",
//     )),
//     repr(C, align(64))
// )]
// pub struct SignalTree64 {
//     value: std::sync::atomic::AtomicU64,
// }

// impl Default for SignalTree64 {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl SignalTree64 {
//     pub const CAPACITY: u64 = 64;

//     pub const fn new() -> Self {
//         Self {
//             value: std::sync::atomic::AtomicU64::new(0),
//         }
//     }

//     /// Load the root value with relaxed ordering (for SIMD scanning)
//     #[inline]
//     pub fn load_relaxed(&self) -> u64 {
//         self.value.load(std::sync::atomic::Ordering::Relaxed)
//     }

//     /// Clear a signal bit atomically
//     #[inline]
//     pub fn clear_bit(&self, signal_index: SignalIndex) -> bool {
//         let bit = 0x8000_0000_0000_0000u64 >> signal_index;
//         let prev = self
//             .value
//             .fetch_and(!bit, std::sync::atomic::Ordering::AcqRel);
//         (prev & bit) != 0
//     }
// }

// impl SignalTreeOps for SignalTree64 {
//     const CAPACITY: u64 = 64;

//     #[inline]
//     fn empty(&self) -> bool {
//         self.value.load(std::sync::atomic::Ordering::Relaxed) == 0
//     }

//     #[inline]
//     fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
//         // Use high-to-low bit ordering to match node.rs convention
//         let bit = 0x8000_0000_0000_0000u64 >> signal_index;
//         let prev = self
//             .value
//             .fetch_or(bit, std::sync::atomic::Ordering::AcqRel);
//         // (tree_was_empty, set_successful)
//         (prev == 0, (prev & bit) == 0)
//     }

//     #[inline]
//     fn select(&self, bias: u64) -> (SignalIndex, bool) {
//         let (idx, empty, _) = self.select_with_hint(bias, 0);
//         (idx, empty)
//     }

//     #[inline]
//     fn select_with_hint(&self, bias: u64, _hint: u64) -> (SignalIndex, bool, u64) {
//         use std::sync::atomic::Ordering;

//         let value = self.value.load(Ordering::Acquire);
//         if value == 0 {
//             return (INVALID_SIGNAL_INDEX, true, 0);
//         }

//         // Use simple cycling: start from bias position and find next set bit
//         // Bits are numbered high-to-low: signal 0 = bit 63, signal 63 = bit 0
//         // So we use rotate_left and leading_zeros
//         let start_pos = (bias & 63) as u32;

//         // Rotate value so that start_pos becomes the high bit, then find first set bit
//         let rotated = value.rotate_left(start_pos);
//         let offset = rotated.leading_zeros();

//         if offset >= 64 {
//             // No bits set (shouldn't happen since we checked value != 0)
//             return (INVALID_SIGNAL_INDEX, value == 0, 0);
//         }

//         // The actual signal index
//         let result = ((start_pos + offset) & 63) as u64;

//         // Try to acquire the bit (using high-to-low bit ordering to match set())
//         let bit = 0x8000_0000_0000_0000u64 >> result;
//         let prev = self.value.fetch_and(!bit, Ordering::AcqRel);

//         if (prev & bit) != 0 {
//             let is_empty = (prev & !bit) == 0;
//             // Return the selected index as the new hint for cycling
//             (result, is_empty, result)
//         } else {
//             // Lost race - bit was cleared by another thread
//             (INVALID_SIGNAL_INDEX, prev == 0, result)
//         }
//     }

//     #[inline]
//     fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
//         use std::sync::atomic::Ordering;

//         let value = self.value.load(Ordering::Acquire);
//         if value == 0 {
//             return (INVALID_SIGNAL_INDEX, true);
//         }

//         // Find the largest signal index (highest numbered signal that is set)
//         // With high-to-low ordering: signal 0 = bit 63, signal 63 = bit 0
//         // So the largest signal index corresponds to the lowest set bit position
//         // trailing_zeros gives the position of the lowest set bit
//         // signal_index = 63 - bit_position
//         let bit_pos = value.trailing_zeros() as u64;
//         // let bit_pos = crate::util::bits::find_nearest_by_distance(value, bias & 63);
//         let signal_index = 63 - bit_pos;

//         // Calculate the bit to clear using high-to-low ordering (same as set())
//         let bit = 0x8000_0000_0000_0000u64 >> signal_index;
//         let prev = self.value.fetch_and(!bit, Ordering::AcqRel);

//         if (prev & bit) != 0 {
//             let is_empty = (prev & !bit) == 0;
//             (signal_index, is_empty)
//         } else {
//             (INVALID_SIGNAL_INDEX, prev == 0)
//         }
//     }
// }

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
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
    )),
    repr(C, align(64))
)]
pub struct SignalTree64 {
    root: Level64_1,
}

impl Default for SignalTree64 {
    fn default() -> Self {
        Self::new()
    }
}

/// Find the nearest set bit to start_index using high-to-low bit ordering.
/// Bit ordering: signal N = bit (63 - N), i.e., signal 0 = bit 63 (MSB)
/// Equivalent to: let bit = 0x8000_0000_0000_0000u64 >> signal_index
/// Returns 64 if no bits are set.
pub fn find_nearest_by_distance(value: u64, start_index: u64) -> u64 {
    if start_index >= 64 || value == 0 {
        return 64;
    }

    // Signal N is at bit position (63 - N)
    // "Forward" = signals >= start_index = bits with positions <= (63 - start_index)
    // "Backward" = signals < start_index = bits with positions > (63 - start_index)

    // Mask for forward signals (>= start_index): clear high bits, keep low bits
    // For start_index=0: keep all bits. For start_index=1: clear bit 63, etc.
    let mask_forward = 0xFFFF_FFFF_FFFF_FFFFu64 >> start_index;
    let forward_bits = value & mask_forward;

    // Mask for backward signals (< start_index): the complement
    let backward_bits = value & !mask_forward;

    if forward_bits != 0 {
        // Find first set bit in forward region (smallest signal index >= start_index)
        let forward_index = forward_bits.leading_zeros() as u64;

        if backward_bits == 0 {
            return forward_index;
        }

        let forward_dist = forward_index - start_index;

        // Find last set bit in backward region (largest signal index < start_index)
        let backward_index = 63 - backward_bits.trailing_zeros() as u64;
        let backward_dist = start_index - backward_index;

        if forward_dist <= backward_dist {
            forward_index
        } else {
            backward_index
        }
    } else if backward_bits != 0 {
        // Only backward bits set - find closest (largest signal index < start_index)
        63 - backward_bits.trailing_zeros() as u64
    } else {
        64
    }
}

impl SignalTree64 {
    pub const CAPACITY: u64 = 64;

    pub const fn new() -> Self {
        Self {
            root: Level64_1::new(),
        }
    }

    #[inline]
    pub fn load(&self) -> u64 {
        self.root.load()
        // self.value.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Load the root value with relaxed ordering (for SIMD scanning)
    #[inline]
    pub fn load_relaxed(&self) -> u64 {
        self.root.load_relaxed()
        // self.value.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Clear a signal bit atomically
    #[inline]
    pub fn clear_bit(&self, signal_index: SignalIndex) -> bool {
        self.root.clear_bit(signal_index)
    }

    /// Try to atomically claim a specific signal bit.
    /// Returns (success, tree_is_now_empty).
    /// If success is true, the caller owns this signal and must process it.
    #[inline]
    pub fn try_claim(&self, signal_index: SignalIndex) -> (bool, bool) {
        let bit = 0x8000_0000_0000_0000u64 >> signal_index;
        let prev = self.root.value().fetch_and(!bit, Ordering::AcqRel);
        let success = (prev & bit) != 0;
        let is_empty = (prev & !bit) == 0;
        (success, is_empty)
    }
}

impl SignalTreeOps for SignalTree64 {
    const CAPACITY: u64 = 64;

    #[inline]
    fn empty(&self) -> bool {
        self.root.empty()
    }

    #[inline]
    fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.root.set(signal_index)
    }

    #[inline]
    fn select(&self, bias: u64) -> (SignalIndex, bool) {
        let (idx, empty, _) = self.select_with_hint(bias, 0);
        (idx, empty)
    }

    // #[inline]
    // fn select_with_hint(&self, bias: u64, _hint: u64) -> (SignalIndex, bool, u64) {
    //     use std::sync::atomic::Ordering;

    //     let value = self.root.value().load(Ordering::Acquire);
    //     if value == 0 {
    //         return (INVALID_SIGNAL_INDEX, true, 0);
    //     }

    //     // Use simple cycling: start from bias position and find next set bit
    //     // Bits are numbered high-to-low: signal 0 = bit 63, signal 63 = bit 0
    //     // So we use rotate_left and leading_zeros
    //     let start_pos = (bias & 63) as u32;

    //     // Rotate value so that start_pos becomes the high bit, then find first set bit
    //     let rotated = value.rotate_left(start_pos);
    //     let offset = rotated.leading_zeros();

    //     if offset >= 64 {
    //         // No bits set (shouldn't happen since we checked value != 0)
    //         return (INVALID_SIGNAL_INDEX, value == 0, 0);
    //     }

    //     // The actual signal index
    //     let result = ((start_pos + offset) & 63) as u64;

    //     // Try to acquire the bit (using high-to-low bit ordering to match set())
    //     let bit = 0x8000_0000_0000_0000u64 >> result;
    //     let prev = self.root.value().fetch_and(!bit, Ordering::AcqRel);

    //     if (prev & bit) != 0 {
    //         let is_empty = (prev & !bit) == 0;
    //         // Return the selected index as the new hint for cycling
    //         (result, is_empty, result)
    //     } else {
    //         // Lost race - bit was cleared by another thread
    //         (INVALID_SIGNAL_INDEX, prev == 0, result)
    //     }
    // }

    // #[inline]
    // fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
    //     // let value = self.root.value().load(Ordering::Acquire);
    //     // if value == 0 {
    //     //     return (INVALID_SIGNAL_INDEX, true, 0);
    //     // }

    //     // // let bit_index = crate::util::bits::find_nearest_by_distance(value, hint & 63);
    //     // let bit_index = crate::util::bits::select_random_bit(value, hint & 63);

    //     // // let start_pos = hint & 63;
    //     // // let offset = value.trailing_zeros();
    //     // // let result = ((start_pos + offset) & 63) as u64;

    //     // // Try to acquire the bit (using high-to-low bit ordering to match set())
    //     // let bit = 0x8000_0000_0000_0000u64 >> bit_index;

    //     // let prev = self.root.value().fetch_and(!bit, Ordering::AcqRel);

    //     // if (prev & bit) != 0 {
    //     //     let is_empty = (prev & !bit) == 0;
    //     //     // Return the selected index as the new hint for cycling
    //     //     (bit_index, is_empty, hint.wrapping_add(1))
    //     // } else {
    //     //     // Lost race - bit was cleared by another thread
    //     //     (INVALID_SIGNAL_INDEX, prev == 0, hint)
    //     // }
    //     use std::sync::atomic::Ordering;

    //     let value = self.root.value().load(Ordering::Acquire);
    //     if value == 0 {
    //         return (INVALID_SIGNAL_INDEX, true, hint);
    //     }

    //     // Select the Nth set bit, where N = hint mod popcount
    //     // This ensures fair round-robin across all set bits regardless of their positions
    //     let popcount = value.count_ones();
    //     let target_bit = (hint as u32) % popcount;

    //     // Find the target_bit-th set bit
    //     let mut remaining = target_bit;
    //     let mut current = value;
    //     loop {
    //         // Find position of first set bit (signal 0 = bit 63, signal 63 = bit 0)
    //         let leading = current.leading_zeros();
    //         if leading >= 64 {
    //             // No more bits - shouldn't happen but handle gracefully
    //             return (INVALID_SIGNAL_INDEX, value == 0, hint);
    //         }

    //         let signal_index = leading as u64;
    //         let bit = 0x8000_0000_0000_0000u64 >> signal_index;

    //         if remaining == 0 {
    //             // This is the target bit - try to claim it
    //             let prev = self.root.value().fetch_and(!bit, Ordering::AcqRel);
    //             if (prev & bit) != 0 {
    //                 let is_empty = (prev & !bit) == 0;
    //                 return (signal_index, is_empty, hint.wrapping_add(1));
    //             }
    //             // Lost race - reload and try again with fresh value
    //             let new_value = self.root.value().load(Ordering::Acquire);
    //             if new_value == 0 {
    //                 return (INVALID_SIGNAL_INDEX, true, hint);
    //             }
    //             // Recalculate with new value
    //             let new_popcount = new_value.count_ones();
    //             let new_target = (hint as u32) % new_popcount;
    //             remaining = new_target;
    //             current = new_value;
    //             continue;
    //         }

    //         // Not the target bit yet - clear it and continue
    //         remaining -= 1;
    //         current &= !bit;
    //     }
    // }

    #[inline]
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, new_hint) = self.root.select::<DefaultSelector>(bias, 0, hint);
        (idx, empty, new_hint)
    }

    // #[inline]
    // fn select_with_hint(&self, _bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
    //     use std::sync::atomic::Ordering;

    //     let value = self.root.value().load(Ordering::Acquire);
    //     if value == 0 {
    //         return (INVALID_SIGNAL_INDEX, true, hint);
    //     }

    //     // Select the Nth set bit, where N = hint mod popcount
    //     // This ensures fair round-robin across all set bits regardless of their positions
    //     let popcount = value.count_ones();
    //     let target_bit = (hint as u32) % popcount;

    //     // Find the target_bit-th set bit using leading_zeros
    //     let mut remaining = target_bit;
    //     let mut current = value;
    //     loop {
    //         // Find position of first set bit (signal 0 = bit 63, signal 63 = bit 0)
    //         let leading = current.leading_zeros();
    //         if leading >= 64 {
    //             // No more bits - shouldn't happen but handle gracefully
    //             return (INVALID_SIGNAL_INDEX, value == 0, hint);
    //         }

    //         let signal_index = leading as u64;
    //         let bit = 0x8000_0000_0000_0000u64 >> signal_index;

    //         if remaining == 0 {
    //             // This is the target bit - try to claim it
    //             let prev = self.root.value().fetch_and(!bit, Ordering::AcqRel);
    //             if (prev & bit) != 0 {
    //                 let is_empty = (prev & !bit) == 0;
    //                 return (signal_index, is_empty, hint);
    //             }
    //             // Lost race - reload and try again with fresh value
    //             let new_value = self.root.value().load(Ordering::Acquire);
    //             if new_value == 0 {
    //                 return (INVALID_SIGNAL_INDEX, true, hint);
    //             }
    //             // Recalculate with new value
    //             let new_popcount = new_value.count_ones();
    //             let new_target = (hint as u32) % new_popcount;
    //             remaining = new_target;
    //             current = new_value;
    //             continue;
    //         }

    //         // Not the target bit yet - clear it from current and continue
    //         remaining -= 1;
    //         current &= !bit;
    //     }
    // }

    // #[inline]
    // fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
    //     use std::sync::atomic::Ordering;

    //     let value = self.root.value().load(Ordering::Acquire);
    //     if value == 0 {
    //         return (INVALID_SIGNAL_INDEX, true);
    //     }

    //     // Find the largest signal index (highest numbered signal that is set)
    //     // With high-to-low ordering: signal 0 = bit 63, signal 63 = bit 0
    //     // So the largest signal index corresponds to the lowest set bit position
    //     // trailing_zeros gives the position of the lowest set bit
    //     // signal_index = 63 - bit_position
    //     let bit_pos = value.trailing_zeros() as u64;
    //     // let bit_pos = crate::util::bits::select_random_bit(value, bias & 63);
    //     let signal_index = 63 - bit_pos;

    //     // Calculate the bit to clear using high-to-low ordering (same as set())
    //     let bit = 0x8000_0000_0000_0000u64 >> signal_index;
    //     let prev = self.root.value().fetch_and(!bit, Ordering::AcqRel);

    //     if (prev & bit) != 0 {
    //         let is_empty = (prev & !bit) == 0;
    //         (signal_index, is_empty)
    //     } else {
    //         (INVALID_SIGNAL_INDEX, prev == 0)
    //     }
    // }

    #[inline]
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, _) = self.root.select::<LargestChildSelector>(bias, 0, 0);
        (idx, empty)
    }
}

/// Signal tree with capacity 512
pub struct SignalTree512 {
    root: Level512_1,
}

impl Default for SignalTree512 {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalTree512 {
    pub const CAPACITY: u64 = 512;

    pub const fn new() -> Self {
        Self {
            root: Level512_1::new(),
        }
    }
}

impl SignalTreeOps for SignalTree512 {
    const CAPACITY: u64 = 512;

    #[inline]
    fn empty(&self) -> bool {
        self.root.empty()
    }

    #[inline]
    fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.root.set(signal_index)
    }

    #[inline]
    fn select(&self, bias: u64) -> (SignalIndex, bool) {
        let (idx, empty, _) = self.select_with_hint(bias, 0);
        (idx, empty)
    }

    #[inline]
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        self.root.select::<DefaultSelector>(bias, 0, hint)
    }

    #[inline]
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, _) = self.root.select::<LargestChildSelector>(bias, 0, 0);
        (idx, empty)
    }
}

/// Signal tree with capacity 2048
pub struct SignalTree2048 {
    root: Level2048_1,
}

impl Default for SignalTree2048 {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalTree2048 {
    pub const CAPACITY: u64 = 2048;

    pub const fn new() -> Self {
        Self {
            root: Level2048_1::new(),
        }
    }
}

impl SignalTreeOps for SignalTree2048 {
    const CAPACITY: u64 = 2048;

    #[inline]
    fn empty(&self) -> bool {
        self.root.empty()
    }

    #[inline]
    fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.root.set(signal_index)
    }

    #[inline]
    fn select(&self, bias: u64) -> (SignalIndex, bool) {
        let (idx, empty, _) = self.select_with_hint(bias, 0);
        (idx, empty)
    }

    #[inline]
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        self.root.select::<DefaultSelector>(bias, 0, hint)
    }

    #[inline]
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, _) = self.root.select::<LargestChildSelector>(bias, 0, 0);
        (idx, empty)
    }
}

/// Signal tree with capacity 8192
pub struct SignalTree8192 {
    root: Level8192_1,
}

impl Default for SignalTree8192 {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalTree8192 {
    pub const CAPACITY: u64 = 8192;

    pub const fn new() -> Self {
        Self {
            root: Level8192_1::new(),
        }
    }
}

impl SignalTreeOps for SignalTree8192 {
    const CAPACITY: u64 = 8192;

    #[inline]
    fn empty(&self) -> bool {
        self.root.empty()
    }

    #[inline]
    fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.root.set(signal_index)
    }

    #[inline]
    fn select(&self, bias: u64) -> (SignalIndex, bool) {
        let (idx, empty, _) = self.select_with_hint(bias, 0);
        (idx, empty)
    }

    #[inline]
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        self.root.select::<DefaultSelector>(bias, 0, hint)
    }

    #[inline]
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, _) = self.root.select::<LargestChildSelector>(bias, 0, 0);
        (idx, empty)
    }
}

/// Signal tree with capacity 32768
pub struct SignalTree32768 {
    root: Level32768_1,
}

impl Default for SignalTree32768 {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalTree32768 {
    pub const CAPACITY: u64 = 32768;

    pub const fn new() -> Self {
        Self {
            root: Level32768_1::new(),
        }
    }
}

impl SignalTreeOps for SignalTree32768 {
    const CAPACITY: u64 = 32768;

    #[inline]
    fn empty(&self) -> bool {
        self.root.empty()
    }

    #[inline]
    fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.root.set(signal_index)
    }

    #[inline]
    fn select(&self, bias: u64) -> (SignalIndex, bool) {
        let (idx, empty, _) = self.select_with_hint(bias, 0);
        (idx, empty)
    }

    #[inline]
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        self.root.select::<DefaultSelector>(bias, 0, hint)
    }

    #[inline]
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, _) = self.root.select::<LargestChildSelector>(bias, 0, 0);
        (idx, empty)
    }
}

/// Signal tree with capacity 131072
pub struct SignalTree131072 {
    root: Level131072_1,
}

impl Default for SignalTree131072 {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalTree131072 {
    pub const CAPACITY: u64 = 131072;

    pub const fn new() -> Self {
        Self {
            root: Level131072_1::new(),
        }
    }
}

impl SignalTreeOps for SignalTree131072 {
    const CAPACITY: u64 = 131072;

    #[inline]
    fn empty(&self) -> bool {
        self.root.empty()
    }

    #[inline]
    fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.root.set(signal_index)
    }

    #[inline]
    fn select(&self, bias: u64) -> (SignalIndex, bool) {
        let (idx, empty, _) = self.select_with_hint(bias, 0);
        (idx, empty)
    }

    #[inline]
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        self.root.select::<DefaultSelector>(bias, 0, hint)
    }

    #[inline]
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, _) = self.root.select::<LargestChildSelector>(bias, 0, 0);
        (idx, empty)
    }
}

/// Signal tree with capacity 1048576 (~1M)
pub struct SignalTree1048576 {
    root: Level1048576_1,
}

impl Default for SignalTree1048576 {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalTree1048576 {
    pub const CAPACITY: u64 = 1048576;

    pub fn new() -> Self {
        Self {
            root: Level1048576_1::new(),
        }
    }
}

impl SignalTreeOps for SignalTree1048576 {
    const CAPACITY: u64 = 1048576;

    #[inline]
    fn empty(&self) -> bool {
        self.root.empty()
    }

    #[inline]
    fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.root.set(signal_index)
    }

    #[inline]
    fn select(&self, bias: u64) -> (SignalIndex, bool) {
        let (idx, empty, _) = self.select_with_hint(bias, 0);
        (idx, empty)
    }

    #[inline]
    fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        self.root.select::<DefaultSelector>(bias, 0, hint)
    }

    #[inline]
    fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        let bias_bits = 65 - minimum_bit_count(Self::CAPACITY);
        let bias = bias << bias_bits;
        let (idx, empty, _) = self.root.select::<LargestChildSelector>(bias, 0, 0);
        (idx, empty)
    }
}

/// Dynamic signal tree that can hold different sized trees
pub enum SignalTree {
    Tree64(Box<SignalTree64>),
    Tree512(Box<SignalTree512>),
    Tree2048(Box<SignalTree2048>),
    Tree8192(Box<SignalTree8192>),
    Tree32768(Box<SignalTree32768>),
    Tree131072(Box<SignalTree131072>),
    Tree1048576(Box<SignalTree1048576>),
}

impl SignalTree {
    /// Create a new signal tree with at least the requested capacity
    pub fn new(requested_capacity: u64) -> Self {
        let capacity = super::select_tree_size(requested_capacity);
        match capacity {
            64 => SignalTree::Tree64(Box::new(SignalTree64::new())),
            512 => SignalTree::Tree512(Box::new(SignalTree512::new())),
            2048 => SignalTree::Tree2048(Box::new(SignalTree2048::new())),
            8192 => SignalTree::Tree8192(Box::new(SignalTree8192::new())),
            32768 => SignalTree::Tree32768(Box::new(SignalTree32768::new())),
            131072 => SignalTree::Tree131072(Box::new(SignalTree131072::new())),
            1048576 => SignalTree::Tree1048576(Box::new(SignalTree1048576::new())),
            _ => SignalTree::Tree1048576(Box::new(SignalTree1048576::new())),
        }
    }

    /// Get the capacity of this tree
    pub fn capacity(&self) -> u64 {
        match self {
            SignalTree::Tree64(_) => 64,
            SignalTree::Tree512(_) => 512,
            SignalTree::Tree2048(_) => 2048,
            SignalTree::Tree8192(_) => 8192,
            SignalTree::Tree32768(_) => 32768,
            SignalTree::Tree131072(_) => 131072,
            SignalTree::Tree1048576(_) => 1048576,
        }
    }

    /// Check if the tree is empty
    #[inline]
    pub fn empty(&self) -> bool {
        match self {
            SignalTree::Tree64(t) => t.empty(),
            SignalTree::Tree512(t) => t.empty(),
            SignalTree::Tree2048(t) => t.empty(),
            SignalTree::Tree8192(t) => t.empty(),
            SignalTree::Tree32768(t) => t.empty(),
            SignalTree::Tree131072(t) => t.empty(),
            SignalTree::Tree1048576(t) => t.empty(),
        }
    }

    /// Set a signal
    #[inline]
    pub fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        match self {
            SignalTree::Tree64(t) => t.set(signal_index),
            SignalTree::Tree512(t) => t.set(signal_index),
            SignalTree::Tree2048(t) => t.set(signal_index),
            SignalTree::Tree8192(t) => t.set(signal_index),
            SignalTree::Tree32768(t) => t.set(signal_index),
            SignalTree::Tree131072(t) => t.set(signal_index),
            SignalTree::Tree1048576(t) => t.set(signal_index),
        }
    }

    /// Select a signal
    #[inline]
    pub fn select(&self, bias: u64) -> (SignalIndex, bool) {
        match self {
            SignalTree::Tree64(t) => t.select(bias),
            SignalTree::Tree512(t) => t.select(bias),
            SignalTree::Tree2048(t) => t.select(bias),
            SignalTree::Tree8192(t) => t.select(bias),
            SignalTree::Tree32768(t) => t.select(bias),
            SignalTree::Tree131072(t) => t.select(bias),
            SignalTree::Tree1048576(t) => t.select(bias),
        }
    }

    /// Select a signal with hint
    #[inline]
    pub fn select_with_hint(&self, bias: u64, hint: u64) -> (SignalIndex, bool, u64) {
        match self {
            SignalTree::Tree64(t) => t.select_with_hint(bias, hint),
            SignalTree::Tree512(t) => t.select_with_hint(bias, hint),
            SignalTree::Tree2048(t) => t.select_with_hint(bias, hint),
            SignalTree::Tree8192(t) => t.select_with_hint(bias, hint),
            SignalTree::Tree32768(t) => t.select_with_hint(bias, hint),
            SignalTree::Tree131072(t) => t.select_with_hint(bias, hint),
            SignalTree::Tree1048576(t) => t.select_with_hint(bias, hint),
        }
    }

    /// Select using largest child selector
    #[inline]
    pub fn select_largest(&self, bias: u64) -> (SignalIndex, bool) {
        match self {
            SignalTree::Tree64(t) => t.select_largest(bias),
            SignalTree::Tree512(t) => t.select_largest(bias),
            SignalTree::Tree2048(t) => t.select_largest(bias),
            SignalTree::Tree8192(t) => t.select_largest(bias),
            SignalTree::Tree32768(t) => t.select_largest(bias),
            SignalTree::Tree131072(t) => t.select_largest(bias),
            SignalTree::Tree1048576(t) => t.select_largest(bias),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_tree_64_basic() {
        let tree = SignalTree64::new();

        assert!(tree.empty());

        // Set signal 0
        let (was_empty, success) = tree.set(0);
        assert!(was_empty);
        assert!(success);
        assert!(!tree.empty());

        // Select it back
        let (idx, is_empty) = tree.select(0);
        assert_eq!(idx, 0);
        assert!(is_empty);
        assert!(tree.empty());
    }

    #[test]
    fn test_signal_tree_64_multiple() {
        let tree = SignalTree64::new();

        // Set multiple signals
        for i in 0..64 {
            let (_, success) = tree.set(i);
            assert!(success);
        }

        // Select them all back
        for _ in 0..64 {
            let (idx, _) = tree.select(0);
            assert_ne!(idx, INVALID_SIGNAL_INDEX);
        }

        assert!(tree.empty());
    }

    #[test]
    fn test_signal_tree_512() {
        let tree = SignalTree512::new();

        assert!(tree.empty());

        // Set signals across the tree
        for i in (0..512).step_by(7) {
            let (_, success) = tree.set(i);
            assert!(success);
        }

        assert!(!tree.empty());

        // Select them all
        let mut count = 0;
        loop {
            let (idx, _) = tree.select(count);
            if idx == INVALID_SIGNAL_INDEX {
                break;
            }
            count += 1;
        }

        assert!(tree.empty());
        assert_eq!(count, (512 + 6) / 7); // ceil(512/7)
    }

    #[test]
    fn test_dynamic_signal_tree() {
        let tree = SignalTree::new(100);
        assert_eq!(tree.capacity(), 512);

        let (was_empty, success) = tree.set(50);
        assert!(was_empty);
        assert!(success);

        let (idx, is_empty) = tree.select(0);
        assert_eq!(idx, 50);
        assert!(is_empty);
    }
}

#[cfg(test)]
mod large_tree_tests {
    use super::*;

    #[test]
    fn test_signal_tree_1048576_basic() {
        let tree = SignalTree1048576::new();

        assert!(tree.empty());

        // Set signal 0
        let (was_empty, success) = tree.set(0);
        assert!(was_empty, "Tree should have been empty");
        assert!(success, "Set should succeed");
        assert!(!tree.empty());

        // Select it back
        let (idx, is_empty) = tree.select(0);
        assert_eq!(idx, 0, "Expected to select index 0, got {}", idx);
        assert!(is_empty, "Tree should now be empty");
        assert!(tree.empty());
    }

    #[test]
    fn test_signal_tree_1048576_multiple_signals() {
        let tree = SignalTree1048576::new();

        // Set 1000 signals spread across the tree
        let mut set_count = 0;
        for i in (0..1048576).step_by(1048) {
            let (_, success) = tree.set(i);
            if success {
                set_count += 1;
            }
        }

        println!("Set {} signals", set_count);

        // Select them all
        let mut select_count = 0;
        loop {
            let (idx, _) = tree.select(select_count);
            if idx == INVALID_SIGNAL_INDEX {
                break;
            }
            select_count += 1;
        }

        println!("Selected {} signals", select_count);
        assert_eq!(
            set_count, select_count,
            "Should select exactly as many as set"
        );
    }
}
