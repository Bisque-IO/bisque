use std::sync::atomic::{AtomicU64, Ordering};

#[inline(always)]
pub fn size(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed).count_ones() as u64
}

#[inline(always)]
pub fn is_empty(value: &AtomicU64) -> bool {
    value.load(Ordering::Relaxed).count_ones() == 0
}

#[inline(always)]
pub fn set(value: &AtomicU64, index: u64) -> (bool, bool) {
    // let bit = 0x8000000000000000u64 >> index;
    let bit = 1u64 << index;
    let prev = value.fetch_or(bit, Ordering::AcqRel);
    // was empty; was_set
    (prev == 0, (prev & bit) == 0)
}

#[inline(always)]
pub fn clear(value: &AtomicU64, index: u64) -> (bool, bool) {
    // let bit = 0x8000000000000000u64 >> index;
    let bit = 1u64 << index;
    let prev = value.fetch_and(!bit, Ordering::AcqRel);
    // was empty; was_set
    (prev == 0, (prev & bit) == 0)
}

#[inline(always)]
pub fn set_with_bit(value: &AtomicU64, bit: u64) -> u64 {
    value.fetch_or(bit, Ordering::AcqRel)
}

#[inline(always)]
pub fn acquire(value: &AtomicU64, index: u64) -> bool {
    // let bit = 0x8000000000000000u64 >> index;
    if !is_set(value, index) {
        return false;
    }
    let bit = 1u64 << index;
    let previous = value.fetch_and(!bit, Ordering::AcqRel);
    (previous & bit) == bit
}

#[inline(always)]
pub fn try_acquire(value: &AtomicU64, index: u64) -> (u64, u64, bool) {
    if !is_set(value, index) {
        return (0, 0, false);
    }
    let bit = 1u64 << index;
    let previous = value.fetch_and(!bit, Ordering::AcqRel);
    (bit, previous, (previous & bit) == bit)
}

#[inline(always)]
pub fn is_set(value: &AtomicU64, index: u64) -> bool {
    // let bit = 0x8000000000000000u64 >> index;
    let bit = 1u64 << index;
    (value.load(Ordering::Relaxed) & bit) != 0
}

pub fn find_nearest_set_bit(value: u64, start_index: u64) -> u64 {
    if start_index >= 64 {
        return 64;
    }

    // First, try to find a set bit at or after the start_index
    let mask_forward = !((1u64 << start_index) - 1); // Clear bits before start_index
    let forward_bits = value & mask_forward;

    if forward_bits != 0 {
        // Found a set bit at or after start_index
        return forward_bits.trailing_zeros() as u64;
    }

    // If no bit found forward, search backwards from start_index
    let mask_backward = (1u64 << start_index) - 1; // Keep only bits before start_index
    let backward_bits = value & mask_backward;

    if backward_bits != 0 {
        // Found a set bit before start_index
        return 63 - backward_bits.leading_zeros() as u64;
    }

    // No set bits found
    64
}

pub fn find_nearest_by_distance0(value: u64, start_index: u64) -> u64 {
    let out_of_bounds = start_index >= 64;
    let idx = start_index & 63;

    let forward_bits = value & !((1u64 << idx) - 1);
    let backward_bits = value & ((1u64 << idx) - 1);

    let f_idx = forward_bits.trailing_zeros() as u64;
    let b_idx = 63 - backward_bits.leading_zeros() as u64;

    let f_valid = forward_bits != 0;
    let b_valid = backward_bits != 0;

    let f_dist = f_idx - idx;
    let b_dist = idx - b_idx;

    // Branchless selection: prefer forward on tie, handle invalid cases
    let use_forward = f_valid && (!b_valid || f_dist <= b_dist);
    let use_backward = b_valid && !use_forward;

    let result = if use_forward {
        f_idx
    } else if use_backward {
        b_idx
    } else {
        64
    };

    if out_of_bounds { 64 } else { result }
}

pub fn find_nearest_by_distance_branchless(value: u64, start_index: u64) -> u64 {
    // Handle out of bounds case
    let valid = (start_index < 64) as u64;
    let clamped_index = start_index & 63; // Equivalent to start_index % 64

    // Search forward and backward
    let mask_forward = !((1u64 << clamped_index) - 1);
    let forward_bits = value & mask_forward;
    let mask_backward = (1u64 << clamped_index) - 1;
    let backward_bits = value & mask_backward;

    // Calculate indices using bit manipulation to avoid branches
    let forward_tz = forward_bits.trailing_zeros() as u64;
    let forward_valid = (forward_bits != 0) as u64;
    let forward_index = forward_tz | (64 * (1 - forward_valid));

    let backward_lz = backward_bits.leading_zeros() as u64;
    let backward_valid = (backward_bits != 0) as u64;
    let backward_index = (63 - backward_lz) | (64 * (1 - backward_valid));

    // Calculate distances
    let forward_dist = forward_index - clamped_index;
    let backward_dist = clamped_index - backward_index;

    // Choose the closer one (forward wins ties)
    let choose_forward = ((forward_dist <= backward_dist) & (forward_valid != 0)) as u64;
    let choose_backward = ((backward_dist < forward_dist) & (backward_valid != 0)) as u64;

    let result = forward_index * choose_forward
        + backward_index * choose_backward
        + 64 * (1 - choose_forward) * (1 - choose_backward);

    // Return 64 if start_index was out of bounds, otherwise return result
    result | (64 * (1 - valid))
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

// // Alternative version that returns the distance as well
// pub fn find_nearest_set_bit_with_distance(value: u64, start_index: u64) -> Option<(u64, u64)> {
//     if start_index >= 64 {
//         return None;
//     }
//
//     // Search forward
//     let mask_forward = !((1u64 << start_index) - 1);
//     let forward_bits = value & mask_forward;
//
//     let forward_result = if forward_bits != 0 {
//         let index = forward_bits.trailing_zeros() as u64;
//         Some((index, index - start_index))
//     } else {
//         None
//     };
//
//     // Search backward
//     let mask_backward = (1u64 << start_index) - 1;
//     let backward_bits = value & mask_backward;
//
//     let backward_result = if backward_bits != 0 {
//         let index = 63 - backward_bits.leading_zeros() as u64;
//         Some((index, start_index - index))
//     } else {
//         None
//     };
//
//     // Return the closer one, preferring forward in case of tie
//     match (forward_result, backward_result) {
//         (Some((f_idx, f_dist)), Some((b_idx, b_dist))) => {
//             if f_dist <= b_dist {
//                 Some((f_idx, f_dist))
//             } else {
//                 Some((b_idx, b_dist))
//             }
//         }
//         (Some(forward), None) => Some(forward),
//         (None, Some(backward)) => Some(backward),
//         (None, None) => None,
//     }
// }
//
// // Version that prioritizes forward search (like your original Java code)
// pub fn find_nearest_set_bit_forward_priority(value: u64, start_index: u64) -> Option<u64> {
//     if start_index >= 64 {
//         return None;
//     }
//
//     // Try forward first (including start_index)
//     if start_index < 64 {
//         let forward_mask = value >> start_index;
//         if forward_mask != 0 {
//             let offset = forward_mask.trailing_zeros() as u64;
//             let found = start_index + offset;
//             if found < 64 {
//                 return Some(found);
//             }
//         }
//     }
//
//     // If forward search failed, try backward
//     if start_index > 0 {
//         let backward_mask = value << (64 - start_index);
//         if backward_mask != 0 {
//             let leading_zeros = backward_mask.leading_zeros() as u64;
//             return Some(start_index - 1 - leading_zeros);
//         }
//     }
//
//     None
// }

pub fn find_nearest(value: u64, signal_index: u64) -> u64 {
    63 - value.leading_zeros() as u64
    // find_nearest_by_distance(value, signal_index)
    // find_nearest_set_bit(value, value.trailing_zeros() as u64)
    // select_random_bit(value, crate::util::random::next_u64() & 63)
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
    (bit_in_rotated + rotate_amount as u64) & 63
}

/// Select a bit using hint, searching both directions from the hint position.
/// This guarantees finding a set bit if value is non-zero.
/// Bit order: signal N = 0x8000_0000_0000_0000u64 >> N
/// Returns the signal index (0-63), or 64 if value is zero.
#[inline]
pub fn select_bit_bidirectional(value: u64, hint: u64) -> u64 {
    if value == 0 {
        return 64;
    }

    let start = hint & 63;

    // Bit pattern: 0x8000_0000_0000_0000u64 >> signal_index
    // Signal 0 = bit 63, signal 63 = bit 0
    // Mask for signals [start, 63] = bits [0, 63-start]
    let mask_from_start = 0x8000_0000_0000_0000u64 >> start;
    let mask_from_start = mask_from_start | (mask_from_start - 1); // fill all bits to the right

    // First try signals from start onwards
    let forward = value & mask_from_start;
    if forward != 0 {
        return forward.leading_zeros() as u64;
    }

    // Then try signals before start (wrap around)
    let backward = value & !mask_from_start;
    if backward != 0 {
        return backward.leading_zeros() as u64;
    }

    // Should never reach here if value != 0
    64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_test() {
        let signal = AtomicU64::new(0);
        let _ = set(&signal, 33u64);
        println!("33: {}", is_set(&signal, 33));
        println!("32: {}", is_set(&signal, 32));
        set(&signal, 63);
        println!(
            "nearest by dist 35: {}",
            find_nearest_by_distance(signal.load(Ordering::Relaxed), 35)
        );
        println!(
            "nearest by dist branchless 35: {}",
            find_nearest_by_distance_branchless(signal.load(Ordering::Relaxed), 35)
        );
        println!(
            "nearest 35: {}",
            find_nearest_set_bit(signal.load(Ordering::Relaxed), 35)
        );
        println!(
            "nearest 31: {}",
            find_nearest_set_bit(signal.load(Ordering::Relaxed), 31)
        );
        println!(
            "nearest 1: {}",
            find_nearest_set_bit(signal.load(Ordering::Relaxed), 1)
        );
    }

    #[test]
    fn test_select_random_bit_uniform() {
        // Test with 2 bits set - should be 50/50
        let value = 0b11u64; // bits 0 and 1
        let mut counts = [0u64; 64];
        for r in 0..6400 {
            let selected = select_random_bit(value, r);
            assert!(selected < 64, "selected {} should be < 64", selected);
            counts[selected as usize] += 1;
        }
        // With 2 bits and 6400 samples, each should get ~3200
        // Allow 20% tolerance
        assert!(
            counts[0] > 2500 && counts[0] < 3900,
            "bit 0 count {} not in range",
            counts[0]
        );
        assert!(
            counts[1] > 2500 && counts[1] < 3900,
            "bit 1 count {} not in range",
            counts[1]
        );
        for i in 2..64 {
            assert_eq!(counts[i], 0, "bit {} should have 0 count", i);
        }
    }
}
