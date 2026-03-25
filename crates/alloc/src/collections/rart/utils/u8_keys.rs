use crate::collections::rart::utils::bitset::BitsetTrait;

// ---------------------------------------------------------------------------
// x86_64 SSE2 (baseline — always available)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
mod x86_simd {
    use std::arch::x86_64::*;

    /// Find the first lane where `keys[i] == key`, masked by `valid_mask`.
    #[inline]
    pub unsafe fn find_key_16(key: u8, keys: &[u8], valid_mask: u32) -> Option<usize> {
        unsafe {
            let key_vec = _mm_set1_epi8(key as i8);
            let data_vec = _mm_loadu_si128(keys.as_ptr() as *const __m128i);
            let cmp = _mm_cmpeq_epi8(key_vec, data_vec);
            let bitfield = (_mm_movemask_epi8(cmp) as u32) & valid_mask;
            if bitfield != 0 {
                Some(bitfield.trailing_zeros() as usize)
            } else {
                None
            }
        }
    }

    /// Find the first lane where `key < keys[i]` (unsigned), masked by `valid_mask`.
    ///
    /// SSE2 only provides *signed* `cmpgt`/`cmplt`, so we XOR both operands
    /// with 0x80 to flip the sign bit, converting unsigned order into signed
    /// order.
    #[inline]
    pub unsafe fn find_insert_pos_16(key: u8, keys: &[u8], valid_mask: u32) -> Option<usize> {
        unsafe {
            let flip = _mm_set1_epi8(-128i8); // 0x80
            let key_vec = _mm_xor_si128(_mm_set1_epi8(key as i8), flip);
            let data_vec = _mm_xor_si128(_mm_loadu_si128(keys.as_ptr() as *const __m128i), flip);
            // signed(key ^ 0x80) < signed(keys[i] ^ 0x80)  ≡  unsigned key < unsigned keys[i]
            let cmp = _mm_cmplt_epi8(key_vec, data_vec);
            let bitfield = (_mm_movemask_epi8(cmp) as u32) & valid_mask;
            if bitfield != 0 {
                Some(bitfield.trailing_zeros() as usize)
            } else {
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// aarch64 NEON (baseline — always available)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "aarch64")]
mod neon_simd {
    use std::arch::aarch64::*;

    /// Pack the MSB of each byte lane into a 16-bit mask (aarch64 equivalent
    /// of x86 `_mm_movemask_epi8`).
    ///
    /// Input lanes must be 0x00 or 0xFF (comparison results).
    #[inline]
    unsafe fn movemask_u8(v: uint8x16_t) -> u16 {
        // Each half gets AND-ed with [1,2,4,8,16,32,64,128] then summed
        // across, producing one byte per half that encodes 8 result bits.
        const POWERS: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
        let mask = vld1_u8(POWERS.as_ptr());

        let lo = vand_u8(vget_low_u8(v), mask);
        let hi = vand_u8(vget_high_u8(v), mask);

        let lo_byte = vaddv_u8(lo) as u16;
        let hi_byte = vaddv_u8(hi) as u16;

        (hi_byte << 8) | lo_byte
    }

    /// Find the first lane where `keys[i] == key`, masked by `valid_mask`.
    #[inline]
    pub unsafe fn find_key_16(key: u8, keys: &[u8], valid_mask: u32) -> Option<usize> {
        let key_vec = vdupq_n_u8(key);
        let data_vec = vld1q_u8(keys.as_ptr());
        let cmp = vceqq_u8(key_vec, data_vec);
        let bitfield = (movemask_u8(cmp) as u32) & valid_mask;
        if bitfield != 0 {
            Some(bitfield.trailing_zeros() as usize)
        } else {
            None
        }
    }

    /// Find the first lane where `key < keys[i]` (unsigned), masked by `valid_mask`.
    ///
    /// NEON has native unsigned less-than (`vcltq_u8`), so no sign-bit
    /// flip is needed unlike SSE2.
    #[inline]
    pub unsafe fn find_insert_pos_16(key: u8, keys: &[u8], valid_mask: u32) -> Option<usize> {
        let key_vec = vdupq_n_u8(key);
        let data_vec = vld1q_u8(keys.as_ptr());
        let cmp = vcltq_u8(key_vec, data_vec); // key < keys[i], unsigned
        let bitfield = (movemask_u8(cmp) as u32) & valid_mask;
        if bitfield != 0 {
            Some(bitfield.trailing_zeros() as usize)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Scalar fallbacks
// ---------------------------------------------------------------------------

fn binary_find_key(key: u8, keys: &[u8], num_children: usize) -> Option<usize> {
    let mut left = 0;
    let mut right = num_children;
    while left < right {
        let mid = (left + right) / 2;
        match keys[mid].cmp(&key) {
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Equal => return Some(mid),
            std::cmp::Ordering::Greater => right = mid,
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Find the position of `key` in the *sorted* keys array.
pub fn u8_keys_find_key_position_sorted<const WIDTH: usize>(
    key: u8,
    keys: &[u8],
    num_children: usize,
) -> Option<usize> {
    // Width 4 and under: linear search is fastest.
    if WIDTH <= 4 {
        return (0..num_children).find(|&i| keys[i] == key);
    }

    // SIMD path for WIDTH >= 16 (Node16).
    #[cfg(target_arch = "x86_64")]
    if WIDTH >= 16 {
        return unsafe { x86_simd::find_key_16(key, keys, (1u32 << num_children) - 1) };
    }

    #[cfg(target_arch = "aarch64")]
    if WIDTH >= 16 {
        return unsafe { neon_simd::find_key_16(key, keys, (1u32 << num_children) - 1) };
    }

    binary_find_key(key, keys, num_children)
}

/// Find the sorted insertion position for `key` among the first `num_children`
/// entries. Returns the index of the first element greater than `key`, or
/// `num_children` if `key` is larger than all existing entries.
pub fn u8_keys_find_insert_position_sorted<const WIDTH: usize>(
    key: u8,
    keys: &[u8],
    num_children: usize,
) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    if WIDTH >= 16 {
        return unsafe { x86_simd::find_insert_pos_16(key, keys, (1u32 << num_children) - 1) }
            .or(Some(num_children));
    }

    #[cfg(target_arch = "aarch64")]
    if WIDTH >= 16 {
        return unsafe { neon_simd::find_insert_pos_16(key, keys, (1u32 << num_children) - 1) }
            .or(Some(num_children));
    }

    // Scalar fallback: linear scan for first entry > key.
    (0..num_children)
        .find(|&i| key < keys[i])
        .or(Some(num_children))
}

/// Find the position of `key` in an *unsorted* keys array (used by
/// `KeyedMapping`). The `children_bitmask` is consulted for the special
/// key 0xFF to avoid false positives from empty slots.
pub fn u8_keys_find_key_position<const WIDTH: usize, Bitset: BitsetTrait>(
    key: u8,
    keys: &[u8],
    children_bitmask: &Bitset,
) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    if WIDTH >= 16 {
        let mut mask = (1u32 << WIDTH) - 1;
        if key == 255 {
            mask &= children_bitmask.as_bitmask() as u32;
        }
        return unsafe { x86_simd::find_key_16(key, keys, mask) };
    }

    #[cfg(target_arch = "aarch64")]
    if WIDTH >= 16 {
        let mut mask = (1u32 << WIDTH) - 1;
        if key == 255 {
            mask &= children_bitmask.as_bitmask() as u32;
        }
        return unsafe { neon_simd::find_key_16(key, keys, mask) };
    }

    // Scalar fallback for small WIDTH or other architectures.
    for (i, k) in keys.iter().enumerate() {
        if key == 255 && !children_bitmask.check(i) {
            continue;
        }
        if *k == key {
            return Some(i);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Scalar reference implementations (used by tests to cross-validate SIMD)
// ---------------------------------------------------------------------------

/// Scalar reference: find first index where `keys[i] == key` within `valid_mask`.
#[cfg(test)]
fn scalar_find_key_16(key: u8, keys: &[u8], valid_mask: u32) -> Option<usize> {
    for i in 0..16 {
        if (valid_mask >> i) & 1 == 1 && keys[i] == key {
            return Some(i);
        }
    }
    None
}

/// Scalar reference: find first index where `key < keys[i]` (unsigned) within `valid_mask`.
#[cfg(test)]
fn scalar_find_insert_pos_16(key: u8, keys: &[u8], valid_mask: u32) -> Option<usize> {
    for i in 0..16 {
        if (valid_mask >> i) & 1 == 1 && key < keys[i] {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::rart::utils::bitset::{Bitset16, BitsetTrait};

    // ===================================================================
    // Low-level SIMD intrinsics — direct tests
    // ===================================================================

    #[cfg(target_arch = "x86_64")]
    mod x86_tests {
        use super::super::{scalar_find_insert_pos_16, scalar_find_key_16, x86_simd};

        #[test]
        fn find_key_present_at_each_position() {
            for pos in 0..16u8 {
                let mut keys = [255u8; 16];
                keys[pos as usize] = 42;
                let mask = 0xFFFF;
                let result = unsafe { x86_simd::find_key_16(42, &keys, mask) };
                assert_eq!(result, Some(pos as usize), "key at position {pos}");
            }
        }

        #[test]
        fn find_key_not_present() {
            let keys = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            let result = unsafe { x86_simd::find_key_16(99, &keys, 0xFFFF) };
            assert_eq!(result, None);
        }

        #[test]
        fn find_key_duplicates_returns_first() {
            let keys = [
                10, 20, 30, 20, 20, 50, 60, 70, 80, 90, 20, 11, 12, 13, 14, 15,
            ];
            let result = unsafe { x86_simd::find_key_16(20, &keys, 0xFFFF) };
            assert_eq!(result, Some(1));
        }

        #[test]
        fn find_key_mask_excludes_match() {
            let keys = [
                42, 42, 42, 42, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
            ];
            // Mask out positions 0-3, only positions 4+ are valid
            let mask = 0xFFF0;
            let result = unsafe { x86_simd::find_key_16(42, &keys, mask) };
            assert_eq!(result, None, "masked-out positions should not match");
        }

        #[test]
        fn find_key_mask_selects_later_match() {
            let keys = [42, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            // Mask out position 0, keep position 3
            let mask = 0b1111_1111_1111_1000;
            let result = unsafe { x86_simd::find_key_16(42, &keys, mask) };
            assert_eq!(result, Some(3));
        }

        #[test]
        fn find_key_zero() {
            let keys = [1, 2, 0, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
            let result = unsafe { x86_simd::find_key_16(0, &keys, 0xFFFF) };
            assert_eq!(result, Some(2));
        }

        #[test]
        fn find_key_0xff() {
            let keys = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255];
            let result = unsafe { x86_simd::find_key_16(255, &keys, 0xFFFF) };
            assert_eq!(result, Some(15));
        }

        #[test]
        fn find_key_all_same() {
            let keys = [77u8; 16];
            let result = unsafe { x86_simd::find_key_16(77, &keys, 0xFFFF) };
            assert_eq!(result, Some(0));
        }

        #[test]
        fn find_key_empty_mask() {
            let keys = [42u8; 16];
            let result = unsafe { x86_simd::find_key_16(42, &keys, 0) };
            assert_eq!(result, None, "empty mask should never match");
        }

        #[test]
        fn insert_pos_key_smaller_than_first() {
            let keys = [
                10, 20, 30, 40, 50, 60, 70, 80, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { x86_simd::find_insert_pos_16(5, &keys, 0xFF) };
            assert_eq!(result, Some(0));
        }

        #[test]
        fn insert_pos_key_between_elements() {
            let keys = [
                10, 20, 30, 40, 50, 60, 70, 80, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { x86_simd::find_insert_pos_16(25, &keys, 0xFF) };
            assert_eq!(result, Some(2), "25 should insert before 30 at index 2");
        }

        #[test]
        fn insert_pos_key_larger_than_all() {
            let keys = [
                10, 20, 30, 40, 50, 60, 70, 80, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { x86_simd::find_insert_pos_16(90, &keys, 0xFF) };
            // 90 < 255 is true at position 8, but mask is 0xFF so positions 8-15 are masked out
            assert_eq!(result, None, "key larger than all masked entries");
        }

        #[test]
        fn insert_pos_key_equal_not_matched() {
            let keys = [
                10, 20, 30, 40, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            // key == 30, strict less-than: 30 < 30 is false, 30 < 40 is true
            let result = unsafe { x86_simd::find_insert_pos_16(30, &keys, 0x0F) };
            assert_eq!(result, Some(3), "equal key not matched by strict less-than");
        }

        #[test]
        fn insert_pos_unsigned_boundary_127_vs_128() {
            // Key 127 (0x7F) should be < 128 (0x80) in unsigned comparison.
            // This crosses the signed boundary: signed 127 > signed -128.
            // The sign-bit-flip trick must handle this correctly.
            let keys = [
                128, 129, 130, 200, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { x86_simd::find_insert_pos_16(127, &keys, 0x0F) };
            assert_eq!(result, Some(0), "unsigned: 127 < 128");
        }

        #[test]
        fn insert_pos_unsigned_boundary_128_vs_127() {
            let keys = [
                100, 127, 200, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { x86_simd::find_insert_pos_16(128, &keys, 0x0F) };
            assert_eq!(result, Some(2), "unsigned: 128 < 200");
        }

        #[test]
        fn insert_pos_key_zero() {
            let keys = [
                0, 1, 2, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            // 0 < 0 is false, 0 < 1 is true
            let result = unsafe { x86_simd::find_insert_pos_16(0, &keys, 0x0F) };
            assert_eq!(result, Some(1));
        }

        #[test]
        fn insert_pos_key_254_with_255_in_array() {
            let keys = [
                10, 20, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { x86_simd::find_insert_pos_16(254, &keys, 0x03) };
            assert_eq!(result, None, "254 is not < 10 or 20; mask only covers 0-1");
        }

        #[test]
        fn insert_pos_key_255_nothing_greater() {
            let keys = [
                0, 50, 100, 150, 200, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { x86_simd::find_insert_pos_16(255, &keys, 0x3F) };
            assert_eq!(result, None, "nothing is greater than 255");
        }

        /// Cross-validate SIMD find_key against scalar for all byte values.
        #[test]
        fn find_key_exhaustive_cross_validate() {
            let keys: [u8; 16] = [
                0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
            ];
            for key in 0..=255u8 {
                for num_valid in 1..=16u32 {
                    let mask = (1u32 << num_valid) - 1;
                    let simd = unsafe { x86_simd::find_key_16(key, &keys, mask) };
                    let scalar = scalar_find_key_16(key, &keys, mask);
                    assert_eq!(simd, scalar, "key={key}, mask=0x{mask:04x}");
                }
            }
        }

        /// Cross-validate SIMD find_insert_pos against scalar for all byte values.
        #[test]
        fn insert_pos_exhaustive_cross_validate() {
            let keys: [u8; 16] = [
                0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
            ];
            for key in 0..=255u8 {
                for num_valid in 1..=16u32 {
                    let mask = (1u32 << num_valid) - 1;
                    let simd = unsafe { x86_simd::find_insert_pos_16(key, &keys, mask) };
                    let scalar = scalar_find_insert_pos_16(key, &keys, mask);
                    assert_eq!(simd, scalar, "key={key}, num_valid={num_valid}");
                }
            }
        }

        /// Cross-validate with random key arrays.
        #[test]
        fn find_key_random_cross_validate() {
            // Use a simple LCG for deterministic "random" data, no rand dependency needed.
            let mut rng_state: u64 = 0xDEAD_BEEF_CAFE_BABE;
            let mut next = || -> u8 {
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (rng_state >> 33) as u8
            };

            for _ in 0..1000 {
                let mut keys = [0u8; 16];
                for k in keys.iter_mut() {
                    *k = next();
                }
                let key = next();
                let num_valid = (next() % 16) as u32 + 1;
                let mask = (1u32 << num_valid) - 1;

                let simd = unsafe { x86_simd::find_key_16(key, &keys, mask) };
                let scalar = scalar_find_key_16(key, &keys, mask);
                assert_eq!(simd, scalar, "key={key}, keys={keys:?}, mask=0x{mask:04x}");
            }
        }

        /// Cross-validate insert_pos with random sorted key arrays.
        #[test]
        fn insert_pos_random_cross_validate() {
            let mut rng_state: u64 = 0x1234_5678_9ABC_DEF0;
            let mut next = || -> u8 {
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (rng_state >> 33) as u8
            };

            for _ in 0..1000 {
                let mut keys = [255u8; 16];
                let num_children = (next() % 16) as usize + 1;
                // Generate sorted keys
                let mut vals: Vec<u8> = (0..num_children).map(|_| next()).collect();
                vals.sort();
                for (i, v) in vals.iter().enumerate() {
                    keys[i] = *v;
                }

                let key = next();
                let mask = (1u32 << num_children) - 1;

                let simd = unsafe { x86_simd::find_insert_pos_16(key, &keys, mask) };
                let scalar = scalar_find_insert_pos_16(key, &keys, mask);
                assert_eq!(
                    simd, scalar,
                    "key={key}, keys={keys:?}, num_children={num_children}"
                );
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    mod neon_tests {
        use super::super::{neon_simd, scalar_find_insert_pos_16, scalar_find_key_16};

        #[test]
        fn find_key_present_at_each_position() {
            for pos in 0..16u8 {
                let mut keys = [255u8; 16];
                keys[pos as usize] = 42;
                let mask = 0xFFFF;
                let result = unsafe { neon_simd::find_key_16(42, &keys, mask) };
                assert_eq!(result, Some(pos as usize), "key at position {pos}");
            }
        }

        #[test]
        fn find_key_not_present() {
            let keys = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            let result = unsafe { neon_simd::find_key_16(99, &keys, 0xFFFF) };
            assert_eq!(result, None);
        }

        #[test]
        fn find_key_duplicates_returns_first() {
            let keys = [
                10, 20, 30, 20, 20, 50, 60, 70, 80, 90, 20, 11, 12, 13, 14, 15,
            ];
            let result = unsafe { neon_simd::find_key_16(20, &keys, 0xFFFF) };
            assert_eq!(result, Some(1));
        }

        #[test]
        fn find_key_mask_excludes_match() {
            let keys = [
                42, 42, 42, 42, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
            ];
            let mask = 0xFFF0;
            let result = unsafe { neon_simd::find_key_16(42, &keys, mask) };
            assert_eq!(result, None);
        }

        #[test]
        fn find_key_zero_and_0xff() {
            let keys = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255];
            assert_eq!(unsafe { neon_simd::find_key_16(0, &keys, 0xFFFF) }, Some(0));
            assert_eq!(
                unsafe { neon_simd::find_key_16(255, &keys, 0xFFFF) },
                Some(15)
            );
        }

        #[test]
        fn find_key_empty_mask() {
            let keys = [42u8; 16];
            let result = unsafe { neon_simd::find_key_16(42, &keys, 0) };
            assert_eq!(result, None);
        }

        #[test]
        fn insert_pos_key_smaller_than_first() {
            let keys = [
                10, 20, 30, 40, 50, 60, 70, 80, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { neon_simd::find_insert_pos_16(5, &keys, 0xFF) };
            assert_eq!(result, Some(0));
        }

        #[test]
        fn insert_pos_key_between_elements() {
            let keys = [
                10, 20, 30, 40, 50, 60, 70, 80, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { neon_simd::find_insert_pos_16(25, &keys, 0xFF) };
            assert_eq!(result, Some(2));
        }

        #[test]
        fn insert_pos_unsigned_boundary_127_vs_128() {
            let keys = [
                128, 129, 130, 200, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { neon_simd::find_insert_pos_16(127, &keys, 0x0F) };
            assert_eq!(result, Some(0), "unsigned: 127 < 128");
        }

        #[test]
        fn insert_pos_key_255_nothing_greater() {
            let keys = [
                0, 50, 100, 150, 200, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let result = unsafe { neon_simd::find_insert_pos_16(255, &keys, 0x3F) };
            assert_eq!(result, None);
        }

        #[test]
        fn find_key_exhaustive_cross_validate() {
            let keys: [u8; 16] = [
                0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
            ];
            for key in 0..=255u8 {
                for num_valid in 1..=16u32 {
                    let mask = (1u32 << num_valid) - 1;
                    let simd = unsafe { neon_simd::find_key_16(key, &keys, mask) };
                    let scalar = scalar_find_key_16(key, &keys, mask);
                    assert_eq!(simd, scalar, "key={key}, mask=0x{mask:04x}");
                }
            }
        }

        #[test]
        fn insert_pos_exhaustive_cross_validate() {
            let keys: [u8; 16] = [
                0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
            ];
            for key in 0..=255u8 {
                for num_valid in 1..=16u32 {
                    let mask = (1u32 << num_valid) - 1;
                    let simd = unsafe { neon_simd::find_insert_pos_16(key, &keys, mask) };
                    let scalar = scalar_find_insert_pos_16(key, &keys, mask);
                    assert_eq!(simd, scalar, "key={key}, num_valid={num_valid}");
                }
            }
        }

        #[test]
        fn find_key_random_cross_validate() {
            let mut rng_state: u64 = 0xDEAD_BEEF_CAFE_BABE;
            let mut next = || -> u8 {
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (rng_state >> 33) as u8
            };

            for _ in 0..1000 {
                let mut keys = [0u8; 16];
                for k in keys.iter_mut() {
                    *k = next();
                }
                let key = next();
                let num_valid = (next() % 16) as u32 + 1;
                let mask = (1u32 << num_valid) - 1;

                let simd = unsafe { neon_simd::find_key_16(key, &keys, mask) };
                let scalar = scalar_find_key_16(key, &keys, mask);
                assert_eq!(simd, scalar, "key={key}, keys={keys:?}, mask=0x{mask:04x}");
            }
        }

        #[test]
        fn insert_pos_random_cross_validate() {
            let mut rng_state: u64 = 0x1234_5678_9ABC_DEF0;
            let mut next = || -> u8 {
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (rng_state >> 33) as u8
            };

            for _ in 0..1000 {
                let mut keys = [255u8; 16];
                let num_children = (next() % 16) as usize + 1;
                let mut vals: Vec<u8> = (0..num_children).map(|_| next()).collect();
                vals.sort();
                for (i, v) in vals.iter().enumerate() {
                    keys[i] = *v;
                }

                let key = next();
                let mask = (1u32 << num_children) - 1;

                let simd = unsafe { neon_simd::find_insert_pos_16(key, &keys, mask) };
                let scalar = scalar_find_insert_pos_16(key, &keys, mask);
                assert_eq!(
                    simd, scalar,
                    "key={key}, keys={keys:?}, num_children={num_children}"
                );
            }
        }
    }

    // ===================================================================
    // Public API tests (platform-independent, exercise SIMD via dispatch)
    // ===================================================================

    // --- find_key_position_sorted (Node4 = linear, Node16 = SIMD) ---

    #[test]
    fn sorted_find_key_width4_basic() {
        let keys = [10, 20, 30, 40];
        assert_eq!(u8_keys_find_key_position_sorted::<4>(20, &keys, 4), Some(1));
        assert_eq!(u8_keys_find_key_position_sorted::<4>(10, &keys, 4), Some(0));
        assert_eq!(u8_keys_find_key_position_sorted::<4>(40, &keys, 4), Some(3));
        assert_eq!(u8_keys_find_key_position_sorted::<4>(15, &keys, 4), None);
    }

    #[test]
    fn sorted_find_key_width4_partial_fill() {
        let keys = [5, 10, 255, 255];
        assert_eq!(u8_keys_find_key_position_sorted::<4>(5, &keys, 2), Some(0));
        assert_eq!(u8_keys_find_key_position_sorted::<4>(10, &keys, 2), Some(1));
        // 255 is present but outside num_children
        assert_eq!(u8_keys_find_key_position_sorted::<4>(255, &keys, 2), None);
    }

    #[test]
    fn sorted_find_key_width16_basic() {
        let mut keys = [255u8; 16];
        for i in 0..8 {
            keys[i] = (i as u8) * 10;
        }
        assert_eq!(u8_keys_find_key_position_sorted::<16>(0, &keys, 8), Some(0));
        assert_eq!(
            u8_keys_find_key_position_sorted::<16>(30, &keys, 8),
            Some(3)
        );
        assert_eq!(
            u8_keys_find_key_position_sorted::<16>(70, &keys, 8),
            Some(7)
        );
        assert_eq!(u8_keys_find_key_position_sorted::<16>(35, &keys, 8), None);
    }

    #[test]
    fn sorted_find_key_width16_full() {
        let keys: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        for i in 0..16u8 {
            assert_eq!(
                u8_keys_find_key_position_sorted::<16>(i, &keys, 16),
                Some(i as usize)
            );
        }
        assert_eq!(u8_keys_find_key_position_sorted::<16>(16, &keys, 16), None);
    }

    #[test]
    fn sorted_find_key_width16_high_values() {
        let keys: [u8; 16] = [
            128, 130, 140, 150, 200, 210, 220, 230, 240, 245, 250, 251, 252, 253, 254, 255,
        ];
        assert_eq!(
            u8_keys_find_key_position_sorted::<16>(128, &keys, 16),
            Some(0)
        );
        assert_eq!(
            u8_keys_find_key_position_sorted::<16>(255, &keys, 16),
            Some(15)
        );
        assert_eq!(u8_keys_find_key_position_sorted::<16>(129, &keys, 16), None);
    }

    // --- find_insert_position_sorted ---

    #[test]
    fn sorted_insert_pos_width4() {
        let keys = [10, 20, 30, 40];
        assert_eq!(
            u8_keys_find_insert_position_sorted::<4>(5, &keys, 4),
            Some(0)
        );
        assert_eq!(
            u8_keys_find_insert_position_sorted::<4>(15, &keys, 4),
            Some(1)
        );
        assert_eq!(
            u8_keys_find_insert_position_sorted::<4>(25, &keys, 4),
            Some(2)
        );
        assert_eq!(
            u8_keys_find_insert_position_sorted::<4>(50, &keys, 4),
            Some(4)
        );
    }

    #[test]
    fn sorted_insert_pos_width16_basic() {
        let mut keys = [255u8; 16];
        keys[..6].copy_from_slice(&[10, 20, 30, 40, 50, 60]);
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(5, &keys, 6),
            Some(0)
        );
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(15, &keys, 6),
            Some(1)
        );
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(35, &keys, 6),
            Some(3)
        );
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(65, &keys, 6),
            Some(6)
        );
    }

    #[test]
    fn sorted_insert_pos_width16_equal_goes_after() {
        let mut keys = [255u8; 16];
        keys[..4].copy_from_slice(&[10, 20, 30, 40]);
        // Inserting 20: 20 < 20 is false, 20 < 30 is true → position 2
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(20, &keys, 4),
            Some(2)
        );
    }

    #[test]
    fn sorted_insert_pos_width16_signed_boundary() {
        let mut keys = [255u8; 16];
        keys[..4].copy_from_slice(&[100, 127, 128, 200]);

        // 126 < 127 at pos 1
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(126, &keys, 4),
            Some(1)
        );
        // 127 < 128 at pos 2 (unsigned boundary)
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(127, &keys, 4),
            Some(2)
        );
        // 128 < 200 at pos 3
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(128, &keys, 4),
            Some(3)
        );
        // 199 < 200 at pos 3
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(199, &keys, 4),
            Some(3)
        );
        // 200 is not < anything remaining → append
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(200, &keys, 4),
            Some(4)
        );
    }

    #[test]
    fn sorted_insert_pos_width16_all_zeros() {
        let mut keys = [255u8; 16];
        keys[..4].copy_from_slice(&[0, 0, 0, 0]);
        // 0 < 0 is false for all → append at end
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(0, &keys, 4),
            Some(4)
        );
    }

    #[test]
    fn sorted_insert_pos_width16_full_range() {
        let keys: [u8; 16] = [
            0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
        ];
        // 1 < 17 → pos 1
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(1, &keys, 16),
            Some(1)
        );
        // 255: nothing is > 255 → append
        assert_eq!(
            u8_keys_find_insert_position_sorted::<16>(255, &keys, 16),
            Some(16)
        );
    }

    // --- find_key_position (unsorted, with bitmask) ---

    #[test]
    fn unsorted_find_key_width16_basic() {
        let keys = [
            50, 10, 30, 20, 40, 60, 80, 70, 90, 100, 110, 120, 130, 140, 150, 160,
        ];
        let mut bitmask = Bitset16::<1>::default();
        for i in 0..16 {
            bitmask.set(i);
        }
        assert_eq!(
            u8_keys_find_key_position::<16, _>(50, &keys, &bitmask),
            Some(0)
        );
        assert_eq!(
            u8_keys_find_key_position::<16, _>(10, &keys, &bitmask),
            Some(1)
        );
        assert_eq!(
            u8_keys_find_key_position::<16, _>(160, &keys, &bitmask),
            Some(15)
        );
        assert_eq!(
            u8_keys_find_key_position::<16, _>(99, &keys, &bitmask),
            None
        );
    }

    #[test]
    fn unsorted_find_key_0xff_with_empty_slots() {
        // Keys array with 255 as the "empty" sentinel. Three real children
        // plus the rest are empty (255). We insert a child with key 255 at
        // position 3.
        let mut keys = [255u8; 16];
        keys[0] = 10;
        keys[1] = 20;
        keys[2] = 30;
        keys[3] = 255; // Real child with key 255

        let mut bitmask = Bitset16::<1>::default();
        bitmask.set(0);
        bitmask.set(1);
        bitmask.set(2);
        bitmask.set(3);

        // Should find 255 at position 3 (the real child), not at position 4+
        // (empty slots that also contain 255 but are not in the bitmask).
        assert_eq!(
            u8_keys_find_key_position::<16, _>(255, &keys, &bitmask),
            Some(3)
        );
    }

    #[test]
    fn unsorted_find_key_0xff_no_valid_match() {
        // All slots contain 255 but only position 0 is occupied (with key 10).
        // When searching for 255, the bitmask restricts to position 0 which
        // has key 10, not 255.
        let mut keys = [255u8; 16];
        keys[0] = 10;

        let mut bitmask = Bitset16::<1>::default();
        bitmask.set(0);

        assert_eq!(
            u8_keys_find_key_position::<16, _>(255, &keys, &bitmask),
            None
        );
    }

    #[test]
    fn unsorted_find_key_width4_fallback() {
        let keys = [30, 10, 20, 40];
        let mut bitmask = Bitset16::<1>::default();
        for i in 0..4 {
            bitmask.set(i);
        }
        assert_eq!(
            u8_keys_find_key_position::<4, _>(10, &keys, &bitmask),
            Some(1)
        );
        assert_eq!(u8_keys_find_key_position::<4, _>(99, &keys, &bitmask), None);
    }

    // --- Cross-validation: public API vs scalar for many inputs ---

    #[test]
    fn sorted_find_key_width16_cross_validate_all_bytes() {
        let keys: [u8; 16] = [
            0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
        ];
        for key in 0..=255u8 {
            let result = u8_keys_find_key_position_sorted::<16>(key, &keys, 16);
            let expected = keys.iter().position(|&k| k == key);
            assert_eq!(result, expected, "key={key}");
        }
    }

    #[test]
    fn sorted_insert_pos_width16_cross_validate_all_bytes() {
        let keys: [u8; 16] = [
            0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
        ];
        for key in 0..=255u8 {
            let result = u8_keys_find_insert_position_sorted::<16>(key, &keys, 16);
            // Scalar reference: first index where key < keys[i], or num_children
            let expected = (0..16usize).find(|&i| key < keys[i]).or(Some(16));
            assert_eq!(result, expected, "key={key}");
        }
    }

    /// Verify SIMD and scalar agree for many random unsorted key arrays.
    #[test]
    fn unsorted_find_key_random_cross_validate() {
        let mut rng_state: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let mut next = || -> u8 {
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (rng_state >> 33) as u8
        };

        for _ in 0..500 {
            let mut keys = [255u8; 16];
            let num_children = (next() % 16) as usize + 1;
            let mut bitmask = Bitset16::<1>::default();
            for i in 0..num_children {
                keys[i] = next();
                bitmask.set(i);
            }
            let key = next();

            let simd_result = u8_keys_find_key_position::<16, _>(key, &keys, &bitmask);

            // Scalar reference (matches the fallback logic)
            let mut scalar_result = None;
            for (i, k) in keys.iter().enumerate() {
                if key == 255 && !bitmask.check(i) {
                    continue;
                }
                if *k == key {
                    scalar_result = Some(i);
                    break;
                }
            }
            assert_eq!(
                simd_result, scalar_result,
                "key={key}, keys={keys:?}, num_children={num_children}"
            );
        }
    }
}
