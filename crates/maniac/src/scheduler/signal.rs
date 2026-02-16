use std::sync::atomic::{AtomicU64, Ordering};

/// Type alias for signal indices
pub type SignalIndex = u64;

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
pub struct SignalLeaf {
    value: AtomicU64,
}

impl SignalLeaf {
    pub const INVALID: SignalIndex = u64::MAX;

    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn value(&self) -> &AtomicU64 {
        &self.value
    }

    #[inline(always)]
    pub fn load(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.value.load(Ordering::Acquire) == 0
    }

    #[inline(always)]
    pub fn set(&self, bit_index: SignalIndex) -> (bool, bool) {
        let value = self.value.load(Ordering::Acquire);
        let bit = 0x8000_0000_0000_0000u64 >> bit_index;
        if value & bit != 0 {
            return (false, false);
        }
        let prev = self.value.fetch_or(bit, Ordering::AcqRel);
        // was_empty, was_set
        (prev == 0, (prev & bit) == 0)
    }

    #[inline(always)]
    pub fn set_no_precheck(&self, bit_index: SignalIndex) -> (bool, bool) {
        let bit = 0x8000_0000_0000_0000u64 >> bit_index;
        let prev = self.value.fetch_or(bit, Ordering::AcqRel);
        // was_empty, was_set
        (prev == 0, (prev & bit) == 0)
    }

    #[inline(always)]
    pub fn clear(&self, bit_index: SignalIndex) -> bool {
        let value = self.value.load(Ordering::Acquire);
        let bit = 0x8000_0000_0000_0000u64 >> bit_index;
        if value & bit == 0 {
            return false;
        }
        let prev = self.value.fetch_and(!bit, Ordering::AcqRel);
        (prev & bit) != 0
    }

    #[inline(always)]
    pub fn clear_no_precheck(&self, bit_index: SignalIndex) -> bool {
        let bit = 0x8000_0000_0000_0000u64 >> bit_index;
        let prev = self.value.fetch_and(!bit, Ordering::AcqRel);
        (prev & bit) != 0
    }

    #[inline(always)]
    pub fn acquire(&self, bit_index: SignalIndex) -> bool {
        let value = self.value.load(Ordering::Acquire);
        let bit = 0x8000_0000_0000_0000u64 >> bit_index;
        if value & bit == 0 {
            return false;
        }
        let previous = self.value.fetch_and(!bit, Ordering::AcqRel);
        previous & bit == 0
    }

    #[inline(always)]
    pub fn acquire_no_precheck(&self, bit_index: SignalIndex) -> bool {
        let bit = 0x8000_0000_0000_0000u64 >> bit_index;
        let previous = self.value.fetch_and(!bit, Ordering::AcqRel);
        previous & bit == 0
    }

    #[inline(always)]
    pub fn acquire_largest(&self) -> SignalIndex {
        let mut expected = self.value.load(Ordering::Acquire);
        let mut bit_index = expected.leading_zeros() as u64;

        while expected != 0 {
            let bit = 0x8000_0000_0000_0000u64 >> bit_index;
            expected = self.value.fetch_and(!bit, Ordering::AcqRel);
            if (expected & bit) == bit {
                return bit_index;
            }

            bit_index = expected.leading_zeros() as u64;
        }

        Self::INVALID
    }

    #[inline(always)]
    pub fn acquire_with_bias(&self, bias_flags: u64, bias_hint: u64) -> (SignalIndex, bool, u64) {
        const BIAS_BITS: u32 = 65 - minimum_bit_count(64);
        let bias_flags = bias_flags << BIAS_BITS;

        let mut expected = self.value.load(Ordering::Acquire);

        while expected != 0 {
            let (bit_index, new_hint) = select_with_bias(expected, bias_flags, bias_hint);
            let bit = 0x8000_0000_0000_0000u64 >> bit_index;
            expected = self.value.fetch_and(!bit, Ordering::AcqRel);
            if (expected & bit) == bit {
                return (bit_index, expected == bit, new_hint);
            }
        }

        (Self::INVALID, false, bias_hint)
    }

    #[inline(always)]
    pub fn try_acquire_with_bias(
        &self,
        bias_flags: u64,
        bias_hint: u64,
    ) -> (SignalIndex, bool, u64) {
        const BIAS_BITS: u32 = 65 - minimum_bit_count(64);
        let bias_flags = bias_flags << BIAS_BITS;

        let mut expected = self.value.load(Ordering::Acquire);
        let (bit_index, new_hint) = select_with_bias(expected, bias_flags, bias_hint);
        let bit = 0x8000_0000_0000_0000u64 >> bit_index;
        expected = self.value.fetch_and(!bit, Ordering::AcqRel);
        if (expected & bit) == bit {
            (bit_index, expected == bit, new_hint)
        } else {
            (Self::INVALID, expected == 0, bias_hint)
        }
    }

    #[inline(always)]
    pub fn select_with_bias(&self, bias_flags: u64, bias_hint: u64) -> (SignalIndex, u64) {
        select_with_bias(self.value.load(Ordering::Acquire), bias_flags, bias_hint)
    }
}

/// Calculate the minimum number of bits needed to represent a value
#[inline]
pub const fn minimum_bit_count(value: u64) -> u32 {
    if value == 0 {
        0
    } else {
        64 - value.leading_zeros()
    }
}

#[inline(always)]
pub fn select_with_bias(leaf_value: u64, bias_flags: u64, bias_hint: u64) -> (SignalIndex, u64) {
    default_select_64(bias_flags, leaf_value, 0, bias_hint)
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
