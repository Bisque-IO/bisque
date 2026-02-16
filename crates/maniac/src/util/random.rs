use std::{
    cell::UnsafeCell,
    hash::{Hash, Hasher},
    time::Instant,
};

const RND_MULTIPLIER: u64 = 0x5DEECE66D;
const RND_ADDEND: u64 = 0xB;
const RND_MASK: u64 = (1 << 48) - 1;

/// 64*64 to 128 bit multiply
///
/// Returns the (low, high) 64 bits of the 128 bit result.
///
/// # From the C code:
/// Calculates 128-bit C = *A * *B.
///
/// When RAPIDHASH_FAST is defined:
/// Overwrites A contents with C's low 64 bits.
/// Overwrites B contents with C's high 64 bits.
///
/// When RAPIDHASH_PROTECTED is defined:
/// Xors and overwrites A contents with C's low 64 bits.
/// Xors and overwrites B contents with C's high 64 bits.
#[inline(always)]
#[must_use]
pub(crate) const fn rapid_mum<const PROTECTED: bool>(a: u64, b: u64) -> (u64, u64) {
    let r = (a as u128).wrapping_mul(b as u128);

    if !PROTECTED {
        (r as u64, (r >> 64) as u64)
    } else {
        (a ^ r as u64, b ^ (r >> 64) as u64)
    }
}

/// Folded 64-bit multiply. [rapid_mum] then XOR the results together.
#[inline(always)]
#[must_use]
pub(crate) const fn rapid_mix<const PROTECTED: bool>(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);

    if !PROTECTED {
        (r as u64) ^ (r >> 64) as u64
    } else {
        (a ^ r as u64) ^ (b ^ (r >> 64) as u64)
    }
}

/// Uses the V1 rapid seed.
const RAPID_SEED: u64 = 0xbdd89aa982704029;

/// Uses the V1 rapid secrets.
const RAPID_SECRET: [u64; 3] = [0x2d358dccaa6c78a5, 0x8bb84b93962eacc9, 0x4b33a62ed433d4a3];

/// Generate a random number using rapidhash mixing.
///
/// This RNG is deterministic and optimized for throughput. It is not a cryptographic random number
/// generator.
///
/// This implementation is equivalent in logic and performance to
/// [wyhash::wyrng](https://docs.rs/wyhash/latest/wyhash/fn.wyrng.html) and
/// [fasthash::u64](https://docs.rs/fastrand/latest/fastrand/), but uses rapidhash
/// constants/secrets.
///
/// The weakness with this RNG is that at best it's a single cycle over the u64 space, as the seed
/// is simple a position in a constant sequence. Future work could involve using a wider state to
/// ensure we can generate many different sequences.
#[inline]
pub fn rapidrng_fast(seed: &mut u64) -> u64 {
    // let old_seed = *seed;
    // let next_seed = (old_seed
    //     .wrapping_mul(RND_MULTIPLIER)
    //     .wrapping_add(RND_ADDEND))
    //     & RND_MASK;
    // *seed = next_seed;
    // next_seed >> 16
    *seed = seed.wrapping_add(RAPID_SECRET[0]);
    rapid_mix::<false>(*seed, *seed ^ RAPID_SECRET[1])
}

pub fn getrandom_u64() -> Result<u64, getrandom::Error> {
    let mut buf = [0u8; 8];
    getrandom::fill(&mut buf)?;
    Ok(u64::from_ne_bytes(buf))
}

pub struct Random {
    seed: u64,
}

impl Random {
    pub fn new() -> Self {
        Self { seed: next_u64() }
    }

    pub fn new_with_seed(seed: u64) -> Self {
        Self { seed }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn next(&mut self) -> u64 {
        rapidrng_fast(&mut self.seed)
        // let old_seed = self.seed;
        // let next_seed = (old_seed
        //     .wrapping_mul(RND_MULTIPLIER)
        //     .wrapping_add(RND_ADDEND))
        //     & RND_MASK;
        // self.seed = next_seed;
        // next_seed
        // next_seed >> 16
    }
}

thread_local! {
    // static THREAD_RND: UnsafeCell<Random> = UnsafeCell::new(Random { seed: getrandom_u64().unwrap().wrapping_mul(getrandom_u64().unwrap()).wrapping_mul(Instant::now().elapsed().as_nanos() as u64) });
    static THREAD_RND: UnsafeCell<Random> = UnsafeCell::new(Random { seed: getrandom_u64().unwrap() });
}

pub fn next_u64() -> u64 {
    THREAD_RND.with(|r| unsafe { &mut *r.get() }.next())
}

pub fn next_usize() -> usize {
    THREAD_RND.with(|r| unsafe { &mut *r.get() }.next()) as usize
}
