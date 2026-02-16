//! TreeSummary - Summary bits for tracking which trees have work
//!
//! Each AtomicU64 word covers 64 trees (1 bit per tree).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{scheduler::BiasState, util::CachePadded};

/// Summary words for tracking which trees have work (1 bit per tree)
/// Each AtomicU64 covers 64 trees
pub struct TreeSummary {
    pub words: Vec<CachePadded<AtomicU64>>,
    pub word_count: usize,
    pub word_mask: usize,
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

        let value = self.words[word_idx].load(Ordering::Acquire);
        if value & bit != 0 {
            return (false, false);
        }

        let prev = self.words[word_idx].fetch_or(bit, Ordering::AcqRel);
        ((prev & bit) == 0, prev == 0)
    }

    /// Clear bit for tree becoming empty.
    /// Returns true if the bit was actually cleared (was set before).
    #[inline]
    pub(crate) fn clear_bit(&self, tree_index: u64) -> bool {
        let word_idx = (tree_index >> 6) as usize;
        let bit = 1u64 << (tree_index & 63);
        let value = self.words[word_idx].load(Ordering::Acquire);
        if value & bit == 0 {
            return false;
        }
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

    /// Get the number of summary words
    #[inline]
    pub(crate) fn word_count(&self) -> usize {
        self.word_count
    }

    /// Load a summary word by index
    #[inline]
    pub(crate) fn load_word(&self, word_idx: usize) -> u64 {
        self.words[word_idx & self.word_mask].load(Ordering::Acquire)
    }

    /// Select a tree index using random bias for fairness.
    /// Returns (tree_index, new_bias) or (u64::MAX, bias) if no trees have work.
    #[inline]
    pub(crate) fn select_with_bias(&self, bias: &mut BiasState) -> (u64, u64) {
        // Use random to pick starting word
        let start_word = ((bias.summary_bias >> 6) as usize) & self.word_mask;

        for i in 0..self.word_count {
            // let word_idx = (bias.summary_bias as usize + i) & self.word_mask;
            let word_idx = crate::util::random::rapidrng_fast(&mut bias.summary_bias) as usize
                & self.word_mask;
            let word = self.words[word_idx].load(Ordering::Acquire);
            if word == 0 {
                continue;
            }

            // crate::util::random::rapidrng_fast(&mut random);

            // Select a uniformly random set bit within word
            // let bit_pos = crate::util::bits::select_random_bit(word, bias.flags ^ word_idx as u64);
            let bit_pos = crate::util::bits::find_nearest_by_distance(
                word,
                crate::util::random::next_u64() & 63,
                // crate::util::random::rapidrng_fast(&mut bias.hint) & 63,
            );
            // let bit_pos = crate::util::bits::select_random_bit(word, random & 63);
            // let bit_pos = crate::util::bits::find_nearest_by_distance(word, 37);

            if bit_pos < 64 {
                let tree_index = (word_idx as u64) * 64 + bit_pos;
                // bias.summary_bias = bias.summary_bias.wrapping_add(1);
                return (tree_index, bias.summary_bias);
            }

            bias.summary_bias = bias.summary_bias.wrapping_add(1);
            // random = random.wrapping_add(1);
            // random = crate::util::random::next_u64();
            // crate::util::random::rapidrng_fast(&mut random);
        }

        (u64::MAX, bias.summary_bias)
    }
}
