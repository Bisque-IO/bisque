//! Compact key and partial types optimized for u64 lookups.
//!
//! `U64Key`: 9 bytes (`[u8; 8]` big-endian data + `u8` len), alignment 1, no padding.
//! `U64Partial`: 9 bytes (`[u8; 8]` data + `u8` len), alignment 1, no padding.
//!
//! Compare to `ArrayKey<16>` + `ArrPartial<16>`: 24 bytes each.
//! For a tree with 100K nodes, this saves ~2.8 MiB of prefix storage.

use std::cmp::min;

use super::KeyTrait;
use crate::collections::rart::partials::Partial;

// -----------------------------------------------------------------------
// U64Key — 8 bytes data + 1 byte len = 9 bytes, alignment 1
// -----------------------------------------------------------------------

/// A compact key for u64 values stored in big-endian byte order.
///
/// 9 bytes: `[u8; 8]` data (alignment 1) + `u8` len = no padding.
/// Compare to `ArrayKey<16>`: 24 bytes.
///
/// The `len` field tracks how many leading bytes are "filled" — needed
/// by the ART iterator which builds keys incrementally via
/// `extend_from_partial` / `truncate`. For full keys created from `u64`,
/// `len` is always 8.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct U64Key {
    /// Big-endian byte representation. Byte 0 is the MSB.
    data: [u8; 8],
    /// Number of valid bytes (0..=8).
    len: u8,
}

impl U64Key {
    #[inline]
    pub fn new(v: u64) -> Self {
        Self {
            data: v.to_be_bytes(),
            len: 8,
        }
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        u64::from_be_bytes(self.data)
    }
}

impl AsRef<[u8]> for U64Key {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}

impl PartialOrd for U64Key {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for U64Key {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl From<u64> for U64Key {
    #[inline]
    fn from(v: u64) -> Self {
        Self::new(v)
    }
}
impl From<&u64> for U64Key {
    #[inline]
    fn from(v: &u64) -> Self {
        Self::new(*v)
    }
}
impl From<u32> for U64Key {
    #[inline]
    fn from(v: u32) -> Self {
        Self::new(v as u64)
    }
}
impl From<&u32> for U64Key {
    #[inline]
    fn from(v: &u32) -> Self {
        Self::new(*v as u64)
    }
}

impl KeyTrait for U64Key {
    type PartialType = U64Partial;
    const MAXIMUM_SIZE: Option<usize> = Some(8);

    #[inline]
    fn new_from_slice(data: &[u8]) -> Self {
        debug_assert!(data.len() <= 8);
        let mut buf = [0u8; 8];
        buf[..data.len()].copy_from_slice(data);
        Self {
            data: buf,
            len: data.len() as u8,
        }
    }

    #[inline]
    fn new_from_partial(partial: &U64Partial) -> Self {
        Self {
            data: partial.data,
            len: partial.len,
        }
    }

    #[inline]
    fn extend_from_partial(&self, partial: &U64Partial) -> Self {
        let my_len = self.len as usize;
        let p_len = partial.len as usize;
        debug_assert!(my_len + p_len <= 8);
        let mut new_data = self.data;
        new_data[my_len..my_len + p_len].copy_from_slice(&partial.data[..p_len]);
        Self {
            data: new_data,
            len: (my_len + p_len) as u8,
        }
    }

    #[inline]
    fn truncate(&self, at_depth: usize) -> Self {
        debug_assert!(at_depth <= 8);
        let mut new_data = self.data;
        // Zero out bytes beyond at_depth
        for i in at_depth..8 {
            new_data[i] = 0;
        }
        Self {
            data: new_data,
            len: at_depth as u8,
        }
    }

    #[inline(always)]
    fn at(&self, pos: usize) -> u8 {
        self.data[pos]
    }

    #[inline(always)]
    fn length_at(&self, at_depth: usize) -> usize {
        (self.len as usize).saturating_sub(at_depth)
    }

    #[inline]
    fn to_partial(&self, at_depth: usize) -> U64Partial {
        let remaining = (self.len as usize).saturating_sub(at_depth);
        let mut new_data = [0u8; 8];
        if remaining > 0 && at_depth < 8 {
            new_data[..remaining].copy_from_slice(&self.data[at_depth..at_depth + remaining]);
        }
        U64Partial {
            data: new_data,
            len: remaining as u8,
        }
    }

    #[inline]
    fn matches_slice(&self, slice: &[u8]) -> bool {
        self.as_ref() == slice
    }
}

// -----------------------------------------------------------------------
// U64Partial — same layout as U64Key, 9 bytes, alignment 1
// -----------------------------------------------------------------------

/// Compact partial for `U64Key`. Stores up to 8 bytes in `[u8; 8]`.
#[derive(Clone, Copy, Debug, Eq)]
pub struct U64Partial {
    /// Bytes packed from position 0. Byte 0 is the first byte.
    data: [u8; 8],
    len: u8,
}

impl U64Partial {
    #[inline]
    pub fn from_slice(src: &[u8]) -> Self {
        debug_assert!(src.len() <= 8);
        let mut data = [0u8; 8];
        data[..src.len()].copy_from_slice(src);
        Self {
            data,
            len: src.len() as u8,
        }
    }
}

impl PartialEq for U64Partial {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.data[..self.len as usize] == other.data[..self.len as usize]
    }
}

impl AsRef<[u8]> for U64Partial {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}

impl From<U64Key> for U64Partial {
    #[inline]
    fn from(key: U64Key) -> Self {
        Self {
            data: key.data,
            len: key.len,
        }
    }
}

impl Partial for U64Partial {
    #[inline]
    fn partial_before(&self, length: usize) -> Self {
        debug_assert!(length <= self.len as usize);
        let mut data = [0u8; 8];
        data[..length].copy_from_slice(&self.data[..length]);
        Self {
            data,
            len: length as u8,
        }
    }

    #[inline]
    fn partial_from(&self, src_offset: usize, length: usize) -> Self {
        debug_assert!(src_offset + length <= self.len as usize);
        let mut data = [0u8; 8];
        data[..length].copy_from_slice(&self.data[src_offset..src_offset + length]);
        Self {
            data,
            len: length as u8,
        }
    }

    #[inline]
    fn partial_after(&self, start: usize) -> Self {
        debug_assert!(start <= self.len as usize);
        let remaining = self.len as usize - start;
        let mut data = [0u8; 8];
        data[..remaining].copy_from_slice(&self.data[start..start + remaining]);
        Self {
            data,
            len: remaining as u8,
        }
    }

    #[inline]
    fn partial_extended_with(&self, other: &Self) -> Self {
        let my_len = self.len as usize;
        let other_len = other.len as usize;
        debug_assert!(my_len + other_len <= 8);
        let mut data = self.data;
        data[my_len..my_len + other_len].copy_from_slice(&other.data[..other_len]);
        Self {
            data,
            len: (my_len + other_len) as u8,
        }
    }

    #[inline(always)]
    fn at(&self, pos: usize) -> u8 {
        debug_assert!(pos < self.len as usize);
        self.data[pos]
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    fn prefix_length_common(&self, other: &Self) -> usize {
        let len = min(self.len, other.len) as usize;
        if len == 0 {
            return 0;
        }
        // XOR trick: compare all 8 bytes at once using big-endian interpretation.
        // Leading zeros in the BE u64 correspond to matching leading bytes.
        let xor = u64::from_be_bytes(self.data) ^ u64::from_be_bytes(other.data);
        if xor == 0 {
            return len;
        }
        let first_diff = (xor.leading_zeros() / 8) as usize;
        min(first_diff, len)
    }

    fn prefix_length_key<'a, K>(&self, key: &'a K, at_depth: usize) -> usize
    where
        K: KeyTrait<PartialType = Self> + 'a,
    {
        let len = min(self.len as usize, key.length_at(at_depth));
        for i in 0..len {
            if self.data[i] != key.at(i + at_depth) {
                return i;
            }
        }
        len
    }

    #[inline]
    fn prefix_length_slice(&self, slice: &[u8]) -> usize {
        let len = min(self.len as usize, slice.len());
        for i in 0..len {
            if self.data[i] != slice[i] {
                return i;
            }
        }
        len
    }

    #[inline]
    fn to_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_check() {
        assert_eq!(std::mem::size_of::<U64Key>(), 9);
        assert_eq!(std::mem::size_of::<U64Partial>(), 9);
    }

    #[test]
    fn key_roundtrip() {
        for v in [0u64, 1, 255, 256, 1000, u32::MAX as u64, u64::MAX] {
            let k = U64Key::new(v);
            assert_eq!(k.as_u64(), v, "roundtrip failed for {v}");
            assert_eq!(k.len, 8);
        }
    }

    #[test]
    fn key_ordering() {
        let a = U64Key::new(100);
        let b = U64Key::new(200);
        let c = U64Key::new(u64::MAX);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn key_byte_access() {
        let k = U64Key::new(0x0102030405060708);
        assert_eq!(k.at(0), 0x01);
        assert_eq!(k.at(1), 0x02);
        assert_eq!(k.at(7), 0x08);
    }

    #[test]
    fn key_as_ref_big_endian() {
        let k = U64Key::new(0x0102030405060708);
        assert_eq!(k.as_ref(), &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn partial_from_slice() {
        let p = U64Partial::from_slice(&[0xAA, 0xBB, 0xCC]);
        assert_eq!(p.len(), 3);
        assert_eq!(p.at(0), 0xAA);
        assert_eq!(p.at(1), 0xBB);
        assert_eq!(p.at(2), 0xCC);
    }

    #[test]
    fn partial_before_after() {
        let p = U64Partial::from_slice(&[1, 2, 3, 4, 5]);
        assert_eq!(p.partial_before(3).as_ref(), &[1, 2, 3]);
        assert_eq!(p.partial_after(2).as_ref(), &[3, 4, 5]);
        assert_eq!(p.partial_from(1, 3).as_ref(), &[2, 3, 4]);
    }

    #[test]
    fn partial_extend() {
        let a = U64Partial::from_slice(&[1, 2, 3]);
        let b = U64Partial::from_slice(&[4, 5]);
        let c = a.partial_extended_with(&b);
        assert_eq!(c.as_ref(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn partial_prefix_common_uses_xor() {
        let a = U64Partial::from_slice(&[1, 2, 3, 4]);
        let b = U64Partial::from_slice(&[1, 2, 9, 9]);
        assert_eq!(a.prefix_length_common(&b), 2);

        let c = U64Partial::from_slice(&[1, 2, 3, 4]);
        assert_eq!(a.prefix_length_common(&c), 4);
    }

    #[test]
    fn key_extend_from_partial() {
        let k = U64Key::new_from_partial(&U64Partial::from_slice(&[0xAA, 0xBB]));
        assert_eq!(k.len, 2);
        let ext = k.extend_from_partial(&U64Partial::from_slice(&[0xCC, 0xDD]));
        assert_eq!(ext.len, 4);
        assert_eq!(ext.at(0), 0xAA);
        assert_eq!(ext.at(1), 0xBB);
        assert_eq!(ext.at(2), 0xCC);
        assert_eq!(ext.at(3), 0xDD);
    }

    #[test]
    fn key_truncate() {
        let k = U64Key::new(0x0102030405060708);
        let t = k.truncate(3);
        assert_eq!(t.len, 3);
        assert_eq!(t.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn key_to_partial_at_depth() {
        let k = U64Key::new(0x0102030405060708);
        let p = k.to_partial(3);
        assert_eq!(p.len(), 5);
        assert_eq!(p.as_ref(), &[4, 5, 6, 7, 8]);
    }

    #[test]
    fn tree_insert_get() {
        use crate::HeapMaster;
        use crate::collections::rart::tree::AdaptiveRadixTree;

        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<U64Key, u64>::new(&master);

        for i in 0..1000u64 {
            tree.insert(i, i * 10).unwrap();
        }
        for i in 0..1000u64 {
            assert_eq!(tree.get(i), Some(&(i * 10)), "missing key {i}");
        }
    }

    #[test]
    fn tree_iter_sorted() {
        use crate::HeapMaster;
        use crate::collections::rart::tree::AdaptiveRadixTree;

        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<U64Key, u64>::new(&master);

        for i in [500u64, 100, 900, 1, 50000] {
            tree.insert(i, i).unwrap();
        }

        let keys: Vec<u64> = tree.iter().map(|(k, _)| k.as_u64()).collect();
        assert_eq!(keys, vec![1, 100, 500, 900, 50000]);
    }

    #[test]
    fn tree_range() {
        use crate::HeapMaster;
        use crate::collections::rart::tree::AdaptiveRadixTree;

        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<U64Key, u64>::new(&master);

        for i in 0..100u64 {
            tree.insert(i, i).unwrap();
        }

        let start: U64Key = 50u64.into();
        let end: U64Key = 60u64.into();
        let range: Vec<u64> = tree.range(start..end).map(|(k, _)| k.as_u64()).collect();
        assert_eq!(range, (50..60).collect::<Vec<u64>>());
    }

    #[test]
    fn tree_remove() {
        use crate::HeapMaster;
        use crate::collections::rart::tree::AdaptiveRadixTree;

        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<U64Key, u64>::new(&master);

        tree.insert(10u64, 100).unwrap();
        tree.insert(20u64, 200).unwrap();
        assert_eq!(tree.remove(10u64).unwrap(), Some(100));
        assert_eq!(tree.get(10u64), None);
        assert_eq!(tree.get(20u64), Some(&200));
    }
}
