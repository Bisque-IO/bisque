use crate::collections::rart::keys::KeyTrait;

pub mod array_partial;
pub mod vector_partial;

pub trait Partial: AsRef<[u8]> {
    fn partial_before(&self, length: usize) -> Self;
    fn partial_from(&self, src_offset: usize, length: usize) -> Self;
    fn partial_after(&self, start: usize) -> Self;
    fn partial_extended_with(&self, other: &Self) -> Self;
    fn at(&self, pos: usize) -> u8;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn prefix_length_common(&self, other: &Self) -> usize;
    fn prefix_length_key<'a, K>(&self, key: &'a K, at_depth: usize) -> usize
    where
        K: KeyTrait<PartialType = Self> + 'a;
    fn prefix_length_slice(&self, slice: &[u8]) -> usize;
    fn to_slice(&self) -> &[u8];

    fn iter(&self) -> std::slice::Iter<'_, u8> {
        self.as_ref().iter()
    }

    fn starts_with(&self, prefix: &[u8]) -> bool {
        self.as_ref().starts_with(prefix)
    }

    fn ends_with(&self, suffix: &[u8]) -> bool {
        self.as_ref().ends_with(suffix)
    }
}
