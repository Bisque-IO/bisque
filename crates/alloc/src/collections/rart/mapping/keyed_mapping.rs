use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::collections::rart::mapping::NodeMapping;
use crate::collections::rart::mapping::indexed_mapping::IndexedMapping;
use crate::collections::rart::utils::bitarray::BitArray;
use crate::collections::rart::utils::bitset::BitsetTrait;
use crate::collections::rart::utils::u8_keys::u8_keys_find_key_position;

pub struct KeyedMapping<N, const WIDTH: usize, Bitset>
where
    Bitset: BitsetTrait,
{
    pub(crate) keys: [u8; WIDTH],
    pub(crate) children: BitArray<N, WIDTH, Bitset>,
    pub(crate) num_children: u8,
}

impl<N, const WIDTH: usize, Bitset> KeyedMapping<N, WIDTH, Bitset>
where
    Bitset: BitsetTrait,
{
    #[inline]
    pub fn new(heap: &Heap) -> Result<Self, AllocError> {
        Ok(Self {
            keys: [255; WIDTH],
            children: BitArray::new(heap)?,
            num_children: 0,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn from_indexed<const IDX_WIDTH: usize, FromBitset: BitsetTrait>(
        im: &mut IndexedMapping<N, IDX_WIDTH, FromBitset>,
        heap: &Heap,
    ) -> Result<Self, AllocError> {
        let mut new_mapping = KeyedMapping::new(heap)?;
        im.num_children = 0;
        im.move_into(&mut new_mapping);
        Ok(new_mapping)
    }

    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn from_resized_grow<const OLD_WIDTH: usize, OldBitset: BitsetTrait>(
        km: &mut KeyedMapping<N, OLD_WIDTH, OldBitset>,
        heap: &Heap,
    ) -> Result<Self, AllocError> {
        debug_assert!(WIDTH > OLD_WIDTH);
        let mut new = KeyedMapping::new(heap)?;

        for i in 0..OLD_WIDTH {
            new.keys[i] = km.keys[i];
            let stolen = km.children.erase(i);
            if let Some(n) = stolen {
                new.children.set(i, n);
            }
        }
        km.children.clear();
        new.num_children = km.num_children;
        Ok(new)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (u8, &N)> {
        self.keys
            .iter()
            .enumerate()
            .filter(|p| self.children.check(p.0))
            .map(|p| (*p.1, self.children.get(p.0).unwrap()))
    }
}

impl<N, const WIDTH: usize, Bitset: BitsetTrait> NodeMapping<N, WIDTH>
    for KeyedMapping<N, WIDTH, Bitset>
{
    #[inline]
    fn add_child(&mut self, key: u8, node: N) {
        let idx = self.children.first_empty().unwrap();
        debug_assert!(idx < WIDTH);
        self.keys[idx] = key;
        self.children.set(idx, node);
        self.num_children += 1;
    }

    fn seek_child(&self, key: u8) -> Option<&N> {
        let idx = u8_keys_find_key_position::<WIDTH, _>(key, &self.keys, &self.children.bitset)?;
        self.children.get(idx)
    }

    fn seek_child_mut(&mut self, key: u8) -> Option<&mut N> {
        let idx = u8_keys_find_key_position::<WIDTH, _>(key, &self.keys, &self.children.bitset)?;
        self.children.get_mut(idx)
    }

    fn delete_child(&mut self, key: u8) -> Option<N> {
        let idx = u8_keys_find_key_position::<WIDTH, _>(key, &self.keys, &self.children.bitset)?;
        let result = self.children.erase(idx);
        if result.is_some() {
            self.keys[idx] = 255;
            self.num_children -= 1;
        }

        result
    }

    #[inline(always)]
    fn num_children(&self) -> usize {
        self.num_children as usize
    }
}

impl<N, const WIDTH: usize, Bitset: BitsetTrait> Drop for KeyedMapping<N, WIDTH, Bitset> {
    fn drop(&mut self) {
        self.children.clear();
        self.num_children = 0;
    }
}

#[cfg(test)]
mod tests {
    use crate::HeapMaster;
    use crate::collections::rart::mapping::NodeMapping;
    use crate::collections::rart::mapping::keyed_mapping::KeyedMapping;
    use crate::collections::rart::utils::bitset::Bitset8;

    #[test]
    fn test_add_seek_delete() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut node = KeyedMapping::<u8, 4, Bitset8<4>>::new(&heap).unwrap();
        node.add_child(1, 1);
        node.add_child(2, 2);
        node.add_child(3, 3);
        node.add_child(4, 4);
        assert_eq!(node.num_children(), 4);
        assert_eq!(node.seek_child(1), Some(&1));
        assert_eq!(node.seek_child(2), Some(&2));
        assert_eq!(node.seek_child(3), Some(&3));
        assert_eq!(node.seek_child(4), Some(&4));
        assert_eq!(node.seek_child(5), None);
        assert_eq!(node.delete_child(1), Some(1));
        assert_eq!(node.delete_child(2), Some(2));
        assert_eq!(node.delete_child(3), Some(3));
        assert_eq!(node.delete_child(4), Some(4));
        assert_eq!(node.delete_child(5), None);
        assert_eq!(node.num_children(), 0);
    }

    #[test]
    fn test_ff_regression() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut node = KeyedMapping::<u8, 4, Bitset8<4>>::new(&heap).unwrap();
        node.add_child(1, 1);
        node.add_child(2, 255);
        node.add_child(3, 3);
        node.delete_child(3);
    }
}
