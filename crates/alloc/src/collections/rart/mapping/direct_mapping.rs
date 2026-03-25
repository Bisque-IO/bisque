use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::collections::rart::mapping::NodeMapping;
use crate::collections::rart::mapping::indexed_mapping::IndexedMapping;
use crate::collections::rart::utils::bitarray::BitArray;
use crate::collections::rart::utils::bitset::{Bitset64, BitsetOnesIter, BitsetTrait};

pub struct DirectMapping<N> {
    pub(crate) children: BitArray<N, 256, Bitset64<4>>,
    num_children: usize,
}

impl<N> IntoIterator for DirectMapping<N> {
    type Item = (u8, N);
    type IntoIter = DirectMappingIntoIter<N>;

    fn into_iter(self) -> Self::IntoIter {
        DirectMappingIntoIter {
            key_iter: self.children.bitset.iter(),
            mapping: self,
        }
    }
}

pub struct DirectMappingIntoIter<N> {
    key_iter: BitsetOnesIter<u64, 4>,
    mapping: DirectMapping<N>,
}

impl<N> Iterator for DirectMappingIntoIter<N> {
    type Item = (u8, N);

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.key_iter.next()? as u8;
        let child = self.mapping.delete_child(key)?;
        Some((key, child))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.mapping.num_children;
        (remaining, Some(remaining))
    }
}

impl<N> ExactSizeIterator for DirectMappingIntoIter<N> {}

impl<N> DirectMapping<N> {
    pub fn new(heap: &Heap) -> Result<Self, AllocError> {
        Ok(Self {
            children: BitArray::new(heap)?,
            num_children: 0,
        })
    }

    pub fn from_indexed<const WIDTH: usize, FromBitset: BitsetTrait>(
        im: &mut IndexedMapping<N, WIDTH, FromBitset>,
        heap: &Heap,
    ) -> Result<Self, AllocError> {
        let mut new_mapping = DirectMapping::<N>::new(heap)?;
        im.num_children = 0;
        im.move_into(&mut new_mapping);
        Ok(new_mapping)
    }

    #[inline]
    pub fn iter(&self) -> DirectMappingIter<'_, N> {
        DirectMappingIter {
            key_iter: self.children.bitset.iter(),
            mapping: self,
        }
    }
}

pub struct DirectMappingIter<'a, N> {
    key_iter: BitsetOnesIter<u64, 4>,
    mapping: &'a DirectMapping<N>,
}

impl<'a, N> Iterator for DirectMappingIter<'a, N> {
    type Item = (u8, &'a N);

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.key_iter.next()? as u8;
        let child = self.mapping.children.get(key as usize)?;
        Some((key, child))
    }
}

impl<N> NodeMapping<N, 256> for DirectMapping<N> {
    #[inline]
    fn add_child(&mut self, key: u8, node: N) {
        self.children.set(key as usize, node);
        self.num_children += 1;
    }

    #[inline]
    fn seek_child(&self, key: u8) -> Option<&N> {
        self.children.get(key as usize)
    }

    #[inline]
    fn seek_child_mut(&mut self, key: u8) -> Option<&mut N> {
        self.children.get_mut(key as usize)
    }

    #[inline]
    fn delete_child(&mut self, key: u8) -> Option<N> {
        let n = self.children.erase(key as usize);
        if n.is_some() {
            self.num_children -= 1;
        }
        n
    }

    #[inline]
    fn num_children(&self) -> usize {
        self.num_children
    }
}

#[cfg(test)]
mod tests {
    use crate::HeapMaster;
    use crate::collections::rart::mapping::NodeMapping;

    #[test]
    fn direct_mapping_test() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut dm = super::DirectMapping::new(&heap).unwrap();
        for i in 0..255 {
            dm.add_child(i, i);
            assert_eq!(*dm.seek_child(i).unwrap(), i);
            assert_eq!(dm.delete_child(i), Some(i));
            assert_eq!(dm.seek_child(i), None);
        }
    }

    #[test]
    fn iter_preserves_key_order_for_sparse_children() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut dm = super::DirectMapping::new(&heap).unwrap();
        for key in [200u8, 3, 250, 17, 128] {
            dm.add_child(key, key);
        }

        let keys: Vec<u8> = dm.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![3, 17, 128, 200, 250]);
    }
}
