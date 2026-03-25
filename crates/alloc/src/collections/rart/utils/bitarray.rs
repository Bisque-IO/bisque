use std::mem::MaybeUninit;
use std::ops::Index;

use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::HeapBox;
use crate::collections::rart::utils::bitset::BitsetTrait;

pub struct BitArray<X, const RANGE_WIDTH: usize, BitsetType>
where
    BitsetType: BitsetTrait + std::default::Default,
{
    pub(crate) bitset: BitsetType,
    storage: HeapBox<[MaybeUninit<X>; RANGE_WIDTH]>,
}

impl<X, const RANGE_WIDTH: usize, BitsetType> BitArray<X, RANGE_WIDTH, BitsetType>
where
    BitsetType: BitsetTrait + std::default::Default,
{
    pub fn new(heap: &Heap) -> Result<Self, AllocError> {
        Ok(Self {
            bitset: Default::default(),
            storage: HeapBox::new([const { MaybeUninit::uninit() }; RANGE_WIDTH], heap)?,
        })
    }

    #[allow(dead_code)]
    pub fn push(&mut self, x: X) -> Option<usize> {
        let pos = self.bitset.first_empty()?;
        debug_assert!(pos < RANGE_WIDTH);
        self.bitset.set(pos);
        unsafe {
            self.storage[pos].as_mut_ptr().write(x);
        }
        Some(pos)
    }

    #[allow(dead_code)]
    pub fn pop(&mut self) -> Option<X> {
        let pos = self.bitset.last()?;
        self.bitset.unset(pos);
        let old = std::mem::replace(&mut self.storage[pos], MaybeUninit::uninit());
        Some(unsafe { old.assume_init() })
    }

    #[allow(dead_code)]
    pub fn last(&self) -> Option<&X> {
        self.bitset
            .last()
            .map(|pos| unsafe { self.storage[pos].assume_init_ref() })
    }

    #[inline]
    #[allow(dead_code)]
    pub fn last_used_pos(&self) -> Option<usize> {
        self.bitset.last()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn first_used(&self) -> Option<usize> {
        self.bitset.first_set()
    }

    #[inline]
    pub fn first_empty(&mut self) -> Option<usize> {
        let first_empty = self.bitset.first_empty()?;
        if first_empty > RANGE_WIDTH {
            return None;
        }
        Some(first_empty)
    }

    #[inline]
    pub fn check(&self, pos: usize) -> bool {
        self.bitset.check(pos)
    }

    #[inline]
    pub fn get(&self, pos: usize) -> Option<&X> {
        debug_assert!(pos < RANGE_WIDTH);
        if self.bitset.check(pos) {
            Some(unsafe { self.storage[pos].assume_init_ref() })
        } else {
            None
        }
    }

    #[inline]
    pub fn get_mut(&mut self, pos: usize) -> Option<&mut X> {
        debug_assert!(pos < RANGE_WIDTH);
        if self.bitset.check(pos) {
            Some(unsafe { self.storage[pos].assume_init_mut() })
        } else {
            None
        }
    }

    #[inline]
    pub fn set(&mut self, pos: usize, x: X) {
        debug_assert!(pos < RANGE_WIDTH);
        unsafe {
            self.storage[pos].as_mut_ptr().write(x);
        };
        self.bitset.set(pos);
    }

    #[inline]
    #[allow(dead_code)]
    pub fn update(&mut self, pos: usize, x: X) -> Option<X> {
        let old = self.take_internal(pos);
        unsafe {
            self.storage[pos].as_mut_ptr().write(x);
        };
        self.bitset.set(pos);
        old
    }

    #[inline]
    pub fn erase(&mut self, pos: usize) -> Option<X> {
        let old = self.take_internal(pos)?;
        self.bitset.unset(pos);
        Some(old)
    }

    #[inline]
    fn take_internal(&mut self, pos: usize) -> Option<X> {
        debug_assert!(pos < RANGE_WIDTH);
        if self.bitset.check(pos) {
            let old = std::mem::replace(&mut self.storage[pos], MaybeUninit::uninit());
            Some(unsafe { old.assume_init() })
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        for i in 0..RANGE_WIDTH {
            if self.bitset.check(i) {
                unsafe { self.storage[i].assume_init_drop() }
            }
        }
        self.bitset.clear();
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.bitset.is_empty()
    }

    #[allow(dead_code)]
    pub fn size(&mut self) -> usize {
        self.bitset.size()
    }

    pub fn iter_keys(&self) -> impl DoubleEndedIterator<Item = usize> + '_ {
        self.storage.iter().enumerate().filter_map(|x| {
            if !self.bitset.check(x.0) {
                None
            } else {
                Some(x.0)
            }
        })
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (usize, &X)> {
        self.storage.iter().enumerate().filter_map(|x| {
            if !self.bitset.check(x.0) {
                None
            } else {
                Some((x.0, unsafe { x.1.assume_init_ref() }))
            }
        })
    }

    #[allow(dead_code)]
    pub fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = (usize, &mut X)> {
        self.storage.iter_mut().enumerate().filter_map(|x| {
            if !self.bitset.check(x.0) {
                None
            } else {
                Some((x.0, unsafe { x.1.assume_init_mut() }))
            }
        })
    }
}

impl<X, const RANGE_WIDTH: usize, BitsetType> Index<usize> for BitArray<X, RANGE_WIDTH, BitsetType>
where
    BitsetType: BitsetTrait + std::default::Default,
{
    type Output = X;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl<X, const RANGE_WIDTH: usize, BitsetType> Drop for BitArray<X, RANGE_WIDTH, BitsetType>
where
    BitsetType: BitsetTrait + std::default::Default,
{
    fn drop(&mut self) {
        for i in 0..RANGE_WIDTH {
            if self.bitset.check(i) {
                unsafe { self.storage[i].assume_init_drop() }
            }
        }
        self.bitset.clear();
    }
}

#[cfg(test)]
mod test {
    use crate::HeapMaster;
    use crate::collections::rart::utils::bitarray::BitArray;
    use crate::collections::rart::utils::bitset::Bitset16;

    #[test]
    fn u8_vector() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut vec: BitArray<u8, 48, Bitset16<3>> = BitArray::new(&heap).unwrap();
        assert_eq!(vec.first_empty(), Some(0));
        assert_eq!(vec.last_used_pos(), None);
        assert_eq!(vec.push(123).unwrap(), 0);
        assert_eq!(vec.first_empty(), Some(1));
        assert_eq!(vec.last_used_pos(), Some(0));
        assert_eq!(vec.get(0), Some(&123));
        assert_eq!(vec.push(124).unwrap(), 1);
        assert_eq!(vec.push(55).unwrap(), 2);
        assert_eq!(vec.push(126).unwrap(), 3);
        assert_eq!(vec.pop(), Some(126));
        assert_eq!(vec.first_empty(), Some(3));
        vec.erase(0);
        assert_eq!(vec.first_empty(), Some(0));
        assert_eq!(vec.last_used_pos(), Some(2));
        assert_eq!(vec.size(), 2);
        vec.set(0, 126);
        assert_eq!(vec.get(0), Some(&126));
        assert_eq!(vec.update(0, 123), Some(126));
    }
}
