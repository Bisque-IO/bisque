use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::collections::rart::mapping::NodeMapping;
use crate::collections::rart::mapping::direct_mapping::DirectMapping;
use crate::collections::rart::mapping::direct_mapping::DirectMappingIter;
use crate::collections::rart::mapping::indexed_mapping::IndexedMapping;
use crate::collections::rart::mapping::indexed_mapping::IndexedMappingIter;
use crate::collections::rart::mapping::sorted_keyed_mapping::SortedKeyedMapping;
use crate::collections::rart::mapping::sorted_keyed_mapping::SortedKeyedMappingIter;
use crate::collections::rart::partials::Partial;
use crate::collections::rart::utils::bitset::Bitset64;

pub trait Node<P: Partial, V> {
    fn new_leaf(partial: P, value: V) -> Self;

    fn value(&self) -> Option<&V>;
    fn value_mut(&mut self) -> Option<&mut V>;
    fn is_leaf(&self) -> bool;
    fn is_inner(&self) -> bool;
    fn seek_child(&self, key: u8) -> Option<&Self>;
    fn seek_child_mut(&mut self, key: u8) -> Option<&mut Self>;
    fn capacity(&self) -> usize;
    fn num_children(&self) -> usize;
}

pub struct DefaultNode<P: Partial, V> {
    pub(crate) prefix: P,
    pub(crate) content: Content<P, V>,
}

pub(crate) enum Content<P: Partial, V> {
    Leaf(V),
    Node4(SortedKeyedMapping<DefaultNode<P, V>, 4>),
    Node16(SortedKeyedMapping<DefaultNode<P, V>, 16>),
    Node48(IndexedMapping<DefaultNode<P, V>, 48, Bitset64<1>>),
    Node256(DirectMapping<DefaultNode<P, V>>),
}

pub enum NodeIter<'a, P: Partial, V> {
    Node4(SortedKeyedMappingIter<'a, DefaultNode<P, V>, 4>),
    Node16(SortedKeyedMappingIter<'a, DefaultNode<P, V>, 16>),
    Node48(IndexedMappingIter<'a, DefaultNode<P, V>, 48, Bitset64<1>>),
    Node256(DirectMappingIter<'a, DefaultNode<P, V>>),
    Empty,
}

impl<'a, P: Partial, V> Iterator for NodeIter<'a, P, V> {
    type Item = (u8, &'a DefaultNode<P, V>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NodeIter::Node4(iter) => iter.next(),
            NodeIter::Node16(iter) => iter.next(),
            NodeIter::Node48(iter) => iter.next(),
            NodeIter::Node256(iter) => iter.next(),
            NodeIter::Empty => None,
        }
    }
}

impl<P: Partial, V> Node<P, V> for DefaultNode<P, V> {
    #[inline]
    fn new_leaf(partial: P, value: V) -> Self {
        Self {
            prefix: partial,
            content: Content::<P, V>::Leaf(value),
        }
    }

    #[inline]
    fn value(&self) -> Option<&V> {
        match &self.content {
            Content::<P, V>::Leaf(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    fn value_mut(&mut self) -> Option<&mut V> {
        match &mut self.content {
            Content::Leaf(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    fn is_leaf(&self) -> bool {
        matches!(&self.content, Content::Leaf(_))
    }

    #[inline]
    fn is_inner(&self) -> bool {
        !self.is_leaf()
    }

    fn seek_child(&self, key: u8) -> Option<&Self> {
        match &self.content {
            Content::Node4(m) => m.seek_child(key),
            Content::Node16(m) => m.seek_child(key),
            Content::Node48(m) => m.seek_child(key),
            Content::Node256(m) => m.seek_child(key),
            Content::Leaf(_) => None,
        }
    }

    fn seek_child_mut(&mut self, key: u8) -> Option<&mut Self> {
        match &mut self.content {
            Content::Node4(m) => m.seek_child_mut(key),
            Content::Node16(m) => m.seek_child_mut(key),
            Content::Node48(m) => m.seek_child_mut(key),
            Content::Node256(m) => m.seek_child_mut(key),
            Content::Leaf(_) => None,
        }
    }

    fn capacity(&self) -> usize {
        match &self.content {
            Content::Node4(_) => 4,
            Content::Node16(_) => 16,
            Content::Node48(_) => 48,
            Content::Node256(_) => 256,
            Content::Leaf(_) => 0,
        }
    }

    fn num_children(&self) -> usize {
        match &self.content {
            Content::Node4(m) => m.num_children(),
            Content::Node16(m) => m.num_children(),
            Content::Node48(m) => m.num_children(),
            Content::Node256(m) => m.num_children(),
            Content::Leaf(_) => 0,
        }
    }
}

impl<P: Partial, V> DefaultNode<P, V> {
    /// Create a new inner node (Node4) with the given prefix.
    ///
    /// Requires a `&Heap` because inner nodes allocate backing storage.
    pub fn new_inner(prefix: P, heap: &Heap) -> Result<Self, AllocError> {
        Ok(Self {
            prefix,
            content: Content::Node4(SortedKeyedMapping::new(heap)?),
        })
    }

    /// Add a child to this inner node under the given key byte.
    ///
    /// If the current mapping is full, it is grown to the next larger size
    /// before inserting. Returns `AllocError` if the growth allocation fails.
    ///
    /// # Panics
    ///
    /// Panics if this node is a leaf.
    pub fn add_child(&mut self, key: u8, node: Self, heap: &Heap) -> Result<(), AllocError> {
        match &self.content {
            Content::Node4(m) if m.num_children() >= 4 => self.grow(heap)?,
            Content::Node16(m) if m.num_children() >= 16 => self.grow(heap)?,
            Content::Node48(m) if m.num_children() >= 48 => self.grow(heap)?,
            _ => {}
        }

        match &mut self.content {
            Content::Node4(m) => m.add_child(key, node),
            Content::Node16(m) => m.add_child(key, node),
            Content::Node48(m) => m.add_child(key, node),
            Content::Node256(m) => m.add_child(key, node),
            Content::Leaf(_) => panic!("cannot add child to leaf node"),
        }

        Ok(())
    }

    /// Delete a child from this inner node by key byte.
    ///
    /// If the mapping is under-utilized after removal, it is shrunk to the
    /// next smaller size. Returns `AllocError` if the shrink allocation fails.
    pub fn delete_child(&mut self, key: u8, heap: &Heap) -> Result<Option<Self>, AllocError> {
        let removed = match &mut self.content {
            Content::Node4(m) => m.delete_child(key),
            Content::Node16(m) => m.delete_child(key),
            Content::Node48(m) => m.delete_child(key),
            Content::Node256(m) => m.delete_child(key),
            Content::Leaf(_) => return Ok(None),
        };

        if removed.is_some() {
            self.try_shrink(heap)?;
        }

        Ok(removed)
    }

    /// Grow the current mapping to the next larger node type.
    fn grow(&mut self, heap: &Heap) -> Result<(), AllocError> {
        // Safety: we read the old content out via ptr::read, leaving `self.content`
        // temporarily invalid. We must write a valid value back before returning
        // (including on the error path). On allocation failure, we write the old
        // content back so the node remains valid.
        unsafe {
            let old = std::ptr::read(&self.content);
            let result = match old {
                Content::Node4(mut km) => match SortedKeyedMapping::from_resized(&mut km, heap) {
                    Ok(new_km) => {
                        std::ptr::write(&mut self.content, Content::Node16(new_km));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, Content::Node4(km));
                        Err(e)
                    }
                },
                Content::Node16(mut km) => match IndexedMapping::from_sorted_keyed(&mut km, heap) {
                    Ok(im) => {
                        std::ptr::write(&mut self.content, Content::Node48(im));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, Content::Node16(km));
                        Err(e)
                    }
                },
                Content::Node48(mut im) => match DirectMapping::from_indexed(&mut im, heap) {
                    Ok(dm) => {
                        std::ptr::write(&mut self.content, Content::Node256(dm));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, Content::Node48(im));
                        Err(e)
                    }
                },
                other => {
                    std::ptr::write(&mut self.content, other);
                    Ok(())
                }
            };
            result
        }
    }

    /// Shrink the current mapping to the next smaller node type if it is
    /// under-utilized. Called after `delete_child`.
    fn try_shrink(&mut self, heap: &Heap) -> Result<(), AllocError> {
        let should_shrink = match &self.content {
            Content::Node16(m) => m.num_children() <= 4,
            Content::Node48(m) => m.num_children() <= 16,
            Content::Node256(m) => m.num_children() <= 48,
            _ => false,
        };

        if !should_shrink {
            return Ok(());
        }

        self.shrink(heap)
    }

    /// Unconditionally shrink the current mapping to the next smaller type.
    fn shrink(&mut self, heap: &Heap) -> Result<(), AllocError> {
        // Safety: same ptr::read/ptr::write pattern as `grow`. On allocation
        // failure we restore the original content so the node stays valid.
        unsafe {
            let old = std::ptr::read(&self.content);
            let result = match old {
                Content::Node16(mut km) => match SortedKeyedMapping::from_resized(&mut km, heap) {
                    Ok(new_km) => {
                        std::ptr::write(&mut self.content, Content::Node4(new_km));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, Content::Node16(km));
                        Err(e)
                    }
                },
                Content::Node48(mut im) => match SortedKeyedMapping::from_indexed(&mut im, heap) {
                    Ok(new_km) => {
                        std::ptr::write(&mut self.content, Content::Node16(new_km));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, Content::Node48(im));
                        Err(e)
                    }
                },
                Content::Node256(mut dm) => match IndexedMapping::from_direct(&mut dm, heap) {
                    Ok(im) => {
                        std::ptr::write(&mut self.content, Content::Node48(im));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, Content::Node256(dm));
                        Err(e)
                    }
                },
                other => {
                    std::ptr::write(&mut self.content, other);
                    Ok(())
                }
            };
            result
        }
    }

    /// Return an iterator over the children of this inner node.
    pub fn iter(&self) -> NodeIter<'_, P, V> {
        match &self.content {
            Content::Node4(m) => NodeIter::Node4(m.iter()),
            Content::Node16(m) => NodeIter::Node16(m.iter()),
            Content::Node48(m) => NodeIter::Node48(m.iter()),
            Content::Node256(m) => NodeIter::Node256(m.iter()),
            Content::Leaf(_) => NodeIter::Empty,
        }
    }

    /// Replace the value in a leaf node, returning the old value.
    ///
    /// # Panics
    ///
    /// Panics if this node is not a leaf.
    pub fn replace_value(&mut self, value: V) -> V {
        match &mut self.content {
            Content::Leaf(v) => std::mem::replace(v, value),
            _ => panic!("replace_value called on inner node"),
        }
    }

    /// Take the leaf value out, leaving an uninitialized placeholder.
    ///
    /// # Safety
    ///
    /// The caller must ensure this node is a leaf and is not used after
    /// this call except to be dropped or overwritten.
    pub fn take_value(self) -> Option<V> {
        match self.content {
            Content::Leaf(v) => Some(v),
            _ => None,
        }
    }

    /// Returns `true` if this is a Node4 with exactly one child.
    pub fn is_singleton(&self) -> bool {
        matches!(&self.content, Content::Node4(m) if m.num_children() == 1)
    }

    /// Extract the single child from a singleton Node4.
    ///
    /// Returns `(key, child)`. Panics if not a singleton.
    pub fn take_singleton(&mut self) -> (u8, Self) {
        match &mut self.content {
            Content::Node4(m) => m.take_value_for_leaf(),
            _ => panic!("take_singleton called on non-Node4"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::HeapMaster;
    use crate::collections::rart::node::{DefaultNode, Node};
    use crate::collections::rart::partials::array_partial::ArrPartial;

    #[test]
    fn test_n4() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let test_key: ArrPartial<16> = ArrPartial::key("abc".as_bytes());

        let mut n4 = DefaultNode::new_inner(test_key.clone(), &heap).unwrap();
        n4.add_child(5, DefaultNode::new_leaf(test_key.clone(), 1), &heap)
            .unwrap();
        n4.add_child(4, DefaultNode::new_leaf(test_key.clone(), 2), &heap)
            .unwrap();
        n4.add_child(3, DefaultNode::new_leaf(test_key.clone(), 3), &heap)
            .unwrap();
        n4.add_child(2, DefaultNode::new_leaf(test_key.clone(), 4), &heap)
            .unwrap();

        assert_eq!(*n4.seek_child(5).unwrap().value().unwrap(), 1);
        assert_eq!(*n4.seek_child(4).unwrap().value().unwrap(), 2);
        assert_eq!(*n4.seek_child(3).unwrap().value().unwrap(), 3);
        assert_eq!(*n4.seek_child(2).unwrap().value().unwrap(), 4);

        n4.delete_child(5, &heap).unwrap();
        assert!(n4.seek_child(5).is_none());
        assert_eq!(*n4.seek_child(4).unwrap().value().unwrap(), 2);
        assert_eq!(*n4.seek_child(3).unwrap().value().unwrap(), 3);
        assert_eq!(*n4.seek_child(2).unwrap().value().unwrap(), 4);

        n4.delete_child(2, &heap).unwrap();
        assert!(n4.seek_child(5).is_none());
        assert!(n4.seek_child(2).is_none());

        n4.add_child(2, DefaultNode::new_leaf(test_key, 4), &heap)
            .unwrap();
        n4.delete_child(3, &heap).unwrap();
        assert!(n4.seek_child(5).is_none());
        assert!(n4.seek_child(3).is_none());
    }

    #[test]
    fn test_n16() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let test_key: ArrPartial<16> = ArrPartial::key("abc".as_bytes());

        let mut n16 = DefaultNode::new_inner(test_key.clone(), &heap).unwrap();

        for i in (0..16).rev() {
            n16.add_child(i, DefaultNode::new_leaf(test_key.clone(), i), &heap)
                .unwrap();
        }

        for i in 0..16 {
            debug_assert_eq!(*n16.seek_child(i).unwrap().value().unwrap(), i);
        }

        n16.delete_child(15, &heap).unwrap();
        n16.delete_child(14, &heap).unwrap();
        debug_assert!(n16.seek_child(15).is_none());
        debug_assert!(n16.seek_child(14).is_none());
        for i in 0..14 {
            debug_assert_eq!(*n16.seek_child(i).unwrap().value().unwrap(), i);
        }
    }

    #[test]
    fn test_n48() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let test_key: ArrPartial<16> = ArrPartial::key("abc".as_bytes());

        let mut n48 = DefaultNode::new_inner(test_key.clone(), &heap).unwrap();

        for i in 0..48 {
            n48.add_child(i, DefaultNode::new_leaf(test_key.clone(), i), &heap)
                .unwrap();
        }

        for i in 0..48 {
            debug_assert_eq!(*n48.seek_child(i).unwrap().value().unwrap(), i);
        }

        n48.delete_child(47, &heap).unwrap();
        n48.delete_child(46, &heap).unwrap();
        debug_assert!(n48.seek_child(47).is_none());
        debug_assert!(n48.seek_child(46).is_none());
        for i in 0..46 {
            debug_assert_eq!(*n48.seek_child(i).unwrap().value().unwrap(), i);
        }
    }

    #[test]
    fn test_n_256() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let test_key: ArrPartial<16> = ArrPartial::key("abc".as_bytes());

        let mut n256 = DefaultNode::new_inner(test_key.clone(), &heap).unwrap();

        for i in 0..=255u8 {
            n256.add_child(i, DefaultNode::new_leaf(test_key.clone(), i), &heap)
                .unwrap();
        }
        for i in 0..=255u8 {
            debug_assert_eq!(*n256.seek_child(i).unwrap().value().unwrap(), i);
        }

        n256.delete_child(47, &heap).unwrap();
        n256.delete_child(46, &heap).unwrap();
        debug_assert!(n256.seek_child(47).is_none());
        debug_assert!(n256.seek_child(46).is_none());
        for i in 0..46u8 {
            debug_assert_eq!(*n256.seek_child(i).unwrap().value().unwrap(), i);
        }
        for i in 48..=255u8 {
            debug_assert_eq!(*n256.seek_child(i).unwrap().value().unwrap(), i);
        }
    }
}
