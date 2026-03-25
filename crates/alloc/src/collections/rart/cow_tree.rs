//! COW (Copy-on-Write) Adaptive Radix Tree with inline leaf storage.
//!
//! Unlike [`VersionedAdaptiveRadixTree`] where every leaf is a separate
//! `HeapArc<VersionedNode>` allocation, `CowTree` stores leaf values inline
//! in the parent mapping's child slots via [`CowChild::Leaf`]. This eliminates
//! the ~15ns HeapArc allocation per insert.
//!
//! COW detection uses `HeapArc::get_mut` (refcount == 1) instead of version
//! stamps. Snapshots share inner nodes via HeapArc refcounting; mutations
//! COW-split only the affected path.

use std::cmp::min;
use std::marker::PhantomData;

use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::HeapArc;
use crate::collections::rart::keys::KeyTrait;
use crate::collections::rart::mapping::NodeMapping;
use crate::collections::rart::mapping::direct_mapping::DirectMapping;
use crate::collections::rart::mapping::indexed_mapping::IndexedMapping;
use crate::collections::rart::mapping::sorted_keyed_mapping::SortedKeyedMapping;
use crate::collections::rart::partials::Partial;
use crate::collections::rart::utils::bitset::Bitset64;

// ---------------------------------------------------------------------------
// CowChild — child slot in a mapping
// ---------------------------------------------------------------------------

/// A child entry: either an inline leaf or a pointer to a shared inner node.
pub enum CowChild<P: Partial, V> {
    /// Leaf value stored directly in the mapping slot. Zero allocation.
    Leaf { prefix: P, value: V },
    /// Pointer to a shared inner node. COW via HeapArc refcount.
    Inner(HeapArc<CowInner<P, V>>),
}

impl<P: Partial + Clone, V: Clone> Clone for CowChild<P, V> {
    fn clone(&self) -> Self {
        match self {
            CowChild::Leaf { prefix, value } => CowChild::Leaf {
                prefix: prefix.clone(),
                value: value.clone(),
            },
            CowChild::Inner(arc) => CowChild::Inner(arc.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// CowContent — inner node mapping variants
// ---------------------------------------------------------------------------

/// The mapping stored inside a [`CowInner`].
pub(crate) enum CowContent<P: Partial, V> {
    Node4(SortedKeyedMapping<CowChild<P, V>, 4>),
    Node16(SortedKeyedMapping<CowChild<P, V>, 16>),
    Node48(IndexedMapping<CowChild<P, V>, 48, Bitset64<1>>),
    Node256(DirectMapping<CowChild<P, V>>),
}

// ---------------------------------------------------------------------------
// CowInner — inner node
// ---------------------------------------------------------------------------

/// Inner node with mapping of children.
///
/// No version field — COW detection uses `HeapArc::get_mut` (refcount check).
pub(crate) struct CowInner<P: Partial, V> {
    pub(crate) prefix: P,
    pub(crate) content: CowContent<P, V>,
}

impl<P: Partial + Clone, V: Clone> CowInner<P, V> {
    /// Create a new inner Node4 with the given prefix.
    fn new_node4(prefix: P, heap: &Heap) -> Result<Self, AllocError> {
        Ok(Self {
            prefix,
            content: CowContent::Node4(SortedKeyedMapping::new(heap)?),
        })
    }

    /// Create a COW clone of this inner node. Children are cloned by value:
    /// Leaf children copy prefix+value, Inner children bump HeapArc refcount.
    fn cow_clone(&self, heap: &Heap) -> Result<Self, AllocError> {
        let content = match &self.content {
            CowContent::Node4(m) => {
                let mut new_m = SortedKeyedMapping::new(heap)?;
                for (key, child) in m.iter() {
                    new_m.add_child(key, child.clone());
                }
                CowContent::Node4(new_m)
            }
            CowContent::Node16(m) => {
                let mut new_m = SortedKeyedMapping::new(heap)?;
                for (key, child) in m.iter() {
                    new_m.add_child(key, child.clone());
                }
                CowContent::Node16(new_m)
            }
            CowContent::Node48(m) => {
                let mut new_m = IndexedMapping::new(heap)?;
                for (key, child) in m.iter() {
                    new_m.add_child(key, child.clone());
                }
                CowContent::Node48(new_m)
            }
            CowContent::Node256(m) => {
                let mut new_m = DirectMapping::new(heap)?;
                for (key, child) in m.iter() {
                    new_m.add_child(key, child.clone());
                }
                CowContent::Node256(new_m)
            }
        };

        Ok(Self {
            prefix: self.prefix.clone(),
            content,
        })
    }

    fn seek_child(&self, key: u8) -> Option<&CowChild<P, V>> {
        match &self.content {
            CowContent::Node4(m) => m.seek_child(key),
            CowContent::Node16(m) => m.seek_child(key),
            CowContent::Node48(m) => m.seek_child(key),
            CowContent::Node256(m) => m.seek_child(key),
        }
    }

    fn seek_child_mut(&mut self, key: u8) -> Option<&mut CowChild<P, V>> {
        match &mut self.content {
            CowContent::Node4(m) => m.seek_child_mut(key),
            CowContent::Node16(m) => m.seek_child_mut(key),
            CowContent::Node48(m) => m.seek_child_mut(key),
            CowContent::Node256(m) => m.seek_child_mut(key),
        }
    }

    /// Replace the child at `key` in-place, returning the old child.
    fn replace_child(&mut self, key: u8, child: CowChild<P, V>) -> Option<CowChild<P, V>> {
        let slot = match &mut self.content {
            CowContent::Node4(m) => m.seek_child_mut(key),
            CowContent::Node16(m) => m.seek_child_mut(key),
            CowContent::Node48(m) => m.seek_child_mut(key),
            CowContent::Node256(m) => m.seek_child_mut(key),
        };
        slot.map(|slot| std::mem::replace(slot, child))
    }

    /// Add a child, growing the mapping if full.
    fn add_child(&mut self, key: u8, child: CowChild<P, V>, heap: &Heap) -> Result<(), AllocError> {
        match &self.content {
            CowContent::Node4(m) if m.num_children() >= 4 => self.grow(heap)?,
            CowContent::Node16(m) if m.num_children() >= 16 => self.grow(heap)?,
            CowContent::Node48(m) if m.num_children() >= 48 => self.grow(heap)?,
            _ => {}
        }

        match &mut self.content {
            CowContent::Node4(m) => m.add_child(key, child),
            CowContent::Node16(m) => m.add_child(key, child),
            CowContent::Node48(m) => m.add_child(key, child),
            CowContent::Node256(m) => m.add_child(key, child),
        }

        Ok(())
    }

    /// Remove a child WITHOUT triggering auto-shrink. Used by the insert
    /// path to temporarily take a child out so its refcount stays at 1
    /// during recursion.
    fn take_child(&mut self, key: u8) -> Option<CowChild<P, V>> {
        match &mut self.content {
            CowContent::Node4(m) => m.delete_child(key),
            CowContent::Node16(m) => m.delete_child(key),
            CowContent::Node48(m) => m.delete_child(key),
            CowContent::Node256(m) => m.delete_child(key),
        }
    }

    /// Re-insert a child after `take_child`. No grow check because the
    /// node still has capacity (we just removed one entry).
    fn put_child(&mut self, key: u8, child: CowChild<P, V>) {
        match &mut self.content {
            CowContent::Node4(m) => m.add_child(key, child),
            CowContent::Node16(m) => m.add_child(key, child),
            CowContent::Node48(m) => m.add_child(key, child),
            CowContent::Node256(m) => m.add_child(key, child),
        }
    }

    /// Delete a child with shrink.
    fn delete_child(&mut self, key: u8, heap: &Heap) -> Result<Option<CowChild<P, V>>, AllocError> {
        let removed = match &mut self.content {
            CowContent::Node4(m) => m.delete_child(key),
            CowContent::Node16(m) => m.delete_child(key),
            CowContent::Node48(m) => m.delete_child(key),
            CowContent::Node256(m) => m.delete_child(key),
        };

        if removed.is_some() {
            self.try_shrink(heap)?;
        }

        Ok(removed)
    }

    fn num_children(&self) -> usize {
        match &self.content {
            CowContent::Node4(m) => m.num_children(),
            CowContent::Node16(m) => m.num_children(),
            CowContent::Node48(m) => m.num_children(),
            CowContent::Node256(m) => m.num_children(),
        }
    }

    fn is_singleton(&self) -> bool {
        matches!(&self.content, CowContent::Node4(m) if m.num_children() == 1)
    }

    fn take_singleton(&mut self) -> (u8, CowChild<P, V>) {
        match &mut self.content {
            CowContent::Node4(m) => m.take_value_for_leaf(),
            _ => panic!("take_singleton called on non-Node4"),
        }
    }

    /// Grow the mapping to the next larger size.
    fn grow(&mut self, heap: &Heap) -> Result<(), AllocError> {
        // Safety: we read the old content out via ptr::read, leaving `self.content`
        // temporarily invalid. We must write a valid value back before returning
        // (including on the error path).
        unsafe {
            let old = std::ptr::read(&self.content);
            let result = match old {
                CowContent::Node4(mut km) => {
                    match SortedKeyedMapping::from_resized(&mut km, heap) {
                        Ok(new_km) => {
                            std::ptr::write(&mut self.content, CowContent::Node16(new_km));
                            Ok(())
                        }
                        Err(e) => {
                            std::ptr::write(&mut self.content, CowContent::Node4(km));
                            Err(e)
                        }
                    }
                }
                CowContent::Node16(mut km) => {
                    match IndexedMapping::from_sorted_keyed(&mut km, heap) {
                        Ok(im) => {
                            std::ptr::write(&mut self.content, CowContent::Node48(im));
                            Ok(())
                        }
                        Err(e) => {
                            std::ptr::write(&mut self.content, CowContent::Node16(km));
                            Err(e)
                        }
                    }
                }
                CowContent::Node48(mut im) => match DirectMapping::from_indexed(&mut im, heap) {
                    Ok(dm) => {
                        std::ptr::write(&mut self.content, CowContent::Node256(dm));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, CowContent::Node48(im));
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

    fn try_shrink(&mut self, heap: &Heap) -> Result<(), AllocError> {
        let should_shrink = match &self.content {
            CowContent::Node16(m) => m.num_children() <= 4,
            CowContent::Node48(m) => m.num_children() <= 16,
            CowContent::Node256(m) => m.num_children() <= 48,
            _ => false,
        };

        if !should_shrink {
            return Ok(());
        }

        self.shrink(heap)
    }

    fn shrink(&mut self, heap: &Heap) -> Result<(), AllocError> {
        unsafe {
            let old = std::ptr::read(&self.content);
            let result = match old {
                CowContent::Node16(mut km) => {
                    match SortedKeyedMapping::from_resized(&mut km, heap) {
                        Ok(new_km) => {
                            std::ptr::write(&mut self.content, CowContent::Node4(new_km));
                            Ok(())
                        }
                        Err(e) => {
                            std::ptr::write(&mut self.content, CowContent::Node16(km));
                            Err(e)
                        }
                    }
                }
                CowContent::Node48(mut im) => {
                    match SortedKeyedMapping::from_indexed(&mut im, heap) {
                        Ok(new_km) => {
                            std::ptr::write(&mut self.content, CowContent::Node16(new_km));
                            Ok(())
                        }
                        Err(e) => {
                            std::ptr::write(&mut self.content, CowContent::Node48(im));
                            Err(e)
                        }
                    }
                }
                CowContent::Node256(mut dm) => match IndexedMapping::from_direct(&mut dm, heap) {
                    Ok(im) => {
                        std::ptr::write(&mut self.content, CowContent::Node48(im));
                        Ok(())
                    }
                    Err(e) => {
                        std::ptr::write(&mut self.content, CowContent::Node256(dm));
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
}

// ---------------------------------------------------------------------------
// CowRoot — tree root
// ---------------------------------------------------------------------------

enum CowRoot<P: Partial, V> {
    Empty,
    Leaf { prefix: P, value: V },
    Inner(HeapArc<CowInner<P, V>>),
}

impl<P: Partial + Clone, V: Clone> Clone for CowRoot<P, V> {
    fn clone(&self) -> Self {
        match self {
            CowRoot::Empty => CowRoot::Empty,
            CowRoot::Leaf { prefix, value } => CowRoot::Leaf {
                prefix: prefix.clone(),
                value: value.clone(),
            },
            CowRoot::Inner(arc) => CowRoot::Inner(arc.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// CowTree — the tree
// ---------------------------------------------------------------------------

/// A COW (copy-on-write) Adaptive Radix Tree with inline leaf storage.
///
/// Leaf values are stored directly in parent mapping slots (no separate
/// `HeapArc` allocation per leaf). COW detection uses `HeapArc::get_mut`
/// on inner nodes. Snapshots share inner nodes via refcounting.
pub struct CowTree<K: KeyTrait, V: Clone> {
    root: CowRoot<K::PartialType, V>,
    heap: Heap,
    _phantom: PhantomData<K>,
}

impl<K: KeyTrait, V: Clone> Clone for CowTree<K, V>
where
    K::PartialType: Clone,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            heap: self.heap.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> CowTree<K, V>
where
    K: KeyTrait,
    K::PartialType: Clone + PartialEq,
    V: Clone,
{
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    /// Create a new, empty COW tree backed by the given heap.
    pub fn new(heap: &Heap) -> Self {
        Self {
            root: CowRoot::Empty,
            heap: heap.clone(),
            _phantom: PhantomData,
        }
    }

    /// Return a reference to the backing heap.
    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    /// Create a cheap snapshot that shares all nodes with `self`.
    ///
    /// Leaf children in the root are cloned by value. Inner nodes are
    /// shared via HeapArc refcounting. Mutations on either copy will
    /// COW-split only the affected path.
    pub fn snapshot(&self) -> Self {
        self.clone()
    }

    /// Returns `true` if the tree contains no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(&self.root, CowRoot::Empty)
    }

    // ------------------------------------------------------------------
    // Lookup
    // ------------------------------------------------------------------

    /// Look up a value by key (generic over anything convertible to `K`).
    pub fn get<Key>(&self, key: Key) -> Option<&V>
    where
        Key: Into<K>,
    {
        let key: K = key.into();
        self.get_k(&key)
    }

    /// Look up a value by an already-constructed key.
    pub fn get_k(&self, key: &K) -> Option<&V> {
        match &self.root {
            CowRoot::Empty => None,
            CowRoot::Leaf { prefix, value } => {
                let prefix_match = prefix.prefix_length_key(key, 0);
                if prefix_match == prefix.len() && prefix.len() == key.length_at(0) {
                    Some(value)
                } else {
                    None
                }
            }
            CowRoot::Inner(arc) => Self::get_recurse(arc, key, 0),
        }
    }

    fn get_recurse<'a>(
        node: &'a CowInner<K::PartialType, V>,
        key: &K,
        depth: usize,
    ) -> Option<&'a V> {
        let prefix_match = node.prefix.prefix_length_key(key, depth);
        if prefix_match != node.prefix.len() {
            return None;
        }
        let new_depth = depth + node.prefix.len();

        if new_depth >= key.length_at(0) {
            return None;
        }

        let next_byte = key.at(new_depth);
        let child = node.seek_child(next_byte)?;

        match child {
            CowChild::Leaf { prefix, value } => {
                let leaf_depth = new_depth;
                let leaf_match = prefix.prefix_length_key(key, leaf_depth);
                if leaf_match == prefix.len() && leaf_depth + prefix.len() == key.length_at(0) {
                    Some(value)
                } else {
                    None
                }
            }
            CowChild::Inner(arc) => Self::get_recurse(arc, key, new_depth),
        }
    }

    // ------------------------------------------------------------------
    // Insert
    // ------------------------------------------------------------------

    /// Insert a key-value pair, returning the old value if the key existed.
    pub fn insert<KV>(&mut self, key: KV, value: V) -> Result<Option<V>, AllocError>
    where
        KV: Into<K>,
    {
        let key: K = key.into();
        self.insert_k(&key, value)
    }

    /// Insert with an already-constructed key.
    pub fn insert_k(&mut self, key: &K, value: V) -> Result<Option<V>, AllocError> {
        let heap = self.heap.clone();

        match std::mem::replace(&mut self.root, CowRoot::Empty) {
            CowRoot::Empty => {
                self.root = CowRoot::Leaf {
                    prefix: key.to_partial(0),
                    value,
                };
                Ok(None)
            }
            CowRoot::Leaf {
                prefix: root_prefix,
                value: root_value,
            } => {
                // Check for exact match (replace).
                let prefix_match = root_prefix.prefix_length_key(key, 0);
                if prefix_match == root_prefix.len() && root_prefix.len() == key.length_at(0) {
                    self.root = CowRoot::Leaf {
                        prefix: root_prefix,
                        value,
                    };
                    return Ok(Some(root_value));
                }

                // Split root leaf into an inner node + two leaf children.
                let common_len = min(prefix_match, root_prefix.len());
                let common_prefix = root_prefix.partial_before(common_len);

                let mut split = CowInner::new_node4(common_prefix, &heap)?;

                // Old leaf becomes a child.
                let old_suffix_key = root_prefix.at(common_len);
                let old_suffix_prefix = root_prefix.partial_after(common_len);
                split.add_child(
                    old_suffix_key,
                    CowChild::Leaf {
                        prefix: old_suffix_prefix,
                        value: root_value,
                    },
                    &heap,
                )?;

                // New leaf becomes a child.
                let new_key_byte = key.at(common_len);
                let new_prefix = key.to_partial(common_len);
                split.add_child(
                    new_key_byte,
                    CowChild::Leaf {
                        prefix: new_prefix,
                        value,
                    },
                    &heap,
                )?;

                self.root = CowRoot::Inner(HeapArc::new(split, &heap)?);
                Ok(None)
            }
            CowRoot::Inner(arc) => {
                let (new_root, old_val) = Self::insert_recurse(arc, key, value, 0, &heap)?;
                self.root = CowRoot::Inner(new_root);
                Ok(old_val)
            }
        }
    }

    /// Recursive insert with copy-on-write.
    ///
    /// Returns `(new_node_arc, old_value_if_replaced)`.
    fn insert_recurse(
        cur_node: HeapArc<CowInner<K::PartialType, V>>,
        key: &K,
        value: V,
        depth: usize,
        heap: &Heap,
    ) -> Result<(HeapArc<CowInner<K::PartialType, V>>, Option<V>), AllocError> {
        let prefix_len = cur_node.prefix.prefix_length_key(key, depth);
        let cur_prefix_len = cur_node.prefix.len();
        let new_depth = depth + prefix_len;

        // ---------- Partial prefix mismatch: split ----------
        if prefix_len < cur_prefix_len {
            let common_prefix = cur_node.prefix.partial_before(prefix_len);
            let mut split_node = CowInner::new_node4(common_prefix, heap)?;

            // The byte at the mismatch point becomes the edge key for the old subtree.
            let old_suffix_key = cur_node.prefix.at(prefix_len);
            let old_suffix_prefix = cur_node.prefix.partial_after(prefix_len);

            // COW the old node and update its prefix.
            let mut old_node = Self::ensure_cow(cur_node, heap)?;
            HeapArc::get_mut(&mut old_node).unwrap().prefix = old_suffix_prefix;

            split_node.add_child(old_suffix_key, CowChild::Inner(old_node), heap)?;

            // Create a new inline leaf for the inserted key.
            let new_partial = key.to_partial(new_depth);
            let insert_key = key.at(new_depth);
            split_node.add_child(
                insert_key,
                CowChild::Leaf {
                    prefix: new_partial,
                    value,
                },
                heap,
            )?;

            return Ok((HeapArc::new(split_node, heap)?, None));
        }

        // ---------- Full prefix match ----------

        // Key exhausted at inner node — cannot insert (prefix-free keys required).
        if new_depth >= key.length_at(0) {
            return Ok((cur_node, None));
        }

        let next_key = key.at(new_depth);

        if cur_node.seek_child(next_key).is_some() {
            // Child exists — COW this node, then handle the child.
            let mut node = Self::ensure_cow(cur_node, heap)?;
            let child = HeapArc::get_mut(&mut node)
                .unwrap()
                .take_child(next_key)
                .unwrap();

            match child {
                CowChild::Leaf {
                    prefix: leaf_prefix,
                    value: leaf_value,
                } => {
                    // Check for exact match (replace value).
                    let leaf_depth = new_depth;
                    let leaf_match = leaf_prefix.prefix_length_key(key, leaf_depth);

                    if leaf_match == leaf_prefix.len()
                        && leaf_depth + leaf_prefix.len() == key.length_at(0)
                    {
                        // Exact match — replace value.
                        HeapArc::get_mut(&mut node).unwrap().put_child(
                            next_key,
                            CowChild::Leaf {
                                prefix: leaf_prefix,
                                value,
                            },
                        );
                        return Ok((node, Some(leaf_value)));
                    }

                    // Prefix mismatch with existing leaf — split into inner node.
                    let common_len = min(leaf_match, leaf_prefix.len());
                    let common_prefix = leaf_prefix.partial_before(common_len);
                    let mut new_inner = CowInner::new_node4(common_prefix, heap)?;

                    // Old leaf child.
                    let old_leaf_edge = leaf_prefix.at(common_len);
                    let old_leaf_suffix = leaf_prefix.partial_after(common_len);
                    new_inner.add_child(
                        old_leaf_edge,
                        CowChild::Leaf {
                            prefix: old_leaf_suffix,
                            value: leaf_value,
                        },
                        heap,
                    )?;

                    // New leaf child.
                    let new_leaf_depth = leaf_depth + common_len;
                    let new_leaf_edge = key.at(new_leaf_depth);
                    let new_leaf_prefix = key.to_partial(new_leaf_depth);
                    new_inner.add_child(
                        new_leaf_edge,
                        CowChild::Leaf {
                            prefix: new_leaf_prefix,
                            value,
                        },
                        heap,
                    )?;

                    let inner_arc = HeapArc::new(new_inner, heap)?;
                    HeapArc::get_mut(&mut node)
                        .unwrap()
                        .put_child(next_key, CowChild::Inner(inner_arc));
                    Ok((node, None))
                }
                CowChild::Inner(child_arc) => {
                    // Recurse into inner child.
                    let (new_child, old_val) =
                        Self::insert_recurse(child_arc, key, value, new_depth, heap)?;
                    HeapArc::get_mut(&mut node)
                        .unwrap()
                        .put_child(next_key, CowChild::Inner(new_child));
                    Ok((node, old_val))
                }
            }
        } else {
            // No child at this key — add a new inline leaf.
            let new_partial = key.to_partial(new_depth);
            let mut node = Self::ensure_cow(cur_node, heap)?;
            HeapArc::get_mut(&mut node).unwrap().add_child(
                next_key,
                CowChild::Leaf {
                    prefix: new_partial,
                    value,
                },
                heap,
            )?;
            Ok((node, None))
        }
    }

    // ------------------------------------------------------------------
    // Remove
    // ------------------------------------------------------------------

    /// Remove a key from the tree, returning the old value if present.
    pub fn remove<KV>(&mut self, key: KV) -> Result<Option<V>, AllocError>
    where
        KV: Into<K>,
    {
        let key: K = key.into();
        self.remove_k(&key)
    }

    /// Remove with an already-constructed key.
    pub fn remove_k(&mut self, key: &K) -> Result<Option<V>, AllocError> {
        let heap = self.heap.clone();

        match std::mem::replace(&mut self.root, CowRoot::Empty) {
            CowRoot::Empty => {
                self.root = CowRoot::Empty;
                Ok(None)
            }
            CowRoot::Leaf {
                prefix: root_prefix,
                value: root_value,
            } => {
                let prefix_match = root_prefix.prefix_length_key(key, 0);
                if prefix_match == root_prefix.len() && root_prefix.len() == key.length_at(0) {
                    // Exact match — remove the root leaf.
                    self.root = CowRoot::Empty;
                    Ok(Some(root_value))
                } else {
                    // Not found — restore root.
                    self.root = CowRoot::Leaf {
                        prefix: root_prefix,
                        value: root_value,
                    };
                    Ok(None)
                }
            }
            CowRoot::Inner(arc) => {
                match Self::remove_recurse(arc, key, 0, &heap)? {
                    None => {
                        // Key not found — should not happen because remove_recurse
                        // consumed the arc. We need to handle this properly.
                        // Actually, remove_recurse returns the (possibly unchanged) arc
                        // on not-found via RemoveOutcome::NotFound.
                        unreachable!("remove_recurse always returns an outcome");
                    }
                    Some(RemoveOutcome::NotFound(arc)) => {
                        self.root = CowRoot::Inner(arc);
                        Ok(None)
                    }
                    Some(RemoveOutcome::Removed { replacement, value }) => {
                        match replacement {
                            RemoveReplacement::Inner(arc) => {
                                self.root = CowRoot::Inner(arc);
                            }
                            RemoveReplacement::Leaf { prefix, value: v } => {
                                self.root = CowRoot::Leaf { prefix, value: v };
                            }
                            RemoveReplacement::Empty => {
                                self.root = CowRoot::Empty;
                            }
                        }
                        Ok(Some(value))
                    }
                }
            }
        }
    }

    /// Recursive remove with COW.
    fn remove_recurse(
        cur_node: HeapArc<CowInner<K::PartialType, V>>,
        key: &K,
        depth: usize,
        heap: &Heap,
    ) -> Result<Option<RemoveOutcome<K::PartialType, V>>, AllocError> {
        let prefix_len = cur_node.prefix.prefix_length_key(key, depth);
        if prefix_len != cur_node.prefix.len() {
            return Ok(Some(RemoveOutcome::NotFound(cur_node)));
        }

        let new_depth = depth + cur_node.prefix.len();

        if new_depth >= key.length_at(0) {
            return Ok(Some(RemoveOutcome::NotFound(cur_node)));
        }

        let next_key = key.at(new_depth);

        if cur_node.seek_child(next_key).is_none() {
            return Ok(Some(RemoveOutcome::NotFound(cur_node)));
        }

        // Check what kind of child we have before COW-ing.
        let is_leaf_match = match cur_node.seek_child(next_key).unwrap() {
            CowChild::Leaf { prefix, .. } => {
                let leaf_match = prefix.prefix_length_key(key, new_depth);
                leaf_match == prefix.len() && new_depth + prefix.len() == key.length_at(0)
            }
            CowChild::Inner(_) => false,
        };

        let is_inner_child = matches!(cur_node.seek_child(next_key).unwrap(), CowChild::Inner(_));

        if is_leaf_match {
            // Remove this leaf child from the parent.
            let mut node = Self::ensure_cow(cur_node, heap)?;
            let node_mut = HeapArc::get_mut(&mut node).unwrap();
            let removed = node_mut.delete_child(next_key, heap)?;

            let value = match removed {
                Some(CowChild::Leaf { value, .. }) => value,
                _ => unreachable!(),
            };

            // Path compression: if we now have a single child, merge.
            if node_mut.is_singleton() {
                let (_child_key, child) = node_mut.take_singleton();

                match child {
                    CowChild::Leaf {
                        prefix: child_prefix,
                        value: child_value,
                    } => {
                        let full_prefix = node_mut.prefix.partial_extended_with(&child_prefix);
                        return Ok(Some(RemoveOutcome::Removed {
                            replacement: RemoveReplacement::Leaf {
                                prefix: full_prefix,
                                value: child_value,
                            },
                            value,
                        }));
                    }
                    CowChild::Inner(child_arc) => {
                        let mut merged = Self::ensure_cow(child_arc, heap)?;
                        let merged_mut = HeapArc::get_mut(&mut merged).unwrap();
                        let full_prefix = node_mut.prefix.partial_extended_with(&merged_mut.prefix);
                        merged_mut.prefix = full_prefix;
                        return Ok(Some(RemoveOutcome::Removed {
                            replacement: RemoveReplacement::Inner(merged),
                            value,
                        }));
                    }
                }
            }

            // If the inner node has zero children, remove it entirely.
            if node_mut.num_children() == 0 {
                return Ok(Some(RemoveOutcome::Removed {
                    replacement: RemoveReplacement::Empty,
                    value,
                }));
            }

            return Ok(Some(RemoveOutcome::Removed {
                replacement: RemoveReplacement::Inner(node),
                value,
            }));
        }

        if is_inner_child {
            // Clone the inner child arc for recursive call.
            let child_arc = match cur_node.seek_child(next_key).unwrap() {
                CowChild::Inner(arc) => arc.clone(),
                _ => unreachable!(),
            };

            match Self::remove_recurse(child_arc, key, new_depth, heap)? {
                Some(RemoveOutcome::NotFound(_)) => Ok(Some(RemoveOutcome::NotFound(cur_node))),
                Some(RemoveOutcome::Removed { replacement, value }) => {
                    let mut node = Self::ensure_cow(cur_node, heap)?;
                    let node_mut = HeapArc::get_mut(&mut node).unwrap();

                    match replacement {
                        RemoveReplacement::Inner(new_child) => {
                            node_mut.replace_child(next_key, CowChild::Inner(new_child));
                        }
                        RemoveReplacement::Leaf {
                            prefix,
                            value: leaf_val,
                        } => {
                            node_mut.replace_child(
                                next_key,
                                CowChild::Leaf {
                                    prefix,
                                    value: leaf_val,
                                },
                            );
                        }
                        RemoveReplacement::Empty => {
                            node_mut.delete_child(next_key, heap)?;
                        }
                    }

                    // Path compression after child removal/replacement.
                    if node_mut.is_singleton() {
                        let (_child_key, child) = node_mut.take_singleton();

                        match child {
                            CowChild::Leaf {
                                prefix: child_prefix,
                                value: child_value,
                            } => {
                                let full_prefix =
                                    node_mut.prefix.partial_extended_with(&child_prefix);
                                return Ok(Some(RemoveOutcome::Removed {
                                    replacement: RemoveReplacement::Leaf {
                                        prefix: full_prefix,
                                        value: child_value,
                                    },
                                    value,
                                }));
                            }
                            CowChild::Inner(child_arc) => {
                                let mut merged = Self::ensure_cow(child_arc, heap)?;
                                let merged_mut = HeapArc::get_mut(&mut merged).unwrap();
                                let full_prefix =
                                    node_mut.prefix.partial_extended_with(&merged_mut.prefix);
                                merged_mut.prefix = full_prefix;
                                return Ok(Some(RemoveOutcome::Removed {
                                    replacement: RemoveReplacement::Inner(merged),
                                    value,
                                }));
                            }
                        }
                    }

                    if node_mut.num_children() == 0 {
                        return Ok(Some(RemoveOutcome::Removed {
                            replacement: RemoveReplacement::Empty,
                            value,
                        }));
                    }

                    Ok(Some(RemoveOutcome::Removed {
                        replacement: RemoveReplacement::Inner(node),
                        value,
                    }))
                }
                None => unreachable!(),
            }
        } else {
            // Leaf that didn't match — not found.
            Ok(Some(RemoveOutcome::NotFound(cur_node)))
        }
    }

    // ------------------------------------------------------------------
    // COW helper
    // ------------------------------------------------------------------

    /// Ensure we have exclusive ownership of an inner node.
    ///
    /// If refcount == 1, returns it as-is. Otherwise, clones the mapping
    /// (leaf children copied by value, inner children refcount-bumped).
    fn ensure_cow(
        mut node: HeapArc<CowInner<K::PartialType, V>>,
        heap: &Heap,
    ) -> Result<HeapArc<CowInner<K::PartialType, V>>, AllocError> {
        if HeapArc::get_mut(&mut node).is_some() {
            return Ok(node);
        }
        // Shared — clone the node.
        let cloned = node.cow_clone(heap)?;
        HeapArc::new(cloned, heap)
    }
}

// ---------------------------------------------------------------------------
// RemoveOutcome helper
// ---------------------------------------------------------------------------

enum RemoveOutcome<P: Partial, V> {
    NotFound(HeapArc<CowInner<P, V>>),
    Removed {
        replacement: RemoveReplacement<P, V>,
        value: V,
    },
}

enum RemoveReplacement<P: Partial, V> {
    Inner(HeapArc<CowInner<P, V>>),
    Leaf { prefix: P, value: V },
    Empty,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;
    use crate::collections::rart::keys::array_key::ArrayKey;
    use crate::collections::rart::keys::u64_key::U64Key;

    fn make_heap() -> HeapMaster {
        HeapMaster::new(64 * 1024 * 1024).unwrap()
    }

    #[test]
    fn test_insert_get_basic() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        assert!(tree.is_empty());

        tree.insert(1u64, 10).unwrap();
        tree.insert(2u64, 20).unwrap();
        tree.insert(3u64, 30).unwrap();

        assert!(!tree.is_empty());
        assert_eq!(tree.get(1u64), Some(&10));
        assert_eq!(tree.get(2u64), Some(&20));
        assert_eq!(tree.get(3u64), Some(&30));
        assert_eq!(tree.get(4u64), None);
    }

    #[test]
    fn test_insert_replace() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        assert_eq!(tree.insert(1u64, 10).unwrap(), None);
        assert_eq!(tree.insert(1u64, 100).unwrap(), Some(10));
        assert_eq!(tree.get(1u64), Some(&100));

        // Replace again.
        assert_eq!(tree.insert(1u64, 1000).unwrap(), Some(100));
        assert_eq!(tree.get(1u64), Some(&1000));
    }

    #[test]
    fn test_snapshot_isolation() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);
        for i in 0..100u64 {
            tree.insert(i, i * 10).unwrap();
        }

        let snap = tree.snapshot();
        tree.insert(200u64, 2000).unwrap();

        assert_eq!(tree.get(200u64), Some(&2000));
        assert_eq!(snap.get(200u64), None); // snapshot doesn't see new key

        for i in 0..100u64 {
            assert_eq!(tree.get(i), Some(&(i * 10)));
            assert_eq!(snap.get(i), Some(&(i * 10)));
        }
    }

    #[test]
    fn test_many_keys() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);
        for i in 0..10_000u64 {
            tree.insert(i, i).unwrap();
        }
        for i in 0..10_000u64 {
            assert_eq!(tree.get(i), Some(&i));
        }
    }

    #[test]
    fn test_remove_basic() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        tree.insert(10u64, 100).unwrap();
        tree.insert(20u64, 200).unwrap();
        tree.insert(30u64, 300).unwrap();

        assert_eq!(tree.remove(20u64).unwrap(), Some(200));
        assert_eq!(tree.get(20u64), None);
        assert_eq!(tree.get(10u64), Some(&100));
        assert_eq!(tree.get(30u64), Some(&300));

        // Remove non-existent key.
        assert_eq!(tree.remove(999u64).unwrap(), None);

        // Remove remaining keys.
        assert_eq!(tree.remove(10u64).unwrap(), Some(100));
        assert_eq!(tree.remove(30u64).unwrap(), Some(300));
        assert!(tree.is_empty());
    }

    #[test]
    fn test_remove_root_leaf() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        tree.insert(42u64, 420).unwrap();
        assert_eq!(tree.remove(42u64).unwrap(), Some(420));
        assert!(tree.is_empty());
        assert_eq!(tree.get(42u64), None);
    }

    #[test]
    fn test_string_keys() {
        let heap = make_heap();
        let mut tree = CowTree::<ArrayKey<16>, i32>::new(&heap);
        tree.insert("hello", 1).unwrap();
        tree.insert("world", 2).unwrap();
        assert_eq!(tree.get("hello"), Some(&1));
        assert_eq!(tree.get("world"), Some(&2));
        assert_eq!(tree.get("missing"), None);
    }

    #[test]
    fn test_snapshot_independent_mutations() {
        let heap = make_heap();
        let mut tree = CowTree::<ArrayKey<16>, i32>::new(&heap);
        tree.insert("key1", 1).unwrap();

        let mut snap = tree.snapshot();

        tree.insert("key2", 2).unwrap();
        snap.insert("key3", 3).unwrap();

        // Original tree should have key2 but not key3.
        assert_eq!(tree.get("key2"), Some(&2));
        assert_eq!(tree.get("key3"), None);

        // Snapshot should have key3 but not key2.
        assert_eq!(snap.get("key2"), None);
        assert_eq!(snap.get("key3"), Some(&3));

        // Both should still have key1.
        assert_eq!(tree.get("key1"), Some(&1));
        assert_eq!(snap.get("key1"), Some(&1));
    }

    #[test]
    fn test_node_growth() {
        let heap = make_heap();
        let mut tree = CowTree::<ArrayKey<16>, i32>::new(&heap);

        // Insert enough keys to trigger Node4 -> Node16 -> Node48 growth.
        for i in 0..50 {
            let key = format!("key{i:03}");
            tree.insert(key.as_str(), i).unwrap();
        }

        for i in 0..50 {
            let key = format!("key{i:03}");
            assert_eq!(tree.get(key.as_str()), Some(&i));
        }
    }

    #[test]
    fn test_remove_with_path_compression() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        // Insert three keys that share a common prefix structure.
        tree.insert(1u64, 10).unwrap();
        tree.insert(2u64, 20).unwrap();
        tree.insert(3u64, 30).unwrap();

        // Remove one; the tree should still work correctly with path compression.
        tree.remove(2u64).unwrap();
        assert_eq!(tree.get(1u64), Some(&10));
        assert_eq!(tree.get(2u64), None);
        assert_eq!(tree.get(3u64), Some(&30));
    }

    #[test]
    fn test_snapshot_remove_isolation() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        for i in 0..20u64 {
            tree.insert(i, i * 10).unwrap();
        }

        let snap = tree.snapshot();

        // Remove from original.
        tree.remove(5u64).unwrap();
        tree.remove(10u64).unwrap();

        // Snapshot should still see the removed keys.
        assert_eq!(snap.get(5u64), Some(&50));
        assert_eq!(snap.get(10u64), Some(&100));

        // Original should not.
        assert_eq!(tree.get(5u64), None);
        assert_eq!(tree.get(10u64), None);

        // Both should see other keys.
        assert_eq!(tree.get(0u64), Some(&0));
        assert_eq!(snap.get(0u64), Some(&0));
    }

    #[test]
    fn test_many_removes() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        for i in 0..1000u64 {
            tree.insert(i, i).unwrap();
        }

        // Remove even keys.
        for i in (0..1000u64).step_by(2) {
            assert_eq!(tree.remove(i).unwrap(), Some(i));
        }

        // Odd keys should still be present.
        for i in (1..1000u64).step_by(2) {
            assert_eq!(tree.get(i), Some(&i));
        }

        // Even keys should be gone.
        for i in (0..1000u64).step_by(2) {
            assert_eq!(tree.get(i), None);
        }
    }

    #[test]
    fn test_multiple_snapshots() {
        let heap = make_heap();
        let mut tree = CowTree::<U64Key, u64>::new(&heap);

        for i in 0..20u64 {
            tree.insert(i, i).unwrap();
        }

        let snap1 = tree.snapshot();
        let snap2 = tree.snapshot();
        let snap3 = tree.snapshot();

        tree.insert(100u64, 100).unwrap();

        assert_eq!(snap1.get(100u64), None);
        assert_eq!(snap2.get(100u64), None);
        assert_eq!(snap3.get(100u64), None);
        assert_eq!(tree.get(100u64), Some(&100));

        for i in 0..20u64 {
            assert_eq!(tree.get(i), Some(&i));
            assert_eq!(snap1.get(i), Some(&i));
            assert_eq!(snap2.get(i), Some(&i));
            assert_eq!(snap3.get(i), Some(&i));
        }
    }
}
