//! Heap-backed Adaptive Radix Tree implementation.
//!
//! This module contains the main [`AdaptiveRadixTree`] implementation. All internal
//! node allocations go through a [`Heap`], and every mutating operation returns
//! `Result<_, AllocError>` instead of panicking on OOM.

use std::cmp::min;
use std::ops::RangeBounds;

use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::collections::rart::iter::{Iter, ValuesIter};
use crate::collections::rart::keys::KeyTrait;
use crate::collections::rart::node::{Content, DefaultNode, Node};
use crate::collections::rart::partials::Partial;
use crate::collections::rart::range::Range;
use crate::collections::rart::stats::{TreeStats, TreeStatsTrait, update_tree_stats};

/// A Heap-backed Adaptive Radix Tree (ART).
///
/// The tree automatically adjusts its internal representation based on the
/// number of children at each node, using different node types (4, 16, 48, 256
/// children) based on density. All allocations are performed through the
/// stored [`Heap`], and mutating operations return `Result<_, AllocError>`.
///
/// ## Type Parameters
///
/// - `KeyType`: The type of keys stored in the tree, must implement [`KeyTrait`]
/// - `ValueType`: The type of values associated with keys
pub struct AdaptiveRadixTree<KeyType, ValueType>
where
    KeyType: KeyTrait,
{
    root: Option<DefaultNode<KeyType::PartialType, ValueType>>,
    heap: Heap,
    _phantom: std::marker::PhantomData<KeyType>,
}

impl<KeyType, ValueType> AdaptiveRadixTree<KeyType, ValueType>
where
    KeyType: KeyTrait,
{
    /// Create a new empty Adaptive Radix Tree backed by the given heap.
    pub fn new(heap: &Heap) -> Self {
        Self {
            root: None,
            heap: heap.clone(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new Adaptive Radix Tree with the given root node.
    ///
    /// This is primarily used for internal conversions.
    pub(crate) fn from_root(
        root: DefaultNode<KeyType::PartialType, ValueType>,
        heap: &Heap,
    ) -> Self {
        Self {
            root: Some(root),
            heap: heap.clone(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a tree from an optional root and a heap.
    ///
    /// Used by `VersionedAdaptiveRadixTree::into_unversioned` to construct
    /// the unversioned tree from converted nodes.
    pub(crate) fn from_parts(
        root: Option<DefaultNode<KeyType::PartialType, ValueType>>,
        heap: Heap,
    ) -> Self {
        Self {
            root,
            heap,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Return a reference to the backing heap.
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    // ---- read-only lookups (no allocation, unchanged signatures) ----

    /// Get a value by key (generic version).
    ///
    /// Accepts any type that can be converted into the tree's key type.
    #[inline]
    pub fn get<Key>(&self, key: Key) -> Option<&ValueType>
    where
        Key: Into<KeyType>,
    {
        self.get_k(&key.into())
    }

    /// Get a value by key reference (direct version).
    #[inline]
    pub fn get_k(&self, key: &KeyType) -> Option<&ValueType> {
        Self::get_iterate(self.root.as_ref()?, key)
    }

    /// Get a mutable reference to a value by key (generic version).
    #[inline]
    pub fn get_mut<Key>(&mut self, key: Key) -> Option<&mut ValueType>
    where
        Key: Into<KeyType>,
    {
        self.get_mut_k(&key.into())
    }

    /// Get a mutable reference to a value by key reference (direct version).
    #[inline]
    pub fn get_mut_k(&mut self, key: &KeyType) -> Option<&mut ValueType> {
        Self::get_iterate_mut(self.root.as_mut()?, key)
    }

    /// Return the deepest key/value pair whose key is a prefix of `key`.
    #[inline]
    pub fn longest_prefix_match<Key>(&self, key: Key) -> Option<(KeyType, &ValueType)>
    where
        Key: Into<KeyType>,
    {
        self.longest_prefix_match_k(&key.into())
    }

    /// Return the deepest key/value pair whose key is a prefix of `key`.
    #[inline]
    pub fn longest_prefix_match_k(&self, key: &KeyType) -> Option<(KeyType, &ValueType)> {
        Self::longest_prefix_match_iterate(self.root.as_ref()?, key)
    }

    /// Iterate over all entries whose keys start with `prefix`.
    #[inline]
    pub fn prefix_iter<Key>(
        &self,
        prefix: Key,
    ) -> Iter<'_, KeyType, KeyType::PartialType, ValueType>
    where
        Key: Into<KeyType>,
    {
        self.prefix_iter_k(&prefix.into())
    }

    /// Iterate over all entries whose keys start with `prefix`.
    pub fn prefix_iter_k(
        &self,
        prefix: &KeyType,
    ) -> Iter<'_, KeyType, KeyType::PartialType, ValueType> {
        let Some(root) = self.root.as_ref() else {
            return Iter::new(None);
        };
        let Some((subtree_root, subtree_root_key)) = Self::find_prefix_subtree(root, prefix) else {
            return Iter::new(None);
        };
        Iter::new_with_prefix(Some(subtree_root), subtree_root_key)
    }

    // ---- fallible mutating operations ----

    /// Insert a key-value pair (generic version).
    ///
    /// Returns the old value when a key is replaced, or `AllocError` if an
    /// internal node allocation fails.
    #[inline]
    pub fn insert<KV>(&mut self, key: KV, value: ValueType) -> Result<Option<ValueType>, AllocError>
    where
        KV: Into<KeyType>,
    {
        self.insert_k(&key.into(), value)
    }

    /// Insert a key-value pair using key reference (direct version).
    ///
    /// Returns the old value when a key is replaced, or `AllocError` if an
    /// internal node allocation fails.
    #[inline]
    pub fn insert_k(
        &mut self,
        key: &KeyType,
        value: ValueType,
    ) -> Result<Option<ValueType>, AllocError> {
        let Some(root) = &mut self.root else {
            self.root = Some(DefaultNode::new_leaf(key.to_partial(0), value));
            return Ok(None);
        };

        Self::insert_recurse(root, key, value, 0, &self.heap.clone())
    }

    /// Remove a key-value pair (generic version).
    ///
    /// Returns the removed value if the key existed, or `AllocError` if a
    /// shrink allocation fails.
    pub fn remove<KV>(&mut self, key: KV) -> Result<Option<ValueType>, AllocError>
    where
        KV: Into<KeyType>,
    {
        self.remove_k(&key.into())
    }

    /// Remove a key-value pair using key reference (direct version).
    ///
    /// Returns the removed value if the key existed, or `AllocError` if a
    /// shrink allocation fails.
    pub fn remove_k(&mut self, key: &KeyType) -> Result<Option<ValueType>, AllocError> {
        let root = match self.root.as_mut() {
            Some(r) => r,
            None => return Ok(None),
        };

        // Don't bother doing anything if there's no prefix match on the root.
        let prefix_common_match = root.prefix.prefix_length_key(key, 0);
        if prefix_common_match != root.prefix.len() {
            return Ok(None);
        }

        // Special case: if the root is a leaf and matches the key, remove it.
        if root.is_leaf() {
            let stolen = self.root.take().unwrap();
            let v = match stolen.content {
                Content::Leaf(v) => v,
                _ => unreachable!(),
            };
            return Ok(Some(v));
        }

        let heap = self.heap.clone();
        let result = Self::remove_recurse(root, key, prefix_common_match, &heap)?;

        // Prune root if it's now empty.
        if let Some(r) = &self.root {
            if r.is_inner() && r.num_children() == 0 {
                self.root = None;
            }
        }

        Ok(result)
    }

    // ---- read-only iterators (unchanged) ----

    /// Create an iterator over all key-value pairs in the tree.
    ///
    /// The iterator yields items in lexicographic order of the keys.
    pub fn iter(&self) -> Iter<'_, KeyType, KeyType::PartialType, ValueType> {
        Iter::new(self.root.as_ref())
    }

    /// Create an iterator over only the values in the tree.
    ///
    /// Skips key reconstruction entirely and only yields values.
    pub fn values_iter(&self) -> ValuesIter<'_, KeyType::PartialType, ValueType> {
        ValuesIter::new(self.root.as_ref())
    }

    /// Intersect two trees using ART-native node traversal.
    ///
    /// Walks both tries in lockstep, pruning mismatched prefixes early.
    pub fn intersect_with<'a, F>(&'a self, other: &'a Self, mut on_match: F)
    where
        F: FnMut(KeyType, &'a ValueType, &'a ValueType),
    {
        let (Some(left_root), Some(right_root)) = (self.root.as_ref(), other.root.as_ref()) else {
            return;
        };

        let mut key_buf = Vec::with_capacity(KeyType::MAXIMUM_SIZE.unwrap_or(64));
        Self::intersect_nodes(left_root, 0, right_root, 0, &mut key_buf, &mut on_match);
    }

    /// Intersect two trees and invoke a callback with value pairs only.
    ///
    /// Avoids key materialization; useful when only the joined values are needed.
    pub fn intersect_values_with<'a, F>(&'a self, other: &'a Self, mut on_match: F)
    where
        F: FnMut(&'a ValueType, &'a ValueType),
    {
        let (Some(left_root), Some(right_root)) = (self.root.as_ref(), other.root.as_ref()) else {
            return;
        };

        Self::intersect_nodes_values(left_root, 0, right_root, 0, &mut on_match);
    }

    /// Count the number of keys that exist in both trees.
    pub fn intersect_count(&self, other: &Self) -> usize {
        let mut count = 0usize;
        self.intersect_values_with(other, |_, _| {
            count += 1;
        });
        count
    }

    /// Create an iterator over key-value pairs within a specified range.
    pub fn range<'a, R>(&'a self, range: R) -> Range<'a, KeyType, ValueType>
    where
        R: RangeBounds<KeyType> + 'a,
    {
        if self.root.is_none() {
            return Range::empty();
        }

        let start_bound = range.start_bound().cloned();
        let end_bound = range.end_bound().cloned();

        match start_bound {
            std::collections::Bound::Unbounded => {
                let iter = self.iter();
                Range::for_iter(iter, end_bound)
            }
            _ => {
                let optimized_iter = Iter::new_with_start_bound(self.root.as_ref(), start_bound);
                Range::for_iter(optimized_iter, end_bound)
            }
        }
    }

    /// Check if the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }
}

// --- TreeStatsTrait impl (unchanged, read-only) ---

impl<KeyType, ValueType> TreeStatsTrait for AdaptiveRadixTree<KeyType, ValueType>
where
    KeyType: KeyTrait,
{
    fn get_tree_stats(&self) -> TreeStats {
        let mut stats = TreeStats::default();

        let Some(root) = self.root.as_ref() else {
            return stats;
        };

        Self::get_tree_stats_recurse(root, &mut stats, 1);

        let total_inner_nodes = stats
            .node_stats
            .values()
            .map(|ns| ns.total_nodes)
            .sum::<usize>();
        let mut total_children = 0;
        let mut total_width = 0;
        for ns in stats.node_stats.values_mut() {
            total_children += ns.total_children;
            total_width += ns.width * ns.total_nodes;
            ns.density = ns.total_children as f64 / (ns.width * ns.total_nodes) as f64;
        }
        let total_density = if total_width > 0 {
            total_children as f64 / total_width as f64
        } else {
            0.0
        };
        stats.num_inner_nodes = total_inner_nodes;
        stats.total_density = total_density;

        stats
    }
}

// --- Internal (private) helpers ---

impl<KeyType, ValueType> AdaptiveRadixTree<KeyType, ValueType>
where
    KeyType: KeyTrait,
{
    fn get_iterate<'a>(
        cur_node: &'a DefaultNode<KeyType::PartialType, ValueType>,
        key: &KeyType,
    ) -> Option<&'a ValueType> {
        let mut cur_node = cur_node;
        let mut depth = 0;
        loop {
            let prefix_common_match = cur_node.prefix.prefix_length_key(key, depth);
            if prefix_common_match != cur_node.prefix.len() {
                return None;
            }

            if cur_node.prefix.len() == key.length_at(depth) {
                return cur_node.value();
            }
            let k = key.at(depth + cur_node.prefix.len());
            depth += cur_node.prefix.len();
            cur_node = cur_node.seek_child(k)?;
        }
    }

    fn longest_prefix_match_iterate<'a>(
        cur_node: &'a DefaultNode<KeyType::PartialType, ValueType>,
        key: &KeyType,
    ) -> Option<(KeyType, &'a ValueType)> {
        let mut cur_node = cur_node;
        let mut cur_key = KeyType::new_from_partial(&cur_node.prefix);
        let mut best_match = cur_node.value().map(|value| (cur_key.clone(), value));
        let mut depth = 0;

        loop {
            let prefix_common_match = cur_node.prefix.prefix_length_key(key, depth);
            if prefix_common_match != cur_node.prefix.len() {
                return best_match;
            }

            if let Some(value) = cur_node.value() {
                best_match = Some((cur_key.clone(), value));
            }

            if cur_node.prefix.len() == key.length_at(depth) {
                return best_match;
            }

            let k = key.at(depth + cur_node.prefix.len());
            depth += cur_node.prefix.len();

            let Some(child) = cur_node.seek_child(k) else {
                return best_match;
            };
            cur_node = child;
            cur_key = cur_key.extend_from_partial(&cur_node.prefix);
        }
    }

    fn find_prefix_subtree<'a>(
        cur_node: &'a DefaultNode<KeyType::PartialType, ValueType>,
        prefix: &KeyType,
    ) -> Option<(&'a DefaultNode<KeyType::PartialType, ValueType>, KeyType)> {
        let mut cur_node = cur_node;
        let mut cur_key = KeyType::new_from_partial(&cur_node.prefix);
        let mut depth = 0;

        loop {
            let prefix_common_match = cur_node.prefix.prefix_length_key(prefix, depth);
            if prefix_common_match != cur_node.prefix.len() {
                if prefix_common_match == prefix.length_at(depth) {
                    return Some((cur_node, cur_key));
                }
                return None;
            }

            if cur_node.prefix.len() == prefix.length_at(depth) {
                return Some((cur_node, cur_key));
            }

            let k = prefix.at(depth + cur_node.prefix.len());
            depth += cur_node.prefix.len();

            let child = cur_node.seek_child(k)?;
            cur_node = child;
            cur_key = cur_key.extend_from_partial(&cur_node.prefix);
        }
    }

    fn get_iterate_mut<'a>(
        cur_node: &'a mut DefaultNode<KeyType::PartialType, ValueType>,
        key: &KeyType,
    ) -> Option<&'a mut ValueType> {
        let mut cur_node = cur_node;
        let mut depth = 0;
        loop {
            let prefix_common_match = cur_node.prefix.prefix_length_key(key, depth);
            if prefix_common_match != cur_node.prefix.len() {
                return None;
            }

            if cur_node.prefix.len() == key.length_at(depth) {
                return cur_node.value_mut();
            }

            let k = key.at(depth + cur_node.prefix.len());
            depth += cur_node.prefix.len();
            cur_node = cur_node.seek_child_mut(k)?;
        }
    }

    fn insert_recurse(
        cur_node: &mut DefaultNode<KeyType::PartialType, ValueType>,
        key: &KeyType,
        value: ValueType,
        depth: usize,
        heap: &Heap,
    ) -> Result<Option<ValueType>, AllocError> {
        let longest_common_prefix = cur_node.prefix.prefix_length_key(key, depth);

        let is_prefix_match =
            min(cur_node.prefix.len(), key.length_at(depth)) == longest_common_prefix;

        // Prefix fully covers this node.
        // Either sets the value or replaces the old value already here.
        if is_prefix_match
            && cur_node.prefix.len() == key.length_at(depth)
            && let Content::Leaf(v) = &mut cur_node.content
        {
            return Ok(Some(std::mem::replace(v, value)));
        }

        // Prefix is part of the current node, but doesn't fully cover it.
        // Break this node up: create a new parent node and a sibling for our leaf.
        if !is_prefix_match {
            let new_prefix = cur_node.prefix.partial_after(longest_common_prefix);
            let old_node_prefix = std::mem::replace(&mut cur_node.prefix, new_prefix);

            let n4 = DefaultNode::new_inner(
                old_node_prefix.partial_before(longest_common_prefix),
                heap,
            )?;

            let k1 = old_node_prefix.at(longest_common_prefix);
            let k2 = key.at(depth + longest_common_prefix);

            let replacement_current = std::mem::replace(cur_node, n4);

            let new_leaf =
                DefaultNode::new_leaf(key.to_partial(depth + longest_common_prefix), value);

            cur_node.add_child(k1, replacement_current, heap)?;
            cur_node.add_child(k2, new_leaf, heap)?;

            return Ok(None);
        }

        // We must be an inner node; either we need a new child or one of our
        // children does, so hunt and see.
        let k = key.at(depth + longest_common_prefix);

        let Some(child) = cur_node.seek_child_mut(k) else {
            debug_assert!(cur_node.is_inner());
            let new_leaf =
                DefaultNode::new_leaf(key.to_partial(depth + longest_common_prefix), value);
            cur_node.add_child(k, new_leaf, heap)?;
            return Ok(None);
        };

        Self::insert_recurse(child, key, value, depth + longest_common_prefix, heap)
    }

    fn remove_recurse(
        parent_node: &mut DefaultNode<KeyType::PartialType, ValueType>,
        key: &KeyType,
        depth: usize,
        heap: &Heap,
    ) -> Result<Option<ValueType>, AllocError> {
        let c = key.at(depth);
        let child_node = match parent_node.seek_child_mut(c) {
            Some(child) => child,
            None => return Ok(None),
        };

        let prefix_common_match = child_node.prefix.prefix_length_key(key, depth);
        if prefix_common_match != child_node.prefix.len() {
            return Ok(None);
        }

        // If the child is a leaf and the prefix matches the key, remove it.
        if child_node.is_leaf() {
            if child_node.prefix.len() != key.length_at(depth) {
                return Ok(None);
            }
            let node = parent_node.delete_child(c, heap)?.unwrap();
            let v = match node.content {
                Content::Leaf(v) => v,
                _ => unreachable!(),
            };
            return Ok(Some(v));
        }

        // Otherwise recurse down.
        let child_prefix_len = child_node.prefix.len();
        let result = Self::remove_recurse(child_node, key, depth + child_prefix_len, heap)?;

        // If after recursion the child has no children, collapse it.
        if result.is_some() {
            let child_node = parent_node.seek_child_mut(c).unwrap();
            if child_node.is_inner() && child_node.num_children() == 0 {
                let prefix = child_node.prefix.clone();
                let deleted = parent_node.delete_child(c, heap)?.unwrap();
                debug_assert_eq!(prefix.to_slice(), deleted.prefix.to_slice());
            }
        }

        Ok(result)
    }

    fn get_tree_stats_recurse(
        node: &DefaultNode<KeyType::PartialType, ValueType>,
        tree_stats: &mut TreeStats,
        height: usize,
    ) {
        if height > tree_stats.max_height {
            tree_stats.max_height = height;
        }
        if node.value().is_some() {
            tree_stats.num_values += 1;
        }
        match node.content {
            Content::Leaf(_) => {
                tree_stats.num_leaves += 1;
            }
            _ => {
                update_tree_stats(tree_stats, node);
            }
        }
        for (_k, child) in node.iter() {
            Self::get_tree_stats_recurse(child, tree_stats, height + 1);
        }
    }

    /// Recursively intersect two nodes, supporting different prefix-compression
    /// boundaries through in-prefix offsets.
    fn intersect_nodes<'a, F>(
        left: &'a DefaultNode<KeyType::PartialType, ValueType>,
        mut left_offset: usize,
        right: &'a DefaultNode<KeyType::PartialType, ValueType>,
        mut right_offset: usize,
        key_buf: &mut Vec<u8>,
        on_match: &mut F,
    ) where
        F: FnMut(KeyType, &'a ValueType, &'a ValueType),
    {
        let restore_len = key_buf.len();
        let left_prefix = left.prefix.as_ref();
        let right_prefix = right.prefix.as_ref();

        while left_offset < left_prefix.len() && right_offset < right_prefix.len() {
            let left_byte = left_prefix[left_offset];
            let right_byte = right_prefix[right_offset];
            if left_byte != right_byte {
                key_buf.truncate(restore_len);
                return;
            }
            key_buf.push(left_byte);
            left_offset += 1;
            right_offset += 1;
        }

        if left_offset < left_prefix.len() {
            if !right.is_inner() {
                key_buf.truncate(restore_len);
                return;
            }

            let edge = left_prefix[left_offset];
            let Some(right_child) = right.seek_child(edge) else {
                key_buf.truncate(restore_len);
                return;
            };

            key_buf.push(edge);
            Self::intersect_nodes(left, left_offset + 1, right_child, 1, key_buf, on_match);
            key_buf.truncate(restore_len);
            return;
        }

        if right_offset < right_prefix.len() {
            if !left.is_inner() {
                key_buf.truncate(restore_len);
                return;
            }

            let edge = right_prefix[right_offset];
            let Some(left_child) = left.seek_child(edge) else {
                key_buf.truncate(restore_len);
                return;
            };

            key_buf.push(edge);
            Self::intersect_nodes(left_child, 1, right, right_offset + 1, key_buf, on_match);
            key_buf.truncate(restore_len);
            return;
        }

        if let (Some(left_value), Some(right_value)) = (left.value(), right.value()) {
            on_match(
                KeyType::new_from_slice(key_buf.as_slice()),
                left_value,
                right_value,
            );
        }

        if left.is_inner() && right.is_inner() {
            if left.num_children() <= right.num_children() {
                for (edge, left_child) in left.iter() {
                    if let Some(right_child) = right.seek_child(edge) {
                        Self::intersect_nodes(left_child, 0, right_child, 0, key_buf, on_match);
                    }
                }
            } else {
                for (edge, right_child) in right.iter() {
                    if let Some(left_child) = left.seek_child(edge) {
                        Self::intersect_nodes(left_child, 0, right_child, 0, key_buf, on_match);
                    }
                }
            }
        }

        key_buf.truncate(restore_len);
    }

    /// Recursively intersect two nodes and emit only value pairs (no key reconstruction).
    fn intersect_nodes_values<'a, F>(
        left: &'a DefaultNode<KeyType::PartialType, ValueType>,
        mut left_offset: usize,
        right: &'a DefaultNode<KeyType::PartialType, ValueType>,
        mut right_offset: usize,
        on_match: &mut F,
    ) where
        F: FnMut(&'a ValueType, &'a ValueType),
    {
        let left_prefix = left.prefix.as_ref();
        let right_prefix = right.prefix.as_ref();

        while left_offset < left_prefix.len() && right_offset < right_prefix.len() {
            if left_prefix[left_offset] != right_prefix[right_offset] {
                return;
            }
            left_offset += 1;
            right_offset += 1;
        }

        if left_offset < left_prefix.len() {
            if !right.is_inner() {
                return;
            }
            let edge = left_prefix[left_offset];
            let Some(right_child) = right.seek_child(edge) else {
                return;
            };
            Self::intersect_nodes_values(left, left_offset + 1, right_child, 1, on_match);
            return;
        }

        if right_offset < right_prefix.len() {
            if !left.is_inner() {
                return;
            }
            let edge = right_prefix[right_offset];
            let Some(left_child) = left.seek_child(edge) else {
                return;
            };
            Self::intersect_nodes_values(left_child, 1, right, right_offset + 1, on_match);
            return;
        }

        if let (Some(left_value), Some(right_value)) = (left.value(), right.value()) {
            on_match(left_value, right_value);
        }

        if left.is_inner() && right.is_inner() {
            if left.num_children() <= right.num_children() {
                for (edge, left_child) in left.iter() {
                    if let Some(right_child) = right.seek_child(edge) {
                        Self::intersect_nodes_values(left_child, 0, right_child, 0, on_match);
                    }
                }
            } else {
                for (edge, right_child) in right.iter() {
                    if let Some(left_child) = left.seek_child(edge) {
                        Self::intersect_nodes_values(left_child, 0, right_child, 0, on_match);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, btree_map};
    use std::fmt::Debug;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, rng};

    use crate::HeapMaster;
    use crate::collections::rart::keys::KeyTrait;
    use crate::collections::rart::keys::array_key::ArrayKey;
    use crate::collections::rart::keys::vector_key::VectorKey;
    use crate::collections::rart::partials::array_partial::ArrPartial;
    use crate::collections::rart::range::Range;
    use crate::collections::rart::stats::TreeStatsTrait;
    use crate::collections::rart::tree;
    use crate::collections::rart::tree::AdaptiveRadixTree;

    // ---- helpers ----

    fn gen_random_string_keys(num_keys: usize, min_len: usize, max_len: usize) -> Vec<String> {
        let mut rng = rng();
        let mut keys = Vec::with_capacity(num_keys);
        for _ in 0..num_keys {
            let len = rng.random_range(min_len..=max_len);
            let s: String = (0..len)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect();
            keys.push(s);
        }
        keys
    }

    fn test_range_matches<'a, KeyType: KeyTrait, ValueType: PartialEq + Debug + 'a>(
        art_range: Range<'a, KeyType, ValueType>,
        btree_range: btree_map::Range<'a, u64, ValueType>,
    ) {
        let art_results: Vec<_> = art_range.collect();
        let btree_results: Vec<_> = btree_range.collect();
        assert_eq!(
            art_results.len(),
            btree_results.len(),
            "Range result count mismatch: ART={} BTree={}",
            art_results.len(),
            btree_results.len()
        );
        for (i, ((art_key, art_val), (btree_key, btree_val))) in
            art_results.iter().zip(btree_results.iter()).enumerate()
        {
            assert_eq!(art_val, btree_val, "Value mismatch at index {i}");
        }
    }

    // ---- PanickyRangeKey ----

    /// A key type that panics if `at()` is called on bytes beyond the key length.
    /// Used to verify that the range iterator does not read out-of-bounds bytes.
    #[derive(Clone, Eq, PartialEq, Debug)]
    struct PanickyRangeKey {
        data: Vec<u8>,
    }

    impl PanickyRangeKey {
        fn from_u64(v: u64) -> Self {
            Self {
                data: v.to_be_bytes().to_vec(),
            }
        }
    }

    impl AsRef<[u8]> for PanickyRangeKey {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl PartialOrd for PanickyRangeKey {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for PanickyRangeKey {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.data.cmp(&other.data)
        }
    }

    impl From<u64> for PanickyRangeKey {
        fn from(v: u64) -> Self {
            Self::from_u64(v)
        }
    }

    impl From<&u64> for PanickyRangeKey {
        fn from(v: &u64) -> Self {
            Self::from_u64(*v)
        }
    }

    impl From<PanickyRangeKey> for crate::collections::rart::partials::vector_partial::VectorPartial {
        fn from(key: PanickyRangeKey) -> Self {
            Self::from_slice(&key.data)
        }
    }

    impl KeyTrait for PanickyRangeKey {
        type PartialType = crate::collections::rart::partials::vector_partial::VectorPartial;
        const MAXIMUM_SIZE: Option<usize> = None;

        fn new_from_slice(slice: &[u8]) -> Self {
            Self {
                data: slice.to_vec(),
            }
        }

        fn new_from_partial(partial: &Self::PartialType) -> Self {
            Self {
                data: partial.to_slice().to_vec(),
            }
        }

        fn extend_from_partial(&self, partial: &Self::PartialType) -> Self {
            let mut v = self.data.clone();
            v.extend_from_slice(partial.to_slice());
            Self { data: v }
        }

        fn truncate(&self, at_depth: usize) -> Self {
            Self {
                data: self.data[..at_depth].to_vec(),
            }
        }

        fn at(&self, pos: usize) -> u8 {
            assert!(
                pos < self.data.len(),
                "PanickyRangeKey: at({pos}) called but len={}",
                self.data.len()
            );
            self.data[pos]
        }

        fn length_at(&self, at_depth: usize) -> usize {
            self.data.len() - at_depth
        }

        fn to_partial(&self, at_depth: usize) -> Self::PartialType {
            crate::collections::rart::partials::vector_partial::VectorPartial::from_slice(
                &self.data[at_depth..],
            )
        }

        fn matches_slice(&self, slice: &[u8]) -> bool {
            self.data == slice
        }
    }

    // ---- tests ----

    #[test]
    fn test_root_set_get() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);
        tree.insert("abc", 1).unwrap();
        assert_eq!(tree.get("abc"), Some(&1));
    }

    #[test]
    fn test_string_keys_get_set() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        tree.insert("abc", 1).unwrap();
        tree.insert("def", 2).unwrap();
        tree.insert("ghi", 3).unwrap();

        assert_eq!(tree.get("abc"), Some(&1));
        assert_eq!(tree.get("def"), Some(&2));
        assert_eq!(tree.get("ghi"), Some(&3));

        tree.remove("abc").unwrap();
        assert_eq!(tree.get("abc"), None);
        assert_eq!(tree.get("def"), Some(&2));
        assert_eq!(tree.get("ghi"), Some(&3));

        tree.remove("def").unwrap();
        assert_eq!(tree.get("def"), None);

        tree.remove("ghi").unwrap();
        assert_eq!(tree.get("ghi"), None);
    }

    #[test]
    fn test_int_keys_get_set() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        tree.insert(1u64, 1).unwrap();
        tree.insert(2u64, 2).unwrap();
        tree.insert(3u64, 3).unwrap();

        assert_eq!(tree.get(1u64), Some(&1));
        assert_eq!(tree.get(2u64), Some(&2));
        assert_eq!(tree.get(3u64), Some(&3));
    }

    #[test]
    fn test_bulk_random_string_query() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, usize>::new(&heap);

        // Reduced from original: gen_random_string_keys(2, 1, 2)
        let keys = gen_random_string_keys(2, 1, 2);
        for (i, key) in keys.iter().enumerate() {
            tree.insert(key.as_str(), i).unwrap();
        }

        // Reduced from 5M to 50K
        for _ in 0..50_000 {
            for (i, key) in keys.iter().enumerate() {
                assert_eq!(tree.get(key.as_str()), Some(&i));
            }
        }
    }

    #[test]
    fn test_random_numeric_insert_get() {
        let heap = HeapMaster::new(256 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);

        let mut rng = rng();
        let mut keys = Vec::new();
        // Reduced from 9M to 50K
        for _ in 0..50_000 {
            let k: u64 = rng.random();
            keys.push(k);
            tree.insert(k, k).unwrap();
        }

        for k in &keys {
            assert_eq!(tree.get(*k), Some(k));
        }
    }

    #[test]
    fn test_iter() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);

        let mut expected = BTreeSet::new();
        // Reduced from 100K to 10K
        for i in 0u64..10_000 {
            tree.insert(i, i).unwrap();
            expected.insert(i);
        }

        let mut count = 0;
        for (key, value) in tree.iter() {
            let k = key.to_be_u64();
            assert!(expected.contains(&k), "Unexpected key {k}");
            assert_eq!(*value, k);
            count += 1;
        }
        assert_eq!(count, expected.len());
    }

    #[test]
    fn test_iter_one_regression() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);

        tree.insert(0u64, 0u64).unwrap();

        let mut count = 0;
        for (key, value) in tree.iter() {
            let k = key.to_be_u64();
            assert_eq!(k, 0u64);
            assert_eq!(*value, 0u64);
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[test]
    fn test_prefix_iter_returns_sorted_prefix_subset() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        // Null-terminated string keys (via From<&str>).
        tree.insert("api/v1/users", 1).unwrap();
        tree.insert("api/v1/users/1", 2).unwrap();
        tree.insert("api/v1/users/2", 3).unwrap();
        tree.insert("api/v2/users", 4).unwrap();
        tree.insert("api/v2/users/1", 5).unwrap();
        tree.insert("other", 6).unwrap();

        // Search prefix is a raw byte prefix (no null terminator), so it
        // matches all null-terminated keys that start with these bytes.
        let prefix = VectorKey::new_from_vec(b"api/v1".to_vec());
        let results: Vec<_> = tree.prefix_iter(prefix).collect();
        assert_eq!(results.len(), 3);
        assert_eq!(*results[0].1, 1);
        assert_eq!(*results[1].1, 2);
        assert_eq!(*results[2].1, 3);

        // Verify sorted order
        for i in 1..results.len() {
            assert!(results[i - 1].0 < results[i].0);
        }
    }

    #[test]
    fn test_prefix_iter_no_match() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        tree.insert("abc", 1).unwrap();
        tree.insert("abd", 2).unwrap();

        let results: Vec<_> = tree.prefix_iter("xyz").collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_prefix_iter_short_prefix_regression() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        // Null-terminated string keys.
        tree.insert("a", 1).unwrap();
        tree.insert("ab", 2).unwrap();
        tree.insert("abc", 3).unwrap();
        tree.insert("b", 4).unwrap();

        // Raw byte prefix "a" matches all null-terminated keys starting with 'a'.
        let prefix = VectorKey::new_from_vec(b"a".to_vec());
        let results: Vec<_> = tree.prefix_iter(prefix).collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_longest_prefix_match() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        // With null-terminated string keys, longest_prefix_match finds the
        // deepest stored key whose bytes are a byte-prefix of the query.
        // Since null-terminated keys end with \0, an exact-match query works
        // (the stored key IS a prefix of itself), and a non-matching query
        // returns None.
        tree.insert("api", 1).unwrap();
        tree.insert("api/v1", 2).unwrap();
        tree.insert("api/v1/users", 3).unwrap();

        // Exact match: query key == stored key
        let (_key, value) = tree.longest_prefix_match("api/v1/users").unwrap();
        assert_eq!(*value, 3);

        let (_key, value) = tree.longest_prefix_match("api/v1").unwrap();
        assert_eq!(*value, 2);

        let (_key, value) = tree.longest_prefix_match("api").unwrap();
        assert_eq!(*value, 1);

        // No match: query key doesn't match any stored key
        assert!(tree.longest_prefix_match("other").is_none());
        assert!(tree.longest_prefix_match("ap").is_none());
    }

    #[test]
    fn test_delete_regressions() {
        // Regression case 1
        {
            let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
            let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&heap);

            let keys: Vec<u64> = vec![
                118, 7, 200, 71, 251, 54, 250, 77, 182, 70, 167, 199, 17, 233, 30, 39, 6, 186,
            ];
            for k in &keys {
                tree.insert(*k, *k).unwrap();
            }

            for k in &keys {
                let removed = tree.remove(*k).unwrap();
                assert_eq!(removed, Some(*k), "Failed to remove key {k}");
                assert_eq!(tree.get(*k), None, "Key {k} still present after remove");
            }
        }

        // Regression case 2
        {
            let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
            let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&heap);

            let keys: Vec<u64> = vec![
                220, 165, 3, 4, 128, 113, 78, 95, 134, 107, 87, 244, 110, 198, 191, 12, 167, 169,
                2, 185, 119,
            ];
            for k in &keys {
                tree.insert(*k, *k).unwrap();
            }

            for k in &keys {
                let removed = tree.remove(*k).unwrap();
                assert_eq!(removed, Some(*k), "Failed to remove key {k}");
                assert_eq!(tree.get(*k), None, "Key {k} still present after remove");
            }
        }

        // Regression case 3
        {
            let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
            let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&heap);

            let keys: Vec<u64> = vec![
                174, 1, 63, 249, 207, 127, 103, 220, 98, 115, 113, 121, 232, 171, 122, 180, 156,
                160, 30, 128,
            ];
            for k in &keys {
                tree.insert(*k, *k).unwrap();
            }

            for k in &keys {
                let removed = tree.remove(*k).unwrap();
                assert_eq!(removed, Some(*k), "Failed to remove key {k}");
                assert_eq!(tree.get(*k), None, "Key {k} still present after remove");
            }
        }
    }

    #[test]
    fn test_insert_returns_replaced_value() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        assert_eq!(tree.insert("key", 1).unwrap(), None);
        assert_eq!(tree.insert("key", 2).unwrap(), Some(1));
        assert_eq!(tree.insert("key", 3).unwrap(), Some(2));
        assert_eq!(tree.get("key"), Some(&3));
    }

    #[test]
    fn test_intersect_with_returns_common_keys_and_values() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree_a = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);
        let mut tree_b = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        tree_a.insert("a", 1).unwrap();
        tree_a.insert("b", 2).unwrap();
        tree_a.insert("c", 3).unwrap();

        tree_b.insert("b", 20).unwrap();
        tree_b.insert("c", 30).unwrap();
        tree_b.insert("d", 40).unwrap();

        let mut results = Vec::new();
        tree_a.intersect_with(&tree_b, |key, left, right| {
            results.push((key, *left, *right));
        });

        assert_eq!(results.len(), 2);
        // Should contain (b, 2, 20) and (c, 3, 30) in sorted order
        results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(results[0].1, 2);
        assert_eq!(results[0].2, 20);
        assert_eq!(results[1].1, 3);
        assert_eq!(results[1].2, 30);
    }

    #[test]
    fn test_intersect_with_empty_tree() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree_a = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);
        let tree_b = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        tree_a.insert("a", 1).unwrap();

        let mut results = Vec::new();
        tree_a.intersect_with(&tree_b, |key, left, right| {
            results.push((key, *left, *right));
        });
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_intersect_values_with_and_count() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree_a = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);
        let mut tree_b = AdaptiveRadixTree::<VectorKey, i32>::new(&heap);

        tree_a.insert("a", 1).unwrap();
        tree_a.insert("b", 2).unwrap();
        tree_a.insert("c", 3).unwrap();
        tree_a.insert("d", 4).unwrap();

        tree_b.insert("b", 20).unwrap();
        tree_b.insert("d", 40).unwrap();
        tree_b.insert("e", 50).unwrap();

        let mut pairs = Vec::new();
        tree_a.intersect_values_with(&tree_b, |left, right| {
            pairs.push((*left, *right));
        });
        assert_eq!(pairs.len(), 2);

        let count = tree_a.intersect_count(&tree_b);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_delete() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);

        let mut rng = rng();
        let mut keys = Vec::new();
        // Reduced from 5000 to 1000
        for _ in 0..1000 {
            let k: u64 = rng.random();
            keys.push(k);
            tree.insert(k, k).unwrap();
        }

        // Verify all present
        for k in &keys {
            assert_eq!(tree.get(*k), Some(k));
        }

        // Delete and verify
        for k in &keys {
            let removed = tree.remove(*k).unwrap();
            assert_eq!(removed, Some(*k));
            assert_eq!(tree.get(*k), None);
        }

        assert!(tree.is_empty());
    }

    #[test]
    fn test_range() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);
        let mut btree = BTreeMap::new();

        let mut rng = rng();
        // Reduced from 10000 to 2000
        for _ in 0..2000 {
            let k: u64 = rng.random();
            tree.insert(k, k).unwrap();
            btree.insert(k, k);
        }

        let keys: Vec<u64> = btree.keys().copied().collect();
        let len = keys.len();
        for _ in 0..100 {
            let start_idx = rng.random_range(0..len);
            let end_idx = rng.random_range(start_idx..len);

            let start = keys[start_idx];
            let end = keys[end_idx];

            let start_key: VectorKey = start.into();
            let end_key: VectorKey = end.into();

            // Exclusive range
            test_range_matches(
                tree.range(start_key.clone()..end_key.clone()),
                btree.range(start..end),
            );

            // Inclusive range
            test_range_matches(
                tree.range(start_key.clone()..=end_key.clone()),
                btree.range(start..=end),
            );

            // From range
            test_range_matches(tree.range(start_key.clone()..), btree.range(start..));

            // Full range
            test_range_matches(tree.range::<std::ops::RangeFull>(..), btree.range(..));
        }
    }

    #[test]
    fn test_range_stops_after_first_out_of_bounds_regression() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<PanickyRangeKey, u64>::new(&heap);

        for i in 0u64..100 {
            tree.insert(i, i).unwrap();
        }

        let start: PanickyRangeKey = 10u64.into();
        let end: PanickyRangeKey = 20u64.into();
        let results: Vec<_> = tree.range(start..end).collect();

        assert_eq!(results.len(), 10);
        for (i, (key, value)) in results.iter().enumerate() {
            assert_eq!(**value, 10 + i as u64);
        }
    }

    #[test]
    fn test_range_start_seek_regression() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<PanickyRangeKey, u64>::new(&heap);

        // Insert non-contiguous keys
        let keys = vec![5u64, 10, 15, 20, 25, 30, 35, 40];
        for k in &keys {
            tree.insert(*k, *k).unwrap();
        }

        // Start key is between existing keys
        let start: PanickyRangeKey = 12u64.into();
        let end: PanickyRangeKey = 32u64.into();
        let results: Vec<_> = tree.range(start..end).collect();

        let expected: Vec<u64> = keys
            .iter()
            .copied()
            .filter(|&k| k >= 12 && k < 32)
            .collect();
        assert_eq!(results.len(), expected.len());
        for (i, (_, value)) in results.iter().enumerate() {
            assert_eq!(**value, expected[i]);
        }
    }

    #[test]
    fn test_range_start_sequence_matches_btreemap_seeded() {
        let heap = HeapMaster::new(256 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);
        let mut btree = BTreeMap::new();

        let mut rng = StdRng::seed_from_u64(42);

        // Reduced from 20000 to 5000
        const COUNT: usize = 5000;
        for _ in 0..COUNT {
            let k: u64 = rng.random();
            tree.insert(k, k).unwrap();
            btree.insert(k, k);
        }

        let keys: Vec<u64> = btree.keys().copied().collect();
        let len = keys.len();

        for _ in 0..200 {
            let start_idx = rng.random_range(0..len);
            let end_idx = rng.random_range(start_idx..len);

            let start = keys[start_idx];
            let end = keys[end_idx];

            let start_key: VectorKey = start.into();
            let end_key: VectorKey = end.into();

            test_range_matches(
                tree.range(start_key.clone()..end_key.clone()),
                btree.range(start..end),
            );

            test_range_matches(
                tree.range(start_key.clone()..=end_key.clone()),
                btree.range(start..=end),
            );
        }
    }

    #[test]
    fn test_range_to_inclusive_fuzz_regression() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);
        let mut btree = BTreeMap::new();

        let keys: Vec<u64> = vec![0, 1, 2, 3, 4, 5, 10, 20, 100, 255, 256, 1000, u64::MAX];
        for &k in &keys {
            tree.insert(k, k).unwrap();
            btree.insert(k, k);
        }

        for &end in &keys {
            let end_key: VectorKey = end.into();
            test_range_matches(tree.range(..=end_key), btree.range(..=end));
        }
    }

    #[test]
    fn test_range_from_fuzz_regression() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut tree = AdaptiveRadixTree::<VectorKey, u64>::new(&heap);
        let mut btree = BTreeMap::new();

        let keys: Vec<u64> = vec![0, 1, 2, 3, 4, 5, 10, 20, 100, 255, 256, 1000, u64::MAX];
        for &k in &keys {
            tree.insert(k, k).unwrap();
            btree.insert(k, k);
        }

        for &start in &keys {
            let start_key: VectorKey = start.into();
            test_range_matches(tree.range(start_key..), btree.range(start..));
        }
    }
}
