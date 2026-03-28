//! Copy-on-write insert and remove operations for fixed-key-size ART.

use allocator_api2::alloc::AllocError;

use crate::Heap;

use super::ArtKey;
use super::node::*;

/// Maximum garbage entries a single insert or remove can produce.
/// = K::LEN (ensure_mutable per level) + 1 (leaf) + 2 (collapse) + 1 (shrink).
/// Pre-reserving this before mutation guarantees all pushes succeed.
pub(crate) const fn max_garbage_per_op<K: ArtKey>() -> usize {
    K::LEN + 4
}

/// Ensure a node is mutable (owned by this generation). If not, copy it.
/// Pushes the old node to garbage BEFORE returning the new one.
/// On OOM (garbage push fails), frees the copy and returns Err.
///
/// Note: this only applies to inner nodes (not leaves), so no V parameter needed.
pub(crate) unsafe fn ensure_mutable<K: ArtKey>(
    node: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<usize, AllocError> {
    if node == NULL_CHILD {
        return Ok(NULL_CHILD);
    }
    let hdr = unsafe { node_header(node) };
    if hdr.generation == generation {
        return Ok(node);
    }
    let new = unsafe { copy_node(node, heap, generation) };
    if new == NULL_CHILD {
        return Err(AllocError);
    }
    // Push old node to garbage BEFORE using the new one.
    // If push fails, free the copy and return error — tree unchanged.
    if garbage.push(node).is_err() {
        unsafe { dealloc_inner_node(heap, new) };
        return Err(AllocError);
    }
    Ok(new)
}

/// Insert key/value into a tree rooted at `root`. Uses copy-on-write.
/// Returns (new_root, old_value).
#[allow(dead_code)]
pub(crate) unsafe fn cow_insert<K: ArtKey, V>(
    root: usize,
    key: &K,
    value: V,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    if root == NULL_CHILD {
        let leaf = unsafe { alloc_leaf::<K, V>(heap, *key, value) };
        if leaf == NULL_CHILD {
            return Err(AllocError);
        }
        return Ok((leaf, None));
    }

    if is_leaf(root) {
        let old_leaf = unsafe { &*leaf_ptr::<K, V>(root) };
        if old_leaf.key == *key {
            let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            // Read old value out BEFORE putting leaf in garbage.
            let old_val = unsafe { std::ptr::read(&old_leaf.value) };
            // Track old root BEFORE returning the replacement.
            if garbage.push(root).is_err() {
                // Put old_val back and dealloc the new leaf.
                unsafe { std::ptr::write(&mut (*leaf_ptr::<K, V>(root)).value, old_val) };
                unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                return Err(AllocError);
            }
            return Ok((new_leaf, Some(old_val)));
        }
        let old_key = &old_leaf.key;
        let mut common = 0;
        while common < K::LEN && old_key.byte_at(common) == key.byte_at(common) {
            common += 1;
        }
        let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, value) };
        if new_leaf == NULL_CHILD {
            return Err(AllocError);
        }
        let new_node = unsafe { alloc_node4(heap, generation) };
        if new_node.is_null() {
            unsafe { dealloc_node::<K, V>(heap, new_leaf) };
            return Err(AllocError);
        }
        let n4 = unsafe { &mut *new_node };
        let stored_prefix = common.min(MAX_PREFIX_LEN);
        n4.header.prefix_len = common as u32;
        for i in 0..stored_prefix {
            n4.header.prefix[i] = key.byte_at(i);
        }
        if common < K::LEN {
            unsafe {
                add_child_mut(new_node as usize, old_key.byte_at(common), root);
                add_child_mut(new_node as usize, key.byte_at(common), new_leaf);
            }
        }
        return Ok((new_node as usize, None));
    }

    unsafe { cow_insert_inner::<K, V>(root, key, value, 0, generation, heap, garbage) }
}

/// Recursive inner insert with COW.
pub(crate) unsafe fn cow_insert_inner<K: ArtKey, V>(
    node: usize,
    key: &K,
    value: V,
    depth: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    let node = unsafe { ensure_mutable::<K>(node, generation, heap, garbage)? };
    let hdr = unsafe { node_header(node) };

    let plen = hdr.prefix_len as usize;
    if plen > 0 {
        // Pessimistic prefix matching: only compare stored bytes (up to MAX_PREFIX_LEN).
        let check_len = plen.min(MAX_PREFIX_LEN);
        let mut mismatch = plen;
        for i in 0..check_len {
            if depth + i >= K::LEN || hdr.prefix[i] != key.byte_at(depth + i) {
                mismatch = i;
                break;
            }
        }
        if mismatch < plen {
            // Allocate everything BEFORE mutating the node's prefix.
            let new_parent = unsafe { alloc_node4(heap, generation) };
            if new_parent.is_null() {
                return Err(AllocError);
            }
            let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, value) };
            if new_leaf == NULL_CHILD {
                unsafe { dealloc_inner_node(heap, new_parent as usize) };
                return Err(AllocError);
            }

            // All allocations succeeded — now safe to mutate.
            let np = unsafe { &mut *new_parent };
            let stored_mismatch = mismatch.min(MAX_PREFIX_LEN);
            np.header.prefix_len = mismatch as u32;
            np.header.prefix[..stored_mismatch]
                .copy_from_slice(&hdr.prefix[..stored_mismatch]);

            let hdr_mut = unsafe { node_header_mut(node) };
            let old_byte = if mismatch < MAX_PREFIX_LEN {
                hdr_mut.prefix[mismatch]
            } else {
                // Mismatch beyond stored prefix — use key byte from depth.
                key.byte_at(depth + mismatch)
            };
            let remaining = plen - mismatch - 1;
            let stored_remaining = remaining.min(MAX_PREFIX_LEN);
            if mismatch + 1 < MAX_PREFIX_LEN {
                let src_start = mismatch + 1;
                let copyable = MAX_PREFIX_LEN.saturating_sub(src_start).min(stored_remaining);
                for i in 0..copyable {
                    hdr_mut.prefix[i] = hdr_mut.prefix[src_start + i];
                }
            }
            hdr_mut.prefix_len = remaining as u32;

            unsafe {
                add_child_mut(new_parent as usize, old_byte, node);
                add_child_mut(new_parent as usize, key.byte_at(depth + mismatch), new_leaf);
            }
            return Ok((new_parent as usize, None));
        }
    }

    let child_depth = depth + plen;
    if child_depth >= K::LEN {
        return Ok((node, None));
    }

    let byte = key.byte_at(child_depth);
    let child = unsafe { find_child(node, byte) };

    if child == NULL_CHILD {
        let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, value) };
        if new_leaf == NULL_CHILD {
            return Err(AllocError);
        }
        if unsafe { is_full(node) } {
            let grown = unsafe { grow_node(node, heap, generation) };
            if grown == NULL_CHILD {
                unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                return Err(AllocError);
            }
            // Track old node BEFORE exposing the grown version.
            if garbage.push(node).is_err() {
                unsafe { dealloc_inner_node(heap, grown) };
                unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                return Err(AllocError);
            }
            unsafe { add_child_mut(grown, byte, new_leaf) };
            return Ok((grown, None));
        }
        unsafe { add_child_mut(node, byte, new_leaf) };
        return Ok((node, None));
    }

    if is_leaf(child) {
        let old_leaf = unsafe { &*leaf_ptr::<K, V>(child) };
        if old_leaf.key == *key {
            let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            // Read old value out BEFORE putting leaf in garbage.
            let old_val = unsafe { std::ptr::read(&old_leaf.value) };
            // Track old leaf BEFORE replacing it.
            if garbage.push(child).is_err() {
                // Put old_val back and dealloc the new leaf.
                unsafe { std::ptr::write(&mut (*leaf_ptr::<K, V>(child)).value, old_val) };
                unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                return Err(AllocError);
            }
            unsafe { replace_child_mut(node, byte, new_leaf) };
            return Ok((node, Some(old_val)));
        }
        let old_key = &old_leaf.key;
        let next_depth = child_depth + 1;
        let mut common = 0;
        while next_depth + common < K::LEN
            && old_key.byte_at(next_depth + common) == key.byte_at(next_depth + common)
        {
            common += 1;
        }
        let new_node = unsafe { alloc_node4(heap, generation) };
        if new_node.is_null() {
            return Err(AllocError);
        }
        let n4 = unsafe { &mut *new_node };
        let stored_common = common.min(MAX_PREFIX_LEN);
        n4.header.prefix_len = common as u32;
        for i in 0..stored_common {
            n4.header.prefix[i] = key.byte_at(next_depth + i);
        }

        let split_depth = next_depth + common;
        if split_depth < K::LEN {
            let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, value) };
            if new_leaf == NULL_CHILD {
                unsafe { dealloc_inner_node(heap, new_node as usize) };
                return Err(AllocError);
            }
            unsafe {
                add_child_mut(new_node as usize, old_key.byte_at(split_depth), child);
                add_child_mut(new_node as usize, key.byte_at(split_depth), new_leaf);
            }
        }
        unsafe { replace_child_mut(node, byte, new_node as usize) };
        return Ok((node, None));
    }

    let (new_child, old_val) = unsafe {
        cow_insert_inner::<K, V>(child, key, value, child_depth + 1, generation, heap, garbage)?
    };
    if new_child != child {
        unsafe { replace_child_mut(node, byte, new_child) };
    }
    Ok((node, old_val))
}

// ═══════════════════════════════════════════════════════════════════════════
// COW remove
// ═══════════════════════════════════════════════════════════════════════════

pub(crate) fn should_shrink(kind: NodeKind, count: u16) -> bool {
    match kind {
        NodeKind::N256 => count <= 37,
        NodeKind::N48 => count <= 12,
        NodeKind::N16 => count <= 3,
        NodeKind::N4 => false,
    }
}

/// Remove key from tree. Returns (new_root, removed_value).
#[allow(dead_code)]
pub(crate) unsafe fn cow_remove<K: ArtKey, V>(
    root: usize,
    key: &K,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    if root == NULL_CHILD {
        return Ok((root, None));
    }

    if is_leaf(root) {
        let leaf = unsafe { &*leaf_ptr::<K, V>(root) };
        if leaf.key == *key {
            // Read value out BEFORE putting leaf in garbage.
            let val = unsafe { std::ptr::read(&leaf.value) };
            // Track before returning NULL root.
            if garbage.push(root).is_err() {
                // Put value back.
                unsafe { std::ptr::write(&mut (*leaf_ptr::<K, V>(root)).value, val) };
                return Err(AllocError);
            }
            return Ok((NULL_CHILD, Some(val)));
        }
        return Ok((root, None));
    }

    unsafe { cow_remove_inner::<K, V>(root, key, 0, generation, heap, garbage) }
}

pub(crate) unsafe fn cow_remove_inner<K: ArtKey, V>(
    node: usize,
    key: &K,
    depth: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    let node = unsafe { ensure_mutable::<K>(node, generation, heap, garbage)? };
    let hdr = unsafe { node_header(node) };

    let plen = hdr.prefix_len as usize;
    // Pessimistic prefix matching: only compare stored bytes.
    let check_len = plen.min(MAX_PREFIX_LEN);
    for i in 0..check_len {
        if depth + i >= K::LEN || hdr.prefix[i] != key.byte_at(depth + i) {
            return Ok((node, None));
        }
    }

    let child_depth = depth + plen;
    if child_depth >= K::LEN {
        return Ok((node, None));
    }

    let byte = key.byte_at(child_depth);
    let child = unsafe { find_child(node, byte) };

    if child == NULL_CHILD {
        return Ok((node, None));
    }

    if is_leaf(child) {
        let leaf = unsafe { &*leaf_ptr::<K, V>(child) };
        if leaf.key != *key {
            return Ok((node, None));
        }
        // Read value out BEFORE putting leaf in garbage.
        let val = unsafe { std::ptr::read(&leaf.value) };

        // Track child BEFORE removing it.
        if garbage.push(child).is_err() {
            // Put value back.
            unsafe { std::ptr::write(&mut (*leaf_ptr::<K, V>(child)).value, val) };
            return Err(AllocError);
        }
        unsafe { remove_child_mut(node, byte) };

        let new_count = unsafe { node_header(node) }.num_children;

        // Collapse Node4 with 1 child.
        if new_count == 1 && hdr.kind == NodeKind::N4 {
            let n4 = unsafe { &*(node as *const Node4) };
            let only_child = n4.children[0];
            if is_leaf(only_child) {
                // Safe: capacity pre-reserved by caller.
                garbage.push(node).expect("garbage pre-reserved");
                return Ok((only_child, Some(val)));
            }
            let child_node =
                unsafe { ensure_mutable::<K>(only_child, generation, heap, garbage)? };
            let child_hdr = unsafe { node_header_mut(child_node) };
            let mut new_prefix = [0u8; MAX_PREFIX_LEN];
            let np_len = plen + 1 + child_hdr.prefix_len as usize;
            let stored_plen = plen.min(MAX_PREFIX_LEN);
            new_prefix[..stored_plen].copy_from_slice(&hdr.prefix[..stored_plen]);
            if plen < MAX_PREFIX_LEN {
                new_prefix[plen] = n4.keys[0];
                let cp = child_hdr.prefix_len as usize;
                let cp_stored = cp.min(MAX_PREFIX_LEN);
                let avail = MAX_PREFIX_LEN - plen - 1;
                let copy_len = cp_stored.min(avail);
                if copy_len > 0 {
                    new_prefix[plen + 1..plen + 1 + copy_len]
                        .copy_from_slice(&child_hdr.prefix[..copy_len]);
                }
            }
            let stored_np = np_len.min(MAX_PREFIX_LEN);
            child_hdr.prefix_len = np_len as u32;
            child_hdr.prefix[..stored_np].copy_from_slice(&new_prefix[..stored_np]);
            // Safe: capacity pre-reserved by caller.
            garbage.push(node).expect("garbage pre-reserved");
            return Ok((child_node, Some(val)));
        }

        // Maybe shrink.
        if should_shrink(hdr.kind, new_count) {
            let shrunk = unsafe { shrink_node(node, heap, generation) };
            if shrunk != NULL_CHILD {
                // Safe: capacity pre-reserved by caller.
                garbage.push(node).expect("garbage pre-reserved");
                return Ok((shrunk, Some(val)));
            }
        }

        return Ok((node, Some(val)));
    }

    // Recurse.
    let (new_child, old_val) = unsafe {
        cow_remove_inner::<K, V>(child, key, child_depth + 1, generation, heap, garbage)?
    };
    if new_child != child {
        if new_child == NULL_CHILD {
            unsafe { remove_child_mut(node, byte) };
        } else {
            unsafe { replace_child_mut(node, byte, new_child) };
        }
    }
    Ok((node, old_val))
}
