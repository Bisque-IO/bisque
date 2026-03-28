//! Copy-on-write insert and remove operations.

use allocator_api2::alloc::AllocError;

use crate::Heap;

use super::node::*;

/// Maximum garbage entries a single insert or remove can produce.
/// = KEY_BYTES (ensure_mutable per level) + 1 (leaf) + 2 (collapse) + 1 (shrink).
/// Pre-reserving this before mutation guarantees all pushes succeed.
pub(super) const MAX_GARBAGE_PER_OP: usize = KEY_BYTES + 4;

/// Ensure a node is mutable (owned by this generation). If not, copy it.
/// Pushes the old node to garbage BEFORE returning the new one.
/// On OOM (garbage push fails), frees the copy and returns Err.
pub(super) unsafe fn ensure_mutable(
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
        unsafe { dealloc_node(heap, new) };
        return Err(AllocError);
    }
    Ok(new)
}

/// Insert key/value into a tree rooted at `root`. Uses copy-on-write.
/// Returns (new_root, old_value).
#[allow(dead_code)]
pub(super) unsafe fn cow_insert(
    root: usize,
    key: usize,
    value: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<usize>), AllocError> {
    let key_arr = key_bytes_array(key);

    if root == NULL_CHILD {
        let leaf = unsafe { alloc_leaf(heap, key, value) };
        if leaf == NULL_CHILD {
            return Err(AllocError);
        }
        return Ok((leaf, None));
    }

    if is_leaf(root) {
        let old_leaf = unsafe { &*leaf_ptr(root) };
        if old_leaf.key == key {
            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            // Track old root BEFORE returning the replacement.
            if garbage.push(root).is_err() {
                unsafe { dealloc_node(heap, new_leaf) };
                return Err(AllocError);
            }
            return Ok((new_leaf, Some(old_leaf.value)));
        }
        let old_key_arr = key_bytes_array(old_leaf.key);
        let mut common = 0;
        while common < KEY_BYTES && old_key_arr[common] == key_arr[common] {
            common += 1;
        }
        let new_leaf = unsafe { alloc_leaf(heap, key, value) };
        if new_leaf == NULL_CHILD {
            return Err(AllocError);
        }
        let new_node = unsafe { alloc_node4(heap, generation) };
        if new_node.is_null() {
            unsafe { dealloc_node(heap, new_leaf) };
            return Err(AllocError);
        }
        let n4 = unsafe { &mut *new_node };
        n4.header.prefix_len = common as u32;
        n4.header.prefix[..common].copy_from_slice(&key_arr[..common]);
        unsafe {
            add_child_mut(new_node as usize, old_key_arr[common], root);
            add_child_mut(new_node as usize, key_arr[common], new_leaf);
        }
        return Ok((new_node as usize, None));
    }

    unsafe { cow_insert_inner(root, &key_arr, key, value, 0, generation, heap, garbage) }
}

/// Recursive inner insert with COW.
pub(super) unsafe fn cow_insert_inner(
    node: usize,
    key_arr: &[u8; KEY_BYTES],
    key: usize,
    value: usize,
    depth: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<usize>), AllocError> {
    let node = unsafe { ensure_mutable(node, generation, heap, garbage)? };
    let hdr = unsafe { node_header(node) };

    let plen = hdr.prefix_len as usize;
    if plen > 0 {
        let mut mismatch = plen;
        for i in 0..plen {
            if depth + i >= KEY_BYTES || hdr.prefix[i] != key_arr[depth + i] {
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
            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                unsafe { dealloc_node(heap, new_parent as usize) };
                return Err(AllocError);
            }

            // All allocations succeeded — now safe to mutate.
            let np = unsafe { &mut *new_parent };
            np.header.prefix_len = mismatch as u32;
            np.header.prefix[..mismatch].copy_from_slice(&hdr.prefix[..mismatch]);

            let hdr_mut = unsafe { node_header_mut(node) };
            let old_byte = hdr_mut.prefix[mismatch];
            let remaining = plen - mismatch - 1;
            for i in 0..remaining {
                hdr_mut.prefix[i] = hdr_mut.prefix[mismatch + 1 + i];
            }
            hdr_mut.prefix_len = remaining as u32;

            unsafe {
                add_child_mut(new_parent as usize, old_byte, node);
                add_child_mut(new_parent as usize, key_arr[depth + mismatch], new_leaf);
            }
            return Ok((new_parent as usize, None));
        }
    }

    let child_depth = depth + plen;
    if child_depth >= KEY_BYTES {
        return Ok((node, None));
    }

    let byte = key_arr[child_depth];
    let child = unsafe { find_child(node, byte) };

    if child == NULL_CHILD {
        let new_leaf = unsafe { alloc_leaf(heap, key, value) };
        if new_leaf == NULL_CHILD {
            return Err(AllocError);
        }
        if unsafe { is_full(node) } {
            let grown = unsafe { grow_node(node, heap, generation) };
            if grown == NULL_CHILD {
                unsafe { dealloc_node(heap, new_leaf) };
                return Err(AllocError);
            }
            // Track old node BEFORE exposing the grown version.
            if garbage.push(node).is_err() {
                unsafe { dealloc_node(heap, grown) };
                unsafe { dealloc_node(heap, new_leaf) };
                return Err(AllocError);
            }
            unsafe { add_child_mut(grown, byte, new_leaf) };
            return Ok((grown, None));
        }
        unsafe { add_child_mut(node, byte, new_leaf) };
        return Ok((node, None));
    }

    if is_leaf(child) {
        let old_leaf = unsafe { &*leaf_ptr(child) };
        if old_leaf.key == key {
            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            // Track old leaf BEFORE replacing it.
            if garbage.push(child).is_err() {
                unsafe { dealloc_node(heap, new_leaf) };
                return Err(AllocError);
            }
            unsafe { replace_child_mut(node, byte, new_leaf) };
            return Ok((node, Some(old_leaf.value)));
        }
        let old_key_arr = key_bytes_array(old_leaf.key);
        let next_depth = child_depth + 1;
        let mut common = 0;
        while next_depth + common < KEY_BYTES
            && old_key_arr[next_depth + common] == key_arr[next_depth + common]
        {
            common += 1;
        }
        let new_node = unsafe { alloc_node4(heap, generation) };
        if new_node.is_null() {
            return Err(AllocError);
        }
        let n4 = unsafe { &mut *new_node };
        n4.header.prefix_len = common as u32;
        n4.header.prefix[..common].copy_from_slice(&key_arr[next_depth..next_depth + common]);

        let split_depth = next_depth + common;
        if split_depth < KEY_BYTES {
            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                unsafe { dealloc_node(heap, new_node as usize) };
                return Err(AllocError);
            }
            unsafe {
                add_child_mut(new_node as usize, old_key_arr[split_depth], child);
                add_child_mut(new_node as usize, key_arr[split_depth], new_leaf);
            }
        }
        unsafe { replace_child_mut(node, byte, new_node as usize) };
        return Ok((node, None));
    }

    let (new_child, old_val) = unsafe {
        cow_insert_inner(
            child,
            key_arr,
            key,
            value,
            child_depth + 1,
            generation,
            heap,
            garbage,
        )?
    };
    if new_child != child {
        unsafe { replace_child_mut(node, byte, new_child) };
    }
    Ok((node, old_val))
}

// ═══════════════════════════════════════════════════════════════════════════
// COW remove
// ═══════════════════════════════════════════════════════════════════════════

pub(super) fn should_shrink(kind: NodeKind, count: u16) -> bool {
    match kind {
        NodeKind::N256 => count <= 37,
        NodeKind::N48 => count <= 12,
        NodeKind::N16 => count <= 3,
        NodeKind::N4 => false,
    }
}

/// Remove key from tree. Returns (new_root, removed_value).
#[allow(dead_code)]
pub(super) unsafe fn cow_remove(
    root: usize,
    key: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<usize>), AllocError> {
    if root == NULL_CHILD {
        return Ok((root, None));
    }

    if is_leaf(root) {
        let leaf = unsafe { &*leaf_ptr(root) };
        if leaf.key == key {
            // Track before returning NULL root.
            if garbage.push(root).is_err() {
                return Err(AllocError);
            }
            return Ok((NULL_CHILD, Some(leaf.value)));
        }
        return Ok((root, None));
    }

    let key_arr = key_bytes_array(key);
    unsafe { cow_remove_inner(root, &key_arr, key, 0, generation, heap, garbage) }
}

pub(super) unsafe fn cow_remove_inner(
    node: usize,
    key_arr: &[u8; KEY_BYTES],
    key: usize,
    depth: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut crate::Vec<usize>,
) -> Result<(usize, Option<usize>), AllocError> {
    let node = unsafe { ensure_mutable(node, generation, heap, garbage)? };
    let hdr = unsafe { node_header(node) };

    let plen = hdr.prefix_len as usize;
    for i in 0..plen {
        if depth + i >= KEY_BYTES || hdr.prefix[i] != key_arr[depth + i] {
            return Ok((node, None));
        }
    }

    let child_depth = depth + plen;
    if child_depth >= KEY_BYTES {
        return Ok((node, None));
    }

    let byte = key_arr[child_depth];
    let child = unsafe { find_child(node, byte) };

    if child == NULL_CHILD {
        return Ok((node, None));
    }

    if is_leaf(child) {
        let leaf = unsafe { &*leaf_ptr(child) };
        if leaf.key != key {
            return Ok((node, None));
        }
        let val = leaf.value;

        // Track child BEFORE removing it.
        if garbage.push(child).is_err() {
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
            let child_node = unsafe { ensure_mutable(only_child, generation, heap, garbage)? };
            let child_hdr = unsafe { node_header_mut(child_node) };
            let mut new_prefix = [0u8; KEY_BYTES];
            let np_len = plen + 1 + child_hdr.prefix_len as usize;
            new_prefix[..plen].copy_from_slice(&hdr.prefix[..plen]);
            new_prefix[plen] = n4.keys[0];
            let cp = child_hdr.prefix_len as usize;
            if plen + 1 + cp <= KEY_BYTES {
                new_prefix[plen + 1..plen + 1 + cp].copy_from_slice(&child_hdr.prefix[..cp]);
            }
            child_hdr.prefix_len = np_len.min(KEY_BYTES) as u32;
            child_hdr.prefix[..np_len.min(KEY_BYTES)]
                .copy_from_slice(&new_prefix[..np_len.min(KEY_BYTES)]);
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
        cow_remove_inner(
            child,
            key_arr,
            key,
            child_depth + 1,
            generation,
            heap,
            garbage,
        )?
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
