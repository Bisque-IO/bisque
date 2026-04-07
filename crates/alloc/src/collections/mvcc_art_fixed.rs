//! MVCC Adaptive Radix Tree with fixed-size byte-array keys.
//!
//! This is the const-generic variant of [`super::mvcc_art`]. Keys are
//! `[u8; K]` where `K` is the key size in bytes. Supports u128 (K=16),
//! UUIDs (K=16), SHA-256 hashes (K=32), or any fixed-size key.
//!
//! The `usize`-keyed variant in [`super::mvcc_art`] is optimized for
//! integer keys and should be preferred when keys fit in a `usize`.
//!
//! # Example
//!
//! ```rust,ignore
//! use bisque_alloc::collections::mvcc_art_fixed::FixedArt;
//! use bisque_alloc::HeapMaster;
//!
//! let heap = HeapMaster::new(64 * 1024 * 1024).unwrap().heap();
//! let tree = FixedArt::<16, u64>::new(&heap);
//!
//! let key = 42u128.to_be_bytes();
//! tree.insert(&key, 100).unwrap();
//! assert_eq!(tree.get(&key), Some(100));
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use allocator_api2::alloc::AllocError;
use parking_lot::{Mutex, RwLock};

use crate::Heap;
use crate::HeapArc;

// ═══════════════════════════════════════════════════════════════════════════
// Node types (same structure as mvcc_art, but leaf stores [u8; K] key)
// ═══════════════════════════════════════════════════════════════════════════

const LEAF_TAG: usize = 1;
const NULL_CHILD: usize = 0;
const EMPTY_INDEX: u8 = 0xFF;
/// Maximum prefix bytes stored inline in the header.
const MAX_PREFIX: usize = 16;

#[inline]
fn is_leaf(p: usize) -> bool {
    p & LEAF_TAG != 0
}

#[inline]
fn tag_leaf(p: *mut u8) -> usize {
    (p as usize) | LEAF_TAG
}

#[inline]
fn leaf_ptr(p: usize) -> *mut u8 {
    (p & !LEAF_TAG) as *mut u8
}

/// Leaf layout: [key: [u8; K]][value: V]
/// Allocated as raw bytes from the heap.
#[inline]
fn leaf_key<const K: usize>(p: usize) -> &'static [u8; K] {
    unsafe { &*(leaf_ptr(p) as *const [u8; K]) }
}

#[inline]
fn leaf_value<V: Copy>(p: usize, key_size: usize) -> V {
    unsafe {
        let val_ptr = leaf_ptr(p).add(key_size) as *const V;
        std::ptr::read_unaligned(val_ptr)
    }
}

unsafe fn alloc_leaf<const K: usize, V: Copy>(heap: &Heap, key: &[u8; K], value: V) -> usize {
    let size = K + std::mem::size_of::<V>();
    let align = std::mem::align_of::<V>().max(1);
    let ptr = heap.alloc(size, align);
    if ptr.is_null() {
        return NULL_CHILD;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(key.as_ptr(), ptr, K);
        let val_ptr = ptr.add(K) as *mut V;
        std::ptr::write_unaligned(val_ptr, value);
    }
    tag_leaf(ptr)
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum NodeKind {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
}

#[repr(C)]
#[derive(Clone)]
struct Header {
    kind: NodeKind,
    num_children: u16,
    prefix_len: u32,
    prefix: [u8; MAX_PREFIX],
    generation: u64,
}

#[repr(C)]
struct Node4 {
    header: Header,
    keys: [u8; 4],
    children: [usize; 4],
}

#[repr(C)]
struct Node16 {
    header: Header,
    keys: [u8; 16],
    children: [usize; 16],
}

#[repr(C)]
struct Node48 {
    header: Header,
    child_index: [u8; 256],
    children: [usize; 48],
}

#[repr(C)]
struct Node256 {
    header: Header,
    children: [usize; 256],
}

// ═══════════════════════════════════════════════════════════════════════════
// Node ops (mirrors mvcc_art but generic over key size)
// ═══════════════════════════════════════════════════════════════════════════

#[inline]
unsafe fn node_header(p: usize) -> &'static Header {
    unsafe { &*(p as *const Header) }
}

#[inline]
unsafe fn node_header_mut(p: usize) -> &'static mut Header {
    unsafe { &mut *(p as *mut Header) }
}

unsafe fn find_child(node: usize, byte: u8) -> usize {
    let hdr = unsafe { node_header(node) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(node as *const Node4) };
            for i in 0..hdr.num_children as usize {
                if n.keys[i] == byte {
                    return n.children[i];
                }
            }
            NULL_CHILD
        }
        NodeKind::N16 => {
            let n = unsafe { &*(node as *const Node16) };
            for i in 0..hdr.num_children as usize {
                if n.keys[i] == byte {
                    return n.children[i];
                }
            }
            NULL_CHILD
        }
        NodeKind::N48 => {
            let n = unsafe { &*(node as *const Node48) };
            let idx = n.child_index[byte as usize];
            if idx == EMPTY_INDEX {
                NULL_CHILD
            } else {
                n.children[idx as usize]
            }
        }
        NodeKind::N256 => {
            let n = unsafe { &*(node as *const Node256) };
            n.children[byte as usize]
        }
    }
}

unsafe fn is_full(node: usize) -> bool {
    let hdr = unsafe { node_header(node) };
    match hdr.kind {
        NodeKind::N4 => hdr.num_children >= 4,
        NodeKind::N16 => hdr.num_children >= 16,
        NodeKind::N48 => hdr.num_children >= 48,
        NodeKind::N256 => false,
    }
}

unsafe fn add_child_mut(node: usize, byte: u8, child: usize) {
    let hdr = unsafe { node_header_mut(node) };
    let idx = hdr.num_children as usize;
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &mut *(node as *mut Node4) };
            let mut pos = idx;
            for i in 0..idx {
                if byte < n.keys[i] {
                    pos = i;
                    break;
                }
            }
            for i in (pos..idx).rev() {
                n.keys[i + 1] = n.keys[i];
                n.children[i + 1] = n.children[i];
            }
            n.keys[pos] = byte;
            n.children[pos] = child;
        }
        NodeKind::N16 => {
            let n = unsafe { &mut *(node as *mut Node16) };
            let mut pos = idx;
            for i in 0..idx {
                if byte < n.keys[i] {
                    pos = i;
                    break;
                }
            }
            for i in (pos..idx).rev() {
                n.keys[i + 1] = n.keys[i];
                n.children[i + 1] = n.children[i];
            }
            n.keys[pos] = byte;
            n.children[pos] = child;
        }
        NodeKind::N48 => {
            let n = unsafe { &mut *(node as *mut Node48) };
            n.child_index[byte as usize] = idx as u8;
            n.children[idx] = child;
        }
        NodeKind::N256 => {
            let n = unsafe { &mut *(node as *mut Node256) };
            n.children[byte as usize] = child;
        }
    }
    hdr.num_children += 1;
}

unsafe fn replace_child_mut(node: usize, byte: u8, new_child: usize) {
    let hdr = unsafe { node_header(node) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &mut *(node as *mut Node4) };
            for i in 0..hdr.num_children as usize {
                if n.keys[i] == byte {
                    n.children[i] = new_child;
                    return;
                }
            }
        }
        NodeKind::N16 => {
            let n = unsafe { &mut *(node as *mut Node16) };
            for i in 0..hdr.num_children as usize {
                if n.keys[i] == byte {
                    n.children[i] = new_child;
                    return;
                }
            }
        }
        NodeKind::N48 => {
            let n = unsafe { &mut *(node as *mut Node48) };
            let idx = n.child_index[byte as usize];
            if idx != EMPTY_INDEX {
                n.children[idx as usize] = new_child;
            }
        }
        NodeKind::N256 => {
            let n = unsafe { &mut *(node as *mut Node256) };
            n.children[byte as usize] = new_child;
        }
    }
}

unsafe fn remove_child_mut(node: usize, byte: u8) {
    let hdr = unsafe { node_header_mut(node) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &mut *(node as *mut Node4) };
            for i in 0..hdr.num_children as usize {
                if n.keys[i] == byte {
                    for j in i..hdr.num_children as usize - 1 {
                        n.keys[j] = n.keys[j + 1];
                        n.children[j] = n.children[j + 1];
                    }
                    let last = hdr.num_children as usize - 1;
                    n.keys[last] = 0;
                    n.children[last] = NULL_CHILD;
                    hdr.num_children -= 1;
                    return;
                }
            }
        }
        NodeKind::N16 => {
            let n = unsafe { &mut *(node as *mut Node16) };
            for i in 0..hdr.num_children as usize {
                if n.keys[i] == byte {
                    for j in i..hdr.num_children as usize - 1 {
                        n.keys[j] = n.keys[j + 1];
                        n.children[j] = n.children[j + 1];
                    }
                    let last = hdr.num_children as usize - 1;
                    n.keys[last] = 0;
                    n.children[last] = NULL_CHILD;
                    hdr.num_children -= 1;
                    return;
                }
            }
        }
        NodeKind::N48 => {
            let n = unsafe { &mut *(node as *mut Node48) };
            let idx = n.child_index[byte as usize];
            if idx != EMPTY_INDEX {
                n.children[idx as usize] = NULL_CHILD;
                n.child_index[byte as usize] = EMPTY_INDEX;
                hdr.num_children -= 1;
            }
        }
        NodeKind::N256 => {
            let n = unsafe { &mut *(node as *mut Node256) };
            n.children[byte as usize] = NULL_CHILD;
            hdr.num_children -= 1;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Node alloc / grow / shrink / copy
// ═══════════════════════════════════════════════════════════════════════════

macro_rules! alloc_node {
    ($name:ident, $ty:ty, $kind:expr) => {
        unsafe fn $name(heap: &Heap, generation: u64) -> *mut $ty {
            let layout = std::alloc::Layout::new::<$ty>();
            let ptr = heap.alloc(layout.size(), layout.align());
            if ptr.is_null() {
                return std::ptr::null_mut();
            }
            unsafe {
                std::ptr::write_bytes(ptr, 0, layout.size());
                let node = &mut *(ptr as *mut $ty);
                node.header.kind = $kind;
                node.header.generation = generation;
            }
            ptr as *mut $ty
        }
    };
}

alloc_node!(alloc_node4, Node4, NodeKind::N4);
alloc_node!(alloc_node16, Node16, NodeKind::N16);
alloc_node!(alloc_node48, Node48, NodeKind::N48);
alloc_node!(alloc_node256, Node256, NodeKind::N256);

// Initialize Node48's child_index to EMPTY_INDEX after alloc.
unsafe fn init_node48(n: *mut Node48) {
    if !n.is_null() {
        unsafe { (*n).child_index.fill(EMPTY_INDEX) };
    }
}

unsafe fn grow_node(old: usize, heap: &Heap, generation: u64) -> usize {
    let hdr = unsafe { node_header(old) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(old as *const Node4) };
            let new = unsafe { alloc_node16(heap, generation) };
            if new.is_null() {
                return NULL_CHILD;
            }
            let new_n = unsafe { &mut *new };
            new_n.header.prefix_len = hdr.prefix_len;
            new_n.header.prefix = hdr.prefix;
            new_n.header.num_children = hdr.num_children;
            new_n.keys[..4].copy_from_slice(&n.keys);
            new_n.children[..4].copy_from_slice(&n.children);
            new as usize
        }
        NodeKind::N16 => {
            let n = unsafe { &*(old as *const Node16) };
            let new = unsafe { alloc_node48(heap, generation) };
            if new.is_null() {
                return NULL_CHILD;
            }
            unsafe { init_node48(new) };
            let new_n = unsafe { &mut *new };
            new_n.header.prefix_len = hdr.prefix_len;
            new_n.header.prefix = hdr.prefix;
            new_n.header.num_children = hdr.num_children;
            for i in 0..hdr.num_children as usize {
                new_n.child_index[n.keys[i] as usize] = i as u8;
                new_n.children[i] = n.children[i];
            }
            new as usize
        }
        NodeKind::N48 => {
            let n = unsafe { &*(old as *const Node48) };
            let new = unsafe { alloc_node256(heap, generation) };
            if new.is_null() {
                return NULL_CHILD;
            }
            let new_n = unsafe { &mut *new };
            new_n.header.prefix_len = hdr.prefix_len;
            new_n.header.prefix = hdr.prefix;
            new_n.header.num_children = hdr.num_children;
            for byte in 0..256usize {
                let idx = n.child_index[byte];
                if idx != EMPTY_INDEX {
                    new_n.children[byte] = n.children[idx as usize];
                }
            }
            new as usize
        }
        NodeKind::N256 => unreachable!(),
    }
}

unsafe fn shrink_node(old: usize, heap: &Heap, generation: u64) -> usize {
    let hdr = unsafe { node_header(old) };
    match hdr.kind {
        NodeKind::N256 => {
            let n = unsafe { &*(old as *const Node256) };
            let new = unsafe { alloc_node48(heap, generation) };
            if new.is_null() {
                return NULL_CHILD;
            }
            unsafe { init_node48(new) };
            let new_n = unsafe { &mut *new };
            new_n.header.prefix_len = hdr.prefix_len;
            new_n.header.prefix = hdr.prefix;
            let mut slot = 0u8;
            for byte in 0..256usize {
                if n.children[byte] != NULL_CHILD {
                    new_n.child_index[byte] = slot;
                    new_n.children[slot as usize] = n.children[byte];
                    slot += 1;
                }
            }
            new_n.header.num_children = slot as u16;
            new as usize
        }
        NodeKind::N48 => {
            let n = unsafe { &*(old as *const Node48) };
            let new = unsafe { alloc_node16(heap, generation) };
            if new.is_null() {
                return NULL_CHILD;
            }
            let new_n = unsafe { &mut *new };
            new_n.header.prefix_len = hdr.prefix_len;
            new_n.header.prefix = hdr.prefix;
            let mut slot = 0;
            for byte in 0..256usize {
                if n.child_index[byte] != EMPTY_INDEX {
                    new_n.keys[slot] = byte as u8;
                    new_n.children[slot] = n.children[n.child_index[byte] as usize];
                    slot += 1;
                }
            }
            new_n.header.num_children = slot as u16;
            new as usize
        }
        NodeKind::N16 => {
            let n = unsafe { &*(old as *const Node16) };
            let new = unsafe { alloc_node4(heap, generation) };
            if new.is_null() {
                return NULL_CHILD;
            }
            let new_n = unsafe { &mut *new };
            new_n.header.prefix_len = hdr.prefix_len;
            new_n.header.prefix = hdr.prefix;
            new_n.header.num_children = hdr.num_children;
            let c = hdr.num_children as usize;
            new_n.keys[..c].copy_from_slice(&n.keys[..c]);
            new_n.children[..c].copy_from_slice(&n.children[..c]);
            new as usize
        }
        NodeKind::N4 => unreachable!(),
    }
}

unsafe fn copy_node(src: usize, heap: &Heap, generation: u64) -> usize {
    let hdr = unsafe { node_header(src) };
    let (size, align) = match hdr.kind {
        NodeKind::N4 => (std::mem::size_of::<Node4>(), std::mem::align_of::<Node4>()),
        NodeKind::N16 => (
            std::mem::size_of::<Node16>(),
            std::mem::align_of::<Node16>(),
        ),
        NodeKind::N48 => (
            std::mem::size_of::<Node48>(),
            std::mem::align_of::<Node48>(),
        ),
        NodeKind::N256 => (
            std::mem::size_of::<Node256>(),
            std::mem::align_of::<Node256>(),
        ),
    };
    let ptr = heap.alloc(size, align);
    if ptr.is_null() {
        return NULL_CHILD;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(src as *const u8, ptr, size);
        (*(ptr as *mut Header)).generation = generation;
    }
    ptr as usize
}

unsafe fn dealloc_node(heap: &Heap, p: usize) {
    if p == NULL_CHILD {
        return;
    }
    if is_leaf(p) {
        unsafe { heap.dealloc(leaf_ptr(p)) };
    } else {
        unsafe { heap.dealloc(p as *mut u8) };
    }
}

unsafe fn free_subtree(heap: &Heap, p: usize) {
    if p == NULL_CHILD {
        return;
    }
    if is_leaf(p) {
        unsafe { dealloc_node(heap, p) };
        return;
    }
    let hdr = unsafe { node_header(p) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(p as *const Node4) };
            for i in 0..hdr.num_children as usize {
                unsafe { free_subtree(heap, n.children[i]) };
            }
        }
        NodeKind::N16 => {
            let n = unsafe { &*(p as *const Node16) };
            for i in 0..hdr.num_children as usize {
                unsafe { free_subtree(heap, n.children[i]) };
            }
        }
        NodeKind::N48 => {
            let n = unsafe { &*(p as *const Node48) };
            for byte in 0..256usize {
                if n.child_index[byte] != EMPTY_INDEX {
                    unsafe { free_subtree(heap, n.children[n.child_index[byte] as usize]) };
                }
            }
        }
        NodeKind::N256 => {
            let n = unsafe { &*(p as *const Node256) };
            for byte in 0..256usize {
                if n.children[byte] != NULL_CHILD {
                    unsafe { free_subtree(heap, n.children[byte]) };
                }
            }
        }
    }
    unsafe { dealloc_node(heap, p) };
}

fn should_shrink(kind: NodeKind, count: u16) -> bool {
    match kind {
        NodeKind::N256 => count <= 37,
        NodeKind::N48 => count <= 12,
        NodeKind::N16 => count <= 3,
        NodeKind::N4 => false,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Lookup
// ═══════════════════════════════════════════════════════════════════════════

fn lookup<const K: usize, V: Copy>(root: usize, key: &[u8; K]) -> Option<V> {
    let mut node = root;
    let mut depth: usize = 0;

    while node != NULL_CHILD {
        if is_leaf(node) {
            let lk = leaf_key::<K>(node);
            return if lk == key {
                Some(leaf_value::<V>(node, K))
            } else {
                None
            };
        }

        let hdr = unsafe { node_header(node) };
        let plen = hdr.prefix_len as usize;
        if plen > 0 {
            let check = plen.min(MAX_PREFIX);
            for i in 0..check {
                if depth + i >= K || hdr.prefix[i] != key[depth + i] {
                    return None;
                }
            }
            depth += plen;
        }

        if depth >= K {
            return None;
        }

        node = unsafe { find_child(node, key[depth]) };
        depth += 1;
    }

    None
}

// ═══════════════════════════════════════════════════════════════════════════
// COW insert / remove
// ═══════════════════════════════════════════════════════════════════════════

unsafe fn ensure_mutable(
    node: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut Vec<usize>,
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
    garbage.push(node);
    Ok(new)
}

unsafe fn cow_insert<const K: usize, V: Copy>(
    root: usize,
    key: &[u8; K],
    value: V,
    generation: u64,
    heap: &Heap,
    garbage: &mut Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    if root == NULL_CHILD {
        let leaf = unsafe { alloc_leaf(heap, key, value) };
        if leaf == NULL_CHILD {
            return Err(AllocError);
        }
        return Ok((leaf, None));
    }

    if is_leaf(root) {
        let old_key = leaf_key::<K>(root);
        if old_key == key {
            let old_val = leaf_value::<V>(root, K);
            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            garbage.push(root);
            return Ok((new_leaf, Some(old_val)));
        }
        let mut common = 0;
        while common < K && old_key[common] == key[common] {
            common += 1;
        }
        let new_leaf = unsafe { alloc_leaf(heap, key, value) };
        if new_leaf == NULL_CHILD {
            return Err(AllocError);
        }
        let new_node = unsafe { alloc_node4(heap, generation) };
        if new_node.is_null() {
            garbage.push(new_leaf);
            return Err(AllocError);
        }
        let n4 = unsafe { &mut *new_node };
        let plen = common.min(MAX_PREFIX);
        n4.header.prefix_len = common as u32;
        n4.header.prefix[..plen].copy_from_slice(&key[..plen]);
        if common < K {
            unsafe {
                add_child_mut(new_node as usize, old_key[common], root);
                add_child_mut(new_node as usize, key[common], new_leaf);
            }
        }
        return Ok((new_node as usize, None));
    }

    unsafe { cow_insert_inner::<K, V>(root, key, value, 0, generation, heap, garbage) }
}

unsafe fn cow_insert_inner<const K: usize, V: Copy>(
    node: usize,
    key: &[u8; K],
    value: V,
    depth: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    let node = unsafe { ensure_mutable(node, generation, heap, garbage)? };
    let hdr = unsafe { node_header(node) };

    let plen = hdr.prefix_len as usize;
    if plen > 0 {
        let check = plen.min(MAX_PREFIX);
        let mut mismatch = plen;
        for i in 0..check {
            if depth + i >= K || hdr.prefix[i] != key[depth + i] {
                mismatch = i;
                break;
            }
        }
        if mismatch < plen {
            let new_parent = unsafe { alloc_node4(heap, generation) };
            if new_parent.is_null() {
                return Err(AllocError);
            }
            let np = unsafe { &mut *new_parent };
            let mp = mismatch.min(MAX_PREFIX);
            np.header.prefix_len = mismatch as u32;
            np.header.prefix[..mp].copy_from_slice(&hdr.prefix[..mp]);

            let hdr_mut = unsafe { node_header_mut(node) };
            let old_byte = hdr_mut.prefix[mismatch];
            let remaining = plen - mismatch - 1;
            for i in 0..remaining.min(MAX_PREFIX) {
                hdr_mut.prefix[i] = hdr_mut.prefix[mismatch + 1 + i];
            }
            hdr_mut.prefix_len = remaining as u32;

            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            unsafe {
                add_child_mut(new_parent as usize, old_byte, node);
                add_child_mut(new_parent as usize, key[depth + mismatch], new_leaf);
            }
            return Ok((new_parent as usize, None));
        }
    }

    let child_depth = depth + plen;
    if child_depth >= K {
        return Ok((node, None));
    }

    let byte = key[child_depth];
    let child = unsafe { find_child(node, byte) };

    if child == NULL_CHILD {
        let new_leaf = unsafe { alloc_leaf(heap, key, value) };
        if new_leaf == NULL_CHILD {
            return Err(AllocError);
        }
        if unsafe { is_full(node) } {
            let grown = unsafe { grow_node(node, heap, generation) };
            if grown == NULL_CHILD {
                garbage.push(new_leaf);
                return Err(AllocError);
            }
            garbage.push(node);
            unsafe { add_child_mut(grown, byte, new_leaf) };
            return Ok((grown, None));
        }
        unsafe { add_child_mut(node, byte, new_leaf) };
        return Ok((node, None));
    }

    if is_leaf(child) {
        let old_key = leaf_key::<K>(child);
        if old_key == key {
            let old_val = leaf_value::<V>(child, K);
            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            unsafe { replace_child_mut(node, byte, new_leaf) };
            garbage.push(child);
            return Ok((node, Some(old_val)));
        }
        let next_depth = child_depth + 1;
        let mut common = 0;
        while next_depth + common < K && old_key[next_depth + common] == key[next_depth + common] {
            common += 1;
        }
        let new_node = unsafe { alloc_node4(heap, generation) };
        if new_node.is_null() {
            return Err(AllocError);
        }
        let n4 = unsafe { &mut *new_node };
        let cp = common.min(MAX_PREFIX);
        n4.header.prefix_len = common as u32;
        n4.header.prefix[..cp].copy_from_slice(&key[next_depth..next_depth + cp]);
        let split_depth = next_depth + common;
        if split_depth < K {
            let new_leaf = unsafe { alloc_leaf(heap, key, value) };
            if new_leaf == NULL_CHILD {
                return Err(AllocError);
            }
            unsafe {
                add_child_mut(new_node as usize, old_key[split_depth], child);
                add_child_mut(new_node as usize, key[split_depth], new_leaf);
            }
        }
        unsafe { replace_child_mut(node, byte, new_node as usize) };
        return Ok((node, None));
    }

    let (new_child, old_val) = unsafe {
        cow_insert_inner::<K, V>(
            child,
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

unsafe fn cow_remove<const K: usize, V: Copy>(
    root: usize,
    key: &[u8; K],
    generation: u64,
    heap: &Heap,
    garbage: &mut Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    if root == NULL_CHILD {
        return Ok((root, None));
    }
    if is_leaf(root) {
        if leaf_key::<K>(root) == key {
            let val = leaf_value::<V>(root, K);
            garbage.push(root);
            return Ok((NULL_CHILD, Some(val)));
        }
        return Ok((root, None));
    }
    unsafe { cow_remove_inner::<K, V>(root, key, 0, generation, heap, garbage) }
}

unsafe fn cow_remove_inner<const K: usize, V: Copy>(
    node: usize,
    key: &[u8; K],
    depth: usize,
    generation: u64,
    heap: &Heap,
    garbage: &mut Vec<usize>,
) -> Result<(usize, Option<V>), AllocError> {
    let node = unsafe { ensure_mutable(node, generation, heap, garbage)? };
    let hdr = unsafe { node_header(node) };

    let plen = hdr.prefix_len as usize;
    let check = plen.min(MAX_PREFIX);
    for i in 0..check {
        if depth + i >= K || hdr.prefix[i] != key[depth + i] {
            return Ok((node, None));
        }
    }

    let child_depth = depth + plen;
    if child_depth >= K {
        return Ok((node, None));
    }

    let byte = key[child_depth];
    let child = unsafe { find_child(node, byte) };

    if child == NULL_CHILD {
        return Ok((node, None));
    }

    if is_leaf(child) {
        if leaf_key::<K>(child) != key {
            return Ok((node, None));
        }
        let val = leaf_value::<V>(child, K);
        garbage.push(child);
        unsafe { remove_child_mut(node, byte) };

        let new_count = unsafe { node_header(node) }.num_children;
        if new_count == 1 && hdr.kind == NodeKind::N4 {
            let n4 = unsafe { &*(node as *const Node4) };
            let only_child = n4.children[0];
            if is_leaf(only_child) {
                garbage.push(node);
                return Ok((only_child, Some(val)));
            }
            let child_node = unsafe { ensure_mutable(only_child, generation, heap, garbage)? };
            let child_hdr = unsafe { node_header_mut(child_node) };
            let mut new_prefix = [0u8; MAX_PREFIX];
            let np_len = plen + 1 + child_hdr.prefix_len as usize;
            let copy_len = np_len.min(MAX_PREFIX);
            new_prefix[..plen.min(MAX_PREFIX)].copy_from_slice(&hdr.prefix[..plen.min(MAX_PREFIX)]);
            if plen < MAX_PREFIX {
                new_prefix[plen] = n4.keys[0];
            }
            let cp = child_hdr.prefix_len as usize;
            if plen + 1 < MAX_PREFIX && cp > 0 {
                let dst_start = plen + 1;
                let copy = cp.min(MAX_PREFIX - dst_start);
                new_prefix[dst_start..dst_start + copy].copy_from_slice(&child_hdr.prefix[..copy]);
            }
            child_hdr.prefix_len = np_len as u32;
            child_hdr.prefix[..copy_len].copy_from_slice(&new_prefix[..copy_len]);
            garbage.push(node);
            return Ok((child_node, Some(val)));
        }

        if should_shrink(hdr.kind, new_count) {
            let shrunk = unsafe { shrink_node(node, heap, generation) };
            if shrunk != NULL_CHILD {
                garbage.push(node);
                return Ok((shrunk, Some(val)));
            }
        }
        return Ok((node, Some(val)));
    }

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

// ═══════════════════════════════════════════════════════════════════════════
// MVCC snapshot (same pattern as mvcc_art)
// ═══════════════════════════════════════════════════════════════════════════

struct SnapshotInner {
    root: usize,
    garbage: Mutex<Vec<usize>>,
    heap: Heap,
}

impl Drop for SnapshotInner {
    fn drop(&mut self) {
        for &p in self.garbage.get_mut().iter() {
            unsafe { dealloc_node(&self.heap, p) };
        }
    }
}

unsafe impl Send for SnapshotInner {}
unsafe impl Sync for SnapshotInner {}

// ═══════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════

/// A concurrent MVCC ART with fixed-size `[u8; K]` keys.
///
/// `K` is the key size in bytes. `V` must be `Copy`.
/// For `usize` keys, prefer [`super::mvcc_art::Art`] which is optimized
/// for integer keys.
pub struct FixedArt<const K: usize, V: Copy> {
    current: RwLock<HeapArc<SnapshotInner>>,
    heap: Heap,
    generation: AtomicU64,
    _marker: std::marker::PhantomData<V>,
}

/// A read-only snapshot of a `FixedArt`.
pub struct Snapshot<const K: usize, V: Copy> {
    inner: HeapArc<SnapshotInner>,
    _marker: std::marker::PhantomData<V>,
}

/// A write transaction for a `FixedArt`.
pub struct WriteTxn<'a, const K: usize, V: Copy> {
    art: &'a FixedArt<K, V>,
    root: usize,
    generation: u64,
    garbage: Vec<usize>,
}

impl<const K: usize, V: Copy> FixedArt<K, V> {
    pub fn new(heap: &Heap) -> Self {
        let snap = HeapArc::new(
            SnapshotInner {
                root: NULL_CHILD,
                garbage: Mutex::new(Vec::new()),
                heap: heap.clone(),
            },
            heap,
        )
        .expect("failed to allocate initial snapshot");
        Self {
            current: RwLock::new(snap),
            heap: heap.clone(),
            generation: AtomicU64::new(1),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    #[inline]
    pub fn snapshot(&self) -> Snapshot<K, V> {
        let arc = self.current.read().clone();
        Snapshot {
            inner: arc,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn get(&self, key: &[u8; K]) -> Option<V> {
        let guard = self.current.read();
        lookup::<K, V>(guard.root, key)
    }

    pub fn write(&self) -> WriteTxn<'_, K, V> {
        let guard = self.current.read();
        let root = guard.root;
        let next_gen = self.generation.fetch_add(1, Ordering::Relaxed);
        WriteTxn {
            art: self,
            root,
            generation: next_gen,
            garbage: Vec::new(),
        }
    }

    pub fn insert(&self, key: &[u8; K], val: V) -> Result<Option<V>, AllocError> {
        let mut txn = self.write();
        let old = txn.insert(key, val)?;
        txn.commit()?;
        Ok(old)
    }

    pub fn remove(&self, key: &[u8; K]) -> Result<Option<V>, AllocError> {
        let mut txn = self.write();
        let old = txn.remove(key)?;
        txn.commit()?;
        Ok(old)
    }

    pub fn is_empty(&self) -> bool {
        self.current.read().root == NULL_CHILD
    }
}

impl<const K: usize, V: Copy> Snapshot<K, V> {
    #[inline]
    pub fn get(&self, key: &[u8; K]) -> Option<V> {
        lookup::<K, V>(self.inner.root, key)
    }
}

impl<'a, const K: usize, V: Copy> WriteTxn<'a, K, V> {
    pub fn insert(&mut self, key: &[u8; K], val: V) -> Result<Option<V>, AllocError> {
        let (new_root, old) = unsafe {
            cow_insert::<K, V>(
                self.root,
                key,
                val,
                self.generation,
                &self.art.heap,
                &mut self.garbage,
            )?
        };
        self.root = new_root;
        Ok(old)
    }

    pub fn remove(&mut self, key: &[u8; K]) -> Result<Option<V>, AllocError> {
        let (new_root, old) = unsafe {
            cow_remove::<K, V>(
                self.root,
                key,
                self.generation,
                &self.art.heap,
                &mut self.garbage,
            )?
        };
        self.root = new_root;
        Ok(old)
    }

    #[inline]
    pub fn get(&self, key: &[u8; K]) -> Option<V> {
        lookup::<K, V>(self.root, key)
    }

    pub fn commit(mut self) -> Result<(), AllocError> {
        let garbage = std::mem::take(&mut self.garbage);
        let new_snap = HeapArc::new(
            SnapshotInner {
                root: self.root,
                garbage: Mutex::new(Vec::new()),
                heap: self.art.heap.clone(),
            },
            &self.art.heap,
        )?;
        let mut current = self.art.current.write();
        let old = std::mem::replace(&mut *current, new_snap);
        drop(current);
        old.garbage.lock().extend(garbage);
        drop(old);
        self.root = NULL_CHILD;
        Ok(())
    }

    pub fn abort(mut self) {
        self.garbage.clear();
    }
}

impl<const K: usize, V: Copy> Drop for WriteTxn<'_, K, V> {
    fn drop(&mut self) {}
}

impl<const K: usize, V: Copy> Drop for FixedArt<K, V> {
    fn drop(&mut self) {
        let snap = self.current.get_mut();
        let inner = HeapArc::get_mut(snap).expect("FixedArt dropped with outstanding snapshots");
        let root = inner.root;
        inner.root = NULL_CHILD;
        unsafe { free_subtree(&self.heap, root) };
    }
}

unsafe impl<const K: usize, V: Copy + Send> Send for FixedArt<K, V> {}
unsafe impl<const K: usize, V: Copy + Send> Sync for FixedArt<K, V> {}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    fn make_heap() -> Heap {
        HeapMaster::new(256 * 1024 * 1024).unwrap().heap()
    }

    #[test]
    fn u128_basic() {
        let h = make_heap();
        let t = FixedArt::<16, u64>::new(&h);
        let k1 = 42u128.to_be_bytes();
        let k2 = 99u128.to_be_bytes();

        assert_eq!(t.insert(&k1, 100).unwrap(), None);
        assert_eq!(t.get(&k1), Some(100));
        assert_eq!(t.get(&k2), None);
    }

    #[test]
    fn u128_replace() {
        let h = make_heap();
        let t = FixedArt::<16, u64>::new(&h);
        let k = 1u128.to_be_bytes();
        assert_eq!(t.insert(&k, 10).unwrap(), None);
        assert_eq!(t.insert(&k, 20).unwrap(), Some(10));
        assert_eq!(t.get(&k), Some(20));
    }

    #[test]
    fn u128_remove() {
        let h = make_heap();
        let t = FixedArt::<16, u64>::new(&h);
        let k = 1u128.to_be_bytes();
        t.insert(&k, 10).unwrap();
        assert_eq!(t.remove(&k).unwrap(), Some(10));
        assert_eq!(t.get(&k), None);
    }

    #[test]
    fn u128_many() {
        let h = make_heap();
        let t = FixedArt::<16, u64>::new(&h);
        for i in 0..5_000u128 {
            let k = i.to_be_bytes();
            t.insert(&k, i as u64).unwrap();
        }
        for i in 0..5_000u128 {
            let k = i.to_be_bytes();
            assert_eq!(t.get(&k), Some(i as u64), "missing {i}");
        }
    }

    #[test]
    fn u128_transaction() {
        let h = make_heap();
        let t = FixedArt::<16, u64>::new(&h);
        {
            let mut txn = t.write();
            for i in 0..100u128 {
                txn.insert(&i.to_be_bytes(), i as u64).unwrap();
            }
            assert!(t.is_empty());
            txn.commit().unwrap();
        }
        for i in 0..100u128 {
            assert_eq!(t.get(&i.to_be_bytes()), Some(i as u64));
        }
    }

    #[test]
    fn u128_snapshot_isolation() {
        let h = make_heap();
        let t = FixedArt::<16, u64>::new(&h);
        let k1 = 1u128.to_be_bytes();
        let k2 = 2u128.to_be_bytes();
        t.insert(&k1, 10).unwrap();

        let snap = t.snapshot();
        t.insert(&k1, 20).unwrap();
        t.insert(&k2, 30).unwrap();

        assert_eq!(snap.get(&k1), Some(10));
        assert_eq!(snap.get(&k2), None);
        assert_eq!(t.get(&k1), Some(20));
        assert_eq!(t.get(&k2), Some(30));
    }

    #[test]
    fn sha256_key() {
        // 32-byte keys (like SHA-256 hashes).
        let h = make_heap();
        let t = FixedArt::<32, u64>::new(&h);
        let mut k1 = [0u8; 32];
        k1[31] = 1;
        let mut k2 = [0u8; 32];
        k2[31] = 2;

        t.insert(&k1, 100).unwrap();
        t.insert(&k2, 200).unwrap();
        assert_eq!(t.get(&k1), Some(100));
        assert_eq!(t.get(&k2), Some(200));
    }
}
