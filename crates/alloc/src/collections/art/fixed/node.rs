//! Node types and operations for the fixed-key-size Adaptive Radix Tree.
//!
//! This is a variant of `crate::collections::art::node` where the key type
//! implements the `ArtKey` trait instead of being fixed to `size_of::<usize>()`.
//! `LeafNode<K, V>` stores the key inline as `K` and the value as `V`.
//! Inner nodes (Node4, Node16, Node48, Node256) are unchanged -- they only
//! store single-byte partial keys and child pointers.

use std::sync::atomic::{AtomicU64, Ordering};

use super::ArtKey;
use crate::Heap;

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

/// Maximum number of prefix bytes stored inline in a node header.
pub(crate) const MAX_PREFIX_LEN: usize = 8;

/// Tag bit for leaf pointers. Since heap-allocated nodes are aligned,
/// the low bit is always 0 for node pointers. We set it to 1 for leaves.
pub(crate) const LEAF_TAG: usize = 1;

/// Null child sentinel.
pub(crate) const NULL_CHILD: usize = 0;

/// Empty index slot for Node48.
pub(crate) const EMPTY_INDEX: u8 = 0xFF;

// ═══════════════════════════════════════════════════════════════════════════
// Leaf and inner node helpers
// ═══════════════════════════════════════════════════════════════════════════

#[inline]
pub(crate) fn is_leaf(p: usize) -> bool {
    p & LEAF_TAG != 0
}

#[inline]
pub(crate) fn leaf_ptr<K: ArtKey, V>(p: usize) -> *mut LeafNode<K, V> {
    (p & !LEAF_TAG) as *mut LeafNode<K, V>
}

#[inline]
pub(crate) fn tag_leaf<K: ArtKey, V>(p: *mut LeafNode<K, V>) -> usize {
    (p as usize) | LEAF_TAG
}

#[repr(C)]
pub(crate) struct LeafNode<K: ArtKey, V> {
    pub(crate) key: K,
    pub(crate) value: V,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum NodeKind {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
}

/// Common header for all inner node types.
///
/// Layout: version_lock is first for cache-line alignment with the
/// hot fields (kind, num_children, prefix) that readers access.
#[repr(C)]
pub(crate) struct Header {
    /// OLC version lock: `[62b version | 1b locked | 1b obsolete]`.
    ///
    /// - **Readers** (optimistic): read version, traverse, re-read version.
    ///   If changed -> retry. No write to shared state on the read path.
    /// - **Writers**: CAS version to set the lock bit, mutate, bump version
    ///   and clear lock bit on drop.
    /// - Published snapshot nodes are frozen -- only working tree nodes are locked.
    pub(crate) version_lock: AtomicU64,
    pub(crate) kind: NodeKind,
    pub(crate) num_children: u16,
    pub(crate) prefix_len: u32,
    pub(crate) prefix: [u8; MAX_PREFIX_LEN],
    /// Generation of the writer that created this node. Nodes with the
    /// same generation as the current batch can be mutated in place
    /// (they were allocated in this batch, not yet published).
    pub(crate) generation: u64,
}

// --- OLC lock constants (kept for future cursor API) ---

#[allow(dead_code)]
pub(crate) const OLC_LOCK_BIT: u64 = 0b10;
#[allow(dead_code)]
pub(crate) const OLC_OBSOLETE_BIT: u64 = 0b01;

#[inline]
#[allow(dead_code)]
pub(crate) fn olc_is_locked(v: u64) -> bool {
    v & OLC_LOCK_BIT != 0
}

#[inline]
#[allow(dead_code)]
pub(crate) fn olc_is_obsolete(v: u64) -> bool {
    v & OLC_OBSOLETE_BIT != 0
}

/// Error returned when an OLC version check fails -- caller must retry.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct OlcRetry;

/// Optimistic read guard. Holds a snapshot of the version at acquisition.
/// The caller reads node fields, then calls `check()` to validate.
#[allow(dead_code)]
pub(crate) struct NodeReadGuard {
    node: usize,
    version: u64,
}

#[allow(dead_code)]
impl NodeReadGuard {
    /// Try to acquire an optimistic read on a node.
    /// Returns `Err(OlcRetry)` if the node is locked or obsolete.
    #[inline]
    pub(crate) fn try_read(node: usize) -> Result<Self, OlcRetry> {
        debug_assert!(!is_leaf(node) && node != NULL_CHILD);
        let hdr = unsafe { &*(node as *const Header) };
        let v = hdr.version_lock.load(Ordering::Acquire);
        if olc_is_locked(v) || olc_is_obsolete(v) {
            return Err(OlcRetry);
        }
        Ok(NodeReadGuard { node, version: v })
    }

    /// Validate that the node hasn't been modified since we read it.
    #[inline]
    pub(crate) fn check(&self) -> Result<(), OlcRetry> {
        // SeqCst fence ensures all prior plain reads of node fields
        // are ordered before the version re-read.
        std::sync::atomic::fence(Ordering::SeqCst);
        let hdr = unsafe { &*(self.node as *const Header) };
        let v = hdr.version_lock.load(Ordering::Acquire);
        if v == self.version {
            Ok(())
        } else {
            Err(OlcRetry)
        }
    }

    /// Try to upgrade to a write lock via CAS.
    #[inline]
    pub(crate) fn try_upgrade(self) -> Result<NodeWriteGuard, OlcRetry> {
        let hdr = unsafe { &*(self.node as *const Header) };
        let locked = self.version | OLC_LOCK_BIT;
        match hdr.version_lock.compare_exchange_weak(
            self.version,
            locked,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(NodeWriteGuard { node: self.node }),
            Err(_) => Err(OlcRetry),
        }
    }

    /// Access the header (caller must call `check()` after reading).
    #[inline]
    pub(crate) fn header(&self) -> &Header {
        unsafe { &*(self.node as *const Header) }
    }
}

/// Exclusive write guard. Acquired via `try_upgrade` from a read guard.
/// Releases the lock (bumps version, clears lock bit) on drop.
#[allow(dead_code)]
pub(crate) struct NodeWriteGuard {
    node: usize,
}

#[allow(dead_code)]
impl NodeWriteGuard {
    /// Try to acquire a write lock directly (without read guard).
    #[inline]
    pub(crate) fn try_lock(node: usize) -> Result<Self, OlcRetry> {
        debug_assert!(!is_leaf(node) && node != NULL_CHILD);
        let hdr = unsafe { &*(node as *const Header) };
        let v = hdr.version_lock.load(Ordering::Acquire);
        if olc_is_locked(v) || olc_is_obsolete(v) {
            return Err(OlcRetry);
        }
        let locked = v | OLC_LOCK_BIT;
        match hdr
            .version_lock
            .compare_exchange_weak(v, locked, Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => Ok(NodeWriteGuard { node }),
            Err(_) => Err(OlcRetry),
        }
    }

    /// Mark this node as obsolete (it's being replaced).
    /// The version bump on drop will include the obsolete bit.
    #[inline]
    pub(crate) fn mark_obsolete(&self) {
        let hdr = unsafe { &*(self.node as *const Header) };
        hdr.version_lock
            .fetch_or(OLC_OBSOLETE_BIT, Ordering::Release);
    }

    /// Access the header.
    #[inline]
    pub(crate) fn header(&self) -> &Header {
        unsafe { &*(self.node as *const Header) }
    }

    /// Access the header mutably.
    #[inline]
    pub(crate) fn header_mut(&self) -> &mut Header {
        unsafe { &mut *(self.node as *mut Header) }
    }

    /// The underlying node pointer.
    #[inline]
    pub(crate) fn node(&self) -> usize {
        self.node
    }
}

impl Drop for NodeWriteGuard {
    #[inline]
    fn drop(&mut self) {
        let hdr = unsafe { &*(self.node as *const Header) };
        // Bump version by 2 (preserves obsolete bit, clears lock bit).
        // version was: [ver | 1(lock) | obs]
        // add 2:       [ver+1 | 0(lock) | obs]
        hdr.version_lock.fetch_add(OLC_LOCK_BIT, Ordering::Release);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Node types
// ═══════════════════════════════════════════════════════════════════════════

// Node4: up to 4 children, sorted keys.
#[repr(C)]
pub(crate) struct Node4 {
    pub(crate) header: Header,
    pub(crate) keys: [u8; 4],
    pub(crate) children: [usize; 4], // tagged: leaf or node pointer
}

// Node16: up to 16 children, sorted keys.
#[repr(C)]
pub(crate) struct Node16 {
    pub(crate) header: Header,
    pub(crate) keys: [u8; 16],
    pub(crate) children: [usize; 16],
}

// Node48: up to 48 children, indexed by key byte.
#[repr(C)]
pub(crate) struct Node48 {
    pub(crate) header: Header,
    pub(crate) child_index: [u8; 256], // key byte -> slot index, EMPTY_INDEX if absent
    pub(crate) children: [usize; 48],
}

// Node256: up to 256 children, direct indexed.
#[repr(C)]
pub(crate) struct Node256 {
    pub(crate) header: Header,
    pub(crate) children: [usize; 256],
}

// ═══════════════════════════════════════════════════════════════════════════
// Node allocation / deallocation
// ═══════════════════════════════════════════════════════════════════════════

pub(crate) unsafe fn alloc_leaf<K: ArtKey, V>(
    heap: &Heap,
    key: K,
    value: V,
) -> usize {
    let layout = std::alloc::Layout::new::<LeafNode<K, V>>();
    let ptr = heap.alloc(layout.size(), layout.align());
    if ptr.is_null() {
        return NULL_CHILD;
    }
    unsafe {
        let leaf = ptr as *mut LeafNode<K, V>;
        std::ptr::addr_of_mut!((*leaf).key).write(key);
        std::ptr::addr_of_mut!((*leaf).value).write(value);
    }
    tag_leaf(ptr as *mut LeafNode<K, V>)
}

pub(crate) unsafe fn alloc_node4(heap: &Heap, generation: u64) -> *mut Node4 {
    let layout = std::alloc::Layout::new::<Node4>();
    let ptr = heap.alloc(layout.size(), layout.align());
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        (ptr as *mut Node4).write(Node4 {
            header: Header {
                version_lock: AtomicU64::new(0),
                kind: NodeKind::N4,
                num_children: 0,
                prefix_len: 0,
                prefix: [0; MAX_PREFIX_LEN],
                generation,
            },
            keys: [0; 4],
            children: [NULL_CHILD; 4],
        });
    }
    ptr as *mut Node4
}

pub(crate) unsafe fn alloc_node16(heap: &Heap, generation: u64) -> *mut Node16 {
    let layout = std::alloc::Layout::new::<Node16>();
    let ptr = heap.alloc(layout.size(), layout.align());
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        (ptr as *mut Node16).write(Node16 {
            header: Header {
                version_lock: AtomicU64::new(0),
                kind: NodeKind::N16,
                num_children: 0,
                prefix_len: 0,
                prefix: [0; MAX_PREFIX_LEN],
                generation,
            },
            keys: [0; 16],
            children: [NULL_CHILD; 16],
        });
    }
    ptr as *mut Node16
}

pub(crate) unsafe fn alloc_node48(heap: &Heap, generation: u64) -> *mut Node48 {
    let layout = std::alloc::Layout::new::<Node48>();
    let ptr = heap.alloc(layout.size(), layout.align());
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        (ptr as *mut Node48).write(Node48 {
            header: Header {
                version_lock: AtomicU64::new(0),
                kind: NodeKind::N48,
                num_children: 0,
                prefix_len: 0,
                prefix: [0; MAX_PREFIX_LEN],
                generation,
            },
            child_index: [EMPTY_INDEX; 256],
            children: [NULL_CHILD; 48],
        });
    }
    ptr as *mut Node48
}

pub(crate) unsafe fn alloc_node256(heap: &Heap, generation: u64) -> *mut Node256 {
    let layout = std::alloc::Layout::new::<Node256>();
    let ptr = heap.alloc(layout.size(), layout.align());
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        (ptr as *mut Node256).write(Node256 {
            header: Header {
                version_lock: AtomicU64::new(0),
                kind: NodeKind::N256,
                num_children: 0,
                prefix_len: 0,
                prefix: [0; MAX_PREFIX_LEN],
                generation,
            },
            children: [NULL_CHILD; 256],
        });
    }
    ptr as *mut Node256
}

/// Get the header of a (non-leaf) node pointer.
#[inline]
pub(crate) unsafe fn node_header(p: usize) -> &'static Header {
    debug_assert!(!is_leaf(p) && p != NULL_CHILD);
    unsafe { &*(p as *const Header) }
}

#[inline]
pub(crate) unsafe fn node_header_mut(p: usize) -> &'static mut Header {
    debug_assert!(!is_leaf(p) && p != NULL_CHILD);
    unsafe { &mut *(p as *mut Header) }
}

// ═══════════════════════════════════════════════════════════════════════════
// Node operations: find child, add child, remove child
// ═══════════════════════════════════════════════════════════════════════════

/// Find the child pointer for a given key byte in a node.
pub(crate) unsafe fn find_child(node: usize, byte: u8) -> usize {
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

/// Return the child pointer at position `pos` in a node, along with its key byte.
/// Used for iteration over node children.
#[allow(dead_code)]
pub(crate) unsafe fn child_at(node: usize, pos: usize) -> Option<(u8, usize)> {
    let hdr = unsafe { node_header(node) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(node as *const Node4) };
            if pos < hdr.num_children as usize {
                Some((n.keys[pos], n.children[pos]))
            } else {
                None
            }
        }
        NodeKind::N16 => {
            let n = unsafe { &*(node as *const Node16) };
            if pos < hdr.num_children as usize {
                Some((n.keys[pos], n.children[pos]))
            } else {
                None
            }
        }
        NodeKind::N48 => {
            let n = unsafe { &*(node as *const Node48) };
            let mut count = 0usize;
            for byte in 0..256usize {
                let idx = n.child_index[byte];
                if idx != EMPTY_INDEX {
                    if count == pos {
                        return Some((byte as u8, n.children[idx as usize]));
                    }
                    count += 1;
                }
            }
            None
        }
        NodeKind::N256 => {
            let n = unsafe { &*(node as *const Node256) };
            let mut count = 0usize;
            for byte in 0..256usize {
                if n.children[byte] != NULL_CHILD {
                    if count == pos {
                        return Some((byte as u8, n.children[byte]));
                    }
                    count += 1;
                }
            }
            None
        }
    }
}

/// Check if a node is full (needs to grow before adding a child).
pub(crate) unsafe fn is_full(node: usize) -> bool {
    let hdr = unsafe { node_header(node) };
    match hdr.kind {
        NodeKind::N4 => hdr.num_children >= 4,
        NodeKind::N16 => hdr.num_children >= 16,
        NodeKind::N48 => hdr.num_children >= 48,
        NodeKind::N256 => false, // N256 is never full
    }
}

/// Add a child to a node (must not be full). Mutates in place.
pub(crate) unsafe fn add_child_mut(node: usize, byte: u8, child: usize) {
    let hdr = unsafe { node_header_mut(node) };
    let idx = hdr.num_children as usize;
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &mut *(node as *mut Node4) };
            // Insert sorted.
            let mut pos = idx;
            for i in 0..idx {
                if byte < n.keys[i] {
                    pos = i;
                    break;
                }
            }
            // Shift right.
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

/// Replace a child pointer in a node. Mutates in place.
pub(crate) unsafe fn replace_child_mut(node: usize, byte: u8, new_child: usize) {
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

/// Remove a child from a node. Mutates in place.
pub(crate) unsafe fn remove_child_mut(node: usize, byte: u8) {
    let hdr = unsafe { node_header_mut(node) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &mut *(node as *mut Node4) };
            for i in 0..hdr.num_children as usize {
                if n.keys[i] == byte {
                    // Shift left.
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

/// Grow a node to the next size. Returns the new node pointer.
/// The old node is NOT freed (caller adds to garbage).
pub(crate) unsafe fn grow_node(old: usize, heap: &Heap, generation: u64) -> usize {
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
        NodeKind::N256 => unreachable!("N256 cannot grow"),
    }
}

/// Shrink a node to a smaller size. Returns the new node pointer.
pub(crate) unsafe fn shrink_node(old: usize, heap: &Heap, generation: u64) -> usize {
    let hdr = unsafe { node_header(old) };
    match hdr.kind {
        NodeKind::N256 => {
            let n = unsafe { &*(old as *const Node256) };
            let new = unsafe { alloc_node48(heap, generation) };
            if new.is_null() {
                return NULL_CHILD;
            }
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
                let idx = n.child_index[byte];
                if idx != EMPTY_INDEX {
                    new_n.keys[slot] = byte as u8;
                    new_n.children[slot] = n.children[idx as usize];
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
            new_n.keys[..hdr.num_children as usize]
                .copy_from_slice(&n.keys[..hdr.num_children as usize]);
            new_n.children[..hdr.num_children as usize]
                .copy_from_slice(&n.children[..hdr.num_children as usize]);
            new as usize
        }
        NodeKind::N4 => unreachable!("N4 cannot shrink"),
    }
}

/// Copy a node, setting the new generation and resetting the OLC lock.
pub(crate) unsafe fn copy_node(src: usize, heap: &Heap, generation: u64) -> usize {
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
        let new_hdr = &mut *(ptr as *mut Header);
        new_hdr.generation = generation;
        // Reset the OLC lock -- new node starts unlocked, not obsolete.
        new_hdr.version_lock = AtomicU64::new(0);
    }
    ptr as usize
}

/// Deallocate an inner node (NOT a leaf). Use for inner nodes only.
pub(crate) unsafe fn dealloc_inner_node(heap: &Heap, p: usize) {
    if p != NULL_CHILD {
        unsafe { heap.dealloc(p as *mut u8) };
    }
}

/// Deallocate a leaf node whose value has ALREADY been moved out (or is Copy).
/// This just frees the memory without dropping V.
///
/// # Safety
/// The value V must have already been moved out via `ptr::read`, or V must not
/// need dropping (e.g. Copy types).
pub(crate) unsafe fn dealloc_leaf_raw<K: ArtKey, V>(heap: &Heap, tagged: usize) {
    debug_assert!(is_leaf(tagged));
    unsafe { heap.dealloc(leaf_ptr::<K, V>(tagged) as *mut u8) };
}

/// Deallocate a node (inner or leaf). For leaves, drops V in place before freeing.
pub(crate) unsafe fn dealloc_node<K: ArtKey, V>(heap: &Heap, p: usize) {
    if p == NULL_CHILD {
        return;
    }
    if is_leaf(p) {
        let leaf = leaf_ptr::<K, V>(p);
        unsafe {
            std::ptr::drop_in_place(leaf);
            heap.dealloc(leaf as *mut u8);
        }
    } else {
        unsafe { heap.dealloc(p as *mut u8) };
    }
}

/// Recursively free an entire subtree, dropping all V values.
pub(crate) unsafe fn free_subtree<K: ArtKey, V>(heap: &Heap, p: usize) {
    if p == NULL_CHILD {
        return;
    }
    if is_leaf(p) {
        unsafe { dealloc_node::<K, V>(heap, p) };
        return;
    }
    // Free children first, then the node itself.
    let hdr = unsafe { node_header(p) };
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(p as *const Node4) };
            for i in 0..hdr.num_children as usize {
                unsafe { free_subtree::<K, V>(heap, n.children[i]) };
            }
        }
        NodeKind::N16 => {
            let n = unsafe { &*(p as *const Node16) };
            for i in 0..hdr.num_children as usize {
                unsafe { free_subtree::<K, V>(heap, n.children[i]) };
            }
        }
        NodeKind::N48 => {
            let n = unsafe { &*(p as *const Node48) };
            for byte in 0..256usize {
                let idx = n.child_index[byte];
                if idx != EMPTY_INDEX {
                    unsafe { free_subtree::<K, V>(heap, n.children[idx as usize]) };
                }
            }
        }
        NodeKind::N256 => {
            let n = unsafe { &*(p as *const Node256) };
            for byte in 0..256usize {
                if n.children[byte] != NULL_CHILD {
                    unsafe { free_subtree::<K, V>(heap, n.children[byte]) };
                }
            }
        }
    }
    unsafe { dealloc_inner_node(heap, p) };
}
