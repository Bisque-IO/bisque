//! Lookup, cursor-based iteration, and range operations over the fixed-key ART.
//!
//! This is the `const K: usize` variant of the ART iterator/cursor module.
//! All key types are `[u8; K]` instead of `usize`, and `MAX_DEPTH = K + 2`.
//!
//! The [`Cursor`] navigates directly over frozen node memory — zero copies,
//! zero heap allocations. Each stack frame is 10 bytes (node pointer + byte
//! position). Total cursor size scales with K.

use super::node::*;

/// Max tree depth. Fixed at 66 — supports keys up to 64 bytes.
/// Uses const instead of K+2 to avoid nightly generic_const_exprs.
const MAX_DEPTH: usize = 66;

// ═══════════════════════════════════════════════════════════════════════════
// Lookup
// ═══════════════════════════════════════════════════════════════════════════

/// Lookup a key, returning a raw pointer to the value inside the leaf.
pub(super) fn lookup<const K: usize, V>(root: usize, key: &[u8; K]) -> Option<*const V> {
    lookup_inner::<K, V>(root, key, 0)
}

/// Lookup with a value-equality function. Fully by-value comparison.
#[inline]
pub(super) fn lookup_with<const K: usize, V>(
    root: usize,
    key: [u8; K],
    eq: fn([u8; K], [u8; K]) -> bool,
) -> Option<*const V> {
    lookup_inner_eq::<K, V>(root, &key, key, 0, eq)
}

pub(super) fn lookup_from<const K: usize, V>(
    root: usize,
    _key_arr: &[u8; K],
    key: &[u8; K],
    start_depth: usize,
) -> Option<*const V> {
    lookup_inner::<K, V>(root, key, start_depth)
}

/// Core lookup — default byte comparison.
#[inline(always)]
fn lookup_inner<const K: usize, V>(
    root: usize,
    key: &[u8; K],
    start_depth: usize,
) -> Option<*const V> {
    let mut node = root;
    let mut depth = start_depth;
    while node != NULL_CHILD {
        if is_leaf(node) {
            let leaf = unsafe { &*leaf_ptr::<K, V>(node) };
            return if keys_eq(&leaf.key, key) {
                Some(&leaf.value as *const V)
            } else {
                None
            };
        }
        let hdr = unsafe { node_header(node) };
        let plen = hdr.prefix_len as usize;
        // Pessimistic prefix: only compare up to MAX_PREFIX_LEN stored bytes.
        // If prefix is longer, we skip the unstored portion and rely on leaf match.
        let check_len = plen.min(MAX_PREFIX_LEN);
        for i in 0..check_len {
            if depth + i >= K || hdr.prefix[i] != key[depth + i] {
                return None;
            }
        }
        depth += plen;
        if depth >= K {
            return None;
        }
        node = unsafe { find_child(node, key[depth]) };
        depth += 1;
    }
    None
}

/// Specialized lookup: `key_ref` for byte traversal, `key_val` for by-value comparison.
/// Same pattern as the usize variant's separate `key_arr` + `key` parameters.
#[inline(always)]
fn lookup_inner_eq<const K: usize, V>(
    root: usize,
    key_ref: &[u8; K],
    key_val: [u8; K],
    start_depth: usize,
    eq: fn([u8; K], [u8; K]) -> bool,
) -> Option<*const V> {
    let mut node = root;
    let mut depth = start_depth;
    while node != NULL_CHILD {
        if is_leaf(node) {
            let leaf = unsafe { &*leaf_ptr::<K, V>(node) };
            return if eq(leaf.key, key_val) {
                Some(&leaf.value as *const V)
            } else {
                None
            };
        }
        let hdr = unsafe { node_header(node) };
        let plen = hdr.prefix_len as usize;
        let check_len = plen.min(MAX_PREFIX_LEN);
        for i in 0..check_len {
            if depth + i >= K || hdr.prefix[i] != key_ref[depth + i] {
                return None;
            }
        }
        depth += plen;
        if depth >= K {
            return None;
        }
        node = unsafe { find_child(node, key_ref[depth]) };
        depth += 1;
    }
    None
}

// ═══════════════════════════════════════════════════════════════════════════
// Direct node child navigation (no copies)
// ═══════════════════════════════════════════════════════════════════════════

/// Find the first (smallest byte) child. Returns (byte, child_ptr) or None.
fn first_child<const K: usize>(node: usize) -> Option<(u8, usize)> {
    next_child_from::<K>(node, -1)
}

/// Find the last (largest byte) child. Returns (byte, child_ptr) or None.
fn last_child<const K: usize>(node: usize) -> Option<(u8, usize)> {
    prev_child_from::<K>(node, 256)
}

/// Find the next child after `after_byte` (exclusive). Scans forward.
fn next_child_from<const K: usize>(node: usize, after_byte: i16) -> Option<(u8, usize)> {
    if after_byte >= 255 {
        return None;
    }
    let hdr = unsafe { node_header(node) };
    let start = (after_byte + 1).max(0) as usize;
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(node as *const Node4) };
            let count = hdr.num_children as usize;
            for i in 0..count {
                if (n.keys[i] as usize) >= start {
                    return Some((n.keys[i], n.children[i]));
                }
            }
            None
        }
        NodeKind::N16 => {
            let n = unsafe { &*(node as *const Node16) };
            let count = hdr.num_children as usize;
            for i in 0..count {
                if (n.keys[i] as usize) >= start {
                    return Some((n.keys[i], n.children[i]));
                }
            }
            None
        }
        NodeKind::N48 => {
            let n = unsafe { &*(node as *const Node48) };
            for byte in start..256 {
                let idx = n.child_index[byte];
                if idx != EMPTY_INDEX {
                    return Some((byte as u8, n.children[idx as usize]));
                }
            }
            None
        }
        NodeKind::N256 => {
            let n = unsafe { &*(node as *const Node256) };
            for byte in start..256 {
                if n.children[byte] != NULL_CHILD {
                    return Some((byte as u8, n.children[byte]));
                }
            }
            None
        }
    }
}

/// Find the previous child before `before_byte` (exclusive). Scans backward.
fn prev_child_from<const K: usize>(node: usize, before_byte: i16) -> Option<(u8, usize)> {
    if before_byte <= 0 {
        return None;
    }
    let hdr = unsafe { node_header(node) };
    let end = (before_byte - 1) as usize;
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(node as *const Node4) };
            let count = hdr.num_children as usize;
            for i in (0..count).rev() {
                if (n.keys[i] as usize) <= end {
                    return Some((n.keys[i], n.children[i]));
                }
            }
            None
        }
        NodeKind::N16 => {
            let n = unsafe { &*(node as *const Node16) };
            let count = hdr.num_children as usize;
            for i in (0..count).rev() {
                if (n.keys[i] as usize) <= end {
                    return Some((n.keys[i], n.children[i]));
                }
            }
            None
        }
        NodeKind::N48 => {
            let n = unsafe { &*(node as *const Node48) };
            for byte in (0..=end).rev() {
                let idx = n.child_index[byte];
                if idx != EMPTY_INDEX {
                    return Some((byte as u8, n.children[idx as usize]));
                }
            }
            None
        }
        NodeKind::N256 => {
            let n = unsafe { &*(node as *const Node256) };
            for byte in (0..=end).rev() {
                if n.children[byte] != NULL_CHILD {
                    return Some((byte as u8, n.children[byte]));
                }
            }
            None
        }
    }
}

/// Find child at exact byte, or the next child >= byte.
fn child_ge<const K: usize>(node: usize, byte: u8) -> Option<(u8, usize)> {
    // Try exact match first (fast path).
    let exact = unsafe { find_child(node, byte) };
    if exact != NULL_CHILD {
        return Some((byte, exact));
    }
    // No exact match — find next.
    next_child_from::<K>(node, byte as i16)
}

/// Find child at exact byte, or the previous child <= byte.
fn child_le<const K: usize>(node: usize, byte: u8) -> Option<(u8, usize)> {
    let exact = unsafe { find_child(node, byte) };
    if exact != NULL_CHILD {
        return Some((byte, exact));
    }
    prev_child_from::<K>(node, byte as i16)
}

// ═══════════════════════════════════════════════════════════════════════════
// Cursor — zero-copy, stack-allocated
// ═══════════════════════════════════════════════════════════════════════════

/// 10 bytes: node pointer (8) + last byte position (2).
#[derive(Clone, Copy)]
struct CursorFrame {
    node: usize,
    /// The key byte of the child we're currently positioned at.
    /// -1 = before first, 256 = past last.
    byte_pos: i16,
}

/// Zero-copy bidirectional cursor over a frozen fixed-key ART tree.
/// Total size scales with K (stack depth = K + 2).
///
/// Stores a raw tagged leaf pointer for the current position. The caller
/// (ReadGuard) is responsible for ensuring the epoch guard outlives the cursor.
pub struct Cursor<const K: usize> {
    stack: [CursorFrame; MAX_DEPTH],
    depth: i8, // -1 = empty
    root: usize,
    /// Tagged leaf pointer for the current leaf, or NULL_CHILD if invalid.
    current_leaf: usize,
}

impl<const K: usize> Cursor<K> {
    pub fn new(root: usize) -> Self {
        Self {
            stack: [CursorFrame {
                node: 0,
                byte_pos: -1,
            }; MAX_DEPTH],
            depth: -1,
            root,
            current_leaf: NULL_CHILD,
        }
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.current_leaf != NULL_CHILD
    }

    /// Returns the key of the current leaf. The leaf must store `LeafNode<K, V>`.
    #[inline]
    pub fn key<V>(&self) -> Option<[u8; K]> {
        if self.current_leaf == NULL_CHILD {
            return None;
        }
        let leaf = unsafe { &*leaf_ptr::<K, V>(self.current_leaf) };
        Some(leaf.key)
    }

    /// Returns a raw pointer to the value inside the current leaf.
    /// Returns None if the cursor is not positioned on a valid leaf.
    ///
    /// # Safety
    /// The leaf must be alive (epoch-pinned) for the duration of any dereference.
    #[inline]
    pub unsafe fn value_ptr<V>(&self) -> Option<*const V> {
        if self.current_leaf == NULL_CHILD {
            return None;
        }
        let leaf = leaf_ptr::<K, V>(self.current_leaf);
        Some(unsafe { std::ptr::addr_of!((*leaf).value) })
    }

    /// Returns (key, raw value pointer) for the current leaf.
    ///
    /// # Safety
    /// The leaf must be alive (epoch-pinned) for the duration of any dereference.
    #[inline]
    pub unsafe fn key_value_raw<V>(&self) -> Option<([u8; K], *const V)> {
        if self.current_leaf == NULL_CHILD {
            return None;
        }
        let leaf = leaf_ptr::<K, V>(self.current_leaf);
        let key = unsafe { std::ptr::addr_of!((*leaf).key).read() };
        let val_ptr = unsafe { std::ptr::addr_of!((*leaf).value) };
        Some((key, val_ptr))
    }

    // ─── Seek ──────────────────────────────────────────────────────────

    pub fn seek_first(&mut self) {
        self.reset();
        if self.root == NULL_CHILD {
            return;
        }
        if is_leaf(self.root) {
            self.current_leaf = self.root;
            return;
        }
        self.descend_left(self.root);
    }

    pub fn seek_last(&mut self) {
        self.reset();
        if self.root == NULL_CHILD {
            return;
        }
        if is_leaf(self.root) {
            self.current_leaf = self.root;
            return;
        }
        self.descend_right(self.root);
    }

    pub fn seek_ge<V>(&mut self, target: &[u8; K]) {
        self.reset();
        if self.root == NULL_CHILD {
            return;
        }
        if is_leaf(self.root) {
            let leaf = unsafe { &*leaf_ptr::<K, V>(self.root) };
            if leaf.key >= *target {
                self.current_leaf = self.root;
            }
            return;
        }
        self.seek_ge_inner::<V>(self.root, target);
    }

    pub fn seek_gt<V>(&mut self, target: &[u8; K]) {
        self.seek_ge::<V>(target);
        if self.current_leaf != NULL_CHILD {
            let leaf = unsafe { &*leaf_ptr::<K, V>(self.current_leaf) };
            if leaf.key == *target {
                self.next();
            }
        }
    }

    pub fn seek_le<V>(&mut self, target: &[u8; K]) {
        self.reset();
        if self.root == NULL_CHILD {
            return;
        }
        if is_leaf(self.root) {
            let leaf = unsafe { &*leaf_ptr::<K, V>(self.root) };
            if leaf.key <= *target {
                self.current_leaf = self.root;
            }
            return;
        }
        self.seek_le_inner::<V>(self.root, target);
    }

    pub fn seek_lt<V>(&mut self, target: &[u8; K]) {
        self.seek_le::<V>(target);
        if self.current_leaf != NULL_CHILD {
            let leaf = unsafe { &*leaf_ptr::<K, V>(self.current_leaf) };
            if leaf.key == *target {
                self.prev();
            }
        }
    }

    pub fn seek_prefix_first<V>(&mut self, prefix: &[u8]) {
        let start = prefix_start::<K>(prefix);
        self.seek_ge::<V>(&start);
        if self.current_leaf != NULL_CHILD {
            let leaf = unsafe { &*leaf_ptr::<K, V>(self.current_leaf) };
            if !key_has_prefix::<K>(&leaf.key, prefix) {
                self.reset();
            }
        }
    }

    pub fn seek_prefix_last<V>(&mut self, prefix: &[u8]) {
        let end = prefix_end::<K>(prefix);
        self.seek_le::<V>(&end);
        if self.current_leaf != NULL_CHILD {
            let leaf = unsafe { &*leaf_ptr::<K, V>(self.current_leaf) };
            if !key_has_prefix::<K>(&leaf.key, prefix) {
                self.reset();
            }
        }
    }

    // ─── Navigation ────────────────────────────────────────────────────

    pub fn next(&mut self) -> bool {
        loop {
            if self.depth < 0 {
                self.current_leaf = NULL_CHILD;
                return false;
            }
            let frame = &self.stack[self.depth as usize];
            match next_child_from::<K>(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.current_leaf = child;
                        return true;
                    }
                    self.descend_left(child);
                    return self.current_leaf != NULL_CHILD;
                }
                None => {
                    self.depth -= 1;
                }
            }
        }
    }

    pub fn prev(&mut self) -> bool {
        loop {
            if self.depth < 0 {
                self.current_leaf = NULL_CHILD;
                return false;
            }
            let frame = &self.stack[self.depth as usize];
            match prev_child_from::<K>(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.current_leaf = child;
                        return true;
                    }
                    self.descend_right(child);
                    return self.current_leaf != NULL_CHILD;
                }
                None => {
                    self.depth -= 1;
                }
            }
        }
    }

    // ─── Internal ──────────────────────────────────────────────────────

    fn reset(&mut self) {
        self.depth = -1;
        self.current_leaf = NULL_CHILD;
    }

    fn push(&mut self, node: usize, byte_pos: i16) {
        self.depth += 1;
        self.stack[self.depth as usize] = CursorFrame { node, byte_pos };
    }

    fn descend_left(&mut self, mut node: usize) {
        loop {
            match first_child::<K>(node) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if is_leaf(child) {
                        self.current_leaf = child;
                        return;
                    }
                    node = child;
                }
                None => return,
            }
        }
    }

    fn descend_right(&mut self, mut node: usize) {
        loop {
            match last_child::<K>(node) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if is_leaf(child) {
                        self.current_leaf = child;
                        return;
                    }
                    node = child;
                }
                None => return,
            }
        }
    }

    fn seek_ge_inner<V>(&mut self, root: usize, target: &[u8; K]) {
        let mut node = root;
        let mut depth: usize = 0;

        loop {
            let hdr = unsafe { node_header(node) };
            let plen = hdr.prefix_len as usize;
            let mut cmp = std::cmp::Ordering::Equal;
            let check_len = plen.min(MAX_PREFIX_LEN);
            for i in 0..check_len {
                if depth + i >= K {
                    cmp = std::cmp::Ordering::Greater;
                    break;
                }
                cmp = hdr.prefix[i].cmp(&target[depth + i]);
                if cmp != std::cmp::Ordering::Equal {
                    break;
                }
            }

            match cmp {
                std::cmp::Ordering::Greater => {
                    self.descend_left(node);
                    return;
                }
                std::cmp::Ordering::Less => {
                    return;
                } // nothing >= target here
                std::cmp::Ordering::Equal => {}
            }

            depth += plen;
            if depth >= K {
                self.descend_left(node);
                return;
            }

            let target_byte = target[depth];
            depth += 1;

            // Find child >= target_byte.
            match child_ge::<K>(node, target_byte) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if byte > target_byte {
                        // This child has keys > target. Take its leftmost.
                        if is_leaf(child) {
                            self.current_leaf = child;
                        } else {
                            self.descend_left(child);
                        }
                        return;
                    }
                    // byte == target_byte — descend.
                    if is_leaf(child) {
                        let leaf = unsafe { &*leaf_ptr::<K, V>(child) };
                        if leaf.key >= *target {
                            self.current_leaf = child;
                            return;
                        }
                        // Leaf < target — advance to next.
                        self.next();
                        return;
                    }
                    node = child;
                }
                None => {
                    // No child >= target_byte. Walk up.
                    self.next_from_stack();
                    return;
                }
            }
        }
    }

    fn seek_le_inner<V>(&mut self, root: usize, target: &[u8; K]) {
        let mut node = root;
        let mut depth: usize = 0;

        loop {
            let hdr = unsafe { node_header(node) };
            let plen = hdr.prefix_len as usize;
            let mut cmp = std::cmp::Ordering::Equal;
            let check_len = plen.min(MAX_PREFIX_LEN);
            for i in 0..check_len {
                if depth + i >= K {
                    cmp = std::cmp::Ordering::Greater;
                    break;
                }
                cmp = hdr.prefix[i].cmp(&target[depth + i]);
                if cmp != std::cmp::Ordering::Equal {
                    break;
                }
            }

            match cmp {
                std::cmp::Ordering::Less => {
                    self.descend_right(node);
                    return;
                }
                std::cmp::Ordering::Greater => {
                    return;
                }
                std::cmp::Ordering::Equal => {}
            }

            depth += plen;
            if depth >= K {
                self.descend_right(node);
                return;
            }

            let target_byte = target[depth];
            depth += 1;

            match child_le::<K>(node, target_byte) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if byte < target_byte {
                        if is_leaf(child) {
                            self.current_leaf = child;
                        } else {
                            self.descend_right(child);
                        }
                        return;
                    }
                    // byte == target_byte — descend.
                    if is_leaf(child) {
                        let leaf = unsafe { &*leaf_ptr::<K, V>(child) };
                        if leaf.key <= *target {
                            self.current_leaf = child;
                            return;
                        }
                        self.prev();
                        return;
                    }
                    node = child;
                }
                None => {
                    self.prev_from_stack();
                    return;
                }
            }
        }
    }

    fn next_from_stack(&mut self) {
        loop {
            if self.depth < 0 {
                self.current_leaf = NULL_CHILD;
                return;
            }
            let frame = &self.stack[self.depth as usize];
            match next_child_from::<K>(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.current_leaf = child;
                        return;
                    }
                    self.descend_left(child);
                    return;
                }
                None => {
                    self.depth -= 1;
                }
            }
        }
    }

    fn prev_from_stack(&mut self) {
        loop {
            if self.depth < 0 {
                self.current_leaf = NULL_CHILD;
                return;
            }
            let frame = &self.stack[self.depth as usize];
            match prev_child_from::<K>(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.current_leaf = child;
                        return;
                    }
                    self.descend_right(child);
                    return;
                }
                None => {
                    self.depth -= 1;
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Prefix helpers
// ═══════════════════════════════════════════════════════════════════════════

pub fn prefix_start<const K: usize>(prefix: &[u8]) -> [u8; K] {
    let mut key = [0u8; K];
    let len = prefix.len().min(K);
    key[..len].copy_from_slice(&prefix[..len]);
    key
}

pub fn prefix_end<const K: usize>(prefix: &[u8]) -> [u8; K] {
    let mut key = [0xFFu8; K];
    let len = prefix.len().min(K);
    key[..len].copy_from_slice(&prefix[..len]);
    key
}

fn key_has_prefix<const K: usize>(key: &[u8; K], prefix: &[u8]) -> bool {
    let len = prefix.len().min(K);
    key[..len] == prefix[..len]
}

// ═══════════════════════════════════════════════════════════════════════════
// Iterator wrappers
// ═══════════════════════════════════════════════════════════════════════════

pub struct Iter<'a, const K: usize, V> {
    cursor: Cursor<K>,
    started: bool,
    _marker: std::marker::PhantomData<&'a V>,
}
impl<'a, const K: usize, V> Iter<'a, K, V> {
    pub fn new(root: usize) -> Self {
        let mut cursor = Cursor::new(root);
        cursor.seek_first();
        Self {
            cursor,
            started: false,
            _marker: std::marker::PhantomData,
        }
    }
}
impl<'a, const K: usize, V> Iterator for Iter<'a, K, V> {
    type Item = ([u8; K], &'a V);
    fn next(&mut self) -> Option<([u8; K], &'a V)> {
        if !self.started {
            self.started = true;
        } else {
            self.cursor.next();
        }
        // Safety: the epoch guard (held by the ReadGuard that created this Iter)
        // ensures the leaf memory is alive for lifetime 'a.
        unsafe {
            self.cursor
                .key_value_raw::<V>()
                .map(|(k, ptr)| (k, &*ptr))
        }
    }
}

pub struct Keys<const K: usize> {
    cursor: Cursor<K>,
    started: bool,
}
impl<const K: usize> Keys<K> {
    pub fn new(root: usize) -> Self {
        let mut cursor = Cursor::new(root);
        cursor.seek_first();
        Self {
            cursor,
            started: false,
        }
    }
}
impl<const K: usize> Iterator for Keys<K> {
    type Item = [u8; K];
    fn next(&mut self) -> Option<[u8; K]> {
        if !self.started {
            self.started = true;
        } else {
            self.cursor.next();
        }
        if self.cursor.current_leaf == NULL_CHILD {
            return None;
        }
        // Safety: we only read the key portion of the leaf, which is just bytes.
        // We don't need to know V to read the key at offset 0 of LeafNode<K, V>.
        // But we DO need to know the leaf type. Since Keys is V-independent,
        // we read the key directly from the raw pointer.
        let leaf_raw = (self.cursor.current_leaf & !LEAF_TAG) as *const [u8; K];
        Some(unsafe { *leaf_raw })
    }
}

pub struct Values<'a, const K: usize, V> {
    inner: Iter<'a, K, V>,
}
impl<'a, const K: usize, V> Values<'a, K, V> {
    pub fn new(root: usize) -> Self {
        Self {
            inner: Iter::new(root),
        }
    }
}
impl<'a, const K: usize, V> Iterator for Values<'a, K, V> {
    type Item = &'a V;
    fn next(&mut self) -> Option<&'a V> {
        self.inner.next().map(|(_, v)| v)
    }
}

pub struct RevIter<'a, const K: usize, V> {
    cursor: Cursor<K>,
    started: bool,
    _marker: std::marker::PhantomData<&'a V>,
}
impl<'a, const K: usize, V> RevIter<'a, K, V> {
    pub fn new(root: usize) -> Self {
        let mut cursor = Cursor::new(root);
        cursor.seek_last();
        Self {
            cursor,
            started: false,
            _marker: std::marker::PhantomData,
        }
    }
}
impl<'a, const K: usize, V> Iterator for RevIter<'a, K, V> {
    type Item = ([u8; K], &'a V);
    fn next(&mut self) -> Option<([u8; K], &'a V)> {
        if !self.started {
            self.started = true;
        } else {
            self.cursor.prev();
        }
        unsafe {
            self.cursor
                .key_value_raw::<V>()
                .map(|(k, ptr)| (k, &*ptr))
        }
    }
}

pub struct CursorRange<'a, const K: usize, V> {
    cursor: Cursor<K>,
    end: [u8; K],
    started: bool,
    _marker: std::marker::PhantomData<&'a V>,
}
impl<'a, const K: usize, V> CursorRange<'a, K, V> {
    pub fn new(root: usize, start: &[u8; K], end: [u8; K]) -> Self {
        let mut cursor = Cursor::new(root);
        cursor.seek_ge::<V>(start);
        Self {
            cursor,
            end,
            started: false,
            _marker: std::marker::PhantomData,
        }
    }
}
impl<'a, const K: usize, V> Iterator for CursorRange<'a, K, V> {
    type Item = ([u8; K], &'a V);
    fn next(&mut self) -> Option<([u8; K], &'a V)> {
        if !self.started {
            self.started = true;
        } else {
            self.cursor.next();
        }
        let kv = unsafe {
            self.cursor
                .key_value_raw::<V>()
                .map(|(k, ptr)| (k, &*ptr))
        };
        match kv {
            Some((k, _)) if k > self.end => None,
            other => other,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Legacy compat
// ═══════════════════════════════════════════════════════════════════════════

pub(super) fn sorted_children<const K: usize>(
    node: usize,
    buf: &mut [(u8, usize); 256],
) -> &[(u8, usize)] {
    let hdr = unsafe { node_header(node) };
    let count = hdr.num_children as usize;
    match hdr.kind {
        NodeKind::N4 => {
            let n = unsafe { &*(node as *const Node4) };
            for i in 0..count {
                buf[i] = (n.keys[i], n.children[i]);
            }
            &buf[..count]
        }
        NodeKind::N16 => {
            let n = unsafe { &*(node as *const Node16) };
            for i in 0..count {
                buf[i] = (n.keys[i], n.children[i]);
            }
            &buf[..count]
        }
        NodeKind::N48 => {
            let n = unsafe { &*(node as *const Node48) };
            let mut idx = 0;
            for b in 0..256u16 {
                let s = n.child_index[b as usize];
                if s != EMPTY_INDEX {
                    buf[idx] = (b as u8, n.children[s as usize]);
                    idx += 1;
                }
            }
            &buf[..idx]
        }
        NodeKind::N256 => {
            let n = unsafe { &*(node as *const Node256) };
            let mut idx = 0;
            for b in 0..256usize {
                if n.children[b] != NULL_CHILD {
                    buf[idx] = (b as u8, n.children[b]);
                    idx += 1;
                }
            }
            &buf[..idx]
        }
    }
}

pub struct RangeIter<'a, const K: usize, V> {
    cursor_range: CursorRange<'a, K, V>,
}
impl<'a, const K: usize, V> RangeIter<'a, K, V> {
    pub(super) fn new(root: usize, start: &[u8; K], end: [u8; K]) -> Self {
        Self {
            cursor_range: CursorRange::new(root, start, end),
        }
    }
}
impl<'a, const K: usize, V> Iterator for RangeIter<'a, K, V> {
    type Item = ([u8; K], &'a V);
    fn next(&mut self) -> Option<([u8; K], &'a V)> {
        self.cursor_range.next()
    }
}

pub(super) fn range_scan<const K: usize, V>(
    root: usize,
    start: &[u8; K],
    end: [u8; K],
    result: &mut [([u8; K], &V)],
) -> usize {
    let mut count = 0;
    for kv in CursorRange::<K, V>::new(root, start, end) {
        if count >= result.len() {
            break;
        }
        result[count] = kv;
        count += 1;
    }
    count
}
