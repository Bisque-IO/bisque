//! Lookup, cursor-based iteration, and range operations over the ART.
//!
//! The [`Cursor`] navigates directly over frozen node memory — zero copies,
//! zero heap allocations. Each stack frame is 10 bytes (node pointer + byte
//! position). Total cursor size: ~100 bytes for a 10-level tree.

use super::node::*;

// ═══════════════════════════════════════════════════════════════════════════
// Lookup
// ═══════════════════════════════════════════════════════════════════════════

pub(super) fn lookup(root: usize, key: usize) -> Option<usize> {
    let key_arr = key_bytes_array(key);
    lookup_from(root, &key_arr, key, 0)
}

pub(super) fn lookup_from(
    root: usize,
    key_arr: &[u8; KEY_BYTES],
    key: usize,
    start_depth: usize,
) -> Option<usize> {
    let mut node = root;
    let mut depth = start_depth;
    while node != NULL_CHILD {
        if is_leaf(node) {
            let leaf = unsafe { &*leaf_ptr(node) };
            return if leaf.key == key {
                Some(leaf.value)
            } else {
                None
            };
        }
        let hdr = unsafe { node_header(node) };
        let plen = hdr.prefix_len as usize;
        for i in 0..plen {
            if depth + i >= KEY_BYTES || hdr.prefix[i] != key_arr[depth + i] {
                return None;
            }
        }
        depth += plen;
        if depth >= KEY_BYTES {
            return None;
        }
        node = unsafe { find_child(node, key_arr[depth]) };
        depth += 1;
    }
    None
}

// ═══════════════════════════════════════════════════════════════════════════
// Direct node child navigation (no copies)
// ═══════════════════════════════════════════════════════════════════════════

/// Find the first (smallest byte) child. Returns (byte, child_ptr) or None.
fn first_child(node: usize) -> Option<(u8, usize)> {
    next_child_from(node, -1)
}

/// Find the last (largest byte) child. Returns (byte, child_ptr) or None.
fn last_child(node: usize) -> Option<(u8, usize)> {
    prev_child_from(node, 256)
}

/// Find the next child after `after_byte` (exclusive). Scans forward.
fn next_child_from(node: usize, after_byte: i16) -> Option<(u8, usize)> {
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
fn prev_child_from(node: usize, before_byte: i16) -> Option<(u8, usize)> {
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
fn child_ge(node: usize, byte: u8) -> Option<(u8, usize)> {
    // Try exact match first (fast path).
    let exact = unsafe { find_child(node, byte) };
    if exact != NULL_CHILD {
        return Some((byte, exact));
    }
    // No exact match — find next.
    next_child_from(node, byte as i16)
}

/// Find child at exact byte, or the previous child <= byte.
fn child_le(node: usize, byte: u8) -> Option<(u8, usize)> {
    let exact = unsafe { find_child(node, byte) };
    if exact != NULL_CHILD {
        return Some((byte, exact));
    }
    prev_child_from(node, byte as i16)
}

// ═══════════════════════════════════════════════════════════════════════════
// Cursor — zero-copy, stack-allocated
// ═══════════════════════════════════════════════════════════════════════════

const MAX_DEPTH: usize = KEY_BYTES + 2;

/// 10 bytes: node pointer (8) + last byte position (2).
#[derive(Clone, Copy)]
struct CursorFrame {
    node: usize,
    /// The key byte of the child we're currently positioned at.
    /// -1 = before first, 256 = past last.
    byte_pos: i16,
}

/// Zero-copy bidirectional cursor over a frozen ART tree.
/// Total size: ~100 bytes on the stack.
pub struct Cursor {
    stack: [CursorFrame; MAX_DEPTH],
    depth: i8, // -1 = empty
    root: usize,
    current: Option<(usize, usize)>,
}

impl Cursor {
    pub fn new(root: usize) -> Self {
        Self {
            stack: [CursorFrame {
                node: 0,
                byte_pos: -1,
            }; MAX_DEPTH],
            depth: -1,
            root,
            current: None,
        }
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }
    #[inline]
    pub fn key(&self) -> Option<usize> {
        self.current.map(|(k, _)| k)
    }
    #[inline]
    pub fn value(&self) -> Option<usize> {
        self.current.map(|(_, v)| v)
    }
    #[inline]
    pub fn key_value(&self) -> Option<(usize, usize)> {
        self.current
    }

    // ─── Seek ──────────────────────────────────────────────────────────

    pub fn seek_first(&mut self) {
        self.reset();
        if self.root == NULL_CHILD {
            return;
        }
        if is_leaf(self.root) {
            self.set_leaf(self.root);
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
            self.set_leaf(self.root);
            return;
        }
        self.descend_right(self.root);
    }

    pub fn seek_ge(&mut self, target: usize) {
        self.reset();
        if self.root == NULL_CHILD {
            return;
        }
        if is_leaf(self.root) {
            let leaf = unsafe { &*leaf_ptr(self.root) };
            if leaf.key >= target {
                self.current = Some((leaf.key, leaf.value));
            }
            return;
        }
        self.seek_ge_inner(self.root, target);
    }

    pub fn seek_gt(&mut self, target: usize) {
        self.seek_ge(target);
        if let Some((k, _)) = self.current {
            if k == target {
                self.next();
            }
        }
    }

    pub fn seek_le(&mut self, target: usize) {
        self.reset();
        if self.root == NULL_CHILD {
            return;
        }
        if is_leaf(self.root) {
            let leaf = unsafe { &*leaf_ptr(self.root) };
            if leaf.key <= target {
                self.current = Some((leaf.key, leaf.value));
            }
            return;
        }
        self.seek_le_inner(self.root, target);
    }

    pub fn seek_lt(&mut self, target: usize) {
        self.seek_le(target);
        if let Some((k, _)) = self.current {
            if k == target {
                self.prev();
            }
        }
    }

    pub fn seek_prefix_first(&mut self, prefix: &[u8]) {
        self.seek_ge(prefix_start(prefix));
        if let Some((k, _)) = self.current {
            if !key_has_prefix(k, prefix) {
                self.reset();
            }
        }
    }

    pub fn seek_prefix_last(&mut self, prefix: &[u8]) {
        self.seek_le(prefix_end(prefix));
        if let Some((k, _)) = self.current {
            if !key_has_prefix(k, prefix) {
                self.reset();
            }
        }
    }

    // ─── Navigation ────────────────────────────────────────────────────

    pub fn next(&mut self) -> Option<(usize, usize)> {
        loop {
            if self.depth < 0 {
                self.current = None;
                return None;
            }
            let frame = &self.stack[self.depth as usize];
            match next_child_from(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.set_leaf(child);
                        return self.current;
                    }
                    self.descend_left(child);
                    return self.current;
                }
                None => {
                    self.depth -= 1;
                }
            }
        }
    }

    pub fn prev(&mut self) -> Option<(usize, usize)> {
        loop {
            if self.depth < 0 {
                self.current = None;
                return None;
            }
            let frame = &self.stack[self.depth as usize];
            match prev_child_from(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.set_leaf(child);
                        return self.current;
                    }
                    self.descend_right(child);
                    return self.current;
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
        self.current = None;
    }

    fn set_leaf(&mut self, p: usize) {
        let leaf = unsafe { &*leaf_ptr(p) };
        self.current = Some((leaf.key, leaf.value));
    }

    fn push(&mut self, node: usize, byte_pos: i16) {
        self.depth += 1;
        self.stack[self.depth as usize] = CursorFrame { node, byte_pos };
    }

    fn descend_left(&mut self, mut node: usize) {
        loop {
            match first_child(node) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if is_leaf(child) {
                        self.set_leaf(child);
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
            match last_child(node) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if is_leaf(child) {
                        self.set_leaf(child);
                        return;
                    }
                    node = child;
                }
                None => return,
            }
        }
    }

    fn seek_ge_inner(&mut self, root: usize, target: usize) {
        let key_arr = key_bytes_array(target);
        let mut node = root;
        let mut depth: usize = 0;

        loop {
            let hdr = unsafe { node_header(node) };
            let plen = hdr.prefix_len as usize;
            let mut cmp = std::cmp::Ordering::Equal;
            for i in 0..plen {
                if depth + i >= KEY_BYTES {
                    cmp = std::cmp::Ordering::Greater;
                    break;
                }
                cmp = hdr.prefix[i].cmp(&key_arr[depth + i]);
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
            if depth >= KEY_BYTES {
                self.descend_left(node);
                return;
            }

            let target_byte = key_arr[depth];
            depth += 1;

            // Find child >= target_byte.
            match child_ge(node, target_byte) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if byte > target_byte {
                        // This child has keys > target. Take its leftmost.
                        if is_leaf(child) {
                            self.set_leaf(child);
                        } else {
                            self.descend_left(child);
                        }
                        return;
                    }
                    // byte == target_byte — descend.
                    if is_leaf(child) {
                        let leaf = unsafe { &*leaf_ptr(child) };
                        if leaf.key >= target {
                            self.current = Some((leaf.key, leaf.value));
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

    fn seek_le_inner(&mut self, root: usize, target: usize) {
        let key_arr = key_bytes_array(target);
        let mut node = root;
        let mut depth: usize = 0;

        loop {
            let hdr = unsafe { node_header(node) };
            let plen = hdr.prefix_len as usize;
            let mut cmp = std::cmp::Ordering::Equal;
            for i in 0..plen {
                if depth + i >= KEY_BYTES {
                    cmp = std::cmp::Ordering::Greater;
                    break;
                }
                cmp = hdr.prefix[i].cmp(&key_arr[depth + i]);
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
            if depth >= KEY_BYTES {
                self.descend_right(node);
                return;
            }

            let target_byte = key_arr[depth];
            depth += 1;

            match child_le(node, target_byte) {
                Some((byte, child)) => {
                    self.push(node, byte as i16);
                    if byte < target_byte {
                        if is_leaf(child) {
                            self.set_leaf(child);
                        } else {
                            self.descend_right(child);
                        }
                        return;
                    }
                    // byte == target_byte — descend.
                    if is_leaf(child) {
                        let leaf = unsafe { &*leaf_ptr(child) };
                        if leaf.key <= target {
                            self.current = Some((leaf.key, leaf.value));
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
                self.current = None;
                return;
            }
            let frame = &self.stack[self.depth as usize];
            match next_child_from(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.set_leaf(child);
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
                self.current = None;
                return;
            }
            let frame = &self.stack[self.depth as usize];
            match prev_child_from(frame.node, frame.byte_pos) {
                Some((byte, child)) => {
                    self.stack[self.depth as usize].byte_pos = byte as i16;
                    if is_leaf(child) {
                        self.set_leaf(child);
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

pub fn prefix_start(prefix: &[u8]) -> usize {
    let mut key = [0u8; KEY_BYTES];
    let len = prefix.len().min(KEY_BYTES);
    key[..len].copy_from_slice(&prefix[..len]);
    usize::from_be_bytes(key)
}

pub fn prefix_end(prefix: &[u8]) -> usize {
    let mut key = [0xFFu8; KEY_BYTES];
    let len = prefix.len().min(KEY_BYTES);
    key[..len].copy_from_slice(&prefix[..len]);
    usize::from_be_bytes(key)
}

fn key_has_prefix(key: usize, prefix: &[u8]) -> bool {
    let kb = key.to_be_bytes();
    let len = prefix.len().min(KEY_BYTES);
    kb[..len] == prefix[..len]
}

// ═══════════════════════════════════════════════════════════════════════════
// Iterator wrappers
// ═══════════════════════════════════════════════════════════════════════════

pub struct Iter {
    cursor: Cursor,
    started: bool,
}
impl Iter {
    pub fn new(root: usize) -> Self {
        let mut cursor = Cursor::new(root);
        cursor.seek_first();
        Self {
            cursor,
            started: false,
        }
    }
}
impl Iterator for Iter {
    type Item = (usize, usize);
    fn next(&mut self) -> Option<(usize, usize)> {
        if !self.started {
            self.started = true;
            self.cursor.key_value()
        } else {
            self.cursor.next()
        }
    }
}

pub struct Keys {
    inner: Iter,
}
impl Keys {
    pub fn new(root: usize) -> Self {
        Self {
            inner: Iter::new(root),
        }
    }
}
impl Iterator for Keys {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        self.inner.next().map(|(k, _)| k)
    }
}

pub struct Values {
    inner: Iter,
}
impl Values {
    pub fn new(root: usize) -> Self {
        Self {
            inner: Iter::new(root),
        }
    }
}
impl Iterator for Values {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        self.inner.next().map(|(_, v)| v)
    }
}

pub struct RevIter {
    cursor: Cursor,
    started: bool,
}
impl RevIter {
    pub fn new(root: usize) -> Self {
        let mut cursor = Cursor::new(root);
        cursor.seek_last();
        Self {
            cursor,
            started: false,
        }
    }
}
impl Iterator for RevIter {
    type Item = (usize, usize);
    fn next(&mut self) -> Option<(usize, usize)> {
        if !self.started {
            self.started = true;
            self.cursor.key_value()
        } else {
            self.cursor.prev()
        }
    }
}

pub struct CursorRange {
    cursor: Cursor,
    end: usize,
    started: bool,
}
impl CursorRange {
    pub fn new(root: usize, start: usize, end: usize) -> Self {
        let mut cursor = Cursor::new(root);
        cursor.seek_ge(start);
        Self {
            cursor,
            end,
            started: false,
        }
    }
}
impl Iterator for CursorRange {
    type Item = (usize, usize);
    fn next(&mut self) -> Option<(usize, usize)> {
        let kv = if !self.started {
            self.started = true;
            self.cursor.key_value()
        } else {
            self.cursor.next()
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

pub(super) fn sorted_children<'a>(
    node: usize,
    buf: &'a mut [(u8, usize); 256],
) -> &'a [(u8, usize)] {
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

pub struct RangeIter<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    cursor_range: CursorRange,
    _marker: std::marker::PhantomData<(K, V)>,
}
impl<K, V> RangeIter<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    pub(super) fn new(root: usize, start: usize, end: usize) -> Self {
        Self {
            cursor_range: CursorRange::new(root, start, end),
            _marker: std::marker::PhantomData,
        }
    }
}
impl<K, V> Iterator for RangeIter<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<(K, V)> {
        self.cursor_range
            .next()
            .map(|(k, v)| (K::from(k), V::from(v)))
    }
}

pub(super) fn range_scan(
    root: usize,
    start: usize,
    end: usize,
    result: &mut [(usize, usize)],
) -> usize {
    let mut count = 0;
    for kv in CursorRange::new(root, start, end) {
        if count >= result.len() {
            break;
        }
        result[count] = kv;
        count += 1;
    }
    count
}
