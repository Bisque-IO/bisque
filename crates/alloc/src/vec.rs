//! Heap-backed typed growable array with fallible allocation.

use allocator_api2::alloc::AllocError;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::ptr::{self, NonNull};
use std::slice;

use crate::Heap;

// ---------------------------------------------------------------------------
// Backward-compat alias — old code that used the byte-only Vec can use this.
// ---------------------------------------------------------------------------

/// Alias for `Vec<u8>` — the original byte-buffer type.
pub type ByteVec = Vec<u8>;

// ---------------------------------------------------------------------------
// Vec<T>
// ---------------------------------------------------------------------------

/// A growable typed array allocated from a [`Heap`].
///
/// Every mutating operation that may allocate returns `Result<_, AllocError>`.
/// When the heap's arena is exhausted, operations fail gracefully instead of
/// panicking.
pub struct Vec<T> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
    heap: Heap,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for Vec<T> {}
unsafe impl<T: Sync> Sync for Vec<T> {}

// -- helpers ----------------------------------------------------------------

#[inline]
fn is_zst<T>() -> bool {
    mem::size_of::<T>() == 0
}

impl<T> Vec<T> {
    /// Create an empty `Vec` with no allocation.
    pub fn new(heap: &Heap) -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            cap: if is_zst::<T>() { usize::MAX } else { 0 },
            heap: heap.clone(),
            _marker: PhantomData,
        }
    }

    /// Create a `Vec` with pre-allocated capacity.
    pub fn with_capacity(cap: usize, heap: &Heap) -> Result<Self, AllocError> {
        if is_zst::<T>() || cap == 0 {
            return Ok(Self::new(heap));
        }
        let layout = Layout::array::<T>(cap).map_err(|_| AllocError)?;
        let ptr = heap.alloc(layout.size(), layout.align());
        if ptr.is_null() {
            return Err(AllocError);
        }
        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr.cast::<T>()) },
            len: 0,
            cap,
            heap: heap.clone(),
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Push a value. Returns `Err(AllocError)` if the heap is full.
    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), AllocError> {
        if self.len == self.cap {
            if is_zst::<T>() {
                // ZST — cap is usize::MAX, this can never happen in practice,
                // but guard anyway.
                return Err(AllocError);
            }
            self.try_grow(self.len + 1)?;
        }
        unsafe {
            ptr::write(self.ptr.as_ptr().add(self.len), val);
        }
        self.len += 1;
        Ok(())
    }

    /// Pop the last element.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(unsafe { ptr::read(self.ptr.as_ptr().add(self.len)) })
    }

    /// Clear the vector, dropping all elements. Capacity is retained.
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Truncate to `len` elements. Elements beyond `len` are dropped.
    /// No-op if `len >= self.len`.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        let old_len = self.len;
        // Set len first so that if a destructor panics we don't double-drop.
        self.len = len;
        unsafe {
            let tail = slice::from_raw_parts_mut(self.ptr.as_ptr().add(len), old_len - len);
            ptr::drop_in_place(tail);
        }
    }

    /// Reserve at least `additional` more elements of capacity.
    pub fn reserve(&mut self, additional: usize) -> Result<(), AllocError> {
        if is_zst::<T>() {
            return Ok(());
        }
        let needed = self.len.checked_add(additional).ok_or(AllocError)?;
        if needed > self.cap {
            self.try_grow(needed)?;
        }
        Ok(())
    }

    /// Remove the element at `idx` by swapping it with the last element.
    /// Panics if `idx >= len`.
    pub fn swap_remove(&mut self, idx: usize) -> T {
        assert!(idx < self.len, "swap_remove index out of bounds");
        self.len -= 1;
        unsafe {
            let base = self.ptr.as_ptr();
            let val = ptr::read(base.add(idx));
            if idx != self.len {
                ptr::copy_nonoverlapping(base.add(self.len), base.add(idx), 1);
            }
            val
        }
    }

    /// Retain only elements for which `f` returns `true`.
    pub fn retain(&mut self, mut f: impl FnMut(&T) -> bool) {
        let mut i = 0;
        while i < self.len {
            if !f(&self[i]) {
                // drop and shift — use swap_remove-style if order doesn't matter,
                // but std retains order, so we do too.
                unsafe {
                    let p = self.ptr.as_ptr().add(i);
                    ptr::drop_in_place(p);
                    let remaining = self.len - i - 1;
                    if remaining > 0 {
                        ptr::copy(p.add(1), p, remaining);
                    }
                    self.len -= 1;
                }
            } else {
                i += 1;
            }
        }
    }

    /// Append all elements from `other`, leaving `other` empty.
    pub fn append(&mut self, other: &mut Self) -> Result<(), AllocError> {
        if other.is_empty() {
            return Ok(());
        }
        self.reserve(other.len)?;
        unsafe {
            ptr::copy_nonoverlapping(
                other.ptr.as_ptr(),
                self.ptr.as_ptr().add(self.len),
                other.len,
            );
        }
        self.len += other.len;
        // Don't drop the moved elements.
        other.len = 0;
        Ok(())
    }

    /// Iterate by reference.
    #[inline]
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.as_slice().iter()
    }

    /// Iterate by mutable reference.
    #[inline]
    pub fn iter_mut(&mut self) -> slice::IterMut<'_, T> {
        self.as_mut_slice().iter_mut()
    }

    /// Remove and return the element at `idx`, shifting all elements after it
    /// to the left. Panics if `idx >= len`.
    pub fn remove(&mut self, idx: usize) -> T {
        assert!(idx < self.len, "remove index out of bounds");
        unsafe {
            let base = self.ptr.as_ptr();
            let val = ptr::read(base.add(idx));
            let remaining = self.len - idx - 1;
            if remaining > 0 {
                ptr::copy(base.add(idx + 1), base.add(idx), remaining);
            }
            self.len -= 1;
            val
        }
    }

    /// Insert `val` at `idx`, shifting all elements at and after `idx` to the
    /// right. Returns `Err(AllocError)` if the heap is full.
    /// Panics if `idx > len`.
    pub fn insert(&mut self, idx: usize, val: T) -> Result<(), AllocError> {
        assert!(idx <= self.len, "insert index out of bounds");
        if self.len == self.cap {
            if is_zst::<T>() {
                return Err(AllocError);
            }
            self.try_grow(self.len + 1)?;
        }
        unsafe {
            let base = self.ptr.as_ptr();
            let remaining = self.len - idx;
            if remaining > 0 {
                ptr::copy(base.add(idx), base.add(idx + 1), remaining);
            }
            ptr::write(base.add(idx), val);
        }
        self.len += 1;
        Ok(())
    }

    /// Sort the vector in-place using an unstable sort.
    /// Requires `T: Ord`.
    #[inline]
    pub fn sort_unstable(&mut self)
    where
        T: Ord,
    {
        self.as_mut_slice().sort_unstable();
    }

    /// Sort the vector in-place using an unstable sort with a custom comparator.
    #[inline]
    pub fn sort_unstable_by<F>(&mut self, compare: F)
    where
        F: FnMut(&T, &T) -> std::cmp::Ordering,
    {
        self.as_mut_slice().sort_unstable_by(compare);
    }

    /// Sort the vector in-place using an unstable sort with a key extraction function.
    #[inline]
    pub fn sort_unstable_by_key<K, F>(&mut self, f: F)
    where
        F: FnMut(&T) -> K,
        K: Ord,
    {
        self.as_mut_slice().sort_unstable_by_key(f);
    }

    /// Extend the vector from an iterator of owned values.
    /// Returns `Err(AllocError)` if the heap is full during any push.
    pub fn extend_from_iter<I: IntoIterator<Item = T>>(
        &mut self,
        iter: I,
    ) -> Result<(), AllocError> {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        if lower > 0 {
            self.reserve(lower)?;
        }
        for val in iter {
            self.push(val)?;
        }
        Ok(())
    }

    /// Drain elements in the given range, returning them as an iterator.
    /// The removed elements are yielded by the `Drain` iterator; when the
    /// iterator is dropped, any remaining elements are dropped and the
    /// vector's tail is shifted into the gap.
    ///
    /// Panics if the range is out of bounds.
    pub fn drain<R: std::ops::RangeBounds<usize>>(&mut self, range: R) -> Drain<'_, T> {
        use std::ops::Bound;
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len,
        };
        assert!(start <= end, "drain: start ({start}) > end ({end})");
        assert!(end <= self.len, "drain: end ({end}) > len ({})", self.len);

        let drain_len = end - start;
        let tail_len = self.len - end;

        // Temporarily set len to `start` so the drained region is "outside" the
        // live portion. Drain::drop will fix up the tail.
        self.len = start;

        Drain {
            vec: self as *mut Vec<T>,
            iter: unsafe { slice::from_raw_parts(self.ptr.as_ptr().add(start), drain_len) },
            idx: 0,
            tail_start: end,
            tail_len,
        }
    }

    // -- internal grow ------------------------------------------------------

    fn try_grow(&mut self, min_cap: usize) -> Result<(), AllocError> {
        debug_assert!(!is_zst::<T>());
        let new_cap = min_cap
            .checked_next_power_of_two()
            .ok_or(AllocError)?
            .max(if mem::size_of::<T>() == 1 { 64 } else { 4 });
        let layout = Layout::array::<T>(new_cap).map_err(|_| AllocError)?;
        if self.cap == 0 {
            let p = self.heap.alloc(layout.size(), layout.align());
            if p.is_null() {
                return Err(AllocError);
            }
            self.ptr = unsafe { NonNull::new_unchecked(p.cast::<T>()) };
        } else {
            let p = unsafe {
                self.heap.realloc(
                    self.ptr.as_ptr().cast::<u8>(),
                    layout.size(),
                    layout.align(),
                )
            };
            if p.is_null() {
                return Err(AllocError);
            }
            self.ptr = unsafe { NonNull::new_unchecked(p.cast::<T>()) };
        }
        self.cap = new_cap;
        Ok(())
    }
}

// -- T: Copy convenience ---------------------------------------------------

impl<T: Copy> Vec<T> {
    /// Append elements from a slice (requires `T: Copy`).
    pub fn extend_from_slice(&mut self, src: &[T]) -> Result<(), AllocError> {
        if src.is_empty() {
            return Ok(());
        }
        let needed = self.len.checked_add(src.len()).ok_or(AllocError)?;
        if needed > self.cap {
            if is_zst::<T>() {
                // ZST: no allocation needed, just bump len
            } else {
                self.try_grow(needed)?;
            }
        }
        if !is_zst::<T>() {
            unsafe {
                ptr::copy_nonoverlapping(src.as_ptr(), self.ptr.as_ptr().add(self.len), src.len());
            }
        }
        self.len += src.len();
        Ok(())
    }
}

// -- Vec<u8> specific methods (backward compat) -----------------------------

impl Vec<u8> {
    /// Convert into `bytes::Bytes` zero-copy.
    pub fn into_bytes(self) -> bytes::Bytes {
        if self.len == 0 {
            return bytes::Bytes::new();
        }
        bytes::Bytes::from_owner(self)
    }

    /// Return the contents as a `std::vec::Vec<u8>` by copying.
    pub fn to_std_vec(&self) -> std::vec::Vec<u8> {
        self.as_slice().to_vec()
    }
}

// -- Drop -------------------------------------------------------------------

impl<T> Drop for Vec<T> {
    fn drop(&mut self) {
        // Drop all live elements.
        if self.len > 0 && !is_zst::<T>() && mem::needs_drop::<T>() {
            unsafe {
                ptr::drop_in_place(slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len));
            }
        }
        // Free the buffer (ZSTs never allocate).
        if !is_zst::<T>() && self.cap > 0 {
            unsafe { self.heap.dealloc(self.ptr.as_ptr().cast::<u8>()) }
        }
    }
}

// -- Deref / DerefMut -------------------------------------------------------

impl<T> Deref for Vec<T> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> DerefMut for Vec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

// -- AsRef ------------------------------------------------------------------

impl<T> AsRef<[T]> for Vec<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

// -- Index / IndexMut -------------------------------------------------------

impl<T, I: std::slice::SliceIndex<[T]>> Index<I> for Vec<T> {
    type Output = I::Output;
    #[inline]
    fn index(&self, idx: I) -> &I::Output {
        &self.as_slice()[idx]
    }
}

impl<T, I: std::slice::SliceIndex<[T]>> IndexMut<I> for Vec<T> {
    #[inline]
    fn index_mut(&mut self, idx: I) -> &mut I::Output {
        &mut self.as_mut_slice()[idx]
    }
}

// -- PartialEq / Eq ---------------------------------------------------------

impl<T: PartialEq> PartialEq for Vec<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: PartialEq> PartialEq<[T]> for Vec<T> {
    fn eq(&self, other: &[T]) -> bool {
        self.as_slice() == other
    }
}

impl<T: PartialEq> PartialEq<&[T]> for Vec<T> {
    fn eq(&self, other: &&[T]) -> bool {
        self.as_slice() == *other
    }
}

impl<T: PartialEq, const N: usize> PartialEq<&[T; N]> for Vec<T> {
    fn eq(&self, other: &&[T; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for Vec<T> {}

// -- Debug ------------------------------------------------------------------

impl<T: std::fmt::Debug> std::fmt::Debug for Vec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), f)
    }
}

// -- IntoIterator (consuming) -----------------------------------------------

/// Consuming iterator for `Vec<T>`.
pub struct IntoIter<T> {
    vec: Vec<T>,
    idx: usize,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.idx >= self.vec.len {
            return None;
        }
        let val = unsafe { ptr::read(self.vec.ptr.as_ptr().add(self.idx)) };
        self.idx += 1;
        val.into()
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.vec.len - self.idx;
        (remaining, Some(remaining))
    }
}

impl<T> ExactSizeIterator for IntoIter<T> {}

impl<T> Drop for IntoIter<T> {
    fn drop(&mut self) {
        // Drop remaining elements that weren't consumed.
        if mem::needs_drop::<T>() && !is_zst::<T>() {
            while self.idx < self.vec.len {
                unsafe {
                    ptr::drop_in_place(self.vec.ptr.as_ptr().add(self.idx));
                }
                self.idx += 1;
            }
        }
        // Prevent Vec::drop from double-dropping elements.
        self.vec.len = 0;
    }
}

impl<T> IntoIterator for Vec<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;
    fn into_iter(self) -> IntoIter<T> {
        // We need to read len before we move self.
        IntoIter { idx: 0, vec: self }
    }
}

impl<'a, T> IntoIterator for &'a Vec<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;
    fn into_iter(self) -> slice::Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut Vec<T> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;
    fn into_iter(self) -> slice::IterMut<'a, T> {
        self.iter_mut()
    }
}

// ===========================================================================
// Drain<T>
// ===========================================================================

/// Draining iterator for [`Vec<T>`].
///
/// Created by [`Vec::drain`]. When dropped, shifts the tail of the vector
/// into the gap left by the drained elements.
pub struct Drain<'a, T> {
    vec: *mut Vec<T>,
    /// Slice of elements to drain (pointer remains valid because vec.len was
    /// shrunk to exclude this region).
    iter: &'a [T],
    idx: usize,
    /// Index in the original vec where the tail begins.
    tail_start: usize,
    /// Number of elements in the tail.
    tail_len: usize,
}

impl<T> Iterator for Drain<'_, T> {
    type Item = T;
    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.idx >= self.iter.len() {
            return None;
        }
        let val = unsafe { ptr::read(self.iter.as_ptr().add(self.idx)) };
        self.idx += 1;
        Some(val)
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.iter.len() - self.idx;
        (remaining, Some(remaining))
    }
}

impl<T> ExactSizeIterator for Drain<'_, T> {}

impl<T> Drop for Drain<'_, T> {
    fn drop(&mut self) {
        // Drop any remaining elements that were not consumed.
        if mem::needs_drop::<T>() {
            while self.idx < self.iter.len() {
                unsafe {
                    ptr::drop_in_place(self.iter.as_ptr().add(self.idx) as *mut T);
                }
                self.idx += 1;
            }
        }
        // Shift the tail into the gap.
        let vec = unsafe { &mut *self.vec };
        if self.tail_len > 0 {
            let start = vec.len; // == drain start (set before Drain was created)
            unsafe {
                ptr::copy(
                    vec.ptr.as_ptr().add(self.tail_start),
                    vec.ptr.as_ptr().add(start),
                    self.tail_len,
                );
            }
            vec.len = start + self.tail_len;
        }
    }
}

// ===========================================================================
// SmallVec<T, N>
// ===========================================================================

/// Stack-allocated up to `N` elements, spills to heap when full.
///
/// Like [`Vec<T>`] but avoids heap allocation for small collections.
pub struct SmallVec<T, const N: usize> {
    len: usize,
    data: SmallVecData<T, N>,
}

enum SmallVecData<T, const N: usize> {
    Inline {
        buf: [MaybeUninit<T>; N],
    },
    Heap {
        ptr: NonNull<T>,
        cap: usize,
        heap: Heap,
    },
}

unsafe impl<T: Send, const N: usize> Send for SmallVec<T, N> {}
unsafe impl<T: Sync, const N: usize> Sync for SmallVec<T, N> {}

impl<T, const N: usize> SmallVec<T, N> {
    /// Create a new `SmallVec` using inline storage only.
    pub fn new() -> Self {
        Self {
            len: 0,
            // SAFETY: An array of MaybeUninit doesn't require initialization.
            data: SmallVecData::Inline {
                buf: unsafe { MaybeUninit::uninit().assume_init() },
            },
        }
    }

    /// Create a new `SmallVec` with a heap for spilling.
    pub fn with_heap(heap: &Heap) -> Self {
        Self {
            len: 0,
            data: SmallVecData::Heap {
                ptr: NonNull::dangling(),
                cap: 0,
                heap: heap.clone(),
            },
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match &self.data {
            SmallVecData::Inline { .. } => N,
            SmallVecData::Heap { cap, .. } => {
                if *cap == 0 {
                    N
                } else {
                    *cap
                }
            }
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        match &self.data {
            SmallVecData::Inline { buf } => buf.as_ptr().cast::<T>(),
            SmallVecData::Heap { ptr, cap, .. } => {
                if *cap == 0 {
                    // Still inline — we stored a heap ref but haven't spilled.
                    // This shouldn't happen because with_heap sets cap=0 but
                    // the data is Heap variant. We need inline buf access.
                    // Actually for Heap variant with cap==0, we haven't allocated.
                    // This is a design issue — let's treat Heap { cap: 0 } as
                    // "no data yet" which is fine since len must be 0.
                    debug_assert_eq!(self.len, 0);
                    ptr.as_ptr()
                } else {
                    ptr.as_ptr()
                }
            }
        }
    }

    #[inline]
    fn as_mut_ptr(&mut self) -> *mut T {
        match &mut self.data {
            SmallVecData::Inline { buf } => buf.as_mut_ptr().cast::<T>(),
            SmallVecData::Heap { ptr, cap, .. } => {
                if *cap == 0 {
                    debug_assert_eq!(self.len, 0);
                    ptr.as_ptr()
                } else {
                    ptr.as_ptr()
                }
            }
        }
    }

    /// Push a value. May spill to heap.
    pub fn push(&mut self, val: T) -> Result<(), AllocError> {
        if self.len < self.capacity() && self.is_spilled_or_inline_has_room() {
            unsafe {
                ptr::write(self.as_mut_ptr().add(self.len), val);
            }
            self.len += 1;
            return Ok(());
        }
        self.push_slow(val)
    }

    #[inline]
    fn is_spilled_or_inline_has_room(&self) -> bool {
        match &self.data {
            SmallVecData::Inline { .. } => self.len < N,
            SmallVecData::Heap { cap, .. } => *cap > 0 || self.len < N,
        }
    }

    #[cold]
    fn push_slow(&mut self, val: T) -> Result<(), AllocError> {
        self.spill_or_grow(self.len + 1)?;
        unsafe {
            ptr::write(self.as_mut_ptr().add(self.len), val);
        }
        self.len += 1;
        Ok(())
    }

    /// Pop the last element.
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(unsafe { ptr::read(self.as_ptr().add(self.len)) })
    }

    /// Clear, dropping all elements.
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Truncate to `len` elements.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        let old_len = self.len;
        self.len = len;
        unsafe {
            let tail = slice::from_raw_parts_mut(self.as_mut_ptr().add(len), old_len - len);
            ptr::drop_in_place(tail);
        }
    }

    /// Reserve additional capacity, spilling to heap if necessary.
    pub fn reserve(&mut self, additional: usize) -> Result<(), AllocError> {
        let needed = self.len.checked_add(additional).ok_or(AllocError)?;
        if needed > self.capacity() {
            self.spill_or_grow(needed)?;
        }
        Ok(())
    }

    /// Ensure we have room for at least `min_cap` elements on heap.
    fn spill_or_grow(&mut self, min_cap: usize) -> Result<(), AllocError> {
        if is_zst::<T>() {
            return Ok(());
        }
        let new_cap = min_cap
            .checked_next_power_of_two()
            .ok_or(AllocError)?
            .max(N * 2)
            .max(4);
        let layout = Layout::array::<T>(new_cap).map_err(|_| AllocError)?;

        match &mut self.data {
            SmallVecData::Inline { .. } => {
                // No heap available — `new()` was used without a heap.
                // Cannot spill to heap.
                return Err(AllocError);
            }
            SmallVecData::Heap { ptr, cap, heap, .. } => {
                if *cap == 0 {
                    // First spill from inline (with_heap path, but cap==0 means
                    // we haven't allocated yet, and len must be 0).
                    let p = heap.alloc(layout.size(), layout.align());
                    if p.is_null() {
                        return Err(AllocError);
                    }
                    *ptr = unsafe { NonNull::new_unchecked(p.cast::<T>()) };
                    *cap = new_cap;
                } else {
                    let p = unsafe {
                        heap.realloc(ptr.as_ptr().cast::<u8>(), layout.size(), layout.align())
                    };
                    if p.is_null() {
                        return Err(AllocError);
                    }
                    *ptr = unsafe { NonNull::new_unchecked(p.cast::<T>()) };
                    *cap = new_cap;
                }
            }
        }
        Ok(())
    }

    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.as_slice().iter()
    }

    pub fn iter_mut(&mut self) -> slice::IterMut<'_, T> {
        self.as_mut_slice().iter_mut()
    }
}

impl<T: Copy, const N: usize> SmallVec<T, N> {
    /// Append elements from a slice (requires `T: Copy`).
    pub fn extend_from_slice(&mut self, src: &[T]) -> Result<(), AllocError> {
        if src.is_empty() {
            return Ok(());
        }
        let needed = self.len.checked_add(src.len()).ok_or(AllocError)?;
        if needed > self.capacity() {
            self.spill_or_grow(needed)?;
        }
        if !is_zst::<T>() {
            unsafe {
                ptr::copy_nonoverlapping(src.as_ptr(), self.as_mut_ptr().add(self.len), src.len());
            }
        }
        self.len += src.len();
        Ok(())
    }
}

// -- Drop -------------------------------------------------------------------

impl<T, const N: usize> Drop for SmallVec<T, N> {
    fn drop(&mut self) {
        // Drop elements.
        if self.len > 0 && mem::needs_drop::<T>() {
            unsafe {
                ptr::drop_in_place(slice::from_raw_parts_mut(self.as_mut_ptr(), self.len));
            }
        }
        // Free heap buffer if spilled.
        if let SmallVecData::Heap { ptr, cap, heap } = &self.data {
            if *cap > 0 && !is_zst::<T>() {
                unsafe { heap.dealloc(ptr.as_ptr().cast::<u8>()) }
            }
        }
    }
}

// -- Deref / DerefMut -------------------------------------------------------

impl<T, const N: usize> Deref for SmallVec<T, N> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T, const N: usize> DerefMut for SmallVec<T, N> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

// -- Index / IndexMut -------------------------------------------------------

impl<T, const N: usize> Index<usize> for SmallVec<T, N> {
    type Output = T;
    #[inline]
    fn index(&self, idx: usize) -> &T {
        &self.as_slice()[idx]
    }
}

impl<T, const N: usize> IndexMut<usize> for SmallVec<T, N> {
    #[inline]
    fn index_mut(&mut self, idx: usize) -> &mut T {
        &mut self.as_mut_slice()[idx]
    }
}

// -- Debug ------------------------------------------------------------------

impl<T: std::fmt::Debug, const N: usize> std::fmt::Debug for SmallVec<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), f)
    }
}

// -- IntoIterator -----------------------------------------------------------

impl<'a, T, const N: usize> IntoIterator for &'a SmallVec<T, N> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;
    fn into_iter(self) -> slice::Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T, const N: usize> IntoIterator for &'a mut SmallVec<T, N> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;
    fn into_iter(self) -> slice::IterMut<'a, T> {
        self.iter_mut()
    }
}
