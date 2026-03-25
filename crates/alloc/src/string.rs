//! Heap-backed UTF-8 string with fallible allocation.
//!
//! [`String`] mirrors `std::string::String` but is backed by [`BytesMut`],
//! allocating from a [`Heap`] arena with per-heap memory limits.

use std::borrow::{Borrow, BorrowMut, Cow};
use std::ops::{
    Add, AddAssign, Deref, DerefMut, Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive,
    RangeTo, RangeToInclusive,
};
use std::str::{self, Utf8Error};
use std::{cmp, fmt, hash};

use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::bytes::{Bytes, BytesMut};

/// A heap-backed, growable UTF-8 string.
///
/// This is the `bisque-alloc` equivalent of [`std::string::String`]. It wraps
/// [`BytesMut`] and maintains the UTF-8 invariant on every mutation. All
/// allocation is fallible — methods that may allocate return
/// `Result<_, AllocError>`.
///
/// # Examples
///
/// ```rust,no_run
/// use bisque_alloc::{HeapMaster, string::String};
///
/// let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
/// let heap = master.heap();
///
/// let mut s = String::new(&heap);
/// s.push_str("hello").unwrap();
/// s.push(' ').unwrap();
/// s.push_str("world").unwrap();
/// assert_eq!(&*s, "hello world");
/// ```
pub struct String {
    buf: BytesMut,
}

unsafe impl Send for String {}
unsafe impl Sync for String {}

// =========================================================================
// Constructors
// =========================================================================

impl String {
    /// Creates an empty `String` with no allocation.
    #[inline]
    pub fn new(heap: &Heap) -> Self {
        Self {
            buf: BytesMut::new(heap),
        }
    }

    /// Creates an empty `String` with the given capacity pre-allocated.
    #[inline]
    pub fn with_capacity(capacity: usize, heap: &Heap) -> Result<Self, AllocError> {
        Ok(Self {
            buf: BytesMut::with_capacity(capacity, heap)?,
        })
    }

    /// Creates a `String` from a byte vector, if it contains valid UTF-8.
    #[inline]
    pub fn from_utf8(bytes: BytesMut) -> Result<Self, FromUtf8Error> {
        match str::from_utf8(bytes.as_slice()) {
            Ok(_) => Ok(Self { buf: bytes }),
            Err(e) => Err(FromUtf8Error { bytes, error: e }),
        }
    }

    /// Creates a `String` from a byte vector without checking UTF-8 validity.
    ///
    /// # Safety
    ///
    /// The bytes must be valid UTF-8.
    #[inline]
    pub unsafe fn from_utf8_unchecked(bytes: BytesMut) -> Self {
        Self { buf: bytes }
    }

    /// Copies a `&str` into a new `String` allocated from the given heap.
    pub fn from_str(s: &str, heap: &Heap) -> Result<Self, AllocError> {
        let mut buf = BytesMut::with_capacity(s.len(), heap)?;
        buf.extend_from_slice(s.as_bytes())?;
        Ok(Self { buf })
    }

    /// Decodes a slice of bytes as UTF-8 and copies into a new `String`.
    pub fn from_utf8_lossy(v: &[u8], heap: &Heap) -> Result<Self, AllocError> {
        let cow = std::string::String::from_utf8_lossy(v);
        Self::from_str(&cow, heap)
    }
}

// =========================================================================
// Capacity & length
// =========================================================================

impl String {
    /// Returns the length of this string in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Returns `true` if the string has length zero.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Returns the capacity of the underlying buffer in bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Reserves capacity for at least `additional` more bytes.
    #[inline]
    pub fn reserve(&mut self, additional: usize) -> Result<(), AllocError> {
        self.buf.reserve(additional)
    }

    /// Truncates this string to the specified byte length.
    ///
    /// # Panics
    ///
    /// Panics if `new_len` is not on a UTF-8 character boundary, or if it is
    /// greater than the current length.
    #[inline]
    pub fn truncate(&mut self, new_len: usize) {
        if new_len < self.len() {
            assert!(
                self.is_char_boundary(new_len),
                "new_len {new_len} is not a char boundary"
            );
            self.buf.truncate(new_len);
        }
    }

    /// Clears the string, removing all contents.
    #[inline]
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Shrinks the string to the specified byte length.
    ///
    /// If `new_len` is greater than the current length, this has no effect.
    ///
    /// # Panics
    ///
    /// Panics if `new_len` does not lie on a [`char`] boundary.
    #[inline]
    pub fn shrink_to(&mut self, new_len: usize) {
        if new_len < self.len() {
            self.truncate(new_len);
        }
    }
}

// =========================================================================
// Mutation
// =========================================================================

impl String {
    /// Appends a string slice to the end of this string.
    #[inline]
    pub fn push_str(&mut self, s: &str) -> Result<(), AllocError> {
        self.buf.extend_from_slice(s.as_bytes())
    }

    /// Appends a single character to the end of this string.
    #[inline]
    pub fn push(&mut self, ch: char) -> Result<(), AllocError> {
        let mut tmp = [0u8; 4];
        let s = ch.encode_utf8(&mut tmp);
        self.push_str(s)
    }

    /// Removes the last character from the string and returns it.
    ///
    /// Returns `None` if the string is empty.
    pub fn pop(&mut self) -> Option<char> {
        let ch = self.chars().next_back()?;
        let new_len = self.len() - ch.len_utf8();
        // SAFETY: we're truncating to a known char boundary.
        self.buf.truncate(new_len);
        Some(ch)
    }

    /// Removes a character at the given byte index and returns it.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is not on a character boundary or is out of bounds.
    pub fn remove(&mut self, idx: usize) -> char {
        let ch = match self[idx..].chars().next() {
            Some(ch) => ch,
            None => panic!("cannot remove a char from the end of a string"),
        };
        let next = idx + ch.len_utf8();
        let len = self.len();
        let slice = unsafe { self.as_bytes_mut() };
        slice.copy_within(next..len, idx);
        self.buf.truncate(len - (next - idx));
        ch
    }

    /// Inserts a character at the given byte index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is not on a character boundary or is out of bounds.
    pub fn insert(&mut self, idx: usize, ch: char) -> Result<(), AllocError> {
        assert!(self.is_char_boundary(idx));
        let mut tmp = [0u8; 4];
        let s = ch.encode_utf8(&mut tmp);
        self.insert_str(idx, s)
    }

    /// Inserts a string slice at the given byte index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is not on a character boundary or is out of bounds.
    pub fn insert_str(&mut self, idx: usize, string: &str) -> Result<(), AllocError> {
        assert!(self.is_char_boundary(idx));
        let insert_len = string.len();
        if insert_len == 0 {
            return Ok(());
        }
        let old_len = self.len();
        // Reserve space without writing junk bytes.
        self.buf.reserve(insert_len)?;
        unsafe {
            let ptr = self.buf.as_mut_slice().as_mut_ptr();
            // Shift tail right to make room.
            std::ptr::copy(ptr.add(idx), ptr.add(idx + insert_len), old_len - idx);
            // Write inserted bytes into the gap.
            std::ptr::copy_nonoverlapping(string.as_ptr(), ptr.add(idx), insert_len);
            self.buf.set_len(old_len + insert_len);
        }
        Ok(())
    }

    /// Retains only the characters specified by the predicate.
    ///
    /// Single-pass O(n) implementation: walks the string once, copying kept
    /// characters forward over removed gaps.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(char) -> bool,
    {
        let len = self.len();
        let mut del_bytes = 0;
        let mut idx = 0;

        // SAFETY: we only move bytes left (never right), always on char
        // boundaries, so the buffer remains valid UTF-8 at every step.
        unsafe {
            let slice = self.as_bytes_mut();
            while idx < len {
                let ch = str::from_utf8_unchecked(&slice[idx..])
                    .chars()
                    .next()
                    .unwrap();
                let ch_len = ch.len_utf8();

                if !f(ch) {
                    del_bytes += ch_len;
                } else if del_bytes > 0 {
                    slice.copy_within(idx..idx + ch_len, idx - del_bytes);
                }
                idx += ch_len;
            }
        }
        if del_bytes > 0 {
            self.buf.truncate(len - del_bytes);
        }
    }

    /// Removes the specified range from the string and returns it as an
    /// iterator.
    ///
    /// Note: Unlike `std`, this eagerly removes the range because our
    /// allocation model doesn't support the lazy `Drain` iterator pattern
    /// cleanly. Returns the drained substring as a `std::string::String`.
    pub fn drain(&mut self, range: Range<usize>) -> DrainResult {
        let s = &self[range.clone()];
        let drained = std::string::String::from(s);
        let len = self.len();
        let slice = unsafe { self.as_bytes_mut() };
        slice.copy_within(range.end..len, range.start);
        self.buf.truncate(len - (range.end - range.start));
        DrainResult {
            buf: drained,
            idx: 0,
        }
    }

    /// Replaces a range with the given string slice.
    pub fn replace_range(
        &mut self,
        range: Range<usize>,
        replace_with: &str,
    ) -> Result<(), AllocError> {
        assert!(self.is_char_boundary(range.start));
        assert!(self.is_char_boundary(range.end));

        let old_range_len = range.end - range.start;
        let repl_len = replace_with.len();
        let old_total = self.len();
        let final_len = old_total - old_range_len + repl_len;
        let tail_len = old_total - range.end;

        if repl_len > old_range_len {
            // Growing — reserve without zero-filling.
            self.buf.reserve(repl_len - old_range_len)?;
        }

        unsafe {
            let ptr = self.buf.as_mut_slice().as_mut_ptr();
            // Shift tail to its final position.
            std::ptr::copy(
                ptr.add(range.end),
                ptr.add(range.start + repl_len),
                tail_len,
            );
            // Write replacement.
            std::ptr::copy_nonoverlapping(replace_with.as_ptr(), ptr.add(range.start), repl_len);
            self.buf.set_len(final_len);
        }
        Ok(())
    }

    /// Converts this string into a [`Bytes`] (immutable, reference-counted).
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.buf.freeze()
    }

    /// Consumes and returns the underlying [`BytesMut`].
    #[inline]
    pub fn into_bytes_mut(self) -> BytesMut {
        self.buf
    }

    /// Returns a mutable reference to the underlying byte buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the bytes remain valid UTF-8.
    #[inline]
    pub unsafe fn as_mut_bytes(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Returns a byte slice of this string's contents.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_slice()
    }

    /// Returns a mutable byte slice of this string's contents.
    ///
    /// # Safety
    ///
    /// The caller must ensure the bytes remain valid UTF-8.
    #[inline]
    pub unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }

    /// Returns the string as a `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: we maintain the UTF-8 invariant.
        unsafe { str::from_utf8_unchecked(self.buf.as_slice()) }
    }

    /// Returns the string as a `&mut str`.
    #[inline]
    pub fn as_mut_str(&mut self) -> &mut str {
        // SAFETY: we maintain the UTF-8 invariant.
        unsafe { str::from_utf8_unchecked_mut(self.buf.as_mut_slice()) }
    }
}

// =========================================================================
// Drain result (simplified vs std::string::Drain)
// =========================================================================

/// The result of [`String::drain`]. Contains the removed substring.
pub struct DrainResult {
    buf: std::string::String,
    idx: usize,
}

impl DrainResult {
    /// Returns the remaining (not-yet-iterated) drained string.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.buf[self.idx..]
    }

    /// Consumes and returns the full drained string.
    #[inline]
    pub fn into_string(self) -> std::string::String {
        self.buf
    }
}

impl fmt::Display for DrainResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Iterator for DrainResult {
    type Item = char;
    #[inline]
    fn next(&mut self) -> Option<char> {
        let ch = self.buf[self.idx..].chars().next()?;
        self.idx += ch.len_utf8();
        Some(ch)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.buf.len() - self.idx;
        // At least 1 byte per char, at most 4 bytes per char.
        ((remaining + 3) / 4, Some(remaining))
    }
}

// =========================================================================
// FromUtf8Error
// =========================================================================

/// Error returned by [`String::from_utf8`].
#[derive(Debug)]
pub struct FromUtf8Error {
    bytes: BytesMut,
    error: Utf8Error,
}

impl FromUtf8Error {
    /// Returns the bytes that were attempted to be converted.
    pub fn into_bytes(self) -> BytesMut {
        self.bytes
    }

    /// Returns the UTF-8 error.
    pub fn utf8_error(&self) -> &Utf8Error {
        &self.error
    }
}

impl fmt::Display for FromUtf8Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for FromUtf8Error {}

// =========================================================================
// Deref / DerefMut → str
// =========================================================================

impl Deref for String {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl DerefMut for String {
    #[inline]
    fn deref_mut(&mut self) -> &mut str {
        self.as_mut_str()
    }
}

// =========================================================================
// AsRef / AsMut / Borrow
// =========================================================================

impl AsRef<str> for String {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for String {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsMut<str> for String {
    #[inline]
    fn as_mut(&mut self) -> &mut str {
        self.as_mut_str()
    }
}

impl Borrow<str> for String {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl BorrowMut<str> for String {
    #[inline]
    fn borrow_mut(&mut self) -> &mut str {
        self.as_mut_str()
    }
}

// =========================================================================
// Index impls — delegate to str
// =========================================================================

impl Index<Range<usize>> for String {
    type Output = str;
    #[inline]
    fn index(&self, index: Range<usize>) -> &str {
        &self.as_str()[index]
    }
}

impl Index<RangeFrom<usize>> for String {
    type Output = str;
    #[inline]
    fn index(&self, index: RangeFrom<usize>) -> &str {
        &self.as_str()[index]
    }
}

impl Index<RangeTo<usize>> for String {
    type Output = str;
    #[inline]
    fn index(&self, index: RangeTo<usize>) -> &str {
        &self.as_str()[index]
    }
}

impl Index<RangeInclusive<usize>> for String {
    type Output = str;
    #[inline]
    fn index(&self, index: RangeInclusive<usize>) -> &str {
        &self.as_str()[index]
    }
}

impl Index<RangeToInclusive<usize>> for String {
    type Output = str;
    #[inline]
    fn index(&self, index: RangeToInclusive<usize>) -> &str {
        &self.as_str()[index]
    }
}

impl Index<RangeFull> for String {
    type Output = str;
    #[inline]
    fn index(&self, _: RangeFull) -> &str {
        self.as_str()
    }
}

impl IndexMut<Range<usize>> for String {
    #[inline]
    fn index_mut(&mut self, index: Range<usize>) -> &mut str {
        &mut self.as_mut_str()[index]
    }
}

impl IndexMut<RangeFrom<usize>> for String {
    #[inline]
    fn index_mut(&mut self, index: RangeFrom<usize>) -> &mut str {
        &mut self.as_mut_str()[index]
    }
}

impl IndexMut<RangeTo<usize>> for String {
    #[inline]
    fn index_mut(&mut self, index: RangeTo<usize>) -> &mut str {
        &mut self.as_mut_str()[index]
    }
}

impl IndexMut<RangeInclusive<usize>> for String {
    #[inline]
    fn index_mut(&mut self, index: RangeInclusive<usize>) -> &mut str {
        &mut self.as_mut_str()[index]
    }
}

impl IndexMut<RangeToInclusive<usize>> for String {
    #[inline]
    fn index_mut(&mut self, index: RangeToInclusive<usize>) -> &mut str {
        &mut self.as_mut_str()[index]
    }
}

impl IndexMut<RangeFull> for String {
    #[inline]
    fn index_mut(&mut self, _: RangeFull) -> &mut str {
        self.as_mut_str()
    }
}

// =========================================================================
// Comparison traits
// =========================================================================

impl PartialEq for String {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for String {}

impl PartialOrd for String {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for String {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl hash::Hash for String {
    #[inline]
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

// Cross-type PartialEq
impl PartialEq<str> for String {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<String> for str {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<&str> for String {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for &str {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        *self == other.as_str()
    }
}

impl PartialEq<std::string::String> for String {
    #[inline]
    fn eq(&self, other: &std::string::String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<String> for std::string::String {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<Cow<'a, str>> for String {
    #[inline]
    fn eq(&self, other: &Cow<'a, str>) -> bool {
        self.as_str() == other.as_ref()
    }
}

impl<'a> PartialEq<String> for Cow<'a, str> {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        self.as_ref() == other.as_str()
    }
}

// Cross-type PartialOrd
impl PartialOrd<str> for String {
    #[inline]
    fn partial_cmp(&self, other: &str) -> Option<cmp::Ordering> {
        self.as_str().partial_cmp(other)
    }
}

impl PartialOrd<String> for str {
    #[inline]
    fn partial_cmp(&self, other: &String) -> Option<cmp::Ordering> {
        self.partial_cmp(other.as_str())
    }
}

// =========================================================================
// Display / Debug / Write
// =========================================================================

impl fmt::Display for String {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for String {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Write for String {
    #[inline]
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push_str(s).map_err(|_| fmt::Error)
    }

    #[inline]
    fn write_char(&mut self, c: char) -> fmt::Result {
        self.push(c).map_err(|_| fmt::Error)
    }
}

// =========================================================================
// Clone
// =========================================================================

impl Clone for String {
    fn clone(&self) -> Self {
        Self {
            buf: self.buf.clone(),
        }
    }
}

// =========================================================================
// From / Into conversions
// =========================================================================

impl From<String> for BytesMut {
    #[inline]
    fn from(s: String) -> Self {
        s.buf
    }
}

impl From<String> for Bytes {
    #[inline]
    fn from(s: String) -> Self {
        s.buf.freeze()
    }
}

// =========================================================================
// Add / AddAssign
// =========================================================================

impl Add<&str> for String {
    type Output = String;
    #[inline]
    fn add(mut self, other: &str) -> String {
        self.push_str(other).expect("add OOM");
        self
    }
}

impl AddAssign<&str> for String {
    #[inline]
    fn add_assign(&mut self, other: &str) {
        self.push_str(other).expect("add_assign OOM");
    }
}

// =========================================================================
// Extend
// =========================================================================

impl Extend<char> for String {
    fn extend<I: IntoIterator<Item = char>>(&mut self, iter: I) {
        for ch in iter {
            self.push(ch).expect("extend OOM");
        }
    }
}

impl<'a> Extend<&'a char> for String {
    fn extend<I: IntoIterator<Item = &'a char>>(&mut self, iter: I) {
        for &ch in iter {
            self.push(ch).expect("extend OOM");
        }
    }
}

impl<'a> Extend<&'a str> for String {
    fn extend<I: IntoIterator<Item = &'a str>>(&mut self, iter: I) {
        for s in iter {
            self.push_str(s).expect("extend OOM");
        }
    }
}

impl Extend<std::string::String> for String {
    fn extend<I: IntoIterator<Item = std::string::String>>(&mut self, iter: I) {
        for s in iter {
            self.push_str(&s).expect("extend OOM");
        }
    }
}

impl<'a> Extend<Cow<'a, str>> for String {
    fn extend<I: IntoIterator<Item = Cow<'a, str>>>(&mut self, iter: I) {
        for s in iter {
            self.push_str(&s).expect("extend OOM");
        }
    }
}

// =========================================================================
// Iterator — chars, bytes, etc. come from Deref<Target=str>
// =========================================================================

impl<'a> IntoIterator for &'a String {
    type Item = char;
    type IntoIter = std::str::Chars<'a>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.chars()
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    fn heap() -> Heap {
        HeapMaster::new(1024 * 1024).unwrap().heap()
    }

    #[test]
    fn new_is_empty() {
        let s = String::new(&heap());
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!(s.as_str(), "");
    }

    #[test]
    fn from_str_roundtrip() {
        let s = String::from_str("hello", &heap()).unwrap();
        assert_eq!(&*s, "hello");
        assert_eq!(s.len(), 5);
    }

    #[test]
    fn push_and_push_str() {
        let mut s = String::new(&heap());
        s.push_str("hello").unwrap();
        s.push(' ').unwrap();
        s.push_str("world").unwrap();
        assert_eq!(&*s, "hello world");
    }

    #[test]
    fn pop_char() {
        let mut s = String::from_str("café", &heap()).unwrap();
        assert_eq!(s.pop(), Some('é'));
        assert_eq!(&*s, "caf");
        assert_eq!(s.pop(), Some('f'));
    }

    #[test]
    fn pop_empty() {
        let mut s = String::new(&heap());
        assert_eq!(s.pop(), None);
    }

    #[test]
    fn remove_char() {
        let mut s = String::from_str("hello", &heap()).unwrap();
        assert_eq!(s.remove(1), 'e');
        assert_eq!(&*s, "hllo");
    }

    #[test]
    fn insert_char() {
        let mut s = String::from_str("hllo", &heap()).unwrap();
        s.insert(1, 'e').unwrap();
        assert_eq!(&*s, "hello");
    }

    #[test]
    fn insert_str() {
        let mut s = String::from_str("hd", &heap()).unwrap();
        s.insert_str(1, "ello worl").unwrap();
        assert_eq!(&*s, "hello world");
    }

    #[test]
    fn truncate() {
        let mut s = String::from_str("hello", &heap()).unwrap();
        s.truncate(3);
        assert_eq!(&*s, "hel");
    }

    #[test]
    #[should_panic]
    fn truncate_not_char_boundary() {
        let mut s = String::from_str("café", &heap()).unwrap();
        s.truncate(4); // 'é' is 2 bytes, so byte 4 is mid-character
    }

    #[test]
    fn retain() {
        let mut s = String::from_str("hello world", &heap()).unwrap();
        s.retain(|c| c != 'l');
        assert_eq!(&*s, "heo word");
    }

    #[test]
    fn drain_range() {
        let mut s = String::from_str("hello world", &heap()).unwrap();
        let drained = s.drain(5..11);
        assert_eq!(drained.as_str(), " world");
        assert_eq!(&*s, "hello");
    }

    #[test]
    fn replace_range_same_len() {
        let mut s = String::from_str("hello world", &heap()).unwrap();
        s.replace_range(6..11, "earth").unwrap();
        assert_eq!(&*s, "hello earth");
    }

    #[test]
    fn replace_range_grow() {
        let mut s = String::from_str("hi", &heap()).unwrap();
        s.replace_range(0..2, "hello world").unwrap();
        assert_eq!(&*s, "hello world");
    }

    #[test]
    fn replace_range_shrink() {
        let mut s = String::from_str("hello world", &heap()).unwrap();
        s.replace_range(0..11, "hi").unwrap();
        assert_eq!(&*s, "hi");
    }

    #[test]
    fn with_capacity() {
        let s = String::with_capacity(100, &heap()).unwrap();
        assert!(s.capacity() >= 100);
        assert!(s.is_empty());
    }

    #[test]
    fn clone_is_independent() {
        let s = String::from_str("hello", &heap()).unwrap();
        let mut s2 = s.clone();
        s2.push_str(" world").unwrap();
        assert_eq!(&*s, "hello");
        assert_eq!(&*s2, "hello world");
    }

    #[test]
    fn from_utf8_valid() {
        let h = heap();
        let mut buf = BytesMut::with_capacity(5, &h).unwrap();
        buf.extend_from_slice(b"hello").unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(&*s, "hello");
    }

    #[test]
    fn from_utf8_invalid() {
        let h = heap();
        let mut buf = BytesMut::with_capacity(4, &h).unwrap();
        buf.extend_from_slice(&[0xff, 0xfe]).unwrap();
        assert!(String::from_utf8(buf).is_err());
    }

    #[test]
    fn into_bytes() {
        let s = String::from_str("hello", &heap()).unwrap();
        let b: Bytes = s.into_bytes();
        assert_eq!(b.as_slice(), b"hello");
    }

    #[test]
    fn display_and_debug() {
        let s = String::from_str("hello", &heap()).unwrap();
        assert_eq!(format!("{s}"), "hello");
        assert_eq!(format!("{s:?}"), "\"hello\"");
    }

    #[test]
    fn eq_with_str() {
        let s = String::from_str("hello", &heap()).unwrap();
        assert_eq!(s, "hello");
        assert_eq!("hello", s);
        assert_eq!(s, *"hello");
    }

    #[test]
    fn eq_with_std_string() {
        let s = String::from_str("hello", &heap()).unwrap();
        let std_s = std::string::String::from("hello");
        assert_eq!(s, std_s);
        assert_eq!(std_s, s);
    }

    #[test]
    fn add_and_add_assign() {
        let mut s = String::from_str("hello", &heap()).unwrap();
        s += " world";
        assert_eq!(&*s, "hello world");

        let s2 = String::from_str("foo", &heap()).unwrap();
        let s3 = s2 + "bar";
        assert_eq!(&*s3, "foobar");
    }

    #[test]
    fn extend_chars() {
        let mut s = String::new(&heap());
        s.extend(['h', 'e', 'l', 'l', 'o']);
        assert_eq!(&*s, "hello");
    }

    #[test]
    fn extend_strs() {
        let mut s = String::new(&heap());
        s.extend(["hello", " ", "world"]);
        assert_eq!(&*s, "hello world");
    }

    #[test]
    fn fmt_write() {
        use std::fmt::Write;
        let mut s = String::new(&heap());
        write!(s, "hello {}", 42).unwrap();
        assert_eq!(&*s, "hello 42");
    }

    #[test]
    fn into_iterator() {
        let s = String::from_str("hi", &heap()).unwrap();
        let chars: Vec<char> = s.into_iter().collect();
        assert_eq!(chars, vec!['h', 'i']);
    }

    #[test]
    fn index_range() {
        let s = String::from_str("hello world", &heap()).unwrap();
        assert_eq!(&s[0..5], "hello");
        assert_eq!(&s[6..], "world");
        assert_eq!(&s[..5], "hello");
    }

    #[test]
    fn multibyte_chars() {
        let mut s = String::from_str("こんにちは", &heap()).unwrap();
        assert_eq!(s.len(), 15); // 5 × 3 bytes
        assert_eq!(s.pop(), Some('は'));
        assert_eq!(&*s, "こんにち");
    }

    #[test]
    fn from_utf8_lossy() {
        let h = heap();
        let s = String::from_utf8_lossy(b"hello \xff world", &h).unwrap();
        assert!(s.contains('\u{FFFD}'));
    }

    #[test]
    fn ord_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = String::from_str("abc", &heap()).unwrap();
        let b = String::from_str("xyz", &heap()).unwrap();
        assert!(a < b);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        "abc".hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }
}
