#![allow(clippy::missing_safety_doc)]

use std::borrow::{Borrow, BorrowMut};
use std::mem::MaybeUninit;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU32, Ordering, fence};
use std::{cmp, fmt, ptr, slice};

use bisque_mimalloc_sys as ffi;

use crate::{Heap, HeapMaster};

// =========================================================================
// Constants
// =========================================================================

const TAG_INLINE: u8 = 0;
const TAG_HEAP: u8 = 1;
const TAG_STATIC: u8 = 2;
/// Maximum bytes storable inline (no heap allocation).
const INLINE_CAP: usize = 30;

/// Bit 31 of ref_cnt: frozen flag. When set, the buffer is immutable.
const FROZEN_BIT: u32 = 1 << 31;
const COUNT_MASK: u32 = !FROZEN_BIT;

// =========================================================================
// Header
// =========================================================================

#[repr(C)]
struct Header {
    ref_cnt: AtomicU32,
    cap: u32,
    heap: Heap,
}

const HEADER_SIZE: usize = std::mem::size_of::<Header>();

impl Header {
    #[inline]
    fn data_ptr(&self) -> *mut u8 {
        unsafe { (self as *const Self as *mut u8).add(HEADER_SIZE) }
    }

    #[inline]
    fn count(&self) -> u32 {
        self.ref_cnt.load(Ordering::Acquire) & COUNT_MASK
    }
}

// =========================================================================
// Stack representation — 32 bytes, tag at byte 31
// =========================================================================

#[repr(C)]
#[derive(Clone, Copy)]
struct InlineRepr {
    data: [u8; INLINE_CAP],
    len: u8,
    tag: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct OwnedRepr {
    ptr: *const u8,
    len: usize,
    header: *mut Header,
    _pad: [u8; 7],
    tag: u8,
}

#[repr(C)]
union Repr {
    inline: InlineRepr,
    owned: OwnedRepr,
    raw: [u8; 32],
}

const _: () = assert!(std::mem::size_of::<Repr>() == 32);

impl Repr {
    #[inline]
    fn tag(&self) -> u8 {
        unsafe { self.raw[31] }
    }
}

impl Clone for Repr {
    #[inline]
    fn clone(&self) -> Self {
        unsafe {
            let mut dst = Repr { raw: [0; 32] };
            ptr::copy_nonoverlapping(self.raw.as_ptr(), dst.raw.as_mut_ptr(), 32);
            dst
        }
    }
}

// =========================================================================
// MutRepr — 32-byte union for BytesMut (inline + heap + stash)
// =========================================================================

/// Inline variant for BytesMut. Stores up to 22 bytes + a Heap handle.
/// The Heap handle is always available for promotion to heap mode.
///
/// Layout: `[heap:8][data:22][len:1][tag:1]` — tag at byte 31, matching all reprs.
const MUT_INLINE_CAP: usize = 22;

#[repr(C)]
struct MutInlineRepr {
    heap: Heap,                 // 8  (offset 0, 8-aligned)
    data: [u8; MUT_INLINE_CAP], // 22 (offset 8)
    len: u8,                    // 1  (offset 30)
    tag: u8,                    // 1  (offset 31) = 32
}

/// Heap variant for BytesMut. Uses u32 len/cap (max 4 GiB) to fit in 32 bytes.
/// The `Heap` handle is stored in the `Header` on the heap allocation, not here.
#[repr(C)]
#[derive(Clone, Copy)]
struct MutOwnedRepr {
    ptr: *mut u8,        // 8
    len: u32,            // 4
    cap: u32,            // 4
    header: *mut Header, // 8
    _pad: [u8; 7],       // 7
    tag: u8,             // 1 = 32
}

#[repr(C)]
union MutRepr {
    inline: std::mem::ManuallyDrop<MutInlineRepr>,
    owned: MutOwnedRepr,
    raw: [u8; 32],
}

const _: () = assert!(std::mem::size_of::<MutRepr>() == 32);
const _: () = assert!(std::mem::size_of::<MutInlineRepr>() == 32);

impl MutRepr {
    #[inline]
    fn tag(&self) -> u8 {
        unsafe { self.raw[31] }
    }
}

/// Resolve a `RangeBounds` to a concrete `(start, end)` within `[0, len)`.
fn resolve_range(range: impl RangeBounds<usize>, len: usize) -> (usize, usize) {
    let start = match range.start_bound() {
        std::ops::Bound::Included(&n) => n,
        std::ops::Bound::Excluded(&n) => n + 1,
        std::ops::Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        std::ops::Bound::Included(&n) => n + 1,
        std::ops::Bound::Excluded(&n) => n,
        std::ops::Bound::Unbounded => len,
    };
    assert!(
        start <= end && end <= len,
        "range {start}..{end} out of bounds for length {len}"
    );
    (start, end)
}

// =========================================================================
// Bytes
// =========================================================================

/// Immutable byte buffer. 32 bytes on the stack.
///
/// Three modes: inline (≤30B), heap (refcounted, arena-backed), static.
pub struct Bytes {
    repr: Repr,
}

unsafe impl Send for Bytes {}
unsafe impl Sync for Bytes {}

impl Bytes {
    #[inline]
    pub const fn new() -> Self {
        Self {
            repr: Repr {
                inline: InlineRepr {
                    data: [0; INLINE_CAP],
                    len: 0,
                    tag: TAG_INLINE,
                },
            },
        }
    }

    #[inline]
    pub const fn from_static(data: &'static [u8]) -> Self {
        Self {
            repr: Repr {
                owned: OwnedRepr {
                    ptr: data.as_ptr(),
                    len: data.len(),
                    header: ptr::null_mut(),
                    _pad: [0; 7],
                    tag: TAG_STATIC,
                },
            },
        }
    }

    /// Copy a slice into a new `Bytes`. Inline if ≤30 bytes, otherwise single heap alloc.
    pub fn copy_from_slice(
        data: &[u8],
        heap: &Heap,
    ) -> Result<Self, allocator_api2::alloc::AllocError> {
        if data.len() <= INLINE_CAP {
            let mut inline = InlineRepr {
                data: [0; INLINE_CAP],
                len: data.len() as u8,
                tag: TAG_INLINE,
            };
            inline.data[..data.len()].copy_from_slice(data);
            return Ok(Self {
                repr: Repr { inline },
            });
        }
        let (header, data_ptr) = alloc_header_and_data(heap, data.len())?;
        unsafe {
            (*header).ref_cnt.store(1 | FROZEN_BIT, Ordering::Relaxed);
            ptr::copy_nonoverlapping(data.as_ptr(), data_ptr, data.len());
        }
        Ok(Self {
            repr: Repr {
                owned: OwnedRepr {
                    ptr: data_ptr,
                    len: data.len(),
                    header,
                    _pad: [0; 7],
                    tag: TAG_HEAP,
                },
            },
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self.repr.tag() {
            TAG_INLINE => unsafe { self.repr.inline.len as usize },
            _ => unsafe { self.repr.owned.len },
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self.repr.tag() {
            TAG_INLINE => unsafe { &self.repr.inline.data[..self.repr.inline.len as usize] },
            _ => unsafe { slice::from_raw_parts(self.repr.owned.ptr, self.repr.owned.len) },
        }
    }

    /// Truncate to `len` bytes. No-op if `len >= self.len()`.
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len() {
            return;
        }
        unsafe {
            match self.repr.tag() {
                TAG_INLINE => {
                    self.repr.inline.len = len as u8;
                }
                _ => {
                    self.repr.owned.len = len;
                }
            }
        }
    }

    /// Clear all bytes. Equivalent to `truncate(0)`.
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Create a sub-slice. Accepts any range type (`0..5`, `..5`, `5..`, `..`, `0..=4`).
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let (start, end) = resolve_range(range, self.len());
        let data = self.as_slice();
        match self.repr.tag() {
            TAG_INLINE | TAG_STATIC => {
                let sub = &data[start..end];
                if sub.len() <= INLINE_CAP {
                    let mut inline = InlineRepr {
                        data: [0; INLINE_CAP],
                        len: sub.len() as u8,
                        tag: TAG_INLINE,
                    };
                    inline.data[..sub.len()].copy_from_slice(sub);
                    Self {
                        repr: Repr { inline },
                    }
                } else {
                    debug_assert_eq!(self.repr.tag(), TAG_STATIC);
                    unsafe {
                        Self {
                            repr: Repr {
                                owned: OwnedRepr {
                                    ptr: self.repr.owned.ptr.add(start),
                                    len: end - start,
                                    header: ptr::null_mut(),
                                    _pad: [0; 7],
                                    tag: TAG_STATIC,
                                },
                            },
                        }
                    }
                }
            }
            TAG_HEAP => {
                let header = unsafe { &*self.repr.owned.header };
                header.ref_cnt.fetch_add(1, Ordering::Relaxed);
                unsafe {
                    Self {
                        repr: Repr {
                            owned: OwnedRepr {
                                ptr: self.repr.owned.ptr.add(start),
                                len: end - start,
                                header: self.repr.owned.header,
                                _pad: [0; 7],
                                tag: TAG_HEAP,
                            },
                        },
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    /// Get a `Bytes` reference from an interior sub-slice pointer.
    ///
    /// # Panics
    /// Panics if `subset` is not within the bounds of `self`.
    pub fn slice_ref(&self, subset: &[u8]) -> Self {
        if subset.is_empty() {
            return Self::new();
        }
        let self_ptr = self.as_slice().as_ptr() as usize;
        let self_end = self_ptr + self.len();
        let sub_ptr = subset.as_ptr() as usize;
        let sub_end = sub_ptr + subset.len();
        assert!(
            sub_ptr >= self_ptr && sub_end <= self_end,
            "subset is not within Bytes bounds"
        );
        let start = sub_ptr - self_ptr;
        let end = start + subset.len();
        self.slice(start..end)
    }

    /// Split at position, returning `[0..at)`, leaving `[at..len)` in self.
    pub fn split_to(&mut self, at: usize) -> Self {
        let len = self.len();
        assert!(at <= len);
        match self.repr.tag() {
            TAG_HEAP => unsafe {
                let header = &*self.repr.owned.header;
                header.ref_cnt.fetch_add(1, Ordering::Relaxed);
                let base_ptr = self.repr.owned.ptr;
                let head = Self {
                    repr: Repr {
                        owned: OwnedRepr {
                            ptr: base_ptr,
                            len: at,
                            header: self.repr.owned.header,
                            _pad: [0; 7],
                            tag: TAG_HEAP,
                        },
                    },
                };
                self.repr.owned.ptr = base_ptr.add(at);
                self.repr.owned.len = len - at;
                head
            },
            TAG_INLINE => {
                let data = unsafe { &self.repr.inline.data[..len] };
                let mut head = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: at as u8,
                    tag: TAG_INLINE,
                };
                head.data[..at].copy_from_slice(&data[..at]);
                let mut tail = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: (len - at) as u8,
                    tag: TAG_INLINE,
                };
                tail.data[..len - at].copy_from_slice(&data[at..]);
                self.repr = Repr { inline: tail };
                Self {
                    repr: Repr { inline: head },
                }
            }
            TAG_STATIC => unsafe {
                let base_ptr = self.repr.owned.ptr;
                let head = Self {
                    repr: Repr {
                        owned: OwnedRepr {
                            ptr: base_ptr,
                            len: at,
                            header: ptr::null_mut(),
                            _pad: [0; 7],
                            tag: TAG_STATIC,
                        },
                    },
                };
                self.repr.owned.ptr = base_ptr.add(at);
                self.repr.owned.len = len - at;
                head
            },
            _ => unreachable!(),
        }
    }

    /// Split at position, returning `[at..len)`, leaving `[0..at)` in self.
    pub fn split_off(&mut self, at: usize) -> Self {
        let len = self.len();
        assert!(at <= len);
        match self.repr.tag() {
            TAG_HEAP => unsafe {
                let header = &*self.repr.owned.header;
                header.ref_cnt.fetch_add(1, Ordering::Relaxed);
                let base_ptr = self.repr.owned.ptr;
                let tail = Self {
                    repr: Repr {
                        owned: OwnedRepr {
                            ptr: base_ptr.add(at),
                            len: len - at,
                            header: self.repr.owned.header,
                            _pad: [0; 7],
                            tag: TAG_HEAP,
                        },
                    },
                };
                self.repr.owned.len = at;
                tail
            },
            TAG_INLINE => {
                let data = unsafe { &self.repr.inline.data[..len] };
                let mut head = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: at as u8,
                    tag: TAG_INLINE,
                };
                head.data[..at].copy_from_slice(&data[..at]);
                let mut tail = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: (len - at) as u8,
                    tag: TAG_INLINE,
                };
                tail.data[..len - at].copy_from_slice(&data[at..]);
                self.repr = Repr { inline: head };
                Self {
                    repr: Repr { inline: tail },
                }
            }
            TAG_STATIC => unsafe {
                let base_ptr = self.repr.owned.ptr;
                let tail = Self {
                    repr: Repr {
                        owned: OwnedRepr {
                            ptr: base_ptr.add(at),
                            len: len - at,
                            header: ptr::null_mut(),
                            _pad: [0; 7],
                            tag: TAG_STATIC,
                        },
                    },
                };
                self.repr.owned.len = at;
                tail
            },
            _ => unreachable!(),
        }
    }

    /// Try to convert back to mutable. Succeeds only for heap mode with refcount == 1.
    pub fn try_mut(self) -> Result<BytesMut, Self> {
        if self.repr.tag() != TAG_HEAP {
            return Err(self);
        }
        let header = unsafe { self.repr.owned.header };
        let h = unsafe { &*header };
        let current = h.ref_cnt.load(Ordering::Acquire);
        if current & COUNT_MASK != 1 {
            return Err(self);
        }
        h.ref_cnt.store(1, Ordering::Release);
        let ptr = unsafe { self.repr.owned.ptr as *mut u8 };
        let len = unsafe { self.repr.owned.len };
        let cap = h.cap as usize;
        std::mem::forget(self);
        Ok(BytesMut {
            repr: MutRepr {
                owned: MutOwnedRepr {
                    ptr,
                    len: len as u32,
                    cap: cap as u32,
                    header,
                    _pad: [0; 7],
                    tag: TAG_HEAP,
                },
            },
        })
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        match self.repr.tag() {
            TAG_INLINE | TAG_STATIC => true,
            TAG_HEAP => unsafe { (*self.repr.owned.header).count() == 1 },
            _ => false,
        }
    }
}

// -- Bytes trait impls --

impl Clone for Bytes {
    #[inline]
    fn clone(&self) -> Self {
        match self.repr.tag() {
            TAG_INLINE | TAG_STATIC => Self {
                repr: self.repr.clone(),
            },
            TAG_HEAP => {
                unsafe {
                    (*self.repr.owned.header)
                        .ref_cnt
                        .fetch_add(1, Ordering::Relaxed)
                };
                Self {
                    repr: self.repr.clone(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl Drop for Bytes {
    #[inline]
    fn drop(&mut self) {
        if self.repr.tag() != TAG_HEAP {
            return;
        }
        let header = unsafe { self.repr.owned.header };
        let h = unsafe { &*header };
        if h.ref_cnt.fetch_sub(1, Ordering::Release) & COUNT_MASK == 1 {
            fence(Ordering::Acquire);
            // Read heap handle out before freeing the header allocation.
            let heap = unsafe { ptr::read(&h.heap) };
            // Free through the heap — triggers pressure notification if needed.
            unsafe { heap.dealloc(header as *mut u8) };
            drop(heap);
        }
    }
}

impl Default for Bytes {
    fn default() -> Self {
        Self::new()
    }
}
impl AsRef<[u8]> for Bytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl Borrow<[u8]> for Bytes {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}
impl std::ops::Deref for Bytes {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for Bytes {
    fn eq(&self, o: &Self) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl Eq for Bytes {}
impl PartialOrd for Bytes {
    fn partial_cmp(&self, o: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Bytes {
    fn cmp(&self, o: &Self) -> cmp::Ordering {
        self.as_slice().cmp(o.as_slice())
    }
}

impl std::hash::Hash for Bytes {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) {
        self.as_slice().hash(s);
    }
}

// Cross-type PartialEq
impl PartialEq<[u8]> for Bytes {
    fn eq(&self, o: &[u8]) -> bool {
        self.as_slice() == o
    }
}
impl PartialEq<Bytes> for [u8] {
    fn eq(&self, o: &Bytes) -> bool {
        self == o.as_slice()
    }
}
impl PartialEq<&[u8]> for Bytes {
    fn eq(&self, o: &&[u8]) -> bool {
        self.as_slice() == *o
    }
}
impl PartialEq<Bytes> for &[u8] {
    fn eq(&self, o: &Bytes) -> bool {
        *self == o.as_slice()
    }
}
impl PartialEq<str> for Bytes {
    fn eq(&self, o: &str) -> bool {
        self.as_slice() == o.as_bytes()
    }
}
impl PartialEq<Bytes> for str {
    fn eq(&self, o: &Bytes) -> bool {
        self.as_bytes() == o.as_slice()
    }
}
impl PartialEq<&str> for Bytes {
    fn eq(&self, o: &&str) -> bool {
        self.as_slice() == o.as_bytes()
    }
}
impl PartialEq<Bytes> for &str {
    fn eq(&self, o: &Bytes) -> bool {
        self.as_bytes() == o.as_slice()
    }
}
impl PartialEq<Vec<u8>> for Bytes {
    fn eq(&self, o: &Vec<u8>) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl PartialEq<Bytes> for Vec<u8> {
    fn eq(&self, o: &Bytes) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl PartialEq<String> for Bytes {
    fn eq(&self, o: &String) -> bool {
        self.as_slice() == o.as_bytes()
    }
}
impl PartialEq<Bytes> for String {
    fn eq(&self, o: &Bytes) -> bool {
        self.as_bytes() == o.as_slice()
    }
}

// Cross-type PartialOrd
impl PartialOrd<[u8]> for Bytes {
    fn partial_cmp(&self, o: &[u8]) -> Option<cmp::Ordering> {
        self.as_slice().partial_cmp(o)
    }
}
impl PartialOrd<Bytes> for [u8] {
    fn partial_cmp(&self, o: &Bytes) -> Option<cmp::Ordering> {
        self.partial_cmp(o.as_slice())
    }
}
impl PartialOrd<str> for Bytes {
    fn partial_cmp(&self, o: &str) -> Option<cmp::Ordering> {
        self.as_slice().partial_cmp(o.as_bytes())
    }
}
impl PartialOrd<Bytes> for str {
    fn partial_cmp(&self, o: &Bytes) -> Option<cmp::Ordering> {
        self.as_bytes().partial_cmp(o.as_slice())
    }
}

// From conversions
impl From<&'static [u8]> for Bytes {
    fn from(s: &'static [u8]) -> Self {
        Self::from_static(s)
    }
}
impl From<&'static str> for Bytes {
    fn from(s: &'static str) -> Self {
        Self::from_static(s.as_bytes())
    }
}
impl From<BytesMut> for Bytes {
    fn from(bm: BytesMut) -> Self {
        bm.freeze()
    }
}

// IntoIterator
impl<'a> IntoIterator for &'a Bytes {
    type Item = &'a u8;
    type IntoIter = slice::Iter<'a, u8>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

// Buf trait
impl bytes::Buf for Bytes {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }
    #[inline]
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.len(), "advance past end");
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = self.repr.inline.len as usize;
                let new_len = len - cnt;
                if cnt > 0 && new_len > 0 {
                    ptr::copy(
                        self.repr.inline.data.as_ptr().add(cnt),
                        self.repr.inline.data.as_mut_ptr(),
                        new_len,
                    );
                }
                self.repr.inline.len = new_len as u8;
            },
            _ => unsafe {
                self.repr.owned.ptr = self.repr.owned.ptr.add(cnt);
                self.repr.owned.len -= cnt;
            },
        }
    }

    fn copy_to_bytes(&mut self, len: usize) -> bytes::Bytes {
        // Return as bytes crate Bytes for compatibility
        let data = &self.as_slice()[..len];
        let result = bytes::Bytes::copy_from_slice(data);
        self.advance(len);
        result
    }
}

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bytes")
            .field("len", &self.len())
            .field(
                "mode",
                &match self.repr.tag() {
                    TAG_INLINE => "inline",
                    TAG_HEAP => "heap",
                    TAG_STATIC => "static",
                    _ => "?",
                },
            )
            .finish()
    }
}

// =========================================================================
// BytesMut
// =========================================================================

/// Mutable byte buffer allocated from a [`Heap`]. 32 bytes on the stack.
///
/// Two modes, selected by a tag byte at position 31:
/// - **Inline** (tag 0): ≤22 bytes stored directly in the struct, zero heap
///   allocation. The `Heap` handle is always present for future promotion.
/// - **Heap** (tag 1): heap-backed via `Header` (refcounted, arena-backed).
pub struct BytesMut {
    repr: MutRepr,
}

unsafe impl Send for BytesMut {}
unsafe impl Sync for BytesMut {}

impl BytesMut {
    pub fn new(heap: &Heap) -> Self {
        Self {
            repr: MutRepr {
                inline: std::mem::ManuallyDrop::new(MutInlineRepr {
                    heap: heap.clone(),
                    data: [0; MUT_INLINE_CAP],
                    len: 0,
                    tag: TAG_INLINE,
                }),
            },
        }
    }

    pub fn with_capacity(
        cap: usize,
        heap: &Heap,
    ) -> Result<Self, allocator_api2::alloc::AllocError> {
        if cap <= MUT_INLINE_CAP {
            return Ok(Self::new(heap));
        }
        let (header, data_ptr) = alloc_header_and_data(heap, cap)?;
        Ok(Self {
            repr: MutRepr {
                owned: MutOwnedRepr {
                    ptr: data_ptr,
                    len: 0,
                    cap: cap as u32,
                    header,
                    _pad: [0; 7],
                    tag: TAG_HEAP,
                },
            },
        })
    }

    /// Create a zeroed buffer of `len` bytes.
    pub fn zeroed(len: usize, heap: &Heap) -> Result<Self, allocator_api2::alloc::AllocError> {
        if len <= MUT_INLINE_CAP {
            // Inline: data is already zeroed from MutInlineRepr initialization.
            let mut bm = Self::new(heap);
            unsafe {
                (*bm.repr.inline).len = len as u8;
            }
            return Ok(bm);
        }
        let mut bm = Self::with_capacity(len, heap)?;
        unsafe {
            ptr::write_bytes(bm.repr.owned.ptr, 0, len);
            bm.repr.owned.len = len as u32;
        }
        Ok(bm)
    }

    // -- Accessors (inline/heap dispatch) ------------------------------------

    #[inline]
    pub fn len(&self) -> usize {
        match self.repr.tag() {
            TAG_INLINE => unsafe { (*self.repr.inline).len as usize },
            _ => unsafe { self.repr.owned.len as usize }, // TAG_HEAP
        }
    }

    #[inline]
    pub fn cap(&self) -> usize {
        self.capacity()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match self.repr.tag() {
            TAG_INLINE => MUT_INLINE_CAP,
            _ => unsafe { self.repr.owned.cap as usize }, // TAG_HEAP
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = (*self.repr.inline).len as usize;
                &(*self.repr.inline).data[..len]
            },
            _ => {
                let len = unsafe { self.repr.owned.len as usize };
                if len == 0 {
                    &[]
                } else {
                    unsafe { slice::from_raw_parts(self.repr.owned.ptr, len) }
                }
            }
        }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = (*self.repr.inline).len as usize;
                &mut (*self.repr.inline).data[..len]
            },
            _ => {
                let len = unsafe { self.repr.owned.len as usize };
                if len == 0 {
                    &mut []
                } else {
                    unsafe { slice::from_raw_parts_mut(self.repr.owned.ptr, len) }
                }
            }
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        match self.repr.tag() {
            TAG_INLINE => unsafe { (*self.repr.inline).len = 0 },
            _ => unsafe { self.repr.owned.len = 0 },
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            match self.repr.tag() {
                TAG_INLINE => unsafe { (*self.repr.inline).len = len as u8 },
                _ => unsafe { self.repr.owned.len = len as u32 },
            }
        }
    }

    // -- Mutation (may promote inline → heap) --------------------------------

    /// Resize the buffer. If growing, fills new bytes with `value`.
    pub fn resize(
        &mut self,
        new_len: usize,
        value: u8,
    ) -> Result<(), allocator_api2::alloc::AllocError> {
        let old_len = self.len();
        if new_len > old_len {
            self.reserve(new_len - old_len)?;
            match self.repr.tag() {
                TAG_INLINE => unsafe {
                    let p = (*self.repr.inline).data.as_mut_ptr().add(old_len);
                    ptr::write_bytes(p, value, new_len - old_len);
                    (*self.repr.inline).len = new_len as u8;
                },
                _ => unsafe {
                    ptr::write_bytes(self.repr.owned.ptr.add(old_len), value, new_len - old_len);
                    self.repr.owned.len = new_len as u32;
                },
            }
        } else {
            self.truncate(new_len);
        }
        Ok(())
    }

    pub fn extend_from_slice(
        &mut self,
        data: &[u8],
    ) -> Result<(), allocator_api2::alloc::AllocError> {
        if data.is_empty() {
            return Ok(());
        }
        let old_len = self.len();
        let needed = old_len + data.len();

        match self.repr.tag() {
            TAG_INLINE => {
                if needed <= MUT_INLINE_CAP {
                    unsafe {
                        ptr::copy_nonoverlapping(
                            data.as_ptr(),
                            (*self.repr.inline).data.as_mut_ptr().add(old_len),
                            data.len(),
                        );
                        (*self.repr.inline).len = needed as u8;
                    }
                    return Ok(());
                }
                self.promote_to_heap(needed)?;
                unsafe {
                    ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        self.repr.owned.ptr.add(old_len),
                        data.len(),
                    );
                    self.repr.owned.len = needed as u32;
                }
            }
            _ => {
                if needed > unsafe { self.repr.owned.cap } as usize {
                    self.try_grow(needed)?;
                }
                unsafe {
                    ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        self.repr.owned.ptr.add(old_len),
                        data.len(),
                    );
                    self.repr.owned.len = needed as u32;
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn push(&mut self, byte: u8) -> Result<(), allocator_api2::alloc::AllocError> {
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = (*self.repr.inline).len as usize;
                if len < MUT_INLINE_CAP {
                    (*self.repr.inline).data[len] = byte;
                    (*self.repr.inline).len += 1;
                    return Ok(());
                }
                self.promote_to_heap(len + 1)?;
                *self.repr.owned.ptr.add(len) = byte;
                self.repr.owned.len = (len + 1) as u32;
            },
            _ => unsafe {
                if self.repr.owned.len == self.repr.owned.cap {
                    self.try_grow(self.repr.owned.len as usize + 1)?;
                }
                let len = self.repr.owned.len as usize;
                *self.repr.owned.ptr.add(len) = byte;
                self.repr.owned.len += 1;
            },
        }
        Ok(())
    }

    pub fn reserve(&mut self, additional: usize) -> Result<(), allocator_api2::alloc::AllocError> {
        let needed = self.len() + additional;
        match self.repr.tag() {
            TAG_INLINE => {
                if needed > MUT_INLINE_CAP {
                    self.promote_to_heap(needed)?;
                }
            }
            _ => {
                if needed > unsafe { self.repr.owned.cap } as usize {
                    self.try_grow(needed)?;
                }
            }
        }
        Ok(())
    }

    /// Split off all data, leaving self empty. Returns a new `BytesMut` with the data.
    pub fn split(&mut self) -> Self {
        let len = self.len();
        if len == 0 {
            let heap = self.heap_ref();
            return Self::new(&heap);
        }
        match self.repr.tag() {
            TAG_INLINE => {
                // Clone heap for the new empty self, then copy data to result.
                let heap = self.heap_ref();
                let mut result = Self::new(&heap);
                unsafe {
                    ptr::copy_nonoverlapping(
                        (*self.repr.inline).data.as_ptr(),
                        (*result.repr.inline).data.as_mut_ptr(),
                        len,
                    );
                    (*result.repr.inline).len = len as u8;
                    (*self.repr.inline).len = 0;
                }
                result
            }
            _ => {
                // Heap mode: take ownership of the allocation.
                let heap = self.heap_ref();
                let result = unsafe {
                    Self {
                        repr: MutRepr {
                            owned: MutOwnedRepr {
                                ptr: self.repr.owned.ptr,
                                len: self.repr.owned.len,
                                cap: self.repr.owned.cap,
                                header: self.repr.owned.header,
                                _pad: [0; 7],
                                tag: TAG_HEAP,
                            },
                        },
                    }
                };
                // Reset self to empty inline with heap handle.
                self.repr = MutRepr {
                    inline: std::mem::ManuallyDrop::new(MutInlineRepr {
                        heap,
                        data: [0; MUT_INLINE_CAP],
                        len: 0,
                        tag: TAG_INLINE,
                    }),
                };
                result
            }
        }
    }

    /// Split at `at`, returning `[0..at)` and leaving `[at..len)` in self.
    pub fn split_to(&mut self, at: usize) -> Bytes {
        assert!(at <= self.len());
        let data = &self.as_slice()[..at];
        let result = if at <= INLINE_CAP {
            let mut inline = InlineRepr {
                data: [0; INLINE_CAP],
                len: at as u8,
                tag: TAG_INLINE,
            };
            inline.data[..at].copy_from_slice(data);
            Bytes {
                repr: Repr { inline },
            }
        } else {
            let heap = self.heap_ref();
            match Bytes::copy_from_slice(data, &heap) {
                Ok(b) => b,
                Err(_) => panic!("OOM in BytesMut::split_to"),
            }
        };
        // Shift remaining data left.
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = (*self.repr.inline).len as usize;
                let remaining = len - at;
                ptr::copy(
                    (*self.repr.inline).data.as_ptr().add(at),
                    (*self.repr.inline).data.as_mut_ptr(),
                    remaining,
                );
                (*self.repr.inline).len = remaining as u8;
            },
            _ => unsafe {
                let len = self.repr.owned.len as usize;
                ptr::copy(self.repr.owned.ptr.add(at), self.repr.owned.ptr, len - at);
                self.repr.owned.len -= at as u32;
            },
        }
        result
    }

    /// Split at `at`, returning `[at..len)` and leaving `[0..at)` in self.
    pub fn split_off(&mut self, at: usize) -> Bytes {
        assert!(at <= self.len());
        let tail_len = self.len() - at;
        let tail_data = &self.as_slice()[at..];
        let result = if tail_len <= INLINE_CAP {
            let mut inline = InlineRepr {
                data: [0; INLINE_CAP],
                len: tail_len as u8,
                tag: TAG_INLINE,
            };
            inline.data[..tail_len].copy_from_slice(tail_data);
            Bytes {
                repr: Repr { inline },
            }
        } else {
            let heap = self.heap_ref();
            match Bytes::copy_from_slice(tail_data, &heap) {
                Ok(b) => b,
                Err(_) => panic!("OOM in BytesMut::split_off"),
            }
        };
        self.truncate(at);
        result
    }

    /// Unsplit: append `other` to self.
    pub fn unsplit(&mut self, other: BytesMut) -> Result<(), allocator_api2::alloc::AllocError> {
        self.extend_from_slice(other.as_slice())
    }

    /// Set the length directly.
    ///
    /// # Safety
    /// `new_len` must be ≤ `capacity()` and the bytes in `[0..new_len)` must be initialized.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity());
        match self.repr.tag() {
            TAG_INLINE => {
                debug_assert!(new_len <= MUT_INLINE_CAP);
                (*self.repr.inline).len = new_len as u8;
            }
            _ => {
                self.repr.owned.len = new_len as u32;
            }
        }
    }

    /// Access the spare capacity as uninitialized bytes.
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = (*self.repr.inline).len as usize;
                let spare = MUT_INLINE_CAP - len;
                if spare == 0 {
                    return &mut [];
                }
                slice::from_raw_parts_mut(
                    (*self.repr.inline).data.as_mut_ptr().add(len) as *mut MaybeUninit<u8>,
                    spare,
                )
            },
            _ => unsafe {
                let len = self.repr.owned.len as usize;
                let cap = self.repr.owned.cap as usize;
                if cap == 0 || cap == len {
                    return &mut [];
                }
                slice::from_raw_parts_mut(
                    self.repr.owned.ptr.add(len) as *mut MaybeUninit<u8>,
                    cap - len,
                )
            },
        }
    }

    /// Freeze into immutable [`Bytes`]. Zero-cost for heap buffers >30 bytes.
    #[inline]
    pub fn freeze(self) -> Bytes {
        let len = self.len();
        if len == 0 {
            return Bytes::new();
        }
        match self.repr.tag() {
            TAG_INLINE => {
                // Copy inline data into a Bytes inline repr (30-byte capacity).
                let mut inline = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: len as u8,
                    tag: TAG_INLINE,
                };
                unsafe {
                    ptr::copy_nonoverlapping(
                        (*self.repr.inline).data.as_ptr(),
                        inline.data.as_mut_ptr(),
                        len,
                    );
                }
                // self drops here, releasing the heap handle in MutInlineRepr.
                Bytes {
                    repr: Repr { inline },
                }
            }
            _ => {
                let header = unsafe { self.repr.owned.header };
                if len <= INLINE_CAP {
                    let mut inline = InlineRepr {
                        data: [0; INLINE_CAP],
                        len: len as u8,
                        tag: TAG_INLINE,
                    };
                    unsafe {
                        ptr::copy_nonoverlapping(
                            self.repr.owned.ptr,
                            inline.data.as_mut_ptr(),
                            len,
                        );
                    }
                    if !header.is_null() {
                        let heap = unsafe { ptr::read(&(*header).heap) };
                        unsafe { heap.dealloc(header as *mut u8) };
                        drop(heap);
                    }
                    std::mem::forget(self);
                    return Bytes {
                        repr: Repr { inline },
                    };
                }
                if !header.is_null() {
                    unsafe { (*header).ref_cnt.fetch_or(FROZEN_BIT, Ordering::Release) };
                }
                let bytes = Bytes {
                    repr: Repr {
                        owned: OwnedRepr {
                            ptr: unsafe { self.repr.owned.ptr },
                            len,
                            header,
                            _pad: [0; 7],
                            tag: TAG_HEAP,
                        },
                    },
                };
                std::mem::forget(self);
                bytes
            }
        }
    }

    /// Convert into `bytes::Bytes` for ecosystem interop.
    pub fn into_bytes_crate(self) -> ::bytes::Bytes {
        let frozen = self.freeze();
        if frozen.is_empty() {
            return ::bytes::Bytes::new();
        }
        ::bytes::Bytes::from_owner(frozen)
    }

    // -- Internal helpers ----------------------------------------------------

    /// Get a cloned `Heap` handle, regardless of current mode.
    #[inline]
    fn heap_ref(&self) -> Heap {
        match self.repr.tag() {
            TAG_INLINE => unsafe { (*self.repr.inline).heap.clone() },
            _ => unsafe { (*self.repr.owned.header).heap.clone() }, // TAG_HEAP
        }
    }

    /// Promote inline → heap mode with at least `min_cap` capacity.
    /// Copies existing inline data to the new heap allocation.
    fn promote_to_heap(&mut self, min_cap: usize) -> Result<(), allocator_api2::alloc::AllocError> {
        debug_assert_eq!(self.repr.tag(), TAG_INLINE);
        let new_cap = min_cap.next_power_of_two().max(64);
        let old_len = unsafe { (*self.repr.inline).len as usize };

        // Read heap from inline repr before overwriting.
        let heap = unsafe { ptr::read(&(*self.repr.inline).heap) };

        let (header, data_ptr) = alloc_header_and_data(&heap, new_cap)?;
        // Copy existing inline data to the heap allocation.
        if old_len > 0 {
            unsafe {
                ptr::copy_nonoverlapping((*self.repr.inline).data.as_ptr(), data_ptr, old_len);
            }
        }
        // Drop the heap handle we extracted (Header now owns a clone).
        drop(heap);

        self.repr = MutRepr {
            owned: MutOwnedRepr {
                ptr: data_ptr,
                len: old_len as u32,
                cap: new_cap as u32,
                header,
                _pad: [0; 7],
                tag: TAG_HEAP,
            },
        };
        Ok(())
    }

    /// Grow a heap-mode buffer to at least `min_cap`.
    fn try_grow(&mut self, min_cap: usize) -> Result<(), allocator_api2::alloc::AllocError> {
        debug_assert_eq!(self.repr.tag(), TAG_HEAP);
        let new_cap = min_cap.next_power_of_two().max(64);
        let header = unsafe { self.repr.owned.header };
        let heap = unsafe { &(*header).heap };

        let new_total = HEADER_SIZE + new_cap;
        let align = std::mem::align_of::<Header>();
        let new_raw = unsafe {
            ffi::mi_heap_realloc_aligned(
                heap.raw(),
                header as *mut core::ffi::c_void,
                new_total,
                align,
            )
        };
        if new_raw.is_null() {
            return Err(allocator_api2::alloc::AllocError);
        }
        let new_header = new_raw as *mut Header;
        unsafe { (*new_header).cap = new_cap as u32 };
        unsafe {
            self.repr.owned.header = new_header;
            self.repr.owned.ptr = (new_raw as *mut u8).add(HEADER_SIZE);
            self.repr.owned.cap = new_cap as u32;
        }
        Ok(())
    }
}

// -- BytesMut trait impls --

impl Drop for BytesMut {
    fn drop(&mut self) {
        match self.repr.tag() {
            TAG_HEAP => {
                let header = unsafe { self.repr.owned.header };
                if header.is_null() {
                    return;
                }
                let h = unsafe { &*header };
                let heap = unsafe { ptr::read(&h.heap) };
                unsafe { heap.dealloc(header as *mut u8) };
                drop(heap);
            }
            TAG_INLINE => {
                // Drop the inline heap handle via ManuallyDrop.
                unsafe {
                    std::mem::ManuallyDrop::drop(&mut self.repr.inline);
                }
            }
            _ => {}
        }
    }
}

impl Default for BytesMut {
    fn default() -> Self {
        let master = HeapMaster::new(64 * 1024 * 1024).expect("failed to create default heap");
        Self::new(&master)
    }
}

impl Clone for BytesMut {
    fn clone(&self) -> Self {
        match self.repr.tag() {
            TAG_INLINE => {
                let heap = unsafe { (*self.repr.inline).heap.clone() };
                let len = unsafe { (*self.repr.inline).len as usize };
                let mut new = Self::new(&heap);
                unsafe {
                    ptr::copy_nonoverlapping(
                        (*self.repr.inline).data.as_ptr(),
                        (*new.repr.inline).data.as_mut_ptr(),
                        len,
                    );
                    (*new.repr.inline).len = len as u8;
                }
                new
            }
            _ => {
                let heap = unsafe { (*self.repr.owned.header).heap.clone() };
                let len = self.len();
                let mut new = Self::with_capacity(len, &heap).expect("clone OOM");
                new.extend_from_slice(self.as_slice()).expect("clone OOM");
                new
            }
        }
    }
}

impl AsRef<[u8]> for BytesMut {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl AsMut<[u8]> for BytesMut {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}
impl Borrow<[u8]> for BytesMut {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}
impl BorrowMut<[u8]> for BytesMut {
    #[inline]
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}
impl std::ops::Deref for BytesMut {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl std::ops::DerefMut for BytesMut {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl PartialEq for BytesMut {
    fn eq(&self, o: &Self) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl Eq for BytesMut {}
impl PartialOrd for BytesMut {
    fn partial_cmp(&self, o: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for BytesMut {
    fn cmp(&self, o: &Self) -> cmp::Ordering {
        self.as_slice().cmp(o.as_slice())
    }
}
impl std::hash::Hash for BytesMut {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) {
        self.as_slice().hash(s);
    }
}

// Cross-type PartialEq for BytesMut
impl PartialEq<[u8]> for BytesMut {
    fn eq(&self, o: &[u8]) -> bool {
        self.as_slice() == o
    }
}
impl PartialEq<BytesMut> for [u8] {
    fn eq(&self, o: &BytesMut) -> bool {
        self == o.as_slice()
    }
}
impl PartialEq<&[u8]> for BytesMut {
    fn eq(&self, o: &&[u8]) -> bool {
        self.as_slice() == *o
    }
}
impl PartialEq<BytesMut> for &[u8] {
    fn eq(&self, o: &BytesMut) -> bool {
        *self == o.as_slice()
    }
}
impl PartialEq<str> for BytesMut {
    fn eq(&self, o: &str) -> bool {
        self.as_slice() == o.as_bytes()
    }
}
impl PartialEq<BytesMut> for str {
    fn eq(&self, o: &BytesMut) -> bool {
        self.as_bytes() == o.as_slice()
    }
}
impl PartialEq<&str> for BytesMut {
    fn eq(&self, o: &&str) -> bool {
        self.as_slice() == o.as_bytes()
    }
}
impl PartialEq<BytesMut> for &str {
    fn eq(&self, o: &BytesMut) -> bool {
        self.as_bytes() == o.as_slice()
    }
}
impl PartialEq<Vec<u8>> for BytesMut {
    fn eq(&self, o: &Vec<u8>) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl PartialEq<BytesMut> for Vec<u8> {
    fn eq(&self, o: &BytesMut) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl PartialEq<Bytes> for BytesMut {
    fn eq(&self, o: &Bytes) -> bool {
        self.as_slice() == o.as_slice()
    }
}
impl PartialEq<BytesMut> for Bytes {
    fn eq(&self, o: &BytesMut) -> bool {
        self.as_slice() == o.as_slice()
    }
}

// Extend
impl Extend<u8> for BytesMut {
    fn extend<I: IntoIterator<Item = u8>>(&mut self, iter: I) {
        for b in iter {
            self.push(b).expect("extend OOM");
        }
    }
}
impl<'a> Extend<&'a u8> for BytesMut {
    fn extend<I: IntoIterator<Item = &'a u8>>(&mut self, iter: I) {
        for &b in iter {
            self.push(b).expect("extend OOM");
        }
    }
}

// IntoIterator
impl<'a> IntoIterator for &'a BytesMut {
    type Item = &'a u8;
    type IntoIter = slice::Iter<'a, u8>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

// fmt::Write
impl fmt::Write for BytesMut {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.extend_from_slice(s.as_bytes()).map_err(|_| fmt::Error)
    }
}

// Buf trait for BytesMut (read from written region)
impl bytes::Buf for BytesMut {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }
    #[inline]
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }
    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.len(), "advance past end");
        let new_len = self.len() - cnt;
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                if cnt > 0 && new_len > 0 {
                    ptr::copy(
                        (*self.repr.inline).data.as_ptr().add(cnt),
                        (*self.repr.inline).data.as_mut_ptr(),
                        new_len,
                    );
                }
                (*self.repr.inline).len = new_len as u8;
            },
            _ => unsafe {
                if cnt > 0 && new_len > 0 {
                    ptr::copy(self.repr.owned.ptr.add(cnt), self.repr.owned.ptr, new_len);
                }
                self.repr.owned.len = new_len as u32;
            },
        }
    }
}

// BufMut trait for BytesMut
unsafe impl bytes::BufMut for BytesMut {
    #[inline]
    fn remaining_mut(&self) -> usize {
        usize::MAX
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let new_len = self.len() + cnt;
        debug_assert!(new_len <= self.capacity(), "advance_mut past capacity");
        match self.repr.tag() {
            TAG_INLINE => (*self.repr.inline).len = new_len as u8,
            _ => self.repr.owned.len = new_len as u32,
        }
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        if self.len() >= self.capacity() {
            self.reserve(64).expect("chunk_mut OOM");
        }
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = (*self.repr.inline).len as usize;
                let spare = MUT_INLINE_CAP - len;
                bytes::buf::UninitSlice::from_raw_parts_mut(
                    (*self.repr.inline).data.as_mut_ptr().add(len),
                    spare,
                )
            },
            _ => unsafe {
                let ptr = self.repr.owned.ptr.add(self.repr.owned.len as usize);
                let spare = self.repr.owned.cap - self.repr.owned.len;
                bytes::buf::UninitSlice::from_raw_parts_mut(ptr, spare as usize)
            },
        }
    }

    fn put_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src).expect("put_slice OOM");
    }

    fn put_bytes(&mut self, val: u8, cnt: usize) {
        self.reserve(cnt).expect("put_bytes OOM");
        match self.repr.tag() {
            TAG_INLINE => unsafe {
                let len = (*self.repr.inline).len as usize;
                ptr::write_bytes((*self.repr.inline).data.as_mut_ptr().add(len), val, cnt);
                (*self.repr.inline).len += cnt as u8;
            },
            _ => unsafe {
                ptr::write_bytes(
                    self.repr.owned.ptr.add(self.repr.owned.len as usize),
                    val,
                    cnt,
                );
                self.repr.owned.len += cnt as u32;
            },
        }
    }
}

impl fmt::Debug for BytesMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BytesMut")
            .field("len", &self.len())
            .field("cap", &self.capacity())
            .field(
                "mode",
                &match self.repr.tag() {
                    TAG_INLINE => "inline",
                    TAG_HEAP => "heap",
                    _ => "?",
                },
            )
            .finish()
    }
}

// =========================================================================
// Helpers
// =========================================================================

fn alloc_header_and_data(
    heap: &Heap,
    cap: usize,
) -> Result<(*mut Header, *mut u8), allocator_api2::alloc::AllocError> {
    let total = HEADER_SIZE + cap;
    let align = std::mem::align_of::<Header>();
    let raw = heap.alloc(total, align);
    if raw.is_null() {
        return Err(allocator_api2::alloc::AllocError);
    }
    let header = raw as *mut Header;
    unsafe {
        ptr::write(
            header,
            Header {
                ref_cnt: AtomicU32::new(1),
                cap: cap as u32,
                heap: heap.clone(),
            },
        );
    }
    Ok((header, unsafe { raw.add(HEADER_SIZE) }))
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BufMut};
    use std::fmt::Write;

    fn heap() -> HeapMaster {
        HeapMaster::new(64 * 1024 * 1024).unwrap()
    }

    // -- Bytes basic --

    #[test]
    fn bytes_empty() {
        let b = Bytes::new();
        assert!(b.is_empty());
        assert_eq!(&b[..], b"");
    }
    #[test]
    fn bytes_inline() {
        let b = Bytes::copy_from_slice(b"hello", &heap()).unwrap();
        assert_eq!(&b[..], b"hello");
    }
    #[test]
    fn bytes_inline_max() {
        let d = [0xABu8; INLINE_CAP];
        let b = Bytes::copy_from_slice(&d, &heap()).unwrap();
        assert_eq!(&b[..], &d);
    }
    #[test]
    fn bytes_heap() {
        let d = vec![42u8; 128];
        let b = Bytes::copy_from_slice(&d, &heap()).unwrap();
        assert_eq!(&b[..], &d[..]);
    }
    #[test]
    fn bytes_static() {
        let b = Bytes::from_static(b"static");
        assert_eq!(&b[..], b"static");
    }

    // -- Bytes truncate/clear --

    #[test]
    fn bytes_truncate() {
        let h = heap();
        let mut b = Bytes::copy_from_slice(b"hello world", &h).unwrap();
        b.truncate(5);
        assert_eq!(&b[..], b"hello");
        b.truncate(100); // no-op
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn bytes_clear() {
        let mut b = Bytes::copy_from_slice(b"data", &heap()).unwrap();
        b.clear();
        assert!(b.is_empty());
    }

    // -- Bytes clone --

    #[test]
    fn bytes_clone_inline() {
        let b = Bytes::copy_from_slice(b"s", &heap()).unwrap();
        assert_eq!(b.clone()[..], b[..]);
    }
    #[test]
    fn bytes_clone_heap() {
        let b = Bytes::copy_from_slice(&[1u8; 100], &heap()).unwrap();
        let c = b.clone();
        assert_eq!(&b[..], &c[..]);
        assert!(!b.is_unique());
        drop(c);
        assert!(b.is_unique());
    }

    // -- Bytes slice with RangeBounds --

    #[test]
    fn bytes_slice_range_types() {
        let h = heap();
        let b = Bytes::copy_from_slice(b"hello world", &h).unwrap();
        assert_eq!(&b.slice(0..5)[..], b"hello");
        assert_eq!(&b.slice(..5)[..], b"hello");
        assert_eq!(&b.slice(6..)[..], b"world");
        assert_eq!(&b.slice(..)[..], b"hello world");
        assert_eq!(&b.slice(0..=4)[..], b"hello");
    }

    // -- Bytes slice_ref --

    #[test]
    fn bytes_slice_ref() {
        let h = heap();
        let b = Bytes::copy_from_slice(&[0u8; 128], &h).unwrap();
        let sub = &b[10..50];
        let sliced = b.slice_ref(sub);
        assert_eq!(sliced.len(), 40);
        assert_eq!(&sliced[..], &b[10..50]);
    }

    // -- Bytes split --

    #[test]
    fn bytes_split_to() {
        let mut b = Bytes::copy_from_slice(b"hello world", &heap()).unwrap();
        let head = b.split_to(5);
        assert_eq!(&head[..], b"hello");
        assert_eq!(&b[..], b" world");
    }

    #[test]
    fn bytes_split_off() {
        let mut b = Bytes::copy_from_slice(b"hello world", &heap()).unwrap();
        let tail = b.split_off(5);
        assert_eq!(&b[..], b"hello");
        assert_eq!(&tail[..], b" world");
    }

    // -- Bytes comparisons --

    #[test]
    fn bytes_eq_cross_type() {
        let b = Bytes::copy_from_slice(b"abc", &heap()).unwrap();
        assert_eq!(b, b"abc" as &[u8]);
        assert_eq!(b, "abc");
        assert_eq!(b, b"abc".to_vec());
        assert_eq!(b, String::from("abc"));
        // Reverse
        assert_eq!(b"abc" as &[u8], b);
        assert_eq!("abc", b);
    }

    #[test]
    fn bytes_ord() {
        let a = Bytes::copy_from_slice(b"abc", &heap()).unwrap();
        let b = Bytes::copy_from_slice(b"abd", &heap()).unwrap();
        assert!(a < b);
        assert!(b > a);
    }

    // -- Bytes From --

    #[test]
    fn bytes_from_static_slice() {
        let b: Bytes = b"hello".as_slice().into();
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn bytes_from_static_str() {
        let b: Bytes = "hello".into();
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn bytes_from_bytes_mut() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(b"convert").unwrap();
        let b: Bytes = bm.into();
        assert_eq!(&b[..], b"convert");
    }

    // -- Bytes Buf trait --

    #[test]
    fn bytes_buf_trait() {
        let mut b = Bytes::copy_from_slice(b"hello", &heap()).unwrap();
        assert_eq!(b.remaining(), 5);
        assert_eq!(b.chunk(), b"hello");
        b.advance(2);
        assert_eq!(b.remaining(), 3);
        assert_eq!(b.chunk(), b"llo");
    }

    // -- Bytes Borrow --

    #[test]
    fn bytes_borrow() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let key = Bytes::copy_from_slice(b"key", &heap()).unwrap();
        map.insert(key, 42);
        assert_eq!(map.get(b"key".as_slice()), Some(&42));
    }

    // -- BytesMut basic --

    #[test]
    fn bm_size() {
        assert_eq!(
            std::mem::size_of::<BytesMut>(),
            32,
            "BytesMut should be 32 bytes"
        );
        assert_eq!(std::mem::size_of::<Bytes>(), 32, "Bytes should be 32 bytes");
    }
    #[test]
    fn bm_empty() {
        let bm = BytesMut::new(&heap());
        assert!(bm.is_empty());
    }
    #[test]
    fn bm_extend() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"hello ").unwrap();
        bm.extend_from_slice(b"world").unwrap();
        assert_eq!(&bm[..], b"hello world");
    }
    #[test]
    fn bm_push() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        for &b in b"test" {
            bm.push(b).unwrap();
        }
        assert_eq!(&bm[..], b"test");
    }

    // -- BytesMut zeroed --

    #[test]
    fn bm_zeroed() {
        let bm = BytesMut::zeroed(100, &heap()).unwrap();
        assert_eq!(bm.len(), 100);
        assert!(bm.as_slice().iter().all(|&b| b == 0));
    }

    // -- BytesMut resize --

    #[test]
    fn bm_resize_grow() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"hi").unwrap();
        bm.resize(10, 0xFF).unwrap();
        assert_eq!(bm.len(), 10);
        assert_eq!(&bm[..2], b"hi");
        assert!(bm[2..].iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn bm_resize_shrink() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"hello world").unwrap();
        bm.resize(5, 0).unwrap();
        assert_eq!(&bm[..], b"hello");
    }

    // -- BytesMut split --

    #[test]
    fn bm_split() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"hello").unwrap();
        let taken = bm.split();
        assert_eq!(&taken[..], b"hello");
        assert!(bm.is_empty());
    }

    // -- BytesMut unsplit --

    #[test]
    fn bm_unsplit() {
        let h = heap();
        let mut a = BytesMut::new(&h);
        a.extend_from_slice(b"hello ").unwrap();
        let mut b = BytesMut::new(&h);
        b.extend_from_slice(b"world").unwrap();
        a.unsplit(b).unwrap();
        assert_eq!(&a[..], b"hello world");
    }

    // -- BytesMut set_len --

    #[test]
    fn bm_set_len() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(100, &h).unwrap();
        unsafe {
            bm.set_len(50);
        }
        assert_eq!(bm.len(), 50);
    }

    // -- BytesMut spare_capacity_mut --

    #[test]
    fn bm_spare_capacity() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(100, &h).unwrap();
        bm.extend_from_slice(b"hi").unwrap();
        let spare = bm.spare_capacity_mut();
        assert!(spare.len() >= 98);
    }

    // -- BytesMut Clone --

    #[test]
    fn bm_clone() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"clone me").unwrap();
        let c = bm.clone();
        assert_eq!(&bm[..], &c[..]);
    }

    // -- BytesMut comparisons --

    #[test]
    fn bm_eq_cross_type() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"abc").unwrap();
        assert_eq!(bm, b"abc" as &[u8]);
        assert_eq!(bm, "abc");
        assert_eq!(bm, b"abc".to_vec());
        let b = Bytes::copy_from_slice(b"abc", &h).unwrap();
        assert_eq!(bm, b);
        assert_eq!(b, bm);
    }

    // -- BytesMut Extend --

    #[test]
    fn bm_extend_iter() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend(b"hello".iter().copied());
        assert_eq!(&bm[..], b"hello");
    }

    // -- BytesMut fmt::Write --

    #[test]
    fn bm_fmt_write() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        write!(bm, "hello {}", 42).unwrap();
        assert_eq!(&bm[..], b"hello 42");
    }

    // -- BytesMut Buf trait --

    #[test]
    fn bm_buf_trait() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"hello").unwrap();
        assert_eq!(Buf::remaining(&bm), 5);
        assert_eq!(Buf::chunk(&bm), b"hello");
    }

    // -- BytesMut BufMut trait --

    #[test]
    fn bm_bufmut_put_u8() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.put_u8(0x42);
        assert_eq!(&bm[..], &[0x42]);
    }

    #[test]
    fn bm_bufmut_put_u32_le() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.put_u32_le(0xDEADBEEF);
        assert_eq!(&bm[..], &[0xEF, 0xBE, 0xAD, 0xDE]);
    }

    #[test]
    fn bm_bufmut_put_slice() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.put_slice(b"hello");
        assert_eq!(&bm[..], b"hello");
    }

    #[test]
    fn bm_bufmut_put_bytes() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.put_bytes(0xFF, 10);
        assert_eq!(bm.len(), 10);
        assert!(bm.as_slice().iter().all(|&b| b == 0xFF));
    }

    // -- freeze / try_mut --

    #[test]
    fn freeze_small_inline() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(b"tiny").unwrap();
        let b = bm.freeze();
        assert_eq!(&b[..], b"tiny");
    }

    #[test]
    fn freeze_large_heap() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 100]).unwrap();
        let b = bm.freeze();
        assert_eq!(b.len(), 100);
        assert!(b.is_unique());
    }

    #[test]
    fn freeze_then_try_mut() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(b"mutable data that is bigger than inline")
            .unwrap();
        let b = bm.freeze();
        let mut bm2 = b.try_mut().unwrap();
        bm2.extend_from_slice(b" + more").unwrap();
        assert!(bm2.as_slice().starts_with(b"mutable data"));
    }

    #[test]
    fn try_mut_fails_with_clones() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(&[0xFF; 64]).unwrap();
        let b = bm.freeze();
        let _c = b.clone();
        assert!(b.try_mut().is_err());
    }

    // -- interop --

    #[test]
    fn into_bytes_crate() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(b"interop").unwrap();
        let b = bm.into_bytes_crate();
        assert_eq!(&b[..], b"interop");
    }

    // -- OOM --

    #[test]
    fn bytes_mut_oom() {
        let h = HeapMaster::new(32 * 1024 * 1024).unwrap();
        let mut bm = BytesMut::new(&h);
        let mut oom = false;
        for _ in 0..10_000 {
            if bm.extend_from_slice(&[0u8; 8192]).is_err() {
                oom = true;
                break;
            }
        }
        assert!(oom, "expected OOM");
    }
}
