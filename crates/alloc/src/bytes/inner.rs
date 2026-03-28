#![allow(clippy::missing_safety_doc)]

use std::borrow::{Borrow, BorrowMut};
use std::mem::MaybeUninit;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU32, Ordering, fence};
use std::{cmp, fmt, ptr, slice};

use bisque_mimalloc_sys as ffi;

use crate::heap::HeapData;
use crate::{Heap, HeapMaster};

// =========================================================================
// Constants
// =========================================================================

const TAG_INLINE: u8 = 0;
const TAG_HEAP: u8 = 1;
const TAG_STATIC: u8 = 2;
/// Maximum bytes storable inline (no heap allocation).
const INLINE_CAP: usize = 30;

/// Bit 31 of ref_cnt was historically a "frozen" flag, but is now always set
/// at allocation time. All ref_cnt reads use COUNT_MASK to strip it, so it
/// has no behavioral effect. Kept for compatibility with `try_mut()` which
/// clears it via `store(1)`.
const FROZEN_BIT: u32 = 1 << 31;
const COUNT_MASK: u32 = !FROZEN_BIT;

// =========================================================================
// Header
// =========================================================================

#[repr(C)]
struct Header {
    ref_cnt: AtomicU32,
    cap: u32,
    /// Raw pointer to HeapData — does NOT participate in TLRC refcounting.
    ///
    /// This avoids 2 TLRC bracket operations (6 atomic stores + 2 TLS lookups)
    /// per alloc/dealloc cycle. Safety relies on the documented contract that
    /// [`HeapMaster`] must outlive all `Bytes`/`BytesMut` derived from it.
    heap_data: *const HeapData,
}

unsafe impl Send for Header {}
unsafe impl Sync for Header {}

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

/// Immutable inline: up to 30 bytes with no heap allocation.
#[repr(C)]
#[derive(Clone, Copy)]
struct InlineRepr {
    data: [u8; INLINE_CAP],
    len: u8,
    tag: u8,
}

/// Mutable inline: up to 22 bytes + a Heap handle for promotion.
const MUT_INLINE_CAP: usize = 22;

#[repr(C)]
struct MutInlineRepr {
    heap: Heap,                 // 8  (offset 0, 8-aligned)
    data: [u8; MUT_INLINE_CAP], // 22 (offset 8)
    len: u8,                    // 1  (offset 30)
    tag: u8,                    // 1  (offset 31) = 32
}

/// Shared heap representation used by BOTH `Bytes` and `BytesMut`.
///
/// Having an identical heap layout means `freeze()` and `try_mut()` are
/// zero-cost for heap-backed buffers — just a frozen-bit flip + raw 32-byte
/// reinterpret, no field reshuffling or data copying.
#[repr(C)]
#[derive(Clone, Copy)]
struct HeapRepr {
    ptr: *mut u8,        // 8  (offset 0)
    len: u32,            // 4  (offset 8)
    cap: u32,            // 4  (offset 12)
    header: *mut Header, // 8  (offset 16)
    _pad: [u8; 7],       // 7  (offset 24)
    tag: u8,             // 1  (offset 31) = 32
}

// -- Bytes repr (immutable) ------------------------------------------------

#[repr(C)]
union Repr {
    inline: InlineRepr,
    heap: HeapRepr,
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

// -- BytesMut repr (mutable) -----------------------------------------------

#[repr(C)]
union MutRepr {
    inline: std::mem::ManuallyDrop<MutInlineRepr>,
    heap: HeapRepr,
    raw: [u8; 32],
}

const _: () = assert!(std::mem::size_of::<MutRepr>() == 32);
const _: () = assert!(std::mem::size_of::<MutInlineRepr>() == 32);
const _: () = assert!(std::mem::size_of::<HeapRepr>() == 32);

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
                heap: HeapRepr {
                    ptr: data.as_ptr() as *mut u8,
                    len: data.len() as u32,
                    cap: 0,
                    header: ptr::null_mut(),
                    _pad: [0; 7],
                    tag: TAG_STATIC,
                },
            },
        }
    }

    /// Copy a slice into a new `Bytes`. Inline if ≤30 bytes, otherwise single heap alloc.
    #[inline]
    pub fn copy_from_slice(
        data: &[u8],
        heap: &Heap,
    ) -> Result<Self, allocator_api2::alloc::AllocError> {
        if data.len() <= INLINE_CAP {
            // TAG_INLINE == 0, so zeroed raw bytes are already a valid empty inline repr.
            // We only need to write the data and set the length byte.
            unsafe {
                let mut repr = Repr { raw: [0; 32] };
                ptr::copy_nonoverlapping(data.as_ptr(), repr.raw.as_mut_ptr(), data.len());
                repr.raw[30] = data.len() as u8;
                // repr.raw[31] is already 0 == TAG_INLINE
                return Ok(Self { repr });
            }
        }
        let (header, data_ptr) = alloc_header_and_data(heap, data.len())?;
        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), data_ptr, data.len());
        }
        Ok(Self {
            repr: Repr {
                heap: HeapRepr {
                    ptr: data_ptr,
                    len: data.len() as u32,
                    cap: 0,
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
            _ => unsafe { self.repr.heap.len as usize },
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
            _ => unsafe { slice::from_raw_parts(self.repr.heap.ptr, self.repr.heap.len as usize) },
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
                    self.repr.heap.len = len as u32;
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
                                heap: HeapRepr {
                                    ptr: self.repr.heap.ptr.add(start),
                                    len: (end - start) as u32,
                                    cap: 0,
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
                let header = unsafe { &*self.repr.heap.header };
                header.ref_cnt.fetch_add(1, Ordering::Relaxed);
                unsafe {
                    Self {
                        repr: Repr {
                            heap: HeapRepr {
                                ptr: self.repr.heap.ptr.add(start),
                                len: (end - start) as u32,
                                cap: 0,
                                header: self.repr.heap.header,
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
                let header = &*self.repr.heap.header;
                header.ref_cnt.fetch_add(1, Ordering::Relaxed);
                let base_ptr = self.repr.heap.ptr;
                let head = Self {
                    repr: Repr {
                        heap: HeapRepr {
                            ptr: base_ptr,
                            len: at as u32,
                            cap: 0,
                            header: self.repr.heap.header,
                            _pad: [0; 7],
                            tag: TAG_HEAP,
                        },
                    },
                };
                self.repr.heap.ptr = base_ptr.add(at);
                self.repr.heap.len = (len - at) as u32;
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
                let base_ptr = self.repr.heap.ptr;
                let head = Self {
                    repr: Repr {
                        heap: HeapRepr {
                            ptr: base_ptr,
                            len: at as u32,
                            cap: 0,
                            header: ptr::null_mut(),
                            _pad: [0; 7],
                            tag: TAG_STATIC,
                        },
                    },
                };
                self.repr.heap.ptr = base_ptr.add(at);
                self.repr.heap.len = (len - at) as u32;
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
                let header = &*self.repr.heap.header;
                header.ref_cnt.fetch_add(1, Ordering::Relaxed);
                let base_ptr = self.repr.heap.ptr;
                let tail = Self {
                    repr: Repr {
                        heap: HeapRepr {
                            ptr: base_ptr.add(at),
                            len: (len - at) as u32,
                            cap: 0,
                            header: self.repr.heap.header,
                            _pad: [0; 7],
                            tag: TAG_HEAP,
                        },
                    },
                };
                self.repr.heap.len = at as u32;
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
                let base_ptr = self.repr.heap.ptr;
                let tail = Self {
                    repr: Repr {
                        heap: HeapRepr {
                            ptr: base_ptr.add(at),
                            len: (len - at) as u32,
                            cap: 0,
                            header: ptr::null_mut(),
                            _pad: [0; 7],
                            tag: TAG_STATIC,
                        },
                    },
                };
                self.repr.heap.len = at as u32;
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
        let header = unsafe { self.repr.heap.header };
        let h = unsafe { &*header };
        let current = h.ref_cnt.load(Ordering::Acquire);
        if current & COUNT_MASK != 1 {
            return Err(self);
        }
        // Check if ptr has been advanced (by Bytes::split_to or advance).
        // If so, the usable capacity is reduced by the offset.
        let alloc_data_start = h.data_ptr();
        let ptr = unsafe { self.repr.heap.ptr };
        let offset = ptr as usize - alloc_data_start as usize;
        let actual_cap = (h.cap as usize).saturating_sub(offset);

        h.ref_cnt.store(1, Ordering::Release);
        let bm = unsafe {
            BytesMut {
                repr: MutRepr {
                    heap: HeapRepr {
                        ptr,
                        len: self.repr.heap.len,
                        cap: actual_cap as u32,
                        header,
                        _pad: [0; 7],
                        tag: TAG_HEAP,
                    },
                },
            }
        };
        std::mem::forget(self);
        Ok(bm)
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        match self.repr.tag() {
            TAG_INLINE | TAG_STATIC => true,
            TAG_HEAP => unsafe { (*self.repr.heap.header).count() == 1 },
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
                    (*self.repr.heap.header)
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
        let header = unsafe { self.repr.heap.header };
        let h = unsafe { &*header };
        // Fast path: sole owner — plain load avoids expensive `lock xadd`.
        // If count == 1, no other thread holds a reference, so a plain load
        // suffices. On x86, all loads have acquire semantics at hardware level.
        if h.ref_cnt.load(Ordering::Acquire) & COUNT_MASK == 1 {
            unsafe { Heap::dealloc_raw(h.heap_data, header as *mut u8) };
            return;
        }
        // Shared path: atomic decrement.
        if h.ref_cnt.fetch_sub(1, Ordering::Release) & COUNT_MASK == 1 {
            fence(Ordering::Acquire);
            unsafe { Heap::dealloc_raw(h.heap_data, header as *mut u8) };
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
                self.repr.heap.ptr = self.repr.heap.ptr.add(cnt);
                self.repr.heap.len -= cnt as u32;
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
    #[inline]
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

    #[inline]
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
                heap: HeapRepr {
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
            ptr::write_bytes(bm.repr.heap.ptr, 0, len);
            bm.repr.heap.len = len as u32;
        }
        Ok(bm)
    }

    // -- Accessors (inline/heap dispatch) ------------------------------------

    #[inline]
    pub fn len(&self) -> usize {
        match self.repr.tag() {
            TAG_INLINE => unsafe { (*self.repr.inline).len as usize },
            _ => unsafe { self.repr.heap.len as usize }, // TAG_HEAP
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
            _ => unsafe { self.repr.heap.cap as usize }, // TAG_HEAP
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
                let len = unsafe { self.repr.heap.len as usize };
                if len == 0 {
                    &[]
                } else {
                    unsafe { slice::from_raw_parts(self.repr.heap.ptr, len) }
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
                let len = unsafe { self.repr.heap.len as usize };
                if len == 0 {
                    &mut []
                } else {
                    unsafe { slice::from_raw_parts_mut(self.repr.heap.ptr, len) }
                }
            }
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        match self.repr.tag() {
            TAG_INLINE => unsafe { (*self.repr.inline).len = 0 },
            _ => unsafe { self.repr.heap.len = 0 },
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            match self.repr.tag() {
                TAG_INLINE => unsafe { (*self.repr.inline).len = len as u8 },
                _ => unsafe { self.repr.heap.len = len as u32 },
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
                    ptr::write_bytes(self.repr.heap.ptr.add(old_len), value, new_len - old_len);
                    self.repr.heap.len = new_len as u32;
                },
            }
        } else {
            self.truncate(new_len);
        }
        Ok(())
    }

    #[inline]
    pub fn extend_from_slice(
        &mut self,
        data: &[u8],
    ) -> Result<(), allocator_api2::alloc::AllocError> {
        let data_len = data.len();
        if data_len == 0 {
            return Ok(());
        }
        // Fast path: heap mode with enough spare capacity.
        if self.repr.tag() == TAG_HEAP {
            let old_len = unsafe { self.repr.heap.len as usize };
            let needed = old_len + data_len;
            if needed <= unsafe { self.repr.heap.cap as usize } {
                unsafe {
                    ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        self.repr.heap.ptr.add(old_len),
                        data_len,
                    );
                    self.repr.heap.len = needed as u32;
                }
                return Ok(());
            }
        }
        self.extend_from_slice_slow(data)
    }

    #[cold]
    #[inline(never)]
    fn extend_from_slice_slow(
        &mut self,
        data: &[u8],
    ) -> Result<(), allocator_api2::alloc::AllocError> {
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
                        self.repr.heap.ptr.add(old_len),
                        data.len(),
                    );
                    self.repr.heap.len = needed as u32;
                }
            }
            _ => {
                // Heap mode but needs growth.
                self.try_grow(needed)?;
                unsafe {
                    ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        self.repr.heap.ptr.add(old_len),
                        data.len(),
                    );
                    self.repr.heap.len = needed as u32;
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
                *self.repr.heap.ptr.add(len) = byte;
                self.repr.heap.len = (len + 1) as u32;
            },
            _ => unsafe {
                if self.repr.heap.len == self.repr.heap.cap {
                    self.try_grow(self.repr.heap.len as usize + 1)?;
                }
                let len = self.repr.heap.len as usize;
                *self.repr.heap.ptr.add(len) = byte;
                self.repr.heap.len += 1;
            },
        }
        Ok(())
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) -> Result<(), allocator_api2::alloc::AllocError> {
        let needed = self.len() + additional;
        match self.repr.tag() {
            TAG_INLINE => {
                if needed > MUT_INLINE_CAP {
                    self.promote_to_heap(needed)?;
                }
            }
            _ => {
                if needed > unsafe { self.repr.heap.cap } as usize {
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
                            heap: HeapRepr {
                                ptr: self.repr.heap.ptr,
                                len: self.repr.heap.len,
                                cap: self.repr.heap.cap,
                                header: self.repr.heap.header,
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

    /// Split at `at`, returning `[0..at)` as immutable [`Bytes`] and leaving
    /// `[at..len)` in self.
    ///
    /// For heap mode this is **zero-copy**: the returned `Bytes` shares the
    /// underlying allocation (refcount bump + pointer advance). BytesMut
    /// continues to own `[at..cap)` exclusively — writes only touch memory
    /// *after* the split point, so no COW check is needed on the write path.
    ///
    /// For inline mode the data is copied (≤22 bytes).
    pub fn split_to(&mut self, at: usize) -> Bytes {
        assert!(at <= self.len());
        match self.repr.tag() {
            TAG_INLINE => {
                let len = unsafe { (*self.repr.inline).len as usize };
                // Inline: copy the head, shift the tail left.
                let mut inline = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: at as u8,
                    tag: TAG_INLINE,
                };
                unsafe {
                    ptr::copy_nonoverlapping(
                        (*self.repr.inline).data.as_ptr(),
                        inline.data.as_mut_ptr(),
                        at,
                    );
                    let remaining = len - at;
                    ptr::copy(
                        (*self.repr.inline).data.as_ptr().add(at),
                        (*self.repr.inline).data.as_mut_ptr(),
                        remaining,
                    );
                    (*self.repr.inline).len = remaining as u8;
                }
                Bytes {
                    repr: Repr { inline },
                }
            }
            _ => unsafe {
                // Heap: zero-copy split — share the allocation.
                let header = self.repr.heap.header;
                (*header).ref_cnt.fetch_add(1, Ordering::Relaxed);
                let head = Bytes {
                    repr: Repr {
                        heap: HeapRepr {
                            ptr: self.repr.heap.ptr,
                            len: at as u32,
                            cap: 0,
                            header,
                            _pad: [0; 7],
                            tag: TAG_HEAP,
                        },
                    },
                };
                // Advance self past the split region.
                self.repr.heap.ptr = self.repr.heap.ptr.add(at);
                self.repr.heap.len -= at as u32;
                self.repr.heap.cap -= at as u32;
                head
            },
        }
    }

    /// Split at `at`, returning `[at..len)` as immutable [`Bytes`] and leaving
    /// `[0..at)` in self.
    ///
    /// For heap mode this is **zero-copy**: the returned `Bytes` shares the
    /// underlying allocation. Self keeps `[0..at)` with the original capacity.
    pub fn split_off(&mut self, at: usize) -> Bytes {
        assert!(at <= self.len());
        match self.repr.tag() {
            TAG_INLINE => {
                let len = unsafe { (*self.repr.inline).len as usize };
                let tail_len = len - at;
                let mut inline = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: tail_len as u8,
                    tag: TAG_INLINE,
                };
                unsafe {
                    ptr::copy_nonoverlapping(
                        (*self.repr.inline).data.as_ptr().add(at),
                        inline.data.as_mut_ptr(),
                        tail_len,
                    );
                    (*self.repr.inline).len = at as u8;
                }
                Bytes {
                    repr: Repr { inline },
                }
            }
            _ => unsafe {
                // Heap: zero-copy split — share the allocation.
                let header = self.repr.heap.header;
                let len = self.repr.heap.len as usize;
                let tail_len = len - at;
                (*header).ref_cnt.fetch_add(1, Ordering::Relaxed);
                let tail = Bytes {
                    repr: Repr {
                        heap: HeapRepr {
                            ptr: self.repr.heap.ptr.add(at),
                            len: tail_len as u32,
                            cap: 0,
                            header,
                            _pad: [0; 7],
                            tag: TAG_HEAP,
                        },
                    },
                };
                self.repr.heap.len = at as u32;
                // Cap must be clamped: [ptr+at..ptr+cap) is now shared
                // with the tail Bytes. BytesMut can only write to [ptr..ptr+at).
                // Extending past `at` will trigger try_grow → new allocation.
                self.repr.heap.cap = at as u32;
                tail
            },
        }
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
                self.repr.heap.len = new_len as u32;
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
                let len = self.repr.heap.len as usize;
                let cap = self.repr.heap.cap as usize;
                if cap == 0 || cap == len {
                    return &mut [];
                }
                slice::from_raw_parts_mut(
                    self.repr.heap.ptr.add(len) as *mut MaybeUninit<u8>,
                    cap - len,
                )
            },
        }
    }

    /// Freeze into immutable [`Bytes`]. Zero-cost for heap buffers >30 bytes.
    ///
    /// The hot path (heap, len > 30) is a transmute — no copies, no atomics.
    /// Cold paths (inline, heap→inline promotion, empty) are outlined to keep
    /// the inlined code tiny.
    #[inline]
    pub fn freeze(self) -> Bytes {
        // Hot path: heap mode with len > INLINE_CAP — transmute (same layout).
        if self.repr.tag() == TAG_HEAP {
            let len = unsafe { self.repr.heap.len as usize };
            if len > INLINE_CAP {
                // BytesMut and Bytes share identical HeapRepr. FROZEN_BIT is
                // already set at alloc time. This is a zero-cost reinterpret.
                return unsafe { std::mem::transmute(self) };
            }
        }
        self.freeze_cold()
    }

    #[cold]
    #[inline(never)]
    fn freeze_cold(self) -> Bytes {
        let len = self.len();
        if len == 0 {
            return Bytes::new();
        }
        match self.repr.tag() {
            TAG_INLINE => {
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
                Bytes {
                    repr: Repr { inline },
                }
            }
            _ => {
                // Heap mode with small data — promote to inline Bytes.
                let header = unsafe { self.repr.heap.header };
                let mut inline = InlineRepr {
                    data: [0; INLINE_CAP],
                    len: len as u8,
                    tag: TAG_INLINE,
                };
                unsafe {
                    ptr::copy_nonoverlapping(self.repr.heap.ptr, inline.data.as_mut_ptr(), len);
                }
                if !header.is_null() {
                    let h = unsafe { &*header };
                    // May be shared via split_to — check refcount.
                    if h.ref_cnt.load(Ordering::Acquire) & COUNT_MASK == 1 {
                        unsafe { Heap::dealloc_raw(h.heap_data, header as *mut u8) };
                    } else {
                        h.ref_cnt.fetch_sub(1, Ordering::Release);
                    }
                }
                std::mem::forget(self);
                Bytes {
                    repr: Repr { inline },
                }
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
    ///
    /// For inline mode: clones the embedded heap handle (cheap).
    /// For heap mode: reconstructs from the header's raw pointer (TLRC increment).
    /// Only used on cold paths (split, clone).
    #[inline]
    fn heap_ref(&self) -> Heap {
        match self.repr.tag() {
            TAG_INLINE => unsafe { (*self.repr.inline).heap.clone() },
            _ => unsafe { Heap::from_data_ptr((*self.repr.heap.header).heap_data) },
        }
    }

    /// Promote inline → heap mode with at least `min_cap` capacity.
    /// Copies existing inline data to the new heap allocation.
    #[inline]
    fn promote_to_heap(&mut self, min_cap: usize) -> Result<(), allocator_api2::alloc::AllocError> {
        debug_assert_eq!(self.repr.tag(), TAG_INLINE);
        let new_cap = min_cap.next_power_of_two().max(64);
        let old_len = unsafe { (*self.repr.inline).len as usize };

        // Allocate using a reference to the inline heap.
        // `alloc_header_and_data` stores a raw HeapData pointer — no TLRC overhead.
        let (header, data_ptr) =
            alloc_header_and_data(unsafe { &(*self.repr.inline).heap }, new_cap)?;

        // Copy existing inline data to the heap allocation.
        if old_len > 0 {
            unsafe {
                ptr::copy_nonoverlapping((*self.repr.inline).data.as_ptr(), data_ptr, old_len);
            }
        }

        // Drop the inline repr (releases the inline Heap TlrcRef — TLRC decrement).
        unsafe { std::mem::ManuallyDrop::drop(&mut self.repr.inline) };

        self.repr = MutRepr {
            heap: HeapRepr {
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
    ///
    /// If the buffer is shared (refcount > 1, from `split_to`/`split_off`),
    /// allocates a fresh buffer and copies. Otherwise reallocs in place.
    #[inline]
    fn try_grow(&mut self, min_cap: usize) -> Result<(), allocator_api2::alloc::AllocError> {
        debug_assert_eq!(self.repr.tag(), TAG_HEAP);
        let new_cap = min_cap.next_power_of_two().max(64);
        let header = unsafe { self.repr.heap.header };
        let heap_data = unsafe { &*(*header).heap_data };

        // Shared buffer: can't realloc (would invalidate Bytes views).
        // Allocate a fresh buffer and copy our data into it.
        if unsafe { (*header).ref_cnt.load(Ordering::Acquire) } & COUNT_MASK > 1 {
            return self.grow_shared(new_cap, header, heap_data);
        }

        let new_total = HEADER_SIZE + new_cap;
        // Header alignment is 8 (== natural mimalloc alignment on 64-bit),
        // so mi_heap_realloc suffices — no need for the aligned variant.
        let new_raw = unsafe {
            ffi::mi_heap_realloc(
                heap_data.mi_heap,
                header as *mut core::ffi::c_void,
                new_total,
            )
        };
        if new_raw.is_null() {
            return Err(allocator_api2::alloc::AllocError);
        }
        let new_header = new_raw as *mut Header;
        unsafe { (*new_header).cap = new_cap as u32 };
        unsafe {
            self.repr.heap.header = new_header;
            self.repr.heap.ptr = (new_raw as *mut u8).add(HEADER_SIZE);
            self.repr.heap.cap = new_cap as u32;
        }
        Ok(())
    }

    /// Cold path: grow when the buffer is shared with Bytes views.
    /// Allocates a fresh buffer, copies data, decrements the old refcount.
    #[cold]
    #[inline(never)]
    fn grow_shared(
        &mut self,
        new_cap: usize,
        old_header: *mut Header,
        heap_data: &HeapData,
    ) -> Result<(), allocator_api2::alloc::AllocError> {
        let old_len = unsafe { self.repr.heap.len as usize };
        let new_total = HEADER_SIZE + new_cap;
        let new_raw = unsafe { ffi::mi_heap_malloc(heap_data.mi_heap, new_total) as *mut u8 };
        if new_raw.is_null() {
            return Err(allocator_api2::alloc::AllocError);
        }
        let new_header = new_raw as *mut Header;
        unsafe {
            ptr::write(
                new_header,
                Header {
                    ref_cnt: AtomicU32::new(1 | FROZEN_BIT),
                    cap: new_cap as u32,
                    heap_data: heap_data as *const HeapData,
                },
            );
            let new_data = new_raw.add(HEADER_SIZE);
            // Copy only our live data (not the entire old allocation).
            if old_len > 0 {
                ptr::copy_nonoverlapping(self.repr.heap.ptr, new_data, old_len);
            }
            // Release our reference to the old allocation.
            // Other Bytes views keep it alive via their own refcounts.
            (*old_header).ref_cnt.fetch_sub(1, Ordering::Release);
            // Point to the new allocation.
            self.repr.heap.header = new_header;
            self.repr.heap.ptr = new_data;
            self.repr.heap.cap = new_cap as u32;
        }
        Ok(())
    }
}

// -- BytesMut trait impls --

impl Drop for BytesMut {
    #[inline]
    fn drop(&mut self) {
        match self.repr.tag() {
            TAG_HEAP => {
                let header = unsafe { self.repr.heap.header };
                if header.is_null() {
                    return;
                }
                let h = unsafe { &*header };
                // If shared (Bytes views from split_to/split_off still alive),
                // just decrement the refcount. Otherwise free directly.
                let cnt = h.ref_cnt.load(Ordering::Acquire);
                if cnt & COUNT_MASK == 1 {
                    unsafe { Heap::dealloc_raw(h.heap_data, header as *mut u8) };
                } else {
                    h.ref_cnt.fetch_sub(1, Ordering::Release);
                }
            }
            TAG_INLINE => unsafe {
                std::mem::ManuallyDrop::drop(&mut self.repr.inline);
            },
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
                let heap = self.heap_ref();
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
                    ptr::copy(self.repr.heap.ptr.add(cnt), self.repr.heap.ptr, new_len);
                }
                self.repr.heap.len = new_len as u32;
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
            _ => self.repr.heap.len = new_len as u32,
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
                let ptr = self.repr.heap.ptr.add(self.repr.heap.len as usize);
                let spare = self.repr.heap.cap - self.repr.heap.len;
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
                    self.repr.heap.ptr.add(self.repr.heap.len as usize),
                    val,
                    cnt,
                );
                self.repr.heap.len += cnt as u32;
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

#[inline]
fn alloc_header_and_data(
    heap: &Heap,
    cap: usize,
) -> Result<(*mut Header, *mut u8), allocator_api2::alloc::AllocError> {
    let total = HEADER_SIZE + cap;
    let raw = unsafe { ffi::mi_heap_malloc(heap.data().mi_heap, total) as *mut u8 };
    if raw.is_null() {
        return Err(allocator_api2::alloc::AllocError);
    }
    let header = raw as *mut Header;
    unsafe {
        ptr::write(
            header,
            Header {
                ref_cnt: AtomicU32::new(1 | FROZEN_BIT),
                cap: cap as u32,
                heap_data: heap.data_ptr(),
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

    // -- Zero-copy split_to (shared buffer) --

    #[test]
    fn split_to_zero_copy_shares_allocation() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(b"AAAABBBBCCCC").unwrap();

        // Split out "AAAA" as zero-copy Bytes.
        let frame1 = bm.split_to(4);
        assert_eq!(&frame1[..], b"AAAA");
        assert_eq!(&bm[..], b"BBBBCCCC");

        // Split out "BBBB".
        let frame2 = bm.split_to(4);
        assert_eq!(&frame2[..], b"BBBB");
        assert_eq!(&bm[..], b"CCCC");

        // BytesMut can still append without affecting the Bytes views.
        bm.extend_from_slice(b"DDDD").unwrap();
        assert_eq!(&bm[..], b"CCCCDDDD");
        // Frames are unaffected.
        assert_eq!(&frame1[..], b"AAAA");
        assert_eq!(&frame2[..], b"BBBB");

        // All three can coexist and drop in any order.
        drop(frame1);
        drop(bm);
        drop(frame2);
    }

    #[test]
    fn split_to_then_grow_detaches() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 32]).unwrap();

        let frame = bm.split_to(16);
        assert_eq!(frame.len(), 16);
        assert_eq!(bm.len(), 16);

        // Capacity was reduced by the split.
        assert!(bm.capacity() <= 64 - 16);

        // Append enough to force a grow — this must allocate a new buffer
        // (can't realloc because Bytes `frame` still references the old one).
        bm.extend_from_slice(&[0xBB; 128]).unwrap();
        assert_eq!(bm.len(), 16 + 128);
        assert_eq!(&bm[..16], &[0xAA; 16]);
        assert_eq!(&bm[16..], &[0xBB; 128]);

        // Original frame is unaffected.
        assert_eq!(&frame[..], &[0xAA; 16]);
    }

    #[test]
    fn split_off_zero_copy() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(b"HEADTAIL").unwrap();

        let tail = bm.split_off(4);
        assert_eq!(&bm[..], b"HEAD");
        assert_eq!(&tail[..], b"TAIL");
        // Cap is clamped to `at` — can't write into tail's region.
        assert_eq!(bm.capacity(), 4);

        // Extending triggers try_grow → new allocation (shared buffer).
        bm.extend_from_slice(b"MORE").unwrap();
        assert_eq!(&bm[..], b"HEADMORE");
        // Tail is unaffected — shared allocation kept alive.
        assert_eq!(&tail[..], b"TAIL");
    }

    #[test]
    fn split_to_freeze_round_trip() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(b"frame1frame2partial").unwrap();

        let f1 = bm.split_to(6);
        let f2 = bm.split_to(6);
        assert_eq!(&f1[..], b"frame1");
        assert_eq!(&f2[..], b"frame2");
        assert_eq!(&bm[..], b"partial");

        // Freeze the remaining partial — should work even with shared buffer.
        let frozen = bm.freeze();
        assert_eq!(&frozen[..], b"partial");

        // All views are independent.
        drop(f1);
        drop(f2);
        drop(frozen);
    }

    #[test]
    fn split_to_protocol_parsing_pattern() {
        // Simulates the network protocol parsing use case:
        // read big chunk → split frames → compact partial → repeat.
        let h = heap();
        let mut read_buf = BytesMut::with_capacity(1024, &h).unwrap();

        // Simulate first read: 3 complete 100B frames + 50B partial.
        let mut data = vec![0u8; 350];
        for i in 0..3 {
            data[i * 100..(i + 1) * 100].fill((i + 1) as u8);
        }
        data[300..350].fill(0xFF); // partial frame
        read_buf.extend_from_slice(&data).unwrap();

        // Parse frames.
        let mut frames = Vec::new();
        for _ in 0..3 {
            frames.push(read_buf.split_to(100));
        }
        assert_eq!(read_buf.len(), 50); // partial frame remains

        // Verify frames.
        assert!(frames[0].iter().all(|&b| b == 1));
        assert!(frames[1].iter().all(|&b| b == 2));
        assert!(frames[2].iter().all(|&b| b == 3));
        assert!(read_buf.as_slice().iter().all(|&b| b == 0xFF));

        // Append more data (simulates next read). BytesMut is shared but
        // can still append within remaining capacity.
        let more_data = vec![0xEE; 50]; // complete the partial frame
        read_buf.extend_from_slice(&more_data).unwrap();
        assert_eq!(read_buf.len(), 100);

        // Split the completed frame.
        let f4 = read_buf.split_to(100);
        assert_eq!(&f4[..50], &[0xFF; 50]);
        assert_eq!(&f4[50..], &[0xEE; 50]);

        // Original frames still valid.
        assert!(frames[0].iter().all(|&b| b == 1));
    }

    #[test]
    fn drop_shared_bytesmut_does_not_free() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(b"keep alive").unwrap();

        let frame = bm.split_to(4);
        assert_eq!(&frame[..], b"keep");

        // Drop BytesMut while frame is alive — must not free the allocation.
        drop(bm);
        // frame should still be valid.
        assert_eq!(&frame[..], b"keep");
    }

    #[test]
    fn split_to_inline_still_copies() {
        let h = heap();
        let mut bm = BytesMut::new(&h);
        bm.extend_from_slice(b"small").unwrap();
        assert_eq!(bm.capacity(), MUT_INLINE_CAP); // inline mode

        let head = bm.split_to(3);
        assert_eq!(&head[..], b"sma");
        assert_eq!(&bm[..], b"ll");
    }

    // -- Boundary conditions --

    #[test]
    fn split_to_at_zero() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(b"hello").unwrap();
        let head = bm.split_to(0);
        assert!(head.is_empty());
        assert_eq!(&bm[..], b"hello");
    }

    #[test]
    fn split_to_at_len() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(b"hello").unwrap();
        let head = bm.split_to(5);
        assert_eq!(&head[..], b"hello");
        assert!(bm.is_empty());
    }

    #[test]
    fn split_off_at_zero() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(b"hello").unwrap();
        let tail = bm.split_off(0);
        assert_eq!(&tail[..], b"hello");
        assert!(bm.is_empty());
        assert_eq!(bm.capacity(), 0);
    }

    #[test]
    fn split_off_at_len() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(b"hello").unwrap();
        let tail = bm.split_off(5);
        assert!(tail.is_empty());
        assert_eq!(&bm[..], b"hello");
    }

    // -- Exhaustive split_to capacity drain --

    #[test]
    fn split_to_exhaust_then_grow() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 60]).unwrap();

        // Split off most of the data in chunks.
        let mut frames = Vec::new();
        for _ in 0..6 {
            frames.push(bm.split_to(10));
        }
        assert_eq!(bm.len(), 0);
        assert!(bm.capacity() <= 4); // only a few bytes left

        // Now extend — forces grow_shared (refcount > 1).
        bm.extend_from_slice(&[0xBB; 100]).unwrap();
        assert_eq!(bm.len(), 100);
        assert!(bm.as_slice().iter().all(|&b| b == 0xBB));

        // Original frames unaffected.
        for f in &frames {
            assert!(f.iter().all(|&b| b == 0xAA));
        }
    }

    // -- try_mut after Bytes::split_to (ptr offset) --

    #[test]
    fn try_mut_after_bytes_split_to() {
        let h = heap();
        let b = Bytes::copy_from_slice(&[0xAA; 128], &h).unwrap();
        let mut b2 = b;
        let _head = b2.split_to(64); // ptr advances by 64
        // _head holds a refcount, so try_mut fails.
        drop(_head);
        // Now sole owner. try_mut should succeed with reduced cap.
        let bm = b2.try_mut().expect("try_mut should succeed");
        assert_eq!(bm.len(), 64);
        // Cap must NOT be the original 128 — it should be 128 - 64 = 64.
        assert!(
            bm.capacity() <= 64,
            "cap should be reduced, got {}",
            bm.capacity()
        );
        assert!(bm.as_slice().iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn try_mut_after_bytes_advance() {
        let h = heap();
        // Must be > INLINE_CAP (30) to be heap-backed.
        let data: Vec<u8> = (0..64).collect();
        let mut b = Bytes::copy_from_slice(&data, &h).unwrap();
        use bytes::Buf;
        b.advance(32);
        assert_eq!(b.len(), 32);
        assert_eq!(b[0], 32);
        let bm = b.try_mut().expect("sole owner");
        assert_eq!(bm.len(), 32);
        assert_eq!(bm[0], 32);
        // Cap should be 64 - 32 = 32 (original cap minus offset).
        assert!(
            bm.capacity() <= 32,
            "cap should be reduced, got {}",
            bm.capacity()
        );
    }

    // -- freeze after split_to (shared buffer) --

    #[test]
    fn freeze_shared_buffer_small() {
        // Freeze a shared BytesMut where remaining data fits inline.
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(b"ABCDhello").unwrap();
        let frame = bm.split_to(4);
        assert_eq!(&frame[..], b"ABCD");

        // Remaining "hello" (5 bytes) fits inline. freeze_cold should
        // decrement the shared refcount instead of freeing.
        let frozen = bm.freeze();
        assert_eq!(&frozen[..], b"hello");
        assert_eq!(&frame[..], b"ABCD"); // still valid
    }

    // -- Multi-threaded tests --

    #[test]
    fn concurrent_bytes_clone_drop() {
        use std::sync::Arc;
        let h = heap();
        let b = Bytes::copy_from_slice(&[42u8; 256], &h).unwrap();
        let shared = Arc::new(b);

        std::thread::scope(|s| {
            for _ in 0..8 {
                let shared = shared.clone();
                s.spawn(move || {
                    for _ in 0..10_000 {
                        let c = (*shared).clone();
                        assert_eq!(c.len(), 256);
                        assert_eq!(c[0], 42);
                        drop(c);
                    }
                });
            }
        });
        // After all threads finish, refcount should be back to 1
        // (the Arc's Bytes). Dropping should not panic/leak.
        let b = Arc::try_unwrap(shared).unwrap();
        assert!(b.is_unique());
    }

    #[test]
    fn concurrent_split_to_frames() {
        // Simulate protocol parsing: one thread produces frames via split_to,
        // consumer threads receive and drop them.
        use std::sync::Arc;
        use std::sync::Mutex;
        let h = heap();
        let mut bm = BytesMut::with_capacity(8192, &h).unwrap();
        // Fill with 100 frames of 80 bytes each.
        for i in 0u8..100 {
            bm.extend_from_slice(&[i; 80]).unwrap();
        }

        // Split all frames.
        let frames: Vec<Bytes> = (0..100).map(|_| bm.split_to(80)).collect();
        let frames = Arc::new(Mutex::new(frames));

        // Consumer threads pop frames and verify.
        std::thread::scope(|s| {
            for _ in 0..4 {
                let frames = frames.clone();
                s.spawn(move || {
                    loop {
                        let frame = {
                            let mut guard = frames.lock().unwrap();
                            guard.pop()
                        };
                        match frame {
                            Some(f) => {
                                assert_eq!(f.len(), 80);
                                let expected = f[0];
                                assert!(f.iter().all(|&b| b == expected));
                            }
                            None => break,
                        }
                    }
                });
            }
        });

        // BytesMut should be empty. All frames dropped — allocation freed.
        assert!(bm.is_empty());
    }

    #[test]
    fn concurrent_bytes_split_to_clone() {
        // Bytes::split_to + clone across threads.
        use std::sync::Arc;
        let h = heap();
        let b = Bytes::copy_from_slice(&[0xAA; 1024], &h).unwrap();
        let shared = Arc::new(b);

        std::thread::scope(|s| {
            for t in 0..4 {
                let shared = shared.clone();
                s.spawn(move || {
                    let mut local = (*shared).clone();
                    // Each thread slices a different region.
                    let offset = t * 256;
                    let sub = local.slice(offset..offset + 256);
                    assert_eq!(sub.len(), 256);
                    assert!(sub.iter().all(|&b| b == 0xAA));
                    // Clone and drop many times.
                    for _ in 0..1000 {
                        let c = sub.clone();
                        assert_eq!(c.len(), 256);
                        drop(c);
                    }
                });
            }
        });
    }

    // -- Refcount leak detection --

    #[test]
    fn no_leak_split_to_all_dropped() {
        // Ensure refcount reaches 0 and allocation is freed.
        let h = heap();
        let mut bm = BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(&[0xFF; 200]).unwrap();

        let f1 = bm.split_to(50);
        let f2 = bm.split_to(50);
        let f3 = bm.split_to(50);
        let remaining = bm.freeze();

        // Drop in scrambled order.
        drop(f2);
        drop(remaining);
        drop(f1);
        drop(f3);
        // If refcount is wrong, this will leak or double-free.
        // Miri would catch double-free; leak detectors would catch leaks.
    }

    #[test]
    fn no_leak_grow_shared_then_drop_all() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(&[1; 32]).unwrap();

        let frame = bm.split_to(16); // refcount=2
        // Grow triggers grow_shared: new alloc, old refcount decremented to 1.
        bm.extend_from_slice(&[2; 128]).unwrap();

        // frame still points to old allocation.
        assert_eq!(&frame[..], &[1; 16]);
        // bm points to new allocation.
        assert_eq!(&bm[..16], &[1; 16]);
        assert_eq!(&bm[16..], &[2; 128]);

        drop(frame); // old allocation freed (refcount 1 → 0)
        drop(bm); // new allocation freed
    }

    // -- Edge cases --

    #[test]
    fn split_to_then_clear_then_extend() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 64]).unwrap();

        let frame = bm.split_to(32);
        assert_eq!(bm.len(), 32);

        // Clear and reuse remaining capacity.
        bm.clear();
        assert_eq!(bm.len(), 0);
        assert!(bm.capacity() > 0);

        bm.extend_from_slice(&[0xBB; 32]).unwrap();
        assert_eq!(&bm[..], &[0xBB; 32]);
        // frame unaffected.
        assert_eq!(&frame[..], &[0xAA; 32]);
    }

    #[test]
    fn bytes_split_to_heap_then_clone_and_drop() {
        let h = heap();
        let mut b = Bytes::copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8], &h).unwrap();
        let head = b.split_to(4); // refcount 1→2
        let head2 = head.clone(); // refcount 2→3
        assert_eq!(&head[..], &[1, 2, 3, 4]);
        assert_eq!(&head2[..], &[1, 2, 3, 4]);
        assert_eq!(&b[..], &[5, 6, 7, 8]);

        drop(head); // 3→2
        drop(b); // 2→1
        assert_eq!(&head2[..], &[1, 2, 3, 4]); // still valid
        drop(head2); // 1→0, freed
    }

    #[test]
    fn bytesmut_split_multiple_then_freeze_remaining() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 200]).unwrap();

        let f1 = bm.split_to(50); // refcount 1→2
        let f2 = bm.split_to(50); // refcount 2→3
        let f3 = bm.split_to(50); // refcount 3→4

        // Freeze remaining 50 bytes (still shared, refcount=4).
        // freeze uses transmute (len=50 > INLINE_CAP=30).
        let frozen = bm.freeze(); // BytesMut consumed, but refcount stays 4

        assert_eq!(f1.len(), 50);
        assert_eq!(f2.len(), 50);
        assert_eq!(f3.len(), 50);
        assert_eq!(frozen.len(), 50);

        drop(f1); // 4→3
        drop(f3); // 3→2
        drop(frozen); // 2→1
        drop(f2); // 1→0, freed
    }

    // -- BufMut with shared buffers --

    #[test]
    fn bufmut_put_slice_after_split_to() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 64]).unwrap();

        let frame = bm.split_to(32); // shared, cap reduced
        assert_eq!(frame.len(), 32);
        assert_eq!(bm.len(), 32);

        // BufMut::put_slice should work — appends within remaining capacity
        // or triggers grow_shared if capacity is exceeded.
        use bytes::BufMut;
        bm.put_slice(&[0xBB; 64]);
        assert_eq!(bm.len(), 96);
        assert_eq!(&bm[..32], &[0xAA; 32]);
        assert_eq!(&bm[32..], &[0xBB; 64]);
        assert_eq!(&frame[..], &[0xAA; 32]); // unaffected
    }

    #[test]
    fn bufmut_put_bytes_after_split_to() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(&[1; 32]).unwrap();

        let _frame = bm.split_to(16);
        // put_bytes calls reserve + write_bytes.
        use bytes::BufMut;
        bm.put_bytes(0xFF, 100);
        assert_eq!(bm.len(), 116);
        assert_eq!(&bm[..16], &[1; 16]);
        assert_eq!(&bm[16..], &[0xFF; 100]);
    }

    #[test]
    fn bufmut_chunk_mut_after_split_to() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 64]).unwrap();

        let _frame = bm.split_to(32);
        // chunk_mut should return spare capacity in the remaining region.
        use bytes::BufMut;
        let chunk = bm.chunk_mut();
        assert!(chunk.len() > 0);
    }

    // -- into_bytes_crate with shared buffers --

    #[test]
    fn into_bytes_crate_after_split_to() {
        let h = heap();
        let mut bm = BytesMut::with_capacity(128, &h).unwrap();
        bm.extend_from_slice(b"shared-buffer-data!!").unwrap();

        let frame = bm.split_to(14);
        assert_eq!(&frame[..], b"shared-buffer-");

        // into_bytes_crate freezes then wraps in bytes::Bytes.
        let ecosystem_bytes = bm.into_bytes_crate();
        assert_eq!(&ecosystem_bytes[..], b"data!!");
        // frame still valid (original allocation alive).
        assert_eq!(&frame[..], b"shared-buffer-");
    }

    // -- Miri-oriented tests (exercise unsafe code paths) --
    // These are specifically designed to catch UB under miri:
    // - Use-after-free
    // - Double-free
    // - Out-of-bounds access
    // - Uninitialized reads
    // - Data races (miri -Zmiri-check-number-validity)

    #[test]
    fn miri_bytes_lifecycle_inline() {
        // Exercise inline Bytes: construct, clone, slice, split, drop.
        let h = heap();
        let b = Bytes::copy_from_slice(b"hello miri!", &h).unwrap();
        assert_eq!(b.len(), 11);

        let c = b.clone();
        assert_eq!(&c[..], b"hello miri!");

        let s = b.slice(6..10);
        assert_eq!(&s[..], b"miri");

        let mut b2 = b;
        let head = b2.split_to(5);
        assert_eq!(&head[..], b"hello");
        assert_eq!(&b2[..], b" miri!");

        drop(c);
        drop(s);
        drop(head);
        drop(b2);
    }

    #[test]
    fn miri_bytes_lifecycle_heap() {
        // Exercise heap Bytes: construct, clone, slice, split, drop.
        let h = heap();
        let data = vec![0xCDu8; 128];
        let b = Bytes::copy_from_slice(&data, &h).unwrap();

        let c1 = b.clone(); // refcount 1→2
        let c2 = b.clone(); // 2→3
        let s = b.slice(10..50); // 3→4
        let mut b2 = b;
        let head = b2.split_to(64); // 4→5

        assert_eq!(c1.len(), 128);
        assert_eq!(c2.len(), 128);
        assert_eq!(s.len(), 40);
        assert_eq!(head.len(), 64);
        assert_eq!(b2.len(), 64);

        // Drop in non-obvious order.
        drop(s); // 5→4
        drop(b2); // 4→3
        drop(c1); // 3→2
        drop(head); // 2→1
        drop(c2); // 1→0 — freed
    }

    #[test]
    fn miri_bytesmut_promote_and_grow() {
        // Exercise the inline→heap promotion and growth paths.
        let h = heap();
        let mut bm = BytesMut::new(&h);
        assert_eq!(bm.capacity(), MUT_INLINE_CAP);

        // Fill inline.
        for i in 0..MUT_INLINE_CAP {
            bm.push(i as u8).unwrap();
        }
        assert_eq!(bm.len(), MUT_INLINE_CAP);

        // One more byte triggers promote_to_heap.
        bm.push(0xFF).unwrap();
        assert!(bm.capacity() > MUT_INLINE_CAP);
        assert_eq!(bm.len(), MUT_INLINE_CAP + 1);
        assert_eq!(bm[MUT_INLINE_CAP], 0xFF);

        // Grow several times.
        for _ in 0..10 {
            bm.extend_from_slice(&[0xBB; 200]).unwrap();
        }
        assert_eq!(bm.len(), MUT_INLINE_CAP + 1 + 2000);
    }

    #[test]
    fn miri_bytesmut_split_to_shared_lifecycle() {
        // Exercise the full shared buffer lifecycle that touches all unsafe paths:
        // alloc → extend → split_to (refcount bump) → extend (within cap) →
        // grow_shared (new alloc, copy, refcount decrement) → freeze (transmute) →
        // Bytes::Drop (sole-owner fast path) → Bytes::Drop (last ref frees old alloc).
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        // Split creates shared buffer.
        let frame = bm.split_to(4); // refcount 1→2
        assert_eq!(&frame[..], &[1, 2, 3, 4]);
        assert_eq!(&bm[..], &[5, 6, 7, 8]);

        // Extend within remaining capacity (no grow needed).
        let remaining_cap = bm.capacity() - bm.len();
        if remaining_cap > 0 {
            let fill = vec![0xAA; remaining_cap];
            bm.extend_from_slice(&fill).unwrap();
        }

        // Now extend past capacity — triggers grow_shared.
        bm.extend_from_slice(&[0xBB; 128]).unwrap();

        // bm is now on a new allocation. frame still on the old one.
        assert_eq!(&frame[..], &[1, 2, 3, 4]);
        assert_eq!(&bm[..4], &[5, 6, 7, 8]);

        // Freeze the new allocation.
        let frozen = bm.freeze();
        assert_eq!(&frozen[..4], &[5, 6, 7, 8]);

        // Drop everything.
        drop(frame); // old allocation freed (refcount was 1 after grow_shared decremented)
        drop(frozen); // new allocation freed
    }

    #[test]
    fn miri_freeze_transmute_roundtrip() {
        // Verify the transmute in freeze doesn't cause UB.
        let h = heap();
        let mut bm = BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(&[0xDE; 64]).unwrap();

        // freeze uses transmute (len=64 > INLINE_CAP=30).
        let frozen = bm.freeze();
        assert_eq!(frozen.len(), 64);
        assert!(frozen.iter().all(|&b| b == 0xDE));

        // try_mut reverses it.
        let mut bm2 = frozen.try_mut().unwrap();
        assert_eq!(bm2.len(), 64);
        bm2.extend_from_slice(&[0xAD; 32]).unwrap();
        assert_eq!(bm2.len(), 96);
        assert_eq!(&bm2[..64], &[0xDE; 64]);
        assert_eq!(&bm2[64..], &[0xAD; 32]);
    }

    #[test]
    fn miri_try_mut_offset_ptr_write() {
        // After try_mut on an offset Bytes, ensure writes don't overflow.
        let h = heap();
        let mut b = Bytes::copy_from_slice(&[0u8; 64], &h).unwrap();
        let _head = b.split_to(48); // ptr offset by 48, refcount 1→2
        drop(_head); // refcount 2→1

        let mut bm = b.try_mut().unwrap();
        assert_eq!(bm.len(), 16);
        // Cap should be 64 - 48 = 16.
        assert!(bm.capacity() <= 16);

        // Clear and fill to capacity — must not write out of bounds.
        bm.clear();
        for i in 0..bm.capacity() {
            bm.push(i as u8).unwrap();
        }
        assert_eq!(bm.len(), bm.capacity());
    }

    #[test]
    fn miri_static_bytes_operations() {
        // Static mode exercises different code paths — no refcount, no alloc.
        let b = Bytes::from_static(b"static data for miri");
        let c = b.clone();
        assert_eq!(&c[..], b"static data for miri");

        let s = b.slice(7..11);
        assert_eq!(&s[..], b"data");

        let mut b2 = b;
        let head = b2.split_to(7);
        assert_eq!(&head[..], b"static ");
        assert_eq!(&b2[..], b"data for miri");

        drop(c);
        drop(s);
        drop(head);
        drop(b2);
    }

    #[test]
    fn miri_bytesmut_as_mut_slice_no_alias() {
        // Ensure as_mut_slice doesn't alias with split_to'd Bytes.
        let h = heap();
        let mut bm = BytesMut::with_capacity(64, &h).unwrap();
        bm.extend_from_slice(&[0xAA; 32]).unwrap();

        let frame = bm.split_to(16);

        // Mutate bm's region — must not affect frame.
        let slice = bm.as_mut_slice();
        slice.fill(0xBB);
        assert_eq!(&bm[..], &[0xBB; 16]);
        assert_eq!(&frame[..], &[0xAA; 16]); // unaffected
    }
}
