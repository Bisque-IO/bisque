//! Heap-backed growable byte buffer with fallible allocation.

use allocator_api2::alloc::AllocError;
use std::ptr::NonNull;

use crate::Heap;

/// A growable byte buffer allocated from a [`Heap`].
///
/// Every mutating operation returns `Result<_, AllocError>`. When the
/// heap's arena is exhausted, operations fail gracefully instead of
/// panicking.
///
/// Convertible to `bytes::Bytes` zero-copy via [`into_bytes`](Vec::into_bytes).
pub struct Vec {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
    heap: Heap,
}

unsafe impl Send for Vec {}
unsafe impl Sync for Vec {}

impl Vec {
    /// Create an empty `Vec` with no allocation.
    pub fn new(heap: &Heap) -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            cap: 0,
            heap: heap.clone(),
        }
    }

    /// Create a `Vec` with pre-allocated capacity.
    pub fn with_capacity(cap: usize, heap: &Heap) -> Result<Self, AllocError> {
        if cap == 0 {
            return Ok(Self::new(heap));
        }
        let ptr = heap.alloc(cap, 1);
        if ptr.is_null() {
            return Err(AllocError);
        }
        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            len: 0,
            cap,
            heap: heap.clone(),
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
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Append bytes. Returns `Err(AllocError)` if the heap is full.
    /// On error, existing data is preserved.
    pub fn extend_from_slice(&mut self, src: &[u8]) -> Result<(), AllocError> {
        let needed = self.len + src.len();
        if needed > self.cap {
            self.try_grow(needed)?;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.ptr.as_ptr().add(self.len), src.len());
        }
        self.len += src.len();
        Ok(())
    }

    /// Push a single byte.
    #[inline]
    pub fn push(&mut self, byte: u8) -> Result<(), AllocError> {
        if self.len == self.cap {
            self.try_grow(self.len + 1)?;
        }
        unsafe {
            *self.ptr.as_ptr().add(self.len) = byte;
        }
        self.len += 1;
        Ok(())
    }

    /// Reserve at least `additional` more bytes.
    pub fn reserve(&mut self, additional: usize) -> Result<(), AllocError> {
        let needed = self.len + additional;
        if needed > self.cap {
            self.try_grow(needed)?;
        }
        Ok(())
    }

    /// Truncate to `len` bytes. No-op if `len >= self.len`.
    pub fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
        }
    }

    /// Convert into `bytes::Bytes` zero-copy.
    pub fn into_bytes(self) -> bytes::Bytes {
        if self.len == 0 {
            return bytes::Bytes::new();
        }
        bytes::Bytes::from_owner(self)
    }

    /// Return the contents as a `std::vec::Vec<u8>` by copying.
    pub fn to_std_vec(&self) -> std::vec::Vec<u8> {
        self.as_ref().to_vec()
    }

    fn try_grow(&mut self, min_cap: usize) -> Result<(), AllocError> {
        let new_cap = min_cap.next_power_of_two().max(64);
        if self.cap == 0 {
            let p = self.heap.alloc(new_cap, 1);
            if p.is_null() {
                return Err(AllocError);
            }
            self.ptr = unsafe { NonNull::new_unchecked(p) };
        } else {
            let p = unsafe { self.heap.realloc(self.ptr.as_ptr(), new_cap, 1) };
            if p.is_null() {
                return Err(AllocError);
            }
            self.ptr = unsafe { NonNull::new_unchecked(p) };
        }
        self.cap = new_cap;
        Ok(())
    }
}

impl Drop for Vec {
    fn drop(&mut self) {
        if self.cap > 0 {
            unsafe { self.heap.dealloc(self.ptr.as_ptr()) }
        }
    }
}

impl AsRef<[u8]> for Vec {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl std::ops::Deref for Vec {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl std::ops::DerefMut for Vec {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl std::fmt::Debug for Vec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vec")
            .field("len", &self.len)
            .field("cap", &self.cap)
            .finish()
    }
}
