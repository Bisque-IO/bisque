//! Heap-backed `Arc` (atomically reference-counted pointer) with fallible allocation.

use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Heap;
use allocator_api2::alloc::AllocError;

/// Layout: [refcount: AtomicUsize] [value: T]
/// Stored in a single heap allocation.
///
/// The high bit of `refcount` (bit 63) is the **slab flag**: if set, this
/// allocation lives inside a slab page and must NOT be individually freed
/// via `heap.dealloc()`. The slab page is freed when the heap is destroyed.
/// The actual reference count is in the lower 63 bits.
#[repr(C)]
pub(crate) struct HeapArcInner<T> {
    pub(crate) refcount: AtomicUsize,
    pub(crate) value: T,
}

/// Bit mask for the slab-allocated flag in the refcount field.
const SLAB_FLAG: usize = 1 << (usize::BITS - 1); // bit 63
/// Mask to extract the actual refcount (lower 63 bits).
const REFCOUNT_MASK: usize = !SLAB_FLAG;

/// A thread-safe, atomically reference-counted pointer allocated from a [`Heap`].
///
/// Equivalent to `std::sync::Arc<T>` but allocated from a specific heap
/// with fallible construction. When the last `Arc` is dropped, the value
/// is dropped and the memory is freed back to the heap.
///
/// # Examples
///
/// ```rust
/// use bisque_alloc::HeapMaster;
/// use bisque_alloc::Arc;
///
/// let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
/// let a = Arc::new(42u64, &heap).unwrap();
/// let b = a.clone();
/// assert_eq!(*a, 42);
/// assert_eq!(*b, 42);
/// assert_eq!(Arc::strong_count(&a), 2);
/// ```
pub struct HeapArc<T> {
    ptr: *mut HeapArcInner<T>,
    heap: Heap,
}

unsafe impl<T: Send + Sync> Send for HeapArc<T> {}
unsafe impl<T: Send + Sync> Sync for HeapArc<T> {}

impl<T> HeapArc<T> {
    /// Allocate and initialize a reference-counted value on the heap.
    ///
    /// Returns `Err(AllocError)` if the arena is full.
    pub fn new(value: T, heap: &Heap) -> Result<Self, AllocError> {
        let layout = std::alloc::Layout::new::<HeapArcInner<T>>();
        let raw = heap.alloc(layout.size(), layout.align());
        if raw.is_null() {
            return Err(AllocError);
        }
        let inner = raw as *mut HeapArcInner<T>;
        unsafe {
            std::ptr::write(
                inner,
                HeapArcInner {
                    refcount: AtomicUsize::new(1),
                    value,
                },
            );
        }
        Ok(Self {
            ptr: inner,
            heap: heap.clone(),
        })
    }

    /// Construct a `HeapArc` from a raw `HeapArcInner` pointer and a `Heap`.
    ///
    /// # Safety
    ///
    /// - `ptr` must point to a valid, initialized `HeapArcInner<T>` with
    ///   refcount already set (typically to 1).
    /// - `heap` must be able to deallocate `ptr` when the refcount drops to 0.
    /// - The caller must not use `ptr` after this call except through the
    ///   returned `HeapArc`.
    #[inline]
    pub unsafe fn from_raw_parts(ptr: *mut HeapArcInner<T>, heap: Heap) -> Self {
        Self { ptr, heap }
    }

    /// Returns the number of strong references to this value.
    #[inline]
    pub fn strong_count(this: &Self) -> usize {
        unsafe { (*this.ptr).refcount.load(Ordering::Acquire) & REFCOUNT_MASK }
    }

    /// Returns a reference to the underlying heap.
    #[inline]
    pub fn heap(this: &Self) -> &Heap {
        &this.heap
    }

    /// Returns a mutable reference to the inner value if this is the only
    /// reference (strong count == 1). Returns `None` otherwise.
    ///
    /// This is the `HeapArc` equivalent of [`std::sync::Arc::get_mut`].
    #[inline]
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        let rc = unsafe { (*this.ptr).refcount.load(Ordering::Acquire) };
        if rc & REFCOUNT_MASK == 1 {
            // We are the sole owner — safe to hand out &mut.
            // Acquire fence above synchronizes with the Release in other drops.
            Some(unsafe { &mut (*this.ptr).value })
        } else {
            None
        }
    }

    /// Try to unwrap the `Arc`, returning the inner value if this is the
    /// only reference. Returns `Err(self)` if there are other references.
    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        let inner = unsafe { &*this.ptr };
        let current = inner.refcount.load(Ordering::Acquire);
        let is_slab = current & SLAB_FLAG != 0;
        // CAS: compare against current value (which includes the slab flag),
        // swap to 0 (or SLAB_FLAG to preserve the flag for debugging).
        let expected = if is_slab { SLAB_FLAG | 1 } else { 1 };
        if inner
            .refcount
            .compare_exchange(expected, 0, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // We are the sole owner. Read out the value and free.
            let value = unsafe { std::ptr::read(&inner.value) };
            let raw = this.ptr as *mut u8;
            let heap = this.heap.clone();
            std::mem::forget(this); // prevent Drop from running
            if !is_slab {
                unsafe { heap.dealloc(raw) };
            }
            // Slab-allocated: don't dealloc — the slab page is freed with the heap.
            Ok(value)
        } else {
            Err(this)
        }
    }
}

impl<T> Deref for HeapArc<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &(*self.ptr).value }
    }
}

impl<T> Clone for HeapArc<T> {
    #[inline]
    fn clone(&self) -> Self {
        let old = unsafe { (*self.ptr).refcount.fetch_add(1, Ordering::Relaxed) };
        debug_assert!(old & REFCOUNT_MASK > 0, "Arc::clone on a zero refcount");
        Self {
            ptr: self.ptr,
            heap: self.heap.clone(),
        }
    }
}

impl<T> Drop for HeapArc<T> {
    fn drop(&mut self) {
        let inner = unsafe { &*self.ptr };
        let prev = inner.refcount.fetch_sub(1, Ordering::Release);
        // The actual refcount is in the lower 63 bits. Check if it was 1
        // (i.e., this was the last reference), regardless of the slab flag.
        if prev & REFCOUNT_MASK == 1 {
            // Last reference — acquire fence to synchronize with other drops.
            std::sync::atomic::fence(Ordering::Acquire);
            let is_slab = prev & SLAB_FLAG != 0;
            unsafe {
                std::ptr::drop_in_place(&mut (*self.ptr).value);
                if !is_slab {
                    self.heap.dealloc(self.ptr as *mut u8);
                }
                // Slab-allocated: value is dropped but memory stays in the
                // slab page, freed when the Heap/HeapMaster is destroyed.
            }
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for HeapArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for HeapArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: PartialEq> PartialEq for HeapArc<T> {
    fn eq(&self, other: &Self) -> bool {
        // Pointer equality first (cheap), then value equality.
        std::ptr::eq(self.ptr, other.ptr) || (**self).eq(&**other)
    }
}

impl<T: Eq> Eq for HeapArc<T> {}

impl<T: std::hash::Hash> std::hash::Hash for HeapArc<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl<T> AsRef<T> for HeapArc<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    #[test]
    fn arc_basic() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let a = HeapArc::new(42u64, &heap).unwrap();
        assert_eq!(*a, 42);
        assert_eq!(HeapArc::strong_count(&a), 1);
    }

    #[test]
    fn arc_clone_and_drop() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let a = HeapArc::new(String::from("hello"), &heap).unwrap();
        let b = a.clone();
        let c = b.clone();
        assert_eq!(HeapArc::strong_count(&a), 3);
        assert_eq!(&*a, "hello");

        drop(b);
        assert_eq!(HeapArc::strong_count(&a), 2);

        drop(c);
        assert_eq!(HeapArc::strong_count(&a), 1);
    }

    #[test]
    fn arc_try_unwrap() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let a = HeapArc::new(vec![1, 2, 3], &heap).unwrap();
        let v = HeapArc::try_unwrap(a).unwrap();
        assert_eq!(v, vec![1, 2, 3]);
    }

    #[test]
    fn arc_try_unwrap_fails_with_multiple_refs() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let a = HeapArc::new(10u32, &heap).unwrap();
        let _b = a.clone();
        let result = HeapArc::try_unwrap(a);
        assert!(result.is_err());
    }

    #[test]
    fn arc_send_sync() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let a = HeapArc::new(42u64, &heap).unwrap();
        let b = a.clone();
        let handle = std::thread::spawn(move || {
            assert_eq!(*b, 42);
        });
        handle.join().unwrap();
        assert_eq!(*a, 42);
    }

    #[test]
    fn arc_debug() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let a = HeapArc::new(123i32, &heap).unwrap();
        assert_eq!(format!("{a:?}"), "123");
    }

    #[test]
    fn arc_drop_calls_value_destructor() {
        use std::sync::atomic::AtomicBool;

        static DROPPED: AtomicBool = AtomicBool::new(false);

        struct Sentinel;
        impl Drop for Sentinel {
            fn drop(&mut self) {
                DROPPED.store(true, Ordering::Relaxed);
            }
        }

        DROPPED.store(false, Ordering::Relaxed);
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let a = HeapArc::new(Sentinel, &heap).unwrap();
        assert!(!DROPPED.load(Ordering::Relaxed));
        drop(a);
        assert!(DROPPED.load(Ordering::Relaxed));
    }
}
