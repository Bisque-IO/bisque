//! Heap-backed `Arc` (atomically reference-counted pointer) with fallible allocation.

use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Heap;
use allocator_api2::alloc::AllocError;

/// Layout: [refcount: AtomicUsize] [value: T]
/// Stored in a single heap allocation.
#[repr(C)]
struct HeapArcInner<T> {
    refcount: AtomicUsize,
    value: T,
}

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

    /// Returns the number of strong references to this value.
    #[inline]
    pub fn strong_count(this: &Self) -> usize {
        unsafe { (*this.ptr).refcount.load(Ordering::Acquire) }
    }

    /// Returns a reference to the underlying heap.
    #[inline]
    pub fn heap(this: &Self) -> &Heap {
        &this.heap
    }

    /// Try to unwrap the `Arc`, returning the inner value if this is the
    /// only reference. Returns `Err(self)` if there are other references.
    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        let inner = unsafe { &*this.ptr };
        if inner
            .refcount
            .compare_exchange(1, 0, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // We are the sole owner. Read out the value and free.
            let value = unsafe { std::ptr::read(&inner.value) };
            let raw = this.ptr as *mut u8;
            let heap = this.heap.clone();
            std::mem::forget(this); // prevent Drop from running
            unsafe { heap.dealloc(raw) };
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
        debug_assert!(old > 0, "Arc::clone on a zero refcount");
        Self {
            ptr: self.ptr,
            heap: self.heap.clone(),
        }
    }
}

impl<T> Drop for HeapArc<T> {
    fn drop(&mut self) {
        let inner = unsafe { &*self.ptr };
        if inner.refcount.fetch_sub(1, Ordering::Release) == 1 {
            // Last reference — acquire fence to synchronize with other drops.
            std::sync::atomic::fence(Ordering::Acquire);
            unsafe {
                std::ptr::drop_in_place(&mut (*self.ptr).value);
                self.heap.dealloc(self.ptr as *mut u8);
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
