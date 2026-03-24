//! Heap-backed `Box` with fallible allocation.

use std::fmt;
use std::ops::{Deref, DerefMut};

use crate::Heap;
use allocator_api2::alloc::AllocError;

/// A heap-allocated value. Equivalent to `std::boxed::Box<T>` but allocated
/// from a [`Heap`] with fallible construction.
///
/// # Examples
///
/// ```rust
/// use bisque_alloc::HeapMaster;
/// use bisque_alloc::Box;
///
/// let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
/// let b = Box::new(42u64, &heap).unwrap();
/// assert_eq!(*b, 42);
/// ```
pub struct HeapBox<T: ?Sized> {
    ptr: *mut T,
    heap: Heap,
}

unsafe impl<T: ?Sized + Send> Send for HeapBox<T> {}
unsafe impl<T: ?Sized + Sync> Sync for HeapBox<T> {}

impl<T> HeapBox<T> {
    /// Allocate and initialize a value on the heap.
    ///
    /// Returns `Err(AllocError)` if the arena is full.
    pub fn new(value: T, heap: &Heap) -> Result<Self, AllocError> {
        let layout = std::alloc::Layout::new::<T>();
        if layout.size() == 0 {
            // ZST — no allocation needed, use a dangling aligned pointer.
            let ptr = layout.align() as *mut T;
            // Forget the value since we store it "at" the dangling pointer.
            // On drop, we read it back via ptr::read (which is a no-op for ZSTs).
            std::mem::forget(value);
            return Ok(Self {
                ptr,
                heap: heap.clone(),
            });
        }
        let raw = heap.alloc(layout.size(), layout.align());
        if raw.is_null() {
            return Err(AllocError);
        }
        unsafe {
            std::ptr::write(raw as *mut T, value);
        }
        Ok(Self {
            ptr: raw as *mut T,
            heap: heap.clone(),
        })
    }
}

impl<T: ?Sized> HeapBox<T> {
    /// Returns a reference to the underlying heap.
    #[inline]
    pub fn heap(this: &Self) -> &Heap {
        &this.heap
    }

    /// Returns a raw pointer to the value, consuming the `HeapBox`.
    ///
    /// The caller is responsible for calling [`HeapBox::from_raw`] to
    /// reconstruct and drop the value later. Failing to do so leaks
    /// both the value and the Heap clone.
    #[inline]
    pub fn into_raw(this: Self) -> *mut T {
        let ptr = this.ptr;
        std::mem::forget(this);
        ptr
    }

    /// Returns the raw pointer AND the Heap, consuming the `HeapBox`.
    ///
    /// Use this when you need to store the Heap separately (e.g., in a
    /// HashMap alongside the pointer) to avoid cloning later.
    #[inline]
    pub fn into_raw_parts(this: Self) -> (*mut T, Heap) {
        let ptr = this.ptr;
        let heap = unsafe { std::ptr::read(&this.heap) };
        std::mem::forget(this);
        (ptr, heap)
    }

    /// Reconstructs a `HeapBox` from a raw pointer and a `Heap`.
    ///
    /// # Safety
    /// - `ptr` must have been obtained from [`HeapBox::into_raw`].
    /// - `heap` must be a clone of the same Heap that allocated `ptr`.
    /// - Must be called at most once per `into_raw` call.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut T, heap: Heap) -> Self {
        Self { ptr, heap }
    }
}

impl<T: ?Sized> Deref for HeapBox<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.ptr }
    }
}

impl<T: ?Sized> DerefMut for HeapBox<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.ptr }
    }
}

impl<T: ?Sized> Drop for HeapBox<T> {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::for_value::<T>(unsafe { &*self.ptr });
        unsafe {
            std::ptr::drop_in_place(self.ptr);
        }
        if layout.size() > 0 {
            unsafe { self.heap.dealloc(self.ptr as *mut u8) }
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for HeapBox<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for HeapBox<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + PartialEq> PartialEq for HeapBox<T> {
    fn eq(&self, other: &Self) -> bool {
        (**self).eq(&**other)
    }
}

impl<T: ?Sized + Eq> Eq for HeapBox<T> {}

impl<T: ?Sized + PartialOrd> PartialOrd for HeapBox<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (**self).partial_cmp(&**other)
    }
}

impl<T: ?Sized + Ord> Ord for HeapBox<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (**self).cmp(&**other)
    }
}

impl<T: ?Sized + std::hash::Hash> std::hash::Hash for HeapBox<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl<T> AsRef<T> for HeapBox<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for HeapBox<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    #[test]
    fn box_basic() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = HeapBox::new(42u64, &heap).unwrap();
        assert_eq!(*b, 42);
    }

    #[test]
    fn box_drop() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = HeapBox::new(String::from("hello"), &heap).unwrap();
        assert_eq!(&*b, "hello");
        drop(b); // should free both the String's data and the Box allocation
    }

    #[test]
    fn box_zst() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = HeapBox::new((), &heap).unwrap();
        assert_eq!(*b, ());
    }

    #[test]
    fn box_debug() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = HeapBox::new(123i32, &heap).unwrap();
        assert_eq!(format!("{b:?}"), "123");
    }

    #[test]
    fn box_deref_mut() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut b = HeapBox::new(10u32, &heap).unwrap();
        *b += 5;
        assert_eq!(*b, 15);
    }
}
