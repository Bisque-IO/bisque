//! Heap-backed `Box` with fallible allocation.

use std::fmt;
use std::ops::{Deref, DerefMut};

use crate::{Heap, HeapMaster};
use allocator_api2::alloc::AllocError;

/// A heap-allocated value. Equivalent to `std::boxed::Box<T>` but allocated
/// from a [`Heap`] with fallible construction.
///
/// # Examples
///
/// ```rust
/// use bisque_alloc::Heap;
/// use bisque_alloc::collections::Box;
///
/// let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
/// let b = Box::new(42u64, &heap).unwrap();
/// assert_eq!(*b, 42);
/// ```
pub struct Box<T: ?Sized> {
    ptr: *mut T,
    heap: Heap,
}

unsafe impl<T: ?Sized + Send> Send for Box<T> {}
unsafe impl<T: ?Sized + Sync> Sync for Box<T> {}

impl<T> Box<T> {
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

impl<T: ?Sized> Box<T> {
    /// Returns a reference to the underlying heap.
    #[inline]
    pub fn heap(this: &Self) -> &Heap {
        &this.heap
    }
}

impl<T: ?Sized> Deref for Box<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.ptr }
    }
}

impl<T: ?Sized> DerefMut for Box<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.ptr }
    }
}

impl<T: ?Sized> Drop for Box<T> {
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

impl<T: ?Sized + fmt::Debug> fmt::Debug for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + PartialEq> PartialEq for Box<T> {
    fn eq(&self, other: &Self) -> bool {
        (**self).eq(&**other)
    }
}

impl<T: ?Sized + Eq> Eq for Box<T> {}

impl<T: ?Sized + PartialOrd> PartialOrd for Box<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (**self).partial_cmp(&**other)
    }
}

impl<T: ?Sized + Ord> Ord for Box<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (**self).cmp(&**other)
    }
}

impl<T: ?Sized + std::hash::Hash> std::hash::Hash for Box<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl<T> AsRef<T> for Box<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for Box<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn box_basic() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = Box::new(42u64, &heap).unwrap();
        assert_eq!(*b, 42);
    }

    #[test]
    fn box_drop() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = Box::new(String::from("hello"), &heap).unwrap();
        assert_eq!(&*b, "hello");
        drop(b); // should free both the String's data and the Box allocation
    }

    #[test]
    fn box_zst() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = Box::new((), &heap).unwrap();
        assert_eq!(*b, ());
    }

    #[test]
    fn box_debug() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let b = Box::new(123i32, &heap).unwrap();
        assert_eq!(format!("{b:?}"), "123");
    }

    #[test]
    fn box_deref_mut() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut b = Box::new(10u32, &heap).unwrap();
        *b += 5;
        assert_eq!(*b, 15);
    }
}
