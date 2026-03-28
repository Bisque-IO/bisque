//! Epoch-based shared pointers and concurrent memory reclamation.
//!
//! This module vendors the [`seize`] crate as submodules and provides
//! [`EpochBox`] / [`EpochRef`] helpers on top.
//!
//! [`EpochBox`] owns an allocation and retires it on drop. [`EpochRef`] is a
//! zero-cost, `Copy` view — cloning and dropping are no-ops. Safe reads
//! require an epoch guard from the [`collector()`].
//!
//! # Architecture
//!
//! ```text
//! EpochBox::new(data)            ← global allocator, infallible
//! EpochBox::new_in(data, &heap)  ← heap-backed, fallible
//!   ├── .epoch_ref() → EpochRef  ← pointer copy, Copy, no-op drop
//!   ├── .epoch_ref() → EpochRef
//!   └── drop (EpochBox)          ← collector.retire() defers deallocation
//!         └── reclaimed when all epoch guards have advanced
//! ```
//!
//! # Performance characteristics
//!
//! | Operation             | Cost                          |
//! |-----------------------|-------------------------------|
//! | `EpochBox::new()`     | One allocation                |
//! | `EpochBox::new_in()`  | One heap allocation           |
//! | `.epoch_ref()`        | Pointer copy (zero atomics)   |
//! | `ref.clone()`         | Pointer copy (zero atomics)   |
//! | `.load(&guard)`       | Pointer deref                 |
//! | `collector().enter()` | Thread-local epoch enter      |
//! | Drop (`EpochRef`)     | No-op                         |
//! | Drop (`EpochBox`)     | One `retire()` call (batched) |
//!
//! # Safety invariant
//!
//! The `EpochBox` must not be dropped while any `EpochRef` is accessing data
//! outside of an epoch guard. In practice, hold a guard across reads, and
//! ensure the box outlives all refs (or that refs only access through guards
//! obtained before the box was dropped).

// --- Vendored seize submodules ---
pub(crate) mod raw;
pub mod collector;
pub mod guard;
pub mod reclaim;

// --- Public re-exports (seize API surface) ---
pub use collector::Collector;
pub use guard::{Guard, LocalGuard, OwnedGuard};

use std::marker::PhantomData;

use crate::Heap;
use allocator_api2::alloc::AllocError;

/// Global epoch-based collector shared by all epoch pointers.
pub fn collector() -> &'static Collector {
    static COLLECTOR: std::sync::LazyLock<Collector> =
        std::sync::LazyLock::new(Collector::new);
    &COLLECTOR
}

/// Inner allocation layout: optional heap handle + data.
#[repr(C)]
struct EpochInner<T> {
    heap: Option<Heap>,
    data: T,
}

/// An owning epoch-protected pointer. Retires the allocation on drop.
///
/// Like [`Box`](std::boxed::Box), `EpochBox` owns the allocation. Unlike
/// `Box`, deallocation is deferred through the epoch collector until all
/// threads have advanced past the retirement epoch.
///
/// Supports both global allocation ([`new`](EpochBox::new)) and heap-backed
/// allocation ([`new_in`](EpochBox::new_in)).
///
/// Produce lightweight [`EpochRef`]s via [`epoch_ref()`](EpochBox::epoch_ref)
/// for sharing across threads.
///
/// # Example
///
/// ```ignore
/// let owner = EpochBox::new(42);
/// let r = owner.epoch_ref();
/// let guard = bisque_alloc::boxed::epoch::collector().enter();
/// assert_eq!(*r.load(&guard), 42);
/// ```
pub struct EpochBox<T> {
    ptr: *mut EpochInner<T>,
    _marker: PhantomData<T>,
}

/// A lightweight, non-owning view of epoch-protected data.
///
/// `Copy` and `Clone` are zero-cost pointer copies. There is no `Drop`
/// impl — dropping is a no-op. Access requires an epoch guard via
/// [`load()`](EpochRef::load).
pub struct EpochRef<T> {
    ptr: *mut EpochInner<T>,
    _marker: PhantomData<T>,
}

impl<T> Clone for EpochRef<T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for EpochRef<T> {}

impl<T> EpochBox<T> {
    /// Allocate with the global allocator (infallible).
    pub fn new(data: T) -> Self {
        let inner = Box::new(EpochInner { heap: None, data });
        Self {
            ptr: Box::into_raw(inner),
            _marker: PhantomData,
        }
    }

    /// Allocate from a [`Heap`] (fallible).
    ///
    /// Returns `Err(AllocError)` if the heap is full.
    pub fn new_in(data: T, heap: &Heap) -> Result<Self, AllocError> {
        let layout = std::alloc::Layout::new::<EpochInner<T>>();
        let raw = heap.alloc(layout.size(), layout.align());
        if raw.is_null() {
            return Err(AllocError);
        }
        unsafe {
            std::ptr::write(
                raw as *mut EpochInner<T>,
                EpochInner {
                    heap: Some(heap.clone()),
                    data,
                },
            );
        }
        Ok(Self {
            ptr: raw as *mut EpochInner<T>,
            _marker: PhantomData,
        })
    }

    /// Create a lightweight ref for sharing across threads.
    /// Zero-cost: just copies the pointer.
    #[inline]
    pub fn epoch_ref(&self) -> EpochRef<T> {
        EpochRef {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }

    /// Load a reference to the data, protected by an epoch guard.
    ///
    /// The returned reference is valid for the lifetime of the guard.
    #[inline]
    pub fn load<'g>(&self, _guard: &'g LocalGuard<'_>) -> &'g T {
        unsafe { &(*self.ptr).data }
    }

    /// Get a raw pointer to the data.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        unsafe { &(*self.ptr).data as *const T }
    }
}

impl<T> EpochRef<T> {
    /// Load a reference to the data, protected by an epoch guard.
    ///
    /// The returned reference is valid for the lifetime of the guard.
    /// While the guard is held, the collector will not reclaim this
    /// pointer even if the owning `EpochBox` has been dropped.
    #[inline]
    pub fn load<'g>(&self, _guard: &'g LocalGuard<'_>) -> &'g T {
        unsafe { &(*self.ptr).data }
    }

    /// Get a raw pointer to the data.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        unsafe { &(*self.ptr).data as *const T }
    }
}

impl<T> Drop for EpochBox<T> {
    #[inline]
    fn drop(&mut self) {
        // Retire through the epoch collector. The actual deallocation
        // happens when all threads have advanced past this epoch.
        unsafe {
            collector().retire(self.ptr, reclaim_inner::<T>);
        }
    }
}

/// Reclaim function for `Collector::retire`.
///
/// Handles both global-alloc and heap-backed allocations by checking
/// the inner `heap` field.
///
/// # Safety
/// `ptr` must be a valid pointer to an `EpochInner<T>` allocated by
/// either `EpochBox::new` or `EpochBox::new_in`.
unsafe fn reclaim_inner<T>(ptr: *mut EpochInner<T>, _: &Collector) {
    unsafe {
        // Move everything out before deallocating the memory.
        let EpochInner { heap, data } = std::ptr::read(ptr);
        drop(data);
        match heap {
            Some(h) => h.dealloc(ptr as *mut u8),
            None => {
                std::alloc::dealloc(ptr as *mut u8, std::alloc::Layout::new::<EpochInner<T>>());
            }
        }
    }
}

unsafe impl<T: Send + Sync> Send for EpochBox<T> {}
unsafe impl<T: Send + Sync> Sync for EpochBox<T> {}
unsafe impl<T: Send + Sync> Send for EpochRef<T> {}
unsafe impl<T: Send + Sync> Sync for EpochRef<T> {}

impl<T: std::fmt::Debug> std::fmt::Debug for EpochBox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochBox").field("ptr", &self.ptr).finish()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for EpochRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochRef").field("ptr", &self.ptr).finish()
    }
}
