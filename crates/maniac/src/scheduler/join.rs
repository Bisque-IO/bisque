//! JoinSlot - Zero-allocation slot for storing join results
//!
//! The slot is provided by the caller and stores the result inline.
//! This avoids heap allocation for the result storage.

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

/// Slot for storing a join result - allows zero-allocation joins with typed results
///
/// The slot is provided by the caller and stores the result inline.
/// This avoids heap allocation for the result storage.
pub struct JoinSlot<T> {
    result: UnsafeCell<Option<T>>,
    waker: UnsafeCell<Option<Waker>>,
    completed: AtomicBool,
}

impl<T> JoinSlot<T> {
    /// Create a new empty join slot
    pub const fn new() -> Self {
        Self {
            result: UnsafeCell::new(None),
            waker: UnsafeCell::new(None),
            completed: AtomicBool::new(false),
        }
    }

    /// Store the result and mark as completed
    ///
    /// # Safety
    /// Must only be called once, from the future that owns this slot
    #[inline]
    pub unsafe fn complete(&self, value: T) {
        unsafe {
            *self.result.get() = Some(value);
            self.completed.store(true, Ordering::Release);
            if let Some(waker) = (*self.waker.get()).take() {
                waker.wake();
            }
        }
    }

    /// Check if the slot has a result
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    /// Take the result if completed
    ///
    /// # Safety
    /// Must only be called after is_completed returns true
    #[inline]
    pub unsafe fn take_result(&self) -> Option<T> {
        unsafe { (*self.result.get()).take() }
    }

    /// Register a waker to be called when the result is ready
    #[inline]
    pub fn register_waker(&self, waker: &Waker) {
        // Check if already completed first
        if self.completed.load(Ordering::Acquire) {
            return;
        }

        // Store the waker
        unsafe {
            *self.waker.get() = Some(waker.clone());
        }

        // Re-check to handle race
        if self.completed.load(Ordering::Acquire) {
            unsafe {
                if let Some(w) = (*self.waker.get()).take() {
                    w.wake();
                }
            }
        }
    }
}

impl<T> Default for JoinSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: JoinSlot uses atomic for synchronization
unsafe impl<T: Send> Send for JoinSlot<T> {}
unsafe impl<T: Send> Sync for JoinSlot<T> {}

/// Inner storage for a typed task - contains both the future and result slot.
///
/// This is wrapped in Arc to allow the slot to outlive the future execution.
/// Single allocation contains both the future and the result storage.
#[repr(C)]
pub struct TypedTaskInner<F, T>
where
    F: Future<Output = T>,
{
    /// The result slot - stored inline
    pub(crate) slot: JoinSlot<T>,
    /// The inner future to poll
    future: UnsafeCell<Option<F>>,
}

impl<F, T> TypedTaskInner<F, T>
where
    F: Future<Output = T>,
{
    /// Create a new typed task inner with the given future
    #[inline]
    pub fn new(future: F) -> Self {
        Self {
            slot: JoinSlot::new(),
            future: UnsafeCell::new(Some(future)),
        }
    }
}

// Safety: TypedTaskInner is Send if both F and T are Send
unsafe impl<F, T> Send for TypedTaskInner<F, T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
}

// Safety: TypedTaskInner is Sync if F is Send and T is Send (slot uses atomic sync)
unsafe impl<F, T> Sync for TypedTaskInner<F, T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
}

/// A task wrapper that polls a future stored in an Arc<TypedTaskInner>.
///
/// This allows the result slot to outlive the task execution, since the
/// TypedFutureContract also holds a reference to the Arc.
pub struct TypedTask<F, T>
where
    F: Future<Output = T>,
{
    inner: Arc<TypedTaskInner<F, T>>,
}

impl<F, T> TypedTask<F, T>
where
    F: Future<Output = T>,
{
    /// Create a new typed task wrapping the given Arc
    #[inline]
    pub fn new(inner: Arc<TypedTaskInner<F, T>>) -> Self {
        Self { inner }
    }
}

impl<F, T> Future for TypedTask<F, T>
where
    F: Future<Output = T>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: We have exclusive access through the contract system.
        // The future is only polled by one thread at a time.
        let future_opt = unsafe { &mut *self.inner.future.get() };

        let future = match future_opt {
            Some(f) => f,
            None => return Poll::Ready(()), // Already completed
        };

        // Safety: The future is stored in the Arc which is pinned by being in the contract.
        // We maintain the pin invariant by never moving the future once created.
        let future = unsafe { Pin::new_unchecked(future) };

        match future.poll(cx) {
            Poll::Ready(result) => {
                // Take ownership of the future to drop it
                *future_opt = None;

                // Safety: We are the only ones completing this slot
                unsafe {
                    self.inner.slot.complete(result);
                }
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// Safety: TypedTask is Send if both F and T are Send
unsafe impl<F, T> Send for TypedTask<F, T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
}
