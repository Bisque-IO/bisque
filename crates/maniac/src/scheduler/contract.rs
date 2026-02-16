//! FutureContract - A handle to an async task
//!
//! A future contract represents an async task that can be scheduled for polling.
//! Contracts can be rescheduled, released, and moved but not copied.

use super::ContractId;
use super::group::ContractGroupInner;
use super::join::{JoinSlot, TypedTaskInner};
use crate::ptr::StripedArc;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A handle to a future contract
///
/// Future contracts are movable but not clonable. The contract remains
/// scheduled until the future completes or the group is dropped.
pub struct FutureContract {
    owner: StripedArc<ContractGroupInner>,
    id: ContractId,
}

// Safety: FutureContract can be sent between threads
unsafe impl Send for FutureContract {}

impl FutureContract {
    /// Create a future contract with the given parameters
    pub(crate) fn with_params(owner: StripedArc<ContractGroupInner>, id: ContractId) -> Self {
        Self { owner, id }
    }

    /// Schedule this contract for polling
    #[inline]
    pub fn schedule(&self) {
        self.owner.schedule(self.id);
    }

    /// Get the contract ID
    #[allow(dead_code)]
    pub(crate) fn get_id(&self) -> ContractId {
        self.id
    }

    /// Create a join handle that can be awaited for this contract's completion.
    ///
    /// This is a zero-allocation operation - the waker is stored inline in the
    /// contract's internal storage.
    ///
    /// # Example
    /// ```ignore
    /// let contract = group.create_contract_scheduled(async { /* work */ }, true);
    /// let join_handle = contract.join();
    /// // ... later, await completion
    /// join_handle.await;
    /// ```
    pub fn join(&self) -> JoinHandle<'_> {
        JoinHandle { contract: self }
    }

    pub unsafe fn release(&self) {
        unsafe {
            self.owner.release_contract(self.id.index);
        }
    }
}

/// A zero-allocation join handle for awaiting contract completion (unit result).
///
/// The waker is stored inline in the contract's internal storage,
/// so no heap allocation is required.
pub struct JoinHandle<'a> {
    contract: &'a FutureContract,
}

impl Future for JoinHandle<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let index = self.contract.id.index;

        // Check if already completed
        if self.contract.owner.is_completed(index) {
            return Poll::Ready(());
        }

        // Register our waker and check again (handles race)
        if self.contract.owner.register_join_waker(index, cx.waker()) {
            return Poll::Ready(());
        }

        Poll::Pending
    }
}

/// A typed future contract that can be awaited to get a result of type `T`.
///
/// The result slot is stored in an Arc-wrapped TypedTaskInner, which is shared
/// between the executing task and this contract. This allows a single allocation
/// to contain both the future and the result storage.
///
/// # Example
/// ```ignore
/// let contract = group.create_typed_contract(async {
///     compute_something() // returns T
/// }, true);
///
/// // The contract is directly awaitable
/// let value: T = contract.await;
/// ```
pub struct TypedFutureContract<F, T>
where
    F: Future<Output = T>,
{
    /// The underlying untyped contract handle
    contract: FutureContract,
    /// Arc to the task inner containing both the future and result slot.
    /// The slot remains valid even after the task completes because we hold the Arc.
    inner: Arc<TypedTaskInner<F, T>>,
}

// Safety: TypedFutureContract can be sent between threads if F and T are Send
unsafe impl<F, T> Send for TypedFutureContract<F, T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
}

impl<F, T> TypedFutureContract<F, T>
where
    F: Future<Output = T>,
{
    /// Create a typed future contract with a shared task inner
    pub(crate) fn new(contract: FutureContract, inner: Arc<TypedTaskInner<F, T>>) -> Self {
        Self { contract, inner }
    }

    /// Schedule this contract for polling
    pub fn schedule(&self) {
        self.contract.schedule();
    }

    /// Check if the result is ready
    pub fn is_completed(&self) -> bool {
        self.inner.slot.is_completed()
    }

    /// Get access to the underlying untyped contract
    pub fn as_untyped(&self) -> &FutureContract {
        &self.contract
    }

    /// Get a reference to the slot
    fn slot(&self) -> &JoinSlot<T> {
        &self.inner.slot
    }
}

impl<F, T> Future for TypedFutureContract<F, T>
where
    F: Future<Output = T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slot = self.slot();

        if slot.is_completed() {
            // Safety: is_completed returned true, so result is available
            let result = unsafe { slot.take_result() };
            return Poll::Ready(result.expect("result should be present after completion"));
        }

        // Register waker and check again
        slot.register_waker(cx.waker());

        if slot.is_completed() {
            let result = unsafe { slot.take_result() };
            return Poll::Ready(result.expect("result should be present after completion"));
        }

        Poll::Pending
    }
}
