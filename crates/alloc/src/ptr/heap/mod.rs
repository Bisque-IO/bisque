//! Smart pointers: heap-backed [`Box`], [`Arc`], epoch-based [`EpochBox`]/[`EpochRef`],
//! and heap-backed thread-local refcounted [`HeapTlrc`]/[`HeapTlrcRef`].

mod arc;
mod boxed;

pub use arc::HeapArc;
pub use boxed::HeapBox;
