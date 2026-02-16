//! Smart pointer utilities

mod striped;

pub use striped::StripedArc;
pub(crate) use striped::{StripedArcInner, StripedRefCount, thread_stripe};
