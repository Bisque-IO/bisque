//! # generator
//!
//! Rust generator library
//!

#![allow(missing_docs)]
#![allow(deprecated)]
#![allow(unsafe_op_in_unsafe_fn)]

mod detail;
mod gen_impl;
mod reg_context;
mod rt;
mod scope;
mod stack;
mod yield_;

pub use crate::generator::gen_impl::{DEFAULT_STACK_SIZE, Generator, Gn, LocalGenerator};
pub use crate::generator::rt::{Error, get_local_data, is_generator};
pub use crate::generator::scope::Scope;
pub use crate::generator::yield_::{
    co_get_yield, co_set_para, co_yield_with, done, get_yield, yield_, yield_from, yield_now,
    yield_with,
};
