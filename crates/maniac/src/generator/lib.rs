//! # generator
//!
//! Rust generator library
//!

#![allow(missing_docs)]
#![allow(deprecated)]

mod detail;
mod gen_impl;
mod reg_context;
mod rt;
mod scope;
mod stack;
mod yield_;

pub use crate::generator::gen_impl::{Generator, Gn, LocalGenerator, DEFAULT_STACK_SIZE};
pub use crate::generator::rt::{get_local_data, is_generator, Error};
pub use crate::generator::scope::Scope;
pub use crate::generator::yield_::{
	co_get_yield, co_set_para, co_yield_with, done, get_yield, yield_, yield_from, yield_with,
};
