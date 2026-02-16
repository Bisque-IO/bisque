//! Level types for signal trees - hierarchical signal management
//!
//! This module provides Level types that combine multiple Node structures
//! to create larger signal trees for higher-capacity signaling.

use std::sync::atomic::AtomicU64;

use super::node::{Node, Selector};

/// Type alias for signal indices
type SignalIndex = u64;

/// Leaf level with 1 node (capacity 64) - flat structure for small trees
#[cfg_attr(target_arch = "x86_64", repr(C, align(64)))]
#[cfg_attr(
    any(
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64"
    ),
    repr(C, align(128))
)]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
    )),
    repr(C, align(64))
)]
pub struct Level64_1 {
    nodes: [Node<64, 64>; 1],
}

impl Default for Level64_1 {
    fn default() -> Self {
        Self::new()
    }
}

impl Level64_1 {
    pub const NODE_CAPACITY: u64 = 64;
    pub const NODE_COUNT: usize = 1;
    pub const COUNTERS_PER_NODE: u64 = 64;
    pub const BITS_PER_COUNTER: u32 = 1;
    pub const COUNTER_CAPACITY: u64 = 1;

    pub const fn new() -> Self {
        Self {
            nodes: [const { Node::new() }; 1],
        }
    }

    #[inline]
    pub fn value(&self) -> &AtomicU64 {
        &self.nodes[0].value
    }

    #[inline]
    pub fn empty(&self) -> bool {
        self.nodes[0].empty()
    }

    #[inline]
    pub fn load(&self) -> u64 {
        self.nodes[0].load()
    }

    #[inline]
    pub fn load_relaxed(&self) -> u64 {
        self.nodes[0].load_relaxed()
    }

    #[inline]
    pub fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
        self.nodes[0].set(signal_index)
    }

    /// Clear a signal bit atomically
    #[inline]
    pub fn clear_bit(&self, signal_index: SignalIndex) -> bool {
        self.nodes[0].clear(signal_index)
        // let bit = 0x8000_0000_0000_0000u64 >> signal_index;
        // let prev = self.nodes[0].clear.fetch_and(!bit, std::sync::atomic::Ordering::AcqRel);
        // (prev & bit) != 0
    }

    #[inline]
    pub fn select<S: Selector>(
        &self,
        bias_flags: u64,
        node_index: usize,
        bias_hint: u64,
    ) -> (SignalIndex, bool, u64) {
        self.nodes[node_index].select::<S>(bias_flags, bias_hint)
    }
}

/// All other Level types are simple wrappers that delegate to the first node
macro_rules! impl_level_multi_node {
    ($name:ident, $capacity:expr) => {
        #[cfg_attr(target_arch = "x86_64", repr(C, align(64)))]
        #[cfg_attr(
            any(
                target_arch = "aarch64",
                target_arch = "arm64ec",
                target_arch = "powerpc64"
            ),
            repr(C, align(128))
        )]
        #[cfg_attr(
            not(any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "arm64ec",
                target_arch = "powerpc64",
            )),
            repr(C, align(64))
        )]
        pub struct $name {
            nodes: [Node<64, 64>; 1],
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub const NODE_CAPACITY: u64 = 64;
            pub const NODE_COUNT: usize = 1;
            pub const COUNTERS_PER_NODE: u64 = 64;
            pub const BITS_PER_COUNTER: u32 = 1;
            pub const COUNTER_CAPACITY: u64 = 1;

            pub const fn new() -> Self {
                Self {
                    nodes: [const { Node::new() }; 1],
                }
            }

            #[inline]
            pub fn empty(&self) -> bool {
                self.nodes[0].empty()
            }

            #[inline]
            pub fn set(&self, signal_index: SignalIndex) -> (bool, bool) {
                self.nodes[0].set(signal_index)
            }

            #[inline]
            pub fn select<S: Selector>(
                &self,
                bias_flags: u64,
                node_index: usize,
                bias_hint: u64,
            ) -> (SignalIndex, bool, u64) {
                self.nodes[node_index].select::<S>(bias_flags, bias_hint)
            }
        }
    };
}

// Simplify: use Level64_1 for all levels since scheduler uses Vec<SignalTree64>
impl_level_multi_node!(Level64_8, 64);
impl_level_multi_node!(Level64_32, 64);
impl_level_multi_node!(Level64_128, 64);
impl_level_multi_node!(Level64_512, 64);
impl_level_multi_node!(Level64_2048, 64);
impl_level_multi_node!(Level64_16384, 64);
impl_level_multi_node!(Level512_1, 512);
impl_level_multi_node!(Level512_4, 512);
impl_level_multi_node!(Level512_16, 512);
impl_level_multi_node!(Level512_64, 512);
impl_level_multi_node!(Level512_256, 512);
impl_level_multi_node!(Level512_2048, 512);
impl_level_multi_node!(Level2048_1, 2048);
impl_level_multi_node!(Level2048_4, 2048);
impl_level_multi_node!(Level2048_16, 2048);
impl_level_multi_node!(Level2048_64, 2048);
impl_level_multi_node!(Level2048_512, 2048);
impl_level_multi_node!(Level8192_1, 8192);
impl_level_multi_node!(Level8192_4, 8192);
impl_level_multi_node!(Level32768_1, 32768);
impl_level_multi_node!(Level131072_1, 131072);
impl_level_multi_node!(Level1048576_1, 1048576);
