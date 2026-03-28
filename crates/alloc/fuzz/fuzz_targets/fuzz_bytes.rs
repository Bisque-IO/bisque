//! Fuzz target for bisque-alloc Bytes and BytesMut.
//!
//! Exercises: alloc, extend, split_to, split_off, freeze, try_mut, clone,
//! drop ordering, grow_shared, and refcount lifecycle.
//!
//! Run: cd crates/alloc/fuzz && cargo +nightly fuzz run fuzz_bytes

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bisque_alloc::HeapMaster;
use bisque_alloc::bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;

/// Operations the fuzzer can perform on a BytesMut.
#[derive(Debug, Arbitrary)]
enum Op {
    /// Extend with `len` bytes of value `val`.
    Extend { len: u8, val: u8 },
    /// Split off `[0..pos)` as Bytes and stash it.
    SplitTo { pos: u8 },
    /// Split off `[pos..len)` as Bytes and stash it.
    SplitOff { pos: u8 },
    /// Freeze the BytesMut into Bytes, then try_mut back.
    FreezeAndTryMut,
    /// Freeze the BytesMut, stash the Bytes, start fresh.
    FreezeAndStash,
    /// Clone a random stashed Bytes.
    CloneStashed { idx: u8 },
    /// Drop a random stashed Bytes.
    DropStashed { idx: u8 },
    /// Clear the BytesMut.
    Clear,
    /// Push a single byte.
    Push { val: u8 },
    /// Truncate to `len`.
    Truncate { len: u8 },
}

const MAX_STASHED: usize = 64;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);

    // Parse a bounded sequence of operations to avoid OOM.
    let op_count: usize = match u.int_in_range(1..=256) {
        Ok(n) => n,
        Err(_) => return,
    };
    let mut ops = Vec::with_capacity(op_count);
    for _ in 0..op_count {
        match u.arbitrary() {
            Ok(op) => ops.push(op),
            Err(_) => break,
        }
    }
    if ops.is_empty() {
        return;
    }

    let master = match HeapMaster::new(16 * 1024 * 1024) {
        Ok(m) => m,
        Err(_) => return,
    };

    let mut bm = BytesMut::with_capacity(256, &master).unwrap();
    let mut stashed: Vec<Bytes> = Vec::new();

    for op in &ops {
        match op {
            Op::Extend { len, val } => {
                let n = (*len as usize).min(512);
                let data = vec![*val; n];
                let _ = bm.extend_from_slice(&data);
            }
            Op::SplitTo { pos } => {
                let pos = (*pos as usize).min(bm.len());
                if pos > 0 {
                    let frame = bm.split_to(pos);
                    if stashed.len() < MAX_STASHED {
                        stashed.push(frame);
                    }
                }
            }
            Op::SplitOff { pos } => {
                let pos = (*pos as usize).min(bm.len());
                let tail = bm.split_off(pos);
                if !tail.is_empty() && stashed.len() < MAX_STASHED {
                    stashed.push(tail);
                }
            }
            Op::FreezeAndTryMut => {
                let len = bm.len();
                let frozen = bm.freeze();
                assert_eq!(frozen.len(), len);
                match frozen.try_mut() {
                    Ok(recovered) => {
                        bm = recovered;
                    }
                    Err(frozen) => {
                        // Shared — can't get it back. Stash and start fresh.
                        if stashed.len() < MAX_STASHED {
                            stashed.push(frozen);
                        }
                        bm = BytesMut::with_capacity(256, &master).unwrap();
                    }
                }
            }
            Op::FreezeAndStash => {
                let frozen = bm.freeze();
                if stashed.len() < MAX_STASHED {
                    stashed.push(frozen);
                }
                bm = BytesMut::with_capacity(256, &master).unwrap();
            }
            Op::CloneStashed { idx } => {
                if !stashed.is_empty() && stashed.len() < MAX_STASHED {
                    let i = *idx as usize % stashed.len();
                    let cloned = stashed[i].clone();
                    stashed.push(cloned);
                }
            }
            Op::DropStashed { idx } => {
                if !stashed.is_empty() {
                    let i = *idx as usize % stashed.len();
                    stashed.swap_remove(i);
                }
            }
            Op::Clear => {
                bm.clear();
            }
            Op::Push { val } => {
                let _ = bm.push(*val);
            }
            Op::Truncate { len } => {
                bm.truncate(*len as usize);
            }
        }

        // Invariant checks after each operation.
        let bm_data = bm.as_slice();
        assert_eq!(bm_data.len(), bm.len());
        assert!(bm.len() <= bm.capacity());

        for s in &stashed {
            // Every stashed Bytes must be readable without UB.
            let _ = s.len();
            let _ = s.as_slice();
        }
    }

    // Drop in reverse order to exercise different refcount paths.
    while let Some(b) = stashed.pop() {
        drop(b);
    }
    drop(bm);
});
