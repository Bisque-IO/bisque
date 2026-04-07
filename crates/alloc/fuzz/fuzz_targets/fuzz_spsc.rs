//! Fuzz target for bounded Spsc and unbounded UnboundedSpsc.
//!
//! Exercises: push, pop, push_n, pop_n, consume_in_place, close, node advancement,
//! node freeing, cross-thread producer/consumer migration, and drop ordering.
//!
//! The SPSC contract guarantees at most one producer and one consumer at a time,
//! but they may migrate between threads. This fuzzer validates that property by
//! occasionally moving the sender/receiver to a new thread.
//!
//! Run: cd crates/alloc/fuzz && cargo +nightly fuzz run fuzz_spsc

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bisque_alloc::collections::spsc::{
    NoOpSignal, Sender, Receiver, Spsc,
    UnboundedSender, UnboundedReceiver, UnboundedSpsc,
};
use libfuzzer_sys::fuzz_target;
use std::collections::VecDeque;

/// Operations the fuzzer can perform.
#[derive(Debug, Arbitrary)]
enum Op {
    /// Push a single value.
    Push { val: u8 },
    /// Pop a single value.
    Pop,
    /// Push N values (batch).
    PushN { count: u8 },
    /// Pop N values (batch).
    PopN { count: u8 },
    /// Consume in-place up to N items.
    ConsumeInPlace { count: u8 },
    /// Close the channel from the sender side.
    Close,
    /// Move the sender to a new thread and back (tests cross-thread migration).
    MigrateSender,
    /// Move the receiver to a new thread and back (tests cross-thread migration).
    MigrateReceiver,
}

// ─────────────────────────────────────────────────────────────────────────────
// Bounded Spsc fuzzer
// ─────────────────────────────────────────────────────────────────────────────

/// Small bounded queue: P=2 (4 items/seg), NUM_SEGS_P2=2 (4 segs) → capacity=15.
/// Small size maximizes segment boundary crossings and full/empty transitions.
type BoundedQ = Spsc<u64, 2, 2, NoOpSignal>;

fn fuzz_bounded(ops: &[Op]) {
    let (tx, mut rx): (Sender<u64, 2, 2, NoOpSignal>, Receiver<u64, 2, 2, NoOpSignal>) =
        BoundedQ::new_with_gate(NoOpSignal);
    let mut tx = Some(tx);

    // Oracle: track expected contents.
    let mut oracle: VecDeque<u64> = VecDeque::new();
    let mut seq = 0u64;

    for op in ops {
        match op {
            Op::Push { val } => {
                let Some(ref sender) = tx else { continue };
                let v = seq * 256 + *val as u64;
                match sender.try_push(v) {
                    Ok(()) => {
                        oracle.push_back(v);
                        seq += 1;
                    }
                    Err(_) => {}
                }
            }
            Op::Pop => {
                let got = rx.try_pop();
                let expected = oracle.pop_front();
                assert_eq!(got, expected, "pop mismatch: got {:?}, expected {:?}", got, expected);
            }
            Op::PushN { count } => {
                let Some(ref sender) = tx else { continue };
                let n = (*count as usize).min(32);
                let mut items: Vec<u64> = (0..n as u64).map(|i| seq + i).collect();
                match sender.try_push_n(&mut items) {
                    Ok(pushed) => {
                        for i in 0..pushed {
                            oracle.push_back(seq + i as u64);
                        }
                        seq += pushed as u64;
                    }
                    Err(_) => {}
                }
            }
            Op::PopN { count } => {
                let n = (*count as usize).min(32);
                let mut buf = vec![0u64; n];
                match rx.try_pop_n(&mut buf) {
                    Ok(popped) => {
                        for i in 0..popped {
                            let expected = oracle.pop_front().unwrap();
                            assert_eq!(buf[i], expected,
                                "pop_n mismatch at index {}: got {}, expected {}", i, buf[i], expected);
                        }
                    }
                    Err(_) => {}
                }
            }
            Op::ConsumeInPlace { count } => {
                let n = (*count as usize).min(32);
                let mut consumed_items = Vec::new();
                rx.consume_in_place(n, |slice| {
                    consumed_items.extend_from_slice(slice);
                    slice.len()
                });
                for item in &consumed_items {
                    let expected = oracle.pop_front().unwrap();
                    assert_eq!(*item, expected,
                        "consume_in_place mismatch: got {}, expected {}", item, expected);
                }
            }
            Op::Close => {
                tx.take(); // Drop sender → closes channel
            }
            Op::MigrateSender => {
                // Move sender to another thread and back to exercise Send.
                let Some(sender) = tx.take() else { continue };
                let returned = std::thread::spawn(move || sender).join().unwrap();
                tx = Some(returned);
            }
            Op::MigrateReceiver => {
                // Move receiver to another thread and back to exercise Send.
                rx = std::thread::spawn(move || rx).join().unwrap();
            }
        }
    }

    // Drain remaining and verify.
    drop(tx);
    while let Some(got) = rx.try_pop() {
        let expected = oracle.pop_front().unwrap();
        assert_eq!(got, expected, "final drain mismatch");
    }
    assert!(oracle.is_empty(), "oracle has {} items remaining", oracle.len());
}

// ─────────────────────────────────────────────────────────────────────────────
// Unbounded Spsc fuzzer
// ─────────────────────────────────────────────────────────────────────────────

/// Small unbounded queue: P=1 (2 items/seg), NUM_SEGS_P2=1 (2 segs) → 3 items/node.
/// Tiny nodes maximize node creation, advancement, and freeing.
type UnboundedQ = UnboundedSpsc<u64, 1, 1>;

fn fuzz_unbounded(ops: &[Op]) {
    let (tx, mut rx): (UnboundedSender<u64, 1, 1>, UnboundedReceiver<u64, 1, 1>) =
        UnboundedQ::new();
    let mut tx = Some(tx);

    let mut oracle: VecDeque<u64> = VecDeque::new();
    let mut seq = 0u64;

    for op in ops {
        match op {
            Op::Push { val } => {
                let Some(ref sender) = tx else { continue };
                let v = seq * 256 + *val as u64;
                match sender.try_push(v) {
                    Ok(()) => {
                        oracle.push_back(v);
                        seq += 1;
                    }
                    Err(_) => {}
                }
            }
            Op::Pop => {
                let got = rx.try_pop().ok();
                let expected = oracle.pop_front();
                assert_eq!(got, expected, "unbounded pop mismatch: got {:?}, expected {:?}", got, expected);
            }
            Op::PushN { count } => {
                let Some(ref sender) = tx else { continue };
                let n = (*count as usize).min(32);
                let mut items: Vec<u64> = (0..n as u64).map(|i| seq + i).collect();
                match sender.try_push_n(&mut items) {
                    Ok(pushed) => {
                        for i in 0..pushed {
                            oracle.push_back(seq + i as u64);
                        }
                        seq += pushed as u64;
                    }
                    Err(_) => {}
                }
            }
            Op::PopN { count } => {
                let n = (*count as usize).min(32);
                let mut buf = vec![0u64; n];
                match rx.try_pop_n(&mut buf) {
                    Ok(popped) => {
                        for i in 0..popped {
                            let expected = oracle.pop_front().unwrap();
                            assert_eq!(buf[i], expected,
                                "unbounded pop_n mismatch at {}: got {}, expected {}", i, buf[i], expected);
                        }
                    }
                    Err(_) => {
                        // Empty or Closed — both fine. The queue may have items
                        // the consumer hasn't seen yet (head cache staleness).
                    }
                }
            }
            Op::ConsumeInPlace { count } => {
                let n = (*count as usize).min(32);
                let mut consumed_items = Vec::new();
                rx.consume_in_place(n, |slice| {
                    consumed_items.extend_from_slice(slice);
                    slice.len()
                });
                for item in &consumed_items {
                    let expected = oracle.pop_front().unwrap();
                    assert_eq!(*item, expected,
                        "unbounded consume_in_place mismatch: got {}, expected {}", item, expected);
                }
            }
            Op::Close => {
                tx.take(); // Drop sender → closes channel
            }
            Op::MigrateSender => {
                let Some(sender) = tx.take() else { continue };
                let returned = std::thread::spawn(move || sender).join().unwrap();
                tx = Some(returned);
            }
            Op::MigrateReceiver => {
                rx = std::thread::spawn(move || rx).join().unwrap();
            }
        }
    }

    drop(tx);
    while let Ok(got) = rx.try_pop() {
        let expected = oracle.pop_front().unwrap();
        assert_eq!(got, expected, "unbounded final drain mismatch");
    }
    assert!(oracle.is_empty(), "unbounded oracle has {} items remaining", oracle.len());
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent stress fuzzer — producer and consumer on separate threads
// ─────────────────────────────────────────────────────────────────────────────

fn fuzz_concurrent_bounded(data: &[u8]) {
    if data.len() < 4 {
        return;
    }
    let item_count = u16::from_le_bytes([data[0], data[1]]) as usize;
    let item_count = item_count.min(64);
    if item_count == 0 {
        return;
    }

    let (tx, rx): (Sender<u64, 2, 2, NoOpSignal>, Receiver<u64, 2, 2, NoOpSignal>) =
        BoundedQ::new_with_gate(NoOpSignal);

    let producer = std::thread::spawn(move || {
        for i in 0..item_count as u64 {
            loop {
                match tx.try_push(i) {
                    Ok(()) => break,
                    Err(_) => std::hint::spin_loop(),
                }
            }
        }
        // tx dropped → closes channel
    });

    let consumer = std::thread::spawn(move || {
        let mut received = Vec::with_capacity(item_count);
        // Consume exactly item_count items. The producer guarantees it will
        // push all of them, so spinning until we get them all is safe.
        while received.len() < item_count {
            if let Some(v) = rx.try_pop() {
                received.push(v);
            } else {
                std::hint::spin_loop();
            }
        }
        received
    });

    producer.join().unwrap();
    let received = consumer.join().unwrap();

    assert_eq!(received.len(), item_count);
    for (i, &v) in received.iter().enumerate() {
        assert_eq!(v, i as u64, "bounded concurrent FIFO violation at index {}", i);
    }
}

/// Small unbounded concurrent test. Capped at 64 items to keep node count
/// manageable (each node holds 3 items → ~22 nodes max).
fn fuzz_concurrent_unbounded(data: &[u8]) {
    if data.len() < 4 {
        return;
    }
    let item_count = u16::from_le_bytes([data[0], data[1]]) as usize;
    let item_count = item_count.min(64);
    if item_count == 0 {
        return;
    }

    let (tx, rx): (UnboundedSender<u64, 1, 1>, UnboundedReceiver<u64, 1, 1>) = UnboundedQ::new();

    let producer = std::thread::spawn(move || {
        for i in 0..item_count as u64 {
            loop {
                match tx.try_push(i) {
                    Ok(()) => break,
                    Err(_) => std::hint::spin_loop(),
                }
            }
        }
        // tx dropped → closes channel
    });

    let consumer = std::thread::spawn(move || {
        let mut received = Vec::with_capacity(item_count);
        while received.len() < item_count {
            if let Ok(v) = rx.try_pop() {
                received.push(v);
            } else {
                std::hint::spin_loop();
            }
        }
        received
    });

    producer.join().unwrap();
    let received = consumer.join().unwrap();

    assert_eq!(received.len(), item_count);
    for (i, &v) in received.iter().enumerate() {
        assert_eq!(v, i as u64, "unbounded concurrent FIFO violation at index {}", i);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // First byte selects the fuzzer variant.
    let variant = data[0] % 4;
    let rest = &data[1..];

    match variant {
        0 => {
            // Bounded sequential
            let mut u = Unstructured::new(rest);
            let op_count: usize = match u.int_in_range(1..=128) {
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
            if !ops.is_empty() {
                fuzz_bounded(&ops);
            }
        }
        1 => {
            // Unbounded sequential
            let mut u = Unstructured::new(rest);
            let op_count: usize = match u.int_in_range(1..=128) {
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
            if !ops.is_empty() {
                fuzz_unbounded(&ops);
            }
        }
        2 => {
            // Bounded concurrent
            fuzz_concurrent_bounded(rest);
        }
        3 => {
            // Unbounded concurrent
            fuzz_concurrent_unbounded(rest);
        }
        _ => unreachable!(),
    }
});
