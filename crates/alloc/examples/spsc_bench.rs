//! Benchmark: bounded Spsc vs unbounded UnboundedSpsc.
//!
//! Run with: cargo run --release -p bisque-alloc --example spsc-bench

use std::hint::black_box;
use std::thread;
use std::time::{Duration, Instant};

use bisque_alloc::collections::spsc::{
    NoOpSignal, Receiver, Sender, Spsc, UnboundedReceiver, UnboundedSender, UnboundedSpsc,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct BenchResult {
    name: String,
    ops: u64,
    elapsed: Duration,
}

impl BenchResult {
    fn print(&self) {
        let ns = self.elapsed.as_nanos() as f64 / self.ops as f64;
        let mops = self.ops as f64 / self.elapsed.as_secs_f64() / 1_000_000.0;
        println!("  {:<52} {:>10.2} Mops/s  {:>8.1} ns/op", self.name, mops, ns);
    }
}

// ---------------------------------------------------------------------------
// Bounded Spsc benchmarks
// ---------------------------------------------------------------------------

/// Single-threaded ping-pong: push 1, pop 1.
fn bench_bounded_pingpong(duration: Duration) -> BenchResult {
    type Q = Spsc<u64, 6, 8, NoOpSignal>;
    let (tx, rx): (Sender<u64, 6, 8, NoOpSignal>, Receiver<u64, 6, 8, NoOpSignal>) =
        Q::new_with_gate(NoOpSignal);

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        for _ in 0..1000 {
            tx.try_push(black_box(42u64)).unwrap();
            black_box(rx.try_pop().unwrap());
        }
        ops += 1000;
    }
    BenchResult {
        name: "bounded: push-pop pingpong".into(),
        ops,
        elapsed: start.elapsed(),
    }
}

/// Single-threaded burst: push N, then pop N.
fn bench_bounded_burst(duration: Duration, burst: usize) -> BenchResult {
    type Q = Spsc<u64, 6, 8, NoOpSignal>;
    let (tx, rx): (Sender<u64, 6, 8, NoOpSignal>, Receiver<u64, 6, 8, NoOpSignal>) =
        Q::new_with_gate(NoOpSignal);

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        for i in 0..burst {
            tx.try_push(black_box(i as u64)).unwrap();
        }
        for _ in 0..burst {
            black_box(rx.try_pop().unwrap());
        }
        ops += burst as u64 * 2;
    }
    BenchResult {
        name: format!("bounded: burst push+pop ({})", burst),
        ops,
        elapsed: start.elapsed(),
    }
}

/// Two-thread throughput: producer and consumer work concurrently, one item at a time.
fn bench_bounded_concurrent_1x1(duration: Duration) -> BenchResult {
    type Q = Spsc<u64, 10, 6, NoOpSignal>;
    let (tx, rx): (Sender<u64, 10, 6, NoOpSignal>, Receiver<u64, 10, 6, NoOpSignal>) =
        Q::new_with_gate(NoOpSignal);

    let dur = duration;
    let consumer = thread::spawn(move || {
        let mut count = 0u64;
        let start = Instant::now();
        while start.elapsed() < dur {
            if rx.try_pop().is_some() {
                count += 1;
            }
        }
        while rx.try_pop().is_some() {
            count += 1;
        }
        count
    });

    let start = Instant::now();
    let mut produced = 0u64;
    while start.elapsed() < duration {
        if tx.try_push(black_box(produced)).is_ok() {
            produced += 1;
        }
    }

    let consumed = consumer.join().unwrap();
    BenchResult {
        name: "bounded: 2-thread 1×1".into(),
        ops: consumed,
        elapsed: start.elapsed(),
    }
}

/// Two-thread throughput: producer bursts N items via push_n, consumer drains via pop_n.
fn bench_bounded_concurrent_bulk(duration: Duration, batch: usize) -> BenchResult {
    type Q = Spsc<u64, 10, 6, NoOpSignal>;
    let (tx, rx): (Sender<u64, 10, 6, NoOpSignal>, Receiver<u64, 10, 6, NoOpSignal>) =
        Q::new_with_gate(NoOpSignal);

    let dur = duration;
    let b = batch;
    let consumer = thread::spawn(move || {
        let mut buf = vec![0u64; b];
        let mut count = 0u64;
        let start = Instant::now();
        while start.elapsed() < dur {
            match rx.try_pop_n(&mut buf) {
                Ok(n) => count += n as u64,
                Err(_) => {}
            }
        }
        while rx.try_pop_n(&mut buf).is_ok() {
            // drain
        }
        count
    });

    let start = Instant::now();
    let mut produced = 0u64;
    let mut src: Vec<u64> = (0..batch as u64).collect();
    while start.elapsed() < duration {
        match tx.try_push_n(&mut src) {
            Ok(n) => {
                produced += n as u64;
                src.clear();
                src.extend(0..batch as u64);
            }
            Err(_) => {}
        }
    }

    let consumed = consumer.join().unwrap();
    BenchResult {
        name: format!("bounded: 2-thread bulk ({})", batch),
        ops: consumed,
        elapsed: start.elapsed(),
    }
}

/// Bulk push_n / pop_n throughput (single-threaded).
fn bench_bounded_bulk(duration: Duration, batch: usize) -> BenchResult {
    type Q = Spsc<u64, 10, 6, NoOpSignal>;
    let (tx, rx): (Sender<u64, 10, 6, NoOpSignal>, Receiver<u64, 10, 6, NoOpSignal>) =
        Q::new_with_gate(NoOpSignal);

    let mut src: Vec<u64> = (0..batch as u64).collect();
    let mut dst = vec![0u64; batch];

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        let pushed = tx.try_push_n(&mut src).unwrap();
        let popped = rx.try_pop_n(&mut dst).unwrap();
        // Refill src for next iteration (try_push_n drains it).
        src.clear();
        src.extend(0..batch as u64);
        ops += pushed as u64 + popped as u64;
    }
    BenchResult {
        name: format!("bounded: bulk push_n+pop_n ({})", batch),
        ops,
        elapsed: start.elapsed(),
    }
}

/// consume_in_place throughput (single-threaded).
fn bench_bounded_consume_in_place(duration: Duration, batch: usize) -> BenchResult {
    type Q = Spsc<u64, 10, 6, NoOpSignal>;
    let (tx, rx): (Sender<u64, 10, 6, NoOpSignal>, Receiver<u64, 10, 6, NoOpSignal>) =
        Q::new_with_gate(NoOpSignal);

    let mut src: Vec<u64> = (0..batch as u64).collect();

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        let pushed = tx.try_push_n(&mut src).unwrap();
        src.clear();
        src.extend(0..batch as u64);
        ops += pushed as u64;
        let consumed = rx.consume_in_place(batch, |slice| {
            black_box(slice);
            slice.len()
        });
        ops += consumed as u64;
    }
    BenchResult {
        name: format!("bounded: consume_in_place ({})", batch),
        ops,
        elapsed: start.elapsed(),
    }
}

// ---------------------------------------------------------------------------
// Unbounded Spsc benchmarks
// ---------------------------------------------------------------------------

/// Single-threaded ping-pong: push 1, pop 1.
fn bench_unbounded_pingpong(duration: Duration) -> BenchResult {
    type Q = UnboundedSpsc<u64>;
    let (tx, rx): (UnboundedSender<u64>, UnboundedReceiver<u64>) = Q::new();

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        for _ in 0..1000 {
            tx.try_push(black_box(42u64)).unwrap();
            black_box(rx.try_pop().unwrap());
        }
        ops += 1000;
    }
    BenchResult {
        name: "unbounded: push-pop pingpong".into(),
        ops,
        elapsed: start.elapsed(),
    }
}

/// Single-threaded burst: push N, then pop N.
fn bench_unbounded_burst(duration: Duration, burst: usize) -> BenchResult {
    type Q = UnboundedSpsc<u64>;
    let (tx, rx): (UnboundedSender<u64>, UnboundedReceiver<u64>) = Q::new();

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        for i in 0..burst {
            tx.try_push(black_box(i as u64)).unwrap();
        }
        for _ in 0..burst {
            black_box(rx.try_pop().unwrap());
        }
        ops += burst as u64 * 2;
    }
    BenchResult {
        name: format!("unbounded: burst push+pop ({})", burst),
        ops,
        elapsed: start.elapsed(),
    }
}

/// Two-thread throughput: one item at a time (cache-line bouncing baseline).
fn bench_unbounded_concurrent_1x1(duration: Duration) -> BenchResult {
    type Q = UnboundedSpsc<u64>;
    let (tx, rx): (UnboundedSender<u64>, UnboundedReceiver<u64>) = Q::new();

    let dur = duration;
    let consumer = thread::spawn(move || {
        let mut count = 0u64;
        let start = Instant::now();
        while start.elapsed() < dur {
            if rx.try_pop().is_ok() {
                count += 1;
            }
        }
        while rx.try_pop().is_ok() {
            count += 1;
        }
        count
    });

    let start = Instant::now();
    let mut produced = 0u64;
    while start.elapsed() < duration {
        if tx.try_push(black_box(produced)).is_ok() {
            produced += 1;
        }
    }

    let consumed = consumer.join().unwrap();
    BenchResult {
        name: "unbounded: 2-thread 1×1".into(),
        ops: consumed,
        elapsed: start.elapsed(),
    }
}

/// Two-thread throughput: producer bursts via push_n, consumer drains via pop_n.
fn bench_unbounded_concurrent_bulk(duration: Duration, batch: usize) -> BenchResult {
    type Q = UnboundedSpsc<u64>;
    let (tx, rx): (UnboundedSender<u64>, UnboundedReceiver<u64>) = Q::new();

    let dur = duration;
    let b = batch;
    let consumer = thread::spawn(move || {
        let mut buf = vec![0u64; b];
        let mut count = 0u64;
        let start = Instant::now();
        while start.elapsed() < dur {
            match rx.try_pop_n(&mut buf) {
                Ok(n) => count += n as u64,
                Err(_) => {}
            }
        }
        while rx.try_pop_n(&mut buf).is_ok() {}
        count
    });

    let start = Instant::now();
    let mut produced = 0u64;
    let mut src: Vec<u64> = (0..batch as u64).collect();
    while start.elapsed() < duration {
        match tx.try_push_n(&mut src) {
            Ok(n) => {
                produced += n as u64;
                src.clear();
                src.extend(0..batch as u64);
            }
            Err(_) => {}
        }
    }

    let consumed = consumer.join().unwrap();
    BenchResult {
        name: format!("unbounded: 2-thread bulk ({})", batch),
        ops: consumed,
        elapsed: start.elapsed(),
    }
}

/// Bulk try_push_n throughput (single-threaded).
fn bench_unbounded_bulk(duration: Duration, batch: usize) -> BenchResult {
    type Q = UnboundedSpsc<u64>;
    let (tx, rx): (UnboundedSender<u64>, UnboundedReceiver<u64>) = Q::new();

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        let mut items: Vec<u64> = (0..batch as u64).collect();
        let pushed = tx.try_push_n(&mut items).unwrap();
        ops += pushed as u64;
        for _ in 0..pushed {
            black_box(rx.try_pop().unwrap());
        }
        ops += pushed as u64;
    }
    BenchResult {
        name: format!("unbounded: bulk push_n+pop ({})", batch),
        ops,
        elapsed: start.elapsed(),
    }
}

/// consume_in_place throughput (single-threaded).
fn bench_unbounded_consume_in_place(duration: Duration, batch: usize) -> BenchResult {
    type Q = UnboundedSpsc<u64>;
    let (tx, rx): (UnboundedSender<u64>, UnboundedReceiver<u64>) = Q::new();

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        for i in 0..batch {
            tx.try_push(black_box(i as u64)).unwrap();
        }
        ops += batch as u64;
        let consumed = rx.consume_in_place(batch, |slice| {
            black_box(slice);
            slice.len()
        });
        ops += consumed as u64;
    }
    BenchResult {
        name: format!("unbounded: consume_in_place ({})", batch),
        ops,
        elapsed: start.elapsed(),
    }
}

/// Stress test: producer far ahead, consumer catches up.
fn bench_unbounded_producer_ahead(duration: Duration) -> BenchResult {
    type Q = UnboundedSpsc<u64>;
    let (tx, rx): (UnboundedSender<u64>, UnboundedReceiver<u64>) = Q::new();

    let start = Instant::now();
    let mut ops = 0u64;
    while start.elapsed() < duration {
        // Producer bursts ahead
        for i in 0..10_000 {
            tx.try_push(black_box(i as u64)).unwrap();
        }
        // Consumer catches up
        for _ in 0..10_000 {
            black_box(rx.try_pop().unwrap());
        }
        ops += 20_000;
    }
    BenchResult {
        name: "unbounded: producer-ahead burst (10k)".into(),
        ops,
        elapsed: start.elapsed(),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let dur = Duration::from_millis(500);

    println!("\n=== Bounded Spsc<u64, 6, 8> (capacity={}) ===",
        Spsc::<u64, 6, 8, NoOpSignal>::capacity());
    bench_bounded_pingpong(dur).print();
    bench_bounded_burst(dur, 64).print();
    bench_bounded_burst(dur, 1024).print();
    bench_bounded_bulk(dur, 64).print();
    bench_bounded_bulk(dur, 1024).print();
    bench_bounded_consume_in_place(dur, 64).print();
    bench_bounded_consume_in_place(dur, 1024).print();
    bench_bounded_concurrent_1x1(dur).print();
    bench_bounded_concurrent_bulk(dur, 64).print();
    bench_bounded_concurrent_bulk(dur, 1024).print();

    println!("\n=== Unbounded UnboundedSpsc<u64> (node cap={}) ===",
        UnboundedSpsc::<u64>::NODE_CAP);
    bench_unbounded_pingpong(dur).print();
    bench_unbounded_burst(dur, 64).print();
    bench_unbounded_burst(dur, 1024).print();
    bench_unbounded_bulk(dur, 64).print();
    bench_unbounded_bulk(dur, 1024).print();
    bench_unbounded_consume_in_place(dur, 64).print();
    bench_unbounded_consume_in_place(dur, 1024).print();
    bench_unbounded_concurrent_1x1(dur).print();
    bench_unbounded_concurrent_bulk(dur, 64).print();
    bench_unbounded_concurrent_bulk(dur, 1024).print();
    bench_unbounded_producer_ahead(dur).print();

    println!();
}
