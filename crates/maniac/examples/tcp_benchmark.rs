//! TCP Benchmark: Maniac vs Tokio
//!
//! This benchmark compares TCP performance between:
//! 1. Tokio scheduler + Maniac IO (IoPool for TCP)
//! 2. Tokio scheduler + Tokio net (standard tokio::net::TcpStream)
//!
//! Run with: cargo run --example tcp_benchmark --features io-driver,tokio-bench --release
//!
//! The benchmark measures:
//! - Throughput (bytes/sec and messages/sec)
//! - Latency (min, max, avg, p50, p99)
//! - Connection establishment time

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::time::{Duration, Instant};

use maniac::BufResult;
use maniac::driver::{IoPool, IoPoolBuilder};
use maniac::net::{TcpListener as ManiacListener, TcpStream as ManiacStream};

// ============================================================================
// Configuration
// ============================================================================

const WARMUP_ITERATIONS: usize = 100;
const BENCHMARK_ITERATIONS: usize = 1000;
const MESSAGE_SIZES: &[usize] = &[64, 512, 1024, 4096, 16384];
const CONCURRENT_CONNECTIONS: &[usize] = &[1, 10, 100];
const NUM_SCHEDULER_THREADS: usize = 4;

// ============================================================================
// Statistics
// ============================================================================

#[derive(Debug, Clone, Copy, Default)]
struct LatencyStats {
    min: Duration,
    max: Duration,
    avg: Duration,
    p50: Duration,
    p99: Duration,
}

impl LatencyStats {
    fn from_samples(mut samples: Vec<Duration>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        samples.sort();

        let min = samples[0];
        let max = samples[samples.len() - 1];
        let sum: Duration = samples.iter().sum();
        let avg = sum / samples.len() as u32;
        let p50 = samples[samples.len() / 2];
        let p99_idx = (samples.len() * 99) / 100;
        let p99 = samples[p99_idx.min(samples.len() - 1)];

        Self {
            min,
            max,
            avg,
            p50,
            p99,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct BenchmarkResult {
    throughput_bytes_per_sec: f64,
    throughput_msgs_per_sec: f64,
    latency: LatencyStats,
    total_bytes: usize,
    total_messages: usize,
    duration: Duration,
}

impl BenchmarkResult {
    fn print(&self, name: &str) {
        println!("\n=== {} Results ===", name);
        println!("  Duration:          {:?}", self.duration);
        println!("  Total bytes:       {}", self.total_bytes);
        println!("  Total messages:    {}", self.total_messages);
        println!(
            "  Throughput:        {:.2} MB/s",
            self.throughput_bytes_per_sec / 1_000_000.0
        );
        println!("  Messages/sec:      {:.0}", self.throughput_msgs_per_sec);
        println!("  Latency (min):     {:?}", self.latency.min);
        println!("  Latency (avg):     {:?}", self.latency.avg);
        println!("  Latency (p50):     {:?}", self.latency.p50);
        println!("  Latency (p99):     {:?}", self.latency.p99);
        println!("  Latency (max):     {:?}", self.latency.max);
    }
}

// ============================================================================
// Maniac Implementation
// ============================================================================

async fn maniac_echo_server(
    pool: Arc<IoPool>,
    port: Arc<AtomicU16>,
    ready: Arc<AtomicBool>,
    num_connections: usize,
) -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = ManiacListener::listen(addr, pool)?;
    let local_addr = listener.local_addr()?;

    port.store(local_addr.port(), Ordering::Release);
    ready.store(true, Ordering::Release);

    for _ in 0..num_connections {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    loop {
                        let BufResult(result, recv_buf) = stream.recv(buf).await;
                        buf = recv_buf;

                        match result {
                            Ok(0) => break,
                            Ok(n) => {
                                let BufResult(result, send_buf) =
                                    stream.send(buf[..n].to_vec()).await;
                                buf = send_buf;
                                if result.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Server accept error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn run_maniac_benchmark(
    pool: Arc<IoPool>,
    addr: SocketAddr,
    msg_size: usize,
    num_messages: usize,
) -> io::Result<BenchmarkResult> {
    let stream = ManiacStream::connect(addr, pool).await?;
    let msg = vec![0xABu8; msg_size];

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let BufResult(result, _) = stream.send(msg.clone()).await;
        result?;
        let BufResult(result, buf) = stream.recv(vec![0u8; msg_size]).await;
        result?;
        let _ = buf;
    }

    // Benchmark
    let mut latencies = Vec::with_capacity(num_messages);
    let start = Instant::now();

    for _ in 0..num_messages {
        let msg_start = Instant::now();

        let BufResult(result, _) = stream.send(msg.clone()).await;
        result?;

        let BufResult(result, buf) = stream.recv(vec![0u8; msg_size]).await;
        result?;
        let _ = buf;

        latencies.push(msg_start.elapsed());
    }

    let duration = start.elapsed();
    let total_bytes = num_messages * msg_size * 2; // send + receive

    Ok(BenchmarkResult {
        throughput_bytes_per_sec: total_bytes as f64 / duration.as_secs_f64(),
        throughput_msgs_per_sec: num_messages as f64 / duration.as_secs_f64(),
        latency: LatencyStats::from_samples(latencies),
        total_bytes,
        total_messages: num_messages,
        duration,
    })
}

async fn run_maniac_concurrent_benchmark(
    pool: Arc<IoPool>,
    addr: SocketAddr,
    msg_size: usize,
    num_messages: usize,
    num_connections: usize,
) -> io::Result<BenchmarkResult> {
    let mut handles = Vec::with_capacity(num_connections);

    for _ in 0..num_connections {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            run_maniac_benchmark(pool, addr, msg_size, num_messages / num_connections).await
        }));
    }

    let mut results = Vec::with_capacity(num_connections);
    for handle in handles {
        results.push(handle.await.unwrap()?);
    }

    // Aggregate results
    let total_bytes: usize = results.iter().map(|r| r.total_bytes).sum();
    let total_messages: usize = results.iter().map(|r| r.total_messages).sum();
    let duration = results.iter().map(|r| r.duration).max().unwrap();

    // Note: In a real implementation, we'd need to collect individual latencies
    // For simplicity, use average
    let avg_latency =
        results.iter().map(|r| r.latency.avg).sum::<Duration>() / num_connections as u32;

    Ok(BenchmarkResult {
        throughput_bytes_per_sec: total_bytes as f64 / duration.as_secs_f64(),
        throughput_msgs_per_sec: total_messages as f64 / duration.as_secs_f64(),
        latency: LatencyStats {
            min: avg_latency,
            max: avg_latency,
            avg: avg_latency,
            p50: avg_latency,
            p99: avg_latency,
        },
        total_bytes,
        total_messages,
        duration,
    })
}

fn benchmark_maniac() -> io::Result<Vec<(usize, usize, BenchmarkResult)>> {
    println!("\n========================================");
    println!("  Benchmarking: Maniac");
    println!("========================================\n");

    let pool = Arc::new(IoPoolBuilder::new().force_poll(false).build()?);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let results = rt.block_on(async {
        let mut all_results = Vec::new();

        for &num_conns in CONCURRENT_CONNECTIONS {
            for &msg_size in MESSAGE_SIZES {
                println!(
                    "  Testing: {} connections, {} byte messages",
                    num_conns, msg_size
                );

                let port = Arc::new(AtomicU16::new(0));
                let ready = Arc::new(AtomicBool::new(false));

                // Start server
                let server_pool = Arc::clone(&pool);
                let server_port = Arc::clone(&port);
                let server_ready = Arc::clone(&ready);

                let server_handle = tokio::spawn(async move {
                    maniac_echo_server(server_pool, server_port, server_ready, num_conns).await
                });

                // Wait for server
                while !ready.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }

                let addr: SocketAddr = format!("127.0.0.1:{}", port.load(Ordering::Acquire))
                    .parse()
                    .unwrap();

                // Run benchmark
                let result = if num_conns == 1 {
                    run_maniac_benchmark(Arc::clone(&pool), addr, msg_size, BENCHMARK_ITERATIONS)
                        .await?
                } else {
                    run_maniac_concurrent_benchmark(
                        Arc::clone(&pool),
                        addr,
                        msg_size,
                        BENCHMARK_ITERATIONS,
                        num_conns,
                    )
                    .await?
                };

                result.print(&format!("Maniac ({} conns, {} bytes)", num_conns, msg_size));
                all_results.push((num_conns, msg_size, result));

                // Wait for server to complete
                let _ = server_handle.await;
            }
        }

        Ok::<_, io::Error>(all_results)
    })?;

    Ok(results)
}

// ============================================================================
// Tokio Implementation
// ============================================================================

async fn tokio_echo_server(
    port: Arc<AtomicU16>,
    ready: Arc<AtomicBool>,
    num_connections: usize,
) -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    port.store(local_addr.port(), Ordering::Release);
    ready.store(true, Ordering::Release);

    for _ in 0..num_connections {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                tokio::spawn(async move {
                    let (mut rd, mut wr) = stream.split();
                    let mut buf = vec![0u8; 65536];

                    loop {
                        match rd.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                if wr.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Server accept error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn run_tokio_benchmark(
    addr: SocketAddr,
    msg_size: usize,
    num_messages: usize,
) -> io::Result<BenchmarkResult> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let msg = vec![0xABu8; msg_size];
    let (mut rd, mut wr) = stream.split();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        wr.write_all(&msg).await?;
        let mut buf = vec![0u8; msg_size];
        rd.read_exact(&mut buf).await?;
    }

    // Benchmark
    let mut latencies = Vec::with_capacity(num_messages);
    let start = Instant::now();

    for _ in 0..num_messages {
        let msg_start = Instant::now();

        wr.write_all(&msg).await?;
        let mut buf = vec![0u8; msg_size];
        rd.read_exact(&mut buf).await?;

        latencies.push(msg_start.elapsed());
    }

    let duration = start.elapsed();
    let total_bytes = num_messages * msg_size * 2;

    Ok(BenchmarkResult {
        throughput_bytes_per_sec: total_bytes as f64 / duration.as_secs_f64(),
        throughput_msgs_per_sec: num_messages as f64 / duration.as_secs_f64(),
        latency: LatencyStats::from_samples(latencies),
        total_bytes,
        total_messages: num_messages,
        duration,
    })
}

async fn run_tokio_concurrent_benchmark(
    addr: SocketAddr,
    msg_size: usize,
    num_messages: usize,
    num_connections: usize,
) -> io::Result<BenchmarkResult> {
    let mut handles = Vec::with_capacity(num_connections);

    for _ in 0..num_connections {
        handles.push(tokio::spawn(async move {
            run_tokio_benchmark(addr, msg_size, num_messages / num_connections).await
        }));
    }

    let mut results = Vec::with_capacity(num_connections);
    for handle in handles {
        results.push(handle.await.unwrap()?);
    }

    let total_bytes: usize = results.iter().map(|r| r.total_bytes).sum();
    let total_messages: usize = results.iter().map(|r| r.total_messages).sum();
    let duration = results.iter().map(|r| r.duration).max().unwrap();

    let avg_latency =
        results.iter().map(|r| r.latency.avg).sum::<Duration>() / num_connections as u32;

    Ok(BenchmarkResult {
        throughput_bytes_per_sec: total_bytes as f64 / duration.as_secs_f64(),
        throughput_msgs_per_sec: total_messages as f64 / duration.as_secs_f64(),
        latency: LatencyStats {
            min: avg_latency,
            max: avg_latency,
            avg: avg_latency,
            p50: avg_latency,
            p99: avg_latency,
        },
        total_bytes,
        total_messages,
        duration,
    })
}

fn benchmark_tokio() -> io::Result<Vec<(usize, usize, BenchmarkResult)>> {
    println!("\n========================================");
    println!("  Benchmarking: Tokio");
    println!("========================================\n");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(NUM_SCHEDULER_THREADS)
        .enable_all()
        .build()
        .unwrap();

    let results = rt.block_on(async {
        let mut all_results = Vec::new();

        for &num_conns in CONCURRENT_CONNECTIONS {
            for &msg_size in MESSAGE_SIZES {
                println!(
                    "  Testing: {} connections, {} byte messages",
                    num_conns, msg_size
                );

                let port = Arc::new(AtomicU16::new(0));
                let ready = Arc::new(AtomicBool::new(false));

                // Start server
                let server_port = Arc::clone(&port);
                let server_ready = Arc::clone(&ready);

                let server_handle = tokio::spawn(async move {
                    tokio_echo_server(server_port, server_ready, num_conns).await
                });

                // Wait for server
                while !ready.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }

                let addr: SocketAddr = format!("127.0.0.1:{}", port.load(Ordering::Acquire))
                    .parse()
                    .unwrap();

                // Run benchmark
                let result = if num_conns == 1 {
                    run_tokio_benchmark(addr, msg_size, BENCHMARK_ITERATIONS).await?
                } else {
                    run_tokio_concurrent_benchmark(addr, msg_size, BENCHMARK_ITERATIONS, num_conns)
                        .await?
                };

                result.print(&format!("Tokio ({} conns, {} bytes)", num_conns, msg_size));
                all_results.push((num_conns, msg_size, result));

                // Wait for server to complete
                let _ = server_handle.await;
            }
        }

        Ok::<_, io::Error>(all_results)
    })?;

    Ok(results)
}

// ============================================================================
// Main
// ============================================================================

use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn print_comparison(
    maniac_results: &[(usize, usize, BenchmarkResult)],
    tokio_results: &[(usize, usize, BenchmarkResult)],
) {
    println!("\n========================================");
    println!("  Comparison Summary");
    println!("========================================\n");

    println!(
        "{:<12} {:<12} {:<15} {:<15} {:<12}",
        "Connections", "Msg Size", "Maniac MB/s", "Tokio MB/s", "Speedup"
    );
    println!("{}", "-".repeat(66));

    for ((m_conns, m_size, m_res), (t_conns, t_size, t_res)) in
        maniac_results.iter().zip(tokio_results.iter())
    {
        assert_eq!(m_conns, t_conns);
        assert_eq!(m_size, t_size);

        let maniac_mbps = m_res.throughput_bytes_per_sec / 1_000_000.0;
        let tokio_mbps = t_res.throughput_bytes_per_sec / 1_000_000.0;
        let speedup = maniac_mbps / tokio_mbps;

        println!(
            "{:<12} {:<12} {:<15.2} {:<15.2} {:<12.2}x",
            m_conns, m_size, maniac_mbps, tokio_mbps, speedup
        );
    }

    println!(
        "\n{:<12} {:<12} {:<15} {:<15} {:<12}",
        "Connections", "Msg Size", "Maniac Latency", "Tokio Latency", "Ratio"
    );
    println!("{}", "-".repeat(70));

    for ((m_conns, m_size, m_res), (_t_conns, _t_size, t_res)) in
        maniac_results.iter().zip(tokio_results.iter())
    {
        let maniac_us = m_res.latency.avg.as_micros();
        let tokio_us = t_res.latency.avg.as_micros();
        let ratio = maniac_us as f64 / tokio_us as f64;

        println!(
            "{:<12} {:<12} {:<15} {:<15} {:<12.2}",
            m_conns,
            m_size,
            format!("{:?}", m_res.latency.avg),
            format!("{:?}", t_res.latency.avg),
            format!("{:.2}x", ratio)
        );
    }
}

fn main() -> io::Result<()> {
    println!("========================================");
    println!("  TCP Benchmark: Maniac vs Tokio");
    println!("========================================");
    println!("  Iterations per test: {}", BENCHMARK_ITERATIONS);
    println!("  Message sizes: {:?}", MESSAGE_SIZES);
    println!("  Concurrent connections: {:?}", CONCURRENT_CONNECTIONS);

    // Run benchmarks
    let maniac_results = benchmark_maniac()?;
    let tokio_results = benchmark_tokio()?;

    // Print comparison
    print_comparison(&maniac_results, &tokio_results);

    println!("\n========================================");
    println!("  Benchmark Complete");
    println!("========================================");

    Ok(())
}
