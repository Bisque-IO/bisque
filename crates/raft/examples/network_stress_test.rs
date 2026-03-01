//! Network Stress Test
//!
//! This example stress tests the tokio TCP transport and RPC layer
//! without any Raft logic, to isolate networking performance issues.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Simple framed protocol: 4-byte length prefix + payload
async fn write_frame<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8]) -> std::io::Result<()> {
    let len = data.len() as u32;
    let len_bytes = len.to_le_bytes();

    // Write length prefix
    writer.write_all(&len_bytes).await?;

    // Write payload
    writer.write_all(data).await?;

    Ok(())
}

async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    // Read length prefix
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;

    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Frame too large: {} bytes", len),
        ));
    }

    // Read payload
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;

    Ok(payload)
}

/// Stats tracker
struct Stats {
    requests_sent: AtomicU64,
    requests_received: AtomicU64,
    responses_sent: AtomicU64,
    responses_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    errors: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            requests_sent: AtomicU64::new(0),
            requests_received: AtomicU64::new(0),
            responses_sent: AtomicU64::new(0),
            responses_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    fn print(&self, elapsed: Duration) {
        let req_sent = self.requests_sent.load(Ordering::Relaxed);
        let req_recv = self.requests_received.load(Ordering::Relaxed);
        let resp_sent = self.responses_sent.load(Ordering::Relaxed);
        let resp_recv = self.responses_received.load(Ordering::Relaxed);
        let bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
        let bytes_recv = self.bytes_received.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);

        let secs = elapsed.as_secs_f64();
        let req_per_sec = resp_recv as f64 / secs;
        let mb_per_sec = (bytes_sent + bytes_recv) as f64 / 1024.0 / 1024.0 / secs;

        println!("  Requests sent:     {}", req_sent);
        println!("  Requests received: {}", req_recv);
        println!("  Responses sent:    {}", resp_sent);
        println!("  Responses received:{}", resp_recv);
        println!("  Bytes sent:        {} KB", bytes_sent / 1024);
        println!("  Bytes received:    {} KB", bytes_recv / 1024);
        println!("  Errors:            {}", errors);
        println!("  Throughput:        {:.0} req/s", req_per_sec);
        println!("  Bandwidth:         {:.2} MB/s", mb_per_sec);
    }
}

/// Simple echo server - receives requests, sends back responses
async fn run_server(addr: SocketAddr, stats: Arc<Stats>, shutdown: Arc<AtomicU64>) {
    let listener = TcpListener::bind(addr).await.expect("Failed to bind");
    println!("  Server listening on {}", addr);

    while shutdown.load(Ordering::Relaxed) == 0 {
        // Accept with timeout so we can check shutdown flag
        match tokio::time::timeout(Duration::from_millis(100), listener.accept()).await {
            Ok(Ok((stream, peer))) => {
                let stats = stats.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer, stats, shutdown).await;
                });
            }
            Ok(Err(e)) => {
                println!("  Accept error: {}", e);
            }
            Err(_timeout) => {
                // Just check shutdown flag and continue
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    _peer: SocketAddr,
    stats: Arc<Stats>,
    shutdown: Arc<AtomicU64>,
) {
    let (mut read_half, mut write_half) = stream.into_split();

    // Use kanal channel for responses from reader to writer
    let (tx, rx) = kanal::bounded_async::<Vec<u8>>(1024);

    // Spawn writer task
    let writer_stats = stats.clone();
    let writer_shutdown = shutdown.clone();
    tokio::spawn(async move {
        while writer_shutdown.load(Ordering::Relaxed) == 0 {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Ok(data)) => {
                    let len = data.len();
                    if write_frame(&mut write_half, &data).await.is_ok() {
                        writer_stats.responses_sent.fetch_add(1, Ordering::Relaxed);
                        writer_stats
                            .bytes_sent
                            .fetch_add(len as u64, Ordering::Relaxed);
                    }
                }
                Ok(Err(_)) => break, // Channel closed
                Err(_) => {}         // Timeout, continue
            }
        }
    });

    // Reader loop
    while shutdown.load(Ordering::Relaxed) == 0 {
        match tokio::time::timeout(Duration::from_millis(500), read_frame(&mut read_half)).await {
            Ok(Ok(data)) => {
                stats.requests_received.fetch_add(1, Ordering::Relaxed);
                stats
                    .bytes_received
                    .fetch_add(data.len() as u64, Ordering::Relaxed);

                // Echo back the data
                if tx.send(data).await.is_err() {
                    break;
                }
            }
            Ok(Err(_)) => break, // Connection closed or error
            Err(_) => {}         // Timeout, continue
        }
    }
}

/// Client that sends requests and receives responses
async fn run_client(
    id: u32,
    addr: SocketAddr,
    stats: Arc<Stats>,
    shutdown: Arc<AtomicU64>,
    payload_size: usize,
    concurrent_requests: usize,
) {
    // Connect to server
    let stream = match TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            println!("  Client {} failed to connect: {}", id, e);
            stats.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    let (mut read_half, mut write_half) = stream.into_split();

    // Channel for coordinating request/response using kanal
    let (resp_tx, resp_rx) = kanal::bounded_async::<()>(1024);

    // Spawn reader task
    let reader_stats = stats.clone();
    let reader_shutdown = shutdown.clone();
    tokio::spawn(async move {
        while reader_shutdown.load(Ordering::Relaxed) == 0 {
            match tokio::time::timeout(Duration::from_millis(500), read_frame(&mut read_half))
                .await
            {
                Ok(Ok(data)) => {
                    reader_stats
                        .responses_received
                        .fetch_add(1, Ordering::Relaxed);
                    reader_stats
                        .bytes_received
                        .fetch_add(data.len() as u64, Ordering::Relaxed);
                    // Signal that we can send another request
                    let _ = resp_tx.send(()).await;
                }
                Ok(Err(_)) => break,
                Err(_) => {} // Timeout
            }
        }
    });

    // Create payload
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 256) as u8).collect();

    // Writer loop - send requests as fast as we get responses back
    let mut outstanding = 0usize;

    while shutdown.load(Ordering::Relaxed) == 0 {
        // Send a request if we have capacity
        if outstanding < concurrent_requests {
            if write_frame(&mut write_half, &payload).await.is_ok() {
                stats.requests_sent.fetch_add(1, Ordering::Relaxed);
                stats
                    .bytes_sent
                    .fetch_add(payload_size as u64, Ordering::Relaxed);
                outstanding += 1;
            } else {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }

        // Check for responses (non-blocking)
        match tokio::time::timeout(Duration::from_micros(100), resp_rx.recv()).await {
            Ok(Ok(())) => {
                outstanding = outstanding.saturating_sub(1);
            }
            Ok(Err(_)) => break, // Channel closed
            Err(_) => {}         // Timeout, continue sending
        }
    }
}

async fn run_stress_test(
    num_clients: u32,
    payload_size: usize,
    concurrent_per_client: usize,
    duration_secs: u64,
) {
    println!(
        "\n=== Stress Test: {} clients, {} byte payloads, {} concurrent/client ===\n",
        num_clients, payload_size, concurrent_per_client
    );

    let server_addr: SocketAddr = "127.0.0.1:19001".parse().unwrap();
    let server_stats = Arc::new(Stats::new());
    let client_stats = Arc::new(Stats::new());
    let shutdown = Arc::new(AtomicU64::new(0));

    // Start server
    let server_stats_clone = server_stats.clone();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        run_server(server_addr, server_stats_clone, shutdown_clone).await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start clients
    for i in 0..num_clients {
        let stats = client_stats.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_client(
                i,
                server_addr,
                stats,
                shutdown,
                payload_size,
                concurrent_per_client,
            )
            .await;
        });
    }

    // Run for specified duration, printing stats periodically
    let start = Instant::now();
    let mut last_print = start;

    while start.elapsed() < Duration::from_secs(duration_secs) {
        tokio::time::sleep(Duration::from_secs(1)).await;

        if last_print.elapsed() >= Duration::from_secs(2) {
            let elapsed = start.elapsed();
            println!("--- Stats at {:.1}s ---", elapsed.as_secs_f64());
            println!("Server:");
            server_stats.print(elapsed);
            println!("Client:");
            client_stats.print(elapsed);
            last_print = Instant::now();
        }
    }

    // Signal shutdown
    shutdown.store(1, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Print final stats
    let elapsed = start.elapsed();
    println!("\n=== Final Stats ({:.1}s) ===", elapsed.as_secs_f64());
    println!("\nServer:");
    server_stats.print(elapsed);
    println!("\nClient:");
    client_stats.print(elapsed);
}

async fn async_main() {
    println!("=== Tokio Network Stress Test ===\n");

    // Test 1: Small messages, high concurrency
    run_stress_test(4, 64, 100, 10).await;

    // Test 2: Medium messages, medium concurrency
    run_stress_test(4, 1024, 50, 10).await;

    // Test 3: Large messages, low concurrency
    run_stress_test(4, 8192, 10, 10).await;

    // Test 4: Single client baseline
    run_stress_test(1, 256, 100, 10).await;

    println!("\n=== All tests completed ===\n");
}

#[tokio::main]
async fn main() {
    println!("Starting network stress test...\n");

    async_main().await;

    println!("Done.");
}
