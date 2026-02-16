//! TCP Echo Server and Client Example
//!
//! This example demonstrates:
//! 1. Using `ContractGroup` as an async runtime
//! 2. Creating a TCP listener and accepting connections
//! 3. Spawning client handlers as separate tasks
//! 4. Connecting as a client and sending/receiving data
//!
//! Run with: cargo run --example tcp_echo --features io-driver

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use maniac::BufResult;
use maniac::driver::{IoPool, IoPoolBuilder};
use maniac::net::{TcpListener, TcpStream};
use maniac::scheduler::{BiasState, ContractGroup};

/// Run the scheduler until stopped.
fn run_scheduler(group: &ContractGroup, stop: &AtomicBool, thread_id: usize) {
    let mut bias = group.new_bias_state();
    let mut iter = 0;
    while !stop.load(Ordering::Relaxed) {
        if group.poll_next(&mut bias).is_none() {
            std::thread::yield_now();
        }
        iter += 1;
    }
}

/// Handle a single client connection - echo all received data back.
async fn handle_client(
    stream: TcpStream,
    client_id: usize,
    clients_done: Arc<AtomicUsize>,
) -> io::Result<()> {
    println!("[Server] Client {} handler started", client_id);

    let mut total_bytes = 0usize;
    let mut msg_count = 0usize;
    loop {
        let buf = vec![0u8; 1024];
        let BufResult(result, buf) = stream.recv(buf).await;

        match result {
            Ok(0) => {
                println!(
                    "[Server] Client {} disconnected (total {} bytes echoed)",
                    client_id, total_bytes
                );
                break;
            }
            Ok(n) => {
                msg_count += 1;
                let data = buf[..n].to_vec();
                let BufResult(result, _) = stream.send(data).await;
                match result {
                    Ok(sent) => {
                        total_bytes += sent;
                    }
                    Err(e) => {
                        eprintln!("[Server] Client {} send error: {}", client_id, e);
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("[Server] Client {} recv error: {}", client_id, e);
                break;
            }
        }
    }

    clients_done.fetch_add(1, Ordering::Release);
    Ok(())
}

/// Run the echo server - accepts connections and spawns handlers.
async fn run_server(
    pool: Arc<IoPool>,
    group: Arc<ContractGroup>,
    port: Arc<AtomicU16>,
    ready: Arc<AtomicBool>,
    clients_done: Arc<AtomicUsize>,
    num_clients: usize,
) -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::listen(addr, pool)?;
    let local_addr = listener.local_addr()?;

    port.store(local_addr.port(), Ordering::Release);
    println!("[Server] Listening on {}", local_addr);
    ready.store(true, Ordering::Release);

    for client_id in 0..num_clients {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                println!("[Server] Accepted connection from {}", peer_addr);
                let clients_done = Arc::clone(&clients_done);
                group
                    .spawn(
                        async move {
                            if let Err(e) = handle_client(stream, client_id, clients_done).await {
                                eprintln!("[Server] Client {} handler error: {}", client_id, e);
                            }
                        },
                        true,
                    )
                    .expect("Failed to spawn client handler");
            }
            Err(e) => {
                eprintln!("[Server] Accept error: {}", e);
            }
        }
    }

    Ok(())
}

/// Run a client that connects, sends messages, and receives echoes.
async fn run_client(pool: Arc<IoPool>, addr: SocketAddr, client_name: &str) -> io::Result<()> {
    println!("[Client {}] Connecting to {}", client_name, addr);
    let stream = TcpStream::connect(addr, pool).await?;
    println!("[Client {}] Connected!", client_name);

    let messages = ["Hello, server!", "This is a test", "Echo me back!"];

    for msg in messages {
        let BufResult(result, _) = stream.send(msg.as_bytes().to_vec()).await;
        match result {
            Ok(sent) => {
                println!("[Client {}] Sent {} bytes: {:?}", client_name, sent, msg);
            }
            Err(e) => {
                eprintln!("[Client {}] Send error: {}", client_name, e);
                return Err(e);
            }
        }

        let buf = vec![0u8; 1024];
        let BufResult(result, buf) = stream.recv(buf).await;
        match result {
            Ok(n) => {
                let response = String::from_utf8_lossy(&buf[..n]);
                println!("[Client {}] Received echo: {:?}", client_name, response);
            }
            Err(e) => {
                eprintln!("[Client {}] Recv error: {}", client_name, e);
                return Err(e);
            }
        }
    }

    println!("[Client {}] Done", client_name);
    Ok(())
}

fn main() -> io::Result<()> {
    println!("=== TCP Echo Server/Client Example ===\n");

    let pool = Arc::new(IoPoolBuilder::new().force_poll(false).build()?);
    println!(
        "IO Pool created with {} threads (using poll driver)",
        pool.num_threads()
    );

    let group = Arc::new(ContractGroup::new_blocking());
    let stop = Arc::new(AtomicBool::new(false));
    let server_ready = Arc::new(AtomicBool::new(false));
    let server_port = Arc::new(AtomicU16::new(0));
    let clients_done = Arc::new(AtomicUsize::new(0));
    const NUM_CLIENTS: usize = 3;

    // Spawn server task
    {
        let pool = Arc::clone(&pool);
        let group_clone = Arc::clone(&group);
        let ready = Arc::clone(&server_ready);
        let port = Arc::clone(&server_port);
        let clients_done = Arc::clone(&clients_done);
        group
            .spawn(
                async move {
                    if let Err(e) =
                        run_server(pool, group_clone, port, ready, clients_done, NUM_CLIENTS).await
                    {
                        eprintln!("[Server] Error: {}", e);
                    }
                },
                true,
            )
            .expect("Failed to spawn server task");
    }

    // Start scheduler threads
    let num_scheduler_threads = 2;
    let scheduler_handles: Vec<_> = (0..num_scheduler_threads)
        .enumerate()
        .map(|(thread_id, _)| {
            let group = Arc::clone(&group);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                run_scheduler(&group, &stop, thread_id);
            })
        })
        .collect();

    while !server_ready.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(10));
    }

    let port = server_port.load(Ordering::Acquire);
    let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    println!(
        "\n--- Server is ready on port {}, starting {} clients ---\n",
        port, NUM_CLIENTS
    );

    // Spawn all client tasks
    for i in 0..NUM_CLIENTS {
        let pool = Arc::clone(&pool);
        let client_name = format!("C{}", i);
        let addr = server_addr;
        group
            .spawn(
                async move {
                    if let Err(e) = run_client(pool, addr, &client_name).await {
                        eprintln!("[Client {}] Error: {}", client_name, e);
                    }
                },
                true,
            )
            .expect("Failed to spawn client task");
    }

    // Wait for all clients to complete
    let mut last_count = 0;
    while clients_done.load(Ordering::Acquire) < NUM_CLIENTS {
        let count = clients_done.load(Ordering::Acquire);
        if count != last_count {
            println!(
                "waiting for clients to complete (now={} target={})",
                count, NUM_CLIENTS
            );
            last_count = count;
        }
        thread::sleep(Duration::from_millis(50));
    }
    println!("all clients complete!");

    // Shutdown
    println!("\n--- Shutting down ---");
    stop.store(true, Ordering::Release);
    group.stop();

    for handle in scheduler_handles {
        handle.join().expect("Scheduler thread panicked");
    }

    match Arc::try_unwrap(pool) {
        Ok(pool) => {
            pool.shutdown();
            println!("IO Pool shutdown complete");
        }
        Err(_) => {
            println!("Warning: IO Pool still has references");
        }
    }

    println!("\n=== Example Complete ===");
    Ok(())
}
