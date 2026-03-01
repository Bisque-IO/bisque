//! TCP Echo Server and Client Example (Tokio Scheduler + Maniac IO)
//!
//! This example demonstrates:
//! 1. Using tokio as the async runtime/scheduler
//! 2. Using maniac's IO pool for all network operations (TcpListener, TcpStream)
//! 3. Spawning client handlers as tokio tasks
//! 4. Connecting as a client and sending/receiving data
//!
//! This is a variant of `tcp_echo.rs` that replaces maniac's `ContractGroup`
//! scheduler with tokio's multi-threaded runtime, while keeping maniac for IO.
//!
//! Run with: cargo run --example tcp_echo_tokio --features io-driver,tokio-bench

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};

use maniac::BufResult;
use maniac::driver::{IoPool, IoPoolBuilder};
use maniac::net::{TcpListener, TcpStream};

/// Handle a single client connection - echo all received data back.
async fn handle_client(
    stream: TcpStream,
    client_id: usize,
    clients_done: Arc<AtomicUsize>,
) -> io::Result<()> {
    println!("[Server] Client {} handler started", client_id);

    let mut total_bytes = 0usize;
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

/// Run the echo server - accepts connections and spawns handlers via tokio.
async fn run_server(
    pool: Arc<IoPool>,
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
                // Spawn handler as a tokio task instead of a ContractGroup task
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, client_id, clients_done).await {
                        eprintln!("[Server] Client {} handler error: {}", client_id, e);
                    }
                });
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

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("=== TCP Echo Server/Client Example (Tokio + Maniac IO) ===\n");

    let pool = Arc::new(IoPoolBuilder::new().force_poll(false).build()?);
    println!(
        "IO Pool created with {} threads (using maniac IO driver)",
        pool.num_threads()
    );

    let server_ready = Arc::new(AtomicBool::new(false));
    let server_port = Arc::new(AtomicU16::new(0));
    let clients_done = Arc::new(AtomicUsize::new(0));
    const NUM_CLIENTS: usize = 3;

    // Spawn server task on tokio
    let server_handle = {
        let pool = Arc::clone(&pool);
        let ready = Arc::clone(&server_ready);
        let port = Arc::clone(&server_port);
        let clients_done = Arc::clone(&clients_done);
        tokio::spawn(async move {
            if let Err(e) = run_server(pool, port, ready, clients_done, NUM_CLIENTS).await {
                eprintln!("[Server] Error: {}", e);
            }
        })
    };

    // Wait for server to be ready
    while !server_ready.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    let port = server_port.load(Ordering::Acquire);
    let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    println!(
        "\n--- Server is ready on port {}, starting {} clients ---\n",
        port, NUM_CLIENTS
    );

    // Spawn all client tasks on tokio
    let mut client_handles = Vec::new();
    for i in 0..NUM_CLIENTS {
        let pool = Arc::clone(&pool);
        let client_name = format!("C{}", i);
        let addr = server_addr;
        client_handles.push(tokio::spawn(async move {
            if let Err(e) = run_client(pool, addr, &client_name).await {
                eprintln!("[Client {}] Error: {}", client_name, e);
            }
        }));
    }

    // Wait for all client tasks to finish
    for handle in client_handles {
        handle.await.expect("Client task panicked");
    }

    // Wait for server to finish accepting all clients
    server_handle.await.expect("Server task panicked");

    // Wait for all client handlers to complete
    while clients_done.load(Ordering::Acquire) < NUM_CLIENTS {
        tokio::task::yield_now().await;
    }
    println!("all clients complete!");

    // Shutdown
    println!("\n--- Shutting down ---");
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
