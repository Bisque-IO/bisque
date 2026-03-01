//! TLS TCP Echo Server and Client Example
//!
//! This example demonstrates:
//! 1. Using `ContractGroup` as an async runtime
//! 2. Creating a TLS server (TcpListener + TlsAcceptor)
//! 3. Spawning TLS client handlers as separate tasks
//! 4. Connecting as a TLS client (TcpStream + TlsConnector) and sending/receiving data
//!
//! Run with: cargo run --example tcp_echo_tls --features tls

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use maniac::BufResult;
use maniac::driver::{IoPool, IoPoolBuilder};
use maniac::net::TcpStream;
use maniac::net::tls::{ClientConfig, TlsAcceptor, TlsConnector, TlsStream};
use maniac::net::TcpListener;
use maniac::scheduler::ContractGroup;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

/// Generate a self-signed certificate for "localhost" using rcgen.
fn generate_test_certs() -> (Vec<CertificateDer<'static>>, rustls::pki_types::PrivateKeyDer<'static>) {
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let key_der = key_pair.serialize_der();
    let cert_der = cert.der().clone();

    (
        vec![cert_der],
        rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(key_der)),
    )
}

/// Run the scheduler until stopped.
fn run_scheduler(group: &ContractGroup, stop: &AtomicBool) {
    let mut bias = group.new_bias_state();
    while !stop.load(Ordering::Relaxed) {
        if group.poll_next(&mut bias).is_none() {
            std::thread::yield_now();
        }
    }
}

/// Handle a single TLS client connection — echo all received data back.
async fn handle_client(
    mut stream: TlsStream,
    client_id: usize,
    clients_done: Arc<AtomicUsize>,
) -> io::Result<()> {
    println!("[Server] Client {} TLS handler started", client_id);

    let mut total_bytes = 0usize;
    loop {
        let buf = vec![0u8; 1024];
        let BufResult(result, buf) = stream.recv(buf).await;

        match result {
            Ok(0) => {
                println!(
                    "[Server] Client {} disconnected (total {} bytes echoed over TLS)",
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

/// Run the TLS echo server.
async fn run_server(
    pool: Arc<IoPool>,
    group: Arc<ContractGroup>,
    acceptor: TlsAcceptor,
    port: Arc<AtomicU16>,
    ready: Arc<AtomicBool>,
    clients_done: Arc<AtomicUsize>,
    num_clients: usize,
) -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::listen(addr, pool)?;
    let local_addr = listener.local_addr()?;

    port.store(local_addr.port(), Ordering::Release);
    println!("[Server] Listening on {} (TLS)", local_addr);
    ready.store(true, Ordering::Release);

    for client_id in 0..num_clients {
        match listener.accept().await {
            Ok((tcp_stream, peer_addr)) => {
                println!("[Server] Accepted TCP connection from {}", peer_addr);

                let acceptor = acceptor.clone();
                let clients_done = Arc::clone(&clients_done);
                group
                    .spawn(
                        async move {
                            // Perform TLS handshake
                            match acceptor.accept(tcp_stream).await {
                                Ok(tls_stream) => {
                                    println!("[Server] TLS handshake complete with client {}", client_id);
                                    if let Err(e) =
                                        handle_client(tls_stream, client_id, clients_done).await
                                    {
                                        eprintln!(
                                            "[Server] Client {} handler error: {}",
                                            client_id, e
                                        );
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[Server] TLS handshake failed for client {}: {}",
                                        client_id, e
                                    );
                                    clients_done.fetch_add(1, Ordering::Release);
                                }
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

/// Run a TLS client that connects, sends messages, and receives echoes.
async fn run_client(
    pool: Arc<IoPool>,
    connector: TlsConnector,
    addr: SocketAddr,
    client_name: &str,
) -> io::Result<()> {
    println!("[Client {}] Connecting to {} (TLS)", client_name, addr);

    let tcp_stream = TcpStream::connect(addr, pool).await?;
    let mut tls_stream = connector.connect("localhost", tcp_stream).await?;
    println!("[Client {}] TLS handshake complete!", client_name);

    let messages = ["Hello, TLS server!", "Encrypted echo test", "Secure channel!"];

    for msg in messages {
        let BufResult(result, _) = tls_stream.send(msg.as_bytes().to_vec()).await;
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
        let BufResult(result, buf) = tls_stream.recv(buf).await;
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
    println!("=== TLS TCP Echo Server/Client Example ===\n");

    // Generate self-signed test certificates.
    let (certs, key) = generate_test_certs();
    println!("Generated self-signed certificate for localhost");

    // Build server TLS config.
    let server_config = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs.clone(), key)
            .expect("invalid server config"),
    );
    let acceptor = TlsAcceptor::new(server_config);

    // Build client TLS config that trusts our self-signed cert.
    let mut root_store = RootCertStore::empty();
    root_store.add(certs[0].clone()).expect("failed to add root cert");
    let client_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );
    let connector = TlsConnector::new(client_config);

    let pool = Arc::new(IoPoolBuilder::new().force_poll(false).build()?);
    println!(
        "IO Pool created with {} threads",
        pool.num_threads()
    );

    let group = Arc::new(ContractGroup::new_blocking());
    let stop = Arc::new(AtomicBool::new(false));
    let server_ready = Arc::new(AtomicBool::new(false));
    let server_port = Arc::new(AtomicU16::new(0));
    let clients_done = Arc::new(AtomicUsize::new(0));
    const NUM_CLIENTS: usize = 3;

    // Spawn server task.
    {
        let pool = Arc::clone(&pool);
        let group_clone = Arc::clone(&group);
        let ready = Arc::clone(&server_ready);
        let port = Arc::clone(&server_port);
        let clients_done = Arc::clone(&clients_done);
        let acceptor = acceptor.clone();
        group
            .spawn(
                async move {
                    if let Err(e) = run_server(
                        pool,
                        group_clone,
                        acceptor,
                        port,
                        ready,
                        clients_done,
                        NUM_CLIENTS,
                    )
                    .await
                    {
                        eprintln!("[Server] Error: {}", e);
                    }
                },
                true,
            )
            .expect("Failed to spawn server task");
    }

    // Start scheduler threads.
    let num_scheduler_threads = 2;
    let scheduler_handles: Vec<_> = (0..num_scheduler_threads)
        .map(|_| {
            let group = Arc::clone(&group);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                run_scheduler(&group, &stop);
            })
        })
        .collect();

    while !server_ready.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(10));
    }

    let port = server_port.load(Ordering::Acquire);
    let server_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    println!(
        "\n--- Server is ready on port {} (TLS), starting {} clients ---\n",
        port, NUM_CLIENTS
    );

    // Spawn all client tasks.
    for i in 0..NUM_CLIENTS {
        let pool = Arc::clone(&pool);
        let connector = connector.clone();
        let client_name = format!("C{}", i);
        let addr = server_addr;
        group
            .spawn(
                async move {
                    if let Err(e) = run_client(pool, connector, addr, &client_name).await {
                        eprintln!("[Client {}] Error: {}", client_name, e);
                    }
                },
                true,
            )
            .expect("Failed to spawn client task");
    }

    // Wait for all clients to complete.
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

    // Shutdown.
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
