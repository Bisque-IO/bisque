//! Storage throughput benchmark for mmap-based raft log storage.
//!
//! Measures raw write and read throughput with configurable payload sizes,
//! batch sizes, and number of groups.
//!
//! Run with: cargo run --release --example storage_bench
//! Options:  cargo run --release --example storage_bench -- --help

use bisque_raft::multi::codec::{BorrowPayload, CodecError, Decode, Encode};
use bisque_raft::multi::storage_mmap::{
    MmapGroupLogStorage, MmapPerGroupLogStorage, MmapStorageConfig,
};
use bisque_raft::multi::type_config::ManiacRaftTypeConfig;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::type_config::async_runtime::{AsyncRuntime, Oneshot};
use openraft::{LogId, RaftTypeConfig};
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Bench data type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BenchData(Vec<u8>);

impl std::fmt::Display for BenchData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BenchData(len={})", self.0.len())
    }
}

impl Encode for BenchData {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        (self.0.len() as u32).encode(writer)?;
        writer.write_all(&self.0)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.0.len()
    }
}

impl Decode for BenchData {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(Self(buf))
    }
}

impl BorrowPayload for BenchData {
    fn payload_bytes(&self) -> &[u8] {
        &self.0
    }
}

type C = ManiacRaftTypeConfig<BenchData, ()>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Rt = <C as RaftTypeConfig>::AsyncRuntime;
type Os = <Rt as AsyncRuntime>::Oneshot;

fn oneshot() -> (
    <Os as Oneshot>::Sender<Result<(), io::Error>>,
    <Os as Oneshot>::Receiver<Result<(), io::Error>>,
) {
    Os::channel()
}

fn make_entry(index: u64, payload: &[u8]) -> openraft::impls::Entry<C> {
    openraft::impls::Entry::<C> {
        log_id: LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 1,
                node_id: 1,
            },
            index,
        },
        payload: openraft::EntryPayload::Normal(BenchData(payload.to_vec())),
    }
}

fn fmt_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn fmt_rate(bytes: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return "inf".to_string();
    }
    let bytes_per_sec = bytes as f64 / secs;
    fmt_bytes(bytes_per_sec as u64)
}

fn print_result(label: &str, entries: u64, bytes: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = if secs > 0.0 {
        entries as f64 / secs
    } else {
        f64::INFINITY
    };
    println!(
        "    {:<16} {:>10} entries  {:>10}  {:>12}/s  {:>12} ops/s  {:.3}s",
        label,
        entries,
        fmt_bytes(bytes),
        fmt_rate(bytes, elapsed),
        format_ops(ops_per_sec),
        secs,
    );
}

fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.2}M", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.2}K", ops / 1_000.0)
    } else {
        format!("{:.0}", ops)
    }
}

// ---------------------------------------------------------------------------
// Storage wrapper
// ---------------------------------------------------------------------------

struct MmapStorage(MmapPerGroupLogStorage<C>);

impl MmapStorage {
    async fn create(dir: PathBuf, segment_size: u64, fsync_delay: Duration) -> io::Result<Self> {
        let cfg = MmapStorageConfig::new(dir)
            .with_segment_size(segment_size)
            .with_fsync_delay(fsync_delay);
        Ok(Self(MmapPerGroupLogStorage::<C>::new(cfg).await?))
    }

    async fn get_group(&self, group_id: u64) -> io::Result<MmapGroupLogStorage<C>> {
        self.0.get_log_storage(group_id).await
    }

    fn stop(&self) {
        self.0.stop();
    }
}

// ---------------------------------------------------------------------------
// Benchmark configs
// ---------------------------------------------------------------------------

struct BenchConfig {
    label: &'static str,
    payload_size: usize,
    batch_size: usize,
    total_entries: u64,
}

fn bench_configs() -> Vec<BenchConfig> {
    vec![
        // Small payloads - tests overhead per entry
        BenchConfig {
            label: "64B payload, batch=1",
            payload_size: 64,
            batch_size: 1,
            total_entries: 50_000,
        },
        BenchConfig {
            label: "64B payload, batch=100",
            payload_size: 64,
            batch_size: 100,
            total_entries: 100_000,
        },
        BenchConfig {
            label: "64B payload, batch=1000",
            payload_size: 64,
            batch_size: 1000,
            total_entries: 200_000,
        },
        // Medium payloads - typical raft entries
        BenchConfig {
            label: "1KB payload, batch=1",
            payload_size: 1024,
            batch_size: 1,
            total_entries: 50_000,
        },
        BenchConfig {
            label: "1KB payload, batch=100",
            payload_size: 1024,
            batch_size: 100,
            total_entries: 100_000,
        },
        BenchConfig {
            label: "1KB payload, batch=1000",
            payload_size: 1024,
            batch_size: 1000,
            total_entries: 200_000,
        },
        // Large payloads - throughput bound
        BenchConfig {
            label: "64KB payload, batch=1",
            payload_size: 64 * 1024,
            batch_size: 1,
            total_entries: 10_000,
        },
        BenchConfig {
            label: "64KB payload, batch=10",
            payload_size: 64 * 1024,
            batch_size: 10,
            total_entries: 10_000,
        },
    ]
}

// ---------------------------------------------------------------------------
// Write benchmark
// ---------------------------------------------------------------------------

async fn bench_write(
    cfg: &BenchConfig,
    dir: PathBuf,
    segment_size: u64,
    fsync_delay: Duration,
) -> io::Result<Duration> {
    let storage = MmapStorage::create(dir, segment_size, fsync_delay).await?;
    let mut group = storage.get_group(1).await?;

    let payload = vec![0x42u8; cfg.payload_size];
    let total = cfg.total_entries;
    let batch = cfg.batch_size as u64;

    // Pre-warm: write a small batch to trigger segment creation
    {
        let entry = make_entry(0, &payload);
        let (tx, rx) = oneshot();
        group
            .append(vec![entry], IOFlushed::<C>::signal(tx))
            .await?;
        let _ = rx.await;
    }

    let start = Instant::now();
    let mut index = 1u64;
    let mut pending = Vec::new();

    while index <= total {
        let end = (index + batch).min(total + 1);
        let entries: Vec<_> = (index..end).map(|i| make_entry(i, &payload)).collect();
        let count = entries.len();

        let (tx, rx) = oneshot();
        group.append(entries, IOFlushed::<C>::signal(tx)).await?;
        pending.push(rx);

        index += count as u64;
    }

    // Wait for all writes to be durable
    for rx in pending {
        let result: Result<(), io::Error> = rx
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("callback recv: {}", e)))?;
        result?;
    }

    let elapsed = start.elapsed();

    storage.stop();
    tokio::time::sleep(Duration::from_millis(20)).await;

    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// Read benchmark (sequential scan)
// ---------------------------------------------------------------------------

async fn bench_read_seq(
    cfg: &BenchConfig,
    dir: PathBuf,
    segment_size: u64,
    fsync_delay: Duration,
) -> io::Result<Duration> {
    // Populate
    let storage = MmapStorage::create(dir.clone(), segment_size, fsync_delay).await?;
    let mut group = storage.get_group(1).await?;

    let payload = vec![0x42u8; cfg.payload_size];
    let total = cfg.total_entries;
    let batch = cfg.batch_size as u64;

    let mut index = 1u64;
    let mut pending = Vec::new();
    while index <= total {
        let end = (index + batch).min(total + 1);
        let entries: Vec<_> = (index..end).map(|i| make_entry(i, &payload)).collect();
        let count = entries.len();

        let (tx, rx) = oneshot();
        group.append(entries, IOFlushed::<C>::signal(tx)).await?;
        pending.push(rx);
        index += count as u64;
    }
    for rx in pending {
        let _ = rx.await;
    }

    // Drop all handles and re-open
    drop(group);
    storage.stop();
    drop(storage);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let storage = MmapStorage::create(dir, segment_size, fsync_delay).await?;
    let mut group = storage.get_group(1).await?;

    let read_batch = batch.max(100);
    let start = Instant::now();

    let mut read_index = 1u64;
    while read_index <= total {
        let end = (read_index + read_batch).min(total + 1);
        let entries: Vec<openraft::impls::Entry<C>> =
            group.try_get_log_entries(read_index..end).await?;
        assert!(
            !entries.is_empty(),
            "expected entries for range {}..{}",
            read_index,
            end
        );
        read_index += entries.len() as u64;
    }

    let elapsed = start.elapsed();

    storage.stop();
    tokio::time::sleep(Duration::from_millis(20)).await;

    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// Read benchmark (random access)
// ---------------------------------------------------------------------------

async fn bench_read_random(
    cfg: &BenchConfig,
    dir: PathBuf,
    segment_size: u64,
    fsync_delay: Duration,
) -> io::Result<Duration> {
    // Populate
    let storage = MmapStorage::create(dir.clone(), segment_size, fsync_delay).await?;
    let mut group = storage.get_group(1).await?;

    let payload = vec![0x42u8; cfg.payload_size];
    let total = cfg.total_entries;
    let batch = cfg.batch_size as u64;

    let mut index = 1u64;
    let mut pending = Vec::new();
    while index <= total {
        let end = (index + batch).min(total + 1);
        let entries: Vec<_> = (index..end).map(|i| make_entry(i, &payload)).collect();
        let count = entries.len();

        let (tx, rx) = oneshot();
        group.append(entries, IOFlushed::<C>::signal(tx)).await?;
        pending.push(rx);
        index += count as u64;
    }
    for rx in pending {
        let _ = rx.await;
    }

    // Drop all handles and re-open
    drop(group);
    storage.stop();
    drop(storage);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let storage = MmapStorage::create(dir, segment_size, fsync_delay).await?;
    let mut group = storage.get_group(1).await?;

    // Build a pseudo-random index sequence (deterministic for reproducibility)
    let num_reads = total.min(20_000);
    let indices: Vec<u64> = (0..num_reads)
        .map(|i| {
            let h = (i
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407))
                % total;
            h + 1
        })
        .collect();

    let start = Instant::now();

    for &idx in &indices {
        let entries: Vec<openraft::impls::Entry<C>> = group.try_get_log_entries(idx..=idx).await?;
        assert_eq!(entries.len(), 1, "expected 1 entry at index {}", idx);
    }

    let elapsed = start.elapsed();

    storage.stop();
    tokio::time::sleep(Duration::from_millis(20)).await;

    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// Multi-group write benchmark
// ---------------------------------------------------------------------------

async fn bench_multi_group_write(
    num_groups: u64,
    payload_size: usize,
    entries_per_group: u64,
    batch_size: u64,
    dir: PathBuf,
    segment_size: u64,
    fsync_delay: Duration,
) -> io::Result<Duration> {
    let storage = MmapStorage::create(dir, segment_size, fsync_delay).await?;

    let payload = vec![0x42u8; payload_size];

    let mut groups = Vec::with_capacity(num_groups as usize);
    for g in 0..num_groups {
        groups.push(storage.get_group(g).await?);
    }

    let start = Instant::now();
    let mut pending = Vec::new();

    for round in 0..(entries_per_group / batch_size) {
        for (_g, group) in groups.iter_mut().enumerate() {
            let base = round * batch_size + 1;
            let entries: Vec<_> = (0..batch_size)
                .map(|i| make_entry(base + i, &payload))
                .collect();

            let (tx, rx) = oneshot();
            group.append(entries, IOFlushed::<C>::signal(tx)).await?;
            pending.push(rx);
        }
    }

    for rx in pending {
        let result: Result<(), io::Error> = rx
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("callback recv: {}", e)))?;
        result?;
    }

    let elapsed = start.elapsed();

    storage.stop();
    tokio::time::sleep(Duration::from_millis(20)).await;

    Ok(elapsed)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("Usage: storage_bench [OPTIONS]");
        println!();
        println!("Options:");
        println!("  --write-only     Run only write benchmarks");
        println!("  --read-only      Run only read benchmarks");
        println!("  --multi-only     Run only multi-group benchmarks");
        println!("  --quick          Run with reduced entry counts");
        println!("  --fsync-none     Fsync after every write (no batching)");
        println!("  --segment-size N Segment size in MB (default: 64)");
        println!("  --help           Print this help");
        return Ok(());
    }

    let write_only = args.iter().any(|a| a == "--write-only");
    let read_only = args.iter().any(|a| a == "--read-only");
    let multi_only = args.iter().any(|a| a == "--multi-only");
    let quick = args.iter().any(|a| a == "--quick");
    let fsync_none = args.iter().any(|a| a == "--fsync-none");

    let segment_size_mb: u64 = args
        .windows(2)
        .find(|w| w[0] == "--segment-size")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(64);

    let run_write = !read_only && !multi_only;
    let run_read = !write_only && !multi_only;
    let run_multi = !write_only && !read_only;

    let segment_size_bytes = segment_size_mb * 1024 * 1024;

    let fsync_delay = if fsync_none {
        Duration::ZERO
    } else {
        Duration::from_millis(10)
    };

    println!("==========================================================");
    println!("  Bisque Raft Storage Throughput Benchmark (Mmap)");
    println!("==========================================================");
    println!("  segment_size:   {} MB", segment_size_mb);
    println!(
        "  fsync_delay:    {}",
        if fsync_delay.is_zero() {
            "every write".to_string()
        } else {
            format!("{}ms (batched)", fsync_delay.as_millis())
        }
    );
    if quick {
        println!("  mode:           quick (reduced counts)");
    }
    println!();

    let mut configs = bench_configs();
    if quick {
        for c in &mut configs {
            c.total_entries = (c.total_entries / 10).max(1000);
        }
    }

    // -----------------------------------------------------------------------
    // Write benchmarks
    // -----------------------------------------------------------------------
    if run_write {
        println!("--- Write Throughput (durable, waiting for fsync) ---");
        println!();

        for cfg in &configs {
            let total_bytes = cfg.total_entries * cfg.payload_size as u64;
            println!("  {}", cfg.label);

            let dir = tempfile::tempdir()?;
            match bench_write(
                cfg,
                dir.path().to_path_buf(),
                segment_size_bytes,
                fsync_delay,
            )
            .await
            {
                Ok(elapsed) => print_result("Mmap", cfg.total_entries, total_bytes, elapsed),
                Err(e) => println!("    {:<16} ERROR: {}", "Mmap", e),
            }
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Read benchmarks (sequential)
    // -----------------------------------------------------------------------
    if run_read {
        println!("--- Sequential Read Throughput (cold, no cache) ---");
        println!();

        for cfg in &configs {
            let total_bytes = cfg.total_entries * cfg.payload_size as u64;
            println!("  {}", cfg.label);

            let dir = tempfile::tempdir()?;
            match bench_read_seq(
                cfg,
                dir.path().to_path_buf(),
                segment_size_bytes,
                fsync_delay,
            )
            .await
            {
                Ok(elapsed) => print_result("Mmap", cfg.total_entries, total_bytes, elapsed),
                Err(e) => println!("    {:<16} ERROR: {}", "Mmap", e),
            }
        }
        println!();

        println!("--- Random Read Throughput (cold, no cache) ---");
        println!();

        let random_cfgs: Vec<&BenchConfig> = configs
            .iter()
            .filter(|c| c.batch_size == 1 || (c.payload_size >= 1024 && c.batch_size <= 100))
            .collect();

        for cfg in &random_cfgs {
            let num_reads = cfg.total_entries.min(20_000);
            let total_bytes = num_reads * cfg.payload_size as u64;
            println!("  {}", cfg.label);

            let dir = tempfile::tempdir()?;
            match bench_read_random(
                cfg,
                dir.path().to_path_buf(),
                segment_size_bytes,
                fsync_delay,
            )
            .await
            {
                Ok(elapsed) => print_result("Mmap", num_reads, total_bytes, elapsed),
                Err(e) => println!("    {:<16} ERROR: {}", "Mmap", e),
            }
        }
        println!();
    }

    // -----------------------------------------------------------------------
    // Multi-group benchmarks
    // -----------------------------------------------------------------------
    if run_multi {
        println!("--- Multi-Group Write Throughput ---");
        println!();

        let multi_cfgs: Vec<(u64, usize, u64, u64, &str)> = if quick {
            vec![
                (4, 256, 2_500, 100, "4 groups, 256B, batch=100"),
                (16, 256, 1_000, 100, "16 groups, 256B, batch=100"),
                (64, 256, 500, 100, "64 groups, 256B, batch=100"),
            ]
        } else {
            vec![
                (4, 256, 25_000, 100, "4 groups, 256B, batch=100"),
                (16, 256, 10_000, 100, "16 groups, 256B, batch=100"),
                (64, 256, 5_000, 100, "64 groups, 256B, batch=100"),
                (256, 256, 2_000, 100, "256 groups, 256B, batch=100"),
            ]
        };

        for (num_groups, payload_size, entries_per_group, batch_size, label) in &multi_cfgs {
            let total_entries = num_groups * entries_per_group;
            let total_bytes = total_entries * *payload_size as u64;
            println!("  {}", label);

            let dir = tempfile::tempdir()?;
            match bench_multi_group_write(
                *num_groups,
                *payload_size,
                *entries_per_group,
                *batch_size,
                dir.path().to_path_buf(),
                segment_size_bytes,
                fsync_delay,
            )
            .await
            {
                Ok(elapsed) => print_result("Mmap", total_entries, total_bytes, elapsed),
                Err(e) => println!("    {:<16} ERROR: {}", "Mmap", e),
            }
        }
        println!();
    }

    println!("Done.");
    Ok(())
}
