//! Oracle batch cost, by stage.
//!
//! - `frame`: the two payload-sized memory passes a request used to make, no
//!   Postgres. One is the request copy into a second buffer the bridge
//!   framed; the other is the zeroing pass a fresh response buffer pays
//!   before `read_exact` overwrites it. Both scale with the batch, and
//!   `ORACLE_BATCH_SEAL_BYTES` puts that at 32 MiB
//! - `roundtrip`: a real shadow Postgres and a real `ENCODE_NATIVE`. Reports
//!   what one batch costs end to end, alone and with the bridge pool
//!   saturated, so the memory passes above have something to be a fraction
//!   of. Also prices a column of `Literal` cells — bytes the daemon already
//!   rendered — against building the same ClickHouse column locally
//!
//! ```text
//! cargo bench --bench oracle_batch -- --stage frame --bytes 33554432
//! cargo bench --bench oracle_batch -- --stage roundtrip --rows 100000 --workers 4
//! ```
//!
//! `roundtrip` needs `initdb` on PATH and `pgext/walshadow.so` built against
//! that major; it says so and skips otherwise.
//!
//! `harness = false`, so nextest lists it by answering `--list`.

// Matches `walshadow-stream`, whose allocator is not glibc's. Request
// buffers here are allocated and freed at 32 MiB, which is exactly where
// allocators differ most
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse_c::{Allocator, ColumnBuilder};
use walshadow::bridge::Bridge;
use walshadow::oracle::{Oracle, OracleCell, OracleColumnBuf, OracleRequestColumn};
use walshadow::schema::NUMERICOID;
use walshadow::shadow::{BridgeConf, Shadow, ShadowConfig};

/// Port outside the integration suites' allocation
const PG_PORT: u16 = 55450;
/// Short-form numeric for 42: header 0x8000, one base-10000 digit
fn numeric_42() -> Vec<u8> {
    let mut out = 0x8000u16.to_le_bytes().to_vec();
    out.extend_from_slice(&42i16.to_le_bytes());
    out
}

/// WKT a PostGIS 2-D point renders to, ie a `Literal` cell
const WKT: &[u8] = b"POINT(-73.987654 40.748817)";

struct Args {
    stage: String,
    bytes: usize,
    rows: usize,
    workers: usize,
    iters: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            stage: "all".into(),
            bytes: 32 << 20,
            rows: 100_000,
            workers: 4,
            iters: 8,
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: --stage frame|roundtrip|all [--bytes N] [--rows N] [--workers N] [--iters N]"
    );
    std::process::exit(2);
}

/// Mean over `iters`, minus a warmup pass whose cost is first-touch
fn timed(iters: usize, mut f: impl FnMut()) -> Duration {
    f();
    let started = Instant::now();
    for _ in 0..iters {
        f();
    }
    started.elapsed() / iters as u32
}

fn mib_per_s(bytes: usize, d: Duration) -> f64 {
    bytes as f64 / (1 << 20) as f64 / d.as_secs_f64()
}

/// The two payload-sized passes a batch no longer makes
fn bench_frame(args: &Args) {
    let n = args.bytes;
    let src = vec![0xa5u8; n];

    // Request side, before: payload into its own buffer, then a second
    // buffer of 4 + len the whole payload is copied into so one write_all
    // ships it. After: cells go straight past a reserved prefix, so neither
    // the allocation nor the copy happens
    let copy = timed(args.iters, || {
        let mut frame = Vec::with_capacity(5 + n);
        frame.extend_from_slice(&[0u8; 5]);
        frame.extend_from_slice(&src);
        std::hint::black_box(frame.len());
    });

    // Response side: `vec![0u8; len]` zeroes bytes read_exact then
    // overwrites. A recycled buffer read into its spare capacity pays
    // neither the allocation nor the zeroing
    let fresh = timed(args.iters, || {
        let mut v = vec![0u8; n];
        v.copy_from_slice(&src);
        std::hint::black_box(v.len());
    });
    let mut scratch: Vec<u8> = Vec::with_capacity(n);
    let recycled = timed(args.iters, || {
        scratch.clear();
        scratch.extend_from_slice(&src);
        std::hint::black_box(scratch.len());
    });

    println!(
        "frame stage, {:.0} MiB payload",
        n as f64 / (1 << 20) as f64
    );
    println!(
        "  request second buffer + copy   {:>8.3} ms  ({:.0} MiB/s)",
        copy.as_secs_f64() * 1e3,
        mib_per_s(n, copy),
    );
    println!(
        "  response vec![0u8; len] + fill {:>8.3} ms  ({:.0} MiB/s)",
        fresh.as_secs_f64() * 1e3,
        mib_per_s(n, fresh),
    );
    println!(
        "  response recycled + fill       {:>8.3} ms  ({:.0} MiB/s)",
        recycled.as_secs_f64() * 1e3,
        mib_per_s(n, recycled),
    );
    println!(
        "  zeroing pass alone             {:>8.3} ms",
        (fresh.as_secs_f64() - recycled.as_secs_f64()) * 1e3,
    );
}

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn pgext_dir() -> Option<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pgext");
    dir.join("walshadow.so").is_file().then_some(dir)
}

struct StopOnDrop {
    sh: Shadow,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

fn start_shadow(tmp: &tempfile::TempDir, lib_dir: PathBuf, workers: usize) -> StopOnDrop {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = PG_PORT;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    let mut bridge = BridgeConf::in_dir(&cfg.socket_dir);
    bridge.library_dir = Some(lib_dir);
    bridge.workers = workers;
    cfg.bridge = Some(bridge);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
    let sh = Shadow::new(cfg);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    StopOnDrop { sh }
}

fn cells(oid: u32, body: &[u8], rows: usize, literal: bool) -> OracleColumnBuf {
    let mut buf = OracleColumnBuf::new(oid, -1);
    for _ in 0..rows {
        buf.push(if literal {
            OracleCell::Literal(body.to_vec())
        } else {
            OracleCell::DiskRaw(body.to_vec())
        });
    }
    buf
}

async fn one_batch(oracle: &Oracle, buf: &OracleColumnBuf, rows: usize) -> Duration {
    let columns = [OracleRequestColumn {
        ordinal: 0,
        name: "c0",
        target_type: "String",
        buf,
    }];
    let started = Instant::now();
    let block = oracle
        .encode_batch(&columns, rows, Allocator::stdlib())
        .await
        .expect("oracle answers");
    let elapsed = started.elapsed();
    assert_eq!(
        block
            .column(0)
            .and_then(|c| c.string())
            .expect("string")
            .0
            .len(),
        rows,
    );
    elapsed
}

/// Mean batch latency, `iters` batches in flight `concurrency` at a time
async fn batches(
    oracle: Arc<Oracle>,
    buf: Arc<OracleColumnBuf>,
    rows: usize,
    iters: usize,
    concurrency: usize,
) -> (Duration, Duration) {
    let started = Instant::now();
    let mut sum = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let wave = concurrency.min(left);
        let mut set = Vec::new();
        for _ in 0..wave {
            let oracle = oracle.clone();
            let buf = buf.clone();
            set.push(tokio::spawn(
                async move { one_batch(&oracle, &buf, rows).await },
            ));
        }
        for h in set {
            sum += h.await.unwrap();
        }
        left -= wave;
    }
    (sum / iters as u32, started.elapsed() / iters as u32)
}

/// `ColumnBuilder::string` over the same cells, ie what a pure-`Literal`
/// column would cost if it never left the daemon
fn local_string_column(rows: usize, body: &[u8]) -> Duration {
    let iters = 8;
    timed(iters, || {
        let mut offsets: Vec<u64> = Vec::with_capacity(rows);
        let mut data: Vec<u8> = Vec::with_capacity(rows * body.len());
        for _ in 0..rows {
            data.extend_from_slice(body);
            offsets.push(data.len() as u64);
        }
        let col = ColumnBuilder::string(&offsets, &data, rows).expect("string column");
        std::hint::black_box(col.n_rows());
    })
}

async fn bench_roundtrip(args: &Args) {
    let Some(lib_dir) = pgext_dir() else {
        println!("roundtrip stage: skip, pgext/walshadow.so missing (make -C pgext)");
        return;
    };
    if !pg_available() {
        println!("roundtrip stage: skip, no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_shadow(&tmp, lib_dir, args.workers);
    let socket = guard.sh.bridge_socket().expect("bridge configured");

    let disk = Arc::new(cells(NUMERICOID, &numeric_42(), args.rows, false));
    let literal = Arc::new(cells(NUMERICOID, WKT, args.rows, true));
    let req_bytes = |b: &OracleColumnBuf| b.approx_size();

    for workers in [1, args.workers] {
        let bridge = Arc::new(
            walshadow::bridge::connect_with_budget(socket, workers, Duration::from_secs(30))
                .await
                .expect("bridge connect"),
        );
        let oracle = Arc::new(Oracle::new(bridge.clone()));
        let serial = batches(oracle.clone(), disk.clone(), args.rows, args.iters, 1).await;
        let parallel = batches(
            oracle.clone(),
            disk.clone(),
            args.rows,
            args.iters,
            workers.max(1),
        )
        .await;
        println!(
            "roundtrip stage, {} DiskRaw numeric cells ({:.1} MiB request), {} sockets",
            args.rows,
            req_bytes(&disk) as f64 / (1 << 20) as f64,
            bridge.pool_size(),
        );
        println!(
            "  one batch at a time   {:>8.2} ms/batch",
            serial.1.as_secs_f64() * 1e3,
        );
        println!(
            "  {:>2} in flight          {:>8.2} ms/batch wall, {:.2} ms latency",
            workers,
            parallel.1.as_secs_f64() * 1e3,
            parallel.0.as_secs_f64() * 1e3,
        );
    }

    // Cells the daemon already rendered, which the resolver now builds
    // locally: this is what shipping them used to cost
    let bridge = Arc::new(
        walshadow::bridge::connect_with_budget(socket, 1, Duration::from_secs(30))
            .await
            .expect("bridge connect"),
    );
    let oracle = Arc::new(Oracle::new(bridge));
    let shipped = batches(oracle, literal.clone(), args.rows, args.iters, 1).await;
    let built = local_string_column(args.rows, WKT);
    println!(
        "literal column, {} cells ({:.1} MiB)",
        args.rows,
        req_bytes(&literal) as f64 / (1 << 20) as f64,
    );
    println!(
        "  through the oracle    {:>8.2} ms/batch",
        shipped.1.as_secs_f64() * 1e3,
    );
    println!(
        "  built locally         {:>8.2} ms/batch",
        built.as_secs_f64() * 1e3,
    );
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    // nextest --all-targets lists benches by running them with --list
    if argv.iter().any(|a| a == "--list") {
        println!("oracle_batch: benchmark");
        return;
    }
    let mut args = Args::default();
    let mut i = 0;
    while i < argv.len() {
        let val = |s: &Option<&String>| -> usize {
            s.and_then(|v| v.parse().ok()).unwrap_or_else(|| usage())
        };
        let next = argv.get(i + 1);
        match argv[i].as_str() {
            "--stage" => args.stage = next.cloned().unwrap_or_else(|| usage()),
            "--bytes" => args.bytes = val(&next).max(1),
            "--rows" => args.rows = val(&next).max(1),
            "--workers" => args.workers = val(&next).clamp(1, 8),
            "--iters" => args.iters = val(&next).max(1),
            // cargo bench passes --bench through
            "--bench" => {
                i += 1;
                continue;
            }
            _ => usage(),
        }
        i += 2;
    }

    if args.stage == "frame" || args.stage == "all" {
        bench_frame(&args);
    }
    if args.stage == "roundtrip" || args.stage == "all" {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(bench_roundtrip(&args));
    }
}

/// Silences the unused-import lint when only `frame` is compiled in
#[allow(dead_code)]
fn _bridge_type_is_used(_: &Bridge) {}
