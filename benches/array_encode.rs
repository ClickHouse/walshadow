//! Built-in array columns encoded in process against shadow conversion.
//!
//! Each shape builds one column of stored `ArrayType` bodies two ways and
//! ends at a ClickHouse column ready for sealing:
//!
//! - `local`: walk bodies into array buffers, then build wrapper columns
//! - `oracle`: queue bodies as `DiskRaw` cells, then one `ENCODE_NATIVE`
//!   round trip through a real shadow Postgres
//!
//! Reports wall time per batch, daemon CPU, and shadow CPU, which local
//! encoding never spends.
//!
//! ```text
//! cargo bench --bench array_encode -- --rows 100000 --iters 8
//! ```
//!
//! Needs `initdb` on PATH and `pgext/walshadow.so` built against that major.
//!
//! `harness = false`, so nextest lists it by answering `--list`.

// Matches `walshadow-stream`, whose allocator is not glibc's
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use clickhouse_c::{Allocator, TypeAst};
use walshadow::ch_emitter::build_array_column;
use walshadow::codecs::{array_elem, array_from_send, decode_jsonb};
use walshadow::oracle::{Oracle, OracleCell, OracleColumnBuf, OracleRequestColumn};
use walshadow::shadow::{BridgeConf, Shadow, ShadowConfig};

/// Port outside integration suites' allocation
const PG_PORT: u16 = 55451;
/// Request payload a batch is sized to, below `ORACLE_BATCH_SEAL_BYTES`
const BATCH_PAYLOAD: usize = 16 << 20;

#[derive(Debug, Parser)]
struct Args {
    /// Rows per batch, lowered for shapes whose payload would pass 16 MiB
    #[arg(long, default_value_t = 100_000)]
    rows: usize,
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..))]
    iters: u32,
    #[arg(long, default_value = "")]
    filter: String,
}

struct Shape {
    name: &'static str,
    array_oid: u32,
    elem_oid: u32,
    target: &'static str,
    /// `typsend` element bodies, `None` for NULL. jsonb elements are stored
    /// binary instead, as no local encoder rebuilds it from text
    elems: Vec<Option<Vec<u8>>>,
}

/// Binary jsonb object of string pairs, keys in storage order
fn jsonb_object(pairs: &[(&str, &str)]) -> Vec<u8> {
    let mut out = (0x2000_0000u32 | pairs.len() as u32).to_le_bytes().to_vec();
    let strings: Vec<&str> = pairs
        .iter()
        .map(|p| p.0)
        .chain(pairs.iter().map(|p| p.1))
        .collect();
    let mut end = 0;
    for (i, s) in strings.iter().enumerate() {
        end += s.len() as u32;
        // First entry holds its end offset, later ones their length
        let entry = if i == 0 {
            0x8000_0000 | end
        } else {
            s.len() as u32
        };
        out.extend_from_slice(&entry.to_le_bytes());
    }
    for s in strings {
        out.extend_from_slice(s.as_bytes());
    }
    out
}

fn shapes() -> Vec<Shape> {
    let int4 = |n: i32| (0..n).map(|i| Some(i.to_be_bytes().to_vec())).collect();
    let mut small: Vec<Option<Vec<u8>>> = int4(8);
    small[3] = None;
    let text = |n: usize, len: usize| {
        (0..n)
            .map(|i| Some(format!("{i:0len$}").into_bytes()))
            .collect()
    };
    vec![
        Shape {
            name: "int4[8] nulls",
            array_oid: 1007,
            elem_oid: 23,
            target: "Array(Nullable(Int32))",
            elems: small,
        },
        Shape {
            name: "int4[1000]",
            array_oid: 1007,
            elem_oid: 23,
            target: "Array(Int32)",
            elems: int4(1000),
        },
        Shape {
            name: "int8[64]",
            array_oid: 1016,
            elem_oid: 20,
            target: "Array(Int64)",
            elems: (0..64i64).map(|i| Some(i.to_be_bytes().to_vec())).collect(),
        },
        Shape {
            name: "text[8] x16B",
            array_oid: 1009,
            elem_oid: 25,
            target: "Array(Nullable(String))",
            elems: text(8, 16),
        },
        Shape {
            name: "text[100] x64B",
            array_oid: 1009,
            elem_oid: 25,
            target: "Array(String)",
            elems: text(100, 64),
        },
        Shape {
            name: "uuid[4]",
            array_oid: 2951,
            elem_oid: 2950,
            target: "Array(UUID)",
            elems: (0..4u8).map(|i| Some(vec![i; 16])).collect(),
        },
        Shape {
            name: "jsonb[8] obj",
            array_oid: 3807,
            elem_oid: 3802,
            target: "Array(Nullable(String))",
            elems: (0..8)
                .map(|i| {
                    let (id, name) = (format!("{i:06}"), format!("{i:016}"));
                    let body = jsonb_object(&[("id", &id), ("name", &name)]);
                    let want = format!("{{\"id\": \"{id}\", \"name\": \"{name}\"}}");
                    assert_eq!(decode_jsonb(&body).unwrap(), want);
                    Some(body)
                })
                .collect(),
        },
        Shape {
            name: "inet[16]",
            array_oid: 1041,
            elem_oid: 869,
            target: "Array(String)",
            elems: (0..16u8)
                .map(|i| Some(vec![2, 32, 0, 4, 10, 0, i, 1]))
                .collect(),
        },
        Shape {
            name: "interval[8]",
            array_oid: 1187,
            elem_oid: 1186,
            target: "Array(String)",
            elems: (0..8i64)
                .map(|i| {
                    let mut v = (i * 3_600_000_000).to_be_bytes().to_vec();
                    v.extend((i as i32).to_be_bytes());
                    v.extend(14i32.to_be_bytes());
                    Some(v)
                })
                .collect(),
        },
    ]
}

/// `array_send` of a one-dimensional array, then its stored body. jsonb
/// elements lay out as text[] varlenas do, under jsonb's element type
fn stored_body(shape: &Shape) -> Vec<u8> {
    let jsonb = shape.elem_oid == 3802;
    let (array_oid, elem_oid) = if jsonb {
        (1009, 25)
    } else {
        (shape.array_oid, shape.elem_oid)
    };
    let has_null = shape.elems.iter().any(Option::is_none);
    let mut send = Vec::new();
    for w in [1, i32::from(has_null), elem_oid as i32] {
        send.extend_from_slice(&w.to_be_bytes());
    }
    send.extend_from_slice(&(shape.elems.len() as i32).to_be_bytes());
    send.extend_from_slice(&1i32.to_be_bytes());
    for e in &shape.elems {
        if let Some(e) = e {
            send.extend_from_slice(&(e.len() as i32).to_be_bytes());
            send.extend_from_slice(e);
        } else {
            send.extend_from_slice(&(-1i32).to_be_bytes());
        }
    }
    let mut body = Vec::new();
    array_from_send(&send, array_elem(array_oid).unwrap(), &mut body).unwrap();
    if jsonb {
        body[8..12].copy_from_slice(&shape.elem_oid.to_le_bytes());
    }
    body
}

/// Nanoseconds on CPU summed over `pid`'s threads
fn cpu_ns(pid: &str) -> u64 {
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return 0;
    };
    tasks
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("schedstat")).ok())
        .filter_map(|s| s.split_whitespace().next()?.parse::<u64>().ok())
        .sum()
}

/// Postmaster and every backend it forked, bridge workers included
fn shadow_cpu_ns(postmaster: &str) -> u64 {
    let children: u64 = std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let pid = e.ok()?.file_name().into_string().ok()?;
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            // Field after `(comm)` state is ppid
            let ppid = stat.rsplit_once(')')?.1.split_whitespace().nth(1)?;
            (ppid == postmaster).then(|| cpu_ns(&pid))
        })
        .sum();
    children + cpu_ns(postmaster)
}

fn postmaster_pid(data: &Path) -> String {
    std::fs::read_to_string(data.join("postmaster.pid"))
        .expect("postmaster.pid")
        .lines()
        .next()
        .expect("postmaster pid line")
        .to_owned()
}

struct Cost {
    wall: Duration,
    daemon_cpu: Duration,
    shadow_cpu: Duration,
}

/// Mean per call over `iters`, after one warmup call
async fn measure<F: AsyncFnMut()>(iters: u32, postmaster: &str, mut f: F) -> Cost {
    f().await;
    let (self_before, shadow_before) = (cpu_ns("self"), shadow_cpu_ns(postmaster));
    let started = Instant::now();
    for _ in 0..iters {
        f().await;
    }
    let wall = started.elapsed() / iters;
    let per = |ns: u64| Duration::from_nanos(ns / u64::from(iters));
    Cost {
        wall,
        daemon_cpu: per(cpu_ns("self") - self_before),
        shadow_cpu: per(shadow_cpu_ns(postmaster).saturating_sub(shadow_before)),
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn tool_ready() -> Option<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pgext");
    if !dir.join("walshadow.so").is_file() {
        println!("skip: pgext/walshadow.so missing (make -C pgext)");
        return None;
    }
    let initdb = Command::new("initdb")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success());
    if !initdb {
        println!("skip: no initdb on PATH");
        return None;
    }
    Some(dir)
}

struct StopOnDrop(Shadow);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.stop();
    }
}

fn start_shadow(tmp: &tempfile::TempDir, lib_dir: PathBuf) -> StopOnDrop {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = PG_PORT;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    let mut bridge = BridgeConf::in_dir(&cfg.socket_dir);
    bridge.library_dir = Some(lib_dir);
    bridge.workers = 1;
    cfg.bridge = Some(bridge);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
    let sh = Shadow::new(cfg);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    StopOnDrop(sh)
}

async fn run(args: &Args) {
    let Some(lib_dir) = tool_ready() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_shadow(&tmp, lib_dir);
    let postmaster = postmaster_pid(&tmp.path().join("data"));
    let socket = guard.0.bridge_socket().expect("bridge configured");
    let bridge = walshadow::bridge::connect_with_budget(socket, 1, Duration::from_secs(30))
        .await
        .expect("bridge connect");
    let oracle = Oracle::new(Arc::new(bridge));
    let alloc = Allocator::global(&mimalloc::MiMalloc);

    println!(
        "{:<16} {:>7} {:>9} | {:>9} {:>9} | {:>9} {:>9} {:>9} | {:>7}",
        "shape",
        "rows",
        "body B",
        "local ms",
        "cpu ms",
        "oracle ms",
        "cpu ms",
        "shadow ms",
        "speedup"
    );
    for shape in shapes() {
        if !shape.name.contains(&args.filter) {
            continue;
        }
        let body = stored_body(&shape);
        let rows = args.rows.min(BATCH_PAYLOAD / body.len()).max(1);
        let elem = array_elem(shape.array_oid).unwrap();
        let ast = TypeAst::parse(shape.target, alloc).unwrap();
        let bodies: Vec<Option<&[u8]>> = vec![Some(&body); rows];

        let local = measure(args.iters, &postmaster, async || {
            let built = build_array_column(elem, &ast, &bodies).unwrap();
            assert_eq!(built, rows);
        })
        .await;
        let oracle = measure(args.iters, &postmaster, async || {
            let mut buf = OracleColumnBuf::new(shape.array_oid, -1, ast.view());
            for b in &bodies {
                buf.push(b.map_or(OracleCell::Default, |b| OracleCell::DiskRaw(b.to_vec())));
            }
            let columns = [OracleRequestColumn {
                ordinal: 0,
                name: "c0",
                target_type: shape.target,
                buf: &buf,
            }];
            let block = oracle
                .encode_batch(Oracle::ANY_DATABASE, &columns, rows, alloc)
                .await
                .expect("oracle answers");
            assert!(block.column(0).is_some());
        })
        .await;
        println!(
            "{:<16} {:>7} {:>9} | {:>9.2} {:>9.2} | {:>9.2} {:>9.2} {:>9.2} | {:>6.1}x",
            shape.name,
            rows,
            body.len(),
            ms(local.wall),
            ms(local.daemon_cpu),
            ms(oracle.wall),
            ms(oracle.daemon_cpu),
            ms(oracle.shadow_cpu),
            oracle.wall.as_secs_f64() / local.wall.as_secs_f64(),
        );
    }
}

fn main() {
    if std::env::args().any(|a| a == "--list") {
        println!("array_encode: benchmark");
        return;
    }
    let args = Args::parse_from(std::env::args().filter(|a| a != "--bench"));
    tokio::runtime::Runtime::new().unwrap().block_on(run(&args));
}
