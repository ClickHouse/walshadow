//! Drain ceiling for a write-heavy WAL mix: one wide table with five btree
//! indexes, two of them partial, and a transaction of one insert plus two
//! non-HOT updates. The daemon is stopped while the load runs, so the drain
//! after SIGCONT runs against a fixed backlog and measures walshadow's
//! ceiling for that mix rather than the source's commit rate.
//!
//! Reports drain MiB/s and rec/s over the whole backlog, CPU nanoseconds per
//! record for each of the daemon's busiest threads, and the btree share of
//! records. Threads are reported by `comm`, not guessed at by role.
//!
//! Knobs (env): `RUNS` (default 4), `CLIENTS` (32), `RUN_SECONDS` (10),
//! `SEED_ROWS` (200000), `CHECKPOINT_S` (unset = standard; a value issues
//! CHECKPOINT on that period for the duration of the load), `TOAST_BYTES`
//! (0 = off; a value adds an EXTERNAL-storage payload column seeded to that
//! size, so the updates carry pointers whose chunks only the store holds).
//!
//! Run with `cargo test --release --test drain_ceiling_e2e -- --ignored --nocapture`.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use walshadow::shadow::Shadow;

const COLUMNS: usize = 38;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn psql_source(source: &Shadow, sql: &str) -> Result<String> {
    let out = Command::new("psql")
        .args([
            "-h",
            source.config().socket_dir.to_str().unwrap(),
            "-p",
            &source.config().port.to_string(),
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-tAXq",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ])
        .output()
        .context("spawn psql")?;
    if !out.status.success() {
        anyhow::bail!(
            "psql failed: {sql}\nstderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn wide_columns() -> Vec<String> {
    (1..=COLUMNS).map(|i| format!("c{i}")).collect()
}

fn create_source_table(source: &Shadow, seed_rows: usize, toast_bytes: usize) -> Result<()> {
    let cols = wide_columns()
        .iter()
        .map(|c| format!("{c} int4 NOT NULL DEFAULT 0"))
        .collect::<Vec<_>>()
        .join(", ");
    let payload = if toast_bytes > 0 {
        ", payload text"
    } else {
        ""
    };
    psql_source(
        source,
        &format!(
            "CREATE TABLE steps (id bigserial PRIMARY KEY, {cols}{payload}); \
             CREATE INDEX steps_c1 ON steps (c1); \
             CREATE INDEX steps_c2 ON steps (c2); \
             CREATE INDEX steps_c3_c4 ON steps (c3, c4); \
             CREATE INDEX steps_c5_hi ON steps (c5) WHERE c5 > 500000; \
             CREATE INDEX steps_c6_even ON steps (c6) WHERE c6 % 2 = 0; \
             ALTER TABLE steps REPLICA IDENTITY FULL"
        ),
    )?;
    let mut vals = (1..=COLUMNS)
        .map(|i| format!("(g * {i}) % 1000000"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut seeded = wide_columns().join(", ");
    if toast_bytes > 0 {
        // EXTERNAL keeps it uncompressed, so the value really goes out of line
        // and the seeding xact owns its chunks. Later updates leave payload
        // untouched, so their new tuples carry a pointer whose chunks are in
        // no live xact map: that is what forces the store fetch
        psql_source(
            source,
            "ALTER TABLE steps ALTER COLUMN payload SET STORAGE EXTERNAL",
        )?;
        seeded.push_str(", payload");
        vals.push_str(&format!(", repeat(md5(g::text), {})", toast_bytes / 32 + 1));
    }
    psql_source(
        source,
        &format!(
            "INSERT INTO steps ({seeded}) \
             SELECT {vals} FROM generate_series(1, {seed_rows}) g"
        ),
    )?;
    psql_source(source, "VACUUM ANALYZE steps")?;
    Ok(())
}

fn ch_table_sql(database: &str, toast_bytes: usize) -> String {
    let mut cols = wide_columns()
        .iter()
        .map(|c| format!("{c} Int32"))
        .collect::<Vec<_>>()
        .join(", ");
    if toast_bytes > 0 {
        cols.push_str(", payload String");
    }
    format!(
        "CREATE OR REPLACE TABLE {database}.public_steps (\
            id Int64, {cols},\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id"
    )
}

fn write_ch_config(
    path: &std::path::Path,
    ch_port: u16,
    database: &str,
    toast_bytes: usize,
) -> Result<()> {
    let mut cols = String::from("  { attnum = 1, target = \"id\", type = \"Int64\" },\n");
    for (i, c) in wide_columns().iter().enumerate() {
        cols.push_str(&format!(
            "  {{ attnum = {}, target = \"{c}\", type = \"Int32\" }},\n",
            i + 2
        ));
    }
    let mut toast = String::new();
    if toast_bytes > 0 {
        cols.push_str(&format!(
            "  {{ attnum = {}, target = \"payload\", type = \"String\" }},\n",
            COLUMNS + 2
        ));
        toast.push_str("[toast]\nmode = \"shadow\"\n\n");
    }
    let body = format!(
        "[ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"{database}\"\n\
         compression = \"none\"\n\
         inserter_pool_size = 16\n\
         decoder_pool_size = 16\n\
         flush_timeout_ms = 1000\n\
         \n\
         {toast}\
         [table.public.steps]\n\
         columns = [\n{cols}]\n"
    );
    fs::write(path, body).context("write ch-config")?;
    Ok(())
}

/// Updates touch indexed columns so they cannot take the HOT path, which is
/// what puts the btree maintenance into the WAL this bench measures
fn write_pgbench_script(path: &std::path::Path, seed_rows: usize) -> Result<()> {
    let vals = (1..=COLUMNS)
        .map(|i| format!(":v{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut body = String::new();
    for i in 1..=COLUMNS {
        body.push_str(&format!("\\set v{i} random(1, 1000000)\n"));
    }
    body.push_str(&format!("\\set u1 random(1, {seed_rows})\n"));
    body.push_str(&format!("\\set u2 random(1, {seed_rows})\n"));
    let cols = wide_columns().join(", ");
    body.push_str("BEGIN;\n");
    body.push_str(&format!("INSERT INTO steps ({cols}) VALUES ({vals});\n"));
    body.push_str("UPDATE steps SET c1 = :v1, c5 = :v5 WHERE id = :u1;\n");
    body.push_str("UPDATE steps SET c2 = :v2, c6 = :v6 WHERE id = :u2;\n");
    body.push_str("END;\n");
    fs::write(path, body).context("write pgbench script")?;
    Ok(())
}

struct Sample {
    at: Instant,
    records: u64,
    filter_lsn: u64,
    queue_depth: u64,
    btree: u64,
    total_classified: u64,
    toast_batches: u64,
    toast_values: u64,
    toast_nanos: u64,
    bridge_replay_lsn: u64,
    bridge_fetch_toast: u64,
    cpu_ns: HashMap<String, u64>,
}

fn thread_cpu_ns(pid: libc::pid_t) -> HashMap<String, u64> {
    let tick_ns = 1_000_000_000u64 / 100;
    let mut out: HashMap<String, u64> = HashMap::new();
    let Ok(dir) = fs::read_dir(format!("/proc/{pid}/task")) else {
        return out;
    };
    for tid in dir.flatten() {
        let path = tid.path();
        let Ok(stat) = fs::read_to_string(path.join("stat")) else {
            continue;
        };
        // comm can contain spaces and parens, so split on the last ')'
        let Some((head, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        let comm = head
            .split_once('(')
            .map(|(_, c)| c.to_string())
            .unwrap_or_default();
        let f: Vec<&str> = rest.split_whitespace().collect();
        let (Some(ut), Some(st)) = (f.get(11), f.get(12)) else {
            continue;
        };
        let ticks = ut.parse::<u64>().unwrap_or(0) + st.parse::<u64>().unwrap_or(0);
        *out.entry(comm).or_default() += ticks * tick_ns;
    }
    out
}

fn bridge_op(body: &str, op: &str) -> u64 {
    let needle = format!("op=\"{op}\"");
    body.lines()
        .filter(|l| l.starts_with("walshadow_bridge_requests_total"))
        .filter(|l| l.contains(&needle))
        .filter_map(|l| l.rsplit_once(' ').and_then(|(_, v)| v.parse::<u64>().ok()))
        .sum()
}

fn sample(addr: SocketAddr, pid: libc::pid_t) -> Result<Sample> {
    let body = fx::http_get(addr, "/metrics")?;
    let get = |n: &str| fx::parse_metric(&body, n).unwrap_or(0);
    let by_rmgr = |want: &str| -> u64 {
        let needle = format!("rmgr=\"{}\"", want.to_ascii_lowercase());
        body.lines()
            .filter(|l| l.starts_with("walshadow_filter_records_total"))
            .filter(|l| l.to_ascii_lowercase().contains(&needle))
            .filter_map(|l| l.rsplit_once(' ').and_then(|(_, v)| v.parse::<u64>().ok()))
            .sum()
    };
    let total_classified = body
        .lines()
        .filter(|l| l.starts_with("walshadow_filter_records_total"))
        .filter_map(|l| l.rsplit_once(' ').and_then(|(_, v)| v.parse::<u64>().ok()))
        .sum();
    Ok(Sample {
        at: Instant::now(),
        records: get("walshadow_queue_records_out_total"),
        filter_lsn: get("walshadow_filter_lsn"),
        queue_depth: get("walshadow_pump_queue_depth"),
        btree: by_rmgr("btree"),
        total_classified,
        toast_batches: get("walshadow_toast_value_fetch_batches_total"),
        toast_values: get("walshadow_toast_values_fetched_total"),
        toast_nanos: body
            .lines()
            .find(|l| l.starts_with("walshadow_toast_value_fetch_seconds_total"))
            .and_then(|l| l.rsplit_once(' '))
            .and_then(|(_, v)| v.parse::<f64>().ok())
            .map(|s| (s * 1e9) as u64)
            .unwrap_or(0),
        bridge_replay_lsn: bridge_op(&body, "replay_lsn"),
        bridge_fetch_toast: bridge_op(&body, "fetch_toast"),
        cpu_ns: thread_cpu_ns(pid),
    })
}

struct RunResult {
    mib_s: f64,
    rec_s: f64,
    toast_batches_s: f64,
    toast_values_per_batch: f64,
    toast_fetch_s_per_s: f64,
    replay_lsn_s: f64,
    fetch_toast_s: f64,
    cpu_ns_per_rec: Vec<(String, f64)>,
    btree_share: f64,
    backlog_mib: f64,
    drain_s: f64,
}

fn spread(label: &str, unit: &str, xs: &[f64]) {
    if xs.is_empty() {
        return;
    }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = v.iter().sum::<f64>() / v.len() as f64;
    let prec = if mean < 100.0 { 1 } else { 0 };
    let each = v
        .iter()
        .map(|x| format!("{x:.prec$}"))
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "  {label:<18} {:>8.prec$} to {:<8.prec$} (mean {mean:.prec$}) {unit}   [{each}]",
        v[0],
        v[v.len() - 1]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "drain ceiling benchmark — run with `cargo test --release --test drain_ceiling_e2e -- --ignored --nocapture`"]
async fn drain_ceiling_for_write_heavy_mix() {
    if !fx::tools::requirements_available() {
        return;
    }
    let runs = env_usize("RUNS", 4);
    let clients = env_usize("CLIENTS", 32);
    let run_seconds = env_usize("RUN_SECONDS", 10);
    let seed_rows = env_usize("SEED_ROWS", 200_000);
    let toast_bytes = env_usize("TOAST_BYTES", 0);
    let checkpoint_s = std::env::var("CHECKPOINT_S")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());

    println!(
        "\n  runs={runs} clients={clients} run_seconds={run_seconds} seed_rows={seed_rows} \
         checkpoint={} toast_bytes={toast_bytes}",
        checkpoint_s.map_or("standard".into(), |s| format!("{s}s"))
    );

    let ports = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let pgext_dir = fx::pgext_dir();

    let source = fx::make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };

    psql_source(&source, "ALTER SYSTEM SET wal_keep_size = '16GB'").expect("wal_keep_size");
    psql_source(&source, "SELECT pg_reload_conf()").expect("reload");
    create_source_table(&source, seed_rows, toast_bytes).expect("create source table");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ports.ch_tcp, ports.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS default")
        .expect("ch database");
    ch.query(&ch_table_sql("default", toast_bytes))
        .expect("ch dest table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config_path, ports.ch_tcp, "default", toast_bytes)
        .expect("write ch-config");

    let script_path = tmp.path().join("mix.sql");
    write_pgbench_script(&script_path, seed_rows).expect("write pgbench script");

    let bootstrap_shadow_data_dir = tmp.path().join("shadow-data");
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();

    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{}", ports.metrics).parse().unwrap();
    let child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &source.config().port.to_string(),
            "--user",
            "postgres",
            "--dbname",
            "postgres",
            "--sslmode",
            "disable",
            "--out-dir",
            shadow_filter_dir.to_str().unwrap(),
            "--shadow-socket-dir",
            shadow_sock.to_str().unwrap(),
            "--shadow-port",
            &ports.shadow.to_string(),
            "--shadow-user",
            "postgres",
            "--shadow-dbname",
            "postgres",
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{}", ports.walsender),
            "--retention-bytes",
            "0",
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-shadow-replay-timeout",
            "180",
            "--bridge-lib-dir",
            pgext_dir.to_str().unwrap(),
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let daemon_stderr = || fs::read_to_string(&stderr_path).unwrap_or_default();
    let result = (|| -> Result<Vec<RunResult>> {
        fx::wait_for_listen(metrics_addr, Duration::from_secs(300))
            .context("daemon metrics endpoint never came up")?;
        let pid = guard.child.as_ref().unwrap().id() as libc::pid_t;
        std::thread::sleep(Duration::from_secs(3));

        let mut results = Vec::new();
        for run in 1..=runs {
            let wal_before: u64 = psql_source(
                &source,
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint",
            )?
            .parse()
            .unwrap_or(0);

            let stop_ck = Arc::new(AtomicBool::new(false));
            let ck_thread = checkpoint_s.map(|period| {
                let flag = stop_ck.clone();
                let dir = source.config().socket_dir.clone();
                let port = source.config().port;
                std::thread::spawn(move || {
                    while !flag.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_secs(period));
                        if flag.load(Ordering::Relaxed) {
                            break;
                        }
                        let _ = Command::new("psql")
                            .args([
                                "-h",
                                dir.to_str().unwrap(),
                                "-p",
                                &port.to_string(),
                                "-U",
                                "postgres",
                                "-d",
                                "postgres",
                                "-tAXq",
                                "-c",
                                "CHECKPOINT",
                            ])
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .status();
                    }
                })
            });

            let bench = Command::new("pgbench")
                .args([
                    "-h",
                    source.config().socket_dir.to_str().unwrap(),
                    "-p",
                    &source.config().port.to_string(),
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-n",
                    "-f",
                    script_path.to_str().unwrap(),
                    "-c",
                    &clients.to_string(),
                    "-j",
                    "8",
                    "-T",
                    &run_seconds.to_string(),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .context("spawn pgbench")?;
            unsafe { libc::kill(pid, libc::SIGSTOP) };
            let out = bench.wait_with_output();
            stop_ck.store(true, Ordering::Relaxed);

            let wal_after: u64 = psql_source(
                &source,
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint",
            )
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);

            unsafe { libc::kill(pid, libc::SIGCONT) };
            let out = out.context("pgbench wait")?;
            anyhow::ensure!(
                out.status.success(),
                "pgbench failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            if let Some(t) = ck_thread {
                let _ = t.join();
            }
            let before = sample(metrics_addr, pid)?;

            let mut last = before.records;
            let mut idle = 0;
            let after = loop {
                std::thread::sleep(Duration::from_millis(250));
                let s = sample(metrics_addr, pid)?;
                if s.records == last && s.queue_depth == 0 {
                    idle += 1;
                    if idle >= 6 {
                        break s;
                    }
                } else {
                    idle = 0;
                }
                last = s.records;
                if s.at.duration_since(before.at) > Duration::from_secs(300) {
                    break s;
                }
            };

            let drain_s = after.at.duration_since(before.at).as_secs_f64();
            let bytes = after.filter_lsn.saturating_sub(before.filter_lsn) as f64;
            let recs = after.records.saturating_sub(before.records) as f64;
            let classified = after
                .total_classified
                .saturating_sub(before.total_classified) as f64;
            let btree = after.btree.saturating_sub(before.btree) as f64;
            let mut cpu: Vec<(String, f64)> = after
                .cpu_ns
                .iter()
                .map(|(k, v)| {
                    let base = before.cpu_ns.get(k).copied().unwrap_or(0);
                    (k.clone(), v.saturating_sub(base) as f64)
                })
                .filter(|(_, ns)| *ns > 0.0)
                .map(|(k, ns)| (k, if recs > 0.0 { ns / recs } else { 0.0 }))
                .collect();
            cpu.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            cpu.truncate(5);

            println!(
                "  run {run}/{runs}: backlog {:.0} MiB, drained in {drain_s:.1}s",
                (wal_after.saturating_sub(wal_before)) as f64 / 1_048_576.0
            );
            let d = |a: u64, b: u64| (b.saturating_sub(a)) as f64;
            let tb = d(before.toast_batches, after.toast_batches);
            results.push(RunResult {
                mib_s: bytes / 1_048_576.0 / drain_s,
                rec_s: recs / drain_s,
                toast_batches_s: tb / drain_s,
                toast_values_per_batch: if tb > 0.0 {
                    d(before.toast_values, after.toast_values) / tb
                } else {
                    0.0
                },
                toast_fetch_s_per_s: d(before.toast_nanos, after.toast_nanos) / 1e9 / drain_s,
                replay_lsn_s: d(before.bridge_replay_lsn, after.bridge_replay_lsn) / drain_s,
                fetch_toast_s: d(before.bridge_fetch_toast, after.bridge_fetch_toast) / drain_s,
                cpu_ns_per_rec: cpu,
                btree_share: if classified > 0.0 {
                    btree / classified * 100.0
                } else {
                    0.0
                },
                backlog_mib: (wal_after.saturating_sub(wal_before)) as f64 / 1_048_576.0,
                drain_s,
            });
        }

        anyhow::ensure!(
            !daemon_stderr().contains("fatal"),
            "daemon logged a fatal: {}",
            daemon_stderr()
        );
        Ok(results)
    })();

    match &result {
        Err(e) => {
            eprintln!("daemon stderr:\n{}", daemon_stderr());
            drop(guard);
            drop(ch);
            panic!("{e:?}");
        }
        Ok(results) => {
            println!("\n  === drain ceiling, {} runs ===", results.len());
            spread(
                "drain",
                "MiB/s",
                &results.iter().map(|r| r.mib_s).collect::<Vec<_>>(),
            );
            spread(
                "drain",
                "rec/s",
                &results.iter().map(|r| r.rec_s).collect::<Vec<_>>(),
            );
            spread(
                "backlog",
                "MiB",
                &results.iter().map(|r| r.backlog_mib).collect::<Vec<_>>(),
            );
            spread(
                "drain time",
                "s",
                &results.iter().map(|r| r.drain_s).collect::<Vec<_>>(),
            );
            spread(
                "btree share",
                "%",
                &results.iter().map(|r| r.btree_share).collect::<Vec<_>>(),
            );
            spread(
                "toast batches/s",
                "",
                &results.iter().map(|r| r.toast_batches_s).collect::<Vec<_>>(),
            );
            spread(
                "toast vals/batch",
                "",
                &results
                    .iter()
                    .map(|r| r.toast_values_per_batch)
                    .collect::<Vec<_>>(),
            );
            spread(
                "toast fetch s/s",
                "",
                &results
                    .iter()
                    .map(|r| r.toast_fetch_s_per_s)
                    .collect::<Vec<_>>(),
            );
            spread(
                "bridge replay_lsn/s",
                "",
                &results.iter().map(|r| r.replay_lsn_s).collect::<Vec<_>>(),
            );
            spread(
                "bridge fetch_toast/s",
                "",
                &results.iter().map(|r| r.fetch_toast_s).collect::<Vec<_>>(),
            );
            let mut names: Vec<String> = results
                .iter()
                .flat_map(|r| r.cpu_ns_per_rec.iter().map(|(n, _)| n.clone()))
                .collect();
            names.sort();
            names.dedup();
            println!("\n  CPU ns per record, by thread");
            for n in names {
                let xs: Vec<f64> = results
                    .iter()
                    .filter_map(|r| {
                        r.cpu_ns_per_rec
                            .iter()
                            .find(|(k, _)| *k == n)
                            .map(|(_, v)| *v)
                    })
                    .collect();
                spread(&n, "ns/rec", &xs);
            }
            if let Some(bytes_per_rec) = results
                .first()
                .filter(|r| r.rec_s > 0.0)
                .map(|r| r.mib_s * 1_048_576.0 / r.rec_s)
            {
                println!("\n  {bytes_per_rec:.0} bytes per record");
            }
        }
    }
    drop(guard);
    drop(ch);
}
