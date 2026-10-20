//! Reproduces the production shape that pins the serial queueing/reorder
//! worker: many tiny transactions against a wide, heavily-indexed table, so
//! most WAL is btree maintenance and each commit carries ~1 row.
//!
//! Prints the stage rates the daemon exports, so a code change can be scored
//! against `queue_records_out` (the worker's throughput) rather than guessed at.
//!
//! Run with `cargo test --release --test queueing_throughput_e2e -- --ignored --nocapture`.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use walshadow::shadow::Shadow;

const COLUMNS: usize = 38;
const CLIENTS: usize = 16;
const RUN_SECONDS: u64 = 80;
const DRAIN_WARMUP_SECONDS: u64 = 2;
const DRAIN_SECONDS: u64 = 6;

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

fn create_source_table(source: &Shadow) -> Result<()> {
    let cols = wide_columns()
        .iter()
        .map(|c| format!("{c} int4 NOT NULL DEFAULT 0"))
        .collect::<Vec<_>>()
        .join(", ");
    psql_source(
        source,
        &format!(
            "CREATE TABLE steps (id bigserial PRIMARY KEY, {cols}); \
             CREATE INDEX steps_c1 ON steps (c1); \
             CREATE INDEX steps_c2 ON steps (c2); \
             CREATE INDEX steps_c3_c4 ON steps (c3, c4); \
             ALTER TABLE steps REPLICA IDENTITY FULL"
        ),
    )?;
    Ok(())
}

fn ch_table_sql(database: &str) -> String {
    let cols = wide_columns()
        .iter()
        .map(|c| format!("{c} Int32"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE OR REPLACE TABLE {database}.public_steps (\
            id Int64, {cols},\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id"
    )
}

fn write_ch_config(path: &std::path::Path, ch_port: u16, database: &str) -> Result<()> {
    let mut cols = String::from("  { attnum = 1, target = \"id\", type = \"Int64\" },\n");
    for (i, c) in wide_columns().iter().enumerate() {
        cols.push_str(&format!(
            "  {{ attnum = {}, target = \"{c}\", type = \"Int32\" }},\n",
            i + 2
        ));
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
         [table.public.steps]\n\
         columns = [\n{cols}]\n"
    );
    fs::write(path, body).context("write ch-config")?;
    Ok(())
}

/// One INSERT per transaction: the production profile is ~1.5 rows/commit.
fn write_pgbench_script(path: &std::path::Path) -> Result<()> {
    let vals = (1..=COLUMNS)
        .map(|i| format!(":v{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut body = String::new();
    for i in 1..=COLUMNS {
        body.push_str(&format!("\\set v{i} random(1, 1000000)\n"));
    }
    let cols = wide_columns().join(", ");
    body.push_str(&format!("INSERT INTO steps ({cols}) VALUES ({vals});\n"));
    fs::write(path, body).context("write pgbench script")?;
    Ok(())
}

struct Sample {
    at: Instant,
    queue_records_out: u64,
    filter_lsn: u64,
    emitter_rows: u64,
    inserter_batches: u64,
    inserter_ch_ms: u64,
    pump_queue_depth: u64,
    xacts_committed: u64,
    lock_waits: u64,
    lock_wait_ms: u64,
}

fn sample(addr: SocketAddr) -> Result<Sample> {
    let body = fx::http_get(addr, "/metrics")?;
    let get = |n: &str| fx::parse_metric(&body, n).unwrap_or(0);
    let ch_seconds = {
        let mut found = 0u64;
        for line in body.lines() {
            if line.starts_with("walshadow_inserter_ch_seconds_total")
                && let Some((_, v)) = line.rsplit_once(' ')
                && let Ok(f) = v.parse::<f64>()
            {
                found = (f * 1000.0) as u64;
            }
        }
        found
    };
    Ok(Sample {
        at: Instant::now(),
        queue_records_out: get("walshadow_queue_records_out_total"),
        filter_lsn: get("walshadow_filter_lsn"),
        emitter_rows: get("walshadow_emitter_rows_total"),
        inserter_batches: get("walshadow_inserter_batches_in_total"),
        inserter_ch_ms: ch_seconds,
        pump_queue_depth: get("walshadow_pump_queue_depth"),
        xacts_committed: get("walshadow_xacts_committed_total"),
        lock_waits: get("walshadow_decoder_buffer_lock_waits_total"),
        lock_wait_ms: body
            .lines()
            .find(|l| l.starts_with("walshadow_decoder_buffer_lock_wait_seconds_total"))
            .and_then(|l| l.rsplit_once(' '))
            .and_then(|(_, v)| v.parse::<f64>().ok())
            .map(|s| (s * 1000.0) as u64)
            .unwrap_or(0),
    })
}

fn report(a: &Sample, b: &Sample) {
    let secs = b.at.duration_since(a.at).as_secs_f64();
    let per_s = |x: u64, y: u64| (y.saturating_sub(x)) as f64 / secs;
    let inserts = per_s(a.inserter_batches, b.inserter_batches);
    let rows = per_s(a.emitter_rows, b.emitter_rows);
    let ch_busy = per_s(a.inserter_ch_ms, b.inserter_ch_ms) / 1000.0;
    println!("\n  window {secs:.1}s");
    println!(
        "  queue_records_out   {:>12.0} rec/s",
        per_s(a.queue_records_out, b.queue_records_out)
    );
    println!(
        "  filter_lsn          {:>12.1} MiB/s",
        per_s(a.filter_lsn, b.filter_lsn) / 1_048_576.0
    );
    println!(
        "  pg commits          {:>12.0} xact/s",
        per_s(a.xacts_committed, b.xacts_committed)
    );
    println!("  emitter rows        {:>12.0} rows/s", rows);
    println!("  inserts             {:>12.1} /s", inserts);
    if inserts > 0.0 {
        println!("  rows per INSERT     {:>12.0}", rows / inserts);
        println!("  ms per INSERT       {:>12.0}", ch_busy / inserts * 1000.0);
    }
    println!("  ch in-flight        {:>12.2} of 16", ch_busy);
    let waits = per_s(a.lock_waits, b.lock_waits);
    let wait_busy = per_s(a.lock_wait_ms, b.lock_wait_ms) / 1000.0;
    println!("  buffer lock waits   {:>12.0} /s", waits);
    println!(
        "  buffer lock blocked {:>12.3} s/s  <-- 1.0 = one worker fully blocked",
        wait_busy
    );
    if waits > 0.0 {
        println!("  us per acquire      {:>12.1}", wait_busy / waits * 1e6);
    }
    println!(
        "  pump_queue_depth    {:>12} -> {}",
        a.pump_queue_depth, b.pump_queue_depth
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput diagnostic — run with `cargo test --release --test queueing_throughput_e2e -- --ignored --nocapture`"]
async fn queueing_worker_throughput_under_tiny_transactions() {
    if !fx::tools::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let pgext_dir = fx::pgext_dir();

    let source = fx::make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };

    // Backlog outgrows the fixture's 256MB keep, slotless source would recycle it
    psql_source(&source, "ALTER SYSTEM SET wal_keep_size = '8GB'").expect("wal_keep_size");
    psql_source(&source, "SELECT pg_reload_conf()").expect("reload");
    create_source_table(&source).expect("create source table");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ports.ch_tcp, ports.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS default")
        .expect("ch database");
    ch.query(&ch_table_sql("default")).expect("ch dest table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config_path, ports.ch_tcp, "default").expect("write ch-config");

    let script_path = tmp.path().join("insert.sql");
    write_pgbench_script(&script_path).expect("write pgbench script");

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
    let result = (|| -> Result<()> {
        fx::wait_for_listen(metrics_addr, Duration::from_secs(300))
            .context("daemon metrics endpoint never came up")?;

        // Stopped daemon lets WAL pile up on the source, so the drain after
        // SIGCONT runs with the queue pinned: records_out then measures the
        // serial worker, not the source's commit rate
        std::thread::sleep(Duration::from_secs(3));
        let pid = guard.child.as_ref().unwrap().id() as libc::pid_t;
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
                &CLIENTS.to_string(),
                "-j",
                "4",
                "-T",
                &RUN_SECONDS.to_string(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawn pgbench")?;
        unsafe { libc::kill(pid, libc::SIGSTOP) };
        let out = bench.wait_with_output();
        unsafe { libc::kill(pid, libc::SIGCONT) };
        let out = out.context("pgbench wait")?;
        anyhow::ensure!(
            out.status.success(),
            "pgbench failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if line.starts_with("tps") || line.starts_with("number of transactions") {
                println!("  pgbench: {line}");
            }
        }

        std::thread::sleep(Duration::from_secs(DRAIN_WARMUP_SECONDS));
        let first = sample(metrics_addr)?;
        std::thread::sleep(Duration::from_secs(DRAIN_SECONDS));
        let second = sample(metrics_addr)?;
        report(&first, &second);
        let rows = psql_source(&source, "SELECT count(*) FROM steps")?;
        println!("  source rows         {rows:>12}");
        anyhow::ensure!(
            !daemon_stderr().contains("fatal"),
            "daemon logged a fatal: {}",
            daemon_stderr()
        );
        Ok(())
    })();

    if let Err(e) = &result {
        eprintln!("daemon stderr:\n{}", daemon_stderr());
        panic!("{e:?}");
    }
    drop(guard);
    drop(ch);
}
