//! Shadow-TOAST cost under pgbench churn of pointer-only updates and value
//! replacements on toasted rows. Daemon is release-built under `cargo bench`
//!
//! Live mode streams while pgbench runs. Reports source throughput, how far
//! horizon pin trails source head, dead row versions VACUUM keeps right after
//! churn, and values lost to reclamation
//!
//! Drain mode builds a backlog of `--drain` transactions while daemon is down,
//! then times catch-up into ClickHouse with pin on. Steady rate excludes
//! daemon startup. `--metrics` prints worker, TOAST and bridge counters
//!
//! ```text
//! cargo bench --bench shadow_toast_horizon -- --secs 10 --reps 3 --pin both
//! cargo bench --bench shadow_toast_horizon -- --drain 160000 --read-ahead off
//! ```
//!
//! Needs PostgreSQL tools, pgbench and ClickHouse on PATH, plus
//! `pgext/walshadow.so` built against that major
//!
//! `harness = false`, so nextest lists it by answering `--list`

#[path = "../tests/common/horizon_fixture.rs"]
mod hz;

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use hz::{ROWS, Rig, SCHEMA, fx, slot_lag, toast_dead_tuples};

const USAGE: &str = "usage: shadow_toast_horizon [--secs N] [--reps N] [--pin on|off|both] \
                     [--read-ahead on|off] [--drain TXNS] [--metrics]";

struct Opts {
    secs: u32,
    reps: u32,
    pins: Vec<bool>,
    read_ahead: bool,
    drain: Option<u32>,
    metrics: bool,
}

/// Pointer-only update then value replacement per transaction, 8 KB of
/// uncompressed chunks each
fn churn_script(path: &Path) -> Result<()> {
    let sql = format!(
        "\\set k random(1, {ROWS})\n\
         UPDATE {SCHEMA}.t SET meta = 'p' || :k WHERE id = :k;\n\
         UPDATE {SCHEMA}.t SET body = repeat(md5(random()::text), 250) WHERE id = :k;\n"
    );
    Ok(fs::write(path, sql)?)
}

/// pgbench over churn script, `limit` is `-T` seconds or `-t` transactions
fn pgbench(rig: &Rig, limit: [&str; 2]) -> Result<Command> {
    let script = rig.tmp.path().join("churn.sql");
    churn_script(&script)?;
    let cfg = rig.source.config();
    let mut cmd = Command::new("pgbench");
    cmd.args(["-h", cfg.socket_dir.to_str().unwrap(), "-p"])
        .arg(cfg.port.to_string())
        .args(["-U", &cfg.user, "-n", "-c", "4"])
        .args(limit)
        .arg("-f")
        .arg(&script)
        .arg(&cfg.dbname);
    Ok(cmd)
}

fn rig(pin: bool, read_ahead: bool) -> Option<Rig> {
    let rig = Rig::new(pin)?;
    let cfg = fs::read_to_string(&rig.config).unwrap();
    let cfg = cfg.replace("read_ahead = false", &format!("read_ahead = {read_ahead}"));
    fs::write(&rig.config, cfg).unwrap();
    Some(rig)
}

/// Prints figures, asserts only that pinned values survive
fn churn(o: &Opts, pin: bool) {
    let Some(rig) = rig(pin, o.read_ahead) else {
        return;
    };
    let guard = fx::ChildGuard::new(rig.spawn().expect("spawn walshadow-stream"));
    let result = (|| -> Result<()> {
        rig.wait_bootstrapped()?;
        let mut bench = pgbench(&rig, ["-T", &o.secs.to_string()])?
            .stdout(Stdio::piped())
            .spawn()
            .context("spawn pgbench")?;
        let (mut ages, mut wal_max) = (Vec::new(), 0);
        while bench.try_wait()?.is_none() {
            let (age, wal) = slot_lag(&rig.source)?;
            ages.push(age);
            wal_max = wal_max.max(wal);
            std::thread::sleep(Duration::from_millis(250));
        }
        let out = bench.wait_with_output()?;
        ensure!(out.status.success(), "pgbench failed");
        let report = String::from_utf8_lossy(&out.stdout);
        let txns: u64 = report
            .lines()
            .find_map(|l| l.strip_prefix("number of transactions actually processed: "))
            .and_then(|n| n.split('/').next())
            .context("pgbench transaction count")?
            .parse()?;
        rig.source.psql_one(&format!("VACUUM {SCHEMA}.t"))?;
        let toast_dead = toast_dead_tuples(&rig.source)?;
        fx::wait_for_ch_value(
            &rig.ch,
            "SELECT count() FROM default.t",
            &(u64::from(ROWS) + 2 * txns).to_string(),
            Duration::from_secs(60),
        )?;
        let superseded = rig.superseded()?;
        ages.sort_unstable();
        let p50 = ages.get(ages.len() / 2).copied().unwrap_or(0);
        let max = ages.last().copied().unwrap_or(0);
        eprintln!(
            "pin={pin} read_ahead={} xacts={txns} xid/s={} pin_age_xids p50={p50} max={max} \
             slot_wal_max={wal_max} toast_dead_after_vacuum={toast_dead} superseded={superseded}",
            o.read_ahead,
            txns / u64::from(o.secs),
        );
        report_metrics(&rig, o.metrics)?;
        ensure!(!pin || superseded == 0, "pinned values lost: {superseded}");
        Ok(())
    })();
    fx::finish_daemon(guard, &rig.daemon, result);
}

fn drain(o: &Opts, txns: u32) {
    let Some(rig) = rig(true, o.read_ahead) else {
        return;
    };
    let mut guard = fx::ChildGuard::new(rig.spawn().expect("spawn walshadow-stream"));
    let result = (|| -> Result<()> {
        rig.wait_bootstrapped()?;
        if let Some(mut first) = guard.child.take() {
            fx::tools::stop_gracefully(&mut first);
        }
        let per_client = txns / 4;
        let st = pgbench(&rig, ["-t", &per_client.to_string()])?
            .stdout(Stdio::null())
            .status()?;
        ensure!(st.success(), "pgbench failed");
        let wal: u64 = rig
            .source
            .psql_one(&format!(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint \
                 FROM pg_replication_slots WHERE slot_name = '{}'",
                hz::SLOT
            ))?
            .trim()
            .parse()?;
        let want = u64::from(ROWS) + 2 * 4 * u64::from(per_client);
        let t0 = Instant::now();
        guard.child = Some(rig.spawn()?);
        // First rows past bootstrap mark end of daemon startup
        let mut first = None;
        loop {
            let n: u64 = rig
                .ch
                .query("SELECT count() FROM default.t")
                .ok()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            if first.is_none() && n > u64::from(ROWS) {
                first = Some((t0.elapsed().as_secs_f64(), n));
            }
            if n >= want {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(300),
                "drain stuck at {n}/{want}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        let secs = t0.elapsed().as_secs_f64();
        let (start, at_start) = first.context("no rows drained")?;
        eprintln!(
            "drain read_ahead={} rows={want} wal_mb={} secs={secs:.2} start={start:.2} \
             steady_rows/s={:.0} superseded={}",
            o.read_ahead,
            wal >> 20,
            (want - at_start) as f64 / (secs - start),
            rig.superseded()?,
        );
        report_metrics(&rig, o.metrics)
    })();
    fx::finish_daemon(guard, &rig.daemon, result);
}

fn report_metrics(rig: &Rig, on: bool) -> Result<()> {
    if !on {
        return Ok(());
    }
    let metrics = fx::http_get(rig.daemon.metrics_addr, "/metrics")?;
    let shown = [
        "worker_seconds_total",
        "toast_value",
        "bridge_request",
        "bridge_errors",
    ];
    for l in metrics.lines().filter(|l| {
        !l.starts_with('#') && shown.iter().any(|s| l.contains(s)) && !l.ends_with(" 0")
    }) {
        eprintln!("  {l}");
    }
    Ok(())
}

fn on_off(v: &str) -> bool {
    match v {
        "on" => true,
        "off" => false,
        _ => panic!("{USAGE}"),
    }
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    // nextest --all-targets lists benches by running them with --list
    if argv.iter().any(|a| a == "--list") {
        println!("shadow_toast_horizon: benchmark");
        return;
    }
    let mut o = Opts {
        secs: 10,
        reps: 1,
        pins: vec![true, false],
        read_ahead: true,
        drain: None,
        metrics: false,
    };
    let mut args = argv.iter().filter(|a| *a != "--bench");
    while let Some(flag) = args.next() {
        if flag == "--metrics" {
            o.metrics = true;
            continue;
        }
        let value = args.next().map(String::as_str).expect(USAGE);
        let n = || value.parse().expect(USAGE);
        match flag.as_str() {
            "--secs" => o.secs = n(),
            "--reps" => o.reps = n(),
            "--drain" => o.drain = Some(n()),
            "--read-ahead" => o.read_ahead = on_off(value),
            "--pin" if value == "both" => o.pins = vec![true, false],
            "--pin" => o.pins = vec![on_off(value)],
            _ => panic!("{USAGE}"),
        }
    }
    for _ in 0..o.reps {
        if let Some(txns) = o.drain {
            drain(&o, txns);
            continue;
        }
        for &pin in &o.pins {
            churn(&o, pin);
        }
    }
}
