//! `kill -9` mid-stream + restart drill (v1.0 acceptance §5).
//!
//! Spawns a source PG + ClickHouse server once, then loops three cutoff
//! strategies × five seeded kill windows = 15 daemon spawn/kill/restart
//! cycles. Each cycle:
//!
//!   1. spawn `walshadow-stream` (basebackup-cloned shadow PG already
//!      wired in by `bootstrap_clusters_for_kill`)
//!   2. wait for the metrics endpoint (post-preflight readiness gate)
//!   3. drive a continuous INSERT loop from a tokio task
//!   4. fire the strategy-specific kill trigger
//!   5. SIGKILL the daemon (`std::process::Child::kill()` sends SIGKILL
//!      on Unix)
//!   6. snapshot source's pg_current_wal_lsn + row state
//!   7. restart the daemon with identical flags — same spill dir, same
//!      walsender bind (SO_REUSEADDR), no `--ignore-cursor`
//!   8. poll source + daemon's `walshadow_emitter_ack_lsn` until ack
//!      catches up to the snapshotted LSN
//!   9. assert CH count + sum(id) + md5(string_agg(name, ',' ORDER BY
//!      id)) matches source's
//!
//! `WALSHADOW_KILL_SEED` env seeds the LCG; unset → fixed 0xC11AC11A so
//! CI is reproducible. Per-(strategy, run) seed derivative shifts the
//! 250-750 ms kill window inside each strategy.
//!
//! Skipped silently when `initdb`, `pg_basebackup`, or the `clickhouse`
//! multitool is absent. Linux-only — `Shadow` fixture is POSIX-style.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use walrus::pg::backup::format_pg_lsn;
use walshadow::mapping::TableTarget;
use walshadow::pg::parse_pg_lsn;
use walshadow::schema::RelName;
use walshadow::shadow::{Shadow, ShadowConfig};

// 17360-range — below the Linux ephemeral port range so outbound
// connects can't grab a port we're about to bind. CH's
// `interserver_http_port = http_port + 1` must dodge METRICS / WALSENDER.
const SOURCE_PORT: u16 = 17361;
const SHADOW_PORT: u16 = 17362;
const CH_TCP_PORT: u16 = 17369;
const CH_HTTP_PORT: u16 = 17370;
const METRICS_PORT: u16 = 17375;
const WALSENDER_PORT: u16 = 17376;

/// Fixed seed for CI reproducibility. Operators rotate locally via
/// `WALSHADOW_KILL_SEED=...` to widen coverage.
const DEFAULT_SEED: u64 = 0xC11AC11A;

/// Cutoff strategies — 5 seeded runs each.
#[derive(Clone, Copy, Debug)]
enum Strategy {
    MidSegment,
    MidXact,
    PostCommit,
}

const STRATEGIES: &[Strategy] = &[
    Strategy::MidSegment,
    Strategy::MidXact,
    Strategy::PostCommit,
];

const RUNS_PER_STRATEGY: u32 = 5;

/// Splitmix-style LCG; deterministic, no dep cost.
fn next_seeded(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn make_pg(tmp: &tempfile::TempDir, name: &str, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join(format!("{name}-data")),
        tmp.path().join(format!("{name}-filtered")),
    );
    cfg.port = port;
    cfg.socket_dir = tmp.path().join(format!("{name}-sock"));
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

// Source needs wal_level=logical for the daemon's preflight, plus
// wal_keep_size so the 250 ms kill gap stays inside the slot-less
// retention window (no replication slot in this drill — `--slot` unset).
fn append_source_conf(sh: &Shadow) -> Result<()> {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path)?;
    writeln!(f, "\n# walshadow kill-restart source overrides")?;
    writeln!(f, "wal_level = logical")?;
    writeln!(f, "max_wal_senders = 4")?;
    // Plan §"Risks": pin retention well past 250 ms of WAL so the
    // restart's START_REPLICATION from the cursor LSN sits inside the
    // retained window.
    writeln!(f, "wal_keep_size = '128MB'")?;
    Ok(())
}

fn pg_basebackup(source: &Shadow, dest: &Path) -> Result<()> {
    let cfg = source.config();
    let out = Command::new("pg_basebackup")
        .args([
            "-h",
            cfg.socket_dir.to_str().context("source sock not utf8")?,
            "-p",
            &cfg.port.to_string(),
            "-U",
            "postgres",
            "-D",
            dest.to_str().context("dest not utf8")?,
            "-X",
            "stream",
            "-c",
            "fast",
            "-w",
            "--no-sync",
        ])
        .output()
        .context("spawn pg_basebackup")?;
    if !out.status.success() {
        bail!(
            "pg_basebackup failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

fn rewrite_for_shadow(data_dir: &Path, port: u16, socket_dir: &Path) -> Result<()> {
    let conf = data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
    writeln!(f, "\n# walshadow kill-restart shadow overrides")?;
    writeln!(f, "port = {port}")?;
    writeln!(f, "unix_socket_directories = '{}'", socket_dir.display())?;
    writeln!(f, "listen_addresses = ''")?;
    writeln!(f, "hot_standby = on")?;
    writeln!(f, "autovacuum = off")?;
    writeln!(f, "fsync = off")?;
    writeln!(f, "wal_retrieve_retry_interval = '100ms'")?;
    Ok(())
}

fn enable_recovery(data_dir: &Path, restore_from: &Path, walsender_port: u16) -> Result<()> {
    fs::write(data_dir.join("standby.signal"), b"")?;
    let conf = data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
    writeln!(f, "\n# walshadow kill-restart recovery")?;
    writeln!(
        f,
        "primary_conninfo = 'host=127.0.0.1 port={walsender_port} user=walshadow application_name=shadow sslmode=disable'",
    )?;
    writeln!(f, "restore_command = 'cp {}/%f %p'", restore_from.display())?;
    writeln!(f, "recovery_target_timeline = 'latest'")?;
    Ok(())
}

/// Daemon argv used by every spawn in the drill. Captures every flag
/// that must stay identical across kill / restart so the cursor + spill
/// dir continuity is preserved.
struct DaemonFlags {
    source_sock: PathBuf,
    shadow_sock: PathBuf,
    filter_dir: PathBuf,
    spill_dir: PathBuf,
    metrics_addr: SocketAddr,
    walsender_bind: SocketAddr,
    ch_config: PathBuf,
}

impl DaemonFlags {
    fn args(&self) -> Vec<String> {
        vec![
            "--host".into(),
            self.source_sock.to_string_lossy().into_owned(),
            "--port".into(),
            SOURCE_PORT.to_string(),
            "--user".into(),
            "postgres".into(),
            "--dbname".into(),
            "postgres".into(),
            "--sslmode".into(),
            "disable".into(),
            "--out-dir".into(),
            self.filter_dir.to_string_lossy().into_owned(),
            "--shadow-socket-dir".into(),
            self.shadow_sock.to_string_lossy().into_owned(),
            "--shadow-port".into(),
            SHADOW_PORT.to_string(),
            "--shadow-user".into(),
            "postgres".into(),
            "--shadow-dbname".into(),
            "postgres".into(),
            "--spill-dir".into(),
            self.spill_dir.to_string_lossy().into_owned(),
            "--status-interval".into(),
            "1".into(),
            "--metrics-bind".into(),
            self.metrics_addr.to_string(),
            "--walsender-bind".into(),
            self.walsender_bind.to_string(),
            "--retention-bytes".into(),
            "0".into(),
            "--ch-config".into(),
            self.ch_config.to_string_lossy().into_owned(),
        ]
    }
}

fn spawn_daemon(flags: &DaemonFlags, stderr_path: &Path) -> Result<Child> {
    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_file = fs::File::create(stderr_path).context("open daemon stderr log")?;
    let child = Command::new(bin)
        .args(flags.args())
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .context("spawn walshadow-stream")?;
    Ok(child)
}

/// SIGKILL + reap. `std::process::Child::kill()` sends SIGKILL on Unix.
fn kill_and_reap(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Drive INSERTs into `kr.t (id, name)` from a dedicated task. Each
/// iteration sleeps ~50 ms so the steady-state rate stays in the
/// 5-20 inserts/s band — plenty for the plan's "≥ 5 inserts/s" floor
/// without saturating the daemon.
async fn small_insert_loop(
    source_sock: PathBuf,
    stop: Arc<AtomicBool>,
    next_id: Arc<std::sync::atomic::AtomicI64>,
) {
    while !stop.load(Ordering::Relaxed) {
        let id = next_id.fetch_add(10, Ordering::Relaxed);
        let sock = source_sock.clone();
        let sql = format!(
            "INSERT INTO kr.t SELECT g::int4, 'row-'||g::text FROM generate_series({}, {}) g",
            id,
            id + 9,
        );
        let _ = tokio::task::spawn_blocking(move || {
            let _ = Command::new("psql")
                .args([
                    "-h",
                    sock.to_str().unwrap(),
                    "-p",
                    &SOURCE_PORT.to_string(),
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    &sql,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Run one large `BEGIN; INSERT × 10000; COMMIT` xact. Sized to spill
/// to disk via `XactBuffer`'s largest-first eviction (each row carries
/// a 200-byte payload; 10k rows × ~250 bytes ≈ 2.5 MiB).
async fn large_xact(source_sock: PathBuf, start_id: i64) {
    let sql = format!(
        "BEGIN; \
         INSERT INTO kr.t SELECT g::int4, repeat('x', 200) FROM generate_series({}, {}) g; \
         COMMIT",
        start_id,
        start_id + 9999,
    );
    let _ = tokio::task::spawn_blocking(move || {
        let _ = Command::new("psql")
            .args([
                "-h",
                source_sock.to_str().unwrap(),
                "-p",
                &SOURCE_PORT.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                &sql,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    })
    .await;
}

/// Poll source's row count until it crosses `target`. Bails on deadline.
async fn wait_for_source_rows(source: &Shadow, target: i64, deadline: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        let n: i64 = source
            .psql_one("SELECT count(*) FROM kr.t")?
            .parse()
            .context("parse source count")?;
        if n >= target {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!("source never reached {target} rows in {deadline:?}");
}

/// Poll the metrics endpoint until `walshadow_xacts_committed_total`
/// crosses zero. Used by the `PostCommit` strategy to fire immediately
/// after the first commit drain returns (simpler shape per plan
/// §"Risks").
async fn wait_for_first_commit(metrics_addr: SocketAddr, deadline: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if let Ok(body) = fx::http_get(metrics_addr, "/metrics")
            && let Some(v) = fx::parse_metric(&body, "walshadow_xacts_committed_total")
            && v > 0
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bail!("walshadow_xacts_committed_total never crossed 0 in {deadline:?}");
}

/// Poll source's `pg_current_wal_lsn` + daemon's
/// `walshadow_emitter_ack_lsn` until the latter catches up. Returns
/// the LSN at which the ack landed.
async fn wait_for_ack_catchup(
    source: &Shadow,
    metrics_addr: SocketAddr,
    deadline: Duration,
) -> Result<u64> {
    let start = Instant::now();
    let target_text = source.psql_one("SELECT pg_current_wal_lsn()::text")?;
    let target = parse_pg_lsn(&target_text).context("parse source LSN")?;
    while start.elapsed() < deadline {
        if let Ok(body) = fx::http_get(metrics_addr, "/metrics")
            && let Some(ack) = fx::parse_metric(&body, "walshadow_emitter_ack_lsn")
            && ack >= target
        {
            return Ok(ack);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("emitter_ack_lsn never reached {target:X} in {deadline:?}");
}

fn write_ch_config(ch_config_path: &Path) -> Result<()> {
    fx::write_ch_config_toml(
        ch_config_path,
        "127.0.0.1",
        CH_TCP_PORT,
        "default",
        &RelName::new("kr", "t"),
        &TableTarget::new("default", "kr_t"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_restart_preserves_end_state() {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !fx::pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    if let Err(e) = run_drill().await {
        panic!("kill-restart drill failed: {e:#}");
    }
}

async fn run_drill() -> Result<()> {
    let seed_env = std::env::var("WALSHADOW_KILL_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEED);

    let tmp = tempfile::tempdir()?;

    // 1. Source PG.
    let source = make_pg(&tmp, "source", SOURCE_PORT);
    source.initdb().context("initdb source")?;
    source.write_base_conf().context("source base conf")?;
    append_source_conf(&source).context("append source conf")?;
    source.start().context("start source")?;
    let _src_stop = fx::StopOnDrop { sh: &source };

    // Pre-create the workload table BEFORE basebackup so shadow
    // inherits the same oids — `pg_relation_filenode(oid)` lookups on
    // shadow must agree with the source's WAL records.
    source
        .apply_schema_dump(
            "CREATE SCHEMA kr;\n\
             CREATE TABLE kr.t (id int4 PRIMARY KEY, name text NOT NULL);\n\
             ALTER TABLE kr.t REPLICA IDENTITY FULL;\n",
        )
        .context("apply source schema")?;

    // 2. Basebackup-clone source into shadow data dir + retarget +
    //    standby.signal.
    let shadow_data = tmp.path().join("shadow-data");
    pg_basebackup(&source, &shadow_data).context("pg_basebackup")?;
    source
        .psql_one("SELECT pg_switch_wal()")
        .context("rotate")?;

    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir)?;
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock)?;
    rewrite_for_shadow(&shadow_data, SHADOW_PORT, &shadow_sock).context("retarget shadow")?;
    enable_recovery(&shadow_data, &shadow_filter_dir, WALSENDER_PORT).context("recovery conf")?;

    let mut shadow_cfg = ShadowConfig::new(shadow_data.clone(), shadow_filter_dir.clone());
    shadow_cfg.port = SHADOW_PORT;
    shadow_cfg.socket_dir = shadow_sock.clone();
    shadow_cfg.ctl_timeout = Duration::from_secs(60);
    let shadow = Shadow::new(shadow_cfg);
    shadow.start().context("start shadow standby")?;
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    // 3. CH server + dest table (alive across all 15 daemon cycles).
    let ch_tmp = tempfile::tempdir()?;
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).context("spawn ch")?;
    fx::create_ch_dest_table(&ch, "default", "kr_t").context("create ch dest table")?;

    // 4. Daemon flags — identical across every spawn so kill / restart
    //    resumes from the cursor + spill dir.
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir)?;
    let ch_config_path = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config_path).context("write ch-config")?;
    let flags = DaemonFlags {
        source_sock: source.config().socket_dir.clone(),
        shadow_sock: shadow_sock.clone(),
        filter_dir: shadow_filter_dir.clone(),
        spill_dir: spill_dir.clone(),
        metrics_addr: format!("127.0.0.1:{METRICS_PORT}").parse().unwrap(),
        walsender_bind: format!("127.0.0.1:{WALSENDER_PORT}").parse().unwrap(),
        ch_config: ch_config_path.clone(),
    };

    // Shared id allocator — every workload task pulls a fresh strided
    // window so concurrent inserts don't collide on the PK.
    let next_id = Arc::new(std::sync::atomic::AtomicI64::new(1));

    // 5. Drill loop: each (strategy, run) is one kill/restart cycle.
    let mut seed = seed_env;
    for strategy in STRATEGIES {
        for run in 0..RUNS_PER_STRATEGY {
            let cycle_seed = next_seeded(&mut seed);
            let kill_delay_ms = 250 + (cycle_seed % 500);
            let stderr_path = tmp
                .path()
                .join(format!("daemon.{strategy:?}.{run}.stderr.log"));

            let outcome = run_cycle(
                *strategy,
                run,
                kill_delay_ms,
                &source,
                &ch,
                &flags,
                &stderr_path,
                next_id.clone(),
            )
            .await;

            if let Err(e) = outcome {
                let stderr_blob = fs::read_to_string(&stderr_path).unwrap_or_default();
                bail!(
                    "cycle {strategy:?}#{run} (seed={cycle_seed:#x}, kill_delay={kill_delay_ms}ms): {e:#}\n\
                     --- daemon stderr ---\n{stderr_blob}",
                );
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    strategy: Strategy,
    run: u32,
    kill_delay_ms: u64,
    source: &Shadow,
    ch: &fx::ChServer,
    flags: &DaemonFlags,
    stderr_path: &Path,
    next_id: Arc<std::sync::atomic::AtomicI64>,
) -> Result<()> {
    eprintln!(
        "kill-restart: cycle start strategy={strategy:?} run={run} kill_delay={kill_delay_ms}ms",
    );

    // 1. Spawn daemon, wait for metrics endpoint (post-preflight ack).
    let child = spawn_daemon(flags, stderr_path).context("spawn daemon")?;
    let mut guard = fx::ChildGuard::new(child);
    fx::wait_for_listen(flags.metrics_addr, Duration::from_secs(60))
        .context("daemon metrics endpoint never came up")?;

    // 2. Spin up workload task(s). `stop` flips true after the kill so
    //    the loop drains cleanly before the snapshot.
    let stop = Arc::new(AtomicBool::new(false));
    let source_sock = source.config().socket_dir.clone();
    let small_loop = tokio::spawn(small_insert_loop(
        source_sock.clone(),
        stop.clone(),
        next_id.clone(),
    ));
    // Strategy 2's parallel large xact runs alongside the small loop;
    // its window overlaps the kill_delay naturally since the xact takes
    // ~hundreds of ms to commit.
    let large_handle = if matches!(strategy, Strategy::MidXact) {
        let large_start = next_id.fetch_add(10_000, Ordering::Relaxed);
        Some(tokio::spawn(large_xact(source_sock.clone(), large_start)))
    } else {
        None
    };

    // 3. Wait for ≥ 100 rows visible on source (plan §3).
    wait_for_source_rows(source, 100, Duration::from_secs(30))
        .await
        .context("source never reached 100 rows")?;

    // 4. Strategy-specific kill trigger.
    match strategy {
        Strategy::MidSegment | Strategy::MidXact => {
            tokio::time::sleep(Duration::from_millis(kill_delay_ms)).await;
        }
        Strategy::PostCommit => {
            wait_for_first_commit(flags.metrics_addr, Duration::from_secs(30))
                .await
                .context("first commit drain never happened")?;
        }
    }

    // 5. SIGKILL. ChildGuard owns the std::process::Child; reach in,
    //    detach, then drive the kill explicitly so the guard's Drop
    //    doesn't double-kill the slot.
    let killed = guard
        .child
        .take()
        .ok_or_else(|| anyhow::anyhow!("daemon child already taken"))?;
    kill_and_reap(killed);

    // 6. Stop workload + wait for any large xact to finish (its psql
    //    invocation keeps running on the source even after daemon dies
    //    — the source's slot retention covers it).
    stop.store(true, Ordering::Relaxed);
    let _ = small_loop.await;
    if let Some(h) = large_handle {
        let _ = h.await;
    }

    // 7. Snapshot source's final state. Don't bother capturing rows
    //    here — the oracle reads source directly post-catchup.
    let post_kill_lsn_text = source.psql_one("SELECT pg_current_wal_lsn()::text")?;
    let post_kill_lsn = parse_pg_lsn(&post_kill_lsn_text).context("parse post-kill lsn")?;
    eprintln!(
        "kill-restart: post-kill source lsn={}",
        format_pg_lsn(post_kill_lsn),
    );

    // 8. Restart daemon — same flags. cursor.bin + spill files persist
    //    across the kill; SO_REUSEADDR on --walsender-bind lets the
    //    fresh process re-bind the same port.
    let restart_stderr = stderr_path.with_extension("restart.log");
    let restart_child = spawn_daemon(flags, &restart_stderr).context("restart daemon")?;
    let restart_guard = fx::ChildGuard::new(restart_child);
    fx::wait_for_listen(flags.metrics_addr, Duration::from_secs(60))
        .context("restart daemon metrics endpoint never came up")?;

    // 9. Wait for emitter ack to catch up to source's idle LSN.
    let _ack = wait_for_ack_catchup(source, flags.metrics_addr, Duration::from_secs(90))
        .await
        .context("emitter ack catchup")?;

    // 10. Oracle. Reuses the count + sum + md5 helper from item 9.
    fx::assert_ch_matches_source(ch, source, "kr.t", "default.kr_t")
        .context("source vs CH parity")?;

    // 11. Drain restart daemon for the next cycle. `into_inner` strips
    //     the guard so we drive the SIGKILL + reap explicitly — letting
    //     the std::process::Child drop on its own would leak the
    //     subprocess (Drop doesn't kill).
    if let Some(c) = restart_guard.into_inner() {
        kill_and_reap(c);
    }

    eprintln!("kill-restart: cycle ok strategy={strategy:?} run={run}");
    Ok(())
}
