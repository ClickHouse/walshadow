//! End-to-end drill against the `walshadow-stream` binary.
//!
//! Spawns the daemon as a subprocess pointed at a basebackup-bootstrapped
//! source / shadow PG pair, drives an INSERT/UPDATE/DELETE workload, and
//! asserts shadow replays the workload before the daemon exits via its
//! `--max-segments` cap. Exercises [bin/stream.rs]'s argv parsing,
//! `run()` setup (preflight + tracker seed + ShadowCatalog connect +
//! cursor write + status loop), the metrics endpoint, retention sweeper
//! poll path, and the partial-segment flush on shutdown — paths the
//! pipeline_e2e / bootstrap_*_e2e fixtures don't reach because they
//! re-implement the daemon's sink chain inline rather than driving the binary.
//!
//! Skipped silently when `initdb` or `pg_basebackup` aren't on `$PATH`.

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use walshadow::shadow::{Shadow, ShadowConfig};

// Reserved port slot for this test binary — 56170-range, distinct
// from pipeline_e2e (56100) / bootstrap_*_e2e (56140) so concurrent `cargo test`
// invocations don't trip over each other.
const SOURCE_PORT: u16 = 26171;
const SHADOW_PORT: u16 = 26172;
const METRICS_PORT: u16 = 26173;
const WALSENDER_PORT: u16 = 26174;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn pg_basebackup_available() -> bool {
    Command::new("pg_basebackup")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
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

// Source needs wal_level=logical for the preflight gate +
// max_wal_senders so both pg_basebackup and the daemon's
// START_REPLICATION can attach.
fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow bin_stream_e2e source overrides").unwrap();
    writeln!(f, "wal_level = logical").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
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
    writeln!(f, "\n# walshadow bin_stream_e2e shadow overrides")?;
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
    writeln!(f, "\n# walshadow bin_stream_e2e recovery")?;
    writeln!(
        f,
        "primary_conninfo = 'host=127.0.0.1 port={walsender_port} user=walshadow application_name=shadow sslmode=disable'",
    )?;
    writeln!(f, "restore_command = 'cp {}/%f %p'", restore_from.display())?;
    writeln!(f, "recovery_target_timeline = 'latest'")?;
    Ok(())
}

/// Poll a TCP listener until accept succeeds or the deadline expires.
/// Used as a coarse "daemon finished init" gate via its metrics bind.
fn wait_for_listen(addr: SocketAddr, deadline: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    bail!("nothing listening on {addr} after {deadline:?}");
}

/// Issue a minimal HTTP/1.0 GET against the daemon's metrics endpoint
/// and return the response body. Hand-rolled rather than pulling a
/// reqwest-class dep into dev-deps — the endpoint is a 2xx-only
/// localhost server.
fn http_get(addr: SocketAddr, path: &str) -> Result<String> {
    let mut sock = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .with_context(|| format!("connect {addr}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(sock, "GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf)?;
    let txt = String::from_utf8_lossy(&buf).into_owned();
    let body_start = txt.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
    Ok(txt[body_start..].to_string())
}

/// Parse one Prom gauge/counter value from a /metrics body.
fn metric_u64(body: &str, name: &str) -> Result<u64> {
    body.lines()
        .find_map(|l| l.strip_prefix(name).map(str::trim))
        .and_then(|v| v.parse().ok())
        .with_context(|| format!("{name} not in metrics body"))
}

/// Wait for a child to exit, polling every 100 ms up to `deadline`.
/// Returns the exit status on success; kills + reaps the child on
/// timeout so a stuck daemon doesn't outlive the test.
fn wait_with_timeout(child: &mut Child, deadline: Duration) -> Result<std::process::ExitStatus> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        match child.try_wait()? {
            Some(s) => return Ok(s),
            None => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    bail!("walshadow-stream did not exit within {deadline:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bin_stream_replicates_segments_and_serves_metrics() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();

    // 1. Source PG, schema before basebackup so shadow inherits the
    //    same oids/filenodes the daemon's tracker seeds against.
    let source = make_pg(&tmp, "source", SOURCE_PORT);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    append_source_conf(&source);
    source.start().expect("start source");
    let _src_stop = StopOnDrop { sh: &source };

    source
        .apply_schema_dump(
            "CREATE SCHEMA bs;\n\
             CREATE TABLE bs.t (id bigint PRIMARY KEY, payload text);\n\
             ALTER TABLE bs.t REPLICA IDENTITY FULL;\n",
        )
        .expect("apply source schema");

    // 2. pg_basebackup -> shadow data dir. Retarget + standby.signal +
    //    restore_command so shadow boots into recovery against the
    //    daemon's --out-dir.
    let shadow_data = tmp.path().join("shadow-data");
    pg_basebackup(&source, &shadow_data).expect("pg_basebackup");
    source.psql_one("SELECT pg_switch_wal()").expect("rotate");

    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    rewrite_for_shadow(&shadow_data, SHADOW_PORT, &shadow_sock).expect("retarget shadow conf");
    enable_recovery(&shadow_data, &shadow_filter_dir, WALSENDER_PORT)
        .expect("enable shadow recovery");

    let mut shadow_cfg = ShadowConfig::new(shadow_data.clone(), shadow_filter_dir.clone());
    shadow_cfg.port = SHADOW_PORT;
    shadow_cfg.socket_dir = shadow_sock.clone();
    shadow_cfg.ctl_timeout = Duration::from_secs(60);
    let shadow = Shadow::new(shadow_cfg);
    if let Err(e) = shadow.start() {
        let log = fs::read_to_string(shadow_data.join("startup.log"))
            .unwrap_or_else(|_| "<no startup.log>".into());
        panic!("start shadow standby failed: {e}\nstartup.log:\n{log}");
    }
    let _shd_stop = StopOnDrop { sh: &shadow };
    assert!(
        shadow.is_in_recovery().expect("probe in-recovery"),
        "shadow must boot in recovery",
    );

    // 3. Spawn walshadow-stream. `--max-segments=1` makes the daemon
    //    exit cleanly after the workload's pg_switch_wal seals a
    //    segment; `--metrics-bind` doubles as a readiness probe.
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();
    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{METRICS_PORT}").parse().unwrap();
    let mut child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &SOURCE_PORT.to_string(),
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
            &SHADOW_PORT.to_string(),
            "--shadow-user",
            "postgres",
            "--shadow-dbname",
            "postgres",
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--max-segments",
            "1",
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{WALSENDER_PORT}"),
            // Retention disabled — no shadow_replay sweeper churn
            // racing the test's max-segments exit. Default would
            // poll shadow on a 60s cadence; we'd never observe it.
            "--retention-bytes",
            "0",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");

    let mut daemon_killed = false;
    let result = (|| -> Result<()> {
        // 4. Wait for the daemon's metrics endpoint — post-preflight,
        //    post-shadow-connect, before the WAL pump's main loop.
        wait_for_listen(metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        // 5. Scrape /metrics. Exercises metrics::serve + handle_client
        //    end-to-end. Body should mention at least one of the
        //    well-known walshadow_ counters.
        let body = http_get(metrics_addr, "/metrics").context("metrics scrape")?;
        assert!(
            body.contains("walshadow_source_received_lsn") || body.contains("walshadow_uptime"),
            "expected walshadow_* counter in /metrics body: {body}",
        );
        // Shadow apply-lag surface is part of the static
        // render shape (zero-valued before any shadow attaches).
        for name in [
            "walshadow_shadow_apply_lag_bytes",
            "walshadow_shadow_apply_lag_seconds",
            "walshadow_shadow_stream_active_connections",
            "walshadow_shadow_stream_dropped_connections_total",
        ] {
            assert!(
                body.contains(name),
                "expected {name} in /metrics body: {body}",
            );
        }
        let ack_at_boot = metric_u64(&body, "walshadow_emitter_ack_lsn")
            .context("emitter_ack gauge in first scrape")?;

        // 6. Drive workload. The daemon filters user-heap WAL records
        //    (rmgr=HEAP, class=User → replaced with XLOG_NOOP) — only
        //    catalog records ship to shadow. So the assertion target is
        //    DDL: CREATE TABLE bs.t2 must materialise on shadow after
        //    replay, while bs.t's INSERTs stay invisible there (their
        //    destination is the CH emitter, exercised by pipeline_e2e).
        //    Autocommit per `-c` keeps each commit in the same segment
        //    as its records; pg_switch_wal seals the work.
        let driver_sock = source.config().socket_dir.clone();
        let out = Command::new("psql")
            .args([
                "-h",
                driver_sock.to_str().unwrap(),
                "-p",
                &SOURCE_PORT.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "CREATE TABLE bs.t2 (id int PRIMARY KEY, payload text)",
                "-c",
                "INSERT INTO bs.t SELECT g, repeat('x', g)::text FROM generate_series(1, 5) g",
                "-c",
                "SELECT pg_switch_wal()",
            ])
            .output()
            .context("spawn workload psql")?;
        if !out.status.success() {
            bail!(
                "workload psql failed: {}",
                String::from_utf8_lossy(&out.stderr),
            );
        }

        // 6b. Null-tail watermark: the metrics-only pipeline's contiguous
        //     ack must advance past the workload's commits — routed-nothing
        //     seqs complete at placement, so a stalled gauge means the
        //     degenerate tail wedged.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let body = http_get(metrics_addr, "/metrics").context("ack poll scrape")?;
            let ack = metric_u64(&body, "walshadow_emitter_ack_lsn")
                .context("emitter_ack gauge in poll scrape")?;
            if ack > ack_at_boot {
                break;
            }
            if std::time::Instant::now() > deadline {
                bail!("emitter_ack_lsn never advanced past boot value {ack_at_boot}");
            }
            std::thread::sleep(Duration::from_millis(200));
        }

        // 7. Wait for the daemon to hit `--max-segments=1` and exit
        //    cleanly. 60s budget covers basebackup retry + status-tick
        //    cadence on slow CI.
        let status =
            wait_with_timeout(&mut child, Duration::from_secs(60)).context("daemon exit")?;
        daemon_killed = true;
        assert!(
            status.success(),
            "daemon exit status: {status:?}\nstderr:\n{}\nshadow startup.log:\n{}",
            fs::read_to_string(&stderr_path).unwrap_or_default(),
            fs::read_to_string(shadow_data.join("startup.log")).unwrap_or_default(),
        );

        // 8. Wait for shadow to replay past the source's final LSN.
        //    The daemon flushed the partial segment + sealed one full
        //    segment via pg_switch_wal; shadow's restore_command poll
        //    catches up on the 100ms cadence we configured.
        let target_text = source
            .psql_one("SELECT pg_current_wal_lsn()::text")
            .expect("source lsn");
        let target = walshadow::pg::parse_pg_lsn(&target_text).expect("parse target lsn");
        let observed = shadow
            .wait_for_replay(target, Duration::from_secs(30))
            .expect("shadow replay catches up");
        assert!(
            observed >= target,
            "shadow replay {observed:X} < target {target:X}",
        );

        // 9a. Catalog mirroring: bs.t2 was created during the
        //     workload — DDL records pass through the filter, shadow
        //     replays them, the table must exist on both sides with
        //     matching column count.
        let src_t2_cols = source
            .psql_one(
                "SELECT count(*)::text FROM information_schema.columns \
                 WHERE table_schema='bs' AND table_name='t2'",
            )
            .expect("source t2 cols");
        let shd_t2_cols = shadow
            .psql_one(
                "SELECT count(*)::text FROM information_schema.columns \
                 WHERE table_schema='bs' AND table_name='t2'",
            )
            .expect("shadow t2 cols");
        assert_eq!(src_t2_cols, "2", "source: bs.t2 should have 2 columns");
        assert_eq!(
            shd_t2_cols, src_t2_cols,
            "shadow catalog did not mirror the CREATE TABLE",
        );

        // 9b. Heap filtering: bs.t's INSERTs do NOT replicate to shadow
        //     (rmgr=HEAP, class=User → XLOG_NOOP). Source has 5 rows;
        //     shadow has 0. Without this assertion we'd miss a
        //     regression that accidentally widens the filter to keep
        //     user-heap records.
        let src_rows = source
            .psql_one("SELECT count(*)::text FROM bs.t")
            .expect("source bs.t count");
        let shd_rows = shadow
            .psql_one("SELECT count(*)::text FROM bs.t")
            .expect("shadow bs.t count");
        assert_eq!(src_rows, "5", "source: bs.t should have 5 inserted rows");
        assert_eq!(
            shd_rows, "0",
            "shadow's bs.t must stay empty — daemon filter should drop user-heap WAL",
        );

        // 10. Daemon side-effects: manifest + at least one filtered
        //     segment file landed under --out-dir.
        assert!(
            spill_dir.join("manifest.toml").exists(),
            "manifest.toml should be written before clean exit",
        );
        let seg_files: Vec<_> = fs::read_dir(&shadow_filter_dir)
            .expect("read filter dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            // 24-hex-char filenames are sealed WAL segments;
            // `.partial` is the in-progress one flushed on shutdown.
            .filter(|n| n.len() == 24 && n.chars().all(|c| c.is_ascii_hexdigit()))
            .collect();
        assert!(
            !seg_files.is_empty(),
            "no sealed segments under {}: {:?}",
            shadow_filter_dir.display(),
            fs::read_dir(&shadow_filter_dir)
                .unwrap()
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect::<Vec<_>>(),
        );

        Ok(())
    })();

    if !daemon_killed {
        let _ = child.kill();
        let _ = child.wait();
    }
    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

// Distinct port slot for the wire-drop test so it can run concurrently.
const WD_SOURCE_PORT: u16 = 26181;
const WD_SHADOW_PORT: u16 = 26182;
const WD_METRICS_PORT: u16 = 26183;
const WD_WALSENDER_PORT: u16 = 26184;

/// `kill -<sig> -<pgid>` on the shadow cluster's process group, read from
/// `postmaster.pid` + `/proc/<pid>/stat` (field 5 = pgrp). Pausing the *group*
/// (not just the postmaster) stops the walreceiver child too, so it stops
/// draining walshadow's wire and the send queue backs up.
/// Kill the shadow's walreceiver (SIGTERM its backend pid). The startup process
/// restarts it within `wal_retrieve_retry_interval`; with the source advancing
/// it reconnects requesting an LSN *behind* the live head — the reconnect-gap
/// the fix must backfill. Retries briefly in case we catch it between restarts.
fn kill_walreceiver(shadow: &Shadow) -> Result<()> {
    for _ in 0..50 {
        let pid = shadow
            .psql_one(
                "SELECT pid::text FROM pg_stat_activity WHERE backend_type='walreceiver' LIMIT 1",
            )
            .unwrap_or_default();
        let pid = pid.trim();
        if !pid.is_empty() {
            let _ = Command::new("kill").arg(pid).status();
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }
    bail!("no walreceiver backend appeared to kill")
}

/// Background WAL generator on the source: a `psql` loop of small inserts into
/// the no-PK `bs.load`, so the wire keeps flowing for the test's duration. Runs
/// as its own process group; killed via [`kill_group`].
fn spawn_writer(socket_dir: &Path) -> Result<Child> {
    // Deliberately slow (~130 KiB/s): enough to advance the head during the 2s
    // reconnect window (the gap), but far too slow to *complete* a 16 MiB
    // segment within the test — so a stranded shadow can't quietly recover via
    // restore_command (which only serves complete segments).
    let script = format!(
        "while :; do psql -h '{}' -p {} -U postgres -d postgres -q -t \
         -c \"INSERT INTO bs.load SELECT repeat('x',100) FROM generate_series(1,200)\" \
         >/dev/null 2>&1; sleep 0.2; done",
        socket_dir.display(),
        WD_SOURCE_PORT,
    );
    Command::new("bash")
        .arg("-c")
        .arg(script)
        .process_group(0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn background writer")
}

fn kill_group(child: &mut Child) {
    let pid = child.id() as i32;
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(format!("-{pid}"))
        .status();
    let _ = child.wait();
}

/// Reproduction / validation harness for the WAL reconnect-gap strand.
///
/// A background writer keeps WAL flowing so the shadow streams the in-progress
/// segment live. We then kill the shadow's walreceiver; it reconnects requesting
/// an LSN behind the now-advanced head (inside the in-progress segment).
///
/// Passes only with the fix: walshadow backfills `[reconnect_lsn, head]` so the
/// stream is contiguous. Without it the reconnect gets a hole, the shadow
/// strands (`restore_command` lacks the incomplete segment), replay never
/// catches up → fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real-PG e2e; run with --ignored. Validates fix-2 (wire reconnect/resume)."]
async fn wire_drop_midsegment_shadow_resumes_streaming() {
    if !pg_available() || !pg_basebackup_available() {
        eprintln!("skip: PG binaries not on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source = make_pg(&tmp, "wd-source", WD_SOURCE_PORT);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    append_source_conf(&source);
    source.start().expect("start source");
    let _src_stop = StopOnDrop { sh: &source };
    source
        .apply_schema_dump(
            "CREATE SCHEMA bs;\n\
             CREATE TABLE bs.t (id bigint PRIMARY KEY, payload text);\n\
             CREATE TABLE bs.load (payload text);\n",
        )
        .expect("apply source schema");

    let shadow_data = tmp.path().join("wd-shadow-data");
    pg_basebackup(&source, &shadow_data).expect("pg_basebackup");
    source.psql_one("SELECT pg_switch_wal()").expect("rotate");

    let filter_dir = tmp.path().join("wd-filtered");
    fs::create_dir_all(&filter_dir).unwrap();
    let shadow_sock = tmp.path().join("wd-shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    rewrite_for_shadow(&shadow_data, WD_SHADOW_PORT, &shadow_sock).expect("retarget shadow");
    enable_recovery(&shadow_data, &filter_dir, WD_WALSENDER_PORT).expect("enable recovery");
    // Slow the walreceiver restart so killing it leaves a multi-second window in
    // which the writer advances the head — guaranteeing the reconnect lands
    // behind it (the gap). Overrides the 100ms from rewrite_for_shadow.
    {
        let conf = shadow_data.join("postgresql.conf");
        let mut f = fs::OpenOptions::new().append(true).open(&conf).unwrap();
        writeln!(f, "wal_retrieve_retry_interval = '2s'").unwrap();
    }

    let mut shadow_cfg = ShadowConfig::new(shadow_data.clone(), filter_dir.clone());
    shadow_cfg.port = WD_SHADOW_PORT;
    shadow_cfg.socket_dir = shadow_sock.clone();
    shadow_cfg.ctl_timeout = Duration::from_secs(60);
    let shadow = Shadow::new(shadow_cfg);
    shadow.start().expect("start shadow");
    let _shd_stop = StopOnDrop { sh: &shadow };

    let spill_dir = tmp.path().join("wd-spill");
    fs::create_dir_all(&spill_dir).unwrap();
    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("wd-daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).unwrap();
    let metrics_addr: SocketAddr = format!("127.0.0.1:{WD_METRICS_PORT}").parse().unwrap();
    let mut child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &WD_SOURCE_PORT.to_string(),
            "--user",
            "postgres",
            "--dbname",
            "postgres",
            "--sslmode",
            "disable",
            "--out-dir",
            filter_dir.to_str().unwrap(),
            "--shadow-socket-dir",
            shadow_sock.to_str().unwrap(),
            "--shadow-port",
            &WD_SHADOW_PORT.to_string(),
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
            &format!("127.0.0.1:{WD_WALSENDER_PORT}"),
            // Default (large) threshold: we force the disconnect by killing the
            // walreceiver, not by overflowing the queue — so baseline streaming
            // never trips a spurious drop.
            "--retention-bytes",
            "0",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");

    let mut writer: Option<Child> = None;
    let result = (|| -> Result<()> {
        wait_for_listen(metrics_addr, Duration::from_secs(30)).context("daemon never came up")?;

        // Continuous WAL so the wire stays active (a lone write goes idle and
        // its trailing record never streams). Started now — shadow is attached
        // at the pump head, so there's no startup backlog to overflow the queue.
        writer = Some(spawn_writer(source.config().socket_dir.as_path()).context("spawn writer")?);

        // Baseline: the live wire keeps the shadow replaying the in-progress segment.
        sleep(Duration::from_secs(1));
        let m = walshadow::pg::parse_pg_lsn(
            &source
                .psql_one("SELECT pg_current_wal_lsn()::text")
                .context("baseline lsn")?,
        )
        .unwrap();
        shadow
            .wait_for_replay(m, Duration::from_secs(25))
            .context("baseline replay over live wire")?;

        // Force a mid-stream reconnect: kill the walreceiver. The writer keeps
        // advancing the head during the ~100ms restart, so it reconnects behind
        // the live head — inside the in-progress segment.
        kill_walreceiver(&shadow).context("kill walreceiver")?;
        // Let the 2s restart window elapse (writer advances the head meanwhile)
        // so the walreceiver reconnects behind it before we check recovery.
        sleep(Duration::from_secs(3));

        // Recovery: replay must reach a post-reconnect LSN — only possible if
        // walshadow backfills the gap on reconnect. Without the fix the shadow
        // gets a hole, strands, and this times out. Budget < the 30s gate.
        let target = walshadow::pg::parse_pg_lsn(
            &source
                .psql_one("SELECT pg_current_wal_lsn()::text")
                .context("target lsn")?,
        )
        .unwrap();
        let observed = shadow
            .wait_for_replay(target, Duration::from_secs(25))
            .context("shadow did not resume streaming after wire drop (strand reproduced)")?;
        if observed < target {
            bail!("replay {observed:X} < target {target:X} after wire drop");
        }

        // A strand would have killed the daemon via the fatal replay-timeout.
        if let Some(status) = child.try_wait().context("poll daemon")? {
            bail!("daemon exited during the wire drop: {status:?}");
        }
        Ok(())
    })();

    if let Some(mut w) = writer {
        kill_group(&mut w);
    }
    let _ = child.kill();
    let _ = child.wait();
    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        let slog = fs::read_to_string(shadow_data.join("startup.log")).unwrap_or_default();
        let tail = &slog[slog.len().saturating_sub(2048)..];
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}\n--- shadow startup.log tail ---\n{tail}");
    }
}
