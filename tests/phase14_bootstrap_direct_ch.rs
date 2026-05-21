//! Phase 14 — bootstrap + CH end-to-end via the direct
//! replication-protocol BASE_BACKUP source.
//!
//! Closes the gap left by `phase12_direct_e2e.rs`, which exercised the
//! direct bootstrap pipeline against a `RecordingObserver` (no live
//! CH). This drill runs the real daemon binary
//! (`target/debug/walshadow-stream`) with
//! `--bootstrap-mode=direct --bootstrap-autospawn-shadow --ch-config`
//! against a self-hosted source PG + spawned `clickhouse server`,
//! then verifies the bootstrap rows land in CH end-to-end.
//!
//! Pipeline:
//!
//! ```text
//! Shadow(source).start()
//!   → schema + INSERT s14.t (64 rows) + CHECKPOINT + pg_switch_wal
//!   → walshadow-stream (subprocess)
//!         → run_bootstrap (DirectSource BASE_BACKUP → MultiplexSink)
//!         → drain_backfill → transitional Emitter → CH default.t
//!         → autospawn shadow PG against bootstrap_shadow_data_dir
//!         → ShadowCatalog connect + preflight + WAL pump
//!   → assert_ch_matches_source(ch, source, "s14.t", "default.t")
//! ```
//!
//! Skipped silently when `initdb`, `pg_basebackup`, or the `clickhouse`
//! multitool is absent. Linux-only — `Shadow` fixture targets
//! POSIX-style data dirs and the daemon uses unix sockets.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use walshadow::shadow::{Shadow, ShadowConfig};

// Reserved port slot — 17300-range. Kept below the Linux ephemeral
// port range (32768-60999) so an outbound TCP connect from the daemon
// (to CH / shadow PG) can't land on a port we're about to bind for the
// metrics / walsender listener. CH's `interserver_http_port` defaults
// to `http_port + 1`, so METRICS / WALSENDER must dodge that slot too.
const SOURCE_PORT: u16 = 17301;
const SHADOW_PORT: u16 = 17302;
const CH_TCP_PORT: u16 = 17309;
const CH_HTTP_PORT: u16 = 17310;
const METRICS_PORT: u16 = 17315;
const WALSENDER_PORT: u16 = 17316;

const N_ROWS: i32 = 64;

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_bootstrap_ch_end_to_end() {
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

    let tmp = tempfile::tempdir().unwrap();

    // 1. Source PG.
    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };

    // 2. Source schema + workload (64 rows).
    fx::load_source_workload(&source, "s14", N_ROWS).expect("load source workload");

    // 3. CH server + dest table.
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    // 4. CH-config TOML.
    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        CH_TCP_PORT,
        "default",
        "s14.t",
        "default.t",
    )
    .expect("write ch-config");

    // 5. Shadow data dir + socket layout for the daemon's autospawn.
    //    `--bootstrap-autospawn-shadow` writes the shadow-side
    //    `port` / `unix_socket_directories` / `listen_addresses`
    //    overrides into the cloned data dir's postgresql.auto.conf and
    //    asserts mode 0700 before pg_ctl start.
    let bootstrap_shadow_data_dir = tmp.path().join("shadow-data");
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();

    // 6. Spawn walshadow-stream subprocess. `--max-segments=2` so the
    //    daemon ships the bootstrap-induced segment-3 transition plus
    //    one more (the post-workload `pg_switch_wal` below) before
    //    exiting. Setting `=1` races: the bootstrap pump consumes the
    //    first segment shipment before `wait_for_listen` polls again,
    //    so the test never catches a listening metrics port.
    //    `--metrics-bind` doubles as a "bootstrap complete + shadow up
    //    + WAL pump running" readiness probe.
    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{METRICS_PORT}").parse().unwrap();
    let child = Command::new(bin)
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
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{WALSENDER_PORT}"),
            "--retention-bytes",
            "0",
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-autospawn-shadow",
            "--bootstrap-shadow-replay-timeout",
            "120",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);
    // Drop guard for the shadow PG the daemon's autospawn brings up —
    // stops it after the test so leftover postmasters don't linger.
    // SAFETY: only set up paths; we'll attach the Shadow handle after
    // the daemon has exited so we can issue pg_ctl stop against it.

    let result = (|| -> Result<()> {
        // 7. Wait for the daemon's metrics endpoint. Crossing this
        //    barrier means: bootstrap finished, autospawn'd shadow is
        //    serving, preflight passed, WAL pump is in its main loop.
        //    Bootstrap-emitter INSERTs flush to CH synchronously before
        //    the daemon hands off to the streaming pump, so the 64-row
        //    fixture is fully on CH by the time we reach this barrier.
        fx::wait_for_listen(metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        // 8. Oracle: count + sum(id) + md5(string_agg(name, ',' ORDER
        //    BY id)) must match across both sides. The test exercises
        //    the bootstrap surface; no post-bootstrap workload here, so
        //    we don't need to drive a `pg_switch_wal` + drain cycle.
        fx::assert_ch_matches_source(&ch, &source, "s14.t", "default.t")
            .context("source vs CH parity")?;

        Ok(())
    })();

    // 11. Tear down the autospawn'd shadow PG so the data dir's
    //     postmaster is reaped before tempdir cleanup. The daemon
    //     doesn't stop shadow on its own (autospawn is start-only).
    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = SHADOW_PORT;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let shadow = Shadow::new(shadow_cfg);
        let _ = shadow.stop();
    }
    // Drain guard so any leftover child also gets cleaned.
    let _ = guard.into_inner().map(|mut c| {
        let _ = c.kill();
        let _ = c.wait();
    });

    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
