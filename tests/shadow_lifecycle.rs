//! Shadow PG lifecycle end-to-end.
//!
//! Skipped silently if `initdb` is not on `$PATH` (CI sandboxes
//! without PG, etc.). Run locally against any installed PG ≥ 12.
//!
//! Three scenarios:
//!
//! 1. `normal_mode_lifecycle` — initdb → start (no recovery signal) →
//!    probe in-recovery false, `pg_class` populated → stop.
//! 2. `standby_mode_lifecycle` — initdb → start normal → stop →
//!    write standby.signal → start_with_floor_retry → wait for replay LSN
//!    → probe in-recovery true → stop.
//! 3. `guc_floor_pause_resumes_then_restarts` — raise a GUC on one cluster,
//!    replay that WAL into a pre-raise copy → replay pauses → resume for the
//!    floor → restart on the raised value → operator pause holds.

#[path = "common/ports.rs"]
mod ports;

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use walrus::pg::wal::segment::is_wal_filename;
use walshadow::shadow::{ResumeOutcome, Shadow, ShadowConfig, SourceGucFloor};

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_shadow(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(30);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

#[test]
fn normal_mode_lifecycle() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, ports::PG_SHADOW_PORT);

    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("write conf");
    shadow.start().expect("start");

    let started = scopeguard_stop(&shadow);

    assert!(shadow.is_running().expect("status"));
    assert!(
        !shadow.is_in_recovery().expect("pg_is_in_recovery"),
        "fresh initdb cluster shouldn't be in recovery without standby.signal",
    );

    let health = shadow.health().expect("health");
    assert!(!health.in_recovery);
    assert!(
        health.pg_class_count > 100,
        "fresh PG should have hundreds of catalog rows; got {}",
        health.pg_class_count,
    );
    assert_eq!(health.pg_proc_relname, "pg_proc");

    drop(started);
}

#[test]
fn standby_mode_lifecycle() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, ports::PG_SHADOW_PORT);

    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("write conf");
    // Boot once in normal mode to verify schema-restore primitive
    // works against a live cluster, then shut down before flipping
    // into standby mode.
    shadow.start().expect("start (normal)");
    shadow
        .apply_schema_dump(
            "CREATE SCHEMA walshadow_test;\n\
             CREATE TABLE walshadow_test.t (id int PRIMARY KEY, payload text);\n",
        )
        .expect("apply schema dump");
    let pre = shadow.health().expect("health pre");
    assert!(!pre.in_recovery);
    shadow.stop().expect("stop (normal)");
    assert!(!shadow.is_running().unwrap());

    // Flip into standby. Empty primary_conninfo: the test cluster
    // has no walsender to point at, so the walreceiver's connection
    // attempt will fail and PG falls back to the archive
    // restore_command path (which also has no files). Standby still
    // boots cleanly into recovery.
    shadow.write_standby_signal().expect("standby signal");
    shadow
        .start_with_floor_retry(Some(
            "host=/dev/null port=1 user=walshadow application_name=test",
        ))
        .expect("start (standby)");
    let started = scopeguard_stop(&shadow);

    // After standby start, hot_standby should let us connect. The
    // cluster is in recovery and (with no source WAL waiting in the
    // filter dir) sits idle at its own initdb terminal LSN. Replay
    // LSN may be NULL for a moment while the startup process catches
    // up, then becomes Some(_). Wait for any replay LSN.
    let lsn = shadow
        .wait_for_replay(0, Duration::from_secs(30))
        .expect("wait_for_replay");
    eprintln!("standby replay LSN: {:#X}", lsn);

    let h = shadow.health().expect("health post");
    assert!(h.in_recovery, "standby.signal cluster must be in recovery");
    assert!(h.replay_lsn.is_some());
    // Schema we loaded in normal mode survived the restart — restore
    // primitive landed durable changes.
    let walshadow_test_count = shadow
        .psql_one("SELECT count(*) FROM pg_class WHERE relname = 't' AND relnamespace = 'walshadow_test'::regnamespace")
        .expect("count walshadow_test.t");
    assert_eq!(
        walshadow_test_count, "1",
        "schema dump must persist into standby"
    );
    assert_eq!(h.pg_proc_relname, "pg_proc");

    drop(started);
}

#[test]
fn restore_command_filename_is_segment_relative() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH (need to populate postgresql.conf)");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, ports::PG_SHADOW_PORT);
    shadow.initdb().expect("initdb");
    shadow.write_standby_signal().expect("standby signal");
    shadow
        .materialize_conf(
            &walshadow::shadow::SourceGucFloor::default(),
            Some("host=/var/run/postgresql port=55501 user=walshadow application_name=shadow"),
        )
        .expect("standby conf");

    let conf = std::fs::read_to_string(tmp.path().join("data/postgresql.conf")).expect("read conf");
    assert!(
        conf.contains("restore_command = 'ln -f ") || conf.contains("restore_command = 'cp "),
        "postgresql.conf missing restore_command line",
    );
    assert!(conf.contains("/%f %p'"));
    assert!(conf.contains("primary_conninfo"));
    assert!(tmp.path().join("data/standby.signal").exists());
}

/// PG pauses hot standby when a replayed `XLOG_PARAMETER_CHANGE` names a value
/// above the running one, and shuts down once that pause is lifted. Producing
/// the record needs a cluster that raises the value and a copy of that cluster
/// from before the raise to replay it
#[test]
fn guc_floor_pause_resumes_then_restarts() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, ports::PG_SHADOW_PORT);
    let data = tmp.path().join("data");
    let pre_raise = tmp.path().join("pre-raise");

    let low = SourceGucFloor::default();
    shadow.initdb().expect("initdb");
    shadow.materialize_conf(&low, None).expect("conf (low)");
    shadow.start().expect("start (low)");
    shadow.stop().expect("stop (low)");
    copy_tree(&data, &pre_raise);

    // Startup logs XLOG_PARAMETER_CHANGE for a value above pg_control's
    let raised = SourceGucFloor {
        max_connections: low.max_connections + 100,
        ..low
    };
    shadow
        .materialize_conf(&raised, None)
        .expect("conf (raised)");
    shadow.start().expect("start (raised)");
    shadow.stop().expect("stop (raised)");
    stage_wal(&data, &shadow.config().filter_out_dir);

    std::fs::remove_dir_all(&data).unwrap();
    copy_tree(&pre_raise, &data);
    shadow.write_standby_signal().expect("standby signal");
    shadow
        .start_with_floor_retry(None)
        .expect("start (standby)");
    let started = scopeguard_stop(&shadow);

    // Replay reaches the record asynchronously, so probe as the daemon does
    let outcome = wait_for("replay pause", Duration::from_secs(30), || {
        match shadow.try_pg_wal_replay_resume() {
            Ok(ResumeOutcome::NotPaused) => None,
            other => Some(other),
        }
    });
    assert_eq!(
        outcome.expect("resume probe"),
        ResumeOutcome::ResumedForFloor
    );
    assert_eq!(
        shadow.control_guc_floor().expect("floor after replay"),
        raised,
        "replayed parameter change writes raised values to pg_control",
    );

    wait_for("postmaster exit", Duration::from_secs(15), || {
        (!shadow.is_running().expect("pg_ctl status")).then_some(())
    });
    shadow.clear_stale_pid().expect("clear stale pid");
    shadow
        .start_with_floor_retry(None)
        .expect("restart on raised floor");
    let conf = std::fs::read_to_string(data.join("postgresql.conf")).unwrap();
    assert!(
        conf.contains(&format!("max_connections = {}", raised.max_connections)),
        "{conf}",
    );
    assert!(shadow.health().expect("health").in_recovery);
    assert_eq!(
        shadow.try_pg_wal_replay_resume().expect("resume probe"),
        ResumeOutcome::NotPaused,
        "raised value satisfies the record on replay",
    );

    // An operator pause reads the same floor as the running values, so it holds
    shadow
        .psql_one("SELECT pg_wal_replay_pause()")
        .expect("operator pause");
    assert_eq!(
        shadow.try_pg_wal_replay_resume().expect("resume probe"),
        ResumeOutcome::PausedForeign,
    );
    assert_ne!(
        shadow
            .psql_one("SELECT pg_get_wal_replay_pause_state()")
            .expect("pause state"),
        "not paused",
    );

    drop(started);
}

// ----- helpers --------------------------------------------------------

/// Best-effort stop on test exit. We can't use a real scopeguard crate
/// without adding a dep, but a tiny RAII wrapper works.
struct StopOnDrop<'a> {
    shadow: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.shadow.stop();
    }
}

fn scopeguard_stop(shadow: &Shadow) -> StopOnDrop<'_> {
    StopOnDrop { shadow }
}

fn copy_tree(src: &Path, dst: &Path) {
    let status = Command::new("cp")
        .args(["-a".as_ref(), src.as_os_str(), dst.as_os_str()])
        .status()
        .expect("cp -a");
    assert!(status.success(), "cp -a {src:?} {dst:?}");
}

/// Feed WAL to a standby the way walshadow does, through `restore_command`
fn stage_wal(data: &Path, filter_dir: &Path) {
    for entry in std::fs::read_dir(data.join("pg_wal")).expect("pg_wal") {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().expect("utf8 segment name");
        if is_wal_filename(name) {
            std::fs::copy(entry.path(), filter_dir.join(name)).expect("stage segment");
        }
    }
}

fn wait_for<T>(what: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(v) = probe() {
            return v;
        }
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}
