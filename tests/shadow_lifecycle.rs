//! Phase 3: shadow PG lifecycle end-to-end.
//!
//! Skipped silently if `initdb` is not on `$PATH` (CI sandboxes
//! without PG, etc.). Run locally against any installed PG ≥ 12.
//!
//! Two scenarios:
//!
//! 1. `normal_mode_lifecycle` — initdb → start (no recovery signal) →
//!    probe in-recovery false, `pg_class` populated → stop.
//! 2. `standby_mode_lifecycle` — initdb → start normal → stop →
//!    enable standby recovery → start → wait for replay LSN to exist
//!    → probe in-recovery true → stop.

use std::process::Command;
use std::time::Duration;

use walshadow::shadow::{Shadow, ShadowConfig};

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
    let shadow = make_shadow(&tmp, 55501);

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
    let shadow = make_shadow(&tmp, 55502);

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

    // Flip into standby.
    shadow.enable_standby_recovery().expect("enable standby");
    shadow.start().expect("start (standby)");
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
    // No PG needed — just check that `enable_standby_recovery` writes
    // a sane `restore_command` line into postgresql.conf.
    if !pg_available() {
        eprintln!("skip: no initdb on PATH (need to populate postgresql.conf)");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55503);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.enable_standby_recovery().expect("standby");

    let conf = std::fs::read_to_string(tmp.path().join("data/postgresql.conf")).expect("read conf");
    assert!(
        conf.contains("restore_command = 'cp "),
        "postgresql.conf missing restore_command line",
    );
    assert!(conf.contains("/%f %p'"));
    assert!(tmp.path().join("data/standby.signal").exists());
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
