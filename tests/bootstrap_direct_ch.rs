//! Bootstrap + CH end-to-end via the direct
//! replication-protocol BASE_BACKUP source.
//!
//! Closes the gap left by `bootstrap_direct_e2e.rs`, which exercised the
//! direct bootstrap pipeline against a `RecordingObserver` (no live
//! CH). This drill runs the real daemon binary
//! (`target/debug/walshadow-stream`) with
//! `--bootstrap-mode=direct --bootstrap-shadow-data-dir --ch-config`
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
//!         → pipeline::bootstrap::drain → shared tail (batcher + inserter
//!           pool + ack) → CH default.t
//!         → start shadow PG against bootstrap_shadow_data_dir
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

use std::time::Duration;

use anyhow::{Context, Result};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

const N_ROWS: i32 = 64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_bootstrap_ch_end_to_end() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    // 1. Source PG.
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    // 2. Source schema + workload (64 rows).
    fx::load_source_workload(&source, "s14", N_ROWS).expect("load source workload");

    // 3. CH server + dest table.
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    // 4. CH-config TOML.
    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new("s14", "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");

    // 5. Shadow data dir and socket layout. Daemon writes listener
    //    config and sets data dir mode to 0700 before pg_ctl start
    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &[])
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        // 6. Wait for the daemon's metrics endpoint (liveness). The daemon
        //    binds it before the bootstrap drains to CH, so this is not a
        //    bootstrap-complete signal on its own.
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        let src_count = source
            .psql_one("SELECT count(*) FROM s14.t")
            .context("source count")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &src_count,
            Duration::from_secs(60),
        )?;

        // 7. Oracle: count + sum(id) + md5(string_agg(name, ',' ORDER BY id))
        //    must match across both sides.
        fx::assert_ch_matches_source(&ch, &source, "s14.t", "default.t")
            .context("source vs CH parity")?;

        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
