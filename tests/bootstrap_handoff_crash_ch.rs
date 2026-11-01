//! Resume from saved bootstrap position after a crash before streaming starts

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

const N_ROWS: i32 = 64;
const SCHEMA: &str = "s26";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_at_bootstrap_handoff_keeps_rows_written_before_restart() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    fx::load_source_workload(&source, SCHEMA, N_ROWS).expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new(SCHEMA, "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let arm = tmp.path().join("crash.arm");
    std::fs::write(&arm, "BootstrapHandoff").unwrap();
    // Slot retains WAL between crash and restart
    let flags = ["--slot", "walshadow_handoff"];
    let child = daemon
        .spawn_mode(
            &source,
            &ch_config_path,
            slot.walsender,
            "direct",
            &flags,
            &[("WALSHADOW_TEST_CRASH", arm.display().to_string())],
        )
        .expect("spawn crashing daemon");
    let mut guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        let status = fx::wait_with_timeout(
            guard.child.as_mut().expect("child"),
            Duration::from_secs(60),
        )?;
        ensure!(
            arm.with_extension("hit").exists(),
            "daemon exited ({status}) without reaching bootstrap completion"
        );
        guard.child = None;

        source
            .psql_one(&format!(
                "INSERT INTO {SCHEMA}.t SELECT g, 'gap-'||g::text \
                 FROM generate_series(1001, 1010) g"
            ))
            .context("insert while daemon is down")?;
        std::fs::remove_file(&arm)?;

        guard.child = Some(
            daemon
                .spawn(&source, &ch_config_path, slot.walsender, &flags)
                .context("respawn daemon")?,
        );
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("restarted daemon metrics endpoint never came up")?;
        source
            .psql_one(&format!("INSERT INTO {SCHEMA}.t VALUES (2001, 'after')"))
            .context("insert after restart")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE id = 2001",
            "1",
            Duration::from_secs(30),
        )?;
        fx::wait_ch_matches_source(
            &ch,
            &source,
            &format!("{SCHEMA}.t"),
            "default.t",
            Duration::from_secs(10),
        )
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
