//! Verify window leg ships commits below pump resume boundary

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::time::Duration;

use anyhow::{Context, Result};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

const N_ROWS: i32 = 64;
/// Keep backup window open for writer
const MAX_RATE_KIB: &str = "32768";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn window_writes_reach_ch() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    fx::load_source_workload(&source, "s15", N_ROWS).expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new("s15", "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(
            &source,
            &ch_config_path,
            slot.walsender,
            // First-tick slot retains window segments
            &[
                "--bootstrap-max-rate-kib",
                MAX_RATE_KIB,
                "--slot",
                "walshadow_window",
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_backup_streaming(&source, Duration::from_secs(60))?;

        // Put batch below pump resume segment
        source
            .apply_schema_dump(
                "INSERT INTO s15.t SELECT g, 'window-'||g::text \
                   FROM generate_series(1001, 1100) g;\n\
                 SELECT pg_switch_wal();\n\
                 UPDATE s15.t SET name = 'updated-'||id::text WHERE id <= 32;\n\
                 SELECT pg_switch_wal();\n",
            )
            .context("window batch below the pump's resume")?;

        // Keep source advancing until backup closes
        let mut round = 0;
        while fx::backup_in_progress(&source) {
            round += 1;
            source
                .apply_schema_dump(&format!(
                    "INSERT INTO s15.t SELECT g, 'late-'||g::text \
                       FROM generate_series({from}, {to}) g;\n\
                     DELETE FROM s15.t WHERE id = {del};\n",
                    from = 2000 + round * 10,
                    to = 2009 + round * 10,
                    del = 33 + round,
                ))
                .context("in-window writes")?;
            std::thread::sleep(Duration::from_millis(50));
        }

        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(60))
            .context("daemon metrics endpoint never came up")?;

        let src_count = source
            .psql_one("SELECT count(*) FROM s15.t")
            .context("source count")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &src_count,
            Duration::from_secs(60),
        )
        .context("window rows never reached CH")?;
        fx::assert_ch_matches_source(&ch, &source, "s15.t", "default.t")
            .context("source vs CH parity across the backup window")?;

        // Leg tally distinguishes delivery from pump overlap
        let log = daemon.stderr();
        let shipped = log
            .lines()
            .find(|l| l.contains("backup window shipped"))
            .context("daemon logged no window-leg summary")?;
        anyhow::ensure!(
            !shipped.contains("rows=0"),
            "window leg shipped no rows: {shipped}",
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
