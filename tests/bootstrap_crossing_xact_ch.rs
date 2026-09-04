//! Verify slot-backed pump rebuilds transaction open across handoff from
//! window leg's `open_floor`

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::source_feed::open_sql_client;

const N_ROWS: i32 = 64;
/// Keep backup window open for writer
const MAX_RATE_KIB: &str = "32768";
const SCHEMA: &str = "s21";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_open_across_the_handoff_reaches_ch() {
    if !fx::requirements_available() {
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
    let child = daemon
        .spawn(
            &source,
            &ch_config_path,
            slot.walsender,
            // Pre-bootstrap slot retains crossing transaction prefix
            &[
                "--bootstrap-max-rate-kib",
                MAX_RATE_KIB,
                "--slot",
                "walshadow_crossing",
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        fx::wait_for_backup_streaming(&source, Duration::from_secs(60))?;

        // Hold transaction across handoff
        let holder = open_sql_client(&fx::pg_cfg(&source, "crossing-xact-test"))
            .await
            .context("holder connect")?;
        holder.batch_execute("BEGIN").await.context("BEGIN")?;
        holder
            .batch_execute(&format!(
                "INSERT INTO {SCHEMA}.t SELECT g, 'crossing-'||g::text \
                 FROM generate_series(1001, 1100) g"
            ))
            .await
            .context("crossing INSERT")?;
        // Move default resume beyond transaction prefix
        source
            .apply_schema_dump("SELECT pg_switch_wal();\nSELECT pg_switch_wal();\n")
            .context("switch wal")?;

        // Advance end_lsn beyond held records
        let mut round = 0;
        while fx::backup_in_progress(&source) {
            round += 1;
            source
                .apply_schema_dump(&format!(
                    "INSERT INTO {SCHEMA}.t SELECT g, 'late-'||g::text \
                       FROM generate_series({from}, {to}) g;\n",
                    from = 2000 + round * 10,
                    to = 2009 + round * 10,
                ))
                .context("in-window writes")?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // The leg's bounded wait runs out with the xact still open
        daemon.wait_for_log(
            "stopped waiting on xacts open across the handoff",
            Duration::from_secs(90),
        )?;
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(60))
            .context("daemon metrics endpoint never came up")?;

        holder.batch_execute("COMMIT").await.context("COMMIT")?;

        let src_count = source
            .psql_one(&format!("SELECT count(*) FROM {SCHEMA}.t"))
            .context("source count")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE _is_deleted = 0",
            &src_count,
            Duration::from_secs(60),
        )
        .context("crossing rows never reached CH")?;
        fx::assert_ch_matches_source(&ch, &source, &format!("{SCHEMA}.t"), "default.t")
            .context("source vs CH parity across the handoff")?;
        let crossing = ch
            .query("SELECT count() FROM default.t FINAL WHERE id BETWEEN 1001 AND 1100")
            .context("crossing count")?;
        ensure!(crossing == "100", "crossing batch incomplete: {crossing}");

        // Confirm pump resumed from recorded floor
        let log = daemon.stderr();
        let shipped = log
            .lines()
            .find(|l| l.contains("backup window shipped"))
            .context("daemon logged no window-leg summary")?;
        ensure!(
            !shipped.contains("open_floor=None"),
            "window leg reported no open xact: {shipped}",
        );
        Ok(())
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
