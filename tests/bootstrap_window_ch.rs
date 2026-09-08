//! Verify window leg ships commits below pump resume boundary

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::io::Write as _;
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
    source
        .apply_schema_dump(
            "CREATE TABLE s15.fixed (id int PRIMARY KEY, n bigint);
         ALTER TABLE s15.fixed REPLICA IDENTITY FULL;
         INSERT INTO s15.fixed SELECT g, g FROM generate_series(1, 64) g;
         CHECKPOINT;",
        )
        .unwrap();

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");
    ch.query(
        "CREATE TABLE default.fixed (id Int32, n Int64, _lsn UInt64, _xid UInt32,
         _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool)
         ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .unwrap();

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
    let mut config = std::fs::OpenOptions::new()
        .append(true)
        .open(&ch_config_path)
        .unwrap();
    writeln!(
        config,
        "\n[table.\"s15\".\"fixed\"]
         target_database = \"default\"
         target_table = \"fixed\"
         columns = [
           {{ attnum = 1, target = \"id\", type = \"Int32\" }},
           {{ attnum = 2, target = \"n\", type = \"Int64\" }},
         ]"
    )
    .unwrap();
    drop(config);

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
                 INSERT INTO s15.fixed SELECT g, -g FROM generate_series(1001, 1100) g;\n\
                 UPDATE s15.fixed SET n = -id WHERE id <= 32;\n\
                 DELETE FROM s15.fixed WHERE id > 32 AND id < 1000;\n\
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
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.fixed FINAL WHERE _is_deleted = 0",
            "132",
            Duration::from_secs(60),
        )?;
        let expected = source.psql_one(
            "SELECT string_agg(id::text || ':' || n::text, ',' ORDER BY id) FROM s15.fixed",
        )?;
        let actual = ch.query(
            "SELECT arrayStringConcat(groupArray(concat(toString(id), ':', toString(n))), ',')
             FROM (SELECT id, n FROM default.fixed FINAL WHERE _is_deleted = 0 ORDER BY id)",
        )?;
        anyhow::ensure!(actual == expected, "fixed-width rows differ: {actual}");
        anyhow::ensure!(
            ch.query(
                "SELECT count() FROM default.fixed FINAL WHERE _is_deleted = 0 AND _commit_ts = 0"
            )? == "0",
            "fixed-width changes must carry WAL commit timestamps"
        );

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
        let gate = log
            .lines()
            .find(|l| l.contains("bootstrap visibility gate settled"))
            .context("no gate summary")?;
        anyhow::ensure!(
            gate.contains("pending_relations=0"),
            "unexpected source repair: {gate}"
        );
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
