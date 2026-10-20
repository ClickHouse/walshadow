//! Daemon reorder path under a burst of tiny commits and one spilled
//! transaction. The queueing worker hands reorder whole record batches, so
//! consecutive small commits share seq registrations, row chunks and one
//! batcher hop; each commit here rewrites the previous one's row, so a
//! dropped or misattributed registration shows up as a stale or missing
//! row. A transaction past `--xact-buffer-max` then drains back from disk,
//! and its spill file must be gone once CH converges.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouped_small_commits_and_spilled_commit_reach_ch() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    fx::load_source_workload(&source, "s", 64).expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");
    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new("s", "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(
            &source,
            &ch_config_path,
            slot.walsender,
            &["--xact-buffer-max", "65536"],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;
        fx::wait_ch_matches_source(&ch, &source, "s.t", "default.t", Duration::from_secs(30))
            .context("bootstrap rows")?;

        source.psql_one(
            "DO $$ BEGIN
               FOR i IN 1..300 LOOP
                 INSERT INTO s.t VALUES (1000 + i, 'small-' || i);
                 UPDATE s.t SET name = 'upd-' || i WHERE id = 999 + i;
                 IF i % 7 = 0 THEN DELETE FROM s.t WHERE id = 997 + i; END IF;
                 COMMIT;
               END LOOP;
             END $$",
        )?;
        source.psql_one(
            "INSERT INTO s.t \
             SELECT 5000 + g, repeat(md5(g::text), 6) FROM generate_series(1, 2000) g",
        )?;
        fx::wait_ch_matches_source(&ch, &source, "s.t", "default.t", Duration::from_secs(30))?;

        let metrics = fx::http_get(daemon.metrics_addr, "/metrics")?;
        let evictions = fx::parse_metric(&metrics, "walshadow_spill_evictions_total")
            .context("spill_evictions_total missing")?;
        ensure!(evictions > 0, "large transaction never spilled");
        let scratch = walshadow::fs::scratch_dir(&daemon.spill_dir);
        let leftover: Vec<_> = std::fs::read_dir(&scratch)
            .with_context(|| format!("read {}", scratch.display()))?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("xid-"))
            .collect();
        ensure!(leftover.is_empty(), "spill files left behind: {leftover:?}");
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
