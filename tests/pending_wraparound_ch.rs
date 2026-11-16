//! Pending xids whose pg_xact segment shadow truncated stay pending across a
//! restart, then settle from the resumed commit records

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use walshadow::shadow::Shadow;
use walshadow::source_feed::open_sql_client;

const N_ROWS: i32 = 64;
/// Keep both writers open across backup window
const MAX_RATE_KIB: &str = "32768";
const SCHEMA: &str = "s27";
/// PG `SLRU_PAGES_PER_SEGMENT`
const SLRU_PAGES_PER_SEGMENT: u64 = 32;
/// Xids reset below the segment boundary, room for both writers
const BOUNDARY_MARGIN: u64 = 16;
/// Retry a refused insert well past the drill, so only the kill ends the run
const CH_RETRY: &str = "retry_max_attempts = 10000\nretry_max_backoff_ms = 100\n";

fn wait_for_metric(addr: SocketAddr, name: &str, least: u64, timeout: Duration) -> Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let value = fx::parse_metric(&fx::http_get(addr, "/metrics")?, name);
        if let Some(v) = value.filter(|v| *v >= least) {
            return Ok(v);
        }
        ensure!(
            Instant::now() < deadline,
            "{name} never reached {least}, last {value:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn run_tool(tool: &str, args: &[&str]) -> Result<String> {
    let out = Command::new(tool)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .with_context(|| format!("spawn {tool}"))?;
    ensure!(
        out.status.success(),
        "{tool} {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Xids per pg_xact segment, 2 status bits per xid
fn xids_per_segment(source: &Shadow) -> Result<u64> {
    let data = source.config().data_dir.to_str().unwrap();
    let out = run_tool("pg_resetwal", &["-n", data])?;
    let blcksz: u64 = out
        .lines()
        .find_map(|l| l.strip_prefix("Database block size:"))
        .context("pg_resetwal -n names no block size")?
        .trim()
        .parse()?;
    Ok(blcksz * 4 * SLRU_PAGES_PER_SEGMENT)
}

/// Next xid just below the second segment boundary on the stopped cluster,
/// the segment it lands in zero-filled as PG
/// `src/test/modules/test_slru/t/002_multixact_wraparound.pl` does
fn jump_xids_below_boundary(source: &Shadow, per_segment: u64) -> Result<u64> {
    let boundary = 2 * per_segment;
    let next = boundary - BOUNDARY_MARGIN;
    let data = &source.config().data_dir;
    run_tool(
        "pg_resetwal",
        &["-x", &next.to_string(), data.to_str().unwrap()],
    )?;
    let segment = data.join(format!("pg_xact/{:04X}", next / per_segment));
    std::fs::write(&segment, vec![0u8; (per_segment / 4) as usize])?;
    Ok(boundary)
}

/// Assign xids until the counter crosses `boundary`, then freeze every
/// database so pg_xact truncates below it
fn freeze_past(source: &Shadow, boundary: u64) -> Result<()> {
    source.apply_schema_dump(&format!(
        "DO $$ BEGIN \
           WHILE pg_snapshot_xmax(pg_current_snapshot())::text::bigint <= {boundary} LOOP \
             PERFORM pg_current_xact_id(); COMMIT; \
           END LOOP; \
         END $$;\n\
         ALTER DATABASE template0 WITH ALLOW_CONNECTIONS true;\n"
    ))?;
    let cfg = source.config();
    run_tool(
        "vacuumdb",
        &[
            "-h",
            cfg.socket_dir.to_str().unwrap(),
            "-p",
            &cfg.port.to_string(),
            "-U",
            "postgres",
            "--all",
            "--freeze",
            "-q",
        ],
    )?;
    source.apply_schema_dump("ALTER DATABASE template0 WITH ALLOW_CONNECTIONS false;\n")?;
    Ok(())
}

fn write_ch_config(path: &Path, ch_port: u16) -> Result<()> {
    let table = |name: &str| {
        format!(
            "[table.\"{SCHEMA}\".\"{name}\"]\n\
             target_database = \"default\"\n\
             target_table = \"{name}\"\n\
             columns = [\n  \
               {{ attnum = 1, target = \"id\",   type = \"Int32\"  }},\n  \
               {{ attnum = 2, target = \"name\", type = \"String\" }},\n\
             ]\n"
        )
    };
    let body = format!(
        "[ch]\nhost = \"127.0.0.1\"\nport = {ch_port}\ndatabase = \"default\"\n\
         compression = \"lz4\"\n{CH_RETRY}\n{}\n{}",
        table("t"),
        table("u"),
    );
    std::fs::write(path, body)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncated_pending_xids_stay_pending_until_resumed_outcomes() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    fx::load_source_workload(&source, SCHEMA, N_ROWS).expect("load source workload");
    source
        .apply_schema_dump(&format!(
            "CREATE TABLE {SCHEMA}.u (id int4 PRIMARY KEY, name text NOT NULL);\n\
             ALTER SYSTEM SET autovacuum = off;\n"
        ))
        .expect("second table");
    source.stop().expect("stop source for reset");
    let per_segment = xids_per_segment(&source).expect("pg_xact geometry");
    let boundary = jump_xids_below_boundary(&source, per_segment).expect("pg_resetwal");
    source.start().expect("restart reset source");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    for table in ["t", "u"] {
        fx::create_ch_dest_table(&ch, "default", table).expect("create ch table");
    }
    let ch_config_path = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config_path, slot.ch_tcp).expect("write ch-config");

    // Backup checkpoint flushes open writers' tuples below redo point
    let committer = open_sql_client(&fx::pg_cfg(&source, "wrap-committer"))
        .await
        .expect("committer connect");
    let aborter = open_sql_client(&fx::pg_cfg(&source, "wrap-aborter"))
        .await
        .expect("aborter connect");
    let truncated_segment = boundary / per_segment - 1;
    for (client, tag, from) in [(&committer, "kept", 1001), (&aborter, "rolled", 2001)] {
        client.batch_execute("BEGIN").await.expect("BEGIN");
        let xid: String = client
            .query_one("SELECT pg_current_xact_id()::text", &[])
            .await
            .expect("assign xid")
            .get(0);
        let xid: u64 = xid.parse().unwrap();
        assert_eq!(
            xid / per_segment,
            truncated_segment,
            "xid {xid} off segment"
        );
        client
            .batch_execute(&format!(
                "INSERT INTO {SCHEMA}.t SELECT g, '{tag}-'||g::text \
                 FROM generate_series({from}, {}) g",
                from + 9
            ))
            .await
            .expect("below-redo INSERT");
    }
    source
        .apply_schema_dump("CHECKPOINT;\n")
        .expect("checkpoint uncommitted pages");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let flags = [
        "--bootstrap-max-rate-kib",
        MAX_RATE_KIB,
        "--bootstrap-wind-down-secs",
        "0",
        "--slot",
        "walshadow_wrap",
    ];
    let spawn = || {
        daemon
            .spawn(&source, &ch_config_path, slot.walsender, &flags)
            .map(fx::ChildGuard::new)
    };
    let first = spawn().expect("spawn walshadow-stream");
    let shadow_segment = daemon
        .shadow_data_dir
        .join(format!("pg_xact/{truncated_segment:04X}"));

    let parked: Result<()> = async {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(120))
            .context("daemon metrics endpoint never came up")?;
        wait_for_metric(
            daemon.metrics_addr,
            "walshadow_pending_rows_total",
            20,
            Duration::from_secs(60),
        )
        .context("pending rows never reached ClickHouse")?;

        // Refused insert parks reorder at the TRUNCATE fence, ahead of both
        // outcomes, while shadow replays on
        ch.query("RENAME TABLE default.u TO default.u_held")?;
        source.apply_schema_dump(&format!(
            "INSERT INTO {SCHEMA}.u VALUES (1, 'fenced');\nTRUNCATE {SCHEMA}.u;\n\
             INSERT INTO {SCHEMA}.u VALUES (2, 'after');\n"
        ))?;
        committer.batch_execute("COMMIT").await.context("COMMIT")?;
        aborter
            .batch_execute("ROLLBACK")
            .await
            .context("ROLLBACK")?;
        freeze_past(&source, boundary)?;
        ensure!(
            !source
                .config()
                .data_dir
                .join(format!("pg_xact/{truncated_segment:04X}"))
                .exists(),
            "source kept pg_xact segment {truncated_segment:04X}"
        );

        let deadline = Instant::now() + Duration::from_secs(60);
        while shadow_segment.exists() {
            ensure!(
                Instant::now() < deadline,
                "shadow never replayed the pg_xact truncation"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        let published = ch.query("SELECT count() FROM default.t FINAL WHERE id > 1000")?;
        ensure!(
            published == "0",
            "parked reorder published pending rows: {published}"
        );
        Ok(())
    }
    .await;
    let mut child = first.into_inner().expect("first run still held its child");
    child.kill().expect("SIGKILL parked daemon");
    child.wait().expect("reap killed daemon");
    daemon.stop_shadow();
    parked.unwrap_or_else(|e| panic!("{e:#}\n--- daemon stderr ---\n{}", daemon.stderr()));
    ch.query("RENAME TABLE default.u_held TO default.u")
        .expect("restore u");

    let guard = spawn().expect("respawn walshadow-stream");
    let result: Result<()> = async {
        daemon
            .wait_for_log(
                "shadow pg_xact lacks deciding xids",
                Duration::from_secs(120),
            )
            .context("boot never found the truncated outcomes")?;
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(120))
            .context("restarted metrics endpoint never came up")?;
        let undecidable = wait_for_metric(
            daemon.metrics_addr,
            "walshadow_pending_undecidable_xids",
            1,
            Duration::from_secs(30),
        )?;
        ensure!(undecidable == 2, "undecidable xids {undecidable}, want 2");

        fx::wait_ch_matches_source(
            &ch,
            &source,
            &format!("{SCHEMA}.t"),
            "default.t",
            Duration::from_secs(90),
        )
        .context("resumed outcomes never settled the retained rows")?;
        fx::wait_for_ch_value(
            &ch,
            "EXISTS TABLE default.t__wspending",
            "0",
            Duration::from_secs(30),
        )
        .context("pending table outlived its outstanding set")?;
        fx::wait_ch_matches_source(
            &ch,
            &source,
            &format!("{SCHEMA}.u"),
            "default.u",
            Duration::from_secs(30),
        )?;
        Ok(())
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
