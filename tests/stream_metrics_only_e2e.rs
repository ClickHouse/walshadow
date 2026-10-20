//! Direct bootstrap and streaming with no `[ch]`: the null-tail pipeline

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_only_bootstrap_streams_and_reports_open_xact() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(
            "CREATE TABLE public.t (id int PRIMARY KEY, v text);
             INSERT INTO public.t SELECT g, 'v' || g FROM generate_series(1, 50) g;
             CHECKPOINT;
             SELECT pg_switch_wal();",
        )
        .expect("load source workload");
    let config = tmp.path().join("no-ch.toml");
    std::fs::write(&config, "").unwrap();
    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(
            &source,
            &config,
            slot.walsender,
            &["--bootstrap-wind-down-secs", "0"],
        )
        .expect("spawn daemon");
    let mut guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        let deadline = Instant::now() + Duration::from_secs(120);
        while !daemon.stderr().contains("walsender connected") {
            let exited = guard.child.as_mut().unwrap().try_wait()?;
            ensure!(exited.is_none(), "daemon exited during boot: {exited:?}");
            ensure!(Instant::now() < deadline, "pump never started");
            std::thread::sleep(Duration::from_millis(100));
        }
        let log = daemon.stderr();
        ensure!(
            log.contains("metrics-only pipeline (null tail) starting"),
            "not metrics-only"
        );
        // Uncommitted xact parks in the buffer; the stall watchdog names it
        let cfg = source.config();
        let mut open = Command::new("psql")
            .args([
                "-h",
                cfg.socket_dir.to_str().unwrap(),
                "-p",
                &cfg.port.to_string(),
            ])
            .args(["-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;
        let mut stdin = open.stdin.take().unwrap();
        stdin.write_all(b"BEGIN; INSERT INTO public.t VALUES (100, 'open');\n")?;
        source.psql_one("SELECT pg_switch_wal()")?;
        daemon.wait_for_log("emitter ack pinned", Duration::from_secs(30))?;
        stdin.write_all(b"COMMIT;\n")?;
        drop(stdin);
        ensure!(open.wait()?.success(), "open xact session failed");
        // No `[ch]` to apply TRUNCATE or DDL against; ack still passes both
        source.psql_one("TRUNCATE public.t")?;
        source.psql_one("ALTER TABLE public.t ADD COLUMN w int")?;
        let applied: u64 = source
            .psql_one("SELECT pg_current_wal_lsn() - '0/0'")?
            .parse()?;
        source.psql_one("SELECT pg_switch_wal()")?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let body = fx::http_get(daemon.metrics_addr, "/metrics")?;
            let ack = fx::parse_metric(&body, "walshadow_position_lsn{at=\"ack\"}");
            if ack.is_some_and(|a| a >= applied) {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "ack {ack:?} stuck below {applied}"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
        Ok(())
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
