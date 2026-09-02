//! The shadow catalog must follow the *applied* source database.
//!
//! `[source] dbname` in `--ch-config` merges over `--dbname`, so the two can
//! disagree. The shadow is a physical clone of the whole source cluster, so its
//! database oids match the source's — which means resolving the shadow catalog
//! against the wrong database yields a valid-looking oid for the wrong data.
//! `set_target_db` reads that oid, and the filter then keeps a different
//! database's records: bootstrap loads one database (its catalog map comes from
//! the source sidecar) while CDC streams another, mixing two databases into one
//! ClickHouse table with no diagnostic.
//!
//! Here the table lives in source database `duptest`, named only by the TOML.
//! `--dbname postgres` disagrees and `--shadow-dbname postgres` is passed as
//! well (deprecated and ignored). Bootstrap rows and post-boot CDC must both
//! come from `duptest`.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use walshadow::shadow::{Shadow, ShadowConfig};

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = fx::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

fn psql_db(source: &Shadow, db: &str, sql: &str) -> Result<()> {
    let out = Command::new("psql")
        .args([
            "-h",
            source.config().socket_dir.to_str().unwrap(),
            "-p",
            &fx::PG_SOURCE_PORT.to_string(),
            "-U",
            "postgres",
            "-d",
            db,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ])
        .output()
        .context("spawn psql")?;
    if !out.status.success() {
        anyhow::bail!(
            "psql -d {db} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

fn write_config(path: &Path, ch_port: u16) -> Result<()> {
    let body = format!(
        "[source]\n\
         dbname = \"duptest\"\n\
         \n\
         [ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         compression = \"lz4\"\n\
         \n\
         [table.\"public\".\"dup\"]\n\
         replicate = true\n"
    );
    fs::write(path, body).context("write ch-config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_catalog_follows_applied_source_dbname() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump("CREATE DATABASE duptest;\n")
        .expect("create duptest db");
    for (db, who) in [("postgres", "from-postgres"), ("duptest", "from-duptest")] {
        psql_db(
            &source,
            db,
            &format!(
                "CREATE TABLE public.dup(a int primary key, b text); \
                 INSERT INTO public.dup VALUES (1,'{who}');"
            ),
        )
        .unwrap_or_else(|e| panic!("load {db} workload: {e:#}"));
    }
    psql_db(&source, "postgres", "SELECT pg_switch_wal();").expect("seal setup WAL");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");

    let ch_config_path = tmp.path().join("ch-config.toml");
    write_config(&ch_config_path, slot.ch_tcp).expect("write ch-config");

    let bootstrap_shadow_data_dir = tmp.path().join("shadow-data");
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();

    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{}", slot.metrics).parse().unwrap();
    let child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &fx::PG_SOURCE_PORT.to_string(),
            "--user",
            "postgres",
            "--dbname",
            "postgres",
            "--sslmode",
            "disable",
            "--out-dir",
            shadow_filter_dir.to_str().unwrap(),
            "--shadow-socket-dir",
            shadow_sock.to_str().unwrap(),
            "--shadow-port",
            &fx::PG_SHADOW_PORT.to_string(),
            "--shadow-user",
            "postgres",
            "--shadow-dbname",
            "postgres",
            "--bridge-lib-dir",
            fx::pgext_dir().to_str().unwrap(),
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{}", slot.walsender),
            "--retention-bytes",
            "0",
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-shadow-replay-timeout",
            "120",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let _guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(metrics_addr, Duration::from_secs(90))
            .context("daemon never bound its metrics endpoint")?;

        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if ch.query("EXISTS TABLE default.dup").unwrap_or_default() == "1"
                && ch
                    .query("SELECT b FROM default.dup FINAL WHERE a = 1")
                    .unwrap_or_default()
                    == "from-duptest"
            {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "bootstrap did not load duptest's row (saw {:?}); the shadow \
                 catalog is resolved against the wrong database",
                ch.query("SELECT b FROM default.dup FINAL WHERE a = 1").ok(),
            );
            std::thread::sleep(Duration::from_millis(200));
        }

        // Let the live pipeline settle before writing: inserting a second
        // after the pump starts races the first descriptor capture under load
        std::thread::sleep(Duration::from_secs(3));

        for (db, who) in [("postgres", "cdc-postgres"), ("duptest", "cdc-duptest")] {
            psql_db(
                &source,
                db,
                &format!("INSERT INTO public.dup VALUES (2,'{who}');"),
            )
            .with_context(|| format!("post-boot insert into {db}"))?;
        }
        psql_db(&source, "postgres", "SELECT pg_switch_wal();").context("seal post-boot WAL")?;
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let got = ch
                .query("SELECT b FROM default.dup FINAL WHERE a = 2")
                .unwrap_or_default();
            anyhow::ensure!(
                got != "cdc-postgres",
                "CDC replicated the wrong database: --dbname/--shadow-dbname \
                 named postgres, [source] dbname named duptest"
            );
            if got == "cdc-duptest" {
                return Ok(());
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "post-boot CDC row never landed (saw {got:?})"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    })();

    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = fx::PG_SHADOW_PORT;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let _ = Shadow::new(shadow_cfg).stop();
    }
    let _ = &ch;

    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
