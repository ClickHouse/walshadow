//! No-PK + REPLICA IDENTITY FULL post-bootstrap CDC over greenfield Direct.
//!
//! Reproduces the "does walshadow update both rows" question: a table with no
//! primary key, RI FULL, two identical rows loaded at bootstrap, then a
//! post-boot `UPDATE` touching both. Asserts the update reaches CH (raw, no
//! FINAL) — i.e. the `replicate=true` opt-in for a greenfield table routes CDC
//! rather than silently parking as a never-materialised forward-declaration.

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

fn write_config(path: &Path, ch_port: u16) -> Result<()> {
    let body = format!(
        "[ch]\n\
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
async fn nopk_ri_full_update_propagates() {
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
        .apply_schema_dump(
            "CREATE TABLE public.dup(a int, b text);\n\
             ALTER TABLE public.dup REPLICA IDENTITY FULL;\n\
             INSERT INTO public.dup VALUES (1,'x'),(1,'x');\n\
             CHECKPOINT;\nSELECT pg_switch_wal();\n",
        )
        .expect("load source workload");

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
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let n = ch
                .query("SELECT count() FROM default.dup")
                .unwrap_or_default();
            if n.parse::<u64>().unwrap_or(0) >= 1 {
                break;
            }
            if Instant::now() >= deadline {
                anyhow::bail!("bootstrap row never reached CH (got {n:?})");
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        source
            .apply_schema_dump("UPDATE public.dup SET b='y';\nSELECT pg_switch_wal();\n")
            .context("post-boot update")?;

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let n = ch
                .query("SELECT count() FROM default.dup WHERE b='y'")
                .unwrap_or_default();
            if n.parse::<u64>().unwrap_or(0) >= 1 {
                break;
            }
            if Instant::now() >= deadline {
                let raw = ch
                    .query("SELECT b, _lsn, _is_deleted FROM default.dup ORDER BY _lsn, b")
                    .unwrap_or_default();
                anyhow::bail!("post-boot UPDATE never reached CH; raw dup:\n{raw}");
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Ok(())
    })();

    let _ = guard.into_inner().map(|mut c| {
        let _ = c.kill();
        let _ = c.wait();
    });
    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = fx::PG_SHADOW_PORT;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let _ = Shadow::new(shadow_cfg).stop();
    }

    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
