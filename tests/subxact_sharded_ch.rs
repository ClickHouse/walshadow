//! Subtransactions under the xid-sharded pipeline (`--queueing-shards > 1`),
//! end-to-end to ClickHouse.
//!
//! A subxact's heaps route to `shard_for(subxid)`, which differs from the top's
//! shard, so its commit drains the tree across shards and merges. This drives a
//! single transaction with committed savepoints (cross-shard) plus a
//! `ROLLBACK TO SAVEPOINT` (a subxact whose rows must NOT replicate), then
//! asserts source↔CH parity — which only holds if the cross-shard drain emits
//! exactly the committed rows.
//!
//! Skipped silently when `initdb`, `pg_basebackup`, or `clickhouse` is absent.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use walshadow::shadow::{Shadow, ShadowConfig};

const SOURCE_PORT: u16 = 17321;
const SHADOW_PORT: u16 = 17322;
const CH_TCP_PORT: u16 = 17329;
const CH_HTTP_PORT: u16 = 17330;
const METRICS_PORT: u16 = 17335;
const WALSENDER_PORT: u16 = 17336;

const N_BOOTSTRAP: i32 = 16;

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharded_subxacts_replicate_committed_rows_only() {
    if !fx::pg_available() || !fx::pg_basebackup_available() {
        eprintln!("skip: PG binaries not on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };

    fx::load_source_workload(&source, "s14", N_BOOTSTRAP).expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");

    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        CH_TCP_PORT,
        "default",
        "s14.t",
        "default.t",
    )
    .expect("write ch-config");

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
    let metrics_addr: SocketAddr = format!("127.0.0.1:{METRICS_PORT}").parse().unwrap();
    let child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &SOURCE_PORT.to_string(),
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
            &SHADOW_PORT.to_string(),
            "--shadow-user",
            "postgres",
            "--shadow-dbname",
            "postgres",
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{WALSENDER_PORT}"),
            "--retention-bytes",
            "0",
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-autospawn-shadow",
            "--bootstrap-shadow-replay-timeout",
            "120",
            "--queueing-shards",
            "4",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(metrics_addr, Duration::from_secs(60))
            .context("daemon metrics endpoint never came up")?;

        // One transaction, savepoints landing subxids in different shards:
        //   1001,1002      top
        //   1003           sp1 subxact   (committed)
        //   1004           sp2 subxact   (ROLLBACK TO → discarded)
        //   1005           post-rollback subxact (committed)
        // Source keeps 1001,1002,1003,1005 (+ the 16 bootstrap rows); 1004 is
        // gone. CH must match.
        source
            .apply_schema_dump(
                "BEGIN;\n\
                 INSERT INTO s14.t VALUES (1001,'x1'),(1002,'x2');\n\
                 SAVEPOINT sp1;\n\
                 INSERT INTO s14.t VALUES (1003,'x3');\n\
                 SAVEPOINT sp2;\n\
                 INSERT INTO s14.t VALUES (1004,'x4');\n\
                 ROLLBACK TO SAVEPOINT sp2;\n\
                 INSERT INTO s14.t VALUES (1005,'x5');\n\
                 RELEASE SAVEPOINT sp1;\n\
                 COMMIT;\n\
                 SELECT pg_switch_wal();\n",
            )
            .context("run savepoint transaction")?;

        let want = (N_BOOTSTRAP + 4).to_string();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let n = ch
                .query("SELECT count() FROM default.t FINAL WHERE _is_deleted = 0")
                .context("ch count")?;
            if n == want {
                break;
            }
            if Instant::now() >= deadline {
                bail!("CH never reached {want} rows (last: {n})");
            }
            std::thread::sleep(Duration::from_millis(200));
        }

        fx::assert_ch_matches_source(&ch, &source, "s14.t", "default.t")
            .context("source vs CH parity after sharded subxacts")
    })();

    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = SHADOW_PORT;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let _ = Shadow::new(shadow_cfg).stop();
    }
    let _ = guard.into_inner().map(|mut c| {
        let _ = c.kill();
        let _ = c.wait();
    });

    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
