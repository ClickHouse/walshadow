//! Restart-safety of catalog-boundary classification: a namespace rename
//! whose pg_namespace WAL (and command-end inval record) precede the
//! restart resume floor must still classify at its commit record — the
//! commit carries the xact tree's full inval set, and pg_namespace
//! catcache invals force capture-all — so rows written after the commit
//! route under the new namespace. Covered for plain COMMIT across a held
//! session and for PREPARE TRANSACTION / COMMIT PREPARED.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::io::Write as _;
use std::process::{Command, Stdio};
use std::time::Duration;

use walshadow::mapping::{ColumnMapping, TableTarget};
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

const SLOT_COMMIT: PortSlot = PortSlot {
    source: 17980,
    shadow: 17981,
    ch_tcp: 17982,
    ch_http: 17983,
    walsender: 17987,
};
const SLOT_PREPARED: PortSlot = PortSlot {
    source: 17990,
    shadow: 17991,
    ch_tcp: 17992,
    ch_http: 17993,
    walsender: 17997,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

/// psql session holding a transaction open across a pipeline restart;
/// statements execute as lines arrive on stdin
struct TxnSession {
    child: std::process::Child,
}

impl TxnSession {
    fn open(source: &Shadow) -> Self {
        let child = Command::new("psql")
            .args([
                "-h",
                source.config().socket_dir.to_str().unwrap(),
                "-p",
                &source.config().port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                "-",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn psql");
        Self { child }
    }

    fn send(&mut self, sql: &str) {
        self.child
            .stdin
            .as_mut()
            .expect("stdin piped")
            .write_all(sql.as_bytes())
            .expect("write to held psql");
    }

    fn finish(mut self) {
        drop(self.child.stdin.take());
        let status = self.child.wait().expect("wait psql");
        assert!(status.success(), "held psql session failed: {status}");
    }
}

/// Poll until the held session's statements executed (session parked idle
/// in transaction) so its WAL precedes the upcoming segment switch
fn wait_idle_in_txn(source: &Shadow) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let n = source
            .psql_one("SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'")
            .expect("poll pg_stat_activity");
        if n.trim() == "1" {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "open txn never appeared"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn spawn_txn(source: &Shadow, body: &str) -> std::thread::JoinHandle<()> {
    let sock = source.config().socket_dir.clone();
    let port = source.config().port;
    let sql = body.to_owned();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        let mut child = Command::new("psql")
            .args([
                "-h",
                sock.to_str().unwrap(),
                "-p",
                &port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                "-",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn psql");
        {
            use std::io::Write as _;
            child
                .stdin
                .as_mut()
                .expect("stdin piped")
                .write_all(sql.as_bytes())
                .unwrap();
        }
        let _ = child.wait();
    })
}

fn create_dest_table(ch: &fx::ChServer, table: &str) {
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(&format!(
        "CREATE OR REPLACE TABLE walshadow_test.{table} (\
            id Int64,\
            v Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY (id)"
    ))
    .expect("create dest table");
}

fn mappings_for(namespace: &str, table: &str) -> Vec<fx::TableMappingSpec> {
    vec![fx::TableMappingSpec {
        source_table: RelName::new(namespace, "t"),
        target_table: TableTarget::new("walshadow_test", table),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "v".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    }]
}

/// Held rename commits only after a restart. Segment 1 (pg_namespace WAL +
/// command-end inval record) ships and the pipeline stops; the rebuilt
/// pipeline resumes at the next segment — dirty state gone, catalog
/// records unseen — and must classify the commit from its inval set alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_commit_after_restart_reroutes() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse on PATH");
        return;
    }
    let slot = SLOT_COMMIT;
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA rs1;\n\
         CREATE TABLE rs1.t (id bigint PRIMARY KEY, v text);\n",
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    create_dest_table(&ch, "restart_t");

    // Mapping pinned under the POST-rename name only
    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state: shadow_stream_state.clone(),
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: mappings_for("rs2", "restart_t"),
        app_name: "walshadow-desc-restart",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    let mut txn = TxnSession::open(&source);
    txn.send("BEGIN;\nALTER SCHEMA rs1 RENAME TO rs2;\n");
    wait_idle_in_txn(&source);
    source.psql_one("SELECT pg_switch_wal()").expect("rotate");
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Restart: rebuilt stream resumes at the current segment head, past the
    // rename's catalog WAL; volatile catalog-dirty state is gone
    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: mappings_for("rs2", "restart_t"),
        app_name: "walshadow-desc-restart-2",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    txn.send(
        "COMMIT;\n\
         INSERT INTO rs2.t (id, v) SELECT g, 'post' FROM generate_series(1, 5) g;\n\
         SELECT pg_switch_wal();\n",
    );
    txn.finish();
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");
    let _ = shadow.stop();
    let _ = source.stop();

    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.restart_t WHERE v = 'post'",
        "5",
        "post-restart commit recaptures namespace, rows route under rs2",
    )
    .await;
}

/// Same classification hole through 2PC: PREPARE lands before the restart,
/// COMMIT PREPARED after. The commit-prepared record carries the prepared
/// xact's inval set; namespace catcache invals must force capture-all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_rename_commit_after_restart_reroutes() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse on PATH");
        return;
    }
    let slot = SLOT_PREPARED;
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA ps1;\n\
         CREATE TABLE ps1.t (id bigint PRIMARY KEY, v text);\n",
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    create_dest_table(&ch, "restart_2pc_t");

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state: shadow_stream_state.clone(),
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: mappings_for("ps2", "restart_2pc_t"),
        app_name: "walshadow-desc-restart-2pc",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "BEGIN;\n\
         ALTER SCHEMA ps1 RENAME TO ps2;\n\
         PREPARE TRANSACTION 'ns_rename_2pc';\n\
         SELECT pg_switch_wal();\n",
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");
    pipeline.shutdown().await.expect("pipeline drains clean");

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: mappings_for("ps2", "restart_2pc_t"),
        app_name: "walshadow-desc-restart-2pc-2",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "COMMIT PREPARED 'ns_rename_2pc';\n\
         INSERT INTO ps2.t (id, v) SELECT g, 'post' FROM generate_series(1, 5) g;\n\
         SELECT pg_switch_wal();\n",
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");
    let _ = shadow.stop();
    let _ = source.stop();

    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.restart_2pc_t WHERE v = 'post'",
        "5",
        "COMMIT PREPARED after restart recaptures namespace, rows route under ps2",
    )
    .await;
}
