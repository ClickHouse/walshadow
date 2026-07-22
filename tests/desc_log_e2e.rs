//! Descriptor-log capture end-to-end: prepared-xact DDL drains at COMMIT
//! PREPARED under the prepared xid, and capture-all (schema rename) keeps
//! decode routing on fresh namespace text.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::process::{Command, Stdio};
use std::time::Duration;

use walshadow::mapping::{ColumnMapping, TableTarget};
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

const SLOT_PREPARED: PortSlot = PortSlot {
    source: 17960,
    shadow: 17961,
    ch_tcp: 17962,
    ch_http: 17963,
    walsender: 17967,
};
const SLOT_RENAME: PortSlot = PortSlot {
    source: 17970,
    shadow: 17971,
    ch_tcp: 17972,
    ch_http: 17973,
    walsender: 17977,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
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

/// Live 2PC: DDL + DML inside a prepared xact reach CH at COMMIT PREPARED.
/// The commit record's header xid is the finishing backend's; both the
/// capture-keyed events and the buffered rows live under the prepared xid
/// (B2) — pre-fix the drain keyed header xid and stranded them. The table
/// pre-exists (same-xact CREATE + INSERT rows stay fenced — stash item 5,
/// out of scope here).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_ddl_drains_at_commit_prepared() {
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
        "CREATE SCHEMA tp;\n\
         CREATE TABLE tp.twophase (id bigint PRIMARY KEY, v text);\n",
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "tp".into(),
        walshadow::mapping::NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            drop_table_strategy: None,
        },
    );

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: vec![],
        app_name: "walshadow-desc-prepared",
        ddl: Some(ddl_args),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "BEGIN;\n\
         ALTER TABLE tp.twophase ADD COLUMN extra text;\n\
         INSERT INTO tp.twophase (id, v, extra) \
            SELECT g, 'x' || g, 'e' || g FROM generate_series(1, 8) g;\n\
         PREPARE TRANSACTION 'desc_log_2pc';\n\
         SELECT pg_switch_wal();\n\
         COMMIT PREPARED 'desc_log_2pc';\n\
         SELECT pg_switch_wal();\n",
    );
    let shipped = fx::pump_segments(&mut pipeline, 2, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 2, "expected ≥2 shipped segments, got {shipped}");

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
        "SELECT count() FROM walshadow_test.twophase",
        "8",
        "prepared xact rows drain at COMMIT PREPARED",
    )
    .await;
    fx::wait_query(
        &ch,
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = 'twophase' AND name = 'extra'",
        "1",
        "prepared ALTER's Changed applies at COMMIT PREPARED",
    )
    .await;
}

/// Capture-all freshness: pg_namespace writes carry no per-relation relcache
/// invals, so a schema rename must recapture every descriptor — rows written
/// after the rename decode with the NEW namespace and route through a
/// mapping keyed under it. With stale descriptors they skip as unmapped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_rename_reroutes_under_new_namespace() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse on PATH");
        return;
    }
    let slot = SLOT_RENAME;
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, "", slot.source, slot.shadow, slot.walsender).await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.renamed_t (\
            id Int64,\
            v Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY (id)",
    )
    .expect("create dest table");

    // Mapping pinned under the POST-rename name only
    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("n2", "t"),
        target_table: TableTarget::new("walshadow_test", "renamed_t"),
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
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings,
        app_name: "walshadow-desc-rename",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "CREATE SCHEMA n1;\n\
         CREATE TABLE n1.t (id bigint PRIMARY KEY, v text);\n\
         INSERT INTO n1.t (id, v) VALUES (1, 'pre');\n\
         ALTER SCHEMA n1 RENAME TO n2;\n\
         INSERT INTO n2.t (id, v) SELECT g, 'post' FROM generate_series(10, 14) g;\n\
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

    // Post-rename rows decode under n2 and route; the pre-rename row's
    // descriptor said n1 (unmapped) — skipped by design
    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.renamed_t WHERE v = 'post'",
        "5",
        "post-rename rows route under the new namespace",
    )
    .await;
    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.renamed_t",
        "5",
        "pre-rename row stays unrouted (n1 unmapped)",
    )
    .await;
}
