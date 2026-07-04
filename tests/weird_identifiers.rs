//! Weird / quoted / special-char table and column names, end-to-end.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::process::{Command, Stdio};
use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;
use walshadow::ch_emitter::TableTarget;
use walshadow::shadow::Shadow;
use walshadow::shadow_catalog::RelName;

// walsender must clear ch_http by >1 (CH binds interserver = ch_http + 1).
const SLOT: PortSlot = PortSlot {
    source: 17750,
    shadow: 17751,
    ch_tcp: 17752,
    ch_http: 17753,
    walsender: 17757,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

const SCHEMA_SQL: &str = "CREATE SCHEMA w;\n\
    CREATE TABLE w.\"table\" (id int PRIMARY KEY, val text);\n\
    CREATE TABLE w.\"MixedCase\" (id int PRIMARY KEY, val text);\n\
    CREATE TABLE w.\"weird-%name\" (id int PRIMARY KEY, val text);\n\
    CREATE TABLE w.cols (id int PRIMARY KEY, \"has space?\" text, \"Dash-Col\" text);\n";

fn mappings() -> Vec<fx::TableMappingSpec> {
    let idval = |t: &str| fx::TableMappingSpec {
        source_table: RelName::new("w", t),
        target_table: TableTarget::new(
            "walshadow_test",
            &format!("w_{}", t.replace(['-', '%'], "_")),
        ),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "val".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    };
    vec![
        idval("table"),
        idval("MixedCase"),
        idval("weird-%name"),
        fx::TableMappingSpec {
            source_table: RelName::new("w", "cols"),
            target_table: TableTarget::new("walshadow_test", "w_cols"),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int32".into(),
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "has space?".into(),
                    target_type: "Nullable(String)".into(),
                },
                ColumnMapping {
                    src_attnum: 3,
                    target_name: "Dash-Col".into(),
                    target_type: "Nullable(String)".into(),
                },
            ],
        },
    ]
}

fn create_ch_dests(ch: &fx::ChServer) {
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    for t in ["w_table", "w_MixedCase", "w_weird__name"] {
        ch.query(&format!(
            "CREATE OR REPLACE TABLE walshadow_test.`{t}` (\
                id Int32, val Nullable(String),\
                _lsn UInt64, _xid UInt32,\
                _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
             ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id"
        ))
        .expect("create dest");
    }
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.w_cols (\
            id Int32, `has space?` Nullable(String), `Dash-Col` Nullable(String),\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create w_cols dest");
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

fn skip_gate() -> bool {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return true;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weird_table_and_column_names_replicate() {
    if skip_gate() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, SCHEMA_SQL, SLOT.source, SLOT.shadow, SLOT.walsender).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, SLOT.ch_tcp, SLOT.ch_http).expect("spawn ch");
    create_ch_dests(&ch);

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: SLOT.ch_tcp,
        mappings: mappings(),
        app_name: "walshadow-weird-idents",
        ddl: None,
    })
    .await;

    let driver = spawn_txn(
        &source,
        "INSERT INTO w.\"table\" VALUES (1, 'kw');\n\
         INSERT INTO w.\"MixedCase\" VALUES (1, 'mc');\n\
         INSERT INTO w.\"weird-%name\" VALUES (1, 'dash');\n\
         INSERT INTO w.cols VALUES (1, 'spaceval', 'dashval');\n\
         SELECT pg_switch_wal();\n",
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    for (tbl, want) in [
        ("w_table", "kw"),
        ("w_MixedCase", "mc"),
        ("w_weird__name", "dash"),
    ] {
        assert_eq!(
            ch.query(&format!(
                "SELECT val FROM walshadow_test.`{tbl}` FINAL WHERE id = 1 AND _is_deleted = 0"
            ))
            .unwrap(),
            want,
            "table `{tbl}` replicated",
        );
    }

    assert_eq!(
        ch.query(
            "SELECT concat(`has space?`, '|', `Dash-Col`) \
             FROM walshadow_test.w_cols FINAL WHERE id = 1 AND _is_deleted = 0"
        )
        .unwrap(),
        "spaceval|dashval",
        "special-char columns replicated",
    );
}
