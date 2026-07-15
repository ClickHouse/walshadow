//! Composite primary key, end-to-end.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::process::{Command, Stdio};
use std::time::Duration;

use walshadow::mapping::ColumnMapping;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

// walsender must clear ch_http by >1 (CH binds interserver = ch_http + 1).
const SLOT_BASIC: PortSlot = PortSlot {
    source: 17700,
    shadow: 17701,
    ch_tcp: 17702,
    ch_http: 17703,
    walsender: 17707,
};
const SLOT_TOAST: PortSlot = PortSlot {
    source: 17710,
    shadow: 17711,
    ch_tcp: 17712,
    ch_http: 17713,
    walsender: 17717,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

const SCHEMA_SQL: &str = "CREATE SCHEMA ck;\n\
    CREATE TABLE ck.t (a int, b int, val text, PRIMARY KEY (a, b));\n\
    ALTER TABLE ck.t REPLICA IDENTITY FULL;\n";

fn mapping() -> Vec<fx::TableMappingSpec> {
    vec![fx::TableMappingSpec {
        source_table: RelName::new("ck", "t"),
        target_table: TableTarget::new("walshadow_test", "ck_t"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "a".into(),
                target_type: "Int32".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "b".into(),
                target_type: "Int32".into(),
            },
            ColumnMapping {
                src_attnum: 3,
                target_name: "val".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    }]
}

fn create_ch_dest(ch: &fx::ChServer) {
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.ck_t (\
            a Int32,\
            b Int32,\
            val Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY (a, b)",
    )
    .expect("create dest table");
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

async fn run_drill(
    slot: PortSlot,
    app_name: &str,
    workload: &str,
) -> (Shadow, fx::ChServer, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, SCHEMA_SQL, slot.source, slot.shadow, slot.walsender).await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    create_ch_dest(&ch);

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: mapping(),
        app_name,
        ddl: None,
    })
    .await;

    let driver = spawn_txn(&source, workload);
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s ({app_name})");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");
    let _ = shadow.stop();

    (source, ch, tmp)
}

fn live_pairs(ch: &fx::ChServer) -> String {
    ch.query(
        "SELECT arrayStringConcat(\
            groupArray(concat(toString(a), '/', toString(b), '=', toString(val))), ','\
         ) FROM (\
             SELECT a, b, argMax(val, _lsn) AS val \
             FROM walshadow_test.ck_t \
             WHERE _is_deleted = 0 \
             GROUP BY a, b ORDER BY a, b\
         )",
    )
    .expect("ch live pairs")
}

fn skip_gate() -> bool {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return true;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_pk_dedup_keys_on_full_tuple() {
    if skip_gate() {
        return;
    }
    let workload = "BEGIN;\n\
        INSERT INTO ck.t VALUES (1, 1, 'a11'), (1, 2, 'a12'), (2, 1, 'a21');\n\
        UPDATE ck.t SET val = 'A12' WHERE a = 1 AND b = 2;\n\
        DELETE FROM ck.t WHERE a = 2 AND b = 1;\n\
        COMMIT;\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) = run_drill(SLOT_BASIC, "walshadow-composite-pk-basic", workload).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(
        source.psql_one("SELECT count(*) FROM ck.t").unwrap(),
        "2",
        "source holds (1,1) + (1,2) post-delete",
    );
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.ck_t FINAL WHERE _is_deleted = 0")
            .unwrap(),
        "2",
    );
    assert_eq!(live_pairs(&ch), "1/1=a11,1/2=A12");
    assert_eq!(
        ch.query("SELECT argMax(_is_deleted, _lsn) FROM walshadow_test.ck_t WHERE a = 2 AND b = 1")
            .unwrap(),
        "true",
        "(2,1) coded as a tombstone",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_pk_with_toast_value() {
    if skip_gate() {
        return;
    }
    // >8KB values force out-of-line TOAST under the composite key.
    let workload = "BEGIN;\n\
        INSERT INTO ck.t VALUES (1, 1, repeat('x', 12000)), (1, 2, repeat('y', 9000));\n\
        COMMIT;\n\
        UPDATE ck.t SET val = repeat('z', 11000) WHERE a = 1 AND b = 2;\n\
        DELETE FROM ck.t WHERE a = 1 AND b = 1;\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) = run_drill(SLOT_TOAST, "walshadow-composite-pk-toast", workload).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(
        source.psql_one("SELECT count(*) FROM ck.t").unwrap(),
        "1",
        "source holds only (1,2) post-delete",
    );
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.ck_t FINAL WHERE _is_deleted = 0")
            .unwrap(),
        "1",
    );
    assert_eq!(
        ch.query(
            "SELECT concat(toString(length(val)), ':', substring(val, 1, 1)) \
             FROM walshadow_test.ck_t FINAL WHERE a = 1 AND b = 2 AND _is_deleted = 0"
        )
        .unwrap(),
        "11000:z",
        "TOAST value round-trips after UPDATE",
    );
}
