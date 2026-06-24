//! Soft-delete CDC edge cases, end-to-end.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::process::{Command, Stdio};
use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;
use walshadow::shadow::Shadow;

// walsender must clear ch_http by >1 (CH binds interserver = ch_http + 1).
const SLOT_IUD: PortSlot = PortSlot {
    source: 17720,
    shadow: 17721,
    ch_tcp: 17722,
    ch_http: 17723,
    walsender: 17727,
};
const SLOT_UD: PortSlot = PortSlot {
    source: 17730,
    shadow: 17731,
    ch_tcp: 17732,
    ch_http: 17733,
    walsender: 17737,
};
const SLOT_RESURRECT: PortSlot = PortSlot {
    source: 17740,
    shadow: 17741,
    ch_tcp: 17742,
    ch_http: 17743,
    walsender: 17747,
};
const SLOT_BIGXACT: PortSlot = PortSlot {
    source: 18000,
    shadow: 18001,
    ch_tcp: 18002,
    ch_http: 18003,
    walsender: 18007,
};
const SLOT_BASIC: PortSlot = PortSlot {
    source: 17800,
    shadow: 17801,
    ch_tcp: 17802,
    ch_http: 17803,
    walsender: 17807,
};
const SLOT_META: PortSlot = PortSlot {
    source: 17810,
    shadow: 17811,
    ch_tcp: 17812,
    ch_http: 17813,
    walsender: 17817,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

const SCHEMA_SQL: &str = "CREATE SCHEMA sd;\n\
    CREATE TABLE sd.t (id int PRIMARY KEY, val text);\n\
    ALTER TABLE sd.t REPLICA IDENTITY FULL;\n";

fn mapping() -> Vec<fx::TableMappingSpec> {
    vec![fx::TableMappingSpec {
        source_table: "sd.t".into(),
        target_table: "walshadow_test.sd_t".into(),
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
    }]
}

fn create_ch_dest(ch: &fx::ChServer) {
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.sd_t (\
            id Int32,\
            val Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
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
    run_drill_with(slot, app_name, workload, |_| {}).await
}

async fn run_drill_with(
    slot: PortSlot,
    app_name: &str,
    workload: &str,
    tune: impl FnOnce(&mut walshadow::ch_emitter::EmitterConfig),
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

    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
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
        },
        tune,
    )
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

fn live_count(ch: &fx::ChServer) -> String {
    ch.query("SELECT count() FROM walshadow_test.sd_t FINAL WHERE _is_deleted = 0")
        .expect("ch live count")
}

fn winning_flag(ch: &fx::ChServer, id: i32) -> String {
    ch.query(&format!(
        "SELECT argMax(_is_deleted, _lsn) FROM walshadow_test.sd_t WHERE id = {id}"
    ))
    .expect("ch winning _is_deleted")
}

fn skip_gate() -> bool {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return true;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iud_same_batch_collapses_to_tombstone() {
    if skip_gate() {
        return;
    }
    let workload = "BEGIN;\n\
        INSERT INTO sd.t VALUES (1, 'a');\n\
        UPDATE sd.t SET val = 'b' WHERE id = 1;\n\
        DELETE FROM sd.t WHERE id = 1;\n\
        COMMIT;\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) = run_drill(SLOT_IUD, "walshadow-sd-iud", workload).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(source.psql_one("SELECT count(*) FROM sd.t").unwrap(), "0");
    assert_eq!(live_count(&ch), "0");
    assert_eq!(winning_flag(&ch, 1), "true", "ends as a tombstone");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ud_same_batch_collapses_to_tombstone() {
    if skip_gate() {
        return;
    }
    let workload = "INSERT INTO sd.t VALUES (1, 'a');\n\
        BEGIN;\n\
        UPDATE sd.t SET val = 'b' WHERE id = 1;\n\
        UPDATE sd.t SET val = 'c' WHERE id = 1;\n\
        DELETE FROM sd.t WHERE id = 1;\n\
        COMMIT;\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) = run_drill(SLOT_UD, "walshadow-sd-ud", workload).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(source.psql_one("SELECT count(*) FROM sd.t").unwrap(), "0");
    assert_eq!(live_count(&ch), "0");
    assert_eq!(winning_flag(&ch, 1), "true", "ends as a tombstone");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_after_delete_resurrects_key() {
    if skip_gate() {
        return;
    }
    let workload = "INSERT INTO sd.t VALUES (1, 'a');\n\
        DELETE FROM sd.t WHERE id = 1;\n\
        INSERT INTO sd.t VALUES (1, 'z');\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) = run_drill(SLOT_RESURRECT, "walshadow-sd-resurrect", workload).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(source.psql_one("SELECT count(*) FROM sd.t").unwrap(), "1");
    assert_eq!(live_count(&ch), "1");
    assert_eq!(
        winning_flag(&ch, 1),
        "false",
        "re-insert overrides the tombstone",
    );
    assert_eq!(
        ch.query("SELECT argMax(toString(val), _lsn) FROM walshadow_test.sd_t WHERE id = 1")
            .unwrap(),
        "z",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_insert_update_delete_ends_as_tombstone() {
    if skip_gate() {
        return;
    }
    let workload = "INSERT INTO sd.t VALUES (1, 'a');\n\
        UPDATE sd.t SET val = 'b' WHERE id = 1;\n\
        DELETE FROM sd.t WHERE id = 1;\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) = run_drill(SLOT_BASIC, "walshadow-sd-basic", workload).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(source.psql_one("SELECT count(*) FROM sd.t").unwrap(), "0");
    assert_eq!(live_count(&ch), "0");
    assert_eq!(winning_flag(&ch, 1), "true", "ends as a tombstone");
    assert_eq!(
        ch.query("SELECT argMax(toString(val), _lsn) FROM walshadow_test.sd_t WHERE id = 1")
            .unwrap(),
        "b",
        "tombstone retains last-known value",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_single_xact_triggers_mid_loop_chunk_flush() {
    if skip_gate() {
        return;
    }
    let workload = "BEGIN;\n\
        INSERT INTO sd.t (id, val) SELECT g, 'v' FROM generate_series(1, 40) AS g;\n\
        COMMIT;\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) =
        run_drill_with(SLOT_BIGXACT, "walshadow-sd-bigxact", workload, |cfg| {
            cfg.decode_chunk_rows = 16;
        })
        .await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(source.psql_one("SELECT count(*) FROM sd.t").unwrap(), "40");
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.sd_t FINAL WHERE _is_deleted = 0")
            .unwrap(),
        "40",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_columns_are_populated() {
    if skip_gate() {
        return;
    }
    let workload = "INSERT INTO sd.t VALUES (1, 'a');\n\
        DELETE FROM sd.t WHERE id = 1;\n\
        INSERT INTO sd.t VALUES (2, 'b');\n\
        SELECT pg_switch_wal();\n";
    let (source, ch, _tmp) = run_drill(SLOT_META, "walshadow-sd-meta", workload).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(winning_flag(&ch, 1), "true", "deleted key is a tombstone");
    assert_eq!(winning_flag(&ch, 2), "false", "live key not deleted");

    assert_eq!(
        ch.query(
            "SELECT count() FROM walshadow_test.sd_t \
             WHERE _commit_ts > '2020-01-01 00:00:00' AND _xid > 0 AND _lsn > 0"
        )
        .unwrap(),
        ch.query("SELECT count() FROM walshadow_test.sd_t").unwrap(),
        "every row carries non-zero _commit_ts/_xid/_lsn",
    );
}
