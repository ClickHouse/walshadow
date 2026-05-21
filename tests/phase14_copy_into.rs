//! Phase 14 item 2 — `XLOG_HEAP2_MULTI_INSERT` per-tuple fan-out.
//!
//! Verifies that `COPY foo FROM stdin` rows reach CH. Pre-phase-14 the
//! decoder returned `Ok(None)` for every Heap2 multi-insert record,
//! dropping every row a COPY produced.
//!
//! Drill shape:
//!   1. CREATE TABLE s14.copy_t (id bigint PRIMARY KEY, name text)
//!   2. COPY s14.copy_t (id, name) FROM stdin           — 500 rows
//!   3. pg_switch_wal
//!
//! Expected: source count() == CH count() == 500, ids 1..=500 match.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::io::Write as _;
use std::process::{Command, Stdio};
use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;

const SOURCE_PORT: u16 = 17421;
const SHADOW_PORT: u16 = 17422;
const CH_TCP_PORT: u16 = 17423;
const CH_HTTP_PORT: u16 = 17424;
const WALSENDER_PORT: u16 = 17452;
const N_ROWS: u32 = 500;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase14_copy_into_multi_insert_replicates() {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !fx::pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
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
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA s14;\n\
         CREATE TABLE s14.copy_t (id bigint PRIMARY KEY, name text NOT NULL);\n\
         ALTER TABLE s14.copy_t REPLICA IDENTITY FULL;\n",
        SOURCE_PORT,
        SHADOW_PORT,
        WALSENDER_PORT,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.s14_copy_t (\
            id Int64,\
            name String,\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: "s14.copy_t".into(),
        target_table: "walshadow_test.s14_copy_t".into(),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "name".into(),
                target_type: "String".into(),
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
        ch_tcp_port: CH_TCP_PORT,
        mappings,
        app_name: "walshadow-phase14-copy",
    })
    .await;

    // Workload runs in its own thread: COPY ... FROM stdin pipes
    // `id\tname\n` rows into psql, then a separate `-c` rotates WAL.
    let driver = spawn_copy_workload(&source);

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(
        shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain",
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.copy_t")
        .expect("source count");
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s14_copy_t FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(src_count, ch_count, "row count after COPY mismatched");
    assert_eq!(src_count, N_ROWS.to_string());

    // md5(string_agg(name, ',' ORDER BY id)) must match across both
    // sides — proves every row's payload survived the multi-insert
    // fan-out, not just the count.
    let src_md5 = source
        .psql_one("SELECT md5(string_agg(name, ',' ORDER BY id)) FROM s14.copy_t")
        .expect("source md5");
    let ch_md5 = ch
        .query(
            "SELECT lower(hex(MD5(arrayStringConcat(groupArray(name), ',')))) FROM (\
                SELECT name FROM walshadow_test.s14_copy_t FINAL \
                WHERE _op != 'delete' ORDER BY id\
             )",
        )
        .expect("ch md5");
    assert_eq!(src_md5, ch_md5);
}

/// Drive `COPY ... FROM STDIN` through psql against the source PG.
/// Pipes N rows of `id\tname\n` then rotates WAL.
fn spawn_copy_workload(source: &walshadow::shadow::Shadow) -> std::thread::JoinHandle<()> {
    let sock = source.config().socket_dir.clone();
    let port = source.config().port;
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
                "-c",
                "COPY s14.copy_t (id, name) FROM stdin",
                "-c",
                "SELECT pg_switch_wal()",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn psql copy");
        {
            let stdin = child.stdin.as_mut().expect("stdin piped");
            for i in 1..=N_ROWS {
                writeln!(stdin, "{i}\trow-{i}").unwrap();
            }
            stdin.write_all(b"\\.\n").unwrap();
        }
        let _ = child.wait();
    })
}
