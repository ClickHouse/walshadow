//! Cross-database scope end-to-end. WAL is cluster-wide: a second source
//! database's DDL and DML ride the followed database's stream and must
//! replay on shadow, yet produce no descriptor capture, no CH table and no
//! CH rows. The same operations in the followed database then behave
//! normally, proving the gate rejects on database rather than on shape.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::NamespaceMapping;

const SOURCE_PORT: u16 = 17561;
const SHADOW_PORT: u16 = 17562;
const CH_TCP_PORT: u16 = 17563;
const CH_HTTP_PORT: u16 = 17564;
const WALSENDER_PORT: u16 = 17568;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_database_ddl_and_dml_never_reach_the_followed_output() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
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
        "CREATE SCHEMA fdb;\n\
         CREATE TABLE fdb.t (id bigint PRIMARY KEY, v text);\n",
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

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "fdb".into(),
        NamespaceMapping {
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
        ch_tcp_port: CH_TCP_PORT,
        mappings: vec![],
        app_name: "walshadow-foreign-db",
        ddl: Some(ddl_args),
    })
    .await;
    let log_stats = pipeline.desc_log.stats_handle();
    let batches_before = log_stats.batches_appended.load(Ordering::Relaxed);

    // Second database, same schema and table names as the followed one
    fx::spawn_workload(&source, vec!["CREATE DATABASE fdb_other".into()])
        .join()
        .expect("create database");
    let driver = fx::spawn_workload_in_db(
        &source,
        "fdb_other",
        vec![
            "CREATE SCHEMA fdb".into(),
            "CREATE TABLE fdb.t (id bigint PRIMARY KEY, v text)".into(),
            "CREATE TABLE fdb.foreign_only (id bigint PRIMARY KEY)".into(),
            "INSERT INTO fdb.t (id, v) VALUES (1, 'foreign')".into(),
            "INSERT INTO fdb.foreign_only (id) VALUES (1)".into(),
            "ALTER TABLE fdb.t ADD COLUMN extra text".into(),
            "TRUNCATE fdb.t".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target, "foreign records still replay on shadow");

    assert_eq!(
        log_stats.batches_appended.load(Ordering::Relaxed),
        batches_before,
        "foreign DDL must not capture descriptors",
    );
    assert_eq!(
        ch.query(
            "SELECT count() FROM system.tables \
             WHERE database = 'walshadow_test' AND name = 'foreign_only'",
        )
        .unwrap(),
        "0",
        "foreign CREATE TABLE must not auto-create a CH dest",
    );
    // `walshadow_test.t` is the followed table's dest, created at attach
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.t").unwrap(),
        "0",
        "identically named foreign table's rows must not land here",
    );
    assert_eq!(
        ch.query(
            "SELECT count() FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 't' AND name = 'extra'",
        )
        .unwrap(),
        "0",
        "foreign ALTER must not reshape the followed dest",
    );

    // Same statements in the followed database: normal output
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO fdb.t (id, v) VALUES (1, 'local')".into(),
            "ALTER TABLE fdb.t ADD COLUMN extra text".into(),
            "INSERT INTO fdb.t (id, v, extra) VALUES (2, 'post', 'e2')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    let target = pipeline.stream.dispatched_lsn();
    shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(
        log_stats.batches_appended.load(Ordering::Relaxed) > batches_before,
        "followed-database DDL still captures",
    );
    pipeline.shutdown().await.expect("pipeline drains clean");

    fx::wait_query(
        &ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=local,2=post",
        "followed-database rows deliver",
    )
    .await;
    assert_eq!(
        ch.query(
            "SELECT count() FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 't' AND name = 'extra'",
        )
        .unwrap(),
        "1",
        "followed-database ALTER reaches CH",
    );
}
