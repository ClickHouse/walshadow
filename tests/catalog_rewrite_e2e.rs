//! Non-mapped catalog rewrites mid-stream. `VACUUM FULL pg_depend` fills a
//! transient heap under a fresh filenode then swaps it in, `REINDEX` moves
//! each index to a fresh filenode; both pg_class updates prefix-compress the
//! OID away. Shadow must replay every write to those filenodes, else later
//! pg_depend writes reference pages shadow never saw.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::mapping::NamespaceMapping;

const PROBE: &str = "SET enable_seqscan = off; SET enable_bitmapscan = off; \
    SELECT count(*) FROM pg_depend \
    WHERE classid = 'pg_class'::regclass AND refobjid = 'crw.t'::regclass";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_rewrites_keep_shadow_replaying() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
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
        "CREATE SCHEMA crw;\n\
         CREATE TABLE crw.t (id bigint PRIMARY KEY, v text);\n",
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "crw".into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            drop_table_strategy: None,
            initial_load: None,
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
        app_name: "walshadow-catalog-rewrite",
        ddl: Some(ddl_args),
    })
    .await;

    // CHECKPOINT makes the next pg_class write carry a full-page image in
    // place of block data; the repeat REINDEX then prefix-compresses
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO crw.t (id, v) VALUES (1, 'pre')".into(),
            "VACUUM FULL pg_depend".into(),
            "REINDEX TABLE pg_depend".into(),
            "CHECKPOINT".into(),
            "VACUUM FULL pg_depend".into(),
            "CHECKPOINT".into(),
            "REINDEX INDEX pg_depend_reference_index".into(),
            "REINDEX INDEX pg_depend_reference_index".into(),
            "ALTER TABLE crw.t ADD COLUMN extra text".into(),
            "CREATE INDEX t_v_idx ON crw.t (v)".into(),
            "CREATE TABLE crw.u (id bigint PRIMARY KEY REFERENCES crw.t)".into(),
            "INSERT INTO crw.t (id, v, extra) VALUES (2, 'post', 'e2')".into(),
            "INSERT INTO crw.u (id) VALUES (2)".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    assert!(
        pipeline
            .stream
            .filter()
            .tracker()
            .stats()
            .pg_class_writes_oid_in_prefix
            > 0,
        "workload must exercise prefix-compressed pg_class updates",
    );
    let target = pipeline.stream.dispatched_lsn();
    shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow keeps replaying past catalog rewrites");

    let want = source.psql_one(PROBE).expect("source probe");
    let got = shadow.psql_one(PROBE).expect("shadow pg_depend index scan");
    assert_eq!(got, want, "shadow pg_depend matches source");
    let want = source
        .psql_one("SELECT count(*) FROM pg_depend")
        .expect("source count");
    let got = shadow
        .psql_one("SELECT count(*) FROM pg_depend")
        .expect("shadow pg_depend heap scan");
    assert_eq!(got, want, "shadow pg_depend heap matches source");
    pipeline.shutdown().await.expect("pipeline drains clean");

    fx::wait_query(
        &ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=pre,2=post",
        "rows deliver across catalog rewrites",
    )
    .await;
    assert_eq!(
        ch.query(
            "SELECT count() FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 't' AND name = 'extra'",
        )
        .unwrap(),
        "1",
        "ALTER after catalog rewrites reaches CH",
    );
}
