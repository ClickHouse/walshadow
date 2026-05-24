//! DDL replicates end-to-end.
//!
//! Three drills:
//!
//! 1. `alter_add_column_replicates_without_toml_edit`
//!    * Source table pre-mapped in TOML with two columns.
//!    * Source ALTER TABLE ADD COLUMN c text — *not* declared in TOML.
//!    * Followed by INSERT into the post-ALTER shape.
//!    * Expect: applicator runs `ALTER TABLE … ADD COLUMN c String`
//!      against CH, auto-extends the mapping, and emits the new row
//!      with `c` populated. No CH DDL by the operator.
//!
//! 2. `create_table_auto_replicates_in_namespace`
//!    * Namespace `s15` flagged `auto_create = true`.
//!    * Source CREATE TABLE s15.new_t — no TOML entry, no CH DDL.
//!    * Followed by INSERT.
//!    * Expect: applicator runs `CREATE TABLE … walshadow_test.new_t`
//!      and the row lands in CH.
//!
//! 3. `drop_table_strategy_drop_removes_dest`
//!    * Auto-created table seeded with rows, then DROP TABLE on source.
//!    * Strategy = "drop" → CH dest disappears.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::ch_emitter::{ColumnMapping, NamespaceMapping};

// Each test shifts these by +0 / +10 / +20. The CH server's
// `interserver_http_port = http_port + 1` so leave a 5-port gap
// between CH_HTTP_PORT and WALSENDER_PORT to avoid collision.
const SOURCE_PORT: u16 = 17461;
const SHADOW_PORT: u16 = 17462;
const CH_TCP_PORT: u16 = 17463;
const CH_HTTP_PORT: u16 = 17464;
const WALSENDER_PORT: u16 = 17468;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_add_column_replicates_without_toml_edit() {
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
        "CREATE SCHEMA s15;\n\
         CREATE TABLE s15.orders (id bigint PRIMARY KEY, payload text);\n\
         ALTER TABLE s15.orders REPLICA IDENTITY FULL;\n",
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
    // Pre-create CH dest with only the original two columns. Note: the
    // `c` column does NOT exist here — the applicator will add it when
    // the ALTER event drains.
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.s15_orders (\
            id Int64,\
            payload Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest");

    let mappings = vec![fx::TableMappingSpec {
        source_table: "s15.orders".into(),
        target_table: "walshadow_test.s15_orders".into(),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "payload".into(),
                target_type: "Nullable(String)".into(),
            },
            // No `c` here — applicator must auto-extend.
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
        app_name: "walshadow-ddl-alter-add",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO s15.orders (id, payload) VALUES (1, 'pre')".into(),
            "ALTER TABLE s15.orders ADD COLUMN c text".into(),
            "INSERT INTO s15.orders (id, payload, c) VALUES (2, 'post', 'hello')".into(),
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
    assert!(observed >= target);

    // Verify CH gained the `c` column (Nullable(String) by default).
    let cols = ch
        .query("SELECT name FROM system.columns WHERE database = 'walshadow_test' AND table = 's15_orders' AND name = 'c'")
        .expect("ch system.columns");
    assert_eq!(cols, "c", "applicator must have added column `c`");

    // Verify row count + the post-ALTER `c` value.
    let src_count = source.psql_one("SELECT count(*) FROM s15.orders").unwrap();
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s15_orders FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(src_count, ch_count);
    assert_eq!(src_count, "2");

    let ch_post = ch
        .query(
            "SELECT argMax(c, _lsn) \
             FROM walshadow_test.s15_orders \
             WHERE _op != 'delete' AND id = 2",
        )
        .expect("ch post-alter c");
    assert_eq!(ch_post, "hello", "post-ALTER row's c must reach CH");

    // Pre-ALTER row has no `c` value in the source heap — surfaces as
    // NULL on CH (no DEFAULT was supplied to ALTER, so PG didn't write
    // an attmissingval).
    let ch_pre = ch
        .query(
            "SELECT argMax(c, _lsn) \
             FROM walshadow_test.s15_orders \
             WHERE _op != 'delete' AND id = 1",
        )
        .expect("ch pre-alter c");
    assert!(
        ch_pre.is_empty() || ch_pre == "\\N" || ch_pre == "NULL",
        "pre-ALTER row's c should be NULL on CH, got {ch_pre:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_auto_replicates_in_namespace() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 10;
    let shadow_port = SHADOW_PORT + 10;
    let ch_tcp_port = CH_TCP_PORT + 10;
    let ch_http_port = CH_HTTP_PORT + 10;
    let walsender_port = WALSENDER_PORT + 10;
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA s15ns;\n",
        source_port,
        shadow_port,
        walsender_port,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    // No per-table mapping; namespace block alone authorises auto_create.
    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "s15ns".into(),
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
        ch_tcp_port,
        mappings: vec![],
        app_name: "walshadow-ddl-create-auto",
        ddl: Some(ddl_args),
    })
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "CREATE TABLE s15ns.new_t (id bigint PRIMARY KEY, body text)".into(),
            "INSERT INTO s15ns.new_t (id, body) VALUES (1, 'auto')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 60s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);

    // CH dest should exist and contain the row.
    let tbls = ch
        .query(
            "SELECT name FROM system.tables WHERE database = 'walshadow_test' AND name = 'new_t'",
        )
        .expect("ch table existence");
    assert_eq!(tbls, "new_t", "applicator must have auto-created CH table");

    let n = ch
        .query("SELECT count() FROM walshadow_test.new_t FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(n, "1");

    let body = ch
        .query(
            "SELECT argMax(body, _lsn) FROM walshadow_test.new_t \
             WHERE _op != 'delete' AND id = 1",
        )
        .expect("ch body");
    assert_eq!(body, "auto");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_table_strategy_drop_removes_dest() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 20;
    let shadow_port = SHADOW_PORT + 20;
    let ch_tcp_port = CH_TCP_PORT + 20;
    let ch_http_port = CH_HTTP_PORT + 20;
    let walsender_port = WALSENDER_PORT + 20;
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA s15drop;\n",
        source_port,
        shadow_port,
        walsender_port,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "s15drop".into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            drop_table_strategy: None,
        },
    );
    ddl_args.drop_table_strategy = Some("drop".into());

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings: vec![],
        app_name: "walshadow-ddl-drop",
        ddl: Some(ddl_args),
    })
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "CREATE TABLE s15drop.gone (id bigint PRIMARY KEY)".into(),
            "INSERT INTO s15drop.gone (id) SELECT generate_series(1, 10)".into(),
            "SELECT pg_switch_wal()".into(),
            "DROP TABLE s15drop.gone".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    // Two segments because we sandwich the DROP with pg_switch_wal calls.
    let shipped = fx::pump_segments(&mut pipeline, 2, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 2, "expected ≥2 shipped segments, got {shipped}");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);

    let exists = ch
        .query(
            "SELECT count() FROM system.tables WHERE database = 'walshadow_test' AND name = 'gone'",
        )
        .expect("ch system.tables count");
    assert_eq!(
        exists, "0",
        "CH dest must be dropped under drop_table_strategy = drop"
    );
}
