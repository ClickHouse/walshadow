//! Pinned-table DDL baseline — `ALTER ADD COLUMN` on an operator-pinned
//! relation propagates to ClickHouse without any priming DML.
//!
//! The docker-demo scenario (`docker/DEMO.md`): `demo.users` is pinned in
//! `ch-config`, gets no traffic, then the presenter runs `ALTER TABLE
//! demo.users ADD COLUMN signup_ts …`. Pre-fix, the very first descriptor
//! fetch already carried the post-ALTER shape, `prev_known` was cold for
//! the oid → `Added` → `apply_added` skips the pinned dest → CH never grew
//! the column. The startup `seed_baseline` warms `prev_known` with the
//! boot shape, so the first ALTER diffs as `Changed` and the column lands.
//!
//! Two drills:
//!
//! 1. `pinned_alter_add_column_replicates_without_priming_dml`
//!    * Pinned `demo.users` (id/name/email), NO DML before the ALTER.
//!    * `ALTER ADD COLUMN signup_ts timestamptz`, then one INSERT.
//!    * Expect: CH grows `signup_ts`, the post-ALTER value lands.
//!
//! 2. `pinned_subset_alter_adds_only_new_column`
//!    * Source has an extra `internal_notes` column the operator did NOT
//!      pin; CH dest omits it deliberately.
//!    * `ALTER ADD COLUMN signup_ts`, then one INSERT touching all cols.
//!    * Expect: CH gains `signup_ts` and NOT `internal_notes` — the
//!      pinned-subset footgun guard. The full-source baseline separates
//!      "excluded by the operator" from "appeared since agreement".

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;

// +0 / +10 shift per test. CH interserver_http_port = http_port + 1, so
// leave a 5-port gap between CH_HTTP_PORT and WALSENDER_PORT.
const SOURCE_PORT: u16 = 17541;
const SHADOW_PORT: u16 = 17542;
const CH_TCP_PORT: u16 = 17543;
const CH_HTTP_PORT: u16 = 17544;
const WALSENDER_PORT: u16 = 17548;

fn skip_if_missing() -> bool {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return true;
    }
    false
}

/// Synthetic-column tail every CH dest carries.
const SYNTHETIC_TAIL: &str = "_lsn UInt64,\
     _xid UInt32,\
     _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
     _commit_ts DateTime64(6, 'UTC')";

/// id/name/email mapping shared by both drills.
fn base_columns() -> Vec<ColumnMapping> {
    vec![
        ColumnMapping {
            src_attnum: 1,
            target_name: "id".into(),
            target_type: "Int64".into(),
        },
        ColumnMapping {
            src_attnum: 2,
            target_name: "name".into(),
            target_type: "Nullable(String)".into(),
        },
        ColumnMapping {
            src_attnum: 3,
            target_name: "email".into(),
            target_type: "Nullable(String)".into(),
        },
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_alter_add_column_replicates_without_priming_dml() {
    if skip_if_missing() {
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
        "CREATE SCHEMA demo;\n\
         CREATE TABLE demo.users (\n\
            id    bigint PRIMARY KEY,\n\
            name  text,\n\
            email text\n\
         );\n\
         ALTER TABLE demo.users REPLICA IDENTITY FULL;\n",
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
    // Pinned dest holds only id/name/email — signup_ts does NOT exist
    // here. The applicator must add it off the seeded-baseline diff.
    ch.query(&format!(
        "CREATE OR REPLACE TABLE walshadow_test.demo_users (\
            id Int64,\
            name Nullable(String),\
            email Nullable(String),\
            {SYNTHETIC_TAIL}\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    ))
    .expect("create dest");

    let mappings = vec![fx::TableMappingSpec {
        source_table: "demo.users".into(),
        target_table: "walshadow_test.demo_users".into(),
        columns: base_columns(),
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
        app_name: "walshadow-pinned-ddl",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    // No DML before the ALTER — the seed is the only thing that put a
    // baseline shape in front of the first descriptor fetch.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "ALTER TABLE demo.users ADD COLUMN signup_ts timestamptz".into(),
            "INSERT INTO demo.users (id, name, email, signup_ts) \
             VALUES (1, 'alice', 'alice@example.com', '2026-06-03 12:00:00+00')"
                .into(),
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

    // CH grew the column off the first ALTER, with no priming DML.
    let col = ch
        .query(
            "SELECT name FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 'demo_users' AND name = 'signup_ts'",
        )
        .expect("ch system.columns");
    assert_eq!(col, "signup_ts", "applicator must have added `signup_ts`");

    let n = ch
        .query("SELECT count() FROM walshadow_test.demo_users FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(n, "1");

    // Post-ALTER value reached CH (non-NULL).
    let v = ch
        .query(
            "SELECT argMax(signup_ts, _lsn) FROM walshadow_test.demo_users \
             WHERE _op != 'delete' AND id = 1",
        )
        .expect("ch signup_ts value");
    assert!(
        !v.is_empty() && v != "\\N" && !v.starts_with("1970-01-01"),
        "post-ALTER signup_ts must land on CH, got {v:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_subset_alter_adds_only_new_column() {
    if skip_if_missing() {
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
        // internal_notes (attnum 4) is in the source but NOT pinned.
        "CREATE SCHEMA demo;\n\
         CREATE TABLE demo.users (\n\
            id             bigint PRIMARY KEY,\n\
            name           text,\n\
            email          text,\n\
            internal_notes text\n\
         );\n\
         ALTER TABLE demo.users REPLICA IDENTITY FULL;\n",
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
    // Dest deliberately omits `internal_notes`. The diff against the
    // full-source baseline must add only `signup_ts`, never re-add the
    // operator-excluded column.
    ch.query(&format!(
        "CREATE OR REPLACE TABLE walshadow_test.demo_users (\
            id Int64,\
            name Nullable(String),\
            email Nullable(String),\
            {SYNTHETIC_TAIL}\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    ))
    .expect("create dest");

    let mappings = vec![fx::TableMappingSpec {
        source_table: "demo.users".into(),
        target_table: "walshadow_test.demo_users".into(),
        // id/name/email only — internal_notes (attnum 4) left unmapped.
        columns: base_columns(),
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings,
        app_name: "walshadow-pinned-subset",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    // signup_ts becomes attnum 5 (internal_notes is attnum 4).
    let driver = fx::spawn_workload(
        &source,
        vec![
            "ALTER TABLE demo.users ADD COLUMN signup_ts timestamptz".into(),
            "INSERT INTO demo.users (id, name, email, internal_notes, signup_ts) \
             VALUES (1, 'bob', 'bob@example.com', 'secret', '2026-06-03 12:00:00+00')"
                .into(),
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

    // signup_ts added…
    let added = ch
        .query(
            "SELECT name FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 'demo_users' AND name = 'signup_ts'",
        )
        .expect("ch system.columns signup_ts");
    assert_eq!(added, "signup_ts", "must add the new column");

    // …and the operator-excluded column must NOT appear.
    let excluded = ch
        .query(
            "SELECT count() FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 'demo_users' AND name = 'internal_notes'",
        )
        .expect("ch system.columns internal_notes");
    assert_eq!(
        excluded, "0",
        "operator-excluded internal_notes must not be re-added to CH",
    );

    let n = ch
        .query("SELECT count() FROM walshadow_test.demo_users FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(n, "1");

    let v = ch
        .query(
            "SELECT argMax(signup_ts, _lsn) FROM walshadow_test.demo_users \
             WHERE _op != 'delete' AND id = 1",
        )
        .expect("ch signup_ts value");
    assert!(
        !v.is_empty() && v != "\\N" && !v.starts_with("1970-01-01"),
        "post-ALTER signup_ts must land on CH, got {v:?}",
    );
}
