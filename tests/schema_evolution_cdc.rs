//! Schema-evolution CDC correctness, end-to-end. DROP/RENAME COLUMN
//! propagation is unimplemented.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::mapping::NamespaceMapping;
use walshadow::shadow::Shadow;

fn skip_gate() -> bool {
    if !fx::requirements_available() {
        return true;
    }
    false
}

async fn run(
    slot: fx::Ports,
    ns: &str,
    app_name: &str,
    schema_sql: &str,
    stmts: Vec<String>,
    segments: u64,
) -> (Shadow, Shadow, fx::ChServer, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, schema_sql, slot.source, slot.shadow, slot.walsender).await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        ns.into(),
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
        app_name,
        ddl: Some(ddl_args),
    })
    .await;

    let driver = fx::spawn_workload(&source, stmts);
    let shipped = fx::pump_segments(&mut pipeline, segments, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(
        shipped >= segments,
        "expected ≥{segments} segments ({app_name})"
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    (source, shadow, ch, tmp)
}

fn has_column(ch: &fx::ChServer, table: &str, col: &str) -> bool {
    ch.query(&format!(
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = '{table}' AND name = '{col}'"
    ))
    .expect("ch system.columns")
        == "1"
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn added_column_without_dml_visible_next_batch() {
    if skip_gate() {
        return;
    }
    let (source, shadow, ch, _tmp) = run(
        fx::Ports::alloc(),
        "se_lost",
        "walshadow-se-lost",
        "CREATE SCHEMA se_lost;\n",
        vec![
            "CREATE TABLE se_lost.t (id bigint PRIMARY KEY, a text)".into(),
            "INSERT INTO se_lost.t (id, a) VALUES (1, 'x')".into(),
            "SELECT pg_switch_wal()".into(),
            "ALTER TABLE se_lost.t ADD COLUMN b text".into(),
            "SELECT pg_switch_wal()".into(),
            "INSERT INTO se_lost.t (id, a, b) VALUES (2, 'y', 'z')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
        3,
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };
    let _shd = fx::StopOnDrop { sh: &shadow };

    assert!(has_column(&ch, "t", "b"), "added column must reach CH");
    assert_eq!(
        ch.query("SELECT argMax(b, _lsn) FROM walshadow_test.t WHERE id = 2 AND _is_deleted = 0")
            .unwrap(),
        "z",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_change_on_one_table_spares_sibling() {
    if skip_gate() {
        return;
    }
    let (source, shadow, ch, _tmp) = run(
        fx::Ports::alloc(),
        "se_cut",
        "walshadow-se-cutoff",
        "CREATE SCHEMA se_cut;\n",
        vec![
            "CREATE TABLE se_cut.t1 (id bigint PRIMARY KEY, a text)".into(),
            "CREATE TABLE se_cut.t2 (id bigint PRIMARY KEY, a text)".into(),
            "INSERT INTO se_cut.t1 (id, a) VALUES (1, 'one')".into(),
            "INSERT INTO se_cut.t2 (id, a) VALUES (1, 'one')".into(),
            "SELECT pg_switch_wal()".into(),
            "ALTER TABLE se_cut.t1 ADD COLUMN b text".into(),
            "INSERT INTO se_cut.t1 (id, a, b) VALUES (2, 'two', 'extra')".into(),
            "INSERT INTO se_cut.t2 (id, a) VALUES (2, 'two')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
        2,
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };
    let _shd = fx::StopOnDrop { sh: &shadow };

    assert!(has_column(&ch, "t1", "b"), "altered table gains b");
    assert!(!has_column(&ch, "t2", "b"), "sibling stays unaltered");
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.t2 FINAL WHERE _is_deleted = 0")
            .unwrap(),
        "2",
    );
    assert_eq!(
        ch.query("SELECT argMax(b, _lsn) FROM walshadow_test.t1 WHERE id = 2 AND _is_deleted = 0")
            .unwrap(),
        "extra",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nullable_add_column_under_replident_full() {
    if skip_gate() {
        return;
    }
    let (source, shadow, ch, _tmp) = run(
        fx::Ports::alloc(),
        "se_null",
        "walshadow-se-nullable",
        "CREATE SCHEMA se_null;\n",
        vec![
            "CREATE TABLE se_null.t (id bigint PRIMARY KEY, a text)".into(),
            "ALTER TABLE se_null.t REPLICA IDENTITY FULL".into(),
            "INSERT INTO se_null.t (id, a) VALUES (1, 'pre')".into(),
            "SELECT pg_switch_wal()".into(),
            "ALTER TABLE se_null.t ADD COLUMN b text".into(),
            "INSERT INTO se_null.t (id, a, b) VALUES (2, 'has', 'val')".into(),
            "INSERT INTO se_null.t (id, a, b) VALUES (3, 'none', NULL)".into(),
            "SELECT pg_switch_wal()".into(),
        ],
        2,
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };
    let _shd = fx::StopOnDrop { sh: &shadow };

    assert!(has_column(&ch, "t", "b"));
    assert_eq!(
        ch.query(
            "SELECT argMax(ifNull(b, '<null>'), _lsn) FROM walshadow_test.t \
             WHERE id = 1 AND _is_deleted = 0"
        )
        .unwrap(),
        "<null>",
        "pre-ALTER row reads NULL",
    );
    assert_eq!(
        ch.query("SELECT argMax(b, _lsn) FROM walshadow_test.t WHERE id = 2 AND _is_deleted = 0")
            .unwrap(),
        "val",
    );
    assert_eq!(
        ch.query(
            "SELECT argMax(ifNull(b, '<null>'), _lsn) FROM walshadow_test.t \
             WHERE id = 3 AND _is_deleted = 0"
        )
        .unwrap(),
        "<null>",
        "explicit NULL stays NULL",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nullable_add_column_under_replident_index() {
    if skip_gate() {
        return;
    }
    let (source, shadow, ch, _tmp) = run(
        fx::Ports::alloc(),
        "se_idx",
        "walshadow-se-nullable-idx",
        "CREATE SCHEMA se_idx;\n",
        vec![
            "CREATE TABLE se_idx.t (id bigint NOT NULL, a text)".into(),
            "CREATE UNIQUE INDEX t_id_uidx ON se_idx.t (id)".into(),
            "ALTER TABLE se_idx.t REPLICA IDENTITY USING INDEX t_id_uidx".into(),
            "INSERT INTO se_idx.t (id, a) VALUES (1, 'pre')".into(),
            "SELECT pg_switch_wal()".into(),
            "ALTER TABLE se_idx.t ADD COLUMN b text".into(),
            "INSERT INTO se_idx.t (id, a, b) VALUES (2, 'has', 'val')".into(),
            "INSERT INTO se_idx.t (id, a, b) VALUES (3, 'none', NULL)".into(),
            "SELECT pg_switch_wal()".into(),
        ],
        2,
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };
    let _shd = fx::StopOnDrop { sh: &shadow };

    assert!(has_column(&ch, "t", "b"));
    assert_eq!(
        ch.query("SELECT argMax(b, _lsn) FROM walshadow_test.t WHERE id = 2 AND _is_deleted = 0")
            .unwrap(),
        "val",
    );
    assert_eq!(
        ch.query(
            "SELECT argMax(ifNull(b, '<null>'), _lsn) FROM walshadow_test.t \
             WHERE id = 3 AND _is_deleted = 0"
        )
        .unwrap(),
        "<null>",
        "explicit NULL stays NULL",
    );
}
