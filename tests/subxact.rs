//! Subxact lineage + `ROLLBACK TO SAVEPOINT`.
//!
//! Verifies that source `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` /
//! `RELEASE SAVEPOINT` and top-xact abort with subxacts replicate
//! correctly:
//!
//!   * `savepoint_rollback_discards_subxact_writes` — top
//!     INSERT survives; INSERT inside an aborted sub does not.
//!   * `savepoint_release_commits_subxact_writes` — RELEASE
//!     folds the sub into top so both INSERTs land.
//!   * `orm_nested_savepoints_only_post_savepoint_writes_survive`
//!     — repeated SAVEPOINT/ROLLBACK pattern (ORM-style) keeps only the
//!     top-level INSERTs.
//!   * `top_abort_with_subxacts_discards_everything` — top
//!     ROLLBACK drops both the top's buffer and every released-sub's
//!     buffer named in the abort record's subxids list.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use fx::spawn_txn;
use std::time::Duration;

use walshadow::mapping::TableTarget;
use walshadow::mapping::{ColumnMapping, NamespaceMapping};
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

// Each test owns a disjoint port slot. Cargo's default test runner
// parallelises tests within a binary, so reusing slots would collide
// on the source PG / shadow PG / CH listener.
const SLOT_ROLLBACK: PortSlot = PortSlot {
    source: 17430,
    shadow: 17431,
    ch_tcp: 17432,
    ch_http: 17433,
    walsender: 17460,
};
const SLOT_RELEASE: PortSlot = PortSlot {
    source: 17434,
    shadow: 17435,
    ch_tcp: 17436,
    ch_http: 17437,
    walsender: 17461,
};
const SLOT_NESTED: PortSlot = PortSlot {
    source: 17438,
    shadow: 17439,
    ch_tcp: 17440,
    ch_http: 17441,
    walsender: 17462,
};
const SLOT_TOP_ABORT: PortSlot = PortSlot {
    source: 17442,
    shadow: 17443,
    ch_tcp: 17444,
    ch_http: 17445,
    walsender: 17463,
};
const SLOT_TOAST_ROLLBACK: PortSlot = PortSlot {
    source: 17780,
    shadow: 17781,
    ch_tcp: 17782,
    ch_http: 17783,
    walsender: 17787,
};
const SLOT_IUD_ABORT: PortSlot = PortSlot {
    source: 17790,
    shadow: 17791,
    ch_tcp: 17792,
    ch_http: 17793,
    walsender: 17797,
};
const SLOT_SP_DDL: PortSlot = PortSlot {
    source: 17870,
    shadow: 17871,
    ch_tcp: 17872,
    ch_http: 17873,
    walsender: 17877,
};
const SLOT_ASSIGN: PortSlot = PortSlot {
    source: 17880,
    shadow: 17881,
    ch_tcp: 17882,
    ch_http: 17883,
    walsender: 17887,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

/// Single mapping shape every subxact test reuses.
fn mapping() -> Vec<fx::TableMappingSpec> {
    vec![fx::TableMappingSpec {
        source_table: RelName::new("s14", "sub_t"),
        target_table: TableTarget::new("walshadow_test", "s14_sub_t"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "payload".into(),
                target_type: "String".into(),
            },
        ],
    }]
}

const SCHEMA_SQL: &str = "CREATE SCHEMA s14;\n\
                          CREATE TABLE s14.sub_t (id bigint PRIMARY KEY, payload text NOT NULL);\n\
                          ALTER TABLE s14.sub_t REPLICA IDENTITY FULL;\n";

fn create_ch_dest(ch: &fx::ChServer) {
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.s14_sub_t (\
            id Int64,\
            payload String,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");
}

/// Shared drill — bootstrap clusters, pump the workload, then return
/// the (source, ch) handles to the caller for assertions.
async fn run_drill<F: FnOnce(&Shadow) -> std::thread::JoinHandle<()>>(
    slot: PortSlot,
    app_name: &str,
    spawn_driver: F,
) -> (Shadow, fx::ChServer, tempfile::TempDir, tempfile::TempDir) {
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

    let driver = spawn_driver(&source);
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(
        shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain ({app_name})",
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Stop shadow now so its leftover postmaster doesn't outlive tempdir.
    let _ = shadow.stop();

    // Caller still needs `source` for its row-count probes; the
    // returned tempdirs keep the data dirs alive.
    (source, ch, tmp, tempfile::tempdir().unwrap())
}

async fn assert_ch_rows(ch: &fx::ChServer, expected: &[(i64, &str)]) {
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s14_sub_t FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(
        ch_count,
        expected.len().to_string(),
        "row count mismatch (expected={expected:?})",
    );
    if expected.is_empty() {
        return;
    }
    let pairs = ch
        .query(
            "SELECT arrayStringConcat(\
                groupArray(concat(toString(id), '=', payload)), ','\
             ) FROM (\
                 SELECT id, argMax(payload, _lsn) AS payload \
                 FROM walshadow_test.s14_sub_t \
                 WHERE _is_deleted = 0 \
                 GROUP BY id ORDER BY id\
             )",
        )
        .expect("ch sample");
    let want = expected
        .iter()
        .map(|(id, p)| format!("{id}={p}"))
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(pairs, want, "ch payload set mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoint_rollback_discards_subxact_writes() {
    if skip_gate() {
        return;
    }
    let driver = |source: &Shadow| {
        spawn_txn(
            source,
            "BEGIN;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (1, 'R1');\n\
             SAVEPOINT s;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (2, 'R2');\n\
             ROLLBACK TO SAVEPOINT s;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (3, 'R3');\n\
             COMMIT;\n\
             SELECT pg_switch_wal();\n",
        )
    };
    let (source, ch, _tmp1, _tmp2) =
        run_drill(SLOT_ROLLBACK, "walshadow-subxact-rollback", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "2", "source must hold id=1 + id=3");
    assert_ch_rows(&ch, &[(1, "R1"), (3, "R3")]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoint_release_commits_subxact_writes() {
    if skip_gate() {
        return;
    }
    let driver = |source: &Shadow| {
        spawn_txn(
            source,
            "BEGIN;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (1, 'R1');\n\
             SAVEPOINT s;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (2, 'R2');\n\
             RELEASE SAVEPOINT s;\n\
             COMMIT;\n\
             SELECT pg_switch_wal();\n",
        )
    };
    let (source, ch, _tmp1, _tmp2) =
        run_drill(SLOT_RELEASE, "walshadow-subxact-release", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "2");
    assert_ch_rows(&ch, &[(1, "R1"), (2, "R2")]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orm_nested_savepoints_only_post_savepoint_writes_survive() {
    if skip_gate() {
        return;
    }
    let driver = |source: &Shadow| {
        spawn_txn(
            source,
            "BEGIN;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (1, 'R1');\n\
             SAVEPOINT s1;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (2, 'R2');\n\
             ROLLBACK TO SAVEPOINT s1;\n\
             SAVEPOINT s2;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (3, 'R3');\n\
             ROLLBACK TO SAVEPOINT s2;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (4, 'R4');\n\
             COMMIT;\n\
             SELECT pg_switch_wal();\n",
        )
    };
    let (source, ch, _tmp1, _tmp2) =
        run_drill(SLOT_NESTED, "walshadow-subxact-nested", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "2", "source must hold id=1 + id=4");
    assert_ch_rows(&ch, &[(1, "R1"), (4, "R4")]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn top_abort_with_subxacts_discards_everything() {
    if skip_gate() {
        return;
    }
    // Top ROLLBACK on its own doesn't drain a commit; pair it with an
    // already-committed INSERT outside the aborting BEGIN block so the
    // pipeline has SOMETHING to ship in the segment before pg_switch_wal.
    // Otherwise pump_segments would time out: nothing to flush, no
    // commit drain to advance dispatched_lsn.
    let driver = |source: &Shadow| {
        spawn_txn(
            source,
            "INSERT INTO s14.sub_t (id, payload) VALUES (99, 'sentinel');\n\
             BEGIN;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (1, 'R1');\n\
             SAVEPOINT s;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (2, 'R2');\n\
             RELEASE SAVEPOINT s;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (3, 'R3');\n\
             ROLLBACK;\n\
             SELECT pg_switch_wal();\n",
        )
    };
    let (source, ch, _tmp1, _tmp2) =
        run_drill(SLOT_TOP_ABORT, "walshadow-subxact-top-abort", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "1", "only the sentinel INSERT survives");
    // The aborted xact's id=1/R1, id=2/R2 (sub), id=3/R3 (post-RELEASE
    // on top) must all be discarded — only the sentinel reaches CH.
    assert_ch_rows(&ch, &[(99, "sentinel")]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toast_value_in_rolled_back_subxact_is_discarded() {
    if skip_gate() {
        return;
    }
    let driver = |source: &Shadow| {
        spawn_txn(
            source,
            "BEGIN;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (1, repeat('A', 12000));\n\
             SAVEPOINT s;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (2, repeat('B', 12000));\n\
             ROLLBACK TO SAVEPOINT s;\n\
             INSERT INTO s14.sub_t (id, payload) VALUES (3, repeat('C', 12000));\n\
             COMMIT;\n\
             SELECT pg_switch_wal();\n",
        )
    };
    let (source, ch, _tmp1, _tmp2) =
        run_drill(SLOT_TOAST_ROLLBACK, "walshadow-subxact-toast", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(
        source.psql_one("SELECT count(*) FROM s14.sub_t").unwrap(),
        "2",
        "source holds id=1 + id=3",
    );
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.s14_sub_t FINAL WHERE _is_deleted = 0")
            .unwrap(),
        "2",
    );
    assert_eq!(
        ch.query(
            "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
                SELECT concat(toString(id), ':', toString(length(argMax(payload, _lsn))), \
                    substring(argMax(payload, _lsn), 1, 1)) AS c \
                FROM walshadow_test.s14_sub_t WHERE _is_deleted = 0 \
                GROUP BY id ORDER BY id)"
        )
        .unwrap(),
        "1:12000A,3:12000C",
    );
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.s14_sub_t WHERE id = 2")
            .unwrap(),
        "0",
        "rolled-back subxact's TOAST row never reaches CH",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iud_in_aborted_top_xact_leaves_rows_untouched() {
    if skip_gate() {
        return;
    }
    let driver = |source: &Shadow| {
        spawn_txn(
            source,
            "INSERT INTO s14.sub_t (id, payload) VALUES (1, 'orig1'), (2, 'orig2');\n\
             BEGIN;\n\
             UPDATE s14.sub_t SET payload = 'changed' WHERE id = 1;\n\
             SAVEPOINT s;\n\
             DELETE FROM s14.sub_t WHERE id = 2;\n\
             RELEASE SAVEPOINT s;\n\
             ROLLBACK;\n\
             SELECT pg_switch_wal();\n",
        )
    };
    let (source, ch, _tmp1, _tmp2) =
        run_drill(SLOT_IUD_ABORT, "walshadow-subxact-iud-abort", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(
        source.psql_one("SELECT count(*) FROM s14.sub_t").unwrap(),
        "2",
    );
    assert_ch_rows(&ch, &[(1, "orig1"), (2, "orig2")]).await;
    assert_eq!(
        ch.query("SELECT argMax(_is_deleted, _lsn) FROM walshadow_test.s14_sub_t WHERE id = 2")
            .unwrap(),
        "false",
        "aborted DELETE leaves no tombstone",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoint_after_ddl_rollback_discards_column_and_rows() {
    if skip_gate() {
        return;
    }
    let slot = SLOT_SP_DDL;
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
        "CREATE SCHEMA s14d;\n",
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
        "s14d".into(),
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
        ch_tcp_port: slot.ch_tcp,
        mappings: vec![],
        app_name: "walshadow-subxact-sp-ddl",
        ddl: Some(ddl_args),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "CREATE TABLE s14d.t (id bigint PRIMARY KEY, payload text);\n\
         INSERT INTO s14d.t (id, payload) VALUES (1, 'base');\n\
         BEGIN;\n\
         INSERT INTO s14d.t (id, payload) VALUES (2, 'pre-sp');\n\
         SAVEPOINT sp;\n\
         ALTER TABLE s14d.t ADD COLUMN extra text;\n\
         INSERT INTO s14d.t (id, payload, extra) VALUES (3, 'with-extra', 'leak');\n\
         ROLLBACK TO SAVEPOINT sp;\n\
         INSERT INTO s14d.t (id, payload) VALUES (4, 'post-rollback');\n\
         COMMIT;\n\
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

    assert_eq!(
        source.psql_one("SELECT count(*) FROM s14d.t").unwrap(),
        "3",
        "source holds id=1,2,4",
    );
    let has_extra = ch
        .query(
            "SELECT count() FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 't' AND name = 'extra'",
        )
        .unwrap();
    assert_eq!(has_extra, "0", "rolled-back ADD COLUMN must not leak to CH");
    assert_eq!(
        ch.query(
            "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
                SELECT concat(toString(id), '=', argMax(payload, _lsn)) AS c \
                FROM walshadow_test.t WHERE _is_deleted = 0 GROUP BY id ORDER BY id)"
        )
        .unwrap(),
        "1=base,2=pre-sp,4=post-rollback",
        "only original-schema rows survive; id=3 discarded",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_subxacts_emit_assignment_record() {
    if skip_gate() {
        return;
    }
    let mut sql = String::from("BEGIN;\nINSERT INTO s14.sub_t (id, payload) VALUES (0, 'r0');\n");
    for i in 1..=70 {
        sql.push_str(&format!(
            "SAVEPOINT s{i};\nINSERT INTO s14.sub_t (id, payload) VALUES ({i}, 'r{i}');\n"
        ));
    }
    sql.push_str("COMMIT;\nSELECT pg_switch_wal();\n");

    let driver = |source: &Shadow| spawn_txn(source, &sql);
    let (source, ch, _t1, _t2) = run_drill(SLOT_ASSIGN, "walshadow-subxact-assign", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    assert_eq!(
        source.psql_one("SELECT count(*) FROM s14.sub_t").unwrap(),
        "71",
    );
    assert_eq!(
        ch.query("SELECT count() FROM walshadow_test.s14_sub_t FINAL WHERE _is_deleted = 0")
            .unwrap(),
        "71",
    );
}

fn skip_gate() -> bool {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return true;
    }
    if !fx::pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return true;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return true;
    }
    false
}
