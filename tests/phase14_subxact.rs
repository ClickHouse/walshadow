//! Phase 14 item 5 — subxact lineage + `ROLLBACK TO SAVEPOINT`.
//!
//! Verifies that source `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` /
//! `RELEASE SAVEPOINT` and top-xact abort with subxacts replicate
//! correctly:
//!
//!   * `phase14_savepoint_rollback_discards_subxact_writes` — top
//!     INSERT survives; INSERT inside an aborted sub does not.
//!   * `phase14_savepoint_release_commits_subxact_writes` — RELEASE
//!     folds the sub into top so both INSERTs land.
//!   * `phase14_orm_nested_savepoints_only_post_savepoint_writes_survive`
//!     — repeated SAVEPOINT/ROLLBACK pattern (ORM-style) keeps only the
//!     top-level INSERTs.
//!   * `phase14_top_abort_with_subxacts_discards_everything` — top
//!     ROLLBACK drops both the top's buffer and every released-sub's
//!     buffer named in the abort record's subxids list.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::process::{Command, Stdio};
use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;
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
        source_table: "s14.sub_t".into(),
        target_table: "walshadow_test.s14_sub_t".into(),
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
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");
}

/// Drive a single explicit transaction containing multi-line SQL.
/// Unlike `fx::spawn_workload` (which uses per-statement `-c` autocommit),
/// this routes through `-f -` piping so SAVEPOINT lineage survives.
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
            let stdin = child.stdin.as_mut().expect("stdin piped");
            stdin.write_all(sql.as_bytes()).unwrap();
        }
        let _ = child.wait();
    })
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

    // Stop shadow now so its leftover postmaster doesn't outlive tempdir.
    let _ = shadow.stop();

    // Caller still needs `source` for its row-count probes; the
    // returned tempdirs keep the data dirs alive.
    (source, ch, tmp, tempfile::tempdir().unwrap())
}

async fn assert_ch_rows(ch: &fx::ChServer, expected: &[(i64, &str)]) {
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s14_sub_t FINAL WHERE _op != 'delete'")
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
                 WHERE _op != 'delete' \
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
async fn phase14_savepoint_rollback_discards_subxact_writes() {
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
        run_drill(SLOT_ROLLBACK, "walshadow-phase14-sub-rollback", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "2", "source must hold id=1 + id=3");
    assert_ch_rows(&ch, &[(1, "R1"), (3, "R3")]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase14_savepoint_release_commits_subxact_writes() {
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
        run_drill(SLOT_RELEASE, "walshadow-phase14-sub-release", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "2");
    assert_ch_rows(&ch, &[(1, "R1"), (2, "R2")]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase14_orm_nested_savepoints_only_post_savepoint_writes_survive() {
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
        run_drill(SLOT_NESTED, "walshadow-phase14-sub-nested", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "2", "source must hold id=1 + id=4");
    assert_ch_rows(&ch, &[(1, "R1"), (4, "R4")]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase14_top_abort_with_subxacts_discards_everything() {
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
        run_drill(SLOT_TOP_ABORT, "walshadow-phase14-sub-top-abort", driver).await;
    let _src_stop = fx::StopOnDrop { sh: &source };

    let src_count = source
        .psql_one("SELECT count(*) FROM s14.sub_t")
        .expect("source count");
    assert_eq!(src_count, "1", "only the sentinel INSERT survives");
    // The aborted xact's id=1/R1, id=2/R2 (sub), id=3/R3 (post-RELEASE
    // on top) must all be discarded — only the sentinel reaches CH.
    assert_ch_rows(&ch, &[(99, "sentinel")]).await;
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
