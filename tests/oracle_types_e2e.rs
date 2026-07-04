//! Oracle-path types (arrays/enums/geometric/pgvector), end-to-end: they decode
//! to `PgPending`/`Unsupported` and resolve via the shadow's `walshadow`
//! extension, created on the source pre-basebackup. Skipped unless `walshadow`
//! is installed (`cd pgext && sudo make install`); pgvector also needs
//! `vector`. Resolved text matches PG `typoutput`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;
use walshadow::ch_emitter::TableTarget;
use walshadow::oracle::Oracle;
use walshadow::shadow::Shadow;
use walshadow::shadow_catalog::RelName;
use walshadow::shadow_catalog::socket_conninfo;

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

const SLOT_ARR: PortSlot = PortSlot {
    source: 17910,
    shadow: 17911,
    ch_tcp: 17912,
    ch_http: 17913,
    walsender: 17917,
};
const SLOT_ENUM: PortSlot = PortSlot {
    source: 17920,
    shadow: 17921,
    ch_tcp: 17922,
    ch_http: 17923,
    walsender: 17927,
};
const SLOT_GEO: PortSlot = PortSlot {
    source: 17930,
    shadow: 17931,
    ch_tcp: 17932,
    ch_http: 17933,
    walsender: 17937,
};
const SLOT_VEC: PortSlot = PortSlot {
    source: 17940,
    shadow: 17941,
    ch_tcp: 17942,
    ch_http: 17943,
    walsender: 17947,
};
const SLOT_RIF: PortSlot = PortSlot {
    source: 17950,
    shadow: 17951,
    ch_tcp: 17952,
    ch_http: 17953,
    walsender: 17957,
};

fn skip_gate() -> bool {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return true;
    }
    false
}

fn extension_available(name: &str) -> bool {
    let out = Command::new("pg_config").arg("--sharedir").output();
    match out {
        Ok(o) if o.status.success() => {
            let dir = String::from_utf8_lossy(&o.stdout).trim().to_string();
            Path::new(&dir)
                .join(format!("extension/{name}.control"))
                .exists()
        }
        _ => false,
    }
}

fn col(attnum: i16, name: &str, ty: &str) -> ColumnMapping {
    ColumnMapping {
        src_attnum: attnum,
        target_name: name.into(),
        target_type: ty.into(),
    }
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

async fn run_oracle(
    slot: PortSlot,
    app_name: &str,
    schema_sql: &str,
    ch_create_sql: &str,
    mappings: Vec<fx::TableMappingSpec>,
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
    ) = fx::bootstrap_clusters(&tmp, schema_sql, slot.source, slot.shadow, slot.walsender).await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(ch_create_sql).expect("create dest table");

    let conninfo = socket_conninfo(
        shadow.config().socket_dir.to_str().unwrap(),
        shadow.config().port,
        "postgres",
        "postgres",
    );
    let oracle = Oracle::connect(&conninfo, 0).await.expect("oracle connect");
    assert!(
        oracle.has_extension(),
        "shadow must expose walshadow_decode_disk",
    );

    let mut pipeline = fx::build_pipeline_with_oracle(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: slot.ch_tcp,
            mappings,
            app_name,
            ddl: None,
        },
        Arc::new(oracle),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrays_resolve_via_oracle() {
    if skip_gate() || !extension_available("walshadow") {
        eprintln!("skip: walshadow extension not installed");
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        SLOT_ARR,
        "walshadow-oracle-arrays",
        "CREATE EXTENSION walshadow;\n\
         CREATE TABLE public.arr (id int PRIMARY KEY, ints int[], texts text[], nums numeric[]);\n",
        "CREATE OR REPLACE TABLE walshadow_test.arr (\
            id Int32, ints Nullable(String), texts Nullable(String), nums Nullable(String),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "arr"),
            target_table: TableTarget::new("walshadow_test", "arr"),
            columns: vec![
                col(1, "id", "Int32"),
                col(2, "ints", "Nullable(String)"),
                col(3, "texts", "Nullable(String)"),
                col(4, "nums", "Nullable(String)"),
            ],
        }],
        "INSERT INTO public.arr VALUES \
            (1, '{1,2,3}', '{a,b,c}', '{1.5,2.25}'),\
            (2, '{}', '{}', '{}'),\
            (3, '{1,NULL,3}', '{x,NULL}', NULL);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    let row = |id: i32| -> Vec<String> {
        ch.query(&format!(
            "SELECT ifNull(ints,'<null>'), ifNull(texts,'<null>'), ifNull(nums,'<null>') \
             FROM walshadow_test.arr FINAL WHERE id = {id} AND _is_deleted = 0"
        ))
        .unwrap()
        .split('\t')
        .map(str::to_owned)
        .collect()
    };
    assert_eq!(row(1), vec!["{1,2,3}", "{a,b,c}", "{1.5,2.25}"]);
    assert_eq!(row(2), vec!["{}", "{}", "{}"], "empty arrays");
    assert_eq!(
        row(3),
        vec!["{1,NULL,3}", "{x,NULL}", "<null>"],
        "NULL elements + SQL NULL array",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enums_resolve_via_oracle() {
    if skip_gate() || !extension_available("walshadow") {
        eprintln!("skip: walshadow extension not installed");
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        SLOT_ENUM,
        "walshadow-oracle-enums",
        "CREATE EXTENSION walshadow;\n\
         CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');\n\
         CREATE TABLE public.en (id int PRIMARY KEY, m mood, ms mood[]);\n",
        "CREATE OR REPLACE TABLE walshadow_test.en (\
            id Int32, m Nullable(String), ms Nullable(String),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "en"),
            target_table: TableTarget::new("walshadow_test", "en"),
            columns: vec![
                col(1, "id", "Int32"),
                col(2, "m", "Nullable(String)"),
                col(3, "ms", "Nullable(String)"),
            ],
        }],
        "INSERT INTO public.en VALUES \
            (1, 'happy', '{sad,happy}'),\
            (2, NULL, NULL);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    assert_eq!(
        ch.query("SELECT m, ms FROM walshadow_test.en FINAL WHERE id = 1 AND _is_deleted = 0")
            .unwrap(),
        "happy\t{sad,happy}",
    );
    assert_eq!(
        ch.query(
            "SELECT ifNull(m,'<null>') FROM walshadow_test.en FINAL WHERE id = 2 AND _is_deleted = 0"
        )
        .unwrap(),
        "<null>",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn geometric_types_resolve_via_oracle() {
    if skip_gate() || !extension_available("walshadow") {
        eprintln!("skip: walshadow extension not installed");
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        SLOT_GEO,
        "walshadow-oracle-geo",
        "CREATE EXTENSION walshadow;\n\
         CREATE TABLE public.geo (\
            id int PRIMARY KEY, p point, ln line, ls lseg, bx box, \
            pth path, poly polygon, c circle);\n",
        "CREATE OR REPLACE TABLE walshadow_test.geo (\
            id Int32, p Nullable(String), ln Nullable(String), ls Nullable(String), \
            bx Nullable(String), pth Nullable(String), poly Nullable(String), c Nullable(String),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "geo"),
            target_table: TableTarget::new("walshadow_test", "geo"),
            columns: vec![
                col(1, "id", "Int32"),
                col(2, "p", "Nullable(String)"),
                col(3, "ln", "Nullable(String)"),
                col(4, "ls", "Nullable(String)"),
                col(5, "bx", "Nullable(String)"),
                col(6, "pth", "Nullable(String)"),
                col(7, "poly", "Nullable(String)"),
                col(8, "c", "Nullable(String)"),
            ],
        }],
        "INSERT INTO public.geo VALUES (1, \
            '(1,2)', '{1,2,3}', '[(0,0),(1,1)]', '(1,1),(0,0)', \
            '[(0,0),(1,1),(2,0)]', '((0,0),(1,1),(1,0))', '<(0,0),1>');\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    // PG typoutput forms (box normalizes to upper-right, lower-left).
    assert_eq!(
        ch.query(
            "SELECT p, ln, ls, bx, pth, poly, c \
             FROM walshadow_test.geo FINAL WHERE id = 1 AND _is_deleted = 0"
        )
        .unwrap(),
        "(1,2)\t{1,2,3}\t[(0,0),(1,1)]\t(1,1),(0,0)\t\
         [(0,0),(1,1),(2,0)]\t((0,0),(1,1),(1,0))\t<(0,0),1>",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgvector_resolves_via_oracle() {
    if skip_gate() || !extension_available("walshadow") || !extension_available("vector") {
        eprintln!("skip: walshadow or vector extension not installed");
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        SLOT_VEC,
        "walshadow-oracle-vector",
        "CREATE EXTENSION walshadow;\n\
         CREATE EXTENSION vector;\n\
         CREATE TABLE public.vec (\
            id int PRIMARY KEY, v vector(3), hv halfvec(3), sv sparsevec(5));\n",
        "CREATE OR REPLACE TABLE walshadow_test.vec (\
            id Int32, v Nullable(String), hv Nullable(String), sv Nullable(String),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "vec"),
            target_table: TableTarget::new("walshadow_test", "vec"),
            columns: vec![
                col(1, "id", "Int32"),
                col(2, "v", "Nullable(String)"),
                col(3, "hv", "Nullable(String)"),
                col(4, "sv", "Nullable(String)"),
            ],
        }],
        "INSERT INTO public.vec VALUES (1, '[1,2,3]', '[4,5,6]', '{1:1,3:2}/5');\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    assert_eq!(
        ch.query("SELECT v, hv, sv FROM walshadow_test.vec FINAL WHERE id = 1 AND _is_deleted = 0")
            .unwrap(),
        "[1,2,3]\t[4,5,6]\t{1:1,3:2}/5",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_update_under_rif_resolves_old_tuple() {
    if skip_gate() || !extension_available("walshadow") {
        eprintln!("skip: walshadow extension not installed");
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        SLOT_RIF,
        "walshadow-oracle-rif",
        "CREATE EXTENSION walshadow;\n\
         CREATE TABLE public.arr (id int PRIMARY KEY, ints int[]);\n\
         ALTER TABLE public.arr REPLICA IDENTITY FULL;\n",
        "CREATE OR REPLACE TABLE walshadow_test.arr (\
            id Int32, ints Nullable(String),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "arr"),
            target_table: TableTarget::new("walshadow_test", "arr"),
            columns: vec![col(1, "id", "Int32"), col(2, "ints", "Nullable(String)")],
        }],
        "INSERT INTO public.arr VALUES (1, '{1,2}');\n\
         UPDATE public.arr SET ints = '{3,4}' WHERE id = 1;\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    assert_eq!(
        ch.query("SELECT ints FROM walshadow_test.arr FINAL WHERE id = 1 AND _is_deleted = 0")
            .unwrap(),
        "{3,4}",
    );
}
