//! Oracle type integration tests
//!
//! Require built `pgext` and source extensions used by each test

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use fx::spawn_txn;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use walshadow::mapping::ColumnMapping;
use walshadow::mapping::TableTarget;
use walshadow::oracle::Oracle;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

fn skip_gate() -> bool {
    if !fx::requirements_available() {
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

async fn run_oracle(
    slot: fx::Ports,
    app_name: &str,
    schema_sql: &str,
    ch_create_sql: &str,
    mappings: Vec<fx::TableMappingSpec>,
    workload: &str,
) -> (Shadow, fx::ChServer, tempfile::TempDir) {
    let (source, ch, tmp, _oracle) = run_oracle_stats(
        slot,
        app_name,
        schema_sql,
        ch_create_sql,
        mappings,
        workload,
    )
    .await;
    (source, ch, tmp)
}

async fn run_oracle_stats(
    slot: fx::Ports,
    app_name: &str,
    schema_sql: &str,
    ch_create_sql: &str,
    mappings: Vec<fx::TableMappingSpec>,
    workload: &str,
) -> (Shadow, fx::ChServer, tempfile::TempDir, Arc<Oracle>) {
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters_with_bridge(
        &tmp,
        schema_sql,
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(ch_create_sql).expect("create dest table");

    // Worker binds only once recovery reaches consistency, so budget the dial
    let socket = shadow.bridge_socket().expect("bridge configured");
    let bridge = walshadow::bridge::connect_with_budget(socket, Duration::from_secs(30))
        .await
        .expect("bridge connect");
    assert!(
        bridge.info().expect("hello").in_recovery,
        "shadow must serve decode while in recovery",
    );
    let oracle = Arc::new(Oracle::new(Arc::new(bridge)));

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
        oracle.clone(),
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

    (source, ch, tmp, oracle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrays_resolve_via_oracle() {
    if skip_gate() {
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-arrays",
        "CREATE TABLE public.arr (id int PRIMARY KEY, ints int[], texts text[], nums numeric[]);\n",
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
    assert_eq!(row(1), ["{1,2,3}", "{a,b,c}", "{1.5,2.25}"]);
    assert_eq!(row(2), ["{}", "{}", "{}"], "empty arrays");
    assert_eq!(
        row(3),
        vec!["{1,NULL,3}", "{x,NULL}", "<null>"],
        "NULL elements + SQL NULL array",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_array_targets_carry_elements_and_nulls() {
    if skip_gate() {
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-native-arrays",
        "CREATE TABLE public.na (\
            id int PRIMARY KEY, ints int[], texts text[], opt int[], ids uuid[]);\n",
        "CREATE OR REPLACE TABLE walshadow_test.na (\
            id Int32, ints Array(Int32), texts Array(String), \
            opt Array(Nullable(Int32)), ids Array(UUID),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "na"),
            target_table: TableTarget::new("walshadow_test", "na"),
            columns: vec![
                col(1, "id", "Int32"),
                col(2, "ints", "Array(Int32)"),
                col(3, "texts", "Array(String)"),
                col(4, "opt", "Array(Nullable(Int32))"),
                col(5, "ids", "Array(UUID)"),
            ],
        }],
        "INSERT INTO public.na VALUES \
            (1, '{1,2,3}', '{a,\"b,c\"}', '{1,NULL,3}', \
                '{00112233-4455-6677-8899-aabbccddeeff}'),\
            (2, '{}', '{}', '{}', '{}'),\
            (3, NULL, NULL, NULL, NULL);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    let row = |id: i32| -> String {
        ch.query(&format!(
            "SELECT ints, texts, opt, ids FROM walshadow_test.na FINAL \
             WHERE id = {id} AND _is_deleted = 0"
        ))
        .unwrap()
    };
    assert_eq!(
        row(1),
        "[1,2,3]\t[\'a\',\'b,c\']\t[1,NULL,3]\t[\'00112233-4455-6677-8899-aabbccddeeff\']",
    );
    assert_eq!(row(2), "[]\t[]\t[]\t[]", "empty arrays");
    // CH forbids Nullable(Array), so a SQL NULL array lands empty
    assert_eq!(row(3), "[]\t[]\t[]\t[]", "NULL array");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hstore_maps_through_the_extension_expander() {
    if skip_gate() || !extension_available("hstore") {
        eprintln!("skip: hstore extension not installed");
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-hstore",
        "CREATE EXTENSION hstore;\n\
         CREATE TABLE public.hs (id int PRIMARY KEY, h hstore);\n",
        "CREATE OR REPLACE TABLE walshadow_test.hs (\
            id Int32, h Map(String, Nullable(String)),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "hs"),
            target_table: TableTarget::new("walshadow_test", "hs"),
            columns: vec![
                col(1, "id", "Int32"),
                col(2, "h", "Map(String, Nullable(String))"),
            ],
        }],
        "INSERT INTO public.hs VALUES \
            (1, '\"a=>b\" => \"c,d\", e => NULL, f => \"\"'),\
            (2, ''),\
            (3, NULL);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    // hstore is unordered, so read keys sorted and values by key
    let row = |id: i32, expr: &str| -> String {
        ch.query(&format!(
            "SELECT {expr} FROM walshadow_test.hs FINAL WHERE id = {id} AND _is_deleted = 0"
        ))
        .unwrap()
    };
    assert_eq!(
        row(
            1,
            "arraySort(mapKeys(h)), h['a=>b'], isNull(h['e']), concat('<', h['f'], '>')"
        ),
        "['a=>b','e','f']\tc,d\t1\t<>",
        "separators, NULL value, and empty value survive",
    );
    assert_eq!(row(2, "h"), "{}", "empty hstore");
    assert_eq!(row(3, "h"), "{}", "SQL NULL hstore");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_targets_take_every_json_shape() {
    if skip_gate() {
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-json",
        "CREATE TABLE public.js (id int PRIMARY KEY, b jsonb, j json);\n",
        "CREATE OR REPLACE TABLE walshadow_test.js (\
            id Int32, b JSON, j JSON,\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "js"),
            target_table: TableTarget::new("walshadow_test", "js"),
            columns: vec![
                col(1, "id", "Int32"),
                col(2, "b", "JSON"),
                col(3, "j", "JSON"),
            ],
        }],
        "INSERT INTO public.js VALUES \
            (1, '{\"a\": 1}', '{\"b\": [1,2]}'),\
            (2, NULL, NULL);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    assert_eq!(
        ch.query("SELECT b.a, j.b FROM walshadow_test.js FINAL WHERE id = 1 AND _is_deleted = 0")
            .unwrap(),
        "1\t[1,2]",
        "JSON paths resolve, so the column really is typed JSON",
    );
    // Non-nullable JSON cannot hold NULL; an absent value is the empty object
    assert_eq!(
        ch.query(
            "SELECT toString(b) FROM walshadow_test.js FINAL WHERE id = 2 AND _is_deleted = 0"
        )
        .unwrap(),
        "{}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_request_covers_a_whole_sealed_batch() {
    if skip_gate() {
        return;
    }
    const ROWS: u64 = 200;
    let (source, ch, _tmp, oracle) = run_oracle_stats(
        fx::Ports::alloc(),
        "walshadow-oracle-batching",
        "CREATE TABLE public.bt (id int PRIMARY KEY, tags int[]);\n",
        "CREATE OR REPLACE TABLE walshadow_test.bt (\
            id Int32, tags Array(Int32),\
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        vec![fx::TableMappingSpec {
            source_table: RelName::new("public", "bt"),
            target_table: TableTarget::new("walshadow_test", "bt"),
            columns: vec![col(1, "id", "Int32"), col(2, "tags", "Array(Int32)")],
        }],
        &format!(
            "INSERT INTO public.bt SELECT g, ARRAY[g, g + 1] FROM generate_series(1, {ROWS}) g;\n\
             SELECT pg_switch_wal();\n"
        ),
    )
    .await;
    let _src = fx::StopOnDrop { sh: &source };

    assert_eq!(
        ch.query("SELECT count(), sum(tags[1]) FROM walshadow_test.bt FINAL WHERE _is_deleted = 0")
            .unwrap(),
        format!("{ROWS}\t{}", ROWS * (ROWS + 1) / 2),
    );

    use std::sync::atomic::Ordering;
    let blocks = oracle.stats.blocks.load(Ordering::Relaxed);
    let rows = oracle.stats.rows.load(Ordering::Relaxed);
    assert_eq!(rows, ROWS, "every row went through the oracle");
    assert!(
        blocks < ROWS / 10,
        "{blocks} requests for {rows} rows reads as per-row, not per-batch",
    );
    assert_eq!(oracle.stats.errors.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enums_resolve_via_oracle() {
    if skip_gate() {
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-enums",
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');\n\
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
    if skip_gate() {
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-geo",
        "CREATE TABLE public.geo (\
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
    if skip_gate() || !extension_available("vector") {
        eprintln!("skip: vector extension not installed");
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-vector",
        "CREATE EXTENSION vector;\n\
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
    if skip_gate() {
        return;
    }
    let (source, ch, _tmp) = run_oracle(
        fx::Ports::alloc(),
        "walshadow-oracle-rif",
        "CREATE TABLE public.arr (id int PRIMARY KEY, ints int[]);\n\
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
