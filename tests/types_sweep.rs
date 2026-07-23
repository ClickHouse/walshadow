//! Broad type-fidelity sweep, end-to-end. Covers the natively-decoded scalar
//! matrix; arrays/enum/geometric/pgvector (oracle path) are a separate drill.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use fx::spawn_txn;
use std::time::Duration;

use walshadow::mapping::ColumnMapping;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

// walsender must clear ch_http by >1 (CH binds interserver = ch_http + 1).
const SLOT_BROAD: PortSlot = PortSlot {
    source: 17760,
    shadow: 17761,
    ch_tcp: 17762,
    ch_http: 17763,
    walsender: 17767,
};
const SLOT_NANINF: PortSlot = PortSlot {
    source: 17770,
    shadow: 17771,
    ch_tcp: 17772,
    ch_http: 17773,
    walsender: 17777,
};
const SLOT_TIME: PortSlot = PortSlot {
    source: 17780,
    shadow: 17781,
    ch_tcp: 17782,
    ch_http: 17783,
    walsender: 17787,
};
const SLOT_NUM: PortSlot = PortSlot {
    source: 17790,
    shadow: 17791,
    ch_tcp: 17792,
    ch_http: 17793,
    walsender: 17797,
};
const SLOT_JSON: PortSlot = PortSlot {
    source: 17900,
    shadow: 17901,
    ch_tcp: 17902,
    ch_http: 17903,
    walsender: 17907,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

fn col(attnum: i16, name: &str, ty: &str) -> ColumnMapping {
    ColumnMapping {
        src_attnum: attnum,
        target_name: name.into(),
        target_type: ty.into(),
    }
}

fn skip_gate() -> bool {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return true;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broad_type_fidelity() {
    if skip_gate() {
        return;
    }
    let schema = "CREATE TABLE public.types_t (\
        id int PRIMARY KEY,\
        i2 smallint, i4 int, i8 bigint,\
        f4 real, f8 double precision,\
        b boolean,\
        t text, vc varchar(20),\
        ba bytea,\
        n numeric(10,2), nbig numeric(50,2), nhuge numeric(90,4),\
        d date, tm time, tmz timetz,\
        ts timestamp, tstz timestamptz,\
        u uuid, ip inet, j json, iv interval);\n";

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
        schema,
        SLOT_BROAD.source,
        SLOT_BROAD.shadow,
        SLOT_BROAD.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, SLOT_BROAD.ch_tcp, SLOT_BROAD.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.types_t (\
            id Int32,\
            i2 Int16, i4 Int32, i8 Int64,\
            f4 Float32, f8 Float64,\
            b Bool,\
            t String, vc String,\
            ba String,\
            n Decimal(10,2), nbig Decimal(50,2), nhuge String,\
            d Date32, tm Time64(6), tmz String,\
            ts DateTime64(6, 'UTC'), tstz DateTime64(6, 'UTC'),\
            u UUID, ip String, j String, iv String,\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "types_t"),
        target_table: TableTarget::new("walshadow_test", "types_t"),
        columns: vec![
            col(1, "id", "Int32"),
            col(2, "i2", "Int16"),
            col(3, "i4", "Int32"),
            col(4, "i8", "Int64"),
            col(5, "f4", "Float32"),
            col(6, "f8", "Float64"),
            col(7, "b", "Bool"),
            col(8, "t", "String"),
            col(9, "vc", "String"),
            col(10, "ba", "String"),
            col(11, "n", "Decimal(10,2)"),
            col(12, "nbig", "Decimal(50,2)"),
            col(13, "nhuge", "String"),
            col(14, "d", "Date32"),
            col(15, "tm", "Time64(6)"),
            col(16, "tmz", "String"),
            col(17, "ts", "DateTime64(6, 'UTC')"),
            col(18, "tstz", "DateTime64(6, 'UTC')"),
            col(19, "u", "UUID"),
            col(20, "ip", "String"),
            col(21, "j", "String"),
            col(22, "iv", "String"),
        ],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: SLOT_BROAD.ch_tcp,
        mappings,
        app_name: "walshadow-types-sweep",
        ddl: None,
    })
    .await;

    let driver = spawn_txn(
        &source,
        "INSERT INTO public.types_t VALUES (\
            1,\
            -12345, 2000000000, 9000000000000000000,\
            1.5, 2.5,\
            true,\
            'hello', 'world',\
            '\\xdeadbeef',\
            1234.56, 12345678901234567890.12, \
            12345678901234567890123456789012345678901234567890123456789012345678901234567890.1234,\
            '2026-06-24', '12:34:56', '12:34:56+02',\
            '2026-06-24 12:34:56', '2026-06-24 12:34:56+00',\
            '00112233-4455-6677-8899-aabbccddeeff', '192.168.0.1', '{\"k\":1}',\
            '1 year 2 mons 3 days 04:05:06'\
         );\n\
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

    let got = ch
        .query(
            "SELECT \
                toString(i2), toString(i4), toString(i8),\
                toString(f4), toString(f8),\
                toString(b),\
                t, vc,\
                hex(ba),\
                toString(n), toString(nbig), nhuge,\
                toString(d), toString(tm), tmz,\
                toString(ts), toString(tstz),\
                toString(u), ip, j, iv \
             FROM walshadow_test.types_t FINAL WHERE id = 1 AND _is_deleted = 0",
        )
        .expect("ch select");
    let cols: Vec<&str> = got.split('\t').collect();
    let want = [
        ("i2", "-12345"),
        ("i4", "2000000000"),
        ("i8", "9000000000000000000"),
        ("f4", "1.5"),
        ("f8", "2.5"),
        ("b", "true"),
        ("t", "hello"),
        ("vc", "world"),
        ("ba", "DEADBEEF"),
        ("n", "1234.56"),
        ("nbig", "12345678901234567890.12"),
        (
            "nhuge",
            "12345678901234567890123456789012345678901234567890123456789012345678901234567890.1234",
        ),
        ("d", "2026-06-24"),
        ("tm", "12:34:56.000000"),
        ("tmz", "12:34:56+02"),
        ("ts", "2026-06-24 12:34:56.000000"),
        ("tstz", "2026-06-24 12:34:56.000000"),
        ("u", "00112233-4455-6677-8899-aabbccddeeff"),
        ("ip", "192.168.0.1"),
        ("j", "{\"k\":1}"),
        ("iv", "1 year 2 mons 3 days 04:05:06"),
    ];
    assert_eq!(cols.len(), want.len(), "column count (got {got:?})");
    for (i, (name, expect)) in want.iter().enumerate() {
        assert_eq!(
            &cols[i], expect,
            "column `{name}` mismatch (full row {got:?})"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nan_and_infinity() {
    if skip_gate() {
        return;
    }
    let schema = "CREATE TABLE public.ni (\
        id int PRIMARY KEY, f4 real, f8 double precision, ntext numeric);\n";

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
        schema,
        SLOT_NANINF.source,
        SLOT_NANINF.shadow,
        SLOT_NANINF.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch =
        fx::ChServer::spawn(ch_tmp, SLOT_NANINF.ch_tcp, SLOT_NANINF.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    // numeric NaN can't live in Decimal, so ntext maps to String.
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.ni (\
            id Int32, f4 Float32, f8 Float64, ntext String,\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "ni"),
        target_table: TableTarget::new("walshadow_test", "ni"),
        columns: vec![
            col(1, "id", "Int32"),
            col(2, "f4", "Float32"),
            col(3, "f8", "Float64"),
            col(4, "ntext", "String"),
        ],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: SLOT_NANINF.ch_tcp,
        mappings,
        app_name: "walshadow-nan-inf",
        ddl: None,
    })
    .await;

    let driver = spawn_txn(
        &source,
        "INSERT INTO public.ni VALUES \
            (1, 'NaN', 'NaN', 'NaN'),\
            (2, 'Infinity', 'Infinity', NULL),\
            (3, '-Infinity', '-Infinity', NULL);\n\
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

    let row = |id: i32| -> Vec<String> {
        ch.query(&format!(
            "SELECT toString(f4), toString(f8), ntext \
             FROM walshadow_test.ni FINAL WHERE id = {id} AND _is_deleted = 0"
        ))
        .expect("ch select")
        .split('\t')
        .map(str::to_owned)
        .collect()
    };
    assert_eq!(row(1), vec!["nan", "nan", "NaN"], "NaN row");
    assert_eq!(&row(2)[0..2], &["inf", "inf"], "+Inf row");
    assert_eq!(&row(3)[0..2], &["-inf", "-inf"], "-Inf row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_precision() {
    if skip_gate() {
        return;
    }
    let schema = "CREATE TABLE public.tp (id int PRIMARY KEY, t time, tz timetz);\n";

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
        schema,
        SLOT_TIME.source,
        SLOT_TIME.shadow,
        SLOT_TIME.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, SLOT_TIME.ch_tcp, SLOT_TIME.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.tp (\
            id Int32, t Time64(6), tz String,\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "tp"),
        target_table: TableTarget::new("walshadow_test", "tp"),
        columns: vec![
            col(1, "id", "Int32"),
            col(2, "t", "Time64(6)"),
            col(3, "tz", "String"),
        ],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: SLOT_TIME.ch_tcp,
        mappings,
        app_name: "walshadow-time-precision",
        ddl: None,
    })
    .await;

    let driver = spawn_txn(
        &source,
        "INSERT INTO public.tp VALUES \
            (1, '00:00:00', '08:00:00+00'),\
            (2, '12:34:56.123456', '12:34:56.5+05:30'),\
            (3, '23:59:59.999999', '23:59:59.999999-08');\n\
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

    let row = |id: i32| -> Vec<String> {
        ch.query(&format!(
            "SELECT toString(t), tz \
             FROM walshadow_test.tp FINAL WHERE id = {id} AND _is_deleted = 0"
        ))
        .expect("ch select")
        .split('\t')
        .map(str::to_owned)
        .collect()
    };
    assert_eq!(row(1), vec!["00:00:00.000000", "08:00:00+00"], "midnight");
    assert_eq!(
        row(2),
        vec!["12:34:56.123456", "12:34:56.5+05:30"],
        "sub-second + half-hour zone",
    );
    assert_eq!(
        row(3),
        vec!["23:59:59.999999", "23:59:59.999999-08"],
        "max µs + west zone",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_numeric() {
    if skip_gate() {
        return;
    }
    let schema = "CREATE TABLE public.ln (id int PRIMARY KEY, d numeric(38,4), huge numeric, neg numeric);\n";

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
        schema,
        SLOT_NUM.source,
        SLOT_NUM.shadow,
        SLOT_NUM.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, SLOT_NUM.ch_tcp, SLOT_NUM.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.ln (\
            id Int32, d Decimal(38,4), huge String, neg String,\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "ln"),
        target_table: TableTarget::new("walshadow_test", "ln"),
        columns: vec![
            col(1, "id", "Int32"),
            col(2, "d", "Decimal(38,4)"),
            col(3, "huge", "String"),
            col(4, "neg", "String"),
        ],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: SLOT_NUM.ch_tcp,
        mappings,
        app_name: "walshadow-large-numeric",
        ddl: None,
    })
    .await;

    let driver = spawn_txn(
        &source,
        "INSERT INTO public.ln VALUES (\
            1,\
            1234567890123456789012345678901234.5678,\
            12345678901234567890123456789012345678901234567890123456789012345678901234567890.1234,\
            -99999999999999999999999999999999999999999999999999999999999999999999999999999999.5\
         );\n\
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

    let got = ch
        .query(
            "SELECT toString(d), huge, neg \
             FROM walshadow_test.ln FINAL WHERE id = 1 AND _is_deleted = 0",
        )
        .expect("ch select");
    let cols: Vec<&str> = got.split('\t').collect();
    assert_eq!(
        cols[0], "1234567890123456789012345678901234.5678",
        "Decimal"
    );
    assert_eq!(
        cols[1],
        "12345678901234567890123456789012345678901234567890123456789012345678901234567890.1234",
        "huge → String verbatim",
    );
    assert_eq!(
        cols[2],
        "-99999999999999999999999999999999999999999999999999999999999999999999999999999999.5",
        "negative huge → String verbatim",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_null_vs_sql_null() {
    if skip_gate() {
        return;
    }
    let schema = "CREATE TABLE public.jn (id int PRIMARY KEY, j json);\n";

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
        schema,
        SLOT_JSON.source,
        SLOT_JSON.shadow,
        SLOT_JSON.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, SLOT_JSON.ch_tcp, SLOT_JSON.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.jn (\
            id Int32, j Nullable(String),\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "jn"),
        target_table: TableTarget::new("walshadow_test", "jn"),
        columns: vec![col(1, "id", "Int32"), col(2, "j", "Nullable(String)")],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: SLOT_JSON.ch_tcp,
        mappings,
        app_name: "walshadow-json-null",
        ddl: None,
    })
    .await;

    let driver = spawn_txn(
        &source,
        "INSERT INTO public.jn VALUES \
            (1, 'null'),\
            (2, NULL),\
            (3, '{\"a\": null, \"b\": 1}'),\
            (4, '[1, null, 3]');\n\
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

    let cell = |id: i32| -> String {
        ch.query(&format!(
            "SELECT ifNull(j, '<sqlnull>') FROM walshadow_test.jn \
             FINAL WHERE id = {id} AND _is_deleted = 0"
        ))
        .expect("ch select")
    };
    assert_eq!(cell(1), "null", "JSON null literal preserved as text");
    assert_eq!(cell(2), "<sqlnull>", "SQL NULL stays NULL, not 'null'");
    assert_eq!(cell(3), "{\"a\": null, \"b\": 1}", "nested null verbatim");
    assert_eq!(cell(4), "[1, null, 3]", "array null verbatim");
}
