//! Compressed-external TOAST end to end: source PG → walshadow filter →
//! shadow PG → heap decoder → chunk mirror → `clickhouse server`.
//!
//! Every other TOAST test sets `STORAGE EXTERNAL`, which turns compression
//! off, so the stored chunks are the value. A value that compresses *and*
//! still exceeds the toast threshold goes out of line compressed, and then
//! the chunks are the inline compressed varlena from its `va_tcinfo` word on
//! — a 4-byte prefix that is not part of the compressed payload — and the
//! pointer's method bits are 0 for pglz, so compression is recognised by
//! `extsize < va_rawsize - VARHDRSZ`, never by those bits.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::ColumnMapping;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

/// 3840 bytes of half-duplicated md5 hex: compresses to ~2.2-2.3 KB under
/// either codec, which still clears the ~2 KB toast threshold, so the datum
/// is pushed out of line *after* compression
const BODY_SQL: &str = "(SELECT string_agg(md5((i / 2)::text), '') FROM generate_series(1, 120) i)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compressed_external_values_rehydrate() {
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
        // Default storage (EXTENDED) — compress first, externalise after.
        // lz4 is a build option, so fall back to pglz where it is absent
        "CREATE TABLE public.doc (id int PRIMARY KEY, pglz_body text, lz4_body text);\n\
         ALTER TABLE public.doc ALTER COLUMN pglz_body SET COMPRESSION pglz;\n\
         DO $$ BEGIN\n\
             EXECUTE 'ALTER TABLE public.doc ALTER COLUMN lz4_body SET COMPRESSION lz4';\n\
         EXCEPTION WHEN OTHERS THEN\n\
             EXECUTE 'ALTER TABLE public.doc ALTER COLUMN lz4_body SET COMPRESSION pglz';\n\
         END $$;\n",
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
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.doc (\
            id Int32,\
            pglz_body Nullable(String),\
            lz4_body Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let column = |attnum: i16, name: &str| ColumnMapping {
        src_attnum: attnum,
        target_name: name.into(),
        target_type: "Nullable(String)".into(),
    };
    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "doc"),
        target_table: TableTarget::new("walshadow_test", "doc"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            },
            column(2, "pglz_body"),
            column(3, "lz4_body"),
        ],
    }];

    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: slot.ch_tcp,
            mappings,
            app_name: "walshadow-toast-compressed",
            ddl: None,
        },
        |_cfg| {},
    )
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, {BODY_SQL}, {BODY_SQL})"),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    // The premise: both columns really are out of line, and really are
    // compressed, i.e. fewer bytes stored than the value holds
    let toast_rel = source
        .psql_one(
            "SELECT relname FROM pg_class WHERE oid = (SELECT reltoastrelid FROM pg_class \
             WHERE oid = 'public.doc'::regclass)",
        )
        .expect("source toast relname");
    assert_eq!(
        source
            .psql_one(&format!(
                "SELECT count(DISTINCT chunk_id) FROM pg_toast.{toast_rel}"
            ))
            .expect("source chunk ids"),
        "2",
        "values stayed inline, so the test is not covering what it claims",
    );
    assert_eq!(
        source
            .psql_one(
                "SELECT pg_column_size(pglz_body) < octet_length(pglz_body) \
                 AND pg_column_size(lz4_body) < octet_length(lz4_body) \
                 FROM public.doc WHERE id = 1"
            )
            .expect("source compression check"),
        "t",
        "values did not compress, so the test is not covering what it claims",
    );

    let digest = source
        .psql_one(&format!("SELECT md5({BODY_SQL})"))
        .expect("source digest");
    for column in ["pglz_body", "lz4_body"] {
        assert_eq!(
            ch.query(&format!(
                "SELECT lower(hex(MD5({column}))) FROM walshadow_test.doc \
                 WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
            ))
            .expect("ch digest"),
            digest,
            "{column} did not survive detoasting",
        );
    }
    assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 0);
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);
}
