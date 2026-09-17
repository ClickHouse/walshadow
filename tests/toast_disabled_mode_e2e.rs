//! End-to-end test for `[toast] mode = "disabled"`: no chunk store at all.
//!
//! INSERT carries chunks in the same transaction, so its value still resolves.
//! Under REPLICA IDENTITY FULL, UPDATE that does not change `body` logs only
//! its TOAST pointer. With no store to read, resolver writes NULL and counts a
//! default fill.
//!
//! Verify ClickHouse has no chunk mirror and the resolver neither writes nor
//! reads a store.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::{ColumnMapping, TableTarget};
use walshadow::schema::RelName;

/// 16 bytes * 512 = 8192, past the ~2KB toast threshold
const BODY_SQL: &str = "repeat('walshadow-toast-', 512)";
/// Force cross-page update so PostgreSQL logs complete tuple
const META2_SQL: &str = "repeat('v2-update-', 60)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unchanged_toast_pointer_fills_null_without_store() {
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
        "CREATE TABLE public.doc (id int PRIMARY KEY, meta text, body text);\n\
         ALTER TABLE public.doc ALTER COLUMN body SET STORAGE EXTERNAL;\n\
         ALTER TABLE public.doc REPLICA IDENTITY FULL;\n",
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
            meta Nullable(String),\
            body Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "doc"),
        target_table: TableTarget::new("walshadow_test", "doc"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "meta".into(),
                target_type: "Nullable(String)".into(),
            },
            ColumnMapping {
                src_attnum: 3,
                target_name: "body".into(),
                target_type: "Nullable(String)".into(),
            },
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
            app_name: "walshadow-toast-disabled-mode",
            ddl: None,
        },
        |cfg| cfg.toast.mode = walshadow::ch_emitter::ToastMode::Disabled,
    )
    .await;

    assert!(
        pipeline.stream.filter_mut().shadow_rels().is_none(),
        "disabled mode must not replay user heaps on shadow",
    );

    // Row 2 keeps body from INSERT chunks
    // Row 1 update logs only pointer to older value
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, 'v1', {BODY_SQL}), (2, 'v1', {BODY_SQL})"),
            format!("UPDATE public.doc SET meta = {META2_SQL} WHERE id = 1"),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    assert_eq!(
        ch.query(&format!(
            "SELECT body = {BODY_SQL} FROM walshadow_test.doc \
             WHERE id = 2 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch insert body"),
        "1",
        "INSERT resolves from chunks in its transaction",
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT meta = {META2_SQL}, isNull(body) FROM walshadow_test.doc \
             WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch update row"),
        "1\t1",
        "UPDATE keeps meta and fills the unchanged pointer with NULL",
    );

    assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 1);
    assert_eq!(
        stats.toast_values_filled_superseded.load(Ordering::Relaxed),
        0
    );
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);
    assert_eq!(
        stats.toast_values_fetched.load(Ordering::Relaxed),
        0,
        "no store to read from",
    );

    // No mirror: not written, and no DDL issued for one
    assert_eq!(stats.toast_chunk_puts.load(Ordering::Relaxed), 0);
    assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 0);
    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    assert_eq!(
        ch.query(&format!(
            "SELECT count() FROM system.tables \
             WHERE database = 'walshadow_test' AND name = 'pg_toast_{toast_relid}'"
        ))
        .expect("mirror presence"),
        "0",
        "disabled mode must create no chunk mirror",
    );
}
