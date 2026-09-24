//! End-to-end `[toast] mode` behaviour where the chunk mirror is absent.
//!
//! Under REPLICA IDENTITY FULL, an UPDATE that does not change `body` logs
//! only the TOAST pointer, so the row's value has to come from somewhere other
//! than its own record:
//!
//! - `shadow` reads it out of shadow's TOAST heap
//! - `disabled` has nowhere to read, so the resolver writes NULL and counts a
//!   default fill; an INSERT still resolves, its chunks being in the same
//!   transaction
//!
//! Both modes must leave ClickHouse without a chunk mirror and write nothing
//! to a store. The mirror-backed mode is `toast_e2e.rs`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::ch_emitter::{EmitterStats, ToastMode};
use walshadow::mapping::{ColumnMapping, TableTarget};
use walshadow::schema::RelName;

/// 16 bytes * 512 = 8192, past the ~2KB toast threshold and spanning several
/// ~2KB chunks, so a partial read would be visible as a short value
const BODY_SQL: &str = "repeat('walshadow-toast-', 512)";
/// Force cross-page update so PostgreSQL logs complete tuple
const META2_SQL: &str = "repeat('v2-update-', 60)";

const SOURCE_DDL: &str = "CREATE TABLE public.doc (id int PRIMARY KEY, meta text, body text);\n\
     ALTER TABLE public.doc ALTER COLUMN body SET STORAGE EXTERNAL;\n\
     ALTER TABLE public.doc REPLICA IDENTITY FULL;\n";

/// Destination shaped for `public.doc`, plus the routing that reaches it
fn dest_table(ch: &fx::ChServer) -> Vec<fx::TableMappingSpec> {
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
    vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "doc"),
        target_table: TableTarget::new("walshadow_test", "doc"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
                type_pinned: false,
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "meta".into(),
                target_type: "Nullable(String)".into(),
                type_pinned: false,
            },
            ColumnMapping {
                src_attnum: 3,
                target_name: "body".into(),
                target_type: "Nullable(String)".into(),
                type_pinned: false,
            },
        ],
    }]
}

/// Neither mode may write a mirror, nor issue the DDL for one
fn assert_no_mirror(ch: &fx::ChServer, source: &walshadow::shadow::Shadow, stats: &EmitterStats) {
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
        "no chunk mirror may exist",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unchanged_toast_pointer_resolves_out_of_shadow() {
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
    ) = fx::bootstrap_clusters_with_bridge(
        &tmp,
        SOURCE_DDL,
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let mappings = dest_table(&ch);

    // Same bridge the oracle uses: a shadow-backed value read rides the
    // worker pool rather than dialling its own socket
    let bridge = Arc::new(
        walshadow::bridge::connect_with_budget(
            shadow.bridge_socket().expect("shadow bridge configured"),
            1,
            Duration::from_secs(20),
        )
        .await
        .expect("dial shadow bridge"),
    );
    let oracle = Arc::new(walshadow::oracle::Oracle::new(bridge));

    let mut pipeline = fx::build_pipeline_tuned(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: slot.ch_tcp,
            mappings,
            app_name: "walshadow-toast-shadow-backend",
            ddl: None,
        },
        |cfg| cfg.toast.mode = ToastMode::Shadow,
        Some(oracle),
    )
    .await;

    let db_oid: u32 = source
        .psql_one("SELECT oid FROM pg_database WHERE datname = current_database()")
        .expect("db oid")
        .parse()
        .unwrap();
    let toast_filenode: u32 = source
        .psql_one(
            "SELECT relfilenode FROM pg_class WHERE oid = \
             (SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass)",
        )
        .expect("toast filenode")
        .parse()
        .unwrap();
    assert!(
        pipeline
            .stream
            .filter_mut()
            .shadow_rels()
            .is_some_and(|rels| rels.contains(&(db_oid, toast_filenode))),
        "shadow mode must route the TOAST relation's records to shadow",
    );

    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, 'v1', {BODY_SQL})"),
            "INSERT INTO public.doc SELECT g, repeat('f', 500), NULL \
             FROM generate_series(2, 17) g"
                .into(),
            format!("UPDATE public.doc SET meta = {META2_SQL} WHERE id = 1"),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    // The read happens against shadow's replay position, so shadow has to
    // have applied the chunk records before the pipeline drains
    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    assert_eq!(
        ch.query(&format!(
            "SELECT meta = {META2_SQL} FROM walshadow_test.doc \
             WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch meta"),
        "1",
        "UPDATE's meta wins under RIF",
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT body = {BODY_SQL} FROM walshadow_test.doc \
             WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch body"),
        "1",
        "the unchanged pointer resolved to the full value out of shadow",
    );
    assert_eq!(
        ch.query(
            "SELECT length(body) FROM walshadow_test.doc \
                  WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
        )
        .expect("ch body length"),
        "8192",
        "a truncated read would still compare unequal, so pin the length too",
    );

    // Nothing filled: a default or a superseded fill would make the value
    // assertions above pass for the wrong reason on a NULL-able column
    assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 0);
    assert_eq!(
        stats.toast_values_filled_superseded.load(Ordering::Relaxed),
        0
    );
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);
    assert!(
        stats.toast_values_fetched.load(Ordering::Relaxed) > 0,
        "the value came from a store read, not from the in-xact chunk map",
    );
    assert_no_mirror(&ch, &source, &stats);
}

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
    ) = fx::bootstrap_clusters(&tmp, SOURCE_DDL, slot.source, slot.shadow, slot.walsender).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let mappings = dest_table(&ch);

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
        |cfg| cfg.toast.mode = ToastMode::Disabled,
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
    assert_no_mirror(&ch, &source, &stats);
}
