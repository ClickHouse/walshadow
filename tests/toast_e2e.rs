//! TOAST end-to-end through the production pipeline wiring: source PG →
//! walshadow filter → shadow PG → heap decoder → xact buffer → reorder →
//! decode pool → inserter pool → spawned `clickhouse server`, with
//! `[toast] mode = clickhouse`.
//!
//! Under REPLICA IDENTITY FULL the old tuple logs every column (full old
//! TOAST value); the new tuple omits the unchanged TOAST. The winning
//! version must still carry the full value, rehydrated from the store.
//! Also pins the store-created mirror's v2 schema (TID key + tombstone
//! column).
//!
//! Default-identity reassembly, tombstone churn and mirror lifecycle live
//! in `toast_tombstone_e2e.rs`; resolver/store semantics in isolation in
//! `toast_resolvers.rs`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;
use walshadow::ch_emitter::TableTarget;
use walshadow::shadow_catalog::RelName;
use walshadow::toast::ToastMode;

const SOURCE_PORT: u16 = 17591;
const SHADOW_PORT: u16 = 17592;
const CH_TCP_PORT: u16 = 17593;
const CH_HTTP_PORT: u16 = 17594;
// 17595 reserved: ChServer interserver port = http + 1
const WALSENDER_PORT: u16 = 17596;

/// 16 bytes * 512 = 8192, comfortably past the ~2KB toast threshold and
/// spanning multiple ~2KB toast chunks.
const BODY_SQL: &str = "repeat('walshadow-toast-', 512)";
/// UPDATE's replacement `meta`: big enough that the new tuple version can't
/// fit in the packed page's leftover space (forces a cross-page update, see
/// workload comment).
const META2_SQL: &str = "repeat('v2-update-', 60)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replident_full_unchanged_toast_update() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
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
        // EXTERNAL: out-of-line, uncompressed — guarantees the 8KB body
        // toasts into multiple chunks instead of compressing inline.
        "CREATE TABLE public.doc (id int PRIMARY KEY, meta text, body text);\n\
         ALTER TABLE public.doc ALTER COLUMN body SET STORAGE EXTERNAL;\n\
         ALTER TABLE public.doc REPLICA IDENTITY FULL;\n",
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
            ch_tcp_port: CH_TCP_PORT,
            mappings,
            app_name: "walshadow-toast-rif",
            ddl: None,
        },
        |cfg| cfg.toast.mode = ToastMode::ClickHouse,
    )
    .await;

    // Autocommit xacts: the INSERT's chunks reassemble inline (same xact)
    // and persist to the CH chunk store at commit; the UPDATE leaves `body`
    // untouched, so its new tuple carries the pointer with no chunks in WAL —
    // the decode pool must rehydrate from the store.
    //
    // PG prefix/suffix-elides unchanged columns when old + new tuple share a
    // page (`log_heap_update`), which would strip the pointer from WAL
    // entirely. Fillers pack id=1's page and the fat replacement `meta`
    // (600B, larger than any leftover slot) forces the new version onto
    // another page, so the full tuple is logged.
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

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    // ORDER BY _lsn DESC, not argMax: Nullable aggregates skip NULL rows, so
    // argMax(body, _lsn) would silently fall back to the INSERT version if
    // the UPDATE's body landed NULL.
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
        "unchanged TOAST body survives the RIF update",
    );
    assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 0);
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);

    // Chunk mirror landed in CH under the source's toast relid with the v2
    // store-created schema: TID key + tombstone column.
    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    assert_eq!(
        ch.query(&format!(
            "SELECT groupArray(name) FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 'pg_toast_{toast_relid}'"
        ))
        .expect("mirror columns"),
        "['blkno','offnum','chunk_id','chunk_seq','chunk_data','_lsn','_is_deleted']"
    );
}
