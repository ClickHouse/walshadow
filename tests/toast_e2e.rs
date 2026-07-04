//! TOAST end-to-end through the production pipeline wiring: source PG →
//! walshadow filter → shadow PG → heap decoder → xact buffer → reorder →
//! decode pool → inserter pool → spawned `clickhouse server`, with
//! `[toast] mode = clickhouse`.
//!
//! Covers both reassembly paths:
//! * INSERT of an out-of-line (`STORAGE EXTERNAL`) value — chunks ride the
//!   same xact, inline reassembly; reorder persists them to the CH
//!   `pg_toast_<relid>` mirror at commit.
//! * UPDATE of a different column — PG does not re-log the unchanged toast
//!   value, the new tuple carries only the pointer, so the decode pool must
//!   rehydrate the chunks from the store (`fetch_into`).
//!
//! Resolver/store semantics in isolation live in `toast_resolvers.rs`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::ch_emitter::ColumnMapping;
use walshadow::ch_emitter::TableTarget;
use walshadow::shadow_catalog::RelName;
use walshadow::toast::ToastMode;

const SOURCE_PORT: u16 = 17581;
const SHADOW_PORT: u16 = 17582;
const CH_TCP_PORT: u16 = 17583;
const CH_HTTP_PORT: u16 = 17584;
// 17585 reserved: ChServer interserver port = http + 1
const WALSENDER_PORT: u16 = 17586;

const RIF_SOURCE_PORT: u16 = 17591;
const RIF_SHADOW_PORT: u16 = 17592;
const RIF_CH_TCP_PORT: u16 = 17593;
const RIF_CH_HTTP_PORT: u16 = 17594;
// 17595 reserved: ChServer interserver port = http + 1
const RIF_WALSENDER_PORT: u16 = 17596;

/// 16 bytes * 512 = 8192, comfortably past the ~2KB toast threshold and
/// spanning multiple ~2KB toast chunks.
const BODY_SQL: &str = "repeat('walshadow-toast-', 512)";
const BODY_LEN: u64 = 8192;
/// UPDATE's replacement `meta`: big enough that the new tuple version can't
/// fit in the packed page's leftover space (forces a cross-page update, see
/// workload comment).
const META2_SQL: &str = "repeat('v2-update-', 60)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toasted_value_replicates_and_rehydrates() {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !fx::pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
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
         ALTER TABLE public.doc ALTER COLUMN body SET STORAGE EXTERNAL;\n",
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
    // `_lsn` in the sort key: this test asserts both id=1 versions (INSERT +
    // UPDATE) are present. With `ORDER BY id` they share a key and RMT
    // collapses them whenever they land in one part — at insert via
    // `optimize_on_insert`, or later via merge — which made the count flaky
    // across PG versions (block layout shifts). Distinct keys never collapse.
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.doc (\
            id Int32,\
            meta Nullable(String),\
            body Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY (id, _lsn)",
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
            app_name: "walshadow-toast-e2e",
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
    assert!(
        shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain",
    );

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Winning (max _lsn) version: UPDATE's meta, with the toast value
    // rebuilt from the store, byte-identical to the source expression.
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
        "UPDATE's meta wins"
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT body = {BODY_SQL} FROM walshadow_test.doc \
             WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch body"),
        "1",
        "rehydrated body byte-identical to source value"
    );
    // Both versions carry the full value: the INSERT's via inline in-xact
    // reassembly, the UPDATE's via the store fetch.
    assert_eq!(
        ch.query(&format!(
            "SELECT countIf(length(body) = {BODY_LEN}) FROM walshadow_test.doc WHERE id = 1"
        ))
        .expect("ch versions"),
        "2",
        "both row versions hold the full detoasted body"
    );

    // Chunk mirror table landed in CH under the source's toast relid, chunks
    // summing to the value (chunk count is PG build-dependent, only >= 2 is
    // pinned).
    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    let chunk_table = format!("walshadow_test.pg_toast_{toast_relid}");
    let chunk_count: u64 = ch
        .query(&format!("SELECT count() FROM {chunk_table}"))
        .expect("chunk count")
        .parse()
        .unwrap();
    assert!(chunk_count >= 2, "multi-chunk value, got {chunk_count}");
    assert_eq!(
        ch.query(&format!(
            "SELECT sum(length(chunk_data)) FROM {chunk_table}"
        ))
        .expect("chunk bytes"),
        BODY_LEN.to_string(),
        "stored chunks sum to the toasted value"
    );

    // Resolver counters: chunks persisted at the INSERT's commit, one value
    // fetched back for the UPDATE, no fills or gaps.
    assert_eq!(
        stats.toast_chunks_stored.load(Ordering::Relaxed),
        chunk_count,
        "every WAL chunk persisted to the store"
    );
    assert!(
        stats.toast_values_fetched.load(Ordering::Relaxed) >= 1,
        "UPDATE rehydrated the unchanged toast value from the store"
    );
    assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 0);
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);
}

/// Under REPLICA IDENTITY FULL the old tuple logs every column (full old TOAST
/// value); the new tuple omits the unchanged TOAST. The winning version must
/// still carry the full value, rehydrated from the store.
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
        "CREATE TABLE public.doc (id int PRIMARY KEY, meta text, body text);\n\
         ALTER TABLE public.doc ALTER COLUMN body SET STORAGE EXTERNAL;\n\
         ALTER TABLE public.doc REPLICA IDENTITY FULL;\n",
        RIF_SOURCE_PORT,
        RIF_SHADOW_PORT,
        RIF_WALSENDER_PORT,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, RIF_CH_TCP_PORT, RIF_CH_HTTP_PORT).expect("spawn ch");
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
            ch_tcp_port: RIF_CH_TCP_PORT,
            mappings,
            app_name: "walshadow-toast-rif",
            ddl: None,
        },
        |cfg| cfg.toast.mode = ToastMode::ClickHouse,
    )
    .await;

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
}
