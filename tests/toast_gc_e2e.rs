//! TOAST chunk GC end-to-end (`plans/TOAST.md`): source PG →
//! walshadow pipeline → CH chunk store, TID-death-driven sweep. No source
//! SQL session: deaths resolve from decoded WAL deletes at commit.
//!
//! Drills the acceptance list in one flow:
//! * UPDATE rewrites a toasted value to a new valueid → old valueid's death
//!   resolves (same-xact chunk delete) and its chunks collect
//! * DELETE of the referring row → its valueid collects
//! * deaths apply only once `emitter_ack ≥ death_lsn` (pending until the
//!   death's own commit drains; the sweep loop below converges on that)
//! * a subsequent referring row (unchanged-toast UPDATE) still reassembles
//!   from the swept store
//! * toast deletes tick `toast_chunk_deletes` + `toast_deaths_resolved`,
//!   never `toast_chunks_malformed` / `toast_deaths_unresolved`
//!
//! Store/death semantics in isolation live in `toast_resolvers.rs` (CH),
//! `src/toast.rs` (disk) and `src/toast_tid.rs` (tracker) unit tests.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use walshadow::ch_emitter::{ColumnMapping, TableTarget};
use walshadow::shadow_catalog::RelName;
use walshadow::toast::ToastMode;
use walshadow::toast_gc::ToastGc;

const SOURCE_PORT: u16 = 17661;
const SHADOW_PORT: u16 = 17662;
const CH_TCP_PORT: u16 = 17663;
const CH_HTTP_PORT: u16 = 17664;
// 17665 reserved: ChServer interserver port = http + 1
const WALSENDER_PORT: u16 = 17666;

/// Distinct lengths per valueid so store byte-sums identify survivors.
const BODY_A_SQL: &str = "repeat('walshadow-toast-', 512)"; // 8192
const BODY_B_SQL: &str = "repeat('gc-keeps-me-alive!', 512)"; // 9216
const BODY_B_LEN: u64 = 9216;
const BODY_C_SQL: &str = "repeat('c-dead-on-delete', 512)"; // 8192
/// Fat replacement meta: with id=1's page packed by fillers, the new version
/// can't fit in leftover space, forcing a cross-page update so the full
/// tuple (with the unchanged toast pointer) is logged — same trick as
/// `toast_e2e.rs`.
const META3_SQL: &str = "repeat('v3-update-', 60)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_collects_dead_values_and_survivors_reassemble() {
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

    let tid_journal = tmp.path().join("toast-tids.journal");
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
            app_name: "walshadow-toast-gc",
            ddl: None,
        },
        move |cfg| {
            cfg.toast.mode = ToastMode::ClickHouse;
            cfg.toast.tid_journal = Some(tid_journal);
        },
    )
    .await;

    // Three valueids reach the store: A (id=1 INSERT), B (id=1 body rewrite;
    // A dies via heap_toast_delete in the same xact), C (id=50, dies with its
    // row's DELETE). Fillers pack id=1's pages for the later meta update.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, 'v1', {BODY_A_SQL})"),
            "INSERT INTO public.doc SELECT g, repeat('f', 500), NULL \
             FROM generate_series(2, 17) g"
                .into(),
            format!("UPDATE public.doc SET body = {BODY_B_SQL} WHERE id = 1"),
            "INSERT INTO public.doc SELECT g, repeat('f', 500), NULL \
             FROM generate_series(18, 48) g"
                .into(),
            format!("INSERT INTO public.doc VALUES (50, 'c', {BODY_C_SQL})"),
            "DELETE FROM public.doc WHERE id = 50".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    let target = pipeline.stream.dispatched_lsn();
    shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");

    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    let chunk_table = format!("walshadow_test.pg_toast_{toast_relid}");
    // Chunk puts ride commit drains; poll until all three valueids landed.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let distinct = ch
            .query(&format!(
                "SELECT countDistinct(chunk_id) FROM {chunk_table}"
            ))
            .unwrap_or_else(|_| "0".into());
        if distinct == "3" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "store never reached 3 valueids (at {distinct})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Sweep with the pipeline's own store + tracker + ack watermark — the
    // daemon's wiring, driven directly. Deaths stay pending until the ack
    // passes their commit; loop until both collect.
    let gc = ToastGc {
        store: pipeline.handle.toast.store().expect("ch store armed"),
        tracker: pipeline.handle.toast.tracker().expect("tid tracker armed"),
        emitter_ack: pipeline.ack.clone(),
        interval: Duration::from_secs(3600),
        stats: pipeline.stats.clone(),
    };
    let mut deleted = 0u64;
    let deadline = Instant::now() + Duration::from_secs(20);
    while deleted < 2 {
        let outcome = gc.sweep_once().await.expect("sweep completes");
        deleted += outcome.deleted;
        assert!(
            Instant::now() < deadline,
            "GC never collected both deaths (deleted {deleted})"
        );
        if deleted < 2 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    assert_eq!(deleted, 2, "A and C dead, B live");
    assert!(pipeline.stats.toast_gc_sweeps.load(Ordering::Relaxed) >= 1);
    assert_eq!(
        pipeline
            .stats
            .toast_gc_values_deleted
            .load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        pipeline.stats.toast_deaths_resolved.load(Ordering::Relaxed),
        2,
        "both deaths resolved from decoded WAL deletes"
    );
    assert_eq!(
        pipeline
            .stats
            .toast_deaths_unresolved
            .load(Ordering::Relaxed),
        0
    );

    // Post-GC referring row: unchanged-toast meta update must rehydrate B
    // from the swept store.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("UPDATE public.doc SET meta = {META3_SQL} WHERE id = 1"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "post-GC segment never shipped");
    let target = pipeline.stream.dispatched_lsn();
    shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up post-GC");

    let stats = pipeline.stats.clone();
    let decoder_stats = pipeline.sinks.decoder.stats_handle();
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Only B survives in the store, byte-complete.
    assert_eq!(
        ch.query(&format!(
            "SELECT countDistinct(chunk_id) FROM {chunk_table}"
        ))
        .expect("distinct after sweep"),
        "1",
        "dead valueids collected, live one kept"
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT sum(length(chunk_data)) FROM {chunk_table}"
        ))
        .expect("chunk bytes"),
        BODY_B_LEN.to_string(),
        "surviving chunks sum to the live value"
    );

    // Winning id=1 version: post-GC meta with body rebuilt from the store.
    assert_eq!(
        ch.query(&format!(
            "SELECT meta = {META3_SQL} FROM walshadow_test.doc \
             WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch meta"),
        "1",
        "post-GC UPDATE's meta wins"
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT body = {BODY_B_SQL} FROM walshadow_test.doc \
             WHERE id = 1 ORDER BY _lsn DESC LIMIT 1"
        ))
        .expect("ch body"),
        "1",
        "body rehydrated from the swept store"
    );
    assert!(
        stats.toast_values_fetched.load(Ordering::Relaxed) >= 1,
        "meta update fetched the unchanged value from the store"
    );
    assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 0);
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);

    // Decoder routing: toast deletes (A's + C's chunk deletes) land on their
    // own counter, never `toast_chunks_malformed`.
    assert!(
        decoder_stats.toast_chunk_deletes.load(Ordering::Relaxed) >= 2,
        "toast chunk deletes observed"
    );
    assert_eq!(
        decoder_stats.toast_chunks_malformed.load(Ordering::Relaxed),
        0,
        "toast deletes must not count as malformed"
    );
}
