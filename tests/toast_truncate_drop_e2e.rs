//! TOAST mirror retirement across pipeline restarts (`plans/TOAST.md`
//! phase 2): owner DROP paths that need a rebuilt pipeline.
//!
//! * cold-restart DROP — a rebuilt pipeline (empty `prev_known`, no chunk
//!   decode) still retires the mirror off the `seed_baseline`-warmed
//!   toast oid
//! * crash-replay — same-xact UPDATE (toast value untouched) + DROP,
//!   pipeline rebuilt before the floor advances: replay must find the
//!   mirror intact, destination bytes exact, zero fills
//! * ledger recovery — floor passes the drop segment before the stop and
//!   no later commit flushes the queue; the rebuilt pipeline's standup
//!   retires off the persisted `toast_retires.bin` alone (resume never
//!   replays the drop)
//!
//! Single-pipeline TRUNCATE + deferred-DROP-retire flow lives in
//! `toast_tombstone_e2e.rs`; store-level wipe semantics in
//! `toast_resolvers.rs` (CH) and `src/toast.rs` (MemChunkStore) unit tests.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use walshadow::ch_emitter::{ColumnMapping, TableTarget};
use walshadow::shadow_catalog::RelName;
use walshadow::toast::ToastMode;

const SOURCE_PORT: u16 = 17671;
const SHADOW_PORT: u16 = 17672;
const CH_TCP_PORT: u16 = 17673;
const CH_HTTP_PORT: u16 = 17674;
// 17675 reserved: ChServer interserver port = http + 1
const WALSENDER_PORT: u16 = 17676;
// drop_crash_replay_keeps_referrer_bytes shifts every port +10
// drop_retire_survives_restart_from_ledger shifts every port +20

const BODY_SQL: &str = "repeat('dies-with-owner-drop', 512)"; // 10240

const DOC_SCHEMA_SQL: &str = "CREATE TABLE public.doc (id int PRIMARY KEY, meta text, body text);\n\
     ALTER TABLE public.doc ALTER COLUMN body SET STORAGE EXTERNAL;\n";

fn create_doc_dest(ch: &fx::ChServer) {
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
}

fn doc_mappings() -> Vec<fx::TableMappingSpec> {
    vec![fx::TableMappingSpec {
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
    }]
}

/// Cold-restart DROP: the mirror predates the pipeline (a prior run built
/// it) and no chunk decode warms the toast descriptor before the owner
/// DROP. `seed_baseline` must have put owner + toast oids in `prev_known`
/// for the sweep to surface both; without it the toast `Dropped` never
/// fires and the mirror leaks indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_restart_drop_retires_mirror() {
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
        DOC_SCHEMA_SQL,
        SOURCE_PORT,
        SHADOW_PORT,
        WALSENDER_PORT,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    create_doc_dest(&ch);

    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    let chunk_table = format!("walshadow_test.pg_toast_{toast_relid}");

    // Run 1: populate the mirror, then drain and drop the whole pipeline —
    // the daemon-restart stand-in (catalog, prev_known, chunk caches all
    // rebuilt from scratch below).
    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state: shadow_stream_state.clone(),
            ch_database: "walshadow_test",
            ch_tcp_port: CH_TCP_PORT,
            mappings: doc_mappings(),
            app_name: "walshadow-toast-cold-restart-1",
            ddl: Some(fx::DdlPipelineArgs::default()),
        },
        move |cfg| {
            cfg.toast.mode = ToastMode::ClickHouse;
        },
    )
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, 'v1', {BODY_SQL})"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    fx::wait_query(
        &ch,
        &format!("SELECT countDistinct(chunk_id) FROM {chunk_table}"),
        "1",
        "chunks never reached the mirror",
    )
    .await;
    pipeline
        .shutdown()
        .await
        .expect("first pipeline drains clean");

    // Run 2: fresh pipeline, cold prev_known. Only seed_baseline knows the
    // toast oid — the DROP is the first WAL this pipeline decodes.
    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: CH_TCP_PORT,
            mappings: doc_mappings(),
            app_name: "walshadow-toast-cold-restart-2",
            ddl: Some(fx::DdlPipelineArgs::default()),
        },
        move |cfg| {
            cfg.toast.mode = ToastMode::ClickHouse;
        },
    )
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "DROP TABLE public.doc".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "drop segment never shipped");
    // Queued off the seed_baseline-warmed toast oid, deferred until the
    // floor passes.
    assert_eq!(
        ch.query(&format!("SELECT count() > 0 FROM {chunk_table}"))
            .expect("mirror rows"),
        "1",
        "retire must defer while the resume floor hasn't passed the drop",
    );
    pipeline.resume_floor.store(u64::MAX, Ordering::Release);
    let driver = fx::spawn_workload(
        &source,
        vec![
            "SELECT txid_current()".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "post-floor poke segment never shipped");
    fx::wait_query(
        &ch,
        &format!("SELECT count() FROM {chunk_table}"),
        "0",
        "cold-restart DROP must retire the mirror once the floor passed",
    )
    .await;
    assert_eq!(
        ch.query(&format!(
            "SELECT count() FROM system.tables \
             WHERE database = 'walshadow_test' AND name = 'pg_toast_{toast_relid}'"
        ))
        .expect("mirror existence"),
        "1",
        "retired mirror table must survive (replay fetch sees empty, never missing)"
    );

    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");

    assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 1);
    assert_eq!(stats.toast_mirror_truncates.load(Ordering::Relaxed), 0);
}

/// Crash-replay across a same-xact UPDATE + DROP. The UPDATE leaves the
/// toasted column untouched, so the new row version reuses the old pointer
/// with no chunk WAL in its xact — detoast must fetch the store. The
/// pipeline is rebuilt before the resume floor ever advances (restart
/// re-reads the un-switched segment, replaying the xact): the mirror must
/// still hold history, and the replayed referrer must not disturb the
/// destination — a wiped mirror would have NULL-filled at the same `_lsn`
/// as the durable original, equal-version rows dedup can't arbitrate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_crash_replay_keeps_referrer_bytes() {
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
        DOC_SCHEMA_SQL,
        SOURCE_PORT + 10,
        SHADOW_PORT + 10,
        WALSENDER_PORT + 10,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT + 10, CH_HTTP_PORT + 10).expect("spawn ch");
    create_doc_dest(&ch);

    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    let chunk_table = format!("walshadow_test.pg_toast_{toast_relid}");
    // Dest keys on (id, _lsn): both row versions persist as history. The
    // invariant is bodies stay full-length — a replayed fill would surface
    // as a NULL body at v2's `_lsn`.
    let dest_row_sql = "SELECT countIf(meta = 'v2'), countIf(body IS NULL), \
         min(length(body)), max(length(body)) \
         FROM walshadow_test.doc FINAL WHERE _is_deleted = 0";

    let build = |state| {
        fx::build_pipeline_with(
            fx::BuildPipelineArgs {
                tmp: &tmp,
                source: &source,
                shadow: &shadow,
                shadow_filter_dir: &shadow_filter_dir,
                shadow_stream_state: state,
                ch_database: "walshadow_test",
                ch_tcp_port: CH_TCP_PORT + 10,
                mappings: doc_mappings(),
                app_name: "walshadow-toast-crash-replay",
                ddl: Some(fx::DdlPipelineArgs::default()),
            },
            move |cfg| {
                cfg.toast.mode = ToastMode::ClickHouse;
            },
        )
    };
    let mut pipeline = build(shadow_stream_state.clone()).await;

    // Seed value; the switch ships its segment so the rebuilt pipeline
    // below re-reads only the update+drop xact's segment.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, 'v1', {BODY_SQL})"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "seed segment never shipped");
    fx::wait_query(
        &ch,
        dest_row_sql,
        "0\t0\t10240\t10240",
        "seed row never landed",
    )
    .await;

    // One xact: new row version reuses the seed's pointer, then the owner
    // drops. No WAL switch — the xact stays in the segment a restart
    // re-reads.
    let driver = fx::spawn_workload(
        &source,
        vec!["BEGIN; UPDATE public.doc SET meta = 'v2'; DROP TABLE public.doc; COMMIT".into()],
    );
    pump_until_query(
        &mut pipeline,
        &ch,
        dest_row_sql,
        "1\t0\t10240\t10240",
        "update+drop xact never drained",
    )
    .await;
    let _ = driver.join();
    // Crash window: retire deferred (floor never advanced), mirror intact.
    // Pre-deferral code wiped here and a replayed referrer NULL-filled.
    assert_eq!(
        ch.query(&format!("SELECT count() > 0 FROM {chunk_table}"))
            .expect("mirror rows"),
        "1",
        "mirror history must survive the drop until the floor passes",
    );
    assert_eq!(
        pipeline.stats.toast_mirror_retires.load(Ordering::Relaxed),
        0
    );
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Restart: re-reads from the segment start, replaying the update+drop
    // xact. The replayed referrer either skips (owner unresolvable at
    // shadow head) or re-emits byte-identical from the intact mirror —
    // never a fill.
    let mut pipeline = build(shadow_stream_state).await;
    let driver = fx::spawn_workload(
        &source,
        vec![
            "SELECT txid_current()".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "replay segment never shipped");

    assert_eq!(
        ch.query(dest_row_sql).expect("dest row"),
        "1\t0\t10240\t10240",
        "replay must not disturb the retained destination rows",
    );
    assert_eq!(
        ch.query(&format!("SELECT count() > 0 FROM {chunk_table}"))
            .expect("mirror rows"),
        "1",
        "mirror history must survive the replay (floor still unpersisted)",
    );

    let stats = pipeline.stats.clone();
    pipeline
        .shutdown()
        .await
        .expect("replay pipeline drains clean");

    assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 0);
    assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 0);
    assert_eq!(
        stats.toast_values_filled_superseded.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats.toast_values_filled_mismatch.load(Ordering::Relaxed),
        0
    );
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);
}

/// The leak window the retire ledger closes: run 1 queues the retire at
/// the DROP and the WAL switch puts the next boot's resume head past the
/// drop segment (durable-cursor stand-in), then the run stops with no
/// later commit to flush the queue. Resume never replays the drop, so
/// run 2's standup flush off `toast_retires.bin` is the only route to
/// the wipe — the mirror must come up empty with no poke commit and no
/// WAL pumped at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_retire_survives_restart_from_ledger() {
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
        DOC_SCHEMA_SQL,
        SOURCE_PORT + 20,
        SHADOW_PORT + 20,
        WALSENDER_PORT + 20,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT + 20, CH_HTTP_PORT + 20).expect("spawn ch");
    create_doc_dest(&ch);

    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    let chunk_table = format!("walshadow_test.pg_toast_{toast_relid}");
    let spill_dir = tmp.path().join("spill");

    let build = |state, app_name| {
        fx::build_pipeline_with(
            fx::BuildPipelineArgs {
                tmp: &tmp,
                source: &source,
                shadow: &shadow,
                shadow_filter_dir: &shadow_filter_dir,
                shadow_stream_state: state,
                ch_database: "walshadow_test",
                ch_tcp_port: CH_TCP_PORT + 20,
                mappings: doc_mappings(),
                app_name,
                ddl: Some(fx::DdlPipelineArgs::default()),
            },
            move |cfg| {
                cfg.toast.mode = ToastMode::ClickHouse;
            },
        )
    };

    // Run 1: populate the mirror, then DROP + switch. The switch moves the
    // next boot's resume head past the drop segment; the stop right after
    // is the window — no later commit ever enters flush_due_retires.
    let mut pipeline = build(shadow_stream_state.clone(), "walshadow-toast-ledger-1").await;
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, 'v1', {BODY_SQL})"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "seed segment never shipped");
    fx::wait_query(
        &ch,
        &format!("SELECT countDistinct(chunk_id) FROM {chunk_table}"),
        "1",
        "chunks never reached the mirror",
    )
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "DROP TABLE public.doc".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "drop segment never shipped");
    assert_eq!(
        ch.query(&format!("SELECT count() > 0 FROM {chunk_table}"))
            .expect("mirror rows"),
        "1",
        "retire must defer while the resume floor hasn't passed the drop",
    );
    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("first pipeline drains");
    assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 0);
    assert_eq!(
        walshadow::toast_retire::RetireLedger::load(&spill_dir)
            .await
            .expect("read ledger")
            .entries()
            .len(),
        1,
        "queued retire must be durable at the stop",
    );

    // Run 2: standup flush retires off the ledger before any WAL pumps.
    let pipeline = build(shadow_stream_state, "walshadow-toast-ledger-2").await;
    assert_eq!(
        ch.query(&format!("SELECT count() FROM {chunk_table}"))
            .expect("mirror rows"),
        "0",
        "boot flush must retire the mirror off the persisted ledger",
    );
    assert_eq!(
        ch.query(&format!(
            "SELECT count() FROM system.tables \
             WHERE database = 'walshadow_test' AND name = 'pg_toast_{toast_relid}'"
        ))
        .expect("mirror existence"),
        "1",
        "retired mirror table must survive (replay fetch sees empty, never missing)",
    );
    assert_eq!(
        pipeline.stats.toast_mirror_retires.load(Ordering::Relaxed),
        1
    );
    assert!(
        walshadow::toast_retire::RetireLedger::load(&spill_dir)
            .await
            .expect("read ledger")
            .is_empty(),
        "executed retire must leave the ledger",
    );
    pipeline.shutdown().await.expect("second pipeline drains");
}

/// Pump in short slices until `sql` returns `want`: commit drains progress
/// only while the pump runs, and without a WAL switch no segment ships to
/// end a plain `pump_segments` early.
async fn pump_until_query(
    pipeline: &mut fx::Pipeline,
    ch: &fx::ChServer,
    sql: &str,
    want: &str,
    what: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut got = String::new();
    loop {
        fx::pump_segments(pipeline, 1, Duration::from_secs(2)).await;
        got = ch.query(sql).unwrap_or(got);
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: want {want:?}, last {got:?}"
        );
    }
}
