//! Rewrite-generation + commit-time stash end-to-end (`plans/TOAST.md`
//! phase 3): records on filenodes invisible at record time stash raw in the
//! xact spill, resolve at commit via `relation_at(rfn, commit_lsn)`, decode
//! against the surviving toast rel, and close each marker-proven generation
//! with residual `O - B` tombstones — never a mirror truncate.
//!
//! `vacuum_full_rewrite_and_same_xact_stash` drills one flow:
//! * `VACUUM FULL` (content swap): chunks arrive as ordinary INSERTs on the
//!   transient toast filenode, resolve to the original toast oid; reused
//!   TIDs supersede, residual old TIDs tombstone at commit, pre-rewrite
//!   as-of windows stay intact (no destructive step)
//! * chunk_ids survive content swap: a post-rewrite unchanged-toast UPDATE
//!   rehydrates its value across the generation boundary
//! * same-xact TRUNCATE + toasted INSERT: post-truncate chunks decode at
//!   commit past the mirror wipe; main tuples stay fenced (skipped) until a
//!   replay fence exists
//! * same-xact CREATE + toasted INSERT: chunks decode into a fresh mirror
//! * CREATE + INSERT + DROP in one xact: unresolvable post-commit, records
//!   discard counted, end-state-neutral
//! * top-level abort: stash dies with the spill, no resolution runs
//!
//! `alter_rewrite_link_swap_retires_old_mirror`: rewriting ALTER (link
//! swap) resolves the generation to the surviving transient toast oid — a
//! new mirror — and retires the old mirror through the normal DROP
//! lifecycle (deferred past the persisted resume floor).
//!
//! Store-level `rewrite_barrier` semantics live in `toast_resolvers.rs`
//! (CH) and `src/toast.rs` (MemChunkStore) unit tests.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::ch_emitter::{ColumnMapping, TableTarget};
use walshadow::shadow_catalog::RelName;
use walshadow::toast::ToastMode;

const SOURCE_PORT: u16 = 17731;
const SHADOW_PORT: u16 = 17732;
const CH_TCP_PORT: u16 = 17733;
const CH_HTTP_PORT: u16 = 17734;
// 17735 reserved: ChServer interserver port = http + 1
const WALSENDER_PORT: u16 = 17736;
// alter_rewrite_link_swap_retires_old_mirror shifts every port +10

/// Distinct byte sums identify values in the mirror (EXTERNAL storage keeps
/// them uncompressed, so mirror bytes == raw length).
const BODY_A_SQL: &str = "repeat('a-value-dies-first!!', 512)"; // 10240
const BODY_B_SQL: &str = "repeat('b-survives-rewrite', 512)"; // 9216
const BODY_B_LEN: u64 = 9216;
const BODY_C_SQL: &str = "repeat('c-also-survives!', 512)"; // 8192
const BODY_C_LEN: u64 = 8192;
const BODY_D_SQL: &str = "repeat('d-truncated-insert!', 512)"; // 9728
const BODY_D_LEN: u64 = 9728;
const BODY_E_SQL: &str = "repeat('e-create-insert!!', 512)"; // 8704
const BODY_E_LEN: u64 = 8704;
/// Fat replacement meta: with id=2's page packed by fillers, the new
/// version can't fit in leftover space, forcing a cross-page update so the
/// full tuple (with the unchanged toast pointer) is logged — same trick as
/// `toast_tombstone_e2e.rs`.
const META3_SQL: &str = "repeat('v3-update-', 60)";

/// Live values at `max_lsn`: per-TID latest version, live rows only,
/// `(value count, total bytes)` so assertions don't depend on chunk_id
/// allocation order.
fn live_sum_sql(chunk_table: &str, max_lsn: &str) -> String {
    format!(
        "SELECT count(), sum(bytes) FROM (\
           SELECT chunk_id, sum(length(chunk_data)) AS bytes FROM (\
             SELECT argMax(chunk_id, _lsn) AS chunk_id, \
                    argMax(chunk_data, _lsn) AS chunk_data, \
                    argMax(_is_deleted, _lsn) AS dead \
             FROM {chunk_table} WHERE _lsn <= {max_lsn} \
             GROUP BY blkno, offnum\
           ) WHERE dead = 0 GROUP BY chunk_id\
         )"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vacuum_full_rewrite_and_same_xact_stash() {
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
    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    let chunk_table = format!("walshadow_test.pg_toast_{toast_relid}");
    // Pre-create the mirror + freeze merges: the pre-rewrite as-of
    // assertions below need history intact, and a global STOP MERGES
    // doesn't cover tables created after it.
    ch.query(&format!(
        "CREATE TABLE IF NOT EXISTS {chunk_table} (\
           `blkno` UInt32, `offnum` UInt16, `chunk_id` UInt32, `chunk_seq` UInt32, \
           `chunk_data` String, `_lsn` UInt64, `_is_deleted` UInt8, \
           INDEX `idx_chunk_id` `chunk_id` TYPE bloom_filter GRANULARITY 1\
         ) ENGINE = ReplacingMergeTree(`_lsn`, `_is_deleted`) ORDER BY (`blkno`, `offnum`)"
    ))
    .expect("pre-create mirror");
    ch.query(&format!("SYSTEM STOP MERGES {chunk_table}"))
        .expect("stop merges");
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
            app_name: "walshadow-toast-rewrite",
            ddl: Some(fx::DdlPipelineArgs::default()),
        },
        move |cfg| {
            cfg.toast.mode = ToastMode::ClickHouse;
        },
    )
    .await;
    let stats = pipeline.stats.clone();

    // Values A (dies pre-rewrite), B, C. Fillers pack the heap pages so the
    // post-rewrite fat-meta UPDATE can't stay on-page (full tuple logged).
    // A's death leaves live TIDs whose positions the rewrite won't reuse:
    // the packed new generation starts at (0,1), so the residual-death
    // barrier has work to do.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, 'a', {BODY_A_SQL})"),
            format!("INSERT INTO public.doc VALUES (2, 'b', {BODY_B_SQL})"),
            format!("INSERT INTO public.doc VALUES (3, 'c', {BODY_C_SQL})"),
            "INSERT INTO public.doc SELECT g, repeat('f', 500), NULL \
             FROM generate_series(4, 40) g"
                .into(),
            "DELETE FROM public.doc WHERE id = 1".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    // A dead, B + C live.
    let live_bc = format!("2\t{}", BODY_B_LEN + BODY_C_LEN);
    fx::wait_query(
        &ch,
        &live_sum_sql(&chunk_table, "18446744073709551615"),
        &live_bc,
        "pre-rewrite live set never reached B + C",
    )
    .await;
    let pre_bound = ch
        .query(&format!("SELECT max(_lsn) FROM {chunk_table}"))
        .expect("pre-rewrite bound");
    let tombs_pre: u64 = ch
        .query(&format!(
            "SELECT countIf(_is_deleted = 1) FROM {chunk_table}"
        ))
        .expect("pre-rewrite tombstones")
        .parse()
        .unwrap();
    assert!(tombs_pre > 0, "A's chunk deletes must have tombstoned");

    // VACUUM FULL: content swap, chunks stash on the transient toast
    // filenode and resolve to the original toast oid at commit.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "VACUUM FULL public.doc".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "rewrite segment never shipped");

    // New generation converges to the same live set: reused TIDs supersede
    // through higher `_lsn`, residual old TIDs die at commit.
    fx::wait_query(
        &ch,
        &live_sum_sql(&chunk_table, "18446744073709551615"),
        &live_bc,
        "post-rewrite live set never converged to B + C",
    )
    .await;
    assert_eq!(
        stats.toast_rewrite_barriers.load(Ordering::Relaxed),
        1,
        "rewrite generation closed by one residual barrier"
    );
    assert_eq!(
        stats.toast_mirror_truncates.load(Ordering::Relaxed),
        0,
        "rewrite must never truncate the mirror"
    );
    assert!(
        stats.toast_stash_decoded.load(Ordering::Relaxed) > 0,
        "generation chunks decoded from the stash"
    );
    // Births landed past the pre-rewrite bound; residual deaths were
    // store-side (INSERT..SELECT), so `toast_tombstones_stored` (WAL-delete
    // tombstones) must not have moved while dead rows grew.
    assert_eq!(
        stats.toast_tombstones_stored.load(Ordering::Relaxed),
        tombs_pre,
        "residual deaths are store-side, not WAL tombstones"
    );
    let tombs_post: u64 = ch
        .query(&format!(
            "SELECT countIf(_is_deleted = 1) FROM {chunk_table}"
        ))
        .expect("post-rewrite tombstones")
        .parse()
        .unwrap();
    assert!(
        tombs_post > tombs_pre,
        "residual old TIDs must tombstone ({tombs_pre} -> {tombs_post})"
    );
    // History preserved: the pre-rewrite window still resolves B + C whole.
    assert_eq!(
        ch.query(&live_sum_sql(&chunk_table, &pre_bound))
            .expect("pre-rewrite as-of"),
        live_bc,
        "pre-rewrite as-of window must survive the rewrite"
    );

    // chunk_ids survive content swap: an unchanged-toast UPDATE after the
    // rewrite rehydrates B across the generation boundary.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("UPDATE public.doc SET meta = {META3_SQL} WHERE id = 2"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "rehydrate segment never shipped");
    fx::wait_query(
        &ch,
        &format!(
            "SELECT countIf(meta = {META3_SQL} AND body = {BODY_B_SQL}) \
             FROM walshadow_test.doc WHERE id = 2"
        ),
        "1",
        "post-rewrite rehydrating UPDATE never landed with B's bytes",
    )
    .await;
    assert!(
        stats.toast_values_fetched.load(Ordering::Relaxed) >= 1,
        "rehydrate fetched B from the mirror across generations"
    );

    // Same-xact TRUNCATE + toasted INSERT (single psql -c = one xact):
    // post-truncate chunks ride the toast rel's new filenode, stash, and
    // decode at commit past the mirror wipe. Main tuples stay fenced.
    let skipped_before = stats.toast_stash_skipped.load(Ordering::Relaxed);
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!(
                "TRUNCATE public.doc; \
                 INSERT INTO public.doc VALUES (70, 'd', {BODY_D_SQL})"
            ),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "truncate segment never shipped");
    fx::wait_query(
        &ch,
        &format!(
            "SELECT countDistinct(chunk_id), sum(length(chunk_data)), \
             countIf(_is_deleted = 1) FROM {chunk_table}"
        ),
        &format!("1\t{BODY_D_LEN}\t0"),
        "mirror should hold exactly D after same-xact TRUNCATE + INSERT",
    )
    .await;
    assert_eq!(stats.toast_mirror_truncates.load(Ordering::Relaxed), 1);
    assert!(
        stats.toast_stash_skipped.load(Ordering::Relaxed) > skipped_before,
        "fenced main tuples counted as skipped"
    );
    // Destination truncate applied; the post-truncate main row stays fenced
    // until a replay fence exists.
    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.doc",
        "0",
        "dest table should be empty after the truncate barrier",
    )
    .await;

    // Same-xact CREATE + toasted INSERT: a brand-new toast rel's chunks
    // decode at commit into a fresh mirror.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!(
                "CREATE TABLE public.doc2 (id int PRIMARY KEY, body text); \
                 ALTER TABLE public.doc2 ALTER COLUMN body SET STORAGE EXTERNAL; \
                 INSERT INTO public.doc2 VALUES (1, {BODY_E_SQL})"
            ),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "create+insert segment never shipped");
    let toast2 = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc2'::regclass")
        .expect("doc2 toast relid");
    fx::wait_query(
        &ch,
        &live_sum_sql(
            &format!("walshadow_test.pg_toast_{toast2}"),
            "18446744073709551615",
        ),
        &format!("1\t{BODY_E_LEN}"),
        "same-xact CREATE + INSERT chunks never reached a fresh mirror",
    )
    .await;

    // CREATE + INSERT + DROP in one xact: the filenode resolves to nothing
    // post-commit; records discard, counted, end-state-neutral.
    let discarded_before = stats.toast_stash_discarded.load(Ordering::Relaxed);
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!(
                "CREATE TABLE public.doc3 (id int PRIMARY KEY, body text); \
                 ALTER TABLE public.doc3 ALTER COLUMN body SET STORAGE EXTERNAL; \
                 INSERT INTO public.doc3 VALUES (1, {BODY_A_SQL}); \
                 DROP TABLE public.doc3"
            ),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "create+drop segment never shipped");
    assert!(
        stats.toast_stash_discarded.load(Ordering::Relaxed) > discarded_before,
        "create-then-drop records must discard with a count"
    );

    // Top-level abort: the stash dies with the spill, resolution never runs.
    let discarded_at_abort = stats.toast_stash_discarded.load(Ordering::Relaxed);
    let decoded_at_abort = stats.toast_stash_decoded.load(Ordering::Relaxed);
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!(
                "BEGIN; \
                 CREATE TABLE public.doc4 (id int PRIMARY KEY, body text); \
                 ALTER TABLE public.doc4 ALTER COLUMN body SET STORAGE EXTERNAL; \
                 INSERT INTO public.doc4 VALUES (1, {BODY_A_SQL}); \
                 ROLLBACK"
            ),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "abort segment never shipped");
    assert_eq!(
        stats.toast_stash_discarded.load(Ordering::Relaxed),
        discarded_at_abort,
        "abort discards without resolution"
    );
    assert_eq!(
        stats.toast_stash_decoded.load(Ordering::Relaxed),
        decoded_at_abort,
        "abort must not decode stashed records"
    );

    let decoder_stats = pipeline.sinks.decoder.stats_handle();
    pipeline.shutdown().await.expect("pipeline drains clean");

    assert!(decoder_stats.toast_stash_buffered.load(Ordering::Relaxed) > 0);
    assert_eq!(
        decoder_stats.toast_chunks_malformed.load(Ordering::Relaxed),
        0
    );
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

/// Rewriting ALTER always link-swaps (PG `src/backend/commands/tablecmds.c`
/// passes `swap_toast_by_content = false`): the transient toast rel
/// survives as the table's new toast relation, the old toast rel drops
/// with the transient heap. The generation resolves to a NEW toast oid —
/// fresh mirror — and the old mirror retires through the DROP lifecycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_rewrite_link_swap_retires_old_mirror() {
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
        "CREATE TABLE public.doc (id int PRIMARY KEY, body text);\n\
         ALTER TABLE public.doc ALTER COLUMN body SET STORAGE EXTERNAL;\n",
        SOURCE_PORT + 10,
        SHADOW_PORT + 10,
        WALSENDER_PORT + 10,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT + 10, CH_HTTP_PORT + 10).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let old_toast = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("old toast relid");
    let old_table = format!("walshadow_test.pg_toast_{old_toast}");

    // No dest mapping: main rows are unsupported-relation no-ops; the toast
    // mirror flows regardless. `ddl` arms the pg_class-delete sweep the
    // retire rides.
    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: CH_TCP_PORT + 10,
            mappings: vec![],
            app_name: "walshadow-toast-alter-rewrite",
            ddl: Some(fx::DdlPipelineArgs::default()),
        },
        move |cfg| {
            cfg.toast.mode = ToastMode::ClickHouse;
        },
    )
    .await;
    let stats = pipeline.stats.clone();

    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("INSERT INTO public.doc VALUES (1, {BODY_B_SQL})"),
            format!("INSERT INTO public.doc VALUES (2, {BODY_C_SQL})"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    let live_bc = format!("2\t{}", BODY_B_LEN + BODY_C_LEN);
    fx::wait_query(
        &ch,
        &live_sum_sql(&old_table, "18446744073709551615"),
        &live_bc,
        "old mirror never populated",
    )
    .await;

    // int -> bigint forces a table rewrite; toast values are re-saved into
    // the transient rel's toast heap with fresh value ids (link swap mints
    // new ones, unlike content swap).
    let driver = fx::spawn_workload(
        &source,
        vec![
            "ALTER TABLE public.doc ALTER COLUMN id TYPE bigint".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "alter segment never shipped");

    let new_toast = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("new toast relid");
    assert_ne!(
        old_toast, new_toast,
        "link swap keeps the transient toast oid"
    );
    fx::wait_query(
        &ch,
        &live_sum_sql(
            &format!("walshadow_test.pg_toast_{new_toast}"),
            "18446744073709551615",
        ),
        &live_bc,
        "rewritten generation never reached the new mirror",
    )
    .await;
    assert_eq!(stats.toast_rewrite_barriers.load(Ordering::Relaxed), 1);
    assert!(stats.toast_stash_decoded.load(Ordering::Relaxed) > 0);
    assert!(
        stats.toast_stash_skipped.load(Ordering::Relaxed) > 0,
        "rewritten main tuples stay fenced"
    );
    assert_eq!(stats.toast_stash_discarded.load(Ordering::Relaxed), 0);

    // Old mirror retires through the DROP lifecycle: queued at the ALTER's
    // commit, deferred until the persisted resume floor passes it.
    assert_eq!(
        ch.query(&format!("SELECT count() > 0 FROM {old_table}"))
            .expect("old mirror rows"),
        "1",
        "retire must defer while the resume floor hasn't passed the drop",
    );
    assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 0);
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
        &format!("SELECT count() FROM {old_table}"),
        "0",
        "old mirror should retire once the floor passed",
    )
    .await;
    assert_eq!(
        ch.query(&format!(
            "SELECT count() FROM system.tables \
             WHERE database = 'walshadow_test' AND name = 'pg_toast_{old_toast}'"
        ))
        .expect("old mirror existence"),
        "1",
        "retired mirror table must survive"
    );

    pipeline.shutdown().await.expect("pipeline drains clean");
    assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 1);
    assert_eq!(stats.toast_mirror_truncates.load(Ordering::Relaxed), 0);
    assert_eq!(stats.toast_fetch_miss.load(Ordering::Relaxed), 0);
}
