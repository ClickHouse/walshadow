//! TOAST tombstone + mirror lifecycle end-to-end (`plans/TOAST.md`):
//! source PG → walshadow pipeline → TID-keyed CH mirror. No GC task, no
//! tracker: a chunk DELETE lands as a tombstone row, reclamation is
//! ReplacingMergeTree merge behavior, fetch is per-TID as-of state.
//!
//! Drills the acceptance list in one flow:
//! * INSERT of an out-of-line value reassembles inline (chunks ride the
//!   same xact) and persists every WAL chunk to the mirror exactly once
//!   (`toast_chunks_stored` equality)
//! * UPDATE rewrites a toasted value A → B: A's chunk deletes land as
//!   tombstones, the as-of aggregate at head sees only B live
//! * DELETE of a referring row: its value C tombstones the same way
//! * as-of at an LSN before A's death still resolves A whole (fetch
//!   semantics, not just end state)
//! * unchanged-toast UPDATE rehydrates B from the mirror, before and after
//!   `OPTIMIZE ... FINAL` collapses dead versions (merge reclaims bytes,
//!   no walshadow GC ran)
//! * owner `TRUNCATE` wipes the mirror via the reorder barrier: prior rows
//!   vanish *as history* (not tombstones), a following toasted INSERT
//!   repopulates. Same-xact post-truncate births stay out of scope: chunks
//!   ride the toast rel's new relfilenode, MVCC-invisible in shadow until
//!   the truncating xact commits (same-xact CREATE+INSERT sibling)
//! * owner `DROP` retires the mirror (emptied but kept, never
//!   `MissingMirror`), deferred until the persisted resume floor's segment
//!   passes the dropping commit — an immediate wipe would be visible to a
//!   crash-replay of that commit's own referrers
//! * counters: `toast_chunk_deletes` / `toast_tombstones_stored` /
//!   `toast_mirror_truncates` / `toast_mirror_retires` /
//!   `truncates_emitted` tick; `toast_chunks_malformed`, fills of every
//!   flavor and `toast_fetch_miss` stay zero
//!
//! Cold-restart DROP and crash-replay (rebuilt pipelines) live in
//! `toast_truncate_drop_e2e.rs`; REPLICA IDENTITY FULL in `toast_e2e.rs`;
//! store semantics in isolation in `toast_resolvers.rs` (CH) and
//! `src/toast.rs` (MemChunkStore) unit tests.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::ch_emitter::{ColumnMapping, TableTarget};
use walshadow::shadow_catalog::RelName;
use walshadow::toast::ToastMode;

const SOURCE_PORT: u16 = 17661;
const SHADOW_PORT: u16 = 17662;
const CH_TCP_PORT: u16 = 17663;
const CH_HTTP_PORT: u16 = 17664;
// 17665 reserved: ChServer interserver port = http + 1
const WALSENDER_PORT: u16 = 17666;

/// Distinct byte sums identify values in the mirror.
const BODY_A_SQL: &str = "repeat('walshadow-toast-', 512)"; // 8192
const BODY_A_LEN: u64 = 8192;
const BODY_B_SQL: &str = "repeat('gc-keeps-me-alive!', 512)"; // 9216
const BODY_B_LEN: u64 = 9216;
const BODY_C_SQL: &str = "repeat('c-dead-on-delete', 512)"; // 8192
/// Post-TRUNCATE repopulation, outlives the wipe.
const BODY_D_SQL: &str = "repeat('survives-truncate', 512)"; // 8704
const BODY_D_LEN: u64 = 8704;
/// Fat replacement meta: with id=1's page packed by fillers, the new version
/// can't fit in leftover space, forcing a cross-page update so the full
/// tuple (with the unchanged toast pointer) is logged — same trick as
/// `toast_e2e.rs`.
const META3_SQL: &str = "repeat('v3-update-', 60)";
const META4_SQL: &str = "repeat('v4-update!', 60)";

/// The fetch SQL's inner aggregate over the whole mirror at `max_lsn`:
/// per-TID latest version, live rows only, summed bytes per value.
fn live_values_sql(chunk_table: &str, max_lsn: &str) -> String {
    format!(
        "SELECT chunk_id, sum(length(chunk_data)) FROM (\
           SELECT argMax(chunk_id, _lsn) AS chunk_id, \
                  argMax(chunk_data, _lsn) AS chunk_data, \
                  argMax(_is_deleted, _lsn) AS dead \
           FROM {chunk_table} WHERE _lsn <= {max_lsn} \
           GROUP BY blkno, offnum\
         ) WHERE dead = 0 GROUP BY chunk_id ORDER BY chunk_id"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstones_supersede_then_truncate_wipes_then_drop_retires() {
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
    // Pre-create the mirror (the store's CREATE IF NOT EXISTS no-ops on it)
    // so merges can be frozen before any row lands: background merges
    // collapse superseded versions on their own schedule (the design's
    // superseded-fill case), and the lagging-bound assertions below need
    // history intact. A global STOP MERGES doesn't cover tables created
    // after it. Re-enabled for the merge stage.
    let toast_relid = source
        .psql_one("SELECT reltoastrelid FROM pg_class WHERE oid = 'public.doc'::regclass")
        .expect("source toast relid");
    let chunk_table = format!("walshadow_test.pg_toast_{toast_relid}");
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

    // `ddl: Some(..)` arms the pg_class-delete sweep + schema-event channel
    // the DROP stage rides; tombstones + TRUNCATE need neither.
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
            app_name: "walshadow-toast-tombstone",
            ddl: Some(fx::DdlPipelineArgs::default()),
        },
        move |cfg| {
            cfg.toast.mode = ToastMode::ClickHouse;
        },
    )
    .await;
    let stats = pipeline.stats.clone();

    // Three valueids reach the mirror: A (id=1 INSERT), B (id=1 body rewrite;
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

    // Rows ride commit drains; poll until all three values + both deaths'
    // tombstones landed (A + C are 5 chunks each at ~2KB chunk size).
    fx::wait_query(
        &ch,
        &format!(
            "SELECT countDistinct(chunk_id) - 1, countIf(_is_deleted = 1) \
             FROM {chunk_table}"
        ),
        "3\t10",
        "mirror never reached 3 values + 10 tombstones",
    )
    .await;

    // Every WAL chunk persisted to the store, exactly once (chunk count is
    // PG build-dependent; equality against mirror data rows pins it).
    assert_eq!(
        stats
            .toast_chunks_stored
            .load(Ordering::Relaxed)
            .to_string(),
        ch.query(&format!(
            "SELECT countIf(_is_deleted = 0) FROM {chunk_table}"
        ))
        .expect("mirror data rows"),
        "every WAL chunk persisted to the store"
    );

    // Tombstone shape: chunk_id 0, empty body, at the delete-record LSNs
    // (strictly after every A/C data row).
    assert_eq!(
        ch.query(&format!(
            "SELECT countIf(chunk_id = 0 AND length(chunk_data) = 0) \
             FROM {chunk_table} WHERE _is_deleted = 1"
        ))
        .expect("tombstone shape"),
        "10"
    );

    // Identify the values without knowing their OIDs: B is the only live id
    // at head; A precedes C in WAL.
    let live_at_head = ch
        .query(&live_values_sql(&chunk_table, "18446744073709551615"))
        .expect("live set at head");
    let (b_id, b_len) = live_at_head
        .split_once('\t')
        .expect("exactly one live value at head");
    assert!(!b_id.contains('\n'), "only B live at head: {live_at_head}");
    assert_eq!(b_len, BODY_B_LEN.to_string(), "B byte-complete");
    let a_id = ch
        .query(&format!(
            "SELECT chunk_id FROM {chunk_table} \
             WHERE chunk_id NOT IN (0, {b_id}) GROUP BY chunk_id \
             ORDER BY min(_lsn) LIMIT 1"
        ))
        .expect("A's chunk_id");

    // Fetch semantics, not just end state: as-of at A's last data row every
    // A chunk is live — a pre-death referrer resolves A whole.
    let a_bound = ch
        .query(&format!(
            "SELECT max(_lsn) FROM {chunk_table} WHERE chunk_id = {a_id}"
        ))
        .expect("A's last row lsn");
    let live_at_a = ch
        .query(&live_values_sql(&chunk_table, &a_bound))
        .expect("live set at A's bound");
    assert!(
        live_at_a.contains(&format!("{a_id}\t{BODY_A_LEN}")),
        "A byte-complete as-of its own window: {live_at_a}"
    );

    // Post-delete referring row: unchanged-toast meta update must rehydrate
    // B from the mirror.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("UPDATE public.doc SET meta = {META3_SQL} WHERE id = 1"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "post-delete segment never shipped");
    let target = pipeline.stream.dispatched_lsn();
    shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up post-delete");

    // Merge is the reclaimer: FINAL collapses superseded versions per TID,
    // dead chunk_data goes away with no walshadow GC; tombstones remain.
    ch.query(&format!("SYSTEM START MERGES {chunk_table}"))
        .expect("start merges");
    ch.query(&format!("OPTIMIZE TABLE {chunk_table} FINAL"))
        .expect("optimize final");
    assert_eq!(
        ch.query(&format!(
            "SELECT sum(length(chunk_data)) FROM {chunk_table}"
        ))
        .expect("bytes after merge"),
        BODY_B_LEN.to_string(),
        "merge reclaimed dead values' bytes"
    );
    // Collapsed history: A's window is gone — the documented superseded-fill
    // case, an empty as-of rather than stale bytes.
    assert_eq!(
        ch.query(&live_values_sql(&chunk_table, &a_bound))
            .expect("live set at A's bound post-merge"),
        "",
    );

    // Fetch over merged parts: another rehydrating UPDATE still resolves B.
    let driver = fx::spawn_workload(
        &source,
        vec![
            format!("UPDATE public.doc SET meta = {META4_SQL} WHERE id = 1"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "post-merge segment never shipped");
    let target = pipeline.stream.dispatched_lsn();
    shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up post-merge");

    // Both rehydrating UPDATEs land with B's bytes: the pre-merge one as its
    // own version, the post-merge one at head — fetch works before and over
    // merged parts. Polled: dest rows ride commit drains asynchronously.
    fx::wait_query(
        &ch,
        &format!(
            "SELECT countIf(meta = {META3_SQL} AND body = {BODY_B_SQL}), \
                    countIf(meta = {META4_SQL} AND body = {BODY_B_SQL}) \
             FROM walshadow_test.doc WHERE id = 1"
        ),
        "1\t1",
        "rehydrating UPDATEs never landed with B's bytes",
    )
    .await;
    assert!(
        stats.toast_values_fetched.load(Ordering::Relaxed) >= 2,
        "both unchanged-toast UPDATEs fetched B from the mirror"
    );

    // Owner TRUNCATE then a repopulating INSERT, separate xacts: B's rows +
    // the tombstones must vanish as history (mirror TRUNCATE, not more
    // tombstones), D's chunks land past the wipe.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "TRUNCATE public.doc".into(),
            format!("INSERT INTO public.doc VALUES (60, 'v5', {BODY_D_SQL})"),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "truncate segment never shipped");
    // Whole-table sums: merge-stable (post-wipe rows are single-version).
    fx::wait_query(
        &ch,
        &format!(
            "SELECT countDistinct(chunk_id), sum(length(chunk_data)), \
             countIf(_is_deleted = 1) FROM {chunk_table}"
        ),
        &format!("1\t{BODY_D_LEN}\t0"),
        "mirror should hold exactly D after owner TRUNCATE",
    )
    .await;
    fx::wait_query(
        &ch,
        "SELECT groupArray(id) FROM (SELECT id FROM walshadow_test.doc FINAL \
         WHERE _is_deleted = 0 ORDER BY id)",
        "[60]",
        "dest table should hold only the post-truncate row",
    )
    .await;

    // DROP: sweep surfaces the toast rel; the retire is queued, deferred
    // until the persisted resume floor's segment passes the dropping
    // commit — an immediate wipe would be visible to a crash-replay of
    // this commit's own referrers. (Cold-restart + crash-replay variants
    // in `toast_truncate_drop_e2e.rs`.)
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
    // Floor never advanced: mirror history must survive the DROP intact —
    // this is the crash window a replayed referrer fetches across.
    assert_eq!(
        ch.query(&format!("SELECT count() > 0 FROM {chunk_table}"))
            .expect("mirror rows"),
        "1",
        "retire must defer while the resume floor hasn't passed the drop",
    );
    assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 0);

    // Cursor persisted past the dropping commit (daemon status-loop
    // stand-in); the next commit executes the queued retire.
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
        "retired mirror should be empty once the floor passed",
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

    let decoder_stats = pipeline.sinks.decoder.stats_handle();
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Counter posture: deletes became tombstones, TRUNCATE/DROP each ticked
    // their mirror counter, nothing malformed, no fills of any flavor.
    assert_eq!(
        stats.toast_tombstones_stored.load(Ordering::Relaxed),
        10,
        "one tombstone per dead chunk TID"
    );
    assert!(
        decoder_stats.toast_chunk_deletes.load(Ordering::Relaxed) >= 10,
        "toast chunk deletes observed"
    );
    assert_eq!(
        decoder_stats.toast_chunks_malformed.load(Ordering::Relaxed),
        0,
        "toast deletes must not count as malformed"
    );
    assert_eq!(stats.toast_mirror_truncates.load(Ordering::Relaxed), 1);
    assert_eq!(stats.toast_mirror_retires.load(Ordering::Relaxed), 1);
    assert_eq!(stats.truncates_emitted.load(Ordering::Relaxed), 1);
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
