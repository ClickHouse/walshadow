//! Pending catalog capture end to end against real WAL: the writing
//! transaction's own uncommitted catalog rows, read at each command boundary
//! off the bridge worker, folded into the commit's descriptor-log batch.
//!
//! Drills:
//!
//! 1. `pending_timeline_decodes_the_post_ddl_row` — an in-place type change
//!    no single descriptor provably reads, then a row past it. The timeline
//!    answers from the command boundary on, so the fence shrinks to the run
//!    before it and the row lands.
//!    `dirty_admission_e2e::physical_in_place_alter_fences_deferred_rows` is
//!    the control: the same shape with every command read failing fences the
//!    whole dirty interval and stops the stream
//! 2. `relation_born_in_xact_keeps_a_slot_per_boundary` — two shapes of a
//!    relation created in the same transaction, one slot each, the first at
//!    the generation's smgr marker
//! 3. `savepoint_rollback_drops_its_pending_slots` — a rolled-back savepoint's
//!    shapes never reach the durable log, and its column never reaches CH
//! 4. `namespace_boundaries_refresh_names_without_missing` checks that
//!    creating a schema refreshes names already captured in the transaction.
//! 5. `late_temp_table_keeps_the_timeline` checks that the session's first
//!    temp table preserves history from an earlier type change.
//! 6. `backfill_then_set_not_null_with_late_temp_table_delivers` checks that
//!    backfilled values reach ClickHouse after `SET NOT NULL` and temp
//!    table creation.
//! 7. `unread_shape_change_reaches_later_rows` checks that a failed second
//!    `ADD COLUMN` read preserves both new columns in later rows.
//! 8. `unread_shape_change_after_rewrite_reaches_later_rows` checks that a
//!    failed `ADD COLUMN` read after `TRUNCATE` still preserves the new column.
//! 9. `every_change_a_row_needs_is_read` runs more schema changes than the
//!    old per-transaction limit, each followed by a row.
//! 10. `catalog_only_migration_reads_at_commit` checks that only the commit
//!     reads when no row follows any command.
//! 11. `rows_of_other_tables_leave_reads_unneeded` checks that rows of other
//!     tables, including one created in the transaction, skip reads.
//! 12. `idle_source_reads_at_the_current_boundary` checks that capture reads
//!     with the WAL at hand when input stops after a command.
//! 13. `row_position_picks_the_read` compares a row after two changes with a
//!     row between them.
//! 14. `rewrite_ctas_and_rollback_keep_their_reads` covers a rewrite,
//!     `CREATE TABLE AS` and savepoint rollback.
//!
//! `varchar(10)` → `text` is binary coercible, so PG rewrites `atttypid` in
//! place: same filenode, same walk fields, different type. That is the
//! physically-unproven in-place transition, reachable without a rewrite.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use fx::spawn_txn;
use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::NamespaceMapping;

struct Drill {
    source: fx::ClusterGuard,
    shadow: fx::ClusterGuard,
    ch: fx::ChServer,
    pipeline: fx::Pipeline,
    _tmp: tempfile::TempDir,
}

async fn build_drill(slot: fx::Ports, schema_sql: &str, app_name: &str) -> Drill {
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
        schema_sql,
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "pc".into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            auto_create_name: Some(walshadow::mapping::NameTemplate::parse("$table$").unwrap()),
            drop_table_strategy: None,
            detach_strategy: None,
            initial_load: None,
        },
    );

    let pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: vec![],
        app_name,
        ddl: Some(ddl_args),
    })
    .await;

    Drill {
        source,
        shadow,
        ch,
        pipeline,
        _tmp: tmp,
    }
}

async fn pump_and_drain(drill: &mut Drill) {
    let shipped = fx::pump_segments(&mut drill.pipeline, 1, Duration::from_secs(45)).await;
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");
    wait_shadow(drill);
}

/// Finish `sql` on the source, then pump its WAL in one push so lookahead
/// decides with the whole workload in view
async fn run_then_pump(drill: &mut Drill, sql: &str) {
    spawn_txn(&drill.source, sql).join().expect("workload");
    let shipped = fx::pump_coalesced(&mut drill.pipeline, 1, Duration::from_secs(45)).await;
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");
    wait_shadow(drill);
}

fn wait_shadow(drill: &Drill) {
    let target = drill.pipeline.stream.dispatched_lsn();
    let observed = drill
        .shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
}

fn capture_stats(drill: &Drill) -> std::sync::Arc<walshadow::catalog_capture::CaptureStats> {
    drill
        .pipeline
        .sinks
        .capture
        .stats_handles()
        .next()
        .expect("one capture")
        .1
}

fn load(c: &std::sync::atomic::AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// Row written after an unproven in-place change decodes under the shape the
/// command boundary captured, where without the timeline the fence would
/// cover it and stop the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_timeline_decodes_the_post_ddl_row() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v varchar(10));\n",
        "walshadow-pending-covered",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         INSERT INTO pc.t (id, v) VALUES (1, 'pre');\n\
         ALTER TABLE pc.t ALTER COLUMN v TYPE text;\n\
         INSERT INTO pc.t (id, v) VALUES (2, 'post');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=pre,2=post",
        "row past the command boundary decodes under the shape it saw",
    )
    .await;
    assert!(
        load(&capture.pending_captures) >= 1,
        "command boundary read into the timeline",
    );
    assert!(
        load(&capture.pending_entries_promoted) >= 1,
        "its shape folded into the commit batch",
    );
    assert!(
        load(&capture.pending_holds) >= 1,
        "command boundary parked publication",
    );
    assert_eq!(
        capture.pending_degraded.iter().map(load).sum::<u64>(),
        0,
        "nothing degraded this transaction",
    );
    assert!(
        load(&capture.ambiguities_published) >= 1,
        "the run before the first boundary is still fenced",
    );
}

/// A relation born inside the transaction takes two shapes across two
/// command boundaries. The first claims the generation's smgr marker — rows
/// can't precede the create — and the second answers from its own boundary,
/// so both land in the batch instead of collapsing onto one position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relation_born_in_xact_keeps_a_slot_per_boundary() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n",
        "walshadow-pending-born",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         CREATE TABLE pc.born (id bigint PRIMARY KEY, v text);\n\
         INSERT INTO pc.born (id, v) VALUES (1, 'first');\n\
         ALTER TABLE pc.born ADD COLUMN extra text;\n\
         INSERT INTO pc.born (id, v, extra) VALUES (2, 'second', 'e2');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        // `concat` over a NULL yields NULL and `groupArray` then skips the
        // row, so the pre-ALTER row's absent column needs a substitute
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn), '/', \
                          ifNull(argMax(extra, _lsn), '-')) AS c \
            FROM walshadow_test.born WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=first/-,2=second/e2",
        "both shapes of a relation born in the xact reach CH",
    )
    .await;
    assert!(
        load(&capture.pending_captures) >= 2,
        "one boundary per command that touched the relation",
    );
    assert!(
        load(&capture.pending_entries_promoted) >= 2,
        "two shapes, two positions — not one slot overwriting the other",
    );
}

/// A rolled-back savepoint's shapes die on the pump, before the commit that
/// would promote them: the reverted column reaches neither the durable log
/// nor CH.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn savepoint_rollback_drops_its_pending_slots() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v text);\n",
        "walshadow-pending-savepoint",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         SAVEPOINT sp;\n\
         ALTER TABLE pc.t ADD COLUMN reverted text;\n\
         ROLLBACK TO SAVEPOINT sp;\n\
         INSERT INTO pc.t (id, v) VALUES (1, 'after');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = 't' AND name = 'reverted'",
        "0",
        "rolled-back column never reaches CH",
    )
    .await;
    fx::wait_query(
        &drill.ch,
        "SELECT argMax(v, _lsn) FROM walshadow_test.t WHERE id = 1 AND _is_deleted = 0",
        "after",
        "post-rollback row delivers under the unmodified shape",
    )
    .await;
    assert!(
        load(&capture.pending_captures) >= 1,
        "the savepoint's command boundary was read",
    );
    assert!(
        load(&capture.pending_entries_dropped_abort) >= 1,
        "its slots dropped with the aborted subtree",
    );
    assert_eq!(
        load(&capture.pending_entries_promoted),
        0,
        "nothing from the aborted subtree became durable",
    );
}

fn unread(capture: &walshadow::catalog_capture::CaptureStats) -> u64 {
    capture.pending_degraded.iter().map(load).sum()
}

/// Creating a schema refreshes any relation names already read in the
/// transaction. Before the first catalog read, there are no names to refresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_boundaries_refresh_names_without_missing() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n",
        "walshadow-pending-capture-all",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         CREATE SCHEMA pc_a;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v text);\n\
         INSERT INTO pc.t VALUES (1, 'before');\n\
         CREATE SCHEMA pc_b;\n\
         INSERT INTO pc.t VALUES (2, 'after');\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=before,2=after",
        "ClickHouse receives rows before and after schema creation",
    )
    .await;
    assert_eq!(unread(&capture), 0, "no missed catalog reads");
    assert!(
        load(&capture.pending_captures) >= 2,
        "capture reads at table creation and refreshes at second schema creation",
    );
}

/// PostgreSQL creates `pg_temp_N` for the session's first temp table.
/// Refreshing names there preserves the descriptor used after the type change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_temp_table_keeps_the_timeline() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v varchar(10));\n",
        "walshadow-pending-late-temp",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         INSERT INTO pc.t (id, v) VALUES (1, 'pre');\n\
         ALTER TABLE pc.t ALTER COLUMN v TYPE text;\n\
         INSERT INTO pc.t (id, v) VALUES (2, 'post');\n\
         CREATE TEMP TABLE scratch (x int);\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', argMax(v, _lsn)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=pre,2=post",
        "the row after the type change uses the updated descriptor",
    )
    .await;
    assert_eq!(unread(&capture), 0, "temp table caused no missed reads");
}

/// Reproduce the production sequence in one transaction. Each backfilled
/// row stores the new column before `SET NOT NULL` and temp table creation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfill_then_set_not_null_with_late_temp_table_delivers() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.lots (id int PRIMARY KEY, item uuid NOT NULL);\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, lot_id int NOT NULL);\n\
         INSERT INTO pc.lots VALUES \
           (1, '00000000-0000-0000-0000-00000000000a'), \
           (2, '00000000-0000-0000-0000-00000000000b');\n\
         INSERT INTO pc.t VALUES (1, 1), (2, 2), (3, 1);\n",
        "walshadow-pending-backfill-not-null",
    )
    .await;
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         ALTER TABLE pc.t ADD COLUMN item_id uuid;\n\
         ALTER TABLE pc.t ADD CONSTRAINT t_lot_fk FOREIGN KEY (lot_id) REFERENCES pc.lots(id);\n\
         UPDATE pc.t f SET item_id = l.item FROM pc.lots l WHERE l.id = f.lot_id;\n\
         ALTER TABLE pc.t ALTER COLUMN item_id SET NOT NULL;\n\
         CREATE TEMP TABLE scratch (x int);\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', right(toString(argMax(item_id, _lsn)), 1)) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=a,2=b,3=a",
        "ClickHouse receives every backfilled item_id",
    )
    .await;
    assert_eq!(unread(&capture), 0, "temp table caused no missed reads");
    assert_eq!(
        load(&capture.ambiguities_published),
        0,
        "this transaction records no ambiguity intervals",
    );
}

/// Fail every read after the first so the second `ADD COLUMN` goes unread.
/// The commit descriptor can decode earlier rows, so use it from the missed
/// read onward. An earlier descriptor used to stay in effect after commit
/// and drop the second column from later rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unread_shape_change_reaches_later_rows() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v int);\n",
        "walshadow-pending-unread-shape",
    )
    .await;
    drill.pipeline.sinks.capture.fail_command_read_after(1);
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         ALTER TABLE pc.t ADD COLUMN a int;\n\
         INSERT INTO pc.t VALUES (1, 1, 11);\n\
         ALTER TABLE pc.t ADD COLUMN b int;\n\
         INSERT INTO pc.t VALUES (2, 2, 22, 222);\n\
         COMMIT;\n\
         INSERT INTO pc.t VALUES (3, 3, 33, 333);\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', toString(argMax(a, _lsn)), '/', \
                ifNull(toString(argMax(b, _lsn)), 'NULL')) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=11/NULL,2=22/222,3=33/333",
        "ClickHouse receives both new columns before and after commit",
    )
    .await;
    assert_eq!(unread(&capture), 1, "one transaction missed a read");
    assert_eq!(
        load(&capture.ambiguities_published),
        0,
        "the added columns leave earlier rows readable",
    );
}

/// Capture reads the new filenode at `TRUNCATE` and fails the `ADD COLUMN`
/// read. The commit descriptor can decode rows from both versions of the
/// table on the new filenode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unread_shape_change_after_rewrite_reaches_later_rows() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v int);\n",
        "walshadow-pending-unread-rewrite",
    )
    .await;
    drill.pipeline.sinks.capture.fail_command_read_after(1);
    let capture = capture_stats(&drill);

    let driver = spawn_txn(
        &drill.source,
        "BEGIN;\n\
         TRUNCATE pc.t;\n\
         ALTER TABLE pc.t ADD COLUMN b int;\n\
         INSERT INTO pc.t VALUES (1, 1, 11);\n\
         COMMIT;\n\
         INSERT INTO pc.t VALUES (2, 2, 22);\n\
         SELECT pg_switch_wal();\n",
    );
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
            SELECT concat(toString(id), '=', toString(argMax(v, _lsn)), '/', \
                ifNull(toString(argMax(b, _lsn)), 'NULL')) AS c \
            FROM walshadow_test.t WHERE _is_deleted = 0 \
            GROUP BY id ORDER BY id)",
        "1=1/11,2=2/22",
        "ClickHouse receives the new column before and after commit",
    )
    .await;
    assert_eq!(unread(&capture), 1, "one transaction missed a read");
    assert_eq!(
        load(&capture.ambiguities_published),
        0,
        "the added column leaves rewritten rows readable",
    );
}

/// More schema changes than the old per-transaction read limit, each
/// followed by a row using the new column. Every row needs the read before it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_change_a_row_needs_is_read() {
    if !fx::tools::requirements_available() {
        return;
    }
    const CHANGES: usize = 70;
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY);\n",
        "walshadow-pending-many",
    )
    .await;
    let capture = capture_stats(&drill);

    let mut sql = String::from("BEGIN;\n");
    for n in 1..=CHANGES {
        sql.push_str(&format!(
            "ALTER TABLE pc.t ADD COLUMN c{n} int;\n\
             INSERT INTO pc.t (id, c{n}) VALUES ({n}, {n});\n"
        ));
    }
    sql.push_str("COMMIT;\nSELECT pg_switch_wal();\n");
    let driver = spawn_txn(&drill.source, &sql);
    pump_and_drain(&mut drill).await;
    let _ = driver.join();

    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    let cols: Vec<String> = (1..=CHANGES).map(|n| format!("c{n}")).collect();
    fx::wait_query(
        &drill.ch,
        &format!(
            "SELECT countIf(v = id) FROM (\
                SELECT id, argMax([{}][id], _lsn) AS v \
                FROM walshadow_test.t WHERE _is_deleted = 0 GROUP BY id)",
            cols.join(", ")
        ),
        &CHANGES.to_string(),
        "every row keeps the value of the column added just before it",
    )
    .await;
    assert!(
        load(&capture.pending_captures) >= CHANGES as u64,
        "each change a row needs is read",
    );
    assert_eq!(unread(&capture), 0, "no read was missed");
}

/// Catalog-only migration: commit is in buffered WAL and no row is written
/// before it, so the commit read alone covers every command
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_only_migration_reads_at_commit() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v int);\n",
        "walshadow-pending-catalog-only",
    )
    .await;
    let capture = capture_stats(&drill);

    run_then_pump(
        &mut drill,
        "BEGIN;\n\
         ALTER TABLE pc.t ADD COLUMN a int;\n\
         ALTER TABLE pc.t ADD COLUMN b int;\n\
         ALTER TABLE pc.t ADD COLUMN c int;\n\
         COMMIT;\n\
         INSERT INTO pc.t VALUES (1, 1, 11, 111, 1111);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT concat(toString(argMax(a, _lsn)), '/', toString(argMax(b, _lsn)), '/', \
            toString(argMax(c, _lsn))) FROM walshadow_test.t WHERE _is_deleted = 0",
        "11/111/1111",
        "row after the migration carries every new column",
    )
    .await;
    assert_eq!(load(&capture.pending_captures), 0, "no command read");
    assert_eq!(load(&capture.pending_holds), 0, "no command hold");
    assert!(load(&capture.pending_skipped) >= 3, "each command skipped");
    assert!(load(&capture.sql_captures) >= 1, "commit read");
}

/// Rows of other tables, one committed and one created in this transaction,
/// need neither `ALTER` read
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_of_other_tables_leave_reads_unneeded() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v int);\n\
         CREATE TABLE pc.o (id bigint PRIMARY KEY);\n",
        "walshadow-pending-other-tables",
    )
    .await;
    let capture = capture_stats(&drill);

    run_then_pump(
        &mut drill,
        "BEGIN;\n\
         CREATE TABLE pc.n (id bigint PRIMARY KEY);\n\
         INSERT INTO pc.n VALUES (1);\n\
         ALTER TABLE pc.t ADD COLUMN a int;\n\
         INSERT INTO pc.n VALUES (2);\n\
         INSERT INTO pc.o VALUES (1);\n\
         ALTER TABLE pc.t ADD COLUMN b int;\n\
         COMMIT;\n\
         INSERT INTO pc.t VALUES (1, 1, 11, 111);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let skipped = load(&capture.pending_skipped);
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    let ids = |table: &str| {
        format!(
            "SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (\
                SELECT id FROM walshadow_test.{table} WHERE _is_deleted = 0 \
                GROUP BY id ORDER BY id)"
        )
    };
    fx::wait_query(&drill.ch, &ids("n"), "1,2", "rows of the new table").await;
    fx::wait_query(&drill.ch, &ids("o"), "1", "rows of the committed table").await;
    fx::wait_query(
        &drill.ch,
        "SELECT concat(toString(argMax(a, _lsn)), '/', toString(argMax(b, _lsn))) \
         FROM walshadow_test.t WHERE _is_deleted = 0",
        "11/111",
        "row after commit",
    )
    .await;
    assert!(skipped >= 2, "both ALTER reads skipped, got {skipped}");
    assert_eq!(unread(&capture), 0, "no read was missed");
}

/// Input stops after a command: capture reads at that boundary with the WAL
/// it has rather than waiting for the commit
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_source_reads_at_the_current_boundary() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v int);\n",
        "walshadow-pending-idle",
    )
    .await;
    let capture = capture_stats(&drill);

    let mut txn = fx::TxnSession::open(&drill.source);
    txn.send("BEGIN;\nALTER TABLE pc.t ADD COLUMN a int;\n");
    fx::wait_idle_in_txn(&drill.source, "ADD COLUMN a int;");
    drill.source.psql_one("CHECKPOINT").expect("flush WAL");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while load(&capture.pending_captures) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "capture waited for WAL past an idle command",
        );
        fx::pump_segments(&mut drill.pipeline, 1, Duration::from_millis(500)).await;
    }
    assert_eq!(
        load(&capture.pending_skipped),
        0,
        "nothing proved the read unneeded"
    );

    txn.send(
        "INSERT INTO pc.t VALUES (1, 1, 11);\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    txn.finish();
    pump_and_drain(&mut drill).await;
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    fx::wait_query(
        &drill.ch,
        "SELECT toString(argMax(a, _lsn)) FROM walshadow_test.t WHERE _is_deleted = 0",
        "11",
        "row after the idle command uses its read",
    )
    .await;
}

/// A row after two changes needs only the read after the second. A row
/// between them needs the read after the first, and the commit read covers
/// the second
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_position_picks_the_read() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v int);\n\
         CREATE TABLE pc.u (id bigint PRIMARY KEY, v int);\n",
        "walshadow-pending-row-position",
    )
    .await;
    let capture = capture_stats(&drill);

    run_then_pump(
        &mut drill,
        "BEGIN;\n\
         ALTER TABLE pc.t ADD COLUMN a int;\n\
         ALTER TABLE pc.t ADD COLUMN b int;\n\
         INSERT INTO pc.t VALUES (1, 1, 11, 111);\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    let after = (
        load(&capture.pending_captures),
        load(&capture.pending_skipped),
    );
    assert_eq!(after.0, 1, "one read, after the second change");
    assert!(after.1 >= 1, "first change skipped");

    run_then_pump(
        &mut drill,
        "BEGIN;\n\
         ALTER TABLE pc.u ADD COLUMN a int;\n\
         INSERT INTO pc.u VALUES (1, 1, 11);\n\
         ALTER TABLE pc.u ADD COLUMN b int;\n\
         COMMIT;\n\
         INSERT INTO pc.u VALUES (2, 2, 22, 222);\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    assert_eq!(
        load(&capture.pending_captures) - after.0,
        1,
        "one read, after the first change",
    );
    assert!(
        load(&capture.pending_skipped) > after.1,
        "second change skipped"
    );
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    let rows = |table: &str| {
        format!(
            "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
                SELECT concat(toString(id), '=', toString(argMax(a, _lsn)), '/', \
                    ifNull(toString(argMax(b, _lsn)), 'NULL')) AS c \
                FROM walshadow_test.{table} WHERE _is_deleted = 0 \
                GROUP BY id ORDER BY id)"
        )
    };
    fx::wait_query(&drill.ch, &rows("t"), "1=11/111", "row after both changes").await;
    fx::wait_query(
        &drill.ch,
        &rows("u"),
        "1=11/NULL,2=22/222",
        "row between the changes and row after commit",
    )
    .await;
    assert_eq!(unread(&capture), 0, "no read was missed");
}

/// Rewrites, `CREATE TABLE AS` and savepoint rollback write rows or drop
/// reads in ways only the read at each boundary can place
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rewrite_ctas_and_rollback_keep_their_reads() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut drill = build_drill(
        fx::Ports::alloc(),
        "CREATE SCHEMA pc;\n\
         CREATE TABLE pc.t (id bigint PRIMARY KEY, v int);\n\
         INSERT INTO pc.t VALUES (1, 1);\n",
        "walshadow-pending-rewrite-ctas",
    )
    .await;
    let capture = capture_stats(&drill);

    run_then_pump(
        &mut drill,
        "BEGIN;\n\
         ALTER TABLE pc.t ALTER COLUMN v TYPE bigint USING v * 10;\n\
         INSERT INTO pc.t VALUES (2, 20);\n\
         CREATE TABLE pc.u AS SELECT id, v FROM pc.t;\n\
         ALTER TABLE pc.u ADD PRIMARY KEY (id);\n\
         SAVEPOINT s;\n\
         ALTER TABLE pc.t ADD COLUMN gone int;\n\
         ROLLBACK TO s;\n\
         INSERT INTO pc.t VALUES (3, 30);\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    )
    .await;
    drill.pipeline.shutdown().await.expect("pipeline drains");
    let _ = drill.shadow.stop();
    let _ = drill.source.stop();

    let rows = |table: &str| {
        format!(
            "SELECT arrayStringConcat(groupArray(c), ',') FROM (\
                SELECT concat(toString(id), '=', toString(argMax(v, _lsn))) AS c \
                FROM walshadow_test.{table} WHERE _is_deleted = 0 \
                GROUP BY id ORDER BY id)"
        )
    };
    fx::wait_query(&drill.ch, &rows("t"), "1=10,2=20,3=30", "rewritten table").await;
    fx::wait_query(
        &drill.ch,
        &rows("u"),
        "1=10,2=20",
        "table created from a query",
    )
    .await;
    assert!(load(&capture.pending_captures) >= 2, "reads kept");
    assert_eq!(unread(&capture), 0, "no read was missed");
}
