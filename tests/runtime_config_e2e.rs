//! Runtime-config overlay e2e drills (plans/future/runtime_config_from_pg.md
//! §Acceptance drills): operator writes to source-PG `walshadow.config_table`
//! drive per-table scope live off the WAL stream.
//!
//! 1. `opt_in_via_config_table_replicates_new_table`
//!    * Pre-existing empty `app.events`, no TOML mapping.
//!    * Operator inserts `config_table (replicate=true, initial_load='copy')`.
//!    * Expect: daemon auto-creates the CH table from the descriptor and
//!      subsequent inserts land — no TOML edit, no CH DDL, no restart.
//!
//! 2. `opt_out_mid_stream_drains_and_halts`
//!    * `app.orders` TOML-mapped and replicating.
//!    * Operator inserts `config_table (replicate=false)` between two
//!      multi-row INSERTs.
//!    * Expect: the xact committed before the opt-out drains whole, the one
//!      after never emits (no partial xact either side), CH target retained.
//!
//! 3. `forward_decl_materializes_on_create_table`
//!    * Operator inserts `config_table (replicate=true)` for a table that
//!      does not exist; row parks as a forward-declaration.
//!    * Source later runs `CREATE TABLE`; the parked row materialises and
//!      subsequent inserts land on CH under the declared config.
//!
//! 4. `opt_in_non_empty_backfills_pre_opt_in_rows`
//!    * `app.inventory` populated before the WAL stream ever starts (rows
//!      unreachable via WAL), then opted in with `initial_load='copy'`.
//!    * Expect: COPY backfill lands the pre-opt-in rows at `_lsn = S`; a
//!      post-opt-in UPDATE outranks the COPY baseline by `commit_lsn > S`;
//!      a post-opt-in INSERT streams normally. Exercises native
//!      (int8/timestamptz), numeric-as-text, and cast-to-text (jsonb) wire
//!      decode paths.
//!
//! 5. `opt_in_then_alter_add_column_reaches_ch`
//!    * `app.gadgets` opted in via `config_table`, then source runs
//!      `ALTER TABLE ... ADD COLUMN`.
//!    * Expect: the ALTER diffs against the baseline the opt-in dispatch
//!      recorded at the config row's commit LSN → `Changed` → CH
//!      `ADD COLUMN`, and a trailing INSERT carries the new column. A cold
//!      baseline would instead surface `Added`, which `apply_added` skips
//!      for mapped rels — CH would stay a column behind
//!      (plans/future/pinned_ddl_baseline.md).
//!
//! 6. `column_target_type_override_reaches_projection`
//!    * CH dest pre-created `Decimal(38, 2)`, TOML maps the stale
//!      `Decimal(38, 0)`, operator inserts `config_column.target_type`.
//!    * Expect: post-override rows encode at the override's scale — the
//!      stored `123.45` (vs a scale-0 `123`) proves the override reached
//!      the projection (plans/config.md §Column overrides).
//!
//! 7. `auto_create_namespace_via_config_namespace`
//!    * Operator inserts `config_namespace (auto_create=true)`, no
//!      `config_table` row and no TOML mapping.
//!    * Source `CREATE TABLE` in the namespace + INSERT.
//!    * Expect: the namespace flag alone auto-creates the CH table and the
//!      row lands.
//!
//! 8. `pre_opt_in_xact_discards_post_opt_in_routes`
//!    * No TOML mapping, no `initial_load`: a row committed before the
//!      `config_table` opt-in plans against no route (counted discard),
//!      a row committed after routes and lands.
//!    * Route snapshots attach at planning: a transaction planned before
//!      the opt-in never re-routes, one planned after routes whole.
//!
//! Source-side `config_*` install runs the real `sql/runtime_config_install.sql`
//! inside the bootstrap schema dump, so the drills double as install-script
//! coverage (psql `\if` default-schema guard included).

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::mapping::ColumnMapping;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

const INSTALL_SQL: &str = include_str!("../sql/runtime_config_install.sql");

// Each test shifts these by +0 / +10 / +20. CH `interserver_http_port =
// http_port + 1`, keep a gap before WALSENDER_PORT.
const SOURCE_PORT: u16 = 17701;
const SHADOW_PORT: u16 = 17702;
const CH_TCP_PORT: u16 = 17703;
const CH_HTTP_PORT: u16 = 17704;
const WALSENDER_PORT: u16 = 17708;

fn overlay_ddl_args() -> fx::DdlPipelineArgs {
    fx::DdlPipelineArgs {
        config_schema: Some("walshadow".into()),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opt_in_via_config_table_replicates_new_table() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.events (id bigint PRIMARY KEY, body text);\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, SOURCE_PORT, SHADOW_PORT, WALSENDER_PORT).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    // No TOML mapping — the config row alone brings app.events into scope.
    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: CH_TCP_PORT,
        mappings: vec![],
        app_name: "walshadow-config-opt-in",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load) \
             VALUES ('app', 'events', true, 'copy')"
                .into(),
            "INSERT INTO app.events (id, body) VALUES (1, 'in-scope')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    let tbls = ch
        .query(
            "SELECT name FROM system.tables WHERE database = 'walshadow_test' AND name = 'events'",
        )
        .expect("ch table existence");
    assert_eq!(tbls, "events", "opt-in must auto-create the CH table");

    let n = ch
        .query("SELECT count() FROM walshadow_test.events FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(n, "1", "post-opt-in insert must reach CH");

    let body = ch
        .query(
            "SELECT argMax(body, _lsn) FROM walshadow_test.events \
             WHERE _is_deleted = 0 AND id = 1",
        )
        .expect("ch body");
    assert_eq!(body, "in-scope");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opt_out_mid_stream_drains_and_halts() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 10;
    let shadow_port = SHADOW_PORT + 10;
    let ch_tcp_port = CH_TCP_PORT + 10;
    let ch_http_port = CH_HTTP_PORT + 10;
    let walsender_port = WALSENDER_PORT + 10;
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.orders (id bigint PRIMARY KEY, note text);\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, source_port, shadow_port, walsender_port).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.orders (\
            id Int64,\
            note Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("app", "orders"),
        target_table: TableTarget::new("walshadow_test", "orders"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "note".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings,
        app_name: "walshadow-config-opt-out",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    // Commit order fixes semantics: id=1 precedes the opt-out (drains to CH),
    // id=2 follows it (never emits). The opt-out applies inside the barrier
    // fence, after id=1 is durable.
    // Multi-row xacts on both sides of the boundary: whole-transaction route
    // granularity means each side lands or discards as a unit, never partial.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO app.orders (id, note) \
             SELECT i, 'before opt-out' FROM generate_series(1, 5) AS i"
                .into(),
            "INSERT INTO walshadow.config_table (namespace, relname, replicate) \
             VALUES ('app', 'orders', false)"
                .into(),
            "INSERT INTO app.orders (id, note) \
             SELECT i, 'after opt-out' FROM generate_series(6, 10) AS i"
                .into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Source has both xacts; CH stopped at the opt-out boundary with the
    // before-xact complete — no partial transaction on either side.
    let src = source.psql_one("SELECT count(*) FROM app.orders").unwrap();
    assert_eq!(src, "10");
    let n = ch
        .query("SELECT count() FROM walshadow_test.orders FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(n, "5", "before-xact whole, after-xact absent");
    let ids = ch
        .query("SELECT max(id) FROM walshadow_test.orders FINAL WHERE _is_deleted = 0")
        .expect("ch ids");
    assert_eq!(ids, "5", "no row committed after replicate=false emits");

    // Target retained (opt-out is a routing change, not a DROP).
    let exists = ch
        .query(
            "SELECT count() FROM system.tables WHERE database = 'walshadow_test' AND name = 'orders'",
        )
        .expect("ch system.tables");
    assert_eq!(exists, "1", "opt-out must retain the CH target");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forward_decl_materializes_on_create_table() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 20;
    let shadow_port = SHADOW_PORT + 20;
    let ch_tcp_port = CH_TCP_PORT + 20;
    let ch_http_port = CH_HTTP_PORT + 20;
    let walsender_port = WALSENDER_PORT + 20;
    let schema_sql = format!("{INSTALL_SQL}\nCREATE SCHEMA app;\n");
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, source_port, shadow_port, walsender_port).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings: vec![],
        app_name: "walshadow-config-fwd-decl",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    // Phase A: the config row lands and applies while `app.later` does not
    // exist anywhere — deterministically parks as a forward-declaration.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_table (namespace, relname, replicate) \
             VALUES ('app', 'later', true)"
                .into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "phase A: no segments shipped in 45s");

    // Parked: nothing materialised yet.
    let pre = ch
        .query(
            "SELECT count() FROM system.tables WHERE database = 'walshadow_test' AND name = 'later'",
        )
        .expect("ch system.tables");
    assert_eq!(pre, "0", "forward-decl must not create a CH table yet");

    // Phase B: CREATE TABLE arrives; the parked row materialises inside the
    // same barrier, so the trailing insert routes.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "CREATE TABLE app.later (id bigint PRIMARY KEY, body text)".into(),
            "INSERT INTO app.later (id, body) VALUES (1, 'declared-first')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "phase B: no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    let tbls = ch
        .query(
            "SELECT name FROM system.tables WHERE database = 'walshadow_test' AND name = 'later'",
        )
        .expect("ch table existence");
    assert_eq!(
        tbls, "later",
        "CREATE TABLE must materialise the parked opt-in"
    );

    let body = ch
        .query(
            "SELECT argMax(body, _lsn) FROM walshadow_test.later \
             WHERE _is_deleted = 0 AND id = 1",
        )
        .expect("ch body");
    assert_eq!(body, "declared-first");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opt_in_non_empty_backfills_pre_opt_in_rows() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 30;
    let shadow_port = SHADOW_PORT + 30;
    let ch_tcp_port = CH_TCP_PORT + 30;
    let ch_http_port = CH_HTTP_PORT + 30;
    let walsender_port = WALSENDER_PORT + 30;
    // Rows land before the WAL stream ever starts, so COPY is the only path
    // that can carry them to CH. Column mix drives all three wire-decode
    // paths: int8/text/timestamptz native, numeric via ::text, jsonb cast.
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.inventory (\
            id bigint PRIMARY KEY,\
            name text,\
            price numeric(10,2),\
            added_at timestamptz,\
            meta jsonb);\n\
         INSERT INTO app.inventory VALUES\
            (1, 'anvil',  10.00, '2024-01-02 03:04:05+00', '{{\"a\": 1}}'),\
            (2, 'bolt',   12.50, '2024-01-02 03:04:06+00', '{{\"b\": 2}}'),\
            (3, 'crate',  99.99, NULL, NULL);\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, source_port, shadow_port, walsender_port).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings: vec![],
        app_name: "walshadow-config-backfill",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    // Opt-in commits at S; the UPDATE + INSERT commit after S so they ride
    // WAL with commit_lsn > S and outrank the COPY baseline at _lsn = S.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load) \
             VALUES ('app', 'inventory', true, 'copy')"
                .into(),
            "UPDATE app.inventory SET name = 'anvil-v2' WHERE id = 1".into(),
            "INSERT INTO app.inventory (id, name, price) VALUES (100, 'dowel', 0.25)".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    // Backfill runs as a detached task on its own CH tail; poll for
    // convergence (3 COPY rows + 1 streamed row) rather than racing it.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut n = String::new();
    while std::time::Instant::now() < deadline {
        n = ch
            .query("SELECT count(DISTINCT id) FROM walshadow_test.inventory FINAL WHERE _is_deleted = 0")
            .unwrap_or_default();
        if n == "4" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(n, "4", "3 backfilled + 1 streamed row must reach CH");

    // Untouched pre-opt-in row: COPY carried every column faithfully.
    let bolt = ch
        .query(
            "SELECT argMax(name, _lsn), argMax(price, _lsn), argMax(added_at, _lsn), \
                    argMax(meta, _lsn) \
             FROM walshadow_test.inventory WHERE _is_deleted = 0 AND id = 2",
        )
        .expect("ch backfilled row");
    assert_eq!(bolt, "bolt\t12.5\t2024-01-02 03:04:06.000000\t{\"b\": 2}");

    // NULLs survive the wire.
    let crate_row = ch
        .query(
            "SELECT argMax(name, _lsn), isNull(argMax(added_at, _lsn)), \
                    isNull(argMax(meta, _lsn)) \
             FROM walshadow_test.inventory WHERE _is_deleted = 0 AND id = 3",
        )
        .expect("ch null row");
    assert_eq!(crate_row, "crate\t1\t1");

    // Post-opt-in UPDATE (commit_lsn > S) beats the COPY baseline.
    let anvil = ch
        .query(
            "SELECT argMax(name, _lsn) FROM walshadow_test.inventory \
             WHERE _is_deleted = 0 AND id = 1",
        )
        .expect("ch mutated row");
    assert_eq!(
        anvil, "anvil-v2",
        "WAL mutation must outrank the COPY baseline"
    );

    // Post-opt-in INSERT streams via WAL, no COPY involvement.
    let dowel = ch
        .query(
            "SELECT argMax(name, _lsn) FROM walshadow_test.inventory \
             WHERE _is_deleted = 0 AND id = 100",
        )
        .expect("ch streamed row");
    assert_eq!(dowel, "dowel");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opt_in_then_alter_add_column_reaches_ch() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 40;
    let shadow_port = SHADOW_PORT + 40;
    let ch_tcp_port = CH_TCP_PORT + 40;
    let ch_http_port = CH_HTTP_PORT + 40;
    let walsender_port = WALSENDER_PORT + 40;
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.gadgets (id bigint PRIMARY KEY, name text);\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, source_port, shadow_port, walsender_port).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    // No TOML mapping — scope and baseline both come from the config row.
    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings: vec![],
        app_name: "walshadow-config-opt-in-alter",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    // Commit order fixes semantics: the opt-in records the two-column
    // baseline, id=1 routes at that shape, the ALTER diffs against it
    // (Changed → CH ADD COLUMN inside the barrier), id=2 carries qty.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_table (namespace, relname, replicate) \
             VALUES ('app', 'gadgets', true)"
                .into(),
            "INSERT INTO app.gadgets (id, name) VALUES (1, 'pre-alter')".into(),
            "ALTER TABLE app.gadgets ADD COLUMN qty integer".into(),
            "INSERT INTO app.gadgets (id, name, qty) VALUES (2, 'post-alter', 7)".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    // qty on CH proves the ALTER surfaced as Changed: the opt-in CREATE
    // pre-dates the ALTER, so only an applied CH ADD COLUMN puts it there.
    let qty_col = ch
        .query(
            "SELECT count() FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 'gadgets' AND name = 'qty'",
        )
        .expect("ch column existence");
    assert_eq!(qty_col, "1", "post-opt-in ALTER must ADD COLUMN on CH");

    // Post-ALTER row carries the new column (mapping extended with the DDL).
    let post = ch
        .query(
            "SELECT argMax(name, _lsn), argMax(qty, _lsn) \
             FROM walshadow_test.gadgets WHERE _is_deleted = 0 AND id = 2",
        )
        .expect("ch post-alter row");
    assert_eq!(post, "post-alter\t7");

    // Pre-ALTER row backfills NULL for the added column.
    let pre = ch
        .query(
            "SELECT argMax(name, _lsn), isNull(argMax(qty, _lsn)) \
             FROM walshadow_test.gadgets WHERE _is_deleted = 0 AND id = 1",
        )
        .expect("ch pre-alter row");
    assert_eq!(pre, "pre-alter\t1");
}

/// Drill 7: `config_namespace.auto_create = true` alone (no `config_table`
/// row, no TOML mapping) authorises namespace-wide auto-create. A source
/// `CREATE TABLE` in the flagged namespace must run `CREATE TABLE` on CH and
/// the trailing INSERT must land — proving the overlay's namespace layer
/// drives auto-create, not just the per-table `replicate=true` opt-in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_create_namespace_via_config_namespace() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 60;
    let shadow_port = SHADOW_PORT + 60;
    let ch_tcp_port = CH_TCP_PORT + 60;
    let ch_http_port = CH_HTTP_PORT + 60;
    let walsender_port = WALSENDER_PORT + 60;
    let schema_sql = format!("{INSTALL_SQL}\nCREATE SCHEMA app;\n");
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, source_port, shadow_port, walsender_port).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    // No TOML namespaces — the config_namespace row alone authorises it.
    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings: vec![],
        app_name: "walshadow-config-ns-auto-create",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    // The auto_create row commits before the CREATE TABLE, so `apply_added`
    // sees the namespace in `auto_create_namespaces` when the DDL drains.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_namespace (namespace, target_database, auto_create) \
             VALUES ('app', 'walshadow_test', true)"
                .into(),
            "CREATE TABLE app.thing (id bigint PRIMARY KEY, body text)".into(),
            "INSERT INTO app.thing (id, body) VALUES (1, 'ns-auto')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    let tbls = ch
        .query(
            "SELECT name FROM system.tables WHERE database = 'walshadow_test' AND name = 'thing'",
        )
        .expect("ch table existence");
    assert_eq!(
        tbls, "thing",
        "config_namespace.auto_create must create the CH table"
    );

    let body = ch
        .query(
            "SELECT argMax(body, _lsn) FROM walshadow_test.thing \
             WHERE _is_deleted = 0 AND id = 1",
        )
        .expect("ch body");
    assert_eq!(body, "ns-auto");
}

/// Drill 6: `config_column.target_type` reaches the emitted projection
/// (plans/config.md §Column overrides). CH dest pre-created with
/// `Decimal(38, 2)` while TOML deliberately maps the stale bridge default
/// `Decimal(38, 0)`; the override row lands via WAL before the DML. The
/// stored scale is the witness: an applied override encodes `123.45`, a
/// dropped one encodes scale-0 `123`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn column_target_type_override_reaches_projection() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 50;
    let shadow_port = SHADOW_PORT + 50;
    let ch_tcp_port = CH_TCP_PORT + 50;
    let ch_http_port = CH_HTTP_PORT + 50;
    let walsender_port = WALSENDER_PORT + 50;
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.ledger (id bigint PRIMARY KEY, amount numeric);\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, source_port, shadow_port, walsender_port).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    // Operator-migrated dest type; the override is what makes the
    // projection match it
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.ledger (\
            id Int64,\
            amount Decimal(38, 2),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("app", "ledger"),
        target_table: TableTarget::new("walshadow_test", "ledger"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "amount".into(),
                target_type: "Decimal(38, 0)".into(),
            },
        ],
    }];

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings,
        app_name: "walshadow-config-column-override",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    // Override commits before the DML, so the row's plan build (barrier
    // fence flushed the cache at the config apply) sees scale 2.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_column (namespace, relname, attname, target_type) \
             VALUES ('app', 'ledger', 'amount', 'Decimal(38, 2)')"
                .into(),
            "INSERT INTO app.ledger (id, amount) VALUES (1, 123.45)".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    let amount = ch
        .query(
            "SELECT argMax(amount, _lsn) \
             FROM walshadow_test.ledger WHERE _is_deleted = 0 AND id = 1",
        )
        .expect("ch row");
    assert_eq!(
        amount, "123.45",
        "override must drive the encode scale (a dropped override stores 123)"
    );
}

/// Drill 8: transaction planned before the opt-in discards, one planned
/// after routes. No TOML mapping and no `initial_load`, so the pre-opt-in
/// row has exactly one path to CH — a route resolved at planning — and it
/// must not take it. The post-opt-in row proves the opt-in commit preceding
/// heap rows in WAL routes those rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_opt_in_xact_discards_post_opt_in_routes() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let source_port = SOURCE_PORT + 70;
    let shadow_port = SHADOW_PORT + 70;
    let ch_tcp_port = CH_TCP_PORT + 70;
    let ch_http_port = CH_HTTP_PORT + 70;
    let walsender_port = WALSENDER_PORT + 70;
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.metrics (id bigint PRIMARY KEY, v text);\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, source_port, shadow_port, walsender_port).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ch_tcp_port, ch_http_port).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port,
        mappings: vec![],
        app_name: "walshadow-config-pre-opt-in-discard",
        ddl: Some(overlay_ddl_args()),
    })
    .await;

    // Commit order fixes semantics: id=1 plans against no route (discard),
    // the opt-in applies inside the barrier fence, id=2 plans after it.
    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO app.metrics (id, v) VALUES (1, 'pre-opt-in')".into(),
            "INSERT INTO walshadow.config_table (namespace, relname, replicate) \
             VALUES ('app', 'metrics', true)"
                .into(),
            "INSERT INTO app.metrics (id, v) VALUES (2, 'post-opt-in')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    let discarded = pipeline
        .stats
        .unsupported_relations
        .load(std::sync::atomic::Ordering::Relaxed);
    pipeline.shutdown().await.expect("pipeline drains clean");

    assert!(discarded >= 1, "pre-opt-in xact must be a counted discard");

    let n = ch
        .query("SELECT count() FROM walshadow_test.metrics FINAL WHERE _is_deleted = 0")
        .expect("ch count");
    assert_eq!(n, "1", "exactly the post-opt-in row lands");

    let gone = ch
        .query("SELECT count() FROM walshadow_test.metrics WHERE id = 1")
        .expect("ch pre-opt-in row");
    assert_eq!(gone, "0", "pre-opt-in row must never reach CH");

    let v = ch
        .query(
            "SELECT argMax(v, _lsn) FROM walshadow_test.metrics \
             WHERE _is_deleted = 0 AND id = 2",
        )
        .expect("ch v");
    assert_eq!(v, "post-opt-in");
}
