//! Descriptor-log capture end-to-end: prepared-xact DDL drains at COMMIT
//! PREPARED under the prepared xid, and capture-all (schema rename) keeps
//! decode routing on fresh namespace text.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use fx::spawn_txn;
use std::time::Duration;

use walshadow::mapping::{ColumnMapping, TableTarget};
use walshadow::schema::RelName;

const SLOT_PREPARED: PortSlot = PortSlot {
    source: 17960,
    shadow: 17961,
    ch_tcp: 17962,
    ch_http: 17963,
    walsender: 17967,
};
const SLOT_RENAME: PortSlot = PortSlot {
    source: 17970,
    shadow: 17971,
    ch_tcp: 17972,
    ch_http: 17973,
    walsender: 17977,
};
const SLOT_INTERVAL: PortSlot = PortSlot {
    source: 18020,
    shadow: 18021,
    ch_tcp: 18022,
    ch_http: 18023,
    walsender: 18027,
};

struct PortSlot {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    walsender: u16,
}

/// Live 2PC: DDL inside a prepared xact reaches CH at COMMIT PREPARED.
/// The commit record's header xid is the finishing backend's; the
/// capture-keyed events live under the prepared xid (B2) — pre-fix the
/// drain keyed header xid and stranded them. Rows written AFTER the ALTER
/// in the same xact are catalog-dirty admissions: they enter raw spill and
/// fence at commit (counted Skip) until raw decode replaces the fence —
/// pre-fence they decoded inline with the pre-ALTER descriptor, silently
/// dropping the new column's values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_ddl_drains_at_commit_prepared() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse on PATH");
        return;
    }
    let slot = SLOT_PREPARED;
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
        "CREATE SCHEMA tp;\n\
         CREATE TABLE tp.twophase (id bigint PRIMARY KEY, v text);\n",
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
        "tp".into(),
        walshadow::mapping::NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            drop_table_strategy: None,
        },
    );

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: vec![],
        app_name: "walshadow-desc-prepared",
        ddl: Some(ddl_args),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "BEGIN;\n\
         ALTER TABLE tp.twophase ADD COLUMN extra text;\n\
         INSERT INTO tp.twophase (id, v, extra) \
            SELECT g, 'x' || g, 'e' || g FROM generate_series(1, 8) g;\n\
         PREPARE TRANSACTION 'desc_log_2pc';\n\
         SELECT pg_switch_wal();\n\
         COMMIT PREPARED 'desc_log_2pc';\n\
         SELECT pg_switch_wal();\n",
    );
    let shipped = fx::pump_segments(&mut pipeline, 2, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 2, "expected ≥2 shipped segments, got {shipped}");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    let stats = pipeline.stats.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");
    let _ = shadow.stop();
    let _ = source.stop();

    // INSERT...SELECT may land as one MULTI_INSERT record; raw decode fans
    // every row out at commit resolution
    assert_eq!(
        stats.raw_decode_rows_ops.load().iter().sum::<u64>(),
        8,
        "post-ALTER rows decode from the raw stash at commit"
    );
    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.twophase",
        "8",
        "catalog-dirty rows deliver via raw decode",
    )
    .await;
    fx::wait_query(
        &ch,
        "SELECT count() FROM system.columns \
         WHERE database = 'walshadow_test' AND table = 'twophase' AND name = 'extra'",
        "1",
        "prepared ALTER's Changed applies at COMMIT PREPARED",
    )
    .await;
}

/// Capture-all freshness: pg_namespace writes carry no per-relation relcache
/// invals, so a schema rename must recapture every descriptor — rows written
/// after the rename decode with the NEW namespace and route through a
/// mapping keyed under it. With stale descriptors they skip as unmapped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_rename_reroutes_under_new_namespace() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse on PATH");
        return;
    }
    let slot = SLOT_RENAME;
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, "", slot.source, slot.shadow, slot.walsender).await;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.renamed_t (\
            id Int64,\
            v Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY (id)",
    )
    .expect("create dest table");

    // Mapping pinned under the POST-rename name only
    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("n2", "t"),
        target_table: TableTarget::new("walshadow_test", "renamed_t"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int64".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "v".into(),
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
        ch_tcp_port: slot.ch_tcp,
        mappings,
        app_name: "walshadow-desc-rename",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "CREATE SCHEMA n1;\n\
         CREATE TABLE n1.t (id bigint PRIMARY KEY, v text);\n\
         INSERT INTO n1.t (id, v) VALUES (1, 'pre');\n\
         ALTER SCHEMA n1 RENAME TO n2;\n\
         INSERT INTO n2.t (id, v) SELECT g, 'post' FROM generate_series(10, 14) g;\n\
         SELECT pg_switch_wal();\n",
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");
    let _ = shadow.stop();
    let _ = source.stop();

    // Post-rename rows decode under n2 and route; the pre-rename row's
    // descriptor said n1 (unmapped) — skipped by design
    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.renamed_t WHERE v = 'post'",
        "5",
        "post-rename rows route under the new namespace",
    )
    .await;
    fx::wait_query(
        &ch,
        "SELECT count() FROM walshadow_test.renamed_t",
        "5",
        "pre-rename row stays unrouted (n1 unmapped)",
    )
    .await;
}

/// Phase-1 interval semantics against real captured history: a compatible
/// in-place change (column rename) answers `Present` across its dirty
/// interval; an unproven one (varchar widen = typmod change, no rewrite so
/// same rfn) answers `Ambiguous` over `[first_touch, next_lsn)` with the
/// predecessor before the interval and the final version at its end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_place_intervals_compatible_and_ambiguous() {
    use walrus::pg::walparser::RelFileNode;
    use walshadow::desc_log::{AmbiguityReason, LookupResult};

    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse on PATH");
        return;
    }
    let slot = SLOT_INTERVAL;
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
        "CREATE SCHEMA iv;\n\
         CREATE TABLE iv.t_ren (id bigint PRIMARY KEY, v text);\n\
         CREATE TABLE iv.t_wid (id bigint PRIMARY KEY, v varchar(10));\n",
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;

    let db_oid: u32 = source
        .psql_one("SELECT oid FROM pg_database WHERE datname = current_database()")
        .expect("db oid")
        .parse()
        .unwrap();
    let rfn_of = |rel: &str| -> (u32, RelFileNode) {
        let row = source
            .psql_one(&format!(
                "SELECT c.oid::text || ' ' || c.relfilenode::text \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'iv' AND c.relname = '{rel}'"
            ))
            .expect("rel identity");
        let mut it = row.split_whitespace();
        let oid: u32 = it.next().unwrap().parse().unwrap();
        let rel_node: u32 = it.next().unwrap().parse().unwrap();
        (
            oid,
            RelFileNode {
                spc_node: 1663, // pg_default, reltablespace 0 resolved at capture
                db_node: db_oid,
                rel_node,
            },
        )
    };
    let (oid_ren, rfn_ren) = rfn_of("t_ren");
    let (_oid_wid, rfn_wid) = rfn_of("t_wid");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: vec![],
        app_name: "walshadow-desc-interval",
        ddl: Some(fx::DdlPipelineArgs::default()),
    })
    .await;

    let driver = spawn_txn(
        &source,
        "BEGIN;\n\
         INSERT INTO iv.t_ren (id, v) VALUES (1, 'a');\n\
         ALTER TABLE iv.t_ren RENAME COLUMN v TO w;\n\
         COMMIT;\n\
         BEGIN;\n\
         INSERT INTO iv.t_wid (id, v) VALUES (1, 'aaaa');\n\
         ALTER TABLE iv.t_wid ALTER COLUMN v TYPE varchar(20);\n\
         COMMIT;\n\
         SELECT pg_switch_wal();\n",
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "expected ≥1 shipped segment, got {shipped}");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    let log = pipeline.desc_log.clone();
    pipeline.shutdown().await.expect("pipeline drains clean");
    let _ = shadow.stop();
    let _ = source.stop();

    // Unproven widen: one rfn-scoped interval, Ambiguous inside, final
    // version at through_lsn (= boundary next_lsn), predecessor before
    let ambs = log.rfn_ambiguities(rfn_wid);
    assert_eq!(ambs.len(), 1, "widen publishes one ambiguity: {ambs:?}");
    let amb = &ambs[0];
    assert_eq!(amb.reason, AmbiguityReason::UnknownMutationPosition);
    assert!(amb.from_lsn < amb.through_lsn);
    assert!(matches!(
        log.descriptor_at(rfn_wid, amb.from_lsn),
        LookupResult::Ambiguous(_)
    ));
    assert!(matches!(
        log.descriptor_at(rfn_wid, amb.through_lsn - 1),
        LookupResult::Ambiguous(_)
    ));
    match log.descriptor_at(rfn_wid, amb.through_lsn) {
        // varchar(20) typmod = 24
        LookupResult::Present(d) => assert_eq!(d.attributes[1].typmod, 24),
        other => panic!("expected final Present at through_lsn, got {other:?}"),
    }
    let pred = log
        .present_before(rfn_wid, amb.from_lsn)
        .expect("durable predecessor");
    assert_eq!(pred.attributes[1].typmod, 14, "varchar(10) predecessor");

    // Compatible rename: no ambiguity, bias-early Present answers across
    // the interval with the final attribute name
    assert!(log.rfn_ambiguities(rfn_ren).is_empty());
    match log.descriptor_by_oid_at(oid_ren, u64::MAX) {
        LookupResult::Present(d) => assert_eq!(d.attributes[1].name, "w"),
        other => panic!("expected renamed Present, got {other:?}"),
    }
    match log.descriptor_at(rfn_ren, log.head()) {
        LookupResult::Present(d) => assert_eq!(d.attributes[1].name, "w"),
        other => panic!("expected Present across rename interval, got {other:?}"),
    }
}
