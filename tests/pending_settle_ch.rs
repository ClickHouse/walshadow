//! Pending visibility settle against ClickHouse: which entries settle, stay,
//! or fail the round

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::time::Duration;

use walshadow::backfill_staging::StagingSession;
use walshadow::backup_page_walk::{BackfillTuple, CatalogMap, TupleData};
use walshadow::ch_emitter::{EmitterConfig, EmitterStats, InstanceConfig, RetryConfig};
use walshadow::config::DestEmitter;
use walshadow::datum::{Datum, build};
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};
use walshadow::toast::ToastResolver;
use walshadow::visibility::PendingXids;
use walshadow::visibility_pending::{
    PENDING_LEDGER_FILENAME, PendingLedger, PendingManifest, PendingRel, PendingSpool, settle, ship,
};

struct Ch {
    server: fx::ChServer,
    sess: StagingSession,
}

async fn ch() -> Ch {
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let server = fx::ChServer::spawn(tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    server
        .query("CREATE DATABASE IF NOT EXISTS db")
        .expect("create db");
    let cfg = EmitterConfig {
        conn: InstanceConfig {
            host: "127.0.0.1".into(),
            port: slot.ch_tcp,
            ..Default::default()
        },
        ..Default::default()
    };
    let sess = StagingSession::connect(DestEmitter::new(Arc::new(cfg), None))
        .await
        .expect("staging session");
    Ch { server, sess }
}

fn manifest(table: &str, xids: Vec<u32>) -> PendingManifest {
    PendingManifest {
        rel: PendingRel {
            rel: RelName::new("public", table),
            target: TableTarget::new("db", table),
            tees: Vec::new(),
            leaf: None,
        },
        start_lsn: 0x100,
        xids,
        rows: 1,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_entry_waits_and_dropped_pending_table_retires() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let mut ch = ch().await;
    let spill = tempfile::tempdir().unwrap();
    let mut ledger = PendingLedger::load(spill.path(), 7).await.unwrap();
    ledger.hold(&manifest("held", vec![101]));
    // No pending table on CH: a crash after DROP, before the ledger persist
    ledger.stage(&manifest("gone", vec![102]));

    settle(&mut ledger, &mut ch.sess, &EmitterStats::default())
        .await
        .expect("settle");
    let left: Vec<&str> = ledger.entries().iter().map(|e| &*e.relname).collect();
    assert_eq!(left, ["held"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promote_without_shared_columns_fails() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let mut ch = ch().await;
    let m = manifest("t", vec![101]);
    ch.server
        .query("CREATE TABLE db.t (a Int32) ENGINE = MergeTree ORDER BY a")
        .expect("target");
    ch.server
        .query(&format!(
            "CREATE TABLE db.{} (b Int32) ENGINE = MergeTree ORDER BY b",
            m.rel.pending_table()
        ))
        .expect("pending table");
    let spill = tempfile::tempdir().unwrap();
    let mut ledger = PendingLedger::load(spill.path(), 7).await.unwrap();
    ledger.stage(&m);
    ledger.note(101, &[], true);

    let err = settle(&mut ledger, &mut ch.sess, &EmitterStats::default())
        .await
        .expect_err("promote without shared columns");
    assert!(err.contains("no shared columns"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwritable_ledger_fails_the_round() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let mut ch = ch().await;
    let spill = tempfile::tempdir().unwrap();
    let mut ledger = PendingLedger::load(spill.path(), 7).await.unwrap();
    std::fs::create_dir_all(spill.path().join(PENDING_LEDGER_FILENAME).join("blocker")).unwrap();

    let err = settle(&mut ledger, &mut ch.sess, &EmitterStats::default())
        .await
        .expect_err("persist into a directory");
    assert!(err.contains("ledger persist"), "{err}");
}

fn dest(port: u16, instances: &[&str]) -> Arc<DestEmitter> {
    let conn = InstanceConfig {
        host: "127.0.0.1".into(),
        port,
        ..Default::default()
    };
    let cfg = EmitterConfig {
        instances: instances
            .iter()
            .map(|name| (name.to_string(), conn.clone()))
            .collect(),
        conn,
        // Refusals are final here, skip the backoff
        retry: RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        },
        ..Default::default()
    };
    DestEmitter::new(Arc::new(cfg), None)
}

fn int_rel(name: &str) -> Arc<RelDescriptor> {
    Arc::new(RelDescriptor {
        rfn: walrus::pg::walparser::RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16400,
        },
        oid: 16400,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", name),
        kind: 'r',
        persistence: 'p',
        ancestors: Vec::new(),
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "a".into(),
            type_oid: 23,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: "int4".into(),
            type_byval: true,
            type_len: 4,
            type_align: 'i',
            type_storage: 'p',
            missing_default: None,
        }],
    })
}

/// Spool holding one undecided row of `rel`
async fn one_row_spool(
    dir: &std::path::Path,
    rel: &Arc<RelDescriptor>,
    value: Datum,
) -> PendingSpool {
    let mut catalog = CatalogMap::new();
    catalog.insert(rel.clone());
    let mut spool = PendingSpool::new(dir.join("pending.bin"), catalog);
    let tuple = BackfillTuple {
        rfn: rel.rfn,
        xid: 101,
        xmax: 0,
        infomask: 0,
        source_lsn: 0x5000,
        page_lsn: 0,
        blkno: 0,
        offnum: 1,
        data: TupleData::from_cells(&[Some(value)]),
        pending: None,
    };
    let xids = PendingXids {
        insert: 101,
        delete: 0,
    };
    assert_eq!(spool.push(tuple, xids).await, Ok(true));
    spool
}

fn mapping_of(rel: &RelDescriptor, table: &str) -> walshadow::mapping::MappingSnapshot {
    let m = TableMapping {
        target: TableTarget::new("db", table),
        columns: vec![ColumnMapping {
            src_attnum: 1,
            target_name: "a".into(),
            target_type: "Int32".into(),
            type_pinned: false,
        }],
        tees: Vec::new(),
    };
    Arc::new(walshadow::mapping::shared(
        [(rel.rel_name.clone(), m)].into_iter().collect(),
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ship_discards_unmapped_rows_and_names_failed_rebuild() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let ch = ch().await;
    let scratch = tempfile::tempdir().unwrap();
    let rel = int_rel("t");
    let ship_to = |live: walshadow::mapping::MappingSnapshot, spool| {
        let dest = dest(ch.server.port, &[]);
        let scratch = scratch.path().to_path_buf();
        async move {
            ship(
                spool,
                &live,
                dest,
                Arc::new(EmitterStats::default()),
                ToastResolver::disabled(),
                None,
                None,
                &scratch,
            )
            .await
        }
    };

    let spool = one_row_spool(scratch.path(), &rel, build::int4(7)).await;
    let manifests = ship_to(Arc::default(), spool).await.expect("unmapped ship");
    assert!(manifests.is_empty());
    let tables = ch.server.query("SHOW TABLES FROM db").expect("list tables");
    assert_eq!(
        tables.trim(),
        "",
        "no pending table for an unmapped relation"
    );

    let spool = one_row_spool(scratch.path(), &rel, build::int4(7)).await;
    let err = ship_to(mapping_of(&rel, "missing"), spool)
        .await
        .expect_err("pending table cloned from a missing target");
    assert!(err.starts_with("pending visibility: "), "{err}");

    // Row past the in-memory prefix spools to disk; losing the file fails replay
    ch.server
        .query("CREATE TABLE db.t (a Int32) ENGINE = MergeTree ORDER BY a")
        .expect("target");
    let wide = Datum::Varlena(vec![b'x'; walshadow::spool::DEFERRED_SPOOL_MEM_MAX]);
    let spool = one_row_spool(scratch.path(), &rel, wide).await;
    std::fs::remove_file(scratch.path().join("pending.bin")).unwrap();
    let err = ship_to(mapping_of(&rel, "t"), spool)
        .await
        .expect_err("spool file gone");
    assert!(err.starts_with("pending visibility: spool seal: "), "{err}");
}

/// Tee on another instance whose pending table never landed promotes
/// main target alone, then retires the entry
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promote_skips_remote_tee_without_pending_table() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let ch = ch().await;
    let mut m = manifest("t", vec![101]);
    m.rel.tees = vec![TableTarget {
        instance: Some("audit".into()),
        database: "db".into(),
        table: "t_log".into(),
    }];
    ch.server
        .query("CREATE TABLE db.t (a Int32) ENGINE = MergeTree ORDER BY a")
        .expect("target");
    ch.server
        .query(&format!(
            "CREATE TABLE db.{} (a Int32, _ws_xmin UInt32, _ws_xmax UInt32) \
             ENGINE = MergeTree ORDER BY a",
            m.rel.pending_table()
        ))
        .expect("pending table");
    ch.server
        .query(&format!(
            "INSERT INTO db.{} VALUES (7, 101, 0)",
            m.rel.pending_table()
        ))
        .expect("pending row");
    let mut sess = StagingSession::connect(dest(ch.server.port, &["audit"]))
        .await
        .expect("staging session");
    let mut ledger = PendingLedger::empty();
    ledger.stage(&m);
    ledger.note(101, &[], true);

    settle(&mut ledger, &mut sess, &EmitterStats::default())
        .await
        .expect("settle");
    assert!(ledger.is_empty());
    let rows = ch.server.query("SELECT a FROM db.t").expect("promoted");
    assert_eq!(rows.trim(), "7");
}
