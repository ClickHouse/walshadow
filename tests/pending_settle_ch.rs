//! Pending visibility settle against ClickHouse: which entries settle, stay,
//! or fail the round

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;

use walshadow::backfill_staging::StagingSession;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats, InstanceConfig};
use walshadow::config::DestEmitter;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::visibility_pending::{
    PENDING_LEDGER_FILENAME, PendingLedger, PendingManifest, PendingRel, settle,
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
