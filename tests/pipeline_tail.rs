#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use walshadow::ch_emitter::{CompressionChoice, EmitterConfig, EmitterStats};
use walshadow::pipeline::Fatal;
use walshadow::pipeline::tail;

fn emitter(port: u16) -> EmitterConfig {
    EmitterConfig {
        host: "127.0.0.1".into(),
        port,
        database: "walshadow_test".into(),
        compression: CompressionChoice::Lz4,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_finish_flushes_and_drains_clean() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, 17960, 17961).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let fatal = Fatal::new();
    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (msg_tx, ack, parts) = tail::spawn(&emitter(17960), 2, stats, emitter_ack, fatal.clone())
        .await
        .expect("spawn tail");

    parts
        .finish(msg_tx, ack, 0, &fatal)
        .await
        .expect("finish drains clean");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_finish_returns_fatal_message() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, 17970, 17971).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let fatal = Fatal::new();
    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (msg_tx, ack, parts) = tail::spawn(&emitter(17970), 2, stats, emitter_ack, fatal.clone())
        .await
        .expect("spawn tail");

    fatal.set("boom".into());
    let err = parts
        .finish(msg_tx, ack, 0, &fatal)
        .await
        .expect_err("fatal must surface");
    assert!(err.contains("boom"), "got {err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_finish_fatal_during_drain() {
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, 17980, 17981).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let fatal = Fatal::new();
    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let (msg_tx, ack, parts) = tail::spawn(&emitter(17980), 2, stats, emitter_ack, fatal.clone())
        .await
        .expect("spawn tail");

    // Flush has no rows so it acks immediately; finish then parks on the
    // durability drain (watermark never reaches u64::MAX). Trip fatal there.
    let f = fatal.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        f.set("drain boom".into());
    });
    let err = parts
        .finish(msg_tx, ack, u64::MAX, &fatal)
        .await
        .expect_err("fatal during drain must surface");
    assert!(
        err.contains("drain boom") || err.contains("fatal"),
        "got {err:?}"
    );
}
