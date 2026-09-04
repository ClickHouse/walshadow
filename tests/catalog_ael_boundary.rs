//! A recovery-held catalog `AccessExclusiveLock` must not wedge descriptor
//! capture.
//!
//! Source-side, `VACUUM`'s truncation step takes `AccessExclusiveLock` on the
//! catalog it is vacuuming and releases it before its transaction commits
//! (`vacuumlazy.c`, `lazy_truncate_heap`). A standby has no record of that
//! early release: `StandbyAcquireAccessExclusiveLock` holds the replayed lock
//! until the owning transaction's commit record arrives. So between the lock
//! record and that commit, the shadow's startup process holds AEL on a catalog
//! while other transactions keep committing catalog changes.
//!
//! walshadow stops publishing successor WAL at a catalog boundary and then
//! reads the shadow's catalog. If that read waits on the recovery-held lock,
//! the release it waits for is in the WAL it is withholding — a closed cycle.
//! `pg_type` makes it total: every statement resolves types at parse time, so
//! the entire shadow stops answering.
//!
//! The lock is manufactured with `pgext/test/wstest.so` rather than by waiting
//! for autovacuum, so the required ordering — `V` lock < `D` boundary < `V`
//! commit — is deterministic.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use walshadow::mapping::NamespaceMapping;
use walshadow::record::{Record, RecordSink, SinkError};
use walshadow::segment_sink::DirSegmentSink;
use walshadow::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use walshadow::wal_stream::WalStream;

/// Long enough that `V` is still open for the whole capture attempt.
const HOLD_SECS: u64 = 90;
/// Capture is a few local round trips; a fixed budget well above that turns
/// the deadlock into a test failure instead of a hung run.
const CAPTURE_BUDGET: Duration = Duration::from_secs(25);

fn wstest_module() -> PathBuf {
    let so = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("pgext/test")
        .join("wstest.so");
    assert!(
        so.is_file(),
        "{} missing, run `make -C pgext/test`",
        so.display()
    );
    so
}

/// Tracks per-record progress: `WalStream::dispatched_lsn` only moves at
/// segment boundaries, so it cannot tell a stalled pump from a few KB of WAL.
struct Progress<'s> {
    inner: &'s mut fx::PipelineSinks,
    max_next_lsn: u64,
}

impl RecordSink for Progress<'_> {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SinkError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.inner.on_record(record).await?;
            self.max_next_lsn = self.max_next_lsn.max(record.next_lsn);
            Ok(())
        })
    }
}

/// Pump until a record whose `next_lsn` reaches `target` has been *processed*.
/// Separate `&mut` fields rather than `&mut Pipeline` so borrows stay disjoint.
async fn pump_to_lsn(
    feed: &mut SourceFeed,
    stream: &mut WalStream,
    sinks: &mut Progress<'_>,
    segs: &mut DirSegmentSink,
    buf: &mut Vec<u8>,
    target: u64,
) -> Result<(), String> {
    while sinks.max_next_lsn < target {
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_event(StandbyStatus::collapsed(stream.dispatched_lsn()), buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(SourceEvent::Wal(c))) => c,
            Ok(Ok(_)) => break,
            Ok(Err(e)) => return Err(format!("source feed: {e:#}")),
            Err(_) => continue,
        };
        stream
            .push(chunk.start_lsn, chunk.data, sinks, segs)
            .await
            .map_err(|e| format!("push: {e}"))?;
    }
    Ok(())
}

/// Drive one catalog boundary through capture. `hold_lock` decides whether a
/// transaction leaves a recovery-held AEL on `pg_type` straddling it.
/// Returns how far capture got, or `Err` once the budget expires.
async fn run_boundary(hold_lock: bool) -> Result<(), String> {
    let so = wstest_module();
    let ports = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let schema = format!(
        "CREATE TABLE demo (id int primary key, v text);\n\
         INSERT INTO demo VALUES (1, 'a');\n\
         CREATE FUNCTION ws_test_lock_unlock_relation(oid) RETURNS void \
         AS '{}', 'ws_test_lock_unlock_relation' LANGUAGE c;\n",
        so.display(),
    );

    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema, ports.source, ports.shadow, ports.walsender).await;
    let _keep_shadow = &shadow;

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ports.ch_tcp, ports.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create ch db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "public".into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            drop_table_strategy: None,
            initial_load: None,
        },
    );

    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: ports.ch_tcp,
        mappings: vec![],
        app_name: "catalog-ael-boundary",
        ddl: Some(ddl_args),
    })
    .await;

    // Transaction V: take AEL on pg_type, release it source-side, stay open.
    // The shadow keeps the replayed lock until V commits.
    let v = hold_lock.then(|| {
        fx::spawn_txn(
            &source,
            &format!(
                "BEGIN;\n\
                 SELECT ws_test_lock_unlock_relation('pg_catalog.pg_type'::regclass);\n\
                 SELECT pg_sleep({HOLD_SECS});\n\
                 COMMIT;\n"
            ),
        )
    });

    // V parks in pg_sleep, so its lock record is written and its commit is
    // not. The source-side lock is already gone, which is what lets D run.
    if hold_lock {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let parked = source
                .psql_one(
                    "SELECT count(*) FROM pg_stat_activity \
                     WHERE state = 'active' AND query LIKE 'SELECT pg_sleep%'",
                )
                .map_err(|e| format!("probe V: {e}"))?;
            if parked.trim() == "1" {
                break;
            }
            if Instant::now() >= deadline {
                return Err("transaction V never parked".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Transaction D: an unrelated catalog mutation, so its boundary lands in
    // the WAL between V's lock and V's commit.
    source
        .psql_one("ALTER TABLE demo ADD COLUMN w int")
        .map_err(|e| format!("D ddl: {e}"))?;
    let target = {
        let s = source
            .psql_one("SELECT pg_current_wal_insert_lsn()")
            .map_err(|e| format!("insert lsn: {e}"))?;
        walshadow::pg::parse_pg_lsn(&s).map_err(|e| format!("parse lsn: {e}"))?
    };

    let mut progress = Progress {
        inner: &mut pipeline.sinks,
        max_next_lsn: 0,
    };
    let pumped = tokio::time::timeout(
        CAPTURE_BUDGET,
        pump_to_lsn(
            &mut pipeline.feed,
            &mut pipeline.stream,
            &mut progress,
            &mut pipeline.segment_sink,
            &mut pipeline.chunk_buf,
            target,
        ),
    )
    .await;
    let reached = progress.max_next_lsn;
    let captures = pipeline.desc_log.covered_through();
    drop(v);

    match pumped {
        Err(_) => Err(format!(
            "pump made no progress past {reached:#X} within {CAPTURE_BUDGET:?} \
             (target {target:#X}, descriptor log covered through {captures:#X})"
        )),
        Ok(Err(e)) => Err(e),
        Ok(Ok(())) => Ok(()),
    }
}

/// Control: the identical boundary with no lock straddling it must capture
/// promptly. Guards the repro below against a harness-level stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boundary_capture_completes_without_held_catalog_lock() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }
    run_boundary(false).await.expect("control boundary");
}

/// Regression: capture must not wait on a lock whose release is in the WAL
/// walshadow is withholding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boundary_capture_survives_recovery_held_catalog_lock() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }
    if let Err(e) = run_boundary(true).await {
        panic!(
            "boundary capture wedged behind a recovery-held AccessExclusiveLock \
             on pg_type: {e}"
        );
    }
}
