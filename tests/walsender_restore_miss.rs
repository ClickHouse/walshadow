//! Shadow's `restore_command` misses a segment the daemon seals and trims from
//! its backfill before the walreceiver's `START_REPLICATION` arrives for it.
//!
//! PG's startup process trusts the walreceiver's shared flush position across
//! restarts (`walreceiverfuncs.c` `RequestXLogStreaming` resets it only on a
//! first start or timeline change) and opens any segment below it as streamed
//! (`xlogrecovery.c` `WaitForWALToBecomeAvailable`, `XLogFileRead` with
//! `XLOG_FROM_STREAM`). Live bytes framed past the requested start therefore
//! PANIC it on a segment no one wrote.

#[path = "common/inproc_harness.rs"]
mod harness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use walrus::pg::wal::segment::SegmentName;
use walshadow::pg::parse_pg_lsn;
use walshadow::record::{RecordBytesSink, WAL_SEG_SIZE};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_stream::{ShadowStreamSink, ShadowStreamState, WalSenderAddr};
use walshadow::wal_stream::WalStream;

fn segment_name(lsn: u64) -> String {
    SegmentName::from_lsn(1, lsn, WAL_SEG_SIZE).format()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_miss_below_the_backfill_never_panics_shadow() {
    if !harness::tools::pg_available() || !harness::tools::pg_basebackup_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = harness::make_pg(&tmp, "source", harness::PG_SOURCE_PORT);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    harness::append_source_conf(&source);
    source.start().unwrap();
    let _source_stop = harness::StopOnDrop { sh: &source };
    source.psql_one("CREATE TABLE t (i int)").unwrap();
    let standby_data = tmp.path().join("standby-data");
    harness::pg_basebackup(&source, &standby_data).unwrap();

    // Backup stop switches segments, so the standby asks for `missed` next
    let lsn = |sql: &str| parse_pg_lsn(&source.psql_one(sql).unwrap()).unwrap();
    let missed = WalStream::align_down(lsn("SELECT pg_current_wal_lsn()"), WAL_SEG_SIZE);
    source
        .psql_one("INSERT INTO t SELECT generate_series(1, 1000)")
        .unwrap();
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    source
        .psql_one("INSERT INTO t SELECT generate_series(1001, 2000)")
        .unwrap();
    let end = lsn("SELECT pg_current_wal_flush_lsn()");
    let live = missed + WAL_SEG_SIZE;
    assert_eq!(WalStream::align_down(end, WAL_SEG_SIZE), live);
    let src_wal = source.config().data_dir.join("pg_wal");
    let live_bytes = std::fs::read(src_wal.join(segment_name(live))).unwrap();
    let live_bytes = &live_bytes[..(end - live) as usize];

    let archive = tmp.path().join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    let sysid = source
        .psql_one("SELECT system_identifier::text FROM pg_control_system()")
        .unwrap();
    let state = Arc::new(Mutex::new(ShadowStreamState::new(
        1,
        sysid,
        live,
        64 * 1024 * 1024,
    )));
    let mut sink = ShadowStreamSink::new(state.clone());
    let (head, tail) = live_bytes.split_at(live_bytes.len() / 2);
    sink.on_wire_chunk(live, head).await.unwrap();
    let walsender_port = harness::reserve_port();
    let _listener = walshadow::shadow_stream::spawn_listener(
        WalSenderAddr::Tcp(format!("127.0.0.1:{walsender_port}").parse().unwrap()),
        state.clone(),
        Duration::from_millis(5),
    )
    .await
    .unwrap();

    let standby_sock = tmp.path().join("standby-sock");
    std::fs::create_dir_all(&standby_sock).unwrap();
    harness::rewrite_for_shadow(&standby_data, harness::PG_SHADOW_PORT, &standby_sock).unwrap();
    harness::enable_recovery(&standby_data, &archive, walsender_port).unwrap();
    let mut cfg = ShadowConfig::new(standby_data.clone(), archive.clone());
    cfg.port = harness::PG_SHADOW_PORT;
    cfg.socket_dir = standby_sock;
    cfg.ctl_timeout = Duration::from_secs(60);
    let standby = Shadow::new(cfg);
    standby.start().unwrap();
    let _standby_stop = harness::StopOnDrop { sh: &standby };

    // `missed` seals into the archive once the walreceiver dials in, after
    // restore_command looked for it, and the live pump frames the rest of `live`
    let deadline = Instant::now() + Duration::from_secs(60);
    let sealed = archive.join(segment_name(missed));
    let mut tail = Some(tail);
    loop {
        let agg = state.lock().await.aggregate();
        if agg.accepted_total > 0 && !sealed.exists() {
            std::fs::copy(src_wal.join(segment_name(missed)), &sealed).unwrap();
        }
        if agg.active_connections > 0
            && let Some(bytes) = tail.take()
        {
            sink.on_wire_chunk(live + head.len() as u64, bytes)
                .await
                .unwrap();
        }
        let log = std::fs::read_to_string(standby_data.join("startup.log")).unwrap();
        assert!(!log.contains("PANIC"), "{log}");
        if standby.psql_one("SELECT count(*) FROM t").ok().as_deref() == Some("2000") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "standby never replayed `live`:\n{log}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
