//! A shadow whose walreceiver `walshadow.so` wraps streams compact frames:
//! its `pg_wal` matches the archive byte for byte across a reconnect
//! mid-segment, and a corrupt frame costs only that connection. Restarted
//! without the module, the same shadow streams plain frames.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::path::Path;
use std::time::{Duration, Instant};

use walshadow::pos::Pos;
use walshadow::record::{CountingRecordSink, WAL_SEG_SIZE};
use walshadow::segment_sink::DirSegmentSink;
use walshadow::source_feed::SourceFeed;
use walshadow::wal_stream::WalStream;

/// Rows wide enough that a few statements fill segments, all dropped by the
/// filter, so the stream is mostly NOOP runs
const INSERT: &str = "INSERT INTO t SELECT g, repeat('x', 300) FROM generate_series(1, 20000) g";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrapped_walreceiver_replays_compact_frames() {
    if !fx::tools::pg_available() || !fx::tools::pg_basebackup_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        state,
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE TABLE t (id bigint, payload text);\n",
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;
    let mut feed = SourceFeed::connect(&fx::pg_cfg(&source, "compact-stream"))
        .await
        .unwrap()
        .with_status_interval(Duration::from_millis(500));
    let ident = feed.identify_system().await.unwrap();
    let start = WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE);
    feed.start_physical_replication(None, start, ident.timeline)
        .await
        .unwrap();
    let mut stream = WalStream::builder(ident.timeline, WAL_SEG_SIZE, Pos::new(start)).unwrap();
    stream.set_bytes_sink(Box::new(walshadow::shadow_stream::ShadowStreamSink::new(
        state.clone(),
    )));
    let mut rig = Rig {
        feed,
        stream: stream.start(),
        records: CountingRecordSink::default(),
        segments: DirSegmentSink::new(shadow_filter_dir.clone()).unwrap(),
        buf: Vec::new(),
    };

    rig.pump(&source).await;
    assert!(state.lock().await.aggregate().compact_connections >= 1);
    // Drop the walreceiver mid-segment: it restarts at the segment start and
    // is backfilled from the retained bytes, framed compact
    shadow
        .psql_one("SELECT pg_terminate_backend(pid) FROM pg_stat_wal_receiver")
        .unwrap();
    rig.pump(&source).await;
    assert!(state.lock().await.aggregate().compact_connections >= 1);
    // A cut header; then a payload cut short, a literal past its end and a
    // zero run past the expanded length, each claiming 10 bytes
    let mut frames = vec![vec![b'z'; 25]];
    for payload in [&[][..], &[5, 1, 2][..], &[0, 100][..]] {
        let mut frame = vec![b'z'; 25];
        frame.extend_from_slice(&10u32.to_be_bytes());
        frame.extend_from_slice(payload);
        frames.push(frame);
    }
    for (n, frame) in frames.iter().enumerate() {
        state.lock().await.inject(frame);
        await_log(
            &shadow.config().data_dir,
            "corrupt compact WAL frame",
            n + 1,
        )
        .await;
    }
    rig.pump(&source).await;
    caught_up(&shadow, &rig.stream, &shadow_filter_dir);

    // Without the module the walreceiver asks for nothing and reads 'w'
    shadow.stop().unwrap();
    let conf = shadow.config().data_dir.join("postgresql.conf");
    let text = std::fs::read_to_string(&conf).unwrap();
    std::fs::write(&conf, text.replace("shared_preload_libraries", "#")).unwrap();
    shadow.start().unwrap();
    rig.pump(&source).await;
    let agg = state.lock().await.aggregate();
    assert!(agg.active_connections >= 1 && agg.compact_connections == 0);
    caught_up(&shadow, &rig.stream, &shadow_filter_dir);
}

struct Rig {
    feed: SourceFeed,
    stream: WalStream,
    records: CountingRecordSink,
    segments: DirSegmentSink,
    buf: Vec<u8>,
}

impl Rig {
    /// Seal a segment of the workload's WAL
    async fn pump(&mut self, source: &walshadow::shadow::Shadow) {
        let workload = fx::spawn_workload(source, vec![INSERT.into(); 4]);
        let sealed = fx::pump_until(
            &mut self.feed,
            &mut self.stream,
            &mut self.records,
            &mut self.segments,
            &mut self.buf,
            1,
            Duration::from_secs(60),
        )
        .await;
        workload.join().unwrap();
        assert!(sealed >= 1, "no segment sealed");
    }
}

/// Shadow replayed through the stream, its received WAL the archive's bytes
fn caught_up(shadow: &walshadow::shadow::Shadow, stream: &WalStream, archive: &Path) {
    shadow
        .wait_for_replay(stream.dispatched_lsn(), Duration::from_secs(60))
        .unwrap();
    let pg_wal = shadow.config().data_dir.join("pg_wal");
    let mut compared = 0;
    for entry in std::fs::read_dir(archive).unwrap() {
        let name = entry.unwrap().file_name();
        let (Ok(sealed), Ok(received)) = (
            std::fs::read(archive.join(&name)),
            std::fs::read(pg_wal.join(&name)),
        ) else {
            continue;
        };
        assert!(sealed == received, "{name:?} differs from the archive");
        compared += 1;
    }
    assert!(compared > 0, "no streamed segment left to compare");
}

/// Shadow's log names `needle` at least `count` times
async fn await_log(data_dir: &Path, needle: &str, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let log = std::fs::read_to_string(data_dir.join("startup.log")).unwrap_or_default();
        if log.matches(needle).count() >= count {
            return;
        }
        assert!(Instant::now() < deadline, "{needle:?} not logged {count}x");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
