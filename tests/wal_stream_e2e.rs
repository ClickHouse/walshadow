//! PRE5 item 1: full WAL capture pipeline.
//!
//! Source PG → `SourceFeed` (`START_REPLICATION PHYSICAL`) →
//! `WalStream` (segment-aligned filter) → `DirSegmentSink` →
//! filtered segments on disk that `WalParser` can re-parse.
//!
//! Skipped silently when `initdb` is not on `$PATH`. Source PG is
//! configured with `wal_level=replica` + `max_wal_senders=4` so the
//! replication protocol works over the test unix socket.
//!
//! The test issues DDL + DML on source, then forces a `pg_switch_wal()`
//! to roll a segment boundary so the filter has a full segment to
//! produce. After segments land, it re-parses one through wal-rs's
//! `WalParser` and asserts the manifest agrees with the parser's
//! record count.

use std::fs::OpenOptions;
use std::io::Write;
use std::process::Command;
use std::time::Duration;

use wal_rs::pg::replication::conn::PgConfig;
use wal_rs::pg::replication::tls::SslMode;
use wal_rs::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;
use wal_rs::pg::walparser::{WAL_PAGE_SIZE, WalParser};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::source_feed::SourceFeed;
use walshadow::wal_stream::{CollectingRecordSink, DirSegmentSink, WAL_SEG_SIZE, WalStream};

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_source(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("source"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("source_sock");
    cfg.ctl_timeout = Duration::from_secs(30);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

/// Append walshadow's base conf, then upgrade `wal_level` to `replica`
/// and enable `max_wal_senders` so the replication-protocol connection
/// works.
fn append_replication_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow source-mode overrides").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
    writeln!(f, "wal_level = replica").unwrap();
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_pipeline_source_to_filtered_segments_on_disk() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, 55801);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("base conf");
    append_replication_conf(&source);
    source.start().expect("start source");
    let _stop = StopOnDrop { sh: &source };

    // Lay down some catalog + user state, then force a segment switch
    // so the filter has a full segment to chew.
    source
        .apply_schema_dump(
            "CREATE SCHEMA src;\n\
             CREATE TABLE src.t (id bigint primary key, payload text);\n\
             INSERT INTO src.t SELECT g, repeat('x', 80) FROM generate_series(1,1000) g;\n\
             SELECT pg_switch_wal();\n",
        )
        .expect("apply workload");

    // Connect via replication protocol over the source's unix socket.
    let cfg = source.config();
    let pgcfg = PgConfig {
        host: cfg.socket_dir.to_string_lossy().into_owned(),
        port: cfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-test".into(),
        sslmode: SslMode::Disable,
    };
    let mut feed = SourceFeed::connect(&pgcfg)
        .await
        .expect("source feed connect")
        .with_status_interval(Duration::from_millis(500));
    let ident = feed.identify_system().await.expect("IDENTIFY_SYSTEM");
    assert!(ident.timeline >= 1);
    assert!(ident.xlogpos > 0);

    let aligned = WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE);

    feed.start_physical_replication(None, aligned, ident.timeline)
        .await
        .expect("START_REPLICATION");

    let out_dir = tmp.path().join("filtered");
    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned).unwrap();
    let mut records = CollectingRecordSink::default();
    let mut segs = DirSegmentSink::new(out_dir.clone()).expect("open out dir");
    let mut buf = Vec::with_capacity(64 * 1024);

    // Trigger more WAL activity on source from a second thread to keep
    // chunks flowing while we pump from this thread.
    let pump_path = source.config().data_dir.clone();
    let pump_port = source.config().port;
    let pump_sock = source.config().socket_dir.clone();
    let driver = std::thread::spawn(move || {
        // Wait briefly so START_REPLICATION is fully established before
        // we start writing.
        std::thread::sleep(Duration::from_millis(200));
        for _ in 0..3 {
            let _ = Command::new("psql")
                .args([
                    "-h",
                    pump_sock.to_str().unwrap(),
                    "-p",
                    &pump_port.to_string(),
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-c",
                    "INSERT INTO src.t SELECT g+1000000, repeat('y', 80) FROM generate_series(1,500) g; SELECT pg_switch_wal();",
                ])
                .output();
            std::thread::sleep(Duration::from_millis(100));
        }
        // Silence unused warning.
        let _ = pump_path;
    });

    // Pump until we've collected at least one full segment OR a 15s
    // budget elapses.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut segments_shipped = 0u64;
    let mut prev_dispatched = stream.dispatched_lsn();
    while segments_shipped < 1 && std::time::Instant::now() < deadline {
        let apply_lsn = stream.dispatched_lsn();
        let next =
            tokio::time::timeout(Duration::from_secs(2), feed.next_chunk(apply_lsn, &mut buf))
                .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break, // CopyDone
            Ok(Err(e)) => panic!("source feed error: {e:#}"),
            Err(_) => continue, // status tick
        };
        stream
            .push(chunk.start_lsn, chunk.data, &mut records, &mut segs)
            .expect("push");
        let now_dispatched = stream.dispatched_lsn();
        if now_dispatched != prev_dispatched {
            let new_segs = (now_dispatched - prev_dispatched) / WAL_SEG_SIZE;
            segments_shipped += new_segs;
            prev_dispatched = now_dispatched;
        }
    }
    let _ = driver.join();

    assert!(
        segments_shipped >= 1,
        "no segments shipped in 15s — pipeline didn't drain",
    );

    // Sanity: out_dir contains at least one 24-hex segment file plus a
    // manifest sidecar.
    let mut seg_files = vec![];
    for entry in std::fs::read_dir(&out_dir).unwrap() {
        let e = entry.unwrap();
        let n = e.file_name().to_string_lossy().into_owned();
        if n.len() == 24 && n.chars().all(|c| c.is_ascii_hexdigit()) {
            seg_files.push(e.path());
        }
    }
    assert!(
        !seg_files.is_empty(),
        "expected ≥1 filtered segment file in {}",
        out_dir.display(),
    );
    let seg = std::fs::read(&seg_files[0]).unwrap();
    assert_eq!(seg.len(), DEFAULT_WAL_SEG_SIZE as usize);
    // Re-parse via WalParser; every record must come back cleanly
    // because the filter rewrote dropped records as NOOPs of identical
    // xl_tot_len.
    let mut parser = WalParser::new();
    let mut count = 0;
    for page in seg.chunks(WAL_PAGE_SIZE as usize) {
        let (_, recs) = parser
            .parse_records_from_page(page)
            .expect("parse filtered page");
        count += recs.len();
    }
    assert!(count > 0, "filtered segment had zero records");

    // Manifest sidecar exists alongside.
    let mani_path = seg_files[0]
        .with_extension("manifest.json")
        .with_file_name(format!(
            "{}.manifest.json",
            seg_files[0].file_name().unwrap().to_string_lossy()
        ));
    assert!(
        mani_path.exists(),
        "manifest sidecar at {}",
        mani_path.display(),
    );
    let manifest: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&mani_path).expect("open manifest"))
            .expect("parse manifest");
    assert_eq!(
        manifest["records"].as_array().unwrap().len(),
        count,
        "manifest record count must match WalParser's count on filtered bytes",
    );
    eprintln!(
        "e2e: shipped {} segment(s), {} record events surfaced via RecordSink",
        segments_shipped,
        records.events.len(),
    );
}
