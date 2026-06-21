//! Full WAL capture pipeline against a live source PG.
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
//! produce. After segments land, it re-parses one through wal-rus's
//! `WalParser` and asserts the manifest agrees with the parser's
//! record count.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::SslMode;
use walrus::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;
use walrus::pg::walparser::{WAL_PAGE_SIZE, WalParser};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::source_feed::{SourceFeed, StandbyStatus};
use walshadow::wal_stream::{
    CollectingRecordSink, DirSegmentSink, MetricsRecordSink, WAL_SEG_SIZE, WalStream,
};

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
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_chunk(StandbyStatus::collapsed(apply_lsn), &mut buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break, // CopyDone
            Ok(Err(e)) => panic!("source feed error: {e:#}"),
            Err(_) => continue, // status tick
        };
        stream
            .push(chunk.start_lsn, chunk.data, &mut records, &mut segs)
            .await
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
    // RecordSink contract: parsed records arrive at the RecordSink with
    // their full XLogRecord shape. Prove the wal-rus →
    // filter_segment → WalStream chain forwards `parsed.header` and
    // `parsed.blocks` rather than the old scalar-only RecordEvent.
    assert!(!records.records.is_empty(), "RecordSink saw zero records");
    // PG 17 baseline tops out at RmId::LogicalMsg = 21.
    const MAX_KNOWN_RMID: u8 = 21;
    for r in &records.records {
        assert!(
            r.parsed.header.resource_manager_id <= MAX_KNOWN_RMID,
            "parsed resource_manager_id out of range: {}",
            r.parsed.header.resource_manager_id,
        );
        assert!(r.source_lsn >= aligned);
        assert!(r.parsed.header.total_record_length >= 24);
    }
    // At least one record carries a populated block locator so
    // `blocks[0].header.location.rel` flows through with non-zero
    // fields.
    let with_block = records
        .records
        .iter()
        .find(|r| {
            r.parsed
                .blocks
                .iter()
                .any(|b| b.header.location.rel.rel_node != 0)
        })
        .expect("at least one record with a populated block ref");
    let rel = with_block
        .parsed
        .blocks
        .iter()
        .find(|b| b.header.location.rel.rel_node != 0)
        .map(|b| b.header.location.rel)
        .unwrap();
    assert!(rel.rel_node > 0);
    // At least one record carries a non-zero xact_id. A live INSERT +
    // pg_switch_wal workload always emits ≥1 xact-bearing record
    // (heap insert under the auto-commit xact).
    let xid_bearing = records
        .records
        .iter()
        .find(|r| r.parsed.header.xact_id != 0)
        .expect("at least one record with a populated xact_id");
    eprintln!(
        "e2e: shipped {} segment(s), {} records surfaced via RecordSink (sample xid={})",
        segments_shipped,
        records.records.len(),
        xid_bearing.parsed.header.xact_id,
    );
}

fn pg_class_filenode(sh: &Shadow) -> u32 {
    sh.psql_one("SELECT pg_relation_filenode('pg_class'::regclass)::int8")
        .expect("filenode")
        .parse()
        .expect("integer")
}

/// Catalog-seed regression: walshadow-stream against a source whose pg_class
/// was rotated above 16384 *before* attach. Without
/// `seed_from_source`, heap writes targeting the rotated pg_class
/// filenode classify as User and get dropped (the bootstrap rule
/// `< FirstNormalObjectId` misses post-rotation filenodes, and the
/// authoritative `XLOG_RELMAP_UPDATE` sits in pre-attach WAL the
/// stream never sees).
///
/// With seed, the tracker knows pg_class's current filenode at
/// attach; subsequent DDL writes on that filenode are kept and
/// surfaced via `tracker.pg_class_writes_decoded`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_rotated_pg_class_seed_keeps_catalog_writes() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, 55802);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("base conf");
    append_replication_conf(&source);
    source.start().expect("start source");
    let _stop = StopOnDrop { sh: &source };

    // Rotate pg_class until its filenode crosses 16384 — otherwise the
    // bootstrap rule already catches it and the seed has nothing to
    // contribute.
    let mut iters = 0;
    loop {
        source
            .psql_one("VACUUM FULL pg_class")
            .expect("vacuum full pg_class");
        if pg_class_filenode(&source) >= 16384 {
            break;
        }
        iters += 1;
        assert!(
            iters < 200,
            "pg_class filenode stayed below 16384 after {iters} VACUUM FULL passes",
        );
    }
    let pg_class_fn = pg_class_filenode(&source);
    assert!(pg_class_fn >= 16384);
    let db: u32 = source
        .psql_one("SELECT oid::int8 FROM pg_database WHERE datname = current_database()")
        .unwrap()
        .parse()
        .unwrap();

    // Force a segment switch so the rotation's WAL records sit in a
    // closed segment behind us — the attach starts fresh and never
    // sees the corresponding XLOG_RELMAP_UPDATE.
    source.psql_one("SELECT pg_switch_wal()").expect("switch");

    let cfg = source.config();
    let pgcfg = PgConfig {
        host: cfg.socket_dir.to_string_lossy().into_owned(),
        port: cfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-pre-rotated".into(),
        sslmode: SslMode::Disable,
    };
    let mut feed = SourceFeed::connect(&pgcfg)
        .await
        .expect("source feed connect")
        .with_status_interval(Duration::from_millis(500));
    let ident = feed.identify_system().await.expect("IDENTIFY_SYSTEM");
    let aligned = WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE);
    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned).unwrap();

    // Seed *before* START_REPLICATION. Without this line the test
    // would catch the regression: tracker would never learn the
    // rotated filenode and the pg_class writes below would be dropped.
    {
        let sql_client = feed.sql_client().await.expect("sidecar sql client");
        let added = stream
            .filter_mut()
            .tracker
            .seed_from_source(sql_client)
            .await
            .expect("seed_from_source");
        assert!(added > 0, "seed must add ≥1 catalog filenode");
    }
    assert!(
        stream.filter().tracker.is_catalog(db, pg_class_fn),
        "after seed, rotated pg_class filenode {pg_class_fn} (db {db}) must be catalog",
    );

    feed.start_physical_replication(None, aligned, ident.timeline)
        .await
        .expect("START_REPLICATION");

    // Generate DDL that updates pg_class on the rotated filenode, then
    // switch to roll a segment.
    let pump_sock = source.config().socket_dir.clone();
    let pump_port = source.config().port;
    let driver = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
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
                "CREATE SCHEMA s; \
                 CREATE TABLE s.a (id int primary key); \
                 CREATE TABLE s.b (id int primary key); \
                 CREATE TABLE s.c (id int primary key); \
                 ALTER TABLE s.a ADD COLUMN payload text; \
                 SELECT pg_switch_wal();",
            ])
            .output();
    });

    let out_dir = tmp.path().join("filtered");
    let mut records = CollectingRecordSink::default();
    let mut segs = DirSegmentSink::new(out_dir.clone()).expect("out dir");
    let mut buf = Vec::with_capacity(64 * 1024);

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut segments_shipped = 0u64;
    let mut prev = stream.dispatched_lsn();
    while segments_shipped < 1 && std::time::Instant::now() < deadline {
        let apply_lsn = stream.dispatched_lsn();
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_chunk(StandbyStatus::collapsed(apply_lsn), &mut buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("source feed error: {e:#}"),
            Err(_) => continue,
        };
        stream
            .push(chunk.start_lsn, chunk.data, &mut records, &mut segs)
            .await
            .expect("push");
        let now = stream.dispatched_lsn();
        if now != prev {
            segments_shipped += (now - prev) / WAL_SEG_SIZE;
            prev = now;
        }
    }
    let _ = driver.join();
    assert!(segments_shipped >= 1, "no segments shipped in 15s");

    let filter = stream.filter();
    let tracker = &filter.tracker;
    // CREATE TABLE / ALTER TABLE write to pg_class. With seed,
    // `is_pg_class_relfilenode(db, pg_class_fn)` is true and the
    // decoder runs on every such block — counters must move off zero.
    let recognized = tracker.pg_class_writes_decoded + tracker.pg_class_writes_undecoded;
    assert!(
        recognized > 0,
        "expected pg_class heap writes on rotated filenode {pg_class_fn} to be recognized; \
         got decoded={} undecoded={}",
        tracker.pg_class_writes_decoded,
        tracker.pg_class_writes_undecoded,
    );
    // The same writes were classified User but promoted to Keep via
    // the tracker's catalog set — kept_user counts them.
    assert!(
        filter.stats.kept_user > 0,
        "expected pg_class writes on rotated filenode to be kept as user-classified \
         catalog records; kept_user={}",
        filter.stats.kept_user,
    );
    eprintln!(
        "pre-rotated e2e: pg_class_fn={pg_class_fn}, recognized={recognized}, \
         kept_user={}, dropped={}",
        filter.stats.kept_user, filter.stats.dropped,
    );
}

fn openssl_available() -> bool {
    Command::new("openssl")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// One-shot self-signed cert + key suitable for PG `ssl_cert_file` /
/// `ssl_key_file`. `nodes` skips passphrase prompting; SAN covers
/// `localhost` + `127.0.0.1` so a future verify-full test could reuse
/// the same cert.
fn generate_self_signed(out_dir: &Path) -> (PathBuf, PathBuf) {
    let cert = out_dir.join("server.crt");
    let key = out_dir.join("server.key");
    let out = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            cert.to_str().unwrap(),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ])
        .output()
        .expect("openssl exec");
    assert!(
        out.status.success(),
        "openssl req failed: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    // PG refuses to start if the key file is group/world-readable.
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).expect("chmod key");
    (cert, key)
}

fn append_ssl_conf(sh: &Shadow, cert: &Path, key: &Path) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow tls test overrides").unwrap();
    writeln!(f, "ssl = on").unwrap();
    writeln!(f, "ssl_cert_file = '{}'", cert.display()).unwrap();
    writeln!(f, "ssl_key_file = '{}'", key.display()).unwrap();
    // Override the base conf's empty `listen_addresses` so 127.0.0.1
    // accepts TCP. Last setting wins in postgresql.conf.
    writeln!(f, "listen_addresses = '127.0.0.1'").unwrap();
}

/// Force SSL on TCP. `hostnossl … reject` makes plain TCP a hard
/// negative so the `pg_stat_ssl` assertion is not satisfied by an
/// accidental fallback.
fn require_ssl_on_tcp(sh: &Shadow) {
    let path = sh.config().data_dir.join("pg_hba.conf");
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f).unwrap();
    writeln!(f, "hostssl all all 127.0.0.1/32 trust").unwrap();
    writeln!(f, "hostnossl all all 127.0.0.1/32 reject").unwrap();
    writeln!(f, "hostssl replication all 127.0.0.1/32 trust").unwrap();
    writeln!(f, "hostnossl replication all 127.0.0.1/32 reject").unwrap();
}

/// Exercises `SourceFeed::sql_client()`'s TLS path explicitly: PG bound
/// to 127.0.0.1 with `ssl=on` + `hostnossl … reject`, so a successful
/// `SELECT 1` and a `pg_stat_ssl.ssl=true` reading prove the sidecar
/// negotiated TLS (not a plain-TCP fallback). Replication-side TLS
/// rides the same wal-rus `maybe_upgrade` and is covered by
/// `walrus::pg::replication::tls` tests; this test pins the
/// tokio-postgres-side wiring (`Config::connect_raw(stream, NoTls)`
/// with the wal-rus-wrapped stream).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sidecar_sql_client_negotiates_tls_over_tcp() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !openssl_available() {
        eprintln!("skip: no openssl on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, 55803);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("base conf");
    append_replication_conf(&source);

    let (cert, key) = generate_self_signed(tmp.path());
    append_ssl_conf(&source, &cert, &key);
    require_ssl_on_tcp(&source);

    source.start().expect("start source with ssl");
    let _stop = StopOnDrop { sh: &source };

    let cfg = source.config();
    let pgcfg = PgConfig {
        host: "127.0.0.1".into(),
        port: cfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-tls-test".into(),
        sslmode: SslMode::Require,
    };
    let mut feed = SourceFeed::connect(&pgcfg)
        .await
        .expect("source feed connect over tls");

    let client = feed
        .sql_client()
        .await
        .expect("sidecar sql_client over tls");

    let rows = client
        .query("SELECT 1::int8", &[])
        .await
        .expect("select 1 over tls");
    let val: i64 = rows[0].get(0);
    assert_eq!(val, 1);

    let rows = client
        .query(
            "SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .expect("pg_stat_ssl");
    let ssl: bool = rows[0].get(0);
    let version: Option<&str> = rows[0].get(1);
    assert!(ssl, "expected SSL session for sidecar sql client");
    let v = version.expect("pg_stat_ssl.version populated under SSL");
    assert!(
        v.starts_with("TLSv1"),
        "expected TLSv1.x session, got {v:?}",
    );
    eprintln!("sidecar tls e2e: pg_stat_ssl reports version={v}");
}

/// Shutdown contract: `walshadow-stream` must call `WalStream::close()` on shutdown
/// so the in-flight partial segment lands on disk rather than evaporating
/// with the process. Exercises the `close()` path directly (signaling a
/// subprocess is racy) plus the resume-from-segment-boundary contract
/// the docstring promises: a fresh `WalStream` at the same aligned LSN
/// must pump cleanly past the partial without tripping on misalignment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_writes_partial_segment_and_resume_from_start_lsn_continues() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, 55804);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("base conf");
    append_replication_conf(&source);
    source.start().expect("start source");
    let _stop = StopOnDrop { sh: &source };

    // Create schema only (skip pg_switch_wal so xlogpos stays mid-segment
    // — otherwise START_REPLICATION at the segment boundary has nothing
    // to ship and we never accumulate bytes into current_buf to write
    // out as a partial).
    source
        .apply_schema_dump(
            "CREATE SCHEMA sd;\n\
             CREATE TABLE sd.t (id bigint primary key, payload text);\n\
             INSERT INTO sd.t SELECT g, repeat('z', 80) FROM generate_series(1, 200) g;\n",
        )
        .expect("apply pre-shutdown workload");

    let cfg = source.config();
    let pgcfg = PgConfig {
        host: cfg.socket_dir.to_string_lossy().into_owned(),
        port: cfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-shutdown".into(),
        sslmode: SslMode::Disable,
    };
    let mut feed = SourceFeed::connect(&pgcfg)
        .await
        .expect("source feed connect")
        .with_status_interval(Duration::from_millis(500));
    let ident = feed.identify_system().await.expect("IDENTIFY_SYSTEM");
    let aligned = WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE);
    // Without WAL beyond aligned, there is nothing to flush. The
    // pre-workload above is the source of those bytes.
    assert!(
        ident.xlogpos > aligned,
        "xlogpos={:#X} aligned={:#X} — INSERT didn't push past segment boundary",
        ident.xlogpos,
        aligned,
    );
    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned).unwrap();

    feed.start_physical_replication(None, aligned, ident.timeline)
        .await
        .expect("START_REPLICATION");

    let out_dir = tmp.path().join("filtered_shutdown");
    let mut metrics = MetricsRecordSink::default();
    let mut segs = DirSegmentSink::new(out_dir.clone()).expect("open out dir");
    let mut buf = Vec::with_capacity(64 * 1024);

    // Pump until current_buf holds at least some bytes from the
    // pre-workload, but stop well before the segment fills (the source
    // is quiet — no concurrent writers — so we will not race past the
    // segment boundary).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut chunks_seen = 0;
    while std::time::Instant::now() < deadline {
        let apply_lsn = stream.dispatched_lsn();
        let next = tokio::time::timeout(
            Duration::from_secs(1),
            feed.next_chunk(StandbyStatus::collapsed(apply_lsn), &mut buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("source feed error: {e:#}"),
            Err(_) => continue,
        };
        stream
            .push(chunk.start_lsn, chunk.data, &mut metrics, &mut segs)
            .await
            .expect("push");
        chunks_seen += 1;
        // Once next_lsn has caught up to (or past) ident.xlogpos, the
        // source has no more to send for now; bail out.
        if stream.next_lsn() >= ident.xlogpos {
            break;
        }
        // Defensive cap: we never expect this in a quiet-source test
        // but if we somehow filled a segment, stop now to avoid a
        // misleading "no partial" assertion later.
        if chunks_seen >= 64 {
            break;
        }
    }
    assert!(
        chunks_seen > 0,
        "no WAL chunks received before shutdown drill",
    );

    let dispatched_at_close = stream.dispatched_lsn();
    let next_at_close = stream.next_lsn();
    assert!(
        next_at_close > dispatched_at_close,
        "current_buf must hold ≥1 byte for close() to produce a .partial \
         (dispatched={dispatched_at_close:#X}, next={next_at_close:#X})",
    );

    // Drive the shutdown path. Equivalent to the daemon's ctrl_c branch
    // calling stream.close(Some(&mut segs), &mut metrics).
    stream
        .close(Some(&mut segs), &mut metrics)
        .await
        .expect("close writes partial");

    // The partial segment file must exist under <name>.partial, and the
    // matching complete-segment path must NOT — otherwise shadow PG's
    // restore_command would treat it as a real segment.
    let mut partials: Vec<PathBuf> = vec![];
    for entry in std::fs::read_dir(&out_dir).unwrap() {
        let e = entry.unwrap();
        let n = e.file_name().to_string_lossy().into_owned();
        if n.ends_with(".partial") && !n.ends_with(".manifest.json.partial") {
            partials.push(e.path());
        }
    }
    assert_eq!(
        partials.len(),
        1,
        "expected exactly one .partial under {} after close(); got {:?}",
        out_dir.display(),
        partials,
    );
    let partial = &partials[0];
    let partial_name = partial.file_name().unwrap().to_string_lossy().into_owned();
    let segname_str = partial_name.strip_suffix(".partial").unwrap();
    assert_eq!(segname_str.len(), 24, "partial name shape: {partial_name}");
    assert!(
        !out_dir.join(segname_str).exists(),
        "complete-segment path leaked alongside partial: {segname_str}",
    );
    assert!(
        out_dir
            .join(format!("{segname_str}.partial.manifest.json"))
            .exists(),
        "partial manifest sidecar missing alongside {partial_name}",
    );
    // Partial is exactly one segment's worth, with the tail zero-padded
    // per `close()`'s contract.
    let on_disk = std::fs::read(partial).unwrap();
    assert_eq!(
        on_disk.len(),
        WAL_SEG_SIZE as usize,
        "partial must be padded to segment size",
    );
    drop(feed);

    // Resume: fresh SourceFeed + WalStream at the same aligned start
    // LSN. The contract is that --start-lsn at the segment boundary
    // works; the .partial bytes themselves aren't replayed.
    let mut feed2 = SourceFeed::connect(&pgcfg)
        .await
        .expect("resume feed connect")
        .with_status_interval(Duration::from_millis(500));
    let ident2 = feed2.identify_system().await.expect("IDENTIFY_SYSTEM #2");
    let mut stream2 = WalStream::new(ident2.timeline, WAL_SEG_SIZE, aligned).unwrap();
    feed2
        .start_physical_replication(None, aligned, ident2.timeline)
        .await
        .expect("START_REPLICATION resume");

    // Drive more WAL after resume so the source has fresh bytes to ship.
    let pump_sock = source.config().socket_dir.clone();
    let pump_port = source.config().port;
    let driver = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
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
                "INSERT INTO sd.t SELECT g+200, repeat('q', 80) FROM generate_series(1, 500) g; \
                 SELECT pg_switch_wal();",
            ])
            .output();
    });

    let resume_out = tmp.path().join("filtered_resume");
    let mut resume_metrics = CollectingRecordSink::default();
    let mut resume_segs = DirSegmentSink::new(resume_out.clone()).expect("open resume out dir");
    let mut resume_buf = Vec::with_capacity(64 * 1024);
    let deadline2 = std::time::Instant::now() + Duration::from_secs(15);
    let mut resume_segments = 0u64;
    let mut prev2 = stream2.dispatched_lsn();
    while resume_segments < 1 && std::time::Instant::now() < deadline2 {
        let apply_lsn = stream2.dispatched_lsn();
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed2.next_chunk(StandbyStatus::collapsed(apply_lsn), &mut resume_buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("resume feed error: {e:#}"),
            Err(_) => continue,
        };
        stream2
            .push(
                chunk.start_lsn,
                chunk.data,
                &mut resume_metrics,
                &mut resume_segs,
            )
            .await
            .expect("resume push");
        let now2 = stream2.dispatched_lsn();
        if now2 != prev2 {
            resume_segments += (now2 - prev2) / WAL_SEG_SIZE;
            prev2 = now2;
        }
    }
    let _ = driver.join();
    assert!(
        resume_segments >= 1,
        "no segments shipped after resume in 15s",
    );
    assert!(
        !resume_metrics.records.is_empty(),
        "resume RecordSink saw zero records",
    );
    eprintln!(
        "shutdown drill: partial={partial_name}, resume shipped {resume_segments} segment(s), {} records",
        resume_metrics.records.len(),
    );
}
