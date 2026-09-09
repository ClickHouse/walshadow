//! A full-page image of a user relation must not replay on the shadow.
//!
//! `XLOG_FPI` / `XLOG_FPI_FOR_HINT` ride the Xlog resource manager, which is
//! otherwise recovery plumbing the shadow needs unconditionally. Their payload
//! is an 8 KiB page of an arbitrary relation, so keeping them writes user pages
//! into the shadow's `base/` — and because redo extends a relation to whatever
//! block an image names, the shadow grows toward the source relation's whole
//! size rather than the volume of changes. `data_checksums = on` makes every
//! hint-bit set emit one, so a single scan of a freshly loaded table is enough.
//!
//! `wal_log_hints = on` produces the same records without needing `initdb -k`.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod h;

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::pin::Pin;
use std::process::Command;
use std::time::{Duration, Instant};

use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};
use walshadow::record::{Record, RecordSink, Route, SinkError, WAL_SEG_SIZE, rmgr_label};
use walshadow::schema::FIRST_NORMAL_OBJECT_ID;
use walshadow::segment_sink::DirSegmentSink;
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use walshadow::wal_stream::WalStream;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("source-data"), tmp.path().join("filtered"));
    cfg.port = h::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\nwal_level = logical").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
    writeln!(f, "autovacuum = off").unwrap();
    // Stands in for data_checksums = on: makes hint-bit sets WAL-logged
    writeln!(f, "wal_log_hints = on").unwrap();
    // VACUUM FULL checkpoints, which would recycle the slotless pump's segments
    writeln!(f, "wal_keep_size = 2GB").unwrap();
    writeln!(f, "fsync = off").unwrap();
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

#[derive(Default)]
struct ImageCensus {
    max_next_lsn: u64,
    user_images_to_shadow: u64,
    user_images_dropped: u64,
    catalog_images_to_shadow: u64,
    catalog_images_dropped: u64,
    leaked_by_op: BTreeMap<String, u64>,
}

impl ImageCensus {
    fn observe(&mut self, record: &Record<'_>) {
        self.max_next_lsn = self.max_next_lsn.max(record.next_lsn);
        for blk in &record.parsed.blocks {
            if !blk.header.has_image() {
                continue;
            }
            let rel = blk.header.location.rel.rel_node;
            if rel == 0 {
                continue;
            }
            let user = rel >= FIRST_NORMAL_OBJECT_ID;
            match (user, record.route) {
                (true, Route::ToShadow) => {
                    self.user_images_to_shadow += 1;
                    let h = &record.parsed.header;
                    *self
                        .leaked_by_op
                        .entry(format!(
                            "{}/{:#04X}",
                            rmgr_label(h.resource_manager_id),
                            h.info & 0xF0
                        ))
                        .or_default() += 1;
                }
                (true, Route::ToDecoder) => self.user_images_dropped += 1,
                (false, Route::ToShadow) => self.catalog_images_to_shadow += 1,
                (false, Route::ToDecoder) => self.catalog_images_dropped += 1,
            }
        }
    }
}

impl RecordSink for ImageCensus {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.observe(record);
            Ok(())
        })
    }
}

fn current_db_oid(sh: &Shadow) -> u32 {
    sh.psql_one("SELECT oid::int8 FROM pg_database WHERE datname = current_database()")
        .expect("db oid")
        .parse()
        .expect("integer")
}

fn wal_insert_lsn(sh: &Shadow) -> u64 {
    let s = sh
        .psql_one("SELECT pg_current_wal_insert_lsn()")
        .expect("lsn");
    walshadow::pg::parse_pg_lsn(&s).expect("parse lsn")
}

async fn attach(source: &Shadow) -> (SourceFeed, WalStream) {
    let cfg = source.config();
    let pgcfg = PgConfig {
        host: cfg.socket_dir.to_string_lossy().into_owned(),
        port: cfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "fpi-user-pages".into(),
        sslmode: SslMode::Disable,
        tls: TlsParams::default(),
    };
    let mut feed = SourceFeed::connect(&pgcfg)
        .await
        .expect("feed connect")
        .with_status_interval(Duration::from_millis(500));
    let ident = feed.identify_system().await.expect("IDENTIFY_SYSTEM");
    let aligned = WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE);
    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned).unwrap();
    stream.filter_mut().set_target_db(current_db_oid(source));
    {
        let sql_client = feed.sql_client().await.expect("sql client");
        stream
            .filter_mut()
            .tracker_mut()
            .seed_from_source(sql_client)
            .await
            .expect("seed_from_source");
        stream
            .filter_mut()
            .seed_observed_from_source(sql_client)
            .await
            .expect("seed observed-from xid");
    }
    feed.start_physical_replication(None, aligned, ident.timeline)
        .await
        .expect("START_REPLICATION");
    (feed, stream)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_page_images_never_reach_the_shadow() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp);
    source.initdb().expect("initdb");
    source.write_base_conf().expect("base conf");
    append_source_conf(&source);
    source.start().expect("start");
    let _stop = StopOnDrop { sh: &source };

    source
        .apply_schema_dump(
            "CREATE TABLE big (id bigint primary key, payload text);\n\
             INSERT INTO big SELECT g, repeat('x', 300) FROM generate_series(1, 40000) g;\n",
        )
        .expect("seed schema");

    let (mut feed, mut stream) = attach(&source).await;
    let mut segs = DirSegmentSink::new(tmp.path().join("out")).expect("out dir");
    let mut buf = Vec::with_capacity(64 * 1024);
    let mut census = ImageCensus::default();

    // Checkpoint first so the pages are clean: the next hint-bit set on each
    // is what emits the image. The seq scan sets them across the whole table,
    // and touching a catalog the same way keeps the positive control honest.
    let mut ddl = String::new();
    for i in 0..40 {
        ddl.push_str(&format!("CREATE TABLE churn_{i}(a int, b text);\n"));
    }
    ddl.push_str("CHECKPOINT;\n");
    source.apply_schema_dump(&ddl).expect("catalog churn");

    // Fresh backend, so relcache builds from the on-disk catalogs and sets
    // hint bits on the rows the DDL above just committed. Every page whose
    // last write predates that CHECKPOINT emits an image on first hint-bit set.
    //
    // VACUUM and VACUUM FULL are the paths seen re-materialising a relation on
    // a deployed shadow: the first freezes/prunes and sets the visibility map,
    // the second rewrites into a fresh relfilenode through log_newpage.
    source
        .apply_schema_dump(
            "SELECT count(*) FROM big;\n\
             SELECT count(*) FROM pg_class;\n\
             SELECT count(*) FROM pg_attribute;\n\
             UPDATE big SET payload = repeat('y', 300) WHERE id % 4 = 0;\n\
             DELETE FROM big WHERE id % 7 = 0;\n\
             CHECKPOINT;\n\
             VACUUM (FREEZE) big;\n\
             VACUUM FULL big;\n\
             SELECT pg_switch_wal();\n",
        )
        .expect("dirty hint bits and vacuum");
    let target = wal_insert_lsn(&source);

    let deadline = Instant::now() + Duration::from_secs(60);
    while census.max_next_lsn < target && Instant::now() < deadline {
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_event(StandbyStatus::collapsed(stream.dispatched_lsn()), &mut buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(SourceEvent::Wal(c))) => c,
            Ok(Ok(_)) => break,
            Ok(Err(e)) => panic!("source feed: {e:#}"),
            Err(_) => continue,
        };
        stream
            .push(chunk.start_lsn, chunk.data, &mut census, &mut segs)
            .await
            .expect("push");
    }
    assert!(
        census.max_next_lsn >= target,
        "drained to {:#X}, target {target:#X}",
        census.max_next_lsn,
    );

    // Vacuous unless the workload actually produced user page images
    assert!(
        census.user_images_dropped > 0,
        "workload produced no user page images; wal_log_hints ineffective?",
    );
    assert_eq!(
        census.user_images_to_shadow, 0,
        "user page images routed to the shadow: {} kept, {} dropped, by op {:?}",
        census.user_images_to_shadow, census.user_images_dropped, census.leaked_by_op,
    );
    assert!(
        census.catalog_images_to_shadow > 0,
        "workload produced no catalog page images, so the keep-side assertion \
         below would pass vacuously",
    );
    assert_eq!(
        census.catalog_images_dropped, 0,
        "catalog page images must still replay ({} dropped, {} kept)",
        census.catalog_images_dropped, census.catalog_images_to_shadow,
    );
    eprintln!(
        "user images: {} dropped / {} kept; catalog images: {} kept / {} dropped",
        census.user_images_dropped,
        census.user_images_to_shadow,
        census.catalog_images_to_shadow,
        census.catalog_images_dropped,
    );
}
