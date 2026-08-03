//! Check that maintenance traffic does not create catalog boundaries
//!
//! Run with `--nocapture` to print boundary details

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod h;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write as _;
use std::pin::Pin;
use std::process::Command;
use std::time::{Duration, Instant};

use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};
use walshadow::boundary_hold::{BoundaryGateConfig, CatalogBoundaryGate};
use walshadow::record::{Record, RecordSink, Route, SinkError, WAL_SEG_SIZE, rmgr_label};
use walshadow::schema::FIRST_NORMAL_OBJECT_ID;
use walshadow::segment_sink::DirSegmentSink;
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_stream::ShadowStreamSink;
use walshadow::source_feed::{SourceFeed, StandbyStatus};
use walshadow::wal_stream::WalStream;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_source(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("source-data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

/// Set WAL and vacuum options for controlled test workloads
fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# vacuum census overrides").unwrap();
    writeln!(f, "wal_level = logical").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
    writeln!(f, "autovacuum = off").unwrap();
    writeln!(f, "fsync = off").unwrap();
    writeln!(f, "full_page_writes = off").unwrap();
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

/// Boundary observed by census
#[derive(Debug)]
struct BoundaryRow {
    lsn: u64,
    kind: String,
    /// Catalog relations written by transaction tree
    rels: BTreeSet<u32>,
    capture_all: bool,
    /// Relation oids selected for recapture
    oids: Vec<u32>,
}

/// Count filter decisions and boundary sources
#[derive(Default)]
struct Census {
    records: u64,
    to_shadow: u64,
    to_decoder: u64,
    /// Catalog writes by filenode and WAL operation
    catalog_writes: BTreeMap<(u32, String), u64>,
    /// Catalog writes grouped by transaction
    dirty: BTreeMap<u32, BTreeSet<u32>>,
    boundaries: Vec<BoundaryRow>,
    max_next_lsn: u64,
}

impl Census {
    fn observe(&mut self, record: &Record<'_>) {
        self.records += 1;
        self.max_next_lsn = self.max_next_lsn.max(record.next_lsn);
        match record.route {
            Route::ToShadow => self.to_shadow += 1,
            Route::ToDecoder => self.to_decoder += 1,
        }
        let xid = record.parsed.header.xact_id;
        let op = format!(
            "{}/{:#04X}",
            rmgr_label(record.parsed.header.resource_manager_id),
            record.parsed.header.info & 0xF0,
        );
        for blk in &record.parsed.blocks {
            let rel = blk.header.location.rel.rel_node;
            if rel == 0 || rel >= FIRST_NORMAL_OBJECT_ID {
                continue;
            }
            *self.catalog_writes.entry((rel, op.clone())).or_default() += 1;
            if xid != 0 {
                self.dirty.entry(xid).or_default().insert(rel);
            }
        }
        if let Some(info) = &record.boundary_info {
            let mut rels = self.dirty.remove(&xid).unwrap_or_default();
            for member in &info.members {
                if let Some(sub) = self.dirty.remove(member) {
                    rels.extend(sub);
                }
            }
            self.boundaries.push(BoundaryRow {
                lsn: record.source_lsn,
                kind: format!("{:?}", info.kind),
                rels,
                capture_all: info.capture_all,
                oids: info.oids.iter().map(|a| a.oid).collect(),
            });
        }
    }

    fn report(&self, phase: &str, names: &Names) {
        let name_of = |rel: u32| names.filenode(rel);
        println!(
            "\n=== {phase}: {} records ({} to shadow, {} to decoder), {} boundaries",
            self.records,
            self.to_shadow,
            self.to_decoder,
            self.boundaries.len(),
        );
        let mut by_count: Vec<_> = self.catalog_writes.iter().collect();
        by_count.sort_by_key(|((rel, op), count)| (std::cmp::Reverse(**count), *rel, op.clone()));
        for ((rel, op), count) in by_count {
            println!("    {count:6}  {} {op}", name_of(*rel));
        }
        for b in &self.boundaries {
            let rels: Vec<String> = b.rels.iter().map(|r| name_of(*r)).collect();
            let oids: Vec<String> = b.oids.iter().map(|o| names.oid(*o)).collect();
            println!(
                "    boundary {:#X} {} capture_all={} recaptures [{}] via writes to [{}]",
                b.lsn,
                b.kind,
                b.capture_all,
                oids.join(", "),
                rels.join(", "),
            );
        }
    }

    /// Catalog relations that dirtied at least one boundary
    fn boundary_rels(&self) -> BTreeSet<u32> {
        self.boundaries
            .iter()
            .flat_map(|b| b.rels.clone())
            .collect()
    }
}

impl RecordSink for Census {
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

/// Report how far sink has consumed
trait Drained {
    fn max_next_lsn(&self) -> u64;
}

impl Drained for Census {
    fn max_next_lsn(&self) -> u64 {
        self.max_next_lsn
    }
}

/// Census with publication holds enabled
struct HoldingCensus {
    census: Census,
    gate: CatalogBoundaryGate,
    holds: u64,
    held: Duration,
}

impl Drained for HoldingCensus {
    fn max_next_lsn(&self) -> u64 {
        self.census.max_next_lsn
    }
}

impl RecordSink for HoldingCensus {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.census.observe(record);
            if record.catalog_boundary {
                let parked = Instant::now();
                self.gate
                    .hold(record.source_lsn, record.next_lsn, || true)
                    .await?;
                self.holds += 1;
                self.held += parked.elapsed();
            }
            Ok(())
        })
    }
}

/// Resolve relation names from filenodes and oids
struct Names {
    by_filenode: BTreeMap<u32, String>,
    by_oid: BTreeMap<u32, String>,
}

impl Names {
    fn load(sh: &Shadow) -> Self {
        let rows = sh
            .psql_one(
                "SELECT string_agg(pg_relation_filenode(oid)::text || ' ' || oid::text \
                 || ' ' || relname, E'\\n') \
                 FROM pg_class WHERE pg_relation_filenode(oid) IS NOT NULL",
            )
            .expect("relation map");
        let mut by_filenode = BTreeMap::new();
        let mut by_oid = BTreeMap::new();
        for line in rows.lines() {
            let mut parts = line.splitn(3, ' ');
            let (Some(node), Some(oid), Some(name)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            by_filenode.insert(node.parse().expect("filenode"), name.to_string());
            by_oid.insert(oid.parse().expect("oid"), name.to_string());
        }
        Self {
            by_filenode,
            by_oid,
        }
    }

    fn filenode(&self, node: u32) -> String {
        self.by_filenode
            .get(&node)
            .cloned()
            .unwrap_or_else(|| format!("filenode {node}"))
    }

    fn oid(&self, oid: u32) -> String {
        self.by_oid
            .get(&oid)
            .cloned()
            .unwrap_or_else(|| format!("oid {oid}"))
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

/// Run workload and pump its WAL into sink
async fn pump_phase<S: RecordSink + Drained + Send>(
    sh: &Shadow,
    feed: &mut SourceFeed,
    stream: &mut WalStream,
    segs: &mut DirSegmentSink,
    buf: &mut Vec<u8>,
    sink: &mut S,
    sql: &str,
) -> Duration {
    sh.apply_schema_dump(sql).expect("workload");
    sh.psql_one("INSERT INTO marker(at) VALUES (now())")
        .expect("marker");
    let target = wal_insert_lsn(sh);

    let started = Instant::now();
    let deadline = started + Duration::from_secs(60);
    while sink.max_next_lsn() < target && Instant::now() < deadline {
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_chunk(StandbyStatus::collapsed(stream.dispatched_lsn()), buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("source feed: {e:#}"),
            Err(_) => continue,
        };
        stream
            .push(chunk.start_lsn, chunk.data, sink, segs)
            .await
            .expect("push");
    }
    let elapsed = started.elapsed();
    assert!(
        sink.max_next_lsn() >= target,
        "phase drained to {:#X}, target {target:#X}",
        sink.max_next_lsn(),
    );
    elapsed
}

/// Run pump phase with a new census
async fn phase(
    sh: &Shadow,
    feed: &mut SourceFeed,
    stream: &mut WalStream,
    segs: &mut DirSegmentSink,
    buf: &mut Vec<u8>,
    sql: &str,
) -> Census {
    let mut census = Census::default();
    pump_phase(sh, feed, stream, segs, buf, &mut census, sql).await;
    census
}

/// Attach replication feed and filter to source
async fn attach(source: &Shadow, app_name: &str) -> (SourceFeed, WalStream) {
    let cfg = source.config();
    let pgcfg = PgConfig {
        host: cfg.socket_dir.to_string_lossy().into_owned(),
        port: cfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: app_name.into(),
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

/// Verify maintenance workloads do not create boundaries
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_traffic_costs_no_catalog_boundary() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, h::PG_SOURCE_PORT);
    source.initdb().expect("initdb");
    source.write_base_conf().expect("base conf");
    append_source_conf(&source);
    source.start().expect("start");
    let _stop = StopOnDrop { sh: &source };

    source
        .apply_schema_dump(
            "CREATE TABLE churn (id bigint primary key, payload text);\n\
             CREATE TABLE marker (at timestamptz);\n\
             INSERT INTO churn SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g;\n\
             DELETE FROM churn WHERE id % 3 = 0;\n",
        )
        .expect("seed schema");

    let names = Names::load(&source);
    let (mut feed, mut stream) = attach(&source, "vacuum-census").await;
    let mut segs = DirSegmentSink::new(tmp.path().join("out")).expect("out dir");
    let mut buf = Vec::with_capacity(64 * 1024);

    // Drain schema setup before measurement
    phase(
        &source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        "SELECT 1;\n",
    )
    .await;

    let dml = phase(
        &source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        "UPDATE churn SET payload = repeat('y', 200) WHERE id % 7 = 0;\n\
         INSERT INTO churn SELECT g, repeat('z', 200) FROM generate_series(20001, 22000) g;\n",
    )
    .await;
    dml.report("dml only", &names);

    let analyze = phase(
        &source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        "ANALYZE churn;\n",
    )
    .await;
    analyze.report("analyze", &names);

    let vacuum = phase(
        &source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        "VACUUM churn;\n",
    )
    .await;
    vacuum.report("vacuum", &names);

    let vacuum_analyze = phase(
        &source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        "VACUUM (ANALYZE) churn;\n",
    )
    .await;
    vacuum_analyze.report("vacuum analyze", &names);

    let name_of = |rel: &u32| names.filenode(*rel);
    for (phase_name, census) in [
        ("dml only", &dml),
        ("analyze", &analyze),
        ("vacuum", &vacuum),
        ("vacuum analyze", &vacuum_analyze),
    ] {
        let rels: Vec<String> = census.boundary_rels().iter().map(name_of).collect();
        assert!(
            census.boundaries.is_empty(),
            "{phase_name}: {} catalog boundaries, dirtied by [{}] — each one stalls the pump",
            census.boundaries.len(),
            rels.join(", "),
        );
    }
}

/// Verify maintenance workloads do not park a live pump
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_traffic_parks_the_pump_for_nothing() {
    if !h::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !h::pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let walsender_port = h::reserve_port();
    let (clusters, shadow_state) = h::bootstrap_clusters(
        &tmp,
        "CREATE TABLE churn (id bigint primary key, payload text);\n\
         CREATE TABLE marker (at timestamptz);\n\
         INSERT INTO churn SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g;\n\
         DELETE FROM churn WHERE id % 3 = 0;\n",
        h::PG_SOURCE_PORT,
        h::PG_SHADOW_PORT,
        walsender_port,
    )
    .await;
    let source = &clusters.source;
    let _stop_source = StopOnDrop { sh: source };
    let _stop_shadow = StopOnDrop {
        sh: &clusters.shadow,
    };
    // Disable background maintenance during controlled phases
    source
        .apply_schema_dump("ALTER SYSTEM SET autovacuum = off;\nSELECT pg_reload_conf();\n")
        .expect("autovacuum off");

    let (mut feed, mut stream) = attach(source, "vacuum-hold").await;
    stream.set_bytes_sink(Box::new(ShadowStreamSink::new(shadow_state.clone())));
    let mut segs = DirSegmentSink::new(clusters.shadow_filter_dir.clone()).expect("filter dir");
    let mut buf = Vec::with_capacity(64 * 1024);
    let mut sink = HoldingCensus {
        census: Census::default(),
        gate: CatalogBoundaryGate::new(shadow_state, BoundaryGateConfig::default()),
        holds: 0,
        held: Duration::ZERO,
    };

    let report = |name: &str, sink: &mut HoldingCensus, elapsed: Duration| {
        let (records, holds, held) = (sink.census.records, sink.holds, sink.held);
        println!(
            "{name:>16}: {records:6} records in {elapsed:>10.2?}, \
             {holds} holds costing {held:.2?}",
        );
        sink.census = Census::default();
        sink.holds = 0;
        sink.held = Duration::ZERO;
        (holds, held)
    };

    let elapsed = pump_phase(
        source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        &mut sink,
        "SELECT 1;\n",
    )
    .await;
    report("bootstrap wal", &mut sink, elapsed);

    let elapsed = pump_phase(
        source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        &mut sink,
        "UPDATE churn SET payload = repeat('y', 200) WHERE id % 7 = 0;\n\
         INSERT INTO churn SELECT g, repeat('z', 200) FROM generate_series(20001, 22000) g;\n",
    )
    .await;
    let (dml_holds, _) = report("dml only", &mut sink, elapsed);

    let elapsed = pump_phase(
        source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        &mut sink,
        "ANALYZE churn;\nVACUUM (ANALYZE) churn;\nANALYZE;\n",
    )
    .await;
    let (maintenance_holds, maintenance_held) = report("maintenance", &mut sink, elapsed);

    // Use DDL to confirm boundary holds still work
    let elapsed = pump_phase(
        source,
        &mut feed,
        &mut stream,
        &mut segs,
        &mut buf,
        &mut sink,
        "ALTER TABLE churn ADD COLUMN c int;\n",
    )
    .await;
    let (ddl_holds, ddl_held) = report("add column", &mut sink, elapsed);

    assert_eq!(dml_holds, 0, "DML alone must not park the pump");
    assert_eq!(
        maintenance_holds, 0,
        "VACUUM / ANALYZE parked the pump {maintenance_holds} times for {maintenance_held:.2?}",
    );
    assert!(
        ddl_holds > 0,
        "ADD COLUMN must still park: the gate is what proves the census above",
    );
    println!(
        "one publication hold costs {:.2?}",
        ddl_held / ddl_holds as u32
    );
}
