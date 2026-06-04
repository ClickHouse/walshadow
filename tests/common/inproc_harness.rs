//! Shared in-process scaffolding for inline DML/DDL drill tests.
//!
//! Mirrors the harness `pipeline_e2e.rs` builds inline: source PG +
//! basebackup-cloned shadow PG in standby mode, a CH server, the
//! walshadow WAL pipeline driven from the source's replication
//! protocol, and a workload driver. Test bodies wire schema +
//! mapping + workload + assertions; this module owns the bootstrap +
//! pump loop.
//!
//! Included via `#[path = "common/inproc_harness.rs"]` rather than
//! `tests/common/mod.rs` so cargo doesn't build it as a free-standing
//! test binary.
//!
//! Scope: in-process pipeline only. The daemon-binary-spawn drills
//! (bootstrap_*_ch, kill_restart, pgbench_acceptance) keep their
//! own `bootstrap_ch_fixture.rs`.

#![allow(dead_code)]

use std::fs;
use std::io::Write as _;
use std::net::TcpStream;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use std::sync::atomic::AtomicU64;
use tokio::sync::Mutex;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::SslMode;

use walshadow::ch_ddl::{DdlApplicator, DdlConfig};
use walshadow::ch_emitter::{
    ColumnMapping, CompressionChoice, EmitterConfig, EmitterStats, MappingHandle, NamespaceMapping,
    TableMapping,
};
use walshadow::pipeline::reorder::ReorderSink;
use walshadow::pipeline::{PipelineConfig, PipelineHandle};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::{ShadowCatalog, ShadowCatalogConfig, socket_conninfo};
use walshadow::source_feed::{SourceFeed, StandbyStatus};
use walshadow::wal_stream::{
    DirSegmentSink, MetricsRecordSink, Record, RecordSink, SinkError, WAL_SEG_SIZE, WalStream,
};
use walshadow::xact_buffer::{BufferingDecoderSink, SubxactTracker, XactBuffer, XactBufferConfig};

// ---------------------------------------------------------------------------
// Skip-gate probes
// ---------------------------------------------------------------------------

pub fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub fn pg_basebackup_available() -> bool {
    Command::new("pg_basebackup")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub fn clickhouse_available() -> bool {
    Command::new("clickhouse")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// PG fixture helpers
// ---------------------------------------------------------------------------

pub fn make_pg(tmp: &tempfile::TempDir, name: &str, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join(format!("{name}-data")),
        tmp.path().join(format!("{name}-filtered")),
    );
    cfg.port = port;
    cfg.socket_dir = tmp.path().join(format!("{name}-sock"));
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

/// Add `wal_level=logical` + `max_wal_senders` so basebackup +
/// START_REPLICATION can attach.
pub fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow inproc source overrides").unwrap();
    writeln!(f, "wal_level = logical").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
}

pub struct StopOnDrop<'a> {
    pub sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

pub fn pg_basebackup(source: &Shadow, dest: &Path) -> Result<()> {
    let cfg = source.config();
    let out = Command::new("pg_basebackup")
        .args([
            "-h",
            cfg.socket_dir.to_str().context("source sock not utf8")?,
            "-p",
            &cfg.port.to_string(),
            "-U",
            "postgres",
            "-D",
            dest.to_str().context("dest not utf8")?,
            "-X",
            "stream",
            "-c",
            "fast",
            "-w",
            "--no-sync",
        ])
        .output()
        .context("spawn pg_basebackup")?;
    if !out.status.success() {
        bail!(
            "pg_basebackup failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// Append shadow-side overrides to a basebackup-cloned data dir's conf.
/// Last-wins overrides the source's settings inherited via basebackup.
pub fn rewrite_for_shadow(data_dir: &Path, port: u16, socket_dir: &Path) -> Result<()> {
    let conf = data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
    writeln!(f, "\n# walshadow inproc shadow overrides")?;
    writeln!(f, "port = {port}")?;
    writeln!(f, "unix_socket_directories = '{}'", socket_dir.display())?;
    writeln!(f, "listen_addresses = ''")?;
    writeln!(f, "hot_standby = on")?;
    writeln!(f, "autovacuum = off")?;
    writeln!(f, "fsync = off")?;
    writeln!(f, "wal_retrieve_retry_interval = '100ms'")?;
    Ok(())
}

pub fn enable_recovery(data_dir: &Path, restore_from: &Path, walsender_port: u16) -> Result<()> {
    fs::write(data_dir.join("standby.signal"), b"")?;
    let conf = data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
    writeln!(f, "\n# walshadow inproc recovery")?;
    writeln!(
        f,
        "primary_conninfo = 'host=127.0.0.1 port={walsender_port} user=walshadow application_name=shadow sslmode=disable'",
    )?;
    writeln!(f, "restore_command = 'cp {}/%f %p'", restore_from.display())?;
    writeln!(f, "recovery_target_timeline = 'latest'")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ClickHouse server harness
// ---------------------------------------------------------------------------

pub struct ChServer {
    child: Child,
    pub port: u16,
    #[allow(dead_code)]
    tmp: tempfile::TempDir,
}

impl ChServer {
    pub fn spawn(tmp: tempfile::TempDir, tcp_port: u16, http_port: u16) -> Result<Self> {
        let data_dir = tmp.path().join("ch");
        fs::create_dir_all(&data_dir)?;
        let log_dir = tmp.path().join("ch-logs");
        fs::create_dir_all(&log_dir)?;
        // Default-profile users config that also enables CH `Time64`
        // (the bridge maps PG `time` to it; gated behind this
        // experimental setting in CH 25.x — production must enable it
        // server-side too). Replaces the built-in users config, so the
        // passwordless default user is restated here.
        let users_xml = tmp.path().join("users.xml");
        fs::write(
            &users_xml,
            "<clickhouse>\
                <profiles><default>\
                    <enable_time_time64_type>1</enable_time_time64_type>\
                </default></profiles>\
                <users><default>\
                    <password></password>\
                    <networks><ip>::/0</ip></networks>\
                    <profile>default</profile>\
                    <quota>default</quota>\
                    <access_management>1</access_management>\
                </default></users>\
                <quotas><default/></quotas>\
            </clickhouse>",
        )?;
        let child = Command::new("clickhouse")
            .args([
                "server",
                "--",
                &format!("--tcp_port={tcp_port}"),
                &format!("--http_port={http_port}"),
                &format!("--interserver_http_port={}", http_port + 1),
                "--mysql_port=",
                "--postgresql_port=",
                "--grpc_port=",
                "--prometheus.port=",
                "--listen_host=127.0.0.1",
                &format!("--path={}/", data_dir.display()),
                &format!("--logger.log={}/server.log", log_dir.display()),
                &format!("--logger.errorlog={}/error.log", log_dir.display()),
                "--logger.level=warning",
                &format!("--users_config={}", users_xml.display()),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .context("spawn clickhouse server")?;
        let s = Self {
            child,
            port: tcp_port,
            tmp,
        };
        s.wait_for_listen(Duration::from_secs(60))?;
        Ok(s)
    }

    fn wait_for_listen(&self, deadline: Duration) -> Result<()> {
        let start = Instant::now();
        let addr = format!("127.0.0.1:{}", self.port);
        while start.elapsed() < deadline {
            if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(200))
                .is_ok()
                && self.query("SELECT 1").is_ok()
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        bail!("clickhouse server failed to accept queries within {deadline:?}");
    }

    pub fn query(&self, sql: &str) -> Result<String> {
        let out = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.port.to_string(),
                "--query",
                sql,
            ])
            .output()
            .context("spawn clickhouse client")?;
        if !out.status.success() {
            bail!(
                "clickhouse query failed: {} (stderr={})",
                sql,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

impl Drop for ChServer {
    fn drop(&mut self) {
        let _ = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.port.to_string(),
                "--query",
                "SYSTEM SHUTDOWN",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        for _ in 0..50 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        let pgid = self.child.id() as i32;
        let _ = Command::new("kill")
            .args(["-KILL", &format!("-{pgid}")])
            .stderr(Stdio::null())
            .status();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Bootstrap orchestrator. Returns the source + shadow pair + the
// `ShadowStreamState` the walsender listener is hosting.
// ---------------------------------------------------------------------------

pub struct BootstrappedClusters {
    pub source: Shadow,
    pub shadow: Shadow,
    pub shadow_filter_dir: PathBuf,
}

pub async fn bootstrap_clusters(
    tmp: &tempfile::TempDir,
    schema_sql: &str,
    source_port: u16,
    shadow_port: u16,
    walsender_port: u16,
) -> (
    BootstrappedClusters,
    Arc<Mutex<walshadow::shadow_stream::ShadowStreamState>>,
) {
    let source = make_pg(tmp, "source", source_port);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    append_source_conf(&source);
    source.start().expect("start source");

    source
        .apply_schema_dump(schema_sql)
        .expect("bootstrap source schema");

    let shadow_data = tmp.path().join("shadow-data");
    pg_basebackup(&source, &shadow_data).expect("pg_basebackup source → shadow");

    source.psql_one("SELECT pg_switch_wal()").expect("rotate");

    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    rewrite_for_shadow(&shadow_data, shadow_port, &shadow_sock).expect("retarget conf");
    enable_recovery(&shadow_data, &shadow_filter_dir, walsender_port)
        .expect("standby.signal + restore_command");

    let sysid = source
        .psql_one("SELECT system_identifier::text FROM pg_control_system()")
        .expect("read source sysid");
    let xlogpos_str = source
        .psql_one("SELECT pg_current_wal_lsn()::text")
        .expect("read source xlogpos");
    let xlogpos = walshadow::shadow::parse_pg_lsn(&xlogpos_str).expect("parse xlogpos");
    let aligned = WalStream::align_down(xlogpos, WAL_SEG_SIZE);
    let shadow_stream_state = Arc::new(Mutex::new(
        walshadow::shadow_stream::ShadowStreamState::new(1, sysid, aligned, 64 * 1024 * 1024),
    ));
    let walsender_task = walshadow::shadow_stream::spawn_listener(
        walshadow::shadow_stream::WalSenderAddr::Tcp(
            format!("127.0.0.1:{walsender_port}").parse().unwrap(),
        ),
        shadow_stream_state.clone(),
        Duration::from_millis(5),
    )
    .await
    .expect("spawn walsender");
    std::mem::forget(walsender_task);

    let mut shadow_cfg = ShadowConfig::new(shadow_data.clone(), shadow_filter_dir.clone());
    shadow_cfg.port = shadow_port;
    shadow_cfg.socket_dir = shadow_sock;
    shadow_cfg.ctl_timeout = Duration::from_secs(60);
    let shadow = Shadow::new(shadow_cfg);
    if let Err(e) = shadow.start() {
        let log = fs::read_to_string(shadow_data.join("startup.log"))
            .unwrap_or_else(|_| "<no startup.log>".into());
        panic!("start shadow standby failed: {e}\nstartup.log:\n{log}");
    }
    assert!(
        shadow.is_in_recovery().expect("probe in-recovery"),
        "shadow must boot into recovery from the basebackup + standby.signal",
    );

    (
        BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    )
}

// ---------------------------------------------------------------------------
// Pipeline builder + pump loop. `build_pipeline` wires SourceFeed →
// WalStream → the parallel decode+insert pipeline (`src/pipeline`:
// reorder → decode pool → batcher → inserter pool, the same wiring
// `bin/stream.rs` stands up behind `--ch-config`) → DirSegmentSink against
// a pre-bootstrapped PG pair + CH server. `pump_segments` runs the inner
// loop until `segments_needed` segments ship or `deadline` elapses; tests
// then `pipeline.shutdown().await` to drain the tail before asserting CH
// state (the tail batches, so rows aren't durable until the drain).
// ---------------------------------------------------------------------------

pub struct TableMappingSpec {
    pub source_table: String,
    pub target_table: String,
    pub columns: Vec<ColumnMapping>,
}

pub struct BuildPipelineArgs<'a> {
    pub tmp: &'a tempfile::TempDir,
    pub source: &'a Shadow,
    pub shadow: &'a Shadow,
    pub shadow_filter_dir: &'a Path,
    pub shadow_stream_state: Arc<Mutex<walshadow::shadow_stream::ShadowStreamState>>,
    pub ch_database: &'a str,
    pub ch_tcp_port: u16,
    pub mappings: Vec<TableMappingSpec>,
    pub app_name: &'a str,
    /// DDL replication — when set, the pipeline wires a CH DDL applicator on a
    /// second TCP connection + subscribes the decoder to the catalog's
    /// schema-event channel. Auto-create namespaces flagged in
    /// `namespaces` get `CREATE TABLE` on first sight; per-table
    /// mappings still win as overrides.
    pub ddl: Option<DdlPipelineArgs>,
}

#[derive(Default)]
pub struct DdlPipelineArgs {
    pub namespaces: std::collections::HashMap<String, NamespaceMapping>,
    pub drop_table_strategy: Option<String>,
}

// ---------------------------------------------------------------------------
// `build_pipeline` wires the feed/catalog bootstrap and the `src/pipeline`
// fan-out (decode pool → batcher → inserter pool, reorder coordinator) — the
// same wiring `bin/stream.rs` stands up behind `--ch-config`.
//
// `TRUNCATE` rides the reorder barrier as a `HeapOp::Truncate` heap and so
// needs no extra wiring (the applicator is always built below). Schema-
// evolution DDL (ALTER/CREATE/DROP) flows as reorder `ordered_events`, which
// only surface when the decoder is subscribed to the catalog's schema-event
// channel — so `ddl: Some(..)` wires that subscription + the pg_class delete
// epoch + baseline seeding. `ddl: None` leaves the DML-only path untouched.
// ---------------------------------------------------------------------------

/// Record-sink chain feeding the parallel pipeline: `metrics → decoder
/// (heaps → xact buffer) → reorder (commit → dispatch to decode pool)`.
/// Clone of `bin/stream.rs`'s `DaemonSinks` with the reorder coordinator
/// as the drain half.
pub struct PipelineSinks {
    pub metrics: MetricsRecordSink,
    pub decoder: BufferingDecoderSink,
    pub reorder: ReorderSink,
}

impl RecordSink for PipelineSinks {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.metrics.on_record(record).await?;
            self.decoder.on_record(record).await?;
            self.reorder.on_record(record).await?;
            Ok(())
        })
    }
}

pub struct Pipeline {
    pub feed: SourceFeed,
    pub stream: WalStream,
    pub sinks: PipelineSinks,
    pub segment_sink: DirSegmentSink,
    pub xact_buffer: Arc<Mutex<XactBuffer>>,
    pub chunk_buf: Vec<u8>,
    pub handle: PipelineHandle,
    pub ack: Arc<AtomicU64>,
    /// Same `EmitterStats` Arc the daemon exports to Prometheus; lets tests
    /// assert the parallel path keeps the emitter counters live.
    pub stats: Arc<EmitterStats>,
}

impl Pipeline {
    /// Drop the sink chain (closing the reorder coordinator's job + row
    /// channels) and await the drain cascade: decoders finish → batcher
    /// final-flushes → inserters drain to `EndOfStream` → ack collector
    /// exits. Surfaces any pipeline-fatal error.
    pub async fn shutdown(self) -> std::result::Result<(), String> {
        let Pipeline { sinks, handle, .. } = self;
        drop(sinks);
        handle.join().await
    }
}

pub async fn build_pipeline(args: BuildPipelineArgs<'_>) -> Pipeline {
    build_pipeline_with(args, |_| {}).await
}

/// `build_pipeline` with final say over the emitter config (eg `[toast]`
/// mode); `tune` runs after mappings + DDL overrides are folded in, before
/// anything reads the config.
pub async fn build_pipeline_with(
    args: BuildPipelineArgs<'_>,
    tune: impl FnOnce(&mut EmitterConfig),
) -> Pipeline {
    let BuildPipelineArgs {
        tmp,
        source,
        shadow,
        shadow_filter_dir,
        shadow_stream_state,
        ch_database,
        ch_tcp_port,
        mappings,
        app_name,
        ddl,
    } = args;
    let scfg = source.config();
    let pgcfg = PgConfig {
        host: scfg.socket_dir.to_string_lossy().into_owned(),
        port: scfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: app_name.into(),
        sslmode: SslMode::Disable,
    };
    let mut feed = SourceFeed::connect(&pgcfg)
        .await
        .expect("source feed connect")
        .with_status_interval(Duration::from_millis(500));
    let ident = feed.identify_system().await.expect("IDENTIFY_SYSTEM");
    let aligned = WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE);
    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned).unwrap();
    stream.set_bytes_sink(Box::new(walshadow::shadow_stream::ShadowStreamSink::new(
        shadow_stream_state,
    )));

    {
        let sql_client = feed.sql_client().await.expect("sidecar sql client");
        stream
            .filter_mut()
            .tracker
            .seed_from_source(sql_client)
            .await
            .expect("seed_from_source");
    }

    let shadow_conninfo = socket_conninfo(
        shadow.config().socket_dir.to_str().unwrap(),
        shadow.config().port,
        "postgres",
        "postgres",
    );
    let cat_cfg = ShadowCatalogConfig {
        replay_timeout: Duration::from_secs(60),
        replay_poll: Duration::from_millis(50),
        ..Default::default()
    };
    let catalog = ShadowCatalog::connect(&shadow_conninfo, cat_cfg)
        .await
        .expect("connect shadow catalog");
    let catalog = Arc::new(Mutex::new(catalog));

    let inv_epoch = Arc::new(AtomicU64::new(0));
    stream
        .filter_mut()
        .tracker
        .set_invalidation_epoch(inv_epoch.clone());
    catalog.lock().await.set_invalidation_epoch(inv_epoch);

    // pg_class delete epoch gates the reorder coordinator's commit-boundary
    // DROP sweep. Only the DDL path needs it (mirrors bin/stream.rs).
    let del_epoch = Arc::new(AtomicU64::new(0));
    if ddl.is_some() {
        stream
            .filter_mut()
            .tracker
            .set_pg_class_delete_epoch(del_epoch.clone());
        catalog
            .lock()
            .await
            .set_pg_class_delete_epoch(del_epoch.clone());
    }

    feed.start_physical_replication(None, aligned, ident.timeline)
        .await
        .expect("START_REPLICATION");

    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();
    let xact_buf_cfg = XactBufferConfig {
        xact_buffer_max: walshadow::xact_buffer::DEFAULT_XACT_BUFFER_MAX,
        spill_dir,
    };
    let xact_buffer = XactBuffer::new(xact_buf_cfg).expect("xact buffer");
    let xact_buffer = Arc::new(Mutex::new(xact_buffer));

    let mut emitter_cfg = EmitterConfig {
        host: "127.0.0.1".into(),
        port: ch_tcp_port,
        database: ch_database.into(),
        compression: CompressionChoice::Lz4,
        ..Default::default()
    };
    for spec in mappings {
        emitter_cfg.tables.insert(
            spec.source_table,
            TableMapping {
                target: spec.target_table,
                columns: spec.columns,
            },
        );
    }

    // DDL wiring (mirrors bin/stream.rs --ch-config). Fold namespace /
    // drop-strategy overrides into the emitter config *before* the
    // applicator reads it, seed the schema-diff baseline for mapped
    // relations so a pinned table's first ALTER surfaces as Changed, and
    // subscribe the decoder to the catalog's schema-event channel so
    // ALTER/CREATE/DROP reach the reorder coordinator as `ordered_events`.
    let mut schema_events: Option<walshadow::xact_buffer::SchemaEventRx> = None;
    if let Some(d) = ddl.as_ref() {
        emitter_cfg.namespaces = d.namespaces.clone();
        if let Some(s) = &d.drop_table_strategy {
            emitter_cfg.drop_table_strategy = s.clone();
        }
        let names: Vec<String> = emitter_cfg.tables.keys().cloned().collect();
        catalog
            .lock()
            .await
            .seed_baseline(&names)
            .await
            .expect("seed schema-diff baseline");
        let rx = catalog.lock().await.subscribe();
        schema_events = Some(Arc::new(std::sync::Mutex::new(rx)));
    }

    tune(&mut emitter_cfg);

    // SIGHUP-reloadable mapping shared by the DDL applicator + decode pool.
    let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(emitter_cfg.tables.clone()));
    let ddl_cfg = DdlConfig::from_emitter(&emitter_cfg);
    let applicator = DdlApplicator::new(&emitter_cfg, ddl_cfg, mapping.clone())
        .await
        .expect("ddl applicator init");
    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let pcfg = PipelineConfig {
        emitter: emitter_cfg,
        decoder_pool_size: 2,
        inserter_pool_size: 2,
        catalog: catalog.clone(),
        mapping,
        oracle: None,
        applicator,
        buffer: xact_buffer.clone(),
        subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
        schema_events: schema_events.clone(),
        pg_class_delete_epoch: ddl.as_ref().map(|_| del_epoch.clone()),
        stats: stats.clone(),
        span_registry: None,
    };
    let (reorder, handle) = pcfg
        .spawn(emitter_ack.clone())
        .await
        .expect("spawn decode+insert pipeline");

    let mut decoder = BufferingDecoderSink::new(catalog, xact_buffer.clone());
    if let Some(rx) = &schema_events {
        decoder = decoder.with_schema_events(rx.clone());
    }
    let sinks = PipelineSinks {
        metrics: MetricsRecordSink::default(),
        decoder,
        reorder,
    };
    let segment_sink =
        DirSegmentSink::new(shadow_filter_dir.to_path_buf()).expect("open shadow filter dir");
    let chunk_buf = Vec::with_capacity(64 * 1024);

    Pipeline {
        feed,
        stream,
        sinks,
        segment_sink,
        xact_buffer,
        chunk_buf,
        handle,
        ack: emitter_ack,
        stats,
    }
}

/// Drive the WAL pump against the raw feed/stream/sink trio until
/// `segments_needed` segments have shipped or `deadline` elapses.
/// Returns the count shipped. Generic over the record sink so the
/// [`Pipeline`] drive and the bootstrap drills reuse one loop.
pub async fn pump_until<S: RecordSink>(
    feed: &mut SourceFeed,
    stream: &mut WalStream,
    record_sink: &mut S,
    segment_sink: &mut DirSegmentSink,
    chunk_buf: &mut Vec<u8>,
    segments_needed: u64,
    deadline: Duration,
) -> u64 {
    let end = Instant::now() + deadline;
    let mut segments_shipped = 0u64;
    let mut prev = stream.dispatched_lsn();
    while segments_shipped < segments_needed && Instant::now() < end {
        let apply_lsn = stream.dispatched_lsn();
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_chunk(StandbyStatus::collapsed(apply_lsn), chunk_buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("source feed: {e:#}"),
            Err(_) => continue,
        };
        stream
            .push(chunk.start_lsn, chunk.data, record_sink, segment_sink)
            .await
            .expect("push");
        let now = stream.dispatched_lsn();
        if now != prev {
            segments_shipped += (now - prev) / WAL_SEG_SIZE;
            prev = now;
        }
    }
    segments_shipped
}

/// Drive the WAL pump until `segments_needed` segments have shipped or
/// `deadline` elapses. Returns the count shipped.
pub async fn pump_segments(
    pipeline: &mut Pipeline,
    segments_needed: u64,
    deadline: Duration,
) -> u64 {
    pump_until(
        &mut pipeline.feed,
        &mut pipeline.stream,
        &mut pipeline.sinks,
        &mut pipeline.segment_sink,
        &mut pipeline.chunk_buf,
        segments_needed,
        deadline,
    )
    .await
}

/// Driver thread that fires a sequence of `-c` statements at the source
/// after a brief delay, so the test's `pump_segments` is already
/// listening when commits land.
///
/// Each `&str` becomes its own `-c` argument; psql treats each as an
/// autocommit xact, ensuring the COMMIT record lands in the same
/// segment as its heap records.
pub fn spawn_workload(source: &Shadow, statements: Vec<String>) -> std::thread::JoinHandle<()> {
    let sock = source.config().socket_dir.clone();
    let port = source.config().port;
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        let mut args: Vec<String> = vec![
            "-h".into(),
            sock.to_str().unwrap().to_string(),
            "-p".into(),
            port.to_string(),
            "-U".into(),
            "postgres".into(),
            "-d".into(),
            "postgres".into(),
            "-v".into(),
            "ON_ERROR_STOP=1".into(),
        ];
        for stmt in statements {
            args.push("-c".into());
            args.push(stmt);
        }
        let _ = Command::new("psql").args(&args).output();
    })
}
