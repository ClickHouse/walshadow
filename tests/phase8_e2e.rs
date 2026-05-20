//! Phase 8 — end-to-end DDL drill: source PG → walshadow filter →
//! shadow PG (recovery via `restore_command`, bootstrapped through
//! `pg_basebackup`) → heap decoder → xact buffer → CH-Native emitter →
//! spawned `clickhouse server`.
//!
//! Skipped silently when `initdb`, `pg_basebackup`, or the `clickhouse`
//! multitool isn't on `$PATH`.
//!
//! Workload for v1: pre-created table with `REPLICA IDENTITY FULL`,
//! mixed INSERT/UPDATE/DELETE under autocommit. `ALTER TABLE ADD
//! COLUMN` + `DROP TABLE` follow once schema-evolution handling on
//! the emitter side has its own targeted coverage; the drill here is
//! the meat of the heap → CH wire.
//!
//! Why `pg_basebackup` and not `apply_schema_dump`: shadow PG's
//! `pg_relation_filenode(oid)` lookup needs the same oids/filenodes
//! the source assigned, otherwise the decoder's catalog probe misses
//! every WAL record's relation. Schema-dump rebootstrap would create
//! fresh oids on shadow; only a `pg_basebackup` (or the BASE_BACKUP
//! primitive wal-rs already vendors) preserves cluster identity.

use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::Mutex;
use wal_rs::pg::replication::conn::PgConfig;
use wal_rs::pg::replication::tls::SslMode;
use walshadow::ch_emitter::{
    ColumnMapping, CompressionChoice, Emitter, EmitterConfig, EmitterObserver, TableMapping,
};
use walshadow::decoder_sink::TupleObserver;
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::{ShadowCatalog, ShadowCatalogConfig, socket_conninfo};
use walshadow::source_feed::{SourceFeed, StandbyStatus};
use walshadow::wal_stream::{
    DirSegmentSink, MetricsRecordSink, Record, RecordSink, SinkError, WAL_SEG_SIZE, WalStream,
};
use walshadow::xact_buffer::{BufferingDecoderSink, XactBuffer, XactBufferConfig, XactRecordSink};

// Non-overlapping ports — Phase 8 tests live in the 56100-range; pick
// one slot per (cluster, ch-server) per test so a leftover from a
// crashed prior run doesn't shadow the next one's port binding.
const SOURCE_PORT: u16 = 17101;
const SHADOW_PORT: u16 = 17102;
const CH_TCP_PORT: u16 = 17109;
const CH_HTTP_PORT: u16 = 17110;
// Far enough from CH ports that CH's auto-derived sidecar ports
// (tcp_port_secure, interserver, etc.) don't accidentally bind it.
const WALSENDER_PORT: u16 = 17150;
// Schema-evolution drill ports.
const SOURCE_PORT_S: u16 = 17221;
const SHADOW_PORT_S: u16 = 17222;
const CH_TCP_PORT_S: u16 = 17229;
const CH_HTTP_PORT_S: u16 = 17230;
const WALSENDER_PORT_S: u16 = 17280;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn pg_basebackup_available() -> bool {
    Command::new("pg_basebackup")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn clickhouse_available() -> bool {
    Command::new("clickhouse")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn make_pg(tmp: &tempfile::TempDir, name: &str, port: u16) -> Shadow {
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

// Source needs wal_level=logical (PLAN.md §4) + max_wal_senders so
// pg_basebackup + the daemon's START_REPLICATION can attach.
fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow Phase 8 source overrides").unwrap();
    writeln!(f, "wal_level = logical").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

fn pg_basebackup(source: &Shadow, dest: &Path) -> Result<()> {
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

// Append the bare minimum to retarget a `pg_basebackup`-cloned data
// dir at a new port + socket dir; PG honours last-wins for duplicate
// keys in postgresql.conf, so the source-side values from basebackup
// stay intact in the file but are overridden by what we append here.
fn rewrite_for_shadow(data_dir: &Path, port: u16, socket_dir: &Path) -> Result<()> {
    let conf = data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
    writeln!(f, "\n# walshadow Phase 8 shadow overrides")?;
    writeln!(f, "port = {port}")?;
    writeln!(f, "unix_socket_directories = '{}'", socket_dir.display())?;
    writeln!(f, "listen_addresses = ''")?;
    writeln!(f, "hot_standby = on")?;
    writeln!(f, "autovacuum = off")?;
    writeln!(f, "fsync = off")?;
    // Default is 5 s — far too slow for a test that ships a segment
    // and expects shadow to ingest it before the decoder's
    // `relation_at` replay-LSN gate times out.
    writeln!(f, "wal_retrieve_retry_interval = '100ms'")?;
    // Leave max_wal_senders + wal_level inherited from source: PG
    // refuses to start a standby whose values are lower than the
    // primary's, and the basebackup-cloned conf already matches.
    Ok(())
}

fn enable_recovery(data_dir: &Path, restore_from: &Path, walsender_port: u16) -> Result<()> {
    fs::write(data_dir.join("standby.signal"), b"")?;
    let conf = data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
    writeln!(f, "\n# walshadow recovery")?;
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

struct ChServer {
    child: Child,
    port: u16,
    #[allow(dead_code)]
    tmp: tempfile::TempDir,
}

impl ChServer {
    fn spawn(tmp: tempfile::TempDir, tcp_port: u16, http_port: u16) -> Result<Self> {
        let data_dir = tmp.path().join("ch");
        fs::create_dir_all(&data_dir)?;
        let log_dir = tmp.path().join("ch-logs");
        fs::create_dir_all(&log_dir)?;
        // CH server's embedded config defaults to listening on
        // mysql_port (9004), postgresql_port (9005), grpc_port (9100),
        // prometheus.port (9363), interserver_http_port (9009) — all
        // unrelated to the Native TCP path the test uses. Two test
        // runs sharing a host can collide on those defaults; override
        // the listeners we DON'T use to fresh ports (or to remove
        // them — CH accepts `--<port>=` empty-string as "don't bind").
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
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            // CH server forks a watchdog parent + a worker child. Put
            // both in a fresh process group so Drop can SIGTERM the
            // whole tree — `child.kill()` only signals the immediate
            // pid and would orphan the worker.
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

    fn query(&self, sql: &str) -> Result<String> {
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
        // `SYSTEM SHUTDOWN` is CH's documented graceful-exit path
        // (https://clickhouse.com/docs/sql-reference/statements/system#shutdown).
        // The watchdog + worker pair both wind down on receipt, and
        // the immediate child reaped via `try_wait` reflects the
        // whole-tree exit. Avoids the process-group SIGTERM dance,
        // which is fragile when the watchdog has already exited
        // (defunct) and `kill -TERM -<pgid>` can't reliably find a
        // live recipient.
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
        // Wait up to ~5 s for the watchdog + worker to wind down.
        for _ in 0..50 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        // Fallback: hard kill the process group if `SYSTEM SHUTDOWN`
        // did not take effect. `process_group(0)` at spawn time put
        // both watchdog + worker in the same group; signal -<pgid>
        // hits the whole tree.
        let pgid = self.child.id() as i32;
        let _ = Command::new("kill")
            .args(["-KILL", &format!("-{pgid}")])
            .stderr(Stdio::null())
            .status();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Daemon sink chain (clone of bin/stream.rs::DaemonSinks, kept inline so
// the test owns the dispatch order without re-exporting daemon-private
// types).
// ---------------------------------------------------------------------------

struct DaemonSinks {
    metrics: MetricsRecordSink,
    decoder: BufferingDecoderSink,
    xact_drain: XactRecordSink<Box<dyn TupleObserver>>,
}

impl RecordSink for DaemonSinks {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<(), SinkError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.metrics.on_record(record).await?;
            self.decoder.on_record(record).await?;
            self.xact_drain.on_record(record).await?;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Bootstrap helper — shared by every Phase 8 test. Builds a source PG
// + a basebackup-cloned shadow PG in standby mode, returning the two
// `Shadow` wrappers plus the directory the shadow's `restore_command`
// reads from (where the daemon's `DirSegmentSink` should write).
// ---------------------------------------------------------------------------

struct BootstrappedClusters {
    source: Shadow,
    shadow: Shadow,
    shadow_filter_dir: PathBuf,
}

async fn bootstrap_clusters(
    tmp: &tempfile::TempDir,
    schema_sql: &str,
    source_port: u16,
    shadow_port: u16,
    walsender_port: u16,
) -> (
    BootstrappedClusters,
    Arc<Mutex<walshadow::shadow_stream::ShadowStreamState>>,
) {
    // 1. Source PG — wal_level=logical so UPDATE/DELETE WAL records
    // carry the old-tuple bytes the heap decoder expects.
    let source = make_pg(tmp, "source", source_port);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    append_source_conf(&source);
    source.start().expect("start source");

    // 2. Apply the test's initial schema BEFORE pg_basebackup so the
    // basebackup snapshot already carries the pg_class / pg_attribute
    // rows the decoder needs for `pg_relation_filenode(oid)` lookups
    // on shadow — schema-dump-after-basebackup would create fresh
    // oids that don't match the source's WAL.
    source
        .apply_schema_dump(schema_sql)
        .expect("bootstrap source schema");

    // 3. Clone source cluster into shadow's data dir.
    let shadow_data = tmp.path().join("shadow-data");
    pg_basebackup(&source, &shadow_data).expect("pg_basebackup source → shadow");

    // 4. Force a segment rotation on source so basebackup's stop LSN
    // doesn't straddle a not-yet-shipped segment boundary. The
    // daemon's START_REPLICATION resumes at the source's *current*
    // xlogpos (aligned down) and we need the segment containing that
    // LSN to be fresh-bytes-only — local pg_wal/ would otherwise
    // shadow-replay a stale frozen copy and skip the post-basebackup
    // workload tail.
    source.psql_one("SELECT pg_switch_wal()").expect("rotate");

    // 5. Retarget the cloned data dir at shadow's port + socket, drop
    // a standby.signal pointing at the filter output dir.
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    rewrite_for_shadow(&shadow_data, shadow_port, &shadow_sock).expect("retarget conf");
    enable_recovery(&shadow_data, &shadow_filter_dir, walsender_port)
        .expect("standby.signal + restore_command");

    // 6. Spawn the walsender BEFORE shadow.start() so shadow's
    // walreceiver hits an already-listening socket on its first
    // connection attempt. PG's "invalid magic" check on pg_wal/seg3
    // races the walreceiver bytes write — losing that race
    // terminates the walreceiver and lets restore_command (which
    // also has nothing yet) starve the recovery.
    //
    // Identity is read from source via psql so we can stamp the
    // ShadowStreamState's `system_identifier` before any walreceiver
    // connects — IDENTIFY_SYSTEM round-trip must match the source's
    // basebackup-derived sysid or walreceiver bails immediately.
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
    let _walsender_task = walshadow::shadow_stream::spawn_listener(
        walshadow::shadow_stream::WalSenderAddr::Tcp(
            format!("127.0.0.1:{walsender_port}").parse().unwrap(),
        ),
        shadow_stream_state.clone(),
        Duration::from_millis(5),
    )
    .await
    .expect("spawn walsender");
    // Detach: caller test holds the state Arc; listener task lives
    // for as long as the runtime.
    std::mem::forget(_walsender_task);

    // 7. Wrap the populated data dir in a Shadow for lifecycle control.
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
// The drill.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_insert_update_delete_replicates_to_clickhouse() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }
    if !clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();

    // 1-6. Source + shadow PG pair, basebackup-bootstrapped. The
    // initial schema lives on source before basebackup so shadow
    // inherits the same oids / filenodes — the decoder's
    // `pg_relation_filenode(oid)` probes on shadow then resolve every
    // WAL record's relation.
    let (
        BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA s8;\n\
         CREATE TABLE s8.t (id bigint PRIMARY KEY, payload text);\n\
         ALTER TABLE s8.t REPLICA IDENTITY FULL;\n",
        SOURCE_PORT,
        SHADOW_PORT,
        WALSENDER_PORT,
    )
    .await;
    let _src_stop = StopOnDrop { sh: &source };
    let _shd_stop = StopOnDrop { sh: &shadow };

    // 7. CH server + destination table.
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = ChServer::spawn(ch_tmp, CH_TCP_PORT, CH_HTTP_PORT).expect("spawn ch");
    // `IF NOT EXISTS` + `OR REPLACE` keep the test idempotent against a
    // CH server leaked from an earlier panic — we recreate the data
    // dir per spawn via tempdir, but a stray daemon owning the port
    // would otherwise trip the next run on stale schema.
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.s8_t (\
            id Int64,\
            payload Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    // 8. SourceFeed: connect to source's replication protocol.
    let scfg = source.config();
    let pgcfg = PgConfig {
        host: scfg.socket_dir.to_string_lossy().into_owned(),
        port: scfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-phase8".into(),
        sslmode: SslMode::Disable,
    };
    let mut feed = SourceFeed::connect(&pgcfg)
        .await
        .expect("source feed connect")
        .with_status_interval(Duration::from_millis(500));
    let ident = feed.identify_system().await.expect("IDENTIFY_SYSTEM");
    let aligned = WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE);
    let mut stream = WalStream::new(ident.timeline, WAL_SEG_SIZE, aligned).unwrap();
    // Walsender already bound by bootstrap_clusters before shadow.start();
    // install the sink that bridges WalStream → that listener task.
    stream.set_bytes_sink(Box::new(walshadow::shadow_stream::ShadowStreamSink::new(
        shadow_stream_state.clone(),
    )));

    // 9. Seed catalog tracker from source pg_class so pre-attach filenode
    // rotations don't bite us.
    {
        let sql_client = feed.sql_client().await.expect("sidecar sql client");
        stream
            .filter_mut()
            .tracker
            .seed_from_source(sql_client)
            .await
            .expect("seed_from_source");
    }

    // 10. Shadow catalog — gates on shadow's pg_last_wal_replay_lsn,
    // so the decoder's relation_at calls block until shadow has caught
    // up past the WAL record's LSN.
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

    let inv_epoch = Arc::new(std::sync::atomic::AtomicU64::new(0));
    stream
        .filter_mut()
        .tracker
        .set_invalidation_epoch(inv_epoch.clone());
    catalog.lock().await.set_invalidation_epoch(inv_epoch);

    feed.start_physical_replication(None, aligned, ident.timeline)
        .await
        .expect("START_REPLICATION");

    // 11. Xact buffer + emitter.
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
        port: CH_TCP_PORT,
        database: "walshadow_test".into(),
        compression: CompressionChoice::Lz4,
        ..Default::default()
    };
    emitter_cfg.tables.insert(
        "s8.t".into(),
        TableMapping {
            target: "walshadow_test.s8_t".into(),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int64".into(),
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "payload".into(),
                    target_type: "Nullable(String)".into(),
                },
            ],
        },
    );

    let tcp = TcpStream::connect(("127.0.0.1", CH_TCP_PORT)).expect("tcp connect ch");
    tcp.set_nodelay(true).ok();
    tcp.set_nonblocking(false)
        .expect("blocking socket for chc_posix_io");
    let emitter = Emitter::new(emitter_cfg, catalog.clone(), tcp).expect("init emitter");
    let observer: Box<dyn TupleObserver> = Box::new(EmitterObserver::new(emitter));

    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder: BufferingDecoderSink::new(catalog.clone(), xact_buffer.clone()),
        xact_drain: XactRecordSink::new(xact_buffer.clone(), catalog.clone(), observer),
    };
    let mut segment_sink =
        DirSegmentSink::new(shadow_filter_dir.clone()).expect("open shadow filter dir");
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    // 12. Workload driver: INSERT/UPDATE/DELETE under autocommit, then
    // pg_switch_wal so the segment carrying the work seals cleanly.
    let driver_sock = source.config().socket_dir.clone();
    let driver_port = source.config().port;
    let driver = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        // Each -c is its own autocommit xact. Combining them into one
        // string puts them all in a single implicit transaction whose
        // COMMIT lands after pg_switch_wal (in the next segment) —
        // which would mean the daemon never sees the commit record
        // until a *second* segment ships.
        let _ = Command::new("psql")
            .args([
                "-h",
                driver_sock.to_str().unwrap(),
                "-p",
                &driver_port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "INSERT INTO s8.t SELECT g, repeat('a', 8 + g)::text FROM generate_series(1, 5) g",
                "-c",
                "UPDATE s8.t SET payload = upper(payload) WHERE id <= 3",
                "-c",
                "DELETE FROM s8.t WHERE id = 5",
                "-c",
                "SELECT pg_switch_wal()",
            ])
            .output();
    });

    // 13. Pump WAL until we've shipped one full segment (the workload's
    // pg_switch_wal seals the segment carrying every commit record).
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut segments_shipped = 0u64;
    let mut prev = stream.dispatched_lsn();
    while segments_shipped < 1 && Instant::now() < deadline {
        let apply_lsn = stream.dispatched_lsn();
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_chunk(StandbyStatus::collapsed(apply_lsn), &mut chunk_buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("source feed: {e:#}"),
            Err(_) => continue,
        };
        stream
            .push(
                chunk.start_lsn,
                chunk.data,
                &mut record_sink,
                &mut segment_sink,
            )
            .await
            .expect("push");
        let now = stream.dispatched_lsn();
        if now != prev {
            segments_shipped += (now - prev) / WAL_SEG_SIZE;
            prev = now;
        }
    }
    let _ = driver.join();
    assert!(
        segments_shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain",
    );

    // 14. Block until shadow has replayed past the daemon's dispatched
    // LSN. The decoder's relation_at already gates on this per record,
    // so by the time we reach here every xact's catalog state on shadow
    // matches what the source held at commit time.
    let target = stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    eprintln!(
        "phase8: shipped {} segments, target_lsn={:X}/{:X}, decoder={}, xact_buffer={}",
        segments_shipped,
        target >> 32,
        target as u32,
        record_sink.decoder.stats().summary(),
        xact_buffer.lock().await.stats().summary(),
    );

    // 15. Verify CH end-state matches source. ReplacingMergeTree FINAL
    // dedups on `_lsn`; filter `_op != 'delete'` to mirror the
    // tombstone-as-event model.
    let src_count = source
        .psql_one("SELECT count(*) FROM s8.t")
        .expect("source count");
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s8_t FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(
        src_count, ch_count,
        "row count mismatch: source={src_count}, ch={ch_count}",
    );

    // Spot-check the post-UPDATE payload values: ids 1..=3 land as
    // upper-case strings on both sides. argMax(payload, _lsn) gives
    // the latest version per id, mirroring how a ReplacingMergeTree
    // reader would see the table.
    let src_sample = source
        .psql_one("SELECT string_agg(payload, ',' ORDER BY id) FROM s8.t WHERE id <= 3")
        .expect("source sample");
    let ch_sample = ch
        .query(
            "SELECT arrayStringConcat(groupArray(payload), ',') FROM (\
                 SELECT id, argMax(payload, _lsn) AS payload \
                 FROM walshadow_test.s8_t \
                 WHERE _op != 'delete' AND id <= 3 \
                 GROUP BY id \
                 ORDER BY id\
             )",
        )
        .expect("ch sample");
    assert_eq!(
        src_sample, ch_sample,
        "payload mismatch: source={src_sample:?}, ch={ch_sample:?}",
    );
}

// ---------------------------------------------------------------------------
// Schema-evolution drill: `ALTER TABLE ... ADD COLUMN` mid-stream.
//
// Source starts with a 2-column table. The CH dest table + the
// emitter mapping pre-declare the 3-column post-ALTER shape. Workload:
//
//   1. INSERT into the 2-column table (pre-ALTER xact: descriptor has
//      attnums 1,2 only — the mapping's attnum 3 has no source value
//      yet, `TableEncoder::append_row` emits NULL for it).
//   2. ALTER TABLE ADD COLUMN c int DEFAULT 7 (catalog-only xact;
//      walshadow filter keeps it, shadow PG's standby recovery
//      replays into pg_class / pg_attribute, `ShadowCatalog`
//      generation bumps and the next post-ALTER xact reads a
//      3-attnum descriptor).
//   3. INSERT into the 3-column table (descriptor + tuple both have
//      3 columns; emitter encodes `c` as a real value).
//   4. pg_switch_wal to seal the segment.
//
// CH end-state should mirror what a reader of source would see: id=1
// row with c=NULL (the pre-ALTER row never had c written; the source's
// `default 7` would surface via PG read-time injection, which the
// decoder doesn't replicate), id=2 row with c=42.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_add_column_replicates_pre_and_post_alter() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !pg_basebackup_available() {
        eprintln!("skip: no pg_basebackup on PATH");
        return;
    }
    if !clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let (
        BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA s8;\n\
         CREATE TABLE s8.t (id bigint PRIMARY KEY, payload text);\n\
         ALTER TABLE s8.t REPLICA IDENTITY FULL;\n",
        SOURCE_PORT_S,
        SHADOW_PORT_S,
        WALSENDER_PORT_S,
    )
    .await;
    let _src_stop = StopOnDrop { sh: &source };
    let _shd_stop = StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = ChServer::spawn(ch_tmp, CH_TCP_PORT_S, CH_HTTP_PORT_S).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    // Dest table carries the post-ALTER 3-column shape from day one.
    // `c Nullable(Int32)` so pre-ALTER rows (where the source heap
    // tuple has no c value) land cleanly as NULL.
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.s8_t_evo (\
            id Int64,\
            payload Nullable(String),\
            c Nullable(Int32),\
            _lsn UInt64,\
            _xid UInt32,\
            _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),\
            _commit_ts DateTime64(6, 'UTC')\
         ) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id",
    )
    .expect("create dest table");

    let scfg = source.config();
    let pgcfg = PgConfig {
        host: scfg.socket_dir.to_string_lossy().into_owned(),
        port: scfg.port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-phase8-evo".into(),
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
        shadow_stream_state.clone(),
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

    let inv_epoch = Arc::new(std::sync::atomic::AtomicU64::new(0));
    stream
        .filter_mut()
        .tracker
        .set_invalidation_epoch(inv_epoch.clone());
    catalog.lock().await.set_invalidation_epoch(inv_epoch);

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

    // Mapping declares attnum 3 ("c") from the start — before the
    // ALTER on source actually adds it. `TablePlan::build` is now
    // tolerant of mapping attnums that aren't in the catalog
    // descriptor yet; pre-ALTER xacts encode a NULL for `c` because
    // the source tuple has no value at that attnum.
    let mut emitter_cfg = EmitterConfig {
        host: "127.0.0.1".into(),
        port: CH_TCP_PORT_S,
        database: "walshadow_test".into(),
        compression: CompressionChoice::Lz4,
        ..Default::default()
    };
    emitter_cfg.tables.insert(
        "s8.t".into(),
        TableMapping {
            target: "walshadow_test.s8_t_evo".into(),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int64".into(),
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "payload".into(),
                    target_type: "Nullable(String)".into(),
                },
                ColumnMapping {
                    src_attnum: 3,
                    target_name: "c".into(),
                    target_type: "Nullable(Int32)".into(),
                },
            ],
        },
    );

    let tcp = TcpStream::connect(("127.0.0.1", CH_TCP_PORT_S)).expect("tcp connect ch");
    tcp.set_nodelay(true).ok();
    tcp.set_nonblocking(false)
        .expect("blocking socket for chc_posix_io");
    let emitter = Emitter::new(emitter_cfg, catalog.clone(), tcp).expect("init emitter");
    let observer: Box<dyn TupleObserver> = Box::new(EmitterObserver::new(emitter));

    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder: BufferingDecoderSink::new(catalog.clone(), xact_buffer.clone()),
        xact_drain: XactRecordSink::new(xact_buffer.clone(), catalog.clone(), observer),
    };
    let mut segment_sink =
        DirSegmentSink::new(shadow_filter_dir.clone()).expect("open shadow filter dir");
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    // Workload: pre-ALTER INSERT, ALTER, post-ALTER INSERT, rotate.
    // The pre-ALTER xact exercises the "mapping has attnum=3 but
    // descriptor has only 1,2" path; the post-ALTER xact exercises
    // the "mapping + descriptor agree" path. Each statement runs in
    // its own autocommit xact (`-c` per statement) so every COMMIT
    // record lands in the same segment as its heap records.
    let driver_sock = source.config().socket_dir.clone();
    let driver_port = source.config().port;
    let driver = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        let _ = Command::new("psql")
            .args([
                "-h",
                driver_sock.to_str().unwrap(),
                "-p",
                &driver_port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "INSERT INTO s8.t (id, payload) VALUES (1, 'pre')",
                "-c",
                "ALTER TABLE s8.t ADD COLUMN c int DEFAULT 7",
                "-c",
                "INSERT INTO s8.t (id, payload, c) VALUES (2, 'post', 42)",
                "-c",
                "SELECT pg_switch_wal()",
            ])
            .output();
    });

    let deadline = Instant::now() + Duration::from_secs(45);
    let mut segments_shipped = 0u64;
    let mut prev = stream.dispatched_lsn();
    while segments_shipped < 1 && Instant::now() < deadline {
        let apply_lsn = stream.dispatched_lsn();
        let next = tokio::time::timeout(
            Duration::from_secs(2),
            feed.next_chunk(StandbyStatus::collapsed(apply_lsn), &mut chunk_buf),
        )
        .await;
        let chunk = match next {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("source feed: {e:#}"),
            Err(_) => continue,
        };
        stream
            .push(
                chunk.start_lsn,
                chunk.data,
                &mut record_sink,
                &mut segment_sink,
            )
            .await
            .expect("push");
        let now = stream.dispatched_lsn();
        if now != prev {
            segments_shipped += (now - prev) / WAL_SEG_SIZE;
            prev = now;
        }
    }
    let _ = driver.join();
    assert!(
        segments_shipped >= 1,
        "no segments shipped in 45s — pipeline didn't drain",
    );

    let target = stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    assert!(observed >= target);

    eprintln!(
        "phase8-evo: shipped {} segments, decoder={}, xact_buffer={}",
        segments_shipped,
        record_sink.decoder.stats().summary(),
        xact_buffer.lock().await.stats().summary(),
    );

    // Both rows should be present on both sides.
    let src_count = source
        .psql_one("SELECT count(*) FROM s8.t")
        .expect("source count");
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.s8_t_evo FINAL WHERE _op != 'delete'")
        .expect("ch count");
    assert_eq!(
        src_count, ch_count,
        "row count mismatch: source={src_count}, ch={ch_count}",
    );
    assert_eq!(src_count, "2");

    // Pre-ALTER row: source's PG read-time default fills c=7; the
    // decoder doesn't replicate read-time defaults (the pre-ALTER
    // heap tuple physically has no c column), so CH sees c=NULL for
    // id=1. Post-ALTER row: the source tuple physically carries c=42
    // and CH agrees. `clickhouse client` renders NULL as the literal
    // `\N` in its default TabSeparated output.
    let ch_pre = ch
        .query(
            "SELECT argMax(c, _lsn) \
             FROM walshadow_test.s8_t_evo \
             WHERE _op != 'delete' AND id = 1",
        )
        .expect("ch pre-alter c");
    assert_eq!(
        ch_pre, "\\N",
        "pre-ALTER row should land with c=NULL (heap tuple has no c \
         value; decoder doesn't apply PG read-time missing defaults)",
    );
    let ch_post = ch
        .query(
            "SELECT argMax(c, _lsn) \
             FROM walshadow_test.s8_t_evo \
             WHERE _op != 'delete' AND id = 2",
        )
        .expect("ch post-alter c");
    assert_eq!(ch_post, "42");
}
