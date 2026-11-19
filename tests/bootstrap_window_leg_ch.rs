//! Verify live window leg reads through exact `end_lsn` and patches commits

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walrus::pg::backup::{format_pg_lsn, parse_pg_lsn};
use walshadow::backfill_bootstrap::seed_catalog_from_source;
use walshadow::bootstrap_window::{
    WindowLegConfig, replay_segments, segments_in_dir, stream_window,
};
use walshadow::ch::CompressionChoice;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats, InstanceConfig};
use walshadow::config::ResolvedConfig;
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::pipeline::Fatal;
use walshadow::record::WAL_SEG_SIZE;
use walshadow::schema::RelName;
use walshadow::source_feed::{SourceFeed, open_sql_client};
use walshadow::toast::{ChunkStore, ChunkStoreError, FetchedValue, MemChunkStore, ToastResolver};
use walshadow::visibility::PgXactPatch;
use walshadow::wal_stream::WalStream;

const SCHEMA: &str = "s23";
const N_ROWS: i32 = 100;

fn emitter(port: u16) -> EmitterConfig {
    let mut cfg = EmitterConfig {
        conn: InstanceConfig {
            host: "127.0.0.1".into(),
            port,
            database: "walshadow_test".into(),
            ..Default::default()
        },
        compression: CompressionChoice::None,
        flush_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    cfg.tables.insert(
        RelName::new(SCHEMA, "t"),
        TableMapping {
            target: TableTarget::new("walshadow_test", "t"),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int32".into(),
                    type_pinned: false,
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "name".into(),
                    target_type: "String".into(),
                    type_pinned: false,
                },
            ],
            tees: Vec::new(),
        },
    );
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_transaction_failure_returns_from_live_and_archived_replay() {
    if !fx::tools::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::make_pg(&tmp, "source", ports.source);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    fx::append_source_conf(&source);
    source.start().unwrap();
    let _stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(&format!(
            "CREATE SCHEMA {SCHEMA};
         CREATE TABLE {SCHEMA}.t (id int PRIMARY KEY, name text);
         ALTER TABLE {SCHEMA}.t ALTER COLUMN name SET STORAGE EXTERNAL;
         ALTER TABLE {SCHEMA}.t REPLICA IDENTITY FULL;
         INSERT INTO {SCHEMA}.t VALUES (2, repeat('external', 1000));
         SELECT pg_switch_wal();"
        ))
        .unwrap();
    let ch =
        fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http).unwrap();
    ch.query("CREATE DATABASE walshadow_test").unwrap();
    ch.query(
        "CREATE TABLE walshadow_test.t (id Int32, name String, _lsn UInt64, _xid UInt32,
         _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool)
         ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .unwrap();
    let pg = fx::pg_cfg(&source, "window-error-test");
    let sql = open_sql_client(&pg).await.unwrap();
    let catalog = seed_catalog_from_source(&sql).await.unwrap();
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    sql.batch_execute(&format!("INSERT INTO {SCHEMA}.t VALUES (1, 'inline')"))
        .await
        .unwrap();
    sql.batch_execute(&format!("UPDATE {SCHEMA}.t SET id=id WHERE id=2"))
        .await
        .unwrap();
    let end = walshadow::pg::current_wal_lsn(&sql).await.unwrap();
    sql.batch_execute("SELECT pg_switch_wal()").await.unwrap();
    let stats = Arc::new(EmitterStats::default());
    let emitter = emitter(ports.ch_tcp);
    let cfg = WindowLegConfig {
        wind_down: Duration::from_secs(5),
        mapping: walshadow::mapping::mapping_handle(emitter.tables.clone()),
        dest: walshadow::config::DestEmitter::new(Arc::new(emitter), None),
        config: Arc::new(ResolvedConfig::default()),
        stats: stats.clone(),
        resolver: ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats),
        oracle: None,
        fatal: Fatal::new(),
        scratch_dir: tmp.path().join("leg"),
        patch: Arc::new(std::sync::Mutex::new(PgXactPatch::new())),
        catalog,
        pg_major: (feed.server_version_num() / 10000) as u32,
        system_id: ident.system_id.to_string(),
        timeline: ident.timeline,
    };
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(Some(end));
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        stream_window(cfg.clone(), &mut feed, ident.xlogpos, stop_rx),
    )
    .await
    .expect("live replay hung after partial transaction")
    .unwrap_err();
    assert!(format!("{err:#}").contains("no mirror"), "{err:#}");

    let segments = segments_in_dir(
        &source.config().data_dir.join("pg_wal"),
        ident.timeline,
        ident.xlogpos,
        end,
    )
    .await
    .unwrap();
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        replay_segments(cfg, &segments, ident.xlogpos, end),
    )
    .await
    .expect("archived replay hung after partial transaction")
    .unwrap_err();
    assert!(format!("{err:#}").contains("no mirror"), "{err:#}");
    assert_eq!(
        ch.query("SELECT id FROM walshadow_test.t FINAL").unwrap(),
        "1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leg_reads_through_end_lsn_inside_its_first_segment() {
    if !fx::tools::pg_available() {
        return;
    }
    if !fx::tools::clickhouse_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::make_pg(&tmp, "source", slot.source);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source);
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };
    // Fresh segment, so the window below lives in one segment with room
    source
        .apply_schema_dump(&format!(
            "CREATE SCHEMA {SCHEMA};\n\
             CREATE TABLE {SCHEMA}.t (id int4 PRIMARY KEY, name text NOT NULL);\n\
             ALTER TABLE {SCHEMA}.t REPLICA IDENTITY FULL;\n\
             SELECT pg_switch_wal();\n"
        ))
        .expect("source schema");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.t (\
            id Int32,\
            name String,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let result: Result<()> = async {
        let pg = fx::pg_cfg(&source, "window-leg-test");
        let sql = open_sql_client(&pg).await.context("sidecar connect")?;
        let catalog = seed_catalog_from_source(&sql)
            .await
            .context("seed catalog")?;
        let mut feed = SourceFeed::connect(&pg).await.context("feed connect")?;
        let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
        let from_lsn = ident.xlogpos;

        // Key change during replay must retire old key
        sql.batch_execute(&format!(
            "INSERT INTO {SCHEMA}.t SELECT g, 'leg-'||g::text FROM generate_series(1, {N_ROWS}) g;
             UPDATE {SCHEMA}.t SET id = id + {N_ROWS} WHERE id = 1"
        ))
        .await
        .context("window INSERT")?;
        let end: String = sql
            .query_one("SELECT pg_current_wal_flush_lsn()::text", &[])
            .await
            .context("flush lsn")?
            .get(0);
        let end_lsn = parse_pg_lsn(&end)?;
        ensure!(
            WalStream::align_down(from_lsn, WAL_SEG_SIZE)
                == WalStream::align_down(end_lsn, WAL_SEG_SIZE),
            "window crossed a segment: from={} end={end}",
            format_pg_lsn(from_lsn),
        );
        // One record past `end_lsn`, so the leg's position can pass it
        sql.batch_execute("SELECT txid_current()")
            .await
            .context("trailing commit")?;

        let (stop_tx, stop_rx) = tokio::sync::watch::channel(Some(end_lsn));
        let patch = Arc::new(std::sync::Mutex::new(PgXactPatch::new()));
        let cfg = WindowLegConfig {
            wind_down: Duration::from_secs(5),
            dest: walshadow::config::DestEmitter::new(Arc::new(emitter(slot.ch_tcp)), None),
            mapping: walshadow::mapping::mapping_handle(emitter(slot.ch_tcp).tables),
            config: Arc::new(ResolvedConfig::default()),
            stats: Arc::new(EmitterStats::default()),
            resolver: ToastResolver::disabled(),
            oracle: None,
            fatal: Fatal::new(),
            scratch_dir: tmp.path().join("leg-scratch"),
            patch: patch.clone(),
            catalog,
            pg_major: (feed.server_version_num() / 10000) as u32,
            system_id: ident.system_id.to_string(),
            timeline: ident.timeline,
        };
        let leg = tokio::time::timeout(
            Duration::from_secs(30),
            stream_window(cfg, &mut feed, from_lsn, stop_rx),
        )
        .await
        .context("leg never covered end_lsn")?
        .context("window leg")?;
        drop(stop_tx);

        ensure!(
            leg.through_lsn >= end_lsn,
            "leg sealed at {} below end_lsn {end}",
            format_pg_lsn(leg.through_lsn),
        );
        ensure!(
            leg.replay.rows_replayed >= N_ROWS as u64,
            "leg shipped {} rows, want {N_ROWS}",
            leg.replay.rows_replayed,
        );
        let patched = patch.lock().unwrap().len();
        ensure!(patched >= 1, "leg patched no commit");
        let n = ch
            .query("SELECT count(), min(id), max(id) FROM walshadow_test.t FINAL")
            .context("ch count")?;
        let want = format!("{N_ROWS}\t2\t{}", N_ROWS + 1);
        ensure!(n == want, "CH holds {n:?}, want {want:?}");
        Ok(())
    }
    .await;

    if let Err(e) = result {
        panic!("{e:#}");
    }
}

fn leg_cfg(
    port: u16,
    dir: &std::path::Path,
    catalog: walshadow::backfill::backup_page_walk::CatalogMap,
    feed: &SourceFeed,
    system_id: u64,
    timeline: u32,
) -> WindowLegConfig {
    WindowLegConfig {
        wind_down: Duration::from_secs(5),
        dest: walshadow::config::DestEmitter::new(Arc::new(emitter(port)), None),
        mapping: walshadow::mapping::mapping_handle(emitter(port).tables),
        config: Arc::new(ResolvedConfig::default()),
        stats: Arc::new(EmitterStats::default()),
        resolver: ToastResolver::disabled(),
        oracle: None,
        fatal: Fatal::new(),
        scratch_dir: dir.to_path_buf(),
        patch: Arc::new(std::sync::Mutex::new(PgXactPatch::new())),
        catalog,
        pg_major: (feed.server_version_num() / 10000) as u32,
        system_id: system_id.to_string(),
        timeline,
    }
}

/// Every way a live leg stops short of a clean seal: waiting out an xact
/// open across the handoff, a caller unwinding, the source promoting, and
/// the source shutting down
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leg_waits_out_open_xacts_and_reports_how_the_stream_ended() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::make_pg(&tmp, "source", slot.source);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    fx::append_source_conf(&source);
    source.start().unwrap();
    let _stop = fx::StopOnDrop { sh: &source };
    // Frequent reply requests: walsender notices promotion, and its shutdown
    // sees the leg's ack, within a second instead of a minute
    source
        .psql_one("ALTER SYSTEM SET wal_sender_timeout = '2s'")
        .unwrap();
    source.psql_one("SELECT pg_reload_conf()").unwrap();
    source
        .apply_schema_dump(&format!(
            "CREATE SCHEMA {SCHEMA};
             CREATE TABLE {SCHEMA}.t (id int PRIMARY KEY, name text NOT NULL);
             ALTER TABLE {SCHEMA}.t REPLICA IDENTITY FULL;
             SELECT pg_switch_wal();"
        ))
        .unwrap();
    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http).unwrap();
    ch.query("CREATE DATABASE walshadow_test").unwrap();
    ch.query(
        "CREATE TABLE walshadow_test.t (id Int32, name String, _lsn UInt64, _xid UInt32,
         _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool)
         ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .unwrap();
    let pg = fx::pg_cfg(&source, "window-end-test");
    let sql = open_sql_client(&pg).await.unwrap();
    let catalog = seed_catalog_from_source(&sql).await.unwrap();
    let scratch = tmp.path().join("leg");

    // Xact open across the handoff: the leg waits until it commits
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let cfg = leg_cfg(
        slot.ch_tcp,
        &scratch,
        catalog.clone(),
        &feed,
        ident.system_id,
        1,
    );
    let holder = open_sql_client(&pg).await.unwrap();
    holder
        .batch_execute(&format!(
            "BEGIN; INSERT INTO {SCHEMA}.t VALUES (500, 'spanning')"
        ))
        .await
        .unwrap();
    let handoff: String = sql
        .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let handoff = parse_pg_lsn(&handoff).unwrap();
    // Committed xact flushes the open one's insert out to the leg
    sql.batch_execute("SELECT txid_current()").await.unwrap();
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(Some(handoff));
    let (leg, ()) = tokio::join!(
        stream_window(cfg.clone(), &mut feed, ident.xlogpos, stop_rx),
        async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            holder.batch_execute("COMMIT").await.unwrap();
        }
    );
    let leg = leg.unwrap();
    assert_eq!(leg.open_floor, None, "leg sealed before the commit landed");
    assert_eq!(
        ch.query("SELECT name FROM walshadow_test.t FINAL WHERE id = 500")
            .unwrap(),
        "spanning"
    );

    // Caller unwinding: closed stop watch seals where the leg stands
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(None);
    drop(stop_tx);
    let leg = stream_window(cfg.clone(), &mut feed, ident.xlogpos, stop_rx)
        .await
        .unwrap();
    assert_eq!(
        leg.through_lsn,
        WalStream::align_down(ident.xlogpos, WAL_SEG_SIZE)
    );

    // Promotion ends the timeline the leg reads
    source.stop().unwrap();
    source.write_standby_signal().unwrap();
    source.start().unwrap();
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let promoter = open_sql_client(&pg).await.unwrap();
    let far = tokio::sync::watch::channel(Some(u64::MAX));
    let (leg, ()) = tokio::join!(
        stream_window(cfg.clone(), &mut feed, ident.xlogpos, far.1.clone()),
        async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            promoter
                .batch_execute("SELECT pg_promote(true, 60)")
                .await
                .unwrap();
        }
    );
    let err = format!("{:#}", leg.unwrap_err());
    assert!(
        err.contains("source ended timeline 1 inside the backup window"),
        "{err}"
    );

    // Shutdown closes the stream of the promoted timeline
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let cfg = leg_cfg(slot.ch_tcp, &scratch, catalog, &feed, ident.system_id, 2);
    let data_dir = source.config().data_dir.clone();
    let (leg, ()) = tokio::join!(stream_window(cfg, &mut feed, ident.xlogpos, far.1), async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        // Off this task, so the leg keeps acking while the walsender drains
        let status = tokio::task::spawn_blocking(move || {
            std::process::Command::new("pg_ctl")
                .arg("-D")
                .arg(data_dir)
                .args(["-m", "fast", "-w", "stop"])
                .status()
        })
        .await
        .unwrap()
        .unwrap();
        assert!(status.success());
    });
    let err = format!("{:#}", leg.unwrap_err());
    assert!(
        err.contains("source closed the stream inside the backup window"),
        "{err}"
    );
}

/// Takes chunk rows, refuses to write them
struct RefusesPuts;

#[async_trait::async_trait]
impl ChunkStore for RefusesPuts {
    fn accepts_writes(&self) -> bool {
        true
    }
    async fn fetch_many(
        &self,
        _: u32,
        _: u32,
        values: &[(u32, usize)],
        _: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        Ok(values.iter().map(|_| FetchedValue::Missing).collect())
    }
}

/// Tail closing mid-leg fails next flush, a refused mirror write fails seal
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leg_reports_tail_failures_at_flush_and_finish() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::make_pg(&tmp, "source", slot.source);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    fx::append_source_conf(&source);
    source.start().unwrap();
    let _stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(&format!(
            "CREATE SCHEMA {SCHEMA};
             CREATE TABLE {SCHEMA}.t (id int PRIMARY KEY, name text NOT NULL);
             ALTER TABLE {SCHEMA}.t ALTER COLUMN name SET STORAGE EXTERNAL;
             INSERT INTO {SCHEMA}.t VALUES (2, repeat('external', 1000));"
        ))
        .unwrap();
    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http).unwrap();
    ch.query("CREATE DATABASE walshadow_test").unwrap();
    ch.query(
        "CREATE TABLE walshadow_test.t (id Int32, name String, _lsn UInt64, _xid UInt32,
         _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool)
         ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .unwrap();
    let pg = fx::pg_cfg(&source, "window-tail-test");
    let sql = open_sql_client(&pg).await.unwrap();
    let catalog = seed_catalog_from_source(&sql).await.unwrap();
    let scratch = tmp.path().join("leg");

    // Batcher refuses a destination type it cannot plan and exits
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let mut cfg = leg_cfg(
        slot.ch_tcp,
        &scratch,
        catalog.clone(),
        &feed,
        ident.system_id,
        ident.timeline,
    );
    let mut bad_type = emitter(slot.ch_tcp);
    for m in bad_type.tables.values_mut() {
        m.columns[1].target_type = "NoSuchType".into();
    }
    cfg.mapping = walshadow::mapping::mapping_handle(bad_type.tables.clone());
    cfg.dest = walshadow::config::DestEmitter::new(Arc::new(bad_type), None);
    let writer = open_sql_client(&pg).await.unwrap();
    let inserts = tokio::spawn(async move {
        for id in 100..10_000 {
            writer
                .batch_execute(&format!("INSERT INTO {SCHEMA}.t VALUES ({id}, 'x')"))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(None);
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        stream_window(cfg, &mut feed, ident.xlogpos, stop_rx),
    )
    .await
    .expect("leg outlived its tail")
    .unwrap_err();
    inserts.abort();
    assert_eq!(
        format!("{err:#}"),
        "bootstrap window leg: flush: wal_replay: tail closed"
    );

    // Delete of a toasted row queues chunk tombstones the store refuses
    sql.batch_execute("SELECT pg_switch_wal()").await.unwrap();
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let mut cfg = leg_cfg(
        slot.ch_tcp,
        &scratch,
        catalog,
        &feed,
        ident.system_id,
        ident.timeline,
    );
    cfg.resolver = ToastResolver::with_store(Arc::new(RefusesPuts), cfg.stats.clone());
    sql.batch_execute(&format!("DELETE FROM {SCHEMA}.t WHERE id = 2"))
        .await
        .unwrap();
    let end = walshadow::pg::current_wal_lsn(&sql).await.unwrap();
    sql.batch_execute("SELECT txid_current()").await.unwrap();
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(Some(end));
    let err = stream_window(cfg, &mut feed, ident.xlogpos, stop_rx)
        .await
        .unwrap_err();
    let err = format!("{err:#}");
    assert!(err.starts_with("bootstrap window leg: finish: "), "{err}");
}
