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
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::config::ResolvedConfig;
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::pipeline::Fatal;
use walshadow::record::WAL_SEG_SIZE;
use walshadow::schema::RelName;
use walshadow::source_feed::{SourceFeed, open_sql_client};
use walshadow::toast::{MemChunkStore, ToastResolver};
use walshadow::visibility::PgXactPatch;
use walshadow::wal_stream::WalStream;

const SCHEMA: &str = "s23";
const N_ROWS: i32 = 100;

fn emitter(port: u16) -> EmitterConfig {
    let mut cfg = EmitterConfig {
        host: "127.0.0.1".into(),
        port,
        database: "walshadow_test".into(),
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
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "name".into(),
                    target_type: "String".into(),
                },
            ],
        },
    );
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_transaction_failure_returns_from_live_and_archived_replay() {
    if !fx::requirements_available() {
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
    sql.batch_execute(&format!(
        "BEGIN; INSERT INTO {SCHEMA}.t VALUES (1, 'inline');
         UPDATE {SCHEMA}.t SET id=id WHERE id=2; COMMIT"
    ))
    .await
    .unwrap();
    let end = walshadow::pg::current_wal_lsn(&sql).await.unwrap();
    sql.batch_execute("SELECT pg_switch_wal()").await.unwrap();
    let stats = Arc::new(EmitterStats::default());
    let emitter = emitter(ports.ch_tcp);
    let cfg = WindowLegConfig {
        wind_down: Duration::from_secs(5),
        mapping: walshadow::mapping::mapping_handle(emitter.tables.clone()),
        emitter,
        config: Arc::new(ResolvedConfig::default()),
        stats: stats.clone(),
        resolver: ToastResolver::with_store(Arc::new(MemChunkStore::new()), stats),
        oracle: None,
        fatal: Fatal::new(),
        scratch_dir: tmp.path().join("leg"),
        patch: Arc::new(std::sync::Mutex::new(PgXactPatch::new())),
        catalog,
        pg_major: (feed.server_version_num() / 10000) as u32,
        system_id: ident.sysid,
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
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
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

        sql.batch_execute(&format!(
            "INSERT INTO {SCHEMA}.t SELECT g, 'leg-'||g::text FROM generate_series(1, {N_ROWS}) g"
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
            emitter: emitter(slot.ch_tcp),
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
            system_id: ident.sysid.clone(),
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
            .query(&format!(
                "SELECT count() FROM walshadow_test.t FINAL WHERE id <= {N_ROWS}"
            ))
            .context("ch count")?;
        ensure!(
            n == N_ROWS.to_string(),
            "CH holds {n} of {N_ROWS} window rows"
        );
        Ok(())
    }
    .await;

    if let Err(e) = result {
        panic!("{e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ctid_repair_converges_with_updates_deletes_reuse_and_crossing_transaction() {
    if !fx::requirements_available() {
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
    source.apply_schema_dump(&format!(
        "CREATE SCHEMA {SCHEMA};
         CREATE TABLE {SCHEMA}.t (id int PRIMARY KEY, name text) WITH (autovacuum_enabled=false);
         INSERT INTO {SCHEMA}.t VALUES (1,'keep'), (2,'old'), (3,'delete'), (4,'reuse'), (5,'before');
         SELECT pg_switch_wal();"
    )).unwrap();
    let ch =
        fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http).unwrap();
    ch.query("CREATE DATABASE walshadow_test").unwrap();
    ch.query(
        "CREATE TABLE walshadow_test.t (id Int32, name String, _lsn UInt64, _xid UInt32,
        _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool)
        ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .unwrap();
    let pg = fx::pg_cfg(&source, "repair-convergence-test");
    let sql = open_sql_client(&pg).await.unwrap();
    let catalog = seed_catalog_from_source(&sql).await.unwrap();
    let desc = catalog
        .descriptors()
        .find(|d| d.rel_name == RelName::new(SCHEMA, "t"))
        .unwrap();
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let tids: Vec<String> = sql
        .query(
            &format!("SELECT ctid::text FROM {SCHEMA}.t ORDER BY id"),
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    let tuples = tids
        .iter()
        .map(|tid| {
            let (blk, off) = tid.trim_matches(['(', ')']).split_once(',').unwrap();
            walshadow::backup_page_walk::BackfillTuple {
                rfn: desc.rfn,
                xid: 100,
                xmax: 10,
                infomask: walshadow::visibility::HEAP_XMIN_COMMITTED
                    | walshadow::visibility::HEAP_XMAX_IS_MULTI,
                source_lsn: ident.xlogpos,
                blkno: blk.parse().unwrap(),
                offnum: off.parse().unwrap(),
                columns: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    sql.batch_execute(&format!(
        "UPDATE {SCHEMA}.t SET name='new' WHERE id=2;
         DELETE FROM {SCHEMA}.t WHERE id IN (3,4)"
    ))
    .await
    .unwrap();
    sql.batch_execute(&format!("VACUUM {SCHEMA}.t"))
        .await
        .unwrap();
    sql.batch_execute(&format!("INSERT INTO {SCHEMA}.t VALUES (6,'reused')"))
        .await
        .unwrap();
    let reused: String = sql
        .query_one(
            &format!("SELECT ctid::text FROM {SCHEMA}.t WHERE id=6"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        tids.contains(&reused),
        "test requires CTID reuse: {reused}, {tids:?}"
    );
    let crossing = open_sql_client(&pg).await.unwrap();
    crossing
        .batch_execute(&format!(
            "BEGIN; UPDATE {SCHEMA}.t SET name='cross' WHERE id=5"
        ))
        .await
        .unwrap();

    let emitter = emitter(ports.ch_tcp);
    let mapping = walshadow::mapping::mapping_handle(emitter.tables.clone());
    let routes = mapping.snapshot().await;
    let stats = Arc::new(EmitterStats::default());
    let tail = walshadow::pipeline::tail::OwnedTail::spawn(
        &emitter,
        1,
        stats.clone(),
        Fatal::new(),
        None,
        None,
        "repair convergence",
    )
    .await
    .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let (out_tx, out_rx) = tokio::sync::mpsc::channel(1);
    let repair = tokio::spawn(
        walshadow::visibility_repair::RowRepair {
            source: pg,
            catalog: catalog.clone(),
            scope: walshadow::visibility_repair::RepairScope::mapped(&catalog, &routes, |_| true),
            rate: walshadow::copy_backfill::CopyRate::new(None),
        }
        .run(rx, out_tx),
    );
    let drain = tokio::spawn(walshadow::pipeline::bootstrap::drain(
        out_rx,
        catalog.clone(),
        routes,
        tail.msg_tx.clone(),
        tail.ack.clone(),
        stats.clone(),
        ToastResolver::disabled(),
        None,
        Default::default(),
        None,
        Default::default(),
    ));
    let mut spool = walshadow::spool::DeferredSpool::new(tmp.path().join("gate"), 0);
    for t in tuples {
        spool.push(t).await.unwrap();
    }
    let accum = walshadow::visibility::PgXactAccum::new();
    let patch = PgXactPatch::new();
    let multi = walshadow::visibility::PgMultiXactAccum::new();
    let view = walshadow::visibility::PgXactView::new(&accum, &patch).with_multixact(&multi);
    let mut gate_stats = walshadow::visibility_gate::GateStats::default();
    walshadow::visibility_gate::resolve_phase(
        spool,
        &view,
        &walshadow::visibility_gate::GateOutput::Repair(&tx),
        &mut gate_stats,
    )
    .await
    .unwrap();
    drop(tx);
    repair.await.unwrap().unwrap();
    let drained = drain.await.unwrap().unwrap();
    tail.finish(drained.next_seq).await.unwrap();
    assert_eq!(gate_stats.unresolved, 5);
    crossing.batch_execute("COMMIT").await.unwrap();
    let end = walshadow::pg::current_wal_lsn(&sql).await.unwrap();
    sql.batch_execute("SELECT pg_switch_wal()").await.unwrap();
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(Some(end));
    let cfg = WindowLegConfig {
        emitter,
        mapping,
        config: Arc::new(ResolvedConfig::default()),
        stats,
        resolver: ToastResolver::disabled(),
        oracle: None,
        fatal: Fatal::new(),
        scratch_dir: tmp.path().join("leg"),
        patch: Arc::new(std::sync::Mutex::new(PgXactPatch::new())),
        catalog,
        pg_major: (feed.server_version_num() / 10000) as u32,
        system_id: ident.sysid,
        timeline: ident.timeline,
        wind_down: Duration::ZERO,
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        stream_window(cfg, &mut feed, ident.xlogpos, stop_rx),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        ch.query("SELECT id, name FROM walshadow_test.t FINAL WHERE NOT _is_deleted ORDER BY id")
            .unwrap(),
        "1\tkeep\n2\tnew\n5\tcross\n6\treused"
    );
}
