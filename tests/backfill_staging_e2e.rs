#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use walrus::pg::replication::conn::PgConfig;
use walshadow::backfill_staging::{self, StagingRel, StagingSession};
use walshadow::backfill_types::BackupRequest;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats, InstanceConfig};
use walshadow::copy_backfill::CopyBackfiller;
use walshadow::desc_log::{BatchRecord, DescLogIdentity, DescriptorLog, LogEntry, LogValue};
use walshadow::mapping::{ColumnMapping, MappingHandle, TableMapping, TableTarget, mapping_handle};
use walshadow::runtime_config::InitialLoadMode;
use walshadow::schema::{RelDescriptor, RelName};
use walshadow::shadow::Shadow;
use walshadow::shadow_catalog::ShadowCatalog;
use walshadow::timeline::TimelineHistory;
use walshadow::visibility_pending::PendingLedger;

struct Fixture {
    source: Shadow,
    ch: fx::ChServer,
    emitter: EmitterConfig,
    mapping: MappingHandle,
    catalog: Arc<Mutex<ShadowCatalog>>,
    log: Arc<DescriptorLog>,
    desc: Arc<RelDescriptor>,
    stats: Arc<EmitterStats>,
    _tmp: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let source = fx::start_bridged_source(&tmp);
        let stop = fx::StopOnDrop { sh: &source };
        source
            .psql_one(
                "CREATE TABLE public.t (id int PRIMARY KEY, name text NOT NULL); \
                 ALTER TABLE public.t REPLICA IDENTITY FULL; \
                 INSERT INTO public.t VALUES (1, 'one'), (2, 'two'), (3, 'three'); \
                 CHECKPOINT",
            )
            .unwrap();
        let (bridge, mut catalog) = fx::connect_catalog(&source, "backfill-staging").await;
        let (_, descs) = catalog.fetch_all_descriptors().await.unwrap();
        let desc = Arc::new(
            descs
                .into_iter()
                .find(|d| d.rel_name == RelName::new("public", "t"))
                .unwrap(),
        );
        std::fs::create_dir_all(tmp.path().join("descriptors")).unwrap();
        let log = Arc::new(
            DescriptorLog::open(
                &tmp.path().join("descriptors"),
                DescLogIdentity {
                    pg_major: bridge.info().pg_version_num / 10000,
                    system_id: source
                        .psql_one("SELECT system_identifier FROM pg_control_system()")
                        .unwrap(),
                    timeline: 1,
                    db_oid: desc.rfn.db_node,
                    wal_seg_size: 16 << 20,
                },
            )
            .await
            .unwrap(),
        );
        let ports = fx::Ports::alloc();
        let ch =
            fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http).unwrap();
        fx::create_ch_dest_table(&ch, "default", "t").unwrap();
        let emitter = EmitterConfig {
            conn: InstanceConfig {
                port: ports.ch_tcp,
                ..Default::default()
            },
            inserter_pool_size: 4,
            byte_budget: 1 << 20,
            row_budget: 1,
            insert_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let mapping = mapping_handle(
            [(
                desc.rel_name.clone(),
                TableMapping {
                    target: TableTarget::new("default", "t"),
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
            )]
            .into_iter()
            .collect(),
        );
        std::mem::forget(stop);
        Self {
            source,
            ch,
            emitter,
            mapping,
            catalog: Arc::new(Mutex::new(catalog)),
            log,
            desc,
            stats: Arc::new(EmitterStats::default()),
            _tmp: tmp,
        }
    }

    async fn backfiller(&self, dir: &Path) -> Arc<CopyBackfiller> {
        self.backfiller_as(dir, fx::pg_cfg(&self.source, "backfill-staging"))
            .await
    }

    async fn backfiller_as(&self, dir: &Path, pg: PgConfig) -> Arc<CopyBackfiller> {
        let source_major: u32 = self
            .source
            .psql_one("SELECT current_setting('server_version_num')::int / 10000")
            .unwrap()
            .parse()
            .unwrap();
        Arc::new(
            CopyBackfiller::new(
                pg,
                walshadow::config::DestEmitter::new(Arc::new(self.emitter.clone()), None),
                self.mapping.clone(),
                self.stats.clone(),
                self.catalog.clone(),
                self.log.clone(),
                dir,
                None,
                watch::channel(Arc::new(TimelineHistory::root(1))).1,
                None,
                None,
                source_major,
                self.system_id(),
                PendingLedger::load(dir, self.system_id())
                    .await
                    .unwrap()
                    .shared(),
            )
            .await
            .unwrap(),
        )
    }

    fn system_id(&self) -> u64 {
        self.source
            .psql_one("SELECT system_identifier FROM pg_control_system()")
            .unwrap()
            .parse()
            .unwrap()
    }

    async fn prepare_staging(&self) -> StagingRel {
        let mut plan = backfill_staging::prepare(
            walshadow::config::DestEmitter::new(Arc::new(self.emitter.clone()), None),
            &self.mapping,
            &[BackupRequest {
                desc: self.desc.clone(),
                s_lsn: 100,
            }],
            false,
        )
        .await
        .unwrap();
        assert_eq!(plan.rels.len(), 1);
        let rel = plan.rels.pop().unwrap();
        assert_eq!(
            plan.mapping.snapshot()[&rel.rel].target.table,
            rel.staging_table()
        );
        rel
    }

    fn rows(&self) -> String {
        self.ch
            .query("SELECT id, name FROM default.t FINAL WHERE NOT _is_deleted ORDER BY id")
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.source.stop().unwrap();
    }
}

async fn wait_done(backfiller: &CopyBackfiller, dir: &Path) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while backfiller.pending_count() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("backfill completes");
    // `publish` marks the backfill done before `settle` promotes what the load
    // held, so the carry ledger draining is what says those rows landed
    tokio::time::timeout(Duration::from_secs(30), async {
        while std::fs::read_to_string(walshadow::visibility_pending::ledger_path(dir))
            .is_ok_and(|l| l.contains("[[carry]]"))
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("pending visibility settles");
    let ledger = read_ledger(dir);
    let entry = &ledger["backfill"][0];
    assert_eq!(entry["done"].as_bool(), Some(true));
    assert_eq!(entry["swapped"].as_bool(), Some(false));
    assert!(entry.get("staging_uuid").is_none());
    assert_eq!(backfiller.pending_by_mode(), [0, 0, 0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staging_follows_named_instance() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut fixture = Fixture::new().await;
    let ports = fx::Ports::alloc();
    let remote =
        fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http).unwrap();
    fx::create_ch_dest_table(&remote, "default", "t").unwrap();
    fixture.emitter.instances.insert(
        "archive".into(),
        walshadow::ch_emitter::InstanceConfig {
            host: "127.0.0.1".into(),
            port: ports.ch_tcp,
            database: "history".into(),
            ..Default::default()
        },
    );
    fixture.mapping.mutate(|map| {
        let entry = Arc::make_mut(map)
            .get_mut(&fixture.desc.rel_name)
            .expect("fixture mapping present");
        Arc::make_mut(entry).target.instance = Some("archive".into());
    });
    let rel = fixture.prepare_staging().await;
    assert_eq!(rel.target.instance.as_deref(), Some("archive"));
    assert_eq!(remote.query("EXISTS DATABASE history").unwrap(), "1");
    assert_eq!(
        fixture.ch.query("EXISTS TABLE default.t__wsstg").unwrap(),
        "0"
    );
    remote
        .query("INSERT INTO default.t__wsstg (id, name, _lsn) VALUES (7, 'live', 101)")
        .unwrap();
    // Live rules name the LSN column copy-back filters on
    let mut session = StagingSession::connect(walshadow::config::DestEmitter::new(
        Arc::new(fixture.emitter.clone()),
        None,
    ))
    .await
    .unwrap()
    .with_rules(Some(
        Arc::new(walshadow::table_rules::TableRules::default()),
    ));
    session.copy_back(&rel).await.unwrap();
    assert_eq!(
        remote
            .query("SELECT id, name FROM default.t FINAL")
            .unwrap(),
        "7\tlive"
    );
    assert_eq!(
        fixture.ch.query("SELECT count() FROM default.t").unwrap(),
        "0"
    );
    session.drop_staging(&rel).await.unwrap();
    assert_eq!(remote.query("EXISTS TABLE default.t__wsstg").unwrap(), "0");
    remote
        .query("CREATE TABLE default.t__wsstg (other Int32) ENGINE = MergeTree ORDER BY other")
        .unwrap();
    let err = session.copy_back(&rel).await.unwrap_err();
    assert!(err.to_string().contains("no shared columns"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_opt_in_replaces_stale_rows_and_reloads_after_opt_out() {
    if !fx::tools::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    fx.ch
        .query("INSERT INTO default.t (id, name, _lsn) VALUES (99, 'stale', 1)")
        .unwrap();
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 100)
        .await;
    assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(fx.rows(), "1\tone\n2\ttwo\n3\tthree");
    let walked = fx
        .stats
        .backfill_backup_walk
        .tuples_emitted
        .load(Ordering::Relaxed);
    let tapped = fx
        .stats
        .backfill_backup_pump
        .bytes_tapped
        .load(Ordering::Relaxed);
    assert!(walked >= 3);
    assert!(tapped >= 8192);
    assert_eq!(fx.stats.backfill_copy_rows.load(Ordering::Relaxed), 0);
    assert_eq!(fx.ch.query("EXISTS default.t__wsstg").unwrap(), "0");

    fx.source.psql_one("DELETE FROM public.t WHERE id = 2; UPDATE public.t SET name = 'updated' WHERE id = 1; INSERT INTO public.t VALUES (4, 'four'); CHECKPOINT").unwrap();
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 200)
        .await;
    assert_eq!(backfiller.pending_count(), 0);
    assert_eq!(fx.rows(), "1\tone\n2\ttwo\n3\tthree");
    backfiller.note_opt_out(&fx.desc.rel_name).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 200)
        .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(fx.rows(), "1\tupdated\n3\tthree\n4\tfour");
    assert!(
        fx.stats
            .backfill_backup_walk
            .tuples_emitted
            .load(Ordering::Relaxed)
            > walked
    );
    assert!(
        fx.stats
            .backfill_backup_pump
            .bytes_tapped
            .load(Ordering::Relaxed)
            > tapped
    );
    assert_eq!(
        fx.ch
            .query("SELECT uniqExact(_lsn), min(_lsn) FROM default.t FINAL")
            .unwrap(),
        "1\t200"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_backfill_resumes_each_publish_phase() {
    if !fx::tools::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    let mut session = StagingSession::connect(walshadow::config::DestEmitter::new(
        Arc::new(fx.emitter.clone()),
        None,
    ))
    .await
    .unwrap();
    let system_id = fx.system_id();
    for phase in [
        "before_exchange",
        "after_exchange",
        "after_copy_back",
        "after_drop",
    ] {
        let dir = tempfile::tempdir().unwrap();
        fx.ch.query("TRUNCATE TABLE default.t").unwrap();
        fx.ch.query("INSERT INTO default.t (id, name, _lsn, _is_deleted) VALUES (9, 'stale', 99, false), (8, 'boundary', 100, false), (2, 'live', 101, false), (3, 'deleted', 102, true)").unwrap();
        write_held_pending(&fx, dir.path());
        let rel = fx.prepare_staging().await;
        fx.ch.query("INSERT INTO default.t__wsstg (id, name, _lsn) VALUES (1, 'snapshot', 100), (2, 'old', 100), (3, 'old', 100)").unwrap();
        let uuid = session
            .table_uuid("default", "t__wsstg")
            .await
            .unwrap()
            .unwrap();
        write_swapped_ledger(dir.path(), &uuid);
        if phase != "before_exchange" {
            fx.ch
                .query("EXCHANGE TABLES default.t AND default.t__wsstg")
                .unwrap();
            assert_eq!(
                session.table_uuid("default", "t").await.unwrap().as_deref(),
                Some(uuid.as_str())
            );
        }
        if phase == "after_exchange" {
            fx.ch
                .query("ALTER TABLE default.t ADD COLUMN extra String DEFAULT 'new'")
                .unwrap();
        }
        if matches!(phase, "after_copy_back" | "after_drop") {
            session.copy_back(&rel).await.unwrap();
        }
        if phase == "after_drop" {
            session.drop_staging(&rel).await.unwrap();
        }
        let backfiller = fx.backfiller(dir.path()).await;
        assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
        backfiller
            .note_opt_in(&fx.desc, InitialLoadMode::Copy, 999)
            .await;
        wait_done(&backfiller, dir.path()).await;
        // Done mark lands before pending rows settle
        tokio::time::timeout(Duration::from_secs(10), async {
            while !PendingLedger::load(dir.path(), system_id)
                .await
                .unwrap()
                .is_empty()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("pending rows settle");
        assert_eq!(fx.rows(), "1\tsnapshot\n2\tlive\n5\tpending", "{phase}");
        assert_eq!(
            fx.ch
                .query("SELECT id, max(_lsn), argMax(_is_deleted, _lsn) FROM default.t GROUP BY id ORDER BY id")
                .unwrap(),
            "1\t100\tfalse\n2\t101\tfalse\n3\t102\ttrue\n5\t100\tfalse",
            "{phase}"
        );
        assert_eq!(fx.ch.query("EXISTS default.t__wspending").unwrap(), "0");
        assert_eq!(
            session.table_uuid("default", "t__wsstg").await.unwrap(),
            None
        );
        if phase == "after_exchange" {
            assert_eq!(
                fx.ch
                    .query("SELECT uniqExact(extra), any(extra) FROM default.t FINAL")
                    .unwrap(),
                "1\tnew"
            );
            fx.ch
                .query("ALTER TABLE default.t DROP COLUMN extra")
                .unwrap();
        }
        let restarted = fx.backfiller(dir.path()).await;
        restarted
            .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 1000)
            .await;
        assert_eq!(restarted.pending_count(), 0);
        assert_eq!(fx.rows(), "1\tsnapshot\n2\tlive\n5\tpending", "{phase}");
    }
}

fn read_ledger(dir: &Path) -> toml::Value {
    toml::from_str(&std::fs::read_to_string(dir.join("backfills.toml")).unwrap()).unwrap()
}

/// Pass held its pending rows before EXCHANGE; xid 500 committed meanwhile
fn write_held_pending(fx: &Fixture, dir: &Path) {
    fx.ch
        .query("CREATE TABLE default.t__wspending AS default.t ENGINE = MergeTree ORDER BY tuple() PRIMARY KEY tuple() PARTITION BY tuple()")
        .unwrap();
    fx.ch
        .query("ALTER TABLE default.t__wspending ADD COLUMN _ws_xmin UInt32, ADD COLUMN _ws_xmax UInt32, ADD COLUMN _ws_infomask UInt16")
        .unwrap();
    fx.ch
        .query("INSERT INTO default.t__wspending (id, name, _lsn, _ws_xmin) VALUES (5, 'pending', 100, 500)")
        .unwrap();
    std::fs::write(
        walshadow::visibility_pending::ledger_path(dir),
        format!(
            "version = 1\nsystem_id = {}\n[[carry]]\nnamespace = 'public'\nrelname = 't'\n\
             database = 'default'\ntable = 't'\nstart_lsn = '0/64'\ncommitted = [500]\n\
             held = true\n",
            fx.system_id()
        ),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_discards_held_pending_without_swap() {
    if !fx::tools::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    // Crash after hold, before swap mark, then opt-out drops the backfill entry
    write_held_pending(&fx, dir.path());
    fx.backfiller(dir.path()).await;
    assert!(
        PendingLedger::load(dir.path(), fx.system_id())
            .await
            .unwrap()
            .is_empty()
    );
}

fn write_swapped_ledger(dir: &Path, uuid: &str) {
    std::fs::write(dir.join("backfills.toml"), format!("version = 1\n[[backfill]]\nnamespace = 'public'\nrelname = 't'\ns_lsn = '0/64'\ndone = false\nmode = 'base_backup'\nswapped = true\nstaging_uuid = '{uuid}'\n")).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_schema_change_discards_load_and_keeps_retry_pending() {
    if !fx::tools::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    fx.ch
        .query("INSERT INTO default.t (id, name, _lsn) VALUES (9, 'live', 101)")
        .unwrap();
    fx.prepare_staging().await;
    fx.ch
        .query("INSERT INTO default.t__wsstg (id, name, _lsn) VALUES (8, 'discard', 100)")
        .unwrap();
    let mut session = StagingSession::connect(walshadow::config::DestEmitter::new(
        Arc::new(fx.emitter.clone()),
        None,
    ))
    .await
    .unwrap();
    let uuid = session
        .table_uuid("default", "t__wsstg")
        .await
        .unwrap()
        .unwrap();
    write_swapped_ledger(dir.path(), &uuid);
    write_held_pending(&fx, dir.path());
    fx.ch
        .query("ALTER TABLE default.t ADD COLUMN extra String DEFAULT 'new'")
        .unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 999)
        .await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while read_ledger(dir.path())["backfill"][0]["swapped"].as_bool() != Some(false) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("schema mismatch clears swap phase");
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["done"].as_bool(),
        Some(false)
    );
    assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
    assert_eq!(fx.rows(), "9\tlive");
    assert!(
        PendingLedger::load(dir.path(), fx.system_id())
            .await
            .unwrap()
            .is_empty(),
        "discarded load never promotes its pending rows"
    );
    assert_eq!(
        session.table_uuid("default", "t__wsstg").await.unwrap(),
        None
    );
    let restarted = fx.backfiller(dir.path()).await;
    restarted
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 999)
        .await;
    wait_done(&restarted, dir.path()).await;
    assert_eq!(fx.rows(), "1\tone\n2\ttwo\n3\tthree\n9\tlive");
    assert_eq!(
        fx.ch
            .query("SELECT uniqExact(extra), any(extra) FROM default.t FINAL")
            .unwrap(),
        "1\tnew"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumed_copy_preserves_boundary_and_newer_live_rows() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut fx = Fixture::new().await;
    fx.emitter.bootstrap.copy_fallback = Some(false);
    let dir = tempfile::tempdir().unwrap();
    fx.source.psql_one("ALTER TABLE public.t ALTER COLUMN name SET STORAGE EXTERNAL; UPDATE public.t SET name = repeat('external', 1000) WHERE id = 1").unwrap();
    fx.ch
        .query("INSERT INTO default.t (id, name, _lsn) VALUES (2, 'live', 200)")
        .unwrap();
    std::fs::write(
        dir.path().join("backfills.toml"),
        r#"version = 1
[[backfill]]
namespace = "public"
relname = "t"
s_lsn = "0/64"
done = false
mode = "copy"
swapped = false
"#,
    )
    .unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::ObjectStore, 300)
        .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(
        fx.ch
            .query("SELECT id, length(name), _lsn FROM default.t FINAL ORDER BY id")
            .unwrap(),
        "1\t8000\t100\n2\t4\t200\n3\t5\t100"
    );
    let ledger = read_ledger(dir.path());
    assert_eq!(ledger["backfill"][0]["mode"].as_str(), Some("copy"));
    assert_eq!(ledger["backfill"][0]["s_lsn"].as_str(), Some("0/64"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_table_backfill_finishes() {
    if !fx::tools::requirements_available() {
        return;
    }
    for mode in [InitialLoadMode::Copy, InitialLoadMode::ObjectStore] {
        let fx = Fixture::new().await;
        let dir = tempfile::tempdir().unwrap();
        let backfiller = fx.backfiller(dir.path()).await;
        fx.source.psql_one("DROP TABLE public.t").unwrap();
        backfiller.note_opt_in(&fx.desc, mode, 100).await;
        wait_done(&backfiller, dir.path()).await;
        assert_eq!(fx.backfiller(dir.path()).await.pending_count(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_backup_defaults_to_copy_at_original_boundary() {
    if !fx::tools::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    fx.ch
        .query("INSERT INTO default.t (id, name, _lsn) VALUES (2, 'live', 200)")
        .unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::ObjectStore, 100)
        .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(fx.rows(), "1\tone\n2\tlive\n3\tthree");
    assert_eq!(
        fx.stats
            .backfill_copy_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );
    assert_eq!(
        fx.stats
            .backfill_copy_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        23
    );
    let ledger = read_ledger(dir.path());
    assert_eq!(ledger["backfill"][0]["mode"].as_str(), Some("copy"));
    assert_eq!(ledger["backfill"][0]["s_lsn"].as_str(), Some("0/64"));
    assert_eq!(
        fx.ch
            .query("SELECT id, _lsn FROM default.t FINAL ORDER BY id")
            .unwrap(),
        "1\t100\n2\t200\n3\t100"
    );
}

/// Pending tables a backup pass records settle xids live apply already
/// passed from source `pg_xact`, widened to the current epoch; running xids
/// stay outstanding for live commits to fold
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_decides_ended_pending_xids() {
    if !fx::tools::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    let cfg = fx::pg_cfg(&fx.source, "pending-xids");
    let client = walshadow::source_feed::open_sql_client(&cfg).await.unwrap();
    let running = walshadow::source_feed::open_sql_client(&cfg).await.unwrap();
    let xid = |rows: Vec<tokio_postgres::SimpleQueryMessage>| -> u32 {
        rows.into_iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => {
                    Some(r.get(0).unwrap().parse::<u64>().unwrap() as u32)
                }
                _ => None,
            })
            .unwrap()
    };
    let committed = xid(client.simple_query("SELECT txid_current()").await.unwrap());
    let aborted = xid(client
        .simple_query("BEGIN; SELECT txid_current()")
        .await
        .unwrap());
    client.batch_execute("ROLLBACK").await.unwrap();
    let open = xid(running
        .simple_query("BEGIN; SELECT txid_current()")
        .await
        .unwrap());
    let mut got = walshadow::copy_backfill::xid_outcomes(&client, &[committed, aborted, open])
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![(committed, true), (aborted, false)]);
    running.batch_execute("COMMIT").await.unwrap();
}

/// Fallback COPY reads detoasted rows, yet later updates carrying a pointer
/// unchanged resolve only from the chunk mirror, so it copies the TOAST heap
/// too, versioned at the load boundary
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_backup_copy_fallback_seeds_chunk_mirror() {
    if !fx::tools::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    fx.source
        .psql_one(
            "ALTER TABLE public.t ALTER COLUMN name SET STORAGE EXTERNAL; \
             UPDATE public.t SET name = repeat('external', 1000) WHERE id = 1",
        )
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::ObjectStore, 100)
        .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(
        fx.ch
            .query(&format!(
                "SELECT count(), sum(length(chunk_data)), min(_lsn), max(_lsn) \
                 FROM default.{} FINAL",
                walshadow::toast::mirror_table_name(fx.desc.rfn.db_node, fx.desc.toast_oid)
            ))
            .unwrap(),
        "5\t8000\t100\t100"
    );
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["toast_seeded"].as_bool(),
        Some(true)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_backup_can_disable_copy_fallback() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut fx = Fixture::new().await;
    fx.emitter.bootstrap.copy_fallback = Some(false);
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::ObjectStore, 100)
        .await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while fx.ch.query("EXISTS default.t__wsstg").unwrap() != "1" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(backfiller.pending_by_mode(), [0, 0, 1]);
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["mode"].as_str(),
        Some("object_store")
    );
    assert_eq!(fx.rows(), "");
}

/// Archive every WAL file in source's `pg_wal`, current segment included, so
/// the gap through the opt-in boundary is fetchable
async fn archive_wal(
    source: &Shadow,
    settings: &walrus::config::Settings,
    storage: &walrus::storage::DynStorage,
) {
    let pg_wal = source.config().data_dir.join("pg_wal");
    for entry in std::fs::read_dir(pg_wal).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if name.len() == 24 && name.chars().all(|c| c.is_ascii_hexdigit()) {
            walrus::pg::wal::push::handle(settings, storage.clone(), &path)
                .await
                .unwrap();
        }
    }
}

/// Backup predates the opt-in: walk loads the backup image, gap replay
/// carries changes between backup redo and the boundary
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_store_opt_in_replays_archive_gap() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut fx = Fixture::new().await;
    // Live capture fills the log in production; replay decodes through it
    fx.log
        .seed(
            BatchRecord {
                captured_at: 0x1000,
                commit_lsn: 0,
                observations: Vec::new(),
                ambiguities: Vec::new(),
                entries: vec![Arc::new(LogEntry {
                    valid_from: 0x1000,
                    oid: fx.desc.oid,
                    rfn: fx.desc.rfn,
                    value: LogValue::Present(fx.desc.clone()),
                })],
            },
            0x1000,
        )
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let storage: walrus::storage::DynStorage =
        Arc::new(walrus::storage::fs::FsStorage::new(root.path()).unwrap());
    let settings = walrus::config::Settings {
        storage: walrus::config::StorageSettings::Fs {
            path: root.path().to_string_lossy().into_owned(),
        },
        compression: walrus::compression::Method::None,
        compression_level: 0,
        ..Default::default()
    };
    walrus::pg::backup::push::handle(
        &settings,
        storage.clone(),
        walrus::pg::backup::push::PushArgs::default(),
        fx::pg_cfg(&fx.source, "backfill-push"),
    )
    .await
    .unwrap();
    fx.source
        .psql_one(
            "UPDATE public.t SET name = 'updated' WHERE id = 1; \
             DELETE FROM public.t WHERE id = 2; \
             INSERT INTO public.t VALUES (4, 'four')",
        )
        .unwrap();
    let s_lsn = fx.source.psql_one("SELECT pg_current_wal_lsn()").unwrap();
    let s_lsn = walrus::pg::backup::parse_pg_lsn(&s_lsn).unwrap();
    fx.source.psql_one("SELECT pg_switch_wal()").unwrap();
    archive_wal(&fx.source, &settings, &storage).await;
    fx.emitter.backup = Some(settings);

    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::ObjectStore, s_lsn)
        .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(fx.rows(), "1\tupdated\n3\tthree\n4\tfour");
    assert_eq!(fx.stats.backfill_copy_rows.load(Ordering::Relaxed), 0);
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["mode"].as_str(),
        Some("object_store")
    );
}

/// Use `current_thread` runtimes to capture logs from spawned tasks too
#[derive(Clone, Default)]
struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Logs;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Logs {
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Self::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish();
        (logs, tracing::subscriber::set_default(subscriber))
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }

    async fn wait_for(&self, needle: &str) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while !self.text().contains(needle) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("log {needle:?} never appeared:\n{}", self.text()));
    }
}

/// Block ledger writes by occupying temporary file path with directory
fn block_ledger(dir: &Path) {
    std::fs::create_dir(dir.join("backfills.toml.tmp")).unwrap();
}

/// Spread public.t over several heap blocks
fn widen_source(fx: &Fixture) -> u64 {
    fx.source
        .psql_one(
            "INSERT INTO public.t SELECT g, repeat('x', 1000) FROM generate_series(4, 30) g; \
             CHECKPOINT",
        )
        .unwrap();
    let blocks: u64 = fx
        .source
        .psql_one("SELECT pg_relation_size('public.t') / current_setting('block_size')::int8")
        .unwrap()
        .parse()
        .unwrap();
    assert!(blocks >= 3, "{blocks} blocks");
    blocks
}

#[tokio::test(flavor = "current_thread")]
async fn copy_backfill_outlives_ledger_persist_failures() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let mut fx = Fixture::new().await;
    fx.emitter.bootstrap.copy_chunk_blocks = std::num::NonZeroU32::new(1);
    widen_source(&fx);
    let dir = tempfile::tempdir().unwrap();
    block_ledger(dir.path());
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 100)
        .await;
    logs.wait_for("next boot re-COPYs (idempotent)").await;
    assert_eq!(backfiller.pending_count(), 0);
    assert_eq!(
        fx.ch
            .query("SELECT count(), min(_lsn), max(_lsn) FROM default.t FINAL")
            .unwrap(),
        "30\t100\t100"
    );
    for lost in [
        "a crash before completion re-streams without backfill",
        "restart re-seeds the chunk mirror",
        "COPY cursor persist failed; restart repeats the chunk",
    ] {
        assert!(logs.text().contains(lost), "{lost}:\n{}", logs.text());
    }
    assert!(!dir.path().join("backfills.toml").exists());

    backfiller.note_opt_out(&fx.desc.rel_name).await;
    assert!(
        logs.text()
            .contains("backfill ledger persist failed on opt-out")
    );
    // Opt out during coalescing to cancel queued request
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 300)
        .await;
    assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
    backfiller.note_opt_out(&fx.desc.rel_name).await;
    assert_eq!(backfiller.pending_by_mode(), [0, 0, 0]);
    // Repeat finds nothing queued
    backfiller.note_opt_out(&fx.desc.rel_name).await;
    assert_eq!(backfiller.pending_by_mode(), [0, 0, 0]);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(fx.ch.query("EXISTS default.t__wsstg").unwrap(), "0");
    assert_eq!(
        fx.stats
            .backfill_backup_walk
            .tuples_emitted
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_resumes_from_cursor_and_skips_empty_table() {
    if !fx::tools::requirements_available() {
        return;
    }
    let mut fx = Fixture::new().await;
    fx.emitter.bootstrap.copy_chunk_blocks = std::num::NonZeroU32::new(1);
    widen_source(&fx);
    // Chunk over an emptied block copies nothing
    fx.source
        .psql_one("DELETE FROM public.t WHERE ctid >= '(2,0)'::tid AND ctid < '(3,0)'::tid")
        .unwrap();
    let filenode = fx
        .source
        .psql_one("SELECT relfilenode FROM pg_class WHERE oid = 'public.t'::regclass")
        .unwrap();
    let past_first: u64 = fx
        .source
        .psql_one("SELECT count(*) FROM public.t WHERE ctid >= '(1,0)'::tid")
        .unwrap()
        .parse()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("backfills.toml"),
        format!(
            "version = 1\n[[backfill]]\nnamespace = 'public'\nrelname = 't'\n\
             s_lsn = '0/64'\ndone = false\nmode = 'copy'\n\
             copy_relfilenode = {filenode}\ncopy_next_block = 1\n"
        ),
    )
    .unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 999)
        .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(
        fx.stats.backfill_copy_rows.load(Ordering::Relaxed),
        past_first
    );
    assert_eq!(
        fx.ch
            .query("SELECT count(), min(_lsn), max(_lsn), countIf(id = 1) FROM default.t FINAL")
            .unwrap(),
        format!("{past_first}\t100\t100\t0"),
        "resume must skip first block"
    );

    fx.source.psql_one("DELETE FROM public.t").unwrap();
    backfiller.note_opt_out(&fx.desc.rel_name).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 2000)
        .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(
        fx.stats.backfill_copy_rows.load(Ordering::Relaxed),
        past_first
    );
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["s_lsn"].as_str(),
        Some("0/7D0")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unreadable_source_rows_leave_copy_pending() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let mut fx = Fixture::new().await;
    fx.source
        .psql_one("CREATE ROLE ws_narrow LOGIN; GRANT SELECT (id) ON public.t TO ws_narrow")
        .unwrap();
    let mut pg = fx::pg_cfg(&fx.source, "backfill-narrow");
    pg.user = "ws_narrow".into();
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller_as(dir.path(), pg.clone()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 100)
        .await;
    assert!(
        logs.text()
            .contains("grant pg_read_all_data to the source role"),
        "{}",
        logs.text()
    );
    assert_eq!(backfiller.pending_by_mode(), [1, 0, 0]);
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["done"].as_bool(),
        Some(false)
    );

    // Disable TOAST seeding to reach COPY's column permission check
    fx.emitter.toast.mode = walshadow::ch_emitter::ToastMode::Disabled;
    let restarted = fx.backfiller_as(dir.path(), pg.clone()).await;
    restarted
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 100)
        .await;
    logs.wait_for("permission denied for table t").await;
    assert_eq!(restarted.pending_by_mode(), [1, 0, 0]);
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["done"].as_bool(),
        Some(false)
    );
    assert_eq!(fx.rows(), "");

    // Unreachable source cannot prove a drop, so entry stays pending
    fx.source.psql_one("ALTER ROLE ws_narrow NOLOGIN").unwrap();
    let refused = fx.backfiller_as(dir.path(), pg).await;
    refused
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 100)
        .await;
    logs.wait_for("backfill: source sql connect").await;
    assert_eq!(refused.pending_by_mode(), [1, 0, 0]);
}

#[tokio::test(flavor = "current_thread")]
async fn swap_withheld_when_ledger_cannot_record_it() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let fx = Fixture::new().await;
    fx.ch
        .query("INSERT INTO default.t (id, name, _lsn) VALUES (9, 'live', 101)")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    block_ledger(dir.path());
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 100)
        .await;
    logs.wait_for("staging swap failed; boot resumes from the recorded phase")
        .await;
    assert!(
        logs.text()
            .contains("exchange withheld, entry stays pending")
    );
    assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
    assert_eq!(fx.rows(), "9\tlive");
    assert_eq!(
        fx.ch
            .query("SELECT count() FROM default.t__wsstg FINAL")
            .unwrap(),
        "3"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unrecorded_copy_fallback_keeps_backup_mode_pending() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let fx = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    block_ledger(dir.path());
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::ObjectStore, 100)
        .await;
    logs.wait_for("COPY fallback ledger persist failed; entry stays pending")
        .await;
    assert_eq!(backfiller.pending_by_mode(), [0, 0, 1]);
    assert_eq!(fx.stats.backfill_copy_rows.load(Ordering::Relaxed), 0);
    assert_eq!(fx.rows(), "");
}

#[tokio::test(flavor = "current_thread")]
async fn unmapped_rel_finishes_load_and_strands_swap() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let fx = Fixture::new().await;
    fx.mapping.mutate(|map| {
        Arc::make_mut(map).remove(&fx.desc.rel_name);
    });
    let dir = tempfile::tempdir().unwrap();
    block_ledger(dir.path());
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 100)
        .await;
    logs.wait_for("backfill ledger persist failed; next boot resumes (idempotent)")
        .await;
    assert!(
        logs.text()
            .contains("no mapping at pass start; rows will skip")
    );
    assert_eq!(backfiller.pending_by_mode(), [0, 0, 0]);
    assert_eq!(fx.ch.query("EXISTS default.t__wsstg").unwrap(), "0");
    assert_eq!(fx.rows(), "");

    let swapped = tempfile::tempdir().unwrap();
    write_swapped_ledger(swapped.path(), "gone-uuid");
    let backfiller = fx.backfiller(swapped.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 999)
        .await;
    logs.wait_for("swapped entry public.t unmapped; staging table orphaned")
        .await;
    assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
    assert_eq!(
        read_ledger(swapped.path())["backfill"][0]["staging_uuid"].as_str(),
        Some("gone-uuid")
    );

    // Recorded staging target outlives mapping; copy-back already ran
    let recorded = tempfile::tempdir().unwrap();
    write_swapped_ledger(recorded.path(), "gone-uuid");
    let mut ledger = std::fs::read_to_string(recorded.path().join("backfills.toml")).unwrap();
    ledger.push_str("staging_target = { database = 'default', table = 't' }\n");
    std::fs::write(recorded.path().join("backfills.toml"), ledger).unwrap();
    let backfiller = fx.backfiller(recorded.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 999)
        .await;
    wait_done(&backfiller, recorded.path()).await;
}

impl Logs {
    fn count(&self, needle: &str) -> usize {
        self.text().matches(needle).count()
    }

    async fn wait_for_count(&self, needle: &str, want: usize) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while self.count(needle) < want {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("log {needle:?} never reached {want}:\n{}", self.text()));
    }
}

const PERSIST_RETRY: &str = "pending visibility ledger persist failed; retrying";
const PASS_DONE: &str = "backup backfill pass complete";

/// Start a backup pass and wait until it retries persisting its pending
/// hold; removing the returned path lets it publish
async fn stall_at_pending_hold(
    fx: &Fixture,
    backfiller: &Arc<CopyBackfiller>,
    dir: &Path,
    logs: &Logs,
) -> std::path::PathBuf {
    let stalls = logs.count(PERSIST_RETRY);
    let blocker = walshadow::visibility_pending::ledger_path(dir).with_extension("toml.tmp");
    std::fs::create_dir(&blocker).unwrap();
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 100)
        .await;
    logs.wait_for_count(PERSIST_RETRY, stalls + 1).await;
    blocker
}

/// Run `between` while the pass holds its pending rows, then let it publish
async fn pass_through_held_pending(
    fx: &Fixture,
    backfiller: &Arc<CopyBackfiller>,
    dir: &Path,
    logs: &Logs,
    between: impl AsyncFnOnce(),
) {
    let passes = logs.count(PASS_DONE);
    let blocker = stall_at_pending_hold(fx, backfiller, dir, logs).await;
    between().await;
    std::fs::remove_dir(&blocker).unwrap();
    logs.wait_for_count(PASS_DONE, passes + 1).await;
}

/// Wait for EXCHANGE to move a new table under `database.table`
async fn wait_exchanged(sess: &mut StagingSession, database: &str, table: &str) {
    let before = sess.table_uuid(database, table).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while sess.table_uuid(database, table).await.unwrap() == before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("exchange applies");
}

async fn staging_session(fx: &Fixture) -> StagingSession {
    let dest = walshadow::config::DestEmitter::new(Arc::new(fx.emitter.clone()), None);
    StagingSession::connect(dest).await.unwrap()
}

/// Source transactions open across the backup leave their rows pending; a
/// pass discarding its load drops them with it, a published one promotes
/// what the source decided while the pass held them
#[tokio::test(flavor = "current_thread")]
async fn backup_pass_holds_open_xact_rows_until_source_decides() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let fx = Fixture::new().await;
    let cfg = fx::pg_cfg(&fx.source, "pending-open");
    let kept = walshadow::source_feed::open_sql_client(&cfg).await.unwrap();
    let rolled = walshadow::source_feed::open_sql_client(&cfg).await.unwrap();
    for (client, from, tag) in [(&kept, 101, "kept"), (&rolled, 201, "rolled")] {
        client
            .batch_execute(&format!(
                "BEGIN; INSERT INTO public.t SELECT g, '{tag}' FROM generate_series({from}, {}) g",
                from + 9
            ))
            .await
            .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    let pending_empty = async || {
        PendingLedger::load(dir.path(), fx.system_id())
            .await
            .unwrap()
            .is_empty()
    };

    // Opt-out mid-pass: staging dropped, held rows discarded
    let mapping = fx.mapping.snapshot()[&fx.desc.rel_name].clone();
    pass_through_held_pending(&fx, &backfiller, dir.path(), &logs, async || {
        fx.mapping.mutate(|map| {
            Arc::make_mut(map).remove(&fx.desc.rel_name);
        });
    })
    .await;
    assert!(
        logs.text()
            .contains("rel unmapped at publish (opt-out mid-pass)")
    );
    assert!(pending_empty().await);
    assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
    fx.mapping.mutate(|map| {
        Arc::make_mut(map).insert(fx.desc.rel_name.clone(), mapping.clone());
    });

    // Destination DDL mid-pass: load discarded, entry stays pending
    pass_through_held_pending(&fx, &backfiller, dir.path(), &logs, async || {
        fx.ch
            .query("ALTER TABLE default.t ADD COLUMN extra String DEFAULT 'new'")
            .unwrap();
    })
    .await;
    assert!(logs.text().contains("destination schema changed mid-pass"));
    assert!(pending_empty().await);
    assert_eq!(backfiller.pending_by_mode(), [0, 1, 0]);
    assert_eq!(fx.rows(), "");
    fx.ch
        .query("ALTER TABLE default.t DROP COLUMN extra")
        .unwrap();

    // Both xids end while the pass holds their rows; release asks the source
    pass_through_held_pending(&fx, &backfiller, dir.path(), &logs, async || {
        kept.batch_execute("COMMIT").await.unwrap();
        rolled.batch_execute("ROLLBACK").await.unwrap();
    })
    .await;
    wait_done(&backfiller, dir.path()).await;
    assert_eq!(fx.stats.pending_xacts_settled.load(Ordering::Relaxed), 2);
    assert_eq!(
        fx.ch
            .query("SELECT name, count() FROM default.t FINAL WHERE NOT _is_deleted GROUP BY name ORDER BY name")
            .unwrap(),
        "kept\t10\none\t1\nthree\t1\ntwo\t1"
    );
    assert_eq!(fx.ch.query("EXISTS default.t__wspending").unwrap(), "0");
}

/// Wait for COPY cursor to pass first block of filenode other than `old`
async fn wait_cursor_past(dir: &Path, old: Option<i64>) -> i64 {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(dir.join("backfills.toml"))
                && let Ok(ledger) = toml::from_str::<toml::Value>(&text)
                && let Some(node) = ledger["backfill"][0]
                    .get("copy_relfilenode")
                    .and_then(toml::Value::as_integer)
                && Some(node) != old
            {
                return node;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("COPY cursor advances")
}

/// CLUSTER between chunks relocates rows, so COPY restarts from block 0 once;
/// a second rewrite fails the load, leaving it pending for the next boot
#[tokio::test(flavor = "current_thread")]
async fn rewrite_between_chunks_restarts_copy_once() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let mut fx = Fixture::new().await;
    fx.emitter.bootstrap.copy_chunk_blocks = std::num::NonZeroU32::new(1);
    fx.emitter.row_budget = 1 << 16;
    widen_many(&fx);
    let client = walshadow::source_feed::open_sql_client(&fx::pg_cfg(&fx.source, "rewriter"))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 100)
        .await;
    let first = wait_cursor_past(dir.path(), None).await;
    client
        .batch_execute("CLUSTER public.t USING t_pkey")
        .await
        .unwrap();
    logs.wait_for("relation rewritten mid-COPY; restarting from first block")
        .await;
    wait_cursor_past(dir.path(), Some(first)).await;
    client
        .batch_execute("CLUSTER public.t USING t_pkey")
        .await
        .unwrap();
    logs.wait_for("relation rewritten twice during COPY").await;
    assert_eq!(backfiller.pending_by_mode(), [1, 0, 0]);

    let restarted = fx.backfiller(dir.path()).await;
    restarted
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 100)
        .await;
    wait_done(&restarted, dir.path()).await;
    assert_eq!(
        fx.ch
            .query("SELECT count(), min(_lsn), max(_lsn) FROM default.t FINAL")
            .unwrap(),
        "120\t100\t100"
    );
    assert!(fx.stats.backfill_copy_rows.load(Ordering::Relaxed) > 120);
}

/// Backup opt-ins inside one coalesce window ride a single pass
#[tokio::test(flavor = "current_thread")]
async fn backup_opt_ins_coalesce_into_one_pass() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let fx = Fixture::new().await;
    fx.source
        .psql_one(
            "CREATE TABLE public.u (id int PRIMARY KEY, name text NOT NULL); \
             INSERT INTO public.u VALUES (7, 'seven'); CHECKPOINT",
        )
        .unwrap();
    let (_, descs) = fx
        .catalog
        .lock()
        .await
        .fetch_all_descriptors()
        .await
        .unwrap();
    let u = Arc::new(
        descs
            .into_iter()
            .find(|d| d.rel_name == RelName::new("public", "u"))
            .unwrap(),
    );
    fx::create_ch_dest_table(&fx.ch, "default", "u").unwrap();
    let mut mapping = (*fx.mapping.snapshot()[&fx.desc.rel_name]).clone();
    mapping.target = TableTarget::new("default", "u");
    fx.mapping.mutate(|map| {
        Arc::make_mut(map).insert(u.rel_name.clone(), Arc::new(mapping));
    });
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    let mut sess = staging_session(&fx).await;
    for desc in [&fx.desc, &u] {
        backfiller
            .note_opt_in(desc, InitialLoadMode::BaseBackup, 100)
            .await;
    }
    assert_eq!(backfiller.pending_by_mode(), [0, 2, 0]);
    // Opt-out between exchange and copy-back finishes the publish unrecorded
    wait_exchanged(&mut sess, "default", "u").await;
    backfiller.note_opt_out(&u.rel_name).await;
    logs.wait_for(PASS_DONE).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while backfiller.pending_count() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both loads publish");
    assert_eq!(logs.count(PASS_DONE), 1);
    assert!(logs.text().contains("tables=2"), "{}", logs.text());
    assert_eq!(fx.rows(), "1\tone\n2\ttwo\n3\tthree");
    assert_eq!(
        fx.ch.query("SELECT id, name FROM default.u FINAL").unwrap(),
        "7\tseven"
    );
    let ledger = read_ledger(dir.path());
    assert_eq!(ledger["backfill"].as_array().unwrap().len(), 1);
    assert_eq!(ledger["backfill"][0]["relname"].as_str(), Some("t"));
}

/// Unreachable source leaves held rows outstanding past publish; a failed
/// copy-back keeps the swap, and boot's resume settles what source decided
#[tokio::test(flavor = "current_thread")]
async fn pending_rows_outlast_unreachable_source_and_failed_copy_back() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let fx = Fixture::new().await;
    fx.source
        .psql_one("CREATE ROLE ws_pass LOGIN SUPERUSER REPLICATION")
        .unwrap();
    let mut pg = fx::pg_cfg(&fx.source, "pending-pass");
    pg.user = "ws_pass".into();
    let open = walshadow::source_feed::open_sql_client(&fx::pg_cfg(&fx.source, "pending-open"))
        .await
        .unwrap();
    open.batch_execute(
        "BEGIN; INSERT INTO public.t SELECT g, 'kept' FROM generate_series(101, 110) g",
    )
    .await
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller_as(dir.path(), pg.clone()).await;
    let mut sess = staging_session(&fx).await;
    let blocker = stall_at_pending_hold(&fx, &backfiller, dir.path(), &logs).await;
    fx.source.psql_one("ALTER ROLE ws_pass NOLOGIN").unwrap();
    std::fs::remove_dir(&blocker).unwrap();
    wait_exchanged(&mut sess, "default", "t").await;
    fx.ch.query("DROP TABLE default.t__wsstg").unwrap();
    logs.wait_for("staging copy-back failed; entry stays swapped")
        .await;
    logs.wait_for("pending visibility source check failed")
        .await;
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["swapped"].as_bool(),
        Some(true)
    );
    let held = PendingLedger::load(dir.path(), fx.system_id())
        .await
        .unwrap();
    assert_eq!(held.outstanding().len(), 1);
    assert_eq!(fx.rows(), "1\tone\n2\ttwo\n3\tthree");

    open.batch_execute("COMMIT").await.unwrap();
    fx.source.psql_one("ALTER ROLE ws_pass LOGIN").unwrap();
    let restarted = fx.backfiller_as(dir.path(), pg).await;
    restarted
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 100)
        .await;
    wait_done(&restarted, dir.path()).await;
    assert_eq!(
        fx.ch
            .query("SELECT count() FROM default.t FINAL WHERE name = 'kept'")
            .unwrap(),
        "10"
    );
}

/// Unrecorded swap clear keeps staging, so boot re-checks it rather than
/// reading missing staging as published
#[tokio::test(flavor = "current_thread")]
async fn unrecorded_swap_clear_keeps_staging() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let fx = Fixture::new().await;
    let dir = tempfile::tempdir().unwrap();
    fx.prepare_staging().await;
    let mut sess = staging_session(&fx).await;
    let uuid = sess.table_uuid("default", "t__wsstg").await.unwrap();
    write_swapped_ledger(dir.path(), uuid.as_deref().unwrap());
    // Stamped, so load needs no upgrade write
    let ledger = std::fs::read_to_string(dir.path().join("backfills.toml")).unwrap();
    std::fs::write(
        dir.path().join("backfills.toml"),
        format!("system_id = {}\n{ledger}", fx.system_id()),
    )
    .unwrap();
    fx.ch
        .query("ALTER TABLE default.t ADD COLUMN extra String DEFAULT 'new'")
        .unwrap();
    block_ledger(dir.path());
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 999)
        .await;
    logs.wait_for("swap clear unrecorded; staging kept for next boot")
        .await;
    assert!(
        logs.text()
            .contains("backfill ledger persist failed on swap clear")
    );
    assert_eq!(sess.table_uuid("default", "t__wsstg").await.unwrap(), uuid);
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["swapped"].as_bool(),
        Some(true)
    );

    std::fs::remove_dir(dir.path().join("backfills.toml.tmp")).unwrap();
    let restarted = fx.backfiller(dir.path()).await;
    restarted
        .note_opt_in(&fx.desc, InitialLoadMode::BaseBackup, 999)
        .await;
    logs.wait_for("load discarded, entry re-pends").await;
    assert_eq!(sess.table_uuid("default", "t__wsstg").await.unwrap(), None);
    assert_eq!(
        read_ledger(dir.path())["backfill"][0]["swapped"].as_bool(),
        Some(false)
    );
    assert_eq!(restarted.pending_by_mode(), [0, 1, 0]);
}

/// Opt-out mid-COPY lets the load drain without recording it done
#[tokio::test(flavor = "current_thread")]
async fn opt_out_mid_copy_leaves_no_entry() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (logs, _guard) = Logs::capture();
    let mut fx = Fixture::new().await;
    fx.emitter.bootstrap.copy_chunk_blocks = std::num::NonZeroU32::new(1);
    fx.emitter.row_budget = 1 << 16;
    widen_many(&fx);
    let dir = tempfile::tempdir().unwrap();
    let backfiller = fx.backfiller(dir.path()).await;
    backfiller
        .note_opt_in(&fx.desc, InitialLoadMode::Copy, 100)
        .await;
    wait_cursor_past(dir.path(), None).await;
    backfiller.note_opt_out(&fx.desc.rel_name).await;
    logs.wait_for("backfill complete; converged once WAL apply passes p_hi")
        .await;
    assert!(
        read_ledger(dir.path())
            .get("backfill")
            .and_then(toml::Value::as_array)
            .is_none_or(Vec::is_empty)
    );
    assert_eq!(backfiller.pending_count(), 0);
}

fn widen_many(fx: &Fixture) {
    fx.source
        .psql_one(
            "INSERT INTO public.t SELECT g, repeat('x', 1000) FROM generate_series(4, 120) g; \
             CHECKPOINT",
        )
        .unwrap();
}
