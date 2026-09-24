#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use walshadow::backfill_staging::{self, StagingRel, StagingSession};
use walshadow::backfill_types::BackupRequest;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::copy_backfill::CopyBackfiller;
use walshadow::desc_log::{DescLogIdentity, DescriptorLog};
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
                    pg_major: bridge.info().unwrap().pg_version_num / 10000,
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
            port: ports.ch_tcp,
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
        let source_major: u32 = self
            .source
            .psql_one("SELECT current_setting('server_version_num')::int / 10000")
            .unwrap()
            .parse()
            .unwrap();
        Arc::new(
            CopyBackfiller::new(
                fx::pg_cfg(&self.source, "backfill-staging"),
                self.emitter.clone(),
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
            Arc::new(self.emitter.clone()),
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
            plan.mapping.snapshot().await[&rel.rel].target.table,
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
    let ledger = read_ledger(dir);
    let entry = &ledger["backfill"][0];
    assert_eq!(entry["done"].as_bool(), Some(true));
    assert_eq!(entry["swapped"].as_bool(), Some(false));
    assert!(entry.get("staging_uuid").is_none());
    assert_eq!(backfiller.pending_by_mode(), [0, 0, 0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_opt_in_replaces_stale_rows_and_reloads_after_opt_out() {
    if !fx::requirements_available() {
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
    if !fx::requirements_available() {
        return;
    }
    let fx = Fixture::new().await;
    let mut session = StagingSession::connect(Arc::new(fx.emitter.clone()))
        .await
        .unwrap();
    for phase in [
        "before_exchange",
        "after_exchange",
        "after_copy_back",
        "after_drop",
    ] {
        let dir = tempfile::tempdir().unwrap();
        fx.ch.query("TRUNCATE TABLE default.t").unwrap();
        fx.ch.query("INSERT INTO default.t (id, name, _lsn, _is_deleted) VALUES (9, 'stale', 99, false), (8, 'boundary', 100, false), (2, 'live', 101, false), (3, 'deleted', 102, true)").unwrap();
        let rel = fx.prepare_staging().await;
        fx.ch.query("INSERT INTO default.t__wsstg (id, name, _lsn) VALUES (1, 'snapshot', 100), (2, 'old', 100), (3, 'old', 100)").unwrap();
        let uuid = session
            .table_uuid("default", "t__wsstg")
            .await
            .unwrap()
            .unwrap();
        write_swapped_ledger(dir.path(), &uuid);
        if phase != "before_exchange" {
            session.exchange(&rel).await.unwrap();
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
        assert_eq!(fx.rows(), "1\tsnapshot\n2\tlive", "{phase}");
        assert_eq!(
            fx.ch
                .query("SELECT id, max(_lsn), argMax(_is_deleted, _lsn) FROM default.t GROUP BY id ORDER BY id")
                .unwrap(),
            "1\t100\tfalse\n2\t101\tfalse\n3\t102\ttrue",
            "{phase}"
        );
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
        assert_eq!(fx.rows(), "1\tsnapshot\n2\tlive", "{phase}");
    }
}

fn read_ledger(dir: &Path) -> toml::Value {
    toml::from_str(&std::fs::read_to_string(dir.join("backfills.toml")).unwrap()).unwrap()
}

fn write_swapped_ledger(dir: &Path, uuid: &str) {
    std::fs::write(dir.join("backfills.toml"), format!("version = 1\n[[backfill]]\nnamespace = 'public'\nrelname = 't'\ns_lsn = '0/64'\ndone = false\nmode = 'base_backup'\nswapped = true\nstaging_uuid = '{uuid}'\n")).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_schema_change_discards_load_and_keeps_retry_pending() {
    if !fx::requirements_available() {
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
    let mut session = StagingSession::connect(Arc::new(fx.emitter.clone()))
        .await
        .unwrap();
    let uuid = session
        .table_uuid("default", "t__wsstg")
        .await
        .unwrap()
        .unwrap();
    write_swapped_ledger(dir.path(), &uuid);
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
    if !fx::requirements_available() {
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
async fn failed_backup_defaults_to_copy_at_original_boundary() {
    if !fx::requirements_available() {
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
    if !fx::requirements_available() {
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
    if !fx::requirements_available() {
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
                 FROM default.pg_toast_{} FINAL",
                fx.desc.toast_oid
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
    if !fx::requirements_available() {
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
