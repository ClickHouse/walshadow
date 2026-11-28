#![cfg(target_os = "linux")]
//! Restore external TOAST values from object store backup and replay WAL gap

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use walshadow::backfill::pass::{PassContext, PassError, PassOutcome};
use walshadow::backfill_types::BackupRequest;
use walshadow::backup_checkpoint::BackupCheckpoint;
use walshadow::copy_backfill::CopyBackfiller;
use walshadow::desc_log::{BatchRecord, DescLogIdentity, DescriptorLog, LogEntry, LogValue};
use walshadow::emit::stats::EmitterStats;
use walshadow::emitter_config::{EmitterConfig, InstanceConfig};
use walshadow::mapping::{
    ColumnMapping, InitialLoadMode, MappingHandle, TableMapping, TableTarget, mapping_handle,
};
use walshadow::schema::{RelDescriptor, RelName};
use walshadow::shadow::Shadow;
use walshadow::timeline::TimelineHistory;
use walshadow::visibility_pending::PendingLedger;

const T_ROWS: &str = "1\t8000\texternal\n2\t9000\tgapgapga";

async fn wait_done(backfiller: &CopyBackfiller) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while backfiller.pending_count() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("backfill completes");
}

/// Archive source WAL, including current segment
async fn archive_wal(
    pg_wal: &Path,
    settings: &walrus::config::Settings,
    storage: &walrus::storage::DynStorage,
) {
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

fn system_id(source: &Shadow) -> u64 {
    source
        .psql_one("SELECT system_identifier FROM pg_control_system()")
        .unwrap()
        .parse()
        .unwrap()
}

/// Log seeded with `descs`, as live catalog capture would supply them
async fn seeded_log(
    dir: &Path,
    pg_major: u32,
    system_id: u64,
    descs: &[&Arc<RelDescriptor>],
) -> Arc<DescriptorLog> {
    std::fs::create_dir_all(dir).unwrap();
    let log = Arc::new(
        DescriptorLog::open(
            dir,
            DescLogIdentity {
                pg_major,
                system_id: system_id.to_string(),
                timeline: 1,
                db_oid: descs[0].rfn.db_node,
                wal_seg_size: 16 << 20,
            },
        )
        .await
        .unwrap(),
    );
    log.seed(
        BatchRecord {
            captured_at: 0x1000,
            commit_lsn: 0,
            observations: Vec::new(),
            ambiguities: Vec::new(),
            entries: descs
                .iter()
                .map(|d| {
                    Arc::new(LogEntry {
                        valid_from: 0x1000,
                        oid: d.oid,
                        rfn: d.rfn,
                        value: LogValue::Present((*d).clone()),
                    })
                })
                .collect(),
        },
        0x1000,
    )
    .await
    .unwrap();
    log
}

fn fs_archive(root: &Path) -> (walrus::config::Settings, walrus::storage::DynStorage) {
    let storage: walrus::storage::DynStorage =
        Arc::new(walrus::storage::fs::FsStorage::new(root).unwrap());
    let settings = walrus::config::Settings {
        storage: walrus::config::StorageSettings::Fs {
            path: root.to_string_lossy().into_owned(),
        },
        compression: walrus::compression::Method::None,
        compression_level: 0,
        ..Default::default()
    };
    (settings, storage)
}

/// `(id, name)` relations mapped onto same-named tables in `default`
fn id_name_mapping(rels: &[&RelName]) -> MappingHandle {
    let column = |src_attnum, name: &str, ty: &str| ColumnMapping {
        src_attnum,
        target_name: name.into(),
        target_type: ty.into(),
        type_pinned: false,
    };
    mapping_handle(
        rels.iter()
            .map(|rel| {
                (
                    (*rel).clone(),
                    TableMapping {
                        target: TableTarget::new("default", &rel.name),
                        columns: vec![column(1, "id", "Int32"), column(2, "name", "String")],
                        tees: Vec::new(),
                    },
                )
            })
            .collect(),
    )
}

fn find_desc(descs: &[RelDescriptor], rel: &RelName) -> Arc<RelDescriptor> {
    Arc::new(descs.iter().find(|d| d.rel_name == *rel).unwrap().clone())
}

/// Heap scan precedes TOAST scan, so defer value resolution until both finish
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_store_pass_resolves_deferred_toast_referrers() {
    if !fx::tools::requirements_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_bridged_source(&tmp);
    let _stop = fx::StopOnDrop { sh: &source };
    source
        .psql_one(
            "CREATE TABLE public.t (id int PRIMARY KEY, name text NOT NULL); \
             ALTER TABLE public.t REPLICA IDENTITY FULL; \
             ALTER TABLE public.t ALTER COLUMN name SET STORAGE EXTERNAL; \
             INSERT INTO public.t VALUES (1, repeat('external', 1000)), (2, 'two'), \
                 (3, repeat('toasted', 900)); \
             CHECKPOINT",
        )
        .unwrap();
    let (bridge, mut catalog) = fx::connect_catalog(&source, "backup-toast").await;
    let (_, descs) = catalog.fetch_all_descriptors().await.unwrap();
    let desc = find_desc(&descs, &RelName::new("public", "t"));
    let toast = Arc::new(descs.into_iter().find(|d| d.oid == desc.toast_oid).unwrap());
    let system_id = system_id(&source);
    let source_major = bridge.info().pg_version_num / 10000;
    let log = seeded_log(
        &tmp.path().join("descriptors"),
        source_major,
        system_id,
        &[&desc, &toast],
    )
    .await;

    let ports = fx::Ports::alloc();
    let ch =
        fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http).unwrap();
    fx::create_ch_dest_table(&ch, "default", "t").unwrap();

    let root = tempfile::tempdir().unwrap();
    let (settings, storage) = fs_archive(root.path());
    walrus::pg::backup::push::handle(
        &settings,
        storage.clone(),
        walrus::pg::backup::push::PushArgs::default(),
        fx::pg_cfg(&source, "backup-toast-push"),
    )
    .await
    .unwrap();
    source
        .psql_one(
            "UPDATE public.t SET name = repeat('gap', 3000) WHERE id = 2; \
             DELETE FROM public.t WHERE id = 3",
        )
        .unwrap();
    let s_lsn = source.psql_one("SELECT pg_current_wal_lsn()").unwrap();
    let s_lsn = walrus::pg::backup::parse_pg_lsn(&s_lsn).unwrap();
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    archive_wal(
        &source.config().data_dir.join("pg_wal"),
        &settings,
        &storage,
    )
    .await;

    let emitter = EmitterConfig {
        conn: InstanceConfig {
            port: ports.ch_tcp,
            ..Default::default()
        },
        inserter_pool_size: 4,
        byte_budget: 1 << 20,
        row_budget: 1,
        backup: Some(settings),
        ..Default::default()
    };
    let mapping = id_name_mapping(&[&desc.rel_name]);
    let stats = Arc::new(EmitterStats::default());
    let dir = tempfile::tempdir().unwrap();
    let backfiller = Arc::new(
        CopyBackfiller::new(
            fx::pg_cfg(&source, "backup-toast"),
            walshadow::config::DestEmitter::new(Arc::new(emitter), None),
            mapping,
            stats.clone(),
            Arc::new(Mutex::new(catalog)),
            log,
            dir.path(),
            None,
            watch::channel(Arc::new(TimelineHistory::root(1))).1,
            None,
            None,
            source_major,
            system_id,
            PendingLedger::load(dir.path(), system_id)
                .await
                .unwrap()
                .shared(),
        )
        .await
        .unwrap(),
    );
    backfiller
        .note_opt_in(&desc, InitialLoadMode::ObjectStore, s_lsn)
        .await;
    wait_done(&backfiller).await;
    assert_eq!(
        ch.query(
            "SELECT id, length(name), substring(name, 1, 8) FROM default.t FINAL \
             WHERE NOT _is_deleted ORDER BY id"
        )
        .unwrap(),
        T_ROWS
    );
    assert_eq!(stats.backfill_copy_rows.load(Ordering::Relaxed), 0);
    let ledger: toml::Value =
        toml::from_str(&std::fs::read_to_string(dir.path().join("backfills.toml")).unwrap())
            .unwrap();
    assert_eq!(ledger["backfill"][0]["mode"].as_str(), Some("object_store"));
}

fn find_control_part(dir: &Path) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if let Some(found) = find_control_part(&path) {
                return Some(found);
            }
        } else if path.to_string_lossy().contains("pg_control") {
            return Some(path);
        }
    }
    None
}

/// Shared pass inputs; each pass takes a fresh checkpoint and dest
struct Passes {
    pg: walrus::pg::replication::conn::PgConfig,
    emitter: EmitterConfig,
    mapping: MappingHandle,
    stats: Arc<EmitterStats>,
    catalog: Arc<Mutex<walshadow::shadow_catalog::ShadowCatalog>>,
    log: Arc<DescriptorLog>,
    scratch: PathBuf,
    source_major: u32,
    reqs: Vec<BackupRequest>,
}

impl Passes {
    fn ctx(&self, emitter: EmitterConfig, checkpoint: BackupCheckpoint) -> PassContext {
        PassContext {
            pg: self.pg.clone(),
            dest: walshadow::config::DestEmitter::new(Arc::new(emitter), None),
            mapping: self.mapping.clone(),
            published: self.mapping.clone(),
            stats: self.stats.clone(),
            catalog: self.catalog.clone(),
            log: self.log.clone(),
            scratch_dir: self.scratch.clone(),
            config_rx: None,
            history_rx: watch::channel(Arc::new(TimelineHistory::root(1))).1,
            budget: None,
            oracle: None,
            source_major: self.source_major,
            checkpoint,
            checkpoint_period: Duration::from_millis(1),
        }
    }

    async fn saved(&self) -> BackupCheckpoint {
        BackupCheckpoint::load(&self.scratch)
            .await
            .unwrap()
            .unwrap()
    }

    async fn run(&self, checkpoint: BackupCheckpoint) -> Result<PassOutcome, PassError> {
        let ctx = self.ctx(self.emitter.clone(), checkpoint);
        walshadow::backup_backfill::run_pass(&ctx, InitialLoadMode::ObjectStore, &self.reqs).await
    }
}

fn walk_stat(stats: &EmitterStats) -> (u64, u64) {
    (
        stats
            .backfill_backup_walk
            .files_resumed
            .load(Ordering::Relaxed),
        stats
            .backfill_backup_walk
            .pages_walked
            .load(Ordering::Relaxed),
    )
}

/// Mapped files a walk tapped and did not resume
fn files_walked(stats: &EmitterStats) -> u64 {
    let w = &stats.backfill_backup_walk;
    w.files_walked.load(Ordering::Relaxed) + w.toast_files_observed.load(Ordering::Relaxed)
        - w.files_resumed.load(Ordering::Relaxed)
}

/// A pass that fails mid-walk resumes past its recorded files; one that
/// finished its walk resumes past the walk; unusable walk spools re-walk
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_store_pass_resumes_from_its_checkpoint() {
    if !fx::tools::requirements_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_bridged_source(&tmp);
    let _stop = fx::StopOnDrop { sh: &source };
    source
        .psql_one(
            "CREATE TABLE public.t (id int PRIMARY KEY, name text NOT NULL); \
             ALTER TABLE public.t REPLICA IDENTITY FULL; \
             ALTER TABLE public.t ALTER COLUMN name SET STORAGE EXTERNAL; \
             INSERT INTO public.t VALUES (1, repeat('external', 1000)), (2, 'two'), \
                 (3, repeat('toasted', 900)); \
             CREATE TABLE public.u (id int PRIMARY KEY, name text NOT NULL); \
             INSERT INTO public.u SELECT g, 'u' || g FROM generate_series(1, 100) g; \
             CHECKPOINT",
        )
        .unwrap();
    let (bridge, mut catalog) = fx::connect_catalog(&source, "backup-resume").await;
    let (_, descs) = catalog.fetch_all_descriptors().await.unwrap();
    let t = find_desc(&descs, &RelName::new("public", "t"));
    let u = find_desc(&descs, &RelName::new("public", "u"));
    let toast = Arc::new(descs.into_iter().find(|d| d.oid == t.toast_oid).unwrap());
    let source_major = bridge.info().pg_version_num / 10000;
    let log = seeded_log(
        &tmp.path().join("descriptors"),
        source_major,
        system_id(&source),
        &[&t, &toast, &u],
    )
    .await;

    let ports = fx::Ports::alloc();
    let ch =
        fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http).unwrap();
    fx::create_ch_dest_table(&ch, "default", "t").unwrap();
    fx::create_ch_dest_table(&ch, "default", "u").unwrap();

    let root = tempfile::tempdir().unwrap();
    let (settings, storage) = fs_archive(root.path());
    // Many parts, so a resumed walk has whole parts to skip
    walrus::pg::backup::push::handle(
        &settings,
        storage.clone(),
        walrus::pg::backup::push::PushArgs {
            tar_size_threshold: 1 << 20,
            ..Default::default()
        },
        fx::pg_cfg(&source, "backup-resume-push"),
    )
    .await
    .unwrap();
    // Gap: TOAST update, delete, and a transaction past 64 subtransactions
    source
        .psql_one(
            "UPDATE public.t SET name = repeat('gap', 3000) WHERE id = 2; \
             DELETE FROM public.t WHERE id = 3; \
             DO $$ BEGIN FOR i IN 101..170 LOOP \
                 BEGIN INSERT INTO public.u VALUES (i, 'u' || i); \
                 EXCEPTION WHEN others THEN RAISE; END; \
             END LOOP; END $$",
        )
        .unwrap();
    // Open across a segment boundary, so a replay checkpoint lands below it
    source
        .psql_one(
            "BEGIN; INSERT INTO public.u VALUES (171, 'u171'); SELECT pg_switch_wal(); \
             INSERT INTO public.u VALUES (172, 'u172'); COMMIT",
        )
        .unwrap();
    let s_lsn = source.psql_one("SELECT pg_current_wal_lsn()").unwrap();
    let s_lsn = walrus::pg::backup::parse_pg_lsn(&s_lsn).unwrap();
    // Past S: the live stream's to deliver, never the gap replay's
    source
        .psql_one("INSERT INTO public.u VALUES (1000, 'past s')")
        .unwrap();
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    archive_wal(
        &source.config().data_dir.join("pg_wal"),
        &settings,
        &storage,
    )
    .await;

    let emitter = EmitterConfig {
        conn: InstanceConfig {
            port: ports.ch_tcp,
            ..Default::default()
        },
        inserter_pool_size: 4,
        backup: Some(settings.clone()),
        bootstrap: walshadow::emitter_config::BootstrapSettings {
            object_store_parallelism: std::num::NonZeroUsize::new(1),
            ..Default::default()
        },
        ..Default::default()
    };
    let mapping = id_name_mapping(&[&t.rel_name, &u.rel_name]);
    let reqs: Vec<_> = [&t, &u]
        .into_iter()
        .map(|d| BackupRequest {
            desc: d.clone(),
            s_lsn,
        })
        .collect();
    let fresh = BackupCheckpoint::new(
        InitialLoadMode::ObjectStore,
        &reqs,
        &mapping.snapshot(),
        &emitter,
        None,
    );
    let stats = Arc::new(EmitterStats::default());
    let spill = tempfile::tempdir().unwrap();
    let mut passes = Passes {
        pg: fx::pg_cfg(&source, "backup-resume"),
        emitter: emitter.clone(),
        mapping,
        stats: stats.clone(),
        catalog: Arc::new(Mutex::new(catalog)),
        log,
        scratch: spill.path().join("backup_backfill"),
        source_major,
        reqs,
    };
    let assert_ch = || {
        assert_eq!(
            ch.query(
                "SELECT id, length(name), substring(name, 1, 8) FROM default.t FINAL \
                 WHERE NOT _is_deleted ORDER BY id"
            )
            .unwrap(),
            T_ROWS
        );
        assert_eq!(
            ch.query("SELECT count(), max(id) FROM default.u FINAL WHERE NOT _is_deleted")
                .unwrap(),
            "172\t172"
        );
    };

    // Pass 1 blocks on pg_control, a part read only after every data part,
    // then fails on its garbage. Swap the part once listing has named it
    let control = find_control_part(root.path()).unwrap();
    let held = tmp.path().join("pg_control.held");
    let settings_for_skew = settings.clone();
    let mut throttled = emitter.clone();
    throttled.backup = Some(walrus::config::Settings {
        network_rate_limit: 32 << 20,
        ..settings
    });
    let ctx = passes.ctx(throttled, fresh.clone());
    let reqs = passes.reqs.clone();
    let pass = tokio::spawn(async move {
        walshadow::backup_backfill::run_pass(&ctx, InitialLoadMode::ObjectStore, &reqs).await
    });
    let pump = stats.backfill_backup_pump.clone();
    tokio::time::timeout(Duration::from_secs(30), async {
        while pump.parts_total.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    std::fs::rename(&control, &held).unwrap();
    let mkfifo = std::process::Command::new("mkfifo")
        .arg(&control)
        .status()
        .unwrap();
    assert!(mkfifo.success());
    let (done, total) = (
        pump.parts_done.load(Ordering::Relaxed),
        pump.parts_total.load(Ordering::Relaxed),
    );
    assert!(
        done + 1 < total,
        "{done} of {total} parts drained before the swap"
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        while !BackupCheckpoint::load(&passes.scratch)
            .await
            .unwrap()
            .is_some_and(|c| c.walk_started() && !c.walk.parts().is_empty())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let fifo = control.clone();
    tokio::task::spawn_blocking(move || {
        let mut w = std::fs::OpenOptions::new().write(true).open(fifo).unwrap();
        w.write_all(&[0xff; 1024]).unwrap();
    })
    .await
    .unwrap();
    let err = pass.await.unwrap().unwrap_err();
    assert!(format!("{err:#}").contains("tar unpack"), "{err:#}");
    std::fs::remove_file(&control).unwrap();
    std::fs::rename(&held, &control).unwrap();
    let mid_walk = std::fs::read(passes.scratch.join("backup_replay.json")).unwrap();

    // Pass 2 skips recorded files and settled parts. Pass 1 tapped every
    // mapped file; a recorded one skips alone, or untapped with its settled
    // part when readdir order at push gave it no SLRU neighbour
    let saved = passes.saved().await;
    assert!(saved.walk_started());
    let first = files_walked(&stats);
    let second = passes.run(saved).await.unwrap();
    let again = files_walked(&stats) - first;
    assert!(again < first, "pass 2 walked {again} of {first} files");
    let (resumed, _) = walk_stat(&stats);
    assert!(pump.parts_skipped.load(Ordering::Relaxed) > 0);
    assert!(second.replay_commits_past_s > 0);
    assert!(second.gap_segments > 0);
    assert_ch();

    // Pass 3 picks up after the walk: nothing walked, counts carried over
    let saved = passes.saved().await;
    assert!(saved.ready());
    assert!(saved.replay_from.is_some());
    let walked = walk_stat(&stats);
    let third = passes.run(saved).await.unwrap();
    assert_eq!(walk_stat(&stats), walked);
    assert_eq!(third.counts.walked, second.counts.walked);
    assert_ch();
    let mut saved = passes.saved().await;
    saved.replay_from = Some(s_lsn);
    let late = passes.run(saved).await.unwrap();
    assert_eq!(late.rows_replayed, 0);
    assert_ch();

    // Pass 4: mid-walk checkpoint whose gate spool resolution consumed
    // walks it all
    std::fs::write(passes.scratch.join("backup_replay.json"), mid_walk).unwrap();
    assert!(!passes.scratch.join("gate_deferred.bin").exists());
    let saved = passes.saved().await;
    assert!(saved.walk_started());
    passes.run(saved).await.unwrap();
    let (resumed_after, pages_after) = walk_stat(&stats);
    assert_eq!(resumed_after, resumed);
    assert!(pages_after > walked.1);
    assert_ch();

    // Opt-in below the backup: walked rows lose to every replayed version
    for r in &mut passes.reqs {
        r.s_lsn = 1;
    }
    let walk_only = passes.run(fresh.clone()).await.unwrap();
    assert_eq!(walk_only.gap_segments, 0);
    assert_ch();

    // Walk that cannot record its progress fails the pass
    let blocker = passes.scratch.join("backup_replay.json.tmp");
    std::fs::create_dir(&blocker).unwrap();
    let err = passes.run(fresh.clone()).await.unwrap_err();
    std::fs::remove_dir(&blocker).unwrap();
    let kind = err
        .inner()
        .downcast_ref::<std::io::Error>()
        .map(std::io::Error::kind);
    assert_eq!(kind, Some(std::io::ErrorKind::IsADirectory), "{err:#}");

    // Finished walk that deferred no TOAST referrer still resumes
    std::fs::remove_file(passes.scratch.join("bootstrap_deferred.bin")).ok();
    passes.reqs.retain(|r| r.desc.oid == u.oid);
    passes.run(fresh.clone()).await.unwrap();
    let saved = passes.saved().await;
    assert!(saved.ready());
    passes.run(saved).await.unwrap();
    assert_ch();

    let mut skewed = passes.saved().await;
    skewed.catalog ^= 1;
    let err = passes.run(skewed).await.unwrap_err();
    assert!(err.to_string().contains("catalog changed"), "{err}");
    let mut bogus = fresh.clone();
    bogus.backup = "bogus".into();
    let err = passes.run(bogus).await.unwrap_err();
    assert!(err.to_string().contains("not wal-g shaped"), "{err}");

    // Catalog change inside the gap fails the pass before any walk
    source
        .psql_one("ALTER TABLE public.u DROP COLUMN name")
        .unwrap();
    let skew_lsn = source.psql_one("SELECT pg_current_wal_lsn()").unwrap();
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    archive_wal(
        &source.config().data_dir.join("pg_wal"),
        &settings_for_skew,
        &storage,
    )
    .await;
    for r in &mut passes.reqs {
        r.s_lsn = walrus::pg::backup::parse_pg_lsn(&skew_lsn).unwrap();
    }
    let err = passes.run(fresh.clone()).await.unwrap_err();
    assert!(format!("{err:#}").contains("catalog skew"), "{err:#}");

    let ctx = passes.ctx(emitter, fresh);
    let err = walshadow::backup_backfill::run_pass(&ctx, InitialLoadMode::Copy, &passes.reqs)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("routes via the copy backfiller"),
        "{err}"
    );
}
