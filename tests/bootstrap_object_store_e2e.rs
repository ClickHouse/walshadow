//! File-streaming bootstrap end-to-end via the
//! object-store source.
//!
//! Self-hosted: wal-rus's own `pg::backup::push::handle` builds a
//! wal-g-compatible base backup against a live source PG, serialised
//! to a local `FsStorage` root. ObjectStoreSource then reads it back
//! through the file-level [`BackupSource`] trait, the multiplex sink
//! lands catalogs on a fresh shadow data dir, and the page walker
//! decodes user-heap pages into `BackfillTuple`s.
//!
//! Pipeline:
//!
//! ```text
//! Shadow(source).start()
//!   → schema + INSERT + CHECKPOINT
//!   → walrus::pg::backup::push::handle(.., FsStorage(temp/storage), ..)
//!   → ObjectStoreSource(settings, FsStorage(temp/storage), name)
//!   → MultiplexSink(DiskLanderSink + PageWalkSink)
//!   → mpsc<BackfillTuple>
//!   → drain_backfill → RecordingObserver (records tuples + on_xact_end)
//! ```
//!
//! What this exercises end-to-end (not covered by lib unit tests):
//!
//! - wal-rus's BASE_BACKUP wire (replication protocol -> tar streamer
//!   -> object-store layout)
//! - ObjectStoreSource's part-listing, decompress-on-read, pg_control-last
//!   barrier
//! - tar entry → FileMeta translation across a real BASE_BACKUP layout
//!   (not a hand-rolled synthetic tar)
//! - DiskLanderSink classification on a real catalog tree (hundreds of
//!   `base/<dbid>/<filenode>` files, denylist dirs like `pg_replslot/`,
//!   `pg_dynshmem/`)
//! - PageWalker on real PG heap pages (PageHeaderData + ItemIdData +
//!   on-disk HeapTupleHeader reshape into xl_heap_header for the heap
//!   decoder)
//! - CatalogMap seed off a live `pg_class` (no synthetic RelDescriptor)
//!
//! Skipped silently when `initdb` isn't on `$PATH` (CI sandboxes
//! without PG client tooling).
//!
//! Operator note: upstream wal-g binary can serve as a cross-check
//! oracle if ever wanted; bootstrap doesn't depend on it. This test runs
//! purely off `walshadow` + `walrus` crates.

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use walrus::compression;
use walrus::config::{Settings, StorageSettings, Vars};
use walrus::pg::backup::list;
use walrus::pg::backup::push::{self, PushArgs};
use walrus::pg::replication::conn::PgConfig;
use walrus::storage::DynStorage;
use walrus::storage::fs::FsStorage;
use walshadow::backfill_bootstrap::{
    BootstrapConfig, drain_backfill, seed_in_snapshot, spawn_greenfield_bootstrap,
};
use walshadow::backup_source_object_store::ObjectStoreSource;
use walshadow::heap_decoder::{ColumnValue, HeapOp};
use walshadow::shadow::{Shadow, ShadowConfig};

/// Port slot reserved for the bootstrap object-store drill (56140-range). Single source-PG per
/// test binary so the env-var rendezvous below stays single-writer.
const SOURCE_PORT: u16 = 56141;

/// Row count loaded into the user table before push runs. Must be >0
/// and small enough that one heap page fits the lot — at one
/// ~32-byte tuple per row, 8 KiB holds ~250 rows, so 64 is safely on
/// a single page.
const N_ROWS: i32 = 64;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

/// Append the bare minimum to let the replication protocol attach.
/// `wal_level=replica` + `max_wal_senders` is the floor PG accepts for
/// BASE_BACKUP; `logical` would work too but isn't needed.
fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow bootstrap object-store source overrides").unwrap();
    writeln!(f, "wal_level = replica").unwrap();
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

/// Minimal Settings: FsStorage root, no compression, no crypter, no
/// throttling. Uncompressed parts simplify the test fixture — the
/// ObjectStoreSource decoder reads `part_001.tar` (extensionless) as
/// `Method::None`, an identity passthrough.
fn test_settings(storage_root: PathBuf) -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: storage_root.to_string_lossy().into_owned(),
        },
        compression: compression::Method::None,
        compression_level: 0,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_store_source_self_hosted_via_wal_rs_push() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("base conf");
    append_source_conf(&source);
    source.start().expect("start source");
    let _stop = StopOnDrop { sh: &source };

    // Load a user table + N_ROWS so the page walker has something to
    // produce. CHECKPOINT forces the rows out of the WAL buffer onto
    // a heap page that the subsequent BASE_BACKUP will ship.
    let workload = format!(
        "CREATE SCHEMA s12;\n\
         CREATE TABLE s12.t (id int PRIMARY KEY, payload text NOT NULL);\n\
         INSERT INTO s12.t \
           SELECT g, 'row-'||g::text FROM generate_series(1, {N_ROWS}) g;\n\
         CHECKPOINT;\n\
         SELECT pg_switch_wal();\n",
    );
    source.apply_schema_dump(&workload).expect("apply workload");

    let storage_root = tmp.path().join("storage");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());

    // wal-rus's push::handle resolves its source PG from libpq env vars.
    // We're the only writer in this test binary's address space; other
    // walshadow integration test files are separate cargo test
    // binaries, so the global env state is single-owner here.
    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    // SAFETY: single-test-fn test binary, no other thread reads env
    // vars before push::handle's PgConfig::from_env() picks them up
    // (push spawns its own internal tasks but they inherit this
    // process's env via the runtime — no concurrent set/get races).
    unsafe {
        std::env::set_var("PGHOST", &socket_host);
        std::env::set_var("PGPORT", source.config().port.to_string());
        std::env::set_var("PGUSER", "postgres");
        std::env::set_var("PGDATABASE", "postgres");
        // Belt-and-suspenders: clear PGPASSWORD in case the host has
        // one set; trust-mode auth is the source's default.
        std::env::remove_var("PGPASSWORD");
    }

    let cfg = PgConfig::resolve(&Vars::default()).expect("resolve source PgConfig from libpq env");
    push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
        .await
        .expect("wal-rus push::handle against source PG");

    // Backup name lookup — exactly one backup on a fresh FsStorage.
    let backup_summaries = list::collect(storage.clone())
        .await
        .expect("list backups on FsStorage");
    assert_eq!(
        backup_summaries.len(),
        1,
        "exactly one backup expected on fresh storage, got {} ({:?})",
        backup_summaries.len(),
        backup_summaries
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
    );
    let backup_name = backup_summaries.into_iter().next().unwrap().name;

    // Seed CatalogMap from source PG. Sidecar tokio_postgres client
    // distinct from the replication connection push uses internally.
    let conninfo = format!(
        "host={} port={} user=postgres dbname=postgres",
        source.config().socket_dir.to_str().unwrap(),
        source.config().port,
    );
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .expect("source tokio_postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let catalog_map = seed_in_snapshot(&client).await.expect("seed catalog");
    assert!(
        !catalog_map.is_empty(),
        "seed_in_snapshot returned an empty CatalogMap — s12.t must be present"
    );
    let t_dboid: u32 = client
        .query_one(
            "SELECT oid::oid FROM pg_database WHERE datname = current_database()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let t_filenode: u32 = client
        .query_one("SELECT pg_relation_filenode('s12.t')::oid", &[])
        .await
        .unwrap()
        .get(0);

    // Run the orchestrator. spawn_greenfield_bootstrap yields the
    // receiver up front so drain_backfill runs concurrently with the
    // source pump — bounded by source decode bandwidth, not by total
    // tuple count.
    let shadow_data = tmp.path().join("shadow-data");
    let cfg = BootstrapConfig::new(shadow_data.clone());
    let object_source = ObjectStoreSource::new(settings, storage, backup_name).with_parallelism(2);
    let (rx, pump) = spawn_greenfield_bootstrap(cfg, Box::new(object_source), catalog_map, false);

    // RecordingObserver wraps a CollectingTupleObserver + counts
    // on_xact_end so the test asserts drain_backfill closes the
    // synthetic backfill xact exactly once after channel drain. The
    // production daemon path uses this signal to flush the
    // transitional emitter's INSERT block before swapping to the
    // ShadowCatalog-backed emitter.
    let mut observer = RecordingObserver::default();
    let (drain_res, pump_res) = tokio::join!(drain_backfill(rx, &mut observer), pump);
    let shipped = drain_res.expect("drain task error");
    let outcome = pump_res
        .expect("pump task panicked")
        .expect("pump task returned error");
    assert_eq!(
        observer.xact_end_calls, 1,
        "drain_backfill must call on_xact_end exactly once after channel drain — \
         got {} calls",
        observer.xact_end_calls,
    );

    // --- LSN handoff ---
    assert!(outcome.start.start_lsn > 0, "start_lsn must be non-zero");
    assert!(
        outcome.end.end_lsn >= outcome.start.start_lsn,
        "end_lsn ({:X}) < start_lsn ({:X})",
        outcome.end.end_lsn,
        outcome.start.start_lsn,
    );
    assert_eq!(outcome.start.timeline, outcome.end.timeline);

    // --- DiskLanderSink coverage ---
    // A fresh initdb cluster has many hundreds of catalog files under
    // base/<dbid>/ + global/. Use a generous lower bound rather than
    // a tight count: PG version drift changes the exact number.
    assert!(
        outcome.disk.kept_files > 100,
        "expected >100 catalog files landed, got {}; \
         tuned for PG initdb expectations — bump or investigate \
         a regression",
        outcome.disk.kept_files,
    );
    assert!(
        outcome.disk.skipped_denylist > 0,
        "no denylist files skipped — expected at least one under pg_replslot/ or pg_stat_tmp/",
    );

    // --- pg_control landed ---
    let pg_control_path_a = shadow_data.join("global/pg_control");
    let pg_control_path_b = shadow_data.join("pg_control");
    assert!(
        pg_control_path_a.exists() || pg_control_path_b.exists(),
        "pg_control absent from shadow data_dir {}",
        shadow_data.display(),
    );

    // --- User heap routed to tap (NOT to disk) ---
    // The s12.t relation's filenode is the canonical user-heap probe:
    // it must not exist on disk under shadow_data_dir, because the
    // MultiplexSink routed those bytes to the page walker instead of
    // the lander.
    let t_main = shadow_data.join(format!("base/{t_dboid}/{t_filenode}"));
    assert!(
        !t_main.exists(),
        "user heap landed on disk at {} — MultiplexSink should have tap'd it",
        t_main.display(),
    );
    // pg_class etc (bootstrap rule, filenode < 16384) DO land.
    let pg_class_path = shadow_data.join(format!("base/{t_dboid}/1259"));
    assert!(
        pg_class_path.exists(),
        "pg_class (filenode 1259) must land at {}",
        pg_class_path.display(),
    );

    // --- PageWalkSink stats ---
    assert!(
        outcome.page_walk.files_seen > 0,
        "no user-heap files observed by the tap",
    );
    assert!(
        outcome.page_walk.files_walked > 0,
        "no user-heap files walked (filenode mismatch with CatalogMap?)",
    );
    assert!(outcome.page_walk.pages_walked > 0, "no heap pages walked",);
    assert!(
        outcome.page_walk.tuples_emitted >= N_ROWS as u64,
        "expected >= {N_ROWS} tuples emitted from page walk, got {}",
        outcome.page_walk.tuples_emitted,
    );

    // --- Drain delivered every tuple ---
    assert_eq!(
        shipped, outcome.page_walk.tuples_emitted,
        "drain count != page walk emit count — channel dropped tuples"
    );

    // --- CommittedTuple shape (every captured tuple) ---
    let mut int_ids: HashSet<i32> = HashSet::new();
    for c in &observer.tuples {
        assert_eq!(c.commit_ts, 0, "commit_ts must be 0 for backfill rows");
        assert_eq!(
            c.commit_lsn, outcome.start.start_lsn,
            "commit_lsn must equal start_lsn",
        );
        assert_eq!(c.decoded.op, HeapOp::Insert);
        assert_eq!(c.decoded.source_lsn, outcome.start.start_lsn);
        assert!(c.decoded.old.is_none());
        let new = c.decoded.new.as_ref().expect("Insert must carry `new`");
        assert!(!new.partial);
        // The s12.t.id column is int4 at attnum 1. PG may have given
        // us catalog rows too if their filenode rotated above 16384,
        // but ordinary fresh-cluster catalogs stay < 16384 — the
        // captured stream should be pure s12.t rows here.
        if let Some(Some(ColumnValue::Int4(v))) = new.columns.first() {
            int_ids.insert(*v);
        }
    }
    let want: HashSet<i32> = (1..=N_ROWS).collect();
    assert!(
        want.is_subset(&int_ids),
        "expected id range 1..={N_ROWS} in captured tuples; \
         missing {:?} from {} observed ids",
        want.difference(&int_ids).copied().collect::<Vec<_>>(),
        int_ids.len(),
    );
}

/// Recording observer = `CollectingTupleObserver` + `on_xact_end`
/// counter. Lets the e2e drill assert both per-tuple shape and the
/// synthetic xact-close signal that the daemon's transitional emitter
/// path (Solution 2) relies on to flush its INSERT block.
#[derive(Default)]
struct RecordingObserver {
    tuples: Vec<walshadow::heap_decoder::CommittedTuple>,
    xact_end_calls: u32,
}

impl walshadow::decoder_sink::TupleObserver for RecordingObserver {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a walshadow::heap_decoder::CommittedTuple,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), walshadow::decoder_sink::DecoderSinkError>>
                + Send
                + 'a,
        >,
    > {
        self.tuples.push(committed.clone());
        Box::pin(async { Ok(()) })
    }
    fn on_xact_end<'a>(
        &'a mut self,
        commit_lsn: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<u64, walshadow::decoder_sink::DecoderSinkError>>
                + Send
                + 'a,
        >,
    > {
        self.xact_end_calls += 1;
        Box::pin(async move { Ok(commit_lsn) })
    }
}
