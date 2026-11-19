//! File-streaming bootstrap end-to-end via the direct
//! replication-protocol source.
//!
//! Sibling of `bootstrap_object_store_e2e.rs` for the
//! [`DirectSource`](walshadow::backup_source_direct::DirectSource) path.
//! Object-store fixture proves the wal-g layout decoder; this fixture
//! proves the BASE_BACKUP wire pump, the tar-archive body splitter,
//! and `drive_archive`'s `pump_tar_to_sink` adapter.
//!
//! Pipeline:
//!
//! ```text
//! Shadow(source).start()
//!   → schema + INSERT + CHECKPOINT
//!   → DirectSource(PgConfig→source, BaseBackupOpts{fast_checkpoint})
//!   → MultiplexSink(DiskLanderSink + PageWalkSink)
//!   → mpsc<BackfillTuple>
//!   → collected tuples
//! ```
//!
//! Skipped silently when `initdb` is not on `$PATH`.

#[path = "common/tools.rs"]
mod tools;
use tools::pg_available;

#[path = "common/ports.rs"]
mod ports;

use std::fs;
use std::io::Write;
use std::time::Duration;

use foldhash::{HashSet, HashSetExt};
use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};
use walshadow::backfill_bootstrap::{BootstrapConfig, run_greenfield_bootstrap, seed_in_snapshot};
use walshadow::backup_source_direct::DirectSource;
use walshadow::datum::Datum;
use walshadow::shadow::{Shadow, ShadowConfig};

/// Same row budget as the object-store sibling: one heap page worth so
/// the page walker has guaranteed bytes without a multi-page sweep.
const N_ROWS: i32 = 64;

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = ports::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

fn append_source_conf(sh: &Shadow) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow bootstrap direct source overrides").unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_source_self_hosted_via_replication_protocol() {
    if !pg_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("base conf");
    append_source_conf(&source);
    source.start().expect("start source");
    let _stop = StopOnDrop { sh: &source };

    let workload = format!(
        "CREATE SCHEMA s12;\n\
         CREATE TABLE s12.t (id int PRIMARY KEY, payload text NOT NULL);\n\
         INSERT INTO s12.t \
           SELECT g, 'row-'||g::text FROM generate_series(1, {N_ROWS}) g;\n\
         CHECKPOINT;\n\
         SELECT pg_switch_wal();\n",
    );
    source.apply_schema_dump(&workload).expect("apply workload");

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    let conninfo = format!(
        "host={socket_host} port={} user=postgres dbname=postgres",
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

    // Direct path: PgConfig pointing at the source's unix socket. Trust
    // auth is the cluster default so password stays None.
    let pgcfg = PgConfig {
        host: socket_host,
        port: source.config().port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-bootstrap-direct".into(),
        sslmode: SslMode::Disable,
        tls: TlsParams::default(),
    };
    let opts = BaseBackupOpts {
        label: "bootstrap-direct".into(),
        fast_checkpoint: true,
        no_verify_checksums: false,
        max_rate_kib: None,
        wal: false,
    };
    let direct = DirectSource::new(pgcfg, opts);

    let shadow_data = tmp.path().join("shadow-data");
    let cfg = BootstrapConfig::new(shadow_data.clone());
    let (outcome, tuples) =
        run_greenfield_bootstrap(cfg, Box::new(direct), catalog_map.clone(), false)
            .await
            .expect("bootstrap");

    // LSN handoff
    assert!(outcome.start.start_lsn > 0, "start_lsn must be non-zero");
    assert!(
        outcome.end.end_lsn >= outcome.start.start_lsn,
        "end_lsn ({:X}) < start_lsn ({:X})",
        outcome.end.end_lsn,
        outcome.start.start_lsn,
    );
    assert_eq!(outcome.start.timeline, outcome.end.timeline);

    // DiskLanderSink coverage
    assert!(
        outcome
            .disk
            .kept_files
            .load(std::sync::atomic::Ordering::Relaxed)
            > 100,
        "expected >100 catalog files landed, got {}",
        outcome
            .disk
            .kept_files
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    assert!(
        outcome
            .disk
            .skipped_denylist
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "no denylist files skipped — expected at least one under pg_replslot/ or pg_stat_tmp/",
    );

    // pg_control landed (either layout PG emits)
    let pg_control_path_a = shadow_data.join("global/pg_control");
    let pg_control_path_b = shadow_data.join("pg_control");
    assert!(
        pg_control_path_a.exists() || pg_control_path_b.exists(),
        "pg_control absent from shadow data_dir {}",
        shadow_data.display(),
    );

    // User heap routed to tap (NOT disk); pg_class lands
    let t_main = shadow_data.join(format!("base/{t_dboid}/{t_filenode}"));
    assert!(
        !t_main.exists(),
        "user heap landed on disk at {} — MultiplexSink should have tap'd it",
        t_main.display(),
    );
    let pg_class_path = shadow_data.join(format!("base/{t_dboid}/1259"));
    assert!(
        pg_class_path.exists(),
        "pg_class (filenode 1259) must land at {}",
        pg_class_path.display(),
    );

    // PageWalkSink stats
    assert!(
        outcome
            .page_walk
            .files_seen
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(
        outcome
            .page_walk
            .files_walked
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(
        outcome
            .page_walk
            .pages_walked
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(
        outcome
            .page_walk
            .tuples_emitted
            .load(std::sync::atomic::Ordering::Relaxed)
            >= N_ROWS as u64,
        "expected >= {N_ROWS} tuples emitted from page walk, got {}",
        outcome
            .page_walk
            .tuples_emitted
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    assert_eq!(
        tuples.len() as u64,
        outcome
            .page_walk
            .tuples_emitted
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    let mut int_ids: HashSet<i32> = HashSet::new();
    for t in &tuples {
        assert_eq!(t.source_lsn, outcome.start.start_lsn);
        let rel = catalog_map
            .get(t.rfn.db_node, t.rfn.rel_node)
            .expect("descriptor");
        if let Some(Some(Datum::Fixed(v))) = t.cells(&rel).expect("cells").first() {
            let v = &i32::from_le_bytes(v.as_slice().try_into().expect("int4"));
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
