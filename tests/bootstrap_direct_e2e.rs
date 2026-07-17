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
//!   → drain_backfill → RecordingObserver
//! ```
//!
//! Skipped silently when `initdb` is not on `$PATH`.

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::process::Command;
use std::time::Duration;

use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};
use walshadow::backfill_bootstrap::{
    BootstrapConfig, drain_backfill, seed_in_snapshot, spawn_greenfield_bootstrap,
};
use walshadow::backup_source_direct::DirectSource;
use walshadow::heap_decoder::{ColumnValue, HeapOp};
use walshadow::shadow::{Shadow, ShadowConfig};

/// Reserved port slot — 56145 sits between `bootstrap_object_store_e2e`
/// (56141) and other test binaries' 56170/56300 slots.
const SOURCE_PORT: u16 = 56145;

/// Same row budget as the object-store sibling: one heap page worth so
/// the page walker has guaranteed bytes without a multi-page sweep.
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
    let (rx, pump) = spawn_greenfield_bootstrap(cfg, Box::new(direct), catalog_map, false);

    let mut observer = RecordingObserver::default();
    let (drain_res, pump_res) = tokio::join!(drain_backfill(rx, &mut observer), pump);
    let shipped = drain_res.expect("drain task error");
    let outcome = pump_res
        .expect("pump task panicked")
        .expect("pump task returned error");
    assert_eq!(observer.xact_end_calls, 1);

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
        outcome.disk.kept_files > 100,
        "expected >100 catalog files landed, got {}",
        outcome.disk.kept_files,
    );
    assert!(
        outcome.disk.skipped_denylist > 0,
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
    assert!(outcome.page_walk.files_seen > 0);
    assert!(outcome.page_walk.files_walked > 0);
    assert!(outcome.page_walk.pages_walked > 0);
    assert!(
        outcome.page_walk.tuples_emitted >= N_ROWS as u64,
        "expected >= {N_ROWS} tuples emitted from page walk, got {}",
        outcome.page_walk.tuples_emitted,
    );
    assert_eq!(shipped, outcome.page_walk.tuples_emitted);

    // CommittedTuple shape
    let mut int_ids: HashSet<i32> = HashSet::new();
    for c in &observer.tuples {
        assert_eq!(c.commit_ts, 0);
        assert_eq!(c.commit_lsn, outcome.start.start_lsn);
        assert_eq!(c.decoded.op, HeapOp::Insert);
        assert_eq!(c.decoded.source_lsn, outcome.start.start_lsn);
        assert!(c.decoded.old.is_none());
        let new = c.decoded.new.as_ref().expect("Insert must carry `new`");
        assert!(!new.partial);
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
