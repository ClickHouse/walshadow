//! Bootstrap a source carrying a user tablespace, then recover shadow from
//! the landed data dir with the source's tablespace location gone.
//!
//! Both sources land the tablespace as a real `pg_tblspc/<oid>` directory
//! (direct roots its tablespace archive there) beside a `tablespace_map`
//! naming the source location. Without relocation, recovery fails removing
//! that directory to re-point it, or, with the map gone, panics on a
//! non-symlink `pg_tblspc` entry at consistency.
//!
//! Skipped silently when `initdb` is not on `$PATH`.

#[path = "common/tools.rs"]
mod tools;
use tools::pg_available;

#[path = "common/ports.rs"]
mod ports;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use walrus::compression;
use walrus::config::{Settings, StorageSettings, Vars};
use walrus::pg::backup::list;
use walrus::pg::backup::push::{self, PushArgs};
use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};
use walrus::storage::DynStorage;
use walrus::storage::fs::FsStorage;
use walshadow::backfill_bootstrap::{
    BootstrapConfig, drain_backfill, seed_in_snapshot, spawn_greenfield_bootstrap,
};
use walshadow::backup_source::BackupSource;
use walshadow::backup_source_direct::DirectSource;
use walshadow::backup_source_object_store::ObjectStoreSource;
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_tablespaces::TABLESPACE_DIR;

const N_ROWS: i32 = 16;

fn make_pg(root: &Path, name: &str, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(root.join(format!("{name}-data")), root.join("wal"));
    cfg.port = port;
    cfg.socket_dir = root.join(format!("{name}-sock"));
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

struct StopOnDrop<'a>(&'a Shadow);
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.0.stop();
    }
}

fn source_pg_config(source: &Shadow) -> PgConfig {
    PgConfig {
        host: source.config().socket_dir.to_str().unwrap().into(),
        port: source.config().port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-bootstrap-tablespace".into(),
        sslmode: SslMode::Disable,
        tls: TlsParams::default(),
    }
}

async fn bootstrap(source: &Shadow, backup: Box<dyn BackupSource>, shadow_data: PathBuf) {
    let conninfo = format!(
        "host={} port={} user=postgres dbname=postgres",
        source.config().socket_dir.display(),
        source.config().port,
    );
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let catalog_map = seed_in_snapshot(&client).await.unwrap();
    let (rx, pump) = spawn_greenfield_bootstrap(
        BootstrapConfig::new(shadow_data),
        backup,
        catalog_map,
        false,
    );
    let mut observer = walshadow::decoder_sink::CollectingTupleObserver::default();
    let (drained, pumped) = tokio::join!(drain_backfill(rx, &mut observer), pump);
    drained.unwrap();
    pumped.unwrap().expect("bootstrap pump");
}

fn assert_relocated(shadow_data: &Path, oid: &str) {
    let link = shadow_data.join("pg_tblspc").join(oid);
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink(),
        "{} must be a symlink",
        link.display(),
    );
    assert_eq!(
        fs::read_link(&link).unwrap(),
        Path::new("..").join(TABLESPACE_DIR).join(oid),
    );
    assert!(!shadow_data.join("tablespace_map").exists());
    assert!(shadow_data.join("tablespace_map.old").exists());
    // Direct must not spill its tablespace archive over the data dir root
    let stray: Vec<_> = fs::read_dir(shadow_data)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("PG_") && n != "PG_VERSION")
        .collect();
    assert!(
        stray.is_empty(),
        "tablespace entries at data dir root: {stray:?}"
    );
}

/// Recover to consistency off the source's archived WAL, then read the
/// table stored in the tablespace
fn recover_and_read(root: &Path, name: &str, port: u16) {
    let shadow = make_pg(root, name, port);
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&shadow.config().data_dir, fs::Permissions::from_mode(0o700)).unwrap();
    }
    shadow.write_standby_signal().unwrap();
    shadow
        .start_with_floor_retry(None)
        .unwrap_or_else(|e| panic!("{name}: shadow start: {e:#}"));
    let _stop = StopOnDrop(&shadow);
    assert_eq!(
        shadow
            .psql_one("SELECT count(*) FROM s12.in_ts")
            .unwrap_or_else(|e| panic!("{name}: read tablespace table: {e:#}")),
        N_ROWS.to_string(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_relocates_source_tablespace() {
    if !pg_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let source = make_pg(&root, "source", ports::PG_SOURCE_PORT);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    {
        let path = source.config().data_dir.join("postgresql.conf");
        let mut f = fs::OpenOptions::new().append(true).open(path).unwrap();
        writeln!(f, "max_wal_senders = 4").unwrap();
        writeln!(f, "wal_keep_size = '1GB'").unwrap();
    }
    source.start().unwrap();
    let stop_source = StopOnDrop(&source);

    let location = root.join("source-tablespace");
    fs::create_dir_all(&location).unwrap();
    source
        .apply_schema_dump(&format!(
            "CREATE TABLESPACE ts LOCATION '{}';\n\
             CREATE SCHEMA s12;\n\
             CREATE TABLE s12.in_ts (id int) TABLESPACE ts;\n\
             INSERT INTO s12.in_ts SELECT generate_series(1, {N_ROWS});\n\
             CHECKPOINT;\n",
            location.display(),
        ))
        .unwrap();
    let ts_oid = source
        .psql_one("SELECT oid FROM pg_tablespace WHERE spcname = 'ts'")
        .unwrap();

    let direct = DirectSource::new(
        source_pg_config(&source),
        BaseBackupOpts {
            label: "bootstrap-tablespace".into(),
            fast_checkpoint: true,
            ..Default::default()
        },
    );
    let direct_data = root.join("direct-data");
    bootstrap(&source, Box::new(direct), direct_data.clone()).await;
    assert_relocated(&direct_data, &ts_oid);

    let storage_root = root.join("storage");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = Settings {
        storage: StorageSettings::Fs {
            path: storage_root.to_string_lossy().into_owned(),
        },
        compression: compression::Method::None,
        compression_level: 0,
        ..Default::default()
    };
    // SAFETY: only test in this binary; push resolves its source from env
    unsafe {
        std::env::set_var("PGHOST", &source.config().socket_dir);
        std::env::set_var("PGPORT", source.config().port.to_string());
        std::env::set_var("PGUSER", "postgres");
        std::env::set_var("PGDATABASE", "postgres");
        std::env::remove_var("PGPASSWORD");
    }
    let cfg = PgConfig::resolve(&Vars::default()).unwrap();
    push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
        .await
        .expect("push backup");
    let backup_name = list::collect(storage.clone())
        .await
        .unwrap()
        .pop()
        .unwrap()
        .name;
    let object = ObjectStoreSource::new(settings, storage, backup_name, root.join("part-spool"));
    let object_data = root.join("object-data");
    bootstrap(&source, Box::new(object), object_data.clone()).await;
    assert_relocated(&object_data, &ts_oid);

    // Archive every segment both recoveries need, then take the source and
    // its tablespace location away
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    source.psql_one("CHECKPOINT").unwrap();
    drop(stop_source);
    let wal = source.config().filter_out_dir.clone();
    for entry in fs::read_dir(source.config().data_dir.join("pg_wal")).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name.len() == 24 && entry.file_type().unwrap().is_file() {
            fs::copy(entry.path(), wal.join(&name)).unwrap();
        }
    }
    fs::rename(&location, root.join("source-tablespace.gone")).unwrap();

    recover_and_read(&root, "direct", ports::PG_SHADOW_PORT);
    recover_and_read(&root, "object", ports::PG_SHADOW_PORT);
}
