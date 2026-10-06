#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use walrus::compression;
use walrus::config::{Settings, StorageSettings, Vars};
use walrus::pg::backup::list;
use walrus::pg::backup::push::{self, PushArgs};
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::wal;
use walrus::storage::DynStorage;
use walrus::storage::fs::FsStorage;
use walshadow::shadow::Shadow;

const LATE_ROWS: i32 = 32;

fn psql_db(source: &Shadow, db: &str, sql: &str) -> Result<()> {
    let out = Command::new("psql")
        .args([
            "-h",
            source.config().socket_dir.to_str().unwrap(),
            "-p",
            &source.config().port.to_string(),
            "-U",
            "postgres",
            "-d",
            db,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ])
        .output()
        .context("spawn psql")?;
    anyhow::ensure!(
        out.status.success(),
        "psql -d {db} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

async fn push_completed_wal_segments(
    source: &Shadow,
    settings: &Settings,
    storage: DynStorage,
) -> Result<()> {
    let current = source
        .psql_one("SELECT pg_walfile_name(pg_current_wal_insert_lsn())")
        .context("read the in-progress segment")?;
    let current = current.trim();
    let pg_wal = source.config().data_dir.join("pg_wal");
    for entry in fs::read_dir(&pg_wal).with_context(|| format!("read_dir {}", pg_wal.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.len() != 24 || !name.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        if name[8..] >= current[8..] {
            continue;
        }
        let path = entry.path();
        wal::push::handle(settings, storage.clone(), &path)
            .await
            .with_context(|| format!("wal::push::handle {}", path.display()))?;
    }
    Ok(())
}

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

fn write_config(path: &Path, ch_port: u16, storage_root: &Path) -> Result<()> {
    let body = format!(
        "[source]\n\
         dbname = \"postgres\"\n\
         \n\
         [ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         compression = \"lz4\"\n\
         \n\
         [table.\"public\".\"early\"]\n\
         replicate = true\n\
         target_table = \"early\"\n\
         \n\
         [database.app.table.\"public\".\"late\"]\n\
         replicate = true\n\
         target_table = \"late\"\n\
         \n\
         [backup]\n\
         archive = \"file://{}\"\n",
        storage_root.display()
    );
    fs::write(path, body).context("write ch-config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn database_created_after_the_backup_replicates_once_replayed() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    psql_db(
        &source,
        "postgres",
        "CREATE TABLE public.early(id int primary key, name text); \
         INSERT INTO public.early VALUES (1, 'before-backup');",
    )
    .expect("load postgres workload");

    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(storage_root.clone());

    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    unsafe {
        std::env::set_var("PGHOST", &socket_host);
        std::env::set_var("PGPORT", source.config().port.to_string());
        std::env::set_var("PGUSER", "postgres");
        std::env::set_var("PGDATABASE", "postgres");
        std::env::remove_var("PGPASSWORD");
    }
    let cfg = PgConfig::resolve(&Vars::default()).expect("resolve source PgConfig from libpq env");
    push::handle(&settings, storage.clone(), PushArgs::default(), cfg)
        .await
        .expect("wal-rus push::handle against source PG");
    let backup_name = list::collect(storage.clone())
        .await
        .expect("list backups on FsStorage")
        .into_iter()
        .next()
        .expect("one backup on fresh storage")
        .name;

    source
        .apply_schema_dump("CREATE DATABASE app;\n")
        .expect("create app after the backup");
    psql_db(
        &source,
        "app",
        &format!(
            "CREATE TABLE public.late(id int primary key, name text); \
             INSERT INTO public.late SELECT g, 'after-backup' FROM generate_series(1, {LATE_ROWS}) g;"
        ),
    )
    .expect("load app workload");
    source
        .psql_one("SELECT pg_switch_wal()")
        .expect("seal the WAL that creates app");
    push_completed_wal_segments(&source, &settings, storage.clone())
        .await
        .expect("push WAL segments to storage");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let ch_config_path = tmp.path().join("ch-config.toml");
    write_config(&ch_config_path, slot.ch_tcp, &storage_root).expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn_mode(
            &source,
            &ch_config_path,
            slot.walsender,
            "object-store",
            &["--bootstrap-backup-name", &backup_name],
            &[
                ("PGHOST", socket_host.clone()),
                ("PGPORT", source.config().port.to_string()),
                ("PGUSER", "postgres".into()),
                ("PGDATABASE", "postgres".into()),
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(180))
            .context("daemon metrics endpoint never came up")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.early FINAL WHERE _is_deleted = 0",
            "1",
            Duration::from_secs(120),
        )
        .context("postgres's row from the backup never reached CH")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.late FINAL WHERE _is_deleted = 0",
            &LATE_ROWS.to_string(),
            Duration::from_secs(120),
        )
        .context("rows written to app after the backup never reached CH")?;
        psql_db(
            &source,
            "app",
            "INSERT INTO public.late VALUES (900001, 'post-boot');",
        )
        .context("post-boot insert into app")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.late FINAL WHERE id = 900001",
            "1",
            Duration::from_secs(90),
        )
        .context("live CDC into app never reached CH")?;
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
