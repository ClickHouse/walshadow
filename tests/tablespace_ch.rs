//! Tables in user tablespaces, bootstrap through streaming to ClickHouse
//!
//! Source holds one table in a tablespace created before backup and one in
//! `pg_default`. Daemon bootstraps, then streams row changes, tablespaces
//! created and dropped mid-stream, moves between tablespaces in both
//! directions, and a restart. Source tablespace locations sit outside shadow's data dir,
//! so every shadow-side tablespace must live inside it
//!
//! Skipped silently when `initdb` or the `clickhouse` multitool is absent

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};

const N_ROWS: i32 = 64;
const PARITY: Duration = Duration::from_secs(60);

/// Map `s.<name> (id int4, name text)` onto `default.<name>` for each name
fn write_ch_config(path: &Path, ch_port: u16, tables: &[&str]) -> Result<()> {
    let mut body = format!(
        "[ch]\nhost = \"127.0.0.1\"\nport = {ch_port}\ndatabase = \"default\"\n\
         compression = \"lz4\"\n"
    );
    for t in tables {
        body.push_str(&format!(
            "\n[table.\"s\".\"{t}\"]\n\
             target_database = \"default\"\n\
             target_table = \"{t}\"\n\
             columns = [\n  \
               {{ attnum = 1, target = \"id\",   type = \"Int32\"  }},\n  \
               {{ attnum = 2, target = \"name\", type = \"String\" }},\n\
             ]\n"
        ));
    }
    std::fs::write(path, body).with_context(|| format!("write {}", path.display()))
}

fn tablespace_dir(tmp: &Path, name: &str) -> String {
    let dir = tmp.join(format!("source-{name}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().replace('\'', "''")
}

fn converge(ch: &fx::ChServer, source: &walshadow::shadow::Shadow, tables: &[&str]) -> Result<()> {
    for t in tables {
        fx::wait_ch_matches_source(
            ch,
            source,
            &format!("s.{t}"),
            &format!("default.{t}"),
            PARITY,
        )?;
    }
    Ok(())
}

/// Insert, update and delete a slice of `s.<table>`
fn churn(source: &walshadow::shadow::Shadow, table: &str, base: i32) -> Result<()> {
    Ok(source.apply_schema_dump(&format!(
        "INSERT INTO s.{table} SELECT g, 'new-'||g FROM generate_series({lo}, {hi}) g;\n\
         UPDATE s.{table} SET name = name||'-u' WHERE id % 7 = 0;\n\
         DELETE FROM s.{table} WHERE id % 11 = 0;\n",
        lo = base + 1,
        hi = base + 16,
    ))?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tablespaces_bootstrap_stream_move_and_restart() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    let ts1 = tablespace_dir(tmp.path(), "ts1");
    source
        .apply_schema_dump(&format!(
            "CREATE TABLESPACE ts1 LOCATION '{ts1}';\n\
             CREATE SCHEMA s;\n\
             CREATE TABLE s.t (id int4 PRIMARY KEY, name text NOT NULL) TABLESPACE ts1;\n\
             CREATE TABLE s.u (id int4 PRIMARY KEY, name text NOT NULL);\n\
             ALTER TABLE s.t REPLICA IDENTITY FULL;\n\
             ALTER TABLE s.u REPLICA IDENTITY FULL;\n\
             INSERT INTO s.t SELECT g, 'row-'||g FROM generate_series(1, {N_ROWS}) g;\n\
             INSERT INTO s.u SELECT g, 'row-'||g FROM generate_series(1, {N_ROWS}) g;\n\
             CHECKPOINT;\n\
             SELECT pg_switch_wal();\n"
        ))
        .unwrap();

    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
        .expect("spawn ch");
    let tables = ["t", "u"];
    for t in tables {
        fx::create_ch_dest_table(&ch, "default", t).unwrap();
    }
    let ch_config = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config, slot.ch_tcp, &tables).unwrap();

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).unwrap();
    let guard = fx::ChildGuard::new(
        daemon
            .spawn(&source, &ch_config, slot.walsender, &[])
            .expect("spawn walshadow-stream"),
    );
    let first = (|| -> Result<()> {
        converge(&ch, &source, &tables).context("bootstrap rows")?;
        churn(&source, "t", N_ROWS).context("churn t")?;
        converge(&ch, &source, &tables).context("row changes in ts1")?;

        let ts2 = tablespace_dir(tmp.path(), "ts2");
        // ts3 is in place on source, its record already lands in place
        source.apply_schema_dump(&format!(
            "CREATE TABLESPACE ts2 LOCATION '{ts2}';\n\
             SET allow_in_place_tablespaces = on;\n\
             CREATE TABLESPACE ts3 LOCATION '';\n\
             ALTER TABLE s.u SET TABLESPACE ts2;\n\
             ALTER TABLE s.t SET TABLESPACE pg_default;\n\
             DROP TABLESPACE ts3;\n"
        ))?;
        churn(&source, "u", N_ROWS)?;
        churn(&source, "t", 2 * N_ROWS)?;
        converge(&ch, &source, &tables).context("after moves")
    })();
    fx::finish_daemon(guard, &daemon, first);

    let guard = fx::ChildGuard::new(
        daemon
            .spawn(&source, &ch_config, slot.walsender, &[])
            .expect("respawn walshadow-stream"),
    );
    let second = (|| -> Result<()> {
        churn(&source, "u", 2 * N_ROWS)?;
        source.apply_schema_dump("ALTER TABLE s.u SET TABLESPACE ts1;\n")?;
        churn(&source, "u", 3 * N_ROWS)?;
        converge(&ch, &source, &tables).context("after restart")?;
        let shadow_tblspc = std::fs::read_dir(daemon.shadow_data_dir.join("pg_tblspc"))?
            .map(|e| e.map(|e| e.file_type().map(|t| t.is_dir())))
            .collect::<std::io::Result<std::io::Result<Vec<_>>>>()??;
        anyhow::ensure!(
            shadow_tblspc.len() == 2 && shadow_tblspc.iter().all(|d| *d),
            "shadow pg_tblspc must hold both tablespaces in place: {shadow_tblspc:?}"
        );
        Ok(())
    })();
    fx::finish_daemon(guard, &daemon, second);
}

/// Handle on `source`'s cluster whose psql runs in `dbname`
fn in_database(source: &walshadow::shadow::Shadow, dbname: &str) -> walshadow::shadow::Shadow {
    let mut cfg = source.config().clone();
    cfg.dbname = dbname.into();
    walshadow::shadow::Shadow::new(cfg)
}

/// Followed database moves its default tablespace while streaming. Its
/// relations keep `reltablespace = 0`, so only their physical tablespace
/// changes, with no pg_class write naming them
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followed_database_moves_tablespace() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    let ts = tablespace_dir(tmp.path(), "tsd");
    source
        .apply_schema_dump(&format!(
            "CREATE TABLESPACE tsd LOCATION '{ts}';\nCREATE DATABASE app;\n"
        ))
        .unwrap();
    let app = in_database(&source, "app");
    app.apply_schema_dump(&format!(
        "CREATE SCHEMA s;\n\
         CREATE TABLE s.t (id int4 PRIMARY KEY, name text NOT NULL);\n\
         ALTER TABLE s.t REPLICA IDENTITY FULL;\n\
         INSERT INTO s.t SELECT g, 'row-'||g FROM generate_series(1, {N_ROWS}) g;\n\
         CHECKPOINT;\n\
         SELECT pg_switch_wal();\n"
    ))
    .unwrap();

    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
        .expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").unwrap();
    let ch_config = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config, slot.ch_tcp, &["t"]).unwrap();
    let body = std::fs::read_to_string(&ch_config).unwrap();
    std::fs::write(&ch_config, format!("[source]\ndbname = \"app\"\n\n{body}")).unwrap();

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).unwrap();
    let guard = fx::ChildGuard::new(
        daemon
            .spawn(&source, &ch_config, slot.walsender, &[])
            .expect("spawn walshadow-stream"),
    );
    let result = (|| -> Result<()> {
        converge(&ch, &app, &["t"]).context("bootstrap rows")?;
        churn(&app, "t", N_ROWS)?;
        converge(&ch, &app, &["t"]).context("before move")?;
        source.apply_schema_dump("ALTER DATABASE app SET TABLESPACE tsd;\n")?;
        churn(&app, "t", 2 * N_ROWS)?;
        converge(&ch, &app, &["t"]).context("after move")?;
        source.apply_schema_dump("ALTER DATABASE app SET TABLESPACE pg_default;\n")?;
        churn(&app, "t", 3 * N_ROWS)?;
        converge(&ch, &app, &["t"]).context("after move back")
    })();
    fx::finish_daemon(guard, &daemon, result);
}

/// Followed database created in a user tablespace, so its catalogs live
/// under `pg_tblspc` too, loaded from an object-store backup
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_store_database_in_tablespace() {
    use std::sync::Arc;
    use walrus::pg::replication::conn::PgConfig;
    use walrus::pg::replication::tls::{SslMode, TlsParams};
    use walrus::storage::DynStorage;

    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    let (tsd, tsa) = (
        tablespace_dir(tmp.path(), "tsd"),
        tablespace_dir(tmp.path(), "tsa"),
    );
    source
        .apply_schema_dump(&format!(
            "CREATE TABLESPACE tsd LOCATION '{tsd}';\n\
             CREATE TABLESPACE tsa LOCATION '{tsa}';\n\
             CREATE DATABASE app TABLESPACE tsd;\n"
        ))
        .unwrap();
    let app = in_database(&source, "app");
    let tables = ["t", "u", "v"];
    let mut ddl = String::from("CREATE SCHEMA s;\n");
    for (t, ts) in tables
        .iter()
        .zip(["", " TABLESPACE tsa", " TABLESPACE pg_default"])
    {
        ddl.push_str(&format!(
            "CREATE TABLE s.{t} (id int4 PRIMARY KEY, name text NOT NULL){ts};\n\
             ALTER TABLE s.{t} REPLICA IDENTITY FULL;\n\
             INSERT INTO s.{t} SELECT g, 'row-'||g FROM generate_series(1, {N_ROWS}) g;\n"
        ));
    }
    ddl.push_str("CHECKPOINT;\n");
    app.apply_schema_dump(&ddl).unwrap();

    let storage_root = tmp.path().join("wal-g");
    std::fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(walrus::storage::fs::FsStorage::new(&storage_root).unwrap());
    let settings = walrus::config::Settings {
        storage: walrus::config::StorageSettings::Fs {
            path: storage_root.to_string_lossy().into_owned(),
        },
        compression: walrus::compression::Method::None,
        compression_level: 0,
        ..Default::default()
    };
    let socket_host = source.config().socket_dir.to_str().unwrap().to_string();
    let pg = PgConfig {
        host: socket_host.clone(),
        port: source.config().port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "walshadow-tablespace-push".into(),
        sslmode: SslMode::Disable,
        tls: TlsParams::default(),
    };
    walrus::pg::backup::push::handle(&settings, storage.clone(), Default::default(), pg)
        .await
        .expect("push backup");
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    for entry in std::fs::read_dir(source.config().data_dir.join("pg_wal")).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if name.len() == 24 && name.chars().all(|c| c.is_ascii_hexdigit()) {
            walrus::pg::wal::push::handle(&settings, storage.clone(), &path)
                .await
                .unwrap();
        }
    }
    let backup_name = walrus::pg::backup::list::collect(storage)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .name;

    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
        .expect("spawn ch");
    for t in tables {
        fx::create_ch_dest_table(&ch, "default", t).unwrap();
    }
    let ch_config = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config, slot.ch_tcp, &tables).unwrap();
    let body = std::fs::read_to_string(&ch_config).unwrap();
    std::fs::write(
        &ch_config,
        format!(
            "[source]\ndbname = \"app\"\n\n{body}\n[backup]\narchive = \"file://{}\"\n",
            storage_root.display()
        ),
    )
    .unwrap();

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).unwrap();
    let guard = fx::ChildGuard::new(
        daemon
            .spawn_mode(
                &source,
                &ch_config,
                slot.walsender,
                "object-store",
                &["--bootstrap-backup-name", &backup_name],
                &[
                    ("PGHOST", socket_host),
                    ("PGPORT", source.config().port.to_string()),
                    ("PGUSER", "postgres".into()),
                    ("PGDATABASE", "postgres".into()),
                ],
            )
            .expect("spawn walshadow-stream"),
    );
    let result = (|| -> Result<()> {
        converge(&ch, &app, &tables).context("bootstrap rows")?;
        for t in tables {
            churn(&app, t, N_ROWS)?;
        }
        converge(&ch, &app, &tables).context("row changes")?;
        anyhow::ensure!(
            !daemon.shadow_data_dir.join("tablespace_map").exists(),
            "tablespace_map must retire before shadow starts"
        );
        Ok(())
    })();
    fx::finish_daemon(guard, &daemon, result);
}

/// `[toast] mode = "shadow"` reads unchanged external values back out of
/// shadow's own TOAST heap, which lives in the table's tablespace and
/// moves with it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_toast_in_tablespace() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    let (ts1, ts2) = (
        tablespace_dir(tmp.path(), "ts1"),
        tablespace_dir(tmp.path(), "ts2"),
    );
    source
        .apply_schema_dump(&format!(
            "CREATE TABLESPACE ts1 LOCATION '{ts1}';\n\
             CREATE TABLESPACE ts2 LOCATION '{ts2}';\n\
             CREATE SCHEMA s;\n\
             CREATE TABLE s.t (id int4 PRIMARY KEY, name text NOT NULL) TABLESPACE ts1;\n\
             ALTER TABLE s.t ALTER COLUMN name SET STORAGE EXTERNAL;\n\
             ALTER TABLE s.t REPLICA IDENTITY FULL;\n\
             INSERT INTO s.t SELECT g, repeat(md5(g::text), 400) \
               FROM generate_series(1, {N_ROWS}) g;\n\
             CHECKPOINT;\n\
             SELECT pg_switch_wal();\n"
        ))
        .unwrap();

    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
        .expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").unwrap();
    let ch_config = tmp.path().join("ch-config.toml");
    write_ch_config(&ch_config, slot.ch_tcp, &["t"]).unwrap();
    let body = std::fs::read_to_string(&ch_config).unwrap();
    std::fs::write(&ch_config, format!("{body}\n[toast]\nmode = \"shadow\"\n")).unwrap();

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).unwrap();
    let guard = fx::ChildGuard::new(
        daemon
            .spawn(&source, &ch_config, slot.walsender, &[])
            .expect("spawn walshadow-stream"),
    );
    // Key-only updates carry every external value unchanged
    let shift = |by: i32| format!("UPDATE s.t SET id = id + {by};\n");
    let result = (|| -> Result<()> {
        converge(&ch, &source, &["t"]).context("bootstrap rows")?;
        source.apply_schema_dump(&shift(1000))?;
        converge(&ch, &source, &["t"]).context("unchanged values in ts1")?;
        source.apply_schema_dump(&format!(
            "ALTER TABLE s.t SET TABLESPACE ts2;\n{}",
            shift(1000)
        ))?;
        converge(&ch, &source, &["t"]).context("unchanged values after move")
    })();
    fx::finish_daemon(guard, &daemon, result);
}
