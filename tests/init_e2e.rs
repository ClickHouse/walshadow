//! `walshadow-stream init` against a live source PG + ClickHouse.
//!
//! Covers two connection URLs through bootable config output. Live run proves:
//!
//! - a unix-socket `postgres://…?host=%2F…` URL reaches a socket-only
//!   cluster, and the written `[source]` round-trips back to it
//! - the pre-flight report reads the real `wal_level` / role privileges
//! - a table without a row key is skipped, one with a PK is opted in
//! - `[ch] database` is created when absent — CH refuses the handshake
//!   for a missing one, so the daemon could not create its own
//!
//! Skipped silently without `initdb` or the `clickhouse` multitool.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;

use walshadow::ch_emitter::EmitterConfig;
use walshadow::config::SourceConn;
use walshadow::init::{InitOpts, run};
use walshadow::schema::RelName;

/// Percent-encode a socket dir into the libpq URL spelling
fn socket_url(socket_dir: &std::path::Path, dbname: &str) -> String {
    let encoded = socket_dir.to_str().unwrap().replace('/', "%2F");
    format!(
        "postgres://postgres@/{dbname}?host={encoded}&port={}&sslmode=disable",
        fx::PG_SOURCE_PORT
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_probes_both_ends_and_writes_a_bootable_config() {
    if !fx::tools::pg_available() {
        return;
    }
    if !fx::tools::clickhouse_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };

    // One replicable relation, one the picker must refuse
    source
        .apply_schema_dump(
            "CREATE SCHEMA app;\n\
             CREATE TABLE app.users (id int4 PRIMARY KEY, name text);\n\
             CREATE TABLE app.audit (msg text);\n",
        )
        .expect("source schema");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");

    let config = tmp.path().join("ch-config.toml");
    run(InitOpts {
        config: config.clone(),
        source_url: Some(socket_url(&source.config().socket_dir, "postgres")),
        // `cdc` does not exist yet: init has to create it
        ch_url: Some(format!(
            "clickhouse://default@127.0.0.1:{}/cdc",
            slot.ch_tcp
        )),
        tables: Vec::new(),
        all_tables: true,
        namespace: None,
        initial_load: "copy".into(),
        force: false,
    })
    .await
    .expect("init");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&config).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "config carries passwords");
    }

    let written = fs::read_to_string(&config).expect("read config");
    let root: toml::Table = written.parse().expect("config parses");

    let conn = SourceConn::from_table(&root).expect("[source]");
    assert_eq!(conn.host, source.config().socket_dir.to_str().unwrap());
    assert_eq!(conn.port, fx::PG_SOURCE_PORT);
    assert_eq!(conn.dbname, "postgres");

    let cfg = EmitterConfig::from_table(&root).expect("[ch]");
    assert_eq!(cfg.conn.port, slot.ch_tcp);
    assert_eq!(cfg.conn.database, "cdc");

    let users = cfg
        .table_opt_ins
        .get(&RelName::new("app", "users"))
        .expect("keyed table opted in");
    assert_eq!(users.replicate, Some(true));
    assert_eq!(users.initial_load.as_deref(), Some("copy"));
    assert!(
        !cfg.table_opt_ins
            .contains_key(&RelName::new("app", "audit")),
        "keyless table stays out of the config",
    );

    let exists = ch.query("EXISTS DATABASE cdc").expect("query ch");
    assert_eq!(exists.trim(), "1", "init created the destination database");

    // Second run refuses to clobber, and says how to override
    let again = run(InitOpts {
        config: config.clone(),
        source_url: Some(socket_url(&source.config().socket_dir, "postgres")),
        ch_url: Some(format!(
            "clickhouse://default@127.0.0.1:{}/cdc",
            slot.ch_tcp
        )),
        tables: Vec::new(),
        all_tables: true,
        namespace: None,
        initial_load: "copy".into(),
        force: false,
    })
    .await
    .expect_err("existing config is not overwritten");
    assert!(again.to_string().contains("--force"), "{again}");
}

/// `walshadow-stream init` with stdin closed, PATH holding only `bins`.
/// Returns (success, stdout, stderr)
fn init_bin(
    args: &[&str],
    env_url: Option<&str>,
    bins: &std::path::Path,
) -> (bool, String, String) {
    let mut cmd =
        std::process::Command::new(std::env::var("CARGO_BIN_EXE_walshadow-stream").unwrap());
    cmd.arg("init")
        .args(args)
        .env("PATH", bins)
        .env_remove(walshadow::init::PG_URL_ENV)
        .env_remove(walshadow::init::CH_URL_ENV)
        .env_remove("WALSHADOW_CH_CONFIG")
        .stdin(std::process::Stdio::null());
    if let Some(url) = env_url {
        cmd.env(walshadow::init::PG_URL_ENV, url);
    }
    let out = cmd.output().expect("spawn init");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Spawned `init` on a source the daemon cannot use yet: every finding comes
/// with its fix, and paths that would prompt refuse without a terminal
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_subcommand_reports_fixes_without_a_terminal() {
    if !fx::tools::pg_available() {
        return;
    }
    if !fx::tools::clickhouse_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    // Base conf only: wal_level = replica, max_wal_senders = 0
    let source = fx::make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(
            "CREATE ROLE app LOGIN;\n\
             CREATE SCHEMA app AUTHORIZATION app;\n\
             CREATE SCHEMA empty;\n\
             CREATE TABLE app.users (id int4 PRIMARY KEY, name text);\n",
        )
        .expect("source schema");

    let ch_tmp = tempfile::tempdir().unwrap();
    let _ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let ch_url = format!("clickhouse://default@127.0.0.1:{}/cdc", slot.ch_tcp);
    let encoded = source
        .config()
        .socket_dir
        .to_str()
        .unwrap()
        .replace('/', "%2F");
    let app_url = format!(
        "postgres://app@/postgres?host={encoded}&port={}&sslmode=disable&slot=absent",
        fx::PG_SOURCE_PORT
    );

    // Empty PATH: no `initdb` for the shadow major check
    let no_bins = tmp.path().join("no-bins");
    fs::create_dir_all(&no_bins).unwrap();
    // `initdb` of a major no source runs
    let old_bins = tmp.path().join("old-bins");
    fs::create_dir_all(&old_bins).unwrap();
    let fake_initdb = old_bins.join("initdb");
    fs::write(
        &fake_initdb,
        "#!/bin/sh\necho 'initdb (PostgreSQL) 9.6.24'\n",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fake_initdb, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let config = tmp.path().join("etc/walshadow/ch-config.toml");
    let config_arg = config.to_str().unwrap();

    let (ok, _, err) = init_bin(&["--config", config_arg], None, &no_bins);
    assert!(!ok);
    assert!(err.contains("no Source Postgres URL"), "{err}");

    let (ok, _, err) = init_bin(&["--config", config_arg], Some(&app_url), &no_bins);
    assert!(!ok);
    assert!(err.contains("no ClickHouse URL"), "{err}");

    let base = ["--config", config_arg, "--ch-url", ch_url.as_str()];
    let (ok, _, err) = init_bin(
        &[&base[..], &["--table", "app", "nope"][..]].concat(),
        Some(&app_url),
        &no_bins,
    );
    assert!(!ok);
    assert!(err.contains("app.nope not found on source"), "{err}");

    let (ok, out, err) = init_bin(
        &[
            &base[..],
            &["--table", "app", "users", "--initial-load", "none"][..],
        ]
        .concat(),
        Some(&app_url),
        &old_bins,
    );
    assert!(ok, "{err}");
    for want in [
        "ALTER SYSTEM SET wal_level = logical;",
        "SELECT pg_create_physical_replication_slot('absent');",
        "✗ app lacks REPLICATION",
        "ALTER ROLE app REPLICATION;",
        "✗ max_wal_senders = 0",
        "shadow binaries are PostgreSQL 9, source is",
        "fix the ✗ items above first",
    ] {
        assert!(out.contains(want), "missing {want:?} in:\n{out}");
    }
    let root: toml::Table = fs::read_to_string(&config).unwrap().parse().unwrap();
    let cfg = EmitterConfig::from_table(&root).expect("[ch]");
    let users = cfg
        .table_opt_ins
        .get(&RelName::new("app", "users"))
        .expect("named table opted in");
    assert_eq!(users.initial_load.as_deref(), Some("none"));

    let (ok, out, err) = init_bin(
        &[
            &base[..],
            &["--force", "--all-tables", "--schema", "empty"][..],
        ]
        .concat(),
        Some(&app_url),
        &no_bins,
    );
    assert!(ok, "{err}");
    assert!(out.contains("no `initdb` on PATH"), "{out}");
    assert!(out.contains("no tables selected"), "{out}");

    let (ok, _, err) = init_bin(
        &[&base[..], &["--force"][..]].concat(),
        Some(&app_url),
        &no_bins,
    );
    assert!(!ok);
    assert!(err.contains("no terminal for the table picker"), "{err}");
}
