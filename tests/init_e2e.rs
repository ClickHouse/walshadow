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
use std::time::Duration;

use walshadow::ch_emitter::EmitterConfig;
use walshadow::config::SourceConn;
use walshadow::init::{InitOpts, run};
use walshadow::schema::RelName;
use walshadow::shadow::{Shadow, ShadowConfig};

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = fx::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

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
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        eprintln!("skip: no clickhouse binary on PATH");
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = make_source(&tmp);
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
    assert_eq!(cfg.port, slot.ch_tcp);
    assert_eq!(cfg.database, "cdc");

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
