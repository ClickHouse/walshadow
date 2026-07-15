//! Operational scaffolding integration coverage.
//!
//! Two drills, both lean enough to run with `initdb` on PATH and no
//! ClickHouse or pg_basebackup dependency:
//!
//! 1. Pre-flight rejects a source whose `wal_level != 'logical'` AND
//!    mapped relations without a usable row key (`DEFAULT` without PK,
//!    `NOTHING` even with one), surfacing every finding from one probe
//!    instead of one-error-at-a-time.
//! 2. Pre-flight passes once the source's `wal_level` is fixed and every
//!    mapped relation carries a key: `DEFAULT`+PK, `USING INDEX`, `FULL`.
//!
//! The metrics endpoint + retention trim are covered by lib unit tests
//! (`metrics::tests::http_serve_returns_text_format_body`,
//! `retention::tests::*`); their HTTP/file-system surfaces don't need a
//! live PG to validate.

use std::fs;
use std::io::Write;
use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use walshadow::ch_emitter::EmitterConfig;
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::preflight::{Inputs, PreflightError};
use walshadow::schema::RelName;
use walshadow::shadow::{Shadow, ShadowConfig};

// Non-overlapping ports so a leftover from an earlier failed run doesn't
// shadow the next start.
const SOURCE_PORT_A: u16 = 56301;
const SHADOW_PORT_A: u16 = 56302;
const SOURCE_PORT_B: u16 = 56303;
const SHADOW_PORT_B: u16 = 56304;
const SOURCE_PORT_C: u16 = 56305;
const SHADOW_PORT_C: u16 = 56306;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_pg(tmp: &tempfile::TempDir, name: &str, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join(format!("{name}-data")),
        tmp.path().join(format!("{name}-filtered")),
    );
    cfg.port = port;
    cfg.socket_dir = tmp.path().join(format!("{name}-sock"));
    cfg.ctl_timeout = Duration::from_secs(30);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

fn append_conf(sh: &Shadow, wal_level: &str) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# walshadow ops-test source overrides").unwrap();
    writeln!(f, "wal_level = {wal_level}").unwrap();
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

async fn connect_sql(socket: &std::path::Path, port: u16) -> Result<tokio_postgres::Client> {
    let conninfo = format!(
        "host={} port={} user=postgres dbname=postgres",
        socket.display(),
        port,
    );
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

fn mapping_for(rels: &[(&str, &str)]) -> EmitterConfig {
    let mut cfg = EmitterConfig::default();
    for (ns, rel) in rels {
        cfg.tables.insert(
            RelName::new(ns, rel),
            TableMapping {
                target: TableTarget::new("ch", rel),
                columns: vec![ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int64".into(),
                }],
            },
        );
    }
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflight_rejects_wal_level_and_missing_replica_identity() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_pg(&tmp, "src-bad", SOURCE_PORT_A);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    // wal_level=replica + keyless relations so both validators trip on
    // the same probe: `t` is DEFAULT (`d`) without a PK, `t_nothing` is
    // NOTHING (`n`) — its PK doesn't rescue it.
    append_conf(&source, "replica");
    source.start().expect("start source");
    let _src_stop = StopOnDrop { sh: &source };
    source
        .apply_schema_dump(
            "CREATE SCHEMA s10;\n\
             CREATE TABLE s10.t (id bigint, payload text);\n\
             CREATE TABLE s10.t_nothing (id bigint PRIMARY KEY, payload text);\n\
             ALTER TABLE s10.t_nothing REPLICA IDENTITY NOTHING;\n",
        )
        .expect("schema");

    let shadow = make_pg(&tmp, "shd-bad", SHADOW_PORT_A);
    shadow.initdb().expect("initdb shadow");
    shadow.write_base_conf().expect("shadow base conf");
    shadow.start().expect("start shadow");
    let _shd_stop = StopOnDrop { sh: &shadow };

    let src_sql = connect_sql(&source.config().socket_dir, SOURCE_PORT_A)
        .await
        .expect("source sql");
    let shd_sql = connect_sql(&shadow.config().socket_dir, SHADOW_PORT_A)
        .await
        .expect("shadow sql");

    let ch_config = mapping_for(&[("s10", "t"), ("s10", "t_nothing")]);
    let report = walshadow::preflight::run(Inputs {
        source_version_num: 170_000, // > 16, so version check passes
        source_sql: &src_sql,
        shadow_sql: &shd_sql,
        slot: None,
        ch_config: Some(&ch_config),
    })
    .await
    .expect("preflight probe runs");
    assert!(!report.is_ok(), "{:?}", report.errors);

    let has_wal_level = report
        .errors
        .iter()
        .any(|e| matches!(e, PreflightError::WalLevel { .. }));
    assert!(
        has_wal_level,
        "expected WalLevel error in {:?}",
        report.errors
    );

    let has_default_no_pk = report.errors.iter().any(|e| {
        matches!(
            e,
            PreflightError::BadReplicaIdentity { rel, got } if *rel == RelName::new("s10", "t") && *got == 'd'
        )
    });
    assert!(
        has_default_no_pk,
        "expected BadReplicaIdentity for s10.t got 'd' in {:?}",
        report.errors,
    );

    let has_nothing = report.errors.iter().any(|e| {
        matches!(
            e,
            PreflightError::BadReplicaIdentity { rel, got } if *rel == RelName::new("s10", "t_nothing") && *got == 'n'
        )
    });
    assert!(
        has_nothing,
        "expected BadReplicaIdentity for s10.t_nothing got 'n' in {:?}",
        report.errors,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflight_rejects_old_version_missing_slot_and_unknown_rel() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_pg(&tmp, "src-old", SOURCE_PORT_C);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    append_conf(&source, "logical");
    source.start().expect("start source");
    let _src_stop = StopOnDrop { sh: &source };

    let shadow = make_pg(&tmp, "shd-old", SHADOW_PORT_C);
    shadow.initdb().expect("initdb shadow");
    shadow.write_base_conf().expect("shadow base conf");
    shadow.start().expect("start shadow");
    let _shd_stop = StopOnDrop { sh: &shadow };

    let src_sql = connect_sql(&source.config().socket_dir, SOURCE_PORT_C)
        .await
        .expect("source sql");
    let shd_sql = connect_sql(&shadow.config().socket_dir, SHADOW_PORT_C)
        .await
        .expect("shadow sql");

    let ch_config = mapping_for(&[("s11", "ghost")]);
    let report = walshadow::preflight::run(Inputs {
        source_version_num: 150_000, // < 16: too old, and major 15 ≠ shadow major
        source_sql: &src_sql,
        shadow_sql: &shd_sql,
        slot: Some("walshadow_absent_slot"),
        ch_config: Some(&ch_config),
    })
    .await
    .expect("preflight probe runs");
    assert!(!report.is_ok(), "{:?}", report.errors);

    let has = |f: &dyn Fn(&PreflightError) -> bool| report.errors.iter().any(f);
    assert!(
        has(&|e| matches!(e, PreflightError::SourceVersionTooOld { .. })),
        "{:?}",
        report.errors
    );
    assert!(
        has(&|e| matches!(e, PreflightError::MajorMismatch { .. })),
        "{:?}",
        report.errors
    );
    assert!(
        has(
            &|e| matches!(e, PreflightError::SlotMissing { slot } if slot == "walshadow_absent_slot")
        ),
        "{:?}",
        report.errors
    );
    assert!(
        has(
            &|e| matches!(e, PreflightError::MappedRelMissing { rel } if *rel == RelName::new("s11", "ghost"))
        ),
        "{:?}",
        report.errors
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflight_passes_once_source_is_logical_and_relations_keyed() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_pg(&tmp, "src-ok", SOURCE_PORT_B);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    append_conf(&source, "logical");
    source.start().expect("start source");
    let _src_stop = StopOnDrop { sh: &source };
    // One mapped relation per accepted identity shape: DEFAULT with PK
    // (no ALTER needed), USING INDEX, FULL (keyless is fine under FULL).
    source
        .apply_schema_dump(
            "CREATE SCHEMA s10;\n\
             CREATE TABLE s10.t (id bigint PRIMARY KEY, payload text);\n\
             CREATE TABLE s10.t_idx (id bigint NOT NULL, payload text);\n\
             CREATE UNIQUE INDEX t_idx_key ON s10.t_idx (id);\n\
             ALTER TABLE s10.t_idx REPLICA IDENTITY USING INDEX t_idx_key;\n\
             CREATE TABLE s10.t_full (id bigint, payload text);\n\
             ALTER TABLE s10.t_full REPLICA IDENTITY FULL;\n",
        )
        .expect("schema");

    let shadow = make_pg(&tmp, "shd-ok", SHADOW_PORT_B);
    shadow.initdb().expect("initdb shadow");
    shadow.write_base_conf().expect("shadow base conf");
    shadow.start().expect("start shadow");
    let _shd_stop = StopOnDrop { sh: &shadow };

    let src_sql = connect_sql(&source.config().socket_dir, SOURCE_PORT_B)
        .await
        .expect("source sql");
    let shd_sql = connect_sql(&shadow.config().socket_dir, SHADOW_PORT_B)
        .await
        .expect("shadow sql");

    let ch_config = mapping_for(&[("s10", "t"), ("s10", "t_idx"), ("s10", "t_full")]);
    // Make the version-num arg the real source version so the
    // major-mismatch check exercises against the same version both
    // sides are running (source + shadow share initdb's binary).
    let src_version: i32 = src_sql
        .query_one("SHOW server_version_num", &[])
        .await
        .unwrap()
        .get::<_, String>(0)
        .parse()
        .unwrap();
    let report = walshadow::preflight::run(Inputs {
        source_version_num: src_version,
        source_sql: &src_sql,
        shadow_sql: &shd_sql,
        slot: None,
        ch_config: Some(&ch_config),
    })
    .await
    .expect("preflight probe runs");
    assert!(report.is_ok(), "unexpected findings: {:?}", report.errors);
}
