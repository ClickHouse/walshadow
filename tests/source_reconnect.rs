//! Source replication reconnect + recycled-segment behaviour, driven directly
//! against a throwaway source PG. Deterministic replacement for inducing a drop
//! via a stalled sink + `wal_sender_timeout`.
//!
//! Skipped silently when `initdb`/`psql` are absent.

use std::fs;
use std::io::Write as _;
use std::process::Command;
use std::time::Duration;

use walrus::pg::backup::parse_pg_lsn;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::SslMode;
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::source_feed::{SourceFeed, StandbyStatus};

fn tools_available() -> bool {
    ["initdb", "psql"].iter().all(|bin| {
        Command::new(bin)
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    })
}

struct StopOnDrop<'a>(&'a Shadow);
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.0.stop();
    }
}

fn make_source(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

fn append_conf(sh: &Shadow, extra: &[&str]) {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, "\n# source_reconnect test overrides").unwrap();
    writeln!(f, "wal_level = replica").unwrap();
    writeln!(f, "max_wal_senders = 4").unwrap();
    for line in extra {
        writeln!(f, "{line}").unwrap();
    }
}

fn pg_cfg(sh: &Shadow) -> PgConfig {
    PgConfig {
        host: sh.config().socket_dir.to_str().unwrap().to_string(),
        port: sh.config().port,
        user: "postgres".into(),
        password: None,
        database: "postgres".into(),
        application_name: "source-reconnect-test".into(),
        sslmode: SslMode::Disable,
    }
}

fn status(lsn: u64) -> StandbyStatus {
    StandbyStatus::collapsed(lsn)
}

fn churn_wal(source: &Shadow) {
    source
        .psql_one("CREATE TABLE IF NOT EXISTS churn(id int, pad text)")
        .unwrap();
    for _ in 0..12 {
        source
            .psql_one(
                "INSERT INTO churn SELECT g, repeat('x', 900) \
                 FROM generate_series(1, 20000) g",
            )
            .unwrap();
        source.psql_one("SELECT pg_switch_wal()").unwrap();
    }
    source.psql_one("CHECKPOINT").unwrap();
    source.psql_one("CHECKPOINT").unwrap();
}

/// Killing the walsender drops the stream; reconnecting at a still-retained
/// LSN resumes cleanly.
#[tokio::test]
async fn reconnect_resumes_after_walsender_terminated() {
    if !tools_available() {
        eprintln!("skip: no initdb/psql on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, 55731);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    append_conf(&source, &[]);
    source.start().unwrap();
    let _stop = StopOnDrop(&source);

    let cfg = pg_cfg(&source);
    let mut feed = SourceFeed::connect(&cfg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    feed.start_physical_replication(None, ident.xlogpos, ident.timeline)
        .await
        .unwrap();

    source
        .psql_one("CREATE TABLE t(id int); INSERT INTO t SELECT g FROM generate_series(1, 1000) g")
        .unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        feed.next_chunk(status(ident.xlogpos), &mut buf),
    )
    .await;

    source
        .psql_one(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE backend_type = 'walsender'",
        )
        .unwrap();

    let reconnected = SourceFeed::reconnect(
        &cfg,
        None,
        ident.xlogpos,
        ident.timeline,
        Duration::from_secs(1),
    )
    .await;
    assert!(
        reconnected.is_ok(),
        "reconnect at retained LSN should resume: {:?}",
        reconnected.err()
    );
}

/// Once the source recycles the segment holding the resume LSN, the reconnect
/// surfaces PG's SQLSTATE 58P01 (start is accepted; the error arrives while
/// streaming), classified as [`WalSegmentRemoved`].
#[tokio::test]
async fn recycled_segment_surfaces_58p01() {
    if !tools_available() {
        eprintln!("skip: no initdb/psql on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, 55732);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    append_conf(
        &source,
        &[
            "wal_keep_size = 0",
            "max_wal_size = 48MB",
            "min_wal_size = 32MB",
            "archive_mode = off",
            "autovacuum = off",
        ],
    );
    source.start().unwrap();
    let _stop = StopOnDrop(&source);

    let cfg = pg_cfg(&source);
    let mut feed = SourceFeed::connect(&cfg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let old_lsn = ident.xlogpos;

    churn_wal(&source);

    let res =
        SourceFeed::reconnect(&cfg, None, old_lsn, ident.timeline, Duration::from_secs(1)).await;
    let err = match res {
        Err(e) => e,
        Ok(mut feed) => {
            let mut buf = Vec::new();
            match tokio::time::timeout(
                Duration::from_secs(5),
                feed.next_chunk(status(old_lsn), &mut buf),
            )
            .await
            {
                Ok(Err(e)) => e,
                other => panic!("expected segment-removed error, got {other:?}"),
            }
        }
    };
    assert!(
        walshadow::source_feed::is_wal_segment_removed(&err),
        "expected recycled-segment classification, got: {err:#}"
    );
    assert!(
        err.downcast_ref::<walshadow::source_feed::WalSegmentRemoved>()
            .is_some()
            || format!("{err:#}").contains(walshadow::source_feed::SQLSTATE_UNDEFINED_FILE),
        "expected typed WalSegmentRemoved or SQLSTATE, got: {err:#}"
    );
}

/// A physical slot reserving WAL immediately keeps the resume segment alive
/// through the same churn that recycles it in `recycled_segment_surfaces_58p01`,
/// so reconnecting against the slot resumes cleanly.
#[tokio::test]
async fn slot_prevents_segment_recycle() {
    if !tools_available() {
        eprintln!("skip: no initdb/psql on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = make_source(&tmp, 55733);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    append_conf(
        &source,
        &[
            "wal_keep_size = 0",
            "max_wal_size = 48MB",
            "min_wal_size = 32MB",
            "archive_mode = off",
            "autovacuum = off",
        ],
    );
    source.start().unwrap();
    let _stop = StopOnDrop(&source);

    let cfg = pg_cfg(&source);
    let slot = "walshadow_recycle_test";
    let mut feed = SourceFeed::connect(&cfg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    feed.ensure_physical_slot(slot).await.unwrap();
    let old_lsn = parse_pg_lsn(&source.psql_one("SELECT pg_current_wal_lsn()").unwrap()).unwrap();

    churn_wal(&source);

    let wal_status = source
        .psql_one(&format!(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .unwrap();
    assert!(
        wal_status == "reserved" || wal_status == "extended",
        "slot must still retain WAL, got wal_status={wal_status}"
    );

    let mut resumed = SourceFeed::reconnect(
        &cfg,
        Some(slot),
        old_lsn,
        ident.timeline,
        Duration::from_secs(1),
    )
    .await
    .expect("reconnect against slot should resume at retained LSN");
    let mut buf = Vec::new();
    let chunk = tokio::time::timeout(
        Duration::from_secs(5),
        resumed.next_chunk(status(old_lsn), &mut buf),
    )
    .await
    .expect("next_chunk should not time out");
    assert!(
        chunk.is_ok(),
        "streaming from a slot-retained LSN must not error: {:?}",
        chunk.err()
    );
}
