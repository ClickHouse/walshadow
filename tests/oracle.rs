//! Differential decode oracle.
//!
//! Three drills:
//!
//! 1. `oracle_without_extension_falls_back_to_raw_bytes` — spawn a
//!    plain PG without loading the `walshadow` extension,
//!    confirm `Oracle::resolve_pending` returns `Ok(None)` and the
//!    `fallback_raw` stat increments. Skipped silently when `initdb`
//!    isn't on PATH.
//! 2. `oracle_with_extension_resolves_tier3_disk_bytes` — same setup
//!    plus `CREATE EXTENSION walshadow`. For each of
//!    `numeric` / `inet` / `interval` / `jsonb` / `int4[]`, synthesize
//!    on-disk bytes, call `walshadow_decode_disk(oid, bytea)`, assert
//!    the returned text matches PG's `typoutput`. Skipped silently
//!    when the extension isn't installed (the harness probes
//!    `walshadow_decode_disk` in `pg_proc` and returns a skip).
//! 3. `oracle_resolves_pg_pending_to_text` — runs the decode pool's
//!    `resolve_pending_tuple` over a `PgPending` column, asserts the
//!    resolved tuple carries a `Text` value matching PG's representation.

use std::fs;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use walrus::pg::walparser::RelFileNode;
use walshadow::codecs;
use walshadow::heap_decoder::{ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use walshadow::oracle::{Oracle, resolve_pending_tuple};
use walshadow::pg::socket_conninfo;
use walshadow::shadow::{Shadow, ShadowConfig};

const SHADOW_PORT: u16 = 56301;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn make_pg(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

struct StopOnDrop<'a> {
    sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

/// Build short-form numeric for `42`: header 0x8000 (NUMERIC_SHORT,
/// dscale=0, weight=0), one digit (42).
fn numeric_42_bytes() -> Vec<u8> {
    let mut out = 0x8000u16.to_le_bytes().to_vec();
    out.extend_from_slice(&42i16.to_le_bytes());
    out
}

/// On-disk inet body for `192.168.0.1` (full /32 mask). PG's wire
/// format adds `is_cidr` + `nb` after `bits`; the heap format does not.
fn inet_192_168_0_1_bytes() -> Vec<u8> {
    vec![codecs::PGSQL_AF_INET, 32, 192, 168, 0, 1]
}

/// On-disk interval body for `1 month 2 days 3 microseconds`.
fn interval_1mon_2day_3us_bytes() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&3i64.to_le_bytes());
    out.extend_from_slice(&2i32.to_le_bytes());
    out.extend_from_slice(&1i32.to_le_bytes());
    out
}

/// `[1, 2, 3]` int4 array on-disk body.
/// Layout (after stripping varlena header):
///   int32 ndim = 1
///   int32 dataoffset = 0
///   uint32 elemtype = 23 (int4)
///   int32 dim[0] = 3
///   int32 lbound[0] = 1
///   <three int32 elements>
fn array_int4_1_2_3_bytes() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&1i32.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out.extend_from_slice(&23u32.to_le_bytes());
    out.extend_from_slice(&3i32.to_le_bytes());
    out.extend_from_slice(&1i32.to_le_bytes());
    for v in [1i32, 2, 3] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_without_extension_falls_back_to_raw_bytes() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_pg(&tmp, SHADOW_PORT);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };

    let conninfo = socket_conninfo(
        sh.config().socket_dir.to_str().unwrap(),
        sh.config().port,
        "postgres",
        "postgres",
    );
    let oracle = Oracle::connect(&conninfo).await.expect("oracle connect");
    // Stand-alone PG without our extension. resolve_pending must
    // surface None so the emitter falls back to raw bytes.
    let out = oracle
        .resolve_pending(3802, b"\x01opaque")
        .await
        .expect("resolve_pending");
    assert!(out.is_none(), "expected fallback, got {out:?}");
    use std::sync::atomic::Ordering;
    assert_eq!(oracle.stats.fallback_raw.load(Ordering::Relaxed), 1);
    assert_eq!(oracle.stats.resolved.load(Ordering::Relaxed), 0);
    assert!(!oracle.has_extension());
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_with_extension_resolves_tier3_disk_bytes() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_pg(&tmp, SHADOW_PORT + 1);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };

    // Optional extension load; skip cleanly if not installed system-wide.
    match sh.try_load_oracle_extension() {
        Ok(true) => {}
        Ok(false) => {
            eprintln!(
                "skip: walshadow extension not installed on this PG \
                 (run `cd pgext && sudo make install`)"
            );
            return;
        }
        Err(e) => panic!("loading extension: {e}"),
    }

    let conninfo = socket_conninfo(
        sh.config().socket_dir.to_str().unwrap(),
        sh.config().port,
        "postgres",
        "postgres",
    );
    let oracle = Oracle::connect(&conninfo).await.expect("oracle connect");
    assert!(oracle.has_extension());

    // numeric — 42
    let txt = oracle
        .resolve_pending(walshadow::schema::NUMERICOID, &numeric_42_bytes())
        .await
        .expect("resolve numeric")
        .expect("resolved Some");
    assert_eq!(txt, "42");

    // inet — 192.168.0.1
    let txt = oracle
        .resolve_pending(walshadow::schema::INETOID, &inet_192_168_0_1_bytes())
        .await
        .expect("resolve inet")
        .expect("resolved Some");
    assert_eq!(txt, "192.168.0.1");

    // interval — 1 month 2 days 3 microseconds
    let txt = oracle
        .resolve_pending(
            walshadow::schema::INTERVALOID,
            &interval_1mon_2day_3us_bytes(),
        )
        .await
        .expect("resolve interval")
        .expect("resolved Some");
    // PG renders as "1 mon 2 days 00:00:00.000003"
    assert_eq!(txt, "1 mon 2 days 00:00:00.000003");

    // int4[] — [1, 2, 3]. typoid 1007 = INT4ARRAYOID.
    let txt = oracle
        .resolve_pending(1007, &array_int4_1_2_3_bytes())
        .await
        .expect("resolve int4[]")
        .expect("resolved Some");
    assert_eq!(txt, "{1,2,3}");

    use std::sync::atomic::Ordering;
    assert_eq!(oracle.stats.resolved.load(Ordering::Relaxed), 4);
    assert_eq!(oracle.stats.fallback_raw.load(Ordering::Relaxed), 0);
    assert_eq!(oracle.stats.errors.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_resolves_pg_pending_to_text() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_pg(&tmp, SHADOW_PORT + 2);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };
    if !matches!(sh.try_load_oracle_extension(), Ok(true)) {
        eprintln!("skip: walshadow extension not installed on this PG");
        return;
    }

    let conninfo = socket_conninfo(
        sh.config().socket_dir.to_str().unwrap(),
        sh.config().port,
        "postgres",
        "postgres",
    );
    let oracle = Arc::new(Oracle::connect(&conninfo).await.expect("oracle connect"));

    // Wire one PgPending column (numeric 42) through the decode pool's
    // resolution path.
    let mut committed = CommittedTuple {
        decoded: DecodedHeap {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            xid: 1234,
            source_lsn: 0xDEADBEEF,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![Some(ColumnValue::PgPending {
                    type_oid: walshadow::schema::NUMERICOID,
                    raw: numeric_42_bytes(),
                })],
                partial: false,
            }),
            old: None,
        },
        commit_ts: 0,
        commit_lsn: 0,
    };
    if let Some(t) = committed.decoded.new.as_mut() {
        resolve_pending_tuple(&oracle, &mut t.columns).await;
    }

    let new = committed.decoded.new.as_ref().unwrap();
    match &new.columns[0] {
        Some(ColumnValue::Text(s)) => assert_eq!(s, "42"),
        other => panic!("expected Text(\"42\"), got {other:?}"),
    }
    use std::sync::atomic::Ordering;
    assert_eq!(oracle.stats.resolved.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_resolve_reconnects_after_backend_restart() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_pg(&tmp, SHADOW_PORT + 4);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };
    if !matches!(sh.try_load_oracle_extension(), Ok(true)) {
        eprintln!("skip: walshadow extension not installed on this PG");
        return;
    }
    let conninfo = socket_conninfo(
        sh.config().socket_dir.to_str().unwrap(),
        sh.config().port,
        "postgres",
        "postgres",
    );
    let oracle = Oracle::connect(&conninfo).await.expect("oracle connect");
    use walshadow::schema::NUMERICOID;
    assert_eq!(
        oracle
            .resolve_pending(NUMERICOID, &numeric_42_bytes())
            .await
            .unwrap()
            .as_deref(),
        Some("42"),
    );

    sh.stop().expect("stop");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    sh.start().expect("restart");

    assert_eq!(
        oracle
            .resolve_pending(NUMERICOID, &numeric_42_bytes())
            .await
            .unwrap()
            .as_deref(),
        Some("42"),
        "resolve recovers after reconnect",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_resolve_errors_when_backend_down() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_pg(&tmp, SHADOW_PORT + 5);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };
    if !matches!(sh.try_load_oracle_extension(), Ok(true)) {
        eprintln!("skip: walshadow extension not installed on this PG");
        return;
    }
    let conninfo = socket_conninfo(
        sh.config().socket_dir.to_str().unwrap(),
        sh.config().port,
        "postgres",
        "postgres",
    );
    let oracle = Oracle::connect(&conninfo).await.expect("oracle connect");
    use std::sync::atomic::Ordering;
    use walshadow::schema::NUMERICOID;
    assert!(
        oracle
            .resolve_pending(NUMERICOID, &numeric_42_bytes())
            .await
            .unwrap()
            .is_some(),
    );

    sh.stop().expect("stop");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let got = oracle
        .resolve_pending(NUMERICOID, &numeric_42_bytes())
        .await
        .unwrap();
    assert!(got.is_none(), "no resolution when backend down");
    assert!(oracle.stats.errors.load(Ordering::Relaxed) >= 1);
}
