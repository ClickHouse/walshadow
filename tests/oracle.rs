//! Differential decode oracle.
//!
//! Four drills, each against a PG started with the bridge worker preloaded
//! (`dynamic_library_path` points at the `pgext` build tree, so no
//! `make install` is needed). Skipped silently when `initdb` isn't on PATH or
//! `pgext` hasn't been built:
//!
//! 1. `oracle_resolves_tier3_disk_bytes` — for each of `numeric` / `inet` /
//!    `interval` / `int4[]`, synthesize on-disk bytes and assert the resolved
//!    text matches PG's `typoutput`.
//! 2. `oracle_falls_back_on_undecodable_bytes` — a body `typoutput` rejects
//!    leaves the column unresolved and counts `fallback_raw`, with the oracle
//!    still serving afterwards.
//! 3. `oracle_resolves_pg_pending_to_text` — runs the decode pool's
//!    `resolve_pending_tuple` over a `PgPending` column, asserts the resolved
//!    tuple carries a `Text` value matching PG's representation.
//! 4. `oracle_recovers_after_cluster_restart` — resolution fails while the
//!    cluster is down (counted `errors`), recovers once it is back.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use walrus::pg::walparser::RelFileNode;
use walshadow::codecs;
use walshadow::heap_decoder::{ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use walshadow::oracle::{Oracle, resolve_pending_tuple};
use walshadow::schema::{INETOID, INTERVALOID, NUMERICOID};
use walshadow::shadow::{BridgeConf, Shadow, ShadowConfig};

const SHADOW_PORT: u16 = 56301;
/// int4 array, ie `INT4ARRAYOID`
const INT4ARRAYOID: u32 = 1007;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build tree holding `walshadow.so`, fed to PG as `dynamic_library_path`
fn pgext_dir() -> Option<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("pgext");
    dir.join("walshadow.so").is_file().then_some(dir)
}

struct StopOnDrop {
    sh: Shadow,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

/// `None` skips the caller: no PG, or pgext unbuilt
fn start_pg(tmp: &tempfile::TempDir, port: u16) -> Option<StopOnDrop> {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return None;
    }
    let Some(lib_dir) = pgext_dir() else {
        eprintln!("skip: pgext not built (run `make -C pgext`)");
        return None;
    };
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    let mut bridge = BridgeConf::in_dir(&cfg.socket_dir);
    bridge.library_dir = Some(lib_dir);
    cfg.bridge = Some(bridge);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();

    let sh = Shadow::new(cfg);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    sh.start().expect("start");
    Some(StopOnDrop { sh })
}

async fn oracle_on(sh: &Shadow) -> Oracle {
    let path = sh.bridge_socket().expect("bridge configured");
    let bridge = walshadow::bridge::connect_with_budget(path, Duration::from_secs(20))
        .await
        .unwrap_or_else(|e| panic!("bridge connect on {}: {e}", path.display()));
    Oracle::new(Arc::new(bridge))
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
async fn oracle_resolves_tier3_disk_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, SHADOW_PORT) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;

    let cases: [(u32, Vec<u8>, &str); 4] = [
        (NUMERICOID, numeric_42_bytes(), "42"),
        (INETOID, inet_192_168_0_1_bytes(), "192.168.0.1"),
        (
            INTERVALOID,
            interval_1mon_2day_3us_bytes(),
            // PG renders as "1 mon 2 days 00:00:00.000003"
            "1 mon 2 days 00:00:00.000003",
        ),
        (INT4ARRAYOID, array_int4_1_2_3_bytes(), "{1,2,3}"),
    ];
    for (oid, raw, want) in &cases {
        let got = oracle.resolve_pending(*oid, raw).await;
        assert_eq!(got.as_deref(), Some(*want), "oid {oid}");
    }

    assert_eq!(oracle.stats.resolved.load(Ordering::Relaxed), 4);
    assert_eq!(oracle.stats.fallback_raw.load(Ordering::Relaxed), 0);
    assert_eq!(oracle.stats.errors.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_falls_back_on_undecodable_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, SHADOW_PORT + 1) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;

    // int4 body shorter than typlen: the worker raises per item rather than
    // failing the request
    assert!(oracle.resolve_pending(23, b"\x00").await.is_none());
    // Type oid with no pg_type row
    assert!(oracle.resolve_pending(2147483647, b"\x00").await.is_none());
    assert_eq!(oracle.stats.fallback_raw.load(Ordering::Relaxed), 2);
    assert_eq!(oracle.stats.errors.load(Ordering::Relaxed), 0);

    // Item errors are per item, so the oracle still serves
    assert_eq!(
        oracle
            .resolve_pending(NUMERICOID, &numeric_42_bytes())
            .await
            .as_deref(),
        Some("42"),
    );
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_resolves_pg_pending_to_text() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, SHADOW_PORT + 2) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;

    // Wire two PgPending columns through the decode pool's resolution path;
    // one request must cover the whole tuple.
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
                columns: vec![
                    Some(ColumnValue::PgPending {
                        type_oid: NUMERICOID,
                        raw: numeric_42_bytes(),
                    }),
                    Some(ColumnValue::Int4(7)),
                    Some(ColumnValue::Unsupported {
                        type_oid: INT4ARRAYOID,
                        raw: array_int4_1_2_3_bytes(),
                    }),
                ],
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
    assert!(
        matches!(&new.columns[0], Some(ColumnValue::Text(s)) if s == "42"),
        "got {:?}",
        new.columns[0],
    );
    assert!(matches!(&new.columns[1], Some(ColumnValue::Int4(7))));
    assert!(
        matches!(&new.columns[2], Some(ColumnValue::Text(s)) if s == "{1,2,3}"),
        "got {:?}",
        new.columns[2],
    );
    assert_eq!(oracle.stats.resolved.load(Ordering::Relaxed), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_recovers_after_cluster_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, SHADOW_PORT + 3) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;
    assert_eq!(
        oracle
            .resolve_pending(NUMERICOID, &numeric_42_bytes())
            .await
            .as_deref(),
        Some("42"),
    );

    guard.sh.stop().expect("stop");
    assert!(
        oracle
            .resolve_pending(NUMERICOID, &numeric_42_bytes())
            .await
            .is_none(),
        "no resolution while the cluster is down",
    );
    assert!(oracle.stats.errors.load(Ordering::Relaxed) >= 1);

    guard.sh.start().expect("restart");
    // Postmaster is up before the worker has re-bound its socket
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let got = oracle
            .resolve_pending(NUMERICOID, &numeric_42_bytes())
            .await;
        if got.as_deref() == Some("42") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "oracle never recovered after restart",
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
