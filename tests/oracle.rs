//! Sealed-batch Native oracle integration tests
//!
//! Use preloaded worker from `pgext` build tree

#[path = "common/ports.rs"]
mod ports;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use clickhouse_c::{Allocator, ColumnLayout};
use walshadow::oracle::{Oracle, OracleCell, OracleColumnBuf, OracleRequestColumn};
use walshadow::schema::NUMERICOID;
use walshadow::shadow::{BridgeConf, Shadow, ShadowConfig};

/// int4 array, ie `INT4ARRAYOID`
const INT4ARRAYOID: u32 = 1007;
const JSONBOID: u32 = 3802;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build tree holding `walshadow.so`, fed to PG as `dynamic_library_path`.
/// Module is not optional, so an unbuilt tree fails rather than skips
fn pgext_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("pgext");
    assert!(
        dir.join("walshadow.so").is_file(),
        "pgext/walshadow.so missing, run `make -C pgext`"
    );
    dir
}

struct StopOnDrop {
    sh: Shadow,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

/// `None` skips the caller: no PG
fn start_pg(tmp: &tempfile::TempDir, port: u16) -> Option<StopOnDrop> {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return None;
    }
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    let mut bridge = BridgeConf::in_dir(&cfg.socket_dir);
    bridge.library_dir = Some(pgext_dir());
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
    let bridge = walshadow::bridge::connect_with_budget(path, 1, Duration::from_secs(20))
        .await
        .unwrap_or_else(|e| panic!("bridge connect on {}: {e}", path.display()));
    Oracle::new(Arc::new(bridge))
}

fn alloc() -> Allocator {
    Allocator::stdlib()
}

fn buf(oid: u32, cells: Vec<OracleCell>) -> OracleColumnBuf {
    let mut b = OracleColumnBuf::new(oid, -1);
    for c in cells {
        b.push(c);
    }
    b
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

/// `{"a": "b"}` as jsonb's on-disk body: an object container header, one
/// JEntry per key and value (a payload length, since neither carries
/// `JENTRY_HAS_OFF`), then the payload. Strings need no alignment padding.
fn jsonb_a_b_bytes() -> Vec<u8> {
    // JB_FOBJECT | 1 pair
    let mut out = 0x2000_0001u32.to_le_bytes().to_vec();
    // JENTRY_ISSTRING (0), length 1, for key "a" and value "b"
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(b"ab");
    out
}

/// A 2-D int4 array. PG arrays carry no declared dimensionality, so this is
/// what a runtime value that outgrows a one-layer `Array(T)` target looks like.
fn array_int4_2d_bytes() -> Vec<u8> {
    let mut out = Vec::new();
    for v in [2i32, 0, 23, 2, 2, 1, 1] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    for v in [1i32, 2, 3, 4] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_encodes_tier3_disk_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, ports::PG_SHADOW_PORT) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;

    let arr = buf(
        INT4ARRAYOID,
        vec![OracleCell::DiskRaw(array_int4_1_2_3_bytes())],
    );
    let js = buf(JSONBOID, vec![OracleCell::DiskRaw(jsonb_a_b_bytes())]);
    // Attribute-default text takes typinput path
    let num = buf(NUMERICOID, vec![OracleCell::TextInput(b"42.5".to_vec())]);
    let columns = [
        OracleRequestColumn {
            ordinal: 0,
            name: "tags",
            target_type: "Array(Int32)",
            buf: &arr,
        },
        OracleRequestColumn {
            ordinal: 2,
            name: "doc",
            target_type: "JSON",
            buf: &js,
        },
        OracleRequestColumn {
            ordinal: 5,
            name: "amount",
            target_type: "String",
            buf: &num,
        },
    ];

    let block = oracle
        .encode_batch(&columns, 1, alloc())
        .await
        .expect("oracle answers");

    let tags = block.column(0).expect("tags");
    assert_eq!(tags.array_offsets(), Some(&[3u64][..]));
    let (w, bytes) = tags.array_values().and_then(|c| c.fixed()).expect("int32s");
    assert_eq!(w, 4);
    assert_eq!(
        bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| i32::from_le_bytes(*c))
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
    );

    let doc = block.column(2).expect("doc");
    let (_, json) = doc.string().expect("json strings");
    assert_eq!(std::str::from_utf8(json).unwrap(), r#"{"a": "b"}"#);

    let amount = block.column(5).expect("amount");
    let (_, text) = amount.string().expect("strings");
    assert_eq!(std::str::from_utf8(text).unwrap(), "42.5");

    assert_eq!(oracle.stats.blocks.load(Ordering::Relaxed), 1);
    assert_eq!(oracle.stats.cells.load(Ordering::Relaxed), 3);
    assert_eq!(oracle.stats.errors.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_defaults_fill_absent_cells() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, ports::PG_SHADOW_PORT) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;

    // Default cells permit source OID zero
    let nullable = buf(0, vec![OracleCell::Default]);
    let array = buf(0, vec![OracleCell::Default]);
    let json = buf(0, vec![OracleCell::Default]);
    let columns = [
        OracleRequestColumn {
            ordinal: 0,
            name: "n",
            target_type: "Nullable(String)",
            buf: &nullable,
        },
        OracleRequestColumn {
            ordinal: 1,
            name: "a",
            target_type: "Array(Int32)",
            buf: &array,
        },
        OracleRequestColumn {
            ordinal: 2,
            name: "j",
            target_type: "JSON",
            buf: &json,
        },
    ];

    let block = oracle
        .encode_batch(&columns, 1, alloc())
        .await
        .expect("oracle answers");

    assert_eq!(block.column(0).and_then(|c| c.null_map()), Some(&[1u8][..]));
    assert_eq!(
        block.column(1).and_then(|c| c.array_offsets()),
        Some(&[0u64][..])
    );
    let (_, j) = block.column(2).and_then(|c| c.string()).expect("json");
    assert_eq!(std::str::from_utf8(j).unwrap(), "{}");
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_fails_whole_request_on_bad_cell() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, ports::PG_SHADOW_PORT) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;

    // Second row exceeds target array dimensionality
    let bad = buf(
        INT4ARRAYOID,
        vec![
            OracleCell::DiskRaw(array_int4_1_2_3_bytes()),
            OracleCell::DiskRaw(array_int4_2d_bytes()),
        ],
    );
    let columns = [OracleRequestColumn {
        ordinal: 0,
        name: "tags",
        target_type: "Array(Int32)",
        buf: &bad,
    }];
    let err = oracle
        .encode_batch(&columns, 2, alloc())
        .await
        .expect_err("bad cell fails the request");
    assert!(err.to_string().contains("row 1"), "{err}");
    assert_eq!(oracle.stats.blocks.load(Ordering::Relaxed), 0);
    assert_eq!(oracle.stats.conversion_errors.load(Ordering::Relaxed), 1);

    // Request failure preserves connection
    let good = buf(
        INT4ARRAYOID,
        vec![OracleCell::DiskRaw(array_int4_1_2_3_bytes())],
    );
    let columns = [OracleRequestColumn {
        ordinal: 0,
        name: "tags",
        target_type: "Array(Int32)",
        buf: &good,
    }];
    let block = oracle
        .encode_batch(&columns, 1, alloc())
        .await
        .expect("oracle still serves");
    assert!(matches!(
        block.column(0).and_then(|c| c.layout()),
        Some(ColumnLayout::Array)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn worker_refuses_malformed_requests() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, ports::PG_SHADOW_PORT) else {
        return;
    };
    let socket = guard.sh.bridge_socket().expect("bridge configured");
    let bridge = walshadow::bridge::connect_with_budget(socket, 1, Duration::from_secs(20))
        .await
        .expect("bridge connect");

    // Past the bridge's frame prefix, as `encode_request` builds it
    let framed = |rows: u32, cols: u32, meta: &[u8], cells: &[u8]| -> Vec<u8> {
        let mut v = walshadow::bridge::request_frame(0);
        v.extend_from_slice(&rows.to_be_bytes());
        v.extend_from_slice(&cols.to_be_bytes());
        v.extend_from_slice(meta);
        v.extend_from_slice(cells);
        v
    };
    let meta = |oid: u32| -> Vec<u8> {
        let mut m = oid.to_be_bytes().to_vec();
        m.extend_from_slice(&(-1i32).to_be_bytes());
        m.extend_from_slice(&1u32.to_be_bytes());
        m.push(b'c');
        m.extend_from_slice(&6u32.to_be_bytes());
        m.extend_from_slice(b"String");
        m
    };
    let one = meta(23);
    let cases: [(&str, Vec<u8>); 7] = [
        ("zero rows", framed(0, 1, &one, &[])),
        ("zero columns", framed(1, 0, &[], &[])),
        (
            "cell grid past the frame",
            framed(u32::MAX, u32::MAX, &one, &[]),
        ),
        ("empty column name", {
            let mut m = 23u32.to_be_bytes().to_vec();
            m.extend_from_slice(&(-1i32).to_be_bytes());
            m.extend_from_slice(&0u32.to_be_bytes());
            m.extend_from_slice(&6u32.to_be_bytes());
            m.extend_from_slice(b"String");
            framed(1, 1, &m, &[0x01, 0, 0, 0, 4, 42, 0, 0, 0])
        }),
        ("unknown cell tag", framed(1, 1, &one, &[0x7f])),
        (
            "cell length past the frame",
            framed(1, 1, &one, &[0x01, 0xff, 0xff, 0xff, 0xff]),
        ),
        (
            "value cell with no source type",
            framed(1, 1, &meta(0), &[0x01, 0, 0, 0, 1, b'x']),
        ),
    ];
    for (what, payload) in cases {
        let err = bridge
            .encode_native(payload)
            .await
            .err()
            .unwrap_or_else(|| panic!("{what}: worker accepted a malformed request"));
        assert!(
            matches!(err, walshadow::bridge::BridgeError::Remote(_)),
            "{what}: {err}",
        );
    }

    // Reject bytes beyond declared cells
    let mut trailing = framed(1, 1, &one, &[0x01, 0, 0, 0, 4, 42, 0, 0, 0]);
    trailing.push(0);
    assert!(bridge.encode_native(trailing).await.is_err());

    // Parser errors preserve connection
    let oracle = Oracle::new(Arc::new(bridge));
    let good = buf(
        INT4ARRAYOID,
        vec![OracleCell::DiskRaw(array_int4_1_2_3_bytes())],
    );
    let columns = [OracleRequestColumn {
        ordinal: 0,
        name: "tags",
        target_type: "Array(Int32)",
        buf: &good,
    }];
    oracle
        .encode_batch(&columns, 1, alloc())
        .await
        .expect("worker still serves");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_recovers_after_cluster_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, ports::PG_SHADOW_PORT) else {
        return;
    };
    let oracle = oracle_on(&guard.sh).await;
    let one = buf(
        INT4ARRAYOID,
        vec![OracleCell::DiskRaw(array_int4_1_2_3_bytes())],
    );
    let request = || {
        [OracleRequestColumn {
            ordinal: 0,
            name: "tags",
            target_type: "Array(Int32)",
            buf: &one,
        }]
    };
    oracle
        .encode_batch(&request(), 1, alloc())
        .await
        .expect("first request");

    guard.sh.stop().expect("stop");
    assert!(
        oracle.encode_batch(&request(), 1, alloc()).await.is_err(),
        "no block while the cluster is down",
    );
    assert!(oracle.stats.errors.load(Ordering::Relaxed) >= 1);

    guard.sh.start().expect("restart");
    // Postmaster is up before the worker has re-bound its socket
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if oracle.encode_batch(&request(), 1, alloc()).await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "oracle never recovered after restart",
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
