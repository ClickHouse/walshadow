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

use clickhouse_c::Allocator;
use walshadow::backfill::bootstrap_oracle::BootstrapOracle;
use walshadow::bridge::{Bridge, BridgeError};
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

async fn bridge_on(sh: &Shadow) -> Bridge {
    let path = sh.bridge_socket().expect("bridge configured");
    walshadow::bridge::connect_with_budget(path, Duration::from_secs(20))
        .await
        .unwrap_or_else(|e| panic!("bridge connect on {}: {e}", path.display()))
}

async fn oracle_on(sh: &Shadow) -> Oracle {
    Oracle::new(Arc::new(bridge_on(sh).await))
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

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_oracle_preserves_types_and_removes_cluster() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(source) = start_pg(&tmp, ports::PG_SOURCE_PORT) else {
        return;
    };
    source
        .sh
        .psql_one(
            "CREATE TYPE mood AS ENUM ('quiet', 'busy');
             CREATE EXTENSION hstore;
             CREATE TABLE t (m mood, h hstore);
             INSERT INTO t VALUES ('busy', '\"a\"=>\"one\", \"b\"=>NULL');
             CREATE ROLE bootstrap LOGIN SUPERUSER PASSWORD 'bootstrap-password'",
        )
        .unwrap();
    let oid: u32 = source
        .sh
        .psql_one("SELECT 'mood'::regtype::oid")
        .unwrap()
        .parse()
        .unwrap();
    let enum_oid: u32 = source
        .sh
        .psql_one(
            "SELECT oid FROM pg_enum WHERE enumtypid = 'mood'::regtype AND enumlabel = 'busy'",
        )
        .unwrap()
        .parse()
        .unwrap();
    let hstore_oid: u32 = source
        .sh
        .psql_one("SELECT 'hstore'::regtype::oid")
        .unwrap()
        .parse()
        .unwrap();
    source.sh.stop().unwrap();
    fs::write(
        source.sh.config().data_dir.join("pg_hba.conf"),
        "local all bootstrap scram-sha-256\nlocal all all trust\n",
    )
    .unwrap();
    source.sh.start().unwrap();

    let base = tmp.path().join("oracle");
    for password in [None, Some("bootstrap-password".to_string())] {
        fs::create_dir_all(&base).unwrap();
        fs::write(base.join("stale"), "stale").unwrap();
        let conninfo = walshadow::pg::socket_conninfo(
            source.sh.config().socket_dir.to_str().unwrap(),
            source.sh.config().port,
            if password.is_some() {
                "bootstrap"
            } else {
                "postgres"
            },
            "postgres",
        );
        let bootstrap = BootstrapOracle::provision(
            base.clone(),
            conninfo,
            password,
            Some(pgext_dir()),
            Duration::from_secs(20),
        )
        .await
        .unwrap();
        assert!(!base.join("stale").exists());
        let mut cfg = ShadowConfig::new(base.join("pg"), base.clone());
        cfg.socket_dir = base.join("sock");
        cfg.port = 55440;
        let shadow = Shadow::new(cfg);
        assert!(shadow.is_running().unwrap());
        assert_eq!(shadow.psql_one("SELECT count(*) FROM t").unwrap(), "0");
        assert_eq!(
            shadow.psql_one("SELECT 'mood'::regtype::oid").unwrap(),
            oid.to_string()
        );
        assert_eq!(
            shadow.psql_one("SELECT 'hstore'::regtype::oid").unwrap(),
            hstore_oid.to_string()
        );
        let mood = buf(
            oid,
            vec![OracleCell::DiskRaw(enum_oid.to_le_bytes().to_vec())],
        );
        let hstore = buf(
            hstore_oid,
            vec![OracleCell::TextInput(br#""a"=>"one", "b"=>NULL"#.to_vec())],
        );
        let columns = [
            OracleRequestColumn {
                ordinal: 0,
                name: "m",
                target_type: "String",
                buf: &mood,
            },
            OracleRequestColumn {
                ordinal: 1,
                name: "h",
                target_type: "Map(String, Nullable(String))",
                buf: &hstore,
            },
        ];
        let oracle = bootstrap.oracle();
        assert!(Arc::ptr_eq(&oracle, &bootstrap.oracle()));
        let block = oracle.encode_batch(&columns, 1, alloc()).await.unwrap();
        assert_eq!(
            block.column(0).unwrap().string().unwrap(),
            (&[4][..], &b"busy"[..])
        );
        let map = block.column(1).unwrap();
        assert_eq!(map.array_offsets().unwrap(), [2]);
        let entries = map.array_values().unwrap();
        assert_eq!(
            entries.tuple_child(0).unwrap().string().unwrap(),
            (&[1, 2][..], &b"ab"[..])
        );
        let values = entries.tuple_child(1).unwrap();
        assert_eq!(values.null_map().unwrap(), [0, 1]);
        assert_eq!(
            values.nullable_inner().unwrap().string().unwrap(),
            (&[3, 3][..], &b"one"[..])
        );
        drop(bootstrap);
        assert!(!base.exists());
        assert!(!shadow.is_running().unwrap());
        assert!(oracle.encode_batch(&columns, 1, alloc()).await.is_err());
    }
    assert_eq!(source.sh.psql_one("SELECT count(*) FROM t").unwrap(), "1");
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_oracle_reports_source_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(source) = start_pg(&tmp, ports::PG_SOURCE_PORT) else {
        return;
    };
    source
        .sh
        .psql_one("CREATE ROLE restricted LOGIN; CREATE TABLE private (id int)")
        .unwrap();
    for (user, database, stage, expected) in [
        (
            "postgres",
            "missing",
            "source extensions query failed:",
            "database \"missing\" does not exist",
        ),
        (
            "restricted",
            "postgres",
            "pg_dump failed:",
            "permission denied for table private",
        ),
    ] {
        let base = tmp.path().join("oracle");
        let guard = StopOnDrop {
            sh: Shadow::new(ShadowConfig::new(base.join("pg"), base.clone())),
        };
        let conninfo = walshadow::pg::socket_conninfo(
            source.sh.config().socket_dir.to_str().unwrap(),
            source.sh.config().port,
            user,
            database,
        );
        let err = BootstrapOracle::provision(
            base.clone(),
            conninfo,
            None,
            Some(pgext_dir()),
            Duration::from_secs(20),
        )
        .await
        .err()
        .expect("provision fails");
        let msg = format!("{err:#}");
        assert!(msg.contains("bootstrap oracle provision:"), "{msg}");
        assert!(msg.contains(stage), "{msg}");
        assert!(msg.contains(expected), "{msg}");
        assert!(!base.join("sock/walshadow-bridge.sock").exists());
        drop(guard);
    }
    assert!(source.sh.is_running().unwrap());
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
    let cases = [
        (0, "String", OracleCell::Literal(b"literal".to_vec())),
        (0, "Nullable(String)", OracleCell::Literal(b"text".to_vec())),
        (
            1007,
            "Array(Int32)",
            OracleCell::TextInput(b"{7,8}".to_vec()),
        ),
        (0, "Map(String, String)", OracleCell::Default),
        (0, "JSON", OracleCell::Default),
        (
            0,
            "LowCardinality(String)",
            OracleCell::Literal(b"dictionary".to_vec()),
        ),
        (0, "Array(Int32)", OracleCell::Default),
    ];
    let bufs: Vec<_> = cases
        .iter()
        .map(|(oid, _, value)| {
            buf(
                *oid,
                vec![OracleCell::Default, value.clone(), OracleCell::Default],
            )
        })
        .collect();
    let columns: Vec<_> = cases
        .iter()
        .zip(&bufs)
        .enumerate()
        .map(|(i, ((_, ty, _), buf))| OracleRequestColumn {
            ordinal: i as u32,
            name: ty,
            target_type: ty,
            buf,
        })
        .collect();
    let block = oracle.encode_batch(&columns, 3, alloc()).await.unwrap();
    assert_eq!(
        block.column(0).unwrap().string().unwrap(),
        (&[0, 7, 7][..], &b"literal"[..])
    );
    let nullable = block.column(1).unwrap();
    assert_eq!(nullable.null_map().unwrap(), [1, 0, 1]);
    assert_eq!(
        nullable.nullable_inner().unwrap().string().unwrap(),
        (&[0, 4, 4][..], &b"text"[..])
    );
    let array = block.column(2).unwrap();
    assert_eq!(array.array_offsets().unwrap(), [0, 2, 2]);
    assert_eq!(
        array.array_values().unwrap().fixed().unwrap().1,
        [7, 0, 0, 0, 8, 0, 0, 0]
    );
    assert_eq!(block.column(3).unwrap().array_offsets().unwrap(), [0, 0, 0]);
    assert_eq!(
        block.column(4).unwrap().string().unwrap(),
        (&[2, 4, 6][..], &b"{}{}{}"[..])
    );
    let lc = block.column(5).unwrap().low_cardinality().unwrap();
    let (offsets, data) = lc.dict.string().unwrap();
    let values: Vec<_> = lc
        .keys
        .chunks_exact(lc.key_size)
        .map(|key| {
            let index = key
                .iter()
                .rev()
                .fold(0usize, |n, b| (n << 8) | usize::from(*b));
            let start = if index == 0 {
                0
            } else {
                offsets[index - 1] as usize
            };
            &data[start..offsets[index] as usize]
        })
        .collect();
    assert_eq!(values, [b"".as_slice(), b"dictionary", b""]);
    assert_eq!(block.column(6).unwrap().array_offsets().unwrap(), [0, 0, 0]);
}

#[tokio::test(flavor = "current_thread")]
async fn oracle_fails_whole_request_on_bad_cell() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, ports::PG_SHADOW_PORT) else {
        return;
    };
    let bridge = Arc::new(bridge_on(&guard.sh).await);
    let oracle = Oracle::new(bridge.clone());
    let ids = buf(
        23,
        vec![
            OracleCell::DiskRaw(1i32.to_le_bytes().to_vec()),
            OracleCell::DiskRaw(2i32.to_le_bytes().to_vec()),
        ],
    );
    let cases = [
        (
            INT4ARRAYOID,
            -1,
            "Array(Int32)",
            OracleCell::DiskRaw(array_int4_1_2_3_bytes()),
            OracleCell::DiskRaw(array_int4_2d_bytes()),
            "cannot encode anyarray into ClickHouse Array(Int32)",
        ),
        (
            1043,
            7,
            "String",
            OracleCell::TextInput(b"123".to_vec()),
            OracleCell::TextInput(b"toolong".to_vec()),
            "value too long",
        ),
        (
            23,
            -1,
            "String",
            OracleCell::TextInput(b"123".to_vec()),
            OracleCell::DiskRaw(vec![1]),
            "shorter than typlen",
        ),
    ];
    for (i, (oid, typmod, ty, good, bad, expected)) in cases.into_iter().enumerate() {
        let request = |value| {
            [
                OracleRequestColumn {
                    ordinal: 0,
                    name: "id",
                    target_type: "String",
                    buf: &ids,
                },
                OracleRequestColumn {
                    ordinal: 1,
                    name: "value",
                    target_type: ty,
                    buf: value,
                },
            ]
        };
        let mut value = buf(oid, vec![good.clone(), bad]);
        value.source_typmod = typmod;
        let err = oracle
            .encode_batch(&request(&value), 2, alloc())
            .await
            .expect_err("bad cell fails whole request");
        let msg = err.to_string();
        assert!(msg.contains("column 1 (\"value\"), row 1"), "{msg}");
        assert!(msg.contains(expected), "{msg}");
        assert!(!err.retryable());
        assert_eq!(oracle.stats.blocks.load(Ordering::Relaxed), i as u64);
        assert_eq!(
            oracle.stats.conversion_errors.load(Ordering::Relaxed),
            i as u64 + 1
        );

        let mut value = buf(oid, vec![good.clone(), good]);
        value.source_typmod = typmod;
        let block = oracle
            .encode_batch(&request(&value), 2, alloc())
            .await
            .expect("worker still serves");
        assert_eq!(
            block.column(0).unwrap().string().unwrap(),
            (&[1, 2][..], &b"12"[..])
        );
        let value = block.column(1).unwrap();
        if oid == INT4ARRAYOID {
            assert_eq!(value.array_offsets().unwrap(), [3, 6]);
            assert_eq!(
                value.array_values().unwrap().fixed().unwrap().1,
                [1i32, 2, 3, 1, 2, 3]
                    .into_iter()
                    .flat_map(i32::to_le_bytes)
                    .collect::<Vec<_>>()
            );
        } else {
            assert_eq!(value.string().unwrap(), (&[3, 6][..], &b"123123"[..]));
        }
    }
    assert_eq!(oracle.stats.rows.load(Ordering::Relaxed), 6);
    assert_eq!(bridge.stats.reconnects.load(Ordering::Relaxed), 0);
}

fn native_request(oid: u32, name: &str, ty: &str, cells: &[u8], rows: u32) -> Vec<u8> {
    let mut out = rows.to_be_bytes().to_vec();
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&oid.to_be_bytes());
    out.extend_from_slice(&(-1i32).to_be_bytes());
    for s in [name, ty] {
        out.extend_from_slice(&(s.len() as u32).to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    out.extend_from_slice(cells);
    out
}

#[tokio::test(flavor = "current_thread")]
async fn worker_refuses_malformed_requests() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(guard) = start_pg(&tmp, ports::PG_SHADOW_PORT) else {
        return;
    };
    let bridge = Arc::new(bridge_on(&guard.sh).await);
    let oracle = Oracle::new(bridge.clone());
    let good = buf(23, vec![OracleCell::DiskRaw(42i32.to_le_bytes().to_vec())]);
    let columns = [OracleRequestColumn {
        ordinal: 0,
        name: "c",
        target_type: "String",
        buf: &good,
    }];
    let mut cases = vec![
        (
            native_request(23, "c", "Int32", &[0], 0),
            "0 rows and 1 columns",
        ),
        (vec![0, 0, 0, 1, 0, 0, 0, 0], "1 rows and 0 columns"),
        (vec![255; 8], "bytes remain"),
        (
            native_request(23, "", "Int32", &[0], 1),
            "empty name or type",
        ),
        (native_request(23, "c", "", &[0], 1), "empty name or type"),
        (native_request(23, "c", "NotAType", &[0], 1), "target type:"),
        (
            native_request(23, "c", "Int32", &[255], 1),
            "unknown walshadow cell tag 255",
        ),
        (
            native_request(0, "c", "String", &[1, 0, 0, 0, 0], 1),
            "declares no source type",
        ),
        (
            native_request(0, "c", "String", &[2, 0, 0, 0, 0], 1),
            "declares no source type",
        ),
        (
            native_request(23, "c", "Int32", &[1, 0, 0, 0, 4, 42], 1),
            "cell length 4 past end of request",
        ),
        (
            native_request(23, "c", "String", &[1, 255, 255, 255, 255], 1),
            "cell length 4294967295 past end of request",
        ),
        (
            native_request(23, "c", "Int32", &[0], 1),
            "no default value for ClickHouse type",
        ),
    ];
    let mut bad_name = native_request(23, "c", "Int32", &[0], 1);
    bad_name[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
    cases.push((bad_name, "string length 4294967295 past end of request"));
    let mut bad_type = native_request(23, "c", "Int32", &[0], 1);
    bad_type[21..25].copy_from_slice(&u32::MAX.to_be_bytes());
    cases.push((bad_type, "string length 4294967295 past end of request"));
    let mut trailing = native_request(23, "c", "String", &[1, 0, 0, 0, 4, 42, 0, 0, 0], 1);
    trailing.push(0);
    cases.push((trailing, "invalid message format"));
    for (payload, expected) in cases {
        let err = bridge.encode_native(&payload).await.unwrap_err();
        assert!(
            matches!(err, BridgeError::Remote(ref msg) if msg.contains(expected)),
            "{expected}: {err}"
        );
        let block = oracle
            .encode_batch(&columns, 1, alloc())
            .await
            .expect("worker still serves");
        assert_eq!(
            block.column(0).unwrap().string().unwrap(),
            (&[2][..], &b"42"[..])
        );
    }
    assert_eq!(bridge.stats.reconnects.load(Ordering::Relaxed), 0);
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
