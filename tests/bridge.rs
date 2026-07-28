//! pgext bridge worker, end to end over its unix socket.
//!
//! Needs `walshadow.so` built in `pgext/` (`make -C pgext`) against the same
//! PG major as `initdb` on PATH; fails when it isn't there.
//!
//! Drills cover bridge protocol and lifecycle:
//!
//! 1. `bridge_hello_and_decode_batch` — version gate, `REPLAY_LSN`, and a
//!    `DECODE` batch where a truncated body and an unknown type oid return
//!    item errors without costing their neighbours
//! 2. `bridge_decode_matches_typoutput` — every `ws_decode_datum_text` branch
//!    (varlena, cstring, fixed by-value, fixed by-reference) against PG's own
//!    rendering of the same value
//! 3. `bridge_scans_uncommitted_ddl` — `SCAN` against an open transaction: one
//!    `pg_class` row per oid despite the superseded versions on the page, the
//!    new column with its `attmissingval`, a rolled-back savepoint's column
//!    absent, a relation created in-transaction visible by oid, a foreign
//!    in-progress whole-catalog writer failing the scan as inconclusive, the
//!    committed shape returned after `ROLLBACK`, and every handled error
//!    leaving the same connection serving. Also the two arguments that turn
//!    the same scan into a committed read: top xid 0, and no oid list, which
//!    still projects `attnum >= 1` now that it is legal on every catalog
//! 4. `bridge_reconnects_after_worker_exit` — worker killed mid-flight, daemon
//!    redials, no cluster restart; the restarted worker pins its decode output
//!    GUCs over contrary database defaults
//! 5. `bridge_drops_bad_frames_per_connection` — oversize and zero-length
//!    frames close only their own connection
//! 6. `bridge_error_frames_stay_parseable` — unknown opcode and trailing
//!    request bytes answer one exactly-consumed error frame, same connection
//!    serves the next request
//! 7. `bridge_overlay_descriptors_track_open_ddl` — mirroring statement,
//!    committed scan and overlay scan agree for an untouched relation, then
//!    overlay alone tracks an open transaction's added column, new relation,
//!    schema, and type
//! 8. `bridge_committed_read_falls_back_when_replay_moves` — a committed read
//!    whose pin breaks answers off the mirroring statement; the same break
//!    fails an overlay read
//!
//! Clusters here are plain (non-recovery) PG. The overlay predicate keys off
//! xids, not recovery, so an open transaction exercises the same code a
//! standby's replaying transaction does.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls};
use walshadow::bridge::{
    AttributeRow, Bridge, BridgeError, Catalog, ClassRow, DecodedItem, IndexRow, NamespaceRow,
    PROJECTION_VERSION, PROTO_VERSION,
};
use walshadow::pg::socket_conninfo;
use walshadow::schema::ReplIdent;
use walshadow::shadow::{BridgeConf, Shadow, ShadowConfig};
use walshadow::shadow_catalog::{CatalogError, ShadowCatalog, ShadowCatalogConfig};

const BASE_PORT: u16 = 56401;

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build tree holding `walshadow.so`, fed to shadow as `dynamic_library_path`.
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

fn start_pg(tmp: &tempfile::TempDir, port: u16) -> StopOnDrop {
    let lib_dir = pgext_dir();
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
    StopOnDrop { sh }
}

async fn dial(sh: &Shadow) -> Bridge {
    let path = sh.bridge_socket().expect("bridge configured");
    walshadow::bridge::connect_with_budget(path, Duration::from_secs(20))
        .await
        .unwrap_or_else(|e| panic!("bridge connect on {}: {e}", path.display()))
}

async fn connect_sql(sh: &Shadow) -> Client {
    let conninfo = socket_conninfo(
        sh.config().socket_dir.to_str().unwrap(),
        sh.config().port,
        &sh.config().user,
        &sh.config().dbname,
    );
    let (client, connection) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .expect("sql connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn open_catalog(sh: &Shadow, bridge: Arc<Bridge>) -> ShadowCatalog {
    let conninfo = socket_conninfo(
        sh.config().socket_dir.to_str().unwrap(),
        sh.config().port,
        &sh.config().user,
        &sh.config().dbname,
    );
    ShadowCatalog::connect(&conninfo, ShadowCatalogConfig::default(), bridge)
        .await
        .expect("catalog connect")
}

/// Catalog behind a worker whose replay position moves under every scan, so
/// every committed read it serves answers off the mirroring statement — what a
/// standby away from a publication hold looks like
async fn open_mirror_catalog(sh: &Shadow, sock: &Path) -> (Arc<Bridge>, ShadowCatalog) {
    spawn_moving_worker(tokio::net::UnixListener::bind(sock).expect("bind stand-in"));
    let bridge = Arc::new(
        walshadow::bridge::connect_with_budget(sock, Duration::from_secs(5))
            .await
            .expect("stand-in bridge"),
    );
    let cat = open_catalog(sh, bridge.clone()).await;
    (bridge, cat)
}

async fn scalar(client: &Client, sql: &str) -> String {
    client
        .query_one(sql, &[])
        .await
        .expect(sql)
        .get::<_, String>(0)
}

async fn oid_of(client: &Client, relname: &str) -> u32 {
    scalar(client, &format!("SELECT '{relname}'::regclass::oid::text"))
        .await
        .parse()
        .expect("oid")
}

async fn oid_of_type(client: &Client, typname: &str) -> u32 {
    scalar(client, &format!("SELECT '{typname}'::regtype::oid::text"))
        .await
        .parse()
        .expect("type oid")
}

/// `pg_current_xact_id` is xid8; the wire carries the 32-bit `TransactionId`
async fn top_xid(client: &Client) -> u32 {
    scalar(client, "SELECT pg_current_xact_id()::text")
        .await
        .parse::<u64>()
        .expect("xid8") as u32
}

/// Uncompressed on-disk varlena body, ie header already stripped
fn body(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

/// Short-form numeric for `42`: header 0x8000, one base-10000 digit
fn numeric_42() -> Vec<u8> {
    let mut out = 0x8000u16.to_le_bytes().to_vec();
    out.extend_from_slice(&42i16.to_le_bytes());
    out
}

/// `{1,2,3}` int4 array: ndim, dataoffset, elemtype, dim, lbound, elements
fn array_int4_1_2_3() -> Vec<u8> {
    let mut out = Vec::new();
    for v in [1i32, 0, 23, 3, 1] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    for v in [1i32, 2, 3] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn bridge_hello_and_decode_batch() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT);
    let bridge = dial(&guard.sh).await;

    let info = bridge.info().expect("hello");
    assert_eq!(info.proto, PROTO_VERSION);
    assert_eq!(info.projection, PROJECTION_VERSION);
    assert!(info.pg_version_num >= 160000, "{info:?}");
    // Plain cluster, so no recovery. Shape of the field, not its value
    assert!(!info.in_recovery);
    // Shared memory read, zero outside recovery
    assert_eq!(bridge.replay_lsn().await.expect("replay_lsn"), 0);

    let text = body(b"hi there");
    let bpchar = body(b"pad ");
    let numeric = numeric_42();
    let array = array_int4_1_2_3();
    let items: Vec<(u32, &[u8])> = vec![
        (23, &[42, 0, 0, 0]), // int4
        (16, &[1]),           // bool
        (25, &text),          // text
        (1042, &bpchar),      // bpchar
        (1700, &numeric),     // numeric
        (1007, &array),       // int4[]
        (23, &[0x00]),        // int4 body shorter than typlen
        (999_999, &[0x00]),   // no such type
        (23, &[7, 0, 0, 0]),  // batch keeps going after the errors
    ];
    let out = bridge.decode(&items).await.expect("decode");

    assert_eq!(out.len(), items.len());
    let want = ["42", "t", "hi there", "pad ", "42", "{1,2,3}"];
    for (got, want) in out.iter().zip(want) {
        assert_eq!(got, &DecodedItem::Text(want.to_string()));
    }
    assert!(matches!(out[6], DecodedItem::Error(_)), "{:?}", out[6]);
    assert!(matches!(out[7], DecodedItem::Error(_)), "{:?}", out[7]);
    assert_eq!(out[8], DecodedItem::Text("7".to_string()));

    use std::sync::atomic::Ordering;
    assert_eq!(bridge.stats.decode_item_errors.load(Ordering::Relaxed), 2);
    assert!(bridge.is_up());
}

/// Every branch of `ws_decode_datum_text`, differentially against the same
/// cluster's `typoutput`. Was `pgext`'s pg_regress suite before the SQL
/// surface went away; the socket is the only entry point now.
#[tokio::test(flavor = "current_thread")]
async fn bridge_decode_matches_typoutput() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT + 5);
    let bridge = dial(&guard.sh).await;
    let sql = connect_sql(&guard.sh).await;

    let long_text = "a".repeat(1024);
    let uuid = (0u8..16).map(|i| i * 17).collect::<Vec<_>>();
    // (type name, on-disk body, SQL literal PG renders for comparison)
    let cases: Vec<(&str, Vec<u8>, String)> = vec![
        // varlena: body reused as the datum, no header rebuild
        ("text", b"hello".to_vec(), "'hello'".into()),
        (
            "text",
            "héllo wörld".as_bytes().to_vec(),
            "'héllo wörld'".into(),
        ),
        ("text", Vec::new(), "''".into()),
        ("varchar", b"abc".to_vec(), "'abc'".into()),
        (
            "bytea",
            vec![0xde, 0xad, 0xbe, 0xef],
            "'\\xdeadbeef'".into(),
        ),
        ("json", br#"{"k":1}"#.to_vec(), r#"'{"k":1}'"#.into()),
        // >126 bytes, so PG stores a 4-byte header on disk
        (
            "text",
            long_text.clone().into_bytes(),
            format!("'{long_text}'"),
        ),
        // fixed pass-by-value, little endian on disk
        ("int2", 42i16.to_le_bytes().to_vec(), "42".into()),
        ("int4", 42i32.to_le_bytes().to_vec(), "42".into()),
        ("int4", (-1i32).to_le_bytes().to_vec(), "-1".into()),
        (
            "int8",
            1_234_567_890i64.to_le_bytes().to_vec(),
            "1234567890".into(),
        ),
        ("bool", vec![1], "true".into()),
        ("bool", vec![0], "false".into()),
        ("oid", 1234u32.to_le_bytes().to_vec(), "1234".into()),
        ("float4", 1.0f32.to_le_bytes().to_vec(), "1.0".into()),
        ("float8", 1.0f64.to_le_bytes().to_vec(), "1.0".into()),
        // Trailing bytes past typlen are ignored, not an error
        (
            "int4",
            vec![42, 0, 0, 0, 0xff, 0xff, 0xff, 0xff],
            "42".into(),
        ),
        // fixed pass-by-reference
        (
            "uuid",
            uuid.clone(),
            "'00112233-4455-6677-8899-aabbccddeeff'".into(),
        ),
    ];

    let mut items: Vec<(u32, &[u8])> = Vec::with_capacity(cases.len());
    for (ty, raw, _) in &cases {
        items.push((oid_of_type(&sql, ty).await, raw.as_slice()));
    }
    let out = bridge.decode(&items).await.expect("decode");
    assert_eq!(out.len(), cases.len());

    for (got, (ty, _, literal)) in out.iter().zip(&cases) {
        // `format('%s')` runs the type's output function; `::text` would pick
        // up a pg_cast entry instead (bool renders "true", boolout gives "t")
        let want = scalar(&sql, &format!("SELECT format('%s', ({literal})::{ty})")).await;
        assert_eq!(
            got,
            &DecodedItem::Text(want.clone()),
            "{ty} from {literal} — PG renders {want}",
        );
    }

    // Fixed-width bodies shorter than typlen, and a type oid with no row,
    // are per-item errors
    let empty: &[u8] = &[];
    let short_uuid: &[u8] = &[0x00, 0x11];
    let bad: Vec<(u32, &[u8])> = vec![
        (oid_of_type(&sql, "int4").await, empty),
        (oid_of_type(&sql, "uuid").await, short_uuid),
        (2_147_483_647, &[0x00]),
    ];
    let out = bridge.decode(&bad).await.expect("decode");
    for (i, item) in out.iter().enumerate() {
        assert!(matches!(item, DecodedItem::Error(_)), "{i}: {item:?}");
    }
    assert!(bridge.is_up());
}

#[tokio::test(flavor = "current_thread")]
async fn bridge_scans_uncommitted_ddl() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT + 1);
    let bridge = dial(&guard.sh).await;

    let setup = connect_sql(&guard.sh).await;
    setup
        .batch_execute(
            "CREATE TABLE t (id int PRIMARY KEY, a text);
             CREATE TABLE v (id int);",
        )
        .await
        .expect("committed setup");
    let oid_t = oid_of(&setup, "t").await;
    let oid_v = oid_of(&setup, "v").await;

    // Transaction under test, left open across every scan below
    let ddl = connect_sql(&guard.sh).await;
    ddl.batch_execute(
        "BEGIN;
         ALTER TABLE t ADD COLUMN c int DEFAULT 7;
         CREATE TABLE u (x int);
         CREATE SCHEMA mine;",
    )
    .await
    .expect("open ddl");
    let xid = top_xid(&ddl).await;
    let oid_u = oid_of(&ddl, "u").await;

    // Top xid 0 owns nothing, so the same scan answers the committed view and
    // the open transaction's work is foreign to it
    let committed = bridge
        .scan(Catalog::Attribute, 0, &[oid_t])
        .await
        .expect("committed pg_attribute")
        .parse::<AttributeRow>()
        .expect("attribute rows");
    let cols: Vec<&str> = committed.iter().map(|a| a.attname.as_str()).collect();
    assert_eq!(cols, ["id", "a"], "the added column is not committed");
    // No oid list is the whole catalog, on every catalog and not just the two
    // that never had a list
    let all = bridge
        .scan(Catalog::Class, 0, &[])
        .await
        .expect("whole pg_class")
        .parse::<ClassRow>()
        .expect("class rows");
    assert!(
        all.iter().any(|r| r.relname == "t") && all.iter().any(|r| r.relname == "v"),
        "{} rows and neither committed table in them",
        all.len(),
    );
    assert!(
        !all.iter().any(|r| r.oid == oid_u),
        "a relation created in-transaction is not committed",
    );
    // attnum >= 1 is the projection's shape, not something the oid list
    // happens to buy: system columns would shift every descriptor slot
    let every_attr = bridge
        .scan(Catalog::Attribute, 0, &[])
        .await
        .expect("whole pg_attribute")
        .parse::<AttributeRow>()
        .expect("attribute rows");
    assert!(
        every_attr.iter().all(|a| a.attnum >= 1),
        "system columns in a whole-catalog scan",
    );
    assert!(
        every_attr
            .iter()
            .any(|a| a.attrelid == oid_t && a.attname == "a"),
        "{} rows and none of them t.a",
        every_attr.len(),
    );

    // A relation created inside the open transaction resolves by oid, and the
    // altered one yields one row despite its superseded versions on the page
    let class = bridge
        .scan(Catalog::Class, xid, &[oid_t, oid_u])
        .await
        .expect("scan pg_class")
        .parse::<ClassRow>()
        .expect("class rows");
    let mut names: Vec<&str> = class.iter().map(|r| r.relname.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["t", "u"], "{class:#?}");
    assert!(class.iter().all(|r| r.relkind == 'r'));

    let attrs = bridge
        .scan(Catalog::Attribute, xid, &[oid_t])
        .await
        .expect("scan pg_attribute");
    // System columns sit at negative attnums and the projection filters them
    assert!(attrs.rows.iter().len() >= 3);
    let attrs = attrs.parse::<AttributeRow>().expect("attribute rows");
    let cols: Vec<&str> = attrs.iter().map(|a| a.attname.as_str()).collect();
    assert_eq!(cols, ["id", "a", "c"], "{attrs:#?}");
    assert!(attrs.iter().all(|a| a.attnum >= 1));
    let added = attrs.iter().find(|a| a.attname == "c").unwrap();
    assert_eq!(added.attmissingval.as_deref(), Some("{7}"));
    assert_eq!(added.atttypid, 23);
    assert!(attrs[0].attmissingval.is_none());

    let indexes = bridge
        .scan(Catalog::Index, xid, &[oid_t])
        .await
        .expect("scan pg_index")
        .parse::<IndexRow>()
        .expect("index rows");
    let pkey = indexes.iter().find(|i| i.indisprimary).expect("pkey row");
    assert_eq!(pkey.indkey, vec![1]);
    assert_eq!(pkey.indrelid, oid_t);

    // Whole-catalog scans have no lock argument, and a foreign in-progress
    // writer has no recorded parent, indistinguishable from an unassigned
    // subtransaction of ours: the scan refuses to guess
    let other = connect_sql(&guard.sh).await;
    other
        .batch_execute("BEGIN; CREATE SCHEMA theirs;")
        .await
        .expect("foreign ddl");
    let err = bridge.scan(Catalog::Namespace, xid, &[]).await.unwrap_err();
    assert!(
        matches!(err, BridgeError::Remote(ref m) if m.contains("inconclusive")),
        "{err:?}"
    );
    other.batch_execute("ROLLBACK").await.expect("foreign undo");
    // Aborted, the writer resolves and the scan answers: ours present,
    // the aborted foreign insert skipped
    let namespaces = bridge
        .scan(Catalog::Namespace, xid, &[])
        .await
        .expect("scan pg_namespace")
        .parse::<NamespaceRow>()
        .expect("namespace rows");
    let names: Vec<&str> = namespaces.iter().map(|n| n.nspname.as_str()).collect();
    assert!(names.contains(&"mine"), "{names:?}");
    assert!(!names.contains(&"theirs"), "{names:?}");

    // A reverted savepoint leaves its tuple aborted, and restores the version
    // it superseded, so the column count is unchanged and parentage resolved
    ddl.batch_execute(
        "SAVEPOINT s;
         ALTER TABLE t ADD COLUMN d int;
         ROLLBACK TO SAVEPOINT s;",
    )
    .await
    .expect("savepoint");
    let after = bridge
        .scan(Catalog::Attribute, xid, &[oid_t])
        .await
        .expect("scan after savepoint");
    assert_eq!(after.subtrans_mismatch, 0, "{after:#?}");
    let cols: Vec<String> = after
        .parse::<AttributeRow>()
        .expect("attribute rows")
        .into_iter()
        .map(|a| a.attname)
        .collect();
    assert_eq!(cols, ["id", "a", "c"]);
    let class = bridge
        .scan(Catalog::Class, xid, &[oid_t])
        .await
        .expect("scan pg_class after savepoint");
    assert_eq!(class.rows.len(), 1, "{class:#?}");

    // Nothing the foreign transaction touched leaked into the answer
    let untouched = bridge
        .scan(Catalog::Attribute, xid, &[oid_v])
        .await
        .expect("scan v")
        .parse::<AttributeRow>()
        .expect("attribute rows");
    let cols: Vec<&str> = untouched.iter().map(|a| a.attname.as_str()).collect();
    assert_eq!(cols, ["id"]);

    ddl.batch_execute("ROLLBACK").await.expect("undo");
    let committed = bridge
        .scan(Catalog::Attribute, xid, &[oid_t])
        .await
        .expect("scan after rollback")
        .parse::<AttributeRow>()
        .expect("attribute rows");
    let cols: Vec<&str> = committed.iter().map(|a| a.attname.as_str()).collect();
    assert_eq!(cols, ["id", "a"], "aborted tree still visible");

    // Every handled error above was an answered frame, never a dropped
    // connection: one worker connection served the whole test
    use std::sync::atomic::Ordering;
    assert_eq!(bridge.stats.reconnects.load(Ordering::Relaxed), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn bridge_reconnects_after_worker_exit() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT + 2);
    let bridge = dial(&guard.sh).await;
    assert_eq!(bridge.replay_lsn().await.expect("before"), 0);

    let sql = connect_sql(&guard.sh).await;
    // Contrary database defaults: the restarted worker's fresh connection
    // inherits these unless it pins its decode output environment
    sql.batch_execute(
        "ALTER DATABASE postgres SET timezone TO 'America/New_York';
         ALTER DATABASE postgres SET datestyle TO 'German, DMY';
         ALTER DATABASE postgres SET intervalstyle TO 'sql_standard';
         ALTER DATABASE postgres SET bytea_output TO 'escape';",
    )
    .await
    .expect("contrary defaults");

    let killed = scalar(
        &sql,
        "SELECT count(pg_terminate_backend(pid))::text \
         FROM pg_stat_activity WHERE backend_type = 'walshadow bridge'",
    )
    .await;
    assert_eq!(killed, "1", "bridge worker not running");

    // bgw_restart_time is 5s, so poll rather than assume the next call lands.
    // pg_terminate_backend returns at signal delivery, so a call can still
    // reach the old worker; only an answer after a reconnect is the new one
    use std::sync::atomic::Ordering;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let res = bridge.replay_lsn().await;
        let reconnected = bridge.stats.reconnects.load(Ordering::Relaxed) >= 1;
        match res {
            Ok(_) if reconnected => break,
            Ok(_) | Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok(_) => panic!("worker survived pg_terminate_backend"),
            Err(e) => panic!("bridge never came back: {e}"),
        }
    }
    assert!(bridge.is_up());
    // Postmaster restarted the worker, not the cluster
    assert_eq!(scalar(&sql, "SELECT 'alive'").await, "alive");

    // The defaults did land on fresh connections...
    let fresh = connect_sql(&guard.sh).await;
    assert_eq!(scalar(&fresh, "SHOW timezone").await, "America/New_York");
    // ...and the worker pinned its canonical output over them
    let interval_90s = {
        let mut b = 90_000_000i64.to_le_bytes().to_vec(); // µs
        b.extend_from_slice(&0i32.to_le_bytes()); // days
        b.extend_from_slice(&0i32.to_le_bytes()); // months
        b
    };
    let items: Vec<(u32, &[u8])> = vec![
        (1184, &[0; 8]),       // timestamptz, µs since 2000-01-01 UTC
        (1082, &[0; 4]),       // date, days since 2000-01-01
        (1186, &interval_90s), // interval
        (17, &[0xde, 0xad]),   // bytea
    ];
    let out = bridge.decode(&items).await.expect("decode");
    let want = [
        "2000-01-01 00:00:00+00",
        "2000-01-01",
        "00:01:30",
        r"\xdead",
    ];
    for (got, want) in out.iter().zip(want) {
        assert_eq!(got, &DecodedItem::Text(want.to_string()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bridge_drops_bad_frames_per_connection() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT + 3);
    let bridge = dial(&guard.sh).await;
    let path = guard.sh.bridge_socket().unwrap().to_path_buf();

    // Over walshadow.max_request_mb, and zero-length: both close the
    // connection before any payload is read
    for header in [u32::MAX, 0] {
        let mut raw = UnixStream::connect(&path).expect("raw connect");
        raw.write_all(&header.to_be_bytes()).expect("write header");
        let mut buf = [0u8; 1];
        assert_eq!(
            raw.read(&mut buf).expect("read after bad frame"),
            0,
            "frame header {header} did not close the connection"
        );
    }
    // Abrupt close mid-frame
    {
        let mut raw = UnixStream::connect(&path).expect("raw connect");
        raw.write_all(&64u32.to_be_bytes()).expect("write header");
        raw.write_all(&[0x02]).expect("write op");
    }

    // The healthy connection never noticed
    assert_eq!(bridge.replay_lsn().await.expect("still serving"), 0);
    use std::sync::atomic::Ordering;
    assert_eq!(bridge.stats.reconnects.load(Ordering::Relaxed), 0);
}

/// Three row sources — the mirroring statement, `SCAN` at top xid 0, and
/// `SCAN` under a transaction that wrote nothing — must build the same
/// `RelDescriptor`; overlay then tracks open transaction, and the statement
/// stays on the committed shape. Boundary is 0 throughout because this cluster
/// is not a standby.
#[tokio::test(flavor = "current_thread")]
async fn bridge_overlay_descriptors_track_open_ddl() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT + 6);
    let bridge = Arc::new(dial(&guard.sh).await);
    let mut cat = open_catalog(&guard.sh, bridge).await;
    let (_stand_in, mut mirror) =
        open_mirror_catalog(&guard.sh, &tmp.path().join("moving.sock")).await;

    let setup = connect_sql(&guard.sh).await;
    setup
        .batch_execute(
            "CREATE SCHEMA app;
             CREATE TABLE app.t (id int PRIMARY KEY, a text);
             -- committed attmissingval and a dropped slot, which both sources
             -- must render the same way
             CREATE TABLE app.m (id int, gone text);
             ALTER TABLE app.m ADD COLUMN v numeric[] DEFAULT '{1.5}';
             ALTER TABLE app.m DROP COLUMN gone;",
        )
        .await
        .expect("committed setup");
    let oid_t = oid_of(&setup, "app.t").await;

    let (_, committed) = cat
        .fetch_descriptors_batch(&[oid_t])
        .await
        .expect("committed batch");
    let (_, stated) = mirror
        .fetch_descriptors_batch(&[oid_t])
        .await
        .expect("mirroring statement");
    assert_eq!(stated, committed, "statement diverged from the worker");
    assert_eq!(mirror.stats().mirror_fetches, 1);
    // Capture-all: no oid list on the wire, the eligibility predicate in SQL
    let by_oid = |(_, mut descs): (u64, Vec<_>)| {
        descs.sort_by_key(|d: &walshadow::schema::RelDescriptor| d.oid);
        descs
    };
    assert_eq!(
        mirror.fetch_all_descriptors().await.map(by_oid).unwrap(),
        cat.fetch_all_descriptors().await.map(by_oid).unwrap(),
        "statement and worker disagree on the eligible set",
    );

    // An xid that wrote nothing sees only committed rows
    let idle = connect_sql(&guard.sh).await;
    idle.batch_execute("BEGIN").await.expect("begin idle");
    let idle_xid = top_xid(&idle).await;
    let overlay = cat
        .fetch_overlay_descriptors(&[oid_t], idle_xid, 0)
        .await
        .expect("overlay batch");
    assert_eq!(
        overlay, committed,
        "overlay diverged from the committed read"
    );
    idle.batch_execute("ROLLBACK").await.expect("rollback idle");

    // Transaction under test. Its schema and domain are invisible to the
    // committed name reads, so both fall through to a whole-catalog overlay
    let ddl = connect_sql(&guard.sh).await;
    ddl.batch_execute(
        "BEGIN;
         ALTER TABLE app.t ADD COLUMN c int DEFAULT 7;
         CREATE SCHEMA fresh;
         CREATE DOMAIN fresh.cents AS int;
         CREATE TABLE fresh.u (id int PRIMARY KEY, amount fresh.cents);",
    )
    .await
    .expect("open ddl");
    let xid = top_xid(&ddl).await;
    let oid_u = oid_of(&ddl, "fresh.u").await;

    let mut descs = cat
        .fetch_overlay_descriptors(&[oid_t, oid_u], xid, 0)
        .await
        .expect("overlay under open ddl");
    descs.sort_by_key(|d| d.oid);
    let (t, u) = match descs.as_slice() {
        [t, u] if t.oid == oid_t => (t, u),
        other => panic!("{other:#?}"),
    };

    let cols: Vec<&str> = t.attributes.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(cols, ["id", "a", "c"]);
    assert_eq!(t.attributes[2].missing_text.as_deref(), Some("7"));
    assert_eq!(t.attributes[2].type_name, "int4");
    // Unchanged by the ALTER, so still the committed values
    assert_eq!(t.rfn, committed[0].rfn);
    assert_eq!(t.replident, committed[0].replident);
    assert_eq!(t.rel_name.to_string(), "app.t");

    assert_eq!(u.rel_name.to_string(), "fresh.u", "in-xact schema name");
    assert_eq!(
        u.attributes[1].type_name, "cents",
        "in-xact domain name: {:#?}",
        u.attributes
    );
    assert_eq!(
        u.replident,
        ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        }
    );
    assert_ne!(u.rfn.rel_node, 0, "created in-xact but its storage exists");
    assert_eq!(u.rfn.spc_node, committed[0].rfn.spc_node);
    assert_eq!(u.rfn.db_node, committed[0].rfn.db_node);

    // An MVCC snapshot reaches none of it: the added column, the new relation,
    // and the schema and type the same transaction created
    let (_, stated) = mirror
        .fetch_descriptors_batch(&[oid_t, oid_u])
        .await
        .expect("statement under open ddl");
    assert_eq!(stated, committed, "statement saw uncommitted rows");

    // Replay off the asserted boundary is the caller's whole basis for reading
    // uncommitted rows, so it fails rather than answering
    let err = cat
        .fetch_overlay_descriptors(&[oid_t], xid, 0x1000)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            CatalogError::Bridge(BridgeError::ReplayMismatch { .. })
        ),
        "{err}"
    );

    ddl.batch_execute("ROLLBACK").await.expect("undo");
    let after = cat
        .fetch_overlay_descriptors(&[oid_t, oid_u], xid, 0)
        .await
        .expect("overlay after rollback");
    assert_eq!(after, committed, "aborted tree still visible");
}

fn read_frame(sock: &mut UnixStream) -> Vec<u8> {
    let mut hdr = [0u8; 4];
    sock.read_exact(&mut hdr).expect("frame header");
    let mut body = vec![0u8; u32::from_be_bytes(hdr) as usize];
    sock.read_exact(&mut body).expect("frame body");
    body
}

/// Error status, `u32` length, message, nothing else: the whole frame
fn parse_error_frame(body: &[u8]) -> String {
    assert_eq!(body[0], 1, "status byte: {body:?}");
    let mlen = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
    assert_eq!(body.len(), 5 + mlen, "frame not exactly consumed: {body:?}");
    String::from_utf8(body[5..].to_vec()).expect("utf8 message")
}

#[tokio::test(flavor = "current_thread")]
async fn bridge_error_frames_stay_parseable() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT + 4);
    let bridge = dial(&guard.sh).await;
    let path = guard.sh.bridge_socket().unwrap().to_path_buf();
    let mut raw = UnixStream::connect(&path).expect("raw connect");

    // Unknown opcode: one well-formed error frame, not a status byte
    // overwritten by the frame-length backfill
    raw.write_all(&1u32.to_be_bytes()).expect("write header");
    raw.write_all(&[0xee]).expect("write op");
    let msg = parse_error_frame(&read_frame(&mut raw));
    assert!(msg.contains("opcode"), "{msg}");

    // Trailing request bytes mean the peer framed a different request
    let mut hello = 1u32.to_be_bytes().to_vec();
    hello.push(0x01);
    raw.write_all(&hello).expect("write hello");
    let body = read_frame(&mut raw);
    assert_eq!(body[0], 0, "clean hello answers ok: {body:?}");
    let mut oversized = hello.clone();
    oversized[3] += 2; // frame length now counts the junk
    oversized.extend_from_slice(&[0xba, 0xad]);
    raw.write_all(&oversized).expect("write hello with junk");
    parse_error_frame(&read_frame(&mut raw));

    // Same connection serves the next request
    raw.write_all(&1u32.to_be_bytes()).expect("write header");
    raw.write_all(&[0x04]).expect("write replay_lsn");
    let body = read_frame(&mut raw);
    assert_eq!(body[0], 0, "{body:?}");
    assert_eq!(body.len(), 9);

    assert_eq!(bridge.replay_lsn().await.expect("still serving"), 0);
}

/// Worker stand-in that answers `HELLO` honestly and then reports a replay
/// position that moved inside the scan. Real movement wants a live standby
/// mid-stream; the daemon-side branch is the same either way.
fn spawn_moving_worker(listener: tokio::net::UnixListener) {
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            loop {
                let mut hdr = [0u8; 4];
                if tokio::io::AsyncReadExt::read_exact(&mut sock, &mut hdr)
                    .await
                    .is_err()
                {
                    break;
                }
                let mut req = vec![0u8; u32::from_be_bytes(hdr) as usize];
                if tokio::io::AsyncReadExt::read_exact(&mut sock, &mut req)
                    .await
                    .is_err()
                {
                    break;
                }
                let mut body = vec![0u8];
                if req.first() == Some(&0x01) {
                    body.extend_from_slice(&PROTO_VERSION.to_be_bytes());
                    body.extend_from_slice(&PROJECTION_VERSION.to_be_bytes());
                    body.extend_from_slice(&170_000u32.to_be_bytes());
                    body.push(1);
                } else {
                    // No rows, and the two positions disagree
                    body.extend_from_slice(&0x1000u64.to_be_bytes());
                    body.extend_from_slice(&0x2000u64.to_be_bytes());
                    for _ in 0..3 {
                        body.extend_from_slice(&0u32.to_be_bytes());
                    }
                    body.extend_from_slice(&(Catalog::Class.ncols() as u16).to_be_bytes());
                }
                let mut frame = (body.len() as u32).to_be_bytes().to_vec();
                frame.extend_from_slice(&body);
                if tokio::io::AsyncWriteExt::write_all(&mut sock, &frame)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
}

/// 8. Replay movement sends a committed read to the mirroring statement and
/// fails an overlay read outright.
#[tokio::test(flavor = "current_thread")]
async fn bridge_committed_read_falls_back_when_replay_moves() {
    use std::sync::atomic::Ordering;

    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let guard = start_pg(&tmp, BASE_PORT + 7);
    let setup = connect_sql(&guard.sh).await;
    setup
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY, a text)")
        .await
        .expect("setup");
    let oid_t = oid_of(&setup, "public.t").await;

    let (bridge, mut cat) = open_mirror_catalog(&guard.sh, &tmp.path().join("moving.sock")).await;

    // No sequence of scans answers for one position once replay moves, and
    // the statement's one snapshot always can
    let (_, descs) = cat
        .fetch_descriptors_batch(&[oid_t])
        .await
        .expect("committed read after the pin broke");
    assert_eq!(bridge.stats.scan_replay_moved.load(Ordering::Relaxed), 1);
    assert_eq!(cat.stats().mirror_fetches, 1);
    let (_, scanned) = open_catalog(&guard.sh, Arc::new(dial(&guard.sh).await))
        .await
        .fetch_descriptors_batch(&[oid_t])
        .await
        .expect("worker at a position that holds still");
    assert_eq!(descs, scanned, "fallback built a different descriptor");

    // The caller holds the boundary an overlay read is about, so nothing else
    // can serve that question
    let err = cat
        .fetch_overlay_descriptors(&[oid_t], 700, 0x1000)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::Bridge(BridgeError::ReplayMismatch { .. })
        ),
        "{err:?}"
    );
}
