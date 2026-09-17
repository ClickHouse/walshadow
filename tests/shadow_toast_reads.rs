//! Does PostgreSQL still hand back a TOAST value after the row that referred
//! to it is gone?
//!
//! `plans/shadow_toast.md` proposes reading external values out of shadow's
//! physical TOAST heaps instead of the ClickHouse chunk mirror. The design
//! rests on one property: `HeapTupleSatisfiesToast` ignores xmax, so a value
//! whose referring row version is dead should stay readable until the chunks
//! are physically reclaimed. This measures the property and, more usefully,
//! measures *what* reclaims — because that is what a fence has to withhold.
//!
//! The answer these tests record: it is not VACUUM. Opportunistic pruning
//! (`heap_page_prune_opt`) takes the chunks the moment the cleanup horizon
//! passes the deleting transaction, which on a primary is immediately. Shadow
//! is a physical standby where replayed WAL is the sole writer, so nothing
//! prunes locally and the chunks survive until an `XLOG_HEAP2_PRUNE` record is
//! replayed — the first entry in the plan's destructive-operation list.
//!
//! Requires `pgext/walshadow.so` built against the `initdb` on PATH.

#[path = "common/ports.rs"]
mod ports;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls};
use walshadow::bridge::{Bridge, FetchedChunks, ToastSnapshot};
use walshadow::pg::socket_conninfo;
use walshadow::shadow::{BridgeConf, Shadow, ShadowConfig};

fn have(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

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

fn append_conf(data_dir: &Path, lines: &str) {
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(data_dir.join("postgresql.conf"))
        .expect("open conf");
    f.write_all(lines.as_bytes()).expect("append conf");
}

/// Autovacuum off throughout, so every reclamation in here is one the test
/// asked for and a Missing names the operation that caused it
fn start_pg(tmp: &tempfile::TempDir, port: u16, archive: &Path) -> StopOnDrop {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    let mut bridge = BridgeConf::in_dir(&cfg.socket_dir);
    bridge.library_dir = Some(pgext_dir());
    cfg.bridge = Some(bridge);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    fs::create_dir_all(archive).unwrap();

    let sh = Shadow::new(cfg);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("write_base_conf");
    append_conf(
        &sh.config().data_dir,
        &format!(
            "\n# shadow_toast_reads source\n\
             autovacuum = off\n\
             archive_mode = on\n\
             archive_command = 'cp %p {}/%f'\n\
             max_wal_senders = 4\n",
            archive.display()
        ),
    );
    sh.start().expect("start");
    StopOnDrop { sh }
}

async fn dial(sock: &Path) -> Bridge {
    walshadow::bridge::connect_with_budget(sock, 1, Duration::from_secs(20))
        .await
        .unwrap_or_else(|e| panic!("bridge connect on {}: {e}", sock.display()))
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

async fn exec(c: &Client, sql: &str) {
    c.batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

async fn scalar(c: &Client, sql: &str) -> String {
    c.query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get::<_, Option<String>>(0)
        .unwrap_or_default()
}

/// A toast relation is named for its parent's oid, not its own, so the name is
/// read back rather than derived from `reltoastrelid`
async fn toast_table(c: &Client, toast_relid: u32) -> String {
    let name = scalar(
        c,
        &format!("SELECT quote_ident(relname) FROM pg_class WHERE oid = {toast_relid}"),
    )
    .await;
    format!("pg_toast.{name}")
}

/// One toasted value, captured while it is live: everything a fetch needs plus
/// the plaintext to compare against
#[derive(Clone)]
struct Value {
    table: String,
    toast_relid: u32,
    value_id: u32,
    extsize: usize,
    raw: String,
}

/// `external` disables compression, so stored bytes equal the plaintext and a
/// byte comparison means something. Without it PG compresses first and stored
/// bytes are the compressed form the daemon would still have to inflate
async fn seed_value(c: &Client, name: &str, body_sql: &str, external: bool) -> Value {
    exec(c, &format!("DROP TABLE IF EXISTS {name}")).await;
    exec(
        c,
        &format!("CREATE TABLE {name} (id int primary key, body text)"),
    )
    .await;
    if external {
        exec(
            c,
            &format!("ALTER TABLE {name} ALTER body SET STORAGE EXTERNAL"),
        )
        .await;
    }
    exec(c, &format!("INSERT INTO {name} VALUES (1, {body_sql})")).await;

    let toast_relid: u32 = scalar(
        c,
        &format!("SELECT reltoastrelid::text FROM pg_class WHERE oid = '{name}'::regclass"),
    )
    .await
    .parse()
    .expect("toast relid");
    assert_ne!(toast_relid, 0, "{name} has no toast relation");

    let tt = toast_table(c, toast_relid).await;
    let row = c
        .query_one(
            &format!(
                "SELECT chunk_id::text, sum(length(chunk_data))::text, count(*)::text \
                 FROM {tt} GROUP BY chunk_id"
            ),
            &[],
        )
        .await
        .expect("exactly one value in the toast rel");
    let value_id: u32 = row.get::<_, String>(0).parse().unwrap();
    let extsize: usize = row.get::<_, String>(1).parse().unwrap();
    let chunks: usize = row.get::<_, String>(2).parse().unwrap();
    assert!(chunks > 1, "{name} value should span chunks, got {chunks}");

    let raw = scalar(c, &format!("SELECT body FROM {name} WHERE id = 1")).await;
    Value {
        table: name.into(),
        toast_relid,
        value_id,
        extsize,
        raw,
    }
}

async fn fetch(bridge: &Bridge, v: &Value, snap: ToastSnapshot) -> FetchedChunks {
    bridge
        .fetch_toast(v.toast_relid, &[(v.value_id, v.extsize)], 0, snap)
        .await
        .unwrap_or_else(|e| panic!("fetch {} value {}: {e}", v.table, v.value_id))
        .values
        .pop()
        .expect("one value asked, one answered")
}

fn describe(f: &FetchedChunks, expected: usize) -> String {
    match f {
        FetchedChunks::Stored(b) => format!("Stored {} of {expected}", b.len()),
        FetchedChunks::Missing => "Missing".into(),
        FetchedChunks::Mismatch { got } => format!("Mismatch got {got} of {expected}"),
    }
}

/// Keeps the cluster-wide cleanup horizon behind the deleting transaction, so
/// `heap_page_prune_opt` cannot touch the chunks it killed. Standing in for
/// the standby property that nothing prunes locally
struct PinnedHorizon {
    client: Client,
}

impl PinnedHorizon {
    async fn hold(sh: &Shadow) -> Self {
        let client = connect_sql(sh).await;
        exec(&client, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
        exec(&client, "SELECT 1").await;
        Self { client }
    }

    async fn release(self) {
        exec(&self.client, "COMMIT").await;
    }
}

// ---------------------------------------------------------------------------
// Primary: what the visibility rule allows, and what takes it away
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_value_reads_back_byte_exact() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;

    let v = seed_value(&sql, "t_live", "repeat('ab', 120000)", true).await;
    match fetch(&bridge, &v, ToastSnapshot::Toast).await {
        FetchedChunks::Stored(b) => {
            assert_eq!(b.len(), v.extsize, "stored length");
            assert_eq!(b, v.raw.as_bytes(), "stored bytes are the plaintext");
        }
        other => panic!("live value fetched {}", describe(&other, v.extsize)),
    }
}

/// The load-bearing case. With the horizon pinned the chunks stay on the page,
/// and `SnapshotToast` reads them despite a committed xmax
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_referrer_still_yields_its_value() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;

    let deleted = seed_value(&sql, "t_deleted", "repeat('cd', 120000)", true).await;
    let replaced = seed_value(&sql, "t_replaced", "repeat('ef', 120000)", true).await;
    let hold = PinnedHorizon::hold(&pg.sh).await;

    exec(&sql, "DELETE FROM t_deleted WHERE id = 1").await;
    // An UPDATE externalizes a fresh value under a new id and deletes the old
    // chunks through toast_delete_datum, the same reclamation by another route
    exec(
        &sql,
        "UPDATE t_replaced SET body = repeat('gh', 120000) WHERE id = 1",
    )
    .await;

    for v in [&deleted, &replaced] {
        let got = fetch(&bridge, v, ToastSnapshot::Toast).await;
        assert!(
            matches!(&got, FetchedChunks::Stored(b) if b == v.raw.as_bytes()),
            "{} after its referrer died: {}",
            v.table,
            describe(&got, v.extsize)
        );
    }
    hold.release().await;
}

/// Pruning, not VACUUM, is the reclaimer — and it needs no statement of its
/// own. This is the finding that makes the plan's fence about
/// `XLOG_HEAP2_PRUNE` rather than about vacuum records
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pruning_reclaims_as_soon_as_the_horizon_passes() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;

    let pinned = seed_value(&sql, "t_pinned", "repeat('qr', 120000)", true).await;
    let hold = PinnedHorizon::hold(&pg.sh).await;
    exec(&sql, "DELETE FROM t_pinned WHERE id = 1").await;
    let under_hold = fetch(&bridge, &pinned, ToastSnapshot::Toast).await;
    hold.release().await;

    let free = seed_value(&sql, "t_free", "repeat('st', 120000)", true).await;
    exec(&sql, "DELETE FROM t_free WHERE id = 1").await;
    let no_hold = fetch(&bridge, &free, ToastSnapshot::Toast).await;

    eprintln!(
        "horizon pinned: {}\nhorizon free:   {}",
        describe(&under_hold, pinned.extsize),
        describe(&no_hold, free.extsize)
    );
    assert!(
        matches!(&under_hold, FetchedChunks::Stored(b) if b == pinned.raw.as_bytes()),
        "pinned: {}",
        describe(&under_hold, pinned.extsize)
    );
    assert_ne!(
        no_hold,
        FetchedChunks::Stored(free.raw.clone().into_bytes()),
        "an unpinned horizon lets pruning take the chunks with no VACUUM asked for"
    );
}

/// A partly reclaimed run is what pruning leaves behind, and it must never
/// pass as the value. Density plus total size is the whole check
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partly_reclaimed_run_refuses() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;

    let v = seed_value(&sql, "t_torn", "repeat('uv', 120000)", true).await;
    exec(&sql, "DELETE FROM t_torn WHERE id = 1").await;
    let got = fetch(&bridge, &v, ToastSnapshot::Toast).await;
    match &got {
        FetchedChunks::Mismatch { got } => assert!(
            *got < v.extsize,
            "a torn run reports how far it got: {got} of {}",
            v.extsize
        ),
        FetchedChunks::Missing => {}
        FetchedChunks::Stored(_) => {
            panic!("a pruned run must not read back as the value")
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vacuum_rewrite_and_truncate_each_remove_the_value() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;

    let vacuumed = seed_value(&sql, "t_vac", "repeat('wx', 120000)", true).await;
    exec(&sql, "DELETE FROM t_vac WHERE id = 1").await;
    exec(&sql, "VACUUM t_vac").await;
    assert_eq!(
        fetch(&bridge, &vacuumed, ToastSnapshot::Toast).await,
        FetchedChunks::Missing,
        "VACUUM removes the chunks and their index entries"
    );

    // A rewrite moves the toast rel to a new relfilenode and unlinks the old
    // file, so the old generation is unreachable even by relid
    let rewritten = seed_value(&sql, "t_rewrite", "repeat('ij', 120000)", true).await;
    exec(&sql, "DELETE FROM t_rewrite WHERE id = 1").await;
    exec(&sql, "VACUUM FULL t_rewrite").await;
    assert_eq!(
        fetch(&bridge, &rewritten, ToastSnapshot::Toast).await,
        FetchedChunks::Missing,
    );

    let truncated = seed_value(&sql, "t_trunc", "repeat('kl', 120000)", true).await;
    exec(&sql, "TRUNCATE t_trunc").await;
    assert_eq!(
        fetch(&bridge, &truncated, ToastSnapshot::Toast).await,
        FetchedChunks::Missing,
    );

    // DROP takes the relation with it, which is an error rather than a Missing
    // that would license filling a default
    let dropped = seed_value(&sql, "t_drop", "repeat('yz', 120000)", true).await;
    exec(&sql, "DROP TABLE t_drop").await;
    assert!(
        bridge
            .fetch_toast(
                dropped.toast_relid,
                &[(dropped.value_id, dropped.extsize)],
                0,
                ToastSnapshot::Toast,
            )
            .await
            .is_err(),
        "a dropped toast relation must not answer Missing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compressed_value_comes_back_stored_not_inflated() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;

    // Repeating pattern: compresses hard, still over the toast target, so it
    // lands external *and* compressed
    let v = seed_value(&sql, "t_comp", "repeat('abcdefgh', 200000)", false).await;
    assert!(
        v.extsize < v.raw.len(),
        "value should be stored compressed: {} stored vs {} raw",
        v.extsize,
        v.raw.len()
    );
    match fetch(&bridge, &v, ToastSnapshot::Toast).await {
        FetchedChunks::Stored(b) => {
            assert_eq!(b.len(), v.extsize, "stored, not inflated");
            assert_ne!(b, v.raw.as_bytes(), "bytes are the compressed form");
        }
        other => panic!("compressed value fetched {}", describe(&other, v.extsize)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_refuses_malformed_requests() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;
    let v = seed_value(&sql, "t_bad", "repeat('op', 120000)", true).await;

    // Empty and over-cap lists never reach the socket
    assert!(
        bridge
            .fetch_toast(v.toast_relid, &[], 0, ToastSnapshot::Toast)
            .await
            .is_err()
    );
    let too_many: Vec<(u32, usize)> = (0..walshadow::bridge::MAX_FETCH_VALUES as u32 + 1)
        .map(|i| (i, 0))
        .collect();
    assert!(
        bridge
            .fetch_toast(v.toast_relid, &too_many, 0, ToastSnapshot::Toast)
            .await
            .is_err()
    );

    // An unknown relation is an error, never a Missing that licenses a fill
    assert!(
        bridge
            .fetch_toast(999_999, &[(1, 8)], 0, ToastSnapshot::Toast)
            .await
            .is_err(),
        "absent toast relation must not answer Missing"
    );

    // A replay floor a primary cannot meet is refused rather than answered
    assert!(
        bridge
            .fetch_toast(
                v.toast_relid,
                &[(v.value_id, v.extsize)],
                u64::MAX,
                ToastSnapshot::Toast,
            )
            .await
            .is_err(),
        "min_replay_lsn above the current position must refuse"
    );

    // A value id that was never allocated is Missing, not an error
    assert_eq!(
        bridge
            .fetch_toast(
                v.toast_relid,
                &[(v.value_id.wrapping_add(7919), 8)],
                0,
                ToastSnapshot::Toast
            )
            .await
            .expect("absent id is a per-value result")
            .values
            .pop()
            .unwrap(),
        FetchedChunks::Missing,
    );

    // Wrong expected size is a mismatch, so a torn run can never pass as whole
    assert!(matches!(
        bridge
            .fetch_toast(
                v.toast_relid,
                &[(v.value_id, v.extsize - 1)],
                0,
                ToastSnapshot::Toast
            )
            .await
            .expect("size disagreement is a per-value result")
            .values
            .pop()
            .unwrap(),
        FetchedChunks::Mismatch { .. }
    ));

    // SnapshotAny is wider but must agree on a live, whole value
    assert!(matches!(
        fetch(&bridge, &v, ToastSnapshot::Any).await,
        FetchedChunks::Stored(ref b) if b == v.raw.as_bytes()
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_fetch_aligns_with_its_request() {
    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = dial(pg.sh.bridge_socket().unwrap()).await;
    let sql = connect_sql(&pg.sh).await;

    exec(&sql, "CREATE TABLE t_many (id int primary key, body text)").await;
    exec(&sql, "ALTER TABLE t_many ALTER body SET STORAGE EXTERNAL").await;
    for i in 1..=8 {
        exec(
            &sql,
            &format!("INSERT INTO t_many VALUES ({i}, repeat('{i}', 90000))"),
        )
        .await;
    }
    let toast_relid: u32 = scalar(
        &sql,
        "SELECT reltoastrelid::text FROM pg_class WHERE oid = 't_many'::regclass",
    )
    .await
    .parse()
    .unwrap();

    let tt = toast_table(&sql, toast_relid).await;
    let rows = sql
        .query(
            &format!(
                "SELECT chunk_id::text, sum(length(chunk_data))::text \
                 FROM {tt} GROUP BY chunk_id ORDER BY chunk_id"
            ),
            &[],
        )
        .await
        .unwrap();
    let want: Vec<(u32, usize)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, String>(0).parse().unwrap(),
                r.get::<_, String>(1).parse().unwrap(),
            )
        })
        .collect();
    assert_eq!(want.len(), 8, "one value per row");

    // One round trip, results positional. An absent id in the middle must not
    // shift the ones after it
    let mut asked = want.clone();
    asked.insert(4, (u32::MAX, 16));
    let started = Instant::now();
    let got = bridge
        .fetch_toast(toast_relid, &asked, 0, ToastSnapshot::Toast)
        .await
        .expect("batch fetch");
    let elapsed = started.elapsed();
    assert_eq!(got.values.len(), asked.len());
    assert_eq!(got.values[4], FetchedChunks::Missing);
    let mut bytes = 0usize;
    for (i, (_, expected)) in asked.iter().enumerate() {
        if i == 4 {
            continue;
        }
        match &got.values[i] {
            FetchedChunks::Stored(b) => {
                assert_eq!(b.len(), *expected, "value {i}");
                bytes += b.len();
            }
            other => panic!("value {i} fetched {}", describe(other, *expected)),
        }
    }
    // The number the plan is spent against: the ClickHouse mirror answered one
    // value per 71.9 ms round trip on the terracotta load
    eprintln!(
        "8 values, {bytes} stored bytes, one round trip: {:?} ({:?}/value)",
        elapsed,
        elapsed / 8
    );
    assert!(
        got.replay_lsn_start == 0 && got.replay_lsn_end == 0,
        "a primary reports no replay position"
    );
}

// ---------------------------------------------------------------------------
// Standby: the real target. Replayed WAL is the sole writer, so nothing prunes
// locally and the chunks survive until the prune record arrives
// ---------------------------------------------------------------------------

/// Ship every completed segment to the archive the standby restores from
fn ship_wal(source: &Shadow) {
    source.psql_one("SELECT pg_switch_wal()").expect("switch wal");
    source
        .psql_one("CHECKPOINT")
        .expect("checkpoint so the segment is archived");
}

async fn wait_replay_past(standby: &Client, lsn: &str, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let at = scalar(
            standby,
            &format!("SELECT (pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn)::text"),
        )
        .await;
        if at == "true" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "standby never replayed past {lsn} ({what})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `pg_basebackup` clone started as a hot standby that replays out of
/// `archive` alone. `max_standby_*_delay = -1` makes recovery wait for a
/// conflicting query rather than cancel it, which is what lets a held snapshot
/// act as a fence instead of producing a cancellation
async fn clone_standby(
    tmp: &tempfile::TempDir,
    source: &Shadow,
    archive: &Path,
) -> (StopOnDrop, Bridge, Client) {
    let sb_data = tmp.path().join("sb-data");
    let sb_sock = tmp.path().join("sb-sock");
    let sb_port = ports::reserve_port();
    fs::create_dir_all(&sb_sock).unwrap();
    let out = Command::new("pg_basebackup")
        .args([
            "-h",
            source.config().socket_dir.to_str().unwrap(),
            "-p",
            &source.config().port.to_string(),
            "-U",
            "postgres",
            "-D",
            sb_data.to_str().unwrap(),
            "-X",
            "stream",
            "-c",
            "fast",
            "-w",
            "--no-sync",
        ])
        .output()
        .expect("spawn pg_basebackup");
    assert!(
        out.status.success(),
        "pg_basebackup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let mut sb_cfg = ShadowConfig::new(sb_data.clone(), tmp.path().join("sb-filtered"));
    sb_cfg.port = sb_port;
    sb_cfg.socket_dir = sb_sock.clone();
    sb_cfg.ctl_timeout = Duration::from_secs(60);
    let mut bridge = BridgeConf::in_dir(&sb_sock);
    bridge.library_dir = Some(pgext_dir());
    fs::create_dir_all(&sb_cfg.filter_out_dir).unwrap();
    append_conf(
        &sb_data,
        &format!(
            "\n# shadow_toast_reads standby\n\
             port = {sb_port}\n\
             unix_socket_directories = '{}'\n\
             listen_addresses = ''\n\
             hot_standby = on\n\
             autovacuum = off\n\
             archive_mode = off\n\
             hot_standby_feedback = off\n\
             max_standby_streaming_delay = -1\n\
             max_standby_archive_delay = -1\n\
             wal_retrieve_retry_interval = '100ms'\n\
             restore_command = 'cp {}/%f %p'\n\
             recovery_target_timeline = 'latest'\n{}",
            sb_sock.display(),
            archive.display(),
            bridge.conf_text(&sb_cfg.dbname),
        ),
    );
    fs::write(sb_data.join("standby.signal"), b"").unwrap();
    sb_cfg.bridge = Some(bridge);
    let standby = StopOnDrop {
        sh: Shadow::new(sb_cfg),
    };
    if let Err(e) = standby.sh.start() {
        let log = fs::read_to_string(sb_data.join("startup.log")).unwrap_or_default();
        panic!("standby start: {e}\n{log}");
    }
    assert!(
        standby.sh.is_in_recovery().expect("probe recovery"),
        "must boot into recovery"
    );
    let sb_bridge = dial(standby.sh.bridge_socket().unwrap()).await;
    let sb_sql = connect_sql(&standby.sh).await;
    (standby, sb_bridge, sb_sql)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_keeps_the_value_until_the_prune_record_replays() {
    if !have("initdb") || !have("pg_basebackup") {
        eprintln!("skip: no initdb/pg_basebackup on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let archive = tmp.path().join("archive");
    let source = start_pg(&tmp, ports::reserve_port(), &archive);
    let src_sql = connect_sql(&source.sh).await;

    // Horizon pinned on the source for the whole run, so the source never
    // prunes and no prune record is ever written. This is what a shadow sees
    // by construction: replayed WAL is its only writer
    let hold = PinnedHorizon::hold(&source.sh).await;
    let v = seed_value(&src_sql, "t_sb", "repeat('ab', 120000)", true).await;
    exec(&src_sql, "DELETE FROM t_sb WHERE id = 1").await;
    let after_delete = scalar(&src_sql, "SELECT pg_current_wal_lsn()::text").await;

    // Clone after the delete, so the standby starts from a page carrying dead
    // chunks and an intact run
    let (_standby, sb_bridge, sb_sql) = clone_standby(&tmp, &source.sh, &archive).await;
    ship_wal(&source.sh);
    wait_replay_past(&sb_sql, &after_delete, "the DELETE").await;

    let replay = scalar(&sb_sql, "SELECT pg_last_wal_replay_lsn()::text").await;
    let replay_lsn = walshadow::pg::parse_pg_lsn(&replay).expect("parse replay lsn");
    let got = sb_bridge
        .fetch_toast(
            v.toast_relid,
            &[(v.value_id, v.extsize)],
            replay_lsn,
            ToastSnapshot::Toast,
        )
        .await
        .expect("standby fetch at its own replay position");
    assert!(
        got.replay_lsn_start >= replay_lsn,
        "worker samples a replay position at or past the floor asked for"
    );
    assert!(
        matches!(&got.values[0], FetchedChunks::Stored(b) if b == v.raw.as_bytes()),
        "on a standby the dead referrer's value must still read whole: {}",
        describe(&got.values[0], v.extsize)
    );

    // Now let the source prune and ship the record. Replaying it is what takes
    // the value away, which is exactly what a fence would withhold
    hold.release().await;
    exec(&src_sql, "VACUUM t_sb").await;
    let after_vacuum = scalar(&src_sql, "SELECT pg_current_wal_lsn()::text").await;
    ship_wal(&source.sh);
    wait_replay_past(&sb_sql, &after_vacuum, "the VACUUM").await;

    let after = fetch(&sb_bridge, &v, ToastSnapshot::Toast).await;
    assert_ne!(
        after,
        FetchedChunks::Stored(v.raw.clone().into_bytes()),
        "replaying the reclamation must take the value: {}",
        describe(&after, v.extsize)
    );
    eprintln!("standby after replayed reclamation: {}", describe(&after, v.extsize));
}

/// The `ChunkStore` face the pipeline would see: reads map onto the mirror's
/// own result vocabulary, and every write refuses instead of quietly passing
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_store_reads_and_refuses_writes() {
    use std::sync::Arc;
    use walshadow::toast::shadow_store::ShadowToastStore;
    use walshadow::toast::{ChunkStore, ChunkStoreError, FetchedValue, ToastRow};

    if !have("initdb") {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let pg = start_pg(&tmp, ports::reserve_port(), &tmp.path().join("archive"));
    let bridge = Arc::new(dial(pg.sh.bridge_socket().unwrap()).await);
    let sql = connect_sql(&pg.sh).await;
    let v = seed_value(&sql, "t_store", "repeat('ab', 120000)", true).await;

    let store = ShadowToastStore::new(bridge);
    assert_eq!(
        store
            .fetch(v.toast_relid, v.value_id, 0, v.extsize)
            .await
            .expect("fetch"),
        FetchedValue::Assembled(v.raw.clone().into_bytes()),
    );
    assert_eq!(
        store
            .fetch(v.toast_relid, v.value_id.wrapping_add(7919), 0, 8)
            .await
            .expect("absent id"),
        FetchedValue::Missing,
    );
    assert!(matches!(
        store
            .fetch(v.toast_relid, v.value_id, 0, v.extsize - 1)
            .await
            .expect("size disagreement"),
        FetchedValue::Mismatch { .. }
    ));
    assert!(
        store
            .fetch_many(v.toast_relid, &[], 0)
            .await
            .unwrap()
            .is_empty()
    );
    // The wider snapshot must agree on a live, whole value
    assert_eq!(
        ShadowToastStore::new(Arc::new(dial(pg.sh.bridge_socket().unwrap()).await))
            .with_snapshot(ToastSnapshot::Any)
            .fetch(v.toast_relid, v.value_id, 0, v.extsize)
            .await
            .expect("fetch under SnapshotAny"),
        FetchedValue::Assembled(v.raw.clone().into_bytes()),
    );
    // A relation shadow does not have is an error, not a Missing: absence of a
    // backend is not evidence the value was superseded
    assert!(matches!(
        store.fetch(999_999, 1, 0, 8).await,
        Err(ChunkStoreError::Shadow(_))
    ));

    let row = ToastRow {
        toast_relid: v.toast_relid,
        blkno: 0,
        offnum: 1,
        chunk_id: v.value_id,
        chunk_seq: 0,
        chunk_data: bytes::Bytes::from_static(b"x"),
        lsn: 1,
    };
    for e in [
        store.put(&[row]).await.unwrap_err(),
        store.truncate_mirror(v.toast_relid).await.unwrap_err(),
        store
            .rewrite_barrier(v.toast_relid, 1, 2)
            .await
            .unwrap_err(),
    ] {
        assert!(matches!(e, ChunkStoreError::ReadOnly(_)), "{e}");
    }
}

/// Did replay get past `lsn` within `budget`? Unlike [`wait_replay_past`] this
/// reports rather than asserts, because a stall is sometimes the finding
async fn replay_passed_within(standby: &Client, lsn: &str, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        let at = scalar(
            standby,
            &format!("SELECT (pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn)::text"),
        )
        .await;
        if at == "true" {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Can PG's own standby-conflict machinery serve as the reclamation fence?
///
/// It parks replay of a record carrying a `snapshotConflictHorizon` while a
/// query holds an older snapshot, and `max_standby_archive_delay = -1` makes it
/// wait rather than cancel. If that covered us, the plan's staged-bytes fence
/// would be unnecessary.
///
/// It does not. Measured both ways below: conflict resolution protects tuples
/// the held snapshot can *see*, and a snapshot opened after the referrer died
/// cannot see the row, so PG declines to conflict and the chunks go. A snapshot
/// opened before the delete replays does hold the line — but that is a snapshot
/// that still sees the live row, which is not the state a reader owing old
/// values is in.
///
/// So the fence has to be walshadow's own, which is what
/// `plans/shadow_toast.md` prescribes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_standby_conflicts_do_not_fence_reclamation() {
    if !have("initdb") || !have("pg_basebackup") {
        eprintln!("skip: no initdb/pg_basebackup on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let archive = tmp.path().join("archive");
    let source = start_pg(&tmp, ports::reserve_port(), &archive);
    let src_sql = connect_sql(&source.sh).await;

    // Seed and clone while the row is still live, so a standby snapshot can be
    // opened on either side of the DELETE
    let hold = PinnedHorizon::hold(&source.sh).await;
    let v = seed_value(&src_sql, "t_fence", "repeat('ab', 120000)", true).await;
    let seeded = scalar(&src_sql, "SELECT pg_current_wal_lsn()::text").await;
    let (standby, sb_bridge, sb_sql) = clone_standby(&tmp, &source.sh, &archive).await;
    ship_wal(&source.sh);
    wait_replay_past(&sb_sql, &seeded, "the INSERT").await;

    // Reader A sees the live row: its interest in the chunks is one PG
    // recognises
    let early = connect_sql(&standby.sh).await;
    exec(&early, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    assert_eq!(scalar(&early, "SELECT count(*)::text FROM t_fence").await, "1");

    exec(&src_sql, "DELETE FROM t_fence WHERE id = 1").await;
    hold.release().await;
    exec(&src_sql, "VACUUM t_fence").await;
    let reclaimed = scalar(&src_sql, "SELECT pg_current_wal_lsn()::text").await;
    ship_wal(&source.sh);

    let parked_for_early =
        !replay_passed_within(&sb_sql, &reclaimed, Duration::from_secs(5)).await;
    let under_early = fetch(&sb_bridge, &v, ToastSnapshot::Toast).await;
    eprintln!(
        "snapshot predating the DELETE parks replay: {parked_for_early}, value {}",
        describe(&under_early, v.extsize)
    );
    exec(&early, "COMMIT").await;
    assert!(
        replay_passed_within(&sb_sql, &reclaimed, Duration::from_secs(30)).await,
        "replay must reach the reclamation once nothing conflicts"
    );

    // Reader B opens after the reclamation replayed, which is the state a
    // reader owing pre-window values is actually in. Nothing is left to protect
    let late = connect_sql(&standby.sh).await;
    exec(&late, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    exec(&late, "SELECT 1").await;
    let under_late = fetch(&sb_bridge, &v, ToastSnapshot::Toast).await;
    eprintln!("value to a snapshot opened after it: {}", describe(&under_late, v.extsize));
    exec(&late, "COMMIT").await;

    assert_ne!(
        under_late,
        FetchedChunks::Stored(v.raw.clone().into_bytes()),
        "PG does not keep a value alive for a snapshot that cannot see its row, \
         so the reclamation fence cannot be delegated to standby conflicts"
    );
}

/// Readiness has three shapes, and the store has to tell them apart.
///
/// A primary has no replay position: the bootstrap oracle is one, and its
/// files are staged complete before it serves, so a read must not wait. A
/// standby does have one, and a read below it must wait and then say so rather
/// than hang. And a store whose PostgreSQL is not bound yet must park.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_readiness_distinguishes_primary_standby_and_unbound() {
    use std::sync::Arc;
    use walshadow::toast::shadow_store::{LateBridge, ShadowToastStore};
    use walshadow::toast::{ChunkStore, ChunkStoreError, FetchedValue};

    if !have("initdb") || !have("pg_basebackup") {
        eprintln!("skip: no initdb/pg_basebackup on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let archive = tmp.path().join("archive");
    let source = start_pg(&tmp, ports::reserve_port(), &archive);
    let src_sql = connect_sql(&source.sh).await;
    let v = seed_value(&src_sql, "t_ready", "repeat('ab', 120000)", true).await;
    let seeded = scalar(&src_sql, "SELECT pg_current_wal_lsn()::text").await;

    // Unbound: parks, then names what it was waiting for
    let unbound = LateBridge::default();
    let parked = ShadowToastStore::late(unbound.clone())
        .with_replay_wait_max(Duration::from_millis(300));
    let started = Instant::now();
    let err = parked
        .fetch(v.toast_relid, v.value_id, 0, v.extsize)
        .await
        .expect_err("an unbound store cannot read");
    assert!(
        matches!(&err, ChunkStoreError::Shadow(m) if m.contains("bound")),
        "{err}"
    );
    assert!(started.elapsed() >= Duration::from_millis(250));

    // Binding it makes the same store readable, no restart involved
    assert!(
        unbound
            .set(Arc::new(dial(source.sh.bridge_socket().unwrap()).await))
            .is_ok(),
        "binds once",
    );
    assert!(matches!(
        parked
            .fetch(v.toast_relid, v.value_id, 0, v.extsize)
            .await
            .expect("bound store reads"),
        FetchedValue::Assembled(_)
    ));

    // Primary: a replay floor it can never report must not stall the read,
    // because a primary's files are not pending anything
    let started = Instant::now();
    assert!(matches!(
        parked
            .fetch(v.toast_relid, v.value_id, u64::MAX, v.extsize)
            .await
            .expect("a primary does not wait for replay"),
        FetchedValue::Assembled(_)
    ));
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "primary read waited {:?}",
        started.elapsed()
    );

    // Standby: the floor is real, so a read above its position waits its
    // budget and then reports both positions
    let (_standby, sb_bridge, sb_sql) = clone_standby(&tmp, &source.sh, &archive).await;
    ship_wal(&source.sh);
    wait_replay_past(&sb_sql, &seeded, "the INSERT").await;
    let on_standby = ShadowToastStore::new(Arc::new(sb_bridge))
        .with_replay_wait_max(Duration::from_millis(300));
    assert!(matches!(
        on_standby
            .fetch(v.toast_relid, v.value_id, 0, v.extsize)
            .await
            .expect("standby reads at its own position"),
        FetchedValue::Assembled(_)
    ));
    let started = Instant::now();
    let err = on_standby
        .fetch(v.toast_relid, v.value_id, u64::MAX, v.extsize)
        .await
        .expect_err("a standby must honour a floor it cannot reach");
    assert!(
        matches!(&err, ChunkStoreError::Shadow(m) if m.contains("replay")),
        "{err}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "standby gave up after {:?}",
        started.elapsed()
    );

    // An empty batch neither binds, waits, nor reads
    let started = Instant::now();
    assert!(
        ShadowToastStore::late(LateBridge::default())
            .fetch_many(v.toast_relid, &[], u64::MAX)
            .await
            .expect("empty batch short-circuits everything")
            .is_empty()
    );
    assert!(started.elapsed() < Duration::from_millis(100));
}
