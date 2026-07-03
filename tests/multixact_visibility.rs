//! `PgMultiXactAccum` SLRU parsing against bytes a live PG writes.
//!
//! `SELECT ... FOR KEY SHARE` inside a savepoint, then a non-key `UPDATE`
//! after `RELEASE`: the subxid locker differs from the top-xid updater, so
//! `compute_new_xmax_infomask` (PG src/backend/access/heap/heapam.c) builds
//! a multixact {subxid ForKeyShare, top xid NoKeyUpdate} — single session,
//! deterministic. Cross-checks `updater()` against
//! `pg_get_multixact_members()` and gates through real `pg_xact` bytes.
//!
//! Skipped silently when `initdb` is not on `$PATH`.

use std::process::Command;
use std::time::Duration;

use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::visibility::{
    HEAP_XMAX_IS_MULTI, HEAP_XMIN_COMMITTED, MultiXactUpdater, PgMultiXactAccum, PgXactAccum,
    PgXactPatch, PgXactView, Visibility, tuple_visibility,
};

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_cluster(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(30);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
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

#[test]
fn multixact_updater_matches_live_pg() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_cluster(&tmp, 55731);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };

    sh.psql_one("CREATE TABLE t (id int PRIMARY KEY, v int)")
        .unwrap();
    sh.psql_one("INSERT INTO t VALUES (1, 0)").unwrap();
    sh.psql_one(
        "BEGIN; SAVEPOINT s; SELECT id FROM t WHERE id = 1 FOR KEY SHARE; \
         RELEASE SAVEPOINT s; UPDATE t SET v = 1 WHERE id = 1; COMMIT",
    )
    .unwrap();
    // Flush multixact + xact SLRU pages to disk
    sh.psql_one("CHECKPOINT").unwrap();

    // Fresh cluster, one multi created: it is next_multixact_id - 1
    let next_multi: u32 = sh
        .psql_one("SELECT next_multixact_id FROM pg_control_checkpoint()")
        .unwrap()
        .parse()
        .unwrap();
    let mxid = next_multi - 1;
    let members = sh
        .psql_one(&format!(
            "SELECT string_agg(xid::text || ':' || mode, ',' ORDER BY xid::text::int8) \
             FROM pg_get_multixact_members('{mxid}'::xid)"
        ))
        .unwrap();
    let updater_xid: u32 = members
        .split(',')
        .find_map(|m| m.strip_suffix(":nokeyupd"))
        .unwrap_or_else(|| panic!("no update member in {members:?}"))
        .parse()
        .unwrap();
    assert!(members.contains(":keysh"), "locker member in {members:?}");

    let data_dir = &sh.config().data_dir;
    let mut multi = PgMultiXactAccum::new();
    multi.insert_offsets_segment(
        0,
        std::fs::read(data_dir.join("pg_multixact/offsets/0000")).unwrap(),
    );
    multi.insert_members_segment(
        0,
        std::fs::read(data_dir.join("pg_multixact/members/0000")).unwrap(),
    );
    assert_eq!(multi.updater(mxid), MultiXactUpdater::Updater(updater_xid));
    // Unallocated mxid reads past the written tail: covered by the WAL leg
    assert_eq!(multi.updater(next_multi), MultiXactUpdater::Covered);

    // Committed updater through real pg_xact bytes: old version is dead and
    // its delete may predate WAL coverage — must gate, not resurrect
    let mut xact = PgXactAccum::new();
    xact.insert_segment(0, std::fs::read(data_dir.join("pg_xact/0000")).unwrap());
    let patch = PgXactPatch::new();
    let view = PgXactView::new(&xact, &patch).with_multixact(&multi);
    assert_eq!(
        tuple_visibility(
            100,
            mxid,
            HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI,
            Some(&view)
        ),
        Visibility::Skip
    );
}
