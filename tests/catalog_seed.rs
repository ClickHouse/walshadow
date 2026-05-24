//! `CatalogTracker::seed_from_source` against a live PG.
//!
//! Drives a temp cluster through a `VACUUM FULL pg_class` (rotates the
//! mapped catalog's filenode above 16384) and confirms that
//! `seed_from_source` picks the new filenode up before any WAL streams
//! into the tracker.
//!
//! Skipped silently when `initdb` is not on `$PATH`.
//!
//! Uses the `Shadow` lifecycle helpers as a generic PG cluster wrapper
//! — this stands in for the upstream "source PG", not shadow PG. The
//! same binary serves both roles.

use std::process::Command;
use std::time::Duration;

use tokio_postgres::NoTls;
use walshadow::catalog_tracker::{CatalogTracker, PG_CLASS_OID};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::socket_conninfo;

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

fn pg_class_filenode_via_psql(sh: &Shadow) -> u32 {
    sh.psql_one("SELECT pg_relation_filenode('pg_class'::regclass)::int8")
        .expect("filenode")
        .parse()
        .expect("integer")
}

fn pg_namespace_filenode_via_psql(sh: &Shadow) -> u32 {
    sh.psql_one("SELECT pg_relation_filenode('pg_namespace'::regclass)::int8")
        .expect("filenode")
        .parse()
        .expect("integer")
}

fn current_db_oid(sh: &Shadow) -> u32 {
    sh.psql_one("SELECT oid::int8 FROM pg_database WHERE datname = current_database()")
        .expect("db oid")
        .parse()
        .expect("integer")
}

async fn connect(sh: &Shadow) -> tokio_postgres::Client {
    let cfg = sh.config();
    let conninfo = socket_conninfo(
        cfg.socket_dir.to_str().unwrap(),
        cfg.port,
        "postgres",
        "postgres",
    );
    let (client, conn) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_picks_up_initial_mapped_catalog_filenodes() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_cluster(&tmp, 55701);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };

    let pg_class_fn = pg_class_filenode_via_psql(&sh);
    let pg_namespace_fn = pg_namespace_filenode_via_psql(&sh);
    let db = current_db_oid(&sh);

    let client = connect(&sh).await;
    let mut tracker = CatalogTracker::new();
    let added = tracker
        .seed_from_source(&client)
        .await
        .expect("seed_from_source");

    assert!(added > 0, "expected some catalog rows to seed");
    assert!(
        tracker.is_catalog(db, pg_class_fn),
        "pg_class filenode {} (db {}) must be catalog after seed",
        pg_class_fn,
        db,
    );
    assert!(
        tracker.is_catalog(db, pg_namespace_fn),
        "pg_namespace filenode {} (db {}) must be catalog after seed",
        pg_namespace_fn,
        db,
    );
    // pg_database is shared — seeded under db_node = 0, visible from
    // any db_node via the global fall-through.
    let pg_database_fn: u32 = sh
        .psql_one("SELECT pg_relation_filenode('pg_database'::regclass)::int8")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        tracker.is_catalog(0, pg_database_fn),
        "pg_database shared filenode {} must be catalog under db_node=0",
        pg_database_fn,
    );
    assert!(
        tracker.is_catalog(99, pg_database_fn),
        "pg_database must remain visible from any db_node via global lookup",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_closes_pre_attach_pg_class_rotation_hole() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_cluster(&tmp, 55702);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };

    let pg_class_fn_before = pg_class_filenode_via_psql(&sh);
    // Rotate pg_class until its mapped filenode crosses FirstNormalObjectId
    // (16384). A single `VACUUM FULL pg_class` on a fresh cluster can leave
    // the new filenode below 16384, which silently neuters the assertions
    // below (the bootstrap rule already catches < 16384, so seeding has
    // nothing extra to prove). Loop until rotation exposes the hole the
    // seed exists to close.
    let mut iters = 0;
    loop {
        sh.psql_one("VACUUM FULL pg_class")
            .expect("vacuum full pg_class");
        let fn_now = pg_class_filenode_via_psql(&sh);
        if fn_now >= 16384 {
            break;
        }
        iters += 1;
        assert!(
            iters < 200,
            "pg_class filenode stayed below 16384 after {iters} VACUUM FULL passes",
        );
    }
    let pg_class_fn_after = pg_class_filenode_via_psql(&sh);
    assert_ne!(
        pg_class_fn_before, pg_class_fn_after,
        "VACUUM FULL pg_class should rotate the filenode",
    );
    assert!(
        pg_class_fn_after >= 16384,
        "test invariant: post-rotation filenode {} must be >= 16384 \
         so the bootstrap rule actually misses it",
        pg_class_fn_after,
    );

    let db = current_db_oid(&sh);

    // Tracker with no live WAL — only the bootstrap rule (< 16384) applies.
    // Now that we forced pg_class_fn_after >= 16384, the rule misses it
    // unconditionally.
    let unseeded = CatalogTracker::new();
    assert!(
        !unseeded.is_catalog(db, pg_class_fn_after),
        "without seed_from_source the rotated pg_class filenode {} must NOT be catalog",
        pg_class_fn_after,
    );

    // After seed, post-rotation filenode is in the catalog set.
    let client = connect(&sh).await;
    let mut tracker = CatalogTracker::new();
    tracker.seed_from_source(&client).await.expect("seed");
    assert!(
        tracker.is_catalog(db, pg_class_fn_after),
        "seed_from_source must add pg_class's current filenode {} to the catalog set",
        pg_class_fn_after,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_skips_user_tables() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let sh = make_cluster(&tmp, 55703);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("conf");
    sh.start().expect("start");
    let _stop = StopOnDrop { sh: &sh };

    // User table — pg_class.oid >= 16384.
    sh.apply_schema_dump(
        "CREATE TABLE user_t (id int primary key, name text);\n\
         INSERT INTO user_t VALUES (1, 'one');\n",
    )
    .expect("create user_t");
    let user_t_fn: u32 = sh
        .psql_one("SELECT pg_relation_filenode('user_t'::regclass)::int8")
        .unwrap()
        .parse()
        .unwrap();

    let db = current_db_oid(&sh);
    let client = connect(&sh).await;
    let mut tracker = CatalogTracker::new();
    tracker.seed_from_source(&client).await.expect("seed");
    assert!(
        !tracker.is_catalog(db, user_t_fn),
        "user_t filenode {} must NOT be catalog after seed",
        user_t_fn,
    );
    // PG_CLASS_OID exposure — touch it so it doesn't drift unused.
    let _ = PG_CLASS_OID;
}
