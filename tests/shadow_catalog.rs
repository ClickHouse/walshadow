//! Phase 4: `ShadowCatalog` end-to-end against a live shadow PG.
//!
//! Skipped silently if `initdb` is not on `$PATH`. Each test spins up a
//! fresh data directory under a tempdir; tests pick non-overlapping
//! ports so cargo's parallel runner doesn't collide them.

use std::process::Command;
use std::time::Duration;

use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::{
    ShadowCatalog, ShadowCatalogConfig, socket_conninfo, CatalogError,
};

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_shadow(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("data"),
        tmp.path().join("filtered"),
    );
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(30);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

struct StopOnDrop<'a> {
    shadow: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.shadow.stop();
    }
}

fn stop_on_drop(shadow: &Shadow) -> StopOnDrop<'_> {
    StopOnDrop { shadow }
}

async fn open_catalog(shadow: &Shadow, replay_timeout: Duration) -> ShadowCatalog {
    let cfg = shadow.config();
    let conninfo = socket_conninfo(
        cfg.socket_dir.to_str().unwrap(),
        cfg.port,
        "postgres",
        "postgres",
    );
    let cat_cfg = ShadowCatalogConfig {
        replay_timeout,
        replay_poll: Duration::from_millis(20),
        ..Default::default()
    };
    ShadowCatalog::connect(&conninfo, cat_cfg).await.expect("catalog connect")
}

fn pg_class_filenode_via_psql(shadow: &Shadow) -> u32 {
    shadow
        .psql_one("SELECT pg_relation_filenode('pg_class'::regclass)::int8")
        .expect("psql pg_class filenode")
        .parse()
        .expect("filenode is integer")
}

fn user_relation_filenode(shadow: &Shadow, qualified: &str) -> u32 {
    shadow
        .psql_one(&format!("SELECT pg_relation_filenode('{qualified}'::regclass)::int8"))
        .expect("psql user filenode")
        .parse()
        .expect("filenode is integer")
}

fn current_db_oid(shadow: &Shadow) -> u32 {
    shadow
        .psql_one("SELECT oid::int8 FROM pg_database WHERE datname = current_database()")
        .expect("psql db oid")
        .parse()
        .expect("db oid is integer")
}

fn pg_global_tablespace_oid() -> u32 {
    // pg_global is always oid 1664.
    1664
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_relation_lookup_by_filenode() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55601);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    let pg_class_filenode = pg_class_filenode_via_psql(&shadow);
    let db = current_db_oid(&shadow);

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;

    let rfn = wal_rs::pg::walparser::RelFileNode {
        spc_node: pg_global_tablespace_oid(),
        db_node: db,
        rel_node: pg_class_filenode,
    };
    let desc = cat.relation_at(rfn, 0).await.expect("relation_at pg_class");
    assert_eq!(desc.name, "pg_class");
    assert_eq!(desc.namespace_name, "pg_catalog");
    assert_eq!(desc.kind, 'r');
    assert_eq!(desc.persistence, 'p');
    assert!(
        desc.attributes.iter().any(|a| a.name == "relname"),
        "pg_class must have relname column; got {:?}",
        desc.attributes.iter().map(|a| &a.name).collect::<Vec<_>>(),
    );
    assert!(
        desc.attributes.iter().any(|a| a.name == "oid"),
        "pg_class must expose oid column",
    );
    let nspname_oid_col = desc
        .attributes
        .iter()
        .find(|a| a.name == "relnamespace")
        .expect("relnamespace col");
    // oid type oid is 26.
    assert_eq!(nspname_oid_col.type_oid, 26);
    assert!(nspname_oid_col.not_null);

    // Second lookup must come from cache.
    let before = cat.stats().clone();
    let _again = cat.relation_at(rfn, 0).await.expect("relation_at cached");
    let after = cat.stats().clone();
    assert_eq!(after.hits, before.hits + 1, "second lookup should be a hit");
    assert_eq!(after.fetches, before.fetches, "no extra fetch on hit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_relation_lookup_and_invalidation() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55602);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.things (\n\
                id   bigint PRIMARY KEY,\n\
                name text NOT NULL,\n\
                payload jsonb\n\
             );\n",
        )
        .expect("apply schema dump");

    let filenode = user_relation_filenode(&shadow, "wc.things");
    let db = current_db_oid(&shadow);
    // Default user tablespace is pg_default (oid 1663).
    let rfn = wal_rs::pg::walparser::RelFileNode {
        spc_node: 1663,
        db_node: db,
        rel_node: filenode,
    };

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;
    let desc = cat.relation_at(rfn, 0).await.expect("relation_at things");
    assert_eq!(desc.name, "things");
    assert_eq!(desc.namespace_name, "wc");
    assert_eq!(desc.kind, 'r');
    // id, name, payload — three user columns (pg ≥ 12 dropped system cols
    // from attnum >= 1 visibility).
    assert_eq!(desc.attributes.len(), 3, "{:?}", desc.attributes);
    let id_col = &desc.attributes[0];
    assert_eq!(id_col.name, "id");
    // int8 type oid = 20
    assert_eq!(id_col.type_oid, 20);
    assert!(id_col.not_null);
    let name_col = &desc.attributes[1];
    assert_eq!(name_col.name, "name");
    // text type oid = 25
    assert_eq!(name_col.type_oid, 25);
    assert!(name_col.not_null);
    let payload_col = &desc.attributes[2];
    assert_eq!(payload_col.name, "payload");
    // jsonb type oid = 3802
    assert_eq!(payload_col.type_oid, 3802);
    assert!(!payload_col.not_null);

    // Cache hit on repeat lookup.
    let first_misses = cat.stats().misses;
    let _ = cat.relation_at(rfn, 0).await.unwrap();
    assert_eq!(cat.stats().misses, first_misses, "second lookup should not miss");

    // Generation bump → forced refetch.
    let gen_before = cat.generation();
    cat.invalidate();
    assert_eq!(cat.generation(), gen_before + 1);
    let fetches_before = cat.stats().fetches;
    let again = cat.relation_at(rfn, 0).await.expect("relation_at after invalidate");
    assert_eq!(again.name, "things");
    assert_eq!(
        cat.stats().fetches,
        fetches_before + 1,
        "invalidate must force a re-fetch on next access",
    );

    // by-oid path round-trips back to the same descriptor.
    let by_oid = cat.relation_by_oid(desc.oid).await.expect("relation_by_oid");
    assert_eq!(by_oid.name, "things");
    assert_eq!(by_oid.rfn.rel_node, filenode);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_lsn_gate_times_out_when_not_in_recovery() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55603);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    let mut cat = open_catalog(&shadow, Duration::from_millis(300)).await;
    // pg_last_wal_replay_lsn() is NULL on a non-standby cluster. The
    // gate must time out cleanly rather than spin or crash.
    let err = cat
        .wait_for_replay(0x0100_0000)
        .await
        .expect_err("non-recovering cluster: expected ReplayTimeout");
    match err {
        CatalogError::ReplayTimeout { target, last, .. } => {
            assert_eq!(target, 0x0100_0000);
            assert!(last.is_none());
        }
        other => panic!("expected ReplayTimeout, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonexistent_filenode_errors_not_found() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55604);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    let mut cat = open_catalog(&shadow, Duration::from_secs(2)).await;
    let bogus = wal_rs::pg::walparser::RelFileNode {
        spc_node: 1663,
        db_node: current_db_oid(&shadow),
        rel_node: 99_999_999,
    };
    let err = cat.relation_at(bogus, 0).await.expect_err("bogus filenode");
    matches!(err, CatalogError::NotFoundByFilenode(_));
}
