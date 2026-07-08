//! `ShadowCatalog` end-to-end against a live shadow PG.
//!
//! Skipped silently if `initdb` is not on `$PATH`. Each test spins up a
//! fresh data directory under a tempdir; tests pick non-overlapping
//! ports so cargo's parallel runner doesn't collide them.

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::RelName;
use walshadow::shadow_catalog::{
    CatalogError, ReplIdent, SchemaEvent, ShadowCatalog, ShadowCatalogConfig, socket_conninfo,
    with_transient_retry,
};

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_shadow(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
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
    ShadowCatalog::connect(&conninfo, cat_cfg)
        .await
        .expect("catalog connect")
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
        .psql_one(&format!(
            "SELECT pg_relation_filenode('{qualified}'::regclass)::int8"
        ))
        .expect("psql user filenode")
        .parse()
        .expect("filenode is integer")
}

fn relation_oid(shadow: &Shadow, qualified: &str) -> u32 {
    shadow
        .psql_one(&format!("SELECT '{qualified}'::regclass::oid::int8"))
        .expect("psql relation oid")
        .parse()
        .expect("oid is integer")
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

    let rfn = walrus::pg::walparser::RelFileNode {
        spc_node: pg_global_tablespace_oid(),
        db_node: db,
        rel_node: pg_class_filenode,
    };
    let desc = cat.relation_at(rfn, 0).await.expect("relation_at pg_class");
    assert_eq!(&*desc.rel_name.name, "pg_class");
    assert_eq!(&*desc.rel_name.namespace, "pg_catalog");
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
    let rfn = walrus::pg::walparser::RelFileNode {
        spc_node: 1663,
        db_node: db,
        rel_node: filenode,
    };

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;
    let desc = cat.relation_at(rfn, 0).await.expect("relation_at things");
    assert_eq!(&*desc.rel_name.name, "things");
    assert_eq!(&*desc.rel_name.namespace, "wc");
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
    assert_eq!(
        cat.stats().misses,
        first_misses,
        "second lookup should not miss"
    );

    // Generation bump → forced refetch.
    let gen_before = cat.generation();
    cat.invalidate();
    assert_eq!(cat.generation(), gen_before + 1);
    let fetches_before = cat.stats().fetches;
    let again = cat
        .relation_at(rfn, 0)
        .await
        .expect("relation_at after invalidate");
    assert_eq!(&*again.rel_name.name, "things");
    assert_eq!(
        cat.stats().fetches,
        fetches_before + 1,
        "invalidate must force a re-fetch on next access",
    );

    // by-oid path round-trips back to the same descriptor.
    let by_oid = cat
        .relation_by_oid(desc.oid)
        .await
        .expect("relation_by_oid");
    assert_eq!(&*by_oid.rel_name.name, "things");
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
async fn catalog_reconnects_after_pg_restart() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55605);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    let mut cat = open_catalog(&shadow, Duration::from_secs(10)).await;

    let pg_class_filenode = pg_class_filenode_via_psql(&shadow);
    let pg_namespace_filenode = user_relation_filenode(&shadow, "pg_namespace");
    let db = current_db_oid(&shadow);
    let rfn_class = walrus::pg::walparser::RelFileNode {
        spc_node: pg_global_tablespace_oid(),
        db_node: db,
        rel_node: pg_class_filenode,
    };
    let rfn_namespace = walrus::pg::walparser::RelFileNode {
        spc_node: pg_global_tablespace_oid(),
        db_node: db,
        rel_node: pg_namespace_filenode,
    };
    let first = cat
        .relation_at(rfn_class, 0)
        .await
        .expect("relation_at pre-restart");
    assert_eq!(&*first.rel_name.name, "pg_class");
    let gen_before = cat.generation();
    let reconnects_before = cat.stats().reconnects;
    let bumps_before = cat.stats().generation_bumps;

    // pg_ctl-style restart: stop, then start. Server-side close drops
    // the libpq connection; the next SQL call has to reconnect. Use a
    // different rfn post-restart so the cache miss forces a fetch
    // (same rfn would hit cache and never touch the dead connection).
    shadow.stop().expect("stop");
    shadow.start().expect("restart");

    let after = cat
        .relation_at(rfn_namespace, 0)
        .await
        .expect("relation_at post-restart");
    assert_eq!(&*after.rel_name.name, "pg_namespace");
    assert!(
        cat.generation() > gen_before,
        "reconnect must bump generation (was {gen_before}, now {})",
        cat.generation(),
    );
    assert!(
        cat.stats().reconnects > reconnects_before,
        "reconnect counter must advance (was {reconnects_before}, now {})",
        cat.stats().reconnects,
    );
    assert!(
        cat.stats().generation_bumps > bumps_before,
        "reconnect bumps cache generation alongside the reconnect counter",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_transient_retry_outlasts_a_pg_restart() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55606);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    let cfg = shadow.config();
    let conninfo = socket_conninfo(
        cfg.socket_dir.to_str().unwrap(),
        cfg.port,
        "postgres",
        "postgres",
    );

    // Stop PG so the first connect attempts fail; restart in a background
    // task after a short delay. with_transient_retry must keep retrying
    // until PG is back.
    shadow.stop().expect("stop");
    let shadow_path = shadow.config().data_dir.clone();
    let pg_bin = shadow.config().pg_bin_dir.clone();
    let ctl_secs = shadow.config().ctl_timeout.as_secs().to_string();
    let log_path = shadow_path.join("startup.log");
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        let mut cmd = std::process::Command::new(match pg_bin {
            Some(d) => d.join("pg_ctl"),
            None => std::path::PathBuf::from("pg_ctl"),
        });
        cmd.args([
            "-D",
            shadow_path.to_str().unwrap(),
            "-l",
            log_path.to_str().unwrap(),
            "-w",
            "-t",
            &ctl_secs,
            "start",
        ]);
        let _ = cmd.output();
    });

    let cat = with_transient_retry(
        Duration::from_secs(15),
        Duration::from_millis(50),
        Duration::from_millis(500),
        async move || {
            ShadowCatalog::connect(
                &conninfo,
                ShadowCatalogConfig {
                    replay_timeout: Duration::from_secs(5),
                    replay_poll: Duration::from_millis(20),
                    ..Default::default()
                },
            )
            .await
        },
    )
    .await
    .expect("eventually connects through with_transient_retry");
    drop(cat);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_signal_drives_invalidate_and_refetches_after_ddl() {
    // Production-path verification of the shared
    // `invalidation_epoch` AtomicU64: an upstream bump triggers an
    // inline `invalidate` at the top of `relation_at`. Closes the
    // race-prone mpsc-drain wire it replaces.
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55607);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.things (\n\
                id   bigint PRIMARY KEY,\n\
                name text NOT NULL\n\
             );\n",
        )
        .expect("schema dump");

    let filenode = user_relation_filenode(&shadow, "wc.things");
    let db = current_db_oid(&shadow);
    let rfn = walrus::pg::walparser::RelFileNode {
        spc_node: 1663,
        db_node: db,
        rel_node: filenode,
    };

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;
    let epoch = Arc::new(AtomicU64::new(0));
    cat.set_invalidation_epoch(epoch.clone());

    // Prime the cache so the post-DDL re-fetch is what surfaces the
    // new column (the bug being fixed: without invalidate, the cached
    // descriptor would keep masking ADD COLUMN).
    let desc = cat.relation_at(rfn, 0).await.expect("prime");
    assert_eq!(desc.attributes.len(), 2);
    assert!(desc.attributes.iter().all(|a| a.name != "extra"));

    // DDL through psql so the live shadow's catalog actually changes.
    shadow
        .apply_schema_dump("ALTER TABLE wc.things ADD COLUMN extra text;")
        .expect("alter table");

    let bumps_before = cat.stats().generation_bumps;
    // Production tracker bumps the epoch on observed pg_class writes.
    // Simulate one bump and call relation_at: the catalog must observe
    // the delta in `drain_invalidations` and invalidate before the
    // cache check.
    epoch.fetch_add(1, Ordering::Release);

    let fresh = cat.relation_at(rfn, 0).await.expect("relation_at post-DDL");
    assert_eq!(
        cat.stats().generation_bumps,
        bumps_before + 1,
        "epoch bump must trigger one invalidate on next lookup",
    );
    assert_eq!(
        fresh.attributes.len(),
        3,
        "post-DDL fetch must surface ADD COLUMN; got {:?}",
        fresh.attributes.iter().map(|a| &a.name).collect::<Vec<_>>(),
    );
    assert!(
        fresh.attributes.iter().any(|a| a.name == "extra"),
        "added column must appear in attributes",
    );
}

/// Baseline seed makes a pinned relation's first post-start ALTER emit
/// `Changed`, not `Added`. The executable form of the invariant in
/// `plans/future/pinned_ddl_baseline.md`: cache warmth must not decide
/// the schema event. `seeded` is warmed by `seed_baseline` before
/// `subscribe()`; `cold` is not. Both get the same ADD COLUMN; the
/// seeded table surfaces a `Changed` (→ CH ALTER), the cold table a
/// stale `Added` (→ apply_added skips a pinned dest — the bug).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_baseline_makes_first_alter_emit_changed_not_added() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55610);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.seeded (id bigint PRIMARY KEY, name text);\n\
             CREATE TABLE wc.cold   (id bigint PRIMARY KEY, name text);\n",
        )
        .expect("schema dump");

    let seeded_oid = relation_oid(&shadow, "wc.seeded");
    let cold_oid = relation_oid(&shadow, "wc.cold");

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;
    let epoch = Arc::new(AtomicU64::new(0));
    cat.set_invalidation_epoch(epoch.clone());

    // Warm prev_known for `wc.seeded` only. Pre-subscribe, so no event
    // leaks; `wc.cold` stays cold.
    let seeded = cat
        .seed_baseline(&[RelName::new("wc", "seeded")])
        .await
        .expect("seed_baseline");
    assert_eq!(seeded, 1, "exactly one mapped relation seeded");

    let mut rx = cat.subscribe();
    assert!(
        rx.try_recv().is_err(),
        "seed ran before subscribe — no event must have leaked",
    );

    // Same DDL on both tables.
    shadow
        .apply_schema_dump(
            "ALTER TABLE wc.seeded ADD COLUMN extra text;\n\
             ALTER TABLE wc.cold   ADD COLUMN extra text;\n",
        )
        .expect("alter both");
    // Production tracker bumps the epoch on observed pg_class writes;
    // simulate one so the next lookup invalidates and refetches.
    epoch.fetch_add(1, Ordering::Release);

    // Seeded relation: refetch diffs the evolved shape against the warm
    // baseline → Changed carrying the added column.
    let _ = cat
        .relation_by_oid(seeded_oid)
        .await
        .expect("refetch seeded");
    match rx.try_recv().expect("seeded must emit one event") {
        SchemaEvent::Changed { diff, .. } => {
            assert!(
                diff.added_columns.iter().any(|a| a.name == "extra"),
                "Changed diff must carry the added column; got {diff:?}",
            );
        }
        other => panic!("seeded: expected Changed, got {other:?}"),
    }
    assert!(
        rx.try_recv().is_err(),
        "seeded relation must emit exactly one event",
    );

    // Cold relation: first-ever fetch carries the post-ALTER shape and
    // prev_known is empty → Added. This is the pre-fix behaviour the
    // seed eliminates for pinned tables.
    let _ = cat.relation_by_oid(cold_oid).await.expect("fetch cold");
    match rx.try_recv().expect("cold must emit one event") {
        SchemaEvent::Added { desc } => {
            assert!(
                desc.attributes.iter().any(|a| a.name == "extra"),
                "cold Added carries the already-evolved shape",
            );
        }
        other => panic!("cold: expected Added, got {other:?}"),
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
    let bogus = walrus::pg::walparser::RelFileNode {
        spc_node: 1663,
        db_node: current_db_oid(&shadow),
        rel_node: 99_999_999,
    };
    let err = cat.relation_at(bogus, 0).await.expect_err("bogus filenode");
    matches!(err, CatalogError::NotFoundByFilenode(_));
}

/// Replica-identity carriage: `RelDescriptor::replident` carries the resolved
/// `pg_class.relreplident` and, for `USING INDEX`, the index oid plus
/// `pg_index.indkey` attnum list. Heap decoder reads both off the
/// descriptor to interpret `XLH_UPDATE_CONTAINS_OLD_KEY` payloads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replident_matrix_default_nothing_full_index() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55609);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    // Six tables exercising relreplident matrix. `def_t` has a single-
    // column PK; `no_pk_t` has no PK so Default carries None; `composite_pk_t`
    // exercises multi-column PK indkey lift; `nothing_t` switches to NOTHING;
    // `full_t` to FULL; `idx_t` to USING INDEX on a two-column unique NOT
    // NULL index — REPLICA IDENTITY USING INDEX rejects anything less.
    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.def_t (id bigint PRIMARY KEY, name text);\n\
             CREATE TABLE wc.no_pk_t (a int, b int);\n\
             CREATE TABLE wc.composite_pk_t (\n\
                k1 int,\n\
                k2 int,\n\
                v text,\n\
                PRIMARY KEY (k1, k2)\n\
             );\n\
             CREATE TABLE wc.nothing_t (id bigint, name text);\n\
             ALTER TABLE wc.nothing_t REPLICA IDENTITY NOTHING;\n\
             CREATE TABLE wc.full_t (id bigint, name text);\n\
             ALTER TABLE wc.full_t REPLICA IDENTITY FULL;\n\
             CREATE TABLE wc.idx_t (\n\
                id bigint,\n\
                k1 int NOT NULL,\n\
                k2 int NOT NULL,\n\
                name text\n\
             );\n\
             CREATE UNIQUE INDEX idx_t_keys ON wc.idx_t (k1, k2);\n\
             ALTER TABLE wc.idx_t REPLICA IDENTITY USING INDEX idx_t_keys;\n",
        )
        .expect("schema dump");

    let db = current_db_oid(&shadow);
    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;

    let cases = [
        (
            "wc.def_t",
            ReplIdent::Default {
                pk_attnums: Some(vec![1]),
            },
        ),
        ("wc.no_pk_t", ReplIdent::Default { pk_attnums: None }),
        (
            "wc.composite_pk_t",
            ReplIdent::Default {
                pk_attnums: Some(vec![1, 2]),
            },
        ),
        ("wc.nothing_t", ReplIdent::Nothing),
        ("wc.full_t", ReplIdent::Full { pk_attnums: None }),
    ];
    for (qualified, expected) in cases {
        let rfn = walrus::pg::walparser::RelFileNode {
            spc_node: 1663,
            db_node: db,
            rel_node: user_relation_filenode(&shadow, qualified),
        };
        let desc = cat
            .relation_at(rfn, 0)
            .await
            .unwrap_or_else(|e| panic!("relation_at {qualified}: {e}"));
        assert_eq!(
            desc.replident, expected,
            "{qualified}: expected {expected:?}, got {:?}",
            desc.replident,
        );
    }

    let rfn_idx = walrus::pg::walparser::RelFileNode {
        spc_node: 1663,
        db_node: db,
        rel_node: user_relation_filenode(&shadow, "wc.idx_t"),
    };
    let desc_idx = cat
        .relation_at(rfn_idx, 0)
        .await
        .expect("relation_at idx_t");
    let (index_oid, key_attnums) = match desc_idx.replident.clone() {
        ReplIdent::UsingIndex {
            index_oid,
            key_attnums,
        } => (index_oid, key_attnums),
        other => panic!("idx_t: expected UsingIndex, got {other:?}"),
    };
    let expected_index_oid: u32 = shadow
        .psql_one("SELECT 'wc.idx_t_keys'::regclass::oid::int8")
        .expect("psql idx oid")
        .parse()
        .expect("idx oid integer");
    assert_eq!(index_oid, expected_index_oid);
    // k1, k2 are attnum 2 and 3 on idx_t (id=1, k1=2, k2=3, name=4).
    assert_eq!(
        key_attnums,
        vec![2i16, 3],
        "USING INDEX must surface pg_index.indkey verbatim",
    );
}

/// Arc-mutex sanity check: with the catalog wrapped in
/// `Arc<tokio::sync::Mutex<_>>` at the daemon level, two tasks holding
/// clones of the same `Arc` serialise cleanly across `relation_at`.
/// Validates the wrap shape `BufferingDecoderSink` relies on, not the
/// lock-free hit path (that lands when the spec'd `&self` refactor
/// follows up).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arc_mutex_catalog_serialises_relation_at_across_tasks() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55608);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.things (\n\
                id   bigint PRIMARY KEY,\n\
                name text NOT NULL\n\
             );\n",
        )
        .expect("schema dump");

    let pg_class_filenode = pg_class_filenode_via_psql(&shadow);
    let things_filenode = user_relation_filenode(&shadow, "wc.things");
    let db = current_db_oid(&shadow);
    let rfn_class = walrus::pg::walparser::RelFileNode {
        spc_node: pg_global_tablespace_oid(),
        db_node: db,
        rel_node: pg_class_filenode,
    };
    let rfn_things = walrus::pg::walparser::RelFileNode {
        spc_node: 1663,
        db_node: db,
        rel_node: things_filenode,
    };

    let cat = open_catalog(&shadow, Duration::from_secs(5)).await;
    let cat = Arc::new(tokio::sync::Mutex::new(cat));

    // Task A acquires the lock and holds it across an await on a small
    // sleep, so task B starts its lookup against a held mutex. The
    // tokio::sync::Mutex is fair-ish and async — task B must wait for
    // A to drop the guard, then succeed. Anything else (panic, hang,
    // "would deadlock") fails the test.
    let cat_a = cat.clone();
    let cat_b = cat.clone();
    let task_a = tokio::spawn(async move {
        let mut guard = cat_a.lock().await;
        let desc = guard
            .relation_at(rfn_class, 0)
            .await
            .expect("relation_at pg_class from task A");
        // Hold the guard across an await so task B has to wait.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let again = guard
            .relation_at(rfn_class, 0)
            .await
            .expect("relation_at pg_class second-call from task A");
        assert_eq!(&*desc.rel_name.name, "pg_class");
        assert_eq!(&*again.rel_name.name, "pg_class");
        desc.oid
    });
    let task_b = tokio::spawn(async move {
        // Yield once so task A wins the lock first.
        tokio::task::yield_now().await;
        let mut guard = cat_b.lock().await;
        let desc = guard
            .relation_at(rfn_things, 0)
            .await
            .expect("relation_at wc.things from task B");
        assert_eq!(&*desc.rel_name.name, "things");
        desc.oid
    });
    let started = Instant::now();
    let (oid_a, oid_b) = tokio::join!(task_a, task_b);
    let elapsed = started.elapsed();
    let oid_a = oid_a.expect("task A finished");
    let oid_b = oid_b.expect("task B finished");
    assert_ne!(oid_a, 0);
    assert_ne!(oid_b, 0);
    assert_ne!(
        oid_a, oid_b,
        "pg_class and wc.things must have different oids"
    );
    // Bound the wall clock to a generous ceiling so a hang surfaces.
    assert!(
        elapsed < Duration::from_secs(5),
        "cross-task relation_at took too long: {elapsed:?}",
    );

    // Final state: both descriptors cached, no surprise reconnects.
    let guard = cat.lock().await;
    assert!(guard.cached() >= 2, "cached={}", guard.cached());
    assert_eq!(
        guard.stats().reconnects,
        0,
        "no reconnect should fire on a steady-state shadow",
    );
}
