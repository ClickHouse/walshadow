//! `ShadowCatalog` end-to-end against a live shadow PG: connect/reconnect,
//! replay gate, name-keyed resolution, and the batched descriptor fetch
//! that feeds descriptor-log capture.
//!
//! Skipped silently if `initdb` is not on `$PATH`. Each test spins up a
//! fresh data directory under a tempdir; tests pick non-overlapping
//! ports so cargo's parallel runner doesn't collide them.

use std::process::Command;
use std::time::Duration;

use walshadow::pg::socket_conninfo;
use walshadow::schema::{RelName, ReplIdent};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::{
    CatalogError, ShadowCatalog, ShadowCatalogConfig, with_transient_retry,
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

fn relation_oid(shadow: &Shadow, qualified: &str) -> u32 {
    shadow
        .psql_one(&format!("SELECT '{qualified}'::regclass::oid::int8"))
        .expect("psql relation oid")
        .parse()
        .expect("oid is integer")
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

fn current_db_oid(shadow: &Shadow) -> u32 {
    shadow
        .psql_one("SELECT oid::int8 FROM pg_database WHERE datname = current_database()")
        .expect("psql db oid")
        .parse()
        .expect("db oid is integer")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_relation_lookup_by_name() {
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

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;
    let desc = cat
        .descriptor_by_name(&RelName::new("wc", "things"))
        .await
        .expect("descriptor_by_name")
        .expect("wc.things exists");
    assert_eq!(&*desc.rel_name.name, "things");
    assert_eq!(&*desc.rel_name.namespace, "wc");
    assert_eq!(desc.kind, 'r');
    assert_eq!(desc.attributes.len(), 3, "{:?}", desc.attributes);
    let id_col = &desc.attributes[0];
    assert_eq!(id_col.name, "id");
    assert_eq!(id_col.type_oid, 20);
    assert!(id_col.not_null);
    let payload_col = &desc.attributes[2];
    assert_eq!(payload_col.name, "payload");
    assert_eq!(payload_col.type_oid, 3802);
    assert!(!payload_col.not_null);

    assert!(
        cat.descriptor_by_name(&RelName::new("wc", "ghost"))
            .await
            .expect("lookup runs")
            .is_none(),
        "unknown rel resolves None (forward-declaration parking)",
    );
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
    let first = cat
        .descriptor_by_name(&RelName::new("pg_catalog", "pg_class"))
        .await
        .expect("pre-restart lookup")
        .expect("pg_class exists");
    assert_eq!(&*first.rel_name.name, "pg_class");
    let reconnects_before = cat.stats().reconnects;

    // pg_ctl-style restart: stop, then start. Server-side close drops
    // the libpq connection; the next SQL call has to reconnect.
    shadow.stop().expect("stop");
    shadow.start().expect("restart");

    let after = cat
        .descriptor_by_name(&RelName::new("pg_catalog", "pg_namespace"))
        .await
        .expect("post-restart lookup")
        .expect("pg_namespace exists");
    assert_eq!(&*after.rel_name.name, "pg_namespace");
    assert!(
        cat.stats().reconnects > reconnects_before,
        "reconnect counter must advance (was {reconnects_before}, now {})",
        cat.stats().reconnects,
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

/// Physical fidelity: dropped columns stay in `attributes` as dropped slots
/// (attnum-1 indexing preserved) with attlen/attalign carried from
/// pg_attribute — the pg_type row link is gone (atttypid = 0). Exercises
/// the name-keyed path and `fetch_descriptors_batch` returning identical
/// shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_column_keeps_physical_slot() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, 55613);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    let _stop = stop_on_drop(&shadow);

    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.evolve (id bigint PRIMARY KEY, gone text, kept int);\n\
             ALTER TABLE wc.evolve DROP COLUMN gone;\n\
             ALTER TABLE wc.evolve ADD COLUMN body text;\n",
        )
        .expect("schema dump");

    let db = current_db_oid(&shadow);
    let oid = relation_oid(&shadow, "wc.evolve");
    let filenode = user_relation_filenode(&shadow, "wc.evolve");
    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;

    let desc = cat
        .descriptor_by_name(&RelName::new("wc", "evolve"))
        .await
        .expect("descriptor_by_name")
        .expect("wc.evolve exists");
    // id(1), dropped slot(2), kept(3), body(4)
    assert_eq!(desc.attributes.len(), 4, "{:?}", desc.attributes);
    let slot = &desc.attributes[1];
    assert_eq!(slot.attnum, 2);
    assert!(slot.dropped);
    assert_eq!(slot.type_oid, 0, "DROP COLUMN zeroes atttypid");
    assert_eq!(slot.type_name, "");
    // text physical layout survives the drop: varlena, int-aligned, extended
    assert_eq!(slot.type_len, -1);
    assert_eq!(slot.type_align, 'i');
    assert_eq!(slot.type_storage, 'x');
    assert_eq!(desc.attributes[2].attnum, 3);
    assert!(!desc.attributes[2].dropped);
    assert_ne!(desc.toast_oid, 0, "text column gives wc.evolve a toast rel");

    let (replay_lsn, batch) = cat
        .fetch_descriptors_batch(&[oid, 99_999_999])
        .await
        .expect("fetch_descriptors_batch");
    // Not a standby here: pg_last_wal_replay_lsn() is NULL → 0
    assert_eq!(replay_lsn, 0);
    assert_eq!(batch.len(), 1, "absent oid must be absent, not error");
    let b = &batch[0];
    assert_eq!(b.oid, oid);
    assert_eq!(b.rfn.db_node, db);
    assert_eq!(b.rfn.rel_node, filenode);
    // reltablespace = 0 sentinel resolves to dattablespace (pg_default
    // here), matching WAL locators' physical spcOid
    assert_eq!(b.rfn.spc_node, 1663);
    assert_eq!(b.toast_oid, desc.toast_oid);
    assert_eq!(b.replident, desc.replident);
    assert_eq!(b.attributes, desc.attributes);
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

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;

    let cases = [
        (
            "def_t",
            ReplIdent::Default {
                pk_attnums: Some(vec![1]),
            },
        ),
        ("no_pk_t", ReplIdent::Default { pk_attnums: None }),
        (
            "composite_pk_t",
            ReplIdent::Default {
                pk_attnums: Some(vec![1, 2]),
            },
        ),
        ("nothing_t", ReplIdent::Nothing),
        ("full_t", ReplIdent::Full { pk_attnums: None }),
    ];
    for (name, expected) in cases {
        let desc = cat
            .descriptor_by_name(&RelName::new("wc", name))
            .await
            .unwrap_or_else(|e| panic!("descriptor_by_name wc.{name}: {e}"))
            .unwrap_or_else(|| panic!("wc.{name} missing"));
        assert_eq!(
            desc.replident, expected,
            "wc.{name}: expected {expected:?}, got {:?}",
            desc.replident,
        );
    }

    let desc_idx = cat
        .descriptor_by_name(&RelName::new("wc", "idx_t"))
        .await
        .expect("descriptor_by_name idx_t")
        .expect("idx_t exists");
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

/// `fetch_all_descriptors` (capture-all + boot seed) covers every eligible
/// user rel incl toast, skips indexes/views/sequences.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_all_descriptors_covers_eligible_kinds() {
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

    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.doc (id bigint PRIMARY KEY, body text);\n\
             CREATE VIEW wc.v AS SELECT id FROM wc.doc;\n\
             CREATE SEQUENCE wc.seq;\n",
        )
        .expect("schema dump");

    let mut cat = open_catalog(&shadow, Duration::from_secs(5)).await;
    let (_, descs) = cat
        .fetch_all_descriptors()
        .await
        .expect("fetch_all_descriptors");
    let doc = descs
        .iter()
        .find(|d| &*d.rel_name.name == "doc")
        .expect("wc.doc present");
    assert_eq!(doc.kind, 'r');
    assert_ne!(doc.toast_oid, 0);
    assert!(
        descs
            .iter()
            .any(|d| d.oid == doc.toast_oid && d.kind == 't'),
        "owner's toast rel captured alongside it",
    );
    assert!(
        descs
            .iter()
            .all(|d| matches!(d.kind, 'r' | 'p' | 'm' | 't')),
        "views/sequences/indexes excluded: {:?}",
        descs
            .iter()
            .map(|d| (d.rel_name.name.clone(), d.kind))
            .collect::<Vec<_>>(),
    );
}
