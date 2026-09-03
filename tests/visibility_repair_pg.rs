//! Verify visibility repair against live PostgreSQL, including descriptor
//! drift failures

#![cfg(target_os = "linux")]

#[path = "common/ports.rs"]
mod fx;

use std::fs;
use std::io::Write as _;
use std::process::Command;
use std::time::Duration;

use tokio::sync::mpsc;
use walshadow::backfill_bootstrap::seed_catalog_from_source;
use walshadow::backup_page_walk::{BackfillTuple, CatalogMap};
use walshadow::copy_backfill::CopyRate;
use walshadow::heap_decoder::ColumnValue;
use walshadow::mapping::{TableMapping, TableTarget};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::source_feed::open_sql_client;
use walshadow::visibility_repair::{RepairBatch, RepairScope, RowRepair};

/// Coverage boundary every baseline row carries
const S: u64 = 0x0100_0000;
const APP: &str = "visibility-repair-test";

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

struct Source {
    _tmp: tempfile::TempDir,
    sh: Shadow,
}

impl Drop for Source {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

fn start_source(sql: &str) -> Source {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = fx::reserve_port();
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    let sh = Shadow::new(cfg);
    sh.initdb().expect("initdb");
    sh.write_base_conf().expect("base conf");
    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(sh.config().data_dir.join("postgresql.conf"))
        .unwrap();
    writeln!(f, "\nwal_level = replica").unwrap();
    drop(f);
    sh.start().expect("start");
    sh.apply_schema_dump(sql).expect("workload");
    Source { _tmp: tmp, sh }
}

async fn seed(sh: &Shadow) -> CatalogMap {
    let client = open_sql_client(&fx::pg_cfg(sh, APP))
        .await
        .expect("sql connect");
    seed_catalog_from_source(&client).await.expect("seed")
}

fn scope(catalog: &CatalogMap, name: &str) -> RepairScope {
    let d = catalog
        .descriptors()
        .find(|d| &*d.rel_name.name == name)
        .unwrap();
    let mapping = [(
        d.rel_name.clone(),
        TableMapping {
            target: TableTarget::new("default", name),
            columns: Vec::new(),
        },
    )]
    .into_iter()
    .collect::<ahash::HashMap<_, _>>()
    .into();
    RepairScope::mapped(catalog, &mapping, |_| true)
}

async fn requests(sh: &Shadow, catalog: &CatalogMap, name: &str) -> Vec<BackfillTuple> {
    let d = catalog
        .descriptors()
        .find(|d| &*d.rel_name.name == name)
        .unwrap();
    let client = open_sql_client(&fx::pg_cfg(sh, APP)).await.unwrap();
    client
        .query(
            &format!(
                "SELECT ctid::text FROM ONLY {}.{} ORDER BY ctid",
                walshadow::pg::quote_ident(&d.rel_name.namespace),
                walshadow::pg::quote_ident(name)
            ),
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            let tid: String = r.get(0);
            let (blk, off) = tid.trim_matches(['(', ')']).split_once(',').unwrap();
            BackfillTuple {
                rfn: d.rfn,
                xid: 0,
                xmax: 0,
                infomask: 0,
                source_lsn: S,
                blkno: blk.parse().unwrap(),
                offnum: off.parse().unwrap(),
                columns: Vec::new(),
            }
        })
        .collect()
}

async fn drain(mut rx: mpsc::Receiver<Vec<BackfillTuple>>) -> Vec<BackfillTuple> {
    let mut out = Vec::new();
    while let Some(slab) = rx.recv().await {
        out.extend(slab);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolved_rows_keep_their_physical_relation() {
    if !pg_available() {
        return;
    }
    let src = start_source(
        "CREATE TABLE parent (id int PRIMARY KEY, body text);
         CREATE TABLE child () INHERITS (parent);
         INSERT INTO parent VALUES (1, 'parent'), (2, 'deleted'), (3, 'unrequested');
         INSERT INTO child VALUES (4, 'child');",
    );
    let catalog = seed(&src.sh).await;
    let rows = requests(&src.sh, &catalog, "parent").await;
    src.sh
        .apply_schema_dump("DELETE FROM ONLY parent WHERE id = 2")
        .unwrap();
    let scope = scope(&catalog, "parent");
    let (tx, rx) = mpsc::channel(1);
    tx.send(RepairBatch {
        rows: rows[..2].to_vec(),
        unresolved: true,
    })
    .await
    .unwrap();
    drop(tx);
    let (out_tx, out_rx) = mpsc::channel(1);
    let collect = tokio::spawn(drain(out_rx));
    let stats = RowRepair {
        source: fx::pg_cfg(&src.sh, APP),
        catalog,
        scope,
        rate: CopyRate::new(None),
    }
    .run(rx, out_tx)
    .await
    .unwrap();
    let rows = collect.await.unwrap();
    assert_eq!(stats.rows, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source_lsn, S);
    assert!(matches!(rows[0].columns[0], Some(ColumnValue::Int4(1))));
    assert!(stats.p_hi > S);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_security_cannot_silently_filter_repair() {
    if !pg_available() {
        return;
    }
    let src = start_source(
        "CREATE TABLE t (id int PRIMARY KEY, body text);
         INSERT INTO t VALUES (1, 'visible'), (2, 'hidden');
         CREATE ROLE repl LOGIN REPLICATION;
         GRANT SELECT ON t TO repl;
         ALTER TABLE t ENABLE ROW LEVEL SECURITY;
         CREATE POLICY visible ON t FOR SELECT TO repl USING (id=1);",
    );
    let catalog = seed(&src.sh).await;
    let rows = requests(&src.sh, &catalog, "t").await;
    let scope = scope(&catalog, "t");
    let mut source = fx::pg_cfg(&src.sh, APP);
    source.user = "repl".into();
    let (tx, rx) = mpsc::channel(1);
    tx.send(RepairBatch {
        rows,
        unresolved: true,
    })
    .await
    .unwrap();
    drop(tx);
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let err = RowRepair {
        source,
        catalog,
        scope,
        rate: CopyRate::new(None),
    }
    .run(rx, out_tx)
    .await
    .unwrap_err();
    assert!(format!("{err:#}").contains("row-level security"), "{err:#}");
    assert!(out_rx.recv().await.is_none());
}

async fn assert_repair_rejects_after_read(change: &str, expected: &str) {
    let src = start_source(
        "CREATE TABLE t (id int PRIMARY KEY, body text); INSERT INTO t VALUES (1, 'a')",
    );
    let catalog = seed(&src.sh).await;
    let rows = requests(&src.sh, &catalog, "t").await;
    let scope = scope(&catalog, "t");
    let (tx, rx) = mpsc::channel(1);
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let worker = tokio::spawn(
        RowRepair {
            source: fx::pg_cfg(&src.sh, APP),
            catalog,
            scope,
            rate: CopyRate::new(None),
        }
        .run(rx, out_tx),
    );
    tx.send(RepairBatch {
        rows: rows.clone(),
        unresolved: true,
    })
    .await
    .unwrap();
    assert_eq!(out_rx.recv().await.unwrap().len(), 1);
    src.sh.apply_schema_dump(change).unwrap();
    tx.send(RepairBatch {
        rows,
        unresolved: true,
    })
    .await
    .unwrap();
    drop(tx);
    let err = worker.await.unwrap().unwrap_err();
    assert!(format!("{err:#}").contains(expected), "{err:#}");
    assert!(out_rx.recv().await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewritten_relation_after_first_read_fails() {
    if !pg_available() {
        return;
    }
    assert_repair_rejects_after_read("VACUUM FULL t", "rewritten inside the backup window").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replaced_relation_after_first_read_fails() {
    if !pg_available() {
        return;
    }
    assert_repair_rejects_after_read("DROP TABLE t; CREATE TABLE t (id int PRIMARY KEY, body text); INSERT INTO t VALUES (99, 'replacement')", "gone from the source").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn added_column_after_first_read_fails() {
    if !pg_available() {
        return;
    }
    assert_repair_rejects_after_read(
        "ALTER TABLE t ADD COLUMN extra int",
        "changed inside the backup window",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_holds_relation_lock_through_copy() {
    if !pg_available() {
        return;
    }
    let src = start_source(
        "CREATE TABLE t (id int PRIMARY KEY, body text); INSERT INTO t SELECT g, repeat('x', 700000) FROM generate_series(1, 6) g",
    );
    let catalog = seed(&src.sh).await;
    let rows = requests(&src.sh, &catalog, "t").await;
    let scope = scope(&catalog, "t");
    let (tx, rx) = mpsc::channel(1);
    tx.send(RepairBatch {
        rows,
        unresolved: true,
    })
    .await
    .unwrap();
    drop(tx);
    let (out_tx, out_rx) = mpsc::channel(1);
    let worker = tokio::spawn(
        RowRepair {
            source: fx::pg_cfg(&src.sh, APP),
            catalog,
            scope,
            rate: CopyRate::new(None),
        }
        .run(rx, out_tx),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while out_rx.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let client = open_sql_client(&fx::pg_cfg(&src.sh, "repair-ddl-test"))
        .await
        .unwrap();
    client
        .batch_execute("SET lock_timeout = '100ms'")
        .await
        .unwrap();
    let err = client
        .batch_execute("ALTER TABLE t ADD COLUMN extra int")
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE)
    );
    let rows = drain(out_rx).await;
    assert_eq!(worker.await.unwrap().unwrap().rows, 6);
    assert_eq!(rows.len(), 6);
    client
        .batch_execute("ALTER TABLE t ADD COLUMN extra int")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_repair_streams_selected_versions_before_input_closes() {
    if !pg_available() {
        return;
    }
    use walshadow::heap_decoder::{ColumnValue, ToastPointer};
    use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};

    let src = start_source(
        "CREATE TABLE t (id int PRIMARY KEY, body text) WITH (autovacuum_enabled = false);
         ALTER TABLE t ALTER COLUMN body SET STORAGE EXTERNAL;
         INSERT INTO t SELECT g, repeat('body-' || g::text, 2000) FROM generate_series(1, 5) g;
         CREATE TABLE child () INHERITS (t);
         INSERT INTO child SELECT g, 'child' FROM generate_series(1, 5) g;",
    );
    let catalog = seed(&src.sh).await;
    let desc = catalog
        .descriptors()
        .find(|d| &*d.rel_name.name == "t")
        .unwrap()
        .clone();
    let pg = fx::pg_cfg(&src.sh, APP);
    let client = open_sql_client(&pg).await.unwrap();
    let tids = client
        .query(
            "SELECT ctid::text FROM ONLY t WHERE id BETWEEN 2 AND 4 ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    let tuples = tids
        .iter()
        .map(|row| {
            let tid: String = row.get(0);
            let (blk, off) = tid.trim_matches(['(', ')']).split_once(',').unwrap();
            BackfillTuple {
                rfn: desc.rfn,
                xid: 0,
                xmax: 0,
                infomask: 0,
                source_lsn: S,
                blkno: blk.parse().unwrap(),
                offnum: off.parse().unwrap(),
                columns: vec![
                    Some(ColumnValue::Int4(0)),
                    Some(ColumnValue::ExternalToast(ToastPointer {
                        va_rawsize: 12004,
                        va_extinfo: 12000,
                        va_valueid: 1,
                        va_toastrelid: desc.toast_oid,
                    })),
                ],
            }
        })
        .collect();
    client
        .batch_execute("UPDATE ONLY t SET id = 30 WHERE id = 3; DELETE FROM ONLY t WHERE id = 4;")
        .await
        .unwrap();
    let mapping = [(
        desc.rel_name.clone(),
        TableMapping {
            target: TableTarget::new("default", "t"),
            columns: vec![ColumnMapping {
                src_attnum: 2,
                target_name: "body".into(),
                target_type: "String".into(),
            }],
        },
    )]
    .into_iter()
    .collect::<ahash::HashMap<_, _>>()
    .into();
    let scope = RepairScope::mapped(&catalog, &mapping, |_| true);
    let (tx, rx) = mpsc::channel(1);
    let (out_tx, mut out_rx) = mpsc::channel(1);
    let worker = tokio::spawn(
        RowRepair {
            source: pg,
            catalog,
            scope,
            rate: CopyRate::new(None),
        }
        .run(rx, out_tx),
    );
    tx.send(RepairBatch {
        rows: tuples,
        unresolved: false,
    })
    .await
    .unwrap();
    let rows = tokio::time::timeout(Duration::from_secs(10), out_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exclude moved, deleted, inherited and unrequested rows"
    );
    assert!(matches!(rows[0].columns[0], Some(ColumnValue::Int4(2))));
    assert!(
        matches!(&rows[0].columns[1], Some(ColumnValue::Text(body)) if body == &"body-2".repeat(2000))
    );
    assert_eq!(rows[0].source_lsn, S);
    assert!(!worker.is_finished(), "emit before backup EOF");
    drop(tx);
    assert!(out_rx.recv().await.is_none());
    let stats = worker.await.unwrap().unwrap();
    assert_eq!(stats.rows, 1);
    assert!(stats.p_hi > S);
}
