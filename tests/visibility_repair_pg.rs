//! Verify visibility repair against live PostgreSQL, including descriptor
//! drift failures

#![cfg(target_os = "linux")]

#[path = "common/ports.rs"]
mod fx;

use ahash::{HashSet, HashSetExt};
use std::fs;
use std::io::Write as _;
use std::process::Command;
use std::time::Duration;

use tokio::sync::mpsc;
use walshadow::backfill_bootstrap::seed_catalog_from_source;
use walshadow::backup_page_walk::{BackfillTuple, CatalogMap};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::source_feed::open_sql_client;
use walshadow::visibility_repair::{PendingReason, PendingSet, repair};

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

async fn drain(mut rx: mpsc::Receiver<BackfillTuple>) -> Vec<BackfillTuple> {
    let mut out = Vec::new();
    while let Some(t) = rx.recv().await {
        out.push(t);
    }
    out
}

/// Read TOAST-owning relation whole through PostgreSQL
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toast_capable_relation_is_read_whole_at_the_coverage_lsn() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let src = start_source(
        "CREATE TABLE public.t (id int4 PRIMARY KEY, body text NOT NULL) \
           WITH (autovacuum_enabled = false);\n\
         ALTER TABLE public.t ALTER COLUMN body SET STORAGE EXTERNAL;\n\
         INSERT INTO public.t SELECT g, repeat('body-'||g::text||'---', 700) \
           FROM generate_series(1, 6) g;\n\
         DELETE FROM public.t WHERE id > 4;\n\
         CREATE TABLE public.fixed (id int4 PRIMARY KEY, n int8 NOT NULL);\n\
         INSERT INTO public.fixed SELECT g, g FROM generate_series(1, 3) g;\n",
    );
    let catalog = seed(&src.sh).await;
    let pending = PendingSet::toast_capable(&catalog);
    // `fixed` has no varlena column, so PostgreSQL gave it no toast relation
    assert_eq!(
        pending.len(),
        1,
        "only the text-bearing relation is pending"
    );
    assert_eq!(pending.count_for(PendingReason::ExternalToast), 1);

    let (tx, rx) = mpsc::channel(64);
    let collect = tokio::spawn(drain(rx));
    let stats = repair(
        &pending,
        &catalog,
        &HashSet::new(),
        &fx::pg_cfg(&src.sh, APP),
        S,
        &tx,
    )
    .await
    .expect("repair");
    drop(tx);
    let rows = collect.await.unwrap();

    assert_eq!(stats.relations, 1);
    assert_eq!(stats.rows, 4, "the deleted versions are not visible");
    assert_eq!(rows.len(), 4);
    // Baseline uses coverage LSN
    assert!(rows.iter().all(|r| r.source_lsn == S));
    assert!(
        stats.p_hi > S,
        "p_hi is a frontier, sampled after the reads"
    );
    // Bodies came back detoasted, so nothing here consulted a chunk mirror
    let body = rows[0].columns[1].as_ref().expect("body column");
    assert!(
        format!("{body:?}").contains("body-"),
        "external value arrived inline: {body:?}"
    );
}

/// Skip relations excluded from initial load
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opted_out_relation_is_skipped_not_scanned() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let src = start_source(
        "CREATE TABLE public.t (id int4 PRIMARY KEY, body text NOT NULL);\n\
         INSERT INTO public.t VALUES (1, 'one');\n",
    );
    let catalog = seed(&src.sh).await;
    let pending = PendingSet::toast_capable(&catalog);
    let skip: HashSet<_> = catalog.descriptors().map(|d| d.rel_name.clone()).collect();

    let (tx, rx) = mpsc::channel(4);
    let collect = tokio::spawn(drain(rx));
    let stats = repair(&pending, &catalog, &skip, &fx::pg_cfg(&src.sh, APP), S, &tx)
        .await
        .expect("repair");
    drop(tx);

    assert_eq!(stats.skipped, 1);
    assert_eq!(stats.relations, 0);
    assert_eq!(stats.rows, 0);
    assert!(collect.await.unwrap().is_empty());
}

/// Seed the catalog off a baseline relation, mutate the source, then require
/// repair to reject the drift naming every needle in `expected`
async fn assert_repair_rejects(change: &str, expected: &[&str]) {
    let src = start_source(
        "CREATE TABLE public.t (id int4 PRIMARY KEY, body text NOT NULL);\n\
         INSERT INTO public.t SELECT g, 'row-'||g::text FROM generate_series(1, 4) g;\n",
    );
    let catalog = seed(&src.sh).await;
    let pending = PendingSet::toast_capable(&catalog);
    src.sh.apply_schema_dump(change).expect("source change");

    let (tx, _rx) = mpsc::channel(4);
    let err = repair(
        &pending,
        &catalog,
        &HashSet::new(),
        &fx::pg_cfg(&src.sh, APP),
        S,
        &tx,
    )
    .await
    .expect_err("drifted relation is not repairable");
    let msg = format!("{err:#}");
    for needle in expected {
        assert!(msg.contains(needle), "want {needle:?} in {msg}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewritten_relation_fails_the_pass() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    // Rewrite rotates filenode
    assert_repair_rejects(
        "VACUUM FULL public.t;\n",
        &["rewritten inside the backup window"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_relation_fails_the_pass() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    assert_repair_rejects("DROP TABLE public.t;\n", &["gone from the source"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn added_column_fails_the_pass() {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    // No rewrite, so the filenode still matches; the shape does not. The
    // report names the new column
    assert_repair_rejects(
        "ALTER TABLE public.t ADD COLUMN extra int4;\n",
        &["changed inside the backup window", "extra"],
    )
    .await;
}
