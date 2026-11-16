#![cfg(target_os = "linux")]
//! Bootstrap catalog seed reads each replica identity shape off the source

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use walshadow::backfill_bootstrap::seed_catalog_from_source;
use walshadow::schema::{RelName, ReplIdent};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_reads_replica_identity_index_and_nothing() {
    if !fx::tools::requirements_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _stop = fx::StopOnDrop { sh: &source };
    source
        .psql_one(
            "CREATE TABLE public.ui (id int NOT NULL, k int NOT NULL); \
             CREATE UNIQUE INDEX ui_k ON public.ui (k); \
             ALTER TABLE public.ui REPLICA IDENTITY USING INDEX ui_k; \
             CREATE TABLE public.nothing (id int); \
             ALTER TABLE public.nothing REPLICA IDENTITY NOTHING",
        )
        .unwrap();
    let index_oid: u32 = source
        .psql_one("SELECT 'public.ui_k'::regclass::oid")
        .unwrap()
        .parse()
        .unwrap();
    let cfg = source.config();
    let conninfo = walshadow::pg::socket_conninfo(
        cfg.socket_dir.to_str().unwrap(),
        cfg.port,
        "postgres",
        "postgres",
    );
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    let catalog = seed_catalog_from_source(&client).await.unwrap();
    let replident = |name| {
        catalog
            .descriptors()
            .find(|d| d.rel_name == RelName::new("public", name))
            .unwrap()
            .replident
            .clone()
    };
    assert_eq!(
        replident("ui"),
        ReplIdent::UsingIndex {
            index_oid,
            key_attnums: vec![2],
        }
    );
    assert_eq!(replident("nothing"), ReplIdent::Nothing);
}
