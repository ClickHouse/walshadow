//! Check TOAST availability for relations added after bootstrap
//! Shadow must already hold TOAST heap, since a running standby cannot be seeded

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use tokio::sync::Mutex;
use walshadow::filter::shadow_relations::ShadowHeld;
use walshadow::schema::RelName;
use walshadow::toast::shadow_landing::{unheld_toast, unserved_rels};

#[tokio::test]
async fn unheld_toast_names_only_rels_shadow_cannot_serve() {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_bridged_source(&tmp);
    let _stop = fx::StopOnDrop { sh: &source };
    source
        .psql_one(
            "CREATE TABLE public.plain (id int PRIMARY KEY, n int); \
             CREATE TABLE public.big (id int PRIMARY KEY, body text)",
        )
        .unwrap();

    let (_bridge, catalog) = fx::connect_catalog(&source, "shadow-toast-admission").await;
    let catalog = Mutex::new(catalog);

    let named = async |name: &str| {
        catalog
            .lock()
            .await
            .descriptor_by_name(&RelName::new("public", name))
            .await
            .unwrap()
            .unwrap()
    };
    let plain = named("plain").await;
    let big = named("big").await;
    let toast = catalog
        .lock()
        .await
        .toast_descriptor_for(big.oid)
        .await
        .unwrap()
        .expect("text column gives public.big a TOAST heap");

    let empty = ShadowHeld::default();
    assert!(
        unheld_toast(&catalog, &empty, &plain)
            .await
            .unwrap()
            .is_none(),
        "a rel with no TOAST heap has no external value to serve",
    );
    let refused = unheld_toast(&catalog, &empty, &big)
        .await
        .unwrap()
        .expect("unheld TOAST heap must name itself");
    assert_eq!(refused.rfn.rel_node, toast.rfn.rel_node);

    let held = ShadowHeld::from_iter([(toast.rfn.db_node, toast.rfn.rel_node)]);
    assert!(
        unheld_toast(&catalog, &held, &big).await.unwrap().is_none(),
        "a landed TOAST heap serves its rel",
    );

    // Startup checks all configured relations in one batch
    let names = [
        RelName::new("public", "plain"),
        RelName::new("public", "big"),
        RelName::new("public", "ghost"),
    ];
    let descs = catalog
        .lock()
        .await
        .descriptors_by_name(&names)
        .await
        .unwrap();
    assert_eq!(descs.len(), 2, "an unknown name drops out of the batch");
    assert_eq!(
        unserved_rels(&catalog, &empty, &descs).await.unwrap(),
        vec![big.rel_name.clone()],
    );
    assert!(
        unserved_rels(&catalog, &held, &descs)
            .await
            .unwrap()
            .is_empty(),
        "a landed TOAST heap serves its rel in the batch too",
    );
}
