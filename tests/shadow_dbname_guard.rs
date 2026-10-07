//! A shadow data dir provisioned for another source database must name itself.
//!
//! The resume path adopts any dir holding `PG_VERSION`, and the shadow follows
//! the applied `[source] dbname`, so a dir built for a different database (or
//! by an older `--shadow-dbname`) is reused verbatim. Before this guard the
//! first symptom was `psql: FATAL: database "..." does not exist` from the
//! replay wait, then a missing bridge socket, restarting forever.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use walshadow::shadow::Shadow;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shadow_without_the_applied_database_is_named_not_probed_to_death() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir: _,
        },
        _stream,
    ) = fx::bootstrap_clusters(&tmp, "", slot.source, slot.shadow, slot.walsender).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    assert!(
        shadow
            .has_database()
            .expect("probe the shadow's database list"),
        "the shadow holds the database it was provisioned for",
    );

    let mut foreign = shadow.config().clone();
    foreign.dbname = "a_database_the_shadow_never_had".to_owned();
    let foreign = Shadow::new(foreign);

    // Cluster-level probes read the same postmaster, so they must not depend
    // on the source's database being present
    assert!(
        foreign.is_in_recovery().is_ok(),
        "recovery probe must not need the source database",
    );
    assert!(
        foreign.last_replay_lsn().is_ok(),
        "replay probe must not need the source database",
    );

    assert!(
        !foreign
            .has_database()
            .expect("probe a database the shadow lacks"),
        "a database the shadow never had reads as absent",
    );
}
