//! Proving a pre-created physical slot against a live source: each shape a
//! slot cannot carry the resume position in is named, not created over

#[path = "common/ports.rs"]
mod ports;
#[path = "common/tools.rs"]
mod tools;

use std::fs;
use std::io::Write as _;
use std::time::Duration;

use walshadow::pos::Pos;
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::source_feed::{SlotError, SourceFeed};

struct StopOnDrop<'a>(&'a Shadow);
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.0.stop();
    }
}

#[tokio::test]
async fn slot_proof_names_each_slot_that_cannot_carry_the_resume() {
    if !tools::pg_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = ports::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    let source = Shadow::new(cfg);
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    let conf = source.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(conf).unwrap();
    writeln!(f, "\nwal_level = logical\nmax_wal_senders = 4").unwrap();
    drop(f);
    source.start().unwrap();
    let _stop = StopOnDrop(&source);
    for slot in [
        "pg_create_logical_replication_slot('decoder', 'pgoutput')",
        "pg_create_physical_replication_slot('lazy')",
        "pg_create_physical_replication_slot('reserved', true)",
    ] {
        source.psql_one(&format!("SELECT 1 FROM {slot}")).unwrap();
    }

    let mut feed = SourceFeed::connect(&ports::pg_cfg(&source, "slot-proof"))
        .await
        .unwrap();
    let mut prove = async |slot: &str, resume: u64| {
        feed.prove_physical_slot(slot, Pos::new(resume), Pos::ZERO)
            .await
    };
    let logical = prove("decoder", u64::MAX).await.unwrap_err();
    assert!(
        matches!(&logical, SlotError::Missing { kind: Some(k), .. } if k == "logical"),
        "{logical}"
    );
    let absent = prove("absent", u64::MAX).await.unwrap_err();
    assert!(matches!(absent, SlotError::Missing { kind: None, .. }));
    assert_eq!(prove("lazy", u64::MAX).await.unwrap(), None);
    let too_new = prove("reserved", 0).await.unwrap_err();
    assert!(matches!(too_new, SlotError::TooNew { resume_lsn: 0, .. }));
    let restart = prove("reserved", u64::MAX).await.unwrap();
    assert!(
        restart.is_some_and(|lsn| lsn > 0),
        "pins above a zero floor"
    );
}
