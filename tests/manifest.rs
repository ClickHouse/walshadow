//! Manifest surface integration tests.
//!
//! Checks the lib unit tests can't quite express without leaking private
//! types: (1) a [`manifest::Manifest`] survives a crash-simulated
//! `kill -9` (process drops the file handle mid-write) and the boot path
//! still picks the last fully-rename'd copy; (2) corrupt manifest
//! surfaces an error — boot treats it as fatal absent `--ignore-cursor`
//! / `--start-lsn`; (3) freshly initialised `spill_dir` (no manifest)
//! resumes as greenfield.

use walshadow::manifest::{
    self, Lsn, LsnSet, MANIFEST_FILENAME, MANIFEST_VERSION, Manifest, SourceIdentity,
};

fn ident() -> SourceIdentity {
    SourceIdentity {
        system_id: 7_000_000_000_000_000_001,
        timeline: 3,
    }
}

fn sample() -> Manifest {
    Manifest {
        version: MANIFEST_VERSION,
        floor: Lsn(0x05),
        source: ident(),
        lsn: LsnSet {
            source_received: Lsn(0x10),
            filter_durable: Lsn(0x09),
            shadow_replay: Lsn(0x08),
            drain: Lsn(0x07),
            emitter_ack: Lsn(0x06),
            shadow_flush: Lsn(0x05),
        },
    }
}

#[tokio::test(flavor = "current_thread")]
async fn write_survives_simulated_crash_during_tmp_phase() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let good = sample();
    manifest::write(dir, &good).await.unwrap();
    // Simulate a crash during the *next* write: the .tmp gets partly
    // written but the rename never lands. Boot-time load must still
    // return the prior good manifest (rename never touched it).
    let tmp_path = dir.join(format!("{MANIFEST_FILENAME}.tmp"));
    tokio::fs::write(&tmp_path, b"GARBAGE PARTIAL WRITE")
        .await
        .unwrap();
    let got = manifest::load(dir, &ident())
        .await
        .unwrap()
        .expect("manifest still on disk");
    assert_eq!(got, good);
    // The next clean write recovers — and overwrites the bogus .tmp on
    // its way.
    let mut better = good.clone();
    better.lsn.emitter_ack = Lsn(0x100);
    manifest::write(dir, &better).await.unwrap();
    assert!(
        !tmp_path.exists(),
        "successful write clears its .tmp sidecar"
    );
    let got = manifest::load(dir, &ident())
        .await
        .unwrap()
        .expect("post-recovery manifest");
    assert_eq!(got, better);
}

#[tokio::test(flavor = "current_thread")]
async fn corrupt_manifest_surfaces_error_for_fatal_boot() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    tokio::fs::write(dir.join(MANIFEST_FILENAME), "version = [broken")
        .await
        .unwrap();
    let err = manifest::load(dir, &ident())
        .await
        .expect_err("garbage must error");
    assert!(
        matches!(err, manifest::ManifestError::Parse(_)),
        "expected Parse on garbage TOML, got {err:?}",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn greenfield_spill_dir_resumes_with_none() {
    let tmp = tempfile::tempdir().unwrap();
    // Brand-new dir, no manifest. Boot must surface `Ok(None)` so the
    // daemon falls back to greenfield.
    let got = manifest::load(tmp.path(), &ident()).await.unwrap();
    assert!(got.is_none());
}
