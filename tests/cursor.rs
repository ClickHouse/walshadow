//! Cursor surface integration tests.
//!
//! Two checks the lib unit tests can't quite express without leaking
//! private types: (1) a [`cursor::Cursor`] survives a crash-simulated
//! `kill -9` (process drops the file handle mid-write) and the boot
//! path still picks the last fully-rename'd copy; (2) the cursor
//! resume gate at `cursor.bin` boundary handles a freshly-initialised
//! `spill_dir` (no cursor present yet — greenfield boot) without
//! erroring.

use walshadow::cursor::{self, CURSOR_FILE_LEN, CURSOR_FILENAME, Cursor};

#[tokio::test(flavor = "current_thread")]
async fn write_survives_simulated_crash_during_tmp_phase() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    // Write a real cursor.
    let good = Cursor {
        source_received_lsn: 0x10,
        filter_durable_lsn: 0x09,
        shadow_replay_lsn: 0x08,
        drain_lsn: 0x07,
        emitter_ack_lsn: 0x06,
        shadow_flush_lsn: 0x05,
    };
    cursor::write(dir, &good).await.unwrap();
    // Now simulate a crash during the *next* write: the .tmp gets
    // partly written but the rename never lands. Drop a half-written
    // `.tmp` on disk and verify the boot-time `cursor::read` still
    // returns the prior good cursor (rename never touched cursor.bin).
    let tmp_path = dir.join(format!("{CURSOR_FILENAME}.tmp"));
    tokio::fs::write(&tmp_path, b"GARBAGE PARTIAL WRITE")
        .await
        .unwrap();
    let got = cursor::read(dir)
        .await
        .unwrap()
        .expect("cursor still on disk");
    assert_eq!(got, good);
    // The next clean write recovers — and overwrites the bogus .tmp on
    // its way.
    let better = Cursor {
        emitter_ack_lsn: 0x100,
        ..good
    };
    cursor::write(dir, &better).await.unwrap();
    assert!(
        !tmp_path.exists(),
        "successful write clears its .tmp sidecar"
    );
    let got = cursor::read(dir)
        .await
        .unwrap()
        .expect("post-recovery cursor");
    assert_eq!(got, better);
}

#[tokio::test(flavor = "current_thread")]
async fn corrupt_cursor_surfaces_error_for_boot_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    // Plant a same-sized garbage file under the canonical name.
    let path = dir.join(CURSOR_FILENAME);
    tokio::fs::write(&path, vec![0xAAu8; CURSOR_FILE_LEN])
        .await
        .unwrap();
    let err = cursor::read(dir).await.expect_err("garbage must error");
    assert!(
        matches!(err, cursor::CursorError::BadMagic),
        "expected BadMagic on all-0xAA payload, got {err:?}",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn greenfield_spill_dir_resumes_with_none() {
    let tmp = tempfile::tempdir().unwrap();
    // Brand-new dir, no cursor file. boot path's `cursor::read` must
    // surface `Ok(None)` so the daemon falls back to greenfield.
    let got = cursor::read(tmp.path()).await.unwrap();
    assert!(got.is_none());
}
