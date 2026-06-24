//! Integration test against captured WAL fixture.
//!
//! Classifier fixture is captured by `fixtures/wal/classify/capture.sh`
//! into `fixtures/wal/classify/segments/`. The bytes are not checked
//! in (see .gitignore there); regenerate locally to run this test.
//!
//! Skipped silently if no captured segment is present. Local runs
//! after `capture.sh` get the catalog-fraction-bound assertion the
//! classifier targets.

use std::fs::File;
use std::path::PathBuf;
use std::process::Command;

use walrus::pg::walparser::{WAL_PAGE_SIZE, WalParser};
use walshadow::classify::Summary;

#[path = "common/segment.rs"]
mod segment;
use segment::load_segment;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/wal/classify/segments")
}

fn walk(bytes: &[u8]) -> anyhow::Result<Summary> {
    let mut parser = WalParser::new();
    let mut summary = Summary::default();
    for chunk in bytes.chunks(WAL_PAGE_SIZE as usize) {
        let (_, records) = parser
            .parse_records_from_page(chunk)
            .map_err(|e| anyhow::anyhow!("parse: {e}"))?;
        for r in &records {
            summary.observe(r);
        }
        if chunk.len() < WAL_PAGE_SIZE as usize {
            break;
        }
    }
    Ok(summary)
}

#[tokio::test]
async fn catalog_fraction_under_workload_is_bounded() {
    let dir = fixture_dir();
    let seg = dir.join("000000010000000000000001.gz");
    if !seg.exists() {
        eprintln!(
            "skip: no captured segment at {:?}. Run capture.sh to regenerate.",
            seg
        );
        return;
    }
    let bytes = load_segment(&seg).await.expect("load fixture");
    let summary = walk(&bytes).expect("walk fixture");

    let writer = File::create(dir.parent().unwrap().join("last_run.json")).unwrap();
    serde_json::to_writer_pretty(writer, &summary).unwrap();
    eprintln!(
        "fixture: {} records, {} bytes, catalog {:.4}%",
        summary.records,
        summary.bytes,
        100.0 * summary.catalog_fraction()
    );

    // Sanity bound. Capture workload is intentionally DDL-heavy in a
    // small window so catalog fraction sits around 85–95% on a real
    // capture (validated against docker:14, docker:18, and local PG 18).
    // The < 100% check just confirms the classifier isn't bucketing
    // every record as catalog. A steady-state OLTP capture re-tightens
    // this toward "well under 1%"
    assert!(
        summary.records >= 100,
        "fixture too small: {} records",
        summary.records
    );
    assert!(
        summary.catalog_fraction() < 0.99,
        "catalog fraction {:.4} — classifier may be bucketing everything as catalog",
        summary.catalog_fraction()
    );
    assert!(
        summary.by_class.get("user").map(|c| c.records).unwrap_or(0) > 0,
        "no user-class records — workload produced no non-catalog heap"
    );

    // Sanity: every expected rmgr is observed in some quantity.
    for needed in ["heap", "btree", "xact"] {
        assert!(
            summary.by_rmgr.contains_key(needed),
            "no records for rmgr {needed} in fixture"
        );
    }
}

#[test]
fn cli_produces_json_for_fixture() {
    let dir = fixture_dir();
    let seg = dir.join("000000010000000000000001.gz");
    if !seg.exists() {
        eprintln!("skip: no captured segment at {:?}", seg);
        return;
    }

    let exe = env!("CARGO_BIN_EXE_walshadow-classify");
    let out = Command::new(exe).arg("--json").arg(&seg).output().unwrap();
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json output");
    assert!(parsed["records"].as_u64().unwrap() > 0);
}

#[test]
fn cli_produces_human_summary_for_fixture() {
    let dir = fixture_dir();
    let seg = dir.join("000000010000000000000001.gz");
    if !seg.exists() {
        eprintln!("skip: no captured segment at {:?}", seg);
        return;
    }
    let exe = env!("CARGO_BIN_EXE_walshadow-classify");
    let out = Command::new(exe).arg(&seg).output().unwrap();
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("records:"), "got {stdout:?}");
    assert!(stdout.contains("catalog fraction:"), "got {stdout:?}");
    assert!(stdout.contains("class"), "got {stdout:?}");
}
