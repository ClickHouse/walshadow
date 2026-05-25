//! Round-trip: capture → filter → re-parse with WalParser.
//!
//! Skipped silently if no captured segment is present. `capture.sh`
//! regenerates the fixture; see `fixtures/wal/classify/capture.sh`.
//!
//! Assertions:
//! 1. Filtered segment is the same length as the source (byte-preserving).
//! 2. Every record re-parses through wal-rs's `WalParser` without error.
//! 3. Manifest record count equals source record count.
//! 4. Filter dropped >0 user records on a non-DDL-heavy workload.
//! 5. All `Decision::Drop` records show as `XLOG_NOOP` (rmid=0, info=0x20)
//!    in the filtered output.

use std::path::PathBuf;
use std::process::Command;

use tokio::io::AsyncReadExt;
use wal_rs::pg::wal::segment_file::open_segment_file;
use wal_rs::pg::walparser::{WAL_PAGE_SIZE, WalParser};
use walshadow::filter::Filter;
use walshadow::filter_segment::filter_segment;

fn fixture_segment() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/wal/classify/segments/000000010000000000000001.gz")
}

fn oltp_segment() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/wal/filter/segments/000000010000000000000002.gz")
}

fn vacuum_full_pg_depend_segment() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/wal/vacuum_full_pg_depend/segments/000000010000000000000002.gz")
}

fn xlog_switch_segment() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/wal/xlog_switch/segments/000000010000000000000002.gz")
}

async fn load_segment(path: &PathBuf) -> anyhow::Result<Vec<u8>> {
    let (_seg, mut r) = open_segment_file(path).await?;
    let mut out = Vec::new();
    r.read_to_end(&mut out).await?;
    Ok(out)
}

/// (total_record_count, noop_count) by walking through `WalParser`.
fn parse_all_records(bytes: &[u8]) -> anyhow::Result<(usize, usize)> {
    use wal_rs::pg::walparser::RmId;
    let mut parser = WalParser::new();
    let mut total = 0;
    let mut noops = 0;
    for chunk in bytes.chunks(WAL_PAGE_SIZE as usize) {
        let (_, records) = parser
            .parse_records_from_page(chunk)
            .map_err(|e| anyhow::anyhow!("parse: {e}"))?;
        for r in &records {
            total += 1;
            if r.header.resource_manager_id == RmId::Xlog as u8
                && (r.header.info & 0xF0) == walshadow::rewrite::XLOG_NOOP
            {
                noops += 1;
            }
        }
        if chunk.len() < WAL_PAGE_SIZE as usize {
            break;
        }
    }
    Ok((total, noops))
}

#[tokio::test]
async fn filtered_segment_round_trips_through_wal_parser() {
    let seg = fixture_segment();
    if !seg.exists() {
        eprintln!("skip: no captured segment at {:?}", seg);
        return;
    }
    let bytes = load_segment(&seg).await.expect("load fixture");
    let mut filter = Filter::new();
    let (out, manifest, parsed) = filter_segment(&bytes, "fixture", &mut filter).expect("filter");
    assert_eq!(
        parsed.len(),
        manifest.records.len(),
        "parsed records must align 1:1 with manifest entries",
    );

    // (1) Byte-preserving
    assert_eq!(
        out.len(),
        bytes.len(),
        "filtered segment length must match source"
    );

    // (2) Re-parses cleanly. (3) Record count matches manifest.
    let (filtered_count, noops) = parse_all_records(&out).expect("re-parse filtered segment");

    eprintln!(
        "fixture: source {} bytes, {} records (kept {}, dropped {}, undecoded-pg_class {})",
        bytes.len(),
        manifest.records.len(),
        manifest.stats.kept,
        manifest.stats.dropped,
        manifest.stats.pg_class_writes_undecoded,
    );
    assert_eq!(
        filtered_count as u64, manifest.stats.records,
        "WalParser record count != manifest record count"
    );

    // (4) Filter dropped >0 user records on this fixture.
    assert!(
        manifest.stats.dropped > 0,
        "filter dropped zero records — bug in classifier?"
    );

    // (5) Number of NOOP records in filtered stream == manifest.dropped.
    assert_eq!(
        noops as u64, manifest.stats.dropped,
        "noop count in filtered stream does not match manifest.dropped"
    );

    // Source segment was also parseable (it's a real PG capture).
    let (source_count, _) = parse_all_records(&bytes).expect("re-parse source segment");
    assert_eq!(source_count as u64, manifest.stats.records);
}

/// Acceptance §1: a non-DDL workload's filtered output should keep ≪ 1%
/// of records. `fixtures/wal/filter/capture.sh` runs CREATE TABLE +
/// INSERT in segment 1, then `pg_switch_wal()`, then heavy DML —
/// segment 2 is the OLTP-only slice this test exercises.
#[tokio::test]
async fn oltp_workload_keeps_well_under_one_percent() {
    let seg = oltp_segment();
    if !seg.exists() {
        eprintln!(
            "skip: no OLTP fixture at {:?}. Run fixtures/wal/filter/capture.sh",
            seg
        );
        return;
    }
    let bytes = load_segment(&seg).await.expect("load oltp fixture");
    let mut filter = Filter::new();
    let (out, manifest, _parsed) =
        filter_segment(&bytes, "oltp", &mut filter).expect("filter oltp");

    let kept_frac = manifest.stats.kept as f64 / manifest.stats.records as f64;
    eprintln!(
        "OLTP fixture: {} records, kept {} ({:.4}%), dropped {} ({:.4}%)",
        manifest.stats.records,
        manifest.stats.kept,
        kept_frac * 100.0,
        manifest.stats.dropped,
        100.0 - kept_frac * 100.0,
    );

    assert!(
        manifest.stats.records > 1000,
        "OLTP fixture too small: {} records",
        manifest.stats.records
    );
    assert!(
        kept_frac < 0.01,
        "kept fraction {:.4} exceeds 1% — acceptance §1 violated",
        kept_frac
    );

    // Re-parse filtered output.
    let (filtered_count, noops) = parse_all_records(&out).expect("re-parse filtered");
    assert_eq!(filtered_count as u64, manifest.stats.records);
    assert_eq!(noops as u64, manifest.stats.dropped);
}

/// Captured-fixture cross-check for the synthetic
/// `xlog_switch_record_passes_through_filter` unit test in
/// `src/filter_segment.rs`. A real PG `pg_switch_wal()` lands an
/// XLOG_SWITCH (rmgr 0, info 0x40) in the WAL segment; the filter
/// must keep it byte-identically because shadow's recovery state
/// machine relies on its presence at the segment tail.
#[tokio::test]
async fn xlog_switch_fixture_keeps_switch_record_bytes_intact() {
    use wal_rs::pg::walparser::RmId;
    const XLOG_SWITCH: u8 = 0x40;
    let seg = xlog_switch_segment();
    if !seg.exists() {
        eprintln!(
            "skip: no fixture at {:?}. Run fixtures/wal/xlog_switch/capture.sh",
            seg
        );
        return;
    }
    let bytes = load_segment(&seg).await.expect("load xlog_switch fixture");
    let mut filter = Filter::new();
    let (out, manifest, _parsed) = filter_segment(&bytes, "xsw", &mut filter).expect("filter");

    let switch_entry = manifest
        .records
        .iter()
        .find(|e| e.rmid == RmId::Xlog as u8 && (e.info & 0xF0) == XLOG_SWITCH)
        .expect("captured segment must contain ≥1 XLOG_SWITCH");
    let off = switch_entry.offset as usize;
    let len = switch_entry.len as usize;
    assert_eq!(
        &bytes[off..off + len],
        &out[off..off + len],
        "XLOG_SWITCH bytes must pass through unchanged",
    );
    assert_eq!(
        switch_entry.kind,
        walshadow::manifest::Kind::Kept,
        "XLOG_SWITCH must be kept (special rmgr policy)",
    );
}

/// Prefix-compression regression: pg_class UPDATE records from `VACUUM FULL` on a
/// non-mapped catalog are prefix-compressed past the OID column. The
/// decoder must signal `pg_class_writes_oid_in_prefix` and must NOT
/// tick `pg_class_writes_undecoded` (which would mean the WAL was
/// genuinely malformed, masking the prefix-compression hole).
#[tokio::test]
async fn vacuum_full_pg_depend_ticks_oid_in_prefix_not_undecoded() {
    let seg = vacuum_full_pg_depend_segment();
    if !seg.exists() {
        eprintln!(
            "skip: no fixture at {:?}. Run fixtures/wal/vacuum_full_pg_depend/capture.sh",
            seg
        );
        return;
    }
    let bytes = load_segment(&seg).await.expect("load vacuum-full fixture");
    let mut filter = Filter::new();
    let (_out, manifest, _parsed) = filter_segment(&bytes, "vac", &mut filter).expect("filter");

    let tracker = &filter.tracker;
    eprintln!(
        "VACUUM FULL pg_depend fixture: {} records, oid_in_prefix={}, undecoded={}, decoded={}",
        manifest.stats.records,
        tracker.pg_class_writes_oid_in_prefix,
        tracker.pg_class_writes_undecoded,
        tracker.pg_class_writes_decoded,
    );
    assert!(
        tracker.pg_class_writes_oid_in_prefix > 0,
        "VACUUM FULL pg_<non-mapped> must produce ≥1 oid-in-prefix pg_class write; got 0",
    );
    assert_eq!(
        tracker.pg_class_writes_undecoded, 0,
        "VACUUM FULL pg_<non-mapped> must NOT tick pg_class_writes_undecoded — that signals \
         genuinely malformed WAL, not prefix compression",
    );
    assert_eq!(
        manifest.stats.pg_class_writes_oid_in_prefix, tracker.pg_class_writes_oid_in_prefix,
        "manifest counter must mirror tracker counter",
    );
}

#[tokio::test]
async fn writes_filtered_segment_and_manifest_via_cli() {
    let seg = fixture_segment();
    if !seg.exists() {
        eprintln!("skip: no captured segment at {:?}", seg);
        return;
    }
    // Pass the compressed fixture directly: the CLI now classifies +
    // decompresses on the input side via wal_rs::pg::wal::segment_file
    let bytes = load_segment(&seg).await.expect("load fixture");

    let out_dir = tempfile::tempdir().unwrap();
    let exe = env!("CARGO_BIN_EXE_walshadow-filter");
    let out = Command::new(exe)
        .arg("--in")
        .arg(&seg)
        .arg("--out-dir")
        .arg(out_dir.path())
        .arg("--quiet")
        .output()
        .expect("run walshadow-filter");
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Canonical 24-hex name lands in out_dir + manifest filename
    let canonical = seg
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.split('.').next())
        .expect("fixture name");
    let seg_path = out_dir.path().join(canonical);
    let manifest_path = out_dir.path().join(format!("{canonical}.manifest.json"));
    assert!(seg_path.exists(), "missing filtered segment {seg_path:?}");
    assert!(manifest_path.exists(), "missing manifest {manifest_path:?}");

    let filtered = std::fs::read(&seg_path).expect("read filtered");
    assert_eq!(filtered.len(), bytes.len());
    let manifest: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&manifest_path).unwrap())
            .expect("read manifest");
    assert!(!manifest["records"].as_array().unwrap().is_empty());
}
