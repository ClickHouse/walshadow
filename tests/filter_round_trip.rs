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

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};

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

fn decompress_gz(path: &PathBuf) -> std::io::Result<Vec<u8>> {
    let mut child = Command::new("gunzip")
        .arg("-c")
        .arg(path)
        .stdout(Stdio::piped())
        .spawn()?;
    let mut out = Vec::new();
    child.stdout.as_mut().unwrap().read_to_end(&mut out)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "gunzip {:?} failed: {status}",
            path
        )));
    }
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

#[test]
fn filtered_segment_round_trips_through_wal_parser() {
    let seg = fixture_segment();
    if !seg.exists() {
        eprintln!("skip: no captured segment at {:?}", seg);
        return;
    }
    let bytes = decompress_gz(&seg).expect("gunzip fixture");
    let mut filter = Filter::new();
    let (out, manifest) = filter_segment(&bytes, "fixture", &mut filter).expect("filter");

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
#[test]
fn oltp_workload_keeps_well_under_one_percent() {
    let seg = oltp_segment();
    if !seg.exists() {
        eprintln!(
            "skip: no OLTP fixture at {:?}. Run fixtures/wal/filter/capture.sh",
            seg
        );
        return;
    }
    let bytes = decompress_gz(&seg).expect("gunzip oltp fixture");
    let mut filter = Filter::new();
    let (out, manifest) = filter_segment(&bytes, "oltp", &mut filter).expect("filter oltp");

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

#[test]
fn writes_filtered_segment_and_manifest_via_cli() {
    let seg = fixture_segment();
    if !seg.exists() {
        eprintln!("skip: no captured segment at {:?}", seg);
        return;
    }
    let bytes = decompress_gz(&seg).expect("gunzip fixture");
    let tmp_in = tempfile::NamedTempFile::new().unwrap();
    let mut f = File::create(tmp_in.path()).unwrap();
    use std::io::Write;
    f.write_all(&bytes).unwrap();
    drop(f);

    let out_dir = tempfile::tempdir().unwrap();
    let exe = env!("CARGO_BIN_EXE_walshadow-filter");
    let out = Command::new(exe)
        .arg("--in")
        .arg(tmp_in.path())
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

    let in_name = tmp_in.path().file_name().unwrap();
    let seg_path = out_dir.path().join(in_name);
    let manifest_path = out_dir
        .path()
        .join(format!("{}.manifest.json", in_name.to_string_lossy()));
    assert!(seg_path.exists());
    assert!(manifest_path.exists());

    let filtered = std::fs::read(&seg_path).expect("read filtered");
    assert_eq!(filtered.len(), bytes.len());
    let manifest: serde_json::Value =
        serde_json::from_reader(File::open(&manifest_path).unwrap()).expect("read manifest");
    assert!(!manifest["records"].as_array().unwrap().is_empty());
}
