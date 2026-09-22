//! Round-trip: capture → filter → re-parse with WalParser.
//!
//! Captured-segment tests skip silently when no segment is present.
//! `capture.sh` regenerates each fixture; see `fixtures/wal/classify/capture.sh`.
//!
//! Assertions:
//! 1. Filtered segment is the same length as the source (byte-preserving).
//! 2. Every record re-parses through wal-rus's `WalParser` without error.
//! 3. Filter emits one record per source record.
//! 4. Filter dropped >0 user records on a non-DDL-heavy workload.
//! 5. All `Route::ToDecoder` records show as `XLOG_NOOP` (rmid=0, info=0x20)
//!    in the filtered output.

use std::path::{Path, PathBuf};
use std::process::Command;

use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::{WAL_PAGE_SIZE, WalParser};
use walshadow::pos::Pos;
use walshadow::record::{CollectingRecordSink, CollectingSegmentSink, Record, Route};
use walshadow::wal_stream::WalStream;

#[path = "common/segment.rs"]
mod segment;
use segment::load_segment;

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

/// One segment through the streaming filter
struct Filtered {
    source: Vec<u8>,
    out: Vec<u8>,
    seg_start: u64,
    records: Vec<Record<'static>>,
    stream: WalStream,
}

impl Filtered {
    fn dropped(&self) -> usize {
        self.records
            .iter()
            .filter(|r| r.route == Route::ToDecoder)
            .count()
    }
}

async fn filter(path: &Path) -> Filtered {
    let mut source = load_segment(path).await.expect("load fixture");
    let name = path.file_name().unwrap().to_str().unwrap();
    let seg = SegmentName::parse(&name[..24]).expect("fixture segment name");
    // Long page header's xlp_seg_size; captures trim trailing zero pages
    let seg_size = u32::from_le_bytes(source[32..36].try_into().unwrap()) as u64;
    source.resize(seg_size as usize, 0);
    let seg_start = seg.start_lsn(seg_size);
    let mut stream = WalStream::new(seg.timeline, seg_size, Pos::new(seg_start)).unwrap();
    let mut records = CollectingRecordSink::default();
    let mut segments = CollectingSegmentSink::default();
    stream
        .push(seg_start, &source, &mut records, &mut segments)
        .await
        .expect("filter");
    let (_, out) = segments.segments.pop().expect("segment sealed");
    Filtered {
        source,
        out,
        seg_start,
        records: records.records,
        stream,
    }
}

/// (total_record_count, noop_count) by walking through `WalParser`.
fn parse_all_records(bytes: &[u8]) -> anyhow::Result<(usize, usize)> {
    use walrus::pg::walparser::RmId;
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
    let f = filter(&seg).await;

    // (1) Byte-preserving
    assert_eq!(
        f.out.len(),
        f.source.len(),
        "filtered segment length must match source"
    );

    // (2) Re-parses cleanly. (3) Record count matches filter output.
    let (filtered_count, noops) = parse_all_records(&f.out).expect("re-parse filtered segment");
    let stats = f.stream.filter().stats();
    eprintln!(
        "fixture: source {} bytes, {} records (kept {}, dropped {}, undecoded-pg_class {})",
        f.source.len(),
        f.records.len(),
        stats.kept,
        stats.dropped,
        f.stream
            .filter()
            .tracker()
            .stats()
            .pg_class_writes_undecoded,
    );
    assert_eq!(
        filtered_count,
        f.records.len(),
        "WalParser record count != filtered record count"
    );

    // (4) Filter dropped >0 user records on this fixture.
    assert!(
        f.dropped() > 0,
        "filter dropped zero records — bug in classifier?"
    );

    // (5) Number of NOOP records in filtered stream == dropped.
    assert_eq!(
        noops,
        f.dropped(),
        "noop count in filtered stream does not match dropped"
    );

    // Source segment was also parseable (it's a real PG capture).
    let (source_count, _) = parse_all_records(&f.source).expect("re-parse source segment");
    assert_eq!(source_count, f.records.len());
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
    let f = filter(&seg).await;
    let total = f.records.len();
    let kept = total - f.dropped();
    let kept_frac = kept as f64 / total as f64;
    eprintln!(
        "OLTP fixture: {total} records, kept {kept} ({:.4}%), dropped {} ({:.4}%)",
        kept_frac * 100.0,
        f.dropped(),
        100.0 - kept_frac * 100.0,
    );

    assert!(total > 1000, "OLTP fixture too small: {total} records");
    assert!(
        kept_frac < 0.01,
        "kept fraction {:.4} exceeds 1% — acceptance §1 violated",
        kept_frac
    );

    // Re-parse filtered output.
    let (filtered_count, noops) = parse_all_records(&f.out).expect("re-parse filtered");
    assert_eq!(filtered_count, total);
    assert_eq!(noops, f.dropped());
}

/// A real PG `pg_switch_wal()` lands an XLOG_SWITCH (rmgr 0, info 0x40) in
/// the WAL segment; the filter must keep it byte-identically because
/// shadow's recovery state machine relies on its presence at the segment
/// tail.
#[tokio::test]
async fn xlog_switch_fixture_keeps_switch_record_bytes_intact() {
    use walrus::pg::walparser::RmId;
    const XLOG_SWITCH: u8 = 0x40;
    let seg = xlog_switch_segment();
    let f = filter(&seg).await;

    let switch = f
        .records
        .iter()
        .find(|r| {
            r.parsed.header.resource_manager_id == RmId::Xlog as u8
                && (r.parsed.header.info & 0xF0) == XLOG_SWITCH
        })
        .expect("captured segment must contain ≥1 XLOG_SWITCH");
    let off = (switch.source_lsn - f.seg_start) as usize;
    let len = switch.parsed.header.total_record_length as usize;
    assert_eq!(
        &f.source[off..off + len],
        &f.out[off..off + len],
        "XLOG_SWITCH bytes must pass through unchanged",
    );
    assert_eq!(
        switch.route,
        Route::ToShadow,
        "XLOG_SWITCH must be kept (special rmgr policy)"
    );
}

/// `VACUUM FULL` of non-mapped catalogs: pg_class updates prefix-compress
/// past the OID, and the rebuilt heaps fill transient filenodes before the
/// swap. Catalog-only workload, so the filter must drop nothing — every
/// byte written into a rebuilt catalog's storage reaches shadow.
#[tokio::test]
async fn vacuum_full_pg_depend_keeps_every_rebuilt_catalog_write() {
    let f = filter(&vacuum_full_pg_depend_segment()).await;
    let tracker = f.stream.filter().tracker().stats();
    eprintln!(
        "VACUUM FULL pg_depend fixture: {} records, dropped={}, oid_in_prefix={}, undecoded={}, decoded={}",
        f.records.len(),
        f.dropped(),
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
    let dropped: Vec<_> = f
        .records
        .iter()
        .filter(|r| r.route == Route::ToDecoder)
        .map(|r| {
            let blocks: Vec<_> = r
                .parsed
                .blocks
                .iter()
                .map(|b| b.header.location.rel.rel_node)
                .collect();
            (r.source_lsn, r.parsed.header.resource_manager_id, blocks)
        })
        .collect();
    assert!(
        dropped.is_empty(),
        "catalog rebuild writes dropped: {dropped:?}"
    );
}

#[tokio::test]
async fn writes_filtered_segment_via_cli() {
    let seg = xlog_switch_segment();
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

    // Canonical 24-hex name lands in out_dir, alone
    let names: Vec<_> = std::fs::read_dir(out_dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, ["000000010000000000000002"]);
    let filtered = std::fs::read(out_dir.path().join("000000010000000000000002")).unwrap();
    assert!(filtered == filter(&seg).await.out, "cli matches library");
}

#[tokio::test]
async fn cli_prints_stats_line_without_quiet() {
    let seg = xlog_switch_segment();
    let out_dir = tempfile::tempdir().unwrap();
    let exe = env!("CARGO_BIN_EXE_walshadow-filter");
    let out = Command::new(exe)
        .arg("--in")
        .arg(&seg)
        .arg("--out-dir")
        .arg(out_dir.path())
        .output()
        .expect("run walshadow-filter");
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("filtered"),
        "expected stats line, got {stderr:?}"
    );
    assert!(stderr.contains("records"), "got {stderr:?}");
}
