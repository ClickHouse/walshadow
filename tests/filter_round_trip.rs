//! Round-trip: capture → filter → re-parse with WalParser.
//!
//! Captured-segment tests skip silently when no segment is present.
//! `capture.sh` regenerates each fixture; see `fixtures/wal/classify/capture.sh`.
//!
//! Assertions:
//! 1. Filtered segment is the same length as the source (byte-preserving).
//! 2. Every record re-parses through wal-rus's `WalParser` and `pg_waldump`.
//! 3. Filter emits one record per source record.
//! 4. Filter dropped >0 user records on a non-DDL-heavy workload.
//! 5. Kept records stay, and runs of `Route::ToDecoder` records show as
//!    fewer `XLOG_NOOP`s (rmid=0, info=0x20) in the filtered output.

#[path = "common/tools.rs"]
mod tools;

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
    name: String,
    source: Vec<u8>,
    out: Vec<u8>,
    seg_start: u64,
    records: Vec<Record<'static>>,
    stream: WalStream,
}

impl Filtered {
    /// `pg_waldump` reads every filtered record, CRC and `xl_prev` checked
    fn waldump(&self, expected: usize) {
        if !tools::on_path("pg_waldump", "--version") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(&self.name), &self.out).unwrap();
        let lsn = |l: u64| format!("{:X}/{:X}", l >> 32, l as u32);
        let first = self.records.first().unwrap().source_lsn;
        let end = self.records.last().unwrap().next_lsn;
        let out = Command::new("pg_waldump")
            .arg("-p")
            .arg(dir.path())
            .args(["-s", &lsn(first), "-e", &lsn(end)])
            .output()
            .expect("run pg_waldump");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success(),
            "pg_waldump: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(stdout.lines().count(), expected, "pg_waldump record count");
    }

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
        name: name[..24].to_string(),
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
    if !tools::fixture(&seg) {
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
    f.waldump(filtered_count);
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
        filtered_count - noops,
        f.records.len() - f.dropped(),
        "WalParser kept count != filter kept count"
    );

    // (4) Filter dropped >0 user records on this fixture.
    assert!(
        f.dropped() > 0,
        "filter dropped zero records — bug in classifier?"
    );

    // (5) Runs of dropped records merge into fewer NOOPs
    assert!(
        noops < f.dropped(),
        "{noops} NOOPs for {} dropped records",
        f.dropped()
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
    if !tools::fixture(&seg) {
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
    assert_eq!(filtered_count - noops, kept);
    assert!(
        noops < f.dropped() / 4,
        "{noops} NOOPs for {} dropped",
        f.dropped()
    );
    f.waldump(filtered_count);
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

/// The compact form of filtered fixtures expands back to the same bytes from
/// any cut, page and record boundaries included, and `restore-wal` rebuilds
/// the full segment the shadow's `restore_command` must hand PostgreSQL
#[tokio::test]
async fn compact_segments_expand_to_the_filtered_bytes() {
    use walshadow::compact;
    let exe = env!("CARGO_BIN_EXE_walshadow-stream");
    let dir = tempfile::tempdir().unwrap();
    for path in [fixture_segment(), oltp_segment(), xlog_switch_segment()] {
        if !tools::fixture(&path) {
            return;
        }
        let f = filter(&path).await;
        let page = WAL_PAGE_SIZE as usize;
        // Every page start and record-aligned offset after its header,
        // through the whole segment once
        let starts = (0..f.out.len()).step_by(page).flat_map(|p| [p, p + 24]);
        let lengths = [1, page + 5, 3 * page].into_iter().cycle();
        for (a, len) in starts.zip(lengths).chain([(0, f.out.len())]) {
            let b = (a + len).min(f.out.len());
            let mut packed = Vec::new();
            compact::encode(&f.out[a..b], &mut packed);
            let mut out = Vec::new();
            compact::expand(&packed, b - a, &mut out).unwrap();
            assert!(out == f.out[a..b], "{}: {a:#X}..{b:#X}", f.name);
        }
        let mut file = Vec::new();
        compact::segment_file(&f.out, &mut file);
        eprintln!(
            "{}: compact {} of {} bytes",
            f.name,
            file.len(),
            f.out.len()
        );
        let (src, dst) = (dir.path().join(&f.name), dir.path().join("RECOVERYXLOG"));
        std::fs::write(&src, &file).unwrap();
        let status = Command::new(exe)
            .args(["restore-wal".as_ref(), src.as_os_str(), dst.as_os_str()])
            .status()
            .unwrap();
        assert!(status.success());
        assert!(std::fs::read(&dst).unwrap() == f.out, "{} restored", f.name);
    }
    let history = dir.path().join("00000002.history");
    std::fs::write(&history, "1\t0/3000000\tno recovery target specified\n").unwrap();
    let copy = dir.path().join("RECOVERYHISTORY");
    let restore = |src: &Path| {
        Command::new(exe)
            .args(["restore-wal".as_ref(), src.as_os_str(), copy.as_os_str()])
            .output()
            .unwrap()
    };
    assert!(restore(&history).status.success());
    assert_eq!(
        std::fs::read(&copy).unwrap(),
        std::fs::read(&history).unwrap()
    );
    // PostgreSQL reads 1 as "not archived" and anything above 125 as fatal
    let missing = restore(&dir.path().join("000000010000000000000009"));
    assert_eq!(missing.status.code(), Some(1));
}
