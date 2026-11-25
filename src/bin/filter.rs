//! `walshadow-filter` — drop user-relation WAL records from consecutive
//! segments, writing filtered segments under their own names.
//!
//! ```text
//! walshadow-filter --in seg1.wal[.zst|.lz4] [--in seg2 …] \
//!     --out-dir filtered/
//! ```
//!
//! Handles *segment-file* compression (whole-segment codec envelope from
//! pg_receivewal/archive_command), NOT the orthogonal `wal_compression`
//! GUC that compresses FPIs *inside* records.
//!
//! Drives the daemon's streaming filter, so catalog state carries across
//! segments. A record spanning past the last input holds its segment back;
//! the first segment's leading continuation passes through unchanged.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use tokio::io::AsyncReadExt;
use walrus::pg::wal::segment_file::open_segment_file;
use walshadow::pos::Pos;
use walshadow::record::{CountingRecordSink, WAL_SEG_SIZE};
use walshadow::segment_sink::DirSegmentSink;
use walshadow::wal_stream::WalStream;

#[cfg(test)]
#[path = "../../tests/common/tools.rs"]
mod tools;

#[derive(Debug, Parser)]
#[command(
    name = "walshadow-filter",
    version = walshadow::VERSION,
    about = "Filter consecutive WAL segments to catalog-only."
)]
struct Args {
    /// Input segment files in LSN order. Compression suffix (.zst .lz4 .gz
    /// .lzma .br) is auto-detected, each needs its cargo feature
    #[arg(long = "in", value_name = "SEGMENT", required = true)]
    input: Vec<PathBuf>,
    /// Output directory for filtered segments.
    #[arg(long = "out-dir", value_name = "DIR")]
    out_dir: PathBuf,
    /// Skip the summary line on stderr.
    #[arg(long)]
    quiet: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("walshadow-filter: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    let mut segments = DirSegmentSink::new(args.out_dir.clone())
        .with_context(|| format!("create out-dir {}", args.out_dir.display()))?;
    let mut records = CountingRecordSink::default();
    let mut stream: Option<WalStream> = None;
    for input in &args.input {
        let (seg, mut reader) = open_segment_file(input)
            .await
            .with_context(|| format!("open input {}", input.display()))?;
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .await
            .with_context(|| format!("read input {}", input.display()))?;
        let seg_size = long_header_seg_size(&bytes)
            .with_context(|| format!("{}: no long page header", input.display()))?;
        // Untrusted header: cap before resize, else a tiny file drives a huge allocation.
        ensure!(
            seg_size <= WAL_SEG_SIZE,
            "{}: segment size {seg_size} exceeds cap {WAL_SEG_SIZE}",
            input.display()
        );
        ensure!(
            seg_size.is_power_of_two() && bytes.len() as u64 <= seg_size,
            "{}: {} bytes against segment size {seg_size}",
            input.display(),
            bytes.len()
        );
        // Captures may trim trailing zero pages
        bytes.resize(seg_size as usize, 0);
        let lsn = seg.start_lsn(seg_size);
        let stream = match &mut stream {
            Some(stream) => stream,
            None => stream.insert(WalStream::new(seg.timeline, seg_size, Pos::new(lsn))?),
        };
        ensure!(
            stream.timeline() == seg.timeline && stream.seg_size() == seg_size,
            "{}: timeline or segment size differs from first input",
            input.display()
        );
        stream
            .push(lsn, &bytes, &mut records, &mut segments)
            .await
            .with_context(|| format!("filter {}", input.display()))?;
    }
    let stream = stream.expect("clap requires an input");
    if stream.dispatched_lsn() < stream.next_lsn().get() {
        eprintln!(
            "walshadow-filter: record continues past last input, segment at {:#X} held back",
            stream.dispatched_lsn()
        );
    }
    if !args.quiet {
        let s = stream.filter().stats();
        let t = stream.filter().tracker().stats();
        eprintln!(
            "filtered {} segments: {} records, kept {} ({} bytes), dropped {} ({} bytes), relmap updates {}, pg_class undecoded {}, oid in prefix {}",
            args.input.len(),
            s.kept + s.dropped,
            s.kept,
            s.kept_bytes,
            s.dropped,
            s.dropped_bytes,
            t.relmap_updates,
            t.pg_class_writes_undecoded,
            t.pg_class_writes_oid_in_prefix,
        );
    }
    Ok(())
}

/// `XLogLongPageHeaderData.xlp_seg_size`, present on every segment's first page
fn long_header_seg_size(bytes: &[u8]) -> Option<u64> {
    Some(u32::from_le_bytes(bytes.get(32..36)?.try_into().ok()?) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_errors_on_missing_input() {
        let tmp = tempfile::tempdir().unwrap();
        let err = run(Args {
            input: vec![tmp.path().join("nope.wal")],
            out_dir: tmp.path().join("out"),
            quiet: true,
        })
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("open input"), "{err:#}");
    }

    async fn run_err(input: Vec<PathBuf>, out_dir: PathBuf) -> String {
        let args = Args {
            input,
            out_dir,
            quiet: true,
        };
        format!("{:#}", run(args).await.unwrap_err())
    }

    #[tokio::test]
    async fn run_rejects_unusable_inputs() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("out");
        let file = tmp.path().join("file");
        std::fs::write(&file, b"").unwrap();
        let err = run_err(vec![file.clone()], file.join("out")).await;
        assert!(err.contains("create out-dir"), "{err}");

        let dir = tmp.path().join("000000010000000000000002");
        std::fs::create_dir(&dir).unwrap();
        let err = run_err(vec![dir], out.clone()).await;
        assert!(err.contains("read input"), "{err}");

        let short = tmp.path().join("short/000000010000000000000002");
        std::fs::create_dir(short.parent().unwrap()).unwrap();
        std::fs::write(&short, [0u8; 8]).unwrap();
        let err = run_err(vec![short.clone()], out.clone()).await;
        assert!(err.contains("no long page header"), "{err}");

        let mut oversized = vec![0u8; 64];
        oversized[32..36].copy_from_slice(&16u32.to_le_bytes());
        std::fs::write(&short, oversized).unwrap();
        let err = run_err(vec![short.clone()], out.clone()).await;
        assert!(err.contains("64 bytes against segment size 16"), "{err}");

        let mut huge = vec![0u8; 36];
        huge[32..36].copy_from_slice(&0x8000_0000u32.to_le_bytes());
        std::fs::write(&short, huge).unwrap();
        let err = run_err(vec![short], out).await;
        assert!(err.contains("exceeds cap 16777216"), "{err}");
    }

    /// Decompressed fixture bytes written under `name` in `dir`
    async fn raw_copy(fixture: &std::path::Path, dir: &std::path::Path, name: &str) -> PathBuf {
        let (_, mut r) = open_segment_file(fixture).await.unwrap();
        let mut bytes = Vec::new();
        r.read_to_end(&mut bytes).await.unwrap();
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("fixtures/wal/{name}.zst"))
    }

    #[tokio::test]
    async fn run_filters_fixture_and_writes_segment() {
        let seg = fixture_path("xlog_switch/segments/000000010000000000000002");
        if tools::fixture(&seg) {
            let tmp = tempfile::tempdir().unwrap();
            let out_dir = tmp.path().join("out");
            run(Args {
                input: vec![seg],
                out_dir: out_dir.clone(),
                quiet: false,
            })
            .await
            .expect("run");
            let names: Vec<_> = std::fs::read_dir(&out_dir)
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            assert_eq!(names, ["000000010000000000000002"]);
        }
    }

    #[tokio::test]
    async fn run_rejects_inputs_that_do_not_continue_first() {
        let seg = fixture_path("xlog_switch/segments/000000010000000000000002");
        if tools::fixture(&seg) {
            let tmp = tempfile::tempdir().unwrap();
            let out = tmp.path().join("out");
            let other_tli = raw_copy(&seg, tmp.path(), "000000020000000000000003").await;
            let err = run_err(vec![seg.clone(), other_tli], out.clone()).await;
            assert!(err.contains("timeline or segment size differs"), "{err}");
            let gap = raw_copy(&seg, tmp.path(), "000000010000000000000004").await;
            let err = run_err(vec![seg, gap], out).await;
            assert!(
                err.contains("filter ") && err.contains("000000010000000000000004"),
                "{err}"
            );
        }
    }

    /// One-page segment whose only record claims more bytes than the page holds
    fn spanning_segment() -> Vec<u8> {
        use walrus::pg::walparser::{RmId, WAL_PAGE_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15};
        let mut page = Vec::new();
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes());
        page.extend_from_slice(&[0; 16]);
        page.extend_from_slice(&12345u64.to_le_bytes());
        page.extend_from_slice(&u32::from(WAL_PAGE_SIZE).to_le_bytes());
        page.extend_from_slice(&u32::from(WAL_PAGE_SIZE).to_le_bytes());
        page.extend_from_slice(&(2 * u32::from(WAL_PAGE_SIZE)).to_le_bytes());
        page.extend_from_slice(&[0; 12]);
        page.extend_from_slice(&[0, RmId::Xact as u8, 0, 0]);
        page.resize(WAL_PAGE_SIZE as usize, 0xAA);
        page
    }

    #[tokio::test]
    async fn run_holds_back_segment_whose_last_record_continues() {
        let tmp = tempfile::tempdir().unwrap();
        let seg = tmp.path().join("000000010000000000000000");
        std::fs::write(&seg, spanning_segment()).unwrap();
        let out_dir = tmp.path().join("out");
        run(Args {
            input: vec![seg],
            out_dir: out_dir.clone(),
            quiet: true,
        })
        .await
        .expect("run");
        assert_eq!(std::fs::read_dir(&out_dir).unwrap().count(), 0);
    }
}
