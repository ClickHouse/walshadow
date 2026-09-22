//! `walshadow-filter` — drop user-relation WAL records from consecutive
//! segments, writing filtered segments under their own names.
//!
//! ```text
//! walshadow-filter --in seg1.wal[.zst|.gz|.lz4|.lzma|.br] [--in seg2 …] \
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
use walshadow::record::CountingRecordSink;
use walshadow::segment_sink::DirSegmentSink;
use walshadow::wal_stream::WalStream;

#[derive(Debug, Parser)]
#[command(
    name = "walshadow-filter",
    version = walshadow::VERSION,
    about = "Filter consecutive WAL segments to catalog-only."
)]
struct Args {
    /// Input segment files in LSN order. Compression suffix (.zst .gz .lz4
    /// .lzma .br) is auto-detected.
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

    #[tokio::test]
    async fn run_filters_fixture_and_writes_segment() {
        let seg = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/wal/xlog_switch/segments/000000010000000000000002.gz");
        assert!(seg.exists(), "committed fixture {seg:?}");
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
