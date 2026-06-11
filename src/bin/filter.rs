//! `walshadow-filter` — drop user-relation WAL records, emit filtered
//! segment + manifest sidecar.
//!
//! ```text
//! walshadow-filter --in seg.wal[.zst|.gz|.lz4|.lzma|.br][.partial] \
//!     --out-dir filtered/ [--manifest filtered/seg.json]
//! ```
//!
//! Handles *segment-file* compression (whole-segment codec envelope from
//! pg_receivewal/archive_command), NOT the orthogonal `wal_compression`
//! GUC that compresses FPIs *inside* records — that's `filter_segment`'s
//! concern.

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::io::AsyncReadExt;
use walross::pg::wal::segment_file::open_segment_file;
use walshadow::filter::Filter;
use walshadow::filter_segment::filter_segment;

#[derive(Debug, Parser)]
#[command(
    name = "walshadow-filter",
    about = "Filter WAL segment to catalog-only."
)]
struct Args {
    /// Input segment file. Compression suffix (.zst .gz .lz4 .lzma .br)
    /// is auto-detected; `.partial` peer accepted.
    #[arg(long = "in", value_name = "SEGMENT")]
    input: PathBuf,
    /// Output directory for the filtered segment.
    #[arg(long = "out-dir", value_name = "DIR")]
    out_dir: PathBuf,
    /// Optional explicit manifest path. Default: `<out-dir>/<seg>.json`.
    #[arg(long = "manifest", value_name = "PATH")]
    manifest: Option<PathBuf>,
    /// Print a one-line summary to stderr on success.
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
    let (seg_name, mut reader) = open_segment_file(&args.input)
        .await
        .with_context(|| format!("open input {}", args.input.display()))?;
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .await
        .with_context(|| format!("read input {}", args.input.display()))?;
    let name = seg_name.format();

    let mut filter = Filter::new();
    let (filtered, manifest, _parsed) = filter_segment(&bytes, &name, &mut filter)
        .with_context(|| format!("filter {}", args.input.display()))?;

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create out-dir {}", args.out_dir.display()))?;
    let out_path = args.out_dir.join(&name);
    fs::write(&out_path, &filtered)
        .with_context(|| format!("write filtered segment {}", out_path.display()))?;

    let manifest_path = args
        .manifest
        .unwrap_or_else(|| args.out_dir.join(format!("{name}.manifest.json")));
    let mf = fs::File::create(&manifest_path)
        .with_context(|| format!("create manifest {}", manifest_path.display()))?;
    serde_json::to_writer_pretty(mf, &manifest)
        .with_context(|| format!("write manifest {}", manifest_path.display()))?;

    if !args.quiet {
        let s = &manifest.stats;
        eprintln!(
            "filtered {}: {} records, kept {} ({} bytes), dropped {} ({} bytes), relmap updates {}, pg_class undecoded {}",
            name,
            s.records,
            s.kept,
            s.kept_bytes,
            s.dropped,
            s.dropped_bytes,
            s.relmap_updates,
            s.pg_class_writes_undecoded,
        );
    }
    Ok(())
}
