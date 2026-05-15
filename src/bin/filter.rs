//! `walshadow-filter` — drop user-relation WAL records and emit a
//! filtered segment + manifest sidecar.
//!
//! Usage:
//! ```text
//! walshadow-filter --in seg.wal --out-dir filtered/ [--manifest filtered/seg.json]
//! ```
//! Reads `seg.wal`, walks every record, drops user-relation records by
//! NOOP-replacing them in place (xl_prev chain preserved), writes the
//! result to `filtered/<basename(seg.wal)>` and a JSON sidecar next to
//! it (or to `--manifest` if given).

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::Parser;
use walshadow::filter_segment::filter_segment;

#[derive(Debug, Parser)]
#[command(name = "walshadow-filter", about = "Filter WAL segment to catalog-only.")]
struct Args {
    /// Input segment file. Pass once per segment.
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

fn main() -> ExitCode {
    let args = Args::parse();
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("walshadow-filter: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<()> {
    let bytes = fs::read(&args.input)
        .with_context(|| format!("read input {}", args.input.display()))?;
    let name = args
        .input
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();

    let (filtered, manifest) = filter_segment(&bytes, &name)
        .with_context(|| format!("filter {}", args.input.display()))?;

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create out-dir {}", args.out_dir.display()))?;
    let out_path = args.out_dir.join(&name);
    fs::write(&out_path, &filtered)
        .with_context(|| format!("write filtered segment {}", out_path.display()))?;

    let manifest_path = args.manifest.unwrap_or_else(|| {
        args.out_dir.join(format!("{name}.manifest.json"))
    });
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
