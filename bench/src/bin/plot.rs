use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(about = "Render benchmark logs as PNG graphs and JSON/CSV summaries")]
struct Args {
    #[arg(long = "run", required = true)]
    runs: Vec<PathBuf>,
    #[arg(long = "label")]
    labels: Vec<String>,
    #[arg(long, alias = "output")]
    out: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    walshadow_bench::plot::generate(&args.runs, &args.labels, &args.out)
}
