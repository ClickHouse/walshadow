//! walshadow-classify — walk WAL segment files & print catalog/user/special split.
//!
//! Phase 0 deliverable. Consumes raw pg_wal segment files (16 MiB each by
//! default, the on-disk format pg_receivewal & a running primary write to
//! pg_wal/). Output is either a JSON [`Summary`] or a human-readable table.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use walshadow::classify::Summary;
use wal_rs::pg::walparser::{WAL_PAGE_SIZE, WalParser};

#[derive(Parser, Debug)]
#[command(name = "walshadow-classify", about = "Classify WAL records into catalog/user/special")]
struct Args {
    /// WAL segment files to scan, in LSN order.
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Emit JSON summary instead of the human table.
    #[arg(long)]
    json: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut summary = Summary::default();
    let mut parser = WalParser::new();

    for path in &args.files {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        walk_segment(&mut parser, &mut summary, BufReader::new(f))
            .with_context(|| format!("walk {}", path.display()))?;
    }

    if args.json {
        serde_json::to_writer_pretty(std::io::stdout(), &summary)?;
        println!();
    } else {
        print_human(&summary);
    }
    Ok(())
}

fn walk_segment<R: Read>(
    parser: &mut WalParser,
    summary: &mut Summary,
    mut r: R,
) -> Result<()> {
    let mut page = vec![0u8; WAL_PAGE_SIZE as usize];
    loop {
        let n = read_full(&mut r, &mut page)?;
        if n == 0 {
            return Ok(());
        }
        let buf = &page[..n];
        let (_, records) = parser
            .parse_records_from_page(buf)
            .map_err(|e| anyhow::anyhow!("parse page: {e}"))?;
        for rec in &records {
            summary.observe(rec);
        }
        if n < page.len() {
            return Ok(());
        }
    }
}

fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = r.read(&mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

fn print_human(s: &Summary) {
    println!("records: {}", s.records);
    println!("bytes:   {}", s.bytes);
    println!("catalog fraction: {:.4}%", 100.0 * s.catalog_fraction());
    println!();
    println!("{:<8} {:>10} {:>14}", "class", "records", "bytes");
    for (k, v) in &s.by_class {
        println!("{:<8} {:>10} {:>14}", k, v.records, v.bytes);
    }
    println!();
    println!(
        "{:<14} {:>10} {:>14} {:>8} {:>8} {:>8} {:>8}",
        "rmgr", "records", "bytes", "cat", "user", "spec", "empty"
    );
    for (k, v) in &s.by_rmgr {
        println!(
            "{:<14} {:>10} {:>14} {:>8} {:>8} {:>8} {:>8}",
            k, v.records, v.bytes, v.catalog, v.user, v.special, v.empty
        );
    }
}
