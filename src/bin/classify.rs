//! walshadow-classify — walk WAL segment files, print catalog/user/special split.
//!
//! Consumes raw pg_wal segment files (16 MiB each by default, the on-disk
//! format pg_receivewal and a running primary write to pg_wal/).
//! Compressed archives (`.zst`/`.gz`/`.lz4`/`.lzma`/`.br`, optional
//! `.partial` peer) are auto-detected by suffix and decoded via wal-rs's
//! `open_segment_file`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::io::{AsyncRead, AsyncReadExt};
use wal_rs::pg::wal::segment_file::open_segment_file;
use wal_rs::pg::walparser::{WAL_PAGE_SIZE, WalParser};
use walshadow::classify::Summary;

#[derive(Parser, Debug)]
#[command(
    name = "walshadow-classify",
    about = "Classify WAL records into catalog/user/special"
)]
struct Args {
    /// WAL segment files to scan, in LSN order. Compression suffix
    /// (.zst .gz .lz4 .lzma .br) auto-detected; `.partial` peer accepted.
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Emit JSON summary instead of the human table.
    #[arg(long)]
    json: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut summary = Summary::default();
    let mut parser = WalParser::new();

    for path in &args.files {
        let (_seg, reader) = open_segment_file(path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        walk_segment(&mut parser, &mut summary, reader)
            .await
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

async fn walk_segment<R: AsyncRead + Unpin>(
    parser: &mut WalParser,
    summary: &mut Summary,
    mut r: R,
) -> Result<()> {
    let mut page = vec![0u8; WAL_PAGE_SIZE as usize];
    loop {
        let n = read_full(&mut r, &mut page).await?;
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

async fn read_full<R: AsyncRead + Unpin>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = r.read(&mut buf[filled..]).await?;
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
