//! Transaction buffering and verified commit drain, without PostgreSQL or ClickHouse.
//! `cargo bench --bench xact_spill -- --buffer-bytes 1048576`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use walrus::pg::walparser::RelFileNode;
use walshadow::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, DescribedHeap, HeapOp};
use walshadow::schema::{RelDescriptor, RelName, ReplIdent};
use walshadow::xact_buffer::{StashResolved, XactBuffer, XactBufferConfig};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value_t = 7812)]
    rows: usize,
    #[arg(long, default_value_t = 8)]
    transactions: u32,
    #[arg(long, default_value_t = 1024)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 64 << 20)]
    buffer_bytes: usize,
    #[arg(long, default_value_t = 3)]
    repeats: usize,
    #[arg(long, default_value = "target")]
    dir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    if std::env::args().any(|arg| arg == "--list") {
        return Ok(());
    }
    let args = Args::parse_from(std::env::args().filter(|arg| arg != "--bench"));
    anyhow::ensure!(args.rows > 0 && args.transactions > 0 && args.repeats > 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(args))
}

async fn run(args: Args) -> anyhow::Result<()> {
    let descriptor = Arc::new(RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16385,
        },
        oid: 16385,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", "spill_bench"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: Vec::new(),
    });
    let payload = "x".repeat(args.payload_bytes);
    println!("{args:?}");
    for repeat in 0..args.repeats {
        let dir = tempfile::tempdir_in(&args.dir)?;
        let mut config = XactBufferConfig::new(dir.path().to_path_buf());
        config.xact_buffer_max = args.buffer_bytes;
        let mut buffer = XactBuffer::new(config)?;
        let mut write_time = Duration::ZERO;
        let mut read_time = Duration::ZERO;
        let mut spill_bytes = 0;
        let mut peak_memory = 0;
        for xid in 1..=args.transactions {
            let start = Instant::now();
            for row in 0..args.rows {
                buffer
                    .on_heap(DescribedHeap {
                        decoded: DecodedHeap {
                            rfn: descriptor.rfn,
                            xid,
                            source_lsn: u64::from(xid) * (args.rows as u64 + 1) + row as u64,
                            op: HeapOp::Insert,
                            new: Some(DecodedTuple {
                                columns: vec![Some(ColumnValue::Text(payload.clone()))],
                                partial: false,
                            }),
                            old: None,
                        },
                        descriptor: descriptor.clone(),
                        descriptor_valid_from: 1,
                    })
                    .await?;
                peak_memory = peak_memory.max(buffer.stats().bytes_in_memory);
            }
            write_time += start.elapsed();
            spill_bytes += buffer.stats().spill_bytes_active;
            let start = Instant::now();
            let commit_lsn = u64::from(xid + 1) * (args.rows as u64 + 1);
            let mut drain = buffer
                .drain_committed(
                    StashResolved::nothing_stashed(xid),
                    0,
                    commit_lsn,
                    &[],
                    false,
                )
                .await?;
            let mut rows = 0;
            while let Some(batch) = drain.next_batch(1024, 1 << 20, None).await? {
                for heap in batch.heaps {
                    assert_eq!(heap.decoded.xid, xid);
                    assert_eq!(
                        heap.decoded.source_lsn,
                        u64::from(xid) * (args.rows as u64 + 1) + rows
                    );
                    assert_eq!(heap.descriptor.as_ref(), descriptor.as_ref());
                    assert_eq!(heap.descriptor_valid_from, 1);
                    let tuple = heap.decoded.new.unwrap();
                    assert_eq!(tuple.columns.len(), 1);
                    assert!(
                        matches!(&tuple.columns[0], Some(ColumnValue::Text(s)) if s == &payload)
                    );
                    rows += 1;
                }
            }
            assert_eq!(rows, args.rows as u64);
            drain.finish().await?;
            buffer.resume_safe_lsn(walshadow::pos::Pos::new(commit_lsn));
            read_time += start.elapsed();
        }
        let rows = args.rows as f64 * f64::from(args.transactions);
        println!(
            "repeat={} rows={} spill_bytes={} evictions={} peak_buffer_bytes={} write_s={:.6} read_s={:.6} rows_per_s={:.0} spill_write_mib_s={:.2} spill_read_mib_s={:.2}",
            repeat + 1,
            rows as u64,
            spill_bytes,
            buffer.stats().spill_evictions_total,
            peak_memory,
            write_time.as_secs_f64(),
            read_time.as_secs_f64(),
            rows / (write_time + read_time).as_secs_f64(),
            spill_bytes as f64 / (1 << 20) as f64 / write_time.as_secs_f64(),
            spill_bytes as f64 / (1 << 20) as f64 / read_time.as_secs_f64(),
        );
        assert_eq!(buffer.stats().bytes_in_memory, 0);
        assert_eq!(buffer.stats().spill_bytes_active, 0);
        assert_eq!(std::fs::read_dir(buffer.scratch_dir())?.count(), 0);
    }
    Ok(())
}
