//! Deferred spool and plan spool write and replay, without servers.
//! `cargo bench --bench spool_read -- --rows 200000`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use walrus::pg::walparser::RelFileNode;
use walshadow::backup_page_walk::BackfillTuple;
use walshadow::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, DescribedHeap, HeapOp};
use walshadow::pipeline::plan_spool::{PlanItem, PlanWriter};
use walshadow::schema::{RelDescriptor, RelName, ReplIdent};
use walshadow::spool::DeferredSpool;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value_t = 200_000)]
    rows: usize,
    #[arg(long, default_value_t = 256)]
    payload_bytes: usize,
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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    println!("{args:?}");
    for repeat in 1..=args.repeats {
        runtime.block_on(deferred(&args, repeat))?;
        plan(&args, repeat)?;
    }
    Ok(())
}

fn rfn() -> RelFileNode {
    RelFileNode {
        spc_node: 1663,
        db_node: 5,
        rel_node: 16385,
    }
}

fn report(name: &str, repeat: usize, rows: usize, bytes: u64, write: Duration, read: Duration) {
    println!(
        "{name} repeat={repeat} rows={rows} bytes={bytes} write_s={:.6} read_s={:.6} read_rows_per_s={:.0} read_mib_s={:.2}",
        write.as_secs_f64(),
        read.as_secs_f64(),
        rows as f64 / read.as_secs_f64(),
        bytes as f64 / (1 << 20) as f64 / read.as_secs_f64(),
    );
}

async fn deferred(args: &Args, repeat: usize) -> anyhow::Result<()> {
    let dir = tempfile::tempdir_in(&args.dir)?;
    let payload = "x".repeat(args.payload_bytes);
    let mut spool = DeferredSpool::new(dir.path().join("deferred.spool"), 1 << 20);
    let start = Instant::now();
    for i in 0..args.rows {
        spool
            .push(BackfillTuple {
                rfn: rfn(),
                xid: i as u32,
                xmax: 0,
                infomask: 0,
                source_lsn: i as u64,
                blkno: i as u32,
                offnum: 1,
                columns: vec![
                    Some(ColumnValue::Int4(i as i32)),
                    Some(ColumnValue::Text(payload.clone())),
                ],
            })
            .await?;
    }
    let bytes = spool.spooled_bytes();
    let mut reader = spool.into_reader().await?;
    let write = start.elapsed();
    let start = Instant::now();
    let mut n = 0;
    while let Some(t) = reader.next().await? {
        assert_eq!(t.xid, n as u32);
        assert!(matches!(&t.columns[1], Some(ColumnValue::Text(s)) if s.len() == payload.len()));
        n += 1;
    }
    reader.finish().await?;
    let read = start.elapsed();
    assert_eq!(n, args.rows);
    report("deferred", repeat, n, bytes, write, read);
    Ok(())
}

fn plan(args: &Args, repeat: usize) -> anyhow::Result<()> {
    let dir = tempfile::tempdir_in(&args.dir)?;
    let descriptor = Arc::new(RelDescriptor {
        rfn: rfn(),
        oid: 16385,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", "spool_bench"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: Vec::new(),
    });
    let payload = "x".repeat(args.payload_bytes);
    let start = Instant::now();
    let mut writer = PlanWriter::create(dir.path().join("plan.spool"), u64::MAX, 1 << 20)?;
    for i in 0..args.rows {
        writer.push_heap(
            &DescribedHeap {
                decoded: DecodedHeap {
                    rfn: rfn(),
                    xid: 7,
                    source_lsn: i as u64,
                    op: HeapOp::Insert,
                    new: Some(DecodedTuple {
                        columns: vec![
                            Some(ColumnValue::Int4(i as i32)),
                            Some(ColumnValue::Text(payload.clone())),
                        ],
                        partial: false,
                    }),
                    old: None,
                },
                descriptor: descriptor.clone(),
                descriptor_valid_from: 1,
            },
            None,
        )?;
    }
    let sealed = writer.seal(u64::MAX, 0)?;
    let write = start.elapsed();
    let start = Instant::now();
    sealed.verify()?;
    let verify = start.elapsed();
    let start = Instant::now();
    let mut rd = sealed.replay()?;
    let mut n = 0;
    while let Some(item) = rd.next_item()? {
        let PlanItem::Heap(h) = item else {
            unreachable!()
        };
        assert_eq!(h.described.decoded.source_lsn, n as u64);
        n += 1;
    }
    let replay = start.elapsed();
    assert_eq!(n, args.rows);
    report("plan_verify", repeat, n, sealed.size_bytes, write, verify);
    report("plan_replay", repeat, n, sealed.size_bytes, write, replay);
    Ok(())
}
