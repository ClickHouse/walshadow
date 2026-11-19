//! Transaction buffering and verified commit drain, without PostgreSQL or ClickHouse.
//! `cargo bench --bench xact_spill -- --buffer-bytes 1048576`
//! `--stashed` buffers records as stashed for commit resolution, `--plan`
//! drains into the planner with a mapped route instead of checking rows

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use walrus::pg::walparser::{
    BlockLocation, RelFileNode, RmId, XLogRecord, XLogRecordBlock, XLogRecordBlockHeader,
    XLogRecordHeader,
};
use walshadow::decode::datum::DatumRef;
use walshadow::emit::route::RouteSnapshot;
use walshadow::heap_decoder::view_heap_record;
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::pipeline::planner::{PlanRouteView, Planner};
use walshadow::schema::{RelAttr, RelDescriptor, RelName, ReplIdent, TEXTOID};
use walshadow::spill::{DescribedInput, HeapInput, RawRecord};
use walshadow::toast::ToastResolver;
use walshadow::xact_buffer::{DrainEntry, DrainHeap, StashResolved, XactBuffer, XactBufferConfig};

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
    #[arg(long)]
    stashed: bool,
    #[arg(long)]
    plan: bool,
}

struct Mapped(Arc<RouteSnapshot>);

impl PlanRouteView for Mapped {
    fn route_for(&mut self, _: &Arc<RelDescriptor>) -> Option<Arc<RouteSnapshot>> {
        Some(self.0.clone())
    }

    fn unmapped(&mut self, _: &RelDescriptor, _: u64) {}

    async fn apply(&mut self, _: &DrainEntry) -> Result<(), String> {
        Ok(())
    }
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
        ancestors: Vec::new(),
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "v".into(),
            type_oid: TEXTOID,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: "text".into(),
            type_byval: false,
            type_len: -1,
            type_align: 'i',
            type_storage: 'x',
            missing_default: None,
        }],
    });
    let mut routes = Mapped(RouteSnapshot::freeze(
        Arc::new(TableMapping {
            target: TableTarget::new("db", "spill_bench"),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "v".into(),
                target_type: "String".into(),
                type_pinned: false,
            }],
            tees: Vec::new(),
        }),
        Arc::default(),
        Default::default(),
    ));
    let resolver = ToastResolver::disabled();
    let payload = "x".repeat(args.payload_bytes);
    // XLOG_HEAP_INSERT block data: xl_heap_header, then one 4-byte-header text
    let mut tuple = 1u16.to_le_bytes().to_vec();
    tuple.extend_from_slice(&0u16.to_le_bytes());
    tuple.push(24);
    tuple.push(0);
    tuple.extend_from_slice(&(((4 + payload.len()) as u32) << 2).to_le_bytes());
    tuple.extend_from_slice(payload.as_bytes());
    let record = |xid: u32| XLogRecord {
        header: XLogRecordHeader {
            xact_id: xid,
            resource_manager_id: RmId::Heap as u8,
            ..Default::default()
        },
        blocks: vec![XLogRecordBlock {
            header: XLogRecordBlockHeader {
                location: BlockLocation {
                    rel: descriptor.rfn,
                    block_no: 0,
                },
                ..Default::default()
            },
            data: std::borrow::Cow::Borrowed(&tuple[..]),
            ..Default::default()
        }],
        main_data: std::borrow::Cow::Borrowed(&[1, 0, 8][..]),
        ..Default::default()
    };
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
            let record = record(xid);
            for row in 0..args.rows {
                let source_lsn = u64::from(xid) * (args.rows as u64 + 1) + row as u64;
                if args.stashed {
                    let raw = RawRecord::from_parsed(&record, source_lsn, 0);
                    buffer.stash_raw(xid, raw).await?;
                } else {
                    buffer
                        .on_input(DescribedInput {
                            input: HeapInput::from_parsed(&record, source_lsn, 1),
                            descriptor: descriptor.clone(),
                            descriptor_valid_from: 1,
                        })
                        .await?;
                }
                peak_memory = peak_memory.max(buffer.stats().bytes_in_memory);
            }
            write_time += start.elapsed();
            spill_bytes += buffer.stats().spill_bytes_active;
            let start = Instant::now();
            let commit_lsn = u64::from(xid + 1) * (args.rows as u64 + 1);
            let resolved = if args.stashed {
                StashResolved::ordinary(xid, descriptor.clone(), 1)
            } else {
                StashResolved::nothing_stashed(xid)
            };
            let mut drain = buffer
                .drain_committed(resolved, 0, commit_lsn, &[], false)
                .await?;
            let mut rows = 0;
            if args.plan {
                let mut planner = Planner::create(
                    dir.path().join(format!("plan-{xid}.spool")),
                    u64::MAX,
                    (4096, 1 << 20),
                    0,
                    &mut routes,
                    &resolver,
                );
                while let Some(batch) = drain.next_batch(1024, 1 << 20, None).await? {
                    planner.plan_batch(batch).await?;
                }
                rows = planner.seal(commit_lsn, 0)?.routed_rows;
            }
            while let Some(batch) = drain.next_batch(1024, 1 << 20, None).await? {
                if args.stashed {
                    rows += batch.heaps.len() as u64;
                    continue;
                }
                for heap in batch.heaps {
                    let DrainHeap::Input(input) = heap else {
                        panic!("ordinary rows drain as inputs");
                    };
                    assert_eq!(input.input.xid, xid);
                    assert_eq!(
                        input.input.source_lsn,
                        u64::from(xid) * (args.rows as u64 + 1) + rows
                    );
                    assert_eq!(input.descriptor.as_ref(), descriptor.as_ref());
                    assert_eq!(input.descriptor_valid_from, 1);
                    let record = input.input.to_xlog_record(descriptor.rfn);
                    let images = view_heap_record(&record, &descriptor)?;
                    let datums = &images[0].new.as_ref().expect("insert image").datums;
                    assert!(
                        matches!(datums[..], [DatumRef::Varlena(b)] if b == payload.as_bytes())
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
