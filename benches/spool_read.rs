//! Deferred spool and plan spool write and replay, without servers.
//! `cargo bench --bench spool_read -- --rows 200000`
//! Add `--plan-only --planner --mapped --relations 8 --control-every 10000`
//! to include planning, route caching within benchmark, and row placement.
//! Excludes WAL drain, TOAST I/O, control execution, and destination insertion.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::{HashMap, HashMapExt};
use clap::Parser;
use walrus::pg::walparser::RelFileNode;
use walshadow::backup_page_walk::BackfillTuple;
use walshadow::ch_emitter::EmitterStats;
use walshadow::emit::route::RouteSnapshot;
use walshadow::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, DescribedHeap, HeapOp};
use walshadow::mapping::{TableMapping, TableTarget};
use walshadow::pipeline::decode::place_rows;
use walshadow::pipeline::plan_spool::{PlanItem, PlanWriter};
use walshadow::pipeline::planner::{PlanRouteView, Planner};
use walshadow::schema::{RelDescriptor, RelName, ReplIdent, SchemaEvent};
use walshadow::spool::DeferredSpool;
use walshadow::toast::ToastResolver;
use walshadow::xact_buffer::{DrainEntry, DrainedBatch, OrderedEvent};

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
    #[arg(long)]
    plan_only: bool,
    /// Run Planner and row placement with cached routes.
    #[arg(long)]
    planner: bool,
    #[arg(long)]
    mapped: bool,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..))]
    relations: u16,
    /// Insert unrelated schema control every N rows; use zero to disable.
    #[arg(long, default_value_t = 0)]
    control_every: usize,
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
        if !args.plan_only {
            runtime.block_on(deferred(&args, repeat))?;
        }
        runtime.block_on(plan(&args, repeat))?;
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

struct BenchRoutes {
    route: Option<Arc<RouteSnapshot>>,
    memo: HashMap<RelName, (Arc<RelDescriptor>, Option<Arc<RouteSnapshot>>)>,
    calls: usize,
}

impl PlanRouteView for BenchRoutes {
    fn route_for(&mut self, heap: &DescribedHeap) -> Option<Arc<RouteSnapshot>> {
        self.calls += 1;
        if let Some((desc, route)) = self.memo.get(&heap.descriptor.rel_name)
            && Arc::ptr_eq(desc, &heap.descriptor)
        {
            return route.clone();
        }
        let route = self.route.clone();
        self.memo.insert(
            heap.descriptor.rel_name.clone(),
            (heap.descriptor.clone(), route.clone()),
        );
        route
    }

    fn unmapped(&mut self, _: &RelDescriptor, _: u64) {}

    async fn apply(&mut self, _: &DrainEntry) -> Result<(), String> {
        self.memo.clear();
        Ok(())
    }
}

async fn plan(args: &Args, repeat: usize) -> anyhow::Result<()> {
    let dir = tempfile::tempdir_in(&args.dir)?;
    let descriptors: Vec<_> = (0..args.relations)
        .map(|i| {
            Arc::new(RelDescriptor {
                rfn: RelFileNode {
                    rel_node: 16385 + u32::from(i),
                    ..rfn()
                },
                oid: 16385 + u32::from(i),
                toast_oid: 0,
                namespace_oid: 2200,
                rel_name: RelName::new("public", &format!("spool_bench_{i}")),
                kind: 'r',
                persistence: 'p',
                ancestors: Vec::new(),
                replident: ReplIdent::Default { pk_attnums: None },
                attributes: Vec::new(),
            })
        })
        .collect();
    let route = args.mapped.then(|| {
        RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("db", "spool_bench"),
                columns: Vec::new(),
                tees: Vec::new(),
            }),
            Arc::default(),
            Default::default(),
        )
    });
    let payload = "x".repeat(args.payload_bytes);
    let make_heap = |i: usize| {
        let descriptor = &descriptors[i % descriptors.len()];
        DescribedHeap {
            decoded: DecodedHeap {
                rfn: descriptor.rfn,
                xid: 7 + (i % 4) as u32,
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
        }
    };
    let control = |i: usize| {
        (args.control_every != 0 && i != 0 && i.is_multiple_of(args.control_every)).then(|| {
            DrainEntry::Catalog(SchemaEvent::Dropped {
                oid: 1,
                rel_name: RelName::new("public", "unrelated"),
                partition: None,
            })
        })
    };
    let mut view = BenchRoutes {
        route,
        memo: HashMap::new(),
        calls: 0,
    };
    let resolver = ToastResolver::disabled();
    let start = Instant::now();
    let sealed = if args.planner {
        let mut planner = Planner::create(
            dir.path().join("plan.spool"),
            u64::MAX,
            &mut view,
            &resolver,
        );
        for first in (0..args.rows).step_by(4096) {
            let end = (first + 4096).min(args.rows);
            let mut heaps = Vec::with_capacity(end - first);
            let mut ordered_events = Vec::new();
            for i in first..end {
                if let Some(event) = control(i) {
                    ordered_events.push(OrderedEvent {
                        heap_idx: heaps.len(),
                        row_idx: 0,
                        event,
                    });
                }
                heaps.push(make_heap(i));
            }
            planner
                .plan_batch(DrainedBatch {
                    heaps,
                    ordered_events,
                    chunks: Vec::new(),
                    new_rows: Default::default(),
                    truncate_rows: Vec::new(),
                    is_final: end == args.rows,
                })
                .await?;
        }
        planner.seal(u64::MAX, 0)?
    } else {
        let mut writer = PlanWriter::create(dir.path().join("plan.spool"), u64::MAX, 1 << 20);
        for i in 0..args.rows {
            if let Some(event) = control(i) {
                view.apply(&event).await.map_err(anyhow::Error::msg)?;
                writer.push_control(event, 0);
            }
            let heap = make_heap(i);
            let table = writer.table(&heap, || view.route_for(&heap));
            writer.push_heap(heap.decoded, table)?;
        }
        writer.seal(u64::MAX, 0)?
    };
    let write = start.elapsed();
    let (size_bytes, spilled) = (sealed.size_bytes, sealed.path().is_some());
    let stats = EmitterStats::default();
    let mut staged = Vec::new();
    let mut pending = Vec::with_capacity(4096);
    let mut routed = 0;
    let mut controls = 0;
    let start = Instant::now();
    let mut rd = sealed.replay()?;
    let mut n = 0;
    while let Some(item) = rd.next_item()? {
        let control = matches!(item, PlanItem::Control(_));
        match item {
            PlanItem::Heap(h) => {
                assert_eq!(h.decoded.source_lsn, n as u64);
                assert_eq!(h.decoded.rfn, descriptors[n % descriptors.len()].rfn);
                assert_eq!(h.decoded.xid, 7 + (n % 4) as u32);
                n += 1;
                if args.planner {
                    pending.push(h);
                }
            }
            PlanItem::Control(c) => {
                assert_eq!(c.heap_idx, n);
                controls += 1;
            }
            PlanItem::Truncate { .. } => unreachable!(),
        }
        if pending.len() == 4096 || (control && !pending.is_empty()) {
            routed += place_rows(
                &mut staged,
                &stats,
                4096,
                0,
                0,
                u64::MAX,
                &rd.tables,
                std::mem::take(&mut pending),
                None,
            );
            staged.clear();
        }
    }
    if args.planner {
        routed += place_rows(
            &mut staged,
            &stats,
            4096,
            0,
            0,
            u64::MAX,
            &rd.tables,
            pending,
            None,
        );
        staged.clear();
        assert_eq!(routed, if args.mapped { args.rows as u64 } else { 0 });
    }
    let replay = start.elapsed();
    assert_eq!(n, if args.mapped { args.rows } else { 0 });
    assert_eq!(
        controls,
        args.rows
            .saturating_sub(1)
            .checked_div(args.control_every)
            .unwrap_or(0)
    );
    report(
        if args.planner {
            "plan_replay_place"
        } else {
            "plan_replay"
        },
        repeat,
        n,
        size_bytes,
        write,
        replay,
    );
    let interval = if args.control_every == 0 {
        args.rows.max(1)
    } else {
        args.control_every
    };
    let expected_calls = args.rows / interval * interval.min(descriptors.len())
        + (args.rows % interval).min(descriptors.len());
    assert_eq!(view.calls, expected_calls);
    println!(
        "plan_total repeat={repeat} total_s={:.6} route_calls={} tables={} routed={routed} spilled={}",
        (write + replay).as_secs_f64(),
        view.calls,
        rd.tables.len(),
        spilled
    );
    Ok(())
}
