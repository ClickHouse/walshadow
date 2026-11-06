//! Deferred spool and plan spool write and replay, without servers.
//! `cargo bench --bench spool_read -- --rows 200000`
//! Add `--plan-only --planner --mapped --relations 8 --control-every 10000`
//! to include planning, route caching within benchmark, and column encoding.
//! `--cells` plans decoded rows instead of WAL record bytes.
//! Excludes WAL drain, TOAST I/O, control execution, and destination insertion.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use foldhash::{HashMap, HashMapExt};
use walrus::pg::walparser::RelFileNode;
use walrus::pg::walparser::{
    BlockLocation, RmId, XLogRecord, XLogRecordBlock, XLogRecordBlockHeader, XLogRecordHeader,
};
use walshadow::backup_page_walk::BackfillTuple;
use walshadow::ch_emitter::EncodedChunk;
use walshadow::emit::route::RouteSnapshot;
use walshadow::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, DescribedHeap, HeapOp};
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::pipeline::plan_spool::{PlanItem, PlanWriter, ViewChange};
use walshadow::pipeline::planner::{PlanRouteView, Planner};
use walshadow::schema::{
    INT4OID, RelAttr, RelDescriptor, RelName, ReplIdent, SchemaEvent, TEXTOID,
};
use walshadow::spill::{DescribedInput, HeapInput};
use walshadow::spool::DeferredSpool;
use walshadow::toast::ToastResolver;
use walshadow::xact_buffer::{DrainEntry, DrainHeap, DrainedBatch, OrderedEvent};

#[path = "common/alloc_meter.rs"]
mod alloc_meter;

#[global_allocator]
static GLOBAL: alloc_meter::Metered<mimalloc::MiMalloc> =
    alloc_meter::Metered::new(mimalloc::MiMalloc);

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
    /// Run Planner with cached routes.
    #[arg(long)]
    planner: bool,
    /// Plan decoded rows rather than WAL record bytes.
    #[arg(long)]
    cells: bool,
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
    let base = GLOBAL.reset_peak();
    let start = Instant::now();
    for i in 0..args.rows {
        spool
            .push(BackfillTuple {
                rfn: rfn(),
                xid: i as u32,
                xmax: 0,
                infomask: 0,
                source_lsn: i as u64,
                page_lsn: 0,
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
    let spooled = GLOBAL.snapshot().since(base);
    let base = GLOBAL.reset_peak();
    let start = Instant::now();
    let mut n = 0;
    while let Some(t) = reader.next().await? {
        assert_eq!(t.xid, n as u32);
        assert!(matches!(
            t.columns.as_slice(),
            [Some(ColumnValue::Int4(v)), Some(ColumnValue::Text(s))]
                if *v == n as i32 && *s == payload
        ));
        n += 1;
    }
    reader.finish().await?;
    let read = start.elapsed();
    let replayed = GLOBAL.snapshot().since(base);
    assert_eq!(n, args.rows);
    report("deferred", repeat, n, bytes, write, read);
    println!("deferred_alloc repeat={repeat} spool {spooled} replay {replayed}");
    Ok(())
}

struct BenchRoutes {
    route: Option<Arc<RouteSnapshot>>,
    memo: HashMap<RelName, (Arc<RelDescriptor>, Option<Arc<RouteSnapshot>>)>,
    calls: usize,
}

impl PlanRouteView for BenchRoutes {
    fn route_for(&mut self, rel: &Arc<RelDescriptor>) -> Option<Arc<RouteSnapshot>> {
        self.calls += 1;
        if let Some((desc, route)) = self.memo.get(&rel.rel_name)
            && Arc::ptr_eq(desc, rel)
        {
            return route.clone();
        }
        let route = self.route.clone();
        self.memo
            .insert(rel.rel_name.clone(), (rel.clone(), route.clone()));
        route
    }

    fn unmapped(&mut self, _: &RelDescriptor, _: u64) {}

    async fn apply(&mut self, _: &DrainEntry) -> Result<(), String> {
        self.memo.clear();
        Ok(())
    }
}

fn attr(attnum: i16, type_oid: u32, type_len: i16) -> RelAttr {
    RelAttr {
        attnum,
        name: format!("c{attnum}"),
        type_oid,
        typmod: -1,
        not_null: false,
        dropped: false,
        type_name: String::new(),
        type_byval: type_len > 0,
        type_len,
        type_align: 'i',
        type_storage: if type_len > 0 { 'p' } else { 'x' },
        missing_default: None,
    }
}

/// XLOG_HEAP_INSERT block data: xl_heap_header, int4, 4-byte-header text
fn insert_tuple(id: i32, payload: &[u8]) -> Vec<u8> {
    let mut t = Vec::with_capacity(14 + payload.len());
    t.extend_from_slice(&2u16.to_le_bytes());
    t.extend_from_slice(&0u16.to_le_bytes());
    t.push(24);
    t.push(0);
    t.extend_from_slice(&id.to_le_bytes());
    t.extend_from_slice(&(((4 + payload.len()) as u32) << 2).to_le_bytes());
    t.extend_from_slice(payload);
    t
}

fn insert_record<'a>(xid: u32, rel: &RelDescriptor, tuple: &'a [u8]) -> XLogRecord<'a> {
    XLogRecord {
        header: XLogRecordHeader {
            xact_id: xid,
            resource_manager_id: RmId::Heap as u8,
            ..Default::default()
        },
        blocks: vec![XLogRecordBlock {
            header: XLogRecordBlockHeader {
                location: BlockLocation {
                    rel: rel.rfn,
                    block_no: 0,
                },
                ..Default::default()
            },
            data: std::borrow::Cow::Borrowed(tuple),
            ..Default::default()
        }],
        main_data: std::borrow::Cow::Borrowed(&[1, 0, 8][..]),
        ..Default::default()
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
                attributes: vec![attr(1, INT4OID, 4), attr(2, TEXTOID, -1)],
            })
        })
        .collect();
    let route = args.mapped.then(|| {
        let column = |attnum: i16, ty: &str| ColumnMapping {
            src_attnum: attnum,
            target_name: format!("c{attnum}"),
            target_type: ty.into(),
            type_pinned: false,
        };
        RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("db", "spool_bench"),
                columns: vec![column(1, "Int32"), column(2, "String")],
                tees: Vec::new(),
            }),
            Arc::default(),
            Default::default(),
        )
    });
    let payload = "x".repeat(args.payload_bytes);
    let xid_of = |i: usize| 7 + (i % 4) as u32;
    let make_input = |i: usize| {
        let descriptor = &descriptors[i % descriptors.len()];
        let tuple = insert_tuple(i as i32, payload.as_bytes());
        DescribedInput {
            input: HeapInput::from_parsed(
                &insert_record(xid_of(i), descriptor, &tuple),
                i as u64,
                1,
            ),
            descriptor: descriptor.clone(),
            descriptor_valid_from: 1,
        }
    };
    let make_heap = |i: usize| {
        let descriptor = &descriptors[i % descriptors.len()];
        DescribedHeap {
            decoded: DecodedHeap {
                rfn: descriptor.rfn,
                xid: xid_of(i),
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
    // Drained slices built ahead so timing covers planning alone
    let batches: Vec<DrainedBatch> = if args.planner {
        (0..args.rows)
            .step_by(4096)
            .map(|first| {
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
                    heaps.push(if args.cells {
                        DrainHeap::Cells(make_heap(i))
                    } else {
                        DrainHeap::Input(Box::new(make_input(i)))
                    });
                }
                DrainedBatch {
                    heaps,
                    ordered_events,
                    chunks: Vec::new(),
                    new_rows: Default::default(),
                    truncate_rows: Vec::new(),
                    is_final: end == args.rows,
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    let base = GLOBAL.reset_peak();
    let start = Instant::now();
    let sealed = if args.planner {
        let mut planner = Planner::create(
            dir.path().join("plan.spool"),
            u64::MAX,
            (4096, 1 << 20),
            0,
            &mut view,
            &resolver,
        );
        for batch in batches {
            planner.plan_batch(batch).await?;
        }
        planner.seal(u64::MAX, 0)?
    } else {
        let mut writer = PlanWriter::create(dir.path().join("plan.spool"), u64::MAX, 1 << 20)
            .with_segment_limits(4096, 1 << 20);
        for i in 0..args.rows {
            if let Some(event) = control(i) {
                view.apply(&event).await.map_err(anyhow::Error::msg)?;
                writer.push_control(event, 0)?;
            }
            let rel = &descriptors[i % descriptors.len()];
            let table = writer.table(rel, 1, || view.route_for(rel));
            if !writer.routed(table) {
                writer.discard(table, 1);
            } else if args.cells {
                writer.push_cells(make_heap(i).decoded, table)?;
            } else {
                let input = make_input(i);
                let record = input.input.to_xlog_record(rel.rfn);
                let images = walshadow::heap_decoder::view_heap_record(&record, rel)?;
                let change = ViewChange {
                    op: images[0].op,
                    xid: input.input.xid,
                    source_lsn: input.input.source_lsn,
                    new: images[0].new.as_ref(),
                    old: None,
                };
                writer.push_views(change, table)?;
            }
        }
        writer.seal(u64::MAX, 0)?
    };
    let write = start.elapsed();
    let planned = GLOBAL.snapshot().since(base);
    let (size_bytes, spilled) = (sealed.size_bytes, sealed.path().is_some());
    let routed = sealed.routed_rows;
    let base = GLOBAL.reset_peak();
    let start = Instant::now();
    let mut rd = sealed.replay()?;
    let mut n = 0;
    let mut controls = 0;
    let mut segments = Vec::new();
    while let Some(item) = rd.next_item()? {
        match item {
            PlanItem::Segment(s) => {
                n += s.chunks.iter().map(|c| c.rows()).sum::<usize>();
                segments.push(s.chunks);
            }
            PlanItem::Control { .. } => controls += 1,
            PlanItem::Truncate { .. } => unreachable!(),
        }
    }
    let replay = start.elapsed();
    let replayed = GLOBAL.snapshot().since(base);
    let n_segments = segments.len();
    // Encoded chunks match a reference plan encoding decoded rows
    if args.mapped {
        let mut reference = PlanWriter::create(dir.path().join("ref.spool"), u64::MAX, u64::MAX)
            .with_segment_limits(usize::MAX, usize::MAX);
        let mut tables = HashMap::new();
        for i in 0..args.rows {
            if control(i).is_some() {
                tables.clear();
            }
            let rel = &descriptors[i % descriptors.len()];
            let table = *tables
                .entry(rel.oid)
                .or_insert_with(|| reference.table(rel, 1, || view.route.clone()));
            reference.push_cells(make_heap(i).decoded, table)?;
        }
        // Chunk boundaries differ between plans; compare rows per relation
        let merged = |chunks: Vec<EncodedChunk>| {
            let mut by_rel: HashMap<u32, EncodedChunk> = HashMap::new();
            for c in chunks {
                match by_rel.entry(c.rel_oid()) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        assert!(e.get_mut().merge(c), "one layout per relation");
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(c);
                    }
                }
            }
            let mut out: Vec<_> = by_rel.into_iter().map(|(k, c)| (k, c.to_bytes())).collect();
            out.sort();
            out
        };
        let mut rrd = reference.seal(u64::MAX, 0)?.replay()?;
        let mut want = Vec::new();
        while let Some(PlanItem::Segment(s)) = rrd.next_item()? {
            want.extend(s.chunks);
        }
        assert!(
            merged(want) == merged(segments.into_iter().flatten().collect()),
            "encoded rows differ from reference"
        );
    }
    assert_eq!(n, if args.mapped { args.rows } else { 0 });
    assert_eq!(routed, n as u64);
    assert_eq!(
        controls,
        args.rows
            .saturating_sub(1)
            .checked_div(args.control_every)
            .unwrap_or(0)
    );
    report(
        if args.planner {
            "plan_replay_encode"
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
        "plan_total repeat={repeat} total_s={:.6} route_calls={} tables={} routed={routed} segments={} spilled={} plan {planned} replay {replayed}",
        (write + replay).as_secs_f64(),
        view.calls,
        rd.tables.len(),
        n_segments,
        spilled
    );
    Ok(())
}
