//! Bootstrap pump + drain throughput, no Postgres and no ClickHouse.
//!
//! Two stages the greenfield initial load is made of, measured apart:
//!
//! - `pump`: in-memory BASE_BACKUP tars → `MultiplexSink(DiskLanderSink,
//!   PageWalkSink)` → tuple channel → null drain. Covers tar parse, page
//!   framing, heap decode and the tuple-channel hop, under `--parallelism`
//!   concurrent tar parts so shared-sink contention shows up.
//! - `drain`: synthetic `BackfillTuple`s → `pipeline::bootstrap::drain` →
//!   metrics-only tail. Covers routing plus the `BatcherMsg` hop.
//!
//! ```text
//! cargo bench --bench bootstrap_pump -- \
//!     --stage pump --parts 8 --segments-per-part 4 --pages 1024 --parallelism 4
//! ```
//!
//! Must run on mimalloc, the allocator `walshadow-stream` sets: the walk
//! produces decoded values on one thread and the drain frees them on
//! another, and glibc's shared arena serializes on that pattern hard
//! enough to invert the conclusion — there the parallel pump measures
//! slower than the serial one it replaced.
//!
//! `harness = false`, so nextest lists it by answering `--list`.

// Matches `walshadow-stream`: the walk allocates decoded values on one
// thread and the drain frees them on another, and glibc's shared arena
// serializes on that pattern. A bench on a different allocator measures a
// different program
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use walrus::pg::replication::base_backup::Tablespace;
use walrus::pg::walparser::RelFileNode;

use walshadow::backfill::backup_source::{
    BackupSink, BackupSource, EndInfo, PumpStats, PumpTarget, StartInfo, pump_tar_to_sink,
};
use walshadow::backfill_bootstrap::{BootstrapConfig, spawn_greenfield_bootstrap};
use walshadow::backup_page_walk::{BackfillTuple, CatalogMap, PAGE_BYTES};
use walshadow::heap_decoder::ColumnValue;
use walshadow::mapping::{ColumnMapping, TableMapping, TableTarget};
use walshadow::pipeline::bootstrap;
use walshadow::pos::{EmitterAck, Monotone};
use walshadow::schema::{INT4OID, RelAttr, RelDescriptor, RelName, ReplIdent};
use walshadow::toast::ToastResolver;

const DB_NODE: u32 = 5;
const FIRST_FILENODE: u32 = 16400;
const START_LSN: u64 = 0x5000_0000;
/// `HeapTupleHeaderData` + pad to the 8-byte-aligned user-data offset
const TUPLE_HEADER: usize = 24;
const SIZE_OF_PAGE_HEADER: usize = 24;
const SIZE_OF_ITEM_ID: usize = 4;
const LP_NORMAL: u32 = 1;

#[derive(Clone, Copy)]
struct Shape {
    /// Concurrent tar parts, as an object-store backup is partitioned
    parts: usize,
    /// Relation segments per part, one tar entry each
    segments_per_part: usize,
    pages_per_segment: usize,
    tuples_per_page: usize,
    /// int4 columns per tuple
    columns: usize,
    parallelism: usize,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            parts: 8,
            segments_per_part: 4,
            pages_per_segment: 256,
            tuples_per_page: 100,
            columns: 8,
            parallelism: 4,
        }
    }
}

impl Shape {
    fn segments(self) -> usize {
        self.parts * self.segments_per_part
    }

    fn tuples(self) -> u64 {
        (self.segments() * self.pages_per_segment * self.tuples_per_page) as u64
    }

    fn bytes(self) -> u64 {
        (self.segments() * self.pages_per_segment * PAGE_BYTES) as u64
    }
}

fn descriptor(filenode: u32, columns: usize) -> Arc<RelDescriptor> {
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: DB_NODE,
            rel_node: filenode,
        },
        oid: filenode,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", &format!("t{filenode}")),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: (0..columns)
            .map(|i| RelAttr {
                attnum: i as i16 + 1,
                name: format!("c{i}"),
                type_oid: INT4OID,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_text: None,
            })
            .collect(),
    }
    .into()
}

fn mapping(filenode: u32, columns: usize) -> TableMapping {
    TableMapping {
        target: TableTarget::new("bench", &format!("t{filenode}")),
        columns: (0..columns)
            .map(|i| ColumnMapping {
                src_attnum: i as i16 + 1,
                target_name: format!("c{i}"),
                target_type: "Int32".into(),
            })
            .collect(),
    }
}

/// One 8 KiB heap page carrying `tuples` live int4-only tuples, laid out
/// as PG writes them: line pointers up from the header, tuple bodies down
/// from the page end.
fn heap_page(tuples: usize, columns: usize, seed: i32) -> Vec<u8> {
    let body = TUPLE_HEADER + 4 * columns;
    let mut page = vec![0u8; PAGE_BYTES];
    let mut written = 0usize;
    for i in 0..tuples {
        let off = PAGE_BYTES - (i + 1) * body;
        if off < SIZE_OF_PAGE_HEADER + (i + 1) * SIZE_OF_ITEM_ID {
            break;
        }
        let xmin = 100u32 + i as u32;
        page[off..off + 4].copy_from_slice(&xmin.to_le_bytes());
        page[off + 18..off + 20].copy_from_slice(&(columns as u16).to_le_bytes());
        page[off + 20..off + 22].copy_from_slice(&0u16.to_le_bytes());
        page[off + 22] = TUPLE_HEADER as u8;
        for c in 0..columns {
            let at = off + TUPLE_HEADER + c * 4;
            let v = seed + (i as i32) * 31 + c as i32;
            page[at..at + 4].copy_from_slice(&v.to_le_bytes());
        }
        let slot = SIZE_OF_PAGE_HEADER + i * SIZE_OF_ITEM_ID;
        let raw = ((off as u32) & 0x7FFF) | (LP_NORMAL << 15) | (((body as u32) & 0x7FFF) << 17);
        page[slot..slot + SIZE_OF_ITEM_ID].copy_from_slice(&raw.to_le_bytes());
        written = i + 1;
    }
    let pd_lower = SIZE_OF_PAGE_HEADER + written * SIZE_OF_ITEM_ID;
    let pd_upper = PAGE_BYTES - written * body;
    page[12..14].copy_from_slice(&(pd_lower as u16).to_le_bytes());
    page[14..16].copy_from_slice(&(pd_upper as u16).to_le_bytes());
    page
}

/// One tar part holding `segments_per_part` `base/<db>/<filenode>` entries.
async fn build_part(shape: Shape, part: usize) -> Vec<u8> {
    use tokio::io::AsyncWriteExt;

    let mut builder = tokio_tar::Builder::new(Vec::new());
    for s in 0..shape.segments_per_part {
        let filenode = FIRST_FILENODE + (part * shape.segments_per_part + s) as u32;
        let mut body = Vec::with_capacity(shape.pages_per_segment * PAGE_BYTES);
        for p in 0..shape.pages_per_segment {
            body.extend_from_slice(&heap_page(
                shape.tuples_per_page,
                shape.columns,
                (p * 1_000) as i32,
            ));
        }
        let mut header = tokio_tar::Header::new_gnu();
        header
            .set_path(format!("base/{DB_NODE}/{filenode}"))
            .unwrap();
        header.set_size(body.len() as u64);
        header.set_mode(0o600);
        header.set_entry_type(tokio_tar::EntryType::Regular);
        header.set_cksum();
        builder
            .append(&header, std::io::Cursor::new(body))
            .await
            .unwrap();
    }
    builder.finish().await.unwrap();
    let mut out = builder.into_inner().await.unwrap();
    out.flush().await.unwrap();
    out
}

/// Object-store-shaped source over pre-built tars: one `pump_tar_to_sink`
/// per part, `parallelism` in flight, so the shared sink sees the same
/// interleaving a real fan-out gives it.
struct MemSource {
    parts: Vec<Vec<u8>>,
    parallelism: usize,
}

#[async_trait::async_trait]
impl BackupSource for MemSource {
    async fn run(
        self: Box<Self>,
        data_dir: std::path::PathBuf,
        sink: Arc<dyn BackupSink>,
        stats: Arc<PumpStats>,
    ) -> anyhow::Result<(StartInfo, EndInfo)> {
        use futures::{StreamExt, TryStreamExt};

        let start = StartInfo {
            start_lsn: START_LSN,
            timeline: 1,
            tablespaces: Vec::<Tablespace>::new(),
        };
        let end = EndInfo {
            end_lsn: START_LSN + 0x1000,
            timeline: 1,
        };
        sink.start(&start).await?;
        let target = Arc::new(PumpTarget::new(data_dir, sink.clone(), stats));
        let MemSource { parts, parallelism } = *self;
        futures::stream::iter(parts)
            .map(|blob| {
                let target = target.clone();
                async move {
                    let mut archive = tokio_tar::Archive::new(std::io::Cursor::new(blob));
                    pump_tar_to_sink(&mut archive, &target).await
                }
            })
            .buffer_unordered(parallelism)
            .try_collect::<Vec<_>>()
            .await?;
        sink.finish(&end).await?;
        Ok((start, end))
    }
}

struct Report {
    label: String,
    elapsed_secs: f64,
    tuples: u64,
    bytes: u64,
    detail: Vec<(&'static str, f64)>,
}

impl Report {
    fn print(&self) {
        println!(
            "{:<10} {:>8.3}s  {:>10.1} MiB/s  {:>12.0} tuples/s  ({} tuples, {:.1} MiB)",
            self.label,
            self.elapsed_secs,
            self.bytes as f64 / self.elapsed_secs / (1 << 20) as f64,
            self.tuples as f64 / self.elapsed_secs,
            self.tuples,
            self.bytes as f64 / (1 << 20) as f64,
        );
        for (name, v) in &self.detail {
            println!("    {name:<26} {v:>10.3}");
        }
    }
}

async fn bench_pump(shape: Shape) -> Report {
    let mut parts = Vec::with_capacity(shape.parts);
    for p in 0..shape.parts {
        parts.push(build_part(shape, p).await);
    }

    let mut catalog = CatalogMap::new();
    for i in 0..shape.segments() {
        catalog.insert(descriptor(FIRST_FILENODE + i as u32, shape.columns));
    }

    let tmp = tempfile::tempdir().unwrap();
    let cfg = BootstrapConfig::new(tmp.path().join("data"));
    let progress = cfg.progress.clone();
    let source = Box::new(MemSource {
        parts,
        parallelism: shape.parallelism,
    });

    let started = Instant::now();
    let (mut rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog, false);
    let drained = tokio::spawn(async move {
        let mut n = 0u64;
        while let Some(slab) = rx.recv().await {
            // Touch the payload so the decode can't be optimised away
            n += slab.iter().map(|t| t.columns.len() as u64).sum::<u64>();
        }
        n
    });
    pump.await.unwrap().unwrap();
    let touched = drained.await.unwrap();
    let elapsed = started.elapsed().as_secs_f64();

    let ld = |a: &AtomicU64| a.load(Ordering::Relaxed) as f64;
    Report {
        label: "pump".into(),
        elapsed_secs: elapsed,
        tuples: touched / shape.columns as u64,
        bytes: ld(&progress.pump.bytes_tapped) as u64,
        detail: vec![
            ("pages_walked", ld(&progress.page_walk.pages_walked)),
            ("decode_secs", ld(&progress.page_walk.decode_nanos) / 1e9),
            ("tap_secs", ld(&progress.pump.sink_chunk_nanos) / 1e9),
            (
                "channel_block_secs",
                ld(&progress.page_walk.channel_block_nanos) / 1e9,
            ),
        ],
    }
}

async fn bench_drain(shape: Shape) -> Report {
    let rels: Vec<Arc<RelDescriptor>> = (0..shape.segments())
        .map(|i| descriptor(FIRST_FILENODE + i as u32, shape.columns))
        .collect();
    let mut catalog = CatalogMap::new();
    let mut tables = std::collections::HashMap::new();
    for r in &rels {
        catalog.insert(r.clone());
        tables.insert(r.rel_name.clone(), mapping(r.oid, shape.columns));
    }
    let handle = walshadow::mapping::mapping_handle(tables.into_iter().collect());

    let (msg_tx, ack, tail) =
        walshadow::pipeline::tail::spawn_null(Arc::new(Monotone::<EmitterAck>::new(0)));
    let (tup_tx, tup_rx) = tokio::sync::mpsc::channel::<Vec<BackfillTuple>>(16);

    let per_rel = shape.pages_per_segment as u64 * shape.tuples_per_page as u64;
    let columns = shape.columns;
    let feeder = tokio::spawn(async move {
        for r in rels {
            // Slab at the walk's grain so the drain sees production shape
            for slab_start in (0..per_rel).step_by(shape.tuples_per_page * 16) {
                let end = (slab_start + (shape.tuples_per_page * 16) as u64).min(per_rel);
                let slab: Vec<BackfillTuple> = (slab_start..end)
                    .map(|i| BackfillTuple {
                        rfn: r.rfn,
                        xid: 100 + i as u32,
                        xmax: 0,
                        infomask: 0,
                        source_lsn: START_LSN,
                        blkno: (i / 100) as u32,
                        offnum: (i % 100) as u16 + 1,
                        columns: (0..columns)
                            .map(|c| Some(ColumnValue::Int4(i as i32 + c as i32)))
                            .collect(),
                    })
                    .collect();
                if tup_tx.send(slab).await.is_err() {
                    return;
                }
            }
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let stats = Arc::new(walshadow::ch_emitter::EmitterStats::default());
    let started = Instant::now();
    let outcome = bootstrap::drain(
        tup_rx,
        catalog,
        handle,
        msg_tx.clone(),
        ack.clone(),
        stats.clone(),
        ToastResolver::disabled(),
        walshadow::spool::DeferredSpool::new(
            tmp.path().join("deferred.bin"),
            walshadow::spool::DEFERRED_SPOOL_MEM_MAX,
        ),
        Default::default(),
        None,
        std::collections::HashSet::new(),
    )
    .await
    .unwrap();
    let elapsed = started.elapsed().as_secs_f64();
    feeder.await.unwrap();
    drop(msg_tx);
    drop(ack);
    tail.join().await;

    Report {
        label: "drain".into(),
        elapsed_secs: elapsed,
        tuples: outcome.rows_routed,
        // Int4 payload only; the interesting rate here is rows, not bytes
        bytes: outcome.rows_routed * 4 * shape.columns as u64,
        detail: vec![("seqs", outcome.next_seq as f64)],
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: bootstrap_pump [--list] [--stage pump|drain|all] [--parts N] \
         [--segments-per-part N] [--pages N] [--tuples-per-page N] [--columns N] \
         [--parallelism N]"
    );
    std::process::exit(2)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // nextest --all-targets lists benches by running them with --list
    if args.iter().any(|a| a == "--list") {
        println!("bootstrap_pump: benchmark");
        return;
    }
    let mut shape = Shape::default();
    let mut stage = "all".to_string();
    let mut i = 0;
    while i < args.len() {
        let val = |s: &Option<&String>| -> usize {
            s.and_then(|v| v.parse().ok()).unwrap_or_else(|| usage())
        };
        let next = args.get(i + 1);
        match args[i].as_str() {
            "--stage" => stage = next.cloned().unwrap_or_else(|| usage()),
            "--parts" => shape.parts = val(&next).max(1),
            "--segments-per-part" => shape.segments_per_part = val(&next).max(1),
            "--pages" => shape.pages_per_segment = val(&next).max(1),
            "--tuples-per-page" => shape.tuples_per_page = val(&next).max(1),
            "--columns" => shape.columns = val(&next).max(1),
            "--parallelism" => shape.parallelism = val(&next).max(1),
            // cargo bench passes --bench through
            "--bench" => {
                i += 1;
                continue;
            }
            _ => usage(),
        }
        i += 2;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    println!(
        "shape: {} segments x {} pages x {} tuples x {} int4 cols, {} parts, parallelism {}",
        shape.segments(),
        shape.pages_per_segment,
        shape.tuples_per_page,
        shape.columns,
        shape.parts,
        shape.parallelism,
    );
    println!(
        "       {:.1} MiB of heap, {} tuples",
        shape.bytes() as f64 / (1 << 20) as f64,
        shape.tuples(),
    );
    if stage == "pump" || stage == "all" {
        rt.block_on(bench_pump(shape)).print();
    }
    if stage == "drain" || stage == "all" {
        rt.block_on(bench_drain(shape)).print();
    }
}
