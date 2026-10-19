//! Header-only pump path against the full parse over captured WAL: the
//! walk reads what the parser reads, and a stream handing sinks header-only
//! records emits the same bytes, stats, xid ceilings and verdicts, stripping
//! exactly the records no sink wants

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use walrus::pg::wal::segment::SegmentName;
use walrus::pg::wal::segment_file::open_segment_file;
use walrus::pg::walparser::{parse_record_from_bytes, read_xlog_record_header};

use crate::emit::pipeline::reorder::ReorderSink;
use crate::filter::FilterStats;
use crate::filter::header_walk;
use crate::pos::Pos;
use crate::record::{CollectingSegmentSink, Done, Record, RecordSink, SinkError};
use crate::source::streaming_walker::StreamingWalker;
use crate::source::wal_stream::WalStream;
use crate::toast::xid_ceiling::XidCeiling;
use crate::xact::xact_buffer::BufferingDecoderSink;

/// Generated fixtures are absent until `capture.sh` runs
const FIXTURES: [(&str, bool); 4] = [
    ("classify/segments/000000010000000000000001.gz", false),
    ("filter/segments/000000010000000000000002.gz", false),
    (
        "vacuum_full_pg_depend/segments/000000010000000000000002.gz",
        true,
    ),
    ("xlog_switch/segments/000000010000000000000002.gz", true),
];

/// Daemon's skip rule: neither the decoder nor the reorder stage wants it
fn unwanted(record: &Record<'_>) -> bool {
    !BufferingDecoderSink::wants(record) && !ReorderSink::wants(record)
}

#[derive(Default)]
struct Collect(Vec<Record<'static>>, bool);

impl RecordSink for Collect {
    fn header_only(&self, record: &Record<'_>) -> bool {
        self.1 && unwanted(record)
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.0.push(record.to_static());
        Box::pin(Done)
    }
}

struct Pumped {
    out: Vec<u8>,
    records: Vec<Record<'static>>,
    stats: FilterStats,
    xids: Arc<XidCeiling>,
}

async fn pump(name: SegmentName, seg: &[u8], header_only: bool) -> Pumped {
    let start = name.start_lsn(seg.len() as u64);
    let mut builder = WalStream::builder(name.timeline, seg.len() as u64, Pos::new(start)).unwrap();
    let xids = Arc::new(XidCeiling::default());
    builder.filter_mut().set_xid_ceiling(xids.clone());
    if header_only {
        builder.filter_mut().enable_header_routing();
    }
    let mut stream = builder.start();
    let mut sink = Collect(Vec::new(), header_only);
    let mut segments = CollectingSegmentSink::default();
    stream
        .push(start, seg, &mut sink, &mut segments)
        .await
        .unwrap();
    Pumped {
        out: segments.segments.pop().unwrap().1,
        records: sink.0,
        stats: *stream.filter().stats(),
        xids,
    }
}

#[tokio::test]
async fn header_only_path_matches_full_parse() {
    let (mut header_only, mut toplevel) = (0, 0);
    let mut classify = false;
    for (path, committed) in FIXTURES {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/wal")
            .join(path);
        if !path.exists() {
            assert!(
                !committed && std::env::var_os("WALSHADOW_REQUIRE_TOOLS").is_none(),
                "no fixture at {}",
                path.display()
            );
            continue;
        }
        classify |= path.ends_with("classify/segments/000000010000000000000001.gz");
        let (name, mut r) = open_segment_file(&path).await.unwrap();
        let mut seg = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut r, &mut seg)
            .await
            .unwrap();
        // Long page header's xlp_seg_size; captures trim trailing zero pages
        seg.resize(
            u32::from_le_bytes(seg[32..36].try_into().unwrap()) as usize,
            0,
        );

        let mut walker = StreamingWalker::new(seg.len());
        walker.extend(&seg);
        while let Some(completed) = walker.try_next() {
            let completed = completed.unwrap();
            let bytes = completed.logical_bytes(walker.buffer());
            let full = parse_record_from_bytes(bytes, completed.page_magic).unwrap();
            let mut rels = Vec::new();
            let header = read_xlog_record_header(&mut &*bytes).unwrap();
            let walked = header_walk::walk(header, bytes, completed.page_magic, |rel| {
                rels.push(rel);
                true
            })
            .unwrap();
            assert_eq!(walked.header, full.header);
            assert_eq!(
                (walked.toplevel_xid, walked.origin, walked.main_data_len),
                (full.toplevel_xid, full.origin, full.main_data_len)
            );
            let full_rels: Vec<_> = full.blocks.iter().map(|b| b.header.location.rel).collect();
            assert_eq!(rels, full_rels);
            toplevel += usize::from(full.toplevel_xid != 0);
        }

        let slow = pump(name, &seg, false).await;
        let fast = pump(name, &seg, true).await;
        assert!(
            slow.out == fast.out,
            "{}: filtered bytes differ",
            path.display()
        );
        assert_eq!(slow.stats, fast.stats);
        assert_eq!(slow.records.len(), fast.records.len());
        for (s, f) in slow.records.iter().zip(&fast.records) {
            assert_eq!(
                (s.source_lsn, s.next_lsn, s.route, s.defer_catalog_decode),
                (f.source_lsn, f.next_lsn, f.route, f.defer_catalog_decode)
            );
            assert_eq!(s.parsed.header, f.parsed.header);
            assert_eq!(s.parsed.toplevel_xid, f.parsed.toplevel_xid);
            assert_eq!(s.boundary_info.is_some(), f.boundary_info.is_some());
            assert_eq!(s.aborted_tree.is_some(), f.aborted_tree.is_some());
            assert_eq!(slow.xids.at(s.source_lsn), fast.xids.at(s.source_lsn));
            let payload = |r: &Record| r.parsed.blocks.len() + r.parsed.main_data.len();
            if payload(s) > 0 {
                let stripped = payload(f) == 0;
                header_only += usize::from(stripped);
                assert_eq!(stripped, unwanted(s), "record at {:#X}", s.source_lsn);
            }
        }
    }
    assert!(
        header_only > 0,
        "no fixture record took the header-only path"
    );
    // Classify workload's subtransaction logs its toplevel xid
    assert!(
        !classify || toplevel > 0,
        "no record carried a toplevel xid"
    );
}
