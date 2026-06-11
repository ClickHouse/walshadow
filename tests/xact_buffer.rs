//! `XactBuffer` commit/drain + detoast against a live
//! shadow PG. Skipped silently if `initdb` is not on `$PATH`.
//!
//! The buffer's commit path needs
//! [`ShadowCatalog::relation_at`](walshadow::shadow_catalog::ShadowCatalog::relation_at)
//! to resolve `rfn` → `RelDescriptor` for any heap with an
//! `ExternalToast` column. Mocking that out adds a stub seam to a
//! production cache for tests; the user-pinned approach is to spin
//! up a real shadow PG with the relevant relations, look up their
//! filenodes via psql, and drive the buffer directly with
//! synthetic `DecodedHeap` records keyed on those filenodes.
//!
//! Tests parallel-safe via non-overlapping ports (`55700+`).

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use walross::pg::walparser::{RelFileNode, RmId, XLogRecord, XLogRecordHeader};
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::decoder_sink::{DecoderSinkError, TupleObserver};
use walshadow::filter::Route;
use walshadow::heap_decoder::{
    ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp, ToastPointer,
};
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::shadow_catalog::{ShadowCatalog, ShadowCatalogConfig, socket_conninfo};
use walshadow::spill::ToastChunk;
use walshadow::toast::{ToastConfig, ToastMode, ToastResolver};
use walshadow::wal_stream::Record;
use walshadow::xact_buffer::{XactBuffer, XactBufferConfig, XactBufferError, XactRecordSink};

fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn make_shadow(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("filtered"));
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("sock");
    cfg.ctl_timeout = Duration::from_secs(30);
    std::fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    std::fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

struct StopOnDrop<'a> {
    shadow: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.shadow.stop();
    }
}

fn stop_on_drop(shadow: &Shadow) -> StopOnDrop<'_> {
    StopOnDrop { shadow }
}

async fn open_catalog(shadow: &Shadow) -> ShadowCatalog {
    let cfg = shadow.config();
    let conninfo = socket_conninfo(
        cfg.socket_dir.to_str().unwrap(),
        cfg.port,
        "postgres",
        "postgres",
    );
    let cat_cfg = ShadowCatalogConfig {
        replay_timeout: Duration::from_secs(5),
        replay_poll: Duration::from_millis(20),
        ..Default::default()
    };
    ShadowCatalog::connect(&conninfo, cat_cfg)
        .await
        .expect("catalog connect")
}

fn user_relation_filenode(shadow: &Shadow, qualified: &str) -> u32 {
    shadow
        .psql_one(&format!(
            "SELECT pg_relation_filenode('{qualified}'::regclass)::int8"
        ))
        .expect("psql user filenode")
        .parse()
        .expect("filenode is integer")
}

fn user_relation_toast_oid(shadow: &Shadow, qualified: &str) -> u32 {
    // Returns the pg_class.oid of the table's TOAST relation, not the
    // filenode. The TOAST pointer's `va_toastrelid` matches this.
    shadow
        .psql_one(&format!(
            "SELECT c.reltoastrelid::int8 \
             FROM pg_class c WHERE c.oid = '{qualified}'::regclass"
        ))
        .expect("psql reltoastrelid")
        .parse()
        .expect("toastrelid is integer")
}

fn current_db_oid(shadow: &Shadow) -> u32 {
    shadow
        .psql_one("SELECT oid::int8 FROM pg_database WHERE datname = current_database()")
        .expect("psql db oid")
        .parse()
        .expect("db oid is integer")
}

fn rfn(spc: u32, db: u32, rel: u32) -> RelFileNode {
    RelFileNode {
        spc_node: spc,
        db_node: db,
        rel_node: rel,
    }
}

fn heap(
    rfn: RelFileNode,
    xid: u32,
    lsn: u64,
    op: HeapOp,
    cols: Vec<Option<ColumnValue>>,
) -> DecodedHeap {
    DecodedHeap {
        rfn,
        xid,
        source_lsn: lsn,
        op,
        new: Some(DecodedTuple {
            columns: cols,
            partial: false,
        }),
        old: None,
    }
}

fn cfg(spill_dir: std::path::PathBuf, budget: usize) -> XactBufferConfig {
    XactBufferConfig {
        xact_buffer_max: budget,
        spill_dir,
    }
}

#[derive(Default)]
struct CollectObs {
    seen: Vec<CommittedTuple>,
}

impl TupleObserver for CollectObs {
    fn on_tuple<'a>(
        &'a mut self,
        c: &'a CommittedTuple,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<(), DecoderSinkError>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            self.seen.push(c.clone());
            Ok(())
        })
    }
}

/// Spin up shadow PG with one user table `wc.things(id int, body text)`.
async fn fixture_shadow_with_things(
    port: u16,
) -> Option<(tempfile::TempDir, Shadow, ShadowCatalog, RelFileNode)> {
    if !pg_available() {
        eprintln!("skip: no initdb on PATH");
        return None;
    }
    let tmp = tempfile::tempdir().unwrap();
    let shadow = make_shadow(&tmp, port);
    shadow.initdb().expect("initdb");
    shadow.write_base_conf().expect("conf");
    shadow.start().expect("start");
    shadow
        .apply_schema_dump(
            "CREATE SCHEMA wc;\n\
             CREATE TABLE wc.things (id int4, body text);\n",
        )
        .expect("schema");
    let filenode = user_relation_filenode(&shadow, "wc.things");
    let db = current_db_oid(&shadow);
    let rfn = rfn(1663, db, filenode);
    let cat = open_catalog(&shadow).await;
    Some((tmp, shadow, cat, rfn))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_drains_in_arrival_order_and_clears_state() {
    let Some((tmp, shadow, cat, rfn)) = fixture_shadow_with_things(55701).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let cat = Arc::new(Mutex::new(cat));
    let spill_dir = tmp.path().join("spill");
    let mut b = XactBuffer::new(cfg(spill_dir, 1024)).unwrap();
    let mut obs = CollectObs::default();
    let one_col = |id: i32| {
        vec![
            Some(ColumnValue::Int4(id)),
            Some(ColumnValue::Text("x".into())),
        ]
    };
    b.on_heap(heap(rfn, 7, 100, HeapOp::Insert, one_col(1)))
        .await
        .unwrap();
    b.on_heap(heap(rfn, 7, 200, HeapOp::Update, one_col(2)))
        .await
        .unwrap();
    b.on_heap(heap(rfn, 8, 110, HeapOp::Insert, one_col(3)))
        .await
        .unwrap();
    b.commit(
        7,
        12345,
        300,
        &[],
        &cat,
        &mut obs,
        &ToastResolver::disabled(),
    )
    .await
    .unwrap();
    assert_eq!(obs.seen.len(), 2);
    assert_eq!(obs.seen[0].decoded.source_lsn, 100);
    assert_eq!(obs.seen[1].decoded.source_lsn, 200);
    assert_eq!(obs.seen[0].commit_ts, 12345);
    assert_eq!(obs.seen[1].commit_ts, 12345);
    // Commit-LSN carriage: every tuple carries the commit-record LSN so the
    // emitter can stamp its ack ceiling without re-reading the buffer.
    assert_eq!(obs.seen[0].commit_lsn, 300);
    assert_eq!(obs.seen[1].commit_lsn, 300);
    assert_eq!(b.stats().committed_xacts_total, 1);
    assert_eq!(b.stats().drain_lsn, 300);
    assert_eq!(b.stats().emitter_ack_lsn, 300);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_unknown_xid_no_ops() {
    let Some((tmp, shadow, cat, _rfn)) = fixture_shadow_with_things(55702).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let cat = Arc::new(Mutex::new(cat));
    let spill_dir = tmp.path().join("spill");
    let mut b = XactBuffer::new(cfg(spill_dir, 1024)).unwrap();
    let mut obs = CollectObs::default();
    // Ack-LSN coverage: even with no buffered records, the commit's source LSN
    // must advance both ack-LSN gauges so source's slot can recycle
    // past read-only / filter-dropped xacts.
    b.commit(
        99,
        0,
        0x9000,
        &[],
        &cat,
        &mut obs,
        &ToastResolver::disabled(),
    )
    .await
    .unwrap();
    assert_eq!(b.stats().commits_unknown_xid, 1);
    assert!(obs.seen.is_empty());
    assert_eq!(b.stats().drain_lsn, 0x9000);
    assert_eq!(b.stats().emitter_ack_lsn, 0x9000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_drains_spilled_then_in_memory_entries() {
    let Some((tmp, shadow, cat, rfn)) = fixture_shadow_with_things(55703).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let cat = Arc::new(Mutex::new(cat));
    let spill_dir = tmp.path().join("spill");
    let mut b = XactBuffer::new(cfg(spill_dir, 1024)).unwrap();
    let mut obs = CollectObs::default();
    let fat_col = vec![
        Some(ColumnValue::Int4(0)),
        Some(ColumnValue::Bytea(vec![0u8; 700])),
    ];
    let small_col = vec![
        Some(ColumnValue::Int4(0)),
        Some(ColumnValue::Text("z".into())),
    ];
    // Three big tuples first — spill engages after the second.
    for i in 0..3 {
        b.on_heap(heap(rfn, 5, 100 + i, HeapOp::Insert, fat_col.clone()))
            .await
            .unwrap();
    }
    // Then small ones that stay in memory.
    for i in 0..2 {
        b.on_heap(heap(rfn, 5, 200 + i, HeapOp::Update, small_col.clone()))
            .await
            .unwrap();
    }
    b.commit(5, 0, 250, &[], &cat, &mut obs, &ToastResolver::disabled())
        .await
        .unwrap();
    assert_eq!(obs.seen.len(), 5);
    for (i, c) in obs.seen.iter().enumerate() {
        let lsn = c.decoded.source_lsn;
        if i < 3 {
            assert!(lsn < 200, "entry {i} expected spilled (lsn<200), got {lsn}");
        } else {
            assert!(
                lsn >= 200,
                "entry {i} expected in-memory (lsn≥200), got {lsn}"
            );
        }
    }
}

/// Subxact merge: two per-xid buffers (top xid=7 + sub xid=8) drain
/// as a single merged stream ordered by `source_lsn`. The top's first
/// entry (LSN 100) precedes the sub's entry (LSN 150) which precedes
/// the top's second entry (LSN 200). Wrong-order emit would surface a
/// CDC consumer's "row materialised before its predecessor" race.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_merges_top_and_subxact_in_source_lsn_order() {
    let Some((tmp, shadow, cat, rfn)) = fixture_shadow_with_things(55708).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let cat = Arc::new(Mutex::new(cat));
    let spill_dir = tmp.path().join("spill");
    let mut b = XactBuffer::new(cfg(spill_dir, 1024)).unwrap();
    let mut obs = CollectObs::default();
    let col = |id: i32| {
        vec![
            Some(ColumnValue::Int4(id)),
            Some(ColumnValue::Text("m".into())),
        ]
    };
    b.on_heap(heap(rfn, 7, 100, HeapOp::Insert, col(1)))
        .await
        .unwrap();
    b.on_heap(heap(rfn, 8, 150, HeapOp::Insert, col(2)))
        .await
        .unwrap();
    b.on_heap(heap(rfn, 7, 200, HeapOp::Insert, col(3)))
        .await
        .unwrap();
    b.commit(
        7,
        12345,
        300,
        &[8],
        &cat,
        &mut obs,
        &ToastResolver::disabled(),
    )
    .await
    .unwrap();
    let lsns: Vec<u64> = obs.seen.iter().map(|c| c.decoded.source_lsn).collect();
    assert_eq!(lsns, vec![100, 150, 200]);
    // Per-top accounting: one bump, regardless of subxact count.
    assert_eq!(b.stats().committed_xacts_total, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detoast_concatenates_uncompressed_chunks_into_text() {
    let Some((tmp, shadow, cat, rfn)) = fixture_shadow_with_things(55704).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let toast_oid = user_relation_toast_oid(&shadow, "wc.things");
    let cat = Arc::new(Mutex::new(cat));
    let spill_dir = tmp.path().join("spill");
    let mut b = XactBuffer::new(cfg(spill_dir, 1024)).unwrap();
    let mut obs = CollectObs::default();
    // wc.things schema: (id int4, body text). Column 0 = id, column 1 = body.
    let id_col = Some(ColumnValue::Int4(1));
    let body_ptr = Some(ColumnValue::ExternalToast(ToastPointer {
        va_rawsize: (4 + 3 + 3) + 4, // ext_size + VARHDRSZ
        va_extinfo: 4 + 3 + 3,       // ext_size, no compression bits
        va_valueid: 55,
        va_toastrelid: toast_oid,
    }));
    // source_lsn=0 bypasses the shadow-replay gate in
    // `ShadowCatalog::relation_at` — shadow PG isn't in recovery in
    // this test (no `standby.signal`), so `pg_last_wal_replay_lsn()`
    // returns NULL and would otherwise time out. Matches the
    // convention in `tests/shadow_catalog.rs`.
    b.on_heap(heap(rfn, 33, 0, HeapOp::Insert, vec![id_col, body_ptr]))
        .await
        .unwrap();
    for (seq, body) in [(0u32, &b"Hell"[..]), (1, b"o, "), (2, b"wor")] {
        b.on_toast_chunk(
            ToastChunk {
                toast_relid: toast_oid,
                value_id: 55,
                chunk_seq: seq,
                source_lsn: 102 + seq as u64,
                chunk_data: body.to_vec(),
            },
            33,
        )
        .await
        .unwrap();
    }
    b.commit(
        33,
        12345,
        300,
        &[],
        &cat,
        &mut obs,
        &ToastResolver::disabled(),
    )
    .await
    .unwrap();
    assert_eq!(obs.seen.len(), 1);
    let body_col = &obs.seen[0].decoded.new.as_ref().unwrap().columns[1];
    match body_col {
        Some(ColumnValue::Text(s)) => assert_eq!(s, "Hello, wor"),
        other => panic!("expected Text after detoast, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detoast_missing_chunk_seq_errors_clearly() {
    let Some((tmp, shadow, cat, rfn)) = fixture_shadow_with_things(55705).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let toast_oid = user_relation_toast_oid(&shadow, "wc.things");
    let cat = Arc::new(Mutex::new(cat));
    let spill_dir = tmp.path().join("spill");
    let mut b = XactBuffer::new(cfg(spill_dir, 1024)).unwrap();
    let mut obs = CollectObs::default();
    let id_col = Some(ColumnValue::Int4(1));
    let body_ptr = Some(ColumnValue::ExternalToast(ToastPointer {
        va_rawsize: 8,
        va_extinfo: 6,
        va_valueid: 1,
        va_toastrelid: toast_oid,
    }));
    // source_lsn=0 to bypass the shadow-replay gate; see sibling test
    // for the rationale.
    b.on_heap(heap(rfn, 42, 0, HeapOp::Insert, vec![id_col, body_ptr]))
        .await
        .unwrap();
    // Only chunks 0 + 2 — seq 1 missing.
    for (seq, body) in [(0u32, &b"AAA"[..]), (2, b"CCC")] {
        b.on_toast_chunk(
            ToastChunk {
                toast_relid: toast_oid,
                value_id: 1,
                chunk_seq: seq,
                source_lsn: 101 + seq as u64,
                chunk_data: body.to_vec(),
            },
            42,
        )
        .await
        .unwrap();
    }
    // Gap only errors with an active store (disabled mode NULL-fills);
    // disk mode with an empty store keeps the in-xact chunks authoritative
    let emitter_cfg = EmitterConfig {
        toast: ToastConfig {
            mode: ToastMode::Disk,
            disk_dir: Some(tmp.path().join("toast-store")),
        },
        ..Default::default()
    };
    let resolver =
        ToastResolver::from_config(&emitter_cfg, Arc::new(EmitterStats::default())).unwrap();
    let err = b
        .commit(42, 0, 200, &[], &cat, &mut obs, &resolver)
        .await
        .expect_err("missing chunk surfaces");
    match err {
        XactBufferError::MissingToastChunk {
            value_id, missing, ..
        } => {
            assert_eq!(value_id, 1);
            assert_eq!(missing, 1);
        }
        other => panic!("expected MissingToastChunk, got {other:?}"),
    }
}

fn xact_record(info_op: u8, xid: u32, xact_time: i64) -> Record<'static> {
    let mut main_data = Vec::with_capacity(8);
    main_data.extend_from_slice(&xact_time.to_le_bytes());
    Record {
        parsed: XLogRecord {
            header: XLogRecordHeader {
                resource_manager_id: RmId::Xact as u8,
                info: info_op,
                xact_id: xid,
                ..Default::default()
            },
            main_data: std::borrow::Cow::Owned(main_data),
            ..Default::default()
        },
        source_lsn: 0,
        page_magic: 0xD110,
        route: Route::ToShadow,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xact_record_sink_routes_commit_and_abort() {
    use walshadow::wal_stream::RecordSink as _;
    let Some((tmp, shadow, cat, rfn)) = fixture_shadow_with_things(55706).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let cat = Arc::new(Mutex::new(cat));
    let spill_dir = tmp.path().join("spill");
    let buf = Arc::new(Mutex::new(XactBuffer::new(cfg(spill_dir, 1024)).unwrap()));
    {
        let mut b = buf.lock().await;
        let col = |id: i32| {
            vec![
                Some(ColumnValue::Int4(id)),
                Some(ColumnValue::Text("z".into())),
            ]
        };
        b.on_heap(heap(rfn, 7, 100, HeapOp::Insert, col(1)))
            .await
            .unwrap();
        b.on_heap(heap(rfn, 8, 110, HeapOp::Insert, col(2)))
            .await
            .unwrap();
        b.on_heap(heap(rfn, 9, 120, HeapOp::Insert, col(3)))
            .await
            .unwrap();
    }
    let mut sink = XactRecordSink::new(buf.clone(), cat.clone(), CollectObs::default());
    let commit = xact_record(0x00, 7, 0x1234); // XLOG_XACT_COMMIT
    sink.on_record(&commit).await.unwrap();
    let abort = xact_record(0x20, 8, 0); // XLOG_XACT_ABORT
    sink.on_record(&abort).await.unwrap();
    // PREPARE — buffer must keep xid=9 alive.
    let prepare = xact_record(0x10, 9, 0);
    sink.on_record(&prepare).await.unwrap();
    assert_eq!(sink.observer_mut().seen.len(), 1);
    assert_eq!(sink.observer_mut().seen[0].decoded.xid, 7);
    assert_eq!(sink.observer_mut().seen[0].commit_ts, 0x1234);
    let b = buf.lock().await;
    assert_eq!(b.stats().committed_xacts_total, 1);
    assert_eq!(b.stats().aborted_xacts_total, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_drops_xact_and_unlinks_spill_against_real_shadow() {
    // Same shape as the unit test, but reachable via the production
    // catalog handle so the integration suite covers the bin's
    // dispatch chain in one place.
    let Some((tmp, shadow, _cat, rfn)) = fixture_shadow_with_things(55707).await else {
        return;
    };
    let _stop = stop_on_drop(&shadow);
    let spill_dir = tmp.path().join("spill");
    let mut b = XactBuffer::new(cfg(spill_dir.clone(), 1024)).unwrap();
    let fat_col = vec![
        Some(ColumnValue::Int4(0)),
        Some(ColumnValue::Bytea(vec![0u8; 256])),
    ];
    for i in 0..10 {
        b.on_heap(heap(rfn, 11, 100 + i, HeapOp::Insert, fat_col.clone()))
            .await
            .unwrap();
    }
    assert!(b.stats().spill_xacts_active >= 1);
    b.abort(11, 200, &[]).await.unwrap();
    let leftover: Vec<_> = std::fs::read_dir(&spill_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("xid-"))
        .collect();
    assert!(leftover.is_empty());
}
