//! Greenfield bootstrap orchestrator.
//!
//! Wires [`BackupSource`] through [`MultiplexSink`]([`DiskLanderSink`],
//! [`PageWalkSink`]). Catalog files land on `shadow_data_dir`; user-heap
//! pages page-walk and ship `BackfillTuple`s over an mpsc to a caller-owned
//! emitter drain task.
//!
//! Sequencing:
//!
//! ```text
//! 1. Seed CatalogMap from source PG (sidecar SQL, oid >= 16384)
//! 2. Build BackupSource (Direct | ObjectStore)
//! 3. Build MultiplexSink(DiskLanderSink, PageWalkSink)
//! 4. source.run(shadow_data_dir, mux) → (StartInfo, EndInfo)
//!    - During run: catalog files Keep'd onto shadow_data_dir;
//!      user heap pages decoded → BackfillTuple → mpsc → emitter task
//! 5. Return EndInfo; daemon writes standby.signal w/
//!    recovery_target_lsn = end_lsn, starts shadow, waits replay,
//!    then rebinds WAL pump at end_lsn.
//! ```
//!
//! Catalog seed runs against source PG immediately before `source.run`
//! issues `BASE_BACKUP`. DDL during backfill is operator-quiesced; sub-second
//! seed/issue gap is indistinguishable from BASE_BACKUP's checkpoint window.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use walross::pg::walparser::{Oid, RelFileNode};

use crate::backup_page_walk::{
    BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap, PageWalkSink, PageWalkStats,
};
use crate::backup_sink::{CatalogFilenodes, DiskLanderSink, DiskLanderStats, MultiplexSink};
use crate::backup_source::{BackupSink, BackupSource, EndInfo, StartInfo};
use crate::decoder_sink::TupleObserver;
use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent, parse_array_one_element};

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    /// Orchestrator pre-creates if missing. `Shadow::start` requires it
    /// empty at boot except for what the orchestrator landed
    pub shadow_data_dir: PathBuf,
    /// Rotated catalogs from `CatalogTracker::seed_from_source` that the
    /// `rel_node < 16384` bootstrap rule misses. Empty in greenfield where
    /// seed runs after bootstrap
    pub catalog_filenodes: CatalogFilenodes,
}

impl BootstrapConfig {
    pub fn new(shadow_data_dir: PathBuf) -> Self {
        Self {
            shadow_data_dir,
            catalog_filenodes: CatalogFilenodes::new(),
        }
    }

    pub fn with_catalog_filenodes(mut self, c: CatalogFilenodes) -> Self {
        self.catalog_filenodes = c;
        self
    }
}

#[derive(Debug, Clone)]
pub struct BootstrapOutcome {
    pub start: StartInfo,
    pub end: EndInfo,
    pub disk: DiskLanderStats,
    pub page_walk: PageWalkStats,
}

/// Spawn pump on a tokio task, yield `(rx, handle)` so caller drains
/// `BackfillTuple`s concurrently. Pump drops its sender on return;
/// `rx.recv() == None` signals channel close.
///
/// ```ignore
/// let (rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map, store_toast);
/// let drained = tokio::spawn(drain_backfill(rx, observer));
/// let outcome = pump.await?.context("bootstrap pump")?;
/// let shipped = drained.await?;
/// ```
///
/// Channel is bounded ([`BOOTSTRAP_TUPLE_CHANNEL_CAP`]): PageWalkSink::chunk
/// awaits a free slot, so a slow drain backpressures the source rather than
/// letting the producer hold every tuple in memory until source completes.
///
/// Does not touch [`crate::shadow::Shadow`]; daemon owns shadow lifecycle.
/// After pump returns, caller writes `standby.signal` with
/// `recovery_target_lsn = end_lsn`, starts shadow, waits replay, then rebinds
/// WAL pump.
pub fn spawn_greenfield_bootstrap(
    cfg: BootstrapConfig,
    source: Box<dyn BackupSource>,
    catalog_map: CatalogMap,
    // Walk `pg_toast_*` pages into chunk tuples (chunk store configured) vs
    // count + skip (disabled mode, values NULL/default-filled).
    store_toast: bool,
) -> (
    mpsc::Receiver<BackfillTuple>,
    JoinHandle<Result<BootstrapOutcome>>,
) {
    // Bounded: PageWalkSink::chunk awaits a free slot, so a slow drain
    // backpressures the source instead of buffering the whole relation
    let (tx, rx) = mpsc::channel::<BackfillTuple>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
    let pump = tokio::spawn(async move {
        tokio::fs::create_dir_all(&cfg.shadow_data_dir)
            .await
            .with_context(|| {
                format!(
                    "bootstrap: create shadow data_dir {}",
                    cfg.shadow_data_dir.display()
                )
            })?;

        let lander = DiskLanderSink::new(cfg.catalog_filenodes);
        let page_walk = PageWalkSink::new(catalog_map, tx, store_toast);
        let mux = MultiplexSink::new(lander, page_walk);

        // Keep typed Arc beside erased trait-object Arc to recover stats
        // after source completes; both point at the same Mutex
        let typed: Arc<Mutex<MultiplexSink<PageWalkSink>>> = Arc::new(Mutex::new(mux));
        let erased: Arc<Mutex<dyn BackupSink>> = typed.clone();

        let data_dir = cfg.shadow_data_dir.clone();
        let (start, end) = source
            .run(data_dir, erased)
            .await
            .context("bootstrap: source.run")?;

        // Source dropped its `erased` clone on return, so `try_unwrap`
        // succeeds unless a clone leaked; else read through the Mutex
        let outcome = match Arc::try_unwrap(typed) {
            Ok(mtx) => {
                let mux = mtx.into_inner();
                let (lander, page_walk) = mux.into_inner();
                // Dropping `page_walk` closes the channel sender in `out_tx`,
                // so a concurrent drain observes channel-close on next recv
                BootstrapOutcome {
                    start,
                    end,
                    disk: lander.stats,
                    page_walk: page_walk.stats,
                }
            }
            Err(arc) => {
                let g = arc.lock().await;
                BootstrapOutcome {
                    start,
                    end,
                    disk: g.lander_stats().clone(),
                    page_walk: PageWalkStats::default(),
                }
            }
        };
        Ok(outcome)
    });
    (rx, pump)
}

/// Test/fixture wrapper: run pump to completion, collect every
/// `BackfillTuple` into a `Vec`. Production callers use
/// [`spawn_greenfield_bootstrap`] + [`drain_backfill`] to bound memory by
/// the emitter's drain rate, not the source's full tuple count
pub async fn run_greenfield_bootstrap(
    cfg: BootstrapConfig,
    source: Box<dyn BackupSource>,
    catalog_map: CatalogMap,
    store_toast: bool,
) -> Result<(BootstrapOutcome, Vec<BackfillTuple>)> {
    let (mut rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map, store_toast);
    let drain = tokio::spawn(async move {
        let mut out = Vec::new();
        while let Some(t) = rx.recv().await {
            out.push(t);
        }
        out
    });
    let outcome = pump.await.context("bootstrap pump join")??;
    let tuples = drain.await.context("bootstrap drain join")?;
    Ok((outcome, tuples))
}

/// Drain backfill tuples into a [`TupleObserver`]. Each `BackfillTuple`
/// becomes a synthetic
/// [`CommittedTuple`](crate::heap_decoder::CommittedTuple) with `op = Insert`,
/// `commit_ts = 0`, `commit_lsn = source_lsn` (BASE_BACKUP `start_lsn`).
/// Synthesises committed rows from on-disk pages, bypassing the daemon's
/// xact-buffer + decoder chain.
///
/// `PageWalkSink` emits all rows for one rfn contiguously, so drive
/// `on_xact_end` on every rfn flip: CH Native protocol forbids a new `Query`
/// while a prior INSERT's data stream is open; without per-table flushes the
/// second `send_query` races the first INSERT's body bytes and CH silently
/// drops everything on the connection.
///
/// Final `on_xact_end` after channel-close lets the transitional emitter
/// release CH state before the daemon swaps to the shadow-catalog emitter.
pub async fn drain_backfill<O: TupleObserver + ?Sized>(
    mut rx: mpsc::Receiver<BackfillTuple>,
    observer: &mut O,
) -> Result<u64> {
    let mut shipped: u64 = 0;
    let mut last_rfn: Option<RelFileNode> = None;
    let mut last_lsn: u64 = 0;
    while let Some(tuple) = rx.recv().await {
        if let Some(prev) = last_rfn
            && prev != tuple.rfn
        {
            observer.on_xact_end(last_lsn).await.map_err(|e| {
                anyhow::anyhow!("bootstrap drain: emitter rejected mid-table xact end: {e}")
            })?;
        }
        last_rfn = Some(tuple.rfn);
        last_lsn = tuple.source_lsn;
        let committed = tuple.into_committed_insert();
        observer
            .on_tuple(&committed)
            .await
            .map_err(|e| anyhow::anyhow!("bootstrap drain: emitter rejected tuple: {e}"))?;
        shipped += 1;
    }
    observer
        .on_xact_end(last_lsn)
        .await
        .map_err(|e| anyhow::anyhow!("bootstrap drain: emitter rejected xact end: {e}"))?;
    Ok(shipped)
}

/// Build a [`CatalogMap`] by enumerating every source PG user relation
/// upfront. Mirrors `ShadowCatalog::{fetch_by_rfn,fetch_attributes,
/// fetch_replident}` (shadow_catalog.rs) but eager rather than lazy by
/// filenode. Wrap in [`seed_in_snapshot`] when DDL quiescence isn't external.
/// Filenode 0 (partitioned parents, views) has no heap to page-walk; skipped.
pub async fn seed_catalog_from_source(client: &Client) -> Result<CatalogMap> {
    let db_oid = current_database_oid(client).await?;
    let rows = client
        .query(
            "SELECT \
                c.oid::oid, \
                c.relnamespace::oid, \
                n.nspname::text, \
                c.relname::text, \
                c.relkind::text, \
                c.relpersistence::text, \
                c.relreplident::text, \
                c.reltablespace::oid, \
                coalesce(pg_relation_filenode(c.oid), 0)::oid \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid >= 16384 AND c.relkind IN ('r', 't', 'm')",
            &[],
        )
        .await
        .context("bootstrap: enumerate user relations on source")?;

    let mut map = CatalogMap::new();
    for row in rows {
        let oid: Oid = row.get(0);
        let namespace_oid: Oid = row.get(1);
        let namespace_name: String = row.get(2);
        let name: String = row.get(3);
        let kind = one_char(row.get::<_, String>(4), "relkind")?;
        let persistence = one_char(row.get::<_, String>(5), "relpersistence")?;
        let replident_char = one_char(row.get::<_, String>(6), "relreplident")?;
        let spc_node: Oid = row.get(7);
        let rel_node: Oid = row.get(8);
        if rel_node == 0 {
            // No heap to page-walk (partitioned parent / view / sequence)
            continue;
        }
        let rfn = RelFileNode {
            spc_node,
            db_node: db_oid,
            rel_node,
        };
        let replident = fetch_replident(client, replident_char, oid).await?;
        let attributes = fetch_attributes(client, oid).await?;
        let qualified_name = RelDescriptor::build_qualified_name(&namespace_name, &name);
        let desc = RelDescriptor {
            rfn,
            oid,
            namespace_oid,
            namespace_name,
            name,
            qualified_name,
            kind,
            persistence,
            replident,
            attributes,
        };
        map.insert(Arc::new(desc));
    }
    tracing::info!(
        target = "walshadow::backfill_bootstrap",
        relations = map.len(),
        "catalog seed populated"
    );
    Ok(map)
}

/// Open REPEATABLE READ xact, seed CatalogMap, COMMIT. Stable snapshot
/// prevents a torn read against concurrent DDL during the seed. The
/// sub-second window before `source.run` relies on operator quiesce.
pub async fn seed_in_snapshot(client: &Client) -> Result<CatalogMap> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .context("bootstrap: open seed snapshot xact")?;
    let result = seed_catalog_from_source(client).await;
    // Read-only xact: commit even on failure, only releases the snapshot
    let _ = client.batch_execute("COMMIT").await;
    result
}

async fn fetch_replident(client: &Client, c: char, rel_oid: Oid) -> Result<ReplIdent> {
    match c {
        'd' => {
            let row = client
                .query_opt(
                    "SELECT indkey::int2[] FROM pg_index \
                     WHERE indrelid = $1 AND indisprimary = true LIMIT 1",
                    &[&rel_oid],
                )
                .await?;
            let pk_attnums = row.map(|r| r.get::<_, Vec<i16>>(0));
            Ok(ReplIdent::Default { pk_attnums })
        }
        'n' => Ok(ReplIdent::Nothing),
        'f' => Ok(ReplIdent::Full),
        'i' => {
            let row = client
                .query_one(
                    "SELECT indexrelid::oid, indkey::int2[] FROM pg_index \
                     WHERE indrelid = $1 AND indisreplident = true LIMIT 1",
                    &[&rel_oid],
                )
                .await
                .context("bootstrap: replident='i' missing pg_index row")?;
            Ok(ReplIdent::UsingIndex {
                index_oid: row.get(0),
                key_attnums: row.get(1),
            })
        }
        other => anyhow::bail!("bootstrap: unknown relreplident {other:?}"),
    }
}

async fn fetch_attributes(client: &Client, rel_oid: Oid) -> Result<Vec<RelAttr>> {
    let rows = client
        .query(
            "SELECT \
                a.attnum::int2, \
                a.attname::text, \
                a.atttypid::oid, \
                a.atttypmod::int4, \
                a.attnotnull::bool, \
                a.attisdropped::bool, \
                t.typname::text, \
                t.typbyval::bool, \
                t.typlen::int2, \
                t.typalign::text, \
                t.typstorage::text, \
                CASE WHEN a.atthasmissing THEN a.attmissingval::text END \
             FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = $1 AND a.attnum >= 1 \
             ORDER BY a.attnum",
            &[&rel_oid],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_missing: Option<String> = row.get(11);
        out.push(RelAttr {
            attnum: row.get(0),
            name: row.get(1),
            type_oid: row.get(2),
            typmod: row.get(3),
            not_null: row.get(4),
            dropped: row.get(5),
            type_name: row.get(6),
            type_byval: row.get(7),
            type_len: row.get(8),
            type_align: one_char(row.get::<_, String>(9), "typalign")?,
            type_storage: one_char(row.get::<_, String>(10), "typstorage")?,
            missing_text: raw_missing.as_deref().and_then(parse_array_one_element),
        });
    }
    Ok(out)
}

async fn current_database_oid(client: &Client) -> Result<Oid> {
    let row = client
        .query_one(
            "SELECT oid::oid FROM pg_database WHERE datname = current_database()",
            &[],
        )
        .await
        .context("bootstrap: current_database() oid lookup")?;
    Ok(row.get(0))
}

fn one_char(s: String, what: &str) -> Result<char> {
    let mut it = s.chars();
    match (it.next(), it.next()) {
        (Some(c), None) => Ok(c),
        _ => anyhow::bail!("bootstrap: expected single char for {what}, got {s:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_page_walk::{PAGE_BYTES, make_rel, synth_single_tuple_page};
    use crate::backup_source::{BackupSink, BackupSource, FileKind, FileMeta};
    use crate::heap_decoder::HeapOp;
    use async_trait::async_trait;

    #[test]
    fn with_catalog_filenodes_seeds_whitelist() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = BootstrapConfig::new(tmp.path().to_path_buf());
        assert!(cfg.catalog_filenodes.is_empty());
        let cfg = cfg.with_catalog_filenodes(CatalogFilenodes::from_iter([(5, 50000), (0, 99999)]));
        assert_eq!(cfg.catalog_filenodes.len(), 2);
        assert!(cfg.catalog_filenodes.is_catalog(5, 50000));
    }

    /// Emits a curated file-event sequence; tests orchestrator without live PG
    struct MockSource {
        files: Vec<(FileMeta, Vec<u8>)>,
        start: StartInfo,
        end: EndInfo,
    }

    #[async_trait]
    impl BackupSource for MockSource {
        async fn run(
            self: Box<Self>,
            data_dir: PathBuf,
            sink: Arc<Mutex<dyn BackupSink>>,
        ) -> Result<(StartInfo, EndInfo)> {
            {
                let mut g = sink.lock().await;
                g.start(&self.start).await?;
            }
            for (i, (meta, body)) in self.files.iter().enumerate() {
                let mut cur: &[u8] = body;
                crate::backup_source::pump_entry(
                    &mut cur,
                    meta,
                    &data_dir,
                    &sink,
                    crate::backup_source::EntryId(i as u64),
                )
                .await?;
            }
            {
                let mut g = sink.lock().await;
                g.finish(&self.end).await?;
            }
            Ok((self.start.clone(), self.end.clone()))
        }
    }

    #[tokio::test]
    async fn orchestrator_routes_catalog_lands_userheap_decodes() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();

        let synth = synth_single_tuple_page(42).to_vec();
        let files = vec![
            (
                FileMeta {
                    path: "base/5/1259".into(),
                    size: 7,
                    mode: 0o600,
                    kind: FileKind::File,
                },
                b"catalog".to_vec(),
            ),
            (
                FileMeta {
                    path: "base/5/16400".into(),
                    size: PAGE_BYTES as u64,
                    mode: 0o600,
                    kind: FileKind::File,
                },
                synth,
            ),
            (
                FileMeta {
                    path: "pg_replslot/0/state".into(),
                    size: 4,
                    mode: 0o600,
                    kind: FileKind::File,
                },
                b"slot".to_vec(),
            ),
            (
                FileMeta {
                    path: "pg_control".into(),
                    size: 3,
                    mode: 0o600,
                    kind: FileKind::File,
                },
                b"ctl".to_vec(),
            ),
        ];
        let source = MockSource {
            files,
            start: StartInfo {
                start_lsn: 0xDEAD_BEEF,
                timeline: 1,
                tablespaces: Vec::new(),
            },
            end: EndInfo {
                end_lsn: 0xFFFF_FFFF,
                timeline: 1,
            },
        };

        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(make_rel()));
        let cfg = BootstrapConfig::new(data_dir.clone());
        let (outcome, tuples) = run_greenfield_bootstrap(cfg, Box::new(source), catalog, false)
            .await
            .unwrap();

        assert_eq!(outcome.start.start_lsn, 0xDEAD_BEEF);
        assert_eq!(outcome.end.end_lsn, 0xFFFF_FFFF);

        // Catalog + pg_control landed; user heap + denylist did not
        assert!(data_dir.join("base/5/1259").exists());
        assert!(!data_dir.join("base/5/16400").exists());
        assert!(!data_dir.join("pg_replslot/0/state").exists());
        assert!(data_dir.join("pg_control").exists());

        assert_eq!(tuples.len(), 1, "exactly one synthetic tuple expected");
        let tuple = &tuples[0];
        assert_eq!(tuple.source_lsn, 0xDEAD_BEEF);
        assert_eq!(tuple.xid, 99);
        assert_eq!(tuple.columns.len(), 1);
        assert!(matches!(
            tuple.columns[0],
            Some(crate::heap_decoder::ColumnValue::Int4(42))
        ));
    }

    #[tokio::test]
    async fn drain_backfill_synthesises_inserts_into_observer() {
        use crate::decoder_sink::CollectingTupleObserver;

        let (tx, rx) = mpsc::channel::<BackfillTuple>(64);
        let rfn = RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16400,
        };
        for v in 0..3 {
            tx.send(BackfillTuple {
                rfn,
                xid: 100 + v,
                source_lsn: 0xCAFE,
                columns: vec![Some(crate::heap_decoder::ColumnValue::Int4(v as i32))],
            })
            .await
            .unwrap();
        }
        drop(tx);

        let mut observer = CollectingTupleObserver::default();
        let shipped = drain_backfill(rx, &mut observer).await.unwrap();
        assert_eq!(shipped, 3);
        assert_eq!(observer.tuples.len(), 3);
        for (i, c) in observer.tuples.iter().enumerate() {
            assert_eq!(c.commit_ts, 0);
            assert_eq!(c.commit_lsn, 0xCAFE);
            assert_eq!(c.decoded.op, HeapOp::Insert);
            assert!(c.decoded.old.is_none());
            let new = c.decoded.new.as_ref().unwrap();
            assert!(!new.partial);
            assert!(matches!(
                new.columns[0],
                Some(crate::heap_decoder::ColumnValue::Int4(v)) if v == i as i32
            ));
        }
    }

    #[derive(Default)]
    struct CountingObserver {
        on_tuple_calls: u32,
        on_xact_end_calls: u32,
    }

    impl crate::decoder_sink::TupleObserver for CountingObserver {
        fn on_tuple<'a>(
            &'a mut self,
            _committed: &'a crate::heap_decoder::CommittedTuple,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<(), crate::decoder_sink::DecoderSinkError>>
                    + Send
                    + 'a,
            >,
        > {
            self.on_tuple_calls += 1;
            Box::pin(async { Ok(()) })
        }
        fn on_xact_end<'a>(
            &'a mut self,
            commit_lsn: u64,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<u64, crate::decoder_sink::DecoderSinkError>>
                    + Send
                    + 'a,
            >,
        > {
            self.on_xact_end_calls += 1;
            Box::pin(async move { Ok(commit_lsn) })
        }
    }

    #[tokio::test]
    async fn drain_backfill_calls_on_xact_end_after_channel_closes() {
        let rfn = RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16400,
        };
        let (tx, rx) = mpsc::channel::<BackfillTuple>(64);
        for v in 0..4u32 {
            tx.send(BackfillTuple {
                rfn,
                xid: v,
                source_lsn: 1,
                columns: vec![Some(crate::heap_decoder::ColumnValue::Int4(v as i32))],
            })
            .await
            .unwrap();
        }
        drop(tx);

        let mut obs = CountingObserver::default();
        let shipped = drain_backfill(rx, &mut obs).await.unwrap();
        assert_eq!(shipped, 4);
        assert_eq!(obs.on_tuple_calls, 4);
        // One synthetic xact closes the backfill
        assert_eq!(obs.on_xact_end_calls, 1);
    }

    #[tokio::test]
    async fn drain_backfill_calls_on_xact_end_even_on_empty_channel() {
        // Sender dropped without a tuple; `on_xact_end` still fires so the
        // transitional emitter's INSERT cleanup runs unconditionally
        let (tx, rx) = mpsc::channel::<BackfillTuple>(64);
        drop(tx);
        let mut obs = CountingObserver::default();
        let shipped = drain_backfill(rx, &mut obs).await.unwrap();
        assert_eq!(shipped, 0);
        assert_eq!(obs.on_tuple_calls, 0);
        assert_eq!(obs.on_xact_end_calls, 1);
    }
}
