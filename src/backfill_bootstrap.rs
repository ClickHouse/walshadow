//! Phase 12 — greenfield bootstrap orchestrator.
//!
//! Wires a [`BackupSource`] (Direct or ObjectStore) through a
//! [`MultiplexSink`] composed of [`DiskLanderSink`] +
//! [`PageWalkSink`]. Catalog files land on `shadow_data_dir`;
//! user-heap pages page-walk and ship `BackfillTuple`s through an
//! mpsc into a caller-owned emitter drain task.
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
//! Catalog-seed snapshot binding: the seed query runs against source PG
//! immediately before `source.run` issues `BASE_BACKUP`. Per
//! [PLAN.md §Phase 12 out-of-scope](PLAN.md#phase-12--backfill-bridge),
//! DDL during backfill is operator-quiesced; the sub-second seed/issue
//! gap is operationally indistinguishable from BASE_BACKUP's own
//! checkpoint window.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use wal_rs::pg::walparser::{Oid, RelFileNode};

use crate::backup_page_walk::{BackfillTuple, CatalogMap, PageWalkSink, PageWalkStats};
use crate::backup_sink::{CatalogFilenodes, DiskLanderSink, DiskLanderStats, MultiplexSink};
use crate::backup_source::{BackupSink, BackupSource, EndInfo, StartInfo};
use crate::decoder_sink::TupleObserver;
use crate::heap_decoder::{CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent, parse_array_one_element};

/// Operator-tunable knobs.
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    /// Shadow PG's data directory. The orchestrator pre-creates if
    /// missing; the source writes catalog files into it during the
    /// pump. The daemon's startup hook (`Shadow::start`) needs the
    /// directory to be empty at boot time except for what the
    /// orchestrator landed.
    pub shadow_data_dir: PathBuf,
    /// Catalog-tracker filenode whitelist — rotated catalogs from
    /// `CatalogTracker::seed_from_source` that the bootstrap rule
    /// (`rel_node < 16384`) misses. Empty in greenfield deployments
    /// where the seed runs after bootstrap.
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

/// Result of one successful bootstrap pump.
#[derive(Debug, Clone)]
pub struct BootstrapOutcome {
    pub start: StartInfo,
    pub end: EndInfo,
    pub disk: DiskLanderStats,
    pub page_walk: PageWalkStats,
}

/// Spawn the greenfield bootstrap pump on a tokio task and yield the
/// caller a `(rx, handle)` pair so it can drain `BackfillTuple`s
/// concurrently with the source pump. The pump task drops its sender
/// half on return; `rx.recv() == None` is the channel-close signal the
/// drain task watches for.
///
/// Caller orchestration:
///
/// ```ignore
/// let (rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map);
/// let drained = tokio::spawn(drain_backfill(rx, observer));
/// let outcome = pump.await?.context("bootstrap pump")?;
/// let shipped = drained.await?;
/// ```
///
/// Streaming the drain is load-bearing for large backups: the channel
/// is unbounded by necessity (PageWalkSink::chunk is sync and would
/// `blocking_send`-panic inside the tokio runtime), so without a
/// concurrent drain the producer queues every tuple in memory until
/// the source completes.
///
/// The orchestrator does **not** touch [`crate::shadow::Shadow`] —
/// the daemon owns shadow lifecycle. After the pump returns the
/// caller writes `standby.signal` with `recovery_target_lsn = end_lsn`,
/// starts shadow, and waits for replay before rebinding the WAL pump.
pub fn spawn_greenfield_bootstrap(
    cfg: BootstrapConfig,
    source: Box<dyn BackupSource>,
    catalog_map: CatalogMap,
) -> (
    mpsc::UnboundedReceiver<BackfillTuple>,
    JoinHandle<Result<BootstrapOutcome>>,
) {
    // Unbounded so PageWalkSink::chunk (sync) can send from inside the
    // tokio runtime context without blocking_send-panic. See module
    // docs at backup_page_walk.rs::PageWalkSink::out_tx for the
    // trade-off; the drain task ahead of `rx` is what keeps the queue
    // bounded in practice.
    let (tx, rx) = mpsc::unbounded_channel::<BackfillTuple>();
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
        let page_walk = PageWalkSink::new(catalog_map, tx);
        let mux = MultiplexSink::new(lander, page_walk);

        // Hold the typed Arc alongside the trait-object Arc so we can
        // recover stats after the source completes. Both Arc clones
        // point at the same Mutex; the source sees only the erased
        // view.
        let typed: Arc<Mutex<MultiplexSink<PageWalkSink>>> = Arc::new(Mutex::new(mux));
        let erased: Arc<Mutex<dyn BackupSink>> = typed.clone();

        let data_dir = cfg.shadow_data_dir.clone();
        let (start, end) = source
            .run(data_dir, erased)
            .await
            .context("bootstrap: source.run")?;

        // Recover stats. With the source done, only `typed` should
        // still hold a strong reference (the source's `erased` clone
        // has been dropped on return). `try_unwrap` succeeds if no
        // other clone leaked; otherwise read through the Mutex.
        let outcome = match Arc::try_unwrap(typed) {
            Ok(mtx) => {
                let mux = mtx
                    .into_inner()
                    .map_err(|_| anyhow::anyhow!("bootstrap: mux mutex poisoned at teardown"))?;
                let (lander, page_walk) = mux.into_inner();
                // Dropping `page_walk` here closes the unbounded
                // channel sender held in `out_tx`, so any concurrent
                // drain task observes channel-close on its next recv.
                BootstrapOutcome {
                    start,
                    end,
                    disk: lander.stats,
                    page_walk: page_walk.stats,
                }
            }
            Err(arc) => {
                let g = arc
                    .lock()
                    .map_err(|_| anyhow::anyhow!("bootstrap: mux mutex poisoned at teardown"))?;
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

/// Convenience wrapper around [`spawn_greenfield_bootstrap`] for
/// tests + small fixtures: runs the pump to completion, collects every
/// emitted `BackfillTuple` into a `Vec`, returns the outcome alongside.
/// Production callers should use [`spawn_greenfield_bootstrap`] +
/// [`drain_backfill`] instead so memory exposure is bounded by the
/// downstream emitter's drain rate, not by the source's full tuple
/// count.
pub async fn run_greenfield_bootstrap(
    cfg: BootstrapConfig,
    source: Box<dyn BackupSource>,
    catalog_map: CatalogMap,
) -> Result<(BootstrapOutcome, Vec<BackfillTuple>)> {
    let (mut rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map);
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

/// Drain backfill tuples from the orchestrator's channel into a
/// [`TupleObserver`]. Each `BackfillTuple` becomes a synthetic
/// [`CommittedTuple`] with `op = Insert`, `commit_ts = 0`, and
/// `commit_lsn = source_lsn` (the BASE_BACKUP's `start_lsn`). The
/// daemon's xact-buffer + decoder chain isn't on this path — we are
/// the bottom of the wire, synthesising committed rows directly from
/// on-disk pages.
///
/// `PageWalkSink` emits all rows for one rfn contiguously before
/// moving on, so we drive an `on_xact_end` whenever the rfn flips.
/// CH's Native protocol forbids issuing a new `Query` (`INSERT INTO
/// table_B`) while a prior `INSERT INTO table_A` still has an open
/// data stream; without per-table flushes the second `send_query`
/// races the first INSERT's body bytes and CH silently drops everything
/// emitted on the connection.
///
/// Channel-close (pump task dropped its sender) ends the loop; the
/// final `on_xact_end` closes the last table's INSERT block so the
/// transitional emitter releases its CH state cleanly before the
/// daemon swaps to the shadow-catalog-backed emitter.
pub async fn drain_backfill<O: TupleObserver + ?Sized>(
    mut rx: mpsc::UnboundedReceiver<BackfillTuple>,
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
        let committed = backfill_to_committed(tuple);
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

/// Translate a page-walk tuple into the
/// [`CommittedTuple`](crate::heap_decoder::CommittedTuple) shape the
/// CH-emitter consumes. The synthetic insert carries the BASE_BACKUP's
/// `start_lsn` as both `source_lsn` and `commit_lsn`; `ReplacingMergeTree(_lsn)`
/// in ClickHouse collapses any duplicates the WAL-side decoder
/// re-emits for records in `[start_lsn, end_lsn]`.
fn backfill_to_committed(t: BackfillTuple) -> CommittedTuple {
    CommittedTuple {
        decoded: DecodedHeap {
            rfn: t.rfn,
            xid: t.xid,
            source_lsn: t.source_lsn,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: t.columns,
                partial: false,
            }),
            old: None,
        },
        commit_ts: 0,
        commit_lsn: t.source_lsn,
    }
}

/// Build a [`CatalogMap`] by querying source PG for every user
/// relation. Run inside a snapshot xact (`REPEATABLE READ`) when DDL
/// quiescence isn't enforced externally — see [`seed_in_snapshot`]
/// for the wrapped form.
///
/// Mirrors `ShadowCatalog::fetch_by_rfn` /
/// `ShadowCatalog::fetch_attributes` / `ShadowCatalog::fetch_replident`
/// (all in `shadow_catalog.rs`) but enumerates all user relations
/// upfront instead of resolving lazily by filenode.
///
/// Filenodes of 0 (partitioned parents, views, etc.) are skipped —
/// they have no heap to page-walk.
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
            // Partitioned parent / view / sequence without a heap; nothing
            // to page-walk for this relation
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

/// Snapshot-bound wrapper: open a REPEATABLE READ xact, seed the
/// CatalogMap inside it, COMMIT. Source PG's `BASE_BACKUP` checkpoint
/// is independent of this snapshot, but binding the catalog read to a
/// stable snapshot prevents a torn read against concurrent DDL while
/// the seed runs. The sub-second window between this COMMIT and the
/// caller's `source.run` is the operator-quiesce window mentioned in
/// PHASE12plan.md §"Catalog-seed snapshot binding".
pub async fn seed_in_snapshot(client: &Client) -> Result<CatalogMap> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .context("bootstrap: open seed snapshot xact")?;
    let result = seed_catalog_from_source(client).await;
    // Always commit, even on seed failure — the xact is read-only so
    // commit-vs-rollback is purely about releasing the snapshot.
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
    use crate::backup_page_walk::PAGE_BYTES;
    use crate::backup_source::{BackupSink, BackupSource, FileKind, FileMeta};
    use async_trait::async_trait;

    /// Mock source that emits a curated sequence of file events. Used
    /// to test the orchestrator without a live PG.
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
                let mut g = sink.lock().unwrap();
                g.start(&self.start)?;
            }
            for (meta, body) in &self.files {
                let mut cur: &[u8] = body;
                crate::backup_source::pump_entry(&mut cur, meta, &data_dir, &sink).await?;
            }
            {
                let mut g = sink.lock().unwrap();
                g.finish(&self.end)?;
            }
            Ok((self.start.clone(), self.end.clone()))
        }
    }

    /// Build an 8 KiB heap page with one int4 tuple. Salvaged inline
    /// to avoid a public surface for the test helper.
    fn synth_single_tuple_page(value: i32) -> [u8; PAGE_BYTES] {
        use crate::backup_page_walk::{LP_NORMAL, SIZE_OF_ITEM_ID, SIZE_OF_PAGE_HEADER};
        let mut page = [0u8; PAGE_BYTES];
        let tuple_off = PAGE_BYTES - 32;
        page[tuple_off..tuple_off + 4].copy_from_slice(&99u32.to_le_bytes());
        page[tuple_off + 18..tuple_off + 20].copy_from_slice(&1u16.to_le_bytes());
        page[tuple_off + 20..tuple_off + 22].copy_from_slice(&0u16.to_le_bytes());
        page[tuple_off + 22] = 24;
        page[tuple_off + 24..tuple_off + 28].copy_from_slice(&value.to_le_bytes());
        let tuple_len = 28u16;
        page[12..14].copy_from_slice(&((SIZE_OF_PAGE_HEADER + 4) as u16).to_le_bytes());
        page[14..16].copy_from_slice(&(tuple_off as u16).to_le_bytes());
        let raw = ((tuple_off as u32) & 0x7FFF)
            | (((LP_NORMAL as u32) & 0x3) << 15)
            | (((tuple_len as u32) & 0x7FFF) << 17);
        page[SIZE_OF_PAGE_HEADER..SIZE_OF_PAGE_HEADER + SIZE_OF_ITEM_ID]
            .copy_from_slice(&raw.to_le_bytes());
        page
    }

    fn make_rel() -> RelDescriptor {
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 16400,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name: "t".into(),
            qualified_name: RelDescriptor::build_qualified_name("public", "t"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: crate::heap_decoder::INT4OID,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_text: None,
            }],
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
        let (outcome, tuples) = run_greenfield_bootstrap(cfg, Box::new(source), catalog)
            .await
            .unwrap();

        // LSN handoff intact
        assert_eq!(outcome.start.start_lsn, 0xDEAD_BEEF);
        assert_eq!(outcome.end.end_lsn, 0xFFFF_FFFF);

        // Catalog landed, user heap did not, denylist did not, pg_control landed
        assert!(data_dir.join("base/5/1259").exists());
        assert!(!data_dir.join("base/5/16400").exists());
        assert!(!data_dir.join("pg_replslot/0/state").exists());
        assert!(data_dir.join("pg_control").exists());

        // Page-walked tuple captured by the drain wrapper
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

        let (tx, rx) = mpsc::unbounded_channel::<BackfillTuple>();
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

    /// Recording observer that counts tuples + xact-end calls so tests
    /// can assert `drain_backfill` closes the transitional emitter's
    /// INSERT blocks exactly once after the channel drains.
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
        let (tx, rx) = mpsc::unbounded_channel::<BackfillTuple>();
        for v in 0..4u32 {
            tx.send(BackfillTuple {
                rfn,
                xid: v,
                source_lsn: 1,
                columns: vec![Some(crate::heap_decoder::ColumnValue::Int4(v as i32))],
            })
            .unwrap();
        }
        drop(tx);

        let mut obs = CountingObserver::default();
        let shipped = drain_backfill(rx, &mut obs).await.unwrap();
        assert_eq!(shipped, 4);
        assert_eq!(obs.on_tuple_calls, 4);
        // Exactly one synthetic xact closes the backfill — drain calls
        // on_xact_end once after the receiver is drained.
        assert_eq!(obs.on_xact_end_calls, 1);
    }

    #[tokio::test]
    async fn drain_backfill_calls_on_xact_end_even_on_empty_channel() {
        // Producer drops the sender without ever pushing a tuple. The
        // synthetic xact still closes — `on_xact_end` always fires
        // after channel close so a Solution 2 transitional emitter's
        // INSERT cleanup runs unconditionally.
        let (tx, rx) = mpsc::unbounded_channel::<BackfillTuple>();
        drop(tx);
        let mut obs = CountingObserver::default();
        let shipped = drain_backfill(rx, &mut obs).await.unwrap();
        assert_eq!(shipped, 0);
        assert_eq!(obs.on_tuple_calls, 0);
        assert_eq!(obs.on_xact_end_calls, 1);
    }
}
