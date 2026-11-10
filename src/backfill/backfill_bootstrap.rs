//! Greenfield bootstrap orchestrator.
//!
//! Wires [`BackupSource`] through `MultiplexSink` (`DiskLanderSink`,
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
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use walrus::pg::walparser::{Oid, RelFileNode};

use crate::backfill::backup_page_walk::{
    BOOTSTRAP_TUPLE_CHANNEL_CAP, BackfillTuple, CatalogMap, PageWalkSink, PageWalkStats,
};
use crate::backfill::backup_sink::{
    CatalogFilenodes, DiskLanderSink, DiskLanderStats, MultiplexSink,
};
use crate::backfill::backup_source::{BackupSink, BackupSource, EndInfo, PumpStats, StartInfo};
use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};

/// Live counter handles, readable while the pump runs. The pump publishes
/// into these, so a caller ticks `/metrics` off them instead of waiting for
/// [`BootstrapOutcome`]
#[derive(Debug, Clone, Default)]
pub struct BootstrapProgress {
    pub pump: Arc<PumpStats>,
    pub page_walk: Arc<PageWalkStats>,
}

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    /// Orchestrator pre-creates if missing. `Shadow::start` requires it
    /// empty at boot except for what the orchestrator landed
    pub shadow_data_dir: PathBuf,
    /// Rotated catalogs from `CatalogTracker::seed_from_source` that the
    /// `rel_node < 16384` bootstrap rule misses. Empty in greenfield where
    /// seed runs after bootstrap
    pub catalog_filenodes: CatalogFilenodes,
    /// Filenodes worth decoding: mapped relations plus their TOAST heaps.
    /// `None` taps every seeded relation
    pub tap_filenodes: Option<Arc<foldhash::HashSet<(Oid, Oid)>>>,
    pub progress: BootstrapProgress,
}

impl BootstrapConfig {
    pub fn new(shadow_data_dir: PathBuf) -> Self {
        Self {
            shadow_data_dir,
            catalog_filenodes: CatalogFilenodes::new(),
            tap_filenodes: None,
            progress: BootstrapProgress::default(),
        }
    }

    pub fn with_catalog_filenodes(mut self, c: impl IntoIterator<Item = (Oid, Oid)>) -> Self {
        self.catalog_filenodes = c.into_iter().collect();
        self
    }

    /// Decline every filenode outside `set` at `begin`, so unmapped
    /// relations drain off the wire without a page decode
    pub fn with_tap_filenodes(mut self, set: Arc<foldhash::HashSet<(Oid, Oid)>>) -> Self {
        self.tap_filenodes = Some(set);
        self
    }
}

#[derive(Debug, Clone)]
pub struct BootstrapOutcome {
    pub start: StartInfo,
    pub end: EndInfo,
    pub disk: Arc<DiskLanderStats>,
    pub page_walk: Arc<PageWalkStats>,
    pub pump: Arc<PumpStats>,
}

/// Filenodes worth page-walking: main files of relations `walked` accepts,
/// plus the TOAST heap of every relation `routed` accepts. `begin` declines
/// everything else, so an unreplicated relation's bytes drain off the wire
/// without a page decode.
///
/// The two predicates differ on initial-load opt-outs: their rows never walk,
/// but their CDC updates can carry unchanged external pointers, and those
/// resolve only against a mirror the chunk pages seeded.
///
/// `None` disables the filter: a routed relation whose `reltoastrelid` the
/// seed did not resolve would otherwise have its TOAST heap skipped, and its
/// referring rows would fail resolution mid-drain. Fail open, walk everything.
pub fn tap_filenode_set(
    catalog: &CatalogMap,
    routed: impl Fn(&RelDescriptor) -> bool,
    walked: impl Fn(&RelDescriptor) -> bool,
) -> Option<foldhash::HashSet<(Oid, Oid)>> {
    use foldhash::HashSetExt;
    let by_oid: foldhash::HashMap<Oid, (Oid, Oid)> = catalog
        .descriptors()
        .map(|d| (d.oid, (d.rfn.db_node, d.rfn.rel_node)))
        .collect();
    let mut set = foldhash::HashSet::new();
    for d in catalog.descriptors() {
        if !routed(d) {
            continue;
        }
        if walked(d) {
            set.insert((d.rfn.db_node, d.rfn.rel_node));
        }
        if d.toast_oid == 0 {
            continue;
        }
        match by_oid.get(&d.toast_oid) {
            Some(&key) => {
                set.insert(key);
            }
            None => {
                tracing::warn!(
                    target: "walshadow::bootstrap",
                    relation = %d.rel_name,
                    toast_oid = d.toast_oid,
                    "toast heap absent from the catalog seed, walking every relation",
                );
                return None;
            }
        }
    }
    Some(set)
}

/// Spawn pump on a tokio task, yield `(rx, handle)` so caller drains
/// `BackfillTuple`s concurrently. Pump drops its sender on return;
/// `rx.recv() == None` signals channel close.
///
/// ```ignore
/// let (mut rx, pump) = spawn_greenfield_bootstrap(cfg, source, catalog_map, store_toast);
/// while let Some(slab) = rx.recv().await { /* consume */ }
/// let outcome = pump.await?.context("bootstrap pump")?;
/// ```
///
/// Channel is bounded ([`BOOTSTRAP_TUPLE_CHANNEL_CAP`]): PageWalkSink::chunk
/// awaits a free slot, so a slow drain backpressures the source rather than
/// letting the producer hold every tuple in memory until source completes.
///
/// Does not touch [`crate::catalog::shadow::Shadow`]; daemon owns shadow lifecycle.
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
    mpsc::Receiver<Vec<BackfillTuple>>,
    JoinHandle<Result<BootstrapOutcome>>,
) {
    // Bounded: PageWalkSink::chunk awaits a free slot, so a slow drain
    // backpressures the source instead of buffering the whole relation
    let (tx, rx) = mpsc::channel::<Vec<BackfillTuple>>(BOOTSTRAP_TUPLE_CHANNEL_CAP);
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
        let disk = lander.stats.clone();
        let mut page_walk = PageWalkSink::new(catalog_map, tx, store_toast)
            .with_stats(cfg.progress.page_walk.clone());
        if let Some(set) = cfg.tap_filenodes.clone() {
            page_walk = page_walk.with_tap_filenodes(set);
        }
        // Counters are atomics on shared handles, so the sink never has to
        // come back out of the source
        let sink: Arc<dyn BackupSink> = Arc::new(MultiplexSink::new(lander, page_walk));

        let data_dir = cfg.shadow_data_dir.clone();
        let (start, end) = source
            .run(data_dir, sink, cfg.progress.pump.clone())
            .await
            .context("bootstrap: source.run")?;

        // Dropping the last sink Arc closes the channel sender in `out_tx`,
        // so a concurrent drain observes channel-close on next recv
        Ok(BootstrapOutcome {
            start,
            end,
            disk,
            page_walk: cfg.progress.page_walk.clone(),
            pump: cfg.progress.pump.clone(),
        })
    });
    (rx, pump)
}

/// Test/fixture wrapper: run pump to completion, collect every
/// `BackfillTuple` into a `Vec`. Production callers drain
/// [`spawn_greenfield_bootstrap`] concurrently to bound memory by
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
        while let Some(slab) = rx.recv().await {
            out.extend(slab);
        }
        out
    });
    let outcome = pump.await.context("bootstrap pump join")??;
    let tuples = drain.await.context("bootstrap drain join")?;
    Ok((outcome, tuples))
}

/// Build a [`CatalogMap`] by enumerating every source PG user relation
/// upfront. Mirrors `ShadowCatalog::{fetch_by_rfn,fetch_attributes,
/// fetch_replident}` (shadow_catalog.rs) but eager rather than lazy by
/// filenode. Wrap in [`seed_in_snapshot`] when DDL quiescence isn't external.
/// Partitioned tables have no heap; they land as roots for destination
/// creation and as ancestors of their partitions
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
                c.reltoastrelid::oid, \
                coalesce(nullif(c.reltablespace, 0), \
                         (SELECT dattablespace FROM pg_database \
                          WHERE datname = current_database()))::oid, \
                coalesce(pg_relation_filenode(c.oid), 0)::oid, \
                (SELECT i.inhparent FROM pg_inherits i \
                 WHERE i.inhrelid = c.oid AND c.relispartition)::oid \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid >= 16384 AND c.relkind IN ('r', 't', 'm', 'p')",
            &[],
        )
        .await
        .context("bootstrap: enumerate user relations on source")?;

    let mut by_oid: foldhash::HashMap<Oid, (Arc<RelDescriptor>, Option<Oid>)> =
        foldhash::HashMap::default();
    for row in rows {
        let oid: Oid = row.get(0);
        let namespace_oid: Oid = row.get(1);
        let namespace_name: String = row.get(2);
        let name: String = row.get(3);
        let kind = one_char(row.get::<_, String>(4), "relkind")?;
        let persistence = one_char(row.get::<_, String>(5), "relpersistence")?;
        let replident_char = one_char(row.get::<_, String>(6), "relreplident")?;
        let toast_oid: Oid = row.get(7);
        let spc_node: Oid = row.get(8);
        let rel_node: Oid = row.get(9);
        let parent: Option<Oid> = row.get(10);
        if rel_node == 0 && kind != 'p' {
            continue;
        }
        let rfn = RelFileNode {
            spc_node,
            db_node: db_oid,
            rel_node,
        };
        let replident = fetch_replident(client, replident_char, oid).await?;
        let attributes = fetch_attributes(client, oid).await?;
        let desc = RelDescriptor {
            rfn,
            oid,
            toast_oid,
            namespace_oid,
            rel_name: RelName::new(&namespace_name, &name),
            kind,
            persistence,
            replident,
            attributes,
            ancestors: Vec::new(),
        };
        by_oid.insert(oid, (Arc::new(desc), parent));
    }

    let mut map = CatalogMap::new();
    for (desc, parent) in by_oid.values() {
        let mut desc = (**desc).clone();
        let mut next = *parent;
        while let Some((ancestor, up)) = next.and_then(|p| by_oid.get(&p)) {
            desc.ancestors.push(ancestor.clone());
            next = *up;
        }
        let desc = Arc::new(desc);
        if desc.kind == 'p' {
            map.insert_partitioned(desc);
        } else {
            map.insert(desc);
        }
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
    // Capture the PK even under FULL so the CH ORDER BY uses it, not `_lsn`
    let pk_attnums = if c == 'd' || c == 'f' {
        client
            .query_opt(
                "SELECT indkey::int2[] FROM pg_index \
                 WHERE indrelid = $1 AND indisprimary = true LIMIT 1",
                &[&rel_oid],
            )
            .await?
            .map(|r| r.get::<_, Vec<i16>>(0))
    } else {
        None
    };
    let using_index = if c == 'i' {
        client
            .query_opt(
                "SELECT indexrelid::oid, indkey::int2[] FROM pg_index \
                 WHERE indrelid = $1 AND indisreplident = true LIMIT 1",
                &[&rel_oid],
            )
            .await?
            .map(|r| (r.get(0), r.get(1)))
    } else {
        None
    };
    ReplIdent::from_parts(c, pk_attnums, using_index)
        .map_err(|e| anyhow::anyhow!("bootstrap: {e} for relation {rel_oid}"))
}

async fn fetch_attributes(client: &Client, rel_oid: Oid) -> Result<Vec<RelAttr>> {
    let rows = client.query(crate::pg::ATTR_SQL, &[&rel_oid]).await?;
    rows.iter()
        .map(|row| {
            crate::pg::RawAttr::from_row(row)
                .build()
                .map_err(|e| anyhow::anyhow!("bootstrap: {e}"))
        })
        .collect()
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
    use crate::backfill::backup_page_walk::{PAGE_BYTES, make_rel, synth_single_tuple_page};
    use crate::backfill::backup_source::{BackupSink, BackupSource, FileMeta};
    use crate::decode::datum::build;
    use async_trait::async_trait;
    use walrus::pg::backup::tarball::{Entry, Kind, Reader, Writer};

    #[test]
    fn with_catalog_filenodes_seeds_whitelist() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = BootstrapConfig::new(tmp.path().to_path_buf());
        assert!(cfg.catalog_filenodes.is_empty());
        let cfg = cfg.with_catalog_filenodes([(5, 50000), (0, 99999)]);
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
            sink: Arc<dyn BackupSink>,
            stats: Arc<PumpStats>,
        ) -> Result<(StartInfo, EndInfo)> {
            sink.start(&self.start).await?;
            let target =
                crate::backfill::backup_source::PumpTarget::new(data_dir, sink.clone(), stats);
            let mut tar = Writer::new(Vec::new());
            for (meta, body) in &self.files {
                let path = meta.path.to_string_lossy();
                let e = Entry::new(path, Kind::Regular, body.len() as u64, meta.mode, 0);
                tar.append(&e, body.as_slice()).await?;
            }
            let tar = tar.finish().await?;
            let mut archive = Reader::new(tar.as_slice());
            crate::backfill::backup_source::pump_tar_to_sink(&mut archive, &target).await?;
            sink.finish(&self.end).await?;
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
                    ..Default::default()
                },
                b"catalog".to_vec(),
            ),
            (
                FileMeta {
                    path: "base/5/16400".into(),
                    size: PAGE_BYTES as u64,
                    mode: 0o600,
                    ..Default::default()
                },
                synth,
            ),
            (
                FileMeta {
                    path: "pg_replslot/0/state".into(),
                    size: 4,
                    mode: 0o600,
                    ..Default::default()
                },
                b"slot".to_vec(),
            ),
            (
                FileMeta {
                    path: "pg_control".into(),
                    size: 3,
                    mode: 0o600,
                    ..Default::default()
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
        let columns = tuple.cells(&make_rel()).unwrap();
        assert_eq!(columns, [Some(build::int4(42))]);
    }
}
