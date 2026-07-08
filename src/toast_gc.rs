//! TOAST chunk GC — source anti-join sweep
//! (`plans/TOAST.md`).
//!
//! PG drops superseded chunks in the same xact as the superseding
//! main-table op, but those `XLOG_HEAP_DELETE`s are TID-keyed (toast
//! replica identity is `nothing`) while the store is keyed
//! `(chunk_id, chunk_seq)` — unappliable, so dead values accumulate.
//! Correctness never depends on collecting them (chunks are immutable per
//! `va_valueid`, dedup keeps live values right); the leak is storage-only.
//!
//! Sweep, per store relid: read `pre = pg_current_wal_lsn()` then snapshot
//! `xmax` (ordered statements: any commit record ≤ `pre` then holds a xid
//! below `xmax`) **before** the scans, wait `pg_snapshot_xmin ≥ xmax` — a commit
//! record ships (chunks land, `_lsn ≤ pre`) before the xact turns
//! snapshot-visible (PG commits WAL-first, procarray last, unboundedly late
//! under sync-rep wait), so the barrier makes every commit ≤ `pre`
//! scan-visible while later commits carry `_lsn > pre`. Then read live
//! `chunk_id`s from the source toast relation; candidates = stored values −
//! live, restricted to `max(_lsn) ≤ pre` (absence at a scan's statement
//! snapshot now proves death, bounding `L_dead ≤ snapshot`; fresh commits
//! and reused-OID re-puts stay out; the anti-join runs inside the store
//! scan, so a sweep holds bitmaps + dead ids, never the stored set). Read
//! `S = pg_current_wal_lsn()` **after** the scans (`L_dead ≤ snapshot ≤
//! S`), wait `emitter_ack ≥ S`, delete rows `_lsn ≤ pre`. Once ack passes
//! `S` no fetch can want a candidate: replay re-decode starts at ack, and
//! WAL past `L_dead` cannot re-reference the value. A dropped source rel
//! rides the same argument (empty live set, drop LSN ≤ S), so orphaned
//! store tables collect fully.
//!
//! Stateless: `(candidates, pre, S)` live in memory, a crash or an expired
//! wait discards them and the next sweep recomputes; deletion is
//! idempotent. Degraded mode (source unreachable) skips the sweep and
//! ticks `toast_gc_skipped_source_unreachable` — never an error.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use futures::TryStreamExt;
use roaring::RoaringBitmap;
use thiserror::Error;
use tokio_postgres::Client;
use walrus::pg::replication::conn::PgConfig;

use crate::ch_emitter::EmitterStats;
use crate::pg::{current_wal_lsn, quote_ident, snapshot_xmax, snapshot_xmin};
use crate::source_feed::open_sql_client;
use crate::toast::{ChunkStore, ChunkStoreError, GcHorizon};

/// Ack-gate and visibility-barrier poll cadence; both clear on
/// seconds-scale events, so sub-second polling is plenty.
const ACK_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum SweepError {
    /// Source PG unreachable or a liveness query failed: degraded mode,
    /// sweep skipped (counted, never fatal).
    #[error("source: {0}")]
    Source(String),
    #[error("store: {0}")]
    Store(#[from] ChunkStoreError),
    /// `emitter_ack` did not reach `S` within `ack_wait` (idle or lagging
    /// pipeline); candidates abandoned, recomputed next sweep.
    #[error("ack {ack:#x} below sweep horizon {s:#x} after {waited:?}")]
    AckTimeout { s: u64, ack: u64, waited: Duration },
    /// `pg_snapshot_xmin` stayed below the pre-scan `xmax` for `ack_wait`
    /// (long-running write xact or sync-rep commit wait): scans could miss
    /// already-shipped commits, sweep skipped.
    #[error("snapshot xmin {xmin} below pre-scan xmax {xmax} after {waited:?}")]
    VisibilityTimeout {
        xmax: u64,
        xmin: u64,
        waited: Duration,
    },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SweepOutcome {
    /// Store relids visited.
    pub relids: usize,
    /// Dead values found (post `_lsn > pre` skip).
    pub candidates: usize,
    /// Values deleted (either mode may retain some via its horizon guard).
    pub deleted: u64,
}

/// One sweep task; [`ToastGc::spawn`] runs it at `interval` off the hot
/// path. Requires an armed store — disabled mode has nothing to collect,
/// the caller refuses to arm.
pub struct ToastGc {
    pub store: Arc<dyn ChunkStore>,
    /// Sidecar SQL rides the source-PG params; one short-lived session per
    /// sweep, so a bounced source needs no reconnect state here.
    pub source: PgConfig,
    /// Pipeline's contiguous-done commit watermark
    /// ([`crate::pipeline::PipelineHandle::emitter_ack`]).
    pub emitter_ack: Arc<AtomicU64>,
    pub interval: Duration,
    /// Bound on the ack-gate and visibility-barrier waits; expiry abandons
    /// the round's candidates.
    pub ack_wait: Duration,
    pub stats: Arc<EmitterStats>,
}

impl ToastGc {
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(self.interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Consume the immediate first tick: boot is busy enough.
            tick.tick().await;
            loop {
                tick.tick().await;
                match self.sweep_once().await {
                    Ok(o) if o.deleted > 0 => tracing::info!(
                        target: "walshadow::toast_gc",
                        relids = o.relids,
                        candidates = o.candidates,
                        deleted = o.deleted,
                        "toast GC sweep collected",
                    ),
                    Ok(o) => tracing::debug!(
                        target: "walshadow::toast_gc",
                        relids = o.relids,
                        candidates = o.candidates,
                        "toast GC sweep clean",
                    ),
                    Err(e) => tracing::warn!(
                        target: "walshadow::toast_gc",
                        error = %e,
                        "toast GC sweep skipped",
                    ),
                }
            }
        })
    }

    /// One full sweep; counters tick here so direct callers (tests) and the
    /// task loop stay consistent.
    pub async fn sweep_once(&self) -> Result<SweepOutcome, SweepError> {
        // Wall-clock floor for the disk store's mtime guard, taken before
        // any scan so `mtime < scan_start` implies the re-put's commit
        // precedes every scan snapshot.
        let scan_start = SystemTime::now();
        let client = open_sql_client(&self.source)
            .await
            .map_err(|e| self.source_err(e))?;
        // Deletable ceiling first, barrier anchor second, separate
        // statements: a statement's snapshot is taken before its target
        // list evaluates, so one combined read anchors `xmax` before `pre`
        // — a xact could take xid ≥ xmax yet write its commit record ≤ pre,
        // and the barrier (covers xids < xmax only) would pass with it
        // still invisible. Ordered reads put any commit ≤ pre under a xid
        // below the later snapshot's xmax.
        let pre = current_wal_lsn(&client)
            .await
            .map_err(|e| self.source_err(e))?;
        let xmax = snapshot_xmax(&client)
            .await
            .map_err(|e| self.source_err(e))?;
        self.wait_visible(&client, xmax).await?;
        let relids = self.store.gc_relids().await?;
        let mut live: Vec<(u32, RoaringBitmap)> = Vec::with_capacity(relids.len());
        for relid in relids {
            let set = live_chunk_ids(&client, relid)
                .await
                .map_err(|e| self.source_err(e))?;
            live.push((relid, set));
        }
        // S after every scan: each statement snapshot precedes it, so a
        // value absent at its scan has L_dead ≤ snapshot ≤ S and the ack
        // gate below covers L_dead.
        let s = current_wal_lsn(&client)
            .await
            .map_err(|e| self.source_err(e))?;

        let mut outcome = SweepOutcome {
            relids: live.len(),
            ..SweepOutcome::default()
        };
        let horizon = GcHorizon {
            lsn: pre,
            scan_start,
        };
        let mut candidates: Vec<(u32, RoaringBitmap)> = Vec::new();
        for (relid, live_set) in live {
            let dead = self
                .store
                .gc_dead_values(relid, &live_set, &horizon)
                .await?;
            if !dead.is_empty() {
                outcome.candidates += dead.len() as usize;
                candidates.push((relid, dead));
            }
        }
        if outcome.candidates > 0 {
            self.wait_ack(s).await?;
            for (relid, dead) in candidates {
                outcome.deleted += self.store.gc_delete(relid, &dead, &horizon).await?;
            }
            self.stats
                .toast_gc_values_deleted
                .fetch_add(outcome.deleted, Ordering::Relaxed);
        }
        self.stats.toast_gc_sweeps.fetch_add(1, Ordering::Relaxed);
        Ok(outcome)
    }

    fn source_err(&self, e: impl std::fmt::Display) -> SweepError {
        self.stats
            .toast_gc_skipped_source_unreachable
            .fetch_add(1, Ordering::Relaxed);
        SweepError::Source(e.to_string())
    }

    async fn wait_ack(&self, s: u64) -> Result<(), SweepError> {
        let start = tokio::time::Instant::now();
        loop {
            let ack = self.emitter_ack.load(Ordering::Acquire);
            if ack >= s {
                return Ok(());
            }
            let waited = start.elapsed();
            if waited >= self.ack_wait {
                return Err(SweepError::AckTimeout { s, ack, waited });
            }
            tokio::time::sleep(ACK_POLL).await;
        }
    }

    /// Commit visibility barrier. PG orders commit WAL-record → flush →
    /// sync-rep wait → `ProcArrayEndTransaction` (PG
    /// `src/backend/access/transam/xact.c`), so walsender can ship a commit
    /// — and its chunks land with `_lsn ≤ pre` — while every snapshot still
    /// misses it. Once `pg_snapshot_xmin ≥ xmax` each xact whose commit
    /// record precedes `pre` is scan-visible; later commits carry
    /// `_lsn > pre` and the horizon skips them.
    async fn wait_visible(&self, client: &Client, xmax: u64) -> Result<(), SweepError> {
        let start = tokio::time::Instant::now();
        loop {
            let xmin = snapshot_xmin(client)
                .await
                .map_err(|e| self.source_err(e))?;
            if xmin >= xmax {
                return Ok(());
            }
            let waited = start.elapsed();
            if waited >= self.ack_wait {
                return Err(SweepError::VisibilityTimeout { xmax, xmin, waited });
            }
            tokio::time::sleep(ACK_POLL).await;
        }
    }
}

/// Live `chunk_id`s of one toast relation. Resolves relname by OID each
/// sweep (never cached: rotation/drop safe); no row, or the OID reused
/// outside `pg_toast`, means the rel is gone ⇒ empty live set and the
/// orphaned store table collects fully.
///
/// Streamed (`query_raw`) into a roaring bitmap: peak memory stays
/// O(compressed set), never O(rows). `ORDER BY` keeps the source plan
/// streaming too (index-only over the toast PK + `Unique`, no `HashAgg`)
/// and feeds the bitmap in append order.
async fn live_chunk_ids(
    client: &Client,
    toast_relid: u32,
) -> Result<RoaringBitmap, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.oid = $1 AND n.nspname = 'pg_toast'",
            &[&toast_relid],
        )
        .await?;
    let Some(row) = row else {
        return Ok(RoaringBitmap::new());
    };
    let relname: String = row.get(0);
    let rows = client
        .query_raw(
            &format!(
                "SELECT DISTINCT chunk_id FROM pg_toast.{} ORDER BY chunk_id",
                quote_ident(&relname)
            ),
            std::iter::empty::<u32>(),
        )
        .await?;
    let mut rows = std::pin::pin!(rows);
    let mut set = RoaringBitmap::new();
    while let Some(row) = rows.try_next().await? {
        set.insert(row.get(0));
    }
    Ok(set)
}
