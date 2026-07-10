//! TOAST chunk GC — TID-death-driven sweep (`plans/TOAST.md`).
//!
//! PG drops superseded chunks in the same xact as the superseding
//! main-table op, but those `XLOG_HEAP_DELETE`s are TID-keyed (toast
//! replica identity is `nothing`) while the store is keyed
//! `(chunk_id, chunk_seq)`. The TID tracker ([`crate::toast_tid`]) resolves
//! them at commit into `(chunk_id, death commit LSN)` pairs; this sweep
//! applies the ones the emitter ack has passed. No source PG session, no
//! liveness scan: the death LSN alone bounds the dead generation.
//!
//! Safety argument, per death: fetches serve replay re-decode (starts at
//! `emitter_ack`) and fresh WAL re-referencing a stored `va_valueid`; every
//! record referencing the dead generation precedes its death record, so
//! `ack >= death_lsn` means no fetch can want rows with `lsn <= death_lsn`.
//! A reused OID's rebirth commits past the death and survives the bounded
//! delete ([`ChunkStore::gc_values`]). Deaths whose ack hasn't caught up
//! stay pending for the next sweep; deletion is idempotent, and a death is
//! journaled collected only after its store delete succeeds — a crash
//! between re-deletes nothing that matters.
//!
//! Correctness never depends on collecting (chunks are immutable per
//! generation, fetch keeps the newest); untracked leak classes are the
//! tracker's (`toast_deaths_unresolved`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use thiserror::Error;

use crate::ch_emitter::EmitterStats;
use crate::toast::{ChunkStore, ChunkStoreError};
use crate::toast_tid::{TidTracker, ValueDeath};

#[derive(Debug, Error)]
pub enum SweepError {
    /// Store or journal fault; pending deaths are retained and the next
    /// sweep retries.
    #[error("store: {0}")]
    Store(#[from] ChunkStoreError),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SweepOutcome {
    /// Store relids visited.
    pub relids: usize,
    /// Deaths ready this sweep (`death_lsn <= emitter_ack`).
    pub candidates: usize,
    /// Values whose rows the store actually deleted (a replayed death may
    /// find them already gone).
    pub deleted: u64,
}

/// One sweep task; [`ToastGc::spawn`] runs it at `interval` off the hot
/// path. Requires an armed store + tracker — without tracking there is
/// nothing to collect and the caller refuses to arm.
pub struct ToastGc {
    pub store: Arc<dyn ChunkStore>,
    pub tracker: Arc<TidTracker>,
    /// Pipeline's contiguous-done commit watermark
    /// ([`crate::pipeline::PipelineHandle::emitter_ack`]).
    pub emitter_ack: Arc<AtomicU64>,
    pub interval: Duration,
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
                        "toast GC sweep failed; deaths retained for retry",
                    ),
                }
            }
        })
    }

    /// One sweep: apply every pending death the ack watermark has passed.
    /// Counters tick here so direct callers (tests) and the task loop stay
    /// consistent.
    pub async fn sweep_once(&self) -> Result<SweepOutcome, SweepError> {
        let ack = self.emitter_ack.load(Ordering::Acquire);
        let ready = self.tracker.ready(ack).await;
        let mut outcome = SweepOutcome {
            candidates: ready.len(),
            ..SweepOutcome::default()
        };
        if !ready.is_empty() {
            let mut by_relid: std::collections::HashMap<u32, Vec<&ValueDeath>> =
                std::collections::HashMap::new();
            for d in &ready {
                by_relid.entry(d.toast_relid).or_default().push(d);
            }
            outcome.relids = by_relid.len();
            for (relid, deaths) in by_relid {
                let pairs: Vec<(u32, u64)> =
                    deaths.iter().map(|d| (d.value_id, d.death_lsn)).collect();
                outcome.deleted += self.store.gc_values(relid, &pairs).await?;
            }
            // Journal completions only after every store delete succeeded;
            // a partial failure retains all (idempotent re-delete)
            self.tracker.mark_collected(&ready).await?;
            self.stats
                .toast_gc_values_deleted
                .fetch_add(outcome.deleted, Ordering::Relaxed);
        }
        self.stats.toast_gc_sweeps.fetch_add(1, Ordering::Relaxed);
        Ok(outcome)
    }
}
