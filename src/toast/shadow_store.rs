//! TOAST values read out of shadow PostgreSQL instead of a ClickHouse mirror.
//!
//! Shadow already replays source WAL. Carrying the physical TOAST heaps and
//! indexes too makes every external value a local index lookup, which removes
//! the chunk mirror from both the write path (no puts at all) and the read
//! path. Selected by `[toast] backend = "shadow"`; design in
//! `plans/shadow_toast.md`. Live CDC reads through this; a greenfield
//! bootstrap does not yet, because its deferred referrers resolve before
//! shadow recovery starts.
//!
//! Read-only by construction. The mirror's writes exist to reconstruct history
//! ClickHouse would not otherwise have; shadow *is* that history, so a
//! `ToastRow` has nowhere to go and every write method refuses rather than
//! silently succeeding.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::ops::bridge::{Bridge, FetchedChunks, MAX_FETCH_VALUES, ToastSnapshot};
use crate::toast::{ChunkStore, ChunkStoreError, FetchedValue, ToastRow};

/// Round-trip payload budget. One value can be `inline_value_max` (64 MiB by
/// default) on its own, so a batch takes at least one regardless and this only
/// bounds how many more join it
const FETCH_REQUEST_BYTES: usize = 64 << 20;

/// Poll cadence while waiting for shadow replay to cover a read
const REPLAY_POLL: Duration = Duration::from_millis(20);
/// Ceiling on that wait. Generous because it is bounded by shadow applying
/// bytes already dispatched to it, not by anything downstream; exceeding it
/// means shadow stopped applying, which is fatal either way
const REPLAY_WAIT_MAX: Duration = Duration::from_secs(60);

/// Bridge a resolver is built around before the PostgreSQL behind it exists.
/// Bootstrap builds the resolver, clones it into the gate, drain lanes and
/// tails, and only later has an instance to read from
pub type LateBridge = Arc<tokio::sync::OnceCell<Arc<Bridge>>>;

pub struct ShadowToastStore {
    bridge: LateBridge,
    snapshot: ToastSnapshot,
    replay_wait_max: Duration,
}

impl ShadowToastStore {
    pub fn new(bridge: Arc<Bridge>) -> Self {
        let cell = LateBridge::default();
        cell.set(bridge).ok();
        Self::late(cell)
    }

    /// Store over a bridge bound later. A read before then parks, so an
    /// ordering mistake surfaces as a timeout naming what it waited for
    /// rather than as a fetch against a socket that is not there
    pub fn late(bridge: LateBridge) -> Self {
        Self {
            bridge,
            snapshot: ToastSnapshot::Toast,
            replay_wait_max: REPLAY_WAIT_MAX,
        }
    }

    pub fn with_replay_wait_max(mut self, max: Duration) -> Self {
        self.replay_wait_max = max;
        self
    }

    /// Bound bridge, or a park until one is. Same budget as the replay wait:
    /// both are "shadow is not ready yet"
    async fn bridge(&self) -> Result<&Arc<Bridge>, ChunkStoreError> {
        if let Some(b) = self.bridge.get() {
            return Ok(b);
        }
        let deadline = Instant::now() + self.replay_wait_max;
        loop {
            tokio::time::sleep(REPLAY_POLL).await;
            if let Some(b) = self.bridge.get() {
                return Ok(b);
            }
            if Instant::now() >= deadline {
                return Err(ChunkStoreError::Shadow(format!(
                    "no PostgreSQL bound to read values from after {:?}",
                    self.replay_wait_max
                )));
            }
        }
    }

    /// Park until shadow has applied through `through`.
    ///
    /// Safe to block on: `WalStream` dispatches a record's wire bytes before
    /// awaiting the record sink, so by the time anything asks for a value at
    /// `through` those bytes are already on their way to shadow. The wait
    /// needs no further pump progress, which is the same argument
    /// `BoundaryHoldSink` rests on.
    ///
    /// Polls rather than asking the worker to wait: the bridge serves one
    /// request at a time, so a worker-side wait would stall the catalog reads
    /// a publication hold depends on
    /// Returns the floor to send with the request: the caller's position on a
    /// standby, so the worker re-checks it after the poll, and none on a
    /// primary, which has no replay position to check against
    async fn await_replay(&self, bridge: &Bridge, through: u64) -> Result<u64, ChunkStoreError> {
        // A primary has no replay position and reports 0, which is what the
        // bootstrap oracle is. Nothing to wait for: its files are staged
        // complete before it starts serving
        if through == 0 || !bridge.info().is_some_and(|i| i.in_recovery) {
            return Ok(0);
        }
        let deadline = Instant::now() + self.replay_wait_max;
        loop {
            let at = bridge
                .replay_lsn()
                .await
                .map_err(|e| ChunkStoreError::Shadow(format!("replay position: {e}")))?;
            if at >= through {
                return Ok(through);
            }
            if Instant::now() >= deadline {
                return Err(ChunkStoreError::Shadow(format!(
                    "shadow replay stuck at {at:X}, value needs {through:X} \
                     after {:?}",
                    self.replay_wait_max
                )));
            }
            tokio::time::sleep(REPLAY_POLL).await;
        }
    }

    /// `SnapshotAny` surfaces generations `SnapshotToast` hides, which is how
    /// value-id reuse becomes visible instead of silently resolving. Reads
    /// under it are for measurement, not production
    pub fn with_snapshot(mut self, snapshot: ToastSnapshot) -> Self {
        self.snapshot = snapshot;
        self
    }
}

#[async_trait]
impl ChunkStore for ShadowToastStore {
    fn accepts_writes(&self) -> bool {
        false
    }

    async fn put(&self, _rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        Err(ChunkStoreError::ReadOnly("put"))
    }

    async fn fetch(
        &self,
        toast_relid: u32,
        value_id: u32,
        max_lsn: u64,
        expected_size: usize,
    ) -> Result<FetchedValue, ChunkStoreError> {
        Ok(self
            .fetch_many(toast_relid, &[(value_id, expected_size)], max_lsn)
            .await?
            .into_iter()
            .next()
            .unwrap_or(FetchedValue::Missing))
    }

    /// The mirror reads `max_lsn` as an as-of ceiling; shadow reads it as a
    /// floor on replay. Same position, opposite role: chunks are written below
    /// the record that refers to them, so replay reaching the referrer is what
    /// makes the value present rather than what bounds which version wins
    async fn fetch_many(
        &self,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let bridge = self.bridge().await?;
        // Before the first request, not per slice: one wait covers the batch
        let floor = self.await_replay(bridge, max_lsn).await?;
        let mut out = Vec::with_capacity(values.len());
        // The resolver hands its whole list down and leaves splitting to the
        // store, so the wire caps are this side's to respect
        for slice in request_slices(values) {
            let got = bridge
                .fetch_toast(toast_relid, slice, floor, self.snapshot)
                .await
                .map_err(|e| ChunkStoreError::Shadow(e.to_string()))?;
            out.extend(got.values.into_iter().map(|v| match v {
                FetchedChunks::Stored(b) => FetchedValue::Assembled(b),
                FetchedChunks::Missing => FetchedValue::Missing,
                FetchedChunks::Mismatch { got } => FetchedValue::Mismatch { got },
            }));
        }
        Ok(out)
    }

    async fn truncate_mirror(&self, _toast_relid: u32) -> Result<(), ChunkStoreError> {
        Err(ChunkStoreError::ReadOnly("truncate_mirror"))
    }

    async fn rewrite_barrier(
        &self,
        _toast_relid: u32,
        _marker_lsn: u64,
        _commit_lsn: u64,
    ) -> Result<(), ChunkStoreError> {
        Err(ChunkStoreError::ReadOnly("rewrite_barrier"))
    }
}

/// Split on whichever wire cap binds first, never below one value
fn request_slices(values: &[(u32, usize)]) -> impl Iterator<Item = &[(u32, usize)]> {
    let mut rest = values;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let mut n = 0;
        let mut bytes = 0usize;
        while n < rest.len() && n < MAX_FETCH_VALUES {
            bytes += rest[n].1;
            n += 1;
            if bytes >= FETCH_REQUEST_BYTES {
                break;
            }
        }
        let (head, tail) = rest.split_at(n);
        rest = tail;
        Some(head)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices_respect_both_caps_and_never_drop_a_value() {
        let many: Vec<(u32, usize)> = (0..MAX_FETCH_VALUES as u32 * 2 + 7)
            .map(|i| (i, 1))
            .collect();
        let slices: Vec<_> = request_slices(&many).collect();
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].len(), MAX_FETCH_VALUES);
        assert_eq!(slices[2].len(), 7);
        assert_eq!(
            slices.iter().map(|s| s.len()).sum::<usize>(),
            many.len(),
            "every value ends up in exactly one request"
        );

        // One oversized value still travels alone rather than being dropped
        let huge = [(1u32, FETCH_REQUEST_BYTES * 4), (2, 8)];
        let slices: Vec<_> = request_slices(&huge).collect();
        assert_eq!(slices, vec![&huge[..1], &huge[1..]]);

        assert_eq!(request_slices(&[]).count(), 0);
    }
}
