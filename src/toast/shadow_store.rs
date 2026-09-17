//! TOAST values read out of shadow PostgreSQL instead of a ClickHouse mirror.
//!
//! Shadow stores physical TOAST heaps and indexes while replaying source WAL,
//! so external values can use local index lookups without ClickHouse chunk
//! mirror. Select with `[toast] backend = "shadow"`; see
//! `plans/shadow_toast.md`. Greenfield bootstrap and live CDC both use this
//! backend.
//!
//! Store is read-only. Shadow receives data through WAL replay, so write methods
//! reject decoded `ToastRow` values.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::ops::bridge::{Bridge, FetchedChunks, MAX_FETCH_VALUES, ToastSnapshot};
use crate::toast::{ChunkStore, ChunkStoreError, FetchedValue, ToastRow};

/// Round-trip payload target. Always allow one value even when it exceeds limit
const FETCH_REQUEST_BYTES: usize = 64 << 20;

/// Poll cadence while waiting for shadow replay to cover a read
const REPLAY_POLL: Duration = Duration::from_millis(20);
/// Maximum time to wait for shadow to apply dispatched WAL
const REPLAY_WAIT_MAX: Duration = Duration::from_secs(60);

/// Bridge populated after bootstrap starts PostgreSQL
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

    /// Create store that waits for bridge to become available
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

    /// Return bound bridge or wait until readiness deadline
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

    /// Wait until shadow has applied through `through`.
    ///
    /// `WalStream` dispatches record bytes before awaiting record sink. WAL
    /// needed to reach `through` is already on its way to shadow, so this wait
    /// does not require more pump progress.
    ///
    /// Poll here instead of blocking worker, which must remain available for
    /// catalog reads. Return replay floor for standby request. Return zero for
    /// primary, which has no replay position.
    async fn await_replay(&self, bridge: &Bridge, through: u64) -> Result<u64, ChunkStoreError> {
        // Primary files are complete before service and have no replay position
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

    /// Use wider snapshot to expose reused value-ID generations in tests
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

    /// Treat `max_lsn` as minimum replay position. Chunks precede referring
    /// record, so reaching this position makes value available
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
        // Wait once for complete batch
        let floor = self.await_replay(bridge, max_lsn).await?;
        let mut out = Vec::with_capacity(values.len());
        // Split resolver batch to satisfy wire limits
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

/// Split at first wire limit, always include at least one value
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

        // Send oversized value alone
        let huge = [(1u32, FETCH_REQUEST_BYTES * 4), (2, 8)];
        let slices: Vec<_> = request_slices(&huge).collect();
        assert_eq!(slices, vec![&huge[..1], &huge[1..]]);

        assert_eq!(request_slices(&[]).count(), 0);
    }
}
