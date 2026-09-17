//! TOAST values read out of shadow PostgreSQL instead of a ClickHouse mirror.
//!
//! Shadow stores physical TOAST heaps and indexes while replaying source WAL,
//! so external values can use local index lookups without ClickHouse chunk
//! mirror. Select with `[toast] mode = "shadow"`; see
//! `plans/shadow_toast.md`. Greenfield bootstrap and live CDC both use this
//! store.
//!
//! Store is read-only. Shadow receives data through WAL replay, so write methods
//! reject decoded `ToastRow` values.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::ops::bridge::{Bridge, BridgeError, MAX_FETCH_VALUES};
use crate::toast::{ChunkStore, ChunkStoreError, FetchedValue, ToastRow};

/// Round-trip payload target. Always allow one value even when it exceeds limit
const FETCH_REQUEST_BYTES: usize = 64 << 20;

/// Poll cadence while waiting for shadow replay to cover a read
const REPLAY_POLL: Duration = Duration::from_millis(20);
/// Maximum poll interval while worker socket cannot accept connections
const UNREACHABLE_POLL_MAX: Duration = Duration::from_secs(1);
/// Maximum time without replay progress or a worker connection
const REPLAY_WAIT_MAX: Duration = Duration::from_secs(60);

/// Bridge populated after bootstrap starts PostgreSQL
pub type LateBridge = Arc<tokio::sync::OnceCell<Arc<Bridge>>>;

pub struct ShadowToastStore {
    bridge: LateBridge,
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
            replay_wait_max: REPLAY_WAIT_MAX,
        }
    }

    pub fn with_replay_wait_max(mut self, max: Duration) -> Self {
        self.replay_wait_max = max;
        self
    }

    /// Return bound bridge or wait until readiness deadline
    async fn bridge(&self) -> Result<&Arc<Bridge>, ChunkStoreError> {
        let deadline = Instant::now() + self.replay_wait_max;
        loop {
            if let Some(b) = self.bridge.get() {
                return Ok(b);
            }
            if Instant::now() >= deadline {
                return Err(ChunkStoreError::Shadow(format!(
                    "no PostgreSQL bound to read values from after {:?}",
                    self.replay_wait_max
                )));
            }
            tokio::time::sleep(REPLAY_POLL).await;
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
    ///
    /// Supervisor restarts postmaster after a GUC floor change stops it
    /// Worker socket cannot accept connections during restart. Treat that like
    /// stalled replay and restart timeout whenever replay position changes
    async fn await_replay(&self, bridge: &Bridge, through: u64) -> Result<u64, ChunkStoreError> {
        // Primary files are complete before service and have no replay position
        if through == 0 || !bridge.info().is_some_and(|i| i.in_recovery) {
            return Ok(0);
        }
        let mut since = Instant::now();
        let mut last: Option<u64> = None;
        let mut unreachable = false;
        let mut poll = REPLAY_POLL;
        loop {
            match bridge.replay_lsn().await {
                Ok(at) if at >= through => return Ok(through),
                Ok(at) => {
                    if unreachable || last != Some(at) {
                        since = Instant::now();
                    }
                    if unreachable {
                        tracing::info!(
                            target: "walshadow::toast",
                            replay_lsn = format_args!("{at:X}"),
                            "shadow is reachable again, waiting for replay",
                        );
                    }
                    unreachable = false;
                    last = Some(at);
                    poll = REPLAY_POLL;
                }
                Err(BridgeError::Io(e)) => {
                    if !unreachable {
                        since = Instant::now();
                        tracing::warn!(
                            target: "walshadow::toast",
                            error = %e,
                            "shadow is unreachable, waiting for it to restart",
                        );
                    }
                    unreachable = true;
                    poll = (poll * 2).min(UNREACHABLE_POLL_MAX);
                }
                Err(e) => {
                    return Err(ChunkStoreError::Shadow(format!("replay position: {e}")));
                }
            }
            if since.elapsed() >= self.replay_wait_max {
                return Err(ChunkStoreError::Shadow(if unreachable {
                    format!(
                        "shadow unreachable for {:?}, value needs replay past {through:X}",
                        self.replay_wait_max
                    )
                } else {
                    format!(
                        "shadow replay stuck at {:X}, value needs {through:X} after {:?}",
                        last.unwrap_or(0),
                        self.replay_wait_max
                    )
                }));
            }
            tokio::time::sleep(poll).await;
        }
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
                .fetch_toast(toast_relid, slice, floor)
                .await
                .map_err(|e| ChunkStoreError::Shadow(e.to_string()))?;
            out.extend(got);
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
