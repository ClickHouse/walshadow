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

use crate::ops::bridge::{Bridge, BridgeError, FetchedChunks, MAX_FETCH_VALUES};
use crate::pos::{AppliedWake, Gate, Pos};
use crate::toast::xid_ceiling::{XidCeiling, follows};
use crate::toast::{ChunkStore, ChunkStoreError, FetchedValue};

/// Round-trip payload target. Always allow one value even when it exceeds limit
const FETCH_REQUEST_BYTES: usize = 64 << 20;

/// Poll cadence while waiting for shadow replay to cover a read. Reads ahead
/// usually trail replay by a few records, so start short and back off
const REPLAY_POLL_MIN: Duration = Duration::from_millis(1);
const REPLAY_POLL: Duration = Duration::from_millis(20);
/// Maximum poll interval while worker socket cannot accept connections
const UNREACHABLE_POLL_MAX: Duration = Duration::from_secs(1);
/// Maximum time without replay progress or a worker connection
const REPLAY_WAIT_MAX: Duration = Duration::from_secs(900);

/// Bridge populated after bootstrap starts PostgreSQL
pub type LateBridge = Arc<tokio::sync::OnceCell<Arc<Bridge>>>;

/// Already-dialled bridge as a [`LateBridge`], so callers holding either
/// reach the same constructors
pub fn bound(bridge: Arc<Bridge>) -> LateBridge {
    let cell = LateBridge::default();
    cell.set(bridge).ok();
    cell
}

/// Bridge and xid samples for shadow reads
/// Detect reused value IDs using pump's samples, returning
/// [`FetchedValue::Generation`]. Default ceiling skips generation checks
#[derive(Clone, Default)]
pub struct ShadowRead {
    pub bridge: LateBridge,
    pub ceiling: Arc<XidCeiling>,
    pub applied: Option<Gate<AppliedWake>>,
}

/// Create a read without generation checks
impl From<LateBridge> for ShadowRead {
    fn from(bridge: LateBridge) -> Self {
        Self {
            bridge,
            ceiling: Arc::default(),
            applied: None,
        }
    }
}

impl From<&crate::ops::oracle::Oracle> for ShadowRead {
    fn from(oracle: &crate::ops::oracle::Oracle) -> Self {
        Self {
            bridge: bound(oracle.toast_bridge()),
            ceiling: oracle.xid_ceiling(),
            applied: oracle.replay_wake(),
        }
    }
}

pub struct ShadowToastStore {
    read: ShadowRead,
    replay_wait_max: Duration,
    replay_poll: Duration,
}

impl ShadowToastStore {
    pub fn new(bridge: Arc<Bridge>) -> Self {
        Self::late(bound(bridge))
    }

    /// Create store that waits for bridge to become available
    pub fn late(read: impl Into<ShadowRead>) -> Self {
        Self {
            read: read.into(),
            replay_wait_max: REPLAY_WAIT_MAX,
            replay_poll: REPLAY_POLL,
        }
    }

    #[cfg(test)]
    fn with_replay_poll(mut self, poll: Duration) -> Self {
        self.replay_poll = poll;
        self
    }

    async fn pause(&self, seen: Option<Pos<AppliedWake>>, poll: Duration) {
        if let (Some(gate), Some(seen)) = (&self.read.applied, seen)
            && !matches!(
                tokio::time::timeout(poll, gate.advance(seen)).await,
                Ok(Err(_))
            )
        {
            return;
        }
        tokio::time::sleep(poll).await;
    }

    pub fn with_replay_wait_max(mut self, max: Duration) -> Self {
        self.replay_wait_max = max;
        self
    }

    /// Return bound bridge or wait until readiness deadline
    async fn bridge(&self) -> Result<&Arc<Bridge>, ChunkStoreError> {
        let deadline = Instant::now() + self.replay_wait_max;
        loop {
            if let Some(b) = self.read.bridge.get() {
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
    /// catalog reads.
    ///
    /// Supervisor restarts postmaster after a GUC floor change stops it
    /// Worker socket cannot accept connections during restart. Treat that like
    /// stalled replay and restart timeout whenever replay position changes
    async fn await_replay(&self, bridge: &Bridge, through: u64) -> Result<(), ChunkStoreError> {
        let mut since = Instant::now();
        let mut last: Option<u64> = None;
        let mut unreachable = false;
        let mut poll = if self.read.applied.is_some() {
            self.replay_poll
        } else {
            REPLAY_POLL_MIN.min(self.replay_poll)
        };
        loop {
            let seen = self.read.applied.as_ref().map(Gate::current);
            match bridge.replay_lsn().await {
                Ok(at) if at >= through => return Ok(()),
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
                    poll = poll.min(self.replay_poll);
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
            self.pause(seen, poll).await;
            if !unreachable {
                poll = (poll * 2).min(self.replay_poll);
            }
        }
    }
}

#[async_trait]
impl ChunkStore for ShadowToastStore {
    /// Treat `max_lsn` as minimum replay position. Chunks precede referring
    /// record, so reaching this position makes value available
    async fn fetch_many(
        &self,
        _db_oid: u32,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        let ceiling = self.read.ceiling.at(max_lsn);
        let got = self.read_at(toast_relid, values, max_lsn, true).await?;
        Ok(got.into_iter().map(|c| judge(c, ceiling)).collect())
    }

    fn mixed_bounds(&self) -> bool {
        true
    }

    async fn replayed_through(&self, wait_for: u64) -> Option<u64> {
        let bridge = self.read.bridge.get()?;
        if !bridge.info().in_recovery {
            return Some(u64::MAX);
        }
        if wait_for != 0 {
            self.await_replay(bridge, wait_for).await.ok()?;
        }
        bridge.replay_lsn().await.ok()
    }

    async fn fetch_bounded(
        &self,
        _db_oid: u32,
        toast_relid: u32,
        values: &[(u32, usize, u64)],
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        let newest = values.iter().map(|v| v.2).max().unwrap_or(0);
        let ids: Vec<(u32, usize)> = values.iter().map(|v| (v.0, v.1)).collect();
        let wait = values.iter().all(|v| v.2 == newest);
        let got = self.read_at(toast_relid, &ids, newest, wait).await?;
        Ok(got
            .into_iter()
            .zip(values)
            .map(|(c, v)| judge(c, self.read.ceiling.at(v.2)))
            .collect())
    }
}

impl ShadowToastStore {
    async fn read_at(
        &self,
        toast_relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
        wait: bool,
    ) -> Result<Vec<FetchedChunks>, ChunkStoreError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let bridge = self.bridge().await?;
        // Primary files are complete before service, and a primary's replay
        // position stays where crash recovery ended
        let floor = if bridge.info().in_recovery {
            max_lsn
        } else {
            0
        };
        // Worker refuses a floor above its replay position, so read first and
        // wait for replay only once refused or unreachable. Wait and read once
        // more for a restart that rewinds replay between the wait and the read
        let retry = wait && floor != 0;
        let mut waits = 0;
        loop {
            match read_values(bridge, toast_relid, values, floor).await {
                Err(BridgeError::Remote(_) | BridgeError::Io(_)) if retry && waits < 2 => {
                    waits += 1;
                }
                got => return got.map_err(|e| ChunkStoreError::Shadow(e.to_string())),
            }
            self.await_replay(bridge, floor).await?;
        }
    }
}

/// Split resolver batch to satisfy wire limits
async fn read_values(
    bridge: &Bridge,
    toast_relid: u32,
    values: &[(u32, usize)],
    floor: u64,
) -> Result<Vec<FetchedChunks>, BridgeError> {
    let mut out = Vec::with_capacity(values.len());
    for slice in request_slices(values) {
        let got = bridge.fetch_toast(toast_relid, slice, floor).await?;
        out.extend(got);
    }
    Ok(out)
}

/// Detect reused IDs from chunks newer than referring record
/// PostgreSQL reuses an ID only after all original chunks are gone
/// Skip check if ceiling or normal xmin is unavailable
fn judge(c: FetchedChunks, ceiling: u32) -> FetchedValue {
    if ceiling != 0 && c.xmin != 0 && follows(c.xmin, ceiling) {
        FetchedValue::Generation
    } else {
        c.value
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
    fn a_run_younger_than_the_ceiling_reads_as_a_later_generation() {
        let torn = |xmin| FetchedChunks {
            value: FetchedValue::Mismatch { got: 3 },
            xmin,
        };
        let whole = |xmin| FetchedChunks {
            value: FetchedValue::Assembled(b"abc".to_vec()),
            xmin,
        };

        assert_eq!(judge(whole(200), 100), FetchedValue::Generation);
        assert_eq!(
            judge(torn(200), 100),
            FetchedValue::Generation,
            "a partly reclaimed replacement is still a replacement",
        );
        assert_eq!(
            judge(torn(50), 100),
            FetchedValue::Mismatch { got: 3 },
            "partially reclaimed original remains a size mismatch",
        );
        assert_eq!(
            judge(whole(100), 100),
            whole(100).value,
            "equal is not newer"
        );
        assert_eq!(judge(whole(200), 0), whole(200).value, "unsampled ceiling");
        assert_eq!(judge(whole(0), 100), whole(0).value, "frozen chunks");
        assert_eq!(
            judge(
                FetchedChunks {
                    value: FetchedValue::Missing,
                    xmin: 0,
                },
                100,
            ),
            FetchedValue::Missing,
        );
        assert_eq!(
            judge(whole(4), u32::MAX - 6),
            FetchedValue::Generation,
            "xid order wraps",
        );
    }

    /// Standby refuses reads above its replay position. A read polls replay
    /// only after a refusal, then retries, once more for a restart between
    /// the poll and the read, and then returns the refusal
    #[tokio::test(flavor = "current_thread")]
    async fn refused_read_waits_for_replay_and_retries() {
        use crate::ops::bridge::tests::{
            fetch_body, hello_body, lsn_body, remote_body, spawn_worker,
        };
        use crate::ops::bridge::{Op, PROJECTION_VERSION, PROTO_VERSION};
        use std::sync::atomic::Ordering;
        for refusals in 0..4u64 {
            let mut script = vec![Some(hello_body(PROTO_VERSION, PROJECTION_VERSION))];
            for i in 0..refusals {
                script.push(Some(remote_body("replay below the requested floor")));
                if i < 2 {
                    script.push(Some(lsn_body(0x2000)));
                }
            }
            script.push(Some(fetch_body(&[b"v"])));
            let (_tmp, path) = spawn_worker(script);
            let bridge = Arc::new(Bridge::connect(&path).await.unwrap());
            let store = ShadowToastStore::new(bridge.clone()).with_replay_wait_max(REPLAY_POLL);
            let got = store.fetch(0, 1, 7, 0x2000, 1).await;
            if refusals < 3 {
                assert_eq!(got.unwrap(), FetchedValue::Assembled(b"v".to_vec()));
            } else {
                let err = got.unwrap_err().to_string();
                assert!(err.contains("below the requested"), "{err}");
            }
            let sent = |op: Op| bridge.stats.requests[op.slot()].load(Ordering::Relaxed);
            assert_eq!(sent(Op::ReplayLsn), refusals.min(2), "{refusals} refusals");
            assert_eq!(sent(Op::FetchToast), refusals.min(2) + 1);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mixed_bounds_judge_each_value_against_its_own_referrer() {
        use crate::ops::bridge::tests::{hello_body, spawn_worker};
        use crate::ops::bridge::{PROJECTION_VERSION, PROTO_VERSION};
        let mut body = vec![0u8];
        body.extend_from_slice(&2u32.to_be_bytes());
        for value in [b"a", b"b"] {
            body.push(0);
            body.extend_from_slice(&200u32.to_be_bytes());
            body.extend_from_slice(&1u32.to_be_bytes());
            body.extend_from_slice(value);
        }
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(body),
        ]);
        let ceiling = Arc::new(XidCeiling::default());
        ceiling.observe(0, 100);
        ceiling.observe(1 << 20, 300);
        let store = ShadowToastStore::late(ShadowRead {
            bridge: bound(Arc::new(Bridge::connect(&path).await.unwrap())),
            ceiling,
            applied: None,
        });
        assert!(store.mixed_bounds());
        let got = store
            .fetch_bounded(0, 1, &[(7, 1, 0), (8, 1, 1 << 20)])
            .await
            .unwrap();
        assert_eq!(
            got,
            vec![
                FetchedValue::Generation,
                FetchedValue::Assembled(b"b".to_vec())
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mixed_bounds_never_wait_for_replay() {
        use crate::ops::bridge::tests::{hello_body, remote_body, spawn_worker};
        use crate::ops::bridge::{Op, PROJECTION_VERSION, PROTO_VERSION};
        use std::sync::atomic::Ordering;
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(remote_body("replay below the requested floor")),
        ]);
        let bridge = Arc::new(Bridge::connect(&path).await.unwrap());
        let store = ShadowToastStore::new(bridge.clone());
        let err = store
            .fetch_bounded(0, 1, &[(7, 1, 0x1000), (8, 1, 0x2000)])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("below the requested"), "{err}");
        let sent = |op: Op| bridge.stats.requests[op.slot()].load(Ordering::Relaxed);
        assert_eq!(sent(Op::ReplayLsn), 0);
        assert_eq!(sent(Op::FetchToast), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_position_follows_recovery_state() {
        use crate::ops::bridge::tests::{hello_body, lsn_body, spawn_worker};
        use crate::ops::bridge::{PROJECTION_VERSION, PROTO_VERSION};
        let unbound = ShadowToastStore::late(LateBridge::default());
        assert_eq!(unbound.replayed_through(0).await, None);
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(lsn_body(0x3000)),
            Some(lsn_body(0x3000)),
            Some(lsn_body(0x3000)),
        ]);
        let standby = ShadowToastStore::new(Arc::new(Bridge::connect(&path).await.unwrap()));
        assert_eq!(standby.replayed_through(0).await, Some(0x3000));
        assert_eq!(standby.replayed_through(0x2000).await, Some(0x3000));
        let mut hello = hello_body(PROTO_VERSION, PROJECTION_VERSION);
        hello[13] = 0;
        let (_tmp, path) = spawn_worker(vec![Some(hello)]);
        let primary = ShadowToastStore::new(Arc::new(Bridge::connect(&path).await.unwrap()));
        assert_eq!(primary.replayed_through(0).await, Some(u64::MAX));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_wait_wakes_on_reported_apply_progress() {
        use crate::ops::bridge::tests::{
            fetch_body, hello_body, lsn_body, remote_body, spawn_worker,
        };
        use crate::ops::bridge::{PROJECTION_VERSION, PROTO_VERSION};
        use crate::pos::Monotone;
        let (_tmp, path) = spawn_worker(vec![
            Some(hello_body(PROTO_VERSION, PROJECTION_VERSION)),
            Some(remote_body("replay below the requested floor")),
            Some(lsn_body(0x1000)),
            Some(lsn_body(0x2000)),
            Some(fetch_body(&[b"v"])),
        ]);
        let applied: Arc<Monotone<AppliedWake>> = Arc::default();
        let read = ShadowRead {
            bridge: bound(Arc::new(Bridge::connect(&path).await.unwrap())),
            ceiling: Arc::default(),
            applied: Some(applied.watch()),
        };
        let store = ShadowToastStore::late(read).with_replay_poll(Duration::from_secs(600));
        let waker = tokio::spawn({
            let applied = applied.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    applied.bump();
                }
            }
        });
        let got = tokio::time::timeout(Duration::from_secs(30), store.fetch(0, 1, 7, 0x2000, 1))
            .await
            .expect("woken by apply progress, not the poll");
        waker.abort();
        assert_eq!(got.unwrap(), FetchedValue::Assembled(b"v".to_vec()));
    }

    /// Replay wait rides out a worker restart, and fails on any other
    /// replay-position error
    #[tokio::test(flavor = "current_thread")]
    async fn replay_wait_survives_restart_and_fails_on_remote_error() {
        use crate::ops::bridge::tests::{
            fetch_body, hello_body, lsn_body, remote_body, spawn_worker,
        };
        use crate::ops::bridge::{PROJECTION_VERSION, PROTO_VERSION};
        let hello = || Some(hello_body(PROTO_VERSION, PROJECTION_VERSION));
        let refusal = || Some(remote_body("replay below the requested floor"));
        let (_tmp, path) = spawn_worker(vec![
            hello(),
            refusal(),
            None,
            hello(),
            None,
            hello(),
            Some(lsn_body(0x1000)),
            Some(lsn_body(0x2000)),
            Some(fetch_body(&[b"v"])),
        ]);
        let bridge = Arc::new(Bridge::connect(&path).await.unwrap());
        let store = ShadowToastStore::new(bridge).with_replay_wait_max(Duration::from_secs(5));
        let got = store.fetch(0, 1, 7, 0x2000, 1).await.unwrap();
        assert_eq!(got, FetchedValue::Assembled(b"v".to_vec()));

        let (_tmp, path) = spawn_worker(vec![hello(), refusal(), Some(remote_body("boom"))]);
        let bridge = Arc::new(Bridge::connect(&path).await.unwrap());
        let err = ShadowToastStore::new(bridge)
            .fetch(0, 1, 7, 0x2000, 1)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("replay position: "), "{err}");
    }

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
