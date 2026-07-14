//! Global resident-payload budget: weighted byte permits over one process
//! pool.
//!
//! Channels bound item counts; this bounds bytes. Stages acquire before
//! allocating payload, attach the permit to the owning value, release on
//! drop — decode and insert concurrency divide the pool instead of
//! multiplying per-worker allowances. Acquisition never fails and never
//! waits on itself: a request above a compartment's guaranteed-satisfiable
//! share proceeds with only that share metered (overshoot, counted), so
//! one oversized item softens the bound instead of stalling or failing
//! the pipeline. Upstream disk paths (xact spill eviction, body spool,
//! deferred spool) keep resident state small enough that overshoot stays
//! a pathological-item escape hatch, not normal flow.
//!
//! ## Deadlock model: one pool, two compartments
//!
//! * **Admission** ([`MemoryBudget::admit`]): acquired at pipeline entry
//!   points that can block — drain slice admission (slice heap bytes +
//!   sealed-generation Mem chunk bytes + row batch Mem bytes),
//!   bootstrap/backfill row slices. Transferred with ownership through
//!   `DecodeJob` → routed rows → batcher slabs → in-flight insert blocks,
//!   released when the covering owner drops post-insert-ack. An admitter
//!   can already hold admission units (a drain's sealed generations), so
//!   a request above the whole compartment never waits: it passes
//!   unmetered rather than wait on units it may itself hold.
//! * **Leaf** ([`MemoryBudget::acquire`]): short-lived per-value
//!   allocations made *while holding* an admission permit — store-fetch
//!   assembly, decompress output, body-spool read buffers, JIT mirror-row
//!   materialization slices. The waited share clamps to the leaf reserve
//!   (sized `decoder_pool.max(1) * inline_value_max` at pipeline spawn),
//!   which admission never consumes, so leaf waiters only ever wait on
//!   other leaf holders — those release at insert ack without acquiring,
//!   never on an admission release, so no cycle exists. Pools built
//!   without a reserve have no admission users (leaf-only pools, eg the
//!   bootstrap tail) and clamp to the whole pool instead.
//!
//! Acquisition order: one permit per owner; take admission before any
//! leaf; never hold two leaves; transfer permits with the bytes they
//! cover instead of re-acquiring. A leaf reserved for a resolution peak
//! [`MemoryPermit::shrink`]s to the retained bytes and rides the owning
//! value to release (insert ack), never re-acquires.
//!
//! KiB granularity: tokio semaphore permits are u32-bounded per acquire,
//! KiB units cover pools to 4 TiB.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Semaphore;

const GRANULARITY: usize = 1024;

struct Inner {
    /// Whole pool; every permit holds units here
    sem: Semaphore,
    /// Admission compartment, `total - leaf_reserve`; admission permits
    /// hold units here AND in `sem` (taken in that fixed order)
    admission: Semaphore,
    total: usize,
    admission_max: usize,
    /// Waited-share clamp for leaf acquires: the reserve when one exists
    /// (mixed pool), the whole pool otherwise (leaf-only pool)
    leaf_max: usize,
    cur: AtomicU64,
    peak: AtomicU64,
    waits: AtomicU64,
    overshoots: AtomicU64,
}

/// Shared byte pool; clone hands out the same budget
#[derive(Clone)]
pub struct MemoryBudget {
    inner: Arc<Inner>,
}

impl MemoryBudget {
    pub fn new(total: usize) -> Self {
        Self::with_leaf_reserve(total, 0)
    }

    /// `leaf_reserve` bytes stay out of admission's reach so per-value
    /// leaf acquires under held admission permits always complete
    pub fn with_leaf_reserve(total: usize, leaf_reserve: usize) -> Self {
        let admission_max = total.saturating_sub(leaf_reserve);
        Self {
            inner: Arc::new(Inner {
                sem: Semaphore::new(total.div_ceil(GRANULARITY).max(1)),
                admission: Semaphore::new(admission_max.div_ceil(GRANULARITY).max(1)),
                total,
                admission_max,
                leaf_max: if leaf_reserve == 0 {
                    total
                } else {
                    leaf_reserve
                },
                cur: AtomicU64::new(0),
                peak: AtomicU64::new(0),
                waits: AtomicU64::new(0),
                overshoots: AtomicU64::new(0),
            }),
        }
    }

    pub fn total(&self) -> usize {
        self.inner.total
    }

    /// Bytes currently held by live permits; can exceed `total` while an
    /// overshooting permit is live
    pub fn resident_bytes(&self) -> u64 {
        self.inner.cur.load(Ordering::Relaxed)
    }

    pub fn peak_bytes(&self) -> u64 {
        self.inner.peak.load(Ordering::Relaxed)
    }

    /// Acquisitions that had to wait for releases
    pub fn waits_total(&self) -> u64 {
        self.inner.waits.load(Ordering::Relaxed)
    }

    /// Requests above a compartment's satisfiable share, admitted with
    /// only that share metered
    pub fn overshoots_total(&self) -> u64 {
        self.inner.overshoots.load(Ordering::Relaxed)
    }

    async fn sem_units(sem: &Semaphore, units: u32, waits: &AtomicU64) {
        if let Ok(p) = sem.try_acquire_many(units) {
            p.forget();
            return;
        }
        waits.fetch_add(1, Ordering::Relaxed);
        sem.acquire_many(units)
            .await
            .expect("budget semaphore never closed")
            .forget();
    }

    /// Leaf reserve `bytes` from the whole pool; waits for releases when
    /// contended. For short-lived per-value allocations made under a held
    /// admission permit — never hold two leaves. The waited share clamps
    /// to the leaf reserve; bytes past it are accounted but unmetered
    /// (overshoot) so the wait can always be satisfied by other leaf
    /// holders releasing
    pub async fn acquire(&self, bytes: usize) -> MemoryPermit {
        let metered = bytes.min(self.inner.leaf_max);
        if metered < bytes {
            self.inner.overshoots.fetch_add(1, Ordering::Relaxed);
        }
        let units = units_for(metered);
        Self::sem_units(&self.inner.sem, units, &self.inner.waits).await;
        self.permit(bytes, units, 0)
    }

    /// Admission reserve `bytes` from `total - leaf_reserve`; the pipeline
    /// entry acquire. Fixed order (admission compartment, then pool) keeps
    /// concurrent admitters cycle-free. A request above the compartment
    /// passes unmetered (overshoot): the admitter may already hold
    /// admission units, so waiting for the whole compartment could wait
    /// on itself
    pub async fn admit(&self, bytes: usize) -> MemoryPermit {
        if bytes > self.inner.admission_max {
            self.inner.overshoots.fetch_add(1, Ordering::Relaxed);
            return self.permit(bytes, 0, 0);
        }
        let units = units_for(bytes);
        Self::sem_units(&self.inner.admission, units, &self.inner.waits).await;
        Self::sem_units(&self.inner.sem, units, &self.inner.waits).await;
        self.permit(bytes, units, units)
    }

    fn permit(&self, bytes: usize, units: u32, admission_units: u32) -> MemoryPermit {
        let now = self.inner.cur.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
        self.inner.peak.fetch_max(now, Ordering::Relaxed);
        MemoryPermit {
            inner: self.inner.clone(),
            bytes: bytes as u64,
            units,
            admission_units,
        }
    }
}

/// Owned share of the pool, released on drop. Move it with the bytes it
/// covers (batch hand-off transfers the permit, not the accounting)
pub struct MemoryPermit {
    inner: Arc<Inner>,
    bytes: u64,
    units: u32,
    /// Units also held in the admission compartment (0 for leaf permits)
    admission_units: u32,
}

impl MemoryPermit {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Release the over-reserve once retained bytes are known: a permit
    /// acquired for a resolution peak keeps only what survives into the
    /// owning value. Never grows; admission permits return the freed
    /// units to both compartments
    pub fn shrink(&mut self, bytes: u64) {
        if bytes >= self.bytes {
            return;
        }
        // An overshooting permit holds fewer units than its bytes imply;
        // never free more than held
        let new_units = units_for(bytes as usize).min(self.units);
        let freed = self.units - new_units;
        if freed > 0 {
            self.inner.sem.add_permits(freed as usize);
            let freed_adm = freed.min(self.admission_units);
            if freed_adm > 0 {
                self.inner.admission.add_permits(freed_adm as usize);
                self.admission_units -= freed_adm;
            }
            self.units = new_units;
        }
        self.inner
            .cur
            .fetch_sub(self.bytes - bytes, Ordering::Relaxed);
        self.bytes = bytes;
    }
}

impl std::fmt::Debug for MemoryPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPermit")
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        self.inner.cur.fetch_sub(self.bytes, Ordering::Relaxed);
        self.inner.sem.add_permits(self.units as usize);
        if self.admission_units > 0 {
            self.inner
                .admission
                .add_permits(self.admission_units as usize);
        }
    }
}

fn units_for(bytes: usize) -> u32 {
    u32::try_from(bytes.div_ceil(GRANULARITY)).expect("bytes <= total <= 4 TiB")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn acquire_release_round_trip() {
        let b = MemoryBudget::new(1 << 20);
        let p = b.acquire(512 << 10).await;
        assert_eq!(b.resident_bytes(), 512 << 10);
        let q = b.acquire(256 << 10).await;
        assert_eq!(b.resident_bytes(), 768 << 10);
        drop(p);
        assert_eq!(b.resident_bytes(), 256 << 10);
        drop(q);
        assert_eq!(b.resident_bytes(), 0);
        assert_eq!(b.peak_bytes(), 768 << 10);
        assert_eq!(b.waits_total(), 0);
        assert_eq!(b.overshoots_total(), 0);
    }

    /// Leaf-only pool: a request above the pool waits for the whole pool
    /// (other holders release independently), then proceeds with the
    /// excess accounted but unmetered
    #[tokio::test(flavor = "current_thread")]
    async fn oversized_leaf_overshoots_after_pool_drains() {
        let b = MemoryBudget::new(1 << 20);
        let hold = b.acquire(1 << 20).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.acquire((1 << 20) + 1024).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "full pool blocks the metered share");
        drop(hold);
        let permit = waiter.await.unwrap();
        assert_eq!(permit.bytes(), (1 << 20) + 1024);
        assert_eq!(b.resident_bytes(), (1 << 20) + 1024);
        assert_eq!(b.overshoots_total(), 1);
        drop(permit);
        assert_eq!(b.resident_bytes(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn contended_acquire_waits_for_release() {
        let b = MemoryBudget::new(64 << 10);
        let hold = b.acquire(64 << 10).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.acquire(32 << 10).await.bytes() });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "full pool blocks");
        drop(hold);
        assert_eq!(waiter.await.unwrap(), 32 << 10);
        assert_eq!(b.waits_total(), 1);
        drop(b);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zero_byte_permit_is_free() {
        let b = MemoryBudget::new(1 << 20);
        let p = b.acquire(0).await;
        assert_eq!(p.bytes(), 0);
        assert_eq!(b.resident_bytes(), 0);
    }

    /// Leaf reserve keeps per-value acquires live under full admission:
    /// the deadlock-model guarantee
    #[tokio::test(flavor = "current_thread")]
    async fn leaf_acquire_proceeds_under_full_admission() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        // Admission compartment is 64 KiB; saturate it
        let _admitted = b.admit(64 << 10).await;
        // Leaf draws from the reserve and completes immediately
        let leaf = b.acquire(64 << 10).await;
        assert_eq!(b.resident_bytes(), 128 << 10);
        drop(leaf);
        // Leaf above the reserve clamps its waited share to the reserve:
        // completes without any admission release, excess unmetered
        let big = b.acquire(96 << 10).await;
        assert_eq!(big.bytes(), 96 << 10);
        assert_eq!(b.resident_bytes(), 160 << 10);
        assert_eq!(b.overshoots_total(), 1);
        drop(big);
        // Admission larger than its compartment: unmetered, never waits
        let over = b.admit((64 << 10) + 1).await;
        assert_eq!(b.overshoots_total(), 2);
        drop(over);
        assert_eq!(b.resident_bytes(), 64 << 10);
        assert_eq!(b.waits_total(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shrink_releases_over_reserve() {
        let b = MemoryBudget::new(64 << 10);
        let mut p = b.acquire(64 << 10).await;
        p.shrink(16 << 10);
        assert_eq!(p.bytes(), 16 << 10);
        assert_eq!(b.resident_bytes(), 16 << 10);
        // Freed capacity is immediately acquirable
        let q = b.acquire(48 << 10).await;
        assert_eq!(b.resident_bytes(), 64 << 10);
        assert_eq!(b.waits_total(), 0);
        drop(q);
        drop(p);
        assert_eq!(b.resident_bytes(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shrink_returns_admission_units() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let mut p = b.admit(64 << 10).await;
        p.shrink(32 << 10);
        // Admission compartment regained the freed half
        let q = b.admit(32 << 10).await;
        assert_eq!(b.waits_total(), 0);
        drop(q);
        drop(p);
        assert_eq!(b.resident_bytes(), 0);
    }

    /// Shrinking an overshooting permit frees only held units, and the
    /// remaining accounting stays consistent to release
    #[tokio::test(flavor = "current_thread")]
    async fn shrink_overshooting_permit_stays_consistent() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        // Metered share 64 KiB, 64 KiB overshoot
        let mut p = b.acquire(128 << 10).await;
        assert_eq!(b.overshoots_total(), 1);
        // Retained bytes above the metered share: units unchanged
        p.shrink(96 << 10);
        assert_eq!(b.resident_bytes(), 96 << 10);
        // Now below: frees down to the retained share
        p.shrink(16 << 10);
        assert_eq!(b.resident_bytes(), 16 << 10);
        let q = b.acquire(48 << 10).await;
        assert_eq!(b.waits_total(), 0);
        drop(q);
        drop(p);
        assert_eq!(b.resident_bytes(), 0);
    }

    /// Admission blocked by (insert-shaped) backpressure resumes when the
    /// holder drops; drop returns units to both compartments
    #[tokio::test(flavor = "current_thread")]
    async fn blocked_admission_releases_on_drop() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let inflight = b.admit(64 << 10).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.admit(64 << 10).await.bytes() });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "full admission compartment blocks");
        drop(inflight);
        assert_eq!(waiter.await.unwrap(), 64 << 10);
        assert!(b.waits_total() >= 1);
    }
}
