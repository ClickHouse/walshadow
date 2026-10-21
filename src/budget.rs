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
//!   planned slice → routed rows → batcher slabs → in-flight insert blocks,
//!   released when the covering owner drops post-insert-ack. An admitter
//!   can already hold admission units (a drain's sealed generations), so
//!   a request above the whole compartment never waits: it passes
//!   unmetered rather than wait on units it may itself hold.
//! * **Leaf** ([`MemoryBudget::acquire`]): short-lived per-value
//!   allocations made *while holding* an admission permit — store-fetch
//!   assembly, decompress output, body-spool read buffers, JIT mirror-row
//!   materialization slices. The waited share clamps to the leaf reserve
//!   (sized `decoder_pool.max(1) * value_reserve` at pipeline spawn),
//!   which admission never consumes, so leaf waiters only ever wait on
//!   other leaf holders — those release at insert ack without acquiring,
//!   never on an admission release, so no cycle exists. Pools built
//!   without a reserve have no admission users (leaf-only pools, eg the
//!   bootstrap tail) and clamp to the whole pool instead.
//!
//! Speculative reads use admission permits without waiting or overshooting.
//! Foreground admission reclaims pending results before waiting and blocks
//! new speculation until admitted. Consumed results transfer their permits.
//!
//! ## Large values run one at a time
//!
//! For values larger than reserve, charge only reserve size against pool.
//! Allow one such value at a time. Acquire its slot before pool units so
//! requests waiting for this slot hold no pool units. Use a separate queue
//! so smaller values can proceed. [`MemoryPermit::shrink`] releases this
//! slot once retained bytes fit reserve.
//!
//! Acquisition order: one permit per owner; take admission before any
//! leaf; never hold two leaves; transfer permits with the bytes they
//! cover instead of re-acquiring. A leaf reserved for a resolution peak
//! [`MemoryPermit::shrink`]s to the retained bytes and rides the owning
//! value to release (insert ack), never re-acquires.
//!
//! ## Store capacity and queue flags together
//!
//! Store both compartments' free capacity in one atomic word, in 256 B
//! units with 30 bits each (up to 256 GiB). Also store whether a large
//! value slot is free and whether each queue has waiting requests.
//! Acquiring and releasing capacity each use one atomic update when no
//! requests are queued. Queued requests require a lock and run in arrival
//! order. New requests cannot skip earlier requests in their queue.
//! Keep requests waiting for admission capacity in a separate queue so
//! leaf allocations can proceed while their owners hold admission units.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker};

const GRANULARITY: usize = 256;

const UNIT_BITS: u32 = 30;
const UNIT_MASK: u64 = (1 << UNIT_BITS) - 1;
const ADMISSION_MASK: u64 = UNIT_MASK << UNIT_BITS;
const BIG_FREE: u64 = 1 << 60;
const BIG_QUEUED: u64 = 1 << 61;
const POOL_QUEUED: u64 = 1 << 62;
const ADMISSION_QUEUED: u64 = 1 << 63;
const QUEUED: u64 = BIG_QUEUED | POOL_QUEUED | ADMISSION_QUEUED;
const MAX_BYTES: usize = UNIT_MASK as usize * GRANULARITY;

/// Store pool units below admission units, followed by [`BIG_FREE`].
/// Released units cannot exceed initial capacity, so addition cannot
/// overflow into adjacent fields
fn word(pool: u32, admission: u32) -> u64 {
    u64::from(pool) | (u64::from(admission) << UNIT_BITS)
}

/// Keep capacity on a separate cache line from usage counters
#[repr(align(64))]
struct State(AtomicU64);

#[derive(Clone, Copy)]
enum Queue {
    /// Leaf requests above `leaf_max` waiting for large value slot
    Big = 0,
    /// Leaf requests and admission requests waiting for pool units
    Pool = 1,
    /// Requests waiting for admission units; leaf requests use another queue
    Admission = 2,
}

impl Queue {
    const ALL: [Self; 3] = [Self::Big, Self::Pool, Self::Admission];

    fn bit(self) -> u64 {
        BIG_QUEUED << self as u32
    }

    /// Capacity field needed by requests in this queue
    fn field(self) -> u64 {
        match self {
            Self::Big => BIG_FREE,
            Self::Pool => UNIT_MASK,
            Self::Admission => ADMISSION_MASK,
        }
    }
}

struct Slot {
    id: u64,
    waker: Waker,
}

#[derive(Default)]
struct Waitlist {
    queues: [VecDeque<Slot>; 3],
    next_id: u64,
}

pub(crate) trait Reclaim: Send + Sync {
    fn reclaim(&self);
}

struct AdmissionWait<'a>(&'a AtomicUsize);

impl Drop for AdmissionWait<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Inner {
    /// Track free pool units, free admission units (`total - leaf_reserve`),
    /// large value slot, and queue flags; see [`word`]. Admission permits
    /// reserve units from both compartments. Leaf permits reserve pool
    /// units and, above `leaf_max`, large value slot
    state: State,
    /// Queued requests; release locks this only when requests are queued
    waitlist: Mutex<Waitlist>,
    total: usize,
    admission_max: usize,
    /// Waited-share clamp for leaf acquires: the reserve when one exists
    /// (mixed pool), the whole pool otherwise (leaf-only pool)
    leaf_max: usize,
    cur: AtomicU64,
    peak: AtomicU64,
    waits: AtomicU64,
    overshoots: AtomicU64,
    big_leaf_waits: AtomicU64,
    admission_waiters: AtomicUsize,
    reclaimers: Mutex<Vec<Weak<dyn Reclaim>>>,
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
        let total = total.min(MAX_BYTES);
        let admission_max = total.saturating_sub(leaf_reserve);
        Self {
            inner: Arc::new(Inner {
                state: State(AtomicU64::new(
                    word(units_for(total).max(1), units_for(admission_max).max(1)) | BIG_FREE,
                )),
                waitlist: Mutex::default(),
                total,
                admission_max,
                leaf_max: if leaf_reserve == 0 {
                    total
                } else {
                    leaf_reserve.min(total)
                },
                cur: AtomicU64::new(0),
                peak: AtomicU64::new(0),
                waits: AtomicU64::new(0),
                overshoots: AtomicU64::new(0),
                big_leaf_waits: AtomicU64::new(0),
                admission_waiters: AtomicUsize::new(0),
                reclaimers: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn total(&self) -> usize {
        self.inner.total
    }

    /// Share a single leaf acquire waits on: the reserve on a mixed pool,
    /// the whole pool on a leaf-only one. A holder staying inside it
    /// never withholds bytes admission needs
    pub fn leaf_max(&self) -> usize {
        self.inner.leaf_max
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

    /// Large values that waited for another large value
    pub fn big_leaf_waits_total(&self) -> u64 {
        self.inner.big_leaf_waits.load(Ordering::Relaxed)
    }

    /// Leaf reserve `bytes` from the whole pool; waits for releases when
    /// contended. For short-lived per-value allocations made under a held
    /// admission permit — never hold two leaves. The waited share clamps
    /// to the leaf reserve; bytes past it are accounted but unmetered
    /// (overshoot) so the wait can always be satisfied by other leaf
    /// holders releasing
    pub async fn acquire(&self, bytes: usize) -> MemoryPermit {
        let metered = bytes.min(self.inner.leaf_max);
        let units = units_for(metered);
        if metered == bytes {
            self.inner.take(Queue::Pool, word(units, 0)).await;
            return self.permit(bytes, units, 0);
        }
        self.inner.overshoots.fetch_add(1, Ordering::Relaxed);
        // Release slot if cancelled while waiting for pool units
        let mut permit = self.permit(0, 0, 0);
        self.inner.take(Queue::Big, BIG_FREE).await;
        permit.big_leaf = true;
        self.inner.take(Queue::Pool, word(units, 0)).await;
        permit.units = units;
        permit.bytes = bytes as u64;
        self.inner.meter(permit.bytes);
        permit
    }

    /// Reserve `bytes` at pipeline entry from `total - leaf_reserve`.
    /// Take units from both compartments together when available. Otherwise,
    /// wait for admission units before pool units to prevent deadlocks.
    /// Requests above admission capacity proceed without reserving units:
    /// callers may already hold admission units, so waiting could deadlock
    pub async fn admit(&self, bytes: usize) -> MemoryPermit {
        if bytes > self.inner.admission_max {
            self.inner.overshoots.fetch_add(1, Ordering::Relaxed);
            return self.permit(bytes, 0, 0);
        }
        if let Some(permit) = self.try_admit(bytes) {
            return permit;
        }
        self.inner.admission_waiters.fetch_add(1, Ordering::SeqCst);
        let _priority = AdmissionWait(&self.inner.admission_waiters);
        self.reclaim();
        let units = units_for(bytes);
        // Release admission units if cancelled while waiting for pool units
        let mut permit = self.permit(0, 0, 0);
        self.inner.take(Queue::Admission, word(0, units)).await;
        permit.admission_units = units;
        self.inner.take(Queue::Pool, word(units, 0)).await;
        permit.units = units;
        permit.bytes = bytes as u64;
        self.inner.meter(permit.bytes);
        permit
    }

    /// [`Self::admit`] without waiting: `None` when either compartment is short
    pub fn try_admit(&self, bytes: usize) -> Option<MemoryPermit> {
        if bytes > self.inner.admission_max {
            self.inner.overshoots.fetch_add(1, Ordering::Relaxed);
            return Some(self.permit(bytes, 0, 0));
        }
        let units = units_for(bytes);
        self.inner
            .try_take(word(units, units), POOL_QUEUED | ADMISSION_QUEUED)
            .then(|| self.permit(bytes, units, units))
    }

    /// Speculative admission never waits or overshoots, and yields to foreground waiters.
    pub(crate) fn try_speculate(&self, bytes: usize) -> Option<MemoryPermit> {
        if bytes > self.inner.admission_max
            || bytes as u64 > (self.inner.total as u64).saturating_sub(self.resident_bytes())
            || self.inner.admission_waiters.load(Ordering::SeqCst) != 0
        {
            return None;
        }
        let permit = self.try_admit(bytes)?;
        if self.inner.admission_waiters.load(Ordering::SeqCst) != 0
            || self.resident_bytes() > self.inner.total as u64
        {
            return None;
        }
        Some(permit)
    }

    pub(crate) fn register_reclaimer<T: Reclaim + 'static>(&self, reclaimer: &Arc<T>) {
        let reclaimer: Arc<dyn Reclaim> = reclaimer.clone();
        let mut reclaimers = self.inner.reclaimers.lock().expect("budget reclaimers");
        reclaimers.retain(|r| r.strong_count() != 0);
        reclaimers.push(Arc::downgrade(&reclaimer));
    }

    fn reclaim(&self) {
        let reclaimers: Vec<_> = {
            let mut reclaimers = self.inner.reclaimers.lock().expect("budget reclaimers");
            reclaimers.retain(|r| r.strong_count() != 0);
            reclaimers.iter().filter_map(Weak::upgrade).collect()
        };
        for reclaimer in reclaimers {
            reclaimer.reclaim();
        }
    }

    fn permit(&self, bytes: usize, units: u32, admission_units: u32) -> MemoryPermit {
        self.inner.meter(bytes as u64);
        MemoryPermit {
            inner: self.inner.clone(),
            bytes: bytes as u64,
            units,
            admission_units,
            big_leaf: false,
        }
    }
}

impl Inner {
    fn meter(&self, bytes: u64) {
        let now = self.cur.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    /// Atomically reserve `want` if capacity is available and queues in
    /// `blockers` are empty
    fn try_take(&self, want: u64, blockers: u64) -> bool {
        let fits = |w: u64| {
            w & blockers == 0 && Queue::ALL.iter().all(|q| w & q.field() >= want & q.field())
        };
        self.state
            .0
            .try_update(Ordering::AcqRel, Ordering::Acquire, |w| {
                fits(w).then(|| w - want)
            })
            .is_ok()
    }

    /// Reserve `want` after earlier requests in `queue`
    async fn take(&self, queue: Queue, want: u64) {
        if self.try_take(want, queue.bit()) {
            return;
        }
        if let Queue::Big = queue {
            self.big_leaf_waits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.waits.fetch_add(1, Ordering::Relaxed);
        }
        Waiter {
            inner: self,
            queue,
            want,
            id: None,
        }
        .await;
    }

    fn release(&self, freed: u64) {
        if freed == 0 {
            return;
        }
        let old = self.state.0.fetch_add(freed, Ordering::AcqRel);
        if old & QUEUED == 0 {
            return;
        }
        let list = self.waitlist();
        for queue in Queue::ALL {
            if freed & queue.field() != 0
                && let Some(head) = list.queues[queue as usize].front()
            {
                head.waker.wake_by_ref();
            }
        }
    }

    fn waitlist(&self) -> MutexGuard<'_, Waitlist> {
        self.waitlist.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wake next request, or clear queue flag if no requests remain
    fn next_head(&self, waiters: &VecDeque<Slot>, queue: Queue) {
        if let Some(head) = waiters.front() {
            head.waker.wake_by_ref();
        } else {
            self.state.0.fetch_and(!queue.bit(), Ordering::AcqRel);
        }
    }
}

/// Pending request; remove from queue on drop
struct Waiter<'a> {
    inner: &'a Inner,
    queue: Queue,
    want: u64,
    id: Option<u64>,
}

impl Future for Waiter<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let inner = this.inner;
        let mut list = inner.waitlist();
        let id = if let Some(id) = this.id {
            id
        } else {
            let id = list.next_id;
            list.next_id += 1;
            list.queues[this.queue as usize].push_back(Slot {
                id,
                waker: cx.waker().clone(),
            });
            // Later releases see queue flag and wake first request.
            // Reservation below sees capacity from earlier releases
            inner.state.0.fetch_or(this.queue.bit(), Ordering::AcqRel);
            this.id = Some(id);
            id
        };
        let waiters = &mut list.queues[this.queue as usize];
        // Allow first request to reserve units despite its queue flag
        if waiters.front().is_some_and(|s| s.id == id) && inner.try_take(this.want, 0) {
            waiters.pop_front();
            this.id = None;
            inner.next_head(waiters, this.queue);
            return Poll::Ready(());
        }
        if let Some(slot) = waiters.iter_mut().find(|s| s.id == id) {
            slot.waker.clone_from(cx.waker());
        }
        Poll::Pending
    }
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        let Some(id) = self.id else {
            return;
        };
        let mut list = self.inner.waitlist();
        let waiters = &mut list.queues[self.queue as usize];
        let at = waiters
            .iter()
            .position(|s| s.id == id)
            .expect("queued waiter has a slot");
        waiters.remove(at);
        if at == 0 {
            self.inner.next_head(waiters, self.queue);
        }
    }
}

/// [`MemoryBudget::acquire`] against an optional pool: no pool means no
/// permit and no metering
pub async fn acquire_opt(budget: Option<&MemoryBudget>, bytes: usize) -> Option<MemoryPermit> {
    Some(budget?.acquire(bytes).await)
}

/// [`MemoryBudget::admit`] against an optional pool: no pool means no
/// permit and no metering
pub async fn admit_opt(budget: Option<&MemoryBudget>, bytes: usize) -> Option<MemoryPermit> {
    Some(budget?.admit(bytes).await)
}

/// [`MemoryPermit::absorb`] into an optional permit, taking `other` when empty
pub(crate) fn absorb_into(into: &mut Option<MemoryPermit>, other: MemoryPermit) {
    if let Some(p) = into {
        p.absorb(other);
    } else {
        *into = Some(other);
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
    /// Hold large value slot until retained bytes fit reserve
    big_leaf: bool,
}

impl MemoryPermit {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn absorb(&mut self, mut other: Self) {
        assert!(Arc::ptr_eq(&self.inner, &other.inner), "different budgets");
        assert!(!(self.big_leaf && other.big_leaf), "multiple large leaves");
        self.bytes += other.bytes;
        self.units += other.units;
        self.admission_units += other.admission_units;
        self.big_leaf |= other.big_leaf;
        other.bytes = 0;
        other.units = 0;
        other.admission_units = 0;
        other.big_leaf = false;
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
        let freed_adm = freed.min(self.admission_units);
        self.units = new_units;
        self.admission_units -= freed_adm;
        let mut freed = word(freed, freed_adm);
        if self.big_leaf && bytes as usize <= self.inner.leaf_max {
            self.big_leaf = false;
            freed |= BIG_FREE;
        }
        self.inner.release(freed);
        self.inner
            .cur
            .fetch_sub(self.bytes - bytes, Ordering::Relaxed);
        self.bytes = bytes;
    }
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        self.inner.cur.fetch_sub(self.bytes, Ordering::Relaxed);
        let big = if self.big_leaf { BIG_FREE } else { 0 };
        self.inner
            .release(word(self.units, self.admission_units) | big);
    }
}

/// Return cgroup limit or host memory, whichever is smaller
///
/// Return `None` when neither cgroup limit nor `/proc/meminfo` can be read
pub fn host_memory_limit() -> Option<usize> {
    memory_limit_from(
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        "/proc/meminfo",
    )
}

/// Treat cgroup v2 `max` and cgroup v1 values at or above host memory as no
/// limit
fn memory_limit_from(v2: &str, v1: &str, meminfo: &str) -> Option<usize> {
    let total = read_mem_total(meminfo);
    let limit = read_usize(v2).or_else(|| read_usize(v1));
    match (limit, total) {
        (Some(l), Some(t)) => Some(l.min(t)),
        (l, t) => l.or(t),
    }
}

fn read_usize(path: &str) -> Option<usize> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// First number on a `Key:  N ...` line, as `/proc/meminfo` and
/// `/proc/self/status` write them
pub fn proc_field(text: &str, key: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix(key))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn read_mem_total(path: &str) -> Option<usize> {
    let text = std::fs::read_to_string(path).ok()?;
    usize::try_from(proc_field(&text, "MemTotal:")?)
        .ok()?
        .checked_mul(1024)
}

fn units_for(bytes: usize) -> u32 {
    u32::try_from(bytes.div_ceil(GRANULARITY)).expect("bytes <= total <= 256 GiB")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &std::path::Path, name: &str, body: &str) -> String {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p.to_str().unwrap().to_string()
    }

    /// Prefer cgroup limit when lower than host memory
    #[test]
    fn memory_limit_prefers_the_binding_source() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let meminfo = write(d, "meminfo", "MemFree: 1 kB\nMemTotal:       2048 kB\n");
        let capped = write(d, "memory.max", "524288\n");
        let unlimited = write(d, "unlimited", "max\n");
        let v1_sentinel = write(d, "limit_in_bytes", "9223372036854771712\n");
        let missing = d.join("absent").to_str().unwrap().to_string();

        assert_eq!(
            memory_limit_from(&capped, &missing, &meminfo),
            Some(512 << 10)
        );
        // Unlimited cgroup values use host memory
        assert_eq!(
            memory_limit_from(&unlimited, &v1_sentinel, &meminfo),
            Some(2 << 20)
        );
        // Host memory works without cgroup files
        assert_eq!(
            memory_limit_from(&missing, &missing, &meminfo),
            Some(2 << 20)
        );
        // cgroup limit works without host memory
        assert_eq!(
            memory_limit_from(&capped, &missing, &missing),
            Some(512 << 10)
        );
        assert_eq!(memory_limit_from(&missing, &missing, &missing), None);
        // Invalid host memory is unavailable, not zero
        let garbage = write(d, "garbage", "MemTotal:       lots\n");
        assert_eq!(memory_limit_from(&missing, &missing, &garbage), None);
    }

    /// Never report zero for this host
    #[test]
    fn host_memory_limit_reads_this_host() {
        assert_ne!(host_memory_limit(), Some(0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn speculation_preserves_leaf_reserve_and_never_overshoots() {
        let b = MemoryBudget::with_leaf_reserve(64 << 10, 32 << 10);
        assert!(b.try_speculate((32 << 10) + 1).is_none());
        let speculative = b.try_speculate(32 << 10).unwrap();
        assert!(b.try_speculate(1).is_none());
        let leaf = b.acquire(32 << 10).await;
        assert_eq!(b.resident_bytes(), 64 << 10);
        assert_eq!(b.overshoots_total(), 0);
        assert_eq!(b.waits_total(), 0);
        drop(speculative);
        drop(leaf);
        assert_eq!(b.resident_bytes(), 0);
        let oversized = b.admit(128 << 10).await;
        assert!(b.try_speculate(1).is_none());
        assert_eq!(b.overshoots_total(), 1);
        drop(oversized);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn foreground_waiter_blocks_speculation_until_cancellation() {
        let b = MemoryBudget::with_leaf_reserve(4096, 2048);
        let held = b.admit(2048).await;
        let waiting_budget = b.clone();
        let waiter = tokio::spawn(async move { waiting_budget.admit(2048).await });
        while b.inner.admission_waiters.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(b.try_speculate(0).is_none());
        waiter.abort();
        assert!(matches!(waiter.await, Err(e) if e.is_cancelled()));
        assert_eq!(b.inner.admission_waiters.load(Ordering::SeqCst), 0);
        drop(held);
        assert!(b.try_speculate(2048).is_some());
        assert_eq!(b.resident_bytes(), 0);
    }

    #[test]
    fn try_admit_keeps_speculation() {
        struct Count(AtomicU64);
        impl Reclaim for Count {
            fn reclaim(&self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let b = MemoryBudget::with_leaf_reserve(4096, 2048);
        let count = Arc::new(Count(AtomicU64::new(0)));
        b.register_reclaimer(&count);
        let speculative = b.try_speculate(2048).unwrap();
        assert!(b.try_admit(1).is_none());
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
        drop(speculative);
        assert!(b.try_admit(1).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transferred_admission_and_leaf_release_original_compartments() {
        let b = MemoryBudget::with_leaf_reserve(8192, 4096);
        let mut speculative = b.try_speculate(4096).unwrap();
        speculative.shrink(1024);
        let mut leaf = b.acquire(4096).await;
        leaf.shrink(1024);
        let resident = b.resident_bytes();
        leaf.absorb(speculative);
        assert_eq!(b.resident_bytes(), resident);
        assert_eq!(leaf.bytes(), 2048);
        let admission = b.try_admit(3072).unwrap();
        assert!(b.try_speculate(1).is_none());
        drop(leaf);
        assert!(b.try_speculate(1024).is_some());
        drop(admission);
        assert_eq!(b.resident_bytes(), 0);
    }

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

    /// Allow only one large value above reserve at a time
    #[tokio::test(flavor = "current_thread")]
    async fn oversized_leaves_serialize() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let first = b.acquire(256 << 10).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.acquire(256 << 10).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "second large value waits");
        assert_eq!(b.resident_bytes(), 256 << 10);
        drop(first);
        let second = waiter.await.unwrap();
        assert_eq!(b.resident_bytes(), 256 << 10);
        assert_eq!(b.big_leaf_waits_total(), 1);
        drop(second);
        assert_eq!(b.resident_bytes(), 0);
    }

    /// Decode values within reserve while a large value resolves
    #[tokio::test(flavor = "current_thread")]
    async fn sized_leaf_ignores_the_gate() {
        let b = MemoryBudget::with_leaf_reserve(256 << 10, 128 << 10);
        let big = b.acquire(512 << 10).await;
        let small = b.acquire(64 << 10).await;
        assert_eq!(b.big_leaf_waits_total(), 0);
        drop((big, small));
        assert_eq!(b.resident_bytes(), 0);
    }

    /// Return large-value permit when retained bytes fit reserve
    #[tokio::test(flavor = "current_thread")]
    async fn shrink_below_reserve_releases_the_gate() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let mut peak = b.acquire(256 << 10).await;
        peak.shrink(8 << 10);
        // Large-value permit is free
        let next = b.acquire(256 << 10).await;
        assert_eq!(b.big_leaf_waits_total(), 0);
        drop(next);
        drop(peak);
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

    fn queued(b: &MemoryBudget, queue: Queue) -> bool {
        b.inner.state.0.load(Ordering::Acquire) & queue.bit() != 0
    }

    /// Allow leaf allocation while admission requests wait for capacity
    #[tokio::test(flavor = "current_thread")]
    async fn leaf_proceeds_past_queued_admitter() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let held = b.admit(64 << 10).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.admit(32 << 10).await.bytes() });
        while !queued(&b, Queue::Admission) {
            tokio::task::yield_now().await;
        }
        let leaf = b.acquire(32 << 10).await;
        assert_eq!(b.waits_total(), 1, "only the admitter waited");
        drop(leaf);
        drop(held);
        assert_eq!(waiter.await.unwrap(), 32 << 10);
        assert!(!queued(&b, Queue::Admission));
    }

    /// Serve queued requests before new requests, even if new requests fit
    #[tokio::test(flavor = "current_thread")]
    async fn try_admit_yields_to_queued_admitter() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let mut held = b.admit(64 << 10).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.admit(32 << 10).await.bytes() });
        while !queued(&b, Queue::Admission) {
            tokio::task::yield_now().await;
        }
        held.shrink(48 << 10);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "16 KiB free, head needs 32");
        assert!(b.try_admit(16 << 10).is_none());
        held.shrink(32 << 10);
        assert_eq!(waiter.await.unwrap(), 32 << 10);
        drop(held);
        assert!(b.try_admit(64 << 10).is_some());
    }

    /// Release admission units when cancelled while waiting for pool units
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_pool_wait_returns_admission_units() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let initial = b.inner.state.0.load(Ordering::Acquire);
        let leaf = b.acquire(64 << 10).await;
        let admitted = b.admit(32 << 10).await;
        let extra_leaf = b.acquire(32 << 10).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.admit(32 << 10).await.bytes() });
        while !queued(&b, Queue::Pool) {
            tokio::task::yield_now().await;
        }
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(!queued(&b, Queue::Pool));
        drop(extra_leaf);
        assert!(b.try_admit(32 << 10).is_some());
        drop((leaf, admitted));
        assert_eq!(b.inner.state.0.load(Ordering::Acquire), initial);
        assert_eq!(b.resident_bytes(), 0);
    }

    /// Allow smaller values while large values wait for their slot
    #[tokio::test(flavor = "current_thread")]
    async fn sized_leaf_passes_queued_big_leaf() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let first = b.acquire(256 << 10).await;
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.acquire(256 << 10).await.bytes() });
        while !queued(&b, Queue::Big) {
            tokio::task::yield_now().await;
        }
        let small = b.acquire(32 << 10).await;
        assert_eq!(b.waits_total(), 0);
        drop((small, first));
        assert_eq!(waiter.await.unwrap(), 256 << 10);
        assert!(!queued(&b, Queue::Big));
        assert_eq!(b.big_leaf_waits_total(), 1);
    }

    /// Release large value slot when cancelled while waiting for pool units
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_pool_wait_returns_big_leaf() {
        let b = MemoryBudget::with_leaf_reserve(128 << 10, 64 << 10);
        let initial = b.inner.state.0.load(Ordering::Acquire);
        let full = (b.acquire(64 << 10).await, b.acquire(64 << 10).await);
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.acquire(256 << 10).await.bytes() });
        while !queued(&b, Queue::Pool) {
            tokio::task::yield_now().await;
        }
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(!queued(&b, Queue::Pool));
        drop(full);
        let big = b.acquire(256 << 10).await;
        assert_eq!(b.big_leaf_waits_total(), 0);
        drop(big);
        assert_eq!(b.inner.state.0.load(Ordering::Acquire), initial);
        assert_eq!(b.resident_bytes(), 0);
    }
}
