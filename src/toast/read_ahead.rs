//! PostgreSQL may reuse value IDs after vacuum. Births and schema barriers
//! invalidate speculative reads; earlier rows cannot consume later reads.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use foldhash::{HashMap, HashMapExt};
use tokio::sync::Semaphore;
use tokio_util::task::AbortOnDropHandle;

use crate::budget::{MemoryBudget, MemoryPermit};
use crate::decode::datum::Datum;
use crate::decode::heap_decoder::ToastPointer;
use crate::toast::{pointer_extsize, pointer_footprint};

pub const AHEAD_READS: usize = crate::ops::bridge::MAX_BRIDGE_WORKERS;
pub const AHEAD_QUEUE: usize = 4096;
pub const AHEAD_BATCHED_READS: usize = 1024;
pub const AHEAD_BATCHED_QUEUE: usize = 65536;
pub const AHEAD_BATCH: usize = 256;
const AHEAD_REFERRERS: usize = 4;
// Amortized slot and task bookkeeping
const ENTRY_OVERHEAD: usize = 512;

type Key = (u32, u32);

pub struct ReadValue {
    pub value: Datum,
    pub permit: MemoryPermit,
}

/// Dropping a read aborts it and releases its permit
pub struct Pending {
    lsn: u64,
    started: Arc<AtomicBool>,
    task: AbortOnDropHandle<Option<ReadValue>>,
}

impl Pending {
    /// Planning does not wait for a read still queued
    pub async fn take(self) -> Option<ReadValue> {
        if !self.started.load(Ordering::Acquire) {
            return None;
        }
        self.task.await.ok().flatten()
    }
}

struct Entry {
    db: u32,
    p: ToastPointer,
    read: Pending,
}

pub struct PendingReads {
    budget: MemoryBudget,
    runtime: tokio::runtime::Handle,
    /// Reads not yet finished, queued or running
    queued: Arc<Semaphore>,
    running: Arc<Semaphore>,
    lazy: bool,
    entries: HashMap<Key, Vec<Entry>>,
}

/// Fetch and decompress transients; held values top up once their size is known
fn read_cost(p: &ToastPointer) -> usize {
    pointer_extsize(p)
        .saturating_mul(2)
        .saturating_add(pointer_footprint(p))
        .saturating_add(ENTRY_OVERHEAD)
}

/// Bytes a value of `held` raw bytes retains until planning consumes it
pub fn retained_bytes(held: usize, p: &ToastPointer) -> u64 {
    held.max(pointer_footprint(p)).saturating_mul(2) as u64
}

impl PendingReads {
    pub fn new(budget: MemoryBudget) -> Self {
        Self::admitting(budget, AHEAD_READS, AHEAD_QUEUE, false)
    }

    pub fn batched(budget: MemoryBudget) -> Self {
        Self::admitting(budget, AHEAD_BATCHED_READS, AHEAD_BATCHED_QUEUE, true)
    }

    fn admitting(budget: MemoryBudget, running: usize, queue: usize, lazy: bool) -> Self {
        Self {
            lazy,
            budget,
            runtime: tokio::runtime::Handle::current(),
            queued: Arc::new(Semaphore::new(queue + running)),
            running: Arc::new(Semaphore::new(running)),
            entries: HashMap::new(),
        }
    }

    /// Spawn `read` under a speculative permit for its fetch transients
    pub fn reserve<F>(
        &mut self,
        db: u32,
        p: &ToastPointer,
        lsn: u64,
        read: impl FnOnce(MemoryPermit, Arc<AtomicBool>) -> F,
    ) where
        F: Future<Output = Option<ReadValue>> + Send + 'static,
    {
        let key = (p.va_toastrelid, p.va_valueid);
        if self.entries.get(&key).is_some_and(|held| {
            held.len() >= AHEAD_REFERRERS || held.iter().any(|e| e.read.lsn == lsn)
        }) {
            return;
        }
        let Ok(queued) = self.queued.clone().try_acquire_owned() else {
            return;
        };
        // Refuse when full, older slots are planning's next takes. Unconsumed
        // slots leave on births, clears or foreground reclaim
        let Some(permit) = self.budget.try_speculate(read_cost(p)) else {
            return;
        };
        let started = Arc::new(AtomicBool::new(false));
        let read = read(permit, started.clone());
        let running = self.running.clone();
        let lazy = self.lazy;
        let task = self.runtime.spawn({
            let started = started.clone();
            async move {
                let _queued = queued;
                let _running = running.acquire_owned().await.ok()?;
                if !lazy {
                    started.store(true, Ordering::Release);
                }
                read.await
            }
        });
        self.entries.entry(key).or_default().push(Entry {
            db,
            p: *p,
            read: Pending {
                lsn,
                started,
                task: AbortOnDropHandle::new(task),
            },
        });
    }

    pub fn take(&mut self, db: u32, p: &ToastPointer, lsn: u64) -> Option<Pending> {
        let key = (p.va_toastrelid, p.va_valueid);
        let held = self.entries.get_mut(&key)?;
        let at = held
            .iter()
            .enumerate()
            .filter(|(_, e)| (e.db, e.p) == (db, *p) && e.read.lsn <= lsn)
            .min_by_key(|(_, e)| e.read.lsn)
            .map(|(at, _)| at)?;
        let read = held.swap_remove(at).read;
        if held.is_empty() {
            self.entries.remove(&key);
        }
        Some(read)
    }

    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }

    /// Drop read of `key` reserved below `before`. A later reservation saw
    /// pointer only after birth, so it reads the new value
    pub fn forget(&mut self, key: Key, before: u64) {
        if let Some(held) = self.entries.get_mut(&key) {
            held.retain(|e| e.read.lsn >= before);
            if held.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl crate::budget::Reclaim for std::sync::Mutex<PendingReads> {
    fn reclaim(&self) {
        self.lock().expect("pending reads").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::datum::build;

    fn pointer(id: u32, size: u32) -> ToastPointer {
        ToastPointer {
            va_rawsize: size as i32 + 4,
            va_extinfo: size,
            va_valueid: id,
            va_toastrelid: 16700,
        }
    }

    fn ready(
        value: &str,
    ) -> impl FnOnce(MemoryPermit, Arc<AtomicBool>) -> std::future::Ready<Option<ReadValue>> {
        let value = build::text(value);
        |permit, _| std::future::ready(Some(ReadValue { value, permit }))
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_reads_consume_once_and_keep_bytes_until_dropped() {
        let p = pointer(1, 4104);
        let mut pending = PendingReads::new(MemoryBudget::new(128 << 10));
        let budget = pending.budget().clone();
        pending.reserve(1, &p, 100, ready("v"));
        settle().await;
        assert!(pending.take(2, &p, 100).is_none());
        assert!(pending.take(1, &p, 99).is_none());
        let value = pending.take(1, &p, 100).unwrap().take().await.unwrap();
        assert_eq!(value.value, build::text("v"));
        assert!(pending.take(1, &p, 100).is_none());
        assert!(budget.resident_bytes() > 0);
        drop(value);
        assert_eq!(budget.resident_bytes(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_budget_refuses_new_reads_and_forget_releases_bytes() {
        let mut pending = PendingReads::new(MemoryBudget::new(1024));
        let budget = pending.budget().clone();
        let p = pointer(1, 1);
        let hold = |permit, _| async move {
            let _permit = permit;
            std::future::pending().await
        };
        pending.reserve(1, &p, 100, hold);
        settle().await;
        pending.reserve(1, &pointer(2, 1), 100, ready("v"));
        assert!(pending.take(1, &pointer(2, 1), 100).is_none());
        pending.forget((p.va_toastrelid, p.va_valueid), 100);
        assert!(
            pending.take(1, &p, 100).is_some(),
            "reserved at birth bound"
        );
        pending.reserve(1, &p, 100, hold);
        pending.forget((p.va_toastrelid, p.va_valueid), 101);
        settle().await;
        assert_eq!(budget.resident_bytes(), 0);
        pending.reserve(1, &pointer(2, 1), 100, ready("v"));
        assert!(pending.take(1, &pointer(2, 1), 100).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn referrers_of_one_value_take_reads_oldest_first() {
        let p = pointer(1, 16);
        let mut pending = PendingReads::new(MemoryBudget::new(128 << 10));
        pending.reserve(1, &p, 100, ready("early"));
        pending.reserve(1, &p, 200, ready("late"));
        pending.reserve(1, &p, 200, ready("repeat"));
        settle().await;
        assert!(pending.take(1, &p, 50).is_none());
        let early = pending.take(1, &p, 250).unwrap().take().await.unwrap();
        assert_eq!(early.value, build::text("early"));
        assert!(pending.take(1, &p, 150).is_none());
        let late = pending.take(1, &p, 250).unwrap().take().await.unwrap();
        assert_eq!(late.value, build::text("late"));
        assert!(pending.take(1, &p, 300).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn planning_does_not_wait_for_queued_reads() {
        let mut pending = PendingReads::new(MemoryBudget::new(4096));
        let p = pointer(1, 1);
        pending.reserve(1, &p, 100, ready("v"));
        let slot = pending.take(1, &p, 100).unwrap();
        assert!(slot.take().await.is_none());
    }
}
