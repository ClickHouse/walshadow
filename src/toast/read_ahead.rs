//! PostgreSQL may reuse value IDs after vacuum. Births and schema barriers
//! invalidate speculative reads; earlier rows cannot consume later reads.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ahash::{HashMap, HashMapExt};
use tokio::sync::Semaphore;
use tokio_util::task::AbortOnDropHandle;

use crate::budget::{MemoryBudget, MemoryPermit};
use crate::decode::heap_decoder::{ColumnValue, ToastPointer};
use crate::toast::{pointer_extsize, pointer_footprint};

pub const AHEAD_READS: usize = crate::ops::bridge::MAX_BRIDGE_WORKERS;
pub const AHEAD_QUEUE: usize = 4096;
// Amortized slot and task bookkeeping
const ENTRY_OVERHEAD: usize = 512;

type Key = (u32, u32);

pub struct ReadValue {
    pub value: ColumnValue,
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
    type_oid: u32,
    read: Pending,
}

pub struct PendingReads {
    budget: MemoryBudget,
    runtime: tokio::runtime::Handle,
    /// Reads not yet finished, queued or running
    queued: Arc<Semaphore>,
    running: Arc<Semaphore>,
    entries: HashMap<Key, Entry>,
}

/// Fetch and decompress transients; rendered values top up once their size is known
fn read_cost(p: &ToastPointer) -> usize {
    pointer_extsize(p)
        .saturating_mul(2)
        .saturating_add(pointer_footprint(p))
        .saturating_add(ENTRY_OVERHEAD)
}

/// Bytes a value rendered to `rendered` bytes holds until planning consumes it
pub fn retained_bytes(rendered: usize, p: &ToastPointer) -> u64 {
    rendered.max(pointer_footprint(p)).saturating_mul(2) as u64
}

impl PendingReads {
    pub fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            runtime: tokio::runtime::Handle::current(),
            queued: Arc::new(Semaphore::new(AHEAD_QUEUE + AHEAD_READS)),
            running: Arc::new(Semaphore::new(AHEAD_READS)),
            entries: HashMap::new(),
        }
    }

    /// Spawn `read` under a speculative permit for its fetch transients
    pub fn reserve<F>(
        &mut self,
        db: u32,
        p: &ToastPointer,
        type_oid: u32,
        lsn: u64,
        read: impl FnOnce(MemoryPermit) -> F,
    ) where
        F: Future<Output = Option<ReadValue>> + Send + 'static,
    {
        let key = (p.va_toastrelid, p.va_valueid);
        if self.entries.contains_key(&key) {
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
        let read = read(permit);
        let started = Arc::new(AtomicBool::new(false));
        let running = self.running.clone();
        let task = self.runtime.spawn({
            let started = started.clone();
            async move {
                let _queued = queued;
                let _running = running.acquire_owned().await.ok()?;
                started.store(true, Ordering::Release);
                read.await
            }
        });
        self.entries.insert(
            key,
            Entry {
                db,
                p: *p,
                type_oid,
                read: Pending {
                    lsn,
                    started,
                    task: AbortOnDropHandle::new(task),
                },
            },
        );
    }

    pub fn take(&mut self, db: u32, p: &ToastPointer, type_oid: u32, lsn: u64) -> Option<Pending> {
        let key = (p.va_toastrelid, p.va_valueid);
        let e = self.entries.get(&key)?;
        if (e.db, e.p, e.type_oid) != (db, *p, type_oid) || e.read.lsn > lsn {
            return None;
        }
        Some(self.entries.remove(&key)?.read)
    }

    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }

    pub fn forget(&mut self, key: Key) {
        self.entries.remove(&key);
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
    use crate::schema::{JSONBOID, TEXTOID};

    fn pointer(id: u32, size: u32) -> ToastPointer {
        ToastPointer {
            va_rawsize: size as i32 + 4,
            va_extinfo: size,
            va_valueid: id,
            va_toastrelid: 16700,
        }
    }

    fn ready(value: &str) -> impl FnOnce(MemoryPermit) -> std::future::Ready<Option<ReadValue>> {
        let value = ColumnValue::Text(value.into());
        |permit| std::future::ready(Some(ReadValue { value, permit }))
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
        pending.reserve(1, &p, JSONBOID, 100, ready("v"));
        settle().await;
        assert!(pending.take(2, &p, JSONBOID, 100).is_none());
        assert!(pending.take(1, &p, TEXTOID, 100).is_none());
        assert!(pending.take(1, &p, JSONBOID, 99).is_none());
        let value = pending
            .take(1, &p, JSONBOID, 100)
            .unwrap()
            .take()
            .await
            .unwrap();
        assert_eq!(value.value, ColumnValue::Text("v".into()));
        assert!(pending.take(1, &p, JSONBOID, 100).is_none());
        assert!(budget.resident_bytes() > 0);
        drop(value);
        assert_eq!(budget.resident_bytes(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_budget_refuses_new_reads_and_forget_releases_bytes() {
        let mut pending = PendingReads::new(MemoryBudget::new(1024));
        let budget = pending.budget().clone();
        let p = pointer(1, 1);
        pending.reserve(1, &p, TEXTOID, 100, |permit| async move {
            let _permit = permit;
            std::future::pending().await
        });
        settle().await;
        pending.reserve(1, &pointer(2, 1), TEXTOID, 100, ready("v"));
        assert!(pending.take(1, &pointer(2, 1), TEXTOID, 100).is_none());
        pending.forget((p.va_toastrelid, p.va_valueid));
        settle().await;
        assert_eq!(budget.resident_bytes(), 0);
        pending.reserve(1, &pointer(2, 1), TEXTOID, 100, ready("v"));
        assert!(pending.take(1, &pointer(2, 1), TEXTOID, 100).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn planning_does_not_wait_for_queued_reads() {
        let mut pending = PendingReads::new(MemoryBudget::new(4096));
        let p = pointer(1, 1);
        pending.reserve(1, &p, TEXTOID, 100, ready("v"));
        let slot = pending.take(1, &p, TEXTOID, 100).unwrap();
        assert!(slot.take().await.is_none());
    }
}
