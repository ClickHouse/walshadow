//! Recently decoded TOAST values, so a later row version carrying the same
//! unchanged pointer skips the store round trip, decompression and render
//!
//! PostgreSQL hands a value id out again only once every chunk of its old
//! value is gone from the toast index, and a row update that keeps a value
//! keeps its pointer. Commits plan in commit order, and each commit forgets
//! the ids whose chunks it wrote, so an entry holds the newest committed
//! value for its id. TRUNCATE, rewrites and DDL clear the whole cache: their
//! births land in a new relation generation that may not decode

use std::collections::VecDeque;

use ahash::{HashMap, HashMapExt};

use crate::decode::heap_decoder::{ColumnValue, ToastPointer};

/// Default `[toast] value_cache_bytes`
pub const VALUE_CACHE_BYTES: usize = 1 << 30;
/// Map slot, key and recency slot beside the value
const ENTRY_OVERHEAD: usize = 128;

type Key = (u32, u32);

struct Entry {
    db: u32,
    va_rawsize: i32,
    va_extinfo: u32,
    type_oid: u32,
    tick: u64,
    cost: usize,
    value: ColumnValue,
}

/// Byte-bounded, least recently used first out
pub struct ValueCache {
    budget: usize,
    bytes: usize,
    clock: u64,
    entries: HashMap<Key, Entry>,
    /// Keys by last use; a slot whose tick differs from its entry's is stale
    order: VecDeque<(Key, u64)>,
}

impl ValueCache {
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            bytes: 0,
            clock: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Value for `p` decoded as `type_oid`. A pointer whose sizes differ
    /// from the entry's names another value
    pub fn get(&mut self, db: u32, p: &ToastPointer, type_oid: u32) -> Option<ColumnValue> {
        let key = (p.va_toastrelid, p.va_valueid);
        let e = self.entries.get_mut(&key)?;
        if (e.db, e.va_rawsize, e.va_extinfo, e.type_oid)
            != (db, p.va_rawsize, p.va_extinfo, type_oid)
        {
            return None;
        }
        self.clock += 1;
        e.tick = self.clock;
        let value = e.value.clone();
        self.order.push_back((key, self.clock));
        self.compact();
        Some(value)
    }

    pub fn put(&mut self, db: u32, p: &ToastPointer, type_oid: u32, value: &ColumnValue) {
        let key = (p.va_toastrelid, p.va_valueid);
        self.forget(key);
        let cost = value.approx_bytes() + ENTRY_OVERHEAD;
        if cost > self.budget {
            return;
        }
        while self.bytes + cost > self.budget {
            self.evict_oldest();
        }
        self.clock += 1;
        self.bytes += cost;
        self.entries.insert(
            key,
            Entry {
                db,
                va_rawsize: p.va_rawsize,
                va_extinfo: p.va_extinfo,
                type_oid,
                tick: self.clock,
                cost,
                value: value.clone(),
            },
        );
        self.order.push_back((key, self.clock));
        self.compact();
    }

    /// Drop `(toast_relid, value_id)` in every database
    pub fn forget(&mut self, key: Key) {
        if let Some(e) = self.entries.remove(&key) {
            self.bytes -= e.cost;
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }

    fn evict_oldest(&mut self) {
        while let Some((key, tick)) = self.order.pop_front() {
            if self.entries.get(&key).is_some_and(|e| e.tick == tick) {
                self.forget(key);
                return;
            }
        }
    }

    /// Bound stale recency slots to a multiple of live entries
    fn compact(&mut self) {
        if self.order.len() > 2 * self.entries.len() + 1024 {
            let entries = &self.entries;
            self.order
                .retain(|(key, tick)| entries.get(key).is_some_and(|e| e.tick == *tick));
        }
    }
}
