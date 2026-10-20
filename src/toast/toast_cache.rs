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

use crate::decode::heap_decoder::{ColumnValue, DecodedTuple, ToastPointer};
use crate::schema::RelDescriptor;

/// Map slot, key and recency slot beside the value
const ENTRY_OVERHEAD: usize = 128;

/// Database, toast relation, value id. Relation oids repeat across
/// databases, eg tables cloned from a template database
type Key = (u32, u32, u32);

struct Entry {
    va_rawsize: i32,
    va_extinfo: u32,
    type_oid: u32,
    tick: u64,
    cost: usize,
    value: ColumnValue,
}

/// Byte-bounded, least recently used first out
pub struct ToastCache {
    budget: usize,
    bytes: usize,
    clock: u64,
    entries: HashMap<Key, Entry>,
    /// Keys by last use; a slot whose tick differs from its entry's is stale
    order: VecDeque<(Key, u64)>,
}

fn key(db: u32, p: &ToastPointer) -> Key {
    (db, p.va_toastrelid, p.va_valueid)
}

impl ToastCache {
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
        let key = key(db, p);
        let e = self.entries.get_mut(&key)?;
        if (e.va_rawsize, e.va_extinfo, e.type_oid) != (p.va_rawsize, p.va_extinfo, type_oid) {
            return None;
        }
        self.clock += 1;
        e.tick = self.clock;
        let value = e.value.clone();
        self.order.push_back((key, self.clock));
        self.compact();
        Some(value)
    }

    /// Replace each external column of `t` this cache holds, returning
    /// values and bytes served
    pub fn fill(&mut self, db: u32, t: &mut DecodedTuple, rel: &RelDescriptor) -> (u64, usize) {
        let (mut hits, mut bytes) = (0, 0);
        for (col, attr) in t.columns.iter_mut().zip(&rel.attributes) {
            if let Some(ColumnValue::ExternalToast(p)) = col
                && let Some(v) = self.get(db, p, attr.type_oid)
            {
                hits += 1;
                bytes += v.approx_bytes();
                *col = Some(v);
            }
        }
        (hits, bytes)
    }

    pub fn put(&mut self, db: u32, p: &ToastPointer, type_oid: u32, value: &ColumnValue) {
        let key = key(db, p);
        self.remove(key);
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

    /// Drop `(toast_relid, value_id)` keys born in `db`
    pub fn forget<'a>(&mut self, db: u32, keys: impl IntoIterator<Item = &'a (u32, u32)>) {
        for &(relid, vid) in keys {
            self.remove((db, relid, vid));
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }

    fn remove(&mut self, key: Key) {
        if let Some(e) = self.entries.remove(&key) {
            self.bytes -= e.cost;
        }
    }

    fn evict_oldest(&mut self) {
        while let Some((key, tick)) = self.order.pop_front() {
            if self.entries.get(&key).is_some_and(|e| e.tick == tick) {
                self.remove(key);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{JSONBOID, TEXTOID};

    fn ptr(vid: u32) -> ToastPointer {
        ToastPointer {
            va_rawsize: 104,
            va_extinfo: 100,
            va_valueid: vid,
            va_toastrelid: 16500,
        }
    }

    fn text(n: usize) -> ColumnValue {
        ColumnValue::Text("x".repeat(n))
    }

    #[test]
    fn hit_requires_db_sizes_and_type() {
        let mut c = ToastCache::new(1 << 20);
        c.put(1, &ptr(7), TEXTOID, &text(100));
        assert_eq!(c.get(1, &ptr(7), TEXTOID), Some(text(100)));
        assert_eq!(c.get(2, &ptr(7), TEXTOID), None);
        assert_eq!(c.get(1, &ptr(7), JSONBOID), None);
        let resized = ToastPointer {
            va_extinfo: 99,
            ..ptr(7)
        };
        assert_eq!(c.get(1, &resized, TEXTOID), None);
    }

    /// Same oids in another database are another value
    #[test]
    fn databases_do_not_share_keys() {
        let mut c = ToastCache::new(1 << 20);
        c.put(1, &ptr(7), TEXTOID, &text(1));
        c.put(2, &ptr(7), TEXTOID, &text(2));
        c.forget(2, &[(16500, 7)]);
        assert_eq!(c.get(1, &ptr(7), TEXTOID), Some(text(1)));
        assert_eq!(c.get(2, &ptr(7), TEXTOID), None);
    }

    #[test]
    fn evicts_least_recently_used_within_budget() {
        let cost = text(100).approx_bytes() + ENTRY_OVERHEAD;
        let mut c = ToastCache::new(2 * cost);
        c.put(1, &ptr(1), TEXTOID, &text(100));
        c.put(1, &ptr(2), TEXTOID, &text(100));
        c.get(1, &ptr(1), TEXTOID).unwrap();
        c.put(1, &ptr(3), TEXTOID, &text(100));
        assert_eq!(c.bytes, 2 * cost);
        assert!(c.get(1, &ptr(2), TEXTOID).is_none());
        assert!(c.get(1, &ptr(1), TEXTOID).is_some());
        assert!(c.get(1, &ptr(3), TEXTOID).is_some());
    }

    #[test]
    fn replace_and_oversize_keep_accounting() {
        let mut c = ToastCache::new(1024);
        c.put(1, &ptr(1), TEXTOID, &text(100));
        c.put(1, &ptr(1), TEXTOID, &text(200));
        assert_eq!(c.bytes, text(200).approx_bytes() + ENTRY_OVERHEAD);
        // Larger than budget: stale entry goes, nothing replaces it
        c.put(1, &ptr(1), TEXTOID, &text(2048));
        assert_eq!((c.bytes, c.entries.len()), (0, 0));
        c.put(1, &ptr(2), TEXTOID, &text(100));
        c.clear();
        assert_eq!((c.bytes, c.entries.len(), c.order.len()), (0, 0, 0));
    }

    /// Repeated hits compact recency slots instead of growing them
    #[test]
    fn stale_recency_slots_stay_bounded() {
        let mut c = ToastCache::new(1 << 20);
        c.put(1, &ptr(1), TEXTOID, &text(10));
        for _ in 0..10_000 {
            c.get(1, &ptr(1), TEXTOID).unwrap();
        }
        assert!(c.order.len() <= 2 * c.entries.len() + 1025);
    }
}
