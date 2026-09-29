use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use ahash::HashMap;

use crate::schema::TableKey;

pub type TableRowCounts = KeyedCounts<TableKey>;
/// Keyed by ClickHouse type and `ColumnEncoding::label`
pub type CellCounts = KeyedCounts<(String, &'static str)>;

/// Counter per label set. Hot paths hold the returned handle, so a series
/// exists at zero before its first increment
#[derive(Debug)]
pub struct KeyedCounts<K> {
    counts: RwLock<HashMap<K, Arc<AtomicU64>>>,
}

impl<K> Default for KeyedCounts<K> {
    fn default() -> Self {
        Self {
            counts: RwLock::default(),
        }
    }
}

impl<K: Eq + Hash + Ord + Clone> KeyedCounts<K> {
    pub fn counter(&self, key: &K) -> Arc<AtomicU64> {
        if let Some(n) = self.counts.read().expect("keyed counts lock").get(key) {
            return n.clone();
        }
        self.counts
            .write()
            .expect("keyed counts lock")
            .entry(key.clone())
            .or_default()
            .clone()
    }

    /// Heaviest first
    pub fn snapshot(&self) -> Vec<(K, u64)> {
        let mut rows: Vec<_> = self
            .counts
            .read()
            .expect("keyed counts lock")
            .iter()
            .map(|(k, n)| (k.clone(), n.load(Ordering::Relaxed)))
            .collect();
        rows.sort_unstable_by(|l, r| r.1.cmp(&l.1).then_with(|| l.0.cmp(&r.0)));
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::RelName;

    #[test]
    fn counts_accumulate_per_key_and_rank_by_weight() {
        let counts = TableRowCounts::default();
        let a = TableKey::new(1, RelName::new("public", "a"));
        let b = TableKey::new(1, RelName::new("public", "b"));
        let other_db = TableKey::new(2, RelName::new("public", "a"));
        counts.counter(&b);
        counts.counter(&a).fetch_add(3, Ordering::Relaxed);
        counts.counter(&a).fetch_add(4, Ordering::Relaxed);
        counts.counter(&other_db).fetch_add(9, Ordering::Relaxed);
        assert_eq!(counts.snapshot(), vec![(other_db, 9), (a, 7), (b, 0)]);
    }
}
