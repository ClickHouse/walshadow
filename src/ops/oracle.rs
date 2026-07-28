//! PgPending resolver backed by shadow PG.
//!
//! For varlena types outside walshadow's local matrix (`jsonb`, arrays,
//! `tsvector`, ranges, custom domains, ...), [`Oracle::resolve_pending`]
//! renders on-disk bytes through PG's own `typoutput`, replacing PgPending with
//! [`ColumnValue::Text`].
//!
//! The route is [`Bridge`], the preloaded worker: whole tuple in one round
//! trip, and no `pg_proc` row, so it works on a shadow whose catalog is a
//! read-only physical copy of source's.
//!
//! Daemon requires bridge at startup. Per-item and later transport failures
//! leave affected values as raw on-disk bytes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::decode::heap_decoder::ColumnValue;
use crate::ops::bridge::{Bridge, DecodedItem};

crate::atomic_stats! {
    pub struct OracleStats {
        /// Columns rendered to text
        pub resolved,
        /// Columns left as raw bytes: NULL or a per-item decode error
        pub fallback_raw,
        /// Bridge transport errors, single bucket
        pub errors,
    }
}

pub struct Oracle {
    bridge: Arc<Bridge>,
    pub stats: Arc<OracleStats>,
}

impl Oracle {
    pub fn new(bridge: Arc<Bridge>) -> Self {
        Self {
            bridge,
            stats: Arc::new(OracleStats::default()),
        }
    }

    pub fn bridge(&self) -> &Arc<Bridge> {
        &self.bridge
    }

    /// `None` when the worker is unreachable (counted via `stats.errors`) or
    /// `typoutput` raised, so the emitter falls back to raw bytes.
    pub async fn resolve_pending(&self, type_oid: u32, raw: &[u8]) -> Option<String> {
        self.decode_batch(&[(type_oid, raw)]).await?.pop()?
    }

    /// Render a whole batch in one round trip. `None` means the transport
    /// failed and nothing was answered; `Some` holds one slot per item, `None`
    /// where `typoutput` raised.
    async fn decode_batch(&self, items: &[(u32, &[u8])]) -> Option<Vec<Option<String>>> {
        let answered = match self.bridge.decode(items).await {
            Ok(v) if v.len() == items.len() => v,
            Ok(_) | Err(_) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let mut out = Vec::with_capacity(answered.len());
        for item in answered {
            match item {
                DecodedItem::Text(s) => {
                    self.stats.resolved.fetch_add(1, Ordering::Relaxed);
                    out.push(Some(s));
                }
                DecodedItem::Error(_) => {
                    self.stats.fallback_raw.fetch_add(1, Ordering::Relaxed);
                    out.push(None);
                }
            }
        }
        Some(out)
    }
}

/// PgPending → Text on success; on fall-back PgPending stays put and emitter
/// writes raw bytes via `encode_value`.
///
/// Whole tuple costs one round trip.
pub async fn resolve_pending_tuple(oracle: &Oracle, columns: &mut [Option<ColumnValue>]) {
    let answered = {
        let items: Vec<(u32, &[u8])> = columns
            .iter()
            .filter_map(|col| match col {
                Some(ColumnValue::PgPending { type_oid, raw })
                | Some(ColumnValue::Unsupported { type_oid, raw }) => {
                    Some((*type_oid, raw.as_slice()))
                }
                _ => None,
            })
            .collect();
        if items.is_empty() {
            return;
        }
        oracle.decode_batch(&items).await
    };
    let Some(answered) = answered else {
        return;
    };
    let mut next = answered.into_iter();
    for col in columns.iter_mut() {
        let (Some(ColumnValue::PgPending { .. }) | Some(ColumnValue::Unsupported { .. })) = col
        else {
            continue;
        };
        // Counts matched at request time, so the iterator cannot run dry
        if let Some(Some(s)) = next.next() {
            *col = Some(ColumnValue::Text(s));
        }
    }
}

impl OracleStats {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = format!("oracle resolved={}", ld(&self.resolved));
        let pairs: [(&str, u64); 2] = [
            ("fallback", ld(&self.fallback_raw)),
            ("err", ld(&self.errors)),
        ];
        for (label, n) in pairs {
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_summary_skips_zero_buckets() {
        let s = OracleStats::default();
        s.resolved.store(4, Ordering::Relaxed);
        s.errors.store(2, Ordering::Relaxed);
        let out = s.summary();
        assert!(out.contains("resolved=4"));
        assert!(out.contains("err=2"));
        assert!(!out.contains("fallback"));
    }
}
