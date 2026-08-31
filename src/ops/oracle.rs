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
use crate::schema::RelAttr;

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

/// Render extension types with dynamic OIDs that the bridge can't (or
/// shouldn't) resolve, from their raw on-disk bytes to text, in place:
/// - PostGIS `geography`/`geometry` 2-D points → WKT `POINT(x y)` (typoutput
///   would yield HEXEWKB, and the oracle can't call `ST_AsText`).
/// - pgvector `vector`/`halfvec` → `[a,b,c]` (decoded in-tree so the shadow
///   needs no pgvector; the emitter parses this into `Array(Float32)`).
pub fn render_ext_columns(attrs: &[RelAttr], columns: &mut [Option<ColumnValue>]) {
    for att in attrs {
        if att.dropped {
            continue;
        }
        let Ok(idx) = usize::try_from(att.attnum - 1) else {
            continue;
        };
        let Some(cell) = columns.get_mut(idx) else {
            continue;
        };
        let Some(ColumnValue::PgPending { raw, .. }) = cell.as_ref() else {
            continue;
        };
        let rendered = match att.type_name.as_str() {
            "geography" | "geometry" => gserialized_point_to_wkt(raw),
            "vector" | "halfvec" => vector_to_text(raw),
            _ => None,
        };
        if let Some(text) = rendered {
            *cell = Some(ColumnValue::Text(text));
        }
    }
}

/// pgvector on-disk `vector`: `[dim u16-le][unused u16-le][f32-le × dim]` →
/// `[a,b,c]`. `None` if the body is short of `dim` floats.
fn vector_to_text(raw: &[u8]) -> Option<String> {
    if raw.len() < 4 {
        return None;
    }
    let dim = u16::from_le_bytes(raw[0..2].try_into().ok()?) as usize;
    if raw.len() < 4 + dim * 4 {
        return None;
    }
    let mut out = String::from("[");
    for i in 0..dim {
        if i > 0 {
            out.push(',');
        }
        let off = 4 + i * 4;
        let f = f32::from_le_bytes(raw[off..off + 4].try_into().ok()?);
        out.push_str(&f.to_string());
    }
    out.push(']');
    Some(out)
}

/// PostGIS on-disk GSERIALIZED → `POINT(x y)` for 2-D points. Layout:
/// `[srid(3) + gflags(1)][geomtype u32-le][…]`; POINT has geomtype 1 and its
/// two `f64`-LE coordinates are the trailing 16 bytes. `None` for non-points.
fn gserialized_point_to_wkt(raw: &[u8]) -> Option<String> {
    if raw.len() < 16 {
        return None;
    }
    let geomtype = u32::from_le_bytes(raw[4..8].try_into().ok()?);
    if geomtype != 1 {
        return None;
    }
    let n = raw.len();
    let x = f64::from_le_bytes(raw[n - 16..n - 8].try_into().ok()?);
    let y = f64::from_le_bytes(raw[n - 8..n].try_into().ok()?);
    Some(format!("POINT({x} {y})"))
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
    fn gserialized_2d_point_renders_wkt() {
        // [srid+gflags:4][geomtype=1:4][X f64le][Y f64le]
        let mut raw = vec![0u8, 0, 0, 0, 1, 0, 0, 0];
        raw.extend_from_slice(&30.5_f64.to_le_bytes());
        raw.extend_from_slice(&81.25_f64.to_le_bytes());
        assert_eq!(
            gserialized_point_to_wkt(&raw).as_deref(),
            Some("POINT(30.5 81.25)")
        );
        // non-point geomtype → None
        raw[4] = 2;
        assert_eq!(gserialized_point_to_wkt(&raw), None);
    }

    #[test]
    fn vector_body_renders_bracket_list() {
        let mut raw = vec![3u8, 0, 0, 0]; // dim=3, unused=0
        for f in [1.5f32, 2.0, 3.25] {
            raw.extend_from_slice(&f.to_le_bytes());
        }
        assert_eq!(vector_to_text(&raw).as_deref(), Some("[1.5,2,3.25]"));
    }

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
