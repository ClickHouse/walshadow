//! Record-boundary sidecar for a filtered segment. Filter is
//! byte-preserving so `filtered_lsn == source_lsn` for every record;
//! the replay-driver tool and round-trip test index records by offset.

use serde::{Deserialize, Serialize};

use crate::filter::FilterStats;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub source_segment: String,
    pub filter_version: u32,
    pub records: Vec<Entry>,
    pub stats: ManifestStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// Byte offset within the segment
    pub offset: u64,
    /// xl_tot_len
    pub len: u32,
    pub rmid: u8,
    /// `XLogRecordHeader.info` byte
    pub info: u8,
    pub kind: Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Kept,
    Dropped,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestStats {
    pub records: u64,
    pub kept: u64,
    pub dropped: u64,
    pub kept_bytes: u64,
    pub dropped_bytes: u64,
    pub catalog_keeps: u64,
    pub user_keeps: u64,
    pub special_keeps: u64,
    pub empty_keeps: u64,
    pub relmap_updates: u64,
    /// Malformed pg_class heap-write payloads (truncated / invalid
    /// `t_hoff`); zero on healthy captures
    pub pg_class_writes_undecoded: u64,
    /// pg_class UPDATE / HOT_UPDATE where PG prefix-compressed past the
    /// OID column (typically `VACUUM FULL` on a non-mapped catalog);
    /// filenode rotation recoverable only via `seed_from_source` or a
    /// later `XLOG_RELMAP_UPDATE`
    pub pg_class_writes_oid_in_prefix: u64,
}

impl ManifestStats {
    pub fn from_filter(
        stats: &FilterStats,
        relmap_updates: u64,
        pg_class_writes_undecoded: u64,
        pg_class_writes_oid_in_prefix: u64,
    ) -> Self {
        Self {
            records: stats.kept + stats.dropped,
            kept: stats.kept,
            dropped: stats.dropped,
            kept_bytes: stats.kept_bytes,
            dropped_bytes: stats.dropped_bytes,
            catalog_keeps: stats.kept_catalog,
            user_keeps: stats.kept_user,
            special_keeps: stats.kept_special,
            empty_keeps: stats.kept_empty,
            relmap_updates,
            pg_class_writes_undecoded,
            pg_class_writes_oid_in_prefix,
        }
    }
}

pub const FILTER_VERSION: u32 = 1;
