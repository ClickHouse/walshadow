//! Drive a full segment: walk records, decide `Route`, NOOP-rewrite
//! ToDecoder records in place, emit manifest sidecar.

use thiserror::Error;
use walross::pg::walparser::{ParseError, XLogRecord, parse_record_from_bytes};

use crate::filter::{Filter, Route};
use crate::manifest::{Entry, FILTER_VERSION, Kind, Manifest, ManifestStats};
use crate::rewrite::{RewriteError, noop_replace};
use crate::segment::{SegmentWalker, WalkError};

#[derive(Debug, Error)]
pub enum FilterSegmentError {
    #[error("walk segment: {0}")]
    Walk(#[from] WalkError),
    #[error("parse record at offset {offset}: {source}")]
    Parse {
        offset: usize,
        #[source]
        source: ParseError,
    },
    #[error("rewrite record at offset {offset}: {source}")]
    Rewrite {
        offset: usize,
        #[source]
        source: RewriteError,
    },
}

/// Parsed record + page magic of the page its header sat on. Magic is
/// needed downstream to interpret PG-15-vs-PG-14 FPI bits via
/// `XLogRecordBlockImageHeader::is_compressed`. `'static` (via
/// [`XLogRecord::into_owned`]) so the batch return outlives source bytes
/// the parser borrowed.
#[derive(Debug, Clone)]
pub struct ParsedRecord {
    pub record: XLogRecord<'static>,
    pub page_magic: u16,
}

/// Emit rewritten bytes, manifest sidecar, parsed records in source order.
/// `filter` borrowed mutably so callers spanning multiple segments retain
/// `CatalogTracker` state across calls; emitted [`ManifestStats`] reflect
/// only records processed *this* call. Returned `Vec<ParsedRecord>` is the
/// parses-once hand-off; entries match `manifest.records` by index.
pub fn filter_segment(
    source_bytes: &[u8],
    source_name: &str,
    filter: &mut Filter,
) -> Result<(Vec<u8>, Manifest, Vec<ParsedRecord>), FilterSegmentError> {
    let stats_before = filter.stats;
    let relmap_before = filter.tracker.relmap_updates;
    let pgc_undecoded_before = filter.tracker.pg_class_writes_undecoded;
    let pgc_oid_in_prefix_before = filter.tracker.pg_class_writes_oid_in_prefix;

    let mut out = source_bytes.to_vec();
    let mut entries = Vec::new();
    let mut parsed_records = Vec::new();

    // Collect first: can't borrow `out` mutably while walking it immutably.
    let walked: Vec<_> = SegmentWalker::new(source_bytes).collect::<Result<Vec<_>, _>>()?;

    for record in walked {
        let parsed =
            parse_record_from_bytes(&record.logical_bytes, record.page_magic).map_err(|e| {
                FilterSegmentError::Parse {
                    offset: record.start_offset,
                    source: e,
                }
            })?;
        let route = filter.decide(&parsed);
        let kind = match route {
            Route::ToShadow => Kind::Kept,
            Route::ToDecoder => Kind::Dropped,
        };

        if route == Route::ToDecoder {
            if let [(off, len)] = record.byte_ranges.as_slice() {
                // Single-page: contiguous in `out`, NOOP in place.
                noop_replace(&mut out[*off..*off + *len]).map_err(|e| {
                    FilterSegmentError::Rewrite {
                        offset: record.start_offset,
                        source: e,
                    }
                })?;
            } else {
                // Cross-page: page-fragmented in `out`; NOOP contiguous
                // logical copy then scatter back.
                let mut buf = record.logical_bytes.clone();
                noop_replace(&mut buf).map_err(|e| FilterSegmentError::Rewrite {
                    offset: record.start_offset,
                    source: e,
                })?;
                let mut cursor = 0;
                for &(off, len) in &record.byte_ranges {
                    out[off..off + len].copy_from_slice(&buf[cursor..cursor + len]);
                    cursor += len;
                }
            }
        }

        entries.push(Entry {
            offset: record.start_offset as u64,
            len: parsed.header.total_record_length,
            rmid: parsed.header.resource_manager_id,
            info: parsed.header.info,
            kind,
        });
        parsed_records.push(ParsedRecord {
            record: parsed.into_owned(),
            page_magic: record.page_magic,
        });
    }

    let stats = ManifestStats::from_filter(
        &filter.stats.delta_from(&stats_before),
        filter.tracker.relmap_updates - relmap_before,
        filter.tracker.pg_class_writes_undecoded - pgc_undecoded_before,
        filter.tracker.pg_class_writes_oid_in_prefix - pgc_oid_in_prefix_before,
    );
    let manifest = Manifest {
        source_segment: source_name.to_string(),
        filter_version: FILTER_VERSION,
        records: entries,
        stats,
    };
    Ok((out, manifest, parsed_records))
}

#[cfg(test)]
mod tests {
    use super::*;
    use walross::pg::walparser::{
        WAL_PAGE_SIZE, WalParser, X_LOG_RECORD_HEADER_SIZE, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15,
        XLR_BLOCK_ID_DATA_SHORT,
    };

    const PAGE_SIZE: usize = WAL_PAGE_SIZE as usize;

    fn build_record(rmid: u8, body_payload: &[u8]) -> Vec<u8> {
        build_record_info(rmid, 0, body_payload)
    }

    fn build_record_info(rmid: u8, info: u8, body_payload: &[u8]) -> Vec<u8> {
        // Minimal record: only a SHORT-marked main_data, no block refs.
        let main_len = body_payload.len();
        let body_len = 2 + main_len; // SHORT marker + len + payload
        let total = X_LOG_RECORD_HEADER_SIZE + body_len;
        let mut v = Vec::with_capacity(total);
        v.extend_from_slice(&(total as u32).to_le_bytes());
        v.extend_from_slice(&0u32.to_le_bytes()); // xact
        v.extend_from_slice(&0u64.to_le_bytes()); // prev
        v.push(info);
        v.push(rmid);
        v.push(0);
        v.push(0);
        v.extend_from_slice(&0u32.to_le_bytes()); // crc placeholder
        v.push(XLR_BLOCK_ID_DATA_SHORT);
        v.push(main_len as u8);
        v.extend_from_slice(body_payload);
        let crc = crate::rewrite::compute_crc(&v);
        v[20..24].copy_from_slice(&crc.to_le_bytes());
        v
    }

    fn build_page_with_records(records: &[&[u8]]) -> Vec<u8> {
        let mut page = Vec::with_capacity(PAGE_SIZE);
        page.extend_from_slice(&XLP_PAGE_MAGIC_PG15.to_le_bytes());
        page.extend_from_slice(&XLP_LONG_HEADER.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes()); // timeline
        page.extend_from_slice(&0u64.to_le_bytes()); // page_address
        page.extend_from_slice(&0u32.to_le_bytes()); // remaining_data_len
        page.extend_from_slice(&12345u64.to_le_bytes()); // sysid
        page.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes()); // seg_size
        page.extend_from_slice(&8192u32.to_le_bytes()); // xlog_block_size
        page.extend_from_slice(&[0u8; 4]); // pad to 40
        for r in records {
            page.extend_from_slice(r);
            // align next record to 8 bytes
            let pad = (8 - (page.len() % 8)) % 8;
            page.extend_from_slice(&vec![0u8; pad]);
        }
        page.resize(PAGE_SIZE, 0);
        page
    }

    #[test]
    fn drops_user_keeps_special_round_trips() {
        use walross::pg::walparser::RmId;
        // Heap record has no block refs in this minimal build → Empty →
        // kept by safe default; verify xact record kept + re-parses clean.
        let r1 = build_record(RmId::Xact as u8, &[0xAA, 0xBB]);
        let r2 = build_record(RmId::Heap as u8, &[0xCC; 8]);
        let page = build_page_with_records(&[&r1, &r2]);

        let mut filter = Filter::new();
        let (out, mani, parsed) = filter_segment(&page, "test", &mut filter).unwrap();
        assert_eq!(out.len(), page.len());
        assert_eq!(mani.records.len(), 2);
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed[0].record.header.resource_manager_id,
            mani.records[0].rmid,
        );
        let mut parser = WalParser::new();
        let (_, records) = parser.parse_records_from_page(&out).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn manifest_lists_record_offsets() {
        use walross::pg::walparser::RmId;
        let r = build_record(RmId::Xact as u8, &[0u8; 4]);
        let page = build_page_with_records(&[&r]);
        let mut filter = Filter::new();
        let (_, mani, _) = filter_segment(&page, "seg-test", &mut filter).unwrap();
        assert_eq!(mani.source_segment, "seg-test");
        assert_eq!(mani.records.len(), 1);
        assert_eq!(mani.records[0].offset, 40); // 40-byte long page header
        assert_eq!(mani.records[0].rmid, RmId::Xact as u8);
        assert_eq!(mani.records[0].kind, Kind::Kept);
    }

    /// `XLOG_SWITCH` (rmgr RM_XLOG, info 0x40) must pass through unchanged.
    /// PG emits one at every `pg_switch_wal()` and archive_timeout expiry;
    /// shadow's recovery state machine needs the bytes intact. PG marks the
    /// rest of the segment as padding zeros after it, so the parser stops
    /// scanning past the record.
    #[test]
    fn xlog_switch_record_passes_through_filter() {
        use walross::pg::walparser::RmId;
        const XLOG_SWITCH: u8 = 0x40;
        let before = build_record(RmId::Xact as u8, &[0xDE, 0xAD]);
        let switch_rec = build_record_info(RmId::Xlog as u8, XLOG_SWITCH, &[]);
        let page = build_page_with_records(&[&before, &switch_rec]);

        let mut filter = Filter::new();
        let (out, mani, _) = filter_segment(&page, "switch", &mut filter).expect("filter");

        assert_eq!(out.len(), page.len(), "byte-preserving");
        assert_eq!(mani.records.len(), 2);
        let switch_entry = mani
            .records
            .iter()
            .find(|e| e.rmid == RmId::Xlog as u8 && (e.info & 0xF0) == XLOG_SWITCH)
            .expect("XLOG_SWITCH entry in manifest");
        assert_eq!(
            switch_entry.kind,
            Kind::Kept,
            "XLOG_SWITCH must be kept (special rmgr policy)",
        );
        let off = switch_entry.offset as usize;
        let len = switch_entry.len as usize;
        assert_eq!(
            &page[off..off + len],
            &out[off..off + len],
            "XLOG_SWITCH bytes must be passed through unchanged",
        );

        let mut parser = WalParser::new();
        let (_, parsed) = parser.parse_records_from_page(&out).expect("parse");
        assert!(
            parsed.iter().any(|r| r.is_wal_switch()),
            "filtered output must still contain an XLOG_SWITCH; got {} records",
            parsed.len(),
        );
    }
}
