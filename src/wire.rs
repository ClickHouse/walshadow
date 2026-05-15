//! PG on-disk WAL wire constants not exposed by wal-rs's public API.
//!
//! Mirrored from `src/include/access/xlogrecord.h` &
//! `xlog_internal.h`. Stable since PG 11 (PG 18 covered).

/// `SizeOfXLogRecord` — fixed-size header preceding every record.
pub const X_LOG_RECORD_HEADER_SIZE: usize = 24;

/// Record alignment between records on a page.
pub const X_LOG_RECORD_ALIGNMENT: usize = 8;

/// Main-data marker for ≤255-byte payloads (1-byte length follows).
pub const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
/// Main-data marker for >255-byte payloads (4-byte length follows).
pub const XLR_BLOCK_ID_DATA_LONG: u8 = 254;

/// XLogPageHeader.info: set when long (36-byte) page header is present.
pub const XLP_LONG_HEADER: u16 = 0x0002;

/// Lowest `XLogPageHeader.magic` walshadow accepts. PG 15's magic 0xD110
/// marks the FPI bit-shuffle floor (commit a14354cac moved 0x02 from
/// IS_COMPRESSED to APPLY). wal-rs's parser dispatches FPI-bit semantics
/// off `>= XLP_PAGE_MAGIC_MIN`, so anything PG 15+ re-parses with the
/// new layout. PG 16+ is the operationally supported floor (see
/// PLAN.md "Supported PostgreSQL versions"); PG 15 captures are
/// tolerated because the technical cost of accepting them is zero.
pub const XLP_PAGE_MAGIC_MIN: u16 = 0xD110;
