//! Shared PG-15+ WAL page-header parsing for the two segment walkers.
//!
//! [`parse_page_header`] is pure (no walker state). Validation failures
//! (bad magic / version / flags) surface as `Err`; non-error
//! [`PageHeaderParse`] outcomes let each caller apply its own short-tail
//! policy (EOF vs truncation vs "need more bytes").

use thiserror::Error;
use walrus::pg::walparser::{
    WAL_PAGE_SIZE, X_LOG_RECORD_ALIGNMENT, XLP_LONG_HEADER, XLP_PAGE_MAGIC_PG15,
};

pub const PAGE_SIZE: usize = WAL_PAGE_SIZE as usize;
const SHORT_HEADER_SIZE: usize = 20;
const LONG_HEADER_SIZE: usize = SHORT_HEADER_SIZE + 16;

/// XLP_FIRST_IS_CONTRECORD | XLP_LONG_HEADER | XLP_BKP_REMOVABLE; any
/// other bit means a corrupt page header.
const XLP_ALL_FLAGS: u16 = 0x0007;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WalkError {
    #[error("page header at offset {0} has bad magic 0x{1:04x}")]
    BadPageMagic(usize, u16),
    #[error(
        "page header at offset {0} has magic 0x{1:04x} (PG ≤ 14); walshadow requires PG 15+ (FPI bit layout)"
    )]
    UnsupportedSourceVersion(usize, u16),
    #[error("page header at offset {0} declares invalid info flags 0x{1:04x}")]
    BadPageInfo(usize, u16),
    #[error("record at offset {offset} has zero xl_tot_len")]
    ZeroRecord { offset: usize },
    #[error("record at offset {offset} has xl_tot_len < {min}")]
    ShortRecord { offset: usize, min: usize },
    #[error("page at offset {0} ends mid-record but next page header missing")]
    Truncated(usize),
}

/// Non-error outcome of [`parse_page_header`]. Incomplete variants carry
/// no data; caller decides whether a short tail is clean EOF, truncation,
/// or a signal to wait for more bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum PageHeaderParse {
    /// Fewer than short-header bytes from `page_start`; can't read magic/info.
    ShortHeaderIncomplete,
    /// Long-header flag set but the full long header hasn't landed.
    LongHeaderIncomplete,
    /// `magic == 0 && info == 0`: zero page, end of valid data.
    ZeroPage,
    Valid {
        /// PG-15 vs PG-14 FPI bit semantics key off this.
        magic: u16,
        /// `page_start + align_up(header_size, alignment)`.
        data_start: usize,
        /// `xlp_rem_len`: bytes of a record continued from the prior page.
        remaining_data_len: usize,
    },
}

pub fn parse_page_header(bytes: &[u8], page_start: usize) -> Result<PageHeaderParse, WalkError> {
    if page_start + SHORT_HEADER_SIZE > bytes.len() {
        return Ok(PageHeaderParse::ShortHeaderIncomplete);
    }
    let buf = &bytes[page_start..];
    let magic = u16::from_le_bytes(buf[0..2].try_into().unwrap());
    let info = u16::from_le_bytes(buf[2..4].try_into().unwrap());
    if magic == 0 && info == 0 {
        return Ok(PageHeaderParse::ZeroPage);
    }
    if magic & 0xFF00 != 0xD100 {
        return Err(WalkError::BadPageMagic(page_start, magic));
    }
    if magic < XLP_PAGE_MAGIC_PG15 {
        return Err(WalkError::UnsupportedSourceVersion(page_start, magic));
    }
    if info & !XLP_ALL_FLAGS != 0 {
        return Err(WalkError::BadPageInfo(page_start, info));
    }
    let is_long = (info & XLP_LONG_HEADER) != 0;
    let header_size = if is_long {
        LONG_HEADER_SIZE
    } else {
        SHORT_HEADER_SIZE
    };
    if page_start + header_size > bytes.len() {
        return Ok(PageHeaderParse::LongHeaderIncomplete);
    }
    let remaining_data_len = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
    let data_start = page_start + align_up(header_size, X_LOG_RECORD_ALIGNMENT);
    Ok(PageHeaderParse::Valid {
        magic,
        data_start,
        remaining_data_len,
    })
}

pub fn align_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(magic: u16, info: u16, remaining_data_len: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&magic.to_le_bytes());
        v.extend_from_slice(&info.to_le_bytes());
        v.extend_from_slice(&1u32.to_le_bytes()); // timeline
        v.extend_from_slice(&0u64.to_le_bytes()); // page_address
        v.extend_from_slice(&remaining_data_len.to_le_bytes());
        v
    }

    #[test]
    fn short_buffer_reports_incomplete() {
        assert_eq!(
            parse_page_header(&[0u8; 4], 0).unwrap(),
            PageHeaderParse::ShortHeaderIncomplete
        );
    }

    #[test]
    fn zero_page_detected() {
        assert_eq!(
            parse_page_header(&[0u8; SHORT_HEADER_SIZE], 0).unwrap(),
            PageHeaderParse::ZeroPage
        );
    }

    #[test]
    fn long_header_flag_without_bytes_reports_incomplete() {
        let buf = header(XLP_PAGE_MAGIC_PG15, XLP_LONG_HEADER, 0);
        assert_eq!(buf.len(), SHORT_HEADER_SIZE);
        assert_eq!(
            parse_page_header(&buf, 0).unwrap(),
            PageHeaderParse::LongHeaderIncomplete
        );
    }

    #[test]
    fn short_header_valid_data_start_aligned() {
        let buf = header(XLP_PAGE_MAGIC_PG15, 0, 0);
        match parse_page_header(&buf, 0).unwrap() {
            PageHeaderParse::Valid {
                magic, data_start, ..
            } => {
                assert_eq!(magic, XLP_PAGE_MAGIC_PG15);
                assert_eq!(
                    data_start,
                    align_up(SHORT_HEADER_SIZE, X_LOG_RECORD_ALIGNMENT)
                );
            }
            other => panic!("expected Valid, got {other:?}"),
        }
    }

    #[test]
    fn rejects_garbage_magic() {
        let buf = header(0xFFFF, 1, 0);
        assert!(matches!(
            parse_page_header(&buf, 0),
            Err(WalkError::BadPageMagic(0, 0xFFFF))
        ));
    }

    #[test]
    fn rejects_pre_pg15_magic() {
        let buf = header(0xD10D, 0, 0);
        assert!(matches!(
            parse_page_header(&buf, 0),
            Err(WalkError::UnsupportedSourceVersion(0, 0xD10D))
        ));
    }

    #[test]
    fn rejects_unknown_flags() {
        let buf = header(XLP_PAGE_MAGIC_PG15, 0x0010, 0);
        assert!(matches!(
            parse_page_header(&buf, 0),
            Err(WalkError::BadPageInfo(0, 0x0010))
        ));
    }
}
