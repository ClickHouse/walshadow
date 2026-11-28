//! Heap page layout: header and line pointers. Mirror PostgreSQL
//! `src/include/storage/bufpage.h` and `src/include/storage/itemid.h`

/// `PageHeaderData` size, 24 bytes since PG 8.x
pub const SIZE_OF_PAGE_HEADER: usize = 24;
pub const SIZE_OF_ITEM_ID: usize = 4;
/// `lp_flags` value for a live tuple slot
pub const LP_NORMAL: u8 = 1;

/// Tuple bytes behind `offnum`'s line pointer, `None` unless the slot is
/// `LP_NORMAL` and in-bounds. Serves commit-time decode of a stashed
/// insert whose tuple rides only the FPI (`HEAP_INSERT_NO_LOGICAL` strips
/// `REGBUF_KEEP_DATA`, so a checkpoint mid-rewrite leaves no block data)
pub fn page_tuple_bytes(page: &[u8], offnum: u16) -> Option<&[u8]> {
    if offnum == 0 || page.len() < SIZE_OF_PAGE_HEADER {
        return None;
    }
    let pd_lower = u16::from_le_bytes(page[12..14].try_into().unwrap()) as usize;
    let off = SIZE_OF_PAGE_HEADER + (offnum as usize - 1) * SIZE_OF_ITEM_ID;
    if off + SIZE_OF_ITEM_ID > pd_lower.min(page.len()) {
        return None;
    }
    let raw = u32::from_le_bytes(page[off..off + 4].try_into().unwrap());
    let lp_off = (raw & 0x7FFF) as usize;
    let lp_flags = ((raw >> 15) & 0x3) as u8;
    let lp_len = ((raw >> 17) & 0x7FFF) as usize;
    if lp_flags != LP_NORMAL || lp_len == 0 || lp_off + lp_len > page.len() {
        return None;
    }
    Some(&page[lp_off..lp_off + lp_len])
}

/// Slots the line pointer array holds
pub fn page_max_offnum(page: &[u8]) -> u16 {
    if page.len() < SIZE_OF_PAGE_HEADER {
        return 0;
    }
    let pd_lower = u16::from_le_bytes(page[12..14].try_into().unwrap()) as usize;
    (pd_lower.saturating_sub(SIZE_OF_PAGE_HEADER) / SIZE_OF_ITEM_ID) as u16
}

/// Version every tuple on the page predates. In a restored image this is
/// the version before the carrying record: PG assembles the image inside
/// the critical section, `PageSetLSN` runs after `XLogInsert` returns.
/// `pd_lsn` is `PageXLogRecPtr`, high word (`xlogid`) first
pub fn page_pd_lsn(page: &[u8]) -> u64 {
    if page.len() < SIZE_OF_PAGE_HEADER {
        return 0;
    }
    u64::from_le_bytes(page[0..8].try_into().unwrap()).rotate_left(32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::fpi::PAGE_BYTES;

    /// Header bytes from a PG 18 heap page that `page_header()` reports as
    /// lsn 13/1085150
    #[test]
    fn page_pd_lsn_reads_xlogid_then_xrecoff() {
        let mut page = [0u8; PAGE_BYTES];
        page[0..8].copy_from_slice(&[0x13, 0x00, 0x00, 0x00, 0x50, 0x51, 0x08, 0x01]);
        assert_eq!(page_pd_lsn(&page), 0x13_0108_5150);
    }

    #[test]
    fn page_helpers_read_nothing_from_a_short_page() {
        let short = [0u8; 8];
        assert_eq!(page_tuple_bytes(&short, 1), None);
        assert_eq!(page_max_offnum(&short), 0);
        assert_eq!(page_pd_lsn(&short), 0);
    }
}
