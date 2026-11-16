//! Header-area walk of one record: fixed header, origin, toplevel xid and
//! block locators, leaving block data, images and main data untouched.
//! Rejects every record `parse_record_from_bytes` rejects, so a caller can
//! fall back to the full parse for its error. Layout per PG
//! `src/include/access/xlogrecord.h`, unchanged from PG 15 through 19

use walrus::pg::walparser::{
    BKP_BLOCK_HAS_IMAGE, BLOCK_SIZE, RelFileNode, X_LOG_RECORD_HEADER_SIZE, XLR_BLOCK_ID_DATA_LONG,
    XLR_BLOCK_ID_DATA_SHORT, XLogRecord, XLogRecordBlockImageHeader, XLogRecordHeader,
};

use crate::cursor::Cursor;

const XLR_MAX_BLOCK_ID: u8 = 32;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_SAME_REL: u8 = 0x80;

/// `bytes` as a record holding only `header` and its header area, calling
/// `block` per block reference. `None` when `block` declines one or the
/// record is malformed
pub(crate) fn walk(
    header: XLogRecordHeader,
    bytes: &[u8],
    page_magic: u16,
    mut block: impl FnMut(RelFileNode) -> bool,
) -> Option<XLogRecord<'static>> {
    let mut c = bytes.get(X_LOG_RECORD_HEADER_SIZE..header.total_record_length as usize)?;
    let mut record = XLogRecord {
        header,
        ..Default::default()
    };
    let mut last_rel = None;
    let mut next_block_id = 0;
    while !c.is_empty() {
        match c.u8()? {
            XLR_BLOCK_ID_DATA_SHORT => {
                record.main_data_len = c.u8()?.into();
                c.reserve(record.main_data_len as usize)?;
            }
            XLR_BLOCK_ID_DATA_LONG => {
                record.main_data_len = c.u32()?;
                c.reserve(record.main_data_len as usize)?;
            }
            XLR_BLOCK_ID_ORIGIN => record.origin = c.u16()?,
            XLR_BLOCK_ID_TOPLEVEL_XID => record.toplevel_xid = c.u32()?,
            id if id <= XLR_MAX_BLOCK_ID && id >= next_block_id => {
                next_block_id = id + 1;
                let flags = c.u8()?;
                let data_length = c.u16()?;
                if (flags & BKPBLOCK_HAS_DATA != 0) != (data_length != 0) {
                    return None;
                }
                c.reserve(data_length.into())?;
                if flags & BKP_BLOCK_HAS_IMAGE != 0 {
                    let image_length = image_header(&mut c, page_magic)?;
                    c.reserve(image_length.into())?;
                }
                let rel = if flags & BKPBLOCK_SAME_REL != 0 {
                    last_rel?
                } else {
                    c.rel_file_node()?
                };
                c.u32()?; // block number
                last_rel = Some(rel);
                if !block(rel) {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(record)
}

/// `record` without its block references and payloads
pub(crate) fn header_part(record: &XLogRecord) -> XLogRecord<'static> {
    XLogRecord {
        header: record.header.clone(),
        main_data_len: record.main_data_len,
        origin: record.origin,
        toplevel_xid: record.toplevel_xid,
        ..Default::default()
    }
}

/// Image length of a consistent block image header
fn image_header(c: &mut &[u8], page_magic: u16) -> Option<u16> {
    let mut h = XLogRecordBlockImageHeader {
        image_length: c.u16()?,
        hole_offset: c.u16()?,
        hole_length: 0,
        info: c.u8()?,
    };
    let compressed = h.is_compressed(page_magic);
    let has_hole = h.has_hole();
    h.hole_length = match (compressed, has_hole) {
        (true, true) => c.u16()?,
        (true, false) => 0,
        (false, _) => BLOCK_SIZE.saturating_sub(h.image_length),
    };
    let hole_ok = if has_hole {
        h.hole_offset != 0 && h.hole_length != 0 && h.image_length != BLOCK_SIZE
    } else {
        h.hole_offset == 0 && h.hole_length == 0
    };
    let length_ok = if compressed {
        h.image_length != BLOCK_SIZE
    } else {
        has_hole || h.image_length == BLOCK_SIZE
    };
    (hole_ok && length_ok).then_some(h.image_length)
}

#[cfg(test)]
mod tests {
    use walrus::pg::walparser::{XLP_PAGE_MAGIC_PG15, XLogRecordHeader};

    use super::*;

    const BKP_IMAGE_HAS_HOLE: u8 = 0x01;
    const BKP_IMAGE_COMPRESS_PGLZ: u8 = 0x04;

    fn walk_body(body: &[u8]) -> Option<XLogRecord<'static>> {
        let mut bytes = vec![0; X_LOG_RECORD_HEADER_SIZE];
        bytes.extend_from_slice(body);
        let header = XLogRecordHeader {
            total_record_length: bytes.len() as u32,
            ..Default::default()
        };
        walk(header, &bytes, XLP_PAGE_MAGIC_PG15, |_| true)
    }

    /// Block 0 carrying only a 4-byte image whose header ends in `image`
    fn image_block(image: &[u8]) -> Vec<u8> {
        let mut body = vec![0, BKP_BLOCK_HAS_IMAGE, 0, 0, 4, 0];
        body.extend_from_slice(image);
        body.extend_from_slice(&[0; 16]);
        body.extend_from_slice(&[0xAA; 4]);
        body
    }

    #[test]
    fn origin_rides_the_header() {
        assert_eq!(walk_body(&[253, 0x34, 0x12]).unwrap().origin, 0x1234);
    }

    #[test]
    fn malformed_block_references_reject() {
        // Data flag without length, unknown id, out-of-order ids
        assert!(walk_body(&[0, BKPBLOCK_HAS_DATA, 0, 0]).is_none());
        assert!(walk_body(&[200]).is_none());
        let once = image_block(&[0, 0, BKP_IMAGE_COMPRESS_PGLZ]);
        assert!(walk_body(&once).is_some());
        let header = &once[..once.len() - 4];
        let twice = [header, header, &[0xAA; 8]].concat();
        assert!(walk_body(&twice).is_none());
    }

    #[test]
    fn compressed_images_take_hole_length_only_with_a_hole() {
        let holed = [8, 0, BKP_IMAGE_HAS_HOLE | BKP_IMAGE_COMPRESS_PGLZ, 16, 0];
        assert!(walk_body(&image_block(&holed)).is_some());
        assert!(walk_body(&image_block(&[0, 0, BKP_IMAGE_COMPRESS_PGLZ])).is_some());
        // Hole flag with zero hole length
        let empty_hole = [8, 0, BKP_IMAGE_HAS_HOLE | BKP_IMAGE_COMPRESS_PGLZ, 0, 0];
        assert!(walk_body(&image_block(&empty_hole)).is_none());
    }
}
