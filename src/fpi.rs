//! Reconstruct a full 8 KiB page from `XLogRecordBlock.image`. PG's
//! recovery calls this `RestoreBlockImage`
//! (src/backend/access/transam/xlogreader.c). walshadow mirrors that
//! surface so heap + xact-buffer decoders + BASEBACKUP.md path 1B+2A can
//! consume FPIs produced under `wal_compression = pglz|lz4|zstd`

use wal_rs::pg::walparser::{BLOCK_SIZE, FpiCompressionMethod, XLogRecordBlock};

pub const PAGE_BYTES: usize = BLOCK_SIZE as usize;

#[derive(Debug, thiserror::Error)]
pub enum FpiError {
    #[error("block carries no image")]
    NoImage,
    #[error("unrecognised compression bits {0:#04x}")]
    UnknownCodec(u8),
    #[error("hole offset {offset} + length {length} > BLCKSZ")]
    BadHole { offset: u16, length: u16 },
    #[error("pglz: corrupt stream")]
    Pglz,
    #[error("lz4: {0}")]
    Lz4(String),
    #[error("zstd: {0}")]
    Zstd(String),
    #[error("decoded size {got} != BLCKSZ - hole_length ({expected})")]
    SizeMismatch { got: usize, expected: usize },
}

/// Reconstruct 8 KiB page this block's FPI represents.
/// `page_magic` selects PG-14 vs PG-15 bimg_info bit layout; pass
/// `XLogPageHeader.magic` of the page that started the record
pub fn restore_block_image(
    block: &XLogRecordBlock<'_>,
    page_magic: u16,
) -> Result<[u8; PAGE_BYTES], FpiError> {
    if !block.header.has_image() {
        return Err(FpiError::NoImage);
    }
    let ih = &block.header.image_header;
    let hole_offset = ih.hole_offset as usize;
    let hole_length = ih.hole_length as usize;
    if hole_offset + hole_length > PAGE_BYTES {
        return Err(FpiError::BadHole {
            offset: ih.hole_offset,
            length: ih.hole_length,
        });
    }
    let body_len = PAGE_BYTES - hole_length;
    let mut scratch = [0u8; PAGE_BYTES];
    let body = &mut scratch[..body_len];

    match ih.compression_method(page_magic) {
        Some(FpiCompressionMethod::Pglz) => {
            let written = pglz::decompress_into(&block.image, body, true).ok_or(FpiError::Pglz)?;
            if written != body_len {
                return Err(FpiError::SizeMismatch {
                    got: written,
                    expected: body_len,
                });
            }
        }
        Some(FpiCompressionMethod::Lz4) => {
            let written = lz4_flex::block::decompress_into(&block.image, body)
                .map_err(|e| FpiError::Lz4(e.to_string()))?;
            if written != body_len {
                return Err(FpiError::SizeMismatch {
                    got: written,
                    expected: body_len,
                });
            }
        }
        Some(FpiCompressionMethod::Zstd) => {
            let written = zstd::bulk::decompress_to_buffer(&block.image, body)
                .map_err(|e| FpiError::Zstd(e.to_string()))?;
            if written != body_len {
                return Err(FpiError::SizeMismatch {
                    got: written,
                    expected: body_len,
                });
            }
        }
        None => {
            // uncompressed: image bytes are already BLCKSZ - hole_length
            // (parse.rs enforces this); reject codec-bit corruption
            if ih.is_compressed(page_magic) {
                return Err(FpiError::UnknownCodec(ih.info));
            }
            if block.image.len() != body_len {
                return Err(FpiError::SizeMismatch {
                    got: block.image.len(),
                    expected: body_len,
                });
            }
            body.copy_from_slice(&block.image);
        }
    }

    // splice hole: scratch currently holds body_len pre-hole-removed
    // bytes packed at offset 0; spread around hole zeros
    if hole_length > 0 {
        let mut page = [0u8; PAGE_BYTES];
        page[..hole_offset].copy_from_slice(&scratch[..hole_offset]);
        // hole region remains zero from page init
        page[hole_offset + hole_length..].copy_from_slice(
            &scratch[hole_offset..hole_offset + (PAGE_BYTES - hole_offset - hole_length)],
        );
        Ok(page)
    } else {
        Ok(scratch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::walparser::{
        BKP_BLOCK_HAS_IMAGE, BKP_IMAGE_COMPRESS_LZ4, BKP_IMAGE_COMPRESS_PGLZ,
        BKP_IMAGE_COMPRESS_ZSTD, BKP_IMAGE_HAS_HOLE, XLP_PAGE_MAGIC_PG15, XLogRecordBlockHeader,
        XLogRecordBlockImageHeader,
    };

    fn synth_page() -> [u8; PAGE_BYTES] {
        let mut p = [0u8; PAGE_BYTES];
        for (i, b) in p.iter_mut().enumerate() {
            *b = (i & 0xFF) as u8;
        }
        p
    }

    fn build_block(
        image: Vec<u8>,
        info: u8,
        hole_offset: u16,
        hole_length: u16,
    ) -> XLogRecordBlock<'static> {
        let mut header = XLogRecordBlockHeader::new(0);
        header.fork_flags = BKP_BLOCK_HAS_IMAGE;
        header.image_header = XLogRecordBlockImageHeader {
            image_length: image.len() as u16,
            hole_offset,
            hole_length,
            info,
        };
        XLogRecordBlock {
            header,
            image: std::borrow::Cow::Owned(image),
            data: std::borrow::Cow::Borrowed(&[]),
        }
    }

    #[test]
    fn uncompressed_no_hole_round_trip() {
        let page = synth_page();
        let block = build_block(page.to_vec(), 0, 0, 0);
        let out = restore_block_image(&block, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(out, page);
    }

    #[test]
    fn uncompressed_with_hole_splices() {
        let mut page = synth_page();
        let hole_offset = 1024;
        let hole_length = 2048;
        // hole bytes in source page set to non-zero so we can verify zeroing
        for b in &mut page[hole_offset..hole_offset + hole_length] {
            *b = 0xFF;
        }
        let mut packed = Vec::with_capacity(PAGE_BYTES - hole_length);
        packed.extend_from_slice(&page[..hole_offset]);
        packed.extend_from_slice(&page[hole_offset + hole_length..]);
        let block = build_block(
            packed,
            BKP_IMAGE_HAS_HOLE,
            hole_offset as u16,
            hole_length as u16,
        );
        let out = restore_block_image(&block, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(&out[..hole_offset], &page[..hole_offset]);
        assert!(
            out[hole_offset..hole_offset + hole_length]
                .iter()
                .all(|&b| b == 0)
        );
        assert_eq!(
            &out[hole_offset + hole_length..],
            &page[hole_offset + hole_length..],
        );
    }

    #[test]
    fn no_image_errors() {
        let mut block = build_block(vec![0u8; PAGE_BYTES], 0, 0, 0);
        block.header.fork_flags = 0; // strip HAS_IMAGE
        assert!(matches!(
            restore_block_image(&block, XLP_PAGE_MAGIC_PG15),
            Err(FpiError::NoImage),
        ));
    }

    #[test]
    fn bad_hole_errors() {
        let block = build_block(vec![0u8; 100], BKP_IMAGE_HAS_HOLE, 7000, 2000);
        assert!(matches!(
            restore_block_image(&block, XLP_PAGE_MAGIC_PG15),
            Err(FpiError::BadHole { .. }),
        ));
    }

    #[test]
    fn uncompressed_size_mismatch_errors() {
        // image length != BLCKSZ - hole_length under uncompressed path
        let block = build_block(vec![0u8; 100], 0, 0, 0);
        assert!(matches!(
            restore_block_image(&block, XLP_PAGE_MAGIC_PG15),
            Err(FpiError::SizeMismatch { .. }),
        ));
    }

    #[test]
    fn pglz_round_trip() {
        let page = synth_page();
        let compressed = pglz::compress(&page, &pglz::Strategy::ALWAYS).expect("pglz compress");
        let block = build_block(compressed, BKP_IMAGE_COMPRESS_PGLZ, 0, 0);
        let out = restore_block_image(&block, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(out, page);
    }

    #[test]
    fn pglz_round_trip_with_hole() {
        let page = synth_page();
        let hole_offset = 2048;
        let hole_length = 1024;
        let mut packed = Vec::with_capacity(PAGE_BYTES - hole_length);
        packed.extend_from_slice(&page[..hole_offset]);
        packed.extend_from_slice(&page[hole_offset + hole_length..]);
        let compressed = pglz::compress(&packed, &pglz::Strategy::ALWAYS).expect("pglz compress");
        let block = build_block(
            compressed,
            BKP_IMAGE_COMPRESS_PGLZ | BKP_IMAGE_HAS_HOLE,
            hole_offset as u16,
            hole_length as u16,
        );
        let out = restore_block_image(&block, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(&out[..hole_offset], &page[..hole_offset]);
        assert!(
            out[hole_offset..hole_offset + hole_length]
                .iter()
                .all(|&b| b == 0)
        );
        assert_eq!(
            &out[hole_offset + hole_length..],
            &page[hole_offset + hole_length..],
        );
    }

    #[test]
    fn lz4_round_trip() {
        let page = synth_page();
        let compressed = lz4_flex::block::compress(&page);
        let block = build_block(compressed, BKP_IMAGE_COMPRESS_LZ4, 0, 0);
        let out = restore_block_image(&block, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(out, page);
    }

    #[test]
    fn zstd_round_trip() {
        let page = synth_page();
        let compressed = zstd::bulk::compress(&page, 0).expect("zstd compress");
        let block = build_block(compressed, BKP_IMAGE_COMPRESS_ZSTD, 0, 0);
        let out = restore_block_image(&block, XLP_PAGE_MAGIC_PG15).unwrap();
        assert_eq!(out, page);
    }

    #[test]
    fn lz4_size_mismatch_short_payload() {
        // Compressed payload represents fewer than BLCKSZ bytes; with hole_length=0
        // body_len == BLCKSZ, decompress writes short, SizeMismatch fires.
        let short = vec![0u8; 256];
        let compressed = lz4_flex::block::compress(&short);
        let block = build_block(compressed, BKP_IMAGE_COMPRESS_LZ4, 0, 0);
        assert!(matches!(
            restore_block_image(&block, XLP_PAGE_MAGIC_PG15),
            Err(FpiError::SizeMismatch { .. }),
        ));
    }

    #[test]
    fn zstd_size_mismatch_short_payload() {
        let short = vec![0u8; 256];
        let compressed = zstd::bulk::compress(&short, 0).expect("zstd compress");
        let block = build_block(compressed, BKP_IMAGE_COMPRESS_ZSTD, 0, 0);
        assert!(matches!(
            restore_block_image(&block, XLP_PAGE_MAGIC_PG15),
            Err(FpiError::SizeMismatch { .. }),
        ));
    }

    #[test]
    fn pglz_corrupt_image_errors() {
        // pglz::decompress_into runs with check_complete=true, so any size
        // mismatch returns None and surfaces FpiError::Pglz. Cover that arm.
        let block = build_block(vec![0u8; 16], BKP_IMAGE_COMPRESS_PGLZ, 0, 0);
        assert!(matches!(
            restore_block_image(&block, XLP_PAGE_MAGIC_PG15),
            Err(FpiError::Pglz),
        ));
    }
}
