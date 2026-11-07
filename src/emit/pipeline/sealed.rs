//! Resolved Native blocks kept as prepared Data packet bodies from seal until
//! INSERT acknowledgement.
//!
//! clickhouse-c serializes each block once, compressing wire frames as it
//! goes, and column slabs drop. Inserters write those bytes on connections
//! negotiating the same compression; a connection refusing them, such as a
//! named destination configured otherwise, gets the block decoded and rebuilt

use std::pin::Pin;

use clickhouse_c::{
    Allocator, Block, BlockBuilder, BlockOpts, BlockReader, Codec, PreparedBlock, SliceIo,
};

use crate::ch::{CompressionChoice, EmitterError};

/// TCP Data packet body framing, which prepared bytes carry
const OPTS: BlockOpts = BlockOpts {
    has_block_info: true,
    has_custom_serialization: true,
    read_buffer_bytes: 0,
};

/// CityHash128 checksum, then method byte and two little-endian u32 sizes;
/// ClickHouse `src/Compression/CompressionInfo.h`
const CHECKSUM: usize = 16;
const FRAME_HEADER: usize = 9;
const METHOD_NONE: u8 = 0x02;
const METHOD_LZ4: u8 = 0x82;
const METHOD_ZSTD: u8 = 0x90;

/// One prepared block body, owned until its INSERT is durable
pub(crate) struct SealedBlock {
    prepared: PreparedBlock,
    compression: CompressionChoice,
}

impl SealedBlock {
    /// Serialize `bb` once in `compression`; `codec` must support it
    pub(crate) fn seal(
        bb: &BlockBuilder<'_>,
        compression: CompressionChoice,
        codec: Option<Pin<&Codec>>,
        alloc: Allocator,
    ) -> Result<Self, EmitterError> {
        let codec = codec.filter(|_| compression != CompressionChoice::None);
        Ok(Self {
            prepared: bb.prepare(compression.to_wire(), codec, alloc)?,
            compression,
        })
    }

    pub(crate) fn prepared(&self) -> &PreparedBlock {
        &self.prepared
    }

    /// Serialized body bytes before compression
    pub(crate) fn raw_len(&self) -> usize {
        self.prepared.raw_len()
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.prepared.capacity()
    }

    /// Decode the block, decompressing into `scratch`; the block owns its
    /// columns, so `scratch` is free again once this returns
    pub(crate) fn open(
        &self,
        alloc: Allocator,
        scratch: &mut Vec<u8>,
    ) -> Result<Block, EmitterError> {
        let bytes = self.prepared.as_bytes();
        let body = match self.compression {
            CompressionChoice::None => bytes,
            _ => {
                unframe(bytes, scratch)?;
                &scratch[..]
            }
        };
        if body.len() != self.raw_len() {
            return Err(EmitterError::Type(format!(
                "sealed block body {} bytes, sealed {}",
                body.len(),
                self.raw_len()
            )));
        }
        let mut io = SliceIo::new(body);
        let mut reader = BlockReader::new(io.as_mut(), alloc, OPTS)?;
        reader
            .read()?
            .filter(|b| b.n_rows() == self.prepared.n_rows())
            .ok_or_else(|| EmitterError::Type("sealed block decoded short".into()))
    }
}

/// Concatenate decompressed frames of `bytes` into `out`
fn unframe(mut bytes: &[u8], out: &mut Vec<u8>) -> Result<(), EmitterError> {
    let corrupt = |what: &str| EmitterError::Type(format!("sealed block frame {what}"));
    out.clear();
    while !bytes.is_empty() {
        let header = bytes
            .get(CHECKSUM..CHECKSUM + FRAME_HEADER)
            .ok_or_else(|| corrupt("header short"))?;
        let size = |at: usize| u32::from_le_bytes(header[at..at + 4].try_into().unwrap()) as usize;
        let (stored, raw) = (size(1), size(5));
        let data = stored
            .checked_sub(FRAME_HEADER)
            .and_then(|n| bytes.get(CHECKSUM + FRAME_HEADER..CHECKSUM + FRAME_HEADER + n))
            .ok_or_else(|| corrupt("body short"))?;
        let at = out.len();
        out.resize(at + raw, 0);
        let dst = &mut out[at..];
        let n = match header[0] {
            METHOD_LZ4 => lz4::block::decompress_to_buffer(data, Some(raw as i32), dst)
                .map_err(|e| corrupt(&format!("lz4: {e}")))?,
            METHOD_ZSTD => zstd::bulk::decompress_to_buffer(data, dst)
                .map_err(|e| corrupt(&format!("zstd: {e}")))?,
            METHOD_NONE if data.len() == raw => {
                dst.copy_from_slice(data);
                raw
            }
            m => return Err(corrupt(&format!("method {m:#x}"))),
        };
        if n != raw {
            return Err(corrupt("size"));
        }
        bytes = &bytes[CHECKSUM + stored..];
    }
    Ok(())
}

/// Rebuild a builder over a decoded block's columns, for `send_data`
pub(crate) fn builder_of(block: &Block) -> Result<BlockBuilder<'_>, EmitterError> {
    let mut bb = BlockBuilder::new();
    for i in 0..block.n_columns() {
        let name = block
            .column_name(i)
            .and_then(|n| std::str::from_utf8(n).ok())
            .ok_or_else(|| EmitterError::Type(format!("sealed block column {i} name")))?;
        let (Some(ty), Some(col)) = (block.column_type(i), block.column(i)) else {
            return Err(EmitterError::Type(format!("sealed block column {name}")));
        };
        bb.append_column(name, ty, col)?;
    }
    Ok(bb)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_c::{ColumnBuilder, TypeAst};

    fn alloc() -> Allocator {
        Allocator::global(&mimalloc::MiMalloc)
    }

    /// Block of a UInt64, a Nullable(String) and a UUID column
    fn seal_rows(strings: &[Option<&[u8]>], compression: CompressionChoice) -> SealedBlock {
        let n = strings.len();
        let ids: Vec<u8> = (0..n as u64).flat_map(u64::to_le_bytes).collect();
        let uuids: Vec<u8> = (0..16 * n).map(|i| (i * 7) as u8).collect();
        let (mut offsets, mut data, mut nulls) = (Vec::new(), Vec::new(), Vec::new());
        for s in strings {
            data.extend_from_slice(s.unwrap_or_default());
            offsets.push(data.len() as u64);
            nulls.push(u8::from(s.is_none()));
        }
        let types =
            ["UInt64", "Nullable(String)", "UUID"].map(|t| TypeAst::parse(t, alloc()).unwrap());
        let id = ColumnBuilder::fixed(&ids, 8, n).unwrap();
        let leaf = ColumnBuilder::string(&offsets, &data, n).unwrap();
        let text = leaf.nullable(&nulls).unwrap();
        let uuid = ColumnBuilder::fixed(&uuids, 16, n).unwrap();
        let mut bb = BlockBuilder::new();
        bb.append("id", types[0].view(), &id).unwrap();
        bb.append("text", types[1].view(), &text).unwrap();
        bb.append("uuid", types[2].view(), &uuid).unwrap();
        let codec = compression.build_codec().unwrap();
        SealedBlock::seal(
            &bb,
            compression,
            codec.as_ref().map(|c| c.as_ref()),
            alloc(),
        )
        .unwrap()
    }

    fn check(strings: &[Option<&[u8]>], compression: CompressionChoice) -> SealedBlock {
        let sealed = seal_rows(strings, compression);
        let mut scratch = Vec::new();
        let block = sealed.open(alloc(), &mut scratch).unwrap();
        assert_eq!(block.n_rows(), strings.len());
        let text = block.column(1).unwrap();
        let nulls = text.null_map().unwrap();
        let (offsets, data) = text.nullable_inner().unwrap().string().unwrap();
        let mut start = 0;
        for (i, s) in strings.iter().enumerate() {
            let end = offsets[i] as usize;
            assert_eq!(nulls[i] == 1, s.is_none());
            assert_eq!(&data[start..end], s.unwrap_or_default());
            start = end;
        }
        let (width, ids) = block.column(0).unwrap().fixed().unwrap();
        assert_eq!(width, 8);
        assert_eq!(ids.len(), 8 * strings.len());
        // Rebuilt for a refusing connection, the block prepares identically
        let codec = compression.build_codec().unwrap();
        let again = SealedBlock::seal(
            &builder_of(&block).unwrap(),
            compression,
            codec.as_ref().map(|c| c.as_ref()),
            alloc(),
        )
        .unwrap();
        assert_eq!(again.prepared().as_bytes(), sealed.prepared().as_bytes());
        sealed
    }

    #[test]
    fn blocks_round_trip_through_every_codec() {
        let big = vec![b'x'; 1 << 20];
        let rows: Vec<Option<&[u8]>> = vec![Some(b"a"), None, Some(b""), Some(&big), None];
        for compression in [
            CompressionChoice::None,
            CompressionChoice::Lz4,
            CompressionChoice::Zstd,
        ] {
            let sealed = check(&rows, compression);
            if compression == CompressionChoice::None {
                assert_eq!(sealed.prepared().as_bytes().len(), sealed.raw_len());
            } else {
                assert!(
                    sealed.allocated_bytes() * 10 < sealed.raw_len(),
                    "{compression:?}"
                );
            }
        }
    }

    #[test]
    fn incompressible_body_round_trips() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let noise: Vec<u8> = (0..200 << 10)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        for compression in [CompressionChoice::Lz4, CompressionChoice::Zstd] {
            check(&[Some(&noise)], compression);
        }
    }

    #[test]
    fn corrupt_frames_fail_to_open() {
        let sealed = seal_rows(&[Some(&b"abc"[..]); 100], CompressionChoice::Lz4);
        let bytes = sealed.prepared().as_bytes();
        let mut out = Vec::new();
        unframe(bytes, &mut out).unwrap();
        assert_eq!(out.len(), sealed.raw_len());
        assert!(unframe(&bytes[..bytes.len() - 1], &mut out).is_err());
        assert!(unframe(&bytes[..CHECKSUM + 4], &mut out).is_err());
        let mut method = bytes.to_vec();
        method[CHECKSUM] = 0x7f;
        assert!(unframe(&method, &mut out).is_err());
        let mut sizes = bytes.to_vec();
        sizes[CHECKSUM + 5] ^= 1;
        assert!(unframe(&sizes, &mut out).is_err());
        let none = [
            &[0u8; CHECKSUM][..],
            &[METHOD_NONE, 12, 0, 0, 0, 3, 0, 0, 0],
            b"xyz",
        ]
        .concat();
        unframe(&none, &mut out).unwrap();
        assert_eq!(out, b"xyz");
        let short = [&none[..CHECKSUM + 5], &[4, 0, 0, 0], b"xyz"].concat();
        assert!(unframe(&short, &mut out).is_err());
    }
}
