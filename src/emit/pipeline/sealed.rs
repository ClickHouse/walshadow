//! Resolved Native blocks kept as prepared Data packet bodies from seal until
//! INSERT acknowledgement.
//!
//! clickhouse-c serializes each block once, compressing wire frames as it
//! goes, and column slabs drop. Inserters dial with compression blocks were
//! sealed in, since clickhouse-c refuses prepared bytes otherwise

use std::pin::Pin;

use clickhouse_c::{Allocator, BlockBuilder, Codec, PreparedBlock};

use crate::ch::{CompressionChoice, EmitterError};

/// One prepared block body, owned until its INSERT is durable
pub(crate) struct SealedBlock {
    prepared: PreparedBlock,
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

    #[test]
    fn blocks_seal_in_every_codec() {
        let big = vec![b'x'; 1 << 20];
        let rows: Vec<Option<&[u8]>> = vec![Some(b"a"), None, Some(b""), Some(&big), None];
        for compression in [
            CompressionChoice::None,
            CompressionChoice::Lz4,
            CompressionChoice::Zstd,
        ] {
            let sealed = seal_rows(&rows, compression);
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
}
