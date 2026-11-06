//! Resolved Native blocks kept compressed from seal until INSERT
//! acknowledgement.
//!
//! clickhouse-c serializes each block once; the body is compressed with the
//! configured codec and column slabs drop. An inserter decodes one block at
//! a time back into a scratch buffer to send it, so resident output costs
//! compressed bytes plus one block per inserter. Incompressible bodies stay
//! raw rather than expand

use std::pin::Pin;

use clickhouse_c::{Allocator, Block, BlockBuilder, BlockOpts, BlockReader, Io, SliceIo, sys};

use crate::ch::{CompressionChoice, EmitterError};

/// Native file framing: blocks never leave this process in this form
const OPTS: BlockOpts = BlockOpts {
    has_block_info: false,
    has_custom_serialization: false,
    read_buffer_bytes: 0,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stored {
    Raw,
    Lz4,
    Zstd,
}

/// One serialized block body, owned until its INSERT is durable
pub(crate) struct SealedBlock {
    stored: Stored,
    payload: Box<[u8]>,
    raw_len: usize,
    n_rows: usize,
}

impl SealedBlock {
    /// Serialize `bb` and compress its body. `scratch` holds the raw body
    /// between calls so sealing reuses one buffer
    pub(crate) fn seal(
        bb: &BlockBuilder<'_>,
        n_rows: usize,
        codec: CompressionChoice,
        scratch: &mut Vec<u8>,
    ) -> Result<Self, EmitterError> {
        scratch.clear();
        let mut io = VecIo::new(std::mem::take(scratch));
        let written = bb.write(io.as_mut(), OPTS);
        *scratch = io.as_mut().take();
        written?;
        let raw_len = scratch.len();
        let compressed = match codec {
            CompressionChoice::None => None,
            CompressionChoice::Lz4 => lz4::block::compress(scratch, None, false)
                .ok()
                .map(|c| (Stored::Lz4, c)),
            CompressionChoice::Zstd => zstd::bulk::compress(scratch, 0)
                .ok()
                .map(|c| (Stored::Zstd, c)),
        };
        let (stored, payload) = match compressed {
            Some((stored, c)) if c.len() < raw_len => (stored, c.into_boxed_slice()),
            _ => (Stored::Raw, scratch.as_slice().into()),
        };
        Ok(Self {
            stored,
            payload,
            raw_len,
            n_rows,
        })
    }

    /// Serialized body bytes before compression
    pub(crate) fn raw_len(&self) -> usize {
        self.raw_len
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.payload.len()
    }

    /// Decode the block, decompressing into `scratch`; the block owns its
    /// columns, so `scratch` is free again once this returns
    pub(crate) fn open(
        &self,
        alloc: Allocator,
        scratch: &mut Vec<u8>,
    ) -> Result<Block, EmitterError> {
        let body: &[u8] = match self.stored {
            Stored::Raw => &self.payload,
            Stored::Lz4 => {
                scratch.resize(self.raw_len, 0);
                let n = lz4::block::decompress_to_buffer(
                    &self.payload,
                    Some(self.raw_len as i32),
                    scratch,
                )
                .map_err(|e| EmitterError::Type(format!("sealed block lz4: {e}")))?;
                &scratch[..n]
            }
            Stored::Zstd => {
                scratch.resize(self.raw_len, 0);
                let n = zstd::bulk::decompress_to_buffer(&self.payload, &mut scratch[..])
                    .map_err(|e| EmitterError::Type(format!("sealed block zstd: {e}")))?;
                &scratch[..n]
            }
        };
        if body.len() != self.raw_len {
            return Err(EmitterError::Type(format!(
                "sealed block body {} bytes, sealed {}",
                body.len(),
                self.raw_len
            )));
        }
        let mut io = SliceIo::new(body);
        let mut reader = BlockReader::new(io.as_mut(), alloc, OPTS)?;
        reader
            .read()?
            .filter(|b| b.n_rows() == self.n_rows)
            .ok_or_else(|| EmitterError::Type("sealed block decoded short".into()))
    }
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

/// Write-only transport appending to an owned buffer
struct VecIo {
    io: sys::chc_io,
    out: Vec<u8>,
    _pin: std::marker::PhantomPinned,
}

impl VecIo {
    fn new(out: Vec<u8>) -> Pin<Box<Self>> {
        let mut boxed = Box::pin(Self {
            io: sys::chc_io {
                ud: std::ptr::null_mut(),
                read: Some(vec_read),
                write: Some(vec_write),
                check_cancel: None,
            },
            out,
            _pin: std::marker::PhantomPinned,
        });
        // SAFETY: context set once address is stable; value stays pinned
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            this.io.ud = (this as *mut Self).cast();
        }
        boxed
    }

    fn take(self: Pin<&mut Self>) -> Vec<u8> {
        // SAFETY: buffer is not structurally pinned; callbacks reach it only
        // through `ud` during a write call
        std::mem::take(unsafe { &mut self.get_unchecked_mut().out })
    }
}

// SAFETY: `io_ptr` points at the pinned callback table whose `ud` names
// this value; callbacks follow the chc_io contract
unsafe impl Io for VecIo {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: table address stays fixed while pinned
        unsafe { &mut self.get_unchecked_mut().io }
    }
}

unsafe extern "C" fn vec_write(
    ud: *mut std::ffi::c_void,
    buf: *const std::ffi::c_void,
    len: usize,
    _err: *mut sys::chc_err,
) -> std::ffi::c_int {
    // SAFETY: `ud` points at the pinned VecIo owning this table; C passes
    // `len` readable bytes
    unsafe {
        let this = &mut *ud.cast::<VecIo>();
        this.out
            .extend_from_slice(std::slice::from_raw_parts(buf.cast::<u8>(), len));
    }
    sys::CHC_OK
}

unsafe extern "C" fn vec_read(
    _ud: *mut std::ffi::c_void,
    _buf: *mut std::ffi::c_void,
    _len: usize,
    _out_n: *mut usize,
    err: *mut sys::chc_err,
) -> std::ffi::c_int {
    let _ = err;
    sys::CHC_ERR_USAGE
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_c::{ColumnBuilder, TypeAst};

    fn alloc() -> Allocator {
        Allocator::global(&mimalloc::MiMalloc)
    }

    /// Block of a UInt64, a Nullable(String) and a UUID column
    fn seal_rows(strings: &[Option<&[u8]>], codec: CompressionChoice) -> (SealedBlock, Vec<u8>) {
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
        let mut scratch = Vec::new();
        let sealed = SealedBlock::seal(&bb, n, codec, &mut scratch).unwrap();
        (sealed, scratch)
    }

    fn check(strings: &[Option<&[u8]>], codec: CompressionChoice) -> SealedBlock {
        let (sealed, raw) = seal_rows(strings, codec);
        assert_eq!(sealed.raw_len(), raw.len());
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
        // Reserialized for the wire, the block keeps its body
        let mut again = Vec::new();
        SealedBlock::seal(
            &builder_of(&block).unwrap(),
            strings.len(),
            CompressionChoice::None,
            &mut again,
        )
        .unwrap();
        assert_eq!(again, raw);
        sealed
    }

    #[test]
    fn blocks_round_trip_through_every_codec() {
        let big = vec![b'x'; 1 << 20];
        let rows: Vec<Option<&[u8]>> = vec![Some(b"a"), None, Some(b""), Some(&big), None];
        for codec in [
            CompressionChoice::None,
            CompressionChoice::Lz4,
            CompressionChoice::Zstd,
        ] {
            let sealed = check(&rows, codec);
            if codec == CompressionChoice::None {
                assert_eq!(sealed.stored, Stored::Raw);
            } else {
                assert!(
                    sealed.allocated_bytes() * 10 < sealed.raw_len(),
                    "{codec:?}"
                );
            }
        }
    }

    /// Bodies that do not shrink stay raw rather than expand
    #[test]
    fn incompressible_body_stays_raw() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let noise: Vec<u8> = (0..64 << 10)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        for codec in [CompressionChoice::Lz4, CompressionChoice::Zstd] {
            let sealed = check(&[Some(&noise)], codec);
            assert_eq!(sealed.stored, Stored::Raw);
            assert_eq!(sealed.allocated_bytes(), sealed.raw_len());
        }
    }

    #[test]
    fn corrupt_body_fails_to_open() {
        let (mut sealed, _) = seal_rows(&[Some(&b"abc"[..]); 100], CompressionChoice::Lz4);
        assert_eq!(sealed.stored, Stored::Lz4);
        sealed.raw_len += 1;
        assert!(sealed.open(alloc(), &mut Vec::new()).is_err());
    }
}
