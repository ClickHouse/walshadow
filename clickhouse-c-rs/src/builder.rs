//! Block writer. The C library never copies the slabs handed in via
//! `chc_block_builder_append_*`; this wrapper holds the lifetime
//! invariant through `BlockBuilder<'a>`.

use core::marker::PhantomData;
use core::pin::Pin;
use core::ptr::NonNull;

use crate::alloc::Allocator;
use crate::block::BlockOpts;
use crate::error::{Error, ErrorKind, Result, check};
use crate::io::PosixIo;
use crate::sys;
use crate::types::TypeRef;

/// Append-side counterpart to [`Block`](crate::Block). The lifetime `'a`
/// binds caller-owned column names and slabs that the builder references
/// without copying.
pub struct BlockBuilder<'a> {
    raw: NonNull<sys::chc_block_builder>,
    _alloc: Box<Allocator>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> BlockBuilder<'a> {
    pub fn new(alloc: Allocator) -> Result<Self> {
        let alloc = Box::new(alloc);
        let mut out: *mut sys::chc_block_builder = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_block_builder_init(&mut out, alloc.as_ptr(), &mut err) };
        check(rc, &err)?;
        Ok(Self {
            raw: NonNull::new(out).expect("chc_block_builder_init returned OK with NULL"),
            _alloc: alloc,
            _marker: PhantomData,
        })
    }

    /// Fixed-width column. `data` must be `n_rows * elem_size_of(t)`
    /// little-endian bytes. The slab is borrowed; do not free or mutate
    /// until the builder is dropped.
    pub fn append_fixed(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let elem_size = ty.elem_size();
        if elem_size != 0 {
            require_covers(
                "fixed data",
                data.len(),
                checked_len(n_rows, elem_size, "fixed data")?,
            )?;
        }
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_fixed(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                ty.raw,
                data.as_ptr().cast(),
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// `String` column. `offsets[i]` is the cumulative exclusive end of
    /// row `i` in `data`, host byte order.
    pub fn append_string(
        &mut self,
        name: &'a str,
        offsets: &'a [u64],
        data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        validate_string(offsets, data.len(), n_rows, "string")?;
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_string(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                offsets.as_ptr(),
                data.as_ptr(),
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    pub fn append_nullable_fixed(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        null_map: &'a [u8],
        inner_data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        require_len("nullable null map", null_map.len(), n_rows)?;
        let elem_size = ty.child(0).map(|inner| inner.elem_size()).unwrap_or(0);
        if elem_size != 0 {
            require_covers(
                "nullable fixed data",
                inner_data.len(),
                checked_len(n_rows, elem_size, "nullable fixed data")?,
            )?;
        }
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_nullable_fixed(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                ty.raw,
                null_map.as_ptr(),
                inner_data.as_ptr().cast(),
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    pub fn append_nullable_string(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        null_map: &'a [u8],
        inner_offsets: &'a [u64],
        inner_data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        require_len("nullable null map", null_map.len(), n_rows)?;
        validate_string(inner_offsets, inner_data.len(), n_rows, "nullable string")?;
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_nullable_string(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                ty.raw,
                null_map.as_ptr(),
                inner_offsets.as_ptr(),
                inner_data.as_ptr(),
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    pub fn append_array_fixed(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        offsets: &'a [u64],
        values: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let inner_n = validate_offsets(offsets, n_rows, "array")?;
        let elem_size = ty.child(0).map(|inner| inner.elem_size()).unwrap_or(0);
        if elem_size != 0 {
            require_covers(
                "array fixed values",
                values.len(),
                checked_len(inner_n, elem_size, "array fixed values")?,
            )?;
        }
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_array_fixed(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                ty.raw,
                offsets.as_ptr(),
                values.as_ptr().cast(),
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    pub fn append_array_string(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        offsets: &'a [u64],
        values_offsets: &'a [u64],
        values_data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let inner_n = validate_offsets(offsets, n_rows, "array")?;
        validate_string(
            values_offsets,
            values_data.len(),
            inner_n,
            "array string values",
        )?;
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_array_string(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                ty.raw,
                offsets.as_ptr(),
                values_offsets.as_ptr(),
                values_data.as_ptr(),
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// JSON column, STRING serialization. `ty` must be `Kind::Json`. The
    /// builder emits a one-shot 8-byte LE serialization-version prefix
    /// before the wire bytes; subsequent rows follow the same layout as
    /// [`Self::append_string`]. Caller guarantees each row is valid JSON;
    /// the server rejects malformed documents at INSERT time.
    pub fn append_json_string(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        offsets: &'a [u64],
        data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        validate_string(offsets, data.len(), n_rows, "JSON string")?;
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_json_string(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                ty.raw,
                offsets.as_ptr(),
                data.as_ptr(),
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// LowCardinality(String) or LowCardinality(Nullable(String)). For
    /// the nullable variant the caller must reserve dict slot 0 as the
    /// null sentinel and store key=0 for null rows.
    pub fn append_low_cardinality_string(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        key_size: i32,
        keys: &'a [u8],
        dict_offsets: &'a [u64],
        dict_data: &'a [u8],
        dict_n: usize,
        n_rows: usize,
    ) -> Result<()> {
        validate_low_cardinality_keys(keys, key_size, n_rows, dict_n)?;
        validate_string(
            dict_offsets,
            dict_data.len(),
            dict_n,
            "LowCardinality dictionary",
        )?;
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_low_cardinality_string(
                self.raw.as_ptr(),
                name.as_ptr().cast(),
                name.len(),
                ty.raw,
                key_size,
                keys.as_ptr().cast(),
                dict_offsets.as_ptr(),
                dict_data.as_ptr(),
                dict_n,
                n_rows,
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// Serialize through an `Io`. `opts` matches what [`BlockReader`]
    /// uses; clickhouse-local accepts the default (all-zeros).
    ///
    /// [`BlockReader`]: crate::BlockReader
    pub fn write(&self, io: Pin<&mut PosixIo<'_>>, opts: BlockOpts) -> Result<()> {
        let raw_opts = opts.to_raw();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_write(
                io.io_ptr(),
                self.raw.as_ptr().cast_const(),
                &raw_opts,
                &mut err,
            )
        };
        check(rc, &err)
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const sys::chc_block_builder {
        self.raw.as_ptr().cast_const()
    }
}

fn usage(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Usage, message)
}

fn checked_len(count: usize, width: usize, label: &str) -> Result<usize> {
    count
        .checked_mul(width)
        .ok_or_else(|| usage(format!("{label} length overflow: {count} * {width}")))
}

// Row-count arrays (offsets, null maps, LC keys) restate `n_rows`; a
// mismatch means the caller disagrees with itself, so require exact.
fn require_len(label: &str, actual: usize, expected: usize) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(usage(format!(
            "{label} length mismatch: got {actual}, expected {expected}"
        )))
    }
}

// Raw byte slabs are read as a computed prefix; C never sees the slab
// length, so trailing slack is harmless. Only the too-short case is a bug.
fn require_covers(label: &str, actual: usize, needed: usize) -> Result<()> {
    if actual >= needed {
        Ok(())
    } else {
        Err(usage(format!(
            "{label} too short: got {actual}, need at least {needed}"
        )))
    }
}

fn validate_offsets(offsets: &[u64], n_rows: usize, label: &str) -> Result<usize> {
    require_len(&format!("{label} offsets"), offsets.len(), n_rows)?;
    let mut previous = 0;
    for (row, &end) in offsets.iter().enumerate() {
        if end < previous {
            return Err(usage(format!(
                "{label} offsets not monotonic at row {row}: {end} < {previous}"
            )));
        }
        previous = end;
    }
    usize::try_from(previous).map_err(|_| {
        usage(format!(
            "{label} final offset does not fit usize: {previous}"
        ))
    })
}

fn validate_string(offsets: &[u64], data_len: usize, n_rows: usize, label: &str) -> Result<()> {
    let final_offset = validate_offsets(offsets, n_rows, label)?;
    require_covers(&format!("{label} data"), data_len, final_offset)
}

fn validate_low_cardinality_keys(
    keys: &[u8],
    key_size: i32,
    n_rows: usize,
    dict_n: usize,
) -> Result<()> {
    let key_size = match key_size {
        1 | 2 | 4 | 8 => key_size as usize,
        _ => {
            return Err(usage(format!(
                "LowCardinality key size must be 1, 2, 4, or 8, got {key_size}"
            )));
        }
    };
    require_len(
        "LowCardinality keys",
        keys.len(),
        checked_len(n_rows, key_size, "LowCardinality keys")?,
    )?;
    for (row, key) in keys.chunks_exact(key_size).enumerate() {
        let value = match key_size {
            1 => u64::from(key[0]),
            2 => u64::from(u16::from_ne_bytes(key.try_into().expect("key width"))),
            4 => u64::from(u32::from_ne_bytes(key.try_into().expect("key width"))),
            8 => u64::from_ne_bytes(key.try_into().expect("key width")),
            _ => unreachable!(),
        };
        if value >= dict_n as u64 {
            return Err(usage(format!(
                "LowCardinality key out of range at row {row}: {value} >= {dict_n}"
            )));
        }
    }
    Ok(())
}

impl<'a> Drop for BlockBuilder<'a> {
    fn drop(&mut self) {
        unsafe { sys::chc_block_builder_destroy(self.raw.as_ptr()) };
    }
}

unsafe impl<'a> Send for BlockBuilder<'a> {}
// Sync: a shared `&BlockBuilder` only exposes read-only operations
// (`as_ptr`, `write`); mutation requires `&mut self`. The async client
// holds `&BlockBuilder` across the `send_data` await, so the borrow
// must be `Send`, which needs the builder `Sync`.
unsafe impl<'a> Sync for BlockBuilder<'a> {}
