//! Block writer. The C library never copies the slabs handed in via
//! `chc_block_builder_append_*`; this wrapper holds the lifetime
//! invariant through `BlockBuilder<'a>`.

use core::ffi::c_char;
use core::marker::PhantomData;
use core::pin::Pin;
use core::ptr::NonNull;

use crate::alloc::Allocator;
use crate::block::BlockOpts;
use crate::error::{Result, check};
use crate::io::PosixIo;
use crate::sys;
use crate::types::TypeRef;

/// Append-side counterpart to [`Block`](crate::Block). The lifetime `'a`
/// binds the caller-owned column slabs that the builder references
/// without copying.
pub struct BlockBuilder<'a> {
    raw: NonNull<sys::chc_block_builder>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> BlockBuilder<'a> {
    pub fn new(alloc: Allocator) -> Result<Self> {
        let mut out: *mut sys::chc_block_builder = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_block_builder_init(&mut out, alloc.as_ptr(), &mut err) };
        check(rc, &err)?;
        Ok(Self {
            raw: NonNull::new(out).expect("chc_block_builder_init returned OK with NULL"),
            _marker: PhantomData,
        })
    }

    /// Fixed-width column. `data` must be `n_rows * elem_size_of(t)`
    /// little-endian bytes. The slab is borrowed; do not free or mutate
    /// until the builder is dropped or [`Self::write`] is called.
    pub fn append_fixed(
        &mut self,
        name: &str,
        ty: TypeRef<'a>,
        data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_fixed(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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
        name: &str,
        offsets: &'a [u64],
        data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_string(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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
        name: &str,
        ty: TypeRef<'a>,
        null_map: &'a [u8],
        inner_data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_nullable_fixed(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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
        name: &str,
        ty: TypeRef<'a>,
        null_map: &'a [u8],
        inner_offsets: &'a [u64],
        inner_data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_nullable_string(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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
        name: &str,
        ty: TypeRef<'a>,
        offsets: &'a [u64],
        values: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_array_fixed(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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
        name: &str,
        ty: TypeRef<'a>,
        offsets: &'a [u64],
        values_offsets: &'a [u64],
        values_data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_array_string(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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
        name: &str,
        ty: TypeRef<'a>,
        offsets: &'a [u64],
        data: &'a [u8],
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_json_string(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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
        name: &str,
        ty: TypeRef<'a>,
        key_size: i32,
        keys: &'a [u8],
        dict_offsets: &'a [u64],
        dict_data: &'a [u8],
        dict_n: usize,
        n_rows: usize,
    ) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_builder_append_low_cardinality_string(
                self.raw.as_ptr(),
                name.as_ptr().cast::<c_char>(),
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

    /// Serialize through an `Io`. `opts` matches what [`Block::read`]
    /// uses; clickhouse-local accepts the default (all-zeros).
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

impl<'a> Drop for BlockBuilder<'a> {
    fn drop(&mut self) {
        unsafe { sys::chc_block_builder_destroy(self.raw.as_ptr()) };
    }
}

unsafe impl<'a> Send for BlockBuilder<'a> {}
