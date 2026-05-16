//! Owned [`Block`] and borrowed [`Column`] views.
//!
//! A block is returned by [`Block::read`] (for free-standing `chc_io`,
//! e.g. piping `clickhouse local`'s stdout) or by `Packet::take_block`
//! (TCP path).

use core::pin::Pin;
use core::ptr::NonNull;
use core::slice;

use crate::alloc::Allocator;
use crate::error::{Result, check};
use crate::io::PosixIo;
use crate::sys;
use crate::types::TypeRef;

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ColumnLayout {
    Fixed = sys::CHC_COL_FIXED,
    String = sys::CHC_COL_STRING,
    Nullable = sys::CHC_COL_NULLABLE,
    Array = sys::CHC_COL_ARRAY,
    Tuple = sys::CHC_COL_TUPLE,
    LowCardinality = sys::CHC_COL_LOW_CARDINALITY,
    Nothing = sys::CHC_COL_NOTHING,
}

impl ColumnLayout {
    fn from_raw(k: sys::chc_col_kind) -> Option<Self> {
        Some(match k {
            sys::CHC_COL_FIXED => Self::Fixed,
            sys::CHC_COL_STRING => Self::String,
            sys::CHC_COL_NULLABLE => Self::Nullable,
            sys::CHC_COL_ARRAY => Self::Array,
            sys::CHC_COL_TUPLE => Self::Tuple,
            sys::CHC_COL_LOW_CARDINALITY => Self::LowCardinality,
            sys::CHC_COL_NOTHING => Self::Nothing,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Default)]
pub struct BlockOpts {
    /// TCP path (server_revision >= 51903) ships an 8-byte BlockInfo prefix.
    /// `clickhouse local` does not.
    pub has_block_info: bool,
    /// TCP path (server_revision >= 54454) ships a 1-byte
    /// has_custom_serialization flag after each column type. `clickhouse
    /// local` does not.
    pub has_custom_serialization: bool,
    /// Internal read-buffer size. 0 → 8 KiB default.
    pub read_buffer_bytes: usize,
}

impl BlockOpts {
    pub(crate) fn to_raw(self) -> sys::chc_block_opts {
        sys::chc_block_opts {
            has_block_info: self.has_block_info,
            has_custom_serialization: self.has_custom_serialization,
            read_buffer_bytes: self.read_buffer_bytes,
        }
    }
}

/// Owning handle to a decoded `chc_block *`. `Drop` frees through the
/// same allocator used at construction.
pub struct Block {
    raw: NonNull<sys::chc_block>,
    alloc: Allocator,
}

impl Block {
    /// Decode one block from an `Io`. Returns `Ok(None)` on a clean EOF at
    /// a block boundary.
    pub fn read(io: Pin<&mut PosixIo>, alloc: Allocator, opts: BlockOpts) -> Result<Option<Self>> {
        let raw_opts = opts.to_raw();
        let mut out: *mut sys::chc_block = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_read(io.io_ptr(), alloc.as_ptr(), &raw_opts, &mut out, &mut err)
        };
        check(rc, &err)?;
        Ok(NonNull::new(out).map(|raw| Self { raw, alloc }))
    }

    /// Wrap a raw block pointer with an allocator and take ownership.
    ///
    /// # Safety
    /// The caller must own `raw` and must not use it after this call. The
    /// `alloc` must match the one the block was allocated with.
    pub(crate) unsafe fn from_raw(raw: *mut sys::chc_block, alloc: Allocator) -> Option<Self> {
        NonNull::new(raw).map(|raw| Self { raw, alloc })
    }

    pub fn n_rows(&self) -> usize {
        unsafe { sys::chc_block_n_rows(self.raw.as_ptr().cast_const()) }
    }

    pub fn n_columns(&self) -> usize {
        unsafe { sys::chc_block_n_columns(self.raw.as_ptr().cast_const()) }
    }

    pub fn column_name(&self, i: usize) -> Option<&str> {
        let mut len = 0;
        let p = unsafe { sys::chc_block_column_name(self.raw.as_ptr().cast_const(), i, &mut len) };
        if p.is_null() {
            None
        } else {
            unsafe {
                let bytes = slice::from_raw_parts(p.cast::<u8>(), len);
                Some(core::str::from_utf8_unchecked(bytes))
            }
        }
    }

    pub fn column_type(&self, i: usize) -> Option<TypeRef<'_>> {
        let p = unsafe { sys::chc_block_column_type(self.raw.as_ptr().cast_const(), i) };
        if p.is_null() {
            None
        } else {
            Some(TypeRef {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    pub fn column(&self, i: usize) -> Option<Column<'_>> {
        let p = unsafe { sys::chc_block_column(self.raw.as_ptr().cast_const(), i) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    pub fn is_overflows(&self) -> bool {
        unsafe { sys::chc_block_is_overflows(self.raw.as_ptr().cast_const()) }
    }

    pub fn bucket_num(&self) -> i32 {
        unsafe { sys::chc_block_bucket_num(self.raw.as_ptr().cast_const()) }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        unsafe { sys::chc_block_destroy(self.raw.as_ptr(), self.alloc.as_ptr()) };
    }
}

unsafe impl Send for Block {}

/// Borrowed view into one column of a [`Block`].
#[derive(Clone, Copy)]
pub struct Column<'b> {
    pub(crate) raw: *const sys::chc_column,
    pub(crate) _marker: core::marker::PhantomData<&'b sys::chc_column>,
}

impl<'b> Column<'b> {
    pub fn layout(&self) -> Option<ColumnLayout> {
        ColumnLayout::from_raw(unsafe { sys::chc_column_layout(self.raw) })
    }

    pub fn n_rows(&self) -> usize {
        unsafe { sys::chc_column_n_rows(self.raw) }
    }

    /// Returns `(elem_size, bytes)`. Bytes are little-endian on the wire.
    pub fn fixed(&self) -> Option<(usize, &'b [u8])> {
        if !matches!(self.layout(), Some(ColumnLayout::Fixed)) {
            return None;
        }
        let mut elem_size = 0usize;
        let ptr = unsafe { sys::chc_column_fixed_data(self.raw, &mut elem_size) };
        if ptr.is_null() {
            return None;
        }
        let n = self.n_rows() * elem_size;
        let bytes = unsafe { slice::from_raw_parts(ptr.cast::<u8>(), n) };
        Some((elem_size, bytes))
    }

    /// String column: `(offsets, data)`. `offsets[i]` is the cumulative
    /// end of row `i` in `data` (exclusive ends, host byte order).
    pub fn string(&self) -> Option<(&'b [u64], &'b [u8])> {
        if !matches!(self.layout(), Some(ColumnLayout::String)) {
            return None;
        }
        let n = self.n_rows();
        let offsets_ptr = unsafe { sys::chc_column_string_offsets(self.raw) };
        let data_ptr = unsafe { sys::chc_column_string_data(self.raw) };
        if offsets_ptr.is_null() || (data_ptr.is_null() && n > 0) {
            return None;
        }
        let offsets = unsafe { slice::from_raw_parts(offsets_ptr, n) };
        let data_len = offsets.last().copied().unwrap_or(0) as usize;
        let data = if data_len == 0 || data_ptr.is_null() {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(data_ptr, data_len) }
        };
        Some((offsets, data))
    }

    pub fn null_map(&self) -> Option<&'b [u8]> {
        if !matches!(self.layout(), Some(ColumnLayout::Nullable)) {
            return None;
        }
        let p = unsafe { sys::chc_column_null_map(self.raw) };
        if p.is_null() {
            return None;
        }
        Some(unsafe { slice::from_raw_parts(p, self.n_rows()) })
    }

    pub fn nullable_inner(&self) -> Option<Column<'b>> {
        let p = unsafe { sys::chc_column_nullable_inner(self.raw) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    pub fn array_offsets(&self) -> Option<&'b [u64]> {
        if !matches!(self.layout(), Some(ColumnLayout::Array)) {
            return None;
        }
        let p = unsafe { sys::chc_column_array_offsets(self.raw) };
        if p.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(p, self.n_rows()) })
        }
    }

    pub fn array_values(&self) -> Option<Column<'b>> {
        let p = unsafe { sys::chc_column_array_values(self.raw) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    pub fn tuple_arity(&self) -> usize {
        unsafe { sys::chc_column_tuple_arity(self.raw) }
    }

    pub fn tuple_child(&self, i: usize) -> Option<Column<'b>> {
        let p = unsafe { sys::chc_column_tuple_child(self.raw, i) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    /// LowCardinality keys: returns `(key_size_bytes, raw_key_bytes)` and
    /// the dictionary column.
    pub fn low_cardinality(&self) -> Option<LowCardinalityView<'b>> {
        if !matches!(self.layout(), Some(ColumnLayout::LowCardinality)) {
            return None;
        }
        let key_size = unsafe { sys::chc_column_lc_key_size(self.raw) };
        if key_size <= 0 {
            return None;
        }
        let keys_ptr = unsafe { sys::chc_column_lc_keys(self.raw) };
        let dict_ptr = unsafe { sys::chc_column_lc_dict(self.raw) };
        if keys_ptr.is_null() || dict_ptr.is_null() {
            return None;
        }
        let keys_len = self.n_rows() * key_size as usize;
        let keys = unsafe { slice::from_raw_parts(keys_ptr.cast::<u8>(), keys_len) };
        Some(LowCardinalityView {
            key_size: key_size as usize,
            keys,
            dict: Column {
                raw: dict_ptr,
                _marker: core::marker::PhantomData,
            },
        })
    }
}

pub struct LowCardinalityView<'b> {
    pub key_size: usize,
    pub keys: &'b [u8],
    pub dict: Column<'b>,
}
