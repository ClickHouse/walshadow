//! Block writer. clickhouse-c never copies the slabs handed in, nor the
//! `chc_column` nodes: `chc_build_*` returns a node by value whose wrapper
//! arms alias the caller's child nodes (see the header's stack example).
//! A [`ColumnBuilder`] is one such node, owned by the caller; wrappers borrow
//! their child, so the borrow checker pins the child in place until the write
//! without any heap. [`BlockBuilder::append`] records a borrowed root against
//! a name and type. The `'a` lifetime binds names, slabs and child nodes so
//! all outlive the write.

use core::ffi::c_int;
use core::marker::PhantomData;
use core::pin::Pin;

use crate::block::BlockOpts;
use crate::error::{Error, ErrorKind, Result, check};
use crate::io::PosixIo;
use crate::sys;
use crate::types::TypeRef;

/// One `chc_column` node over caller-owned slabs, mirroring the `chc_build_*`
/// constructors. Compose leaves ([`ColumnBuilder::fixed`],
/// [`ColumnBuilder::string`]) with wrappers ([`ColumnBuilder::nullable`],
/// [`ColumnBuilder::array`], [`ColumnBuilder::low_cardinality`], [`ColumnBuilder::tuple`]) to
/// match any composite the reader emits, e.g. `Array(Nullable(UInt32))`.
/// Every node is a caller local; a wrapper borrows its child, so the child
/// cannot move (its node address stays valid) while the wrapper aliases it:
///
/// ```ignore
/// let leaf = ColumnBuilder::fixed(values, 4, 3)?;
/// let nul = leaf.nullable(null_map)?;   // borrows leaf
/// let arr = nul.array(offsets, 2)?;     // borrows nul
/// bb.append("v", ty.view(), &arr)?;     // borrows arr until the write
/// ```
///
/// Runtime-shaped trees keep their nodes in caller-owned storage (a `Vec`
/// or per-depth arena the consumer manages); the kernel itself never
/// allocates. The `'a` lifetime binds the borrowed slabs and child nodes.
pub struct ColumnBuilder<'a> {
    // `chc_build_*` output; wrapper arms alias caller child nodes borrowed for
    // `'a`, so the node is only valid while those children stay put.
    node: sys::chc_column,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ColumnBuilder<'a> {
    /// Fixed-width leaf. `data` must hold at least `n_rows * elem_size`
    /// little-endian bytes; trailing slack is never read.
    pub fn fixed(data: &'a [u8], elem_size: usize, n_rows: usize) -> Result<Self> {
        if elem_size == 0 {
            return Err(usage("fixed column: elem_size must be nonzero"));
        }
        require_covers(
            "fixed data",
            data.len(),
            checked_len(n_rows, elem_size, "fixed data")?,
        )?;
        Ok(node(unsafe {
            sys::chc_build_fixed(data.as_ptr().cast(), elem_size, n_rows)
        }))
    }

    /// `String` leaf. `offsets[i]` is the cumulative exclusive end of row
    /// `i` in `data`, host byte order. Also used for `JSON` / `Object` and
    /// LowCardinality dictionaries, whose bodies share the string wire
    /// shape.
    pub fn string(offsets: &'a [u64], data: &'a [u8], n_rows: usize) -> Result<Self> {
        validate_string(offsets, data.len(), n_rows, "string")?;
        Ok(node(unsafe {
            sys::chc_build_string(offsets.as_ptr(), data.as_ptr(), n_rows)
        }))
    }

    /// Wrap `self` in a `Nullable`. `null_map[i] == 1` marks row `i` NULL;
    /// its length must equal the inner row count. Rows carry a defined
    /// inner value even when NULL, so the inner slab stays dense. The result
    /// borrows `self` so its node stays put until the write.
    pub fn nullable<'r>(&'r self, null_map: &'r [u8]) -> Result<ColumnBuilder<'r>> {
        require_len("nullable null map", null_map.len(), self.n_rows())?;
        let inner = self.node_ptr().cast_mut();
        Ok(node(unsafe {
            sys::chc_build_nullable(null_map.as_ptr(), inner)
        }))
    }

    /// Wrap `self` (the element values) in an `Array` of `n_rows` rows.
    /// `offsets[i]` is the cumulative exclusive end of row `i`; the final
    /// offset must equal the element row count of `self`.
    pub fn array<'r>(&'r self, offsets: &'r [u64], n_rows: usize) -> Result<ColumnBuilder<'r>> {
        let inner_n = validate_offsets(offsets, n_rows, "array")?;
        require_len("array values", self.n_rows(), inner_n)?;
        let values = self.node_ptr().cast_mut();
        Ok(node(unsafe {
            sys::chc_build_array(offsets.as_ptr(), n_rows, values)
        }))
    }

    /// Wrap `self` (the dictionary) in a `LowCardinality` of `n_rows` rows.
    /// `key_size` is 1/2/4/8 bytes; each key indexes the dictionary. For
    /// `LowCardinality(Nullable(T))` reserve dict slot 0 as the null
    /// sentinel and store key 0 for null rows.
    pub fn low_cardinality<'r>(
        &'r self,
        key_size: i32,
        keys: &'r [u8],
        n_rows: usize,
    ) -> Result<ColumnBuilder<'r>> {
        validate_low_cardinality_keys(keys, key_size, n_rows, self.n_rows())?;
        let dict = self.node_ptr().cast_mut();
        Ok(node(unsafe {
            sys::chc_build_lc(key_size as c_int, keys.as_ptr().cast(), n_rows, dict)
        }))
    }

    /// `Tuple` over `children`, all sharing one row count. Also the basis
    /// for `Map` (`Array(Tuple(K, V))`) and geo types. `chc_build_tuple`
    /// aliases a `*mut chc_column` array, so the caller passes `ptrs` as
    /// scratch (length must equal `children`); both it and `children` stay
    /// borrowed until the write. A fixed-arity tuple can stack-allocate
    /// `ptrs` (`[ptr::null_mut(); N]`); a runtime-arity one owns a `Vec`.
    pub fn tuple<'r>(
        children: &'r [ColumnBuilder<'a>],
        ptrs: &'r mut [*mut sys::chc_column],
    ) -> Result<ColumnBuilder<'r>> {
        let Some(first) = children.first() else {
            return Err(usage("tuple column: needs at least one child"));
        };
        if ptrs.len() != children.len() {
            return Err(usage(format!(
                "tuple ptr scratch length mismatch: {} vs {} children",
                ptrs.len(),
                children.len()
            )));
        }
        let n_rows = first.n_rows();
        for (i, child) in children.iter().enumerate() {
            if child.n_rows() != n_rows {
                return Err(usage(format!(
                    "tuple child {i} row count mismatch: {} vs {n_rows}",
                    child.n_rows()
                )));
            }
            ptrs[i] = child.node_ptr().cast_mut();
        }
        Ok(node(unsafe {
            sys::chc_build_tuple(ptrs.as_mut_ptr(), children.len())
        }))
    }

    /// Row count at this node's level (`chc_column_n_rows`).
    pub fn n_rows(&self) -> usize {
        self.node.n_rows
    }

    fn node_ptr(&self) -> *const sys::chc_column {
        &self.node
    }
}

// Wrap a `chc_build_*` node at whatever lifetime the caller's borrows imply.
fn node<'x>(node: sys::chc_column) -> ColumnBuilder<'x> {
    ColumnBuilder {
        node,
        _marker: PhantomData,
    }
}

/// Append-side counterpart to [`Block`](crate::Block). The lifetime `'a`
/// binds caller-owned column names and the [`ColumnBuilder`] nodes (with the
/// slabs they reference), all borrowed without copying until the write.
pub struct BlockBuilder<'a> {
    // Storage the `chc_block_builder` points at; kept in sync with `raw`.
    // Each `col` aliases a caller node borrowed for `'a`.
    cols: Vec<sys::chc_block_col>,
    raw: sys::chc_block_builder,
    n_rows: Option<usize>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> Default for BlockBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> BlockBuilder<'a> {
    pub fn new() -> Self {
        Self {
            cols: Vec::new(),
            raw: sys::chc_block_builder::zeroed(),
            n_rows: None,
            _marker: PhantomData,
        }
    }

    /// Append a [`ColumnBuilder`] under `name` with full CH type `ty`. The
    /// column must match `ty` structurally (checked by the C writer) and
    /// share the block's row count. The node is borrowed for `'a`, so it (and
    /// the slabs it references) must outlive the write.
    pub fn append(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        col: &'a ColumnBuilder<'a>,
    ) -> Result<()> {
        let n_rows = col.n_rows();
        match self.n_rows {
            Some(prev) if prev != n_rows => {
                return Err(usage(format!(
                    "block_builder: row count mismatch ({prev} vs {n_rows})"
                )));
            }
            _ => self.n_rows = Some(n_rows),
        }
        let col_ptr = col.node_ptr();
        self.cols.push(sys::chc_block_col {
            name: name.as_ptr().cast(),
            name_len: name.len(),
            type_: ty.raw,
            col: col_ptr,
        });
        // `cols` may have just reallocated; re-point `raw` at its buffer.
        self.raw.cols = self.cols.as_mut_ptr();
        self.raw.n_cols = self.cols.len();
        self.raw.n_rows = n_rows;
        Ok(())
    }

    /// Serialize through an `Io`. `opts` matches what [`BlockReader`]
    /// uses; clickhouse-local accepts the default (all-zeros).
    ///
    /// [`BlockReader`]: crate::BlockReader
    pub fn write(&self, io: Pin<&mut PosixIo<'_>>, opts: BlockOpts) -> Result<()> {
        let raw_opts = opts.to_raw();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_block_write(io.io_ptr(), &self.raw, &raw_opts, &mut err) };
        check(rc, &err)
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const sys::chc_block_builder {
        &self.raw
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

// A node is a read-only descriptor over borrowed slabs and child nodes with
// no interior mutability; the raw pointers it carries only alias caller data.
// Callers own nodes as locals now, held across the async send await, so they
// must cross threads on the same terms as `BlockBuilder`.
unsafe impl Send for ColumnBuilder<'_> {}
unsafe impl Sync for ColumnBuilder<'_> {}

unsafe impl Send for BlockBuilder<'_> {}
// Sync: a shared `&BlockBuilder` only exposes read-only operations
// (`as_ptr`, `write`); mutation requires `&mut self`. The async client
// holds `&BlockBuilder` across the `send_data` await, so the borrow
// must be `Send`, which needs the builder `Sync`.
unsafe impl Sync for BlockBuilder<'_> {}
