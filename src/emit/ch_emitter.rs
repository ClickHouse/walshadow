//! CH-native emitter primitives via `clickhouse-c-rs`. Batching, seal
//! triggers, and xact close live in [`crate::emit::pipeline`], not here.
//!
//! Synthetic columns `_lsn UInt64`, `_xid UInt32`, `_commit_ts
//! DateTime64(6, 'UTC')`, `_is_deleted Bool` append after every mapped
//! column; [`SystemColumns`] renames them and can drop the delete marker.
//! `_is_deleted` (1 on delete) wires `ReplacingMergeTree`'s deletion arg
//! unless `EmitterConfig::soft_delete` keeps it queryable.
//! PG `TimestampTz` epoch is 2000-01-01; shift to Unix epoch
//! (`DATETIME64_PG_EPOCH_US`) to match CH `DateTime64(6)`.
//!
//! ## Compression
//!
//! Feature-gated via walshadow's `lz4` / `zstd` Cargo features, which
//! forward to clickhouse-c-rs (see top-level `Cargo.toml`). Default
//! builds advertise LZ4 to match the CH server default.
//!
//! ## Cross-table ordering inside an xact
//!
//! `BoxedAsyncClient` is single-query-at-a-time, so an xact touching T1 and
//! T2 lands all T1 rows (one INSERT) then all T2 (next INSERT); WAL
//! interleaving across tables is not preserved. `_lsn` carries the
//! source LSN so `ReplacingMergeTree` dedup keys on the right value;
//! WAL ordering within a single dest table is preserved

use std::sync::Arc;

use clickhouse_c::{Allocator, Kind, TypeAst};

#[cfg(test)]
use crate::ch::is_retryable;
use crate::ch::{EmitterError, quote_ident, types};
use crate::column_rules::ColumnRules;
use crate::decode::datum::{Datum, DatumRef};
#[cfg(test)]
use crate::decode::decoder_sink::DecoderSinkError;
use crate::decode::heap_decoder::missing_datum;
#[cfg(any(test, feature = "test-support"))]
use crate::decode::heap_decoder::{CommittedTuple, HeapOp};
#[cfg(test)]
use crate::emit::column::Wire;
use crate::emit::column::{
    Codec, ColumnBuf, ColumnEncoding, ColumnPlan, DATETIME64_PG_EPOCH_US, NonFinite, SourceColumn,
    canonical_type, decimal_wire_of, encode_datum, oracle_cell, override_wire, push_fixed,
};
use crate::mapping::{SystemColumns, TableMapping, TableTarget};
use crate::ops::bridge::MAX_REQUEST_BYTES;
use crate::ops::oracle::{
    ORACLE_BATCH_SEAL_BYTES, OracleCell, OracleColumnBuf, REQUEST_FRAME_BYTES, request_column_bytes,
};
use crate::schema::{RelDescriptor, RelName};
use smallvec::SmallVec;

/// Heap op codes for [`TableEncoder::append_row`]; `OP_DELETE` sets the
/// delete marker
pub(crate) const OP_INSERT: i8 = 1;
pub(crate) const OP_UPDATE: i8 = 2;
pub(crate) const OP_DELETE: i8 = 3;

/// Cached plan for one destination table, built lazily on first row.
pub(crate) struct TablePlan {
    pub(crate) columns: Vec<ColumnPlan>,
    pub(crate) synth_lsn: ColumnPlan,
    pub(crate) synth_xid: ColumnPlan,
    pub(crate) synth_commit_ts: ColumnPlan,
    /// Delete marker `Bool` (1 on delete, else 0). `None` when
    /// `[system_columns] is_deleted = false` drops it
    pub(crate) synth_is_deleted: Option<ColumnPlan>,
    /// Source partition `UInt32`, appended last on partitioned-root
    /// destinations
    pub(crate) synth_leaf: Option<ColumnPlan>,
    /// Format once to avoid rebuilding SQL for each row. Main destination
    /// first, then each tee
    pub(crate) inserts: Vec<InsertQuery>,
}

#[derive(Debug, Clone)]
pub struct InsertQuery {
    pub instance: Option<String>,
    pub sql: String,
}

impl TablePlan {
    /// Synthetic columns are always non-nullable
    pub(crate) fn build(
        alloc: Allocator,
        rel: &RelDescriptor,
        mapping: &TableMapping,
        column_rules: &ColumnRules,
        system: &SystemColumns,
    ) -> Result<Self, EmitterError> {
        Self::build_owned(
            alloc,
            rel,
            &rel.rel_name,
            mapping,
            column_rules,
            system,
            None,
        )
    }

    /// Plan for rows routed by `route`: a partition's rows encode under
    /// its root's column rules and stamp the partition column
    pub(crate) fn for_route(
        alloc: Allocator,
        rel: &RelDescriptor,
        route: &crate::emit::route::RouteSnapshot,
    ) -> Result<Self, EmitterError> {
        let system = route.system_columns();
        Self::build_owned(
            alloc,
            rel,
            route.owner(&rel.rel_name),
            &route.mapping,
            &route.column_rules,
            system,
            route.partition.as_ref().and(system.leaf.as_deref()),
        )
    }

    fn build_owned(
        alloc: Allocator,
        rel: &RelDescriptor,
        owner: &RelName,
        mapping: &TableMapping,
        column_rules: &ColumnRules,
        system: &SystemColumns,
        leaf: Option<&str>,
    ) -> Result<Self, EmitterError> {
        let mut columns = Vec::with_capacity(mapping.columns.len());
        let mut col_sql = Vec::with_capacity(mapping.columns.len() + 4);
        // Mapping attnums absent from the catalog descriptor are not a
        // hard error: schema-evolution pre-declares post-ALTER columns,
        // pre-ALTER xacts legitimately see fewer attnums. append_row
        // emits NULL for any missing attnum, so a static-config typo
        // surfaces as an always-NULL column (or CH reject if non-nullable)
        for c in &mapping.columns {
            let att = rel
                .attributes
                .iter()
                .find(|a| a.attnum == c.src_attnum && !a.dropped);
            let ast = TypeAst::parse(&c.target_type, alloc)
                .map_err(|e| EmitterError::Type(format!("{}: {e}", c.target_type)))?;
            let decimal = decimal_wire_of(&ast);
            let mut plan = ColumnPlan {
                name: c.target_name.clone(),
                type_repr: canonical_type(&ast, &c.target_type),
                encoding: ColumnEncoding::choose(att, &ast),
                datetime64_scale: types::strip_nullable(ast.view())
                    .datetime64_scale()
                    .unwrap_or(0),
                ast,
                decimal,
                non_finite: NonFinite::default(),
                source: None,
            };
            let settings = att
                .map(|a| column_rules.settings(owner, &a.name))
                .unwrap_or_default();
            if let Some(ty) = &settings.target_type {
                match TypeAst::parse(ty, alloc) {
                    Ok(oast) => {
                        if let Some(decimal) = override_wire(&plan.ast, &oast) {
                            plan = ColumnPlan {
                                name: plan.name,
                                type_repr: canonical_type(&oast, ty),
                                encoding: ColumnEncoding::choose(att, &oast),
                                datetime64_scale: types::strip_nullable(oast.view())
                                    .datetime64_scale()
                                    .unwrap_or(0),
                                ast: oast,
                                decimal,
                                non_finite: NonFinite::default(),
                                source: None,
                            };
                        } else {
                            tracing::warn!(
                                target: "walshadow::emitter",
                                qname = %rel.rel_name,
                                column = %c.target_name,
                                default = %c.target_type,
                                value = %ty,
                                "config_column.target_type not wire-compatible; keeping default",
                            );
                        }
                    }
                    Err(e) => tracing::warn!(
                        target: "walshadow::emitter",
                        qname = %rel.rel_name,
                        column = %c.target_name,
                        value = %ty,
                        error = %e,
                        "config_column.target_type unparseable; keeping default",
                    ),
                }
            }
            let config_err =
                |e: &str| EmitterError::Config(format!("{}.{}: {e}", rel.rel_name, c.target_name));
            let source_temporal = att.and_then(|a| match a.type_oid {
                crate::schema::DATEOID => Some(Kind::Date32),
                crate::schema::TIMESTAMPOID | crate::schema::TIMESTAMPTZOID => {
                    Some(Kind::DateTime64)
                }
                _ => None,
            });
            if plan.encoding == ColumnEncoding::Local
                && source_temporal.is_some()
                && types::strip_nullable(plan.ast.view()).kind() != source_temporal
            {
                return Err(config_err(
                    "local temporal encoding needs Date32 or DateTime64 matching source type",
                ));
            }
            let local = plan.encoding == ColumnEncoding::Local;
            plan.non_finite =
                NonFinite::compile(&settings, &plan.ast, local).map_err(|e| config_err(&e))?;
            // Oracle cells never pass through substitution
            if !local && plan.non_finite != NonFinite::default() {
                return Err(config_err("substitution needs local encoding"));
            }
            // Attnums past the descriptor still source patched values
            plan.source = usize::try_from(c.src_attnum - 1)
                .ok()
                .and_then(|index| match att {
                    Some(a) => Some(SourceColumn {
                        index,
                        codec: Codec::choose(a),
                        type_oid: a.type_oid,
                    }),
                    None => (index >= rel.attributes.len()).then_some(SourceColumn {
                        index,
                        codec: Codec::Le,
                        type_oid: 0,
                    }),
                });
            columns.push(plan);
            col_sql.push(quote_ident(&c.target_name));
        }
        let mk = |name: &str, ty: &str| ColumnPlan {
            name: name.into(),
            type_repr: ty.into(),
            ast: TypeAst::parse(ty, alloc).expect("synthetic column type parses"),
            decimal: None,
            datetime64_scale: 0,
            non_finite: NonFinite::default(),
            encoding: ColumnEncoding::Local,
            source: None,
        };
        let synth_lsn = mk(&system.lsn, "UInt64");
        let synth_xid = mk(&system.xid, "UInt32");
        let synth_commit_ts = mk(&system.commit_ts, "DateTime64(6, 'UTC')");
        let synth_is_deleted = system.is_deleted.as_deref().map(|name| mk(name, "Bool"));
        let synth_leaf = leaf.map(|name| mk(name, "UInt32"));
        col_sql.push(quote_ident(&synth_lsn.name));
        col_sql.push(quote_ident(&synth_xid.name));
        col_sql.push(quote_ident(&synth_commit_ts.name));
        col_sql.extend(synth_is_deleted.iter().map(|c| quote_ident(&c.name)));
        col_sql.extend(synth_leaf.iter().map(|c| quote_ident(&c.name)));
        let col_sql = col_sql.join(", ");
        let insert = |t: &TableTarget| format!("INSERT INTO {} ({col_sql}) FORMAT Native", t.sql());
        Ok(Self {
            columns,
            synth_lsn,
            synth_xid,
            synth_commit_ts,
            synth_is_deleted,
            synth_leaf,
            inserts: mapping
                .targets()
                .map(|t| InsertQuery {
                    sql: insert(&t),
                    instance: t.instance,
                })
                .collect(),
        })
    }

    pub(crate) fn needs_oracle(&self) -> bool {
        self.columns
            .iter()
            .any(|c| matches!(c.encoding, ColumnEncoding::Oracle { .. }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Append {
    Done,
    /// Row not appended, seal and retry it
    Full,
}

/// One row's mapped datums, inline up to wide tables so encoding a row
/// does not allocate
type RowCells<'a> = SmallVec<[DatumRef<'a>; 64]>;

/// Per-table per-xact accumulator, one block buffer per CH column.
pub(crate) struct TableEncoder {
    pub(crate) plan: TablePlan,
    pub(crate) rows: usize,
    pub(crate) approx_bytes: usize,
    /// Pending oracle frame size
    pub(crate) oracle_bytes: usize,
    oracle_overhead: usize,
    oracle_cells: Vec<OracleCell>,
    /// Mirrors `plan.columns` plus the synthetic columns the plan carries
    pub(crate) buffers: Vec<ColumnBuf>,
    /// Rendered text of the cell being encoded
    text: String,
}

/// Fresh per-column buffers matching `plan` (mapped + synthetic)
fn fresh_buffers(plan: &TablePlan) -> Result<Vec<ColumnBuf>, EmitterError> {
    let mut buffers = Vec::with_capacity(plan.columns.len() + 4);
    for c in &plan.columns {
        buffers.push(match c.encoding {
            ColumnEncoding::Local => ColumnBuf::new_for_ast(&c.ast)?,
            ColumnEncoding::Oracle {
                source_type_oid,
                source_typmod,
            } => ColumnBuf::Oracle(OracleColumnBuf::new(
                source_type_oid,
                source_typmod,
                c.ast.view(),
            )),
        });
    }
    buffers.push(ColumnBuf::Fixed {
        width: 8,
        bytes: Vec::new(),
    }); // lsn UInt64
    buffers.push(ColumnBuf::Fixed {
        width: 4,
        bytes: Vec::new(),
    }); // xid UInt32
    buffers.push(ColumnBuf::Fixed {
        width: 8,
        bytes: Vec::new(),
    }); // commit_ts DateTime64(6)
    if plan.synth_is_deleted.is_some() {
        buffers.push(ColumnBuf::Fixed {
            width: 1,
            bytes: Vec::new(),
        }); // delete marker Bool (1 wire byte, same as UInt8)
    }
    if plan.synth_leaf.is_some() {
        buffers.push(ColumnBuf::Fixed {
            width: 4,
            bytes: Vec::new(),
        }); // partition UInt32
    }
    Ok(buffers)
}

impl TableEncoder {
    pub(crate) fn new(plan: TablePlan) -> Result<Self, EmitterError> {
        let buffers = fresh_buffers(&plan)?;
        let oracle_overhead = REQUEST_FRAME_BYTES
            + plan
                .columns
                .iter()
                .filter(|c| matches!(c.encoding, ColumnEncoding::Oracle { .. }))
                .map(|c| request_column_bytes(&c.name, &c.type_repr))
                .sum::<usize>();
        Ok(Self {
            plan,
            rows: 0,
            approx_bytes: 0,
            oracle_bytes: oracle_overhead,
            oracle_overhead,
            oracle_cells: Vec::new(),
            buffers,
            text: String::new(),
        })
    }

    /// Swap accumulated slabs out for empties of the same shape, returning
    /// old slabs + row count. Transfers ownership rather than reusing
    /// allocations
    pub(crate) fn take_block(&mut self) -> (Vec<ColumnBuf>, usize) {
        let fresh = self.buffers.iter().map(ColumnBuf::empty_like).collect();
        let old = std::mem::replace(&mut self.buffers, fresh);
        let rows = self.rows;
        self.rows = 0;
        self.approx_bytes = 0;
        self.oracle_bytes = self.oracle_overhead;
        (old, rows)
    }

    /// Whether `block` lays out the same columns as these buffers
    pub(crate) fn accepts(&self, block: &[ColumnBuf]) -> bool {
        block.len() == self.buffers.len()
            && self.buffers.iter().zip(block).all(|(a, b)| a.same_shape(b))
    }

    /// Append rows another encoder of this shape sealed. Leaves `block`
    /// untouched on [`Append::Full`], empties it otherwise
    pub(crate) fn append_block(
        &mut self,
        block: &mut Vec<ColumnBuf>,
        rows: usize,
    ) -> Result<Append, EmitterError> {
        if !self.accepts(block) {
            return Err(EmitterError::Type(
                "encoded chunk layout differs from its batch".into(),
            ));
        }
        let oracle_bytes = self.oracle_overhead
            + self
                .buffers
                .iter()
                .zip(block.iter())
                .map(|(a, b)| match (a, b) {
                    (ColumnBuf::Oracle(a), ColumnBuf::Oracle(b)) => a.request_bytes_with(b),
                    _ => 0,
                })
                .sum::<usize>();
        if self.rows > 0 && oracle_bytes > ORACLE_BATCH_SEAL_BYTES {
            return Ok(Append::Full);
        }
        if self.rows == 0 {
            std::mem::swap(&mut self.buffers, block);
        } else {
            for (buf, more) in self.buffers.iter_mut().zip(block.drain(..)) {
                buf.extend(more);
            }
        }
        self.rows += rows;
        self.oracle_bytes = oracle_bytes;
        self.approx_bytes = self.buffers.iter().map(ColumnBuf::approx_size).sum();
        Ok(Append::Done)
    }

    #[cfg(test)]
    pub fn append_row(
        &mut self,
        committed: &CommittedTuple,
        mapping: &TableMapping,
        op_code: i8,
    ) -> Result<Append, EmitterError> {
        self.append_routed(committed, mapping, op_code, None)
    }

    #[cfg(any(test, feature = "test-support"))]
    /// Append one owned row, stamping `leaf` into the partition column.
    /// Return [`Append::Full`] without mutation when oracle threshold is hit
    pub fn append_routed(
        &mut self,
        committed: &CommittedTuple,
        mapping: &TableMapping,
        op_code: i8,
        leaf: Option<u32>,
    ) -> Result<Append, EmitterError> {
        let decoded = &committed.decoded;
        let side = match decoded.op {
            HeapOp::Delete => decoded.old.as_ref(),
            _ => decoded.new.as_ref(),
        };
        let cells: RowCells<'_> = mapping
            .columns
            .iter()
            .map(|col| {
                side.and_then(|t| t.columns.get((col.src_attnum - 1) as usize))
                    .and_then(Option::as_ref)
                    .map_or(DatumRef::Absent, Datum::view)
            })
            .collect();
        let stamp = RowStamp {
            source_lsn: decoded.source_lsn,
            xid: decoded.xid,
            commit_ts: committed.commit_ts,
            op_code,
            leaf,
        };
        self.append_cells(|i| cells[i], &stamp)
    }

    /// Append one image of `rel` read in place. Return [`Append::Full`]
    /// without mutation when oracle threshold is hit. `patch` replaces
    /// attributes by index: resolved TOAST values, and values past `rel`'s
    /// attributes
    pub(crate) fn append_view(
        &mut self,
        rel: &RelDescriptor,
        datums: &[DatumRef<'_>],
        patch: &[(usize, Datum)],
        stamp: &RowStamp,
    ) -> Result<Append, EmitterError> {
        let cells: RowCells<'_> = self
            .plan
            .columns
            .iter()
            .map(|c| {
                let Some(src) = c.source else {
                    return DatumRef::Absent;
                };
                if let Some((_, v)) = patch.iter().find(|(i, _)| *i == src.index) {
                    return v.view();
                }
                match (datums.get(src.index), rel.attributes.get(src.index)) {
                    (Some(DatumRef::Missing), Some(att)) => missing_datum(att),
                    (Some(&datum), Some(_)) => datum,
                    _ => DatumRef::Absent,
                }
            })
            .collect();
        self.append_cells(|i| cells[i], stamp)
    }

    /// Append one row whose mapped sources `slot` yields; columns without a
    /// source attribute stay absent
    pub(crate) fn append_slots<'a>(
        &mut self,
        mut slot: impl FnMut(SourceColumn) -> Result<DatumRef<'a>, EmitterError>,
        stamp: &RowStamp,
    ) -> Result<Append, EmitterError> {
        let mut cells = RowCells::new();
        for c in &self.plan.columns {
            cells.push(
                c.source
                    .map(&mut slot)
                    .transpose()?
                    .unwrap_or(DatumRef::Absent),
            );
        }
        self.append_cells(|i| cells[i], stamp)
    }

    /// Append one row, `cell(i)` yielding `plan.columns[i]`'s datum
    fn append_cells<'c>(
        &mut self,
        cell: impl Fn(usize) -> DatumRef<'c>,
        stamp: &RowStamp,
    ) -> Result<Append, EmitterError> {
        let columns = self.plan.columns.len();
        // Size oracle cells before mutating column buffers
        let mut oracle = std::mem::take(&mut self.oracle_cells);
        oracle.clear();
        let mut row_oracle_bytes = 0usize;
        // Literals bound for a String target never reach the request, so they
        // owe it nothing until a cell needing conversion strands them
        let mut stranded_bytes = 0usize;
        for i in 0..columns {
            if let ColumnBuf::Oracle(o) = &self.buffers[i] {
                let col_plan = &self.plan.columns[i];
                let cell = oracle_cell(cell(i), col_plan.source, o.source_type_oid)
                    .map_err(|e| name_column(e, &col_plan.name))?;
                if !o.resolves_locally(&cell) {
                    row_oracle_bytes += cell.wire_bytes();
                    stranded_bytes += o.stranded_bytes();
                }
                oracle.push(cell);
            }
        }
        if self.rows > 0
            && self.oracle_bytes + row_oracle_bytes + stranded_bytes > ORACLE_BATCH_SEAL_BYTES
        {
            oracle.clear();
            self.oracle_cells = oracle;
            return Ok(Append::Full);
        }
        // Enforce hard frame cap even for first row
        if self.oracle_overhead + row_oracle_bytes > MAX_REQUEST_BYTES {
            return Err(EmitterError::Type(format!(
                "row needs {row_oracle_bytes} oracle bytes, one request carries {MAX_REQUEST_BYTES}"
            )));
        }
        let mut oracle_iter = oracle.iter_mut();
        for i in 0..columns {
            let col_plan = &self.plan.columns[i];
            let buf = &mut self.buffers[i];
            if let ColumnBuf::Oracle(o) = buf {
                let cell = oracle_iter.next().expect("one cell per oracle column");
                o.push(std::mem::replace(cell, OracleCell::Default));
                continue;
            }
            encode_datum(buf, col_plan, cell(i), &mut self.text)
                .map_err(|e| name_column(e, &col_plan.name))?;
        }
        oracle.clear();
        self.oracle_cells = oracle;
        self.oracle_bytes += row_oracle_bytes + stranded_bytes;
        // Synthetic columns: lsn, xid, commit_ts (unix micros), delete marker
        let off = columns;
        push_fixed(&mut self.buffers[off], &stamp.source_lsn.to_le_bytes())?;
        push_fixed(&mut self.buffers[off + 1], &stamp.xid.to_le_bytes())?;
        let unix_us = stamp.commit_ts.saturating_add(DATETIME64_PG_EPOCH_US);
        push_fixed(&mut self.buffers[off + 2], &unix_us.to_le_bytes())?;
        let mut next = off + 3;
        if self.plan.synth_is_deleted.is_some() {
            let is_deleted: u8 = (stamp.op_code == OP_DELETE).into();
            push_fixed(&mut self.buffers[next], &is_deleted.to_le_bytes())?;
            next += 1;
        }
        if self.plan.synth_leaf.is_some() {
            push_fixed(
                &mut self.buffers[next],
                &stamp.leaf.unwrap_or(0).to_le_bytes(),
            )?;
        }
        self.rows += 1;
        self.approx_bytes = self.buffers.iter().map(ColumnBuf::approx_size).sum();
        Ok(Append::Done)
    }
}

/// Rows of one plan table encoded at planning. Relation and route travel
/// once per chunk so the batcher keys and plans its destination
pub struct EncodedChunk {
    pub(crate) rel: Arc<RelDescriptor>,
    pub(crate) route: Arc<crate::emit::route::RouteSnapshot>,
    pub(crate) buffers: Vec<ColumnBuf>,
    pub(crate) rows: usize,
}

impl EncodedChunk {
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Slab capacity plus chunk header and buffer slots, which dominate
    /// for small rows spread across many tables
    pub fn allocated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.buffers.capacity() * std::mem::size_of::<ColumnBuf>()
            + self
                .buffers
                .iter()
                .map(ColumnBuf::allocated_bytes)
                .sum::<usize>()
    }

    /// Wire content as a plan spool frame body
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        crate::xact::spill::push_u32(out, self.rows as u32);
        crate::xact::spill::push_u32(out, self.buffers.len() as u32);
        for b in &self.buffers {
            b.encode_into(out);
        }
    }

    pub(crate) fn decode(
        rel: Arc<RelDescriptor>,
        route: Arc<crate::emit::route::RouteSnapshot>,
        c: &mut crate::xact::spill::Cursor<'_>,
    ) -> Result<Self, crate::xact::spill::SpillError> {
        let rows = c.u32()? as usize;
        let n = c.u32()? as usize;
        let buffers = (0..n)
            .map(|_| ColumnBuf::decode(c))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            rel,
            route,
            buffers,
            rows,
        })
    }
}

/// Per-row synthetic column inputs
pub(crate) struct RowStamp {
    pub(crate) source_lsn: u64,
    pub(crate) xid: u32,
    pub(crate) commit_ts: i64,
    pub(crate) op_code: i8,
    pub(crate) leaf: Option<u32>,
}

fn name_column(mut e: EmitterError, target: &str) -> EmitterError {
    if let EmitterError::UnsupportedValue {
        ref mut target_column,
        ..
    } = e
    {
        *target_column = target.to_string();
    }
    e
}

#[cfg(test)]
impl EncodedChunk {
    /// Mapped cells per row read back from wire buffers: fixed widths as
    /// little-endian bytes, strings as varlena bodies, so builders state both
    pub(crate) fn cells(&self) -> Vec<Vec<Datum>> {
        let fixed = Datum::fixed;
        let string = |offsets: &[u64], data: &[u8], r: usize| {
            let start = r.checked_sub(1).map_or(0, |p| offsets[p] as usize);
            Datum::Varlena(data[start..offsets[r] as usize].to_vec())
        };
        let mapped = self.route.mapping.columns.len();
        let mut rows = vec![Vec::with_capacity(mapped); self.rows];
        for buf in &self.buffers[..mapped] {
            for (r, row) in rows.iter_mut().enumerate() {
                row.push(match buf.wire() {
                    Wire::Fixed { width, bytes } => fixed(&bytes[r * width..(r + 1) * width]),
                    Wire::NullableFixed { null_map, .. }
                    | Wire::NullableString { null_map, .. }
                        if null_map[r] != 0 =>
                    {
                        Datum::Null
                    }
                    Wire::NullableFixed { width, inner, .. } => {
                        fixed(&inner[r * width..(r + 1) * width])
                    }
                    Wire::String { offsets, data, .. }
                    | Wire::NullableString { offsets, data, .. } => string(offsets, data, r),
                    Wire::Oracle(cells) => Datum::Text(format!("{:?}", cells[r]).into_bytes()),
                });
            }
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ch::CompressionChoice;
    use crate::column_rules::{ColumnRule, Substitute};
    use crate::decode::datum::{Datum, build};
    use crate::emit::column::tests::buf;
    use crate::emit::column::{DecimalWidth, DecimalWire, literal_column, timestamp_ticks};
    use crate::mapping::ColumnMapping;
    use crate::mimalloc;
    use crate::runtime_config::MatchKind;

    use crate::decode::heap_decoder::{DecodedHeap, DecodedTuple};
    use walrus::pg::walparser::RelFileNode;

    #[test]
    fn is_retryable_only_for_transport_and_server_faults() {
        assert!(is_retryable(&EmitterError::Io(std::io::Error::other(
            "reset"
        ))));
        assert!(is_retryable(&EmitterError::ServerException {
            code: 241,
            message: "MEMORY_LIMIT_EXCEEDED".into(),
        }));
        // Semantic / config faults are terminal
        assert!(!is_retryable(&EmitterError::Type("bad decimal".into())));
        assert!(!is_retryable(&EmitterError::Config("missing host".into())));
        assert!(!is_retryable(&EmitterError::NoTableMapping(
            "public.t".into()
        )));
        assert!(!is_retryable(&EmitterError::CompressionUnsupported("zstd")));
    }

    #[test]
    fn emitter_error_converts_into_decoder_observer_error() {
        let d: DecoderSinkError = EmitterError::Type("nope".into()).into();
        assert_eq!(format!("{d:?}"), r#"Observer("type: nope")"#);
    }

    fn mk_mapping() -> TableMapping {
        TableMapping {
            target: TableTarget::new("default", "foo"),
            columns: vec![
                ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int32".into(),
                    type_pinned: false,
                },
                ColumnMapping {
                    src_attnum: 2,
                    target_name: "name".into(),
                    target_type: "Nullable(String)".into(),
                    type_pinned: false,
                },
            ],
            tees: Vec::new(),
        }
    }

    fn col_rules(entries: &[(&str, &str)]) -> ColumnRules {
        let mut b = crate::column_rules::ColumnRulesBuilder::new();
        for (attname, target_type) in entries {
            b.add(
                &RelName::new("public", "foo"),
                MatchKind::Exact,
                attname,
                MatchKind::Exact,
                ColumnRule {
                    target_type: Some((*target_type).into()),
                    ..ColumnRule::default()
                },
            );
        }
        b.finish().0
    }

    fn mk_rel() -> RelDescriptor {
        use crate::schema::{RelAttr, ReplIdent};
        use walrus::pg::walparser::RelFileNode;
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            oid: 16385,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "foo"),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![
                RelAttr {
                    attnum: 1,
                    name: "id".into(),
                    type_oid: 23,
                    typmod: -1,
                    not_null: true,
                    dropped: false,
                    type_name: "int4".into(),
                    type_byval: true,
                    type_len: 4,
                    type_align: b'i',
                    type_storage: b'p',
                    missing_default: None,
                },
                RelAttr {
                    attnum: 2,
                    name: "name".into(),
                    type_oid: 25,
                    typmod: -1,
                    not_null: false,
                    dropped: false,
                    type_name: "text".into(),
                    type_byval: false,
                    type_len: -1,
                    type_align: b'i',
                    type_storage: b'x',
                    missing_default: None,
                },
            ],
        }
    }

    fn committed(id: i32, name: Option<&str>) -> CommittedTuple {
        let name_col = Some(name.map(build::text).unwrap_or(Datum::Null));
        CommittedTuple {
            decoded: DecodedHeap {
                rfn: RelFileNode {
                    spc_node: 1663,
                    db_node: 5,
                    rel_node: 16385,
                },
                xid: 42,
                source_lsn: 0xCAFE,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: vec![Some(build::int4(id)), name_col],
                    partial: false,
                }),
                old: None,
            },
            commit_ts: 1_000_000,
            commit_lsn: 0xD00D,
        }
    }

    #[test]
    fn compression_choice_parses_case_insensitively() {
        assert_eq!(
            "LZ4".parse::<CompressionChoice>().unwrap(),
            CompressionChoice::Lz4
        );
        assert_eq!(
            "Zstd".parse::<CompressionChoice>().unwrap(),
            CompressionChoice::Zstd
        );
        assert_eq!(
            "none".parse::<CompressionChoice>().unwrap(),
            CompressionChoice::None
        );
        assert_eq!(
            "".parse::<CompressionChoice>().unwrap(),
            CompressionChoice::None
        );
        "snappy"
            .parse::<CompressionChoice>()
            .expect_err("unknown codec");
    }

    #[test]
    fn compression_choice_build_codec_respects_features() {
        // `None` still installs a decoder: the wire flag is what we send, and
        // the server answers in whatever method it is configured for
        let none = CompressionChoice::None.build_codec().unwrap();
        assert_eq!(none.is_some(), cfg!(any(feature = "lz4", feature = "zstd")));
        let lz4 = CompressionChoice::Lz4.build_codec();
        #[cfg(feature = "lz4")]
        {
            assert!(lz4.unwrap().is_some());
        }
        #[cfg(not(feature = "lz4"))]
        {
            assert!(matches!(
                lz4,
                Err(EmitterError::CompressionUnsupported("lz4"))
            ));
        }
        let zstd = CompressionChoice::Zstd.build_codec();
        #[cfg(feature = "zstd")]
        {
            assert!(zstd.unwrap().is_some());
        }
        #[cfg(not(feature = "zstd"))]
        {
            assert!(matches!(
                zstd,
                Err(EmitterError::CompressionUnsupported("zstd"))
            ));
        }
    }

    #[test]
    fn literal_string_cells_stay_off_the_oracle_batch_estimate() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let mut rel = mk_rel();
        // Source type the local matrix misses, so `name` plans as an oracle
        // column while its String target still renders points in-daemon
        rel.attributes[1].type_oid = 17_000;
        rel.attributes[1].type_name = "geometry".into();
        let m = mk_mapping();
        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .unwrap();
        assert!(matches!(
            plan.columns[1].encoding,
            ColumnEncoding::Oracle { .. }
        ));
        let mut enc = TableEncoder::new(plan).unwrap();
        let overhead = enc.oracle_bytes;
        let row = |id: i32, body: Vec<u8>| {
            let mut row = committed(id, None);
            row.decoded.new.as_mut().expect("insert tuple").columns[1] = Some(build::bytes(body));
            row
        };
        for id in 1..=2 {
            let point = crate::ops::oracle::point_body(f64::from(id), 2.0);
            assert_eq!(
                enc.append_row(&row(id, point), &m, OP_INSERT).unwrap(),
                Append::Done,
            );
        }
        assert_eq!(
            enc.oracle_bytes, overhead,
            "literals owe the request nothing"
        );
        let literal = |enc: &TableEncoder, want: bool| {
            matches!(&enc.buffers[1], ColumnBuf::Oracle(o)
                if literal_column(o, enc.rows).is_some() == want)
        };
        assert!(literal(&enc, true));
        // A cell needing conversion sends the column whole, stranding the
        // literals onto the request
        let raw = OracleCell::DiskRaw(vec![1, 2, 3]).wire_bytes();
        assert_eq!(
            enc.append_row(&row(3, vec![1, 2, 3]), &m, OP_INSERT)
                .unwrap(),
            Append::Done
        );
        assert!(enc.oracle_bytes > overhead + raw, "literals stranded");
        assert!(literal(&enc, false));
    }

    fn stamp() -> RowStamp {
        RowStamp {
            source_lsn: 0xCAFE,
            xid: 42,
            commit_ts: 0,
            op_code: OP_INSERT,
            leaf: None,
        }
    }

    /// `mk_rel` with `name` a type only the oracle decodes
    fn oracle_rel() -> RelDescriptor {
        let mut rel = mk_rel();
        rel.attributes[1].type_oid = 17_000;
        rel.attributes[1].type_name = "geometry".into();
        rel
    }

    fn plan_of(rel: &RelDescriptor) -> TablePlan {
        TablePlan::build(
            Allocator::global(&mimalloc::MiMalloc),
            rel,
            &mk_mapping(),
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .unwrap()
    }

    #[test]
    fn view_rows_default_columns_without_source() {
        let mut rel = mk_rel();
        rel.attributes[1].dropped = true;
        let plan = plan_of(&rel);
        assert_eq!(plan.columns[1].source, None);
        let mut enc = TableEncoder::new(plan).unwrap();
        let id = 1i32.to_le_bytes();
        let datums = [DatumRef::Fixed(&id), DatumRef::Varlena(b"x")];
        enc.append_view(&rel, &datums, &[], &stamp()).unwrap();
        assert_eq!(
            enc.buffers[1].wire(),
            Wire::NullableString {
                offsets: &[0],
                data: b"",
                null_map: &[1],
                absent: b"",
            }
        );
    }

    #[test]
    fn oracle_cell_errors_name_their_column() {
        let mut enc = TableEncoder::new(plan_of(&oracle_rel())).unwrap();
        let id = 1i32.to_le_bytes();
        let err = enc
            .append_slots(
                |s| {
                    Ok(if s.index == 0 {
                        DatumRef::Fixed(&id)
                    } else {
                        DatumRef::BadTag(b"x")
                    })
                },
                &stamp(),
            )
            .unwrap_err();
        assert!(
            matches!(&err, EmitterError::UnsupportedValue { target_column, kind }
                if target_column == "name" && *kind == "unresolved source value on an oracle column"),
            "{err}"
        );
    }

    #[test]
    fn append_block_checks_layout_and_oracle_budget() {
        let rel = oracle_rel();
        let m = mk_mapping();
        let block = |body: Vec<u8>| {
            let mut enc = TableEncoder::new(plan_of(&rel)).unwrap();
            let mut row = committed(1, None);
            row.decoded.new.as_mut().expect("insert tuple").columns[1] = Some(build::bytes(body));
            assert_eq!(enc.append_row(&row, &m, OP_INSERT).unwrap(), Append::Done);
            enc.take_block().0
        };
        let half = ORACLE_BATCH_SEAL_BYTES / 2;
        let mut enc = TableEncoder::new(plan_of(&rel)).unwrap();
        assert_eq!(
            enc.append_block(&mut block(vec![1; half]), 1).unwrap(),
            Append::Done
        );
        let mut over = block(vec![2; half]);
        assert_eq!(enc.append_block(&mut over, 1).unwrap(), Append::Full);
        assert!(!over.is_empty(), "full leaves block in place");
        assert_eq!(
            enc.append_block(&mut block(vec![3]), 1).unwrap(),
            Append::Done
        );
        assert_eq!(enc.rows, 2);
        let mut reshaped = block(vec![4]);
        reshaped[0] = buf("String");
        assert!(matches!(
            enc.append_block(&mut reshaped, 1),
            Err(EmitterError::Type(_))
        ));
    }

    #[test]
    fn chunk_cells_read_nulls_and_oracle_cells() {
        let mut oracle = OracleColumnBuf::string(0, -1);
        oracle.push(OracleCell::Default);
        oracle.push(OracleCell::Literal(b"a".to_vec()));
        let chunk = EncodedChunk {
            rel: Arc::new(mk_rel()),
            route: crate::emit::route::RouteSnapshot::freeze(
                Arc::new(mk_mapping()),
                Arc::default(),
                Default::default(),
            ),
            buffers: vec![
                ColumnBuf::NullableFixed {
                    width: 4,
                    null_map: vec![1, 0],
                    inner: [[0; 4], 7i32.to_le_bytes()].concat(),
                },
                ColumnBuf::Oracle(oracle),
            ],
            rows: 2,
        };
        let text = |s: &str| Datum::Text(s.into());
        assert_eq!(
            chunk.cells(),
            [
                [Datum::Null, text("Default")],
                [build::int4(7), text("Literal([97])")],
            ]
        );
    }

    #[test]
    fn table_plan_rejects_local_temporal_on_other_kind() {
        let mut rel = mk_rel();
        rel.attributes[0].type_oid = crate::schema::DATEOID;
        let result = TablePlan::build(
            Allocator::global(&mimalloc::MiMalloc),
            &rel,
            &mk_mapping(),
            &ColumnRules::default(),
            &SystemColumns::default(),
        );
        assert!(matches!(result, Err(EmitterError::Config(e)) if e.contains("local temporal")));
    }

    #[test]
    fn table_plan_keeps_default_on_unparseable_override() {
        let rules = col_rules(&[("id", "Nope(((")]);
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let plan = TablePlan::build(
            alloc,
            &mk_rel(),
            &mk_mapping(),
            &rules,
            &SystemColumns::default(),
        )
        .unwrap();
        assert_eq!(plan.columns[0].type_repr, "Int32");
    }

    #[test]
    fn predeclared_composite_column_plans_oracle_without_source_type() {
        let mut m = mk_mapping();
        m.columns.push(ColumnMapping {
            src_attnum: 3,
            target_name: "tags".into(),
            target_type: "Array(String)".into(),
            type_pinned: false,
        });
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let plan = TablePlan::build(
            alloc,
            &mk_rel(),
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .unwrap();
        let oracle = ColumnEncoding::Oracle {
            source_type_oid: 0,
            source_typmod: -1,
        };
        assert_eq!(plan.columns[2].encoding, oracle);
    }

    #[test]
    fn quote_ident_escapes_backticks() {
        assert_eq!(quote_ident("foo"), "`foo`");
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert_eq!(quote_ident("a\\b"), "`a\\\\b`");
        assert_eq!(quote_ident("a\\"), "`a\\\\`");
    }

    #[test]
    fn table_plan_builds_insert_with_synthetic_columns() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .expect("plan builds");
        assert_eq!(plan.inserts.len(), 1);
        let insert_sql = &plan.inserts[0];
        assert!(insert_sql.sql.contains("INSERT INTO `default`.`foo`"));
        assert!(insert_sql.sql.contains("`id`"));
        assert!(insert_sql.sql.contains("`name`"));
        assert!(insert_sql.sql.contains("`_lsn`"));
        assert!(insert_sql.sql.contains("`_xid`"));
        assert!(insert_sql.sql.contains("`_commit_ts`"));
        assert!(insert_sql.sql.contains("`_is_deleted`"));
        assert!(insert_sql.sql.ends_with(") FORMAT Native"));
    }

    #[test]
    fn table_plan_sends_same_columns_to_each_tee() {
        use crate::mapping::Tee;
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let mut m = mk_mapping();
        m.tees = vec![
            Tee {
                table: "foo_audit".into(),
                ..Tee::default()
            },
            Tee {
                database: Some("wh".into()),
                table: "foo".into(),
                ..Tee::default()
            },
        ];
        let plan = TablePlan::build(
            alloc,
            &mk_rel(),
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .unwrap();
        let (primary, tees) = plan.inserts.split_first().expect("primary first");
        let cols = primary.sql.strip_prefix("INSERT INTO `default`.`foo` ");
        assert_eq!(
            tees.iter()
                .map(|query| query.sql.clone())
                .collect::<Vec<_>>(),
            [
                format!("INSERT INTO `default`.`foo_audit` {}", cols.unwrap()),
                format!("INSERT INTO `wh`.`foo` {}", cols.unwrap()),
            ]
        );
    }

    #[test]
    fn table_plan_applies_admissible_column_override() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let mut m = mk_mapping();
        // numeric-shaped default: the plan drill `numeric(38,0)` → `Int128`
        m.columns[0].target_type = "Decimal(38, 0)".into();
        let rules = col_rules(&[("id", "Int128")]);
        let plan = TablePlan::build(alloc, &rel, &m, &rules, &SystemColumns::default()).unwrap();
        assert_eq!(plan.columns[0].type_repr, "Int128");
        // scale-0 decimal wire keeps the numeric text→scaled encode path
        assert_eq!(
            plan.columns[0].decimal,
            Some(DecimalWire {
                scale: 0,
                width: DecimalWidth::D128
            })
        );
        assert_eq!(plan.columns[1].type_repr, "Nullable(String)");
    }

    #[test]
    fn table_plan_override_keys_on_source_attname_not_target_name() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let mut m = mk_mapping();
        // Operator-renamed CH column: override still keys on source attname
        m.columns[1].target_name = "label".into();
        let rules = col_rules(&[("name", "String")]);
        let plan = TablePlan::build(alloc, &rel, &m, &rules, &SystemColumns::default()).unwrap();
        assert_eq!(plan.columns[1].name, "label");
        assert_eq!(plan.columns[1].type_repr, "String");
    }

    #[test]
    fn table_plan_keeps_default_on_wire_incompatible_override() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let m = mk_mapping();
        // encode_value writes int4 as 4 LE bytes; no textualization exists,
        // so Int32 → String must fall back rather than poison the batcher
        let rules = col_rules(&[("id", "String")]);
        let plan = TablePlan::build(alloc, &rel, &m, &rules, &SystemColumns::default()).unwrap();
        assert_eq!(plan.columns[0].type_repr, "Int32");
    }

    #[test]
    fn temporal_policy_rejects_oracle_encoding() {
        let mut rel = mk_rel();
        rel.attributes[0].type_oid = 99999;
        let mut mapping = mk_mapping();
        mapping.columns[0].target_type = "Nullable(DateTime64(6, 'UTC'))".into();
        let mut builder = crate::column_rules::ColumnRulesBuilder::new();
        builder.add(
            &rel.rel_name,
            MatchKind::Exact,
            "id",
            MatchKind::Exact,
            ColumnRule {
                pos_inf: Some(Substitute::Null),
                ..ColumnRule::default()
            },
        );
        let result = TablePlan::build(
            Allocator::global(&mimalloc::MiMalloc),
            &rel,
            &mapping,
            &builder.finish().0,
            &SystemColumns::default(),
        );
        assert!(
            matches!(result, Err(EmitterError::Config(e)) if e.contains("needs local encoding"))
        );
    }

    #[test]
    fn temporal_infinity_rows_reject_or_write_null() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        for (ty, oid, values) in [
            (
                "Date32",
                crate::schema::DATEOID,
                vec![build::date(i32::MIN), build::date(i32::MAX)],
            ),
            (
                "DateTime64(3, 'UTC')",
                crate::schema::TIMESTAMPOID,
                vec![build::micros(i64::MIN), build::micros(i64::MAX)],
            ),
            (
                "DateTime64(6, 'UTC')",
                crate::schema::TIMESTAMPTZOID,
                vec![build::micros(i64::MIN), build::micros(i64::MAX)],
            ),
        ] {
            let mut rel = mk_rel();
            rel.attributes[0].type_oid = oid;
            rel.attributes[0].type_len = if oid == crate::schema::DATEOID { 4 } else { 8 };
            let mut mapping = mk_mapping();
            mapping.columns[0].target_type = format!("Nullable({ty})");
            for value in values {
                let mut row = committed(1, None);
                row.decoded.new.as_mut().unwrap().columns[0] = Some(value);
                for substitute in [false, true] {
                    let mut builder = crate::column_rules::ColumnRulesBuilder::new();
                    if substitute {
                        builder.add(
                            &rel.rel_name,
                            MatchKind::Exact,
                            "id",
                            MatchKind::Exact,
                            ColumnRule {
                                pos_inf: Some(Substitute::Null),
                                neg_inf: Some(Substitute::Null),
                                ..ColumnRule::default()
                            },
                        );
                    }
                    let plan = TablePlan::build(
                        alloc,
                        &rel,
                        &mapping,
                        &builder.finish().0,
                        &SystemColumns::default(),
                    )
                    .unwrap();
                    let mut encoder = TableEncoder::new(plan).unwrap();
                    let result = encoder.append_row(&row, &mapping, OP_INSERT);
                    if substitute {
                        result.unwrap();
                        let width = rel.attributes[0].type_len as usize;
                        assert_eq!(
                            encoder.buffers[0].wire(),
                            Wire::NullableFixed {
                                width,
                                null_map: &[1],
                                inner: &vec![0; width],
                            }
                        );
                    } else {
                        let err = result.unwrap_err().to_string();
                        assert!(
                            err.contains("id") && err.contains("temporal infinity"),
                            "{err}"
                        );
                        assert_eq!(encoder.buffers[0].approx_size(), 0);
                    }
                }
            }
        }
    }

    #[test]
    fn timestamp_rows_use_destination_precision() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        for scale in [0, 3, 6, 9] {
            let mut mapping = mk_mapping();
            mapping.columns[0].target_type = format!("Nullable(DateTime64({scale}, 'UTC'))");
            let mut rel = mk_rel();
            rel.attributes[0].type_oid = crate::schema::TIMESTAMPOID;
            rel.attributes[0].type_len = 8;
            let plan = TablePlan::build(
                alloc,
                &rel,
                &mapping,
                &ColumnRules::default(),
                &SystemColumns::default(),
            )
            .unwrap();
            let mut encoder = TableEncoder::new(plan).unwrap();
            for seconds in [-1i64, 0, 946_684_800] {
                let mut row = committed(1, None);
                row.decoded.new.as_mut().unwrap().columns[0] =
                    Some(build::micros(seconds * 1_000_000 - DATETIME64_PG_EPOCH_US));
                encoder.append_row(&row, &mapping, OP_INSERT).unwrap();
            }
            let expected: Vec<u8> = [-1i64, 0, 946_684_800]
                .into_iter()
                .flat_map(|s| (s * 10i64.pow(scale)).to_le_bytes())
                .collect();
            assert_eq!(
                encoder.buffers[0].wire(),
                Wire::NullableFixed {
                    width: 8,
                    null_map: &[0, 0, 0],
                    inner: &expected,
                }
            );
        }
        for us in [-1999, -1001, -1000, -1, 0, 1, 1000, 1001, 1999] {
            assert_eq!(
                timestamp_ticks(us - DATETIME64_PG_EPOCH_US, 3).unwrap(),
                us / 1000
            );
        }
        let err = timestamp_ticks(i64::MAX - 1, 6).unwrap_err();
        assert!(err.to_string().contains("overflows storage"));
        assert!(timestamp_ticks(i64::MAX / 1_000_000 * 1_000_000, 0).is_ok());
        assert!(timestamp_ticks(i64::MIN + 1, 9).is_err());
    }

    #[test]
    fn nan_row_lands_as_configured_substitute() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let mut rel = mk_rel();
        rel.attributes[0].type_oid = crate::schema::NUMERICOID;
        rel.attributes[0].type_len = -1;
        let mut m = mk_mapping();
        m.columns[0].target_type = "Decimal(10, 2)".into();
        let mut row = committed(1, None);
        row.decoded.new.as_mut().expect("insert tuple").columns[0] = Some(build::numeric("NaN"));
        let zero = Wire::Fixed {
            width: 8,
            bytes: &0i64.to_le_bytes(),
        };

        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .unwrap();
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&row, &m, OP_INSERT).unwrap();
        assert_eq!(enc.buffers[0].wire(), zero, "unset defaults to 0");

        let rules_with = |nan: Substitute| {
            let mut b = crate::column_rules::ColumnRulesBuilder::new();
            b.add(
                &RelName::new("public", "*"),
                MatchKind::Glob,
                "*",
                MatchKind::Glob,
                ColumnRule {
                    nan: Some(nan),
                    ..ColumnRule::default()
                },
            );
            b.finish().0
        };

        let rules = rules_with(Substitute::Reject);
        let plan = TablePlan::build(alloc, &rel, &m, &rules, &SystemColumns::default()).unwrap();
        let err = TableEncoder::new(plan)
            .unwrap()
            .append_row(&row, &m, OP_INSERT)
            .unwrap_err();
        assert!(err.to_string().contains("id"), "{err}");

        let rules = rules_with(Substitute::Literal("0".into()));
        let plan = TablePlan::build(alloc, &rel, &m, &rules, &SystemColumns::default()).unwrap();
        assert_eq!(
            plan.columns[1].non_finite,
            NonFinite::default(),
            "String column keeps text"
        );
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&row, &m, OP_INSERT).unwrap();
        assert_eq!(enc.buffers[0].wire(), zero);
    }

    #[test]
    fn is_deleted_codes_delete_in_trailing_buffer() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .expect("plan builds");
        assert!(plan.inserts[0].sql.contains("`_is_deleted`"));
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&committed(1, Some("a")), &m, OP_INSERT)
            .unwrap();
        enc.append_row(&committed(1, Some("a")), &m, OP_DELETE)
            .unwrap();
        // _is_deleted is the trailing buffer: 0 for insert, 1 for delete
        let last = enc.buffers.len() - 1;
        assert_eq!(
            enc.buffers[last].wire(),
            Wire::Fixed {
                width: 1,
                bytes: &[0, 1],
            }
        );
    }

    #[test]
    fn json_target_encodes_locally_and_fills_absent_cells() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let mut rel = mk_rel();
        rel.attributes[1].type_oid = crate::schema::JSONBOID;
        rel.attributes[1].type_name = "jsonb".into();
        for target in ["JSON", "Nullable(JSON)"] {
            let mut m = mk_mapping();
            m.columns[1].target_type = target.into();
            let plan = TablePlan::build(
                alloc,
                &rel,
                &m,
                &ColumnRules::default(),
                &SystemColumns::default(),
            )
            .unwrap();
            assert_eq!(plan.columns[1].encoding, ColumnEncoding::Local, "{target}");
            let mut enc = TableEncoder::new(plan).unwrap();
            let mut doc = committed(1, None);
            doc.decoded.new.as_mut().unwrap().columns[1] = Some(build::out_text(r#"{"a": 1}"#));
            enc.append_row(&doc, &m, OP_INSERT).unwrap();
            enc.append_row(&committed(2, None), &m, OP_INSERT).unwrap();
            let (offsets, data, absent) = (&[8, 10][..], &br#"{"a": 1}{}"#[..], &b"{}"[..]);
            let expected = if target == "JSON" {
                Wire::String {
                    offsets,
                    data,
                    absent,
                }
            } else {
                Wire::NullableString {
                    offsets,
                    data,
                    null_map: &[0, 1],
                    absent,
                }
            };
            assert_eq!(enc.buffers[1].wire(), expected, "{target}");
        }
    }

    /// Key-only old image, the shape a delete logs under non-FULL
    /// replica identity
    #[test]
    fn oracle_row_over_the_seal_threshold_starts_a_new_batch() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let mut m = mk_mapping();
        m.columns[1].target_type = "Array(Nullable(String))".into();
        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .unwrap();
        assert!(matches!(
            plan.columns[1].encoding,
            ColumnEncoding::Oracle { .. }
        ));
        let mut enc = TableEncoder::new(plan).unwrap();
        let big = "x".repeat(ORACLE_BATCH_SEAL_BYTES * 3 / 5);
        assert_eq!(
            enc.append_row(&committed(1, Some(&big)), &m, OP_INSERT)
                .unwrap(),
            Append::Done
        );
        let before = enc.oracle_bytes;
        assert_eq!(
            enc.append_row(&committed(2, Some(&big)), &m, OP_INSERT)
                .unwrap(),
            Append::Full
        );
        assert_eq!(
            (enc.rows, enc.oracle_bytes),
            (1, before),
            "row not appended"
        );
        assert_eq!(enc.take_block().1, 1);
        assert_eq!(
            enc.append_row(&committed(2, Some(&big)), &m, OP_INSERT)
                .unwrap(),
            Append::Done
        );
        assert_eq!(enc.rows, 1);
    }

    /// Encode faults name the column only when the value is unsupported
    #[test]
    fn append_errors_name_unsupported_columns() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let (rules, sys) = (ColumnRules::default(), SystemColumns::default());
        let m = mk_mapping();
        let plan = TablePlan::build(alloc, &mk_rel(), &m, &rules, &sys).unwrap();
        let mut enc = TableEncoder::new(plan).unwrap();
        let mut toast = committed(1, None);
        toast.decoded.new.as_mut().unwrap().columns[0] =
            Some(Datum::External(crate::decode::datum::ToastPointer {
                va_rawsize: 0,
                va_extinfo: 0,
                va_valueid: 0,
                va_toastrelid: 0,
            }));
        let r = enc.append_row(&toast, &m, OP_INSERT);
        assert!(
            matches!(&r, Err(EmitterError::UnsupportedValue { target_column, .. }) if target_column == "id"),
            "{r:?}"
        );

        let plan = TablePlan::build(alloc, &mk_rel(), &m, &rules, &sys).unwrap();
        let mut enc = TableEncoder::new(plan).unwrap();
        let mut wide = committed(1, None);
        wide.decoded.new.as_mut().unwrap().columns[0] = Some(build::int8(1));
        let r = enc.append_row(&wide, &m, OP_INSERT);
        assert!(matches!(r, Err(EmitterError::Type(_))), "{r:?}");
    }

    fn committed_delete(id: i32) -> CommittedTuple {
        CommittedTuple {
            decoded: DecodedHeap {
                rfn: RelFileNode {
                    spc_node: 1663,
                    db_node: 5,
                    rel_node: 16385,
                },
                xid: 42,
                source_lsn: 0xCAFE,
                op: HeapOp::Delete,
                new: None,
                old: Some(DecodedTuple {
                    columns: vec![Some(build::int4(id)), None],
                    partial: false,
                }),
            },
            commit_ts: 1_000_000,
            commit_lsn: 0xD00D,
        }
    }

    #[test]
    fn absent_or_null_coerces_to_default_on_non_nullable_target() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let mut m = mk_mapping();
        m.columns[1].target_type = "String".into();
        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .expect("plan builds");
        let mut enc = TableEncoder::new(plan).unwrap();
        // Delete: non-key column absent from the key-only old image
        enc.append_row(&committed_delete(3), &m, OP_DELETE).unwrap();
        // Insert: genuine NULL mapped onto the non-Nullable column
        enc.append_row(&committed(4, None), &m, OP_INSERT).unwrap();
        assert_eq!(
            enc.buffers[1].wire(),
            Wire::String {
                offsets: &[0, 0],
                data: b"",
                absent: b"",
            }
        );
    }

    #[test]
    fn absent_stays_null_on_nullable_target() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .expect("plan builds");
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&committed_delete(3), &m, OP_DELETE).unwrap();
        assert_eq!(
            enc.buffers[1].wire(),
            Wire::NullableString {
                offsets: &[0],
                data: b"",
                null_map: &[1],
                absent: b"",
            }
        );
    }

    #[test]
    fn encoder_accumulates_into_typed_buffers() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let m = mk_mapping();
        let plan = TablePlan::build(
            alloc,
            &rel,
            &m,
            &ColumnRules::default(),
            &SystemColumns::default(),
        )
        .unwrap();
        let mut enc = TableEncoder::new(plan).unwrap();
        enc.append_row(&committed(7, Some("seven")), &m, OP_INSERT)
            .unwrap();
        enc.append_row(&committed(8, None), &m, OP_INSERT).unwrap();
        enc.append_row(&committed(9, Some("nine")), &m, OP_INSERT)
            .unwrap();
        assert_eq!(enc.rows, 3);
        let ids: Vec<u8> = [7i32, 8, 9].iter().flat_map(|n| n.to_le_bytes()).collect();
        assert_eq!(
            enc.buffers[0].wire(),
            Wire::Fixed {
                width: 4,
                bytes: &ids,
            }
        );
        assert_eq!(
            enc.buffers[1].wire(),
            Wire::NullableString {
                offsets: &[5, 5, 9],
                data: b"sevennine",
                null_map: &[0, 1, 0],
                absent: b"",
            }
        );
        let off = m.columns.len();
        let lsns = 0xCAFEu64.to_le_bytes().repeat(3);
        assert_eq!(
            enc.buffers[off].wire(),
            Wire::Fixed {
                width: 8,
                bytes: &lsns,
            }
        );
        assert_eq!(
            enc.buffers[off + 3].wire(),
            Wire::Fixed {
                width: 1,
                bytes: &[0, 0, 0],
            }
        );
    }

    #[test]
    fn plan_renames_synthetic_columns_and_can_drop_the_marker() {
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let rel = mk_rel();
        let m = mk_mapping();
        let sys = SystemColumns {
            lsn: "_v".into(),
            xid: "_x".into(),
            commit_ts: "_at".into(),
            is_deleted: None,
            leaf: None,
        };
        let plan =
            TablePlan::build(alloc, &rel, &m, &ColumnRules::default(), &sys).expect("plan builds");
        assert!(
            plan.inserts[0]
                .sql
                .ends_with("`_v`, `_x`, `_at`) FORMAT Native"),
            "{:?}",
            plan.inserts
        );
        assert!(plan.synth_is_deleted.is_none());
        let enc = TableEncoder::new(plan).expect("encoder");
        // Mapped columns plus three synthetic, no marker buffer
        assert_eq!(enc.buffers.len(), m.columns.len() + 3);
    }

    /// A partition's rows encode under its root's column rules and stamp
    /// the partition column after the system columns
    #[test]
    fn partition_route_plans_and_stamps_the_partition_column() {
        use crate::emit::route::{PartitionOf, RouteSnapshot};
        use crate::mapping::RowPolicy;
        let alloc = Allocator::global(&mimalloc::MiMalloc);
        let leaf = RelDescriptor {
            rel_name: RelName::new("public", "foo_1"),
            ..mk_rel()
        };
        let root = RelName::new("public", "foo");
        let mut rules = crate::column_rules::ColumnRulesBuilder::new();
        rules.add(
            &root,
            MatchKind::Exact,
            "name",
            MatchKind::Exact,
            ColumnRule {
                target_type: Some("LowCardinality(Nullable(String))".into()),
                ..ColumnRule::default()
            },
        );
        let route = RouteSnapshot::freeze(
            Arc::new(mk_mapping()),
            Arc::new(rules.finish().0),
            RowPolicy::default(),
        )
        .with_partition(PartitionOf {
            root,
            leaf: 16999,
            shared: true,
        });
        let plan = TablePlan::for_route(alloc, &leaf, &route).expect("plan builds");
        assert!(
            plan.inserts[0]
                .sql
                .ends_with("`_is_deleted`, `_leaf_oid`) FORMAT Native"),
            "{:?}",
            plan.inserts
        );
        assert_eq!(
            plan.columns[1].type_repr, "LowCardinality(Nullable(String))",
            "root's column rule applies"
        );
        let mut enc = TableEncoder::new(plan).expect("encoder");
        enc.append_routed(
            &committed(1, Some("a")),
            &route.mapping,
            OP_INSERT,
            Some(16999),
        )
        .unwrap();
        assert!(
            matches!(enc.buffers.last(), Some(ColumnBuf::Fixed { width: 4, bytes })
                if bytes[..] == 16999u32.to_le_bytes()),
            "partition column is UInt32"
        );
    }
}
