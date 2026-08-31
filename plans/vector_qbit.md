# vector_qbit — fix halfvec decode + map pgvector to ClickHouse QBit

Two coupled changes to the pgvector path:

1. **Correctness:** `halfvec` is currently mis-decoded (read as `f32`), so it
   lands empty. Fix the on-disk decode.
2. **Mapping:** map `vector`/`halfvec` to CH `QBit(elem, dim)` (vector search
   type, tunable precision) instead of `Array(Float32)`.

Both live in the pgvector in-tree render + type-bridge + DDL path; the native
insert path is deliberately left untouched.

## Part A — fix halfvec decode

`render_ext_columns` (`src/ops/oracle.rs:141`) routes both `vector` and
`halfvec` to `vector_to_text`, which reads `[dim u16][unused u16][f32 × dim]`
— 4 bytes/element (`src/ops/oracle.rs:168`, `off = 4 + i*4`). Correct for
pgvector `vector` (fp32 elements). Wrong for `halfvec`, whose on-disk layout is
`int16 dim; int16 unused; half x[dim]` — **IEEE binary16, 2 bytes/element**.
Result: wrong offsets + the length guard `raw.len() < 4 + dim*4` fails →
returns `None` → column empties.

Steps:
- Add `halfvec_to_text(raw)` reading element `i` at `off = 4 + i*2`, 2 bytes LE,
  IEEE fp16 → f32, emit `[a,b,c]` (same decimal-list shape `encode_array` /
  `parse_vector_list` already consumes). Length guard `raw.len() >= 4 + dim*2`.
- Route `"halfvec"` → `halfvec_to_text`; keep `"vector"` → `vector_to_text`
  (`src/ops/oracle.rs:143`).
- fp16→f32: add the `half` crate (not currently a dep) — `half::f16::from_le_bytes([lo,hi]).to_f32()`.
  Hand-rolling the bit unpack is the no-dep alternative.
- Tests: known bit patterns — `0x3C00`→1.0, `0x4000`→2.0, `0xC000`→-2.0,
  `0x0001` (min subnormal), `0x7BFF` (max finite ≈65504). (pgvector rejects
  inf/NaN in halfvec, but decode should not panic on them.)

This fix stands on its own regardless of Part B.

## Part B — map vector/halfvec → QBit

### Target types

- `vector` (fp32) → `QBit(Float32, dim)`
- `halfvec` (fp16) → `QBit(BFloat16, dim)` — storage-halved, matches the source's
  intent; lossy vs IEEE fp16 (bf16 = 8-bit exp / 7-bit mantissa), acceptable for
  ANN. Never bit-reinterpreted: decode produces real f32 decimals, the server
  rounds on cast.
- No dimension (`typmod <= 0`, unspecified-dim pgvector column) → fall back to
  `Array(Float32)` (today's behaviour); QBit requires a fixed dimension.

`dim` comes from `att.typmod`. **Verify:** pgvector stores the dimension
directly in `atttypmod` (`vector(3)` → typmod 3, no VARHDRSZ offset), same for
`halfvec`. Helper: `fn pgvector_dim(typmod: i32) -> Option<u32>` returning
`(typmod > 0).then_some(typmod as u32)`.

### The DDL-vs-wire split (the mechanism)

The vendored binding's `Kind` enum has **no QBit variant**
(`clickhouse-c-rs/src/types.rs:16`), so `TypeAst::parse("QBit(...)")` cannot
produce a buildable column, and `ChScalar::from_kind` has no `BFloat16` arm
(`src/emit/ch_emitter.rs:1033`) so `Array(BFloat16)` is unbuildable too.

Therefore the native insert block stays **`Array(Float32)`** and the server
performs the implicit `Array → QBit` cast on INSERT (confirmed: arrays convert
to QBit when length == dimension; element type auto-converts, incl.
Float32→BFloat16 narrowing). The DDL column type is the only thing that becomes
`QBit(...)`.

Implementation — keep the wire type, split out a storage/DDL type:
- `base_type_for` (`src/catalog/type_bridge.rs:123`) keeps returning the **wire
  type** `Array(Float32)` for `vector`/`halfvec` (emitter, batcher, mapping
  `target_type`, block build all untouched — still `Array(Float32)`).
- Add `ddl_column_type(att) -> String` returning `QBit(Float32, dim)` /
  `QBit(BFloat16, dim)` for pgvector-with-dim, else the wire type.
- Carry it on the resolved column (add `ddl_type: Option<String>` alongside
  `ch_type`, default = `ch_type`). `render_create_table`
  (`src/emit/ch_ddl.rs:742`) emits `ddl_type` for the column definition; the
  mapping's `target_type` (insert) stays `ch_type`.

Net: CREATE TABLE says `col QBit(Float32, N)`; every INSERT sends an
`Array(Float32)` block; CH casts. No change to `ch_emitter`, `inserter`, or the
binding.

### Experimental setting

QBit is experimental (CH 25.10+). CREATE TABLE (and possibly INSERT) needs the
enabling setting on the session. Push it via the binding's per-query settings
(`chc_query_setting`, `clickhouse-c-rs/src/sys.rs:602`).
**Verify the exact GUC name** (`allow_experimental_qbit_type`) and whether the
insert session needs it, not just DDL. Deployments run CH 26.3 (>= 25.10), so
availability is fine; gate the QBit mapping on CH version to avoid breaking
older targets (fall back to `Array(Float32)`).

## Decisions / defaults

- Element type: `vector`→Float32, `halfvec`→BFloat16 (per review). Make it
  overridable later via `config_column` if lossless halfvec (Float32) is wanted;
  not required for v1.
- Stride: omit (no grouping).
- Existing tables created as `Array(Float32)` are **not** migrated — only newly
  auto-created tables get QBit. Recreate to adopt. Note in release notes.

## Risks / open items

- **Native `Array → QBit` INSERT (highest risk).** Docs confirm implicit
  conversion on INSERT + explicit CAST; PR ClickHouse#91846 added the Array→QBit
  cast. Must validate over the **native TCP protocol** (not just SQL VALUES)
  end-to-end. If native INSERT does not auto-convert, fallbacks: keep
  `Array(Float32)` DDL, or issue an `ALTER ... MODIFY`/materialized cast — decide
  only if validation fails.
- pgvector `atttypmod` dimension encoding — verify empirically (docker pgvector
  stack) before trusting `pgvector_dim`.
- Exact experimental setting name + whether INSERT needs it.
- Operator TOML mapping path (`render_create_table_from_mapping`,
  `src/emit/ch_ddl.rs:772`) uses `c.target_type` for both DDL and insert, so
  QBit is **auto-create only** in v1; a TOML `QBit` target_type would break the
  insert build. A separate storage-type knob is a follow-up.
- QBit columns are not orderable — never emit into `ORDER BY` (pgvector columns
  aren't PKs, so not an issue in practice).

## Test plan

- Unit: `halfvec_to_text` bit-pattern tests (Part A).
- Unit: `base_type_for`/`ddl_column_type` — `vector(3)`→wire `Array(Float32)` +
  DDL `QBit(Float32, 3)`; `halfvec(4)`→DDL `QBit(BFloat16, 4)`; unspecified-dim
  → `Array(Float32)` both.
- E2E on the docker stack (`stress/`, pgvector `embedding vector(3)`): fresh
  volumes → walshadow auto-creates `embedding QBit(Float32, 3)`; pgbench insert;
  confirm CH stores QBit and a `QBit`-aware distance query returns; add a
  `halfvec(N)` column and confirm values round-trip (decode fix + cast).

## Touch points

- `src/ops/oracle.rs` — `halfvec_to_text`, route halfvec (Part A).
- `Cargo.toml` — `half` crate (if used).
- `src/catalog/type_bridge.rs:123` — keep wire `Array(Float32)`; add
  `ddl_column_type` + `pgvector_dim`.
- `src/emit/ch_ddl.rs` — resolved `ddl_type` field; `render_create_table:742`
  uses it; QBit experimental setting on the DDL session.
- CH session settings wiring (`chc_query_setting`) — experimental flag.
