# catalog capture completeness

Descriptor capture enumerates affected relations from commit-record
relcache invalidations plus pump-side pg_class decodes, falling back to
capture-all when a boundary's effects cannot be enumerated. This doc maps
which catalog writes each mechanism covers, and the residual gaps.

## Dependency taxonomy: catalog → descriptor field

`RelDescriptor` embeds state from five catalogs:

| catalog | fields | enumerated by relcache invals? |
|---|---|---|
| pg_class | rfn, oid, toast_oid, kind, persistence, name, relnamespace | yes — every row change registers a relcache inval for that rel (PG `src/backend/utils/cache/inval.c`) |
| pg_attribute | attributes (physical layout, names, typmods) | yes — same mechanism |
| pg_index | replident pk/key attnums | yes — inval registered for the indexed rel |
| pg_namespace | `rel_name.namespace` text | **no** — namespace rename produces catcache invals only; classified via its syscache ids → capture-all |
| pg_type | `RelAttr.type_name` text | **no** — type rename produces catcache invals only |

## Capture-all trigger set

The filter forces `BoundaryInfo::capture_all` when:

- a dirty xact wrote pg_namespace (tracked by filenode, relocations
  followed via pg_class decode)
- the commit record (or a mid-xact `XLOG_XACT_INVALIDATIONS` record)
  carries a pg_namespace catcache inval (`NAMESPACENAME` / `NAMESPACEOID`,
  syscache ids keyed per WAL page magic) or a whole-catalog inval on oid
  2615 — the restart-safe trigger: commit invals cover the whole xact
  tree, so classification survives a resume floor past the pg_namespace
  writes
- the commit carries a whole-relcache flush (`relId == 0`)

Rationale: pg_namespace writes are rare (CREATE/ALTER/DROP SCHEMA), and a
rename silently changes every embedded namespace text with zero
per-relation invals — decode would route rows under the old name
indefinitely.

Catcache ids are the one per-major surface: `SysCacheIdentifier` values
come from name-sorted generation (PG `src/backend/catalog/genbki.pl`;
stable branches append via Z-prefixed names so ids hold within a major).
Each new major needs the namespace-id pair audited.

pg_type is **consciously excluded** from the trigger: CREATE TABLE writes
pg_type on every run (composite + array type rows), so capture-all would
fire constantly and reduce the enumerated path to dead code.

## Residual gaps

- **`type_name` staleness after `ALTER TYPE ... RENAME`.** Bounded: decode
  never reads `type_name` (physical layout comes from
  attlen/attalign/attbyval), and every SQL capture re-reads live typname —
  only a rel never recaptured after the rename carries the old text into a
  boot `Added` (CH type mapping consults it at CREATE). Remediations, in
  preference order:
  1. decode catcache invals for the pg_type syscaches and resolve hash
     keys → oids via a shadow-side reverse probe (catcache messages carry
     hash values, not oids — needs a `pg_type` scan per hit)
  2. maintain a type-oid → dependent-rel reverse index from captured
     descriptors; a pg_type write with a hit forces recapture of the
     dependents
  3. add pg_type to the capture-all trigger gated on "no relcache invals
     in the same commit" (heuristic: renames travel alone; CREATE TABLE
     always carries its rel's inval)
- **Rename events.** `compute_schema_diff` is attribute-based; a
  namespace or relation rename recaptures fresh descriptors (routing
  stays correct) but emits no `Changed`, so CH-side artifacts keep the old
  name until an operator intervenes. Surfacing renames as first-class
  events needs a diff over `rel_name` plus an applicator strategy
  (CH `RENAME TABLE` vs create-and-cutover).
- **Toast-spool retire on rotation.** A `Retired` entry (filenode
  rotation) does not feed the toast retire ledger; only relation-level
  `Dropped` does. A rewritten toast heap's spooled chunks for the old
  generation linger until the owner drops. Wiring `Retired`-on-toast into
  the ledger closes the leak; needs the same floor-gated deferral as
  `Dropped` retires.

## Full-rfn keying

Cross-tablespace identity is a separate concern:
[TABLESPACES.md](TABLESPACES.md) §0 owns the `(db, rel)` keying decision
and §desc-log seam records what flipping the descriptor-log key to a full
`RelFileNode` would require.
