# greenfield_oracle — a bridge for tier-3 during bootstrap

## Problem

Greenfield bootstrap ([../bootstrap.md](../bootstrap.md)) page-walks a
base/object-store backup and drains rows through `pipeline::bootstrap::drain`
**before the shadow PG and its bridge exist** (bridge is created after
`run_bootstrap` returns). So bridge-routed tier-3 values — jsonb, arrays,
hstore, tsvector, ranges, domains — have no PG to render them and land empty.
In-tree types (geography, vector; see [../oracle.md](../oracle.md)) are fine.

`BackupSource` is a page reader, not a running PG, so there is nothing to
decode on-disk tier-3 Datums during the drain.

## Approach

Stand up a **throwaway Postgres with the walshadow module + the source's
extensions**, point the bridge/oracle at it for the bootstrap drain, then tear
it down once bootstrap completes. `resolve_decoded_heap` already takes an
`Option<&Oracle>`; wire this oracle in for the greenfield drain and the whole
tier-3 path resolves exactly like live.

Lifecycle: create → (extensions/module ready) → pass `Some(oracle)` to
`bootstrap::drain` → drain → drop the temp PG + its datadir/socket before the
real shadow is materialized for streaming.

## The OID-matching constraint (the hard part)

`ws_decode_datum_text` renders a Datum by running the type's `typoutput`,
looked up by **OID**. Built-in tier-3 OIDs are stable across clusters (jsonb
3802, `int4[]` 1007, …) so a fresh `initdb` + `CREATE EXTENSION` handles them.
But **extension type OIDs are assigned at `CREATE EXTENSION` time and differ
per cluster** — a fresh temp PG's `hstore`/`geography`/`vector` OID won't match
the source OID carried in the on-disk bytes, so typoutput lookup misfires.

Two ways to satisfy it:

- **Restore the temp PG from the backup** (it then carries the source catalog,
  OIDs match) — essentially a short-lived shadow. Reuses the base-backup we
  already fetched; heaviest but exact. Overlaps with the Option-A framing in
  the earlier analysis.
- **Resolve by type name, not OID** — extend the bridge `DECODE` protocol to
  carry the type name; the worker looks up `typoutput` via
  `regtype`/`pg_type.typname` in the temp PG (which has the same-named
  extensions installed). Lets a plain `initdb` + extensions work regardless of
  OID drift. Smaller PG, but a protocol + worker change.

## Open questions

- Which extensions to install: derive from the source catalog (types actually
  present) vs a fixed set; fail-soft when one isn't available.
- Cost/timing: temp-PG spin-up vs bootstrap duration; only worth it when tier-3
  columns exist in the mapped set.
- Interaction with restart/resume: bootstrap re-runs must recreate/tear down
  the temp PG idempotently.
- Does not change the emitter contract; it only makes `Some(oracle)` available
  earlier. Orthogonal to [oracle_native_blocks.md](oracle_native_blocks.md).
