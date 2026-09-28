# Handle unrepresentable values

Reject values outside destination domains before silent loss occurs. Preserve
representable values and apply configured substitutes consistently across row
encoding and fast defaults. Keep correctness checks independent of new policy
options

## Remaining work

1. Validate finite date/timestamp values against supported destination calendar
   ranges, accounting for precision and timezone. Choose and document supported
   server ranges before implementation; integer storage bounds are insufficient
2. Check finite Decimal values against declared precision, not just wire width.
   Reject discarded nonzero fractional digits; permit removal of trailing zeros.
   Validate signed/unsigned domains before accepting integer overrides
3. Apply configured non-finite substitutes in
   [fast-default rendering](../src/catalog/type_bridge.rs). Share validation with
   [row encoding](../src/emit/ch_emitter.rs) so SQL literals and wire values agree.
   Reject unsupported policies before destination DDL or load begins, following
   [schema plan](schema.md)
4. Distinguish real source NULL from absent delete payload. Reject source NULL
   into non-nullable columns while preserving required tombstone defaults
5. Count substitutions by relation and reason. Include column and effective
   policy in errors without logging raw values. Keep default substitutions
   separate from row counts; document possible recounting on retries

Apply checks across streaming, COPY, heap-page, and backup loads. Document
changes from implicit truncation or default substitution in
[table selection](../docs/table-selection.md)

## Proposed scope cuts

Prefer rejection over expanding substitution policy until concrete workloads
justify additional behavior

| Proposal to cut or defer | Reason and smaller alternative |
|---|---|
| Finite `below_min` / `above_max` replacement actions | Adds policy combinations across every destination type; reject overflow and require a wider mapping |
| Rounding modes and `precision_loss` replacements | Requires tie, sign, and post-rounding overflow semantics across encoders; accept exact rescaling only |
| Temporal `min`, `max`, and literal substitutes | Couples replacement validation to calendar, timezone, and server ranges; retain reject/null choices |
| Capability negotiation and reconnect validation for temporal ranges | Adds lifecycle state for wider date support; start with a documented conservative range shared by supported servers |
| Configurable `source_null` replacement | Expands a correctness fix into default/literal policy; reject real NULL and require a nullable mapping |
| Oracle policy transport and nested element substitution | Requires coordinated pgext changes, leaf nullability, paths, and counting; reject unsupported policies before loading |
| Array shape and lower-bound preservation | Separate structural mapping problem; leave to [container work](tier2.md) |
| Per-row substitution provenance | Requires destination schema changes; keep aggregate counts and diagnostics |

Keep malformed values, missing TOAST data, codec failures, and transport errors
outside substitution policy. Do not add catch-all `on_error` or substitute whole
containers for unsupported elements

## Acceptance

- Preserve finite calendar boundaries; reject adjacent out-of-range values
- Reject Decimal values exceeding declared precision even when wire width fits
- Accept `1.230` at scale 2; reject `1.234`, including negative equivalents
- Reject negative integers mapped to unsigned destinations
- Render configured substitutes for non-finite `ADD COLUMN` defaults; reject
  unsupported policies before destination effects
- Verify defaults and row encoding agree on epoch, precision, quoting, and
  nullability across supported paths
- Reject real NULL into non-nullable destinations without changing tombstones
- Count substitutions without counting unchanged values or exposing raw data
