# Handle unrepresentable values

Reject values outside destination domains before silent loss occurs. Preserve
representable values and apply configured substitutes consistently across row
encoding and fast defaults. Keep correctness checks independent of new policy
options

## Recover from rejected values

At `1267db7`, PostgreSQL 18.6 / ClickHouse 26.8.1.951 WAL probes preserve
`repeat('x', n)` TEXT at 8 KiB, 32 MiB, and 128 MiB under defaults, with exact
lengths and MD5s. An explicit 64 MiB cap substitutes NULL for 128 MiB under
default overflow policy; `error` returns decoded size `134217728` and cap
`67108864`. `numeric(10,2)` NaN returns an unsupported-value error naming column
and available policies. These are pipeline outcomes, not daemon crash evidence

Test daemon exit and restart for those rejection cases with recorded config,
exit diagnostics, and durable positions. Keep incompressible values, other load
modes, and [recovery matrix](verification.md) separate from successful TEXT case

Keep rejection as default for non-finite Decimal values. PostgreSQL constrained
numeric accepts NaN; ClickHouse Decimal cannot represent it. Existing String
mapping and explicit `nan` substitutes provide policy choices. Do not silently
skip a row or acknowledge failed work to keep pipeline running

Report relation, column, reason, effective policy, and blocked WAL position
without raw payload. For size rejection include decoded bytes and configured
cap. Expose a recovery path that works while daemon is down: persist corrected
local config, then restart from retained WAL. Prove repeated restart without
correction blocks again, and correction delivers failed transaction plus later
work. A later source UPDATE or WAL-carried config edit cannot unblock earlier
rejected WAL by itself

Review default oversize substitution explicitly: NULL/type-default output is
lossy even if process stays alive. Surface effective policy and substitution
counts to operators; distinguish intentional substitution from full-value
success. Keep [large-value tests](verification.md#verify-value-fidelity-and-recovery)
independent of [TOAST reclamation](shadow_toast.md)

## Remaining work

Native insert-tail probes using
[existing harness](../tests/emitter_native_types.rs) reproduce these gaps:

| Input and destination | Observed result | Implementation idea |
|---|---|---|
| Numeric `1.234` into `Decimal(10,2)` | `1.23`, acknowledged | Reject nonzero division remainder during rescaling |
| Numeric `100000000.00` into `Decimal(10,2)` | `100000000`, acknowledged | Carry declared precision in `DecimalWire`; enforce scaled magnitude below `10^p` |
| `Int4(-1)` into `UInt32` | `4294967295`, acknowledged | Validate signedness and domain before copying fixed bytes |

These probes submit decoded cells directly; add source configuration/WAL tests
before claiming every override reaches this path. Wire-width checks already
reject larger physical overflows and do not replace declared-domain checks

Live fast-default probe also diverges: create keyed table, insert one row, then
`ADD COLUMN v numeric DEFAULT 'NaN'` and insert another row with `v = 1.25`.
Automatic `Nullable(String)` stores old row as `nan`, unlike source text `NaN`;
new finite row remains `1.25`. Render non-finite String defaults as quoted source
text before applying Decimal-specific rejection or substitutes. Current renderer
returns bare `nan` regardless of target

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
5. Extend existing aggregate `toast_values_filled_oversize_total` with relation
   and reason attribution and non-finite substitution counts. Include column
   and effective policy in errors without logging raw values. Keep default substitutions
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

- Replay Decimal NaN rejection after configuring a valid substitute; verify
  finite neighbors, failed row, and later transactions converge without skipping
  WAL. Treat remapping an existing Decimal column to String as a schema migration,
  not a config-only retry
- Preserve Float32/Float64 NaN, infinities, and both zero signs by comparing bits
  with source `float4send`/`float8send`; printed zero cannot establish sign fidelity
- Verify automatic numeric mappings at precision 76/77/1000, negative scale,
  scale greater than precision, and unconstrained numeric against source text
  Include trailing fractional zeros, SQL NULL, and String fast default `'NaN'`;
  compare decoded default semantics, not SQL spelling such as `unhex('4e614e')`
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
