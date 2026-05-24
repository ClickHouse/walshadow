# robot:oracle

## sources of truth

- plans/oracle.md
- pgext/ directory (walshadow PG extension)
- src/ oracle path module (find via `ls src/ | grep -iE 'oracle|valid|pending'`)

## subsumes

plans/oracle.md § differential decode flow + extension surface + --validate sampling

## concept

decoder produces ColumnValue; PgPending always routes to oracle, Tier 1/2/3 sampled at --validate N. oracle wraps raw varlena and issues libpq SELECT against shadow PG: with extension `walshadow_decode_disk($1::oid, $2::bytea)::text`, without extension fallback `$1::bytea::TYPE::text`. result is emit-time text for PgPending; for sampling, compared against in-tree decode → mismatch alarm

## clusters

| id | label | purpose |
|---|---|---|
| trigger | trigger sources | PgPending always + Tier 1/2/3 sampled |
| select | shadow SELECT round-trip | wrap raw + dispatch by extension availability |
| ext | pgext detection | walshadow extension loaded? |
| compare | --validate path | re-encode + diff |
| emit | result | text → emitter / mismatch counter |

## key nodes

- pending_in: "← decoder\nPgPending {oid, raw}" — #4D4128, shape=note
- sampled_in: "← decoder\nTier 1/2/3 row\nsampled at --validate N" — #4D4128, shape=note
- enqueue: "oracle queue\nbatch per shadow round-trip" — #4D4D28
- has_ext: "shadow has\nwalshadow_decode_disk?" — #4D4D28
- sel_ext: "SELECT walshadow_decode_disk(\n  $1::oid, $2::bytea\n)::text" — #4D4D28
- sel_fb: "SELECT $1::bytea::TYPE::text\n(text fallback)" — #4D4D28
- shd: "shadow PG\n(libpq client)" — #3D4128, cylinder
- pgext: "pgext/\nshared_preload_libraries\n(must be loaded)" — #4D4D28, shape=note
- result: "result text" — #4D4D28, parallelogram
- pending_out: "→ emitter\n(text emit-time value)" — #5D4628, shape=note
- compare: "re-encode +\ncompare in-tree decode" — #4D4D28
- ok: "match\nvalidate_ok ++" — #4D4D28
- mismatch: "mismatch alarm\nvalidate_fail ++\nlog diff" — #4D4D28
- fb_pending: "PgPending fallback\n<oid:N> placeholder\nunsupported_values ++" — #4D4D28
- cli: "CLI --validate N\nsample rate gate" — #4D3A28, shape=note

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| pending_in | enqueue | default | solid | always |
| cli | sampled_in | default | dashed, constraint=false | gate |
| sampled_in | enqueue | default | solid | sampled |
| enqueue | has_ext | default | solid | dispatch |
| has_ext | sel_ext | default | solid | extension present |
| has_ext | sel_fb | default | solid | no extension |
| has_ext | fb_pending | default | dashed | no extension + PgPending |
| sel_ext | shd | #CBA85E, penwidth=2 | solid | libpq |
| sel_fb | shd | #CBA85E | solid | libpq |
| shd | result | #CBA85E | dashed | rows |
| pgext | shd | default | dotted, constraint=false | loaded into |
| result | pending_out | default | solid | PgPending: text → emit |
| result | compare | default | solid | sampled: compare |
| compare | ok | default | solid | match |
| compare | mismatch | default | solid | diff |
| fb_pending | pending_out | default | dashed | placeholder |

## legend rows

- node-fill key (ShadowCatalog/oracle, shadow PG, emitter exit, CLI input)
- edge-color key (libpq query, control flow)
- trigger sources subtable:
  - PgPending → always
  - Tier 1/2/3 → only when --validate N samples this row
- fallback subtable:
  - extension present → walshadow_decode_disk(oid, bytea)
  - extension absent + sampled → $1::bytea::TYPE::text (regtype lookup needed)
  - extension absent + PgPending → emit `<oid:N>` + unsupported_values bump

## layout hints

- rankdir=LR
- has_ext branches diverge cleanly; sel_ext and sel_fb both terminate at shadow
- compare side path off result, doesn't deform main pending flow

## quality bar

- two-way branch on extension availability legible (not a tangle)
- libpq round-trip reads as one canonical orange edge in/out of shadow
- fallback path visually distinct from main path
