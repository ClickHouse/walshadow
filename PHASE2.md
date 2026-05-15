# PHASE2 — PG-16-minimum cleanup

Closes Phase 2 of `PLAN.md`. Codifies the "Supported PostgreSQL
versions" banner in code: walshadow rejects PG ≤ 14 captures at the
segment walker, defaults fixture capture to `postgres:16`, and
renames the FPI-layout constant in `wire.rs` to reflect its new role
as the minimum accepted page magic.

## What landed

| item | files | tests |
|---|---|---|
| `XLP_PAGE_MAGIC_PG15` → `XLP_PAGE_MAGIC_MIN` (value unchanged) | `src/wire.rs` | downstream tests adopt new name |
| `WalkError::UnsupportedSourceVersion` + magic-floor check | `src/segment.rs` | `rejects_pre_pg15_magic` |
| Capture-script image default: `postgres:14` → `postgres:16` | `fixtures/wal/{classify,filter}/capture.sh` | bash -n parse |
| Local PG version gate (`WALSHADOW_USE_LOCAL=1` requires PG ≥ 15) | `fixtures/wal/{classify,filter}/capture.sh` | bash -n parse |
| FPI workaround comment removed from classify `capture.sh` | `fixtures/wal/classify/capture.sh` | n/a |
| PLAN.md: Supported-versions banner footnote + Phase 2 rewrite | `PLAN.md` | n/a |

## Why PG 15 stays in the accept set

PLAN.md's policy floor is PG 16 (EOL window, RelFileLocator naming
stabilisation). The *technical* floor is PG 15: that's where the FPI
bit shuffle (`bimg_info` 0x02 = IS_COMPRESSED → APPLY,
`a14354cac`) happened. wal-rs's parser dispatches FPI-bit semantics
off `magic >= 0xD110`, so anything from PG 15 onward parses with the
new layout. Adding a PG-16-specific reject would require a second
constant (`0xD113`) and would refuse captures that re-parse correctly
through the existing code path — strictly worse than the policy/code
split. Reject what we can't parse (PG ≤ 14), tolerate what we can.

## Design decisions

### One constant, not two

PLAN.md's earlier Phase 2 sketch proposed adding `XLP_PAGE_MAGIC_PG16
= 0xD113` alongside `XLP_PAGE_MAGIC_MIN = 0xD110`. Phase 2 ships only
the latter: a single "minimum magic walshadow accepts" sentinel,
doc-commented to explain both its FPI-layout meaning and its
supported-version implication. A separate `_PG16` constant would have
no caller — the supported-version banner is policy, not a wire-level
predicate.

### Reject at the walker, not at the daemon

PLAN.md pitfall #7 says "daemon refuses to start on … source-PG < 16".
That gate belongs in Phase 7 (operational) when the daemon binary
exists; Phase 2 plants the same check one layer down, at the segment
walker, where it covers fixture capture + CLI + integration tests
already. When Phase 7 lands, the daemon's startup check can either
delegate to the walker's error (PG < 15) or layer a stricter policy
check on top (refuse PG 15 too, per banner). Either way no rework.

### Error variant, not panic

`UnsupportedSourceVersion(offset, magic)` carries the exact bad magic
so an operator can identify the source PG major from a single line of
log output. Same posture as `BadPageMagic` for non-WAL data.

## Deviations from PLAN.md Phase 2

* Phase 2 description in PLAN.md was rewritten to match what landed
  (single `_MIN` constant, no `_PG16`, PG 15 tolerated). The earlier
  "rename + add `_PG16`" sketch would have introduced a dead
  constant.
* Skipped editing PHASE0.md and PHASE1.md — keeping completed phase
  retrospectives immutable (user direction during Phase 2).
* No upstream wal-rs changes in this phase. The clean-up listed in
  PLAN.md (drop `BKP_IMAGE_IS_COMPRESSED_PG14`, collapse
  `is_compressed(page_magic)`) is tracked separately. walshadow
  works regardless of which side of that change wal-rs is on.

## What didn't get done

* No daemon-side version refusal. Phase 7 territory — no daemon
  binary exists yet.
* No re-capture of fixtures against `postgres:16`. The default
  changed, but the existing local-PG-18 captures still satisfy the
  new floor (0xD118 > 0xD110), so the round-trip tests pass without
  re-capture. A fresh capture against `postgres:16` is a sanity check
  for the next operator who runs `capture.sh`.

## Test counts

* `cargo test --lib`: 38 passed (was 37; +1 = `rejects_pre_pg15_magic`).
* `cargo test --tests`: 5 passed (2 classify fixture + 3 filter
  round-trip; unchanged).
* `cargo clippy --all-targets -- -D warnings`: clean.
* `bash -n` on both `capture.sh` files: clean.

Total: 43 passing.

## Files touched

```
walshadow/src/wire.rs                                   PG15 → MIN rename + expanded doc-comment
walshadow/src/segment.rs                                +UnsupportedSourceVersion variant + floor check + test
walshadow/src/rewrite.rs                                test constant rename
walshadow/src/filter_segment.rs                         test constant rename
walshadow/fixtures/wal/classify/capture.sh              default postgres:16, local PG ≥ 15 gate, drop FPI workaround comment
walshadow/fixtures/wal/filter/capture.sh                default postgres:16, local PG ≥ 15 gate
walshadow/PLAN.md                                       Supported-versions footnote + Phase 2 rewrite
walshadow/PHASE2.md                                     new (this doc)
```

LOC delta in `src/`: +14 lines (mostly the new error variant + magic-floor
check + one synthetic test). Effectively a rename plus one guard.
