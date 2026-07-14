# runtime config from source PG

Source-PG-driven config that builds on the resolver substrate
([../config.md](../config.md)): config rows a DBA writes into `<schema>.config_*`
on source PG, seeded at boot and applied live at each row's commit LSN, merged
**CLI > PG-row > TOML**, detected inline by resolved qualified name, interpreted
into `ConfigEvent`s, applied through `DrainEntry::Config` under the barrier fence.

Implemented baseline lives in [config.md](../config.md) and
[add_table.md](../add_table.md): boot seed, commit-ordered config rows,
per-table opt-in, `initial_load`, and column overrides. This document keeps
only remaining extensions:

- a **signal channel** for imperatives that don't fit a stored row (`flush_now`,
  pause/resume, xact-scoped `ignore-transaction`, `force_reseed`,
  `drop_slot_at_lsn`)
- **net-new knobs** with no runtime path (`engine`, `order_by`, `exclude`,
  `ch_settings`, `sample_rate`, and the TOML-only `signal_prefix` /
  `admin_database`)
- **degraded-mode fallback** when the WAL pump is blocked and overlay freshness
  can't be guaranteed
- **observability** for the layered resolver: per-resolved-key metric with a
  `source` label

## Signal channel via `pg_logical_emit_message`

Orthogonal to config-row state: imperatives that don't make sense to store as a
row (`flush_now`, `pause_emitter`, `resume_emitter`, `force_reseed <rfn>`,
`drop_slot_at_lsn <X>`, debug toggles). The WAL pump already classifies
`RmId::LogicalMsg = 21` records (see `classify`) but discards the body; this
parses the body in the same inline decode path the config-table interception
uses ([../config.md](../config.md)), filters on a configurable prefix (TOML
`[runtime_config] message_prefix`, default `walshadow`), and routes the payload
to a small command parser. Unknown commands log at WARN and increment
`walshadow_signal_unknown_total{cmd=…}` — never crash.

**Signal source scoping.** `xl_logical_message` carries `dbId`, the emitting
session's `MyDatabaseId` stamped by `LogLogicalMessage`, not settable from SQL.
Physical replication delivers messages from every database in the cluster, so
the parser filters on `dbId` before prefix. Global imperatives (`flush_now`,
`pause_emitter`, `drop_slot_at_lsn`, `force_reseed`) accept only from the
database named by TOML `[runtime_config] admin_database`, resolved to a database
OID at attach time by name lookup against `pg_database`, empty meaning the source
database. Point `admin_database` at a locked-down database and the signal channel
inherits PG's database CONNECT privilege as its gate:
`REVOKE CONNECT ON DATABASE <admin_database> FROM PUBLIC` plus GRANT to
operators, enforced daemon-side — a role holding `EXECUTE` on
`pg_logical_emit_message` in an app database emits under that database's `dbId`
and gets dropped. Xact-scoped signals (`ignore-transaction`) are the exception:
they must ride the source xact carrying the DML so their `dbId` is always the
source database and their blast radius is self-scoped. Reading `dbId` needs the
body parse the classifier discards; `dbId` precedes prefix in the record so the
filter is cheap.

`pg_logical_emit_message(transactional bool, prefix text, content text)`
semantics honored: transactional messages drain at commit LSN through the same
`XactBuffer` ordering as heap rows; non-transactional messages drain on receipt,
used for "do it now" signals where ordering against in-flight xacts doesn't
matter. Parser splits the payload on whitespace, shell-style: first token is the
command, remaining tokens are positional args (`force_reseed 16384`,
`drop_slot_at_lsn 0/1A2B3C`). No JSON, no nesting; keeps signals greppable in
logs and typeable by hand at the SQL prompt.

Why messages, not config rows: stored-state config is the wrong fit for "do
once" commands. A `flush_now` row would imply persistent state; toggling it back
and forth in a single transaction would be incoherent. Messages are
fire-and-forget at a defined LSN.

Transactional pause/resume ride a third `DrainEntry::Signal` variant alongside
`Catalog` / `Config`, interleaved by LSN and applied at the barrier.
An append-only `config_signal_log` audit table (lsn, prefix, payload, outcome),
written by a `<schema>.emit_signal` PL/pgSQL helper that inserts the audit row
and calls `pg_logical_emit_message` in the same xact, gives operators a greppable
record sharing the signal's LSN. The daemon never writes it.

### Xact-scoped drop: `ignore-transaction`

A distinct signal class, neither "act at the message LSN" (flush/pause) nor a
stored config row: a per-xact tag consumed at that xact's commit drain. Use case:
delete rows or drop partitions on source while keeping them on CH. Wrap the
destructive statements in a transaction that also emits the tag; walshadow
discards the whole xact's CH-bound changes, cursor still advances.

```sql
SELECT pg_logical_emit_message(true, 'walshadow', 'ignore-transaction');
```

Must be transactional. PG forces xid allocation only for transactional messages
(`LogicalMessageInsert`, PG `src/backend/replication/logical/message.c`), so a
non-transactional message carries no xid to key the drop on and delivers
regardless of the xact's fate. Transactional rides the same xact; PG stamps the
record's `xact_id` with the emitting (sub)xid. Payload is the single command
token `ignore-transaction`, no args.

**Mechanism reuses abort.** Every xact already buffers per-xid in `XactBuffer`;
commit drains to CH, abort discards. `ignore-transaction` is "take the abort path
at commit, but still advance the cursor". Pieces:

- **Decode.** Parse `xl_logical_message` in the buffering decoder sink (which
  already sees every record before the reorder step, so the poison flag lands
  before the same worker's commit processing). Header size via the
  offsetof-equivalent (`SizeOfLogicalMessage` includes the pad after
  `bool transactional`; field-sum under-shoots), then `message` = prefix ++
  payload. Accept on transactional + prefix match + `dbId == source db`. Pure
  byte parse, no catalog lock, no replay gate
- **Poison flag on `XactState`, not a side set.** `XactBuffer::mark_ignore(xid)`
  lazily inserts the state and flips `ignore`. Storing on the state buys
  subxact/savepoint semantics for free: a message in a rolled-back subxact drops
  with that subxact's `abort` (matches PG never delivering it); a message in top
  or a committed subxact rides into the states collected at commit
- **Drop at commit.** Commit drain, after collecting states, if any `ignore`:
  unlink spill, discard heaps + drain events, return no `DrainedBatch` at
  `commit_lsn`. Empty-commit branch registers a
  rows=0 seq so the contiguous ack watermark passes `commit_lsn`, slot recycles,
  nothing reaches CH

**Shadow untouched, only CH suppressed.** Catalog/DDL records replay on shadow
independently of the xact buffer (`Route::ToShadow`), so shadow stays
schema-consistent even for the ignored xact — required because the decoder needs
shadow's post-DDL catalog for later xacts. Heap tuples and the CH DDL applicator's
`SchemaEvent`s both live in the same `XactState`, so dropping the state drops
both: a `DROP PARTITION` updates shadow's catalog but issues no DROP/ALTER against
CH. Dropped DELETEs leave rows on CH at their last `_lsn`, consistent with the
ReplacingMergeTree convergence model.

**Replay-safe without dedup.** Effect scoped to one xact, so restart + WAL replay
from before the commit re-sees the message inside the same xact and re-poisons
identically. No last-signal-LSN checkpoint (contrast `drop_slot_at_lsn` under
open questions); keying on the xact rather than an LSN is the reason.

**Not a `DrainEntry`.** Doesn't ride the transactional `Signal` variant that
pause/resume use; it mutates buffer state the commit drain already reads.
Ordering within the xact is irrelevant — the flag is read at commit, not applied
at the message LSN — so emit it first or last.

**Blast radius self-scoped.** A caller can only drop replication of the xact it
writes. Natural generalization: `ignore-relation <oid>` /
`ignore-changes-for <qname>` filters only some rels out of the drained set,
surgical when the xact also carries changes to keep.

## Implemented baseline dependency

Per-table `replicate`, forward declarations, `initial_load` modes, crash-safe
backfill ledgers, and convergence rules are current behavior. See
[config.md](../config.md) and [add_table.md](../add_table.md). Extensions below
must preserve inclusion-agnostic buffering and `_lsn` convergence guarantees
documented there

## Net-new knobs

Knobs with no runtime path — each needs a TOML/overlay field plus the machinery
behind it, distinct from the knobs the resolver resolves ([../config.md](../config.md)):

| key | table | type | notes |
|---|---|---|---|
| `engine` / `order_by` | table, namespace | text | fixed in `ch_ddl` (engine hardcoded `ReplacingMergeTree`, order_by derived from PK/replica-identity index); shape change, needs rfn drain + `TablePlan` rebuild |
| `exclude` | column | bool | `ColumnMapping` has no such field; drops a column from projection + future DDL; shape change |
| `ch_settings` | global, namespace, table | jsonb | applied to INSERT/CREATE TABLE, merged narrow-wins |
| `sample_rate` | (TOML only) | float | emitter row-drop sampling for debug, distinct from the `--validate` oracle sampler in `src/oracle.rs` |
| `signal_prefix` | (TOML only) | text | which `pg_logical_emit_message` prefix to scan |
| `admin_database` | (TOML only) | text | which database's signals the daemon honors for global imperatives; empty = source db |

Shape-changing keys (`engine`, `order_by`, `exclude`) reuse the
`invalidation_epoch` bump inside the barrier fence — one fake-invalidate per
config event mentioning a relation, so the gate flushes in-flight rows then
rebuilds `TablePlan`. Non-shape keys take effect on the next subscriber-side
snapshot read. Reroutes that can't be done safely mid-stream (`target` rename on a
streaming rfn) are rejected at merge with an explanatory metric.

## Degraded-mode TOML fallback

WAL pump blocked, config rows unreachable (pre-flight failing, slot dropped). The
overlay can't be kept fresh, so the resolver should fall back to TOML + CLI only.
Resolver tracks `config_freshness_lsn`; if
`now() - last_resolver_apply > config_staleness_max` (default 5min), an alarm
fires and the resolver flags itself degraded. New SIGHUPs still apply the TOML
layer; the overlay freezes at last-known state, not zeroed. Pump recovery
re-applies the overlay as WAL replay catches up. Hard guarantee: TOML configures
the connection to source/shadow and everything else when the overlay isn't fresh;
the overlay is strictly additive on top of working TOML.

## Observability

Per-resolved-key Prom metric carrying a source label
(`source="cli|wal|toml"`) so an operator can answer "why is `auto_create` false
for `public`?". Natural follow-up: a `walshadow-stream config explain <key>`
subcommand walking the three layers.

## Anti-goals

- **Overlay enable + schema name + signal prefix + admin database stay TOML.**
  Without these, no overlay rels exist to read, and the admin-database gate can't
  bootstrap from a value the untrusted signal channel supplies. Putting any of
  them in the overlay is a chicken-and-egg
- **No config replication to CH.** Config-table writes are dropped before routing
  by the implicit namespace filter; not overrideable
- **No two-way sync.** The daemon never writes `<schema>.config_*`. Source PG is
  the single writer, the daemon the single reader
- **DDL on `<schema>.config_*` is DBA-run.** The schema grows additively (NULL =
  daemon default), so a newer daemon reads an older install without a version
  handshake; no daemon-side migration

## Open questions

- **Conflicting writes during failover.** Source PG fails over to a replica with
  stale config; the daemon following the new primary sees old config and rolls
  back. Mitigation: resolver records the LSN of every apply; a row with an LSN
  lower than `config_freshness_lsn` is logged and ignored. Real fix is the
  operator `pg_dump`ing `<schema>.config_*` from old primary to new before
  failover — the daemon doesn't try to outsmart the DBA
- **Schema evolution of config tables.** The additive-NULL rule (a new daemon
  column absent from an old install reads as the daemon default) covers
  forward-compat, but there is no gate for the reverse — an install newer than the
  daemon that adds a column the daemon can't interpret is silently ignored. A
  compatibility gate, if needed, is a separate mechanism
- **High-throughput config writes.** An ops script writing 10k `config_column`
  rows in one xact merges per event at the drain. The in-memory merge is µs per
  event, but if a single drain processes more than N config events, batch the
  resolver republish so subscribers see one merged snapshot
- **Source privileges.** Config install needs `CREATE` on the database and
  ownership of the config tables, no superuser. A deployment lacking those sets
  `[runtime_config] schema = ""` for TOML-only. Document in the deployment guide
- **Signal abuse.** `pg_logical_emit_message` has no in-backend privilege check
  and ships with default EXECUTE to PUBLIC (PG 18.4 `logicalfuncs.c`), so the real
  bar is "any role that can connect". Primary gate is daemon-side `dbId` scoping:
  point `admin_database` at a database with `REVOKE CONNECT … FROM PUBLIC`, and the
  global imperatives become reachable only by roles that can connect there,
  unspoofable since `dbId` is stamped from `MyDatabaseId`. Secondary, source-side:
  `REVOKE EXECUTE ON FUNCTION pg_logical_emit_message(...) FROM PUBLIC` plus GRANT
  to a signaler role. Payload signing stays optional defense-in-depth
- **Signal replay.** A signal at LSN N processed once, then the daemon restarts and
  replays WAL from < N. Idempotent commands (flush, pause, resume) tolerate replay;
  one-shot commands (`drop_slot_at_lsn`) need dedup. Mitigation: persist the
  last-processed signal LSN alongside the emitter checkpoint; skip signals at
  LSN ≤ checkpoint on replay
- **Backfill vs DDL.** Backfill of `app.orders` in progress; source runs
  `ALTER TABLE app.orders DROP COLUMN notes` mid-COPY. Additive DDL is tolerated
  (COPY omits the new column, post-DDL WAL rows converge it). Destructive or
  type-changing DDL makes COPY's projection reference a column CH no longer has —
  restart COPY against the post-DDL shape at a new `S'`, cheap since re-COPY needs
  no snapshot; the catalog applicator signals the backfiller on a shape-changing
  `SchemaEvent` for a backfilling rfn
- **Forward-decl pollution.** An operator inserts `config_table` rows for typo'd
  qualnames (`app.ordres`). Pending forever, harmless but noisy. Mitigation: TTL on
  `pending_decl` entries (default 30 days), tick a metric, log at WARN on expiry

## Acceptance drills

- **Signal: flush_now.** Source runs
  `pg_logical_emit_message(false, 'walshadow', 'flush_now')` during an idle period.
  Emitter flushes within one decode-tick, no xact required. Non-transactional path
- **Signal: transactional pause.**
  `pg_logical_emit_message(true, 'walshadow', 'pause_emitter')` followed by rows in
  the same xact. Rows commit on source; the daemon pauses at the message LSN, never
  emits the trailing rows until `resume_emitter` arrives
- **Signal: ignore-transaction.** Source runs a DELETE (or DROP PARTITION) plus
  `pg_logical_emit_message(true, 'walshadow', 'ignore-transaction')` in one xact.
  The xact commits on source, shadow replays any catalog change, CH receives
  nothing, `emitter_ack_lsn` still advances past `commit_lsn`. Variant: the tag in
  a rolled-back savepoint leaves the surrounding xact replicating normally
- **Column exclude.** Set `config_column.exclude = true` for `public.orders.notes`.
  Subsequent rows arrive on CH without the column; re-clearing restores emission,
  projection rebuilds after the in-flight rfn drain
- **CH settings passthrough.** Set `config_table.ch_settings = '{"max_insert_threads":4}'`
  for one table. Inserts for that table carry the SETTINGS clause; other tables
  unaffected; global default merges with table-scoped under narrow-wins
- **Degraded fallback.** Source has `auto_create = true` rows. WAL pump artificially
  blocked (drop publication mid-run). Resolver flips to degraded after
  `config_staleness_max`. Operator SIGHUPs TOML flipping `auto_create = false`;
  subsequent CREATE TABLE no longer mirrors. Pump unblock restores the overlay; new
  CREATE mirrors again under the WAL-driven setting
