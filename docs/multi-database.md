# Replicate several databases in one cluster

PostgreSQL writes one WAL stream for all databases in a cluster, and walshadow
reads that stream once. One process replicates every database its config names:
one replication slot, one shadow PostgreSQL, one decode of each WAL byte

## Name the databases in config

`[table.<schema>.<relname>]` names a table in the database `dbname` selects.
Nest entries under `[database.<dbname>]` to name a table in another database:

```toml
[source]
dbname = "app"

[table.public.orders]                        # app.public.orders
replicate = true

[database.billing.table.public.ledger]       # billing.public.ledger
replicate = true
```

The replicated set is `dbname` plus every database `[database.*]` names.
Nothing else declares it, so a database with no entry of its own is not
replicated even under `replicate_all`

`[database.<dbname>.namespace.<schema>]` does the same for
`[namespace.<schema>]`

Entries outside `[database.*]` belong to `dbname`, so the same table cannot be
configured twice, once in each place. A database and a schema can share a name,
the key level decides which one a name means

Startup checks reject database names missing from `pg_database`, so a typo
causes an error instead of replicating a database nobody asked for

## Use separate destinations

Tables with matching names in different source databases use the same
ClickHouse table by default. Parsing rejects this conflict:

```text
`database.billing.table.public.orders` and `table.public.orders` both write to
cdc.orders, set a different target_database or target_table
```

Set separate destinations for each database, schema, or table:

```toml
[database.billing.namespace.public]
target_database = "billing_cdc"
```

Tables with matching names in different schemas need the same treatment, see
[current limitations](limitations.md). Config checks cover explicit entries;
destinations `replicate_all` derives are claimed as each table is first seen,
and a second claim on one destination is refused

## Run `ctl` against one database

`ctl status` lists every replicated database with its selected tables. Commands
that read or edit one database's scope take `--database`, defaulting to
`dbname`:

```bash
walshadow-stream ctl add public ledger --database billing --initial-load copy
walshadow-stream ctl tables --database billing
```

`ctl remove`, `ctl schemas`, and `ctl columns` take the same flag. `ctl apply`
config changes carry the prefix in the key itself

## Account for the costs

- Every database gets its own shadow catalog connection and its own set of
  bridge workers on shadow: `[ch] inserter_pool_size` workers each, capped at
  8 per database and 64 in total. walshadow raises shadow's
  `max_worker_processes` to seat them
- Every database keeps its own table-shape history under the spill directory.
  The database `dbname` names keeps the spill root; the others get `db-<oid>`
  subdirectories
- `[memory] resident_payload_max` is one pool for the whole process, shared by
  every database's rows

## Config tables in source databases

`config_table` and `config_namespace` rows apply to tables in their own
database. Each database's rows are read over a connection to it, and rows have
no database column. Install `sql/runtime_config_install.sql` in every database
you replicate

## Current limits

- Adding or removing a database means restarting walshadow: shadow registers
  its bridge workers at startup. A reload reports the databases it could not
  follow rather than half-wiring them
- Existing rows load from a cluster backup only for the database `dbname`
  names, which is the database the backup walks. Config parsing rejects
  `initial_load = "base_backup"` and `"object_store"` under `[database.*]`;
  use `"copy"`, which reads the table over SQL after startup
- `replicate_all` names each destination after its source table, so two
  databases holding one table name claim one ClickHouse table. The first claim
  wins and the second logs an error and stays unreplicated. Name the tables to
  replicate, or give each database its own `target_database`

See [plans/multi_database.md](../plans/multi_database.md) for the load paths
and metric labels still to cover
