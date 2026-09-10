# walshadow documentation

Start with path matching your environment:

- [Quickstart](quickstart.md) runs PostgreSQL, ClickHouse, and walshadow
  locally with Docker Compose
- [Getting started](getting-started.md) connects existing PostgreSQL and
  ClickHouse databases

Use remaining guides as needed:

- [Configuration](configuration.md), connection settings and live control
- [Table selection](table-selection.md), replication scope and initial load
- [Destination tables](destination-tables.md), generated ClickHouse schema and
  query patterns
- [Schema changes](schema-changes.md), supported DDL behavior
- [Operations](operations.md), monitoring, restarts, and WAL retention
- [Planned source switchover](failover.md), controlled PostgreSQL primary moves
- [Current limitations](limitations.md), unsupported behavior and safeguards
- [Development](development.md), builds, integration prerequisites, fixtures, and coverage

Read [architecture](../architecture/README.md) for system design and
[plans](../plans/INDEX.md) for unfinished work
