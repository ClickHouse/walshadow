# Handle schema changes

walshadow observes committed PostgreSQL schema changes and orders ClickHouse
changes after earlier rows and before later rows

## Behavior matrix

| PostgreSQL change | ClickHouse behavior |
|---|---|
| `CREATE TABLE` | creates destination when table matches active scope |
| `ADD COLUMN` | adds mapped column automatically |
| `ADD COLUMN ... DEFAULT ...` | adds column and carries supported default |
| `RENAME COLUMN` | renames destination column unless config pins another target name |
| `DROP COLUMN` | drops automatically mapped destination column, custom target mappings can need cleanup |
| column type change | logs warning and requires manual migration |
| `TRUNCATE` | truncates destination table in source order |
| `DROP TABLE` | retains, warns, or drops destination based on policy |
| `VACUUM FULL`, `CLUSTER`, rewriting `ALTER` | continues with rewritten source relation |

## Configure source table drops

Destination retention is default

```toml
[ch]
drop_table_strategy = "retain"
```

Accepted values:

- `retain`, keep destination silently
- `warn`, keep destination and emit warning
- `drop`, remove destination with `DROP TABLE IF EXISTS`

Override per source schema:

```toml
[namespace.staging]
drop_table_strategy = "drop"
```

Choose `drop` only when source owns destination lifecycle. ClickHouse
dependencies and downstream consumers remain operator responsibility

## Change a column type

Source type changes never mutate ClickHouse type automatically. Coordinate a
manual migration:

1. Pause walshadow
2. Wait until `drain` and `emitter_ack` converge in `ctl status`
3. Alter or replace ClickHouse destination column
4. Add matching column type override when automatic mapping differs
5. Resume walshadow

```bash
walshadow-stream ctl pause
walshadow-stream ctl status
walshadow-stream ctl resume
```

Use staging table and swap when ClickHouse cannot alter existing type safely

## Pinned projections

Name-based column rules preserve automatic evolution. `attnum` rules pin full
projection, so newly added source columns stay excluded until config changes

Prefer name-based rules unless stable fixed projection is required
