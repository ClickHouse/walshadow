# Switch PostgreSQL primaries

Use planned switchover only. walshadow verifies descendant timeline and resumes
without reloading ClickHouse when target has fully replayed frozen source head

walshadow does not elect leaders, fence old primary, promote PostgreSQL, or
move DNS

## Requirements

- target is streaming standby of current primary
- writes can stop before promotion
- target can replay through frozen source head
- target shares source system ID and belongs to same timeline lineage
- required WAL remains available
- when using slots, target already has physical slot covering resume position

Physical slots are not synchronized to standbys. Create target slot before
switchover with storage and WAL coverage appropriate for deployment

## Procedure

1. Pause walshadow

   ```bash
   walshadow-stream ctl pause
   walshadow-stream ctl status
   ```

   Record `pause_consumed_lsn` and `pause_received_lsn`

2. Stop writes on old primary, using application fencing or PostgreSQL fast shutdown

3. Repoint walshadow while target is still in recovery

   ```bash
   walshadow-stream ctl source \
       'postgres://replicator@target.internal/app?sslmode=require&slot=walshadow_target'
   ```

   Stable VIP, proxy, or DNS endpoint can skip explicit repoint

4. Wait for promotion gate

   ```bash
   walshadow-stream ctl status
   ```

   Proceed only when:

   ```toml
   promotion_ready = true
   target_in_recovery = true
   ```

   When false, `promotion_blocked_on` names missing condition. Do not promote
   until target replay and receive positions cover frozen source head

5. Promote target with normal PostgreSQL tooling

6. Resume walshadow

   ```bash
   walshadow-stream ctl resume
   ```

7. Watch `ctl status` until timeline fields advance and `crossing_blocked_on` clears

## Why order matters

Pause bounds ClickHouse output to WAL target already owns. Repointing before
promotion lets walshadow verify replay on exact server which will become
primary. Promotion gate prevents rows from abandoned source branch reaching
ClickHouse

Already accepted rows keep draining during pause, so ClickHouse or shadow
outage can delay readiness without losing source data

## Abort switchover

Before promotion, repoint to old primary if changed, then resume. No descendant
state has been committed yet

After promotion begins, do not use `--ignore-cursor` to force progress. Inspect
`crossing_blocked_on`, `crossing_detail`, source timeline, slot coverage, and
ClickHouse acknowledgement state

## Unsupported failover

Unplanned promotion while walshadow is consuming old primary can leave
transactions or rows beyond fork point. Current release fails closed instead
of compensating ClickHouse

Rebuild from known baseline or restore old planned path after unplanned
promotion. See [Current limits](limitations.md)
