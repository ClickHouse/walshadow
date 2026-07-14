# plans/future — index

Planning-only docs for unbuilt work. Drop new proposals here as they
surface; promote into `plans/` once built

* [TABLESPACES.md](TABLESPACES.md) — source-tablespace correctness: bootstrap page-walk + shadow directory materialization
* [DESTINATIONS.md](DESTINATIONS.md) — N:M ClickHouse destination routing: fan-out/fan-in, per-dest ack accounting, slot-advance tension
* [runtime_config_from_pg.md](runtime_config_from_pg.md) — source-PG runtime config: signal channel, net-new knobs, degraded-mode fallback, resolver observability (resolver substrate + per-table opt-in + column overrides in [config.md](../config.md))
* [shadow_schema_export.md](shadow_schema_export.md) — shadow PG as schema-only catalog donor to third-party clusters
* [shadow_toast.md](shadow_toast.md) — shadow-backed TOAST chunk store with WAL replay and crash-safe reclamation fencing
* [sync_commit_witness.md](sync_commit_witness.md) — walshadow as RPO=0 durability standby
* [two_phase_commit.md](two_phase_commit.md) — `XLOG_XACT_PREPARE` handling and gxid-keyed buffer
* [xact_stash.md](xact_stash.md) — generic commit-time raw-record decode: ordinary-heap CREATE/TRUNCATE + INSERT via shadow publication fence + durable descriptor snapshots
* [ch_bounce_recovery.md](ch_bounce_recovery.md) — deeper re-emit-from-spill on retry-budget exhaustion
* [pinned_ddl_baseline.md](pinned_ddl_baseline.md) — schema-event outcome must be a function of config + baseline, not cache warmth: CH-existence / persisted-baseline options for cross-restart consistency, drop detection across downtime, opt-in mapping vs republish
* [coverage100.md](coverage100.md) — drive `cargo llvm-cov` line coverage toward 100%: tiered work list (pure units → fixtures → live e2e → hard tail)
* [FUZZ.md](FUZZ.md) — continuous coverage-guided fuzzing (cargo-fuzz/libFuzzer) across wal-rus + walshadow + clickhouse-c-rs: tiered targets, round-trip/differential oracles, C-boundary ASan, unattended-VM supervisor
* [pipeline_backpressure_and_scaling.md](pipeline_backpressure_and_scaling.md) — parallel decode+insert pipeline: WAL-pump backpressure via wire/record split, decode/insert scaling (bootstrap Option B, hot-table sharding, N/M sizing); pipeline substrate in [emitter.md](../emitter.md)
* [dependencies.md](dependencies.md) — crates.io replacement candidates for generic object storage, MPMC, retry, throttling, and metrics code
* [risks.md](risks.md) — measurement-deferred risks and open questions
* [parked.md](parked.md) — small operational polish + cross-major fixtures + skipped-test drive
