# plans/future — index

Planning-only docs for work that has not shipped. Drop new proposals
here as they surface; promote into `plans/` when they land

* [TABLESPACES.md](TABLESPACES.md) — source-tablespace correctness: bootstrap page-walk + shadow directory materialization
* [DESTINATIONS.md](DESTINATIONS.md) — N:M ClickHouse destination routing: fan-out/fan-in, per-dest ack accounting, slot-advance tension
* [runtime_config_from_pg.md](runtime_config_from_pg.md) — WAL-driven runtime config overlay from source PG
* [shadow_schema_export.md](shadow_schema_export.md) — shadow PG as schema-only catalog donor to third-party clusters
* [sync_commit_witness.md](sync_commit_witness.md) — walshadow as RPO=0 durability standby
* [two_phase_commit.md](two_phase_commit.md) — `XLOG_XACT_PREPARE` handling and gxid-keyed buffer
* [ch_bounce_recovery.md](ch_bounce_recovery.md) — deeper re-emit-from-spill on retry-budget exhaustion
* [pinned_ddl_baseline.md](pinned_ddl_baseline.md) — schema-event outcome must be a function of config + baseline, not cache warmth; seed baseline at start, plus CH-existence / persisted-baseline options for cross-restart consistency
* [coverage100.md](coverage100.md) — drive `cargo llvm-cov` line coverage toward 100%: tiered work list (pure units → fixtures → live e2e → hard tail)
* [FUZZ.md](FUZZ.md) — continuous coverage-guided fuzzing (cargo-fuzz/libFuzzer) across wal-rus + walshadow + clickhouse-c-rs: tiered targets, round-trip/differential oracles, C-boundary ASan, unattended-VM supervisor
* [pipeline_backpressure_and_scaling.md](pipeline_backpressure_and_scaling.md) — parallel decode+insert pipeline design record (core landed, current-state doc is [emitter.md](../emitter.md)); remaining work: WAL-pump backpressure via wire/record split, decode/insert scaling (bootstrap Option B, hot-table sharding, N/M sizing)
* [dependencies.md](dependencies.md) — crates.io replacement candidates for generic object storage, MPMC, retry, throttling, and metrics code
* [risks.md](risks.md) — measurement-deferred risks and open questions
* [parked.md](parked.md) — small operational polish + cross-major fixtures + skipped-test drive
