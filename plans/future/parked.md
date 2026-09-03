# parked — small operational debt and follow-up polish

Operational debt collected from retros, one line per item. Items that fit
a subject doc live there instead: perf and profiling work in
[perf_regression.md](perf_regression.md), coverage and skip-gate hazards in
[coverage100.md](coverage100.md), decoder-fidelity risks in
[risks.md](risks.md), config knobs in
[runtime_config_from_pg.md](runtime_config_from_pg.md), DDL transition
corpus in [ddl_fuzz.md](ddl_fuzz.md)

## v1.0 operational polish

* **Deduplicate `ChServer` fixture across `tests/pipeline_e2e.rs` +
  tests/common.** Two callers using one vendored ChServer is fine;
  third caller lifts to shared. `bootstrap_ch_fixture`
  + `http_get` / `parse_metric` are shared
* **OnceCell shared CH-server fixture.** Five acceptance tests each
  spawn own CH (~5 s × N startup). Total CI cost ~25 s of unique
  boot time. Flag if test count doubles

## Walsender hardening

* **TLS / SCRAM auth.** Trust-over-loopback only today.
  Production multi-host deployments need this. Sized against
  wal-rus's auth machinery — the receive-side already speaks SCRAM,
  send-side mirrors
* **`hot_standby_feedback` (`'h'` frame).** Silently dropped today;
  documented behaviour. Long-running shadow queries that conflict
  with replay still hit `max_standby_streaming_delay`
* **Walsender keepalive-timeout unit test.** Indirectly covered by
  libpq + PG-walreceiver round-trips in `walsender_pg18_walreceiver`;
  explicit unit test is polish
