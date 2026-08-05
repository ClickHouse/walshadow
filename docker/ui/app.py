"""walshadow demo UI — the click-to-drive surface for the compose demo.

Reads three backends every second and pushes one JSON snapshot to every
connected browser over a WebSocket:

  * source PostgreSQL  — rowcount, max(id), whether `signup_ts` exists yet,
                         and the live column list. Also the write target for
                         every operator action.
  * ClickHouse (:8123) — live rowcount, raw version count, max(_lsn), age of
                         the newest change, DESCRIBE TABLE, recent rows.
  * walshadow (:9484)  — the Prometheus text endpoint, for lag and throughput.

Design notes worth knowing before editing:

* No control socket. walshadow's control plane (src/ops/control.rs) would add
  paused / rows_synced / backfills_pending, but reaching its 0600 unix socket
  needs a new named volume in the *base* compose file plus a uid match against
  the daemon image's `postgres` user. Everything the panels need is already in
  /metrics, so this talks HTTP only.

* The pgbench write load is a child process of THIS container, not the sibling
  `pgbench` compose service (which now sits behind the `load` profile and does
  not start with the stack). See PgbenchLoad for why. Consequence: the UI image
  needs the `pgbench` binary — installed in ui/Dockerfile — and the load stops
  when the UI container stops or restarts.

* The DDL beat's UPDATE is bounded (WALSHADOW_UI_DDL_UPDATE_ROWS, default 100).
  DEMO.md's CLI form has no WHERE clause, which is safe there because the table
  holds three rows. Pressed after "insert 1,000,000 rows" an unbounded update
  rewrites a million tuples under REPLICA IDENTITY FULL — full old *and* new
  images, hundreds of MB of WAL in a single transaction, against a 1 GiB
  --xact-buffer-max with pgbench already competing for it. 100 rows proves the
  new column's values ship just as well.

* The recent-rows feed deliberately omits FINAL. The newest rows by _lsn are by
  definition the winning versions, so dedup buys nothing, and showing the raw
  versions is honest about what CDC actually writes. `SELECT * FINAL ORDER BY
  _lsn DESC` would also force a full merge + sort of the table every second.

* Non-finite metric values become None at the parse boundary.
  walshadow_shadow_apply_lag_seconds renders `+Inf` whenever lag bytes > 0 and
  the byte-rate estimator has no rate (common between demo beats), and
  json.dumps(float('inf')) emits the bare token `Infinity` — invalid JSON that
  makes the browser's JSON.parse throw, killing render() for the whole stream
  rather than one tile.

* Rowcount parity counts `FINAL WHERE _is_deleted = 0`, not bare FINAL:
  ReplacingMergeTree(_lsn, _is_deleted) keeps the winning version even when
  that version is a tombstone, so FINAL alone would still count deleted rows.
  FINAL is deliberately absent from the *labels* on the page — the panel says
  what the number means, not which keyword produced it — so keep query and
  label wording independent when editing either.

* Parity covers every table the demo replicates — demo.users plus the four
  pgbench TPC-B tables — one row each, from collect_parity() and its own
  connection pool. WALSHADOW_UI_PARITY_TABLES overrides the list. It reports
  counts only; no per-table rate, since a background merge collapsing versions
  makes a differenced rate read as a counter reset.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from psycopg_pool import AsyncConnectionPool

HERE = os.path.dirname(os.path.abspath(__file__))

PG_DSN = os.environ.get(
    "WALSHADOW_UI_PG_DSN",
    "postgresql://postgres@source:5432/postgres?application_name=walshadow-ui",
)
CH_URL = os.environ.get("WALSHADOW_UI_CH_URL", "http://clickhouse:8123").rstrip("/")
CH_USER = os.environ.get("WALSHADOW_UI_CH_USER", "default")
CH_PASSWORD = os.environ.get("WALSHADOW_UI_CH_PASSWORD", "")
METRICS_URL = os.environ.get(
    "WALSHADOW_UI_METRICS_URL", "http://walshadow:9484/metrics"
)

PG_SCHEMA = os.environ.get("WALSHADOW_UI_PG_SCHEMA", "demo")
PG_RELNAME = os.environ.get("WALSHADOW_UI_PG_RELNAME", "users")
CH_DATABASE = os.environ.get("WALSHADOW_UI_CH_DATABASE", "demo")
CH_TABLE = os.environ.get("WALSHADOW_UI_CH_TABLE", "users")

PG_TABLE = f"{PG_SCHEMA}.{PG_RELNAME}"
CH_QUALIFIED = f"{CH_DATABASE}.{CH_TABLE}"

POLL_INTERVAL_S = float(os.environ.get("WALSHADOW_UI_POLL_INTERVAL", "1.0"))
MAX_INSERT_ROWS = int(os.environ.get("WALSHADOW_UI_MAX_INSERT_ROWS", "5000000"))
CHUNK_ROWS = int(os.environ.get("WALSHADOW_UI_CHUNK_ROWS", "250000"))
DDL_UPDATE_ROWS = int(os.environ.get("WALSHADOW_UI_DDL_UPDATE_ROWS", "100"))
RECENT_ROWS = int(os.environ.get("WALSHADOW_UI_RECENT_ROWS", "12"))

DDL_COLUMN = os.environ.get("WALSHADOW_UI_DDL_COLUMN", "signup_ts")
DDL_COLUMN_TYPE = os.environ.get("WALSHADOW_UI_DDL_COLUMN_TYPE", "timestamptz")

# ---- pgbench background load, driven from the page (see PgbenchLoad below).
# The compose `pgbench` service is behind the `load` profile, so the stack now
# comes up quiet and the operator arms the roar with a button.
PGBENCH_BIN = os.environ.get("WALSHADOW_UI_PGBENCH_BIN", "pgbench")
PGBENCH_HOST = os.environ.get("WALSHADOW_UI_PGBENCH_HOST", "source")
PGBENCH_PORT = os.environ.get("WALSHADOW_UI_PGBENCH_PORT", "5432")
PGBENCH_USER = os.environ.get("WALSHADOW_UI_PGBENCH_USER", "postgres")
PGBENCH_DB = os.environ.get("WALSHADOW_UI_PGBENCH_DB", "postgres")
PGBENCH_CLIENTS = int(os.environ.get("WALSHADOW_UI_PGBENCH_CLIENTS", "4"))
PGBENCH_THREADS = int(os.environ.get("WALSHADOW_UI_PGBENCH_THREADS", "2"))
# pgbench has no "run until I say stop" mode — it needs -T or -t. So cap it a
# day out and treat the STOP button (SIGTERM) as the normal way it ends.
PGBENCH_DURATION_S = int(os.environ.get("WALSHADOW_UI_PGBENCH_DURATION", "86400"))
PGBENCH_PROGRESS_S = int(os.environ.get("WALSHADOW_UI_PGBENCH_PROGRESS", "5"))
PGBENCH_LOG_LINES = 6

# ---- rowcount parity, one row per table the demo replicates: demo.users (the
# click-driven one) plus the four pgbench TPC-B tables the write load hammers.
# Comma-separated `source=destination` pairs; both sides must be schema
# qualified because they differ (pgbench lives in `public` on the source and
# lands in `demo` on ClickHouse, per ch-config.demo.toml).
PARITY_TABLES_RAW = os.environ.get(
    "WALSHADOW_UI_PARITY_TABLES",
    f"{PG_TABLE}={CH_QUALIFIED},"
    "public.pgbench_accounts=demo.pgbench_accounts,"
    "public.pgbench_branches=demo.pgbench_branches,"
    "public.pgbench_tellers=demo.pgbench_tellers,"
    "public.pgbench_history=demo.pgbench_history",
)

# Both names are interpolated straight into SQL — psycopg can parameterize
# values but not identifiers, and ClickHouse is spoken to over HTTP as text.
# So anything that is not a plain schema.table pair is dropped, loudly enough
# to find in the logs, rather than concatenated into a query.
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*$")


def parse_parity_tables(raw: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for pair in (p.strip() for p in raw.split(",")):
        if not pair:
            continue
        src, _, dest = pair.partition("=")
        src, dest = src.strip(), dest.strip()
        if not IDENT_RE.match(src) or not IDENT_RE.match(dest):
            print(f"walshadow-ui: ignoring malformed parity table pair {pair!r}")
            continue
        out.append({"source": src, "ch": dest})
    return out


PARITY_TABLES = parse_parity_tables(PARITY_TABLES_RAW)

METRICS_TIMEOUT_S = 1.5
PG_TIMEOUT_S = 3.5
# ClickHouse gets a wider budget than source PG: right after a big insert the
# ReplacingMergeTree is merging millions of rows while pgbench hammers four
# other tables, and a `count() FINAL` that normally runs in 60ms was measured
# spiking past 1.7s. Paired with CH_STALE_MAX_S below, which is what actually
# keeps the panel from blanking during such a burst.
CH_TIMEOUT_S = 6.0
CH_MAX_EXECUTION_S = 5
# How long to keep showing the last good ClickHouse reading when a poll fails.
# A blank parity card is worst exactly when it matters most — the seconds after
# an insert — so freeze the numbers and label them stale instead.
CH_STALE_MAX_S = 20.0
# The multi-table parity read is two round-trips per backend over five tables,
# including `count(*)` on a pgbench_history that grows for as long as the load
# runs. Wider than either single-backend budget, and it stale-carries the same
# way when it blows through.
PARITY_TIMEOUT_S = 9.0

ACTION_HISTORY_MAX = 8


# ---------------------------------------------------------------- rate helper


class CounterRate:
    """Windowed derivative of a monotonic counter.

    Two guards matter:

      * counter reset — walshadow restarting zeroes every _total, so a naive
        (cur - prev) / dt goes hugely negative. Detect cur < prev, drop the
        whole window, and return None for that tick (the tile shows an em-dash).
      * division by zero — two samples inside the same monotonic instant.
        Require MIN_DT seconds of separation.

    The 5s window is wider than strictly necessary because the UI polls at 1 Hz
    against walshadow's 1s status cadence; the two clocks beat against each
    other and an instantaneous rate alternates 0 / 2x. With no chart to average
    that out for the eye, a KPI tile needs the smoothing.
    """

    WINDOW_S = 5.0
    MIN_DT = 0.25

    def __init__(self) -> None:
        self.samples: deque[tuple[float, float]] = deque()

    def clear(self) -> None:
        self.samples.clear()

    def observe(self, t: float, v: float | None) -> float | None:
        if v is None:
            self.samples.clear()
            return None
        if self.samples and v < self.samples[-1][1]:
            self.samples.clear()  # counter reset
        self.samples.append((t, v))
        while len(self.samples) > 1 and t - self.samples[0][0] > self.WINDOW_S:
            self.samples.popleft()
        if len(self.samples) < 2:
            return None
        (t0, v0), (t1, v1) = self.samples[0], self.samples[-1]
        dt = t1 - t0
        if dt < self.MIN_DT:
            return None
        return (v1 - v0) / dt


# --------------------------------------------------------------------- state


class State:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self.last_snapshot: dict[str, Any] = {}
        self.poll_seq = 0
        self.action_lock = asyncio.Lock()
        self.current_action: dict[str, Any] | None = None
        self.action_history: deque[dict[str, Any]] = deque(maxlen=ACTION_HISTORY_MAX)
        self.next_action_id = 1
        self.last_uptime: float | None = None
        self.last_ch_ok: dict[str, Any] | None = None
        self.last_ch_ok_at: float = 0.0
        self.last_parity_ok: dict[str, Any] | None = None
        self.last_parity_ok_at: float = 0.0
        self.rates: dict[str, CounterRate] = {
            "emitter_rows": CounterRate(),
            "xacts": CounterRate(),
            "decoded": CounterRate(),
            "ch_versions": CounterRate(),
        }


state = State()
http_client: httpx.AsyncClient | None = None
read_pool: AsyncConnectionPool | None = None
write_pool: AsyncConnectionPool | None = None
parity_pool: AsyncConnectionPool | None = None


# ------------------------------------------------------- pgbench load control


class PgbenchLoad:
    """START/STOP the TPC-B hammer as a child process of this container.

    Why in-process and not `docker compose start pgbench`: driving the sibling
    container would mean mounting the docker socket into the UI (root-equivalent
    access to the host daemon) and hard-coding the compose project name. A child
    process needs neither, gives an exit code and stderr straight back to the
    page, and dies with the container if the operator kills the stack mid-load.

    Deliberately NOT routed through the action lock. The insert / DDL actions
    are one-shot and mutually exclusive; the load is a long-lived background
    condition that the whole point of the demo is to run *underneath* them.
    """

    # `progress: 5.0 s, 8214.6 tps, lat 0.485 ms stddev 0.203, 0 failed`
    PROGRESS_RE = re.compile(
        r"^progress:\s+([\d.]+)\s+s,\s+([\d.]+)\s+tps,\s+lat\s+([\d.]+)\s+ms"
    )

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.proc: asyncio.subprocess.Process | None = None
        self.reader: asyncio.Task | None = None
        self.started_at: float | None = None
        self.clients = PGBENCH_CLIENTS
        self.threads = PGBENCH_THREADS
        self.tps: float | None = None
        self.lat_ms: float | None = None
        self.log: deque[str] = deque(maxlen=PGBENCH_LOG_LINES)
        self.last_exit: dict[str, Any] | None = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    def argv(self) -> list[str]:
        return [
            PGBENCH_BIN,
            "-h", PGBENCH_HOST,
            "-p", PGBENCH_PORT,
            "-U", PGBENCH_USER,
            "-d", PGBENCH_DB,
            # -n: never vacuum. The seeded tables are already loaded and a
            # vacuum here would stall the first seconds of every run.
            "-n",
            "-c", str(self.clients),
            "-j", str(self.threads),
            "-T", str(PGBENCH_DURATION_S),
            "-P", str(PGBENCH_PROGRESS_S),
        ]

    async def start(self, clients: int, threads: int) -> dict[str, Any]:
        async with self.lock:
            if self.running:
                raise RuntimeError("write load is already running")
            self.clients = clients
            self.threads = threads
            self.tps = self.lat_ms = None
            self.log.clear()
            self.last_exit = None
            try:
                self.proc = await asyncio.create_subprocess_exec(
                    *self.argv(),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    # trust auth on source, so no PGPASSWORD needed.
                    env={**os.environ, "PGCONNECT_TIMEOUT": "5"},
                )
            except FileNotFoundError as e:
                raise RuntimeError(
                    f"{PGBENCH_BIN} not found in the UI image — rebuild it "
                    f"(`docker compose ... build ui`)"
                ) from e
            self.started_at = time.time()
            self.reader = asyncio.create_task(self._drain())
            return {
                "started": True,
                "clients": self.clients,
                "threads": self.threads,
            }

    async def stop(self) -> dict[str, Any]:
        async with self.lock:
            if not self.running:
                raise RuntimeError("write load is not running")
            proc = self.proc
            assert proc is not None
            # SIGTERM: pgbench stops its clients and prints the final summary,
            # which _drain then parks in the log for the page to show.
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
            return {"stopped": True}

    async def _drain(self) -> None:
        """Consume pgbench's output until EOF, keeping the last few lines.

        Draining is not optional: pgbench writes a progress line every
        PGBENCH_PROGRESS_S seconds and would eventually block on a full pipe.
        """
        proc = self.proc
        assert proc is not None and proc.stdout is not None
        try:
            while True:
                raw = await proc.stdout.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", "replace").rstrip()
                if not line:
                    continue
                self.log.append(line)
                m = self.PROGRESS_RE.match(line)
                if m:
                    self.tps = float(m.group(2))
                    self.lat_ms = float(m.group(3))
        except asyncio.CancelledError:
            raise
        finally:
            code = await proc.wait()
            self.last_exit = {
                "code": code,
                # SIGTERM (-15) is the STOP button, i.e. success. Anything else
                # is a real failure and the tail of the log says why.
                "ok": code in (0, -15),
                "at": time.time(),
                "ran_for_ms": int((time.time() - (self.started_at or time.time())) * 1000),
            }
            self.tps = self.lat_ms = None

    def view(self) -> dict[str, Any]:
        running = self.running
        return {
            "running": running,
            "clients": self.clients,
            "threads": self.threads,
            "duration_s": PGBENCH_DURATION_S,
            "tps": self.tps if running else None,
            "lat_ms": self.lat_ms if running else None,
            "elapsed_ms": (
                int((time.time() - self.started_at) * 1000)
                if running and self.started_at
                else None
            ),
            "log": list(self.log),
            "last_exit": self.last_exit,
        }


load = PgbenchLoad()


# ------------------------------------------------------------ metrics parsing

# All verified present and unlabeled in src/ops/metrics.rs.
METRIC_NAMES = (
    "walshadow_shadow_apply_lag_seconds",
    "walshadow_shadow_apply_lag_bytes",
    "walshadow_source_received_lsn",
    "walshadow_emitter_ack_lsn",
    "walshadow_emitter_rows_total",
    "walshadow_xacts_committed_total",
    "walshadow_decoder_decoded_total",
    "walshadow_xact_active",
    "walshadow_xact_bytes_in_memory",
    "walshadow_spill_bytes_active",
    "walshadow_uptime_seconds",
    "walshadow_bridge_up",
)


def parse_prom(text: str) -> dict[str, float | None]:
    """Unlabeled Prometheus text format -> {name: value}.

    Skips `# HELP` / `# TYPE` and every labeled series (`name{...} v`) — the
    only labeled families walshadow exports are per-rmgr and per-op breakdowns
    this UI does not use.

    Non-finite values (`+Inf`) map to None here, at the boundary. See the
    module docstring for why that is load-bearing.
    """
    out: dict[str, float | None] = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        name, _, raw = line.partition(" ")
        if "{" in name:
            continue
        try:
            v = float(raw)
        except ValueError:
            continue
        out[name] = v if math.isfinite(v) else None
    return out


async def collect_walshadow() -> dict[str, Any]:
    assert http_client is not None
    try:
        resp = await http_client.get(METRICS_URL, timeout=METRICS_TIMEOUT_S)
        resp.raise_for_status()
    except Exception as e:  # noqa: BLE001 — any failure is "not up yet"
        # The overwhelmingly common case on a fresh `up`: :9484 opens only
        # after preflight + bootstrap succeed, which can take minutes.
        for r in ("emitter_rows", "xacts", "decoded"):
            state.rates[r].clear()
        state.last_uptime = None
        return {"up": False, "error": f"{type(e).__name__}: {e}"}

    m = parse_prom(resp.text)
    now = time.monotonic()

    uptime = m.get("walshadow_uptime_seconds")
    # A restart zeroes every counter at once, so clear all the windows, not
    # just whichever one happened to be sampled going backwards.
    if uptime is not None and state.last_uptime is not None and uptime < state.last_uptime:
        for r in ("emitter_rows", "xacts", "decoded"):
            state.rates[r].clear()
    state.last_uptime = uptime

    received = m.get("walshadow_source_received_lsn")
    acked = m.get("walshadow_emitter_ack_lsn")
    lag_bytes_ch = None
    if received is not None and acked is not None:
        lag_bytes_ch = max(0, int(received) - int(acked))

    lag_seconds = m.get("walshadow_shadow_apply_lag_seconds")
    # The metric is present-but-null exactly when the daemon rendered `+Inf`:
    # a nonzero byte backlog with no WAL byte-rate to divide it by.
    lag_unknown = (
        "walshadow_shadow_apply_lag_seconds" in m and lag_seconds is None
    )

    counters = {
        "emitter_rows_total": m.get("walshadow_emitter_rows_total"),
        "xacts_committed_total": m.get("walshadow_xacts_committed_total"),
        "decoder_decoded_total": m.get("walshadow_decoder_decoded_total"),
    }
    rates = {
        "emitter_rows_per_sec": state.rates["emitter_rows"].observe(
            now, counters["emitter_rows_total"]
        ),
        "xacts_per_sec": state.rates["xacts"].observe(
            now, counters["xacts_committed_total"]
        ),
        "decoded_per_sec": state.rates["decoded"].observe(
            now, counters["decoder_decoded_total"]
        ),
    }

    def as_int(v: float | None) -> int | None:
        return None if v is None else int(v)

    return {
        "up": True,
        "error": None,
        "uptime_seconds": as_int(uptime),
        "bridge_up": bool(m.get("walshadow_bridge_up") or 0),
        "lag_seconds": lag_seconds,
        "lag_seconds_unknown": lag_unknown,
        # NOTE: shadow_apply_lag_* measures source -> shadow PG, not
        # source -> ClickHouse (src/bin/stream.rs). The CH-side backlog is
        # source_received_lsn - emitter_ack_lsn. Both are reported; labeling
        # the shadow lag as "lag to ClickHouse" would read implausibly good
        # during a big insert, when the shadow keeps up and the emitter holds
        # the backlog.
        "lag_bytes_shadow": as_int(m.get("walshadow_shadow_apply_lag_bytes")),
        "lag_bytes_ch": lag_bytes_ch,
        "source_received_lsn": as_int(received),
        "emitter_ack_lsn": as_int(acked),
        "xact_active": as_int(m.get("walshadow_xact_active")),
        "xact_bytes_in_memory": as_int(m.get("walshadow_xact_bytes_in_memory")),
        "spill_bytes_active": as_int(m.get("walshadow_spill_bytes_active")),
        "counters": counters,
        "rates": rates,
    }


# ------------------------------------------------------------- source collector

SQL_SOURCE_STATS = f"""
SELECT (SELECT count(*)             FROM {PG_TABLE})              AS row_count,
       (SELECT coalesce(max(id), 0) FROM {PG_TABLE})              AS max_id,
       EXISTS (SELECT 1 FROM pg_attribute
                WHERE attrelid = '{PG_TABLE}'::regclass
                  AND attname = %(col)s AND NOT attisdropped)     AS has_ddl_column
"""

SQL_SOURCE_COLUMNS = f"""
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS type
FROM pg_attribute a
WHERE a.attrelid = '{PG_TABLE}'::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""


async def collect_source() -> dict[str, Any]:
    assert read_pool is not None
    t0 = time.monotonic()
    async with read_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_SOURCE_STATS, {"col": DDL_COLUMN})
            row = await cur.fetchone()
            await cur.execute(SQL_SOURCE_COLUMNS)
            cols = await cur.fetchall()
    return {
        "up": True,
        "error": None,
        "query_ms": int((time.monotonic() - t0) * 1000),
        "row_count": row[0],
        "max_id": row[1],
        "has_signup_ts": bool(row[2]),
        "columns": [{"name": c[0], "type": c[1]} for c in cols],
    }


# --------------------------------------------------------- clickhouse collector

SQL_CH_STATS = f"""
SELECT (SELECT count() FROM {CH_QUALIFIED} FINAL WHERE _is_deleted = 0) AS rows_live,
       count()                                                         AS versions,
       toString(max(_lsn))                                             AS max_lsn,
       if(count() = 0, NULL,
          dateDiff('millisecond', max(_commit_ts), now64(3)))           AS staleness_ms
FROM {CH_QUALIFIED}
FORMAT JSON
"""

SQL_CH_RECENT = f"""
SELECT * FROM {CH_QUALIFIED}
ORDER BY _lsn DESC, id DESC
LIMIT {RECENT_ROWS}
FORMAT JSONCompact
"""

SQL_CH_DESCRIBE = f"DESCRIBE TABLE {CH_QUALIFIED} FORMAT JSONCompact"

# `output_format_json_quote_64bit_integers=0` matters: without it FORMAT JSON
# hands back every 64-bit integer as a JSON *string*. max_lsn still goes
# through toString() anyway, because a real UInt64 LSN can exceed
# Number.MAX_SAFE_INTEGER and JS would silently round it.
CH_PARAMS = {
    "output_format_json_quote_64bit_integers": "0",
    "max_execution_time": str(CH_MAX_EXECUTION_S),
    "default_format": "JSON",
}


async def ch_query(sql: str) -> dict[str, Any]:
    assert http_client is not None
    resp = await http_client.post(
        CH_URL,
        params=CH_PARAMS,
        content=sql.encode(),
        headers={
            "X-ClickHouse-User": CH_USER,
            "X-ClickHouse-Key": CH_PASSWORD,
        },
        timeout=CH_TIMEOUT_S,
    )
    if resp.status_code != 200:
        raise RuntimeError(resp.text.strip()[:400])
    return json.loads(resp.text)


def is_unknown_table(msg: str) -> bool:
    return "Code: 60" in msg or "UNKNOWN_TABLE" in msg or "Code: 81" in msg


def ch_stale_or_down(e: BaseException) -> dict[str, Any]:
    """Degrade a failed ClickHouse poll, preferring the last good reading.

    A timeout here is usually transient — right after a big insert the
    ReplacingMergeTree is merging millions of rows while pgbench hammers four
    other tables, and a `count() FINAL` that normally runs in 60ms can spike
    past a second. Blanking six tiles at exactly the moment the audience is
    watching them converge is the wrong answer, so freeze the numbers and let
    the page label them stale. `table_missing` is never carried forward — that
    is a configuration fact, not a blip.
    """
    msg = f"{type(e).__name__}: {e}"
    missing = is_unknown_table(str(e))
    age = time.monotonic() - state.last_ch_ok_at
    # The rate window would otherwise read a fake dip across the gap.
    state.rates["ch_versions"].clear()
    if state.last_ch_ok is not None and not missing and age <= CH_STALE_MAX_S:
        return {
            **state.last_ch_ok,
            "stale": True,
            "stale_ms": int(age * 1000),
            "error": msg,
            "rates": {"versions_per_sec": None},
        }
    return {"up": False, "error": msg, "stale": False, "table_missing": missing}


async def collect_clickhouse() -> dict[str, Any]:
    t0 = time.monotonic()
    try:
        stats, recent, described = await asyncio.gather(
            ch_query(SQL_CH_STATS),
            ch_query(SQL_CH_RECENT),
            ch_query(SQL_CH_DESCRIBE),
        )
    except Exception as e:  # noqa: BLE001
        return ch_stale_or_down(e)

    d = stats["data"][0]
    versions = int(d["versions"])
    now = time.monotonic()
    staleness = d["staleness_ms"]

    payload = {
        "up": True,
        "error": None,
        "stale": False,
        "stale_ms": None,
        "table_missing": False,
        "query_ms": int((time.monotonic() - t0) * 1000),
        "rows_live": int(d["rows_live"]),
        "versions": versions,
        "max_lsn": d["max_lsn"],
        "staleness_ms": None if staleness is None else int(staleness),
        "columns": [{"name": r[0], "type": r[1]} for r in described["data"]],
        "recent": {
            "columns": [m["name"] for m in recent["meta"]],
            "rows": recent["data"],
        },
        # There is no per-table throughput in /metrics
        # (walshadow_emitter_rows_total is unlabeled), so the one number that
        # responds to a button click has to come from differencing this count.
        "rates": {
            "versions_per_sec": state.rates["ch_versions"].observe(now, versions)
        },
    }
    state.last_ch_ok = payload
    state.last_ch_ok_at = time.monotonic()
    return payload


# ------------------------------------------------------- multi-table parity

# Its own collector, not an extension of collect_source / collect_clickhouse,
# for two reasons. It is the expensive read of the three — five `count() FINAL`
# on ClickHouse plus five exact `count(*)` on the source, and pgbench_accounts /
# pgbench_history are the tables the load makes big — so it gets its own task,
# its own timeout and its own stale-carry. And it must not be able to take the
# demo.users panels down with it: a timeout here leaves the schema, rows feed
# and controls untouched.

SQL_PG_EXISTS = """
SELECT t, to_regclass(t) IS NOT NULL AS present
FROM unnest(%(names)s::text[]) AS t
"""

# system.tables, not a probe query per table: one round-trip, and a missing
# table is then a normal empty row rather than an exception to classify.
CH_EXISTS_SQL = """
SELECT concat(database, '.', name) AS t FROM system.tables
WHERE concat(database, '.', name) IN ({names}) FORMAT JSONCompact
"""


def ch_parity_sql(tables: list[str]) -> str:
    """One UNION ALL over the present tables: rows_live, versions, staleness.

    `count()` without FINAL is part metadata and near-free; the FINAL count is
    the one that costs, which is why the whole thing is a single query rather
    than a fan-out of five.
    """
    parts = [
        f"""SELECT '{t}' AS tbl,
       (SELECT count() FROM {t} FINAL WHERE _is_deleted = 0) AS rows_live,
       (SELECT count() FROM {t}) AS versions,
       (SELECT if(count() = 0, NULL,
                  dateDiff('millisecond', max(_commit_ts), now64(3))) FROM {t})
           AS staleness_ms"""
        for t in tables
    ]
    return " UNION ALL ".join(parts) + " FORMAT JSON"


async def pg_parity_counts() -> dict[str, int]:
    """count(*) per existing source table, skipping the ones that aren't there.

    Exact counts, not reltuples: parity is the claim being made, and an
    estimate that drifts from ClickHouse would read as replication lag.
    """
    assert parity_pool is not None
    names = [t["source"] for t in PARITY_TABLES]
    async with parity_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_PG_EXISTS, {"names": names})
            present = [r[0] for r in await cur.fetchall() if r[1]]
            if not present:
                return {}
            sql = " UNION ALL ".join(
                f"SELECT {i} AS i, count(*) AS n FROM {t}"
                for i, t in enumerate(present)
            )
            await cur.execute(f"{sql} ORDER BY i")
            rows = await cur.fetchall()
    return {present[r[0]]: r[1] for r in rows}


async def ch_parity_counts() -> dict[str, dict[str, Any]]:
    if not PARITY_TABLES:
        return {}
    quoted = ", ".join(f"'{t['ch']}'" for t in PARITY_TABLES)
    found = await ch_query(CH_EXISTS_SQL.format(names=quoted))
    present = [r[0] for r in found["data"]]
    if not present:
        return {}
    stats = await ch_query(ch_parity_sql(present))
    return {r["tbl"]: r for r in stats["data"]}


async def collect_parity() -> dict[str, Any]:
    t0 = time.monotonic()
    try:
        pg_counts, ch_counts = await asyncio.gather(
            pg_parity_counts(), ch_parity_counts()
        )
    except Exception as e:  # noqa: BLE001
        msg = f"{type(e).__name__}: {e}"
        age = time.monotonic() - state.last_parity_ok_at
        if state.last_parity_ok is not None and age <= CH_STALE_MAX_S:
            return {
                **state.last_parity_ok,
                "stale": True,
                "stale_ms": int(age * 1000),
                "error": msg,
            }
        return {"up": False, "error": msg, "stale": False, "tables": []}

    rows = []
    for t in PARITY_TABLES:
        src_rows = pg_counts.get(t["source"])
        ch = ch_counts.get(t["ch"])
        ch_rows = None if ch is None else int(ch["rows_live"])
        versions = None if ch is None else int(ch["versions"])
        staleness = None if ch is None else ch["staleness_ms"]
        delta = None if src_rows is None or ch_rows is None else ch_rows - src_rows
        rows.append(
            {
                "source": t["source"],
                "ch": t["ch"],
                "source_rows": src_rows,
                "ch_rows": ch_rows,
                "versions": versions,
                "delta": delta,
                "in_sync": delta == 0,
                # Clamped at zero: _commit_ts comes from the source's clock and
                # now64() from ClickHouse's, so a table that was written to
                # milliseconds ago can otherwise show a negative age.
                "staleness_ms": None if staleness is None else max(0, int(staleness)),
                "pg_missing": src_rows is None,
                "ch_missing": ch is None,
            }
        )

    payload = {
        "up": True,
        "error": None,
        "stale": False,
        "stale_ms": None,
        "query_ms": int((time.monotonic() - t0) * 1000),
        "tables": rows,
    }
    state.last_parity_ok = payload
    state.last_parity_ok_at = time.monotonic()
    return payload


# ------------------------------------------------------------------- snapshot


def action_view() -> dict[str, Any]:
    cur = state.current_action
    view = None
    if cur is not None:
        view = {
            "id": cur["id"],
            "kind": cur["kind"],
            "label": cur["label"],
            "status": cur["status"],
            "started_at": cur["started_at"],
            # Stamped fresh every tick so the browser shows a live timer
            # without any client-side clock of its own.
            "elapsed_ms": int((time.time() - cur["started_at"]) * 1000),
            "progress": cur.get("progress"),
            "detail": cur.get("detail"),
        }
    return {
        "busy": state.current_action is not None,
        "current": view,
        "history": list(state.action_history),
    }


async def collect_snapshot() -> dict[str, Any]:
    t0 = time.monotonic()
    ws, src, ch, par_tables = await asyncio.gather(
        asyncio.wait_for(collect_walshadow(), timeout=METRICS_TIMEOUT_S + 0.5),
        asyncio.wait_for(collect_source(), timeout=PG_TIMEOUT_S),
        asyncio.wait_for(collect_clickhouse(), timeout=CH_TIMEOUT_S + 1.0),
        # The widest budget of the four: five exact source counts plus five
        # `count() FINAL`, against tables the write load grows.
        asyncio.wait_for(collect_parity(), timeout=PARITY_TIMEOUT_S),
        return_exceptions=True,
    )

    def degrade(r: Any) -> dict[str, Any]:
        if isinstance(r, BaseException):
            return {"up": False, "error": f"{type(r).__name__}: {r}"}
        return r

    ws = degrade(ws)
    src = degrade(src)
    # The collector swallows its own errors, so anything escaping here is the
    # outer wait_for firing. Route it through the same stale-carry path.
    ch = ch_stale_or_down(ch) if isinstance(ch, BaseException) else ch
    par_tables = degrade(par_tables)
    if not par_tables.get("tables"):
        par_tables.setdefault("tables", [])

    src_rows = src.get("row_count") if src.get("up") else None
    ch_rows = ch.get("rows_live") if ch.get("up") else None
    delta = None if src_rows is None or ch_rows is None else ch_rows - src_rows

    state.poll_seq += 1
    return {
        "now": time.time(),
        "poll_ms": int((time.monotonic() - t0) * 1000),
        "poll_seq": state.poll_seq,
        "config": {
            "pg_table": PG_TABLE,
            "ch_table": CH_QUALIFIED,
            "max_insert_rows": MAX_INSERT_ROWS,
            "chunk_rows": CHUNK_ROWS,
            "ddl_update_rows": DDL_UPDATE_ROWS,
            "ddl_column": DDL_COLUMN,
            "ddl_column_type": DDL_COLUMN_TYPE,
            "recent_rows": RECENT_ROWS,
            "pgbench_clients": PGBENCH_CLIENTS,
            "pgbench_threads": PGBENCH_THREADS,
        },
        "walshadow": ws,
        "source": src,
        "clickhouse": ch,
        # `parity` stays the demo.users headline, derived from the same reads
        # the schema and rows panels use. `parity_tables` is the per-table
        # breakdown the parity card renders, from its own collector.
        "parity": {
            "source_rows": src_rows,
            "ch_rows": ch_rows,
            "delta": delta,
            "in_sync": delta == 0,
        },
        "parity_tables": par_tables,
        "action": action_view(),
        "load": load.view(),
    }


async def broadcast(payload: str) -> None:
    dead = []
    for ws in list(state.clients):
        try:
            await ws.send_text(payload)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        state.clients.discard(ws)


async def poll_loop() -> None:
    while True:
        try:
            snap = await collect_snapshot()
            state.last_snapshot = snap
            if state.clients:
                await broadcast(json.dumps(snap))
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — keep the loop alive no matter what
            err = {"error": f"{type(e).__name__}: {e}", "now": time.time()}
            state.last_snapshot = err
            if state.clients:
                await broadcast(json.dumps(err))
        await asyncio.sleep(POLL_INTERVAL_S)


# -------------------------------------------------------------------- actions

SQL_INSERT_CHUNK = f"""
INSERT INTO {PG_TABLE} (id, name, email)
SELECT g, 'user_' || g, 'user_' || g || '@rerum.novarum'
FROM generate_series(%(lo)s::bigint, %(hi)s::bigint) AS g
"""

SQL_MAX_ID = f"SELECT coalesce(max(id), 0) FROM {PG_TABLE}"

# Index-only min/max on the PK plus one index range scan — O(log n), unlike
# `ORDER BY random() LIMIT 1` which seq-scans the whole table on every click.
# Gap-tolerant: the dart lands anywhere in [lo, hi] and the scan takes the
# next existing id at or above it.
SQL_UPDATE_RANDOM = f"""
WITH bounds AS (SELECT min(id) AS lo, max(id) AS hi FROM {PG_TABLE}),
pick AS (
    SELECT u.id FROM {PG_TABLE} u, bounds b
    WHERE u.id >= b.lo + floor(random() * (b.hi - b.lo + 1))::bigint
    ORDER BY u.id LIMIT 1
)
UPDATE {PG_TABLE} u
SET email = 'touched+' || to_char(clock_timestamp(), 'HH24MISSUS') || '@rerum.novarum'
WHERE u.id = (SELECT id FROM pick)
RETURNING u.id, u.email
"""

# Same dart-throw pick as the update. The DELETE is the interesting half: under
# REPLICA IDENTITY FULL the old tuple image ships in full, walshadow emits the
# row with _is_deleted = 1, and the deduplicated count on the parity card drops
# by one while `row versions landed` goes *up* by one.
SQL_DELETE_RANDOM = f"""
WITH bounds AS (SELECT min(id) AS lo, max(id) AS hi FROM {PG_TABLE}),
pick AS (
    SELECT u.id FROM {PG_TABLE} u, bounds b
    WHERE u.id >= b.lo + floor(random() * (b.hi - b.lo + 1))::bigint
    ORDER BY u.id LIMIT 1
)
DELETE FROM {PG_TABLE} u
WHERE u.id = (SELECT id FROM pick)
RETURNING u.id, u.email
"""

SQL_HAS_DDL_COLUMN = f"""
SELECT EXISTS (SELECT 1 FROM pg_attribute
               WHERE attrelid = '{PG_TABLE}'::regclass
                 AND attname = %(col)s AND NOT attisdropped)
"""


def begin_action(kind: str, label: str, progress: dict[str, int] | None = None) -> dict:
    action = {
        "id": state.next_action_id,
        "kind": kind,
        "label": label,
        "status": "running",
        "started_at": time.time(),
        "progress": progress,
        "detail": None,
    }
    state.next_action_id += 1
    state.current_action = action
    return action


def finish_action(action: dict, status: str, detail: str | None) -> None:
    state.action_history.appendleft(
        {
            "id": action["id"],
            "kind": action["kind"],
            "label": action["label"],
            "status": status,
            "detail": detail,
            "duration_ms": int((time.time() - action["started_at"]) * 1000),
            "finished_at": time.time(),
        }
    )
    state.current_action = None


async def run_action(action: dict, coro_factory) -> None:
    try:
        detail = await coro_factory()
        finish_action(action, "ok", detail)
    except asyncio.CancelledError:
        finish_action(action, "error", "cancelled")
        raise
    except Exception as e:  # noqa: BLE001 — surface it, never swallow it
        finish_action(action, "error", f"{type(e).__name__}: {e}")


async def do_insert(rows: int, action: dict) -> str:
    assert write_pool is not None
    async with write_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_MAX_ID)
            base = (await cur.fetchone())[0]
            done = 0
            while done < rows:
                n = min(CHUNK_ROWS, rows - done)
                lo = base + done + 1
                await cur.execute(SQL_INSERT_CHUNK, {"lo": lo, "hi": lo + n - 1})
                done += n
                action["progress"] = {"done": done, "total": rows}
                # Yield so the poll loop gets a turn between chunks; the
                # source count then climbs on screen instead of sitting flat.
                await asyncio.sleep(0)
    return f"{rows:,} rows, ids {base + 1:,}..{base + rows:,}"


async def do_update_random(action: dict) -> str:
    assert write_pool is not None
    async with write_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_UPDATE_RANDOM)
            row = await cur.fetchone()
    if row is None:
        return f"{PG_TABLE} is empty — nothing to update"
    return f"id={row[0]} -> {row[1]}"


async def do_delete_random(action: dict) -> str:
    assert write_pool is not None
    async with write_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_DELETE_RANDOM)
            row = await cur.fetchone()
    if row is None:
        return f"{PG_TABLE} is empty — nothing to delete"
    return f"id={row[0]} ({row[1]}) — tombstone lands with _is_deleted = 1"


async def has_ddl_column() -> bool:
    assert read_pool is not None
    async with read_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SQL_HAS_DDL_COLUMN, {"col": DDL_COLUMN})
            return bool((await cur.fetchone())[0])


async def do_schema_evolve(action: dict) -> str:
    assert write_pool is not None
    async with write_pool.connection() as conn:
        async with conn.cursor() as cur:
            # Deliberately NOT `ADD COLUMN IF NOT EXISTS`: that form succeeds
            # without touching the catalog, so no schema change is decoded and
            # the demo appears to silently do nothing. The handler prechecks;
            # a genuine race here raises and lands in the action log.
            await cur.execute(
                f"ALTER TABLE {PG_TABLE} ADD COLUMN {DDL_COLUMN} {DDL_COLUMN_TYPE}"
            )
            # Bounded — see the module docstring.
            await cur.execute(
                f"UPDATE {PG_TABLE} SET {DDL_COLUMN} = now() "
                f"WHERE id IN (SELECT id FROM {PG_TABLE} ORDER BY id LIMIT %(n)s)",
                {"n": DDL_UPDATE_ROWS},
            )
            touched = cur.rowcount
    return f"ADD COLUMN {DDL_COLUMN} {DDL_COLUMN_TYPE}, then set now() on {touched:,} rows"


async def do_schema_reset(action: dict) -> str:
    assert write_pool is not None
    async with write_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"ALTER TABLE {PG_TABLE} DROP COLUMN IF EXISTS {DDL_COLUMN}"
            )
    return f"DROP COLUMN {DDL_COLUMN} — walshadow replicates the drop to ClickHouse"


async def queue_action(kind: str, label: str, factory, progress=None):
    """Register an action and hand it to the event loop.

    Fire-and-forget on purpose: the POST returns in microseconds and all
    progress reaches the browser through the snapshot stream. That is what
    keeps a million-row insert from hanging a fetch() for a minute.
    """
    async with state.action_lock:
        if state.current_action is not None:
            return JSONResponse(
                {
                    "error": f"{state.current_action['label']} is already running",
                    "busy": True,
                },
                status_code=409,
            )
        action = begin_action(kind, label, progress)
    asyncio.create_task(run_action(action, lambda: factory(action)))
    return {"queued": True, "action_id": action["id"]}


# ------------------------------------------------------------------------ app


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, read_pool, write_pool, parity_pool
    http_client = httpx.AsyncClient()
    # Three pools so a long write can never starve the poll's read, and the
    # multi-table parity counts can never starve the demo.users read.
    read_pool = AsyncConnectionPool(
        PG_DSN,
        min_size=1,
        max_size=2,
        open=False,
        kwargs={"options": "-c statement_timeout=3000"},
    )
    # Its own connection and a wider statement_timeout: an exact count(*) over
    # a pgbench_history that has been growing under load for an hour is a
    # legitimately slower query than anything the demo.users panels run, and
    # sharing read_pool would mean one timeout for both.
    parity_pool = AsyncConnectionPool(
        PG_DSN,
        min_size=1,
        max_size=1,
        open=False,
        kwargs={"options": "-c statement_timeout=8000"},
    )
    write_pool = AsyncConnectionPool(
        PG_DSN,
        min_size=0,
        max_size=2,
        open=False,
        # statement_timeout=0 is essential here: the insert legitimately runs
        # for minutes. autocommit so each chunk / DDL statement is its own
        # transaction, mirroring DEMO.md's separate `-c` flags.
        kwargs={"options": "-c statement_timeout=0", "autocommit": True},
    )
    # wait=False: startup must not block on source PG being reachable; the
    # pools reconnect in the background and the page paints regardless.
    await read_pool.open(wait=False)
    await write_pool.open(wait=False)
    await parity_pool.open(wait=False)
    poller = asyncio.create_task(poll_loop())
    try:
        yield
    finally:
        poller.cancel()
        await asyncio.gather(poller, return_exceptions=True)
        # Never leave the hammer swinging after the server goes away: a
        # `restart ui` would otherwise strand an orphan pgbench inside the
        # container until the whole container died.
        if load.running:
            try:
                await load.stop()
            except Exception:  # noqa: BLE001 — shutdown path, nothing to report to
                pass
        await read_pool.close()
        await parity_pool.close()
        await write_pool.close()
        await http_client.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(os.path.join(HERE, "index.html"))


@app.get("/healthz")
async def healthz() -> dict[str, bool]:
    # Compose healthcheck. Deliberately touches neither PG nor CH — it answers
    # "is the web server alive", not "is the whole demo stack up".
    return {"ok": True}


@app.get("/api/snapshot")
async def api_snapshot() -> dict[str, Any]:
    return state.last_snapshot


@app.post("/api/insert/{rows}")
async def api_insert(rows: int):
    if rows < 1 or rows > MAX_INSERT_ROWS:
        return JSONResponse(
            {"error": f"rows must be between 1 and {MAX_INSERT_ROWS:,}"},
            status_code=400,
        )
    return await queue_action(
        "insert",
        f"insert {rows:,} rows",
        lambda action: do_insert(rows, action),
        progress={"done": 0, "total": rows},
    )


@app.post("/api/update-random")
async def api_update_random():
    return await queue_action("update", "update a random row", do_update_random)


@app.post("/api/delete-random")
async def api_delete_random():
    return await queue_action("delete", "delete a random row", do_delete_random)


@app.post("/api/schema-evolve")
async def api_schema_evolve():
    try:
        if await has_ddl_column():
            return JSONResponse(
                {
                    "error": f"{DDL_COLUMN} already exists on {PG_TABLE} — "
                    f"press DROP COLUMN to reset the beat"
                },
                status_code=409,
            )
    except Exception as e:  # noqa: BLE001 — source unreachable; let the action report it
        return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=503)
    return await queue_action("evolve", f"add column {DDL_COLUMN}", do_schema_evolve)


@app.post("/api/schema-reset")
async def api_schema_reset():
    return await queue_action("reset", f"drop column {DDL_COLUMN}", do_schema_reset)


@app.post("/api/load/start")
async def api_load_start(clients: int | None = None, threads: int | None = None):
    c = PGBENCH_CLIENTS if clients is None else clients
    t = PGBENCH_THREADS if threads is None else threads
    if not 1 <= c <= 256 or not 1 <= t <= 64 or t > c:
        return JSONResponse(
            {"error": "clients must be 1..256, threads 1..64 and <= clients"},
            status_code=400,
        )
    try:
        return await load.start(c, t)
    except RuntimeError as e:
        return JSONResponse({"error": str(e)}, status_code=409)


@app.post("/api/load/stop")
async def api_load_stop():
    try:
        return await load.stop()
    except RuntimeError as e:
        return JSONResponse({"error": str(e)}, status_code=409)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    state.clients.add(ws)
    try:
        if state.last_snapshot:
            # Paint a fresh tab immediately rather than making it wait a tick.
            await ws.send_text(json.dumps(state.last_snapshot))
        while True:
            await ws.receive_text()  # keepalive sink; content ignored
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        pass
    finally:
        state.clients.discard(ws)
