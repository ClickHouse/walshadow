#!/usr/bin/env python3
"""Browser UI for the walshadow control socket.

Serves a local page that reads status + effective config over the daemon's
control socket and writes changes back through `apply` / `unset`.

    python3 tools/ctl-ui.py --socket /run/walshadow/control.sock

There is no authentication: the control socket is mode 0600, so this process
must run as the daemon's user, and the HTTP side binds loopback only.
"""

import argparse
import json
import os
import re
import socket
import tomllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_SOCKET = os.environ.get(
    "WALSHADOW_CONTROL_SOCKET", "/run/walshadow/control.sock"
)
MASK = "***"

SOURCE_FIELDS = [
    ("host", "str"),
    ("port", "int"),
    ("dbname", "str"),
    ("user", "str"),
    ("password", "secret"),
    ("slot", "str"),
    ("sslmode", "str"),
]

CH_FIELDS = [
    ("host", "str"),
    ("port", "int"),
    ("database", "str"),
    ("user", "str"),
    ("password", "secret"),
    ("secure", "bool"),
    ("compression", "str"),
]


class CtlError(Exception):
    pass


def ctl(sock_path, verb, body=""):
    """One request per connection: `verb\\n<toml>`, half-close, read to EOF."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(30)
        s.connect(sock_path)
        s.sendall(f"{verb}\n{body}".encode())
        s.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            b = s.recv(65536)
            if not b:
                break
            chunks.append(b)
    resp = b"".join(chunks).decode("utf-8", "replace")
    head, _, payload = resp.partition("\n")
    if not head.startswith("OK"):
        raise CtlError(resp.removeprefix("ERR ").strip() or "empty response")
    return tomllib.loads(payload) if payload.strip() else {}


# ---- TOML emit -------------------------------------------------------------

BARE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")


def key(k):
    return k if BARE_KEY.match(k) else json.dumps(k)


def scalar(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    return json.dumps(v)


def emit_toml(table, prefix=()):
    """Nested dicts become `[a.b]` sections, scalars become `k = v` lines."""
    lines, subs = [], []
    for k, v in table.items():
        (subs if isinstance(v, dict) else lines).append((k, v))
    out = []
    if lines:
        if prefix:
            out.append("[" + ".".join(key(p) for p in prefix) + "]")
        out += [f"{key(k)} = {scalar(v)}" for k, v in lines]
        out.append("")
    for k, v in subs:
        out.append(emit_toml(v, prefix + (k,)))
    return "\n".join(x for x in out if x is not None)


# ---- state + save ----------------------------------------------------------


def read_state(sock_path):
    state = {"status": ctl(sock_path, "status"), "config": ctl(sock_path, "show")}
    try:
        state["schemas"] = ctl(sock_path, "schemas").get("schemas", [])
        state["tables"] = ctl(sock_path, "tables").get("tables", [])
    except CtlError as e:
        # Introspection opens its own source-PG connection; keep the rest usable
        state["schemas"], state["tables"] = [], []
        state["tables_error"] = str(e)
    pinned = []
    for ns, rels in (state["config"].get("table") or {}).items():
        for rel, block in rels.items():
            if isinstance(block, dict) and "columns" in block:
                pinned.append(f"{ns}.{rel}")
    state["pinned"] = pinned
    state["source_fields"] = [list(f) for f in SOURCE_FIELDS]
    state["ch_fields"] = [list(f) for f in CH_FIELDS]
    return state


def coerce(kind, raw):
    if kind == "int":
        return int(raw)
    if kind == "bool":
        # Never `bool(raw)`: the string "false" is truthy, and flipping
        # `[ch] secure` on by accident points TLS at the plaintext port.
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    return str(raw)


def diff_section(fields, submitted, current):
    """Only changed keys; a blank or masked secret means "leave it alone"."""
    out = {}
    for name, kind in fields:
        if name not in submitted:
            continue
        raw = submitted[name]
        if kind == "secret":
            if not str(raw).strip() or str(raw) == MASK:
                continue
            out[name] = str(raw)
            continue
        if kind != "bool" and str(raw).strip() == "":
            continue
        try:
            val = coerce(kind, raw)
        except (TypeError, ValueError):
            raise CtlError(f"{name}: {raw!r} is not a valid {kind}")
        # An absent bool reads as false, so an untouched checkbox stays absent
        now = bool(current.get(name, False)) if kind == "bool" else current.get(name)
        if now != val:
            out[name] = val
    return out


def build_fragments(form, config):
    """Split the submitted form into an `apply` table and an `unset` mask."""
    apply_t, unset_t = {}, {}

    src = diff_section(SOURCE_FIELDS, form.get("source") or {}, config.get("source") or {})
    if src:
        apply_t["source"] = src
    ch = diff_section(CH_FIELDS, form.get("ch") or {}, config.get("ch") or {})
    if ch:
        apply_t["ch"] = ch

    paused = bool(form.get("paused"))
    if paused != bool((config.get("stream") or {}).get("paused", False)):
        apply_t["stream"] = {"paused": paused}

    cfg_tables = config.get("table") or {}
    for qualified, want in (form.get("tables") or {}).items():
        ns, _, rel = qualified.partition(".")
        if not ns or not rel:
            continue
        block = (cfg_tables.get(ns) or {}).get(rel)
        selected = isinstance(block, dict) and block.get("replicate") is not False
        if bool(want) == selected:
            continue
        if want:
            entry = {"replicate": True}
            if form.get("initial_load"):
                entry["initial_load"] = "copy"
            apply_t.setdefault("table", {}).setdefault(ns, {})[rel] = entry
        elif isinstance(block, dict) and "columns" in block:
            # `unset` only edits the API's own fragment, so an operator-pinned
            # mapping in the base file has to be opted out instead of removed.
            apply_t.setdefault("table", {}).setdefault(ns, {})[rel] = {
                "replicate": False
            }
        else:
            unset_t.setdefault("table", {})[ns] = {rel: ""}

    return apply_t, unset_t


def save(sock_path, form):
    config = ctl(sock_path, "show")
    apply_t, unset_t = build_fragments(form, config)
    sent = {}
    if unset_t:
        sent["unset"] = emit_toml(unset_t)
        ctl(sock_path, "unset", sent["unset"])
    if apply_t:
        sent["apply"] = emit_toml(apply_t)
        ctl(sock_path, "apply", sent["apply"])
    return sent


# ---- HTTP ------------------------------------------------------------------

PAGE = """<!doctype html>
<meta charset=utf-8><title>walshadow ctl</title>
<style>
:root{color-scheme:light dark}
body{font:14px/1.5 ui-sans-serif,system-ui,sans-serif;margin:0;padding:24px;
 max-width:940px}
h1{font-size:16px;margin:0 0 2px}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.06em;opacity:.6;
 margin:26px 0 8px}
.sub{opacity:.55;font-size:12px;margin-bottom:18px}
.strip{display:flex;gap:26px;flex-wrap:wrap;align-items:center;padding:12px 16px;
 border:1px solid #8883;border-radius:8px}
.stat b{display:block;font-size:12px;font-weight:500;opacity:.55}
.stat span{font-variant-numeric:tabular-nums}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px}
label{display:block;font-size:12px;opacity:.7;margin-bottom:3px}
input[type=text]{width:100%;padding:5px 7px;border:1px solid #8886;border-radius:5px;
 background:transparent;color:inherit;font:inherit;box-sizing:border-box}
.note{font-size:12px;opacity:.6;margin:-2px 0 8px}
.tables{border:1px solid #8883;border-radius:8px;max-height:320px;overflow:auto}
.row{display:flex;gap:10px;align-items:center;padding:5px 12px;border-top:1px solid #8882}
.row:first-child{border-top:0}
.row code{flex:1}
.tag{font-size:11px;padding:1px 6px;border-radius:99px;border:1px solid #8886;opacity:.7}
button{font:inherit;padding:6px 14px;border-radius:6px;border:1px solid #8886;
 background:transparent;color:inherit;cursor:pointer}
button.primary{border-color:#3b82f6;color:#3b82f6;font-weight:500}
.bar{display:flex;gap:10px;align-items:center;margin:22px 0 0}
pre{background:#8881;padding:10px 12px;border-radius:6px;overflow:auto;font-size:12px;
 white-space:pre-wrap;margin:12px 0 0}
.err{border-left:3px solid #ef4444;padding-left:10px}
</style>
<h1>walshadow control</h1>
<div class=sub id=sub></div>
<div class=strip id=status></div>

<h2>stream</h2>
<label><input type=checkbox id=paused> paused</label>

<h2>clickhouse</h2>
<div class=grid id=ch></div>

<h2>source <span class=tag>not live</span></h2>
<div class=note>Saved to config, but the pump keeps its connection &mdash; a real
host change needs a daemon restart.</div>
<div class=grid id=source></div>

<h2>tables</h2>
<div class=note>
  <label style="display:inline">schema
    <select id=schema></select></label>
  &nbsp;&nbsp;
  <label style="display:inline"><input type=checkbox id=initial_load>
    backfill existing rows (<code>initial_load = "copy"</code>) on newly checked
    tables</label>
</div>
<div class=tables id=tables></div>

<div class=bar>
  <button class=primary id=save>Save changes</button>
  <button id=reload>Reload config</button>
  <button id=refresh>Refresh</button>
</div>
<div id=out></div>

<script>
let S = null, want = {};

const el = id => document.getElementById(id);
const esc = s => String(s).replace(/[<&]/g, c => c === '<' ? '&lt;' : '&amp;');

function field(section, name, kind, value) {
  const attrs = `data-section="${section}" data-name="${name}"`;
  if (kind === 'bool')
    return `<div><label>${name}</label><input type=checkbox ${attrs}` +
      `${value ? ' checked' : ''}></div>`;
  const secret = kind === 'secret';
  const v = secret ? '' : (value === undefined ? '' : value);
  const ph = secret ? (value ? 'unchanged' : 'unset') : '';
  return `<div><label for="${section}.${name}">${name}</label>` +
    `<input type=text id="${section}.${name}" ${attrs} ` +
    `value="${esc(v)}" placeholder="${ph}"></div>`;
}

function render() {
  const st = S.status, cfg = S.config;
  el('sub').textContent = S.socket;
  el('status').innerHTML = [
    ['state', st.paused ? 'paused' : 'running'],
    ['rows synced', (st.rows_synced ?? 0).toLocaleString()],
    ['backfills pending', st.backfills_pending ?? 0],
    ['lag', `${(st.lag_bytes ?? 0).toLocaleString()} B / ${st.lag_seconds ?? 0}s`],
    ['uptime', `${st.uptime_secs ?? 0}s`],
  ].map(([k, v]) => `<div class=stat><b>${k}</b><span>${esc(v)}</span></div>`).join('');

  el('paused').checked = !!(cfg.stream && cfg.stream.paused);
  for (const [sec, spec] of [['ch', S.ch_fields], ['source', S.source_fields]])
    el(sec).innerHTML = spec
      .map(([n, kind]) => field(sec, n, kind, (cfg[sec] || {})[n])).join('');

  const sel = el('schema'), keep = sel.value;
  const namespaces = S.schemas.length ? S.schemas
    : [...new Set(S.tables.map(t => t.namespace))];
  sel.innerHTML = ['<option value="">all</option>']
    .concat(namespaces.map(n => `<option>${esc(n)}</option>`)).join('');
  sel.value = keep;
  renderTables();
}

function renderTables() {
  const ns = el('schema').value;
  const pinned = new Set(S.pinned);
  const rows = S.tables.filter(t => !ns || t.namespace === ns);
  if (S.tables_error)
    el('tables').innerHTML = `<div class="row err">source introspection failed: ` +
      `${esc(S.tables_error)}</div>`;
  else if (!rows.length)
    el('tables').innerHTML = '<div class=row>no tables</div>';
  else
    el('tables').innerHTML = rows.map(t => {
      const q = `${t.namespace}.${t.name}`;
      const on = q in want ? want[q] : t.selected;
      return `<div class=row><input type=checkbox data-table="${esc(q)}"` +
        `${on ? ' checked' : ''}><code>${esc(q)}</code>` +
        (pinned.has(q) ? '<span class=tag>pinned mapping</span>' : '') +
        `<span class=tag>ident ${esc(t.replica_identity)}</span></div>`;
    }).join('');
  el('tables').querySelectorAll('[data-table]').forEach(c => {
    c.onchange = () => { want[c.dataset.table] = c.checked; };
  });
}

function show(html) { el('out').innerHTML = html; }

async function api(path, body) {
  const r = await fetch(path, body ? {
    method: 'POST', headers: {'content-type': 'application/json'},
    body: JSON.stringify(body),
  } : undefined);
  return r.json();
}

async function load() {
  const d = await api('/api/state');
  if (d.error) return show(`<pre class=err>${esc(d.error)}</pre>`);
  S = d; want = {}; render();
}

el('schema').onchange = renderTables;
el('refresh').onclick = () => { show(''); load(); };

el('reload').onclick = async () => {
  const d = await api('/api/reload', {});
  show(d.error ? `<pre class=err>${esc(d.error)}</pre>` : '<pre>reloaded</pre>');
  load();
};

el('save').onclick = async () => {
  const form = {source: {}, ch: {}, paused: el('paused').checked, tables: want,
                initial_load: el('initial_load').checked};
  document.querySelectorAll('input[data-section]').forEach(i => {
    form[i.dataset.section][i.dataset.name] =
      i.type === 'checkbox' ? i.checked : i.value;
  });
  const d = await api('/api/save', form);
  const sent = Object.entries(d.sent || {})
    .map(([verb, body]) => `${verb}\\n\\n${body}`).join('\\n');
  if (d.error)
    show(`<pre class=err>${esc(d.error)}</pre>` + (sent ? `<pre>${esc(sent)}</pre>` : ''));
  else
    show(`<pre>${esc(sent || 'no changes')}</pre>`);
  load();
};

load();
</script>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "walshadow-ctl-ui"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *a):
        pass

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload, code=200):
        self._send(code, "application/json", json.dumps(payload).encode())

    def do_GET(self):
        if self.path == "/":
            self._send(200, "text/html; charset=utf-8", PAGE.encode())
        elif self.path == "/api/state":
            try:
                state = read_state(self.server.sock_path)
                state["socket"] = self.server.sock_path
                self._json(state)
            except (CtlError, OSError, tomllib.TOMLDecodeError) as e:
                self._json({"error": str(e)})
        else:
            self._send(404, "text/plain", b"not found\n")

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        try:
            form = json.loads(self.rfile.read(n) or b"{}")
        except ValueError as e:
            return self._json({"error": f"bad request: {e}"}, 400)
        try:
            if self.path == "/api/reload":
                ctl(self.server.sock_path, "reload")
                return self._json({"ok": True})
            if self.path == "/api/save":
                return self._json({"ok": True, "sent": save(self.server.sock_path, form)})
        except (CtlError, OSError, tomllib.TOMLDecodeError) as e:
            return self._json({"error": str(e)})
        self._send(404, "text/plain", b"not found\n")


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--socket", default=DEFAULT_SOCKET, help=f"default {DEFAULT_SOCKET}")
    p.add_argument("--bind", default="127.0.0.1:8087", help="host:port, default %(default)s")
    args = p.parse_args()
    host, _, port = args.bind.rpartition(":")
    httpd = ThreadingHTTPServer((host or "127.0.0.1", int(port)), Handler)
    httpd.sock_path = args.socket
    print(f"ctl-ui on http://{host or '127.0.0.1'}:{port} -> {args.socket}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
