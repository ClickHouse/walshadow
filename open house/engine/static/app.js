const $ = (id) => document.getElementById(id);

const COLORS = {
  price: '#ffffff',
  live: '#199e70',
  delayed: '#c98500',
  burst: 'rgba(255,255,255,0.10)',
  burstEdge: '#4a4a46',
  grid: '#383835',
  muted: '#898781',
  surface: '#1a1a19',
};

const state = {
  snap: null,
  lastRx: 0,
  connected: false,
  /// Server-to-browser clock offset, so a marker's server timestamp maps onto
  /// the same axis as the chart's source timestamps.
  skewMs: 0,
};

// ---------------------------------------------------------------- transport

function connect() {
  const es = new EventSource('/api/stream');
  es.onopen = () => { state.connected = true; };
  es.onerror = () => { state.connected = false; paint(); };
  es.onmessage = (ev) => {
    const snap = JSON.parse(ev.data);
    const now = Date.now();
    state.skewMs = now - Date.parse(snap.emitted_wall);
    state.snap = snap;
    state.lastRx = now;
    state.connected = true;
    paint();
  };
}

// ---------------------------------------------------------------- helpers

const fmtInt = (n) => Math.round(n).toLocaleString('en-US');
const fmtMs = (n) => (n == null ? '—' : (n < 10 ? n.toFixed(1) : Math.round(n).toLocaleString('en-US')));

function connState() {
  if (!state.snap) return ['recon', 'CONNECTING'];
  if (!state.connected) return ['down', 'RECONNECTING'];
  const age = (Date.now() - state.lastRx) / 1000;
  if (age > 2.5) return ['stale', 'STALE'];
  const s = state.snap;
  if (s.collector_error) return ['stale', 'FEATURES STALE'];
  if (s.source_stale_secs != null && s.source_stale_secs > 3) return ['stale', 'SOURCE STALE'];
  return ['ok', 'LIVE'];
}

// ---------------------------------------------------------------- chart

function drawChart() {
  const cv = $('chart');
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth, h = cv.clientHeight;
  if (cv.width !== w * dpr || cv.height !== h * dpr) {
    cv.width = w * dpr; cv.height = h * dpr;
  }
  const g = cv.getContext('2d');
  g.setTransform(dpr, 0, 0, dpr, 0, 0);
  g.clearRect(0, 0, w, h);

  const s = state.snap;
  if (!s || s.points.length < 2) return;

  const padL = 0, padR = 54, padT = 26, padB = 20;
  const x0 = padL, x1 = w - padR, y0 = padT, y1 = h - padB;

  const pts = s.points;
  const tMin = pts[0].ts_ms, tMax = pts[pts.length - 1].ts_ms;
  const tSpan = Math.max(1, tMax - tMin);

  let lo = Infinity, hi = -Infinity;
  for (const p of pts) { if (p.price_cents < lo) lo = p.price_cents; if (p.price_cents > hi) hi = p.price_cents; }
  const pad = Math.max(3, (hi - lo) * 0.22);
  lo = Math.max(0, lo - pad); hi = Math.min(100, hi + pad);
  const vSpan = Math.max(1, hi - lo);

  const X = (t) => x0 + ((t - tMin) / tSpan) * (x1 - x0);
  const Y = (v) => y1 - ((v - lo) / vSpan) * (y1 - y0);

  // recessive grid + right-hand price axis
  g.strokeStyle = COLORS.grid; g.lineWidth = 1;
  g.fillStyle = COLORS.muted;
  g.font = '500 11px ui-sans-serif, system-ui, sans-serif';
  g.textBaseline = 'middle';
  const ticks = 4;
  for (let i = 0; i <= ticks; i++) {
    const v = lo + (vSpan * i) / ticks;
    const y = Math.round(Y(v)) + 0.5;
    g.beginPath(); g.moveTo(x0, y); g.lineTo(x1, y); g.stroke();
    g.fillText(`${Math.round(v)}¢`, x1 + 8, y);
  }

  // burst window, shaded from the server's observed burst boundaries
  const bStart = s.burst_start_wall ? Date.parse(s.burst_start_wall) : null;
  const bEnd = s.burst_end_wall ? Date.parse(s.burst_end_wall) : null;
  if (bStart && bEnd && bEnd > tMin && bStart < tMax) {
    const a = X(Math.max(bStart, tMin)), b = X(Math.min(bEnd, tMax));
    g.fillStyle = COLORS.burst;
    g.fillRect(a, y0, Math.max(2, b - a), y1 - y0);
    g.strokeStyle = COLORS.burstEdge;
    g.setLineDash([3, 3]);
    g.beginPath(); g.moveTo(a, y0); g.lineTo(a, y1); g.moveTo(b, y0); g.lineTo(b, y1); g.stroke();
    g.setLineDash([]);
    g.fillStyle = COLORS.burstEdge;
    g.font = '600 10px ui-sans-serif, system-ui, sans-serif';
    g.fillText('BURST', a + 5, y0 + 8);
  }

  // price line
  g.strokeStyle = COLORS.price; g.lineWidth = 2;
  g.lineJoin = 'round'; g.lineCap = 'round';
  g.beginPath();
  pts.forEach((p, i) => { const x = X(p.ts_ms), y = Y(p.price_cents);
    i ? g.lineTo(x, y) : g.moveTo(x, y); });
  g.stroke();

  // alert markers at real emission times, never at animation times
  const run = s.current_run;
  if (run) {
    for (const [rec, color, label] of [
      [run.live, COLORS.live, 'LIVE'],
      [run.delayed, COLORS.delayed, '+5s'],
    ]) {
      if (!rec) continue;
      const t = Date.parse(rec.emitted_wall);
      if (t < tMin || t > tMax) continue;
      const x = X(t);
      g.strokeStyle = color; g.lineWidth = 2;
      g.beginPath(); g.moveTo(x, y0); g.lineTo(x, y1); g.stroke();
      // 2px surface ring keeps the marker legible where it overlaps the line
      g.beginPath(); g.arc(x, y0 + 10, 6, 0, Math.PI * 2);
      g.fillStyle = color; g.fill();
      g.strokeStyle = COLORS.surface; g.lineWidth = 2; g.stroke();
      g.fillStyle = color;
      g.font = '700 11px ui-sans-serif, system-ui, sans-serif';
      g.fillText(label, x + 10, y0 + 10);
    }
  }
}

// ---------------------------------------------------------------- cards

/// Live gauge, driven by the frame that consumer is evaluating right now. The
/// threshold sits at the bar's midpoint so crossing it is unmistakable, and the
/// bar keeps moving between alerts so a quiet panel still looks alive.
function paintGauge(which, view, threshold) {
  const el = $(`${which}-now`), fill = $(`${which}-fill`);
  const box = el.closest('.gauge');
  if (!view) {
    el.textContent = '—'; fill.style.width = '0%';
    box.className = 'gauge under';
    return;
  }
  el.innerHTML = `${view.buy_multiple.toFixed(1)}<small>×</small>`;
  const pct = Math.min(100, (view.buy_multiple / (threshold * 2)) * 100);
  fill.style.width = `${pct}%`;
  box.className = `gauge ${view.buy_multiple > threshold && view.gates_ok ? 'over' : 'under'}`;
}

function paintCard(which, rec, snap) {
  const card = $(`card-${which}`);
  const verdict = $(`${which}-verdict`);
  const line = $(`${which}-alert`);
  const idle = 'No unusual buying';

  if (!rec) {
    card.classList.remove('fired');
    verdict.className = 'verdict waiting';
    verdict.textContent = snap && snap.burst_active && which === 'delayed'
      ? 'Still seeing the old market' : idle;
    line.textContent = snap && snap.burst_active && which === 'delayed' ? 'Waiting…' : 'No alert yet';
    return;
  }
  card.classList.add('fired');
  verdict.className = 'verdict';
  verdict.textContent = 'Unusual buying detected';
  const cls = rec.during_burst ? 'during' : 'after';
  const outcome = rec.during_burst ? 'during burst' : 'after burst ended';
  line.innerHTML = `Alert in <span class="t num">${fmtMs(rec.ms_from_burst_start)}<small> ms</small></span>` +
                   ` · <span class="${cls}">${outcome}</span>`;
}

// ---------------------------------------------------------------- paint

function paint() {
  const s = state.snap;
  const [cls, text] = connState();
  $('conn').className = `conn ${cls === 'ok' ? '' : cls}`;
  $('conn-text').textContent = text;
  if (!s) return;

  $('market-name').textContent = s.market_name;
  const last = s.points.length ? s.points[s.points.length - 1].price_cents : null;
  $('price').textContent = last == null ? '—' : last;

  paintGauge('live', s.live_view, s.buy_multiple_min);
  paintGauge('delayed', s.delayed_view, s.buy_multiple_min);
  paintCard('live', s.current_run && s.current_run.live, s);
  paintCard('delayed', s.current_run && s.current_run.delayed, s);

  $('stat-tps').textContent = fmtInt(s.committed_per_s);
  $('stat-tps-w').textContent = s.rows_per_commit > 1
    ? `${fmtInt(s.rows_per_s)} rows/s · ${s.rows_per_commit} rows per commit · target ${fmtInt(s.target_rate)}`
    : `measured source commits · target ${fmtInt(s.target_rate)}`;

  const r = s.replication;
  const p95 = $('stat-p95');
  p95.innerHTML = r.count ? `${fmtMs(r.p95)}<small> ms</small>` : '—';
  p95.className = `v num${r.count && r.p95 > 500 ? ' miss' : ''}`;
  $('stat-p95-w').textContent = r.count
    ? `${r.count} probes over ${r.span_secs.toFixed(0)}s of a ${r.window_secs}s window` +
      (r.timeouts ? ` · ${r.timeouts} timed out` : '')
    : 'no probes observed yet';

  const live = s.current_run && s.current_run.live;
  const alertEl = $('stat-alert');
  alertEl.innerHTML = live ? `${fmtMs(live.ms_from_burst_start)}<small> ms</small>` : '—';

  // tape
  $('tape').innerHTML = s.tape.map((t) =>
    `<span class="t"><span class="${t.burst ? 'b' : (t.side === 'BUY' ? 'buy' : 'sell')}">${t.side}</span>` +
    `<span class="num">${t.quantity}@${t.price_cents}¢</span></span>`).join('');

  // countdown comes from the backend's accepted event
  const cd = $('countdown');
  if (s.countdown_ms != null && s.countdown_ms > 0) {
    cd.className = 'countdown armed';
    cd.innerHTML = `BURST STARTS IN <b>${Math.ceil(s.countdown_ms / 1000)}</b>`;
  } else if (s.burst_active) {
    cd.className = 'countdown armed';
    cd.innerHTML = `<b>BURST RUNNING</b>`;
  } else {
    cd.className = 'countdown';
    cd.innerHTML = 'NEXT BURST <b>—</b>';
  }

  // retained comparisons
  // A run that produced no alert is still a result and is kept, but it is
  // dimmed so a real comparison is what the eye lands on.
  $('history').innerHTML = s.history.map((h) => {
    if (!h.live && !h.delayed) {
      return `<span class="h miss">M${h.market_id} · no alert</span>`;
    }
    const l = h.live ? `${fmtMs(h.live.ms_from_burst_start)}ms` : '—';
    const d = h.delayed ? `${fmtMs(h.delayed.ms_from_burst_start)}ms` : '—';
    return `<span class="h">M${h.market_id} · <b>${l}</b> / <b>${d}</b></span>`;
  }).join('');

  drawChart();
  paintDrawer(s);
}

// ---------------------------------------------------------------- drawer

let evidence = null, samples = null;
async function refreshEvidence() {
  if (!$('drawer').classList.contains('open')) return;
  try {
    [evidence, samples] = await Promise.all([
      fetch('/api/evidence').then((r) => r.json()),
      fetch('/api/samples').then((r) => r.json()),
    ]);
  } catch { /* offstage only */ }
}

function paintDrawer(s) {
  if (!$('drawer').classList.contains('open')) return;
  const r = s.replication, q = s.query;
  const row = (k, v) => `<tr><th>${k}</th><td class="num">${v}</td></tr>`;
  $('ev-metrics').innerHTML =
    row('commit&rarr;visible p50 / p95 / p99', `${fmtMs(r.p50)} / ${fmtMs(r.p95)} / ${fmtMs(r.p99)} ms`) +
    row('&nbsp;&nbsp;of which source commit p50 / p95',
        `${fmtMs(s.source_commit.p50)} / ${fmtMs(s.source_commit.p95)} ms`) +
    row('replication max', `${fmtMs(r.max)} ms`) +
    row('replication samples', `${r.count} over ${r.span_secs.toFixed(1)}s (window ${r.window_secs}s)`) +
    row('replication timeouts', r.timeouts) +
    row('feature query p50 / p95 / p99', `${fmtMs(q.p50)} / ${fmtMs(q.p95)} / ${fmtMs(q.p99)} ms`) +
    row('feature query samples', q.count) +
    row('committed / rows per s', `${fmtInt(s.committed_per_s)} / ${fmtInt(s.rows_per_s)}`) +
    row('rows per commit', s.rows_per_commit) +
    row('generator errors', s.gen_errors) +
    row('simulated delay band', `${s.simulated_delay_min_ms}–${s.simulated_delay_max_ms} ms (jittered per frame)`) +
    row('actual delay right now',
        s.delayed_view ? `${fmtMs(s.delayed_view.data_age_ms)} ms` : '—') +
    row('row lateness created_at&rarr;arrival p50/p95/max',
        s.row_lateness
          ? `${fmtMs(s.row_lateness.p50_ms)} / ${fmtMs(s.row_lateness.p95_ms)} / ${fmtMs(s.row_lateness.max_ms)} ms (n=${s.row_lateness.rows})`
          : '—') +
    row('features stale', s.features_stale_ms == null ? '—' : `${fmtMs(s.features_stale_ms)} ms`) +
    row('source observer stale', s.source_stale_secs == null ? '—' : `${s.source_stale_secs.toFixed(2)} s`) +
    row('profile', s.profile) +
    row('collector error', s.collector_error || 'none') +
    row('detector gates',
        `baseline&ge;30 trades · recent&ge;10 · multiple&gt;${s.buy_multiple_min} · imbalance&gt;0.8`) +
    row('buy multiple now (live / delayed)',
        `${s.live_view ? s.live_view.buy_multiple.toFixed(2) : '—'} / ` +
        `${s.delayed_view ? s.delayed_view.buy_multiple.toFixed(2) : '—'}`) +
    row('markets evaluated per frame', s.live_view ? '1000 (watched: ' + s.live_view.market_id + ')' : '—') +
    (s.current_run ? row('run', `${s.current_run.run_id.slice(0, 8)} · market ${s.current_run.market_id} · ` +
        `${s.current_run.burst_rows} burst rows · countdown ${fmtMs(s.current_run.countdown_ms_measured)} ms (excluded)`) : '');

  if (samples) {
    $('ev-pg').innerHTML =
      '<tr><th>id</th><th>mkt</th><th>side</th><th>px</th><th>qty</th><th>created_at</th></tr>' +
      (samples.postgres || []).map((r) =>
        `<tr><td class="num">${r.id}</td><td class="num">${r.market_id}</td><td>${r.side}</td>` +
        `<td class="num">${r.price}¢</td><td class="num">${r.qty}</td><td class="num">${r.created_at}</td></tr>`).join('');
    $('ev-ch').innerHTML =
      '<tr><th>id</th><th>created_at</th><th>arrived_at</th><th>trip</th><th>_lsn</th></tr>' +
      (samples.clickhouse || []).map((r) =>
        `<tr><td class="num">${r.id}</td><td class="num">${r.created_at}</td>` +
        `<td class="num">${r.arrived_at}</td><td class="num hl">${r.trip_ms} ms</td>` +
        `<td class="num">${r.lsn}</td></tr>`).join('');
  }

  if (evidence && evidence.frames) {
    // Only completed pairings are informative. The newest frames are always
    // "pending" — the delay queue has not released them yet — so showing the
    // tail would show nothing but pending, every time.
    const paired = evidence.frames.filter((f) => f.actual_delay_ms != null);
    const inflight = evidence.frames.length - paired.length;
    const rows = paired.slice(-12).reverse().map((f) =>
      `<tr><td class="num">${f.frame_id}</td><td class="num">${f.markets_evaluated}</td>` +
      `<td class="num">${fmtMs(f.query_ms)}</td><td class="num">${f.read_rows == null ? '—' : fmtInt(f.read_rows)}</td>` +
      `<td class="num hl">${fmtMs(f.actual_delay_ms)} ms</td></tr>`).join('');
    $('ev-frames').innerHTML =
      '<tr><th>frame</th><th>markets</th><th>query ms</th><th>read rows</th><th>actual queue delay</th></tr>' +
      rows +
      `<tr><td colspan="5" style="color:var(--text-muted)">` +
      `${paired.length} paired · ${inflight} still held by the delay queue ` +
      `(≈${(s.simulated_delay_min_ms / 1000).toFixed(0)}–${(s.simulated_delay_max_ms / 1000).toFixed(0)}s of frames in flight)</td></tr>`;
    $('ev-sql').textContent = evidence.collector_sql || '—';
  }
}

// ---------------------------------------------------------------- input

document.addEventListener('keydown', (e) => {
  if (e.key === 'd' || e.key === 'D') {
    $('drawer').classList.toggle('open');
    if ($('drawer').classList.contains('open')) refreshEvidence().then(() => paint());
  }
  if (e.key === 'f' || e.key === 'F') {
    document.fullscreenElement ? document.exitFullscreen() : document.documentElement.requestFullscreen();
  }
});

window.addEventListener('resize', () => drawChart());
if (location.hash === '#details') {
  $('drawer').classList.add('open');
  refreshEvidence().then(() => paint());
}
setInterval(refreshEvidence, 2000);
// keeps the connection badge honest between pushes
setInterval(() => { if (state.snap) paint(); }, 500);
connect();
