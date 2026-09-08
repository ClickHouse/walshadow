# Narration — two paragraphs, one command

Browser full screen (`F`). Market 101 warm for 31s+. Terminal on the next desktop.

---

## 1 — Intro (browser on screen)

> This is a simulated prediction market — a thousand of them, actually, and
> people are trading all of them right now. Every trade commits to Postgres,
> and ClickHouse is watching all thousand markets at once, looking for one
> thing: a sudden surge of buying in any single market. What gets the trades
> across is walshadow — it ships Postgres's physical WAL, the same bytes a
> replica would get, so the primary never decodes anything for us. Most CDC
> setups decode and batch and land somewhere in the seconds; that number at the
> bottom is ours, measured live, from commit on the source to queryable in
> ClickHouse. Both of these panels run the exact same alert rule on the exact
> same data. The only difference is when they get it. This side gets its
> numbers as fast as walshadow delivers them. This side gets identical numbers,
> five seconds later. Watch the gauges — that's buying pressure against each
> market's own normal, updating live. Right now, everything is quiet.

*(Point at REPLICATION p95 and COMMITTED TRADES/S. Read the p95 off the screen
— it moves, so don't quote a memorised figure.)*

---

## 2 — Run it, then switch back

```bash
./bin/shock.sh --market 101 --duration 2 --in 6
```

> I'm going to create two seconds of very heavy buying in one market.

Command returns instantly. **Switch to the browser now** — the burst has not
started. The countdown is on screen.

---

## 3 — Reveal (let the screen do the work)

Stay quiet through the burst. The left gauge spikes, the left card fires, the
price moves. Wait. The right gauge is still flat. Let the silence run until the
right card fires.

> That was five seconds. The market moved for two. Same rule, same data, same
> answer — one of them just found out while it was still happening, and the
> other found out after it was over. Look at the right-hand gauge now: it's
> reporting a surge of buying at a price that doesn't exist any more. It isn't
> wrong, it's late. If your application has a few seconds to react to
> something, seconds of delay are the whole budget.

*(Read the two alert times off the cards. Read replication p95 off the bottom
strip. Both are measured, not scripted.)*

---

## If it misses

Say it plainly and move on — the numbers on screen are real either way:

> That one didn't clear the threshold. The rule needs a genuine surge, not just
> a price move — that's the point of measuring it rather than animating it.

Re-arm on a **different** market (102, 103…). Same market inside ~30s will not
fire: the previous burst is still inside its own baseline, and the detector is
on a 30s cooldown.

---

## If challenged on the comparison

The defensible line is **seconds-scale vs sub-second**, never "everyone else is
slower than five seconds":

- ClickHouse's own ClickPipes for Postgres page advertises latency of *a few
  seconds*. That is our own product and a fair thing to cite.
- Other architectures and products can and do go lower. Never claim a universal
  floor.
- Logical decoding is not inherently slow — the honest contrast is the
  *approach*: physical WAL shipping avoids decoding work on the primary and
  avoids the batch-and-load step that usually sets the latency budget.
- The right-hand panel is **not** any competitor. It is the same data held back
  five seconds, to show what a delay of that scale costs an application. Say so
  if anyone reads it as a benchmark.

What we measured on this box, ~9k committed trades/s, 1000 markets:
replication p95 **80–230 ms**, feature query p95 **27–170 ms**, alert **150–370
ms** after the first burst commit. Quote what is on screen, not this.

## Don't say

- "Everything else is slower than five seconds" — the delayed panel is a
  controlled input delay, not a competitor benchmark.
- Anything about profit, fills, or execution. It's an alert, not a trade.
- That the alert predicted the move. It detected buying that already happened.
- That Kalshi or Polymarket use this. They don't; it's an inspiration.

Keep "simulated" audible at least once. It's on screen the whole time.
