# robot:INDEX

Machine-readable diagram specs. Not for humans. Pair each `architecture/<comp>.dot` with this directory's `<comp>.md` for regeneration intent

## shared

- [_palette.md](_palette.md) — palette, style defaults, edge channels, legend conventions, quality bar

## system-level (architecture/)

| diagram | spec | scope |
|---|---|---|
| overview.svg | n/a (stable; in tree) | 30-second view: 5 actors, 6 wires |
| internals.svg | n/a (stable; in tree) | full pipeline, taps, caches |
| shadow_communication.svg | n/a (stable; in tree) | three channels walshadow↔shadow |
| timeline_bootstrap.svg | n/a (stable; in tree) | greenfield 5-phase timeline |
| timeline_streaming.svg | n/a (stable; in tree) | one record's journey |
| timeline_restart.svg | n/a (stable; in tree) | clean/kill/overflow restart scenarios |

## component-level

| component | spec | embeds in |
|---|---|---|
| filter | [filter.md](filter.md) | plans/filter.md |
| source | [source.md](source.md) | plans/source.md |
| shadow | [shadow.md](shadow.md) | plans/shadow.md |
| decoder | [decoder.md](decoder.md) | plans/decoder.md |
| xact | [xact.md](xact.md) | plans/xact.md |
| emitter | [emitter.md](emitter.md) | plans/emitter.md |
| bootstrap | [bootstrap.md](bootstrap.md) | plans/bootstrap.md |
| ops | [ops.md](ops.md) | plans/ops.md |
| oracle | [oracle.md](oracle.md) | plans/oracle.md |
| safety | [safety.md](safety.md) | plans/safety.md |

## regenerate workflow

agent receives: "regenerate architecture/<comp>.svg"
1. read [_palette.md](_palette.md) for style invariants
2. read [<comp>.md](<comp>.md) for concept, clusters, key nodes, edges, legend rows
3. read `plans/<comp>.md` for current implementation truth
4. read 1-2 src/ files cited in spec for accuracy anchor
5. write `architecture/<comp>.dot`
6. render svg + png
7. read png; iterate dot until quality bar passes
8. update `plans/<comp>.md` embed if .svg path changed

system-level specs absent because those six are stable and visually saturated. Add specs only on next material rewrite
