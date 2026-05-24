# robot:_palette

Shared style for all architecture/*.dot. Not for humans

## graph defaults

```
bgcolor   #272623
fontcolor #ECE1D7
fontname  Helvetica
fontsize  14 (graph), 10 (node), 9 (edge)
splines   spline
```

## cluster defaults

```
style     rounded,filled
color     #4c4641
fillcolor #34302c
fontcolor #ECE1D7
```

## node fills (actor)

| actor | fill |
|---|---|
| source PG / ingress | #3D3D54 |
| walshadow filter, queue, ingress (sync) | #4D3A28 |
| walshadow output sinks (walsender, segment) | #4D3340 |
| walshadow decoder + xact buffer | #4D4128 |
| walshadow ShadowCatalog + schema event | #4D4D28 |
| walshadow walsender server | #5D3F40 |
| walshadow CH emitter + DdlApplicator | #5D4628 |
| on-disk artifact | #4D3850 (shape=note) |
| shadow Postgres | #3D4128 |
| ClickHouse | #4D4128 |
| unsafe annotation (safety only) | #6D2D2D (dashed border) |

## edge colors (channel)

| channel | color | style |
|---|---|---|
| source replication frame | #A1A9CC | solid |
| walsender wire (hot path) | #BD8183 | solid; thick for primary |
| libpq catalog query | #CBA85E | solid (bidir on cache fill) |
| CH Native rows | #BF8C5F | solid |
| CH DDL SQL (separate TCP) | #BF8C5F | dashed |
| cursor durability | #b380b0 | dotted |
| filesystem / restore_command | #6E6963 | dashed |
| unsafe annotation (safety only) | #B58B86 | dashed, constraint=false |

## sidecar edges

cross-cutting cache feedback, error feedback, gate dependencies must use `constraint=false` + `style=dashed` so main column stays straight

## legend

every diagram ends in `legend [shape=plaintext, label=<HTML table>]`. Required rows:
- node-fill key (only fills present in diagram)
- edge-color key (only channels present)
- optional: domain-specific subtable (e.g. rmgr keep policy for filter)

last edge in graph: `<last_node> -> legend [style=invis];` to anchor

## render

```
cd architecture && dot -Tsvg <comp>.dot -o <comp>.svg && dot -Tpng <comp>.dot -o <comp>.png
```

## quality bar

human review pass for these properties:
- nodes don't overlap
- edges don't form spaghetti (curves cross at most 1-2 times in well-trafficked area)
- legend readable at 100% zoom
- clusters don't span >2x sibling widths
- no orphan nodes outside clusters
- thick edges (`penwidth=2,3`) reserved for primary channels
