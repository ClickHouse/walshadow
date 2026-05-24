# robot:emitter

## sources of truth

- plans/emitter.md
- src/ch_emitter.rs (or per `ls src/ | grep -iE 'ch_|emit'`)
- src/ch_ddl.rs
- src/type_bridge.rs
- clickhouse-c-rs/

## subsumes

plans/emitter.md § "Held-open INSERT" + "DdlApplicator" + "type_bridge"

## concept

commit-drain → TableEncoder per (table) → BlockBuilder accumulates Native columns + synthetic _lsn/_xid/_op/_commit_ts → held-open INSERT over single CH TCP (send_query once, send_data(Some) per block, send_data(None) on deadline). DdlApplicator runs on SECOND CH TCP applying SchemaEvent → CREATE / ALTER / RENAME / DROP. await_ready gate sequences DDL before dependent rows

## clusters

| id | label | purpose |
|---|---|---|
| in | commit drain in | CommittedTuple + SchemaEvent |
| table | per-table encoder | TableEncoder route + BlockBuilder |
| insert | held-open INSERT state | send_query / send_data lifecycle |
| ddl | DDL applicator (separate CH TCP) | type_bridge + apply |
| out | ClickHouse | two TCP endpoints |

## key nodes

- drain_in: "← xact drain\nCommittedTuple per row\nSchemaEvent (drain-interleaved)" — #4D4128, shape=note
- route: "route by table\nTableEncoder lookup" — #5D4628
- block: "BlockBuilder\nNative columns +\n_lsn / _xid / _op / _commit_ts" — #5D4628
- append: "TableEncoder::append_row\nrow + byte budget" — #5D4628
- budget: "row/byte budget hit?\nflush_timeout?" — #5D4628
- sendq: "if INSERT not open:\nsend_query INSERT ...\nstart deadline timer" — #5D4628
- senddata: "send_data(Some(block))\nper block" — #5D4628
- holdopen: "held open\nacross xacts" — #5D4628, shape=note
- deadline: "send_data(None)\non flush_timeout\nemitter_ack_lsn ↑" — #5D4628
- await: "await_ready\ngate" — #5D4628
- bridge: "type_bridge\nRelAttr → CH TypeAst\n(reject widen for now)" — #5D4628
- ddlapp: "DdlApplicator\nCREATE / ADD COL /\nRENAME / DROP TABLE" — #5D4628
- ch_rows: "ClickHouse TCP 1\nrows\nReplacingMergeTree(_lsn)" — #4D4128, cylinder
- ch_ddl: "ClickHouse TCP 2\nDDL session" — #4D4128, cylinder
- ack_out: "→ xact buffer\nemitter_ack_lsn" — #4D4128, shape=note

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| drain_in | route | default | solid | tuple |
| route | block | default | solid | per-table |
| block | append | default | solid | |
| append | budget | default | solid | |
| budget | sendq | default | solid | first row |
| budget | senddata | default | solid | block full |
| sendq | senddata | default | solid | |
| senddata | holdopen | default | dashed | |
| holdopen | deadline | default | dashed | flush_timeout |
| senddata | ch_rows | #BF8C5F, penwidth=2 | solid | Native block |
| deadline | ch_rows | #BF8C5F | dashed | EndOfStream |
| deadline | ack_out | #b380b0 | dotted | emitter_ack_lsn |
| drain_in | await | default | dashed | SchemaEvent |
| await | bridge | default | solid | |
| bridge | ddlapp | default | solid | |
| ddlapp | ch_ddl | #BF8C5F | dashed, penwidth=2 | DDL SQL |
| await | route | default | dotted, constraint=false | gate row INSERT until DDL done |

## legend rows

- node-fill key (CH emitter, ClickHouse, cross-component)
- edge-color key (CH Native rows solid, CH DDL dashed, cursor durability)
- synthetic columns subtable:
  - _lsn ← commit_lsn (ReplacingMergeTree dedup key)
  - _xid ← record xid
  - _op ← INSERT / UPDATE / DELETE / TRUNCATE
  - _commit_ts ← xl_xact_commit.xact_time
- await_ready note: DDL completes on TCP 2 before rows for same table flow on TCP 1

## layout hints

- rankdir=LR (main row flow)
- ddl cluster vertically below row flow, second CH endpoint distinct
- await_ready gate edge crosses laterally with constraint=false

## quality bar

- two CH endpoints visually distinct (separate cylinders, both labelled with TCP number)
- rows path solid #BF8C5F, DDL path dashed #BF8C5F — same color, different style — reads as "same wire family, separate stream"
- send_query → send_data(Some) → send_data(None) lifecycle clear with deadline annotation
