# robot:source

## sources of truth

- plans/source.md
- src/source_feed.rs (or equivalent — confirm via `ls src/`)
- src/wal_stream.rs
- src/queueing_record_sink.rs
- src/walsender_server*.rs

## subsumes

plans/source.md § sink composition / fan-out / QueueingRecordSink

## concept

ingestion → walker → CompositeRecordSink fan-out (three sinks, ordering matters: bytes→walsender, then record→queue, then segment→disk). QueueingRecordSink decouples pump task from worker task so decoder's wait_for_replay never stalls walsender wire

## clusters

| id | label | purpose |
|---|---|---|
| ingest | ingress (main loop, tokio) | wal-rs ReplicationConn + SourceFeed::pump |
| walk | WalStream / StreamingWalker | byte stream → CompletedRecord |
| fanout | CompositeRecordSink (pump task, sync) | three sink dispatch in order |
| queue | QueueingRecordSink (pump ↔ decoder decoupling) | mpsc batch hand-off |
| walsender | walsender server (tokio task) | accept + per-conn send queue + standby status rx |
| disk | on-disk segment | out/<seg> + manifest |

## key nodes

- src: "source PG" — #3D3D54, cylinder
- feed: "SourceFeed::pump\nIDENTIFY_SYSTEM +\nSTART_REPLICATION PHYSICAL" — #3D3D54
- chunks: "WalChunk stream\n(start_lsn, bytes)" — #3D3D54, parallelogram
- push: "WalStream::push\nStreamingWalker" — #4D3A28
- rec: "CompletedRecord\n(parsed, byte_ranges)" — #4D3A28, parallelogram
- bytesink: "❶ ShadowStreamSink\non_record_bytes\n(stays on pump)" — #4D3340
- recsink: "❷ QueueingRecordSink\non_record\n(enqueue, return)" — #4D3340
- segsink: "❸ DirSegmentSink\non_segment\n(16 MiB boundary)" — #4D3340
- qbatch: "pump-side batch\nVec<Record<'static>>\nbatch_size=64" — #4D3A28, parallelogram
- qchan: "unbounded mpsc\nbatches\nsoft cap → yield" — #4D3A28, parallelogram
- qwrk: "worker task\ndrains, fires on_idle" — #4D3A28
- qerr: "shared err slot" — #4D3A28, shape=note
- listener: "accept(unix / TCP)\nIDENTIFY_SYSTEM +\nSTART_REPLICATION" — #5D3F40
- sendq: "per-conn send queue\n'w' XLogData @ record\n'k' keepalive on idle" — #5D3F40
- statrx: "rx 'r' standby status\nflush / apply\n(min across conns)" — #5D3F40
- shd: "shadow PG\nwalreceiver" — #3D4128, cylinder
- decoder_out: "→ decode + xact buffer\n(see decoder.svg)" — #4D4128, shape=note (exit pointer)
- outdir: "out/<seg>\n+ manifest" — #4D3850, shape=note

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| src | feed | #A1A9CC, penwidth=2 | solid | START_REPLICATION |
| feed | chunks | default | solid | |
| chunks | push | default | solid | |
| push | rec | default | solid | |
| rec | bytesink | default | solid | ❶ |
| rec | recsink | default | solid | ❷ |
| rec | segsink | default | dashed | ❸ |
| bytesink | sendq | #BD8183, penwidth=2 | solid | hot wire |
| recsink | qbatch | default | solid | |
| qbatch | qchan | default | solid | |
| qchan | qwrk | default | solid | |
| qwrk | qerr | #B58B86 | dashed | error propagate |
| qwrk | decoder_out | default | dashed, constraint=false | |
| segsink | outdir | #6E6963 | solid | fsync |
| listener | sendq | default | dashed | per-conn |
| sendq | shd | #BD8183, penwidth=2 | solid | 'w' XLogData |
| shd | statrx | #BD8183 | dashed | 'r' standby |

## legend rows

- node-fill key (ingress, filter/queue, sinks, walsender server, shadow, disk)
- edge-color key (source replication, walsender wire, filesystem, error feedback)
- ordering note: fan-out fires ❶❷❸ in order, all on pump task

## layout hints

- rankdir=TB
- walsender cluster aligned right of fanout so bytesink edge runs horizontally
- queue cluster directly below fanout's recsink

## quality bar

- "stays on pump" vs "worker task" labels visually distinct
- ❶❷❸ ordering glyphs visible in fan-out nodes
- listener / sendq / statrx triangle reads as one walsender entity, not three disconnected
