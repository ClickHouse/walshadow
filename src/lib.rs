//! walshadow — schema-only Postgres + WAL replay catalog mirror for CDC.
//!
//! Phase 0: per-record classifier (`classify`).
//! Phase 1: WAL filter + CRC rewrite. Per-record keep/drop decision
//! (`filter`), byte-positioned walker (private `segment` reachable only
//! via `filter_segment`), in-place rewrite + CRC32C (`rewrite`), live
//! catalog tracking (`catalog_tracker`), `main_data` reclassifier
//! (`main_data`), full-segment orchestrator (`filter_segment`), output
//! manifest (`manifest`).
//! Phase 3: shadow PG lifecycle (`shadow`).
//! Phase 4: shadow PG catalog cache (`shadow_catalog`).
//! PRE5: pg_class heap-tuple decoder (`pg_class_decoder`), streaming
//! filter event design (`wal_stream`).
//! Phase 5: user-heap tuple decoder + Tier 1/2 type matrix
//! (`heap_decoder`).
//! Phase 6: per-xact + TOAST reassembly buffer (`xact_buffer`) backed
//! by an append-only per-xid spill file (`spill`).
//! Phase 7: ClickHouse-Native emitter (`ch_emitter`) — driven by the
//! top crate's `lz4` / `zstd` features which forward to clickhouse-c-rs.

pub mod catalog_tracker;
pub mod ch_emitter;
pub mod classify;
pub mod codecs;
pub mod decoder_sink;
pub mod filter;
pub mod filter_segment;
pub mod fpi;
pub mod heap_decoder;
pub mod main_data;
pub mod manifest;
pub mod metrics;
pub mod oracle;
pub mod pg_class_decoder;
pub mod preflight;
pub mod retention;
pub mod rewrite;
mod segment;
pub mod shadow;
pub mod shadow_catalog;
pub mod source_feed;
pub mod spill;
pub mod wal_stream;
pub mod xact_buffer;
