//! walshadow — schema-only Postgres + WAL replay catalog mirror for CDC.
//!
//! Per-record classifier (`classify`).
//! WAL filter + CRC rewrite. Per-record routing decision
//! (`filter`), byte-positioned walker (private `segment` reachable only
//! via `filter_segment`), in-place rewrite + CRC32C (`rewrite`), live
//! catalog tracking (`catalog_tracker`), `main_data` reclassifier
//! (`main_data`), full-segment orchestrator (`filter_segment`), output
//! manifest (`manifest`).
//! Shadow PG lifecycle (`shadow`).
//! Shadow PG catalog cache (`shadow_catalog`).
//! pg_class heap-tuple decoder (`pg_class_decoder`), streaming
//! filter event design (`wal_stream`). Shared PG-15+ WAL page-header
//! parse for both segment walkers (`wal_page`).
//! User-heap tuple decoder + Tier 1/2 type matrix
//! (`heap_decoder`).
//! Per-xact + TOAST reassembly buffer (`xact_buffer`) backed
//! by an append-only per-xid spill file (`spill`).
//! ClickHouse-Native emitter (`ch_emitter`) — driven by the
//! top crate's `lz4` / `zstd` features which forward to clickhouse-c-rs.
//! Durable resume cursor (`cursor`) sits next to the spill
//! files; the boot path consults it before reverting to greenfield.
//! File-streaming backup source trait (`backup_source`) with
//! Direct + ObjectStore impls, catalog-land + page-walk sinks, and the
//! greenfield bootstrap orchestrator (`backfill_bootstrap`).

#[macro_use]
pub mod atomic_stats;
pub mod backfill_bootstrap;
pub mod backup_page_walk;
pub mod backup_sink;
pub mod backup_source;
pub mod backup_source_direct;
pub mod backup_source_object_store;
pub mod catalog_tracker;
pub mod ch_ddl;
pub mod ch_emitter;
pub mod classify;
pub mod codecs;
pub mod cursor;
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
pub mod pipeline;
pub mod preflight;
pub mod queueing_record_sink;
pub mod relation_resolver;
pub mod retention;
pub mod rewrite;
pub mod segment;
pub mod shadow;
pub mod shadow_catalog;
pub mod shadow_stream;
pub mod source_feed;
pub mod spill;
pub mod streaming_walker;
pub mod type_bridge;
pub mod wal_page;
pub mod wal_stream;
pub mod xact_buffer;
