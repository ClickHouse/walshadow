//! walshadow — schema-only Postgres + WAL replay catalog mirror for CDC.
//!
//! `segment` is private, reachable only via `filter_segment`. `wal_page`
//! is the shared PG-15+ page-header parse for both segment walkers.
//! `ch_emitter` is driven by the top crate's `lz4`/`zstd` features which
//! forward to clickhouse-c-rs. `cursor` boot path consults it before
//! reverting to greenfield. `xact_buffer` is backed by an append-only
//! per-xid `spill` file.

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
