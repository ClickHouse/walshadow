//! walshadow — schema-only Postgres + WAL replay catalog mirror for CDC.
//!
//! Phase 0: per-record classifier (`classify`).
//! Phase 1: WAL filter + CRC rewrite. Per-record keep/drop decision
//! (`filter`), byte-positioned walker (`segment`), in-place rewrite +
//! CRC32C (`rewrite`), live catalog tracking (`catalog_tracker`),
//! `main_data` reclassifier (`main_data`), full-segment orchestrator
//! (`filter_segment`), output manifest (`manifest`).

pub mod catalog_tracker;
pub mod classify;
pub mod filter;
pub mod filter_segment;
pub mod main_data;
pub mod manifest;
pub mod rewrite;
pub mod segment;
pub mod wire;
