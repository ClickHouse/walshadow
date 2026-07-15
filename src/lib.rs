//! Schema-only Postgres and WAL replay catalog mirror for CDC
//!
//! Keep WAL parsing internals private. Expose contract modules for schema,
//! records, mappings, PostgreSQL paths, ClickHouse transport, and backfill
//! requests

/// `info_span!(target: "walshadow::trace", …)` when `$on`, else a no-op span
/// (fields unevaluated on the unsampled path).
macro_rules! trace_span {
    ($on:expr, $($span:tt)+) => {
        if $on {
            tracing::info_span!(target: "walshadow::trace", $($span)+)
        } else {
            tracing::Span::none()
        }
    };
}

#[macro_use]
pub mod atomic_stats;
pub mod backfill;
pub mod budget;
pub mod catalog;
pub mod ch;
pub mod config;
pub mod decode;
pub mod emit;
pub mod filter;
pub mod fs;
pub mod mapping;
pub mod ops;
pub mod pg;
pub mod record;
pub mod runtime_config;
pub mod schema;
pub mod source;
pub mod toast;
pub mod xact;

#[doc(hidden)]
pub use backfill::{
    backfill_bootstrap, backfill_staging, backfill_types, backup_backfill, backup_page_walk,
    backup_sentinel, backup_source, backup_source_direct, backup_source_object_store,
    copy_backfill, opt_in, pg_path, spool,
};
#[doc(hidden)]
pub use catalog::{shadow, shadow_catalog, type_bridge};
#[doc(hidden)]
pub use decode::{codecs, decoder_sink, fpi, heap_decoder, visibility, wal_xact};
#[doc(hidden)]
pub use emit::{ch_ddl, ch_emitter, pipeline};
#[doc(hidden)]
pub use filter::{catalog_tracker, classify, filter_segment, main_data, pg_class_decoder, rewrite};
#[doc(hidden)]
pub use ops::{control, metrics, oracle, preflight, retention, trace};
#[doc(hidden)]
pub use source::{
    manifest, queueing_record_sink, segment_sink, shadow_stream, source_feed, wal_stream,
};
#[doc(hidden)]
pub use toast::toast_retire;
#[doc(hidden)]
pub use xact::{spill, xact_buffer};
