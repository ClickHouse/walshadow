//! walshadow — schema-only Postgres + WAL replay catalog mirror for CDC.
//!
//! Phase 0 ships only the WAL record classifier used by later phases to
//! split source-WAL records into catalog-keep / user-drop / special-keep
//! buckets before replay on shadow Postgres. See PLAN.md & PHASE0.md.

pub mod classify;
