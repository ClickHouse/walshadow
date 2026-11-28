//! Relations a backfill pass loads, and the walk counts it reports

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::schema::RelDescriptor;

#[derive(Debug, Clone)]
pub struct BackupRequest {
    pub desc: Arc<RelDescriptor>,
    pub s_lsn: u64,
}

/// Walk-phase counters a resumed pass reports rather than re-deriving from
/// files it skipped
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct WalkCounts {
    pub walked: u64,
    pub gated: u64,
    pub deferred: u64,
    pub multixact: u64,
    pub pg_xact_segments: usize,
}
