use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};
use walrus::pg::replication::conn::PgConfig;

use crate::budget::MemoryBudget;
use crate::catalog::desc_log::DescriptorLog;
use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::mapping::MappingHandle;
use crate::ops::oracle::Oracle;
use crate::schema::RelDescriptor;
use crate::source::timeline::TimelineHistory;

#[derive(Debug, Clone)]
pub struct BackupRequest {
    pub desc: Arc<RelDescriptor>,
    pub s_lsn: u64,
}

pub struct PassContext {
    pub pg: PgConfig,
    pub emitter: Arc<EmitterConfig>,
    /// Routing for this pass's rows; staging targets while a pass is
    /// unpublished
    pub mapping: MappingHandle,
    /// Live published routing. Pending tables are siblings of the destination,
    /// not of staging, since a promote lands after the swap
    pub published: MappingHandle,
    pub stats: Arc<EmitterStats>,
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub log: Arc<DescriptorLog>,
    pub scratch_dir: PathBuf,
    pub config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
    /// Branch the stream proved, re-seated at a crossing: a backup pass names
    /// gap segments off it rather than re-deriving lineage from the archive
    pub history_rx: watch::Receiver<Arc<TimelineHistory>>,
    pub budget: Option<MemoryBudget>,
    pub oracle: Option<Arc<Oracle>>,
    /// Source PG major: picks the backup's pg_multixact offsets width
    pub source_major: u32,
    pub checkpoint: crate::backfill::backup_checkpoint::BackupCheckpoint,
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

#[derive(Debug, Default, Clone)]
pub struct PassOutcome {
    pub counts: WalkCounts,
    /// Undecided rows parked in pending tables
    pub rows_pending: u64,
    pub rows_replayed: u64,
    pub replay_commits_past_s: u64,
    pub gap_segments: u32,
    pub b_redo: u64,
    pub pg_xact_patch_len: usize,
    /// Pending tables the pass wrote; the caller records them once it publishes
    pub pending_tables: Vec<crate::backfill::visibility_pending::PendingManifest>,
}
