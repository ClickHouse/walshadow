use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{Mutex, watch};
use walrus::pg::replication::conn::PgConfig;

use crate::budget::MemoryBudget;
use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::mapping::MappingHandle;
use crate::schema::RelDescriptor;

#[derive(Debug, Clone)]
pub struct BackupRequest {
    pub desc: Arc<RelDescriptor>,
    pub s_lsn: u64,
}

pub struct PassContext {
    pub pg: PgConfig,
    pub emitter: EmitterConfig,
    pub mapping: MappingHandle,
    pub stats: Arc<EmitterStats>,
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub scratch_dir: PathBuf,
    pub config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
    pub budget: Option<MemoryBudget>,
}

#[derive(Debug, Default, Clone)]
pub struct PassOutcome {
    pub rows_walked: u64,
    pub rows_gated: u64,
    pub rows_deferred: u64,
    pub multixact_emitted: u64,
    pub rows_replayed: u64,
    pub replay_commits_past_s: u64,
    pub gap_segments: u32,
    pub b_redo: u64,
    pub pg_xact_segments: usize,
    pub pg_xact_patch_len: usize,
}
