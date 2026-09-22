//! Resume completed backup walks without rebuilding staging or TOAST mirrors

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::backfill::backfill_staging::{StagingPlan, StagingSession};
use crate::backfill::backfill_types::{BackupRequest, WalkCounts};
use crate::backfill::spool::SpoolMark;
use crate::config::ResolvedConfig;
use crate::emit::ch_emitter::EmitterConfig;
use crate::mapping::MappingSnapshot;
use crate::runtime_config::InitialLoadMode;

const FILENAME: &str = "backup_replay.json";
/// Bumped whenever a phase's resume state changes shape; an older file loses
/// its resume and the pass restarts
const VERSION: u32 = 1;

/// FNV-1a digest of what a resume must not change, persisted so it must not
/// depend on build. Inputs render through `Debug`, so a build changing that
/// output reads as a mismatch, which restarts the pass rather than reusing
/// state it cannot vouch for
pub fn digest<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        let len = (part.len() as u64).to_le_bytes();
        for &b in len.iter().chain(part) {
            h = (h ^ u64::from(b)).wrapping_mul(PRIME);
        }
    }
    h
}

/// Page-walk progress. A heap file lands here only once every tuple it
/// produced is durable in staging or in one of the two spools below
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WalkState {
    pub done: bool,
    /// Cluster-relative paths, eg `base/5/16400.2`
    pub files: Vec<String>,
    /// Tar parts holding no SLRU file and no incomplete heap file, so a
    /// resumed walk need not fetch them at all
    pub parts: Vec<String>,
    /// Undecided-xid tuples the gate holds for walk EOF
    pub gate_deferred: SpoolMark,
    /// TOAST referrers the drain could not resolve yet
    pub toast_deferred: SpoolMark,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupCheckpoint {
    version: u32,
    identity: u64,
    pub staging: Vec<(String, String, String)>,
    pub backup: String,
    pub walk: WalkState,
    /// Lowest LSN a resumed gap replay must re-read: the oldest transaction
    /// still buffered when the last replayed segment closed
    pub replay_from: Option<u64>,
    /// Sealed extent of the deferred TOAST spool a resume reads
    pub spool: SpoolMark,
    /// Byte cursor into that spool, behind proven inserts
    pub offset: u64,
    pub rows: u64,
    /// Digest of the walked descriptor set, toast rels included
    pub catalog: u64,
    pub counts: WalkCounts,
}

impl BackupCheckpoint {
    pub fn new(
        mode: InitialLoadMode,
        requests: &[BackupRequest],
        mapping: &MappingSnapshot,
        emitter: &EmitterConfig,
        config: Option<&ResolvedConfig>,
    ) -> Self {
        let mut lines = Vec::new();
        for r in requests {
            let name = &r.desc.rel_name;
            let columns: Vec<_> = r
                .desc
                .attributes
                .iter()
                .map(|a| config.map(|c| c.column_rules.settings(name, &a.name)))
                .collect();
            lines.push(format!(
                "{:?}|{}|{:?}|{:?}|{:?}",
                r.desc,
                r.s_lsn,
                mapping.get(name),
                emitter.row_policy().for_rel(config, name),
                columns
            ));
        }
        lines.sort();
        lines.push(format!(
            "{mode:?}|{}|{:?}",
            emitter.inline_value_max, emitter.inline_value_overflow
        ));
        Self {
            version: VERSION,
            identity: digest(lines.iter().map(String::as_bytes)),
            staging: Vec::new(),
            backup: String::new(),
            walk: WalkState::default(),
            replay_from: None,
            spool: SpoolMark::default(),
            offset: 0,
            rows: 0,
            catalog: 0,
            counts: WalkCounts::default(),
        }
    }

    /// Walk finished: a resumed pass skips straight to deferred TOAST replay
    pub fn ready(&self) -> bool {
        self.walk.done
    }

    /// Mid-walk progress worth resuming from
    pub fn walk_started(&self) -> bool {
        !self.walk.done && !self.walk.files.is_empty()
    }

    /// Any phase progress a restart can pick up
    pub fn resuming(&self) -> bool {
        self.ready() || self.walk_started()
    }

    /// A file this build cannot vouch for is no resume, not a failure: the
    /// caller discards it and walks the backup again
    pub async fn load(dir: &Path) -> Result<Option<Self>> {
        let bytes = match tokio::fs::read(dir.join(FILENAME)).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let unusable = match serde_json::from_slice::<Self>(&bytes) {
            Ok(state) if state.version != VERSION => "checkpoint version".to_string(),
            Ok(state) if state.offset > state.spool.bytes => "cursor exceeds spool".to_string(),
            Ok(state) => return Ok(Some(state)),
            Err(e) => e.to_string(),
        };
        tracing::warn!(
            target: "walshadow::backfill",
            reason = unusable,
            "backup replay checkpoint unusable; walking the backup again",
        );
        Ok(None)
    }

    pub fn matches(&self, other: &Self) -> bool {
        self.identity == other.identity
    }

    pub async fn save(&self, dir: &Path) -> Result<()> {
        crate::fs::write_atomic(dir, FILENAME, &serde_json::to_vec(self)?).await?;
        Ok(())
    }

    pub async fn discard(dir: &Path) -> Result<()> {
        match tokio::fs::remove_file(dir.join(FILENAME)).await {
            Ok(()) => crate::fs::fsync_dir(dir).await?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub async fn capture_staging(
        &mut self,
        plan: &StagingPlan,
        session: &mut StagingSession,
    ) -> Result<()> {
        self.staging.clear();
        for rel in &plan.rels {
            let table = rel.staging_table();
            let uuid = session
                .table_uuid(&rel.database, &table)
                .await?
                .context("staging table disappeared before checkpoint")?;
            self.staging.push((rel.database.clone(), table, uuid));
        }
        Ok(())
    }

    /// False once a staging table the checkpoint named has been replaced:
    /// the rows it vouches for are gone, so the pass restarts
    pub async fn staging_intact(&self, session: &mut StagingSession) -> Result<bool> {
        for (db, table, uuid) in &self.staging {
            if session.table_uuid(db, table).await?.as_ref() != Some(uuid) {
                tracing::warn!(
                    target: "walshadow::backfill",
                    qname = format!("{db}.{table}"),
                    "backup replay staging identity changed; walking the backup again",
                );
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Persisted, so a pinned value catches any build-dependent drift
    #[test]
    fn digest_is_stable_across_builds() {
        assert_eq!(digest([b"a".as_slice(), b"bc"]), 0xba1e_1f0e_0704_d8ea);
        assert_ne!(
            digest([b"ab".as_slice(), b"c"]),
            digest([b"a".as_slice(), b"bc"])
        );
    }

    #[tokio::test]
    async fn checkpoint_drops_a_cursor_past_spool_and_preserves_previous_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = BackupCheckpoint::new(
            InitialLoadMode::ObjectStore,
            &[],
            &std::sync::Arc::default(),
            &EmitterConfig::default(),
            None,
        );
        c.spool.bytes = 100;
        c.offset = 40;
        c.save(dir.path()).await.unwrap();
        tokio::fs::write(dir.path().join(format!("{FILENAME}.tmp")), b"partial")
            .await
            .unwrap();
        assert_eq!(
            BackupCheckpoint::load(dir.path())
                .await
                .unwrap()
                .unwrap()
                .offset,
            40
        );
        c.offset = 101;
        c.save(dir.path()).await.unwrap();
        assert!(
            BackupCheckpoint::load(dir.path()).await.unwrap().is_none(),
            "an inconsistent file restarts the pass rather than failing it",
        );
        BackupCheckpoint::discard(dir.path()).await.unwrap();
        assert!(BackupCheckpoint::load(dir.path()).await.unwrap().is_none());
    }
}
