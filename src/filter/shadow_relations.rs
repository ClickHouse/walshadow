//! Persist replay eligibility, recovery can recreate files without their pages
//!
//! File carries the shadow cluster's system identifier from `pg_control`
//! beside it, so eligibility copied from another cluster fails load

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, ensure};
use foldhash::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use walrus::pg::walparser::RelFileNode;

use crate::schema::DEFAULT_TABLESPACE_OID;

const FILE: &str = "walshadow_relations.toml";
const VERSION: u32 = 2;

/// Share relation membership outside routing task
///
/// Keep per-record routing reads in [`ShadowRelations`] free of locks
/// Less frequent opt-in checks can take a lock
#[derive(Clone, Default)]
pub struct ShadowHeld(Arc<Mutex<HashSet<RelFileNode>>>);

impl ShadowHeld {
    pub fn contains(&self, rel: RelFileNode) -> bool {
        self.0.lock().expect("shadow held poisoned").contains(&rel)
    }

    fn insert(&self, rel: RelFileNode) {
        self.0.lock().expect("shadow held poisoned").insert(rel);
    }
}

impl FromIterator<RelFileNode> for ShadowHeld {
    fn from_iter<I: IntoIterator<Item = RelFileNode>>(rels: I) -> Self {
        Self(Arc::new(Mutex::new(rels.into_iter().collect())))
    }
}

#[derive(Serialize, Deserialize)]
struct Stored {
    version: u32,
    #[serde(default)]
    system_id: Option<u64>,
    creates_from: u64,
    relations: Vec<StoredRel>,
}

/// `(tablespace, database, filenode, created at)`. Version 1 omitted the
/// tablespace, written while shadow held default tablespace files only
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum StoredRel {
    V2(u32, u32, u32, u64),
    V1(u32, u32, u64),
}

pub struct ShadowRelations {
    rels: HashMap<RelFileNode, u64>,
    held: ShadowHeld,
    creates_from: u64,
    dir: Option<PathBuf>,
    dirty: bool,
}

impl ShadowRelations {
    pub fn new(rels: HashSet<RelFileNode>, creates_from: u64) -> Self {
        Self {
            held: rels.iter().copied().collect(),
            rels: rels.into_iter().map(|r| (r, 0)).collect(),
            creates_from,
            dir: None,
            dirty: true,
        }
    }

    pub fn contains(&self, rel: &RelFileNode) -> bool {
        self.rels.contains_key(rel)
    }

    /// Membership handle for opt-in admission
    pub fn held(&self) -> ShadowHeld {
        self.held.clone()
    }

    pub fn len(&self) -> usize {
        self.rels.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rels.is_empty()
    }

    pub(super) fn keeps(&self, rel: RelFileNode, lsn: u64) -> bool {
        self.rels.get(&rel).is_some_and(|from| lsn >= *from)
    }

    pub(super) fn admit(&mut self, rel: RelFileNode, lsn: u64) {
        if lsn < self.creates_from || self.rels.contains_key(&rel) {
            return;
        }
        self.rels.insert(rel, lsn);
        self.held.insert(rel);
        self.dirty = true;
        tracing::info!(
            tablespace = rel.spc_node,
            db = rel.db_node,
            filenode = rel.rel_node,
            lsn,
            "admitted shadow relation from WAL create"
        );
    }

    pub async fn load(dir: &Path) -> Result<Self> {
        let path = dir.join(FILE);
        let raw = tokio::fs::read_to_string(&path).await.with_context(|| {
            format!("read {}; replay eligibility cannot be inferred from relation files, rebootstrap shadow if missing", path.display())
        })?;
        let stored: Stored = toml::from_str(&raw).context("parse shadow replay eligibility")?;
        ensure!(
            stored.version == 1 || stored.version == VERSION,
            "unsupported shadow replay eligibility version"
        );
        let control = control_system_id(dir).await?;
        if let Some(stored_id) = stored.system_id {
            ensure!(
                control == Some(stored_id),
                "{} belongs to system {stored_id}, shadow pg_control says {control:?}; \
                 rebootstrap shadow",
                path.display(),
            );
        }
        let mut rels = HashMap::default();
        for entry in stored.relations {
            let (rfn, lsn) = match entry {
                StoredRel::V2(spc_node, db_node, rel_node, lsn) if stored.version == VERSION => (
                    RelFileNode {
                        spc_node,
                        db_node,
                        rel_node,
                    },
                    lsn,
                ),
                StoredRel::V1(db_node, rel_node, lsn) if stored.version == 1 => (
                    RelFileNode {
                        spc_node: DEFAULT_TABLESPACE_OID,
                        db_node,
                        rel_node,
                    },
                    lsn,
                ),
                _ => anyhow::bail!("shadow relation entry does not match version"),
            };
            ensure!(
                lsn == 0 || lsn >= stored.creates_from,
                "invalid shadow relation creation LSN"
            );
            ensure!(rels.insert(rfn, lsn).is_none(), "duplicate shadow relation");
        }
        Ok(Self {
            held: rels.keys().copied().collect(),
            rels,
            creates_from: stored.creates_from,
            dir: Some(dir.to_path_buf()),
            // Restamp a file predating identifier or tablespaces at next flush
            dirty: (stored.system_id.is_none() && control.is_some()) || stored.version != VERSION,
        })
    }

    pub async fn persist(&mut self, dir: &Path) -> Result<()> {
        self.dir = Some(dir.to_path_buf());
        self.dirty = true;
        self.flush().await
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        let Some(dir) = self.dir.as_ref().filter(|_| self.dirty) else {
            return Ok(());
        };
        let mut relations: Vec<_> = self.rels.iter().map(|(&rfn, &lsn)| (rfn, lsn)).collect();
        relations.sort_unstable();
        let raw = toml::to_string(&Stored {
            version: VERSION,
            relations: relations
                .into_iter()
                .map(|(r, lsn)| StoredRel::V2(r.spc_node, r.db_node, r.rel_node, lsn))
                .collect(),
            system_id: control_system_id(dir).await?,
            creates_from: self.creates_from,
        })
        .expect("shadow relations serialize");
        crate::fs::write_atomic(dir, FILE, raw.as_bytes())
            .await
            .context("persist shadow replay eligibility before WAL publication")?;
        self.dirty = false;
        Ok(())
    }
}

/// `ControlFileData.system_identifier`, its leading field in host byte
/// order; `None` without a cluster in `dir`
async fn control_system_id(dir: &Path) -> Result<Option<u64>> {
    let path = dir.join("global/pg_control");
    let raw = match tokio::fs::read(&path).await {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let id = raw
        .first_chunk::<8>()
        .with_context(|| format!("{} too short", path.display()))?;
    Ok(Some(u64::from_ne_bytes(*id)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rfn(spc_node: u32, rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node,
            db_node: 5,
            rel_node,
        }
    }

    #[tokio::test]
    async fn held_tracks_seed_admissions_and_reload() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(ShadowRelations::new(HashSet::default(), 100).is_empty());
        let mut rels = ShadowRelations::new(HashSet::from_iter([rfn(1663, 17000)]), 100);
        assert!(!rels.is_empty());
        let held = rels.held();
        assert!(held.contains(rfn(1663, 17000)));
        assert!(!held.contains(rfn(1663, 17001)));
        assert!(!held.contains(rfn(16385, 17000)));

        // Below `creates_from` stays out of both views
        rels.admit(rfn(16385, 17001), 99);
        assert!(!held.contains(rfn(16385, 17001)));
        rels.admit(rfn(16385, 17001), 120);
        assert!(held.contains(rfn(16385, 17001)));

        rels.persist(tmp.path()).await.unwrap();
        let reloaded = ShadowRelations::load(tmp.path()).await.unwrap();
        let held = reloaded.held();
        assert!(held.contains(rfn(1663, 17000)) && held.contains(rfn(16385, 17001)));
    }

    /// Version 1 predates tablespaces, when shadow held `base/` files only
    #[tokio::test]
    async fn version_one_reads_as_default_tablespace_and_upgrades() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(
            tmp.path().join(FILE),
            "version = 1\ncreates_from = 100\nrelations = [[5, 17000, 0]]\n",
        )
        .await
        .unwrap();
        let mut rels = ShadowRelations::load(tmp.path()).await.unwrap();
        assert!(rels.contains(&rfn(DEFAULT_TABLESPACE_OID, 17000)));
        rels.flush().await.unwrap();
        let raw = tokio::fs::read_to_string(tmp.path().join(FILE))
            .await
            .unwrap();
        assert!(raw.contains("version = 2"), "{raw}");
        assert!(raw.contains("1663, 5, 17000, 0"), "{raw}");
    }

    #[tokio::test]
    async fn stamps_control_identity_and_refuses_another_cluster() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(
            tmp.path().join(FILE),
            "version = 1\ncreates_from = 100\nrelations = [[5, 17000, 0]]\n",
        )
        .await
        .unwrap();
        tokio::fs::create_dir(tmp.path().join("global"))
            .await
            .unwrap();
        let control = tmp.path().join("global/pg_control");
        tokio::fs::write(&control, 42u64.to_ne_bytes())
            .await
            .unwrap();

        let mut rels = ShadowRelations::load(tmp.path()).await.unwrap();
        rels.flush().await.unwrap();
        let raw = tokio::fs::read_to_string(tmp.path().join(FILE))
            .await
            .unwrap();
        assert!(raw.contains("system_id = 42"), "{raw}");

        tokio::fs::write(&control, 43u64.to_ne_bytes())
            .await
            .unwrap();
        let err = ShadowRelations::load(tmp.path()).await.err().unwrap();
        assert!(err.to_string().contains("belongs to system 42"), "{err}");
    }

    #[tokio::test]
    async fn unreadable_or_short_control_file_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let control = tmp.path().join("global/pg_control");
        tokio::fs::create_dir_all(&control).await.unwrap();
        let err = control_system_id(tmp.path()).await.unwrap_err();
        assert!(err.to_string().starts_with("read "), "{err}");
        tokio::fs::remove_dir(&control).await.unwrap();
        tokio::fs::write(&control, [1, 2, 3]).await.unwrap();
        let err = control_system_id(tmp.path()).await.unwrap_err();
        assert!(err.to_string().ends_with("too short"), "{err}");
    }

    #[tokio::test]
    async fn refuse_missing_or_invalid_eligibility() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(ShadowRelations::load(tmp.path()).await.is_err());
        for raw in [
            "version = 3\ncreates_from = 100\nrelations = []\n",
            "version = 2\ncreates_from = 100\nrelations = [[5, 17000, 0]]\n",
            "version = 1\ncreates_from = 100\nrelations = [[1663, 5, 17000, 0]]\n",
            "version = 1\ncreates_from = 100\nrelations = [[5, 17000, 99]]\n",
            "version = 1\ncreates_from = 100\nrelations = [[5, 17000, 0], [5, 17000, 120]]\n",
            "version = 1\n",
        ] {
            tokio::fs::write(tmp.path().join(FILE), raw).await.unwrap();
            assert!(ShadowRelations::load(tmp.path()).await.is_err());
        }
    }
}
