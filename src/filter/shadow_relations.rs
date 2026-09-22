//! Persist replay eligibility, recovery can recreate files without their pages
//!
//! File carries the shadow cluster's system identifier from `pg_control`
//! beside it, so eligibility copied from another cluster fails load

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ahash::{HashMap, HashSet};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

const FILE: &str = "walshadow_relations.toml";

/// Share relation membership outside routing task
///
/// Keep per-record routing reads in [`ShadowRelations`] free of locks
/// Less frequent opt-in checks can take a lock
#[derive(Clone, Default)]
pub struct ShadowHeld(Arc<Mutex<HashSet<(u32, u32)>>>);

impl ShadowHeld {
    pub fn contains(&self, rel: (u32, u32)) -> bool {
        self.0.lock().expect("shadow held poisoned").contains(&rel)
    }

    fn insert(&self, rel: (u32, u32)) {
        self.0.lock().expect("shadow held poisoned").insert(rel);
    }
}

impl FromIterator<(u32, u32)> for ShadowHeld {
    fn from_iter<I: IntoIterator<Item = (u32, u32)>>(rels: I) -> Self {
        Self(Arc::new(Mutex::new(rels.into_iter().collect())))
    }
}

#[derive(Serialize, Deserialize)]
struct Stored {
    version: u32,
    #[serde(default)]
    system_id: Option<u64>,
    creates_from: u64,
    relations: Vec<(u32, u32, u64)>,
}

pub struct ShadowRelations {
    rels: HashMap<(u32, u32), u64>,
    held: ShadowHeld,
    creates_from: u64,
    dir: Option<PathBuf>,
    dirty: bool,
}

impl ShadowRelations {
    pub fn new(rels: HashSet<(u32, u32)>, creates_from: u64) -> Self {
        Self {
            held: rels.iter().copied().collect(),
            rels: rels.into_iter().map(|r| (r, 0)).collect(),
            creates_from,
            dir: None,
            dirty: true,
        }
    }

    pub fn contains(&self, rel: &(u32, u32)) -> bool {
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

    pub(super) fn keeps(&self, rel: (u32, u32), lsn: u64) -> bool {
        self.rels.get(&rel).is_some_and(|from| lsn >= *from)
    }

    pub(super) fn admit(&mut self, rel: (u32, u32), lsn: u64) {
        if lsn < self.creates_from || self.rels.contains_key(&rel) {
            return;
        }
        self.rels.insert(rel, lsn);
        self.held.insert(rel);
        self.dirty = true;
        tracing::info!(
            db = rel.0,
            filenode = rel.1,
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
            stored.version == 1,
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
        for (db, rel, lsn) in stored.relations {
            ensure!(
                lsn == 0 || lsn >= stored.creates_from,
                "invalid shadow relation creation LSN"
            );
            ensure!(
                rels.insert((db, rel), lsn).is_none(),
                "duplicate shadow relation"
            );
        }
        Ok(Self {
            held: rels.keys().copied().collect(),
            rels,
            creates_from: stored.creates_from,
            dir: Some(dir.to_path_buf()),
            // Stamp a file predating the identifier at next flush
            dirty: stored.system_id.is_none() && control.is_some(),
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
        let mut relations: Vec<_> = self
            .rels
            .iter()
            .map(|(&(db, rel), &lsn)| (db, rel, lsn))
            .collect();
        relations.sort_unstable();
        let raw = toml::to_string(&Stored {
            version: 1,
            system_id: control_system_id(dir).await?,
            creates_from: self.creates_from,
            relations,
        })?;
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

    #[tokio::test]
    async fn held_tracks_seed_admissions_and_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let mut rels = ShadowRelations::new(HashSet::from_iter([(5, 17000)]), 100);
        let held = rels.held();
        assert!(held.contains((5, 17000)));
        assert!(!held.contains((5, 17001)));

        // Below `creates_from` stays out of both views
        rels.admit((5, 17001), 99);
        assert!(!held.contains((5, 17001)));
        rels.admit((5, 17001), 120);
        assert!(held.contains((5, 17001)));

        rels.persist(tmp.path()).await.unwrap();
        let reloaded = ShadowRelations::load(tmp.path()).await.unwrap();
        let held = reloaded.held();
        assert!(held.contains((5, 17000)) && held.contains((5, 17001)));
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
    async fn refuse_missing_or_invalid_eligibility() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(ShadowRelations::load(tmp.path()).await.is_err());
        for raw in [
            "version = 2\ncreates_from = 100\nrelations = []\n",
            "version = 1\ncreates_from = 100\nrelations = [[5, 17000, 99]]\n",
            "version = 1\ncreates_from = 100\nrelations = [[5, 17000, 0], [5, 17000, 120]]\n",
            "version = 1\n",
        ] {
            tokio::fs::write(tmp.path().join(FILE), raw).await.unwrap();
            assert!(ShadowRelations::load(tmp.path()).await.is_err());
        }
    }
}
