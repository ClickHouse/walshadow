//! Shim-local persistence: peer registry + the single mirror record.
//! Connection-parameter truth lives in walshadow-control's state; this
//! copy exists to echo `GetPeerInfo` and re-derive source/dest role on
//! peer reference. Single writer, same durability model as control's
//! `state.json`

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Source,
    Dest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerRecord {
    /// DBType name, `POSTGRES` / `CLICKHOUSE`
    pub db_type: String,
    pub role: Role,
    /// submitted `*_config` verbatim, echoed (redacted) by GetPeerInfo
    pub config: Value,
    pub created_at_unix: i64,
}

/// Dotted strings exist only at control-line interpolation; stored and
/// compared as (namespace, relname) pairs
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    pub namespace: String,
    pub relname: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MirrorRecord {
    pub name: String,
    /// echo of `flow_job_name`; no Temporal behind it
    pub workflow_id: String,
    pub source_name: String,
    pub destination_name: String,
    /// opt-in set of source tables
    pub tables: Vec<TableRef>,
    pub do_initial_snapshot: bool,
    pub created_at_unix: i64,
    /// submitted FlowConnectionConfigs verbatim, echoed by MirrorStatus
    pub config: Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ShimState {
    #[serde(default)]
    pub peers: BTreeMap<String, PeerRecord>,
    #[serde(default)]
    pub mirror: Option<MirrorRecord>,
    /// names of terminated mirrors; MirrorStatus answers
    /// STATUS_TERMINATED for these while ListMirrors stays empty
    #[serde(default)]
    pub terminated: Vec<String>,
}

impl ShimState {
    pub fn peer_by_role(&self, role: Role) -> Option<(&String, &PeerRecord)> {
        self.peers.iter().find(|(_, p)| p.role == role)
    }
}

pub struct Store {
    path: PathBuf,
    state: Mutex<ShimState>,
}

impl Store {
    pub async fn load(path: PathBuf) -> Result<Self> {
        let state = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parse state file {}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ShimState::default(),
            Err(e) => return Err(e).with_context(|| format!("read state file {}", path.display())),
        };
        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    pub async fn get(&self) -> ShimState {
        self.state.lock().await.clone()
    }

    /// Mutate under the lock, then persist; closure's return value is
    /// passed back to the caller
    pub async fn update<T>(&self, f: impl FnOnce(&mut ShimState) -> T) -> Result<T> {
        let mut guard = self.state.lock().await;
        let out = f(&mut guard);
        persist(&self.path, &guard).await?;
        Ok(out)
    }
}

async fn persist(path: &Path, state: &ShimState) -> Result<()> {
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("create state dir {}", dir.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(state).context("serialize state")?;
    tokio::fs::write(path, &bytes)
        .await
        .with_context(|| format!("write state file {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrips_state_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let store = Store::load(path.clone()).await.unwrap();
        store
            .update(|s| {
                s.peers.insert(
                    "pg".into(),
                    PeerRecord {
                        db_type: "POSTGRES".into(),
                        role: Role::Source,
                        config: serde_json::json!({"host": "db"}),
                        created_at_unix: 1,
                    },
                );
            })
            .await
            .unwrap();
        let reloaded = Store::load(path).await.unwrap();
        let state = reloaded.get().await;
        assert_eq!(state.peers["pg"].db_type, "POSTGRES");
        assert_eq!(state.peer_by_role(Role::Source).unwrap().0, "pg");
        assert!(state.peer_by_role(Role::Dest).is_none());
    }
}
