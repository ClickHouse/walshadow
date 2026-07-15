use anyhow::{Result, anyhow};
use walrus::pg::backup::{BackupSentinelDtoV2, TablespaceSpec};
use walrus::pg::replication::base_backup::Tablespace;

use crate::backfill::backup_source::{EndInfo, StartInfo};

pub(crate) fn build_lsn_pair(
    backup_name: &str,
    sentinel: &BackupSentinelDtoV2,
) -> Result<(StartInfo, EndInfo)> {
    let start_lsn = sentinel
        .sentinel
        .backup_start_lsn
        .ok_or_else(|| anyhow!("ObjectStoreSource: sentinel missing LSN (backup_start_lsn)"))?;
    let end_lsn = sentinel.sentinel.backup_finish_lsn.ok_or_else(|| {
        anyhow!("ObjectStoreSource: sentinel missing FinishLSN (backup_finish_lsn)")
    })?;
    let timeline = parse_timeline_from_name(backup_name)?;
    Ok((
        StartInfo {
            start_lsn: start_lsn.into(),
            timeline,
            tablespaces: tablespaces_from_spec(sentinel.sentinel.tablespace_spec.as_ref()),
        },
        EndInfo {
            end_lsn: end_lsn.into(),
            timeline,
        },
    ))
}

pub(crate) fn parse_timeline_from_name(name: &str) -> Result<u32> {
    walrus::pg::backup::parse_timeline_from_backup_name(name)
        .ok_or_else(|| anyhow!("ObjectStoreSource: cannot parse timeline from backup name: {name}"))
}

pub(crate) fn tablespaces_from_spec(spec: Option<&TablespaceSpec>) -> Vec<Tablespace> {
    let Some(spec) = spec else {
        return Vec::new();
    };
    spec.tablespace_names
        .iter()
        .filter_map(|name| {
            Some(Tablespace {
                oid: name.parse().ok()?,
                location: spec.locations.get(name)?.location.clone(),
                size: None,
            })
        })
        .collect()
}
