//! Object-store base-backup source. Fetches a wal-g compatible
//! BASE_BACKUP from a `DynStorage` bucket, decompresses each tar part,
//! pumps file events through [`BackupSink`].
//!
//! Layout (mirrors wal-g, owned by [`wal_rs::pg::backup`]):
//!
//! ```text
//! basebackups_005/
//!   <name>_backup_stop_sentinel.json   ← StartInfo / EndInfo
//!   <name>/
//!     metadata.json
//!     files_metadata.json              ← incremented-file lookup
//!     tar_partitions/
//!       part_001.tar.zst               ← data parts, processed first
//!       part_002.tar.zst                 in parallel up to `parallelism`
//!       pg_control.tar.zst             ← *always* drains last, single task
//! ```
//!
//! `pg_control` is a hard barrier: every other part drains before it
//! opens, matching PG recovery's expectation that pg_control reflects
//! state after every other file landed.
//!
//! ## V1 constraints
//!
//! - Full backups only. A delta chain (`increment_from` set in the
//!   sentinel) errors hard: incremented files need a disk-resident base
//!   to overlay onto, which the streaming page-walk path doesn't produce.

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures::StreamExt;
use wal_rs::compression;
use wal_rs::config::Settings;
use wal_rs::pg::backup::fetch::{fetch_sentinel, list_tar_parts};
use wal_rs::pg::backup::{BackupSentinelDtoV2, TablespaceSpec, tar_partitions_prefix};
use wal_rs::pg::replication::base_backup::Tablespace;
use wal_rs::storage::DynStorage;

use crate::backup_source::{BackupSink, BackupSource, EndInfo, StartInfo, pump_tar_to_sink};

/// `parallelism` bounds in-flight data parts; `pg_control` always runs
/// single-task after they drain.
pub struct ObjectStoreSource {
    pub settings: Settings,
    pub storage: DynStorage,
    pub backup_name: String,
    pub parallelism: usize,
}

impl ObjectStoreSource {
    pub fn new(settings: Settings, storage: DynStorage, backup_name: String) -> Self {
        let parallelism = std::cmp::min(4, num_cpus_or(4));
        Self {
            settings,
            storage,
            backup_name,
            parallelism,
        }
    }

    pub fn with_parallelism(mut self, n: usize) -> Self {
        self.parallelism = n.max(1);
        self
    }
}

#[async_trait]
impl BackupSource for ObjectStoreSource {
    async fn run(
        self: Box<Self>,
        data_dir: PathBuf,
        sink: Arc<Mutex<dyn BackupSink>>,
    ) -> Result<(StartInfo, EndInfo)> {
        let ObjectStoreSource {
            settings,
            storage,
            backup_name,
            parallelism,
        } = *self;

        let resolved = wal_rs::pg::backup::fetch::resolve_name(&storage, &backup_name)
            .await
            .with_context(|| format!("ObjectStoreSource: resolve {backup_name}"))?;
        tracing::info!(
            target = "walshadow::backup_source_object_store",
            backup = %resolved,
            "fetching"
        );

        let sentinel = fetch_sentinel(&storage, &resolved).await?;
        if sentinel.sentinel.increment_from.is_some() {
            bail!(
                "ObjectStoreSource: delta chain not supported in V1; \
                 pass the full base backup (parent: {:?})",
                sentinel.sentinel.increment_from
            );
        }

        let (start, end) = build_lsn_pair(&resolved, &sentinel)?;
        {
            let mut g = sink.lock().map_err(|_| anyhow!("sink mutex poisoned"))?;
            g.start(&start)?;
        }

        let parts = list_tar_parts(&storage, &resolved).await?;
        if parts.is_empty() {
            bail!(
                "ObjectStoreSource: no tar parts under {}/",
                tar_partitions_prefix(&resolved)
            );
        }

        // Partition rather than re-sort to preserve list_tar_parts order
        // (data first, control last) and let future part types slot between
        let (data_parts, control_parts): (Vec<_>, Vec<_>) =
            parts.into_iter().partition(|k| !k.contains("pg_control"));
        tracing::info!(
            target = "walshadow::backup_source_object_store",
            data_parts = data_parts.len(),
            control_parts = control_parts.len(),
            parallelism,
            "draining tar partitions"
        );

        // Shared counter across concurrent parts; unique EntryId per
        // entry keeps interleaved begin/chunk on the shared sink mutex
        // from clobbering each other's page-walk slot.
        let next_entry = Arc::new(AtomicU64::new(0));

        // Phase A: bounded fan-out of data parts via buffer_unordered
        let data_results = futures::stream::iter(data_parts)
            .map(|key| {
                let storage = storage.clone();
                let settings = settings.clone();
                let data_dir = data_dir.clone();
                let sink = sink.clone();
                let next_entry = next_entry.clone();
                async move {
                    unpack_one_part(&settings, &storage, &key, &data_dir, sink, next_entry).await
                }
            })
            .buffer_unordered(parallelism)
            .collect::<Vec<_>>()
            .await;
        for r in data_results {
            r?;
        }

        // Phase B: pg_control barrier, single-task. wal-g emits one
        // control part; walk in sorted order if ever more
        for key in &control_parts {
            unpack_one_part(
                &settings,
                &storage,
                key,
                &data_dir,
                sink.clone(),
                next_entry.clone(),
            )
            .await?;
        }

        {
            let mut g = sink.lock().map_err(|_| anyhow!("sink mutex poisoned"))?;
            g.finish(&end)?;
        }
        Ok((start, end))
    }
}

/// Build (start, end) from sentinel fields. Timeline is the backup
/// name's first 8 hex chars, per wal-rs `format_backup_name`.
fn build_lsn_pair(resolved_name: &str, s: &BackupSentinelDtoV2) -> Result<(StartInfo, EndInfo)> {
    let start_lsn = s
        .sentinel
        .backup_start_lsn
        .ok_or_else(|| anyhow!("ObjectStoreSource: sentinel missing LSN (backup_start_lsn)"))?;
    let end_lsn = s.sentinel.backup_finish_lsn.ok_or_else(|| {
        anyhow!("ObjectStoreSource: sentinel missing FinishLSN (backup_finish_lsn)")
    })?;
    let timeline = parse_timeline_from_name(resolved_name)?;
    let tablespaces = tablespaces_from_spec(s.sentinel.tablespace_spec.as_ref());
    Ok((
        StartInfo {
            start_lsn,
            timeline,
            tablespaces,
        },
        EndInfo { end_lsn, timeline },
    ))
}

/// Wraps `wal_rs::pg::backup::parse_timeline_from_backup_name` with
/// error context
fn parse_timeline_from_name(name: &str) -> Result<u32> {
    wal_rs::pg::backup::parse_timeline_from_backup_name(name)
        .ok_or_else(|| anyhow!("ObjectStoreSource: cannot parse timeline from backup name: {name}"))
}

/// `TablespaceSpec` → `Vec<Tablespace>` so StartInfo speaks wal-rs's
/// protocol shape regardless of source
fn tablespaces_from_spec(spec: Option<&TablespaceSpec>) -> Vec<Tablespace> {
    let Some(spec) = spec else {
        return Vec::new();
    };
    spec.tablespace_names
        .iter()
        .filter_map(|name| {
            let oid: u32 = name.parse().ok()?;
            let loc = spec.locations.get(name)?;
            Some(Tablespace {
                oid,
                location: loc.location.clone(),
                size: None,
            })
        })
        .collect()
}

/// Fetch one tar part, throttle, decrypt, decompress, pump through
/// `pump_tar_to_sink`. Decompressed reader is `AsyncRead`, tokio_tar
/// drives it directly, no spawn_blocking.
async fn unpack_one_part(
    settings: &Settings,
    storage: &DynStorage,
    key: &str,
    data_dir: &std::path::Path,
    sink: Arc<Mutex<dyn BackupSink>>,
    next_entry: Arc<AtomicU64>,
) -> Result<()> {
    let method = method_from_key(key);
    let body = storage
        .get(key)
        .await
        .with_context(|| format!("ObjectStoreSource: get {key}"))?;
    let throttled = settings.throttle_network(body);
    let decrypted = settings.decrypt(throttled);
    let decoded = compression::decode(method, decrypted);

    let mut archive = tokio_tar::Archive::new(decoded);
    pump_tar_to_sink(&mut archive, data_dir, &sink, &next_entry)
        .await
        .with_context(|| format!("ObjectStoreSource: tar unpack {key}"))?;
    tracing::info!(
        target = "walshadow::backup_source_object_store",
        key,
        "tar part drained"
    );
    Ok(())
}

fn method_from_key(key: &str) -> compression::Method {
    let ext = key.rsplit('.').next().unwrap_or("");
    compression::Method::from_extension(ext).unwrap_or(compression::Method::None)
}

/// Fallback so the source builds without pulling `num_cpus`
fn num_cpus_or(fallback: usize) -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal_rs::pg::backup::BackupSentinelDto;

    #[test]
    fn timeline_parses_from_backup_name() {
        let n = wal_rs::pg::backup::format_backup_name(0x42, 0x0300_0000, 16 * 1024 * 1024);
        let tli = parse_timeline_from_name(&n).unwrap();
        assert_eq!(tli, 0x42);
    }

    #[test]
    fn timeline_rejects_malformed_name() {
        assert!(parse_timeline_from_name("not_a_backup").is_err());
        assert!(parse_timeline_from_name("base_short").is_err());
    }

    #[test]
    fn build_lsn_pair_requires_start_and_end() {
        let resolved = wal_rs::pg::backup::format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
        let mut s = BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: Some(0x0300_0000),
                backup_finish_lsn: Some(0x0300_1000),
                increment_from_lsn: None,
                increment_from: None,
                increment_full_name: None,
                increment_count: None,
                pg_version: 160000,
                system_identifier: None,
                uncompressed_size: 0,
                compressed_size: 0,
                data_catalog_size: 0,
                user_data: None,
                files_metadata_disabled: true,
                tablespace_spec: None,
                backup_start_chkp_num: None,
                increment_from_chkp_num: None,
            },
            version: 2,
            start_time: chrono::Utc::now(),
            finish_time: chrono::Utc::now(),
            date_fmt: String::new(),
            hostname: String::new(),
            data_dir: String::new(),
            is_permanent: false,
        };
        let (start, end) = build_lsn_pair(&resolved, &s).unwrap();
        assert_eq!(start.start_lsn, 0x0300_0000);
        assert_eq!(end.end_lsn, 0x0300_1000);
        assert_eq!(start.timeline, 1);

        s.sentinel.backup_start_lsn = None;
        assert!(build_lsn_pair(&resolved, &s).is_err());
    }

    #[test]
    fn tablespaces_from_spec_skips_when_none() {
        assert!(tablespaces_from_spec(None).is_empty());
        let mut spec = TablespaceSpec::new("/var/lib/pg/16/main");
        spec.add(16384, "/srv/a");
        spec.add(16385, "/srv/b");
        let ts = tablespaces_from_spec(Some(&spec));
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].oid, 16384);
        assert_eq!(ts[0].location, "/srv/a");
    }

    #[test]
    fn method_from_key_picks_compression_extension() {
        assert!(matches!(
            method_from_key("part_001.tar.zst"),
            compression::Method::Zstd
        ));
        assert!(matches!(
            method_from_key("part_001.tar.lz4"),
            compression::Method::Lz4
        ));
        assert!(matches!(
            method_from_key("pg_control.tar"),
            compression::Method::None
        ));
    }
}
