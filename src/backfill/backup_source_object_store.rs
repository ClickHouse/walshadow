//! Object-store base-backup source. Fetches a wal-g compatible
//! BASE_BACKUP from a `DynStorage` bucket, decompresses each tar part,
//! pumps file events through [`BackupSink`].
//!
//! Layout (mirrors wal-g, owned by [`walrus::pg::backup`]):
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
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::Mutex;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use walrus::compression;
use walrus::config::Settings;
use walrus::pg::backup::fetch::{fetch_sentinel, list_tar_parts};
use walrus::pg::backup::tar_partitions_prefix;
use walrus::storage::DynStorage;

use crate::backfill::backup_sentinel::build_lsn_pair;
#[cfg(test)]
use crate::backfill::backup_sentinel::{parse_timeline_from_name, tablespaces_from_spec};
use crate::backfill::backup_source::{
    BackupSink, BackupSource, EndInfo, StartInfo, pump_tar_to_sink,
};

/// `parallelism` bounds in-flight data parts; `pg_control` always runs
/// single-task after they drain.
pub struct ObjectStoreSource {
    pub settings: Settings,
    pub storage: DynStorage,
    pub backup_name: String,
    pub parallelism: usize,
    /// Caller's existing scratch root (`--spill-dir` for bootstrap, the
    /// backfill's `scratch_dir`), not a directory of its own. Holds up to
    /// `parallelism` compressed parts at once, so parallelism sets the
    /// disk floor.
    pub part_spool_dir: PathBuf,
}

impl ObjectStoreSource {
    pub fn new(
        settings: Settings,
        storage: DynStorage,
        backup_name: String,
        part_spool_dir: PathBuf,
    ) -> Self {
        let parallelism = std::cmp::min(4, num_cpus_or(4));
        Self {
            settings,
            storage,
            backup_name,
            parallelism,
            part_spool_dir,
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
            part_spool_dir,
        } = *self;
        // Bootstrap runs before `SpillStore::new` creates --spill-dir, so
        // the scratch root is not guaranteed to exist yet.
        tokio::fs::create_dir_all(&part_spool_dir)
            .await
            .with_context(|| format!("part spool dir {}", part_spool_dir.display()))?;

        let resolved = walrus::pg::backup::fetch::resolve_name(&storage, &backup_name)
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
            let mut g = sink.lock().await;
            g.start(&start).await?;
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
        // try_collect short-circuits on the first part error. A plain
        // collect withholds it until every other part has drained.
        futures::stream::iter(data_parts)
            .map(|key| {
                let storage = storage.clone();
                let settings = settings.clone();
                let data_dir = data_dir.clone();
                let sink = sink.clone();
                let next_entry = next_entry.clone();
                let spool = part_spool_dir.clone();
                async move {
                    unpack_one_part(
                        &settings, &storage, &key, &data_dir, &spool, sink, next_entry,
                    )
                    .await
                }
            })
            .buffer_unordered(parallelism)
            .try_collect::<Vec<_>>()
            .await?;

        // Phase B: pg_control barrier, single-task. wal-g emits one
        // control part; walk in sorted order if ever more
        for key in &control_parts {
            unpack_one_part(
                &settings,
                &storage,
                key,
                &data_dir,
                &part_spool_dir,
                sink.clone(),
                next_entry.clone(),
            )
            .await?;
        }

        {
            let mut g = sink.lock().await;
            g.finish(&end).await?;
        }
        Ok((start, end))
    }
}

/// Fetch one tar part, throttle, decrypt, decompress, pump through
/// `pump_tar_to_sink`. Decompressed reader is `AsyncRead`, tokio_tar
/// drives it directly, no spawn_blocking.
async fn unpack_one_part(
    settings: &Settings,
    storage: &DynStorage,
    key: &str,
    data_dir: &std::path::Path,
    part_spool_dir: &std::path::Path,
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
    let spooled = spool_backup_part(part_spool_dir, key, decrypted).await?;
    let decoded = compression::decode(method, Box::pin(spooled));

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

/// Drain the part body to scratch and hand back a reader over it, so the
/// GET completes at network speed instead of at whatever rate the sink
/// drains. Decode against a live body ties request lifetime to the whole
/// downstream pipeline, and walrus caps a request at 60s.
///
/// The file is unlinked as soon as it is written: the fd pins the blocks,
/// so no error or panic path can leak it. Same trick as
/// [`crate::spill::BodySpoolFile`]. Bytes are still compressed here, so
/// scratch tracks object size rather than the unpacked tar.
async fn spool_backup_part(
    dir: &std::path::Path,
    key: &str,
    mut body: compression::AsyncReader,
) -> Result<tokio::fs::File> {
    use tokio::io::AsyncSeekExt;

    let leaf = key.rsplit('/').next().unwrap_or("part");
    let path = dir.join(format!("backup_part_{leaf}"));
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await
        .with_context(|| format!("ObjectStoreSource: open part spool {}", path.display()))?;
    let bytes = tokio::io::copy(&mut body, &mut file)
        .await
        .with_context(|| format!("ObjectStoreSource: spool {key}"))?;
    tokio::fs::remove_file(&path).await.ok();
    file.rewind()
        .await
        .with_context(|| format!("ObjectStoreSource: rewind part spool {key}"))?;
    tracing::info!(
        target = "walshadow::backup_source_object_store",
        key,
        bytes,
        "part spooled"
    );
    Ok(file)
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
    use walrus::pg::backup::{BackupSentinelDto, BackupSentinelDtoV2, TablespaceSpec};

    #[test]
    fn timeline_parses_from_backup_name() {
        let n = walrus::pg::backup::format_backup_name(0x42, 0x0300_0000, 16 * 1024 * 1024);
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
        let resolved = walrus::pg::backup::format_backup_name(1, 0x0300_0000, 16 * 1024 * 1024);
        let mut s = BackupSentinelDtoV2 {
            sentinel: BackupSentinelDto {
                backup_start_lsn: std::num::NonZeroU64::new(0x0300_0000),
                backup_finish_lsn: std::num::NonZeroU64::new(0x0300_1000),
                increment_from_lsn: None,
                increment_from: None,
                increment_full_name: None,
                increment_count: None,
                increment_format: Default::default(),
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
