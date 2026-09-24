//! Hydrate shadow's `pg_wal` from backup archive at bootstrap

use std::path::Path;

use anyhow::{Context, Result};
use walrus::pg::backup::format_pg_lsn;
use walshadow::record::segments_covering;

/// Fetch WAL `[start_lsn, end_lsn]` from archive storage into shadow's `pg_wal/`.
pub(crate) async fn fetch_wal_into_pg_wal(
    settings: &walrus::config::Settings,
    storage: walrus::storage::DynStorage,
    shadow_data_dir: &Path,
    start_lsn: u64,
    end_lsn: u64,
    timeline: u32,
) -> Result<()> {
    let pg_wal_dir = shadow_data_dir.join("pg_wal");
    tokio::fs::create_dir_all(&pg_wal_dir)
        .await
        .with_context(|| format!("create {}", pg_wal_dir.display()))?;
    let segments = segments_covering(timeline, start_lsn..end_lsn.saturating_add(1));
    for seg in &segments {
        let name = seg.format();
        let dst = pg_wal_dir.join(&name);
        // Off: the range is enumerated explicitly, so read-ahead would only
        // duplicate the next fetch & risk downloading past end_lsn
        walrus::pg::wal::fetch::handle(
            settings,
            storage.clone(),
            &name,
            &dst,
            walrus::pg::wal::fetch::Prefetch::Off,
        )
        .await
        .with_context(|| format!("fetch WAL {name} -> {}", dst.display()))?;
    }
    // A direct bootstrap's tar carries pg_wal whole, history files included;
    // this leg enumerates segments, so without it the shadow lands on a
    // promoted branch with no `<tli>.history` and has to ask the walsender for
    // one on its first connection. Absent from the archive is survivable —
    // walshadow serves it from `seed_shadow_branches` — so warn, don't fail.
    let history = walshadow::timeline::history_filename(timeline);
    if timeline > 1 {
        let dst = pg_wal_dir.join(&history);
        match walrus::pg::wal::fetch::handle(
            settings,
            storage.clone(),
            &history,
            &dst,
            walrus::pg::wal::fetch::Prefetch::Off,
        )
        .await
        {
            Ok(()) => {}
            Err(e) => tracing::warn!(
                target: "walshadow::bootstrap",
                timeline,
                error = %e,
                "archive holds no {history}; the shadow will ask the walsender for it",
            ),
        }
    }
    tracing::info!(
        target: "walshadow::bootstrap",
        fetched = segments.len(),
        start_lsn = format_pg_lsn(start_lsn).to_string(),
        end_lsn = format_pg_lsn(end_lsn).to_string(),
        timeline,
        "hydrated shadow pg_wal from object store",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use walshadow::record::WAL_SEG_SIZE;

    async fn archive(settings: &walrus::config::Settings, dir: &Path, name: &str, body: &[u8]) {
        let path = dir.join(name);
        tokio::fs::write(&path, body).await.unwrap();
        walrus::pg::wal::push::handle(settings, settings.build_storage().unwrap(), &path)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn hydrates_every_covering_segment_and_history_when_archived() {
        let tmp = tempfile::tempdir().unwrap();
        let settings = walrus::config::Settings {
            storage: walrus::config::StorageSettings::Fs {
                path: tmp.path().join("archive").display().to_string(),
            },
            ..Default::default()
        };
        let segments = segments_covering(2, WAL_SEG_SIZE..3 * WAL_SEG_SIZE);
        for seg in &segments {
            archive(
                &settings,
                tmp.path(),
                &seg.format(),
                &[0; WAL_SEG_SIZE as usize],
            )
            .await;
        }
        let history = walshadow::timeline::history_filename(2);
        let shadow = tmp.path().join("shadow");

        // No history archived yet: segments still land
        let storage = settings.build_storage().unwrap();
        fetch_wal_into_pg_wal(
            &settings,
            storage.clone(),
            &shadow,
            WAL_SEG_SIZE + 42,
            2 * WAL_SEG_SIZE,
            2,
        )
        .await
        .unwrap();
        for seg in &segments {
            assert!(shadow.join("pg_wal").join(seg.format()).exists());
        }
        assert!(!shadow.join("pg_wal").join(&history).exists());

        archive(
            &settings,
            tmp.path(),
            &history,
            b"1\t0/1000000\tpromotion\n",
        )
        .await;
        fetch_wal_into_pg_wal(
            &settings,
            storage.clone(),
            &shadow,
            WAL_SEG_SIZE,
            WAL_SEG_SIZE,
            2,
        )
        .await
        .unwrap();
        assert!(shadow.join("pg_wal").join(&history).exists());

        let err = fetch_wal_into_pg_wal(&settings, storage, &shadow, 0, 0, 2)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("fetch WAL"), "{err:#}");
    }
}
