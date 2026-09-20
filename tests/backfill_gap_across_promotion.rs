//! Exercise `object_store` gap replay across promotion using real PostgreSQL WAL
//!
//! Ancestor's fork segment remains incomplete, so fetch descendant's copy
//! Test source history validation, segment naming, fetch, and replay without ClickHouse

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use walrus::compression;
use walrus::config::{Settings, StorageSettings};
use walrus::pg::backup::parse_pg_lsn;
use walrus::pg::wal;
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::Oid;
use walrus::storage::DynStorage;
use walrus::storage::fs::FsStorage;
use walshadow::archive_history;
use walshadow::backfill_bootstrap::seed_catalog_from_source;
use walshadow::backup_backfill::fetch_segments;
use walshadow::heap_decoder::{ColumnValue, decode_heap_record};
use walshadow::record::{
    Record, RecordSink, SinkError, WAL_SEG_SIZE, segments_covering, segments_covering_lineage,
};
use walshadow::schema::{RelDescriptor, RelName};
use walshadow::shadow::Shadow;
use walshadow::source_feed::{SourceFeed, open_sql_client};
use walshadow::transition::source_history;
use walshadow::wal_replay::pump_segments_through;

const N_ROWS: i32 = 64;

/// Promote standalone cluster via standby mode
/// Force checkpoint to persist new timeline in `pg_control`
fn promote_once(sh: &Shadow) -> Result<()> {
    sh.stop().context("stop source before promotion")?;
    sh.write_standby_signal().context("write standby.signal")?;
    sh.start().context("restart source in standby mode")?;
    sh.psql_one("SELECT pg_promote(true, 60)")
        .context("pg_promote")?;
    sh.psql_one("CHECKPOINT")
        .context("checkpoint after promote")?;
    Ok(())
}

fn timeline_of(sh: &Shadow) -> u32 {
    sh.psql_one("SELECT timeline_id FROM pg_control_checkpoint()")
        .expect("read source timeline")
        .parse()
        .expect("timeline is an integer")
}

fn wal_lsn(sh: &Shadow) -> u64 {
    let raw = sh
        .psql_one("SELECT pg_current_wal_lsn()")
        .expect("read wal lsn");
    parse_pg_lsn(&raw).expect("wal lsn parses")
}

fn db_oid(sh: &Shadow) -> Oid {
    sh.psql_one("SELECT oid FROM pg_database WHERE datname = current_database()")
        .expect("read database oid")
        .parse()
        .expect("database oid is an integer")
}

/// Derive from LSN, `pg_walfile_name` reports preceding segment at a boundary
fn current_segment(sh: &Shadow) -> SegmentName {
    let lsn = wal_lsn(sh);
    SegmentName {
        timeline: timeline_of(sh),
        log_id: (lsn >> 32) as u32,
        seg_no: ((lsn & 0xFFFF_FFFF) / WAL_SEG_SIZE) as u32,
    }
}

/// Archive history and completed segments on current timeline
/// Exclude incomplete segments, including ancestor's fork segment
async fn push_archivable(source: &Shadow, settings: &Settings, storage: DynStorage) -> Result<()> {
    let current = current_segment(source);
    let pg_wal = source.config().data_dir.join("pg_wal");
    for entry in fs::read_dir(&pg_wal).with_context(|| format!("read_dir {}", pg_wal.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let complete = SegmentName::parse(name).is_ok_and(|seg| {
            seg.timeline == current.timeline
                && (seg.log_id, seg.seg_no) < (current.log_id, current.seg_no)
        });
        if !complete && !name.ends_with(".history") {
            continue;
        }
        let path = entry.path();
        wal::push::handle(settings, storage.clone(), &path)
            .await
            .with_context(|| format!("wal::push::handle {}", path.display()))?;
    }
    Ok(())
}

fn test_settings(storage_root: &Path) -> Settings {
    Settings {
        storage: StorageSettings::Fs {
            path: storage_root.to_string_lossy().into_owned(),
        },
        compression: compression::Method::None,
        compression_level: 0,
        ..Default::default()
    }
}

struct BranchTally {
    switch_lsn: u64,
    below: usize,
    at_or_above: usize,
    desc: Arc<RelDescriptor>,
    rows: Vec<(i32, String)>,
}

impl RecordSink for BranchTally {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        if record.source_lsn < self.switch_lsn {
            self.below += 1;
        } else {
            self.at_or_above += 1;
        }
        if record
            .parsed
            .blocks
            .first()
            .is_some_and(|block| block.header.location.rel == self.desc.rfn)
        {
            for heap in decode_heap_record(&record.parsed, record.source_lsn, &self.desc)
                .expect("decode replayed heap")
            {
                let tuple = heap.new.expect("insert tuple");
                let [Some(ColumnValue::Int4(id)), Some(ColumnValue::Text(name))] =
                    tuple.columns.as_slice()
                else {
                    panic!("unexpected tuple: {tuple:?}");
                };
                self.rows.push((*id, name.clone()));
            }
        }
        Box::pin(std::future::ready(Ok(())))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gap_replay_crosses_a_promotion() {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    fx::load_source_workload(&source, "s1", N_ROWS).expect("load source workload");

    let from_lsn = wal_lsn(&source);
    source
        .psql_one(&format!(
            "INSERT INTO s1.t SELECT g, 'pre-'||g::text \
             FROM generate_series({}, {}) g",
            N_ROWS + 1,
            N_ROWS * 2,
        ))
        .expect("pre-promotion insert");
    // Place fork in next segment
    source.psql_one("SELECT pg_switch_wal()").expect("switch");

    let storage_root = tmp.path().join("wal-g");
    fs::create_dir_all(&storage_root).unwrap();
    let storage: DynStorage = Arc::new(FsStorage::new(&storage_root).unwrap());
    let settings = test_settings(&storage_root);
    let pg = fx::pg_cfg(&source, "gap-promotion-test");
    let seg_dir = tmp.path().join("gap_wal");
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    assert!(
        source_history(&mut feed, ident.timeline)
            .await
            .unwrap()
            .is_none(),
        "timeline 1 has no history file to place",
    );
    drop(feed);
    // Archive before promotion's checkpoint can recycle ancestor segments
    push_archivable(&source, &settings, storage.clone())
        .await
        .expect("archive ancestor WAL");

    promote_once(&source).expect("promote source");
    let tli = timeline_of(&source);
    assert_eq!(tli, 2, "test needs the source one timeline up, got {tli}");
    source
        .psql_one(&format!(
            "INSERT INTO s1.t SELECT g, 'post-'||g::text \
             FROM generate_series(1000, {}) g",
            1000 + N_ROWS,
        ))
        .expect("post-promotion insert");
    // End replay inside fork segment, then complete it for archival
    let to_lsn = wal_lsn(&source);
    source.psql_one("SELECT pg_switch_wal()").expect("switch");
    push_archivable(&source, &settings, storage.clone())
        .await
        .expect("archive descendant WAL");

    // Keep a newer sibling in archive, source still runs timeline 2
    let sibling = tmp.path().join("00000003.history");
    fs::write(&sibling, "1\t0/1000000\tpromotion\n").unwrap();
    wal::push::handle(&settings, storage.clone(), &sibling)
        .await
        .unwrap();
    let mut feed = SourceFeed::connect(&pg).await.unwrap();
    let ident = feed.identify_system().await.unwrap();
    let history = source_history(&mut feed, ident.timeline)
        .await
        .unwrap()
        .expect("promotion archived 00000002.history");
    assert_eq!(
        history.target(),
        2,
        "source pins the branch, not the archive"
    );
    archive_history::verify(&settings, &storage, &history)
        .await
        .expect("archive records the chain the source serves");
    drop(feed);
    let switch_lsn = history
        .switchpoint_of(1)
        .expect("the chain records where timeline 1 ended");

    let range = from_lsn..to_lsn.saturating_add(1);
    let names = segments_covering_lineage(&history, 1, range.clone());
    assert!(
        names.iter().any(|s| s.timeline == 1) && names.iter().any(|s| s.timeline == 2),
        "range must straddle the fork, got {:?}",
        names.iter().map(|s| s.format()).collect::<Vec<_>>(),
    );

    // Ancestor's fork segment was never completed or archived
    let single_branch = segments_covering(1, range);
    let err = fetch_segments(&settings, &storage, &seg_dir, &single_branch)
        .await
        .expect_err("the ancestor never completed the fork segment");
    assert!(err.to_string().contains("fetch WAL"), "{err:#}");

    let segments = fetch_segments(&settings, &storage, &seg_dir, &names)
        .await
        .expect("every segment comes off the branch whose file serves it");
    let sql = open_sql_client(&pg).await.unwrap();
    let catalog = seed_catalog_from_source(&sql).await.unwrap();
    let desc = catalog
        .descriptors()
        .find(|desc| desc.rel_name == RelName::new("s1", "t"))
        .unwrap()
        .clone();
    let mut tally = BranchTally {
        switch_lsn,
        below: 0,
        at_or_above: 0,
        desc,
        rows: Vec::new(),
    };
    pump_segments_through(&segments, db_oid(&source), &mut tally)
        .await
        .expect("replay crosses the switch");
    assert!(tally.below > 0, "no ancestor records replayed");
    assert!(
        tally.at_or_above > 0,
        "replay stopped at the fork: {} records, none at or past {switch_lsn:#X}",
        tally.below,
    );
    let expected: Vec<_> = ((N_ROWS + 1)..=(N_ROWS * 2))
        .map(|id| (id, format!("pre-{id}")))
        .chain((1000..=(1000 + N_ROWS)).map(|id| (id, format!("post-{id}"))))
        .collect();
    assert_eq!(tally.rows, expected);
}
