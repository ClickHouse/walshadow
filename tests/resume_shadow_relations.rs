//! Replaying heap deltas after dropping their earlier images must stay filtered

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use walrus::pg::wal::segment::SegmentName;
use walshadow::backfill::{pg_path, wal_landing};
use walshadow::filter::catalog_tracker::CatalogTracker;
use walshadow::record::{CollectingRecordSink, Route, WAL_SEG_SIZE};
use walshadow::segment_sink::DirSegmentSink;
use walshadow::shadow::{Shadow, ShadowConfig};
use walshadow::wal_stream::WalStream;

fn shadow(root: &Path, name: &str) -> Shadow {
    let mut cfg = ShadowConfig::new(root.join(name), root.join(format!("{name}-out")));
    cfg.socket_dir = root.join(format!("{name}-sock"));
    cfg.ctl_timeout = Duration::from_secs(10);
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    Shadow::new(cfg)
}

struct Stop<'a>(&'a Shadow);
impl Drop for Stop<'_> {
    fn drop(&mut self) {
        let _ = self.0.stop();
    }
}

fn copy_dir(from: &Path, to: &Path) {
    assert!(
        Command::new("cp")
            .arg("-a")
            .arg(from)
            .arg(to)
            .status()
            .unwrap()
            .success()
    );
}

fn lsn(s: &str) -> u64 {
    let (hi, lo) = s.trim().split_once('/').unwrap();
    (u64::from_str_radix(hi, 16).unwrap() << 32) | u64::from_str_radix(lo, 16).unwrap()
}

#[tokio::test]
async fn retained_routes_prevent_invalid_page_recovery_after_resume() {
    if !Command::new("initdb")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
    {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = shadow(tmp.path(), "source");
    let good = shadow(tmp.path(), "good");
    let bad = shadow(tmp.path(), "bad");
    source.initdb().unwrap();
    source.write_base_conf().unwrap();
    source.start().unwrap();
    let _source_stop = Stop(&source);
    source
        .apply_schema_dump("CREATE TABLE t (id int, value int); INSERT INTO t VALUES (1, 0)")
        .unwrap();
    let db: u32 = source
        .psql_one("SELECT oid FROM pg_database WHERE datname = current_database()")
        .unwrap()
        .parse()
        .unwrap();
    let rel: u32 = source
        .psql_one("SELECT pg_relation_filenode('t')")
        .unwrap()
        .parse()
        .unwrap();
    source.stop().unwrap();
    copy_dir(&source.config().data_dir, &good.config().data_dir);
    source.start().unwrap();
    let floor = lsn(&source
        .psql_one("SELECT pg_current_wal_insert_lsn()")
        .unwrap());
    source.psql_one("UPDATE t SET value = 1").unwrap();
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    let resume = lsn(&source
        .psql_one("SELECT pg_current_wal_insert_lsn()")
        .unwrap())
        / WAL_SEG_SIZE
        * WAL_SEG_SIZE;
    source.psql_one("UPDATE t SET value = 2").unwrap();
    let end = lsn(&source
        .psql_one("SELECT pg_current_wal_insert_lsn()")
        .unwrap());
    source.psql_one("SELECT pg_switch_wal()").unwrap();
    let raw = tmp.path().join("raw");
    wal_landing::copy_window_segments(&source.config().data_dir.join("pg_wal"), &raw, 1, 0, end)
        .await
        .unwrap();
    let pg_wal = good.config().data_dir.join("pg_wal");
    for entry in fs::read_dir(&raw).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), pg_wal.join(entry.file_name())).unwrap();
    }
    wal_landing::filter_landed_wal(
        &pg_wal,
        1,
        end,
        CatalogTracker::new(),
        Some((Default::default(), floor)),
    )
    .await
    .unwrap();
    // Match an omitted heap later recreated by recovery plumbing
    fs::write(good.config().data_dir.join(format!("base/{db}/{rel}")), []).unwrap();
    copy_dir(&good.config().data_dir, &bad.config().data_dir);
    let seg = SegmentName {
        timeline: 1,
        log_id: (resume >> 32) as u32,
        seg_no: ((resume & 0xffff_ffff) / WAL_SEG_SIZE) as u32,
    };
    let bytes = fs::read(raw.join(seg.format())).unwrap();
    for (sh, durable) in [(&bad, false), (&good, true)] {
        let mut stream = WalStream::new(1, WAL_SEG_SIZE, resume).unwrap();
        if durable {
            stream
                .filter_mut()
                .load_shadow_rels(&sh.config().data_dir)
                .await
                .unwrap();
        } else {
            let rels = pg_path::user_relation_filenodes(&sh.config().data_dir, db)
                .await
                .unwrap();
            assert!(rels.contains(&(db, rel)));
            stream.filter_mut().keep_user_rels(rels, end);
        }
        stream
            .preserve_resume_prefix(&[sh.config().data_dir.join("pg_wal")])
            .await
            .unwrap();
        let mut records = CollectingRecordSink::default();
        let mut sink = DirSegmentSink::new(sh.config().filter_out_dir.clone()).unwrap();
        stream
            .push(resume, &bytes, &mut records, &mut sink)
            .await
            .unwrap();
        let heap_records: Vec<_> = records
            .records
            .iter()
            .filter(|r| {
                r.parsed
                    .blocks
                    .iter()
                    .any(|b| b.header.location.rel.rel_node == rel)
            })
            .collect();
        assert!(!heap_records.is_empty());
        assert!(heap_records.iter().all(|r| r.route
            == if durable {
                Route::ToDecoder
            } else {
                Route::ToBoth
            }));
        assert!(
            heap_records
                .iter()
                .any(|r| r.parsed.blocks.iter().all(|b| !b.header.has_image()))
        );
        sh.write_base_conf().unwrap();
        sh.write_standby_signal().unwrap();
        let mut conf = fs::OpenOptions::new()
            .append(true)
            .open(sh.config().data_dir.join("postgresql.conf"))
            .unwrap();
        writeln!(
            conf,
            "restore_command = 'cp {}/%f %p'",
            sh.config().filter_out_dir.display()
        )
        .unwrap();
        writeln!(conf, "recovery_target_timeline = 'current'").unwrap();
    }
    let _bad_stop = Stop(&bad);
    let _ = bad.start();
    assert!(bad.wait_for_replay(end, Duration::from_secs(5)).is_err());
    let log = fs::read_to_string(bad.config().data_dir.join("startup.log")).unwrap();
    assert!(
        log.contains("WAL contains references to invalid pages"),
        "{log}"
    );
    let _good_stop = Stop(&good);
    good.start().unwrap();
    good.wait_for_replay(end, Duration::from_secs(10)).unwrap();
    good.stop().unwrap();
    good.start().unwrap();
    good.wait_for_replay(end, Duration::from_secs(10)).unwrap();
}
