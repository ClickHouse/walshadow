//! Source VACUUM while walshadow is down, under `[toast] mode = "shadow"`
//!
//! Rows updated without touching their large value carry only a TOAST pointer.
//! A later update replaces the value and VACUUM reclaims the old chunks. On
//! restart shadow replays that reclamation while older pointer-only rows still
//! wait for ClickHouse, so their value is gone unless source kept it
//!
//! History target keeps every version, so a lost value shows as a wrong body on
//! the pointer-only version

#![cfg(target_os = "linux")]

#[path = "common/horizon_fixture.rs"]
mod hz;

use std::time::Duration;

use anyhow::{Result, ensure};
use hz::{BODY_REPEAT, ROWS, Rig, SCHEMA, fx, slot_xmin, toast_dead_tuples, write_config};
use walshadow::shadow::Shadow;

const FILLER: u32 = 100_000;

/// Pointer-only update, value replacement, then VACUUM, all while daemon is down.
/// Filler rows ahead keep queue worker busy on restart while pump and shadow
/// replay through VACUUM, so pointer-only reads lose their race with it
fn churn_during_outage(source: &Shadow) -> Result<()> {
    source.psql_one(&format!(
        "INSERT INTO {SCHEMA}.t SELECT g, 'filler', 'f' FROM generate_series({}, {}) g",
        ROWS + 1,
        ROWS + FILLER
    ))?;
    source.psql_one(&format!(
        "UPDATE {SCHEMA}.t SET meta = 'v2' WHERE id <= {ROWS}"
    ))?;
    source.psql_one(&format!(
        "UPDATE {SCHEMA}.t SET meta = 'v3', body = 'short-'||id::text WHERE id <= {ROWS}"
    ))?;
    source.psql_one(&format!("VACUUM {SCHEMA}.t"))?;
    Ok(())
}

/// Once ClickHouse holds every row, pin must pass churn so VACUUM reclaims it
fn wait_for_release(source: &Shadow) -> Result<()> {
    let deleter: u64 = source
        .psql_one(&format!("SELECT xmin::text FROM {SCHEMA}.t WHERE id = 1"))?
        .trim()
        .parse()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    // Floor advances by sealed segment, pin by running-xacts sample below it
    while slot_xmin(source)?.is_none_or(|x| x <= deleter) {
        ensure!(
            std::time::Instant::now() < deadline,
            "pin stuck at {:?}, churn xid {deleter}",
            slot_xmin(source)?
        );
        source.psql_one("CHECKPOINT")?;
        source.psql_one("SELECT pg_switch_wal()")?;
        std::thread::sleep(Duration::from_millis(500));
    }
    source.psql_one(&format!("VACUUM {SCHEMA}.t"))?;
    let dead = toast_dead_tuples(source)?;
    ensure!(dead == 0, "released pin left {dead} dead chunks");
    Ok(())
}

fn run(pin: bool) {
    let Some(rig) = Rig::new(pin) else { return };
    let Rig {
        source,
        ch,
        daemon,
        config,
        ports,
        ..
    } = &rig;
    let mut guard = fx::ChildGuard::new(rig.spawn().expect("spawn walshadow-stream"));
    let result = (|| -> Result<()> {
        // Floor must pass bootstrap rows before stop, else restart replays them
        // as well and drowns out the outage rows
        rig.wait_bootstrapped()?;
        if let Some(mut first) = guard.child.take() {
            fx::tools::stop_gracefully(&mut first);
        }
        let held = slot_xmin(source)?;
        churn_during_outage(source)?;
        let toast_dead_after_vacuum = toast_dead_tuples(source)?;

        guard.child = Some(rig.spawn()?);
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(60))?;
        fx::wait_for_ch_value(
            ch,
            "SELECT count() FROM default.t WHERE meta = 'v3'",
            &ROWS.to_string(),
            Duration::from_secs(60),
        )?;
        let v2 = ch.query("SELECT count() FROM default.t WHERE meta = 'v2'")?;
        ensure!(v2 == ROWS.to_string(), "pointer-only versions in CH: {v2}");
        let wrong_v2: u64 = ch
            .query(&format!(
                "SELECT count() FROM default.t WHERE meta = 'v2' AND \
                 body != repeat(concat('orig-', toString(id), '-'), {BODY_REPEAT})"
            ))?
            .parse()?;
        let wrong_v3 = ch.query(
            "SELECT count() FROM default.t WHERE meta = 'v3' AND \
             body != concat('short-', toString(id))",
        )?;
        ensure!(
            wrong_v3 == "0",
            "replacement values resolve from own WAL: {wrong_v3}"
        );
        let superseded = rig.superseded()?;
        eprintln!(
            "pin={pin} slot_xmin_at_stop={held:?} toast_dead_after_vacuum={toast_dead_after_vacuum} \
             superseded={superseded} wrong_v2={wrong_v2}/{ROWS}"
        );
        if !pin {
            ensure!(held.is_none(), "unpinned slot holds xmin {held:?}");
            ensure!(toast_dead_after_vacuum == 0, "VACUUM kept chunks unpinned");
            ensure!(wrong_v2 > 0, "reclamation never reached pointer-only rows");
            ensure!(superseded == wrong_v2, "lost values went uncounted");
            return Ok(());
        }
        ensure!(held.is_some(), "pin never reached slot");
        ensure!(
            toast_dead_after_vacuum > 0,
            "VACUUM reclaimed pinned chunks"
        );
        ensure!(wrong_v2 == 0 && superseded == 0, "pinned values lost");
        wait_for_release(source)?;

        // Disabling pin clears what slot holds, else source bloats forever
        if let Some(mut child) = guard.child.take() {
            fx::tools::stop_gracefully(&mut child);
        }
        write_config(config, ports.ch_tcp, false)?;
        guard.child = Some(rig.spawn()?);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while slot_xmin(source)?.is_some() {
            ensure!(
                std::time::Instant::now() < deadline,
                "slot xmin never released"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
        Ok(())
    })();
    fx::finish_daemon(guard, daemon, result);
}

#[test]
fn vacuum_during_outage_loses_values_without_pin() {
    run(false);
}

#[test]
fn horizon_pin_keeps_values_through_vacuum() {
    run(true);
}

/// Xmin source's walsender holds for walshadow, none while disconnected.
/// Walsender in startup can show its connection snapshot xmin, not feedback
fn walsender_xmin(source: &Shadow) -> Result<Option<u64>> {
    let x = source.psql_one(
        "SELECT coalesce((SELECT backend_xmin::text FROM pg_stat_replication \
         WHERE backend_xmin IS NOT NULL AND state <> 'startup' LIMIT 1), '')",
    )?;
    let x = x.trim();
    Ok((!x.is_empty()).then(|| x.parse()).transpose()?)
}

/// Slotless source holds nothing through an outage. Restart holds xmin saved
/// with floor again, so values deleted meanwhile outlast a later VACUUM
#[test]
fn slotless_restart_holds_saved_xmin() {
    let Some(rig) = Rig::new(true) else { return };
    let Rig {
        source, ch, daemon, ..
    } = &rig;
    let spawn = || daemon.spawn(source, &rig.config, rig.ports.walsender, &[]);
    let mut guard = fx::ChildGuard::new(spawn().expect("spawn walshadow-stream"));
    let result = (|| -> Result<()> {
        rig.wait_bootstrapped()?;
        if let Some(mut first) = guard.child.take() {
            fx::tools::stop_gracefully(&mut first);
        }
        let held = walsender_xmin(source)?;
        ensure!(held.is_none(), "xmin {held:?} outlived its walsender");
        let before: u64 = source
            .psql_one("SELECT pg_snapshot_xmin(pg_current_snapshot())::text")?
            .trim()
            .parse()?;
        source.psql_one(&format!(
            "UPDATE {SCHEMA}.t SET meta = 'v2' WHERE id <= {ROWS}"
        ))?;
        source.psql_one(&format!(
            "UPDATE {SCHEMA}.t SET meta = 'v3', body = 'short-'||id::text WHERE id <= {ROWS}"
        ))?;

        guard.child = Some(spawn()?);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let held = loop {
            if let Some(held) = walsender_xmin(source)? {
                break held;
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "restart never sent feedback"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        ensure!(
            held <= before,
            "restart holds {held}, values deleted since {before} unprotected"
        );
        source.psql_one(&format!("VACUUM {SCHEMA}.t"))?;
        let dead = toast_dead_tuples(source)?;
        ensure!(dead > 0, "VACUUM reclaimed chunks restart holds");
        fx::wait_for_ch_value(
            ch,
            "SELECT count() FROM default.t WHERE meta = 'v3'",
            &ROWS.to_string(),
            Duration::from_secs(60),
        )?;
        let wrong_v2 = ch.query(&format!(
            "SELECT count() FROM default.t WHERE meta = 'v2' AND \
             body != repeat(concat('orig-', toString(id), '-'), {BODY_REPEAT})"
        ))?;
        ensure!(wrong_v2 == "0", "pinned values lost: {wrong_v2}");
        Ok(())
    })();
    fx::finish_daemon(guard, daemon, result);
}
