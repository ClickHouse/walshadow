//! Process-wide elapsed timings, including waits and overlapping work

use std::fmt;
use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;

use prometheus_client::encoding::{EncodeCounterValue, EncodeGaugeValue, NoLabelSet};
use prometheus_client::metrics::MetricType;

use crate::ops::metrics::{declare, gauge};

const UNSET: u64 = u64::MAX;

pub struct Stage {
    name: &'static str,
    title: &'static str,
    order: &'static str,
    nanos: AtomicU64,
    completed: AtomicU64,
    interrupted: AtomicU64,
    active: AtomicU64,
    work_units: AtomicU64,
    work_done: AtomicU64,
    rows: AtomicU64,
}

impl Stage {
    const fn new(name: &'static str, title: &'static str, order: &'static str) -> Self {
        Self {
            name,
            title,
            order,
            nanos: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            interrupted: AtomicU64::new(0),
            active: AtomicU64::new(0),
            work_units: AtomicU64::new(UNSET),
            work_done: AtomicU64::new(UNSET),
            rows: AtomicU64::new(UNSET),
        }
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed) > 0
    }

    pub fn set_work(&self, done: u64, total: u64) {
        if total == 0 {
            return;
        }
        raise(&self.work_done, done);
        raise(&self.work_units, total);
    }

    pub fn set_rows(&self, rows: u64) {
        self.rows.store(rows.min(UNSET - 1), Ordering::Relaxed);
    }

    fn labels(&self) -> [(&'static str, &'static str); 3] {
        [
            ("stage", self.name),
            ("title", self.title),
            ("order", self.order),
        ]
    }

    pub fn start(&self) -> Timer<'_> {
        self.active.fetch_add(1, Ordering::Relaxed);
        Timer {
            stage: self,
            started: Instant::now(),
            completed: false,
        }
    }

    pub async fn measure<T, E>(&self, work: impl Future<Output = Result<T, E>>) -> Result<T, E> {
        let timer = self.start();
        let result = work.await;
        if result.is_ok() {
            timer.finish();
        }
        result
    }
}

pub struct Timer<'a> {
    stage: &'a Stage,
    started: Instant,
    completed: bool,
}

impl Timer<'_> {
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn finish(mut self) {
        self.completed = true;
    }
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.stage
            .nanos
            .fetch_add(self.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let counter = if self.completed {
            let total = self.stage.work_units.load(Ordering::Relaxed);
            if total != UNSET {
                self.stage.work_done.store(total, Ordering::Relaxed);
            }
            &self.stage.completed
        } else {
            &self.stage.interrupted
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.stage.active.fetch_sub(1, Ordering::Relaxed);
    }
}

fn raise(slot: &AtomicU64, value: u64) {
    let value = value.min(UNSET - 1);
    let held = slot.load(Ordering::Relaxed);
    if held == UNSET || held < value {
        slot.store(value, Ordering::Relaxed);
    }
}

macro_rules! stages {
    ($($name:ident => ($label:literal, $title:literal, $order:literal)),+ $(,)?) => {
        $(pub static $name: Stage = Stage::new($label, $title, $order);)+
        static ALL: &[&Stage] = &[$(&$name),+];
    };
}

stages! {
    BOOTSTRAP => ("bootstrap", "Initial load", "010"),
    BOOTSTRAP_WALK => ("bootstrap_walk", "Backup page walk", "020"),
    BOOTSTRAP_WINDOW => ("bootstrap_window", "Backup window replay", "030"),
    SHADOW_REPLAY => ("shadow_replay", "Shadow start", "040"),
    BOOTSTRAP_DEFERRED => ("bootstrap_deferred", "Large-value fill", "050"),
    BOOTSTRAP_GATE => ("bootstrap_gate", "Visibility check", "060"),
    COPY => ("copy", "Table COPY", "070"),
    INSERT_FLUSH => ("insert_flush", "Insert flush", "080"),
    PUBLISH => ("publish", "Publish", "090"),
    SETTLE => ("settle", "Settle", "100"),
}

fn stage_counter<'a, V: EncodeCounterValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    series: impl IntoIterator<Item = (&'a Stage, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Counter)?;
    for (stage, value) in series {
        family
            .encode_family(&stage.labels())?
            .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
    }
    Ok(())
}

fn stage_gauge<'a, V: EncodeGaugeValue>(
    enc: &mut DescriptorEncoder<'_>,
    name: &str,
    help: &str,
    series: impl IntoIterator<Item = (&'a Stage, V)>,
) -> fmt::Result {
    let mut family = declare(enc, name, help, MetricType::Gauge)?;
    for (stage, value) in series {
        family
            .encode_family(&stage.labels())?
            .encode_gauge(&value)?;
    }
    Ok(())
}

fn set_values<'a>(
    field: impl Fn(&'a Stage) -> &'a AtomicU64,
) -> impl Iterator<Item = (&'a Stage, u64)> {
    ALL.iter().filter_map(move |s| {
        let v = field(s).load(Ordering::Relaxed);
        (v != UNSET).then_some((*s, v))
    })
}

/// Stage families off the process-wide registry, one series per stage
#[derive(Debug)]
pub struct StageCollector;

impl Collector for StageCollector {
    fn encode(&self, mut enc: DescriptorEncoder) -> fmt::Result {
        static EPOCH: OnceLock<f64> = OnceLock::new();
        let epoch = EPOCH.get_or_init(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        });
        gauge(
            &mut enc,
            "walshadow_stage_epoch_seconds",
            "Timing registry identity, changes after restart",
            *epoch,
        )?;
        stage_counter(
            &mut enc,
            "walshadow_stage_seconds_total",
            "Elapsed seconds of finished attempts, including waits and overlaps",
            ALL.iter()
                .map(|s| (*s, s.nanos.load(Ordering::Relaxed) as f64 / 1e9)),
        )?;
        stage_counter(
            &mut enc,
            "walshadow_stage_completed_total",
            "Successfully finished stage attempts",
            ALL.iter()
                .map(|s| (*s, s.completed.load(Ordering::Relaxed))),
        )?;
        stage_counter(
            &mut enc,
            "walshadow_stage_interrupted_total",
            "Failed or cancelled stage attempts",
            ALL.iter()
                .map(|s| (*s, s.interrupted.load(Ordering::Relaxed))),
        )?;
        stage_gauge(
            &mut enc,
            "walshadow_stage_active",
            "Currently running stage attempts",
            ALL.iter().map(|s| (*s, s.active.load(Ordering::Relaxed))),
        )?;
        let mut counts = declare(
            &mut enc,
            "walshadow_stage_progress",
            "Per-stage progress by measure: work_total is the units of work the stage has in total (backup parts for the page walk, spool bytes for the large-value fill, deferred tuples for the visibility check), work_done the units finished so far, rows the row count that describes the stage (decoded from backup pages, superseded by window WAL). Work only rises, is updated while its stage runs, and work_done lands on work_total when the stage completes. A measure is absent until a bootstrap reports it, and a stage with no work units never reports work",
            MetricType::Gauge,
        )?;
        for (measure, field) in [
            (
                StageMeasure::WorkTotal,
                (|s: &Stage| &s.work_units) as fn(&Stage) -> &AtomicU64,
            ),
            (StageMeasure::WorkDone, |s: &Stage| &s.work_done),
            (StageMeasure::Rows, |s: &Stage| &s.rows),
        ] {
            for (stage, value) in set_values(field) {
                let [stage_l, title_l, order_l] = stage.labels();
                counts
                    .encode_family(&[stage_l, title_l, order_l, ("measure", measure.label())])?
                    .encode_gauge(&value)?;
            }
        }
        Ok(())
    }
}

crate::label_class! {
    pub enum StageMeasure {
        WorkTotal => "work_total",
        WorkDone => "work_done",
        Rows => "rows",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_and_rows_are_absent_until_set() {
        let stage = Stage::new("t", "T", "990");
        for slot in [&stage.work_units, &stage.work_done, &stage.rows] {
            assert_eq!(slot.load(Ordering::Relaxed), UNSET);
        }
        stage.set_work(3, 12);
        stage.set_rows(40);
        assert_eq!(stage.work_done.load(Ordering::Relaxed), 3);
        assert_eq!(stage.work_units.load(Ordering::Relaxed), 12);
        assert_eq!(stage.rows.load(Ordering::Relaxed), 40);
        assert_eq!(
            BOOTSTRAP_WALK.labels(),
            [
                ("stage", "bootstrap_walk"),
                ("title", "Backup page walk"),
                ("order", "020")
            ]
        );
    }

    #[tokio::test]
    async fn work_only_rises_and_lands_on_total_at_completion() {
        let stage = Stage::new("t", "T", "990");
        stage.set_work(0, 0);
        assert_eq!(stage.work_units.load(Ordering::Relaxed), UNSET);
        stage.set_work(4, 10);
        stage.set_work(2, 5);
        assert_eq!(stage.work_done.load(Ordering::Relaxed), 4);
        assert_eq!(stage.work_units.load(Ordering::Relaxed), 10);
        assert!(stage.measure(async { Err::<(), _>(()) }).await.is_err());
        assert_eq!(stage.work_done.load(Ordering::Relaxed), 4);
        stage.measure(async { Ok::<_, ()>(()) }).await.unwrap();
        assert_eq!(stage.work_done.load(Ordering::Relaxed), 10);
        assert_eq!(stage.active.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn distinguish_success_failure_and_cancellation() {
        let stage = Stage::new("test", "Test", "00");
        stage.measure(async { Ok::<_, ()>(()) }).await.unwrap();
        assert!(stage.measure(async { Err::<(), _>(()) }).await.is_err());
        let timer = stage.start();
        assert_eq!(stage.active.load(Ordering::Relaxed), 1);
        drop(timer);
        assert_eq!(stage.completed.load(Ordering::Relaxed), 1);
        assert_eq!(stage.interrupted.load(Ordering::Relaxed), 2);
        assert_eq!(stage.active.load(Ordering::Relaxed), 0);
    }
}
