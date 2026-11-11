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

    pub fn set_work(&self, done: u64, total: u64) {
        self.work_done.store(done.min(UNSET - 1), Ordering::Relaxed);
        self.work_units
            .store(total.min(UNSET - 1), Ordering::Relaxed);
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
            &self.stage.completed
        } else {
            &self.stage.interrupted
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.stage.active.fetch_sub(1, Ordering::Relaxed);
    }
}

macro_rules! stages {
    ($($name:ident => ($label:literal, $title:literal, $order:literal)),+ $(,)?) => {
        $(pub static $name: Stage = Stage::new($label, $title, $order);)+
        static ALL: &[&Stage] = &[$(&$name),+];
    };
}

stages! {
    BOOTSTRAP => ("bootstrap", "Initial load", "01"),
    BOOTSTRAP_WALK => ("bootstrap_walk", "Backup page walk", "02"),
    BOOTSTRAP_WINDOW => ("bootstrap_window", "Backup window replay", "03"),
    SHADOW_REPLAY => ("shadow_replay", "Shadow start", "04"),
    BOOTSTRAP_DEFERRED => ("bootstrap_deferred", "Large-value fill", "05"),
    BOOTSTRAP_GATE => ("bootstrap_gate", "Visibility check", "06"),
    COPY => ("copy", "Table COPY", "07"),
    INSERT_FLUSH => ("insert_flush", "Insert flush", "08"),
    PUBLISH => ("publish", "Publish", "09"),
    SETTLE => ("settle", "Settle", "10"),
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
            "walshadow_stage_count",
            "Per-stage counts by measure: work_total is the units of work the stage has in total (backup parts for the page walk, spool bytes for the large-value fill, spools for the visibility check), work_done the units finished so far, rows the row count that describes the stage (decoded from backup pages, superseded by window WAL, needing the visibility check). A measure is absent until the stage reports it",
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
        let stage = Stage::new("t", "T", "99");
        assert!(set_values(|_| &stage.rows).next().is_none());
        BOOTSTRAP_WALK.set_work(3, 12);
        BOOTSTRAP_WALK.set_rows(40);
        let work: Vec<_> = set_values(|s| &s.work_units).collect();
        assert!(
            work.iter()
                .any(|(s, v)| s.name == "bootstrap_walk" && *v == 12)
        );
        let rows: Vec<_> = set_values(|s| &s.rows).collect();
        assert!(
            rows.iter()
                .any(|(s, v)| s.name == "bootstrap_walk" && *v == 40)
        );
        assert_eq!(
            BOOTSTRAP_WALK.labels(),
            [
                ("stage", "bootstrap_walk"),
                ("title", "Backup page walk"),
                ("order", "02")
            ]
        );
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
