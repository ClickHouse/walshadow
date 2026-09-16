//! Process-wide elapsed timings, including waits and overlapping work

use std::fmt;
use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;

use crate::ops::metrics::{counter_series, gauge, gauge_series};

pub struct Stage {
    name: &'static str,
    nanos: AtomicU64,
    completed: AtomicU64,
    interrupted: AtomicU64,
    active: AtomicU64,
}

impl Stage {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            nanos: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            interrupted: AtomicU64::new(0),
            active: AtomicU64::new(0),
        }
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
    ($($name:ident => $label:literal),+ $(,)?) => {
        $(pub static $name: Stage = Stage::new($label);)+
        static ALL: &[&Stage] = &[$(&$name),+];
    };
}

stages! {
    BOOTSTRAP => "bootstrap",
    SHADOW_REPLAY => "shadow_replay",
    COPY => "copy",
    INSERT_FLUSH => "insert_flush",
    PUBLISH => "publish",
    SETTLE => "settle",
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
        counter_series(
            &mut enc,
            "walshadow_stage_seconds_total",
            "Elapsed seconds of finished attempts, including waits and overlaps",
            "stage",
            ALL.iter()
                .map(|s| (s.name, s.nanos.load(Ordering::Relaxed) as f64 / 1e9)),
        )?;
        counter_series(
            &mut enc,
            "walshadow_stage_completed_total",
            "Successfully finished stage attempts",
            "stage",
            ALL.iter()
                .map(|s| (s.name, s.completed.load(Ordering::Relaxed))),
        )?;
        counter_series(
            &mut enc,
            "walshadow_stage_interrupted_total",
            "Failed or cancelled stage attempts",
            "stage",
            ALL.iter()
                .map(|s| (s.name, s.interrupted.load(Ordering::Relaxed))),
        )?;
        gauge_series(
            &mut enc,
            "walshadow_stage_active",
            "Currently running stage attempts",
            "stage",
            ALL.iter()
                .map(|s| (s.name, s.active.load(Ordering::Relaxed))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn distinguish_success_failure_and_cancellation() {
        let stage = Stage::new("test");
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
