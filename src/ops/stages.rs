//! Process-wide elapsed timings, including waits and overlapping work

use std::fmt::Write;
use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

pub fn render(out: &mut String) {
    static EPOCH: OnceLock<f64> = OnceLock::new();
    let epoch = EPOCH.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    });
    writeln!(
        out,
        "# HELP walshadow_stage_epoch_seconds Timing registry identity, changes after restart"
    )
    .unwrap();
    writeln!(out, "# TYPE walshadow_stage_epoch_seconds gauge").unwrap();
    writeln!(out, "walshadow_stage_epoch_seconds {epoch}").unwrap();
    for (suffix, kind, help) in [
        (
            "seconds_total",
            "counter",
            "Elapsed seconds of finished attempts, including waits and overlaps",
        ),
        (
            "completed_total",
            "counter",
            "Successfully finished stage attempts",
        ),
        (
            "interrupted_total",
            "counter",
            "Failed or cancelled stage attempts",
        ),
        ("active", "gauge", "Currently running stage attempts"),
    ] {
        writeln!(out, "# HELP walshadow_stage_{suffix} {help}").unwrap();
        writeln!(out, "# TYPE walshadow_stage_{suffix} {kind}").unwrap();
        for stage in ALL {
            let value = match suffix {
                "seconds_total" => stage.nanos.load(Ordering::Relaxed) as f64 / 1e9,
                "completed_total" => stage.completed.load(Ordering::Relaxed) as f64,
                "interrupted_total" => stage.interrupted.load(Ordering::Relaxed) as f64,
                _ => stage.active.load(Ordering::Relaxed) as f64,
            };
            writeln!(
                out,
                "walshadow_stage_{suffix}{{stage=\"{}\"}} {value}",
                stage.name
            )
            .unwrap();
        }
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
