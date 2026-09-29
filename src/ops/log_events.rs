use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, PoisonError, RwLock};

use ahash::{HashMap, HashMapExt};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;

use crate::ops::metrics::{counter_pairs, counter_series};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

type Key = (&'static str, &'static str);

static EVENTS: LazyLock<RwLock<HashMap<Key, AtomicU64>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

const LEVELS: [Level; 5] = [
    Level::ERROR,
    Level::WARN,
    Level::INFO,
    Level::DEBUG,
    Level::TRACE,
];

fn record(key: Key) {
    if let Some(n) = EVENTS
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
    {
        n.fetch_add(1, Ordering::Relaxed);
        return;
    }
    EVENTS
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(key)
        .or_default()
        .fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LogEventLayer;

impl<S: Subscriber> Layer<S> for LogEventLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        record((meta.level().as_str(), meta.target()));
    }
}

#[derive(Debug)]
pub struct LogEventCollector;

impl Collector for LogEventCollector {
    fn encode(&self, mut enc: DescriptorEncoder) -> fmt::Result {
        let map = EVENTS.read().unwrap_or_else(PoisonError::into_inner);
        let mut rows: Vec<_> = map
            .iter()
            .map(|(&(level, target), n)| (level, target, n.load(Ordering::Relaxed)))
            .collect();
        rows.sort_unstable();

        counter_series(
            &mut enc,
            "walshadow_log_level_events",
            "Log events emitted by this process, summed over targets. Always present, so a first event is an increase rather than a new series.",
            "level",
            LEVELS.map(|l| l.as_str()).into_iter().map(|level| {
                let n: u64 = rows
                    .iter()
                    .filter(|(l, _, _)| *l == level)
                    .map(|(_, _, n)| n)
                    .sum();
                (level, n)
            }),
        )?;
        counter_pairs(
            &mut enc,
            "walshadow_log_events",
            "Log events emitted by this process, labeled by level + target.",
            ["level", "target"],
            rows.iter()
                .map(|(level, target, n)| ([*level, *target], *n)),
        )
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::prelude::*;

    use super::*;
    use crate::ops::metrics::{MetricsSnapshot, render};

    const TARGET: &str = "walshadow::log_events_test";

    fn count(level: &str, target: &str) -> u64 {
        EVENTS
            .read()
            .expect("counter map")
            .iter()
            .find(|((l, t), _)| *l == level && *t == target)
            .map_or(0, |(_, n)| n.load(Ordering::Relaxed))
    }

    #[test]
    fn every_level_total_is_present_before_any_event() {
        let out = render(MetricsSnapshot::default());
        for level in LEVELS.map(|l| l.as_str()) {
            assert!(
                out.contains(&format!(
                    "walshadow_log_level_events_total{{level=\"{level}\"}}"
                )),
                "{level} missing from {out}"
            );
        }
    }

    #[test]
    fn counts_by_level_and_target_then_renders_them() {
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(LogEventLayer),
            || {
                tracing::warn!(target: TARGET, "one");
                tracing::info!(target: TARGET, "two");
                tracing::info!(target: TARGET, "three");
            },
        );
        assert_eq!(count("WARN", TARGET), 1);
        assert_eq!(count("INFO", TARGET), 2);
        assert_eq!(count("ERROR", TARGET), 0);

        let out = render(MetricsSnapshot::default());
        assert!(
            out.contains(&format!(
                "walshadow_log_events_total{{level=\"INFO\",target=\"{TARGET}\"}} 2"
            )),
            "{out}"
        );
        let warns = count("WARN", TARGET);
        assert!(
            out.lines().any(|l| l
                .strip_prefix("walshadow_log_level_events_total{level=\"WARN\"} ")
                .and_then(|n| n.trim().parse::<u64>().ok())
                .is_some_and(|n| n >= warns)),
            "per-level total must cover this target's warns: {out}"
        );
    }
}
