use crate::ch::ChClient;
use crate::config::{Collector as CollectorCfg, Destination};
use crate::detector::{Frame, MarketFeatures};
use anyhow::Result;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Rewrites the canonical `sql/collector.sql` for the destination actually in
/// use. The file on disk stays runnable as written; `FINAL` and the delete
/// filter are only added when the destination carries those semantics.
pub fn build_query(template: &str, dest: &Destination) -> String {
    let mut sql = template.replace(
        "FROM market_trades",
        &format!("FROM {}{}", dest.table, if dest.use_final { " FINAL" } else { "" }),
    );
    if dest.deleted_filter {
        sql = sql.replace(
            "WHERE scenario_tag != 'probe'",
            "WHERE _is_deleted = 0\n          AND scenario_tag != 'probe'",
        );
    }
    sql.push_str("\nSETTINGS output_format_json_quote_64bit_integers = 0\nFORMAT JSONCompactEachRow");
    sql
}

fn parse_rows(body: &str) -> Result<Vec<MarketFeatures>> {
    let mut out = Vec::new();
    for line in body.lines().filter(|l| !l.trim().is_empty()) {
        let v: Vec<serde_json::Value> = serde_json::from_str(line)?;
        if v.len() < 9 {
            anyhow::bail!("collector row has {} columns, expected 9", v.len());
        }
        let i = |n: usize| v[n].as_i64().unwrap_or(0);
        let f = |n: usize| v[n].as_f64().unwrap_or(0.0);
        out.push(MarketFeatures {
            market_id: i(0) as i32,
            last_price_cents: i(1) as i32,
            buy_1s: i(2),
            sell_1s: i(3),
            normal_buy_per_s: f(4),
            baseline_trades: i(5),
            recent_trades: i(6),
            buy_multiple: f(7),
            imbalance: f(8),
        });
    }
    Ok(out)
}

pub struct Collector {
    ch: ChClient,
    sql: String,
    cfg: CollectorCfg,
    next_frame_id: AtomicU64,
}

impl Collector {
    pub fn new(ch: ChClient, template: &str, dest: &Destination, cfg: CollectorCfg) -> Self {
        Self {
            ch,
            sql: build_query(template, dest),
            cfg,
            next_frame_id: AtomicU64::new(1),
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// One collector pass. The window end `t` comes from this process's clock,
    /// the same clock the generator stamps `event_ts` with, so the feature
    /// window never depends on cross-machine clock agreement.
    pub async fn collect(&self, feature_ts: chrono::DateTime<chrono::Utc>) -> Result<Frame> {
        let feature_at = Instant::now();
        let param = feature_ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let resp = self
            .ch
            .query(
                &self.sql,
                &[("t", param)],
                Duration::from_millis(self.cfg.query_timeout_ms),
            )
            .await?;
        let completed = Instant::now();
        let rows = parse_rows(&resp.body)?;
        Ok(Frame {
            frame_id: self.next_frame_id.fetch_add(1, Ordering::Relaxed),
            feature_ts,
            feature_at,
            query_completed_at: completed,
            query_elapsed_ms: resp.elapsed_ms(),
            markets_evaluated: rows.len(),
            read_rows: resp.read_rows,
            read_bytes: resp.read_bytes,
            rows,
        })
    }
}

/// Releases each frame to the delayed consumer after a simulated pipeline
/// delay, using the monotonic clock. The frame itself is shared, never
/// recomputed — the delayed side evaluates the original feature values, not a
/// fresh query against a newer window.
///
/// The delay wanders between `min` and `max` rather than sitting on one value,
/// because a batch-and-load pipeline does not deliver on a metronome. It moves
/// as a slow random walk: a step smaller than the collector interval cannot
/// reorder two adjacent frames, and a monotonic clamp backs that up. Frames
/// must reach the delayed consumer in the order the live one saw them, or the
/// comparison stops being a replay.
pub async fn delay_queue(
    mut rx: tokio::sync::mpsc::Receiver<Arc<Frame>>,
    tx: tokio::sync::mpsc::Sender<(Arc<Frame>, Instant)>,
    min: Duration,
    max: Duration,
) {
    const STEP_MS: f64 = 60.0;
    let (lo, hi) = (min.as_secs_f64() * 1000.0, max.as_secs_f64() * 1000.0);
    let mut delay_ms = (lo + hi) / 2.0;
    let mut last_release: Option<Instant> = None;

    while let Some(frame) = rx.recv().await {
        delay_ms = (delay_ms + rand::rng().random_range(-STEP_MS..=STEP_MS)).clamp(lo, hi);
        let mut release_at = frame.feature_at + Duration::from_secs_f64(delay_ms / 1000.0);
        if let Some(prev) = last_release
            && release_at <= prev
        {
            release_at = prev + Duration::from_millis(1);
        }
        last_release = Some(release_at);

        let now = Instant::now();
        if release_at > now {
            tokio::time::sleep(release_at - now).await;
        }
        if tx.send((frame, release_at)).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dest(use_final: bool, deleted: bool) -> Destination {
        Destination {
            use_final,
            deleted_filter: deleted,
            table: "market_trades".into(),
            probe_market_id: 100,
        }
    }

    #[test]
    fn plain_destination_gets_no_final_and_no_delete_filter() {
        let sql = build_query("FROM market_trades\nWHERE scenario_tag != 'probe'", &dest(false, false));
        assert!(!sql.contains("FINAL"));
        assert!(!sql.contains("_is_deleted"));
    }

    #[test]
    fn cdc_destination_gets_both() {
        let sql = build_query("FROM market_trades\nWHERE scenario_tag != 'probe'", &dest(true, true));
        assert!(sql.contains("FROM market_trades FINAL"));
        assert!(sql.contains("_is_deleted = 0"));
    }

    #[test]
    fn renamed_destination_table_is_substituted() {
        let d = Destination { table: "cdc_trades".into(), ..dest(false, false) };
        let sql = build_query("FROM market_trades\nWHERE scenario_tag != 'probe'", &d);
        assert!(sql.contains("FROM cdc_trades"));
    }

    #[test]
    fn parses_a_collector_row() {
        let body = "[101,42,1000,10,10.5,300,200,95.2,0.98]\n";
        let rows = parse_rows(body).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].market_id, 101);
        assert_eq!(rows[0].buy_1s, 1000);
        assert!((rows[0].imbalance - 0.98).abs() < 1e-9);
    }
}
