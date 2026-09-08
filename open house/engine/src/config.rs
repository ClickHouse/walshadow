use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub profile: String,
    pub detector: Detector,
    pub delay: Delay,
    pub collector: Collector,
    pub probe: Probe,
    pub destination: Destination,
    pub shock: Shock,
    pub snapshot: Snapshot,
    pub profiles: HashMap<String, Profile>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Detector {
    pub baseline_trades_min: i64,
    pub recent_trades_min: i64,
    pub buy_multiple_min: f64,
    pub imbalance_min: f64,
    pub cooldown_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Delay {
    pub simulated_min_ms: u64,
    pub simulated_max_ms: u64,
}


#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Collector {
    pub interval_ms: u64,
    pub query_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Probe {
    pub workers: usize,
    pub interval_ms: u64,
    pub poll_interval_ms: u64,
    pub timeout_ms: u64,
    pub window_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Destination {
    #[serde(rename = "final")]
    pub use_final: bool,
    pub deleted_filter: bool,
    pub table: String,
    pub probe_market_id: i32,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Shock {
    pub max_duration_ms: u64,
    pub price_path_to: i32,
    pub recovery_secs: f64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Snapshot {
    pub push_interval_ms: u64,
    pub chart_points: usize,
    pub tape_rows: usize,
    pub history_runs: usize,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Profile {
    pub rate: u64,
    pub rows_per_commit: u64,
    pub connections: usize,
    pub active_markets: i32,
    pub burst_multiplier: u64,
}

impl Config {
    pub fn load(path: &str, profile_override: Option<String>) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {path}"))?;
        let mut cfg: Config = toml::from_str(&text)
            .with_context(|| format!("parsing config {path}"))?;
        if let Some(p) = profile_override {
            cfg.profile = p;
        }
        if !cfg.profiles.contains_key(&cfg.profile) {
            anyhow::bail!("profile '{}' not defined in {path}", cfg.profile);
        }
        Ok(cfg)
    }

    pub fn active_profile(&self) -> Profile {
        self.profiles[&self.profile]
    }
}
