use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ChClient {
    http: reqwest::Client,
    endpoint: String,
    database: String,
    user: String,
    password: String,
}

#[derive(Debug, Clone)]
pub struct ChResponse {
    pub body: String,
    /// Server-reported execution time. Falls back to client-observed elapsed
    /// when the summary header is absent, and says which it used.
    pub server_elapsed_ms: Option<f64>,
    pub client_elapsed_ms: f64,
    pub read_rows: Option<u64>,
    pub read_bytes: Option<u64>,
}

impl ChResponse {
    pub fn elapsed_ms(&self) -> f64 {
        self.server_elapsed_ms.unwrap_or(self.client_elapsed_ms)
    }
}

#[derive(Deserialize)]
struct Summary {
    elapsed_ns: Option<String>,
    read_rows: Option<String>,
    read_bytes: Option<String>,
}

impl ChClient {
    /// Accepts `http://user:pass@host:8123/database`.
    pub fn from_url(url: &str) -> Result<Self> {
        let parsed = reqwest::Url::parse(url).with_context(|| format!("parsing CH_URL {url}"))?;
        let database = parsed.path().trim_matches('/').to_string();
        if database.is_empty() {
            bail!("CH_URL must include a database path, e.g. http://host:8123/openhouse");
        }
        let user = if parsed.username().is_empty() { "default" } else { parsed.username() };
        let password = parsed.password().unwrap_or("").to_string();
        let mut base = parsed.clone();
        base.set_path("");
        let _ = base.set_username("");
        let _ = base.set_password(None);
        Ok(Self {
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(32)
                .build()?,
            endpoint: base.to_string().trim_end_matches('/').to_string(),
            database,
            user: user.to_string(),
            password,
        })
    }

    pub async fn query(
        &self,
        sql: &str,
        params: &[(&str, String)],
        timeout: Duration,
    ) -> Result<ChResponse> {
        let started = std::time::Instant::now();
        let mut req = self
            .http
            .post(&self.endpoint)
            .query(&[("database", self.database.as_str())])
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .timeout(timeout);
        for (k, v) in params {
            req = req.query(&[(format!("param_{k}"), v.as_str())]);
        }
        let resp = req.body(sql.to_string()).send().await?;
        let client_elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

        let summary = resp
            .headers()
            .get("x-clickhouse-summary")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| serde_json::from_str::<Summary>(s).ok());

        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("clickhouse {status}: {}", body.trim());
        }

        let parse = |o: Option<String>| o.and_then(|s| s.parse::<u64>().ok());
        Ok(ChResponse {
            body,
            server_elapsed_ms: summary
                .as_ref()
                .and_then(|s| s.elapsed_ns.clone())
                .and_then(|s| s.parse::<f64>().ok())
                .map(|ns| ns / 1_000_000.0),
            client_elapsed_ms,
            read_rows: parse(summary.as_ref().and_then(|s| s.read_rows.clone())),
            read_bytes: parse(summary.and_then(|s| s.read_bytes)),
        })
    }
}
