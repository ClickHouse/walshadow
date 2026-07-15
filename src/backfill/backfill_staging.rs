//! Staging-table coherence for backup-sourced initial loads
//! (plans/add_table.md §Staging swap).
//!
//! A pass's rows land in `<table>__wsstg`, never the destination; success
//! publishes atomically via `EXCHANGE TABLES` then copies the live-window
//! rows (`_lsn > S`) back from the swapped-out storage; failure leaves the
//! destination untouched and a retry rebuilds staging from scratch. No
//! partial pass can leak rows a retry's source cannot tombstone (LATEST
//! re-resolution drift) and a re-opt-in purges stale rows wholesale.
//!
//! Statement discipline: DROP/CREATE/INSERT..SELECT are idempotent (dedup
//! absorbs a copy-back resend) and retry like the inserter pool; `EXCHANGE`
//! is single-shot — a blind resend after an ambiguous timeout would swap
//! back. Ambiguity resolves through the ledger instead: the staging table's
//! uuid is persisted before the exchange, so recovery can tell "not yet
//! swapped" (uuid unchanged under the staging name) from "swapped" (uuid
//! differs) from "already copied back" (staging name gone).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clickhouse_c::{AsyncClient, Block, Event};

use crate::backfill::backfill_types::BackupRequest;
use crate::ch::{
    EmitterError, backoff_step, connect_client, exec_drain, is_retryable, quote_ident, with_timeout,
};
use crate::emit::ch_emitter::{EmitterConfig, RetryConfig};
use crate::mapping::{MappingHandle, TableMapping, TableTarget};
use crate::schema::RelName;

/// `orders` loads into `orders__wsstg`; deterministic so a retry or boot
/// recovery finds the prior attempt's table
pub const STAGING_SUFFIX: &str = "__wsstg";

/// One rel's swap identities. `database`/`table` are the unquoted
/// destination parts; `s_lsn` drives the copy-back filter.
#[derive(Debug, Clone)]
pub struct StagingRel {
    pub rel: RelName,
    pub database: String,
    pub table: String,
    pub s_lsn: u64,
}

impl StagingRel {
    pub fn staging_table(&self) -> String {
        format!("{}{STAGING_SUFFIX}", self.table)
    }

    pub fn real_sql(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&self.table)
        )
    }

    pub fn staging_sql(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&self.staging_table())
        )
    }
}

/// Per-pass staging setup: routing snapshot targeting the staging tables
/// plus the rels to publish on success. Rels unmapped at prepare are absent
/// from both — their rows walk-and-skip exactly as without staging.
pub struct StagingPlan {
    pub mapping: MappingHandle,
    pub rels: Vec<StagingRel>,
}

/// Rebuild one staging table per mapped rel (`DROP` + `CREATE .. AS` clones
/// structure and engine) and snapshot the routing map against them.
pub async fn prepare(
    emitter: &EmitterConfig,
    live: &MappingHandle,
    reqs: &[BackupRequest],
) -> Result<StagingPlan> {
    let mut sess = StagingSession::connect(emitter).await?;
    let live_map = live.read().await.clone();
    let mut staged: HashMap<RelName, TableMapping> = HashMap::new();
    let mut rels = Vec::new();
    for r in reqs {
        let name = &r.desc.rel_name;
        let Some(m) = live_map.get(name) else {
            tracing::warn!(
                target: "walshadow::backfill_staging",
                qname = %name,
                "no mapping at pass start; rows will skip",
            );
            continue;
        };
        let rel = StagingRel {
            rel: name.clone(),
            database: m.target.database.clone(),
            table: m.target.table.clone(),
            s_lsn: r.s_lsn,
        };
        sess.rebuild_staging(&rel)
            .await
            .with_context(|| format!("backfill_staging: rebuild staging for {name}"))?;
        staged.insert(
            name.clone(),
            TableMapping {
                target: TableTarget::new(&rel.database, &rel.staging_table()),
                columns: m.columns.clone(),
            },
        );
        rels.push(rel);
    }
    Ok(StagingPlan {
        mapping: Arc::new(tokio::sync::RwLock::new(staged)),
        rels,
    })
}

/// One CH control connection for staging DDL + swap statements, with the
/// inserter pool's bounded per-attempt timeout.
pub struct StagingSession {
    client: AsyncClient,
    conn: EmitterConfig,
    retry: RetryConfig,
    timeout: Duration,
}

impl StagingSession {
    pub async fn connect(emitter: &EmitterConfig) -> Result<Self> {
        let client = connect_client(emitter)
            .await
            .map_err(|e| anyhow::anyhow!("backfill_staging: connect: {e}"))?;
        Ok(Self {
            client,
            conn: emitter.clone(),
            retry: emitter.retry.clone(),
            timeout: emitter.insert_timeout,
        })
    }

    async fn attempt_write(&mut self, sql: &str) -> Result<(), EmitterError> {
        exec_drain(&mut self.client, sql, self.timeout).await
    }

    /// Statement safe to re-apply (DROP/CREATE IF NOT EXISTS, dedup-absorbed
    /// INSERT..SELECT): reconnect + resend on retryable failure.
    async fn exec_retry(&mut self, sql: &str) -> Result<()> {
        let mut attempt = 0u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            match self.attempt_write(sql).await {
                Ok(()) => return Ok(()),
                Err(e) if is_retryable(&e) && attempt < self.retry.max_attempts => {
                    tracing::warn!(
                        target: "walshadow::backfill_staging",
                        error = %e, attempt, sql,
                        "statement failed; reconnecting + retrying",
                    );
                    attempt += 1;
                    backoff_step(&mut backoff, self.retry.max_backoff).await;
                    self.client = connect_client(&self.conn)
                        .await
                        .map_err(|e| anyhow::anyhow!("backfill_staging: reconnect: {e}"))?;
                }
                Err(e) => return Err(anyhow::anyhow!("backfill_staging: {sql}: {e}")),
            }
        }
    }

    /// Single attempt, no resend: an ambiguous timeout may have applied
    /// server-side. Callers resolve through the ledger's staging uuid.
    async fn exec_once(&mut self, sql: &str) -> Result<()> {
        self.attempt_write(sql)
            .await
            .map_err(|e| anyhow::anyhow!("backfill_staging: {sql}: {e}"))
    }

    /// Single-column String SELECT, one attempt under the timeout.
    async fn query_strings(&mut self, sql: &str) -> Result<Vec<String>> {
        with_timeout(self.timeout, async {
            self.client.send_query(sql, None).await?;
            let mut out = Vec::new();
            loop {
                match self.client.recv_event().await? {
                    Event::Data(block) => read_string_column(&block, &mut out)?,
                    Event::EndOfStream => break,
                    Event::Exception(exc) => {
                        return Err(EmitterError::ServerException {
                            code: exc.code(),
                            message: String::from_utf8_lossy(exc.display_text()).into_owned(),
                        });
                    }
                    _ => {}
                }
            }
            Ok::<_, EmitterError>(out)
        })
        .await
        .map_err(|e| anyhow::anyhow!("backfill_staging: {sql}: {e}"))
    }

    pub async fn rebuild_staging(&mut self, rel: &StagingRel) -> Result<()> {
        self.exec_retry(&format!("DROP TABLE IF EXISTS {}", rel.staging_sql()))
            .await?;
        // IF NOT EXISTS only shields an ambiguous-timeout resend; the table
        // is fresh from the DROP above either way
        self.exec_retry(&format!(
            "CREATE TABLE IF NOT EXISTS {} AS {}",
            rel.staging_sql(),
            rel.real_sql()
        ))
        .await
    }

    pub async fn drop_staging(&mut self, rel: &StagingRel) -> Result<()> {
        self.exec_retry(&format!("DROP TABLE IF EXISTS {}", rel.staging_sql()))
            .await
    }

    /// Ordered `name type` list; equality across real/staging gates the swap
    /// (a mid-pass DDL means the loaded copy has the pre-DDL shape).
    pub async fn schema_fingerprint(&mut self, database: &str, table: &str) -> Result<Vec<String>> {
        self.query_strings(&format!(
            "SELECT concat(name, ' ', type) FROM system.columns \
             WHERE database = {} AND table = {} ORDER BY position",
            sql_str(database),
            sql_str(table)
        ))
        .await
    }

    /// `None` when the table doesn't exist.
    pub async fn table_uuid(&mut self, database: &str, table: &str) -> Result<Option<String>> {
        let rows = self
            .query_strings(&format!(
                "SELECT toString(uuid) FROM system.tables \
                 WHERE database = {} AND name = {}",
                sql_str(database),
                sql_str(table)
            ))
            .await?;
        Ok(rows.into_iter().next())
    }

    /// Atomic publish; requires an Atomic/Replicated database engine.
    pub async fn exchange(&mut self, rel: &StagingRel) -> Result<()> {
        self.exec_once(&format!(
            "EXCHANGE TABLES {} AND {}",
            rel.real_sql(),
            rel.staging_sql()
        ))
        .await
    }

    /// Recover the live window from the swapped-out storage: rows the live
    /// stream delivered during the pass carry `_lsn > S`; anything at or
    /// below `S` is prior-life state the swap just purged and must not come
    /// back. Column list is the intersection (destination order) so DDL
    /// applied to the destination after the swap can't wedge the copy-back.
    pub async fn copy_back(&mut self, rel: &StagingRel) -> Result<()> {
        let real_cols = self
            .query_strings(&format!(
                "SELECT name FROM system.columns WHERE database = {} AND table = {} \
                 ORDER BY position",
                sql_str(&rel.database),
                sql_str(&rel.table)
            ))
            .await?;
        let staging_cols: HashSet<String> = self
            .query_strings(&format!(
                "SELECT name FROM system.columns WHERE database = {} AND table = {} \
                 ORDER BY position",
                sql_str(&rel.database),
                sql_str(&rel.staging_table())
            ))
            .await?
            .into_iter()
            .collect();
        let cols: Vec<String> = real_cols
            .iter()
            .filter(|c| staging_cols.contains(*c))
            .map(|c| quote_ident(c))
            .collect();
        if cols.is_empty() {
            bail!(
                "backfill_staging: no shared columns between {} and {}",
                rel.real_sql(),
                rel.staging_sql()
            );
        }
        let list = cols.join(", ");
        self.exec_retry(&format!(
            "INSERT INTO {} ({list}) SELECT {list} FROM {} WHERE `_lsn` > {}",
            rel.real_sql(),
            rel.staging_sql(),
            rel.s_lsn
        ))
        .await
    }
}

/// Append one Data block's single String column into `out`; the 0-row
/// header block contributes nothing.
fn read_string_column(block: &Block, out: &mut Vec<String>) -> Result<(), EmitterError> {
    let n = block.n_rows();
    if n == 0 {
        return Ok(());
    }
    let col = block
        .column(0)
        .ok_or_else(|| EmitterError::Type("backfill_staging: missing result column".into()))?;
    let (offsets, data) = col
        .string()
        .ok_or_else(|| EmitterError::Type("backfill_staging: result column not String".into()))?;
    for i in 0..n {
        let start = if i == 0 { 0 } else { offsets[i - 1] as usize };
        let end = offsets[i] as usize;
        out.push(String::from_utf8_lossy(&data[start..end]).into_owned());
    }
    Ok(())
}

/// CH single-quoted string literal.
fn sql_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_rel_renders_sql_names() {
        let rel = StagingRel {
            rel: RelName::new("public", "orders"),
            database: "db".into(),
            table: "orders".into(),
            s_lsn: 0x5000,
        };
        assert_eq!(rel.real_sql(), "`db`.`orders`");
        assert_eq!(rel.staging_table(), "orders__wsstg");
        assert_eq!(rel.staging_sql(), "`db`.`orders__wsstg`");
    }

    #[test]
    fn sql_str_escapes_quotes_and_backslashes() {
        assert_eq!(sql_str("plain"), "'plain'");
        assert_eq!(sql_str("o'brien"), "'o\\'brien'");
        assert_eq!(sql_str("a\\b"), "'a\\\\b'");
    }
}
