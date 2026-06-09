//! Pre-flight validators run at daemon connect. Refuse to start when:
//!
//! - source `server_version_num` < 160_000.
//! - shadow/source major mismatch: a same-physical-WAL standby can't span
//!   majors, PG's catalog layout changes across them.
//! - source `wal_level` not `logical` ([PLAN.md §4]; physical-only WAL
//!   omits the old-tuple bytes UPDATE/DELETE need).
//! - any mapped relation without `REPLICA IDENTITY FULL` ([PLAN.md
//!   §"Pitfall #7"]; UPDATE without FULL emits only changed columns, the
//!   decoder can't reconstruct the new row).
//! - `--slot` names a physical slot absent on source.

use std::fmt;

use thiserror::Error;
use tokio_postgres::Client;

use crate::ch_emitter::EmitterConfig;

/// Catalog accessors assume PG-16 column layouts; PG <16 unsupported.
pub const MIN_SERVER_VERSION_NUM: i32 = 160_000;

#[derive(Debug, Error)]
pub enum PreflightError {
    #[error(
        "source server_version_num {got} < {min} (walshadow requires PostgreSQL 16+; \
         upgrade the source cluster or pin walshadow to a release that supports {got})"
    )]
    SourceVersionTooOld { got: i32, min: i32 },
    #[error(
        "shadow major version {shadow_major} ≠ source major {source_major} \
         (server_version_num shadow={shadow_num}, source={source_num}); \
         a basebackup-cloned shadow must match the source major"
    )]
    MajorMismatch {
        source_num: i32,
        shadow_num: i32,
        source_major: i32,
        shadow_major: i32,
    },
    #[error("source wal_level={got:?}, expected {expected:?}")]
    WalLevel { got: String, expected: &'static str },
    #[error(
        "source replication slot {slot:?} does not exist (create it with \
         SELECT pg_create_physical_replication_slot({slot:?}), or omit --slot)"
    )]
    SlotMissing { slot: String },
    #[error(
        "mapped relation {rel} has REPLICA IDENTITY {got:?}, expected 'f' \
         (FULL); ALTER TABLE {rel} REPLICA IDENTITY FULL on the source"
    )]
    BadReplicaIdentity { rel: String, got: char },
    #[error(
        "mapped relation {rel} not found on source (configured in --ch-config \
         but `{rel}::regclass` resolves to nothing)"
    )]
    MappedRelMissing { rel: String },
    #[error("pg query: {0}")]
    Pg(#[from] tokio_postgres::Error),
    #[error("shadow_version_num could not be parsed: {0:?}")]
    BadShadowVersion(String),
}

/// All validator findings surfaced at once so operators don't fix one
/// issue, restart, and hit the next.
#[derive(Debug)]
pub struct PreflightReport {
    pub errors: Vec<PreflightError>,
}

impl PreflightReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn into_result(self) -> Result<(), PreflightReport> {
        if self.is_ok() { Ok(()) } else { Err(self) }
    }
}

impl fmt::Display for PreflightReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "pre-flight failed ({} issue(s)):", self.errors.len())?;
        for (i, e) in self.errors.iter().enumerate() {
            writeln!(f, "  {}. {e}", i + 1)?;
        }
        Ok(())
    }
}

impl std::error::Error for PreflightReport {}

/// Soft findings append to the report; hard errors (tokio-postgres
/// transport failures) short-circuit [`run`].
pub struct Inputs<'a> {
    pub source_version_num: i32,
    pub source_sql: &'a Client,
    pub shadow_sql: &'a Client,
    pub slot: Option<&'a str>,
    pub ch_config: Option<&'a EmitterConfig>,
}

pub async fn run(input: Inputs<'_>) -> Result<PreflightReport, PreflightError> {
    let mut report = PreflightReport { errors: Vec::new() };

    if input.source_version_num < MIN_SERVER_VERSION_NUM {
        report.errors.push(PreflightError::SourceVersionTooOld {
            got: input.source_version_num,
            min: MIN_SERVER_VERSION_NUM,
        });
    }

    let shadow_num_str = scalar_text(input.shadow_sql, "SHOW server_version_num").await?;
    let shadow_num = shadow_num_str
        .trim()
        .parse::<i32>()
        .map_err(|_| PreflightError::BadShadowVersion(shadow_num_str))?;
    let source_major = input.source_version_num / 10_000;
    let shadow_major = shadow_num / 10_000;
    if source_major != shadow_major {
        report.errors.push(PreflightError::MajorMismatch {
            source_num: input.source_version_num,
            shadow_num,
            source_major,
            shadow_major,
        });
    }

    let wal_level = scalar_text(input.source_sql, "SHOW wal_level").await?;
    if wal_level != "logical" {
        report.errors.push(PreflightError::WalLevel {
            got: wal_level,
            expected: "logical",
        });
    }

    if let Some(slot) = input.slot {
        let row = input
            .source_sql
            .query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await?;
        if row.is_none() {
            report
                .errors
                .push(PreflightError::SlotMissing { slot: slot.into() });
        }
    }

    if let Some(cfg) = input.ch_config {
        for key in cfg.tables.keys() {
            // Keys are `"namespace.relname"`. `to_regclass(text)` honours
            // search_path/quoting and returns NULL (not raise) on a missing
            // relation; the pg_class join yields one row of relreplident.
            let row = input
                .source_sql
                .query_opt(
                    "SELECT c.relreplident::text \
                     FROM pg_class c \
                     WHERE c.oid = to_regclass($1)",
                    &[&key.as_str()],
                )
                .await?;
            match row {
                Some(r) => {
                    let id: String = r.get(0);
                    let ch = id.chars().next().unwrap_or('?');
                    if ch != 'f' {
                        report.errors.push(PreflightError::BadReplicaIdentity {
                            rel: key.clone(),
                            got: ch,
                        });
                    }
                }
                None => report
                    .errors
                    .push(PreflightError::MappedRelMissing { rel: key.clone() }),
            }
        }
    }

    Ok(report)
}

async fn scalar_text(client: &Client, sql: &str) -> Result<String, tokio_postgres::Error> {
    let row = client.query_one(sql, &[]).await?;
    Ok(row.get::<_, String>(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_aggregates_multiple_errors() {
        let r = PreflightReport {
            errors: vec![
                PreflightError::SourceVersionTooOld {
                    got: 150_000,
                    min: MIN_SERVER_VERSION_NUM,
                },
                PreflightError::WalLevel {
                    got: "replica".into(),
                    expected: "logical",
                },
            ],
        };
        let rendered = format!("{r}");
        assert!(rendered.contains("2 issue"), "{rendered}");
        assert!(rendered.contains("server_version_num"), "{rendered}");
        assert!(rendered.contains("wal_level"), "{rendered}");
    }

    #[test]
    fn report_ok_when_empty() {
        let r = PreflightReport { errors: Vec::new() };
        assert!(r.is_ok());
        assert!(r.into_result().is_ok());
    }

    #[test]
    fn major_decode_matches_pg_convention() {
        // post-PG-10 layout: major = num / 10_000
        assert_eq!(160_004 / 10_000, 16);
        assert_eq!(170_000 / 10_000, 17);
        assert_eq!(150_009 / 10_000, 15);
    }
}
