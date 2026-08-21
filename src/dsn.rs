//! Connection URLs → config TOML sections
//!
//! `postgres://` and `clickhouse://` strings are what a hosted provider
//! hands an operator, so they are accepted wherever `[source]` / `[ch]`
//! are configured, with no transcription of fields into TOML. Output is a
//! `[source]` / `[ch]` table, merged like any other config layer
//! ([`crate::ch_emitter::load_effective`])

use percent_encoding::percent_decode_str;
use thiserror::Error;
use tokio_postgres::config::{Host, SslMode};
use toml::{Table, Value};
use url::{ParseError, Url};

#[derive(Debug, Error)]
pub enum DsnError {
    #[error("{url:?}: expected a {expected} URL")]
    Scheme { url: String, expected: &'static str },
    #[error("{url:?}: invalid {kind} URL: {reason}")]
    Invalid {
        url: String,
        kind: &'static str,
        reason: String,
    },
    #[error("{0:?}: no host")]
    NoHost(String),
    #[error("{0:?}: port not a number in 1..=65535")]
    BadPort(String),
    #[error("{url:?}: unknown parameter {key:?} (supported: {supported})")]
    UnknownParam {
        url: String,
        key: String,
        supported: &'static str,
    },
    #[error("{url:?}: parameter {key:?} takes true or false, got {got:?}")]
    NotBool {
        url: String,
        key: String,
        got: String,
    },
    #[error("{0:?}: URL component is not UTF-8")]
    BadEncoding(String),
}

const PG_PARAMS: &str = "sslmode, slot, host, port, user, password, dbname";
const CH_PARAMS: &str = "secure, compression, database, user, password, port";

/// `postgres://user:pass@host:5432/dbname?sslmode=require&slot=walshadow`
///
/// Unix sockets ride the libpq spelling: `postgres:///dbname?host=/run/postgresql`
pub fn source_table(url: &str) -> Result<Table, DsnError> {
    // `user@` with an empty host is a libpq unix socket, which the URL crate
    // refuses, so scheme and query come apart by hand here
    let (base, query) = url.split_once('?').unwrap_or((url, ""));
    let scheme = base.split_once("://").map_or("", |(s, _)| s);
    if !["postgres", "postgresql"].contains(&scheme.to_ascii_lowercase().as_str()) {
        return Err(DsnError::Scheme {
            url: url.into(),
            expected: "postgres://",
        });
    }
    if let Some(port) = authority_port(base) {
        parse_port(url, port)?;
    }
    let mut slot = None;
    let mut host_set = false;
    let mut port_set = false;
    let mut sslmode_set = false;
    let mut retained = Vec::new();
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode(url, raw_key)?.to_ascii_lowercase();
        if key == "slot" {
            slot = Some(decode(url, raw_value)?);
        } else if matches!(
            key.as_str(),
            "sslmode" | "host" | "port" | "user" | "password" | "dbname"
        ) {
            host_set |= key == "host";
            port_set |= key == "port";
            sslmode_set |= key == "sslmode";
            retained.push(format!("{key}={raw_value}"));
        } else {
            return Err(DsnError::UnknownParam {
                url: url.into(),
                key,
                supported: PG_PARAMS,
            });
        }
    }
    let dsn = if retained.is_empty() {
        base.into()
    } else {
        format!("{base}?{}", retained.join("&"))
    };
    let config = dsn
        .parse::<tokio_postgres::Config>()
        .map_err(|e| invalid(url, "PostgreSQL", pg_reason(&e)))?;
    let mut out = Table::new();
    if let Some(v) = config.get_user() {
        out.insert("user".into(), v.into());
    }
    if let Some(v) = config.get_password() {
        let v = std::str::from_utf8(v).map_err(|e| invalid(url, "PostgreSQL", e))?;
        out.insert("password".into(), v.into());
    }
    if let Some(v) = config.get_dbname() {
        out.insert("dbname".into(), v.into());
    }
    if let Some(v) = slot {
        out.insert("slot".into(), v.into());
    }
    let sslmode = match config.get_ssl_mode() {
        SslMode::Disable => "disable",
        SslMode::Prefer => "prefer",
        SslMode::Require => "require",
        mode => {
            return Err(invalid(
                url,
                "PostgreSQL",
                format!("unsupported sslmode {mode:?}"),
            ));
        }
    };
    if sslmode_set {
        out.insert("sslmode".into(), sslmode.into());
    }

    let hosts = config.get_hosts();
    let host = if host_set {
        hosts.last()
    } else {
        match hosts {
            [host] => Some(host),
            [] => None,
            _ => {
                return Err(invalid(
                    url,
                    "PostgreSQL",
                    "multiple hosts are not supported",
                ));
            }
        }
    };
    let host = match host {
        Some(Host::Tcp(host)) => host.clone(),
        #[cfg(unix)]
        Some(Host::Unix(host)) => host
            .to_str()
            .ok_or_else(|| invalid(url, "PostgreSQL", "host is not UTF-8"))?
            .into(),
        None => return Err(DsnError::NoHost(url.into())),
    };
    out.insert("host".into(), host.into());
    let ports = config.get_ports();
    let port = match (port_set, ports) {
        (true, [.., port]) | (false, [port]) if *port != 0 => *port,
        (false, []) => 5432,
        _ => return Err(DsnError::BadPort(url.into())),
    };
    out.insert("port".into(), i64::from(port).into());
    Ok(out)
}

/// `clickhouse://user:pass@host:9000/database?compression=lz4`
///
/// `clickhouses://` is the same with TLS, matching `[ch] secure = true`
pub fn ch_table(url: &str) -> Result<Table, DsnError> {
    let parsed = parse_url(url, &["clickhouse", "clickhouses"], "clickhouse://")?;
    let secure = parsed.scheme() == "clickhouses";
    let mut out = Table::new();
    let mut port = parsed
        .port()
        .map(|port| {
            (port != 0)
                .then_some(port)
                .ok_or_else(|| DsnError::BadPort(url.into()))
        })
        .transpose()?;
    out.insert("secure".into(), secure.into());
    if !parsed.username().is_empty() {
        let v = decode(url, parsed.username())?;
        out.insert("user".into(), v.into());
    }
    if let Some(v) = parsed.password() {
        let v = decode(url, v)?;
        out.insert("password".into(), v.into());
    }
    if let Some(v) = parsed.path().strip_prefix('/').filter(|s| !s.is_empty()) {
        let v = decode(url, v)?;
        out.insert("database".into(), v.into());
    }
    for pair in parsed
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|s| !s.is_empty())
    {
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let k = decode(url, raw_key)?.to_ascii_lowercase();
        let v = decode(url, raw_value)?;
        match k.as_str() {
            "compression" | "database" | "user" | "password" => {
                out.insert(k, v.into());
            }
            "secure" => {
                out.insert("secure".into(), parse_bool(url, &k, &v)?.into());
            }
            "port" => port = Some(parse_port(url, &v)?),
            _ => {
                return Err(DsnError::UnknownParam {
                    url: url.into(),
                    key: k,
                    supported: CH_PARAMS,
                });
            }
        }
    }
    out.insert(
        "host".into(),
        parsed
            .host_str()
            .ok_or_else(|| DsnError::NoHost(url.into()))?
            .into(),
    );
    // 9440 is the CH-Native TLS port, 9000 the plaintext one
    let default_port = if secure_value(&out) { 9440 } else { 9000 };
    out.insert(
        "port".into(),
        i64::from(port.unwrap_or(default_port)).into(),
    );
    Ok(out)
}

fn secure_value(t: &Table) -> bool {
    t.get("secure").and_then(Value::as_bool).unwrap_or(false)
}

fn parse_url(url: &str, schemes: &[&str], expected: &'static str) -> Result<Url, DsnError> {
    let parsed = Url::parse(url).map_err(|e| match e {
        ParseError::InvalidPort => DsnError::BadPort(url.into()),
        _ => invalid(url, "connection", e),
    })?;
    if !schemes.contains(&parsed.scheme()) {
        return Err(DsnError::Scheme {
            url: url.into(),
            expected,
        });
    }
    Ok(parsed)
}

/// Port text of `scheme://[user[:pass]@]host[:port]`, IPv6 brackets aside
fn authority_port(base: &str) -> Option<&str> {
    let authority = base.split_once("://")?.1.split('/').next()?;
    let hostport = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let tail = hostport.rsplit_once(']').map_or(hostport, |(_, t)| t);
    tail.rsplit_once(':').map(|(_, port)| port)
}

/// tokio-postgres keeps the readable half of a parse failure in `source`
fn pg_reason(e: &tokio_postgres::Error) -> String {
    std::error::Error::source(e).map_or_else(|| e.to_string(), |src| format!("{e}: {src}"))
}

fn parse_port(url: &str, raw: &str) -> Result<u16, DsnError> {
    raw.parse::<u16>()
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| DsnError::BadPort(url.into()))
}

fn parse_bool(url: &str, key: &str, raw: &str) -> Result<bool, DsnError> {
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(DsnError::NotBool {
            url: url.into(),
            key: key.into(),
            got: raw.into(),
        }),
    }
}

fn decode(url: &str, value: &str) -> Result<String, DsnError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(String::from)
        .map_err(|_| DsnError::BadEncoding(url.into()))
}

fn invalid(url: &str, kind: &'static str, reason: impl std::fmt::Display) -> DsnError {
    DsnError::Invalid {
        url: url.into(),
        kind,
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t: &Table, k: &str) -> String {
        t.get(k)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{k} missing"))
            .into()
    }
    fn i(t: &Table, k: &str) -> i64 {
        t.get(k)
            .and_then(Value::as_integer)
            .unwrap_or_else(|| panic!("{k} missing"))
    }

    #[test]
    fn source_url_full() {
        let t = source_table("postgres://repl:s3cret@db.example:5433/app?sslmode=require&slot=ws")
            .unwrap();
        assert_eq!(s(&t, "host"), "db.example");
        assert_eq!(i(&t, "port"), 5433);
        assert_eq!(s(&t, "user"), "repl");
        assert_eq!(s(&t, "password"), "s3cret");
        assert_eq!(s(&t, "dbname"), "app");
        assert_eq!(s(&t, "sslmode"), "require");
        assert_eq!(s(&t, "slot"), "ws");
    }

    #[test]
    fn source_url_defaults_port_and_omits_unset_keys() {
        let t = source_table("postgresql://db.example/app").unwrap();
        assert_eq!(i(&t, "port"), 5432);
        assert!(!t.contains_key("user"));
        assert!(!t.contains_key("password"));
        assert!(!t.contains_key("sslmode"));
    }

    #[test]
    fn source_url_unix_socket_via_host_param() {
        let t = source_table("postgres:///app?host=%2Fvar%2Frun%2Fpostgresql").unwrap();
        assert_eq!(s(&t, "host"), "/var/run/postgresql");
        assert_eq!(s(&t, "dbname"), "app");
    }

    #[test]
    fn source_url_unix_socket_keeps_credentials() {
        let t = source_table("postgres://repl@/app?host=%2Fvar%2Frun%2Fpostgresql").unwrap();
        assert_eq!(s(&t, "host"), "/var/run/postgresql");
        assert_eq!(s(&t, "user"), "repl");
        assert_eq!(s(&t, "dbname"), "app");
    }

    #[test]
    fn source_url_query_endpoint_overrides_authority() {
        let t = source_table("postgres://old:5432/app?HOST=new&PORT=5433").unwrap();
        assert_eq!(s(&t, "host"), "new");
        assert_eq!(i(&t, "port"), 5433);
    }

    #[test]
    fn source_url_password_holds_reserved_chars() {
        let t = source_table("postgres://u:p%40ss%3Aword@h/d").unwrap();
        assert_eq!(s(&t, "password"), "p@ss:word");
        assert_eq!(s(&t, "user"), "u");
        assert_eq!(s(&t, "host"), "h");
    }

    #[test]
    fn source_url_ipv6_literal() {
        let t = source_table("postgres://u@[::1]:5433/d").unwrap();
        assert_eq!(s(&t, "host"), "::1");
        assert_eq!(i(&t, "port"), 5433);
    }

    #[test]
    fn source_url_rejects_unknown_param() {
        let e = source_table("postgres://h/d?sslcert=x").unwrap_err();
        assert!(matches!(e, DsnError::UnknownParam { .. }), "{e}");
    }

    #[test]
    fn source_url_uses_postgres_validation() {
        let e = source_table("postgres://h/d?sslmode=maybe").unwrap_err();
        assert!(matches!(e, DsnError::Invalid { .. }), "{e}");
    }

    #[test]
    fn source_url_rejects_other_scheme() {
        assert!(matches!(
            source_table("mysql://h/d").unwrap_err(),
            DsnError::Scheme { .. }
        ));
    }

    #[test]
    fn ch_url_plain_defaults_native_port() {
        let t = ch_table("clickhouse://default@ch.example/cdc").unwrap();
        assert_eq!(i(&t, "port"), 9000);
        assert_eq!(t.get("secure").and_then(Value::as_bool), Some(false));
        assert_eq!(s(&t, "database"), "cdc");
    }

    #[test]
    fn ch_url_tls_scheme_defaults_secure_port() {
        let t = ch_table("clickhouses://u:p@ch.cloud/db?compression=zstd").unwrap();
        assert_eq!(i(&t, "port"), 9440);
        assert_eq!(t.get("secure").and_then(Value::as_bool), Some(true));
        assert_eq!(s(&t, "compression"), "zstd");
    }

    #[test]
    fn ch_url_secure_param_overrides_scheme() {
        let t = ch_table("clickhouse://ch:9440/db?secure=true").unwrap();
        assert_eq!(t.get("secure").and_then(Value::as_bool), Some(true));
        assert_eq!(i(&t, "port"), 9440);
    }

    #[test]
    fn ch_url_rejects_non_bool_secure() {
        assert!(matches!(
            ch_table("clickhouse://ch/db?secure=maybe").unwrap_err(),
            DsnError::NotBool { .. }
        ));
    }

    #[test]
    fn bad_port_is_rejected() {
        assert!(matches!(
            source_table("postgres://h:0/d").unwrap_err(),
            DsnError::BadPort(_)
        ));
        assert!(matches!(
            source_table("postgres://h:99999/d").unwrap_err(),
            DsnError::BadPort(_)
        ));
    }

    #[test]
    fn non_utf8_component_is_rejected() {
        assert!(matches!(
            source_table("postgres://h/d?slot=%FF").unwrap_err(),
            DsnError::BadEncoding(_)
        ));
    }
}
