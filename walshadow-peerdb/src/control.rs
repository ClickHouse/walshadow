//! Client side of walshadow-control's unix-socket line protocol:
//! one `<noun> <verb> [key=value …] [positional …]` request per
//! connection, `OK\n[payload…]` or `ERR <msg>\n` back

use std::collections::HashMap;
use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::error::GrpcError;

#[derive(Debug)]
pub enum ControlError {
    /// socket connect / io failure — daemon down
    Unavailable(String),
    /// daemon predates the verb (control-protocol extension not landed)
    UnknownCommand(String),
    /// `ERR <msg>` from the daemon
    Daemon(String),
}

impl From<ControlError> for GrpcError {
    fn from(e: ControlError) -> Self {
        match e {
            ControlError::Unavailable(m) => GrpcError::unavailable(m),
            ControlError::UnknownCommand(m) => {
                GrpcError::unimplemented(format!("control daemon lacks verb: {m}"))
            }
            ControlError::Daemon(m) => GrpcError::internal(m),
        }
    }
}

/// Line protocol v1 forbids whitespace inside values; reject rather than
/// emit a corrupt (or injected) request line. Lifts once control grows
/// value quoting / JSON framing
pub fn kv(key: &str, value: &str) -> Result<String, GrpcError> {
    if value.chars().any(char::is_whitespace) {
        return Err(GrpcError::invalid(format!(
            "value for {key} contains whitespace, unsupported by control line protocol v1"
        )));
    }
    Ok(format!("{key}={value}"))
}

pub fn positional(value: &str) -> Result<String, GrpcError> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(GrpcError::invalid(format!(
            "identifier {value:?} is empty or contains whitespace, unsupported by control line protocol v1"
        )));
    }
    Ok(value.to_string())
}

#[derive(Clone, Debug)]
pub struct ControlClient {
    socket: PathBuf,
}

impl ControlClient {
    pub fn new(socket: PathBuf) -> Self {
        Self { socket }
    }

    /// Send one request line, return the OK payload (without the OK line)
    pub async fn call(&self, parts: &[String]) -> Result<String, ControlError> {
        let line = parts.join(" ");
        let mut stream = UnixStream::connect(&self.socket).await.map_err(|e| {
            ControlError::Unavailable(format!("control socket {}: {e}", self.socket.display()))
        })?;
        let io = |e: std::io::Error| ControlError::Unavailable(format!("control io: {e}"));
        stream.write_all(line.as_bytes()).await.map_err(io)?;
        stream.write_all(b"\n").await.map_err(io)?;
        stream.flush().await.map_err(io)?;

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.map_err(io)?;
        let (first, rest) = resp.split_once('\n').unwrap_or((resp.as_str(), ""));
        if first.trim_end() == "OK" {
            Ok(rest.to_string())
        } else if let Some(msg) = first.strip_prefix("ERR ") {
            if msg.starts_with("unknown command") {
                Err(ControlError::UnknownCommand(msg.to_string()))
            } else {
                Err(ControlError::Daemon(msg.to_string()))
            }
        } else {
            Err(ControlError::Daemon(format!(
                "malformed control response: {first:?}"
            )))
        }
    }
}

/// Parse a `key=value` payload (`source get`, `stream status`)
pub fn parse_kv_body(body: &str) -> HashMap<String, String> {
    body.lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRow {
    pub namespace: String,
    pub relname: String,
    pub selected: bool,
    pub replica_identity_full: bool,
}

/// Parse `tables list` payload: `<ns.rel>\t<yes|no>\t<full|default>` per line
pub fn parse_tables_body(body: &str) -> Vec<TableRow> {
    body.lines()
        .filter_map(|l| {
            let mut cols = l.split('\t');
            let full = cols.next()?;
            let (namespace, relname) = full.split_once('.')?;
            let selected = cols.next() == Some("yes");
            let rif = cols.next() == Some("full");
            Some(TableRow {
                namespace: namespace.to_string(),
                relname: relname.to_string(),
                selected,
                replica_identity_full: rif,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_rejects_whitespace() {
        assert!(kv("password", "p w").is_err());
        assert!(kv("password", "p\nw").is_err());
        assert_eq!(kv("host", "db").unwrap(), "host=db");
        assert_eq!(kv("password", "").unwrap(), "password=");
    }

    #[test]
    fn positional_rejects_empty_and_whitespace() {
        assert!(positional("").is_err());
        assert!(positional("public users").is_err());
        assert_eq!(positional("public.users").unwrap(), "public.users");
    }

    #[test]
    fn parses_bodies() {
        let kvs = parse_kv_body("state=running\npid=42\nlag_bytes=1024\n");
        assert_eq!(kvs.get("state").unwrap(), "running");
        assert_eq!(kvs.get("pid").unwrap(), "42");

        let rows = parse_tables_body("public.users\tyes\tfull\npublic.orders\tno\tdefault\n");
        assert_eq!(rows.len(), 2);
        assert!(rows[0].selected && rows[0].replica_identity_full);
        assert_eq!(rows[1].namespace, "public");
        assert_eq!(rows[1].relname, "orders");
        assert!(!rows[1].selected && !rows[1].replica_identity_full);
    }
}
