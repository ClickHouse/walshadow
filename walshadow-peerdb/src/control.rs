//! Client side of walshadow-control's TOML socket protocol: one
//! `<verb>\n<toml body>` request per connection, EOF-framed;
//! `OK\n[toml body]` or `ERR <msg>\n` back. TOML bodies preserve config
//! types and carry values (passwords) that would break a line protocol

use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use toml::{Table, Value};

use crate::error::GrpcError;

#[derive(Debug)]
pub enum ControlError {
    /// socket connect / io failure — daemon down
    Unavailable(String),
    /// daemon lacks the verb
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

#[derive(Clone, Debug)]
pub struct ControlClient {
    socket: PathBuf,
}

impl ControlClient {
    pub fn new(socket: PathBuf) -> Self {
        Self { socket }
    }

    /// Send `<verb>\n<toml body>`, return the parsed OK body as a table
    /// (empty when the daemon answered a bare `OK`)
    pub async fn call(&self, verb: &str, config: &Table) -> Result<Table, ControlError> {
        let body = toml::to_string(config)
            .map_err(|e| ControlError::Daemon(format!("serialize request config: {e}")))?;
        let req = format!("{verb}\n{body}");
        let mut stream = UnixStream::connect(&self.socket).await.map_err(|e| {
            ControlError::Unavailable(format!("control socket {}: {e}", self.socket.display()))
        })?;
        let io = |e: std::io::Error| ControlError::Unavailable(format!("control io: {e}"));
        stream.write_all(req.as_bytes()).await.map_err(io)?;
        stream.flush().await.map_err(io)?;
        // half-close so the daemon's read_to_end sees EOF
        stream.shutdown().await.map_err(io)?;

        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.map_err(io)?;
        let (first, rest) = resp.split_once('\n').unwrap_or((resp.as_str(), ""));
        if first.trim_end() == "OK" {
            rest.parse::<Table>()
                .map_err(|e| ControlError::Daemon(format!("parse OK body toml: {e}")))
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRow {
    pub namespace: String,
    pub relname: String,
    pub selected: bool,
    pub replica_identity_full: bool,
}

/// Parse a `tables` reply: `[[tables]]` blocks with `namespace`, `name`,
/// `selected`, `replica_identity` (a `relreplident` char, `f` == full)
pub fn parse_tables(body: &Table) -> Vec<TableRow> {
    body.get("tables")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let t = v.as_table()?;
                    Some(TableRow {
                        namespace: t.get("namespace").and_then(Value::as_str)?.to_string(),
                        relname: t.get("name").and_then(Value::as_str)?.to_string(),
                        selected: t.get("selected").and_then(Value::as_bool).unwrap_or(false),
                        replica_identity_full: t.get("replica_identity").and_then(Value::as_str)
                            == Some("f"),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tables_reply() {
        let body: Table = "[[tables]]\nnamespace = \"public\"\nname = \"users\"\nselected = true\nreplica_identity = \"f\"\n\
             [[tables]]\nnamespace = \"public\"\nname = \"orders\"\nselected = false\nreplica_identity = \"d\"\n"
            .parse()
            .unwrap();
        let rows = parse_tables(&body);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].selected && rows[0].replica_identity_full);
        assert_eq!(rows[1].namespace, "public");
        assert_eq!(rows[1].relname, "orders");
        assert!(!rows[1].selected && !rows[1].replica_identity_full);
        // absent `tables` key degrades to empty, not an error
        assert!(parse_tables(&Table::new()).is_empty());
    }
}
