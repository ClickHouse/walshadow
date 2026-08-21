//! `walshadow-stream ctl` word list → control request, and back
//!
//! The socket speaks TOML fragments ([`crate::control`]). This is the
//! translation layer above the verbs: `ctl add public users` becomes an
//! `apply` of `[table.public.users] replicate = true`, and the
//! `[[tables]]` reply comes back as aligned columns

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use toml::{Table, Value};

#[derive(Debug)]
pub struct Command {
    pub verb: String,
    pub body: Table,
    /// Raw verbs take their body from stdin; sugar builds its own
    pub reads_stdin: bool,
}

#[derive(Debug, Parser)]
#[command(
    name = "walshadow-stream ctl",
    about = "Control a running walshadow-stream daemon"
)]
pub struct Cli {
    #[arg(
        long,
        env = "WALSHADOW_CONTROL_SOCKET",
        default_value = "/run/walshadow/control.sock"
    )]
    socket: PathBuf,
    #[command(subcommand)]
    command: CtlCommand,
}

impl Cli {
    pub fn into_parts(self) -> Result<(PathBuf, Command)> {
        Ok((self.socket, self.command.into_command()?))
    }
}

#[derive(Debug, Subcommand)]
enum CtlCommand {
    /// Show stream position, lag, and pause state
    Status,
    /// Show effective config with passwords masked
    Show,
    /// List source tables, optionally within one schema
    Tables { schema: Option<String> },
    /// List source schemas
    Schemas,
    /// List source columns
    Columns { schema: String, table: String },
    /// Start replicating one table
    Add {
        schema: String,
        table: String,
        #[arg(long, value_enum)]
        initial_load: Option<InitialLoad>,
    },
    /// Stop replicating one table, retain ClickHouse table
    Remove { schema: String, table: String },
    /// Freeze WAL consumption
    Pause,
    /// Resume WAL consumption
    Resume,
    /// Repoint source endpoint
    Source { url: String },
    /// Repoint destination endpoint
    #[command(alias = "destination")]
    Dest { url: String },
    /// Re-read config, same as SIGHUP
    Reload,
    /// Apply TOML fragment from stdin
    Apply,
    /// Unset keys named by TOML fragment from stdin
    Unset,
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
enum InitialLoad {
    None,
    Copy,
    BaseBackup,
    ObjectStore,
}

impl InitialLoad {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Copy => "copy",
            Self::BaseBackup => "base_backup",
            Self::ObjectStore => "object_store",
        }
    }
}

impl CtlCommand {
    fn into_command(self) -> Result<Command> {
        match self {
            Self::Status => Ok(sugar("status", Table::new())),
            Self::Show => Ok(sugar("show", Table::new())),
            Self::Reload => Ok(sugar("reload", Table::new())),
            Self::Schemas => Ok(sugar("schemas", Table::new())),
            Self::Pause => Ok(sugar(
                "apply",
                section("stream", pair("paused", true.into())),
            )),
            Self::Resume => Ok(sugar(
                "apply",
                section("stream", pair("paused", false.into())),
            )),
            Self::Tables { schema } => {
                let mut body = Table::new();
                if let Some(schema) = schema {
                    body.insert("namespace".into(), schema.into());
                }
                Ok(sugar("tables", body))
            }
            Self::Columns { schema, table } => {
                let mut body = pair("namespace", schema.into());
                body.insert("relname".into(), table.into());
                Ok(sugar("columns", body))
            }
            Self::Add {
                schema,
                table,
                initial_load,
            } => {
                let mut block = pair("replicate", true.into());
                if let Some(mode) = initial_load {
                    block.insert("initial_load".into(), mode.as_str().into());
                }
                Ok(sugar("apply", table_block(&schema, &table, block)))
            }
            Self::Remove { schema, table } => {
                let block = pair("replicate", false.into());
                Ok(sugar("apply", table_block(&schema, &table, block)))
            }
            Self::Source { url } => Ok(sugar(
                "apply",
                section("source", crate::dsn::source_table(&url)?),
            )),
            Self::Dest { url } => Ok(sugar("apply", section("ch", crate::dsn::ch_table(&url)?))),
            Self::Apply => Ok(raw("apply")),
            Self::Unset => Ok(raw("unset")),
            Self::External(words) => {
                let verb = words.first().expect("external subcommand has a name");
                Ok(raw(verb))
            }
        }
    }
}

/// Words after `ctl`. Unknown verbs pass through with a stdin body so a
/// newer daemon's verbs work against an older CLI
pub fn parse<S: AsRef<str>>(words: &[S]) -> Result<Command> {
    let args = std::iter::once("walshadow-stream ctl".to_owned())
        .chain(words.iter().map(|word| word.as_ref().to_owned()));
    Cli::try_parse_from(args)?.command.into_command()
}

/// Human view of a reply payload. Unknown shapes pass through verbatim
pub fn render(verb: &str, payload: &str) -> String {
    let parsed: Option<Table> = payload.parse().ok();
    let Some(root) = parsed else {
        return payload.into();
    };
    match verb {
        "tables" => render_tables(&root).unwrap_or_else(|| payload.into()),
        "schemas" => root
            .get("schemas")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_else(|| payload.into()),
        "columns" => render_columns(&root).unwrap_or_else(|| payload.into()),
        _ => payload.into(),
    }
}

fn render_tables(root: &Table) -> Option<String> {
    let rows = root.get("tables")?.as_array()?;
    let cells: Vec<(bool, String, String, String)> = rows
        .iter()
        .filter_map(Value::as_table)
        .map(|t| {
            (
                t.get("selected").and_then(Value::as_bool).unwrap_or(false),
                str_of(t, "namespace"),
                str_of(t, "name"),
                match t.get("has_row_key").and_then(Value::as_bool) {
                    Some(false) => "no row key".into(),
                    _ => format!("identity {}", str_of(t, "replica_identity")),
                },
            )
        })
        .collect();
    let ns_width = cells.iter().map(|c| c.1.len()).max().unwrap_or(0);
    let name_width = cells.iter().map(|c| c.2.len()).max().unwrap_or(0);
    Some(
        cells
            .iter()
            .map(|(selected, ns, name, note)| {
                let mark = if *selected { '*' } else { ' ' };
                format!("{mark} {ns:<ns_width$}  {name:<name_width$}  {note}")
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn render_columns(root: &Table) -> Option<String> {
    let rows = root.get("columns")?.as_array()?;
    let cells: Vec<(String, String, bool)> = rows
        .iter()
        .filter_map(Value::as_table)
        .map(|t| {
            (
                str_of(t, "name"),
                str_of(t, "type"),
                t.get("notnull").and_then(Value::as_bool).unwrap_or(false),
            )
        })
        .collect();
    let width = cells.iter().map(|c| c.0.len()).max().unwrap_or(0);
    Some(
        cells
            .iter()
            .map(|(name, ty, notnull)| {
                let null = if *notnull { " not null" } else { "" };
                format!("  {name:<width$}  {ty}{null}")
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn str_of(t: &Table, key: &str) -> String {
    t.get(key).and_then(Value::as_str).unwrap_or("").into()
}

fn sugar(verb: &str, body: Table) -> Command {
    Command {
        verb: verb.into(),
        body,
        reads_stdin: false,
    }
}

fn raw(verb: &str) -> Command {
    Command {
        verb: verb.into(),
        body: Table::new(),
        reads_stdin: true,
    }
}

fn pair(key: &str, value: Value) -> Table {
    let mut t = Table::new();
    t.insert(key.into(), value);
    t
}

fn section(name: &str, body: Table) -> Table {
    let mut root = Table::new();
    root.insert(name.into(), Value::Table(body));
    root
}

/// `[table.<ns>.<rel>]`, the two name parts kept separate all the way down
fn table_block(ns: &str, rel: &str, block: Table) -> Table {
    section(
        "table",
        section(ns, {
            let mut t = Table::new();
            t.insert(rel.into(), Value::Table(block));
            t
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn cmd(words: &[&str]) -> Command {
        parse(words).unwrap()
    }
    fn err(words: &[&str]) -> String {
        parse(words).unwrap_err().to_string()
    }

    #[test]
    fn add_builds_an_opt_in_fragment() {
        let c = cmd(&["add", "public", "users", "--initial-load", "copy"]);
        assert_eq!(c.verb, "apply");
        assert!(!c.reads_stdin);
        assert_eq!(
            toml::to_string(&c.body).unwrap(),
            "[table.public.users]\ninitial_load = \"copy\"\nreplicate = true\n"
        );
    }

    #[test]
    fn add_defaults_to_no_initial_load_key() {
        let c = cmd(&["add", "public", "users"]);
        assert_eq!(
            toml::to_string(&c.body).unwrap(),
            "[table.public.users]\nreplicate = true\n"
        );
    }

    #[test]
    fn remove_opts_out_without_touching_other_tables() {
        let c = cmd(&["remove", "public", "users"]);
        assert_eq!(
            toml::to_string(&c.body).unwrap(),
            "[table.public.users]\nreplicate = false\n"
        );
    }

    #[test]
    fn dotted_name_is_not_split_into_a_pair() {
        let error = err(&["add", "public.users"]);
        assert!(error.contains("<TABLE>"), "{error}");
    }

    #[test]
    fn pause_and_resume_flip_one_flag() {
        assert_eq!(
            toml::to_string(&cmd(&["pause"]).body).unwrap(),
            "[stream]\npaused = true\n"
        );
        assert_eq!(
            toml::to_string(&cmd(&["resume"]).body).unwrap(),
            "[stream]\npaused = false\n"
        );
    }

    #[test]
    fn source_and_dest_take_urls() {
        let c = cmd(&["source", "postgres://repl@db:5433/app"]);
        let source = c.body["source"].as_table().unwrap();
        assert_eq!(source["host"].as_str(), Some("db"));
        assert_eq!(source["port"].as_integer(), Some(5433));
        let c = cmd(&["dest", "clickhouses://ch.cloud/cdc"]);
        let ch = c.body["ch"].as_table().unwrap();
        assert_eq!(ch["secure"].as_bool(), Some(true));
        assert_eq!(ch["port"].as_integer(), Some(9440));
    }

    #[test]
    fn raw_verbs_still_read_stdin() {
        assert!(cmd(&["apply"]).reads_stdin);
        assert!(cmd(&["unset"]).reads_stdin);
        assert!(cmd(&["some-future-verb", "--new-option", "value"]).reads_stdin);
        assert!(!cmd(&["status"]).reads_stdin);
    }

    #[test]
    fn cli_parses_socket_with_typed_command() {
        let cli = Cli::try_parse_from(["ctl", "--socket", "/tmp/custom.sock", "status"]).unwrap();
        let (socket, command) = cli.into_parts().unwrap();
        assert_eq!(socket, PathBuf::from("/tmp/custom.sock"));
        assert_eq!(command.verb, "status");
    }

    #[test]
    fn bad_initial_load_mode_is_rejected() {
        let error = err(&["add", "public", "users", "--initial-load", "warp"]);
        assert!(error.contains("invalid value 'warp'"), "{error}");
        assert!(error.contains("base_backup"), "{error}");
    }

    #[test]
    fn initial_load_accepts_equals_syntax() {
        let c = cmd(&["add", "public", "users", "--initial-load=object_store"]);
        assert_eq!(
            toml::to_string(&c.body).unwrap(),
            "[table.public.users]\ninitial_load = \"object_store\"\nreplicate = true\n"
        );
    }

    #[test]
    fn known_verbs_reject_extra_arguments() {
        let error = err(&["status", "extra"]);
        assert!(error.contains("unexpected argument 'extra'"), "{error}");
    }

    #[test]
    fn generated_help_lists_control_verbs() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("Commands:"), "{help}");
        assert!(help.contains("add"), "{help}");
        assert!(help.contains("Start replicating one table"), "{help}");
    }

    #[test]
    fn tables_render_marks_selection_and_missing_keys() {
        let payload = "[[tables]]\nselected = true\nnamespace = \"public\"\nname = \"users\"\n\
             replica_identity = \"d\"\nhas_row_key = true\n\
             [[tables]]\nselected = false\nnamespace = \"public\"\nname = \"audit\"\n\
             replica_identity = \"d\"\nhas_row_key = false\n";
        assert_eq!(
            render("tables", payload),
            "* public  users  identity d\n  public  audit  no row key"
        );
    }

    #[test]
    fn unknown_payload_passes_through() {
        assert_eq!(render("status", "paused = false\n"), "paused = false\n");
        assert_eq!(render("tables", "not toml ["), "not toml [");
    }
}
