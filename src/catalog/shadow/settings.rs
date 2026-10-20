//! `[shadow]`: `postgresql.conf` settings for the managed shadow

use std::collections::BTreeMap;

/// Shadow writes every streamed WAL byte, and zero-filling each new segment
/// first doubles that. A WAL budget well above `shared_buffers` lets
/// restartpoints recycle segments and spares a flush of every dirty buffer
/// each few hundred MB of WAL
const DEFAULTS: [(&str, &str); 4] = [
    ("max_wal_size", "8GB"),
    ("min_wal_size", "128MB"),
    ("wal_init_zero", "off"),
    ("wal_recycle", "on"),
];

/// Settings walshadow writes from its own config, the bridge and the
/// source's GUC floor, plus directives that read other files
const OWNED: [&str; 24] = [
    "autovacuum",
    "config_file",
    "data_directory",
    "dynamic_library_path",
    "fsync",
    "full_page_writes",
    "hba_file",
    "hot_standby",
    "ident_file",
    "include",
    "include_dir",
    "include_if_exists",
    "listen_addresses",
    "max_connections",
    "max_locks_per_transaction",
    "max_prepared_transactions",
    "max_wal_senders",
    "max_worker_processes",
    "port",
    "restore_command",
    "shared_preload_libraries",
    "unix_socket_directories",
    "wal_level",
    "wal_retrieve_retry_interval",
];
const OWNED_PREFIXES: [&str; 3] = ["primary_", "recovery_", "walshadow."];

const SIZE_UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];
const TIME_UNITS: [&str; 6] = ["us", "ms", "s", "min", "h", "d"];

/// PostgreSQL reads a bare number in the setting's base unit, 8kB pages for
/// `shared_buffers`, so these take a unit
fn units(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "shared_buffers" | "max_wal_size" | "min_wal_size" => Some(&SIZE_UNITS),
        "checkpoint_timeout" => Some(&TIME_UNITS),
        _ => None,
    }
}

/// A number, optional spaces, then one of `units`
fn has_unit(value: &str, units: &[&str]) -> bool {
    let value = value.trim();
    let unit = value.trim_start_matches(|c: char| c.is_ascii_digit() || c == '.');
    unit.len() < value.len() && units.contains(&unit.trim_start())
}

/// Lowercase, as PostgreSQL folds names, and free of conf syntax
fn is_setting_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.')
}

fn owned(name: &str) -> bool {
    OWNED.contains(&name) || OWNED_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// Shadow settings by name, walshadow's defaults with `[shadow]` entries over
/// them. PostgreSQL validates the values when the shadow starts
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowSettings(BTreeMap<String, String>);

/// An eighth of memory, from 256MB to 8GB, beside the half `[memory]
/// resident_payload_max` takes by default. A shadow holding only catalogs
/// touches few of them
fn default_shared_buffers() -> String {
    let mb = crate::budget::host_memory_limit().map_or(0, |bytes| bytes >> 23);
    format!("{}MB", mb.clamp(256, 8 << 10))
}

impl Default for ShadowSettings {
    fn default() -> Self {
        let defaults = DEFAULTS.iter().map(|(k, v)| (k.to_string(), v.to_string()));
        let shared_buffers = ("shared_buffers".to_string(), default_shared_buffers());
        Self(defaults.chain([shared_buffers]).collect())
    }
}

impl ShadowSettings {
    pub fn parse(table: &toml::Table) -> Result<Self, String> {
        let mut out = Self::default();
        for (name, value) in table {
            let err = |msg: String| format!("`shadow.{name}`: {msg}");
            if !is_setting_name(name) {
                return Err(err("not a PostgreSQL setting name".into()));
            }
            if owned(name) {
                return Err(err("walshadow sets this itself".into()));
            }
            let value = match (units(name), value) {
                (Some(units), toml::Value::String(s)) if has_unit(s, units) => s.clone(),
                (Some(units), v) => {
                    return Err(err(format!("{v} needs a unit, one of {units:?}")));
                }
                (None, toml::Value::String(s)) => s.clone(),
                (None, toml::Value::Boolean(b)) => if *b { "on" } else { "off" }.into(),
                (None, v @ (toml::Value::Integer(_) | toml::Value::Float(_))) => v.to_string(),
                (None, v) => {
                    return Err(err(format!(
                        "expected a string, number or boolean, got {}",
                        v.type_str()
                    )));
                }
            };
            out.0.insert(name.clone(), value);
        }
        Ok(out)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// One quoted line per setting; PostgreSQL accepts every type quoted
    pub(super) fn conf_text(&self) -> String {
        self.iter()
            .map(|(k, v)| format!("{k} = '{}'\n", v.replace('\\', "\\\\").replace('\'', "''")))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> Result<ShadowSettings, String> {
        ShadowSettings::parse(&toml.parse().unwrap())
    }

    /// A size or duration parses exactly when it names one of the units
    /// PostgreSQL accepts for it, case included
    #[test]
    fn sizes_and_durations_need_their_own_units() {
        let candidates = SIZE_UNITS
            .iter()
            .chain(&TIME_UNITS)
            .copied()
            .chain(["", "gb", "Mb", "GiB", "mins", "sec", "k"]);
        for name in [
            "shared_buffers",
            "max_wal_size",
            "min_wal_size",
            "checkpoint_timeout",
        ] {
            let units = units(name).unwrap();
            for unit in candidates.clone() {
                for value in [format!("8{unit}"), format!("1.5 {unit}")] {
                    let got = parse(&format!("{name} = {value:?}"));
                    assert_eq!(
                        got.is_ok(),
                        units.contains(&unit),
                        "{name} = {value:?}: {got:?}"
                    );
                    if let Ok(s) = got {
                        assert!(s.iter().any(|kv| kv == (name, value.as_str())));
                    }
                }
            }
            assert!(parse(&format!("{name} = 8")).is_err(), "{name} bare number");
            assert!(
                parse(&format!("{name} = {:?}", units[0])).is_err(),
                "{name} no number"
            );
        }
    }

    /// Names outside the conf grammar could inject lines or alias a refused
    /// setting through case folding
    #[test]
    fn refuses_names_postgres_would_not_read_verbatim() {
        for bad in [
            "\"Shared_Buffers\"",
            "\"a b\"",
            "\"x\\ny\"",
            "\"1x\"",
            "\"a=b\"",
            "\"\"",
        ] {
            assert!(parse(&format!("{bad} = 1")).is_err(), "{bad}");
        }
        assert!(parse("\"pg_stat_statements.max\" = 1").is_ok());
    }
}
