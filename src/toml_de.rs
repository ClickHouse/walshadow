use std::str::FromStr;

use serde::Deserialize;
use serde::de::{Deserializer, Error};

use crate::ch::EmitterError;

pub(crate) fn de_from_str<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let s = String::deserialize(d)?;
    s.parse().map(Some).map_err(Error::custom)
}

pub(crate) fn de_nonempty<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let s = String::deserialize(d)?;
    Ok((!s.is_empty()).then_some(s))
}

pub(crate) fn de_port<'de, D: Deserializer<'de>>(d: D) -> Result<u16, D::Error> {
    match toml::Value::deserialize(d)? {
        toml::Value::Integer(i) => {
            u16::try_from(i).map_err(|_| Error::custom(format!("port {i} out of range")))
        }
        toml::Value::String(s) => s
            .parse()
            .map_err(|_| Error::custom(format!("port {s:?} is not a number"))),
        v => Err(Error::custom(format!(
            "expected a port number, got {}",
            v.type_str()
        ))),
    }
}

pub(crate) fn message(e: toml::de::Error) -> String {
    e.to_string().trim().replace('\n', " ")
}

pub(crate) fn config_error(e: toml::de::Error) -> EmitterError {
    EmitterError::Config(message(e))
}
