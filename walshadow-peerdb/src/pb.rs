//! proto3-JSON conventions per grpc-gateway: 64-bit ints as strings,
//! enums as strings (ints accepted on input), Timestamp as RFC 3339,
//! absent field = default value

use serde::{Deserialize, Deserializer, Serializer};

/// Serialize i64 as a JSON string; accept string or number on input
pub mod i64_str {
    use super::*;

    pub fn serialize<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Num(i64),
            Str(String),
        }
        match Raw::deserialize(d)? {
            Raw::Num(n) => Ok(n),
            Raw::Str(s) => s.parse().map_err(serde::de::Error::custom),
        }
    }
}

/// Accept a proto3 enum encoded as its name or its number
pub fn enum_name_or_number<'de, D: Deserializer<'de>>(d: D) -> Result<Option<EnumToken>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Num(i64),
        Str(String),
    }
    Ok(match Option::<Raw>::deserialize(d)? {
        None => None,
        Some(Raw::Num(n)) => Some(EnumToken::Number(n)),
        Some(Raw::Str(s)) => Some(EnumToken::Name(s)),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnumToken {
    Name(String),
    Number(i64),
}

pub fn timestamp_rfc3339(unix_secs: i64) -> String {
    use chrono::{Datelike, Timelike};
    chrono::DateTime::from_timestamp(unix_secs, 0)
        .map(|t| {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                t.year(),
                t.month(),
                t.day(),
                t.hour(),
                t.minute(),
                t.second()
            )
        })
        .unwrap_or_default()
}

pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize, Deserialize)]
    struct WithI64 {
        #[serde(with = "i64_str")]
        v: i64,
    }

    #[test]
    fn i64_roundtrip() {
        let j = serde_json::to_string(&WithI64 { v: 1 << 60 }).unwrap();
        assert_eq!(j, format!("{{\"v\":\"{}\"}}", 1i64 << 60));
        let from_str: WithI64 = serde_json::from_str(&j).unwrap();
        assert_eq!(from_str.v, 1 << 60);
        let from_num: WithI64 = serde_json::from_str("{\"v\":42}").unwrap();
        assert_eq!(from_num.v, 42);
    }

    #[test]
    fn timestamp_format() {
        assert_eq!(timestamp_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(timestamp_rfc3339(1784131200), "2026-07-15T16:00:00Z");
    }
}
