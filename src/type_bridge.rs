//! PHASE15 §3 — PG → CH type bridge.
//!
//! Given a [`RelAttr`] resolved from shadow PG's `pg_attribute`, produce
//! the matching CH type string plus an optional `DEFAULT <expr>` clause
//! reconstructed from PG's `attmissingval[1]` (see PHASE14 §1).
//!
//! `EmitterConfig.tables` (TOML mapping) is still the operator-pinned
//! override path; PHASE15 §5 wires the bridge as the fallback for
//! relations matched by namespace pattern only. PHASE15 §4 + §6 consume
//! the bridge through `ch_ddl::DdlApplicator` when reshaping CH tables
//! to track source DDL.
//!
//! ## Type matrix
//!
//! | PG type | CH type | Note |
//! |---|---|---|
//! | `bool` | `Bool` | |
//! | `char` | `Int8` | PG's `"char"` 1-byte type |
//! | `int2/4/8` | `Int16/32/64` | |
//! | `oid` | `UInt32` | |
//! | `float4/8` | `Float32/64` | |
//! | `numeric(p,s)` | `Decimal(p,s)` (p ≤ 76 — CH cap), else `String` | |
//! | `varchar(n)`, `bpchar(n)`, `text`, `name` | `String` | CH has no length cap |
//! | `bytea` | `String` | CH binary lands in String columns |
//! | `date` | `Date32` | covers PG's -infinity / +infinity edges |
//! | `time` / `timetz` | `String` | rendered via `ColumnValue` text form |
//! | `timestamp` / `timestamptz` | `DateTime64(p, 'UTC')` p ≤ 6 | |
//! | `interval` | `String` | |
//! | `uuid` | `UUID` | |
//! | `inet` / `cidr` | `String` | |
//! | `json` / `jsonb` | `String` | CH `JSON` opt-in via namespace config |
//! | array / unknown | `String` | falls through to PGPending bytes |
//!
//! Nullability: `not_null = false` wraps the inner type in `Nullable(_)`
//! unless the column is part of CH's `ORDER BY` (primary-key columns
//! must stay non-nullable; caller enforces).
//!
//! ## Default expression
//!
//! `attmissingval` arrives as PG's `typoutput` text form (see
//! `parse_array_one_element` in `shadow_catalog.rs`). `default_expr`
//! routes it through `heap_decoder::missing_value_for(att) →
//! ColumnValue`, then renders via [`column_value_to_sql_literal`].
//!
//! Booleans → `true` / `false`; ints → numeric literal; strings →
//! single-quoted with `'` escaping; timestamps → `toDateTime64('…', 6,
//! 'UTC')`. Falls through to `String`-typed default for tier-3 / unknown.

use crate::heap_decoder::{
    self, BOOLOID, BPCHAROID, BYTEAOID, CHAROID, ColumnValue, DATEOID, FLOAT4OID, FLOAT8OID,
    INETOID, INT2OID, INT4OID, INT8OID, INTERVALOID, JSONBOID, JSONOID, NAMEOID, NUMERICOID,
    OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UUIDOID, VARCHAROID,
};
use crate::shadow_catalog::RelAttr;

/// PG `cidr` OID — wal_rs constants list it but the heap_decoder
/// module already exports `INETOID`. cidr shares decode with inet.
pub const CIDR_OID: u32 = heap_decoder::CIDROID;

/// PG `VARHDRSZ` — 4-byte varlena header used by `typmod` packing.
const VARHDRSZ: i32 = 4;
/// CH `Decimal` maximum precision (driven by Decimal256 wire shape).
const CH_DECIMAL_MAX_PRECISION: i32 = 76;
/// CH `DateTime64` maximum fractional precision.
const CH_DATETIME64_MAX_PRECISION: i32 = 6;

/// Outcome of resolving a [`RelAttr`] to its CH-side type + optional
/// default. `default_sql` is the SQL fragment after `DEFAULT ` (no
/// leading `DEFAULT` keyword); callers wrap as needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedColumn {
    /// CH type expression as it would appear in a `CREATE TABLE` or
    /// `ALTER TABLE ADD COLUMN` clause. Includes `Nullable(...)` wrap
    /// when applicable.
    pub ch_type: String,
    /// `Some(literal)` when PG's `attmissingval` carried a fast-path
    /// default the bridge could render in CH SQL. `None` when no
    /// missing-default exists or when rendering wasn't possible.
    pub default_sql: Option<String>,
}

/// Error path for `map`. Surfaces a non-fatal hint to the caller; the
/// applicator typically logs and skips the DDL while leaving already-
/// bridged relations intact.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BridgeError {
    #[error("unsupported PG type oid {type_oid} (name {type_name:?})")]
    UnsupportedType { type_oid: u32, type_name: String },
}

/// Map one column. `pk_member = true` forces non-nullable (CH refuses
/// `Nullable` columns in `ORDER BY`); pass `false` for non-key
/// columns where `Nullable(_)` is acceptable.
pub fn map(att: &RelAttr, pk_member: bool) -> Result<ResolvedColumn, BridgeError> {
    let inner = base_type_for(att)?;
    let ch_type = if pk_member || att.not_null {
        inner.clone()
    } else {
        format!("Nullable({inner})")
    };
    let default_sql = render_default(att, &inner);
    Ok(ResolvedColumn {
        ch_type,
        default_sql,
    })
}

/// Base (non-nullable) CH type for one PG attribute. Returns the inner
/// type without `Nullable(_)` wrapping; [`map`] adds the wrap when
/// appropriate.
pub fn base_type_for(att: &RelAttr) -> Result<String, BridgeError> {
    Ok(match att.type_oid {
        BOOLOID => "Bool".into(),
        CHAROID => "Int8".into(),
        INT2OID => "Int16".into(),
        INT4OID => "Int32".into(),
        INT8OID => "Int64".into(),
        OIDOID => "UInt32".into(),
        FLOAT4OID => "Float32".into(),
        FLOAT8OID => "Float64".into(),
        NUMERICOID => numeric_ch_type(att.typmod),
        TEXTOID | VARCHAROID | BPCHAROID | NAMEOID | BYTEAOID => "String".into(),
        DATEOID => "Date32".into(),
        TIMEOID | TIMETZOID | INTERVALOID => "String".into(),
        TIMESTAMPOID | TIMESTAMPTZOID => datetime64_ch_type(att.typmod),
        UUIDOID => "UUID".into(),
        INETOID | CIDR_OID => "String".into(),
        JSONOID | JSONBOID => "String".into(),
        _ => "String".into(),
    })
}

/// Render the SQL fragment for the post-`DEFAULT ` keyword when the
/// attribute carries a `missing_text` (PG fast-path
/// `ALTER TABLE ... ADD COLUMN ... DEFAULT k`). Returns `None` when no
/// fast-path default exists or when the value can't be expressed as a
/// CH literal cleanly.
fn render_default(att: &RelAttr, ch_inner: &str) -> Option<String> {
    let _ = att.missing_text.as_ref()?;
    let value = heap_decoder::missing_value_for(att);
    if matches!(value, ColumnValue::Null) {
        return None;
    }
    column_value_to_sql_literal(&value, ch_inner)
}

/// Decode `pg_attribute.atttypmod` for `numeric(p, s)` and produce the
/// matching CH type. Falls back to `String` when precision exceeds CH's
/// `Decimal256` cap, or when typmod is unset (`-1` ≡ unconstrained
/// numeric).
fn numeric_ch_type(typmod: i32) -> String {
    if typmod < VARHDRSZ {
        return "String".into();
    }
    let packed = typmod - VARHDRSZ;
    let precision = (packed >> 16) & 0xFFFF;
    let scale_raw = packed & 0xFFFF;
    // PG packs scale as a signed 16-bit value (PG 15+ allows negative
    // scale). CH `Decimal(p,s)` requires `0 ≤ s ≤ p`; fall back to
    // String when the packed scale is outside that range.
    let scale = if scale_raw & 0x8000 != 0 {
        // Sign-extend.
        scale_raw | !0xFFFF
    } else {
        scale_raw
    };
    if !(0..=CH_DECIMAL_MAX_PRECISION).contains(&precision) || scale < 0 || scale > precision {
        return "String".into();
    }
    format!("Decimal({precision}, {scale})")
}

/// `timestamp(p)` / `timestamptz(p)` → `DateTime64(p, 'UTC')`. PG's
/// typmod packs the fractional precision (0..=6); the bridge always
/// pins the timezone to UTC because PG's `timestamptz` storage is
/// epoch microseconds, not zoned.
fn datetime64_ch_type(typmod: i32) -> String {
    let precision = if (0..=CH_DATETIME64_MAX_PRECISION).contains(&typmod) {
        typmod
    } else {
        CH_DATETIME64_MAX_PRECISION
    };
    format!("DateTime64({precision}, 'UTC')")
}

/// Render a single PG-decoded value as a CH SQL literal suitable for
/// pasting after `DEFAULT `. Falls back to `None` for shapes the bridge
/// can't express cleanly (e.g. unsupported types whose `Unsupported.raw`
/// would need typmod-aware reconstruction). `ch_inner` is the
/// non-nullable CH type from [`base_type_for`]; used to drive
/// timestamp formatting + cast shape.
pub fn column_value_to_sql_literal(v: &ColumnValue, ch_inner: &str) -> Option<String> {
    match v {
        ColumnValue::Null => Some("NULL".into()),
        ColumnValue::Bool(b) => Some(if *b { "true".into() } else { "false".into() }),
        ColumnValue::Char(i) => Some(i.to_string()),
        ColumnValue::Int2(i) => Some(i.to_string()),
        ColumnValue::Int4(i) => Some(i.to_string()),
        ColumnValue::Int8(i) => Some(i.to_string()),
        ColumnValue::Oid(i) => Some(i.to_string()),
        ColumnValue::Float4(f) => Some(format_float(*f as f64)),
        ColumnValue::Float8(f) => Some(format_float(*f)),
        ColumnValue::Numeric(n) => {
            use crate::codecs::NumericKind;
            match n {
                NumericKind::Finite(s) => {
                    if ch_inner.starts_with("Decimal(") {
                        Some(s.clone())
                    } else {
                        Some(sql_string_literal(s))
                    }
                }
                NumericKind::NaN => Some("nan".into()),
                NumericKind::PInf => Some("inf".into()),
                NumericKind::NInf => Some("-inf".into()),
            }
        }
        ColumnValue::Text(s) | ColumnValue::Name(s) | ColumnValue::Json(s) => {
            Some(sql_string_literal(s))
        }
        ColumnValue::Bytea(b) => Some(sql_bytes_literal(b)),
        ColumnValue::Date(days) => Some(format!("toDate32({days})")),
        ColumnValue::Time(micros) => {
            let txt = render_pg_time(*micros);
            Some(sql_string_literal(&txt))
        }
        ColumnValue::TimeTz { micros, tz_seconds } => {
            let txt = render_pg_timetz(*micros, *tz_seconds);
            Some(sql_string_literal(&txt))
        }
        ColumnValue::Timestamp(micros) | ColumnValue::TimestampTz(micros) => {
            let txt = render_pg_timestamp(*micros);
            let prec = parse_datetime64_precision(ch_inner).unwrap_or(6);
            Some(format!(
                "toDateTime64({}, {prec}, 'UTC')",
                sql_string_literal(&txt)
            ))
        }
        ColumnValue::Interval(v) => Some(sql_string_literal(&v.to_text())),
        ColumnValue::Inet(v) => Some(sql_string_literal(&v.to_text())),
        ColumnValue::Uuid(b) => Some(format!("toUUID({})", sql_string_literal(&format_uuid(b)))),
        ColumnValue::PgPending { raw, .. } => {
            // typoutput text form is operator-meaningful; render as a
            // String default. Caller chose a non-String CH inner means
            // the literal lands as a CH cast, which is what PG would do
            // for `DEFAULT typeinput('…')` semantically.
            Some(sql_bytes_literal(raw))
        }
        ColumnValue::ExternalToast(_) | ColumnValue::Unsupported { .. } => None,
    }
}

fn format_float(f: f64) -> String {
    if f.is_nan() {
        "nan".into()
    } else if f.is_infinite() {
        if f > 0.0 { "inf".into() } else { "-inf".into() }
    } else {
        // Always include a decimal so CH parses as Float, not Int.
        let s = format!("{f}");
        if s.contains('.') || s.contains('e') || s.contains('E') {
            s
        } else {
            format!("{s}.0")
        }
    }
}

/// Render PG `time` (microseconds since midnight) as the typoutput
/// shape: `HH:MM:SS[.ffffff]`.
fn render_pg_time(micros: i64) -> String {
    let m = micros.rem_euclid(86_400_000_000);
    let total_secs = m / 1_000_000;
    let frac = (m % 1_000_000) as u32;
    let h = total_secs / 3_600;
    let mm = (total_secs % 3_600) / 60;
    let ss = total_secs % 60;
    if frac == 0 {
        format!("{h:02}:{mm:02}:{ss:02}")
    } else {
        format!("{h:02}:{mm:02}:{ss:02}.{frac:06}")
    }
}

fn render_pg_timetz(micros: i64, tz_seconds: i32) -> String {
    let time = render_pg_time(micros);
    if tz_seconds == 0 {
        format!("{time}+00")
    } else {
        // PG sign convention: positive tz_seconds means west of UTC, so
        // the displayed offset is the negation.
        let total = -tz_seconds;
        let sign = if total >= 0 { '+' } else { '-' };
        let total = total.abs();
        let h = total / 3_600;
        let m = (total % 3_600) / 60;
        if m == 0 {
            format!("{time}{sign}{h:02}")
        } else {
            format!("{time}{sign}{h:02}:{m:02}")
        }
    }
}

/// Render PG `timestamp` / `timestamptz` (PG-epoch microseconds) as a
/// CH-parseable ISO string with up to 6 fractional digits.
fn render_pg_timestamp(pg_micros: i64) -> String {
    use chrono::{DateTime, TimeZone, Utc};
    let unix_micros = pg_micros.saturating_add(crate::ch_emitter::DATETIME64_PG_EPOCH_US);
    let secs = unix_micros.div_euclid(1_000_000);
    let nanos = (unix_micros.rem_euclid(1_000_000) * 1_000) as u32;
    let dt: DateTime<Utc> = Utc.timestamp_opt(secs, nanos).single().unwrap_or_else(|| {
        // Fallback: huge value clamped to chrono's representable range.
        DateTime::<Utc>::from_timestamp(0, 0).unwrap()
    });
    if dt.timestamp_subsec_micros() == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}

/// Pull the precision out of `DateTime64(P, 'UTC')`. Returns `None` if
/// `inner` doesn't match.
fn parse_datetime64_precision(inner: &str) -> Option<i32> {
    let body = inner.strip_prefix("DateTime64(")?;
    let comma = body.find(',')?;
    body[..comma].trim().parse::<i32>().ok()
}

/// Render a 16-byte UUID in canonical hyphenated form.
fn format_uuid(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, byte) in b.iter().enumerate() {
        s.push_str(&format!("{byte:02x}"));
        if matches!(i, 3 | 5 | 7 | 9) {
            s.push('-');
        }
    }
    s
}

/// CH SQL string-literal escaping. Backslash + single-quote double,
/// other control chars stay literal — CH's parser accepts UTF-8 bytes
/// inside `'…'` directly.
fn sql_string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

/// Render raw bytes as a CH binary literal. Hex form via
/// `unhex('…')` keeps binary defaults round-trip safe for non-UTF-8
/// payloads; CH `String` columns accept the resulting bytes verbatim.
fn sql_bytes_literal(b: &[u8]) -> String {
    let mut hex = String::with_capacity(b.len() * 2);
    for byte in b {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("unhex('{hex}')")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::{IntervalValue, NumericKind};
    use crate::shadow_catalog::RelAttr;

    fn attr(oid: u32, typmod: i32, not_null: bool, missing: Option<&str>) -> RelAttr {
        RelAttr {
            attnum: 1,
            name: "c".into(),
            type_oid: oid,
            typmod,
            not_null,
            dropped: false,
            type_name: "test".into(),
            type_byval: true,
            type_len: 4,
            type_align: 'i',
            type_storage: 'p',
            missing_text: missing.map(String::from),
        }
    }

    #[test]
    fn int_types_map_to_signed_ch() {
        assert_eq!(
            base_type_for(&attr(INT2OID, -1, true, None)).unwrap(),
            "Int16"
        );
        assert_eq!(
            base_type_for(&attr(INT4OID, -1, true, None)).unwrap(),
            "Int32"
        );
        assert_eq!(
            base_type_for(&attr(INT8OID, -1, true, None)).unwrap(),
            "Int64"
        );
        assert_eq!(
            base_type_for(&attr(OIDOID, -1, true, None)).unwrap(),
            "UInt32"
        );
    }

    #[test]
    fn nullable_wraps_when_not_pk_and_not_not_null() {
        let r = map(&attr(INT4OID, -1, false, None), false).unwrap();
        assert_eq!(r.ch_type, "Nullable(Int32)");
        let r = map(&attr(INT4OID, -1, true, None), false).unwrap();
        assert_eq!(r.ch_type, "Int32");
        let r = map(&attr(INT4OID, -1, false, None), true).unwrap();
        assert_eq!(r.ch_type, "Int32", "pk_member forces non-nullable");
    }

    #[test]
    fn numeric_typmod_decodes_precision_and_scale() {
        // numeric(10, 2) → typmod = ((10 << 16) | 2) + 4 = 655370
        let tm = ((10i32 << 16) | 2) + VARHDRSZ;
        assert_eq!(
            base_type_for(&attr(NUMERICOID, tm, true, None)).unwrap(),
            "Decimal(10, 2)"
        );
        // numeric (no typmod) → String
        assert_eq!(
            base_type_for(&attr(NUMERICOID, -1, true, None)).unwrap(),
            "String"
        );
        // numeric(100, 2) → String (CH cap)
        let tm = ((100i32 << 16) | 2) + VARHDRSZ;
        assert_eq!(
            base_type_for(&attr(NUMERICOID, tm, true, None)).unwrap(),
            "String"
        );
    }

    #[test]
    fn timestamp_typmod_drives_precision() {
        assert_eq!(
            base_type_for(&attr(TIMESTAMPOID, 3, true, None)).unwrap(),
            "DateTime64(3, 'UTC')"
        );
        assert_eq!(
            base_type_for(&attr(TIMESTAMPTZOID, -1, true, None)).unwrap(),
            "DateTime64(6, 'UTC')"
        );
    }

    #[test]
    fn text_family_collapses_to_string() {
        for oid in [TEXTOID, VARCHAROID, BPCHAROID, NAMEOID, BYTEAOID] {
            assert_eq!(base_type_for(&attr(oid, -1, true, None)).unwrap(), "String");
        }
    }

    #[test]
    fn unknown_oid_falls_through_to_string() {
        assert_eq!(
            base_type_for(&attr(99999, -1, true, None)).unwrap(),
            "String"
        );
    }

    #[test]
    fn default_for_int_renders_literal_number() {
        let r = map(&attr(INT4OID, -1, true, Some("7")), false).unwrap();
        assert_eq!(r.ch_type, "Int32");
        assert_eq!(r.default_sql.as_deref(), Some("7"));
    }

    #[test]
    fn default_for_text_quotes_and_escapes() {
        let r = map(&attr(TEXTOID, -1, true, Some("o'rly")), false).unwrap();
        assert_eq!(r.default_sql.as_deref(), Some("'o''rly'"));
    }

    #[test]
    fn default_for_bool_renders_true_false() {
        let r = map(&attr(BOOLOID, -1, true, Some("t")), false).unwrap();
        assert_eq!(r.default_sql.as_deref(), Some("true"));
        let r = map(&attr(BOOLOID, -1, true, Some("f")), false).unwrap();
        assert_eq!(r.default_sql.as_deref(), Some("false"));
    }

    #[test]
    fn default_for_timestamp_renders_pgpending_bytes() {
        // missing_value_for routes TIMESTAMPTZ through PgPending (no Tier
        // 1/2 arm for it). The literal writer renders the raw typoutput
        // bytes through `unhex(...)` rather than `toDateTime64(...)`,
        // because reconstructing a typed CH literal from the bytes alone
        // would require oracle help. PHASE15 §4's drill exercises the
        // typed path end-to-end through the oracle's round-trip.
        let r = map(
            &attr(TIMESTAMPTZOID, 6, true, Some("2024-01-02 03:04:05+00")),
            false,
        )
        .unwrap();
        let d = r.default_sql.expect("default rendered");
        assert!(d.starts_with("unhex('"), "{d}");
    }

    #[test]
    fn column_value_text_escapes_singles() {
        let lit = column_value_to_sql_literal(&ColumnValue::Text("a'b".into()), "String").unwrap();
        assert_eq!(lit, "'a''b'");
    }

    #[test]
    fn column_value_int_round_trips() {
        let lit = column_value_to_sql_literal(&ColumnValue::Int4(-42), "Int32").unwrap();
        assert_eq!(lit, "-42");
    }

    #[test]
    fn column_value_numeric_to_decimal_drops_quotes() {
        let lit = column_value_to_sql_literal(
            &ColumnValue::Numeric(NumericKind::Finite("3.14".into())),
            "Decimal(10, 2)",
        )
        .unwrap();
        assert_eq!(lit, "3.14");
        // Same value into a String column gets quoted.
        let lit = column_value_to_sql_literal(
            &ColumnValue::Numeric(NumericKind::Finite("3.14".into())),
            "String",
        )
        .unwrap();
        assert_eq!(lit, "'3.14'");
    }

    #[test]
    fn column_value_interval_renders_pg_text() {
        let v = ColumnValue::Interval(IntervalValue {
            months: 0,
            days: 1,
            micros: 0,
        });
        let lit = column_value_to_sql_literal(&v, "String").unwrap();
        assert!(lit.starts_with('\''), "{lit}");
        assert!(lit.contains("day"), "{lit}");
    }

    #[test]
    fn column_value_uuid_uses_touuid_cast() {
        let uuid = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let lit = column_value_to_sql_literal(&ColumnValue::Uuid(uuid), "UUID").unwrap();
        assert_eq!(
            lit, "toUUID('01234567-89ab-cdef-fedc-ba9876543210')",
            "uuid format mismatch: {lit}"
        );
    }

    #[test]
    fn column_value_timestamp_routes_through_to_datetime64() {
        // PG epoch microseconds: 2024-01-02 03:04:05 UTC =
        // (24y * 365.25d + …) — exact value not asserted, just the
        // surrounding cast shape.
        let lit = column_value_to_sql_literal(&ColumnValue::TimestampTz(0), "DateTime64(6, 'UTC')")
            .unwrap();
        assert!(
            lit.starts_with("toDateTime64('2000-01-01"),
            "PG epoch should render as 2000-01-01: {lit}"
        );
        assert!(lit.ends_with(", 6, 'UTC')"), "{lit}");
    }

    #[test]
    fn column_value_bytea_renders_unhex_literal() {
        let lit =
            column_value_to_sql_literal(&ColumnValue::Bytea(vec![0xDE, 0xAD]), "String").unwrap();
        assert_eq!(lit, "unhex('dead')");
    }

    #[test]
    fn column_value_null_is_explicit_null() {
        let lit = column_value_to_sql_literal(&ColumnValue::Null, "Int32").unwrap();
        assert_eq!(lit, "NULL");
    }

    #[test]
    fn external_toast_does_not_render() {
        use crate::heap_decoder::ToastPointer;
        let v = ColumnValue::ExternalToast(ToastPointer {
            va_rawsize: 1,
            va_extinfo: 0,
            va_valueid: 0,
            va_toastrelid: 0,
        });
        assert!(column_value_to_sql_literal(&v, "String").is_none());
    }

    #[test]
    fn float_default_keeps_decimal() {
        assert_eq!(format_float(0.0), "0.0");
        assert_eq!(format_float(1.5), "1.5");
        assert_eq!(format_float(f64::NAN), "nan");
        assert_eq!(format_float(f64::INFINITY), "inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn datetime64_precision_clamps() {
        assert_eq!(datetime64_ch_type(0), "DateTime64(0, 'UTC')");
        assert_eq!(datetime64_ch_type(6), "DateTime64(6, 'UTC')");
        assert_eq!(datetime64_ch_type(9), "DateTime64(6, 'UTC')");
        assert_eq!(datetime64_ch_type(-1), "DateTime64(6, 'UTC')");
    }
}
