//! PG datum decoders and output formatters, grouped by type family
//! Decode varlena bodies without outer headers, fixed-width datums as stored

mod fixed;
mod inet;
mod interval;
mod jsonb;
mod numeric;
mod text_array;
mod time;
mod uuid;

pub(crate) use fixed::{
    decode_tid, encode_tid, macaddr_to_text, macaddr8_to_text, parse_macaddr, parse_tid,
    tid_to_text,
};
pub use inet::{InetValue, PGSQL_AF_INET, PGSQL_AF_INET6, decode_inet};
pub use interval::{IntervalValue, decode_interval};
pub use jsonb::{decode_jsonb, jsonb_text_len, render_jsonb_text};
pub(crate) use numeric::{Numeric, parse_numeric, scaled_groups};
pub use numeric::{
    Special, numeric_body, numeric_from_text, numeric_special_body, numeric_text_len,
    render_numeric_text,
};
pub use text_array::decode_text_array;
pub(crate) use time::{format_time_us, timetz_to_text};
pub(crate) use uuid::uuid_to_ch_wire;

#[cfg(test)]
pub(crate) use text_array::text_array_body;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("truncated body at offset {offset}: need {need} bytes, have {have}")]
    Truncated {
        offset: usize,
        need: usize,
        have: usize,
    },
    #[error("malformed numeric: weight={weight} ndigits={ndigits} dscale={dscale}")]
    BadNumeric {
        weight: i32,
        ndigits: usize,
        dscale: i32,
    },
    #[error("malformed inet: family={family:#x} bits={bits} addr_len={addr_len}")]
    BadInet {
        family: u8,
        bits: u8,
        addr_len: usize,
    },
    #[error("malformed jsonb container: {0}")]
    BadJsonbContainer(&'static str),
}
