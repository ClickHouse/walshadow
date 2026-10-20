//! PG datum decoders and output formatters, grouped by type family
//! Decode varlena bodies without outer headers, fixed-width datums as stored

mod inet;
mod interval;
mod jsonb;
mod numeric;
mod text_array;
mod time;
mod uuid;

pub use inet::{InetValue, PGSQL_AF_INET, PGSQL_AF_INET6, decode_inet};
pub use interval::{IntervalValue, decode_interval};
pub use jsonb::{decode_jsonb, decode_jsonb_with_capacity, jsonb_text_len};
pub use numeric::{NumericKind, decode_numeric, numeric_text_len};
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
