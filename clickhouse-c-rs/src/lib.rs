//! Rust bindings for [clickhouse-c], a header-only C client for the
//! ClickHouse Native wire format.
//!
//! Two entry points:
//!
//! * [`PosixIo`] over a pipe / socket fd + [`Block`] / [`BlockBuilder`]:
//!   read or write Native blocks without going through the TCP packet
//!   loop. Suitable for piping into `clickhouse local --format Native`
//!   or for talking to any peer that speaks raw block frames.
//! * [`Client`] over a connected TCP `PosixIo`: full Hello / Query /
//!   Data / EOS / Exception / Progress packet loop with optional LZ4 /
//!   ZSTD compression.
//!
//! [clickhouse-c]: https://github.com/ClickHouse/clickhouse-c

pub mod sys;

mod alloc;
mod block;
mod builder;
mod client;
mod codec;
mod error;
mod io;
mod types;

pub use alloc::Allocator;
pub use block::{Block, BlockOpts, Column, ColumnLayout, LowCardinalityView};
pub use builder::BlockBuilder;
pub use client::{
    Client, ClientOpts, DEFAULT_REVISION, Exception, ExceptionRef, Packet, PacketKind, ServerInfo,
};
pub use codec::{cityhash128, Codec, Compression};
pub use error::{Error, ErrorKind, Result};
pub use io::PosixIo;
pub use types::{Kind, TypeAst, TypeRef};
