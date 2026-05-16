//! Error type wrapping clickhouse-c's `chc_err`.

use core::ffi::c_int;

use crate::sys;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Io,
    Eof,
    Protocol,
    Type,
    Oom,
    Cancelled,
    Server,
    Usage,
    Other(c_int),
}

impl ErrorKind {
    pub(crate) fn from_code(code: c_int) -> Self {
        match code {
            sys::CHC_ERR_IO => Self::Io,
            sys::CHC_ERR_EOF => Self::Eof,
            sys::CHC_ERR_PROTOCOL => Self::Protocol,
            sys::CHC_ERR_TYPE => Self::Type,
            sys::CHC_ERR_OOM => Self::Oom,
            sys::CHC_ERR_CANCELLED => Self::Cancelled,
            sys::CHC_ERR_SERVER => Self::Server,
            sys::CHC_ERR_USAGE => Self::Usage,
            other => Self::Other(other),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("clickhouse-c: {kind:?}: {message}")]
pub struct Error {
    pub kind: ErrorKind,
    pub server_code: i32,
    pub message: String,
    pub server_name: String,
}

impl Error {
    pub(crate) fn from_raw(e: &sys::chc_err) -> Self {
        Self {
            kind: ErrorKind::from_code(e.code),
            server_code: e.server_code,
            message: cstr_array_to_string(&e.msg),
            server_name: cstr_array_to_string(&e.server_name),
        }
    }
}

pub type Result<T> = core::result::Result<T, Error>;

fn cstr_array_to_string(buf: &[core::ffi::c_char]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let bytes: &[u8] = unsafe { core::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), end) };
    String::from_utf8_lossy(bytes).into_owned()
}

#[inline]
pub(crate) fn check(rc: c_int, err: &sys::chc_err) -> Result<()> {
    if rc >= 0 && err.code == sys::CHC_OK {
        Ok(())
    } else {
        Err(Error::from_raw(err))
    }
}
