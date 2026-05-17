//! TCP client. Wraps `chc_client_*` from `clickhouse-client.h`.
//!
//! The caller owns the socket (or any other transport) and supplies it
//! via [`PosixIo`]; this crate does not do TCP, DNS, or TLS itself.

use core::ffi::c_char;
use core::pin::Pin;
use core::ptr::NonNull;
use core::slice;

use crate::alloc::Allocator;
use crate::block::Block;
use crate::builder::BlockBuilder;
use crate::codec::{Codec, Compression};
use crate::error::{Error, ErrorKind, Result, check};
use crate::io::PosixIo;
use crate::sys;

pub const DEFAULT_REVISION: u64 = sys::CHC_CLIENT_DEFAULT_REVISION;

/// Connection settings. NUL-terminated `CString`-style buffers held
/// inline so the C side's borrowed pointers stay valid through
/// [`Client::init`].
pub struct ClientOpts {
    client_name: Option<Vec<u8>>,
    database: Option<Vec<u8>>,
    user: Option<Vec<u8>>,
    password: Option<Vec<u8>>,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_version_patch: u64,
    pub client_revision: u64,
    pub compression: Compression,
    pub read_buffer_bytes: usize,
}

impl Default for ClientOpts {
    fn default() -> Self {
        Self {
            client_name: None,
            database: None,
            user: None,
            password: None,
            client_version_major: 0,
            client_version_minor: 0,
            client_version_patch: 0,
            client_revision: 0,
            compression: Compression::None,
            read_buffer_bytes: 0,
        }
    }
}

impl ClientOpts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn client_name(mut self, s: &str) -> Self {
        self.client_name = Some(cstring(s));
        self
    }
    pub fn database(mut self, s: &str) -> Self {
        self.database = Some(cstring(s));
        self
    }
    pub fn user(mut self, s: &str) -> Self {
        self.user = Some(cstring(s));
        self
    }
    pub fn password(mut self, s: &str) -> Self {
        self.password = Some(cstring(s));
        self
    }

    fn to_raw(&self, codec: Option<*const sys::chc_codec>) -> sys::chc_client_opts {
        let mut raw = sys::chc_client_opts::zeroed();
        raw.client_name = ptr_or_null(&self.client_name);
        raw.database = ptr_or_null(&self.database);
        raw.user = ptr_or_null(&self.user);
        raw.password = ptr_or_null(&self.password);
        raw.client_version_major = self.client_version_major;
        raw.client_version_minor = self.client_version_minor;
        raw.client_version_patch = self.client_version_patch;
        raw.client_revision = self.client_revision;
        raw.compression = self.compression as i32;
        raw.codec = codec.unwrap_or(core::ptr::null());
        raw.read_buffer_bytes = self.read_buffer_bytes;
        raw
    }
}

fn cstring(s: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}

fn ptr_or_null(buf: &Option<Vec<u8>>) -> *const c_char {
    buf.as_deref()
        .map(|b| b.as_ptr().cast::<c_char>())
        .unwrap_or(core::ptr::null())
}

/// Information sent by the server during the Hello handshake.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub name: String,
    pub timezone: String,
    pub display_name: String,
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    pub revision: u64,
}

impl ServerInfo {
    fn from_raw(raw: &sys::chc_server_info) -> Self {
        Self {
            name: cstr_array_to_string(&raw.name),
            timezone: cstr_array_to_string(&raw.timezone),
            display_name: cstr_array_to_string(&raw.display_name),
            version_major: raw.version_major,
            version_minor: raw.version_minor,
            version_patch: raw.version_patch,
            revision: raw.revision,
        }
    }
}

fn cstr_array_to_string(buf: &[c_char]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let bytes: &[u8] = unsafe { slice::from_raw_parts(buf.as_ptr().cast::<u8>(), end) };
    String::from_utf8_lossy(bytes).into_owned()
}

/// Open ClickHouse client. Owns the underlying `chc_client *`; the
/// [`PosixIo`] it was constructed with must outlive the client (callers
/// keep both pinned and drop in this order: client first, then I/O).
pub struct Client {
    raw: NonNull<sys::chc_client>,
    // `chc_client_init` stashes `c->al = al`, i.e. it stores a pointer
    // back into the `chc_alloc` struct we pass in. Holding the
    // allocator by value would let it move when `Self` is constructed
    // and again on every Client move, invalidating the pointer the C
    // side dereferences on every subsequent `alloc/realloc/free`. The
    // Box keeps the address stable for the Client's lifetime.
    alloc: Pin<Box<Allocator>>,
}

impl Client {
    /// Performs Hello / HelloAck against the supplied I/O.
    ///
    /// `codec` may be `None` only when `opts.compression` is `None`.
    pub fn init(
        opts: &ClientOpts,
        alloc: Allocator,
        io: Pin<&mut PosixIo>,
        codec: Option<Pin<&Codec>>,
    ) -> Result<Self> {
        let codec_ptr = codec.map(|c| c.as_ptr());
        let raw_opts = opts.to_raw(codec_ptr);
        let alloc = Box::pin(alloc);
        let mut out: *mut sys::chc_client = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_client_init(&mut out, &raw_opts, alloc.as_ptr(), io.io_ptr(), &mut err)
        };
        check(rc, &err)?;
        Ok(Self {
            raw: NonNull::new(out).expect("chc_client_init returned OK with NULL"),
            alloc,
        })
    }

    pub fn server_info(&self) -> Option<ServerInfo> {
        let p = unsafe { sys::chc_client_server_info(self.raw.as_ptr().cast_const()) };
        if p.is_null() {
            None
        } else {
            Some(ServerInfo::from_raw(unsafe { &*p }))
        }
    }

    pub fn send_query(&mut self, sql: &str, query_id: Option<&str>) -> Result<()> {
        let (qid, qid_len) = query_id
            .map(|q| (q.as_ptr().cast::<c_char>(), q.len()))
            .unwrap_or((core::ptr::null(), 0));
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_client_send_query(
                self.raw.as_ptr(),
                sql.as_ptr().cast::<c_char>(),
                sql.len(),
                qid,
                qid_len,
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// Send a Data block. Passing [`None`] writes the empty block that
    /// terminates a query / signals "no more INSERT rows".
    pub fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        let bb_ptr = builder.map(|b| b.as_ptr()).unwrap_or(core::ptr::null());
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_data(self.raw.as_ptr(), bb_ptr, &mut err) };
        check(rc, &err)
    }

    pub fn send_cancel(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_cancel(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    pub fn send_ping(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_ping(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    /// Read the next packet. Any block / exception payload is owned by
    /// the returned [`Packet`] and freed on drop unless taken out.
    pub fn recv_packet(&mut self) -> Result<Packet<'_>> {
        let mut raw = sys::chc_packet::zeroed();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_recv_packet(self.raw.as_ptr(), &mut raw, &mut err) };
        check(rc, &err)?;
        Ok(Packet { raw, client: self })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        unsafe { sys::chc_client_close(self.raw.as_ptr()) };
    }
}

unsafe impl Send for Client {}

/// Server-side exception. Owning wrapper around the C `chc_exception`
/// chain head. [`Drop`] calls `chc_exception_free`, which walks `nested`
/// & frees every link.
pub struct Exception {
    raw: NonNull<sys::chc_exception>,
    alloc: Allocator,
}

impl Exception {
    /// SAFETY: `raw` must point at a `chc_exception` chain owned by the
    /// caller; `alloc` must be the allocator it was created with.
    unsafe fn from_raw(raw: NonNull<sys::chc_exception>, alloc: Allocator) -> Self {
        Self { raw, alloc }
    }

    pub fn code(&self) -> i32 {
        unsafe { (*self.raw.as_ptr()).code }
    }

    pub fn name(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.name, r.name_len)
    }

    pub fn display_text(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.display_text, r.display_text_len)
    }

    pub fn stack_trace(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.stack_trace, r.stack_trace_len)
    }

    /// Next link in the chain, borrowed from this owner.
    pub fn nested(&self) -> Option<ExceptionRef<'_>> {
        let p = unsafe { (*self.raw.as_ptr()).nested };
        NonNull::new(p).map(|raw| ExceptionRef {
            raw: raw.as_ptr().cast_const(),
            _marker: core::marker::PhantomData,
        })
    }
}

impl Drop for Exception {
    fn drop(&mut self) {
        unsafe { sys::chc_exception_free(self.raw.as_ptr(), self.alloc.as_ptr()) };
    }
}

impl core::fmt::Debug for Exception {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Exception")
            .field("code", &self.code())
            .field("name", &String::from_utf8_lossy(self.name()))
            .field(
                "display_text",
                &String::from_utf8_lossy(self.display_text()),
            )
            .field("stack_trace", &String::from_utf8_lossy(self.stack_trace()))
            .field("nested", &self.nested())
            .finish()
    }
}

impl core::fmt::Display for Exception {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{} (code {}): {}",
            String::from_utf8_lossy(self.name()),
            self.code(),
            String::from_utf8_lossy(self.display_text()),
        )
    }
}

impl std::error::Error for Exception {}

unsafe impl Send for Exception {}

impl From<Exception> for Error {
    fn from(exc: Exception) -> Self {
        Self {
            kind: ErrorKind::Server,
            server_code: exc.code(),
            message: String::from_utf8_lossy(exc.display_text()).into_owned(),
            server_name: String::from_utf8_lossy(exc.name()).into_owned(),
        }
    }
}

/// Borrowed view of a chain link. Lifetime ties to the owning
/// [`Exception`] head.
#[derive(Clone, Copy)]
pub struct ExceptionRef<'e> {
    raw: *const sys::chc_exception,
    _marker: core::marker::PhantomData<&'e sys::chc_exception>,
}

impl<'e> ExceptionRef<'e> {
    pub fn code(self) -> i32 {
        unsafe { (*self.raw).code }
    }

    pub fn name(self) -> &'e [u8] {
        let r = unsafe { &*self.raw };
        cstr_bytes(r.name, r.name_len)
    }

    pub fn display_text(self) -> &'e [u8] {
        let r = unsafe { &*self.raw };
        cstr_bytes(r.display_text, r.display_text_len)
    }

    pub fn stack_trace(self) -> &'e [u8] {
        let r = unsafe { &*self.raw };
        cstr_bytes(r.stack_trace, r.stack_trace_len)
    }

    pub fn nested(self) -> Option<ExceptionRef<'e>> {
        let p = unsafe { (*self.raw).nested };
        NonNull::new(p).map(|raw| ExceptionRef {
            raw: raw.as_ptr().cast_const(),
            _marker: core::marker::PhantomData,
        })
    }
}

impl<'e> core::fmt::Debug for ExceptionRef<'e> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ExceptionRef")
            .field("code", &self.code())
            .field("name", &String::from_utf8_lossy(self.name()))
            .field(
                "display_text",
                &String::from_utf8_lossy(self.display_text()),
            )
            .field("stack_trace", &String::from_utf8_lossy(self.stack_trace()))
            .field("nested", &self.nested())
            .finish()
    }
}

fn cstr_bytes<'a>(ptr: *mut c_char, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        return &[];
    }
    unsafe { slice::from_raw_parts(ptr.cast::<u8>(), len) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PacketKind {
    Data = sys::CHC_PKT_DATA,
    Exception = sys::CHC_PKT_EXCEPTION,
    Progress = sys::CHC_PKT_PROGRESS,
    Pong = sys::CHC_PKT_PONG,
    EndOfStream = sys::CHC_PKT_END_OF_STREAM,
    ProfileInfo = sys::CHC_PKT_PROFILE_INFO,
    Totals = sys::CHC_PKT_TOTALS,
    Extremes = sys::CHC_PKT_EXTREMES,
    Log = sys::CHC_PKT_LOG,
    TableColumns = sys::CHC_PKT_TABLE_COLUMNS,
    ProfileEvents = sys::CHC_PKT_PROFILE_EVENTS,
}

impl PacketKind {
    fn from_raw(k: sys::chc_packet_kind) -> Option<Self> {
        Some(match k {
            sys::CHC_PKT_DATA => Self::Data,
            sys::CHC_PKT_EXCEPTION => Self::Exception,
            sys::CHC_PKT_PROGRESS => Self::Progress,
            sys::CHC_PKT_PONG => Self::Pong,
            sys::CHC_PKT_END_OF_STREAM => Self::EndOfStream,
            sys::CHC_PKT_PROFILE_INFO => Self::ProfileInfo,
            sys::CHC_PKT_TOTALS => Self::Totals,
            sys::CHC_PKT_EXTREMES => Self::Extremes,
            sys::CHC_PKT_LOG => Self::Log,
            sys::CHC_PKT_TABLE_COLUMNS => Self::TableColumns,
            sys::CHC_PKT_PROFILE_EVENTS => Self::ProfileEvents,
            _ => return None,
        })
    }
}

/// Borrowed packet, owns its block/exception payloads until dropped.
pub struct Packet<'c> {
    raw: sys::chc_packet,
    client: &'c mut Client,
}

impl<'c> Packet<'c> {
    pub fn kind(&self) -> Option<PacketKind> {
        PacketKind::from_raw(self.raw.kind)
    }

    pub fn progress(&self) -> &sys::chc_packet_progress {
        &self.raw.progress
    }

    pub fn profile(&self) -> &sys::chc_packet_profile {
        &self.raw.profile
    }

    /// Take ownership of the block payload (Data / Totals / Extremes /
    /// Log / ProfileEvents packets). Subsequent calls return `None`.
    pub fn take_block(&mut self) -> Option<Block> {
        if self.raw.block.is_null() {
            return None;
        }
        let raw = self.raw.block;
        self.raw.block = core::ptr::null_mut();
        // SAFETY: ownership transfer; the same allocator was used by
        // chc_client_recv_packet (Client carries it).
        unsafe { Block::from_raw(raw, *self.client.alloc) }
    }

    /// Take ownership of the exception chain on an Exception packet.
    /// Subsequent calls return `None`.
    pub fn take_exception(&mut self) -> Option<Exception> {
        let raw = NonNull::new(self.raw.exception)?;
        self.raw.exception = core::ptr::null_mut();
        // SAFETY: ownership transfer; allocator matches the one that
        // built the chain in chc_client_recv_packet.
        Some(unsafe { Exception::from_raw(raw, *self.client.alloc) })
    }
}

impl<'c> Drop for Packet<'c> {
    fn drop(&mut self) {
        // chc_packet_clear is safe to call with already-NULLed fields.
        unsafe { sys::chc_packet_clear(self.client.raw.as_ptr(), &mut self.raw) };
    }
}
