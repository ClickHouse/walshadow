//! FFI bindings for clickhouse-c.
//!
//! Struct definitions and `extern` declarations are hand-written to keep
//! the surface auditable and avoid pulling in bindgen + libclang.
//!
//! Integer constants from `enum` blocks and a few `#define`s are
//! scanned out of the headers at build time by `build.rs` and pulled in
//! via the `include!` below. Bumping the vendored headers automatically
//! re-syncs the constants.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]

use core::ffi::{c_char, c_int, c_void};

/* ---- named C-enum types (consts that reference these live below) ---- */

pub type chc_kind = c_int;
pub type chc_col_kind = c_int;
pub type chc_compression = c_int;
pub type chc_packet_kind = c_int;

// Bare literal (not behind a #define upstream); kept hand-written.
pub const CHC_ERR_NAME_LEN: usize = 64;

include!(concat!(env!("OUT_DIR"), "/sys_constants.rs"));

/* ---- errors ---- */

#[repr(C)]
pub struct chc_err {
    pub code: c_int,
    pub server_code: c_int,
    pub msg: [c_char; CHC_ERR_MSG_LEN],
    pub server_name: [c_char; CHC_ERR_NAME_LEN],
}

impl chc_err {
    pub const fn zeroed() -> Self {
        Self {
            code: 0,
            server_code: 0,
            msg: [0; CHC_ERR_MSG_LEN],
            server_name: [0; CHC_ERR_NAME_LEN],
        }
    }
}

/* ---- allocator ---- */

#[repr(C)]
#[derive(Clone, Copy)]
pub struct chc_alloc {
    pub ud: *mut c_void,
    pub alloc: Option<unsafe extern "C" fn(ud: *mut c_void, bytes: usize) -> *mut c_void>,
    pub realloc: Option<
        unsafe extern "C" fn(
            ud: *mut c_void,
            p: *mut c_void,
            old_bytes: usize,
            new_bytes: usize,
        ) -> *mut c_void,
    >,
    pub free: Option<unsafe extern "C" fn(ud: *mut c_void, p: *mut c_void, bytes: usize)>,
}

unsafe extern "C" {
    pub fn chc_alloc_stdlib() -> chc_alloc;
}

/* ---- io ---- */

#[repr(C)]
pub struct chc_io {
    pub ud: *mut c_void,
    pub read: Option<
        unsafe extern "C" fn(
            ud: *mut c_void,
            buf: *mut c_void,
            len: usize,
            out_n: *mut usize,
            err: *mut chc_err,
        ) -> c_int,
    >,
    pub write: Option<
        unsafe extern "C" fn(
            ud: *mut c_void,
            buf: *const c_void,
            len: usize,
            err: *mut chc_err,
        ) -> c_int,
    >,
    pub check_cancel: Option<unsafe extern "C" fn(ud: *mut c_void) -> c_int>,
}

#[repr(C)]
pub struct chc_posix_io {
    pub fd: c_int,
    pub check_cancel: Option<unsafe extern "C" fn(ud: *mut c_void) -> bool>,
    pub cancel_ud: *mut c_void,
}

unsafe extern "C" {
    pub fn chc_posix_io_init(
        state: *mut chc_posix_io,
        out_io: *mut chc_io,
        fd: c_int,
        check_cancel: Option<unsafe extern "C" fn(ud: *mut c_void) -> bool>,
        cancel_ud: *mut c_void,
    );
}

/* ---- type AST ---- */

#[repr(C)]
pub struct chc_type {
    _opaque: [u8; 0],
}

unsafe extern "C" {
    pub fn chc_type_parse(
        name: *const c_char,
        name_len: usize,
        al: *const chc_alloc,
        out: *mut *mut chc_type,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_type_destroy(t: *mut chc_type, al: *const chc_alloc);

    pub fn chc_type_kind(t: *const chc_type) -> chc_kind;
    pub fn chc_type_n_children(t: *const chc_type) -> usize;
    pub fn chc_type_child(t: *const chc_type, i: usize) -> *const chc_type;

    pub fn chc_type_fixed_size(t: *const chc_type) -> c_int;
    pub fn chc_type_elem_size(t: *const chc_type) -> usize;
    pub fn chc_type_decimal_precision(t: *const chc_type) -> c_int;
    pub fn chc_type_decimal_scale(t: *const chc_type) -> c_int;
    pub fn chc_type_datetime64_scale(t: *const chc_type) -> c_int;
    pub fn chc_type_timezone(t: *const chc_type, out_len: *mut usize) -> *const c_char;
    pub fn chc_type_name(t: *const chc_type, out_len: *mut usize) -> *const c_char;

    pub fn chc_type_enum_count(t: *const chc_type) -> usize;
    pub fn chc_type_enum_at(
        t: *const chc_type,
        i: usize,
        name: *mut *const c_char,
        name_len: *mut usize,
        value: *mut i64,
    );

    pub fn chc_type_tuple_field_name(
        t: *const chc_type,
        i: usize,
        out_len: *mut usize,
    ) -> *const c_char;

    pub fn chc_type_format(t: *const chc_type, buf: *mut c_char, buf_len: usize) -> usize;
}

/* ---- columns ---- */

#[repr(C)]
pub struct chc_column {
    _opaque: [u8; 0],
}

unsafe extern "C" {
    pub fn chc_column_layout(c: *const chc_column) -> chc_col_kind;
    pub fn chc_column_n_rows(c: *const chc_column) -> usize;
    pub fn chc_column_fixed_data(c: *const chc_column, elem_size: *mut usize) -> *const c_void;
    pub fn chc_column_string_data(c: *const chc_column) -> *const u8;
    pub fn chc_column_string_offsets(c: *const chc_column) -> *const u64;
    pub fn chc_column_null_map(c: *const chc_column) -> *const u8;
    pub fn chc_column_nullable_inner(c: *const chc_column) -> *const chc_column;
    pub fn chc_column_array_offsets(c: *const chc_column) -> *const u64;
    pub fn chc_column_array_values(c: *const chc_column) -> *const chc_column;
    pub fn chc_column_tuple_arity(c: *const chc_column) -> usize;
    pub fn chc_column_tuple_child(c: *const chc_column, i: usize) -> *const chc_column;
    pub fn chc_column_lc_key_size(c: *const chc_column) -> c_int;
    pub fn chc_column_lc_keys(c: *const chc_column) -> *const c_void;
    pub fn chc_column_lc_dict(c: *const chc_column) -> *const chc_column;
}

/* ---- block reader ---- */

#[repr(C)]
pub struct chc_block {
    _opaque: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct chc_block_opts {
    pub has_block_info: bool,
    pub has_custom_serialization: bool,
    pub read_buffer_bytes: usize,
}

impl chc_block_opts {
    pub const fn zeroed() -> Self {
        Self {
            has_block_info: false,
            has_custom_serialization: false,
            read_buffer_bytes: 0,
        }
    }
}

unsafe extern "C" {
    pub fn chc_block_read(
        io: *mut chc_io,
        al: *const chc_alloc,
        opts: *const chc_block_opts,
        out: *mut *mut chc_block,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_block_destroy(b: *mut chc_block, al: *const chc_alloc);

    pub fn chc_block_n_rows(b: *const chc_block) -> usize;
    pub fn chc_block_n_columns(b: *const chc_block) -> usize;
    pub fn chc_block_column_name(
        b: *const chc_block,
        i: usize,
        out_len: *mut usize,
    ) -> *const c_char;
    pub fn chc_block_column_type(b: *const chc_block, i: usize) -> *const chc_type;
    pub fn chc_block_column(b: *const chc_block, i: usize) -> *const chc_column;

    pub fn chc_block_is_overflows(b: *const chc_block) -> bool;
    pub fn chc_block_bucket_num(b: *const chc_block) -> i32;
}

/* ---- block builder ---- */

#[repr(C)]
pub struct chc_block_builder {
    _opaque: [u8; 0],
}

unsafe extern "C" {
    pub fn chc_block_builder_init(
        out: *mut *mut chc_block_builder,
        al: *const chc_alloc,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_block_builder_destroy(bb: *mut chc_block_builder);

    pub fn chc_block_builder_append_fixed(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        t: *const chc_type,
        data: *const c_void,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_builder_append_string(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        offsets: *const u64,
        data: *const u8,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_builder_append_nullable_fixed(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        t: *const chc_type,
        null_map: *const u8,
        inner_data: *const c_void,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_builder_append_nullable_string(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        t: *const chc_type,
        null_map: *const u8,
        inner_offsets: *const u64,
        inner_data: *const u8,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_builder_append_array_fixed(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        t: *const chc_type,
        offsets: *const u64,
        values: *const c_void,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_builder_append_array_string(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        t: *const chc_type,
        offsets: *const u64,
        values_offsets: *const u64,
        values_data: *const u8,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_builder_append_json_string(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        t: *const chc_type,
        offsets: *const u64,
        data: *const u8,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_builder_append_low_cardinality_string(
        bb: *mut chc_block_builder,
        name: *const c_char,
        name_len: usize,
        t: *const chc_type,
        key_size: c_int,
        keys: *const c_void,
        dict_offsets: *const u64,
        dict_data: *const u8,
        dict_n: usize,
        n_rows: usize,
        err: *mut chc_err,
    ) -> c_int;

    pub fn chc_block_write(
        io: *mut chc_io,
        bb: *const chc_block_builder,
        opts: *const chc_block_opts,
        err: *mut chc_err,
    ) -> c_int;
}

/* ---- compression ---- */

#[repr(C)]
pub struct chc_codec {
    pub ud: *mut c_void,
    pub lz4_compress: Option<
        unsafe extern "C" fn(
            ud: *mut c_void,
            src: *const c_void,
            src_len: usize,
            dst: *mut c_void,
            dst_cap: usize,
            dst_n: *mut usize,
            err: *mut chc_err,
        ) -> c_int,
    >,
    pub lz4_decompress: Option<
        unsafe extern "C" fn(
            ud: *mut c_void,
            src: *const c_void,
            src_len: usize,
            dst: *mut c_void,
            original_size: usize,
            err: *mut chc_err,
        ) -> c_int,
    >,
    pub zstd_compress: Option<
        unsafe extern "C" fn(
            ud: *mut c_void,
            src: *const c_void,
            src_len: usize,
            dst: *mut c_void,
            dst_cap: usize,
            dst_n: *mut usize,
            err: *mut chc_err,
        ) -> c_int,
    >,
    pub zstd_decompress: Option<
        unsafe extern "C" fn(
            ud: *mut c_void,
            src: *const c_void,
            src_len: usize,
            dst: *mut c_void,
            original_size: usize,
            err: *mut chc_err,
        ) -> c_int,
    >,
    pub lz4_bound: Option<unsafe extern "C" fn(src_len: usize) -> usize>,
    pub zstd_bound: Option<unsafe extern "C" fn(src_len: usize) -> usize>,
}

unsafe extern "C" {
    pub fn chc_cityhash128(
        data: *const c_void,
        len: usize,
        out_lo: *mut u64,
        out_hi: *mut u64,
    );
}

#[cfg(feature = "lz4")]
unsafe extern "C" {
    pub fn chc_lz4_codec_init(out: *mut chc_codec);
}

#[cfg(feature = "zstd")]
unsafe extern "C" {
    pub fn chc_zstd_codec_init(out: *mut chc_codec);
}

/* ---- client (TCP) ---- */

#[repr(C)]
pub struct chc_client_opts {
    pub client_name: *const c_char,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_version_patch: u64,
    pub client_revision: u64,
    pub database: *const c_char,
    pub user: *const c_char,
    pub password: *const c_char,
    pub compression: chc_compression,
    pub codec: *const chc_codec,
    pub read_buffer_bytes: usize,
}

impl chc_client_opts {
    pub const fn zeroed() -> Self {
        Self {
            client_name: core::ptr::null(),
            client_version_major: 0,
            client_version_minor: 0,
            client_version_patch: 0,
            client_revision: 0,
            database: core::ptr::null(),
            user: core::ptr::null(),
            password: core::ptr::null(),
            compression: CHC_COMP_NONE,
            codec: core::ptr::null(),
            read_buffer_bytes: 0,
        }
    }
}

#[repr(C)]
pub struct chc_server_info {
    pub name: [c_char; 64],
    pub timezone: [c_char; 64],
    pub display_name: [c_char; 128],
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    pub revision: u64,
}

#[repr(C)]
pub struct chc_client {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct chc_exception {
    pub code: i32,
    pub name: *mut c_char,
    pub name_len: usize,
    pub display_text: *mut c_char,
    pub display_text_len: usize,
    pub stack_trace: *mut c_char,
    pub stack_trace_len: usize,
    pub nested: *mut chc_exception,
}

#[repr(C)]
pub struct chc_packet_progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
}

#[repr(C)]
pub struct chc_packet_profile {
    pub rows: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub rows_before_limit: u64,
    pub applied_limit: u8,
    pub calculated_rows_before_limit: u8,
}

#[repr(C)]
pub struct chc_packet {
    pub kind: chc_packet_kind,
    pub block: *mut chc_block,
    pub exception: *mut chc_exception,
    pub progress: chc_packet_progress,
    pub profile: chc_packet_profile,
}

impl chc_packet {
    pub const fn zeroed() -> Self {
        Self {
            kind: 0,
            block: core::ptr::null_mut(),
            exception: core::ptr::null_mut(),
            progress: chc_packet_progress {
                rows: 0,
                bytes: 0,
                total_rows: 0,
                written_rows: 0,
                written_bytes: 0,
            },
            profile: chc_packet_profile {
                rows: 0,
                blocks: 0,
                bytes: 0,
                rows_before_limit: 0,
                applied_limit: 0,
                calculated_rows_before_limit: 0,
            },
        }
    }
}

#[repr(C)]
pub struct chc_query_setting {
    pub name: *const c_char,
    pub value: *const c_char,
    pub important: bool,
    pub custom: bool,
}

#[repr(C)]
pub struct chc_query_param {
    pub name: *const c_char,
    pub value: *const c_char,
}

#[repr(C)]
pub struct chc_query_opts {
    pub query_id: *const c_char,
    pub query_id_len: usize,
    pub settings: *const chc_query_setting,
    pub n_settings: usize,
    pub params: *const chc_query_param,
    pub n_params: usize,
}

unsafe extern "C" {
    pub fn chc_client_init(
        out: *mut *mut chc_client,
        opts: *const chc_client_opts,
        al: *const chc_alloc,
        io: *mut chc_io,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_client_close(c: *mut chc_client);
    pub fn chc_client_server_info(c: *const chc_client) -> *const chc_server_info;
    pub fn chc_client_send_query(
        c: *mut chc_client,
        sql: *const c_char,
        sql_len: usize,
        query_id: *const c_char,
        query_id_len: usize,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_client_send_query_ex(
        c: *mut chc_client,
        sql: *const c_char,
        sql_len: usize,
        opts: *const chc_query_opts,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_client_recv_packet(
        c: *mut chc_client,
        out: *mut chc_packet,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_packet_clear(c: *mut chc_client, p: *mut chc_packet);
    pub fn chc_client_send_data(
        c: *mut chc_client,
        bb: *const chc_block_builder,
        err: *mut chc_err,
    ) -> c_int;
    pub fn chc_client_send_cancel(c: *mut chc_client, err: *mut chc_err) -> c_int;
    pub fn chc_client_send_ping(c: *mut chc_client, err: *mut chc_err) -> c_int;
    pub fn chc_exception_free(e: *mut chc_exception, al: *const chc_alloc);
}
