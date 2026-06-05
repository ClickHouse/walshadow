//! ABI guard for the hand-written `#[repr(C)]` mirrors in `src/sys.rs`.
//!
//! `src/sys.rs` mirrors clickhouse-c's public by-value structs by hand
//! (no bindgen), so a field added / removed / reordered in a vendored-header
//! bump would drift silently into undefined behaviour at the FFI boundary —
//! `build.rs` only re-syncs enum / `#define` constants, not struct geometry.
//!
//! `src/layout_probe.c` is compiled against the genuine headers and exposes
//! each struct's `sizeof` / `_Alignof` / `offsetof`. Here we assert they
//! match Rust's view, turning that drift into a test failure.

use core::mem::{align_of, offset_of, size_of};

use clickhouse_c::sys;

unsafe extern "C" {
    fn chc_rs_size_chc_err() -> usize;
    fn chc_rs_align_chc_err() -> usize;
    fn chc_rs_off_chc_err__server_code() -> usize;
    fn chc_rs_off_chc_err__msg() -> usize;
    fn chc_rs_off_chc_err__server_name() -> usize;

    fn chc_rs_size_chc_alloc() -> usize;
    fn chc_rs_align_chc_alloc() -> usize;
    fn chc_rs_off_chc_alloc__ud() -> usize;
    fn chc_rs_off_chc_alloc__alloc() -> usize;
    fn chc_rs_off_chc_alloc__realloc() -> usize;
    fn chc_rs_off_chc_alloc__free() -> usize;

    fn chc_rs_size_chc_io() -> usize;
    fn chc_rs_align_chc_io() -> usize;
    fn chc_rs_off_chc_io__ud() -> usize;
    fn chc_rs_off_chc_io__read() -> usize;
    fn chc_rs_off_chc_io__write() -> usize;
    fn chc_rs_off_chc_io__check_cancel() -> usize;

    fn chc_rs_size_chc_posix_io() -> usize;
    fn chc_rs_align_chc_posix_io() -> usize;
    fn chc_rs_off_chc_posix_io__fd() -> usize;
    fn chc_rs_off_chc_posix_io__check_cancel() -> usize;
    fn chc_rs_off_chc_posix_io__cancel_ud() -> usize;
    fn chc_rs_off_chc_posix_io__deadline_us() -> usize;

    fn chc_rs_size_chc_block_opts() -> usize;
    fn chc_rs_align_chc_block_opts() -> usize;
    fn chc_rs_off_chc_block_opts__has_block_info() -> usize;
    fn chc_rs_off_chc_block_opts__has_custom_serialization() -> usize;
    fn chc_rs_off_chc_block_opts__read_buffer_bytes() -> usize;

    fn chc_rs_size_chc_codec() -> usize;
    fn chc_rs_align_chc_codec() -> usize;
    fn chc_rs_off_chc_codec__ud() -> usize;
    fn chc_rs_off_chc_codec__lz4_compress() -> usize;
    fn chc_rs_off_chc_codec__lz4_decompress() -> usize;
    fn chc_rs_off_chc_codec__zstd_compress() -> usize;
    fn chc_rs_off_chc_codec__zstd_decompress() -> usize;
    fn chc_rs_off_chc_codec__lz4_bound() -> usize;
    fn chc_rs_off_chc_codec__zstd_bound() -> usize;

    fn chc_rs_size_chc_client_opts() -> usize;
    fn chc_rs_align_chc_client_opts() -> usize;
    fn chc_rs_off_chc_client_opts__client_name() -> usize;
    fn chc_rs_off_chc_client_opts__client_version_major() -> usize;
    fn chc_rs_off_chc_client_opts__client_version_minor() -> usize;
    fn chc_rs_off_chc_client_opts__client_version_patch() -> usize;
    fn chc_rs_off_chc_client_opts__client_revision() -> usize;
    fn chc_rs_off_chc_client_opts__database() -> usize;
    fn chc_rs_off_chc_client_opts__user() -> usize;
    fn chc_rs_off_chc_client_opts__password() -> usize;
    fn chc_rs_off_chc_client_opts__compression() -> usize;
    fn chc_rs_off_chc_client_opts__codec() -> usize;
    fn chc_rs_off_chc_client_opts__read_buffer_bytes() -> usize;

    fn chc_rs_size_chc_server_info() -> usize;
    fn chc_rs_align_chc_server_info() -> usize;
    fn chc_rs_off_chc_server_info__name() -> usize;
    fn chc_rs_off_chc_server_info__timezone() -> usize;
    fn chc_rs_off_chc_server_info__display_name() -> usize;
    fn chc_rs_off_chc_server_info__version_major() -> usize;
    fn chc_rs_off_chc_server_info__version_minor() -> usize;
    fn chc_rs_off_chc_server_info__version_patch() -> usize;
    fn chc_rs_off_chc_server_info__revision() -> usize;

    fn chc_rs_size_chc_exception() -> usize;
    fn chc_rs_align_chc_exception() -> usize;
    fn chc_rs_off_chc_exception__code() -> usize;
    fn chc_rs_off_chc_exception__name() -> usize;
    fn chc_rs_off_chc_exception__name_len() -> usize;
    fn chc_rs_off_chc_exception__display_text() -> usize;
    fn chc_rs_off_chc_exception__display_text_len() -> usize;
    fn chc_rs_off_chc_exception__stack_trace() -> usize;
    fn chc_rs_off_chc_exception__stack_trace_len() -> usize;

    fn chc_rs_size_chc_query_setting() -> usize;
    fn chc_rs_align_chc_query_setting() -> usize;
    fn chc_rs_off_chc_query_setting__name() -> usize;
    fn chc_rs_off_chc_query_setting__value() -> usize;
    fn chc_rs_off_chc_query_setting__important() -> usize;
    fn chc_rs_off_chc_query_setting__custom() -> usize;

    fn chc_rs_size_chc_query_param() -> usize;
    fn chc_rs_align_chc_query_param() -> usize;
    fn chc_rs_off_chc_query_param__name() -> usize;
    fn chc_rs_off_chc_query_param__value() -> usize;

    fn chc_rs_size_chc_query_opts() -> usize;
    fn chc_rs_align_chc_query_opts() -> usize;
    fn chc_rs_off_chc_query_opts__query_id() -> usize;
    fn chc_rs_off_chc_query_opts__query_id_len() -> usize;
    fn chc_rs_off_chc_query_opts__settings() -> usize;
    fn chc_rs_off_chc_query_opts__n_settings() -> usize;
    fn chc_rs_off_chc_query_opts__params() -> usize;
    fn chc_rs_off_chc_query_opts__n_params() -> usize;

    fn chc_rs_size_chc_packet() -> usize;
    fn chc_rs_align_chc_packet() -> usize;
    fn chc_rs_off_chc_packet__kind() -> usize;
    fn chc_rs_off_chc_packet__payload() -> usize;

    fn chc_rs_size_progress() -> usize;
    fn chc_rs_off_progress_rows() -> usize;
    fn chc_rs_off_progress_bytes() -> usize;
    fn chc_rs_off_progress_total_rows() -> usize;
    fn chc_rs_off_progress_written_rows() -> usize;
    fn chc_rs_off_progress_written_bytes() -> usize;

    fn chc_rs_size_profile() -> usize;
    fn chc_rs_off_profile_rows() -> usize;
    fn chc_rs_off_profile_blocks() -> usize;
    fn chc_rs_off_profile_bytes() -> usize;
    fn chc_rs_off_profile_rows_before_limit() -> usize;
    fn chc_rs_off_profile_applied_limit() -> usize;
    fn chc_rs_off_profile_calc_rows() -> usize;
}

macro_rules! layout {
    ($ty:ty, $size:ident, $align:ident) => {{
        assert_eq!(
            size_of::<$ty>(),
            unsafe { $size() },
            concat!("size_of ", stringify!($ty)),
        );
        assert_eq!(
            align_of::<$ty>(),
            unsafe { $align() },
            concat!("align_of ", stringify!($ty)),
        );
    }};
}

macro_rules! field {
    ($ty:ty, $field:ident, $off:ident) => {{
        assert_eq!(
            offset_of!($ty, $field),
            unsafe { $off() },
            concat!("offset_of ", stringify!($ty), ".", stringify!($field)),
        );
    }};
}

#[test]
fn chc_err_matches_c() {
    layout!(sys::chc_err, chc_rs_size_chc_err, chc_rs_align_chc_err);
    field!(sys::chc_err, server_code, chc_rs_off_chc_err__server_code);
    field!(sys::chc_err, msg, chc_rs_off_chc_err__msg);
    field!(sys::chc_err, server_name, chc_rs_off_chc_err__server_name);
}

#[test]
fn chc_alloc_matches_c() {
    layout!(
        sys::chc_alloc,
        chc_rs_size_chc_alloc,
        chc_rs_align_chc_alloc
    );
    field!(sys::chc_alloc, ud, chc_rs_off_chc_alloc__ud);
    field!(sys::chc_alloc, alloc, chc_rs_off_chc_alloc__alloc);
    field!(sys::chc_alloc, realloc, chc_rs_off_chc_alloc__realloc);
    field!(sys::chc_alloc, free, chc_rs_off_chc_alloc__free);
}

#[test]
fn chc_io_matches_c() {
    layout!(sys::chc_io, chc_rs_size_chc_io, chc_rs_align_chc_io);
    field!(sys::chc_io, ud, chc_rs_off_chc_io__ud);
    field!(sys::chc_io, read, chc_rs_off_chc_io__read);
    field!(sys::chc_io, write, chc_rs_off_chc_io__write);
    field!(sys::chc_io, check_cancel, chc_rs_off_chc_io__check_cancel);
}

#[test]
fn chc_posix_io_matches_c() {
    layout!(
        sys::chc_posix_io,
        chc_rs_size_chc_posix_io,
        chc_rs_align_chc_posix_io
    );
    field!(sys::chc_posix_io, fd, chc_rs_off_chc_posix_io__fd);
    field!(
        sys::chc_posix_io,
        check_cancel,
        chc_rs_off_chc_posix_io__check_cancel
    );
    field!(
        sys::chc_posix_io,
        cancel_ud,
        chc_rs_off_chc_posix_io__cancel_ud
    );
    field!(
        sys::chc_posix_io,
        deadline_us,
        chc_rs_off_chc_posix_io__deadline_us
    );
}

#[test]
fn chc_block_opts_matches_c() {
    layout!(
        sys::chc_block_opts,
        chc_rs_size_chc_block_opts,
        chc_rs_align_chc_block_opts
    );
    field!(
        sys::chc_block_opts,
        has_block_info,
        chc_rs_off_chc_block_opts__has_block_info
    );
    field!(
        sys::chc_block_opts,
        has_custom_serialization,
        chc_rs_off_chc_block_opts__has_custom_serialization
    );
    field!(
        sys::chc_block_opts,
        read_buffer_bytes,
        chc_rs_off_chc_block_opts__read_buffer_bytes
    );
}

#[test]
fn chc_codec_matches_c() {
    layout!(
        sys::chc_codec,
        chc_rs_size_chc_codec,
        chc_rs_align_chc_codec
    );
    field!(sys::chc_codec, ud, chc_rs_off_chc_codec__ud);
    field!(
        sys::chc_codec,
        lz4_compress,
        chc_rs_off_chc_codec__lz4_compress
    );
    field!(
        sys::chc_codec,
        lz4_decompress,
        chc_rs_off_chc_codec__lz4_decompress
    );
    field!(
        sys::chc_codec,
        zstd_compress,
        chc_rs_off_chc_codec__zstd_compress
    );
    field!(
        sys::chc_codec,
        zstd_decompress,
        chc_rs_off_chc_codec__zstd_decompress
    );
    field!(sys::chc_codec, lz4_bound, chc_rs_off_chc_codec__lz4_bound);
    field!(sys::chc_codec, zstd_bound, chc_rs_off_chc_codec__zstd_bound);
}

#[test]
fn chc_client_opts_matches_c() {
    layout!(
        sys::chc_client_opts,
        chc_rs_size_chc_client_opts,
        chc_rs_align_chc_client_opts
    );
    field!(
        sys::chc_client_opts,
        client_name,
        chc_rs_off_chc_client_opts__client_name
    );
    field!(
        sys::chc_client_opts,
        client_version_major,
        chc_rs_off_chc_client_opts__client_version_major
    );
    field!(
        sys::chc_client_opts,
        client_version_minor,
        chc_rs_off_chc_client_opts__client_version_minor
    );
    field!(
        sys::chc_client_opts,
        client_version_patch,
        chc_rs_off_chc_client_opts__client_version_patch
    );
    field!(
        sys::chc_client_opts,
        client_revision,
        chc_rs_off_chc_client_opts__client_revision
    );
    field!(
        sys::chc_client_opts,
        database,
        chc_rs_off_chc_client_opts__database
    );
    field!(sys::chc_client_opts, user, chc_rs_off_chc_client_opts__user);
    field!(
        sys::chc_client_opts,
        password,
        chc_rs_off_chc_client_opts__password
    );
    field!(
        sys::chc_client_opts,
        compression,
        chc_rs_off_chc_client_opts__compression
    );
    field!(
        sys::chc_client_opts,
        codec,
        chc_rs_off_chc_client_opts__codec
    );
    field!(
        sys::chc_client_opts,
        read_buffer_bytes,
        chc_rs_off_chc_client_opts__read_buffer_bytes
    );
}

#[test]
fn chc_server_info_matches_c() {
    layout!(
        sys::chc_server_info,
        chc_rs_size_chc_server_info,
        chc_rs_align_chc_server_info
    );
    field!(sys::chc_server_info, name, chc_rs_off_chc_server_info__name);
    field!(
        sys::chc_server_info,
        timezone,
        chc_rs_off_chc_server_info__timezone
    );
    field!(
        sys::chc_server_info,
        display_name,
        chc_rs_off_chc_server_info__display_name
    );
    field!(
        sys::chc_server_info,
        version_major,
        chc_rs_off_chc_server_info__version_major
    );
    field!(
        sys::chc_server_info,
        version_minor,
        chc_rs_off_chc_server_info__version_minor
    );
    field!(
        sys::chc_server_info,
        version_patch,
        chc_rs_off_chc_server_info__version_patch
    );
    field!(
        sys::chc_server_info,
        revision,
        chc_rs_off_chc_server_info__revision
    );
}

#[test]
fn chc_exception_matches_c() {
    layout!(
        sys::chc_exception,
        chc_rs_size_chc_exception,
        chc_rs_align_chc_exception
    );
    field!(sys::chc_exception, code, chc_rs_off_chc_exception__code);
    field!(sys::chc_exception, name, chc_rs_off_chc_exception__name);
    field!(
        sys::chc_exception,
        name_len,
        chc_rs_off_chc_exception__name_len
    );
    field!(
        sys::chc_exception,
        display_text,
        chc_rs_off_chc_exception__display_text
    );
    field!(
        sys::chc_exception,
        display_text_len,
        chc_rs_off_chc_exception__display_text_len
    );
    field!(
        sys::chc_exception,
        stack_trace,
        chc_rs_off_chc_exception__stack_trace
    );
    field!(
        sys::chc_exception,
        stack_trace_len,
        chc_rs_off_chc_exception__stack_trace_len
    );
}

#[test]
fn chc_query_setting_matches_c() {
    layout!(
        sys::chc_query_setting,
        chc_rs_size_chc_query_setting,
        chc_rs_align_chc_query_setting
    );
    field!(
        sys::chc_query_setting,
        name,
        chc_rs_off_chc_query_setting__name
    );
    field!(
        sys::chc_query_setting,
        value,
        chc_rs_off_chc_query_setting__value
    );
    field!(
        sys::chc_query_setting,
        important,
        chc_rs_off_chc_query_setting__important
    );
    field!(
        sys::chc_query_setting,
        custom,
        chc_rs_off_chc_query_setting__custom
    );
}

#[test]
fn chc_query_param_matches_c() {
    layout!(
        sys::chc_query_param,
        chc_rs_size_chc_query_param,
        chc_rs_align_chc_query_param
    );
    field!(sys::chc_query_param, name, chc_rs_off_chc_query_param__name);
    field!(
        sys::chc_query_param,
        value,
        chc_rs_off_chc_query_param__value
    );
}

#[test]
fn chc_query_opts_matches_c() {
    layout!(
        sys::chc_query_opts,
        chc_rs_size_chc_query_opts,
        chc_rs_align_chc_query_opts
    );
    field!(
        sys::chc_query_opts,
        query_id,
        chc_rs_off_chc_query_opts__query_id
    );
    field!(
        sys::chc_query_opts,
        query_id_len,
        chc_rs_off_chc_query_opts__query_id_len
    );
    field!(
        sys::chc_query_opts,
        settings,
        chc_rs_off_chc_query_opts__settings
    );
    field!(
        sys::chc_query_opts,
        n_settings,
        chc_rs_off_chc_query_opts__n_settings
    );
    field!(
        sys::chc_query_opts,
        params,
        chc_rs_off_chc_query_opts__params
    );
    field!(
        sys::chc_query_opts,
        n_params,
        chc_rs_off_chc_query_opts__n_params
    );
}

#[test]
fn chc_packet_matches_c() {
    // Outer packet: kind + the union. Matching size, align, and union
    // offset pin the (anonymous, unnamed-in-C) union's size transitively;
    // the arm structs below verify the union's payload layout directly.
    layout!(
        sys::chc_packet,
        chc_rs_size_chc_packet,
        chc_rs_align_chc_packet
    );
    field!(sys::chc_packet, kind, chc_rs_off_chc_packet__kind);
    field!(sys::chc_packet, payload, chc_rs_off_chc_packet__payload);

    let payload = offset_of!(sys::chc_packet, payload);

    assert_eq!(
        size_of::<sys::chc_packet_progress>(),
        unsafe { chc_rs_size_progress() },
        "size_of chc_packet_progress",
    );
    let progress = [
        (
            offset_of!(sys::chc_packet_progress, rows),
            unsafe { chc_rs_off_progress_rows() },
            "rows",
        ),
        (
            offset_of!(sys::chc_packet_progress, bytes),
            unsafe { chc_rs_off_progress_bytes() },
            "bytes",
        ),
        (
            offset_of!(sys::chc_packet_progress, total_rows),
            unsafe { chc_rs_off_progress_total_rows() },
            "total_rows",
        ),
        (
            offset_of!(sys::chc_packet_progress, written_rows),
            unsafe { chc_rs_off_progress_written_rows() },
            "written_rows",
        ),
        (
            offset_of!(sys::chc_packet_progress, written_bytes),
            unsafe { chc_rs_off_progress_written_bytes() },
            "written_bytes",
        ),
    ];
    for (rust_in_arm, c_abs, name) in progress {
        assert_eq!(payload + rust_in_arm, c_abs, "chc_packet_progress.{name}");
    }

    assert_eq!(
        size_of::<sys::chc_packet_profile>(),
        unsafe { chc_rs_size_profile() },
        "size_of chc_packet_profile",
    );
    let profile = [
        (
            offset_of!(sys::chc_packet_profile, rows),
            unsafe { chc_rs_off_profile_rows() },
            "rows",
        ),
        (
            offset_of!(sys::chc_packet_profile, blocks),
            unsafe { chc_rs_off_profile_blocks() },
            "blocks",
        ),
        (
            offset_of!(sys::chc_packet_profile, bytes),
            unsafe { chc_rs_off_profile_bytes() },
            "bytes",
        ),
        (
            offset_of!(sys::chc_packet_profile, rows_before_limit),
            unsafe { chc_rs_off_profile_rows_before_limit() },
            "rows_before_limit",
        ),
        (
            offset_of!(sys::chc_packet_profile, applied_limit),
            unsafe { chc_rs_off_profile_applied_limit() },
            "applied_limit",
        ),
        (
            offset_of!(sys::chc_packet_profile, calculated_rows_before_limit),
            unsafe { chc_rs_off_profile_calc_rows() },
            "calculated_rows_before_limit",
        ),
    ];
    for (rust_in_arm, c_abs, name) in profile {
        assert_eq!(payload + rust_in_arm, c_abs, "chc_packet_profile.{name}");
    }
}
