//! Malformed bridge requests, byte by byte.
//!
//! The daemon's client cannot produce these frames, so they need a raw socket.
//! Each one must come back as one well-formed error frame on a connection the
//! worker keeps serving, with the worker's own process untouched

#[path = "common/pgext.rs"]
mod pgext;
#[path = "common/ports.rs"]
mod ports;

use std::os::unix::net::UnixStream;
use std::time::Duration;

use pgext::{Cluster, error_of, hello};

const OP_ENCODE_NATIVE: u8 = 0x02;
const OP_SCAN: u8 = 0x03;
const CELL_DEFAULT: u8 = 0x00;
const CELL_DISK_RAW: u8 = 0x01;
const CELL_LITERAL: u8 = 0x03;
/// `pg_class`, the one catalog id every scan case below uses
const CAT_CLASS: u8 = 1;

fn lenstr(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    out.extend_from_slice(s);
}

/// `rows`, `ncols`, then one declaration per column
fn native_head(rows: u32, ncols: u32) -> Vec<u8> {
    let mut out = vec![OP_ENCODE_NATIVE];
    out.extend_from_slice(&rows.to_be_bytes());
    out.extend_from_slice(&ncols.to_be_bytes());
    out
}

fn declare(out: &mut Vec<u8>, oid: u32, typmod: i32, name: &str, target: &str) {
    out.extend_from_slice(&oid.to_be_bytes());
    out.extend_from_slice(&typmod.to_be_bytes());
    lenstr(out, name.as_bytes());
    lenstr(out, target.as_bytes());
}

/// An absent value is the tag alone: nothing follows it on the wire
fn cell_default(out: &mut Vec<u8>) {
    out.push(CELL_DEFAULT);
}

fn cell(out: &mut Vec<u8>, tag: u8, body: &[u8]) {
    out.push(tag);
    lenstr(out, body);
}

/// Declared length with no bytes behind it
fn cell_overlong(out: &mut Vec<u8>, tag: u8, len: u32) {
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
}

/// Catalog, top xid, parked replay boundary, then the oid list and its count
fn scan(cat: u8, top: u32, oids: &[u32]) -> Vec<u8> {
    let mut out = vec![OP_SCAN, cat];
    out.extend_from_slice(&top.to_be_bytes());
    out.extend_from_slice(&0u64.to_be_bytes());
    out.extend_from_slice(&(oids.len() as u32).to_be_bytes());
    for oid in oids {
        out.extend_from_slice(&oid.to_be_bytes());
    }
    out
}

fn open(tmp: &std::path::Path) -> (Cluster, UnixStream) {
    let mut pg = pgext::stage(tmp, ports::PG_SHADOW_PORT, Duration::from_secs(30));
    pg.start(&[]);
    pg.wait_log(0, "walshadow bridge listening");
    let sock = pgext::hello_on(&pg.bridge_path());
    (pg, sock)
}

#[test]
fn scan_rejects_arguments_it_cannot_serve() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let (pg, mut sock) = open(tmp.path());
    let worker = pg.worker_pid().expect("worker running");

    let msg = error_of(&mut sock, &scan(99, 0, &[]));
    assert!(msg.contains("unknown walshadow catalog id 99"), "{msg}");

    // Oid list ceiling, checked before anything is allocated for it
    let mut over = vec![OP_SCAN, CAT_CLASS];
    over.extend_from_slice(&0u32.to_be_bytes());
    over.extend_from_slice(&0u64.to_be_bytes());
    over.extend_from_slice(&65537u32.to_be_bytes());
    let msg = error_of(&mut sock, &over);
    assert!(msg.contains("65537 exceeds 65536"), "{msg}");

    // Oid list shorter than its own count
    let mut truncated = scan(CAT_CLASS, 0, &[1259]);
    truncated[14..18].copy_from_slice(&2u32.to_be_bytes());
    let msg = error_of(&mut sock, &truncated);
    assert!(msg.contains("insufficient data"), "{msg}");

    // Same connection, after three aborted transactions
    let body = pgext::request(&mut sock, &scan(CAT_CLASS, 0, &[]));
    assert_eq!(body[0], 0, "{:?}", &body[..body.len().min(32)]);
    hello(&mut sock);
    assert_eq!(pg.worker_pid(), Some(worker), "an error cost the worker");
}

#[test]
fn native_rejects_frames_it_cannot_read() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let (pg, mut sock) = open(tmp.path());
    let worker = pg.worker_pid().expect("worker running");
    let int4: u32 = pg.sql("SELECT 'int4'::regtype::oid::text").parse().unwrap();

    // No rows and no columns is not a batch
    let mut req = native_head(0, 1);
    declare(&mut req, int4, -1, "c", "Int32");
    cell_default(&mut req);
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("has 0 rows and 1 columns"), "{msg}");

    // Declared cells the frame cannot possibly carry
    let mut req = native_head(1_000_000, 1);
    declare(&mut req, int4, -1, "c", "Int32");
    cell_default(&mut req);
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("declares 1 x 1000000 cells"), "{msg}");

    // A string length past the end of the frame, before any column exists.
    // Padded so the frame clears the declared-cell floor first
    let mut req = native_head(1, 1);
    req.extend_from_slice(&int4.to_be_bytes());
    req.extend_from_slice(&(-1i32).to_be_bytes());
    req.extend_from_slice(&999u32.to_be_bytes());
    req.extend_from_slice(&[0; 8]);
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("string length 999 past end"), "{msg}");

    // A column has to name itself and its target
    let mut req = native_head(1, 1);
    declare(&mut req, int4, -1, "", "Int32");
    cell_default(&mut req);
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("empty name or type"), "{msg}");

    let mut req = native_head(1, 1);
    declare(&mut req, int4, -1, "c", "NotAClickHouseType");
    cell_default(&mut req);
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("target type:"), "{msg}");

    let mut req = native_head(1, 1);
    declare(&mut req, int4, -1, "c", "Int32");
    cell(&mut req, 0x09, b"");
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("unknown walshadow cell tag 9"), "{msg}");

    // Only a literal bypasses the source type, so the others need one
    let mut req = native_head(1, 1);
    declare(&mut req, 0, -1, "c", "String");
    cell(&mut req, CELL_DISK_RAW, b"x");
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("declares no source type"), "{msg}");

    let mut req = native_head(1, 1);
    declare(&mut req, int4, -1, "c", "Int32");
    cell_overlong(&mut req, CELL_DISK_RAW, 999);
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("cell length 999 past end"), "{msg}");

    // A target type with no zero value cannot answer for an absent cell
    let mut req = native_head(1, 1);
    declare(&mut req, int4, -1, "c", "Int32");
    cell_default(&mut req);
    let msg = error_of(&mut sock, &req);
    assert!(
        msg.contains("no default value for ClickHouse type"),
        "{msg}"
    );

    hello(&mut sock);
    assert_eq!(pg.worker_pid(), Some(worker), "an error cost the worker");
}

/// A cstring source is the one typlen the on-disk rebuild has to terminate
/// itself, since nothing on the wire carries the trailing NUL
#[test]
fn native_reconstructs_cstring_bodies() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let (pg, mut sock) = open(tmp.path());
    let cstring: u32 = pg
        .sql("SELECT 'cstring'::regtype::oid::text")
        .parse()
        .unwrap();

    let mut req = native_head(1, 1);
    declare(&mut req, cstring, -1, "c", "String");
    cell(&mut req, CELL_DISK_RAW, b"unterminated");
    let body = pgext::request(&mut sock, &req);
    assert_eq!(body[0], 0, "{:?}", &body[..body.len().min(32)]);
    assert!(
        body.windows(12).any(|w| w == b"unterminated"),
        "block carried no cstring body: {body:?}"
    );
    hello(&mut sock);
}

/// The response cap is the daemon's frame ceiling. A `FixedString` column pads
/// every row to its declared width, so a frame this small can ask for a block
/// past the cap
#[test]
fn native_refuses_a_block_past_the_response_cap() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let (pg, mut sock) = open(tmp.path());
    let worker = pg.worker_pid().expect("worker running");

    // ClickHouse caps FixedString at 0xffffff, so 17 rows is the smallest
    // whole number of them past a 256 MiB response
    let mut req = native_head(17, 1);
    declare(&mut req, 0, -1, "c", "FixedString(16777215)");
    for _ in 0..17 {
        cell(&mut req, CELL_LITERAL, b"x");
    }
    let msg = error_of(&mut sock, &req);
    assert!(msg.contains("block write:"), "{msg}");
    assert!(msg.contains("byte response cap"), "{msg}");

    // Request memory went back with the request
    hello(&mut sock);
    assert_eq!(pg.worker_pid(), Some(worker), "the cap cost the worker");
}

/// Wrappers the encoder looks through before it can decide anything. Both
/// columns carry a source type, which is what puts the expander search in
/// front of them: a `Map` from a type no extension owns has no expander to
/// find, and a `LowCardinality` is a wrapper the search has to unwrap first
#[test]
fn native_looks_through_target_wrappers() {
    if !pgext::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let (pg, mut sock) = open(tmp.path());
    let int4: u32 = pg.sql("SELECT 'int4'::regtype::oid::text").parse().unwrap();

    let mut req = native_head(1, 2);
    declare(&mut req, int4, -1, "m", "Map(String, String)");
    declare(&mut req, int4, -1, "lc", "LowCardinality(String)");
    cell_default(&mut req);
    cell(&mut req, CELL_LITERAL, b"dictionary");
    let body = pgext::request(&mut sock, &req);
    assert_eq!(body[0], 0, "{:?}", &body[..body.len().min(32)]);
    assert!(
        body.windows(10).any(|w| w == b"dictionary"),
        "block carried no dictionary entry: {body:?}"
    );
    hello(&mut sock);
}
