/*
 * clickhouse-compression.h -- compressed-frame layout, CityHash128, codec
 * vtable, and ready-made LZ4 / ZSTD chc_codec wrappers.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * the implementation uses static helpers from clickhouse.h.
 *
 * LZ4 wrapper (chc_lz4_codec_init) is compiled in by default & pulls
 * <lz4.h>; link -llz4. Define CHC_NO_LZ4 before including to opt out
 * (drops the include & the wrapper).
 *
 * ZSTD wrapper (chc_zstd_codec_init) is compiled in by default & pulls
 * <zstd.h>; link -lzstd. Define CHC_NO_ZSTD before including to opt out.
 *
 * Frame layout (matches ClickHouse server / clickhouse-cpp
 * base/compressed.cpp):
 *
 *   [ 16 B CityHash128 of the rest of the frame                ]
 *   [  1 B method (0x82 LZ4, 0x90 ZSTD, 0x02 none)              ]
 *   [  4 B LE: compressed_size_with_header (= 9 + payload bytes)]
 *   [  4 B LE: original_size                                    ]
 *   [    payload                                                ]
 *
 * The CityHash128 covers the method byte, the two size fields, and the
 * payload bytes -- i.e. everything starting at offset 16.
 */

#ifndef CLICKHOUSE_COMPRESSION_H
#define CLICKHOUSE_COMPRESSION_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "clickhouse.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Compression mode for client Data packets. Shared between
 * clickhouse-client.h and clickhouse-compression.h so consumers can
 * include the latter in isolation if they're driving the frame format
 * directly. */
typedef enum chc_compression {
    CHC_COMP_NONE = 0,
    CHC_COMP_LZ4  = 1,
    CHC_COMP_ZSTD = 2,
} chc_compression;

typedef struct chc_codec chc_codec;

struct chc_codec {
    void *ud;

    /* Compress src[0..src_len] into dst[0..dst_cap]. On success returns
     * CHC_OK and writes compressed-size into *dst_n. On insufficient
     * destination capacity returns CHC_ERR_OOM. */
    int (*lz4_compress)  (void *ud, const void *src, size_t src_len,
                          void *dst, size_t dst_cap, size_t *dst_n,
                          chc_err *err);
    /* original_size is the known uncompressed payload length. */
    int (*lz4_decompress)(void *ud, const void *src, size_t src_len,
                          void *dst, size_t original_size,
                          chc_err *err);

    int (*zstd_compress)  (void *ud, const void *src, size_t src_len,
                           void *dst, size_t dst_cap, size_t *dst_n,
                           chc_err *err);
    int (*zstd_decompress)(void *ud, const void *src, size_t src_len,
                           void *dst, size_t original_size,
                           chc_err *err);

    /* Worst-case compressed size for a payload of src_len bytes. May be
     * NULL; chc__compress_emit_frame() falls back to src_len + 256 +
     * src_len/255 in that case (LZ4's classic bound formula). */
    size_t (*lz4_bound) (size_t src_len);
    size_t (*zstd_bound)(size_t src_len);
};

/* Default chunk size for outgoing frames (matches clickhouse-cpp). */
#define CHC_COMPRESS_MAX_CHUNK 65535u

/* Internal-but-exported: 128-bit CityHash. Returned as the two 64-bit
 * halves the wire-format encodes (lo first, hi second). Frozen variant
 * matching CH server / clickhouse-cpp. */
void chc_cityhash128(const void *data, size_t len,
                     uint64_t *out_lo, uint64_t *out_hi);

#ifndef CHC_NO_LZ4
/* Fill lz4_* slots of `out` with wrappers around <lz4.h>. Leaves zstd_*
 * untouched, so callers wanting both codecs can call both init helpers
 * on the same struct in either order. Caller links -llz4. */
void chc_lz4_codec_init(chc_codec *out);
#endif

#ifndef CHC_NO_ZSTD
/* Fill zstd_* slots of `out` with wrappers around <zstd.h>. Leaves lz4_*
 * untouched. Caller links -lzstd. */
void chc_zstd_codec_init(chc_codec *out);
#endif

#ifdef CHC_IMPLEMENTATION

#include <string.h>

/* ============================================================
 * CityHash128 (frozen v1.0.3 variant, ported from city.cc).
 * Original: Copyright (c) 2011 Google, Inc. (MIT licence).
 * Short-string helpers live in clickhouse.h; this block layers
 * the 128-bit driver on top.
 * ============================================================ */

static uint64_t chc__city_rotate(uint64_t v, int s)
{
    return s == 0 ? v : ((v >> s) | (v << (64 - s)));
}

typedef struct { uint64_t a, b; } chc__u128;

static chc__u128
chc__city_weak_seeds(uint64_t w, uint64_t x, uint64_t y, uint64_t z,
                     uint64_t a, uint64_t b)
{
    a += w;
    b = chc__city_rotate(b + a + z, 21);
    uint64_t c = a;
    a += x;
    a += y;
    b += chc__city_rotate(a, 44);
    chc__u128 r = { a + z, b + c };
    return r;
}

static chc__u128 chc__city_weak_s(const char *s, uint64_t a, uint64_t b)
{
    return chc__city_weak_seeds(chc__city_fetch64(s),
                                chc__city_fetch64(s + 8),
                                chc__city_fetch64(s + 16),
                                chc__city_fetch64(s + 24),
                                a, b);
}

static chc__u128
chc__city_murmur(const char *s, size_t len, chc__u128 seed)
{
    uint64_t a = seed.a;
    uint64_t b = seed.b;
    uint64_t c = 0;
    uint64_t d = 0;
    if (len <= 16) {
        a = chc__city_shift_mix(a * chc__city_k1) * chc__city_k1;
        c = b * chc__city_k1 + chc__city_hash_len_0_to_16(s, len);
        d = chc__city_shift_mix(a + (len >= 8 ? chc__city_fetch64(s) : c));
    } else {
        c = chc__city_hash_len_16(chc__city_fetch64(s + len - 8) + chc__city_k1, a);
        d = chc__city_hash_len_16(b + len, c + chc__city_fetch64(s + len - 16));
        a += d;
        do {
            a ^= chc__city_shift_mix(chc__city_fetch64(s)     * chc__city_k1) * chc__city_k1;
            a *= chc__city_k1;
            b ^= a;
            c ^= chc__city_shift_mix(chc__city_fetch64(s + 8) * chc__city_k1) * chc__city_k1;
            c *= chc__city_k1;
            d ^= c;
            s   += 16;
            len -= 16;
        } while (len > 16);
    }
    a = chc__city_hash_len_16(a, c);
    b = chc__city_hash_len_16(d, b);
    chc__u128 r = { a ^ b, chc__city_hash_len_16(b, a) };
    return r;
}

static chc__u128
chc__city_hash128_with_seed(const char *s, size_t len, chc__u128 seed)
{
    if (len < 128) return chc__city_murmur(s, len, seed);

    chc__u128 v = {0}, w = {0};
    uint64_t x = seed.a, y = seed.b;
    uint64_t z = len * chc__city_k1;
    v.a = chc__city_rotate(y ^ chc__city_k1, 49) * chc__city_k1 + chc__city_fetch64(s);
    v.b = chc__city_rotate(v.a, 42) * chc__city_k1 + chc__city_fetch64(s + 8);
    w.a = chc__city_rotate(y + z, 35) * chc__city_k1 + x;
    w.b = chc__city_rotate(x + chc__city_fetch64(s + 88), 53) * chc__city_k1;

    do {
        x = chc__city_rotate(x + y + v.a + chc__city_fetch64(s + 16), 37) * chc__city_k1;
        y = chc__city_rotate(y + v.b + chc__city_fetch64(s + 48), 42) * chc__city_k1;
        x ^= w.b; y ^= v.a;
        z = chc__city_rotate(z ^ w.a, 33);
        v = chc__city_weak_s(s,      v.b * chc__city_k1, x + w.a);
        w = chc__city_weak_s(s + 32, z + w.b, y);
        { uint64_t tmp = z; z = x; x = tmp; }
        s += 64;
        x = chc__city_rotate(x + y + v.a + chc__city_fetch64(s + 16), 37) * chc__city_k1;
        y = chc__city_rotate(y + v.b + chc__city_fetch64(s + 48), 42) * chc__city_k1;
        x ^= w.b; y ^= v.a;
        z = chc__city_rotate(z ^ w.a, 33);
        v = chc__city_weak_s(s,      v.b * chc__city_k1, x + w.a);
        w = chc__city_weak_s(s + 32, z + w.b, y);
        { uint64_t tmp = z; z = x; x = tmp; }
        s += 64;
        len -= 128;
    } while (len >= 128);

    y += chc__city_rotate(w.a, 37) * chc__city_k0 + z;
    x += chc__city_rotate(v.a + z, 49) * chc__city_k0;
    for (size_t td = 0; td < len; ) {
        td += 32;
        y = chc__city_rotate(y - x, 42) * chc__city_k0 + v.b;
        w.a += chc__city_fetch64(s + len - td + 16);
        x = chc__city_rotate(x, 49) * chc__city_k0 + w.a;
        w.a += v.a;
        v = chc__city_weak_s(s + len - td, v.a, v.b);
    }
    x = chc__city_hash_len_16(x, v.a);
    y = chc__city_hash_len_16(y, w.a);
    chc__u128 r = { chc__city_hash_len_16(x + v.b, w.b) + y,
                    chc__city_hash_len_16(x + w.b, y + v.b) };
    return r;
}

static chc__u128 chc__city_hash128_impl(const char *s, size_t len)
{
    chc__u128 seed;
    if (len >= 16) {
        seed.a = chc__city_fetch64(s) ^ chc__city_k3;
        seed.b = chc__city_fetch64(s + 8);
        return chc__city_hash128_with_seed(s + 16, len - 16, seed);
    } else if (len >= 8) {
        seed.a = chc__city_fetch64(s) ^ (len * chc__city_k0);
        seed.b = chc__city_fetch64(s + len - 8) ^ chc__city_k1;
        return chc__city_hash128_with_seed(NULL, 0, seed);
    } else {
        seed.a = chc__city_k0;
        seed.b = chc__city_k1;
        return chc__city_hash128_with_seed(s, len, seed);
    }
}

void
chc_cityhash128(const void *data, size_t len,
                uint64_t *out_lo, uint64_t *out_hi)
{
    chc__u128 r = chc__city_hash128_impl((const char *) data, len);
    *out_lo = r.a;
    *out_hi = r.b;
}

/* ============================================================
 * Frame I/O
 * ============================================================ */

#define CHC__COMP_NONE 0x02u
#define CHC__COMP_LZ4  0x82u
#define CHC__COMP_ZSTD 0x90u
#define CHC__COMP_HEADER_BYTES 9u

/* lz4-style fallback bound. */
static size_t chc__comp_default_bound(size_t n) { return n + 256 + n / 255; }

/* Write one compressed frame to io. Caller has already supplied the
 * codec for the chosen method. */
static int
chc__comp_emit_frame(chc_io *io, const chc_codec *codec, chc_compression m,
                     const void *src, size_t src_len,
                     const chc_alloc *al, chc_err *err)
{
    int rc;
    uint8_t method;
    size_t bound;
    switch (m) {
    case CHC_COMP_LZ4:
        method = CHC__COMP_LZ4;
        bound = codec->lz4_bound ? codec->lz4_bound(src_len)
                                 : chc__comp_default_bound(src_len);
        break;
    case CHC_COMP_ZSTD:
        method = CHC__COMP_ZSTD;
        bound = codec->zstd_bound ? codec->zstd_bound(src_len)
                                  : chc__comp_default_bound(src_len);
        break;
    default:
        return chc__err_set(err, CHC_ERR_USAGE,
                            "chc__comp_emit_frame: unsupported method");
    }
    size_t buf_cap = CHC__COMP_HEADER_BYTES + bound;
    uint8_t *buf = chc__alloc(al, buf_cap, err);
    if (!buf) return CHC_ERR_OOM;

    size_t comp_sz = 0;
    if (m == CHC_COMP_LZ4) {
        rc = codec->lz4_compress(codec->ud, src, src_len,
                                 buf + CHC__COMP_HEADER_BYTES,
                                 buf_cap - CHC__COMP_HEADER_BYTES,
                                 &comp_sz, err);
    } else {
        rc = codec->zstd_compress(codec->ud, src, src_len,
                                  buf + CHC__COMP_HEADER_BYTES,
                                  buf_cap - CHC__COMP_HEADER_BYTES,
                                  &comp_sz, err);
    }
    if (rc != CHC_OK) { al->free(al->ud, buf, buf_cap); return rc; }

    uint32_t comp_with_hdr = (uint32_t) (comp_sz + CHC__COMP_HEADER_BYTES);
    uint32_t orig = (uint32_t) src_len;
    buf[0] = method;
    buf[1] = (uint8_t)  comp_with_hdr;
    buf[2] = (uint8_t) (comp_with_hdr >> 8);
    buf[3] = (uint8_t) (comp_with_hdr >> 16);
    buf[4] = (uint8_t) (comp_with_hdr >> 24);
    buf[5] = (uint8_t)  orig;
    buf[6] = (uint8_t) (orig >> 8);
    buf[7] = (uint8_t) (orig >> 16);
    buf[8] = (uint8_t) (orig >> 24);

    uint64_t lo, hi;
    chc_cityhash128(buf, CHC__COMP_HEADER_BYTES + comp_sz, &lo, &hi);

    rc = chc__write_u64_le(io, lo, err);
    if (rc == CHC_OK) rc = chc__write_u64_le(io, hi, err);
    if (rc == CHC_OK) rc = chc__write_bytes(io, buf,
                                            CHC__COMP_HEADER_BYTES + comp_sz, err);
    al->free(al->ud, buf, buf_cap);
    return rc;
}

/* Mem sink chc_io. Used to capture a block's bytes before compressing. */
typedef struct chc__mem_sink {
    uint8_t         *buf;
    size_t           cap;
    size_t           len;
    const chc_alloc *al;
    int              oom;
} chc__mem_sink;

static int
chc__mem_sink_write(void *ud, const void *p, size_t n, chc_err *err)
{
    chc__mem_sink *s = ud;
    if (s->oom) return chc__err_set(err, CHC_ERR_OOM, "mem sink oom");
    if (s->len + n > s->cap) {
        size_t new_cap = s->cap ? s->cap : 4096;
        while (new_cap < s->len + n) new_cap *= 2;
        uint8_t *nb = chc__realloc(s->al, s->buf, s->cap, new_cap, err);
        if (!nb) { s->oom = 1; return CHC_ERR_OOM; }
        s->buf = nb; s->cap = new_cap;
    }
    memcpy(s->buf + s->len, p, n);
    s->len += n;
    return CHC_OK;
}

static int chc__mem_sink_read(void *ud, void *b, size_t n, size_t *o, chc_err *e)
{ (void) ud; (void) b; (void) n; (void) o; (void) e; return -1; }

static void
chc__mem_sink_init(chc__mem_sink *s, chc_io *io, const chc_alloc *al)
{
    memset(s, 0, sizeof *s);
    s->al = al;
    io->ud           = s;
    io->read         = chc__mem_sink_read;
    io->write        = chc__mem_sink_write;
    io->check_cancel = NULL;
}

static void
chc__mem_sink_free(chc__mem_sink *s)
{
    if (s->buf) s->al->free(s->al->ud, s->buf, s->cap);
    s->buf = NULL; s->cap = 0; s->len = 0;
}

/* Emit a buffered block as one or more compressed frames to io. */
static int
chc__comp_emit_chunks(chc_io *io, const chc_codec *codec, chc_compression m,
                      const void *src, size_t src_len,
                      const chc_alloc *al, chc_err *err)
{
    const uint8_t *p = src;
    size_t left = src_len;
    while (left) {
        size_t take = left > CHC_COMPRESS_MAX_CHUNK
                          ? CHC_COMPRESS_MAX_CHUNK : left;
        int rc = chc__comp_emit_frame(io, codec, m, p, take, al, err);
        if (rc != CHC_OK) return rc;
        p    += take;
        left -= take;
    }
    return CHC_OK;
}

/* ---------- decompression side ---------- */

/* Adapter chc_io that produces decompressed bytes from a compressed
 * frame stream sourced from a chc__in. Each "frame" is read on demand;
 * we hold one frame's uncompressed bytes in scratch. */
typedef struct chc__decomp_src {
    chc__in         *raw;
    const chc_codec *codec;
    const chc_alloc *al;
    uint8_t         *frame_buf;
    size_t           frame_cap;
    size_t           frame_pos;
    size_t           frame_fill;
    uint8_t         *comp_buf;
    size_t           comp_cap;
} chc__decomp_src;

static int
chc__decomp_read_frame(chc__decomp_src *s, chc_err *err)
{
    int rc;
    uint64_t lo, hi;
    rc = chc__read_u64_le(s->raw, &lo, err); if (rc) return rc;
    rc = chc__read_u64_le(s->raw, &hi, err); if (rc) return rc;
    uint8_t header[CHC__COMP_HEADER_BYTES];
    rc = chc__read_bytes(s->raw, header, CHC__COMP_HEADER_BYTES, err);
    if (rc) return rc;
    uint8_t  method = header[0];
    uint32_t comp_with_hdr =
          (uint32_t) header[1]        | ((uint32_t) header[2] << 8)
        | ((uint32_t) header[3] << 16) | ((uint32_t) header[4] << 24);
    uint32_t orig =
          (uint32_t) header[5]        | ((uint32_t) header[6] << 8)
        | ((uint32_t) header[7] << 16) | ((uint32_t) header[8] << 24);
    if (comp_with_hdr < CHC__COMP_HEADER_BYTES)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "compressed frame too short: %u", comp_with_hdr);
    if (comp_with_hdr > 0x40000000u)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "compressed frame oversized: %u", comp_with_hdr);

    size_t payload = comp_with_hdr - CHC__COMP_HEADER_BYTES;
    /* Capture the bytes hashed: header (9B) + payload. */
    if (comp_with_hdr > s->comp_cap) {
        uint8_t *nb = chc__realloc(s->al, s->comp_buf, s->comp_cap,
                                   comp_with_hdr, err);
        if (!nb) return CHC_ERR_OOM;
        s->comp_buf = nb; s->comp_cap = comp_with_hdr;
    }
    memcpy(s->comp_buf, header, CHC__COMP_HEADER_BYTES);
    rc = chc__read_bytes(s->raw, s->comp_buf + CHC__COMP_HEADER_BYTES,
                         payload, err);
    if (rc) return rc;

    uint64_t got_lo, got_hi;
    chc_cityhash128(s->comp_buf, comp_with_hdr, &got_lo, &got_hi);
    if (got_lo != lo || got_hi != hi)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "compressed frame hash mismatch");

    if (orig > s->frame_cap) {
        uint8_t *nb = chc__realloc(s->al, s->frame_buf, s->frame_cap,
                                   orig, err);
        if (!nb) return CHC_ERR_OOM;
        s->frame_buf = nb; s->frame_cap = orig;
    }
    switch (method) {
    case CHC__COMP_LZ4:
        if (!s->codec || !s->codec->lz4_decompress)
            return chc__err_set(err, CHC_ERR_USAGE,
                "LZ4 frame received but no codec configured");
        rc = s->codec->lz4_decompress(s->codec->ud,
            s->comp_buf + CHC__COMP_HEADER_BYTES, payload,
            s->frame_buf, orig, err);
        if (rc) return rc;
        break;
    case CHC__COMP_ZSTD:
        if (!s->codec || !s->codec->zstd_decompress)
            return chc__err_set(err, CHC_ERR_USAGE,
                "ZSTD frame received but no codec configured");
        rc = s->codec->zstd_decompress(s->codec->ud,
            s->comp_buf + CHC__COMP_HEADER_BYTES, payload,
            s->frame_buf, orig, err);
        if (rc) return rc;
        break;
    default:
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "unknown compression method 0x%02x", method);
    }
    s->frame_pos = 0;
    s->frame_fill = orig;
    return CHC_OK;
}

static int
chc__decomp_io_read(void *ud, void *buf, size_t len, size_t *out_n,
                    chc_err *err)
{
    chc__decomp_src *s = ud;
    if (s->frame_pos == s->frame_fill) {
        int rc = chc__decomp_read_frame(s, err);
        if (rc) { *out_n = 0; return rc; }
    }
    size_t avail = s->frame_fill - s->frame_pos;
    size_t take  = len < avail ? len : avail;
    memcpy(buf, s->frame_buf + s->frame_pos, take);
    s->frame_pos += take;
    *out_n = take;
    return CHC_OK;
}

static int
chc__decomp_io_write(void *ud, const void *b, size_t n, chc_err *e)
{ (void) ud; (void) b; (void) n; (void) e; return -1; }

static void
chc__decomp_src_init(chc__decomp_src *s, chc__in *raw, const chc_codec *codec,
                     const chc_alloc *al, chc_io *out_io)
{
    memset(s, 0, sizeof *s);
    s->raw   = raw;
    s->codec = codec;
    s->al    = al;
    out_io->ud           = s;
    out_io->read         = chc__decomp_io_read;
    out_io->write        = chc__decomp_io_write;
    out_io->check_cancel = NULL;
}

static void
chc__decomp_src_free(chc__decomp_src *s)
{
    if (s->frame_buf) s->al->free(s->al->ud, s->frame_buf, s->frame_cap);
    if (s->comp_buf)  s->al->free(s->al->ud, s->comp_buf,  s->comp_cap);
    s->frame_buf = s->comp_buf = NULL;
    s->frame_cap = s->comp_cap = 0;
}

/* ============================================================
 * LZ4 adapter (opt out with CHC_NO_LZ4 before including).
 * ============================================================ */

#ifndef CHC_NO_LZ4

#include <lz4.h>

static int
chc__lz4_compress(void *ud, const void *src, size_t src_len,
                  void *dst, size_t dst_cap, size_t *dst_n, chc_err *err)
{
    (void) ud;
    if (src_len > (size_t) LZ4_MAX_INPUT_SIZE)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "LZ4 input too big: %zu", src_len);
    int n = LZ4_compress_default((const char *) src, (char *) dst,
                                 (int) src_len, (int) dst_cap);
    if (n <= 0)
        return chc__err_set(err, CHC_ERR_OOM,
                            "LZ4_compress_default failed (rc=%d)", n);
    *dst_n = (size_t) n;
    return CHC_OK;
}

static int
chc__lz4_decompress(void *ud, const void *src, size_t src_len,
                    void *dst, size_t original, chc_err *err)
{
    (void) ud;
    int n = LZ4_decompress_safe((const char *) src, (char *) dst,
                                (int) src_len, (int) original);
    if (n < 0 || (size_t) n != original)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "LZ4_decompress_safe rc=%d, expected %zu",
                            n, original);
    return CHC_OK;
}

static size_t chc__lz4_bound(size_t n) { return (size_t) LZ4_compressBound((int) n); }

void
chc_lz4_codec_init(chc_codec *out)
{
    out->ud             = NULL;
    out->lz4_compress   = chc__lz4_compress;
    out->lz4_decompress = chc__lz4_decompress;
    out->lz4_bound      = chc__lz4_bound;
}

#endif /* !CHC_NO_LZ4 */

/* ============================================================
 * ZSTD adapter (opt out with CHC_NO_ZSTD before including).
 * ============================================================ */

#ifndef CHC_NO_ZSTD

#include <zstd.h>

static int
chc__zstd_compress(void *ud, const void *src, size_t src_len,
                   void *dst, size_t dst_cap, size_t *dst_n, chc_err *err)
{
    (void) ud;
    size_t n = ZSTD_compress(dst, dst_cap, src, src_len, ZSTD_CLEVEL_DEFAULT);
    if (ZSTD_isError(n))
        return chc__err_set(err, CHC_ERR_OOM,
                            "ZSTD_compress failed: %s", ZSTD_getErrorName(n));
    *dst_n = n;
    return CHC_OK;
}

static int
chc__zstd_decompress(void *ud, const void *src, size_t src_len,
                     void *dst, size_t original, chc_err *err)
{
    (void) ud;
    size_t n = ZSTD_decompress(dst, original, src, src_len);
    if (ZSTD_isError(n))
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "ZSTD_decompress failed: %s", ZSTD_getErrorName(n));
    if (n != original)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "ZSTD_decompress produced %zu bytes, expected %zu",
                            n, original);
    return CHC_OK;
}

static size_t chc__zstd_bound(size_t n) { return ZSTD_compressBound(n); }

void
chc_zstd_codec_init(chc_codec *out)
{
    out->ud              = NULL;
    out->zstd_compress   = chc__zstd_compress;
    out->zstd_decompress = chc__zstd_decompress;
    out->zstd_bound      = chc__zstd_bound;
}

#endif /* !CHC_NO_ZSTD */

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_COMPRESSION_H */
