# walshadow demo image. Multi-stage on alpine/musl:
#   1. rust:1-alpine        -> static-ish walshadow-stream binary. alpine 3.23
#      ships gcc 15, so clickhouse-c-rs builds under -std=c23.
#   2. postgres:N-alpine     -> walshadow PG extension, compiled against the
#      exact runtime PG (same image) so PG_MODULE_MAGIC matches. The stream
#      binary itself links no PG, so it builds separately.
#   3. postgres:N-alpine runtime carries both, so daemon-owned shadow PG can
#      `CREATE EXTENSION walshadow` against its own $libdir.

ARG PG_MAJOR=18

FROM rust:1-alpine AS rust-builder
# build-base: gcc 15 + musl-dev + make. lz4/zstd dev+headers for the
# clickhouse-c-rs codecs. Dynamic musl link (crt-static off) so the binary
# resolves liblz4.so.1/libzstd.so.1 shipped in postgres:*-alpine at runtime.
RUN apk add --no-cache \
        build-base \
        lz4-dev \
        zstd-dev
ENV RUSTFLAGS="-C target-feature=-crt-static"
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY clickhouse-c-rs ./clickhouse-c-rs
COPY bench ./bench
COPY src ./src
# Cache mounts persist cargo's registry/git and the target dir across builds,
# so a source change recompiles only the changed crates, not all deps. The
# target dir lives in the cache mount (not the image layer), so the binary is
# copied out to a normal path in the same RUN for the runtime stage to pick up.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin walshadow-stream \
    && cp /src/target/release/walshadow-stream /usr/local/bin/walshadow-stream

FROM postgres:${PG_MAJOR}-alpine AS ext-builder
RUN apk add --no-cache build-base
WORKDIR /src
COPY pgext ./pgext
# with_llvm=no: skip the JIT bitcode step (image's PGXS hardcodes clang-19,
# absent here); the .so itself is complete and JIT-independent.
RUN make -C pgext clean \
    && make -C pgext with_llvm=no PG_CONFIG=/usr/local/bin/pg_config


FROM postgres:${PG_MAJOR}-alpine AS runtime
# bash: entrypoint shebang. ca-certificates: TLS to clickhouse.
# liblz4/libzstd already present as PG deps.
RUN apk add --no-cache bash ca-certificates

COPY --from=rust-builder /usr/local/bin/walshadow-stream /usr/local/bin/walshadow-stream
COPY --from=ext-builder /src/pgext/walshadow.so \
        /usr/local/lib/postgresql/walshadow.so
COPY --from=ext-builder /src/pgext/walshadow.control \
        /usr/local/share/postgresql/extension/walshadow.control
COPY --from=ext-builder /src/pgext/walshadow--0.1.sql \
        /usr/local/share/postgresql/extension/walshadow--0.1.sql

COPY docker/entrypoint.sh /usr/local/bin/walshadow-entrypoint
RUN chmod +x /usr/local/bin/walshadow-entrypoint \
    && install -d -o postgres -g postgres \
        /var/lib/walshadow \
        /var/lib/walshadow/shadow-data \
        /var/lib/walshadow/out \
        /var/lib/walshadow/spill \
        /var/run/postgresql \
        /var/run/walshadow \
        /etc/walshadow \
        /etc/walshadow/ch-config.d

USER postgres
ENTRYPOINT ["/usr/local/bin/walshadow-entrypoint"]
