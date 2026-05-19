# walshadow demo image. Multi-stage: cargo + PGXS build first, then
# slim down to a postgres:17 runtime that carries both the
# walshadow-stream binary & the walshadow PG extension so shadow PG
# (auto-spawned in this same container by --bootstrap-autospawn-shadow)
# can `CREATE EXTENSION walshadow` against its own $libdir.

ARG PG_MAJOR=17

FROM rust:1-bookworm AS builder
ARG PG_MAJOR
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        liblz4-dev \
        libzstd-dev \
        ca-certificates \
        gnupg \
        wget \
    && install -d /usr/share/postgresql-common/pgdg \
    && wget -qO /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
        https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
        https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        postgresql-server-dev-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY clickhouse-c-rs ./clickhouse-c-rs
COPY wal-rs ./wal-rs
COPY src ./src
COPY tests ./tests
COPY fixtures ./fixtures
COPY pgext ./pgext

RUN cargo build --release --bin walshadow-stream
RUN make -C pgext clean \
    && make -C pgext PG_CONFIG=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config


FROM postgres:17-bookworm AS runtime
ARG PG_MAJOR
RUN apt-get update && apt-get install -y --no-install-recommends \
        liblz4-1 \
        libzstd1 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/walshadow-stream /usr/local/bin/walshadow-stream
COPY --from=builder /src/pgext/walshadow.so \
        /usr/lib/postgresql/${PG_MAJOR}/lib/walshadow.so
COPY --from=builder /src/pgext/walshadow.control \
        /usr/share/postgresql/${PG_MAJOR}/extension/walshadow.control
COPY --from=builder /src/pgext/walshadow--0.1.sql \
        /usr/share/postgresql/${PG_MAJOR}/extension/walshadow--0.1.sql

COPY docker/entrypoint.sh /usr/local/bin/walshadow-entrypoint
RUN chmod +x /usr/local/bin/walshadow-entrypoint \
    && install -d -o postgres -g postgres \
        /var/lib/walshadow \
        /var/lib/walshadow/shadow-data \
        /var/lib/walshadow/out \
        /var/lib/walshadow/spill \
        /var/run/postgresql

USER postgres
ENTRYPOINT ["/usr/local/bin/walshadow-entrypoint"]
