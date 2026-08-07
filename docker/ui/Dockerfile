# walshadow demo UI image. Deliberately tiny — one FastAPI module and one
# hand-written HTML page, no JS toolchain.
#
# python:3.13-slim (glibc), NOT alpine: pip resolves manylinux wheels for
# psycopg's C driver here. On musl there is no psycopg-binary wheel and the
# build would need gcc + libpq-dev + python3-dev, a ~200MB toolchain layer
# for a 400-line app.
#
# app.py / index.html are COPYed so the image stands alone (plain
# `docker build`, no compose). docker-compose.demo.yml re-mounts both
# read-only over the top, so editing either one is
# `docker compose ... restart ui` — no rebuild.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# `pgbench` for the START WRITE LOAD button (app.py runs it as a child process).
#
# Debian ships pgbench inside the postgresql-17 *server* package, and
# `apt-get install postgresql-client` gives you only the pg_wrapper shim, which
# then dies with "pgbench was not found in /usr/lib/postgresql/17/bin". So pull
# the .deb and extract the one 260 KB binary instead of dragging in the server
# (libllvm19, perl, icu — several hundred MB for a demo UI). libpq5 is the only
# runtime dep it needs beyond libc.
#
# Client 17 against the PG 18 source is fine: the built-in tpcb-like script is
# plain SQL over a stable wire protocol. `pgbench -i` is never run from here —
# the source-init one-shot still owns schema seeding.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libpq5 \
 && apt-get download postgresql-17 \
 && dpkg-deb --fsys-tarfile postgresql-17_*.deb \
      | tar -x -C / ./usr/lib/postgresql/17/bin/pgbench \
 && ln -s /usr/lib/postgresql/17/bin/pgbench /usr/local/bin/pgbench \
 && rm -f postgresql-17_*.deb \
 && rm -rf /var/lib/apt/lists/* \
 && pgbench --version

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY app.py index.html ./

EXPOSE 8088
CMD ["python", "-m", "uvicorn", "app:app", \
     "--host", "0.0.0.0", "--port", "8088", "--log-level", "warning"]
