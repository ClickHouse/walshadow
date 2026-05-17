# walshadow_oracle — PG extension. PGXS-driven build.
#
# Build (against the pg_config on PATH):
#   make
#
# Build into a custom prefix that walshadow's test harness can
# `dynamic_library_path` against without root:
#   make DESTDIR=$PWD/out install
#
# Build against a non-default PG:
#   make PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config

MODULE_big = walshadow_oracle
OBJS = walshadow_oracle.o

EXTENSION = walshadow_oracle
DATA = walshadow_oracle--0.1.sql

# `make installcheck` runs pg_regress against the *running* server pointed
# at by pg_config. pg_regress creates `contrib_regression`, CREATEs the
# extension, runs sql/<name>.sql, and diffs against expected/<name>.out.
REGRESS = walshadow_oracle

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
