# walshadow — PG loadable module. PGXS-driven build.
#
# Not an extension: no control file, no SQL script. The only consumer is a
# shadow standby whose catalog is a read-only physical copy of source's, so
# `CREATE EXTENSION` can never run there. `shared_preload_libraries` is the
# sole entry point, and decode coverage lives in the daemon's tests/bridge.rs.
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

MODULE_big = walshadow
OBJS = native.o overlay.o worker.o

# Unmodified upstream, pinned as a recursive submodule. `native.o` is the one
# translation unit compiling both header-only implementations; -isystem keeps
# their warnings out of walshadow's build
PGCH_DIR = pg-clickhouse-c
PG_CPPFLAGS = -isystem $(PGCH_DIR) -isystem $(PGCH_DIR)/clickhouse-c \
	-DPGCH_MSG_PREFIX='"walshadow oracle: "'

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# PGXS tracks no header deps, and a stale object against a bumped
# WS_PROTO_VERSION links a worker the daemon then refuses
$(OBJS): walshadow.h
native.o: $(wildcard $(PGCH_DIR)/*.h $(PGCH_DIR)/clickhouse-c/*.h)
