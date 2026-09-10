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

ifeq ($(COVERAGE),1)
PG_CFLAGS += --coverage -fprofile-abs-path
SHLIB_LINK += --coverage
endif

EXTRA_CLEAN = coverage *.gcda *.gcno faultshim.so

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# PGXS tracks no header deps, and a stale object against a bumped
# WS_PROTO_VERSION links a worker the daemon then refuses
$(OBJS): walshadow.h
native.o: $(wildcard $(PGCH_DIR)/*.h $(PGCH_DIR)/clickhouse-c/*.h)

# Test-only libc interposer the bridge worker tests LD_PRELOAD onto their own
# cluster. Separate translation unit and separate shared object: never linked
# into walshadow.so, never installed, and outside `all` so the runtime image
# build never has to compile it. Own flags, because PGXS builds shared objects
# with hidden visibility and an interposer has to be visible to the dynamic
# linker
faultshim.so: faultshim.c
	$(CC) -O2 -Wall -Wextra -fPIC -shared -o $@ $< -ldl

COVERAGE_CC = gcc
GCOV = gcov
GCOVR = gcovr
COVERAGE_FILTER = $(CURDIR)/[^/]+[.][ch]$$

# Every line and function is reachable from the test suite, so the line gate is
# the whole of it. Branches are not: `ereport` expands to a test of `errstart`
# whose false arm needs a log level the cluster never runs at, the header-only
# ClickHouse accessors carry null guards their callers cannot reach, and a few
# PostgreSQL macros have legs only pg_upgraded tuples take. The branch floor
# sits below the measured figure by that much
COVERAGE_MIN_LINE ?= 100
COVERAGE_MIN_BRANCH ?= 85

.PHONY: coverage-build coverage-report coverage-html coverage-reset

coverage-build:
	$(MAKE) clean
	$(MAKE) COVERAGE=1 CC=$(COVERAGE_CC) all faultshim.so

coverage-reset:
	rm -rf coverage *.gcda

coverage-report:
	@test -f walshadow.so || { echo "Missing walshadow.so, run make coverage-build" >&2; exit 1; }
	@test -f worker.gcno || { echo "Missing coverage notes, run make coverage-build" >&2; exit 1; }
	@test -f worker.gcda || { echo "Missing profiles, run tests after make coverage-build" >&2; exit 1; }
	rm -rf coverage/raw
	mkdir -p coverage/raw
	cp *.gcno *.gcda coverage/raw/
	$(GCOVR) --gcov-executable $(GCOV) --root .. --filter '$(COVERAGE_FILTER)' \
		--object-directory $(CURDIR) coverage/raw --print-summary \
		--fail-under-line $(COVERAGE_MIN_LINE) \
		--fail-under-branch $(COVERAGE_MIN_BRANCH) \
		--json coverage/coverage.json --lcov coverage/lcov.info

coverage-html: coverage-report
	rm -rf coverage/html
	mkdir -p coverage/html
	$(GCOVR) --root .. --filter '$(COVERAGE_FILTER)' \
		--add-tracefile coverage/coverage.json --html-details coverage/html/index.html
