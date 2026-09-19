# Test-only loadable module; see wstest.c. Built by integration tests that
# need primitives SQL cannot express, on their source cluster.
#
#   make -C pgext/test
MODULES = wstest
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
