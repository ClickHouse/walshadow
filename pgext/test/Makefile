# Test-only loadable module; see wstest.c. Built by the integration tests
# that need VACUUM's lock lifetime without waiting on autovacuum.
#
#   make -C pgext/test
MODULES = wstest
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
