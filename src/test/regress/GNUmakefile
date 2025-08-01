#-------------------------------------------------------------------------
#
# GNUmakefile--
#    Makefile for src/test/regress (the regression tests)
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/regress/GNUmakefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_regress - test driver"
PGAPPICON = win32

subdir = src/test/regress
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# file with extra config for temp build
TEMP_CONF =
ifdef TEMP_CONFIG
TEMP_CONF += --temp-config=$(TEMP_CONFIG)
endif

# maximum simultaneous connections for parallel tests
MAXCONNOPT =
ifdef MAX_CONNECTIONS
MAXCONNOPT += --max-connections=$(MAX_CONNECTIONS)
endif

# stuff to pass into build of pg_regress
EXTRADEFS = '-DHOST_TUPLE="$(host_tuple)"' \
	'-DSHELLPROG="$(SHELL)"' \
	'-DDLSUFFIX="$(DLSUFFIX)"'

##
## Prepare for tests
##

# Build regression test driver

all: pg_regress$(X) twophase_pqexecparams extended_protocol_resqueue

pg_regress$(X): pg_regress.o pg_regress_main.o $(WIN32RES) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@
extended_protocol_resqueue: extended_protocol_resqueue.c
	$(CC) $(CPPFLAGS) -I../../../src/interfaces/libpq -L$(GPHOME)/lib -L../../../src/interfaces/libpq  -o $@ $< -lpq

# dependencies ensure that path changes propagate
pg_regress.o: pg_regress.c $(top_builddir)/src/port/pg_config_paths.h
pg_regress.o: override CPPFLAGS += -I$(top_builddir)/src/port $(EXTRADEFS)
regress_gp.o: override CPPFLAGS += -I$(libpq_srcdir)
regress_gp.o: override LDFLAGS += -L$(top_builddir)/src/interfaces/libpq

twophase_pqexecparams: twophase_pqexecparams.c
	$(CC) $(CPPFLAGS) $(rpath) -I$(top_builddir)/src/interfaces/libpq -L$(GPHOME)/lib -L$(top_builddir)/src/interfaces/libpq  -o $@ $< -lpq

# note: because of the submake dependency, this rule's action is really a no-op
$(top_builddir)/src/port/pg_config_paths.h: | submake-libpgport
	$(MAKE) -C $(top_builddir)/src/port pg_config_paths.h

install: all installdirs
	$(INSTALL_PROGRAM) pg_regress$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'
	$(INSTALL_PROGRAM) regress.so '$(DESTDIR)$(pkglibdir)/regress.so'
	$(INSTALL_PROGRAM) gpdiff.pl '$(DESTDIR)$(pgxsdir)/$(subdir)/gpdiff.pl'
	$(INSTALL_PROGRAM) gpstringsubs.pl '$(DESTDIR)$(pgxsdir)/$(subdir)/gpstringsubs.pl'
	$(INSTALL_PROGRAM) atmsort.pl '$(DESTDIR)$(pgxsdir)/$(subdir)/atmsort.pl'
	$(INSTALL_PROGRAM) atmsort.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/atmsort.pm'
	$(INSTALL_PROGRAM) explain.pl '$(DESTDIR)$(pgxsdir)/$(subdir)/explain.pl'
	$(INSTALL_PROGRAM) explain.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/explain.pm'
	$(INSTALL_PROGRAM) GPTest.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/GPTest.pm'
	$(INSTALL_PROGRAM) scan_flaky_fault_injectors.sh '$(DESTDIR)$(pgxsdir)/$(subdir)/scan_flaky_fault_injectors.sh'
	$(INSTALL_PROGRAM) twophase_pqexecparams '$(DESTDIR)$(pgxsdir)/$(subdir)/twophase_pqexecparams'
	$(INSTALL_PROGRAM) hooktest/test_hook.so '$(DESTDIR)$(pkglibdir)/test_hook.so'
	$(INSTALL_PROGRAM) query_info_hook_test/query_info_hook_test.so '$(DESTDIR)$(pkglibdir)/query_info_hook_test.so'
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

uninstall:
	rm -f '$(DESTDIR)$(pkglibdir)/regress.so'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/gpdiff.pl'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/gpstringsubs.pl'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/atmsort.pl'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/atmsort.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/explain.pl'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/explain.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/GPTest.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/twophase_pqexecparams'
	rm -f '$(DESTDIR)$(pkglibdir)/test_hook.so'
	rm -f '$(DESTDIR)$(pkglibdir)/query_info_hook_test.so'


# Build dynamically-loaded object file for CREATE FUNCTION ... LANGUAGE C.

NAME = regress
OBJS = $(WIN32RES) regress.o

OBJS += regress_gp.o

SHLIB_LINK_INTERNAL = $(libpq)

include $(top_srcdir)/src/Makefile.shlib

all: all-lib

# Ensure parallel safety if a build is started in this directory
$(OBJS): | submake-libpgport submake-generated-headers

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
input_files = $(patsubst $(srcdir)/input/%.source,sql/%.sql, $(wildcard $(srcdir)/input/*.source))
output_files := $(patsubst $(srcdir)/output/%.source,expected/%.out, $(wildcard $(srcdir)/output/*.source))

# not installed by default

regress_data_files = \
	$(filter-out $(addprefix $(srcdir)/,$(output_files)),$(wildcard $(srcdir)/expected/*.out)) \
	$(wildcard $(srcdir)/input/*.source) \
	$(wildcard $(srcdir)/output/*.source) \
	$(wildcard $(srcdir)/output/uao_dml/*.source) \
	$(wildcard $(srcdir)/output/uao_ddl/*.source) \
	$(filter-out $(addprefix $(srcdir)/,$(input_files)),$(wildcard $(srcdir)/sql/*.sql)) \
	$(wildcard $(srcdir)/data/*.data) \
	$(srcdir)/parallel_schedule $(srcdir)/resultmap

install-tests: all install install-lib installdirs-tests
	$(MAKE) -C $(top_builddir)/contrib/spi install
	for file in $(subst $(srcdir)/,,$(regress_data_files)); do \
		$(INSTALL_DATA) $(srcdir)/$$file '$(DESTDIR)$(pkglibdir)/regress/'$$file || exit; \
	done

installdirs-tests: installdirs
	$(MKDIR_P)  $(patsubst $(srcdir)/%/,'$(DESTDIR)$(pkglibdir)/regress/%',$(sort $(dir $(regress_data_files))))


# Get some extra C modules from contrib/spi

all: refint$(DLSUFFIX) autoinc$(DLSUFFIX) hooktest query_info_hook_test

refint$(DLSUFFIX): $(top_builddir)/contrib/spi/refint$(DLSUFFIX)
	cp $< $@

autoinc$(DLSUFFIX): $(top_builddir)/contrib/spi/autoinc$(DLSUFFIX)
	cp $< $@

$(top_builddir)/contrib/spi/refint$(DLSUFFIX): | submake-contrib-spi ;

$(top_builddir)/contrib/spi/autoinc$(DLSUFFIX): | submake-contrib-spi ;

submake-contrib-spi: | submake-libpgport submake-generated-headers
	$(MAKE) -C $(top_builddir)/contrib/spi

.PHONY: submake-contrib-spi

# Tablespace setup

.PHONY: tablespace-setup
tablespace-setup:
	rm -rf ./testtablespace ./testtablespace_*
	mkdir -p ./testtablespace ./testtablespace_otherloc ./testtablespace_unlogged \
	./testtablespace_existing_version_dir/1/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/2/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/3/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/4/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/5/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/6/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/7/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/8/GPDB_99_399999991/ \
        ./testtablespace_1111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000/ \
	./testtablespace_default_tablespace \
	./testtablespace_temp_tablespace \
	./testtablespace_mytempsp0 \
	./testtablespace_mytempsp1 \
	./testtablespace_mytempsp2 \
	./testtablespace_mytempsp3 \
	./testtablespace_mytempsp4 \
	./testtablespace_database_tablespace


.PHONY: hooktest
hooktest:
	$(MAKE) -C $< $@

.PHONY: query_info_hook_test
query_info_hook_test:
	$(MAKE) -C $< $@

##
## Run tests
##

REGRESS_OPTS = --dlpath=. --max-concurrent-tests=24 --init-file=$(srcdir)/init_file --make-testtablespace-dir \
	$(EXTRA_REGRESS_OPTS)

check: all
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule $(MAXCONNOPT) $(EXTRA_TESTS)

check-tests: all | temp-install
	$(pg_regress_check) $(REGRESS_OPTS) $(MAXCONNOPT) $(TESTS) $(EXTRA_TESTS)

#installcheck: all tablespace-setup
#	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/serial_schedule $(EXTRA_TESTS)
installcheck: installcheck-good

installcheck-small: all tablespace-setup
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule $(EXTRA_TESTS)

installcheck-cbdb: all tablespace-setup twophase_pqexecparams hooktest query_info_hook_test
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/greenplum_schedule $(EXTRA_TESTS)

installcheck-good: all tablespace-setup twophase_pqexecparams hooktest query_info_hook_test
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule --schedule=$(srcdir)/greenplum_schedule $(EXTRA_TESTS)

installcheck-parallel: all
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule $(MAXCONNOPT) $(EXTRA_TESTS)

# cbdb parallel plan tests, ignore the incompatible cases, should run with 5 GUCs like below:
installcheck-cbdb-parallel: all tablespace-setup twophase_pqexecparams
	( \
	export PGOPTIONS='-c optimizer=off -c enable_parallel=true -c min_parallel_table_scan_size=0 -c min_parallel_index_scan_size=0 -c force_parallel_mode=1'; \
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule --schedule=$(srcdir)/greenplum_schedule --max-connections=5 $(EXTRA_TESTS) --exclude-tests explain \
	)

installcheck-tests: all
	$(pg_regress_installcheck) $(REGRESS_OPTS) $(TESTS) $(EXTRA_TESTS)

installcheck-fixme: all
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/fixme_schedule $(EXTRA_TESTS)

ifeq ($(BUILD_TYPE),prod)
.PHONY: installcheck-icudp
installcheck-icudp:
	+@echo "icudp tests can not run in production builds, skipped"
else
installcheck-icudp: all tablespace-setup
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/icudp_schedule --dbname=regression_icudp $(EXTRA_TESTS)
endif

standbycheck: all
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/standby_schedule  $(EXTRA_TESTS)

# GPDB: installcheck for hot standby. This is essentially same as the upstream 'standbycheck'
# above but we just make sure that we do the primary preparation and use the desired standby port.
# If no standby port is given, just use the demo cluster's standby port 7001. 
ifeq ($(STANDBY_PGPORT),)
  STANDBY_PGPORT = 7001
endif
installcheck-hot-standby: all
	$(pg_regress_installcheck) $(REGRESS_OPTS) hs_primary_setup
	$(pg_regress_installcheck) $(REGRESS_OPTS) --port=$(STANDBY_PGPORT) --use-existing --schedule=$(srcdir)/standby_schedule  $(EXTRA_TESTS)

# old interfaces follow...

runcheck: check
runtest: installcheck
runtest-parallel: installcheck-parallel

bigtest: all
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule --max-connections=1 numeric_big

bigcheck: all | temp-install
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule $(MAXCONNOPT) numeric_big

##
## Clean up
##

clean maintainer-clean: clean-lib
# things built by `all' target
	rm -f $(OBJS) refint$(DLSUFFIX) autoinc$(DLSUFFIX)
	rm -f pg_regress_main.o pg_regress.o pg_regress$(X)
	rm -f twophase_pqexecparams
	$(MAKE) -C $(top_builddir)/contrib/spi clean
	$(MAKE) -C hooktest/ clean
	$(MAKE) -C query_info_hook_test/ clean
# things created by various check targets
	rm -f $(output_files) $(input_files)
	rm -rf ./testtablespace ./testtablespace_*
	rm -rf $(pg_regress_clean_files)

distclean: clean
	rm -f GPTest.pm
