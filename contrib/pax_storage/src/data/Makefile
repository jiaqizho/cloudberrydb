# contrib/pax_storage/Makefile

MODULE_big = pax
OBJS = \
	$(WIN32RES) 
PG_CPPFLAGS = -I/usr/local/include
PG_CXXFLAGS = -std=c++14

PGFILEDESC = "pax - PAX table access method"
SHLIB_LINK += -luuid

REGRESS = ddl types join update update_gp

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pax_storage/src/data
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: install-data
install-data:
	$(INSTALL_DATA) pax-cdbinit--1.0.sql '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

install: install-data

.PHONY: uninstall-data

uninstall-data:
	$(RM) '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

uninstall: uninstall-data

