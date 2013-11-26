
MAKEFLAGS = $(AM_MAKEFLAGS)

xlator_LTLIBRARIES := dfctest.la
xlatordir = $(gflibdir)/xlator/debug

dfctest_la_CPPFLAGS = -I../src -I../lib
dfctest_la_LIBADD = ../lib/libgfdfc.la $(gfsys)/src/libgfsys.la \
                    $(gfdir)/libglusterfs/src/libglusterfs.la

dfctest_la_LDFLAGS := -module -avoid-version

dfctest_la_SOURCES := dfc-test.c

