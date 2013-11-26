
MAKEFLAGS = $(AM_MAKEFLAGS)

xlator_LTLIBRARIES := dfc.la
xlatordir = $(gflibdir)/xlator/features

dfc_la_LDFLAGS := -module -avoid-version

dfc_la_SOURCES := dfc.c

dfc_la_LIBADD = $(gfdir)/libglusterfs/src/libglusterfs.la $(gfsys)/src/libgfsys.la
