libproxy_la_SOURCES = \
	proxy/proxy.cc \
	proxy/ProxyHandler.cc \
	proxy/ProxyCacher.cc

libproxy_la_CXXFLAGS = ${AM_CXXFLAGS}
libproxy_la_LIBADD = $(LIBRADOS) $(LIBCOMMON) $(LIBOSDC) librados_internal.la
noinst_LTLIBRARIES += libproxy.la

noinst_HEADERS += \
	proxy/proxy.h \
	proxy/proxy_types.h \
	proxy/ProxyHandler.h \
	proxy/ProxyCacher.h
