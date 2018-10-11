#libradosproxy requires librados
libradosproxy_internal_la_SOURCES = \
	libproxy/ProxyClient.cc 
noinst_LTLIBRARIES += libradosproxy_internal.la

libradosproxy_la_SOURCES = \
	libproxy/libproxy.cc

libradosproxy_la_CXXFLAGS = ${AM_CXXFLAGS}

libradosproxy_la_LIBADD = $(LIBPROXYC) $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS) \
	libglobal.la libradosproxy_internal.la
libradosproxy_la_LDFLAGS = ${AM_LDFLAGS} -version-info 1:0:0
if LINUX
libradosproxy_la_CXXFLAGS += -fvisibility=hidden -fvisibility-inlines-hidden
libradosproxy_la_LDFLAGS += -Xcompiler -Xlinker -Xcompiler '--exclude-libs=ALL'
endif
lib_LTLIBRARIES += libradosproxy.la

noinst_HEADERS +=\
	libproxy/ProxyClient.h
