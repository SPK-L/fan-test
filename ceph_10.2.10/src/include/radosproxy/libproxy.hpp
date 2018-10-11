#ifndef CEPH_LIBPROXY_HPP
#define CEPH_LIBPROXY_HPP

#include "libproxy.h"

class Mutex;
class Cond;

namespace ceph {
  namespace buffer {
    class ptr;
    class list;
    class hash;
  }
};

namespace libproxy {
typedef void *config_t;
class ProxyClient;
using bufferlist = ceph::buffer::list;

struct CEPH_PROXY_API AioCompletion {
private:
	Mutex *lock;
	Cond *cond;
	int32_t rval;
	bool completed;
	bufferlist *outdata;
public:
	AioCompletion();
	~AioCompletion() {
		if (lock) {
			delete lock;
			lock = NULL;
		}
		if (cond) {
			delete cond;
			cond = NULL;
		}
	}
	
	bool is_complete();
	int wait_for_complete();
	void complete(int r);
	void set_data(bufferlist &bl);
	void flush(bufferlist &bl);
};

class CEPH_PROXY_API RadosProxy{
private:
	ProxyClient *client;
public:
	RadosProxy();
	~RadosProxy();

	int init(const char* const id);
	int init_with_context(config_t _cct);
	int connect();
	void shutdown();
	int conf_read_file(const char* const path) const;

	int aio_read(int poolid, std::string oid, uint64_t off, uint64_t len,
		bufferlist &bl, AioCompletion *c);
	int aio_write(int poolid, std::string oid, uint64_t off, bufferlist &bl,
		AioCompletion *c);
};

}

#endif
