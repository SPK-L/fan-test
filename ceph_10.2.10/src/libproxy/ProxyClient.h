#ifndef CEPH_PROXYCLIENT_H
#define CEPH_PROXYCLIENT_H

#include <string>

#include "common/Cond.h"
#include "common/Mutex.h"

#include "msg/Dispatcher.h"

#include "proxyc/Proxier.h"

struct AuthAuthorizer;
class CephContext;
struct Connection;
struct md_config_t;
class Message;
class Messenger;

namespace libproxy {
class AioCompletion;

class ProxyClient:public Dispatcher {
public:
	using Dispatcher::cct;
private:
	Proxier *proxier;
	Messenger *messenger;
	int refcnt;
	Mutex lock;

	bool ms_can_fast_dispatch(Message *m);
	void ms_fast_dispatch(Message *m);
	bool ms_dispatch(Message *m);
	bool ms_get_authorizer(int dest_type, AuthAuthorizer **a, bool force_new);
	void ms_handle_connect(Connection *con);
	bool ms_handle_reset(Connection *con);
	void ms_handle_remote_reset(Connection *con);
	bool parse_proxy_addr(std::string host, entity_addr_t &addr);
public:
	ProxyClient(CephContext *_cct);
	~ProxyClient();

	int connect();
	void shutdown();
	void get();
	bool put();

	int aio_read(uint64_t poolid, std::string oid, uint64_t off, uint64_t len, 
		bufferlist &bl, libproxy::AioCompletion *c);
	int aio_write(uint64_t poolid, std::string oid, uint64_t off, 
		bufferlist &bl, libproxy::AioCompletion *c);
};

} //namespace libproxy

#endif //CEPH_PROXYCLIENT_H
