#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sstream>
#include <pthread.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <memory.h>

#include "msg/Messenger.h"
#include "msg/Message.h"
#include "msg/Connection.h"
#include "msg/msg_types.h"

#include "common/dout.h"
#include "common/errno.h"
#include "common/config.h"
#include "common/ceph_context.h"
#include "common/common_init.h"

#include "include/ceph_features.h"
#include "include/assert.h"
#include "include/radosproxy/libproxy.hpp"

#include "auth/Auth.h"

#include "ProxyClient.h"

#define dout_subsys ceph_subsys_proxy

libproxy::ProxyClient::ProxyClient(CephContext *_cct):Dispatcher(_cct->get()),
	proxier(NULL), messenger(NULL), refcnt(1), lock("libproxy::ProxyClient::lock")
{
	//TODO
}

libproxy::ProxyClient::~ProxyClient()
{
	if (messenger) {
		delete messenger;
		messenger = NULL;
	}

	if (proxier) {
		delete proxier;
		proxier = NULL;
	}

	cct->put();
	cct = NULL;
}

bool libproxy::ProxyClient::ms_can_fast_dispatch(Message *m)
{
	return true;
}

void libproxy::ProxyClient::ms_fast_dispatch(Message *m)
{
	ms_dispatch(m);
}

bool libproxy::ProxyClient::ms_dispatch(Message *m)
{
	return false;
}

bool libproxy::ProxyClient::ms_get_authorizer(int dest_type, AuthAuthorizer ** a, 
	bool force_new)
{
	return true;
}

void libproxy::ProxyClient::ms_handle_connect(Connection *con)
{
	return;
}

bool libproxy::ProxyClient::ms_handle_reset(Connection *con)
{
	return true;
}

void libproxy::ProxyClient::ms_handle_remote_reset(Connection *con)
{
	return;
}

bool libproxy::ProxyClient::parse_proxy_addr(std::string host, entity_addr_t &addr)
{
	if (host.empty()) {
		ldout(cct, -1) << "Empty proxy host" << dendl;
		return false;
	}

	const char* p = host.c_str();
	const char* end = p + strlen(p);
	while (p < end) {
		if (!addr.parse(p, &p)) {
			return false;
		}
		p++;
	}
	return true;
}

int libproxy::ProxyClient::connect()
{
	int err = 0;
	entity_addr_t target_addr;

	common_init_finish(cct);
	ldout(cct, 20) << "common_init_finish" << dendl;
	
	if (!parse_proxy_addr(cct->_conf->proxy_host, target_addr)) {
		ldout(cct, -1) << "failed to parse proxy_host" << dendl;
		return -1;
	}
	entity_inst_t target(entity_name_t::PROXY(0), target_addr);

	messenger = Messenger::create_client_messenger(cct, "proxyclient");
	/*
	messenger = Messenger::create(cct, cct->_conf->ms_type, entity_name_t::CLIENT(-1),
		"proxyclient", nonce);
	*/
	if (!messenger) {
		err = -ENOMEM;
		goto out;
	}
	messenger->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));
	ldout(cct, 1) << "starting msgr at " << messenger->get_myaddr() << dendl;

	proxier = new Proxier(cct, messenger, cct->_conf->proxy_op_timeout, target);
	if (!proxier) {
		err = -ENOMEM;
		goto out;
	}

	proxier->init();
	messenger->add_dispatcher_tail(proxier);
	messenger->add_dispatcher_tail(this);

	messenger->start();
	proxier->start();

	return 0;
out:
	if (proxier) {
		delete proxier;
		proxier = NULL;
	}

	if (messenger) {
		delete messenger;
		messenger = NULL;
	}
	return err;
}

void libproxy::ProxyClient::shutdown()
{
	if (messenger) {
		messenger->shutdown();
		messenger->wait();
	}
}

void libproxy::ProxyClient::get()
{
	Mutex::Locker l(lock);
	assert(refcnt > 0);
	refcnt++;
}

bool libproxy::ProxyClient::put()
{
	Mutex::Locker l(lock);
	assert(refcnt > 0);
	refcnt--;
	return (refcnt == 0);
}

int libproxy::ProxyClient::aio_read(uint64_t poolid, std::string oid, uint64_t off, 
	uint64_t len, bufferlist &bl, libproxy::AioCompletion *c)
{
	return proxier->aio_read(poolid, oid, off, len, bl, c);
}

int libproxy::ProxyClient::aio_write(uint64_t poolid, std::string oid, uint64_t off, 
	bufferlist &bl, libproxy::AioCompletion *c)
{
	return proxier->aio_write(poolid, oid, off, bl, c);
}


