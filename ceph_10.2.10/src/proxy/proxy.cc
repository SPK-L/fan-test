#include <iostream>

#include "msg/Messenger.h"
#include "msg/Message.h"
#include "messages/MProxyOpReply.h"
#include "messages/MProxyOp.h"

#include "mon/MonClient.h"
#include "common/debug.h"
#include "common/errno.h"

#include "osdc/Objecter.h"

#include "include/rados/librados.hpp"

#include "proxy_types.h"
#include "ProxyCacher.h"
#include "ProxyHandler.h"
#include "proxy.h"

#define dout_subsys ceph_subsys_proxy

using namespace std;

Proxy::Proxy(CephContext *_cct, Messenger *_cluster_messenger, Messenger *_client_messenger):
	Dispatcher(_cct), cluster_messenger(_cluster_messenger), client_messenger(_client_messenger),
	rados(NULL), cacher_handler(NULL), proxy_cacher(NULL)
{

}

int Proxy::init()
{
	assert(!rados);
	rados = new librados::Rados();

	// TODO::use client.admin replace of proxy.[x] for now, fix me
	//int r = rados->init_with_context(cct);
	int r = rados->init(NULL);
	if (r < 0) {
		dout(0) << "Failed to init rados, r=" << r << dendl;
		goto out;
	}

	r = rados->conf_read_file("/etc/ceph/ceph.conf");
	if (r < 0) {
		dout(0) << "Failed to read conf file for rados, r=" << r << dendl;
		goto out;
	}

	r = rados->connect();
	if (r < 0) {
		dout(0) << "Failed to connect rados, r=" << r << dendl;
		goto out;
	}

	cacher_handler = new ProxyCacheHandler(rados);
	proxy_cacher = new ProxyCacher(cct, cacher_handler, cct->_conf->proxy_max_cache_objects);

	cluster_messenger->add_dispatcher_head(this);
	client_messenger->add_dispatcher_head(this);

	return 0;
out:
	shutdown();
	return r;
}

void Proxy::ms_fast_dispatch(Message *m)
{
	ms_dispatch(m);
}

void Proxy::ms_fast_preprocess(Message *m)
{
	return;
}

bool Proxy::ms_dispatch(Message *m)
{
	switch(m->get_type()) {
		case CEPH_MSG_PROXY_OP:
			do_proxy_op((MProxyOp *)m);
			return true;
	}
	m->put();
	return false;
}

bool Proxy::ms_get_authorizer(int dest_type, AuthAuthorizer **a, bool force_new)
{
	return true;
}

bool Proxy::ms_verify_authorizer(Connection *con, int peer_type, int protocol, 
	ceph::bufferlist &authorizer, ceph::bufferlist &authorizer_reply, bool &isvalid, 
	CryptoKey &session_key)
{
	isvalid = true;
	return isvalid;
}

void Proxy::ms_handle_connect(Connection *con) 
{
	return;
}

void Proxy::ms_handle_fast_connect(Connection *con)
{
	return;
}

void Proxy::ms_handle_fast_accept(Connection *con)
{
	return;
}

bool Proxy::ms_handle_reset(Connection *con)
{
	return true;
}

void Proxy::ms_handle_remote_reset(Connection *con)
{
	return;
}

void Proxy::handle_signal(int signum){
	shutdown();
}

int Proxy::shutdown()
{
	dout(2) << "Proxy shutting down" << dendl;
	client_messenger->shutdown();
	cluster_messenger->shutdown();
	if (proxy_cacher) {
		delete proxy_cacher;
		proxy_cacher = NULL;
	}

	if (cacher_handler) {
		delete cacher_handler;
		cacher_handler = NULL;
	}
	
	if (rados) {
		rados->shutdown();
		delete rados;
		rados = NULL;
	}

	return 0;
}

const char** Proxy::get_tracked_conf_keys() const
{
	static const char* KEYS[] = {
		NULL,
	};
	return KEYS;
}

void Proxy::handle_conf_change(const struct md_config_t *conf, const std::set<std::string> &changed)
{
	return;
}

void Proxy::do_proxy_op(MProxyOp *m)
{
	dout(20) << __func__ << dendl;
	ProxyOp *req = m->get_op();
	C_ProxyOpFinish *fin = new C_ProxyOpFinish(this, m);
	switch(req->get_type()) {
		case CEPH_PROXY_FLAG_READ: {
			do_read(req, fin);
			break;
		}
		case CEPH_PROXY_FLAG_WRITE: {
			do_write(req, fin);
			break;
		}
	}
	req = NULL;
}

int Proxy::do_read(ProxyOp *req, C_ProxyOpFinish *fin)
{
	int poolid = req->poolid;
	std::string oid = req->oid;
	uint64_t offset = req->op.offset;
	uint64_t length = req->op.length;

	return proxy_cacher->object_read(poolid, oid, offset, length, fin->data, fin);
}

int Proxy::do_write(ProxyOp *req, C_ProxyOpFinish *fin)
{
	int poolid = req->poolid;
	std::string oid = req->oid;
	uint64_t offset = req->op.offset;

	return proxy_cacher->object_write(poolid, oid, offset, req->indata, fin);
}

void Proxy::C_ProxyOpFinish::finish(int r)
{
	dout(20) << "proxy op finish" << dendl;
	ProxyOp *req = m->get_op();
	ProxyOp reply_op;
	reply_op.rval = r;
	reply_op.poolid = req->poolid;
	reply_op.oid = req->oid;
	reply_op.op.offset = req->op.offset;
	

	switch(req->get_type()) {
		case CEPH_PROXY_FLAG_READ: {
			reply_op.op.length = data.length();
			reply_op.outdata.claim_append(data);
			break;
		}
		case CEPH_PROXY_FLAG_WRITE: {
			reply_op.op.length = r;
			break;
		}
	}
	
	MProxyOpReply *reply = new MProxyOpReply(m, reply_op);
	proxy->send_message_proxy_client(reply, m->get_connection());
	req = NULL;
	m->put();
}