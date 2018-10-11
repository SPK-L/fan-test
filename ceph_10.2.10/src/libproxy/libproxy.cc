#include <deque>
#include <string>

#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "common/Mutex.h"
#include "common/Cond.h"

#include "include/buffer.h"
#include "include/msgr.h"

#include "include/radosproxy/libproxy.h"
#include "include/radosproxy/libproxy.hpp"
#include "libproxy/ProxyClient.h"


static int proxy_create_common(proxy_t *cluster, const char* const clustername,
	CephInitParameters *iparams)
{
	CephContext *cct = common_preinit(*iparams, CODE_ENVIRONMENT_LIBRARY, 0);
	if (clustername) {
		cct->_conf->cluster = clustername;
	}
	cct->_conf->parse_env();
	cct->_conf->apply_changes(NULL);

	libproxy::ProxyClient *proxy = new libproxy::ProxyClient(cct);
	*cluster = (void *)proxy;

	cct->put();
	return 0;
}

extern "C" int proxy_create(proxy_t *cluster, const char* const id)
{
	CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
	if (id) {
		iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
	}

	int ret = proxy_create_common(cluster, "ceph", &iparams);
	return ret;
}

extern "C" int proxy_conf_read_file(proxy_t cluster, const char* path_list)
{
	libproxy::ProxyClient *client = (libproxy::ProxyClient *)cluster;
	md_config_t *conf = client->cct->_conf;

	int ret = conf->parse_config_files(path_list, NULL, 0);
	if (ret) {
		return ret;
	}

	conf->parse_env();
	conf->apply_changes(NULL);
	client->cct->_conf->complain_about_parse_errors(client->cct);
	return 0;
}


libproxy::AioCompletion::AioCompletion():rval(0), completed(false), outdata(NULL)
{
	lock = new Mutex("libproxy::AioCompletion");
	cond = new Cond();
}

bool libproxy::AioCompletion::is_complete()
{
	lock->Lock();
	bool comp = completed;
	lock->Unlock();
	return comp;
}

int libproxy::AioCompletion::wait_for_complete()
{
	lock->Lock();
	cond->Wait(*lock);
	lock->Unlock();
	return rval;
}

void libproxy::AioCompletion::complete(int r)
{
	lock->Lock();
	rval = r;
	completed = true;
	cond->Signal();
	lock->Unlock();
}

void libproxy::AioCompletion::set_data(bufferlist &bl) 
{
	outdata = &bl;
}

void libproxy::AioCompletion::flush(bufferlist &bl) 
{
	if (outdata) {
		outdata->claim(bl);
		outdata = NULL;
	}
}

libproxy::RadosProxy::RadosProxy():client(NULL)
{
	//TODO
}

libproxy::RadosProxy::~RadosProxy()
{
	shutdown();
}

int libproxy::RadosProxy::init(const char* const id)
{
	return proxy_create((proxy_t *)&client, id);
}

int libproxy::RadosProxy::init_with_context(config_t _cct)
{
	//TODO
	return 0;
}

int libproxy::RadosProxy::connect()
{
	return client->connect();
}

void libproxy::RadosProxy::shutdown()
{
	if (!client) {
		return;
	}
	if (client->put()) {
		client->shutdown();
		delete client;
		client = NULL;
	}
}

int libproxy::RadosProxy::conf_read_file(const char* const path) const
{
	return proxy_conf_read_file((proxy_t)client, path);
}

int libproxy::RadosProxy::aio_read(int poolid, std::string oid, uint64_t off, uint64_t len,
	bufferlist &bl, AioCompletion *c)
{
	return client->aio_read(poolid, oid, off, len, bl, c);
}

int libproxy::RadosProxy::aio_write(int poolid, std::string oid, uint64_t off,
	bufferlist &bl, AioCompletion *c)
{
	return client->aio_write(poolid, oid, off, bl, c);
}
