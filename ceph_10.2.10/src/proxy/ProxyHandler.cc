#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/debug.h"

#include "ProxyHandler.h"

#define dout_subsys ceph_subsys_proxy

ProxyCacheHandler::ProxyCacheHandler(librados::Rados *_rados):rados(_rados)
{
}

int ProxyCacheHandler::read(uint64_t poolid, std::string oid, uint64_t offset, uint64_t len, 
	bufferlist &bl, Context *onfinish)
{
	assert(rados);
	librados::ObjectReadOperation op;
	librados::IoCtx ioctx;
	int r = rados->ioctx_create2(poolid, ioctx);
	if (r < 0) {
		dout(0) << "failed to create ioctx for poolid=" << poolid << ", r=" << r << dendl;
		onfinish->complete(r);
		return r;
	}

	op.read(offset, len, &bl, NULL);
	r = ioctx.operate(oid.c_str(), &op, NULL);
	if (r < 0) {
		dout(0) << "failed to read object=" << oid << ", r=" << r << dendl;
		onfinish->complete(r);
		return r;
	}

	dout(20) << "read object=" << oid << " off=" << offset << " len=" << len << dendl;
	onfinish->complete(0);
	return 0;
}

int ProxyCacheHandler::write(uint64_t poolid, std::string oid, uint64_t offset, 
	bufferlist &bl, Context *onfinish)
{
	assert(rados);
	librados::ObjectWriteOperation op;
	librados::IoCtx ioctx;
	int r = rados->ioctx_create2(poolid, ioctx);
	if (r < 0) {
		dout(0) << "failed to create ioctx for poolid=" << poolid << ", r=" << r << dendl;
		onfinish->complete(r);
		return r;
	}

	op.write(offset, bl);
	r = ioctx.operate(oid.c_str(), &op);
	if (r < 0) {
		dout(0) << "failed to write object=" << oid << ", r=" << r << dendl;
		onfinish->complete(r);
		return r;
	}

	dout(20) << "write object=" << oid << " off=" << offset << dendl;
	onfinish->complete(0);
	return 0;
}

