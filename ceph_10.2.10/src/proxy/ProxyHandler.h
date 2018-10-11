#ifndef CEPH_PROXYHANDLER_H
#define CEPH_PROXYHANDLER_H

#include <string>
#include "include/encoding.h"

namespace librados{
	class Rados;
	class IoCtx;
};

class Context;

class ProxyCacheHandler {
private:
	librados::Rados *rados;
public:
	ProxyCacheHandler(librados::Rados *_rados);
	~ProxyCacheHandler(){}
	int read(uint64_t poolid, std::string oid, uint64_t offset, uint64_t len, 
		bufferlist &bl, Context *onfinish);
	int write(uint64_t poolid, std::string oid, uint64_t offset, bufferlist &bl, 
		Context *onfinish);
	
};

#endif //CEPH_PROXYHANDLER_H
