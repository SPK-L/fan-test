#ifndef CEPH_PROXY_TYPE_H
#define CEPH_PROXY_TYPE_H
#include <memory>
#include <string>

#include "include/object.h"
#include "include/radosproxy.h"
#include "include/encoding.h"

struct ProxyOp{
	ceph_proxy_op op;
	int poolid;
	std::string oid;
	bufferlist indata, outdata;
	int32_t rval;

	ProxyOp():rval(0) {
		memset(&op, 0, sizeof(ceph_proxy_op));
	}

	int get_type(){
		return op.op;
	}

	void encode(bufferlist &bl) const {
		ENCODE_START(1, 1, bl);
		::encode(op, bl);
		::encode(poolid, bl);
		::encode(oid, bl);
		::encode(rval, bl);
		ENCODE_FINISH(bl);
	}

	void decode(bufferlist::iterator &bl) {
		DECODE_START(1, bl);
		::decode(op, bl);
		::decode(poolid, bl);
		::decode(oid, bl);
		::decode(rval, bl);
		DECODE_FINISH(bl);
	}
};
WRITE_CLASS_ENCODER(ProxyOp);


#endif //CEPH_PROXY_TYPE_H
