#ifndef CEPH_RADOSPROXY_H
#define CEPH_RADOSPROXY_H

#include "include/encoding.h"

enum {
	CEPH_PROXY_FLAG_READ  =   0x0001,
	CEPH_PROXY_FLAG_WRITE =   0x0002,
};

struct ceph_proxy_op {
	int op;
	uint64_t offset;
	uint64_t length;
	void encode(bufferlist &bl) const {
		ENCODE_START(1, 1, bl);
		::encode(op, bl);
		::encode(offset, bl);
		::encode(length, bl);
		ENCODE_FINISH(bl);
	}

	void decode(bufferlist::iterator &bl) {
		DECODE_START(1, bl);
		::decode(op, bl);
		::decode(offset, bl);
		::decode(length, bl);
		DECODE_FINISH(bl);
	}

};
WRITE_CLASS_ENCODER(ceph_proxy_op);
#endif //CEPH_RADOSPROXY_H
