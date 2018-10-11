#ifndef CEPH_MPROXYOP_H
#define CEPH_MPROXYOP_H

#include <atomic>
#include <string>

#include "msg/Message.h"
#include "include/ceph_features.h"
#include "proxy/proxy_types.h"


class MProxyOp : public Message {
	static const int HEAD_VERSION = 1;
	static const int COMPAT_VERSION = 1;
private:
	ProxyOp op;
public:
	MProxyOp():Message(CEPH_MSG_PROXY_OP, HEAD_VERSION, COMPAT_VERSION)
		{}
	MProxyOp(ceph_tid_t tid, ProxyOp _op):Message(CEPH_MSG_PROXY_OP, HEAD_VERSION, COMPAT_VERSION),
		op(_op){
			set_tid(tid);
		}
private:
	~MProxyOp(){}
public:
	friend class MProxyOpReply;
	virtual void encode_payload(uint64_t features) {
		data.claim(op.indata);
		header.version = HEAD_VERSION;
		::encode(op, payload);
	}

	virtual void decode_payload() {
		op.indata.claim(data);
		bufferlist::iterator p = payload.begin();
		if (header.version == HEAD_VERSION) {
			::decode(op, p);
		}
	}
	const char *get_type_name() const { return "proxy_op"; }
	ProxyOp* get_op() {return &op;}

	
};

#endif //CEPH_MPROXYOP_H
