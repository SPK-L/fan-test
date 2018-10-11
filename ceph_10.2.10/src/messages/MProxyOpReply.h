#ifndef CEPH_MPROXYOPREPLY_H
#define CEPH_MPROXYOPREPLY_H

#include <atomic>
#include <string>

#include "MProxyOp.h"
#include "msg/Message.h"
#include "include/ceph_features.h"
#include "proxy/proxy_types.h"


class MProxyOpReply : public Message{
	static const int HEAD_VERSION = 1;
	static const int COMPAT_VERSION = 1;
private:
	ProxyOp op;
public:
	MProxyOpReply():Message(CEPH_MSG_PROXY_OP_REPLY, HEAD_VERSION, COMPAT_VERSION)
		{}
	MProxyOpReply(MProxyOp *req, ProxyOp _op):Message(CEPH_MSG_PROXY_OP_REPLY, HEAD_VERSION, COMPAT_VERSION),
		op(_op){
			set_tid(req->get_tid());
		}
private:
	~MProxyOpReply(){}
public:
	virtual void encode_payload(uint64_t features) {
		data.claim(op.outdata);
		header.version = HEAD_VERSION;
		::encode(op, payload);
	}
	virtual void decode_payload() {
		op.outdata.claim(data);
		bufferlist::iterator p = payload.begin();
		if (header.version == HEAD_VERSION) {
			::decode(op, p);
		}
	}
	const char *get_type_name() const { return "proxy_op_reply"; }
	ProxyOp* get_op(){return &op;}
};


#endif //CEPH_MPROXYOPREPLY_H
