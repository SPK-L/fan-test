#ifndef CEPH_PROXIER_H
#define CEPH_PROXIRER_H

#include <set>
#include <string>
#include <map>
#include <vector>

#include "common/ceph_context.h"
#include "common/config_obs.h"
#include "common/Cond.h"
#include "common/Mutex.h"

#include "include/types.h"
#include "include/Context.h"
#include "include/buffer_fwd.h"
#include "proxy/proxy_types.h"

#include "msg/Dispatcher.h"
#include "msg/msg_types.h"
#include "messages/MProxyOp.h"
#include "messages/MProxyOpReply.h"

class Context;
class Messenger;
class Message;
struct Connection;

namespace libproxy{
	class AioCompletion;
};

class OnAck : public Context {
private:
	CephContext *cct;
	libproxy::AioCompletion *c;
	
public:
	OnAck(CephContext *_cct):cct(_cct),   c(NULL){}
	OnAck(CephContext *_cct, libproxy::AioCompletion *_c):cct(_cct), c(_c){}
	~OnAck(){}
	void flush(bufferlist &bl);
protected:
	virtual void finish(int r);
};

class OnCommit : public Context {
private:
	CephContext *cct;
public:
	OnCommit(CephContext *_cct):cct(_cct){}
	~OnCommit(){}
protected:
	virtual void finish(int r);

};

class Proxier : public md_config_obs_t, public Dispatcher {
public:
	virtual const char** get_tracked_conf_keys() const;
	virtual void handle_conf_change(const struct md_config_t *conf, const std::set<std::string> &changed);
private:
	Messenger *messenger;
	double proxy_timeout;
	entity_inst_t target;
	atomic64_t last_tid;
	Mutex proxier_lock;
	struct ProxySession;
	ProxySession *homeless_session;

	struct op_target_t {
		int proxy;

		op_target_t(int p):proxy(p) {}
		~op_target_t(){}
	};
	
	struct Op : public RefCountedObject{
		op_target_t target;
		ceph_tid_t tid;
		OnAck *onack;
		OnCommit *oncommit;
		ProxySession *session;
		ConnectionRef con;
		ProxyOp proxy_op;

		Op(op_target_t _target, OnAck *oa):target(_target), tid(0), onack(oa),
			oncommit(NULL), session(NULL), con(NULL){}
		Op(op_target_t _target, OnAck *oa, OnCommit *oc):target(_target), tid(0),
			onack(oa), oncommit(oc), session(NULL), con(NULL){}
		~Op(){}

		ProxyOp& add_op(int op) {
			proxy_op.op.op = op;
			return proxy_op;
		}
		void add_data(int op, int poolid, std::string oid, uint64_t off, uint64_t len, 
			bufferlist &bl) {
			ProxyOp& _op = add_op(op);
			_op.poolid = poolid;
			_op.oid = oid;
			_op.op.offset = off;
			_op.op.length = len;
			_op.indata.claim_append(bl);
		}
		
	};

	struct ProxySession : public RefCountedObject {
		int proxy;
		ConnectionRef con;
		std::map<ceph_tid_t, Op*> ops;
		ProxySession(CephContext *cct, int p):RefCountedObject(cct), proxy(p){}
		~ProxySession(){}

		bool is_homeless() {return (proxy == -1);}
	};
	std::map<int, ProxySession*> proxy_sessions;

	//messages
	bool ms_dispatch(Message *m);
	bool ms_can_fast_dispatch_any() const;
	bool ms_can_fast_dispatch(Message *m) const;
	void ms_fast_dispatch(Message *m);
	void ms_handle_connect(Connection *con);
	bool ms_handle_reset(Connection *con);
	void ms_handle_remote_reset(Connection *con);
	bool ms_get_authorizer(int dest_type, AuthAuthorizer **a, bool force_new);
	void lock();
	void unlock();
	int op_submit(Op *op);
	void get_session(ProxySession *s);
	void put_session(ProxySession *s);
	void close_session(ProxySession *s);
	int _get_session(int proxy, ProxySession **session);
	void _finish_op(Op *op, int r);
	void _session_op_assign(ProxySession *to, Op *op);
	void _session_op_remove(ProxySession *from, Op *op);
	MProxyOp* _prepare_proxy_op(Op *op);
	void _send_op(Op *op, MProxyOp *m);
public:
	Proxier(CephContext *_cct, Messenger *m, double _proxy_timeout, entity_inst_t to);
	~Proxier();

	void init();
	void start();
	void shutdown();
	void handle_proxy_op_reply(MProxyOpReply *m);
	int aio_read(int poolid, std::string oid, uint64_t off, uint64_t len, 
		bufferlist &bl, libproxy::AioCompletion *c);
	int aio_write(int poolid, std::string oid, uint64_t off,
		bufferlist &bl, libproxy::AioCompletion *c);
	
};

#endif //CEPH_PROXIER_H
