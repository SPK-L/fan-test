#include "msg/Messenger.h"
#include "msg/Message.h"
#include "msg/Connection.h"
#include "messages/MProxyOpReply.h"

#include "common/dout.h"
#include "common/errno.h"
#include "common/config.h"
#include "common/ceph_context.h"

#include "include/radosproxy/libproxy.hpp"

#include "auth/Auth.h"

#include "Proxier.h"

#define dout_subsys ceph_subsys_proxier

using namespace std;

void OnAck::flush(bufferlist &bl)
{
	if(c) {
		ldout(cct, 20) << "flushing data=" << bl.c_str() << dendl;
		c->flush(bl);
	}
}

void OnAck::finish(int r)
{
	ldout(cct, 20) << "proxy op acked" << dendl;
	if (c) {
		c->complete(r);
	}
}

void OnCommit::finish(int r)
{
	ldout(cct, 20) << "proxy op committed" << dendl;
}

Proxier::Proxier(CephContext *_cct, Messenger *m, double _proxy_timeout, entity_inst_t to):
	Dispatcher(_cct), messenger(m), proxy_timeout(_proxy_timeout), target(to), last_tid(0),
	proxier_lock("Proxier"), homeless_session(new ProxySession(cct, -1))
{
	//TODO
}

Proxier::~Proxier()
{
	//TODO
	return;
}

const char** Proxier::get_tracked_conf_keys() const 
{
	//TODO
	static const char* KEYS[] = {
		NULL,
	};
	return KEYS;	
}

void Proxier::handle_conf_change(const struct md_config_t *conf, 
	const std::set<std::string > &changed)
{
	//TODO
	return;
}

void Proxier::init()
{
	//TODO
	return;
}

void Proxier::start()
{
	//TODO
	return;
}

void Proxier::shutdown()
{
	//TODO
	return;
}

int Proxier::op_submit(Op *op)
{
	Mutex::Locker l(proxier_lock);
	ProxySession *s = NULL;
	ldout(cct, 20) << __func__ << dendl;
	int r = _get_session(op->target.proxy, &s);
	assert(r == 0);
	assert(s);

	MProxyOp *m = NULL;
	if (op->tid == 0) {
		op->tid = last_tid.inc();
	}

	_session_op_assign(s, op);

	_send_op(op, m);

	op = NULL;
	put_session(s);
	return 0;
}

//messages
bool Proxier::ms_dispatch(Message *m)
{
	ldout(cct, 20) << "proxier ms_dispatch" << dendl;
	ldout(cct, 20) << "ms_type" << m->get_type() << dendl;
	switch(m->get_type()) {
		case CEPH_MSG_PROXY_OP_REPLY:
			handle_proxy_op_reply((MProxyOpReply *)m);
			return true;
	}
	return false;
}

bool Proxier::ms_can_fast_dispatch_any() const
{
	//TODO
	return true;
}

bool Proxier::ms_can_fast_dispatch(Message *m) const
{
	//TODO
	return true;
}

void Proxier::ms_fast_dispatch(Message *m)
{
	ms_dispatch(m);
}

void Proxier::ms_handle_connect(Connection *con)
{
	//TODO
	return;
}

bool Proxier::ms_handle_reset(Connection *con)
{
	//TODO
	return true;
}

void Proxier::ms_handle_remote_reset(Connection *con)
{
	//TODO
	return;
}

bool Proxier::ms_get_authorizer(int dest_type, AuthAuthorizer **a, bool force_new)
{
	//TODO
	return true;
}

void Proxier::lock()
{
	proxier_lock.Lock(true);
}

void Proxier::unlock()
{
	proxier_lock.Unlock();
}

int Proxier::_get_session(int proxy, ProxySession **session)
{
	if (proxy < 0) {
		*session = homeless_session;
		return 0;
	}

	map<int, ProxySession*>::iterator p = proxy_sessions.find(proxy);
	if (p != proxy_sessions.end()) {
		ldout(cct, 20) << __func__ << " using existing session" << dendl;
		ProxySession *s = p->second;
		s->get();
		*session = s;
		return 0;
	}

	ldout(cct, 20) << __func__ << " new session" << dendl;
	ProxySession *s = new ProxySession(cct, proxy);
	proxy_sessions[proxy] = s;
	s->con = messenger->get_connection(target);
	s->get();
	*session = s;
	return 0;
}

void Proxier::_finish_op(Op *op, int r)
{
	ldout(cct, 20) << __func__ << dendl;
	_session_op_remove(op->session, op);
	op->put();
}

MProxyOp* Proxier::_prepare_proxy_op(Op *op)
{
	assert(op->tid);
	MProxyOp *m = new MProxyOp(op->tid, op->proxy_op);
	return m;
}

void Proxier::_send_op(Op *op, MProxyOp *m)
{
	if (!m) {
		m = _prepare_proxy_op(op);
	}

	ConnectionRef con = op->session->con;
	assert(con);
	con->send_message(m);
	return;
}


void Proxier::_session_op_assign(ProxySession *to, Op *op)
{
	assert(op->session == NULL);
	assert(op->tid);

	get_session(to);
	op->session = to;
	to->ops[op->tid] = op;
}

void Proxier::_session_op_remove(ProxySession *from, Op *op)
{
	if (!from->is_homeless()) {
		from->ops.erase(op->tid);
		put_session(from);
		op->session = NULL;
	}
}

void Proxier::get_session(ProxySession *s)
{
	if (s && !s->is_homeless()) {
		ldout(cct, 20) << __func__ << " s=" << s << " proxy=" << s->proxy << " "
			<< s->get_nref() << dendl;
		s->get();
	}
}

void Proxier::put_session(ProxySession *s)
{
	if (s && !s->is_homeless()) {
		ldout(cct, 20) << __func__ << " s=" << s << " proxy=" << s->proxy << " "
			<< s->get_nref() << dendl;
		s->put();
	}
}

void Proxier::close_session(ProxySession *s)
{
	return;
}

void Proxier::handle_proxy_op_reply(MProxyOpReply *m)
{
	ceph_tid_t tid = m->get_tid();
	int proxy = (int)m->get_source().num();

	lock();
	ldout(cct, 20) << __func__  << "got op:" << tid << "from proxy " << proxy << dendl;

	map<int, ProxySession*>::iterator p = proxy_sessions.find(proxy);
	if (p == proxy_sessions.end()) {
		ldout(cct, 20) << "find no session" << dendl;
		m->put();
		unlock();
		return;
	}

	ProxySession *s = p->second;
	ldout(cct, 20) << "session=" << s << " existing, ops size=" << s->ops << dendl;
	get_session(s);

	map<ceph_tid_t, Op*>::iterator iter = s->ops.find(tid);
	if (iter == s->ops.end()) {
		put_session(s);
		m->put();
		unlock();
		return;
	}

	Op *op = iter->second;
	ldout(cct, 20) << "find op for tid=" << tid << ", op=" << op << dendl;
	OnAck *onack = op->onack;
	OnCommit *oncommit = op->oncommit;

	ProxyOp *proxy_op = m->get_op();
	if (onack) {
		ldout(cct, 20) << "got data len=" << proxy_op->op.length << dendl;
		onack->flush(proxy_op->outdata);
	}
	
	_finish_op(op, 0);
	ldout(cct, 20) << "message proxy reply get_nref=" << m->get_nref() << dendl;
	m->put();
	
	put_session(s);
	unlock();
	if (onack) {
		onack->complete(0);
	}

	if (oncommit) {
		oncommit->complete(0);
	}
	ldout(cct, 20) << __func__ << " finished" << dendl;
}

int Proxier::aio_read(int poolid, std::string oid, uint64_t off, uint64_t len, 
	bufferlist &bl, libproxy::AioCompletion *c)
{
	bufferlist t;
	c->set_data(bl);
	op_target_t target(0);
	OnAck *ack = new OnAck(cct, c);
	Op *op = new Op(target, ack);

	ldout(cct, 20) << __func__ << dendl;
	op->add_data(CEPH_PROXY_FLAG_READ, poolid, oid, off, len, t);
	int r = op_submit(op);
	if (r < 0) {
		ldout(cct, 0) << "failed to submit op, r=" << r << dendl;
		return r;
	}
	ldout(cct, 20) << "aio_read waiting for ack" << dendl;
	return 0;
}

int Proxier::aio_write(int poolid, std::string oid, uint64_t off, 
	bufferlist &bl, libproxy::AioCompletion *c)
{
	op_target_t target(0);
	OnAck *ack = new OnAck(cct, c);
	Op *op = new Op(target, ack);

	ldout(cct, 20) << __func__ << dendl;
	op->add_data(CEPH_PROXY_FLAG_WRITE, poolid, oid, off, 0, bl);
	int r = op_submit(op);
	if (r < 0) {
		ldout(cct, 0) << "failed to submit op, r=" << r << dendl;
		return r;
	}
	ldout(cct, 20) << "aio_write waiting for ack" << dendl;
	
	return 0;
}


