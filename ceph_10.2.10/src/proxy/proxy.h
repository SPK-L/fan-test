#ifndef CEPH_PROXY_H
#define CEPH_PROXY_H
#include <string>
#include <set>
#include <map>

#include "common/ceph_context.h"
#include "msg/Dispatcher.h"
#include "common/config_obs.h"
#include "include/Context.h"

#define CEPH_PROXY_PROTOCOL    10 /* cluster internal */
#define CEPH_PRXOY_PORT 6790

namespace librados{
	class Rados;
	class IoCtx;
};

class Messenger;
class Message;
class MonClient;
struct Connection;
class Objecter;
class ProxyOp;
class MProxyOp;
class ProxyCacher;
class ProxyCacheHandler;

class Proxy: public Dispatcher,
                public md_config_obs_t{
protected:
	Messenger *cluster_messenger;
	Messenger *client_messenger;
	
private:
	//MonClient *mc;
	//Objecter *objecter;
	// use Rados replace of Objecter for now
	librados::Rados *rados;
	ProxyCacheHandler *cacher_handler;
	ProxyCacher *proxy_cacher;
	std::map<uint64_t, librados::IoCtx*> ioctxs;
	
	bool ms_can_fast_dispatch_any() const {return true;}
	bool ms_can_fast_dispatch(Message *m) const {return true;}
	void ms_fast_dispatch(Message *m);
	void ms_fast_preprocess(Message *m);
	bool ms_dispatch(Message *m);
	bool ms_get_authorizer(int dest_type, AuthAuthorizer **a, bool force_new);
	bool ms_verify_authorizer(Connection *con, int peer_type, int protocol, 
		ceph::bufferlist &authorizer, ceph::bufferlist &authorizer_reply, bool &isvalid, 
		CryptoKey &session_key);
	void ms_handle_connect(Connection *con);
	void ms_handle_fast_connect(Connection *con);
	void ms_handle_fast_accept(Connection *con);
	bool ms_handle_reset(Connection *con);
	void ms_handle_remote_reset(Connection *con);
	void send_message_proxy_client(Message *m, const ConnectionRef &con) {
		con->send_message(m);
	}

public:
	class C_ProxyOpFinish : public Context {
	private:
		Proxy *proxy;
		MProxyOp *m;
	public:
		bufferlist data;
		C_ProxyOpFinish(Proxy *_p, MProxyOp *req):proxy(_p), m(req){}
		~C_ProxyOpFinish(){}

		void finish(int r);
	};

	Proxy(CephContext *_cct, Messenger *_cluster_messenger, Messenger *_client_messenger);
	virtual ~Proxy(){}
	int init();
	void handle_signal(int signum);
	int shutdown();
	virtual const char **get_tracked_conf_keys() const;
	void handle_conf_change(const struct md_config_t *conf, const std::set<std::string> &changed);
	void do_proxy_op(MProxyOp *m);
	int do_read(ProxyOp *req, C_ProxyOpFinish *fin);
	int do_write(ProxyOp *req, C_ProxyOpFinish *fin);
};

#endif
