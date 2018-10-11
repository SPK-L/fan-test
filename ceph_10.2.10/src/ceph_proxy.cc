#include <iostream>
#include <vector>
#include <stdlib.h>
#include <string>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/pick_address.h"
#include "common/errno.h"

#include "global/global_init.h"
#include "global/signal_handler.h"
#include "global/global_context.h"

#include "msg/Messenger.h"

#include "include/ceph_features.h"
#include "include/color.h"

#include "proxy/proxy.h"

using namespace std;

Proxy *proxy = NULL;

void handle_proxy_signal(int signum) {
	if (proxy) {
		proxy->handle_signal(signum);
	}
}

void usage()
{
	cout << "usage: ceph-proxy -i <proxyid>\n"
	     << "  --proxy-data PATH data directory\n"
	     << "  --debug_proxy <N>  set debug level (e.g. 10)\n"
		 << std::endl;
	generic_server_usage();
}


int main(int argc, const char **argv) {
	//do_rbd_test();
	
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	vector<const char*> def_args;
	// No default arguments
	auto cct = global_init(&def_args, args, CEPH_ENTITY_TYPE_PROXY, CODE_ENVIRONMENT_DAEMON, 0);

	string val;
	for(vector<const char *>::iterator i = args.begin(); i != args.end(); i++) {
		if (ceph_argparse_double_dash(args, i)) {
			break;
		} else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
			usage();
			exit(0);
		}
	}

	//who am i
	char *end;
	const char *id = g_conf->name.get_id().c_str();
	int whoami = strtol(id, &end, 10);
	if (*end || end == id || whoami < 0) {
		derr << "must specify '-i #' where # is the proxy number" << dendl;
		usage();
	}

	if (g_conf->proxy_data.empty()) {
		derr << "must specify '--proxy-data=foo' data path" << dendl;
		usage();
	}

	entity_addr_t ipaddr;

	pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC | CEPH_PICK_ADDRESS_CLUSTER);
	if (g_conf->public_addr.is_blank_ip() && !g_conf->cluster_addr.is_blank_ip()) {
		derr << TEXT_YELLOW
	 	 << " ** WARNING: specified cluster addr but not public addr; we recommend **\n"
	 	 << " **          you specify neither or both.                             **"
	 	 << TEXT_NORMAL << dendl;
	}
	ipaddr = g_conf->public_addr;
	ipaddr.set_port(CEPH_PRXOY_PORT);
	/*
	Messenger *ms_public = Messenger::create(g_ceph_context, g_conf->ms_type,
					   entity_name_t::PROXY(whoami), "client",
					   getpid());
	*/
	// use 0 fro nonce, otherwise the client has on way to get the nonce
	Messenger *ms_public = Messenger::create(g_ceph_context, g_conf->ms_type,
					   entity_name_t::PROXY(whoami), "client", 0);
	Messenger *ms_cluster = Messenger::create(g_ceph_context, g_conf->ms_type,
					    entity_name_t::PROXY(whoami), "cluster",
					    getpid());
	ms_public->set_cluster_protocol(CEPH_PROXY_PROTOCOL);
	ms_cluster->set_cluster_protocol(CEPH_PROXY_PROTOCOL);
	cout << "starting proxy." << whoami
		 << " at " << ipaddr
		 << " proxy_data " << g_conf->proxy_data
		 << std::endl;

	uint64_t supported =
    CEPH_FEATURE_UID | 
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_MSG_AUTH |
    CEPH_FEATURE_OSD_ERASURE_CODES;

    ms_public->set_default_policy(Messenger::Policy::stateless_server(supported, 0));
    /*
    ms_public->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				   client_byte_throttler.get(),
				   client_msg_throttler.get());
    */
    ms_public->set_policy(entity_name_t::TYPE_MON,
                               Messenger::Policy::lossy_client(supported,
							       CEPH_FEATURE_UID |
							       CEPH_FEATURE_PGID64 |
							       CEPH_FEATURE_OSDENC));
    //try to poison pill any Proxy connections on the wrong address
    ms_public->set_policy(entity_name_t::TYPE_OSD,
			Messenger::Policy::stateless_server(0,0));

    ms_cluster->set_default_policy(Messenger::Policy::stateless_server(0, 0));
    ms_cluster->set_policy(entity_name_t::TYPE_MON, Messenger::Policy::lossy_client(0,0));
    ms_cluster->set_policy(entity_name_t::TYPE_CLIENT,
			 Messenger::Policy::stateless_server(0, 0));
	int r = ms_public->bind(ipaddr);
	if (r < 0) {
		exit(1);
	}

	r = ms_cluster->bind(g_conf->cluster_addr);
	if (r < 0) {
		exit(1);
	}

	// Set up crypto, daemonize, etc.
    global_init_daemonize(g_ceph_context);
	common_init_finish(g_ceph_context);
	
	proxy = new Proxy(g_ceph_context, ms_cluster, ms_public); 

	ms_public->start();
	ms_cluster->start();

	r = proxy->init();
	if (r < 0) {
      derr << TEXT_RED << " ** ERROR: proxy init failed: " << cpp_strerror(-r)
         << TEXT_NORMAL << dendl;
      return 1;
    }

	// install signal handlers
	init_async_signal_handler();
	register_async_signal_handler(SIGHUP, sighup_handler);
	register_async_signal_handler_oneshot(SIGINT, handle_proxy_signal);
	register_async_signal_handler_oneshot(SIGTERM, handle_proxy_signal);

	if (g_conf->inject_early_sigterm)
    	kill(getpid(), SIGTERM);

	ms_public->wait();
	ms_cluster->wait();

	unregister_async_signal_handler(SIGHUP, sighup_handler);
    unregister_async_signal_handler(SIGINT, handle_proxy_signal);
    unregister_async_signal_handler(SIGTERM, handle_proxy_signal);
    shutdown_async_signal_handler();

	g_ceph_context->put();
	delete proxy;
    return 0;
}
