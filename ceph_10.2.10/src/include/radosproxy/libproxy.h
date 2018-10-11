#ifndef CEPH_LIBPROXY_H
#define CEPH_LIBPROXY_H
extern "C"{
#if __GNUC__ >= 4
  #define CEPH_PROXY_API    __attribute__ ((visibility ("default")))
#else
  #define CEPH_PROXY_API
#endif

typedef void* proxy_t;

CEPH_PROXY_API int proxy_create(proxy_t *cluster, const char* const id);
CEPH_PROXY_API int proxy_conf_read_file(proxy_t cluster, const char* path_list);
}

#endif