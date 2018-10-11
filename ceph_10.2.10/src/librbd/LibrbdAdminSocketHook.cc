// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"

#include "librbd/ImageCtx.h"
#include "librbd/LibrbdAdminSocketHook.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbdadminsocket: "

namespace librbd {

class LibrbdAdminSocketCommand {
public:
  std::string format;
  virtual ~LibrbdAdminSocketCommand() {}
  virtual bool call(stringstream *ss) = 0;
};

class FlushCacheCommand : public LibrbdAdminSocketCommand {
public:
  explicit FlushCacheCommand(ImageCtx *ictx) : ictx(ictx) {}

  bool call(stringstream *ss) {
    int r = flush(ictx);
    if (r < 0) {
      *ss << "flush: " << cpp_strerror(r);
      return false;
    }
    return true;
  }

private:
  ImageCtx *ictx;
};

struct InvalidateCacheCommand : public LibrbdAdminSocketCommand {
public:
  explicit InvalidateCacheCommand(ImageCtx *ictx) : ictx(ictx) {}

  bool call(stringstream *ss) {
    int r = invalidate_cache(ictx);
    if (r < 0) {
      *ss << "invalidate_cache: " << cpp_strerror(r);
      return false;
    }
    return true;
  }

private:
  ImageCtx *ictx;
};

struct DumpOpsCommand : public LibrbdAdminSocketCommand {
public:
  explicit DumpOpsCommand(ImageCtx *_ictx) : ictx(_ictx) {}

  void cleanup(utime_t now){
    CephContext *cct = ictx->cct;
    double rbd_op_time_to_keep = cct->_conf->rbd_op_time_to_keep;
    uint32_t max_op_size = cct->_conf->rbd_max_dump_ops_size;
    ldout(ictx->cct ,20) << "DumpOpsCommand::cleanup rbd_op_time_to_keep = " << rbd_op_time_to_keep << dendl;

    while( ictx->can_trim_rbd_io_stat_op(now, rbd_op_time_to_keep, max_op_size) ) {
	  ictx->trim_rbd_io_stat_front_op();
    }
  }
  
  bool call(stringstream *ss){
    //string format = "json-pretty";

    utime_t now = ceph_clock_now(ictx->cct);
    cleanup(now);    
    Formatter *f = Formatter::create(format, "json-pretty", "json-pretty");

	std::string i_name = ictx->get_rbd_io_stat_image_name();
	std::string p_name = ictx->get_rbd_io_stat_pool_name();
	std::list<RbdOpStat> op_stats = ictx->get_rbd_io_stat_op_stats();
	
    f->open_object_section("DumpOps");
    f->dump_string("img_name", i_name);
    f->dump_string("pool_name", p_name);
    f->open_array_section("Ops");

    
    std::list<struct RbdOpStat>::iterator iter = op_stats.begin();

    for(; iter != op_stats.end(); iter++){
	  struct RbdOpStat op_stat = *iter;
	  f->open_object_section("Op");
	  f->dump_string("op" ,op_stat.op);
	  f->dump_float("latency", op_stat.latency);
	  f->dump_stream("complete_time") << op_stat.time;

	  f->open_array_section("oid_latency_map");
	  map<string, utime_t>::iterator oid_iter = iter->oid_latency.begin();
	  for(; oid_iter != iter->oid_latency.end(); oid_iter++){
		f->open_object_section("oid_latency");
		f->dump_string("oid", oid_iter->first);
		f->dump_float("oid_latency", oid_iter->second);
		f->close_section();
	  }
	  f->close_section();
	  
	  f->open_array_section("m_image_extents");
	  std::vector<std::pair<uint64_t,uint64_t> >::iterator i = iter->m_image_extents.begin();
	  for(; i!=iter->m_image_extents.end(); i++){
		f->open_object_section("image_extent");
		f->dump_int("offset" ,i->first);
		f->dump_int("len" ,i->second);
		f->close_section();
	  }
	  f->close_section();
	  
	  f->close_section();
		
    }
    f->close_section();
    f->close_section();
    
    f->flush(*ss);
    delete f;
    f = NULL;
    
    return true;
  }

private:
  ImageCtx *ictx;
};


LibrbdAdminSocketHook::LibrbdAdminSocketHook(ImageCtx *ictx) :
  admin_socket(ictx->cct->get_admin_socket()) {

  std::string command;
  std::string imagename;
  int r;

  imagename = ictx->md_ctx.get_pool_name() + "/" + ictx->name;
  command = "rbd cache flush " + imagename;

  r = admin_socket->register_command(command, command, this,
				     "flush rbd image " + imagename +
				     " cache");
  if (r == 0) {
    commands[command] = new FlushCacheCommand(ictx);
  }

  command = "rbd cache invalidate " + imagename;
  r = admin_socket->register_command(command, command, this,
				     "invalidate rbd image " + imagename + 
				     " cache");
  if (r == 0) {
    commands[command] = new InvalidateCacheCommand(ictx);
  }

  command = "dump_ops";
  r = admin_socket->register_command(command, command, this,
  					"dump recent ops");

  if (r == 0) {
	commands[command] = new DumpOpsCommand(ictx);
  }
}

LibrbdAdminSocketHook::~LibrbdAdminSocketHook() {
  for (Commands::const_iterator i = commands.begin(); i != commands.end();
       ++i) {
    (void)admin_socket->unregister_command(i->first);
    delete i->second;
  }
}

bool LibrbdAdminSocketHook::call(std::string command, cmdmap_t& cmdmap,
				 std::string format, bufferlist& out) {
  Commands::const_iterator i = commands.find(command);
  assert(i != commands.end());
  stringstream ss;
  i->second->format = format;
  bool r = i->second->call(&ss);
  out.append(ss);
  return r;
}

} // namespace librbd
