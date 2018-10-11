// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_LIBRBD_DUMP_OPS_WQ_H
#define CEPH_LIBRBD_DUMP_OPS_WQ_H


#include <map>
#include <list>
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "include/assert.h"
#include "include/utime.h"
#include <string>

namespace librbd {
class ImageCtx;
class DumpOpsWQ;
struct RbdOpStat;

class DumpOpsWQ{
	/*
	class QueueItem{
	public:
		RbdOpStat ros;
		utime_t ti;
	public:
    	explicit QueueItem(RbdOpStat ros, utime_t time) : ros(ros), ti(time){}
	
	};

	list<QueueItem> ros_queue;
	*/

	mutable Mutex lock;
  	Cond cond;
	CephContext *cct;

	class DumpOpsThread:public Thread{
		DumpOpsWQ *do_wq;
	public:
		explicit DumpOpsThread(DumpOpsWQ *do_wq) : do_wq(do_wq) {}
		void *entry(){
			do_wq->entry();
			return 0;
		}
	}dump_ops_thread;

public:
	void entry();
	void start();

	RbdOpStat* dequeue();
	void enqueue(RbdOpStat *ros);
	void process(RbdOpStat *ros);
	
	void cleanup(utime_t trim_time);
	
	void wait();
	void shutdown();
	DumpOpsWQ(ImageCtx *image_ctx,const string &n);

private:
	ImageCtx &m_image_ctx;
	bool stop;
	string name;
	std::list<RbdOpStat *> ros_queue;
	//std::map<utime_t,RbdOpStat *> ros_queue;

};


}

#endif



