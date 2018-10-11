// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::DumpOpsWQ: "

#include "librbd/DumpOpsWQ.h"
#include "librbd/ImageCtx.h"    //DumpOps.h


namespace librbd {
	DumpOpsWQ::DumpOpsWQ(ImageCtx *image_ctx,const string &n)
		:m_image_ctx(*image_ctx),name(n),stop(false),
		 dump_ops_thread(this),lock("Librbd::DumpOpsQueue::lock"){
		cct = m_image_ctx.cct;
	}

	void DumpOpsWQ::start(){
		assert(!stop);
		assert(!dump_ops_thread.is_started());
		ldout(cct ,0) << "DumpOpsWQ dump_ops_thread start" << dendl;
		dump_ops_thread.create("dump_ops_thread");
	}

	void DumpOpsWQ::wait(){
		dump_ops_thread.join();
	}

	void DumpOpsWQ::enqueue(RbdOpStat *ros){
		Mutex::Locker l(lock);
		ldout(cct,30) << "DumpOpsWQ enqueue at time " << ros->time << dendl;
		ros_queue.push_back(ros);
		std::map< string, utime_t>::iterator iter = ros->oid_latency.begin();
      	for(; iter!=ros->oid_latency.end(); iter++){
			ldout(cct ,30) << "DumpOpsWQ::enqueue : operator is "
			               << ros->op << ", oid_latency_map " << iter->first << " " << iter->second << dendl;
      	}
		cond.Signal();
	}

	RbdOpStat* DumpOpsWQ::dequeue(){
		//Mutex::Locker l(lock);
		//if(ros_queue.empty())
		//	return NULL;
		ldout(cct,30) << "Begin DumpOpsWQ::dequeue" << dendl;
		RbdOpStat* item = ros_queue.front();
		ldout(cct,30) << "DumpOpsWQ dequeue to deal with time at " << item->time << dendl;
		ros_queue.pop_front();
		return item;
	}

	void DumpOpsWQ::cleanup(utime_t trim_time){
		double rbd_op_time_to_keep = cct->_conf->rbd_op_time_to_keep;
    	uint32_t max_op_size = cct->_conf->rbd_max_dump_ops_size;   

    	bool can_trim = m_image_ctx.can_trim_rbd_io_stat_op(trim_time ,rbd_op_time_to_keep ,max_op_size);
    	ldout(cct ,30) << "DumpOpsWQ::cleanup rbd_op_time_to_keep = " << rbd_op_time_to_keep 
    				   << ", wheather to trim ris " << can_trim << dendl;
	
		while( m_image_ctx.can_trim_rbd_io_stat_op(trim_time ,rbd_op_time_to_keep ,max_op_size) ) {
	  		m_image_ctx.trim_rbd_io_stat_front_op();
		}
	}
	
	void DumpOpsWQ::process(RbdOpStat *ros){
	    ldout(cct ,30) << "Begin DumpOpsWQ::process" << dendl;
	    cleanup(ros->time);
		m_image_ctx.insert_rbd_io_stat_op(ros);
		
	}

	void DumpOpsWQ::entry(){
		ldout(cct ,20) << "Begin DumpOpsWQ::entry" << dendl;
		lock.Lock();
		while (true){
			while(!ros_queue.empty()){
				RbdOpStat* item = dequeue();	
				utime_t trim_time = item->time;
				lock.Unlock();
				process(item);
				utime_t now = ceph_clock_now(cct);
				utime_t elapsed = now - trim_time;
				ldout(cct ,20) << "DumpOpsWQ::entry enqueue--dequeue--process insert and cleanup ris cost time " 
							   << elapsed << dendl;
				lock.Lock();
			}
			if (stop)
				break;
			cond.Wait(lock);
		}
		lock.Unlock();
	}

	void DumpOpsWQ::shutdown(){
		ldout(cct ,0) << "DumpOpsWQ dump_ops_thread shutdown" << dendl;
		lock.Lock();
		stop = true;
		cond.Signal();
		lock.Unlock();
	}
		
}    // namespace librbd













