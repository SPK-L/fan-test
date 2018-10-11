namespace librbd {

struct RbdOpStat {

	  string op;
	  double latency;
	  utime_t time;
	  std::vector<std::pair<uint64_t,uint64_t> > m_image_extents;
	  std::map<string ,utime_t> oid_latency;

	  public:
	  	RbdOpStat(){}
    };

struct RbdIoStat {
	  string pool_name;
	  string image_name;
	  std::list<RbdOpStat> op_stats;
    };

}
