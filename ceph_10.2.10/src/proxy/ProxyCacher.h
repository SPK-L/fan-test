#ifndef CEPH_PROXYCACHER_H
#define CEPH_PROXYCACHER_H

#include "include/types.h"
#include "include/lru.h"
#include "include/Context.h"
#include "include/xlist.h"
#include "include/atomic.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"

using namespace std;

class CephContext;
class ProxyCacheHandler;

class ProxyCacher {
public:
	class C_ReadFinish;
	class Object;
	struct ProxyRead{
		bufferlist *data;
		uint64_t poolid;
		string oid;
		uint64_t offset;
		uint64_t length;

		ProxyRead():data(NULL){}
		ProxyRead(uint64_t pool, string id, uint64_t off, uint64_t len, bufferlist *bl):
			data(bl), poolid(pool), oid(id), offset(off), length(len){}
	};

	struct ProxyWrite{
		bufferlist *data;
		uint64_t poolid;
		string oid;
		uint64_t offset;

		ProxyWrite():data(NULL){}
		ProxyWrite(uint64_t pool, string id, uint64_t off, bufferlist *bl):
			data(bl), poolid(pool), oid(id), offset(off){}
	};

	class BufferHead  {
	public:
		static const int STATE_MISSING = 0;
		static const int STATE_CLEAN = 1;
		static const int STATE_ZERO = 2;
		static const int STATE_DIRTY = 3;
		static const int STATE_RX = 4;
		static const int STATE_WX = 5;
		static const int STATE_ERROR = 6;
	private:
		int state;
		struct {
			loff_t start, length;
		} ex;
	public:
		Object *object;
		bufferlist data;
		BufferHead(Object *o):state(STATE_ZERO), object(o){}
		loff_t length() const {return ex.length;}
		void set_length(loff_t len) {ex.length = len;}
		loff_t start() const {return ex.start;}
		void set_start(loff_t off) {ex.start = off;}
		loff_t end() const {return ex.start + ex.length;}
		loff_t last() const {return end() - 1;}
		
		void set_state(int s) {state = s;}
		int get_state() const {return state;}
		bool is_missing() const {return state == STATE_MISSING;}
		bool is_clean() const {return state == STATE_CLEAN;}
		bool is_zero() const {return state == STATE_ZERO;}
		bool is_dirty() const {return state == STATE_DIRTY;}
		bool is_rx() const {return state == STATE_RX;}
		bool is_wx() const {return state == STATE_WX;}
		bool is_error() const {return state == STATE_ERROR;}
	};

	class Object : public LRUObject{
	private:
		ProxyCacher *pc;
		uint64_t poolid;
		string oid;
		ceph_tid_t tid;
	public:
		bool complete;
		bool exists;
		atomic_t waitings;
		map<loff_t, BufferHead*> data;
		ceph_tid_t last_write_tid;
		ceph_tid_t last_commit_tid;

	public:
		Object(ProxyCacher *_pc, uint64_t pool, string id):pc(_pc), poolid(pool), oid(id),
			complete(false), exists(false), waitings(0){}
		~Object(){
			map<loff_t, BufferHead*>::iterator p = data.begin();
			for(; p != data.end(); p++) {
				BufferHead *bh = p->second;
				data.erase(p);
				delete bh;
				bh = NULL;
			}
		}
		uint64_t get_poolid() {return poolid;}
		string get_oid(){return oid;}
		bool is_cached(uint64_t off, uint64_t len) {
			return false;
		}
		map<loff_t, BufferHead*>::iterator data_lower_bound(loff_t offset);
		int map_read(loff_t offset, loff_t length, map<loff_t, BufferHead*> &hists,
			map<loff_t, BufferHead*> &missing, map<loff_t, BufferHead*> &wx);
		BufferHead* map_write(loff_t offset, loff_t length);
		BufferHead* bh_add(loff_t off, loff_t len);
		int merge_bh();
	private:
		BufferHead* split(BufferHead *bh, loff_t off);
		void merge_left(BufferHead *left, BufferHead *right);
	};

	void close_object(Object *o);
	ProxyRead* prepare_read(uint64_t pool, string oid, uint64_t off, uint64_t len, bufferlist &bl) {
		return new ProxyRead(pool, oid, off, len, &bl);
	}
	ProxyWrite* prepare_write(uint64_t pool, string oid, uint64_t off, bufferlist &bl) {
		return new ProxyWrite(pool, oid, off, &bl);
	}
	
	int object_read(uint64_t pool, string oid, uint64_t off, uint64_t len, 
		bufferlist &bl, Context *onfinish);
	int object_write(uint64_t pool, string oid, uint64_t off, bufferlist &bl,
		Context *onfinish);
	int bh_read(BufferHead *bh, ProxyRead *pr, Context *onfinish);
	int bh_write(BufferHead *bh, ProxyWrite *pw, Context *onfinish);
	int bh_read_finish(Object *object, ProxyRead *pr, Context *onfinish);
	int bh_write_finish(Object *object, loff_t off, Context *onfinish);
	void mark_dirty(BufferHead *bh){bh->set_state(BufferHead::STATE_DIRTY);}
	void mark_clean(BufferHead *bh){bh->set_state(BufferHead::STATE_CLEAN);}
private:
	CephContext *cct;
	ProxyCacheHandler *cache_handler;
	Mutex lock;
	uint64_t max_objects;
	uint64_t object_nums;
	LRU ob_lru;
	//index by poolid
	vector<ceph::unordered_map<string, Object*> > objects;
	ceph_tid_t last_tid;

	Object* get_object(uint64_t pool, string oid);
	void touch_ob(Object *o);
	int _readx(ProxyRead *pr, Context *onfinish, bool external_call);
	int readx(ProxyRead *pr, Context *onfinish);
	int writex(ProxyWrite *pw, Context *onfinish);

public:
	class C_ReadFinish : public Context {
	private:
		ProxyCacher *pc;
		Object *object;
		ProxyRead *pr;
		Context *onfinish;
	public:
		C_ReadFinish(ProxyCacher *_pc, Object *o, ProxyRead *_pr, Context *f):pc(_pc),
			object(o), pr(_pr), onfinish(f){}
		void finish(int r) {
			pc->bh_read_finish(object, pr, onfinish);
		}
	};
	class C_WriteCommit : public Context {
	private:
		ProxyCacher *pc;
		Object *object;
		loff_t off;
		Context *onfinish;
	public:
		C_WriteCommit(ProxyCacher *_pc, Object *o, ProxyWrite *pw, Context *f):pc(_pc),
			object(o), off(pw->offset), onfinish(f){}
		void finish(int r) {
			pc->bh_write_finish(object, off, onfinish);
		}

	};

	class C_WaitForWrite : public Context {
	private:
		ProxyCacher *pc;
		int64_t poolid;
		string oid;
		loff_t start;
		loff_t length;
		Context *onfinish;
	public:
		C_WaitForWrite(ProxyCacher *_pc, Object *o, loff_t offset, loff_t len, Context *f):pc(_pc),
			poolid(o->get_poolid()), oid(o->get_oid()), start(offset), length(len), onfinish(f){}
		void finish(int r) {
			return;
		}
	};
	ProxyCacher(CephContext *cct , ProxyCacheHandler *h, uint64_t max_objects);
	~ProxyCacher();

};

inline ostream& operator<<(ostream &out, const ProxyCacher::BufferHead &bh)
{
	out << " bh[" << bh.start() << "~" << bh.end() << "]";
	return out;
}

#endif //CEPH_PROXYCACHE_H
