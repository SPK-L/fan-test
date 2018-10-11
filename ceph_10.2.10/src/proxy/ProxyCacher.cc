#include "common/ceph_context.h"
#include "common/debug.h"

#include "ProxyCacher.h"
#include "ProxyHandler.h"

#define dout_subsys ceph_subsys_proxy

ProxyCacher::ProxyCacher(CephContext *_cct, ProxyCacheHandler *h, uint64_t _max_objects):
	cct(_cct), cache_handler(h), lock("ProxyCacher Lock"), max_objects(_max_objects), object_nums(0)
{

}

ProxyCacher::~ProxyCacher()
{

}

map<loff_t, ProxyCacher::BufferHead*>::iterator ProxyCacher::Object::data_lower_bound(loff_t offset)
{
	map<loff_t, BufferHead*>::iterator p = data.lower_bound(offset);
	if (p != data.begin() &&(p == data.end() || p->first > offset)) {
		//might overlap the previous bh
		--p;
		if (p->first + p->second->length() <= offset) {
			//doesn't overlap
			++p;
		}
	}
	return p;
}

int ProxyCacher::Object::map_read(loff_t offset, loff_t length, map<loff_t, ProxyCacher::BufferHead*> &hists, 
	map<loff_t, ProxyCacher::BufferHead*> &missing, map<loff_t, ProxyCacher::BufferHead*> &wx)
{
	assert(pc->lock.is_locked());

	map<loff_t, BufferHead*>::iterator p = data_lower_bound(offset);
	while (length > 0) {
		if (p == data.end()) {
			//no bh found in cache
			BufferHead *n = new BufferHead(this);
			n->set_start(offset);
			n->set_length(length);
			data[offset] = n;
			missing[offset] = n;
			dout(20) << __func__ << " object=" << oid << " miss cache off=" << offset << " len=" << 
				length << dendl;
			break;
		}

		if (p->first <= offset) {
			//have it or part of it
			
			BufferHead *e = p->second;
			hists[offset] = e;

			loff_t left = MIN(e->end() - offset, length);
			dout(20) << __func__ << " object=" << oid << " hits cache off=" << offset << " len="
				<< left << dendl;
			offset += left;
			length -= left;
			++p;
			
			continue;
		} else {
			//gap 
			loff_t next = p->first;
			BufferHead *n = new BufferHead(this);
			loff_t len = MIN(next - offset, length);
			n->set_start(offset);
			n->set_state(len);
			data[offset] = n;
			missing[offset] = n;

			dout(20) << __func__ << " object=" << oid << " miss cache off=" << offset << " len=" 
				<< len << dendl;
			offset += len;
			length -= len;
			
			continue;
		}
	}

	return 0;
}

ProxyCacher::BufferHead* ProxyCacher::Object::map_write(loff_t offset, loff_t length)
{
	assert(pc->lock.is_locked());
	BufferHead *final = NULL;
	loff_t left = length;

	map<loff_t, BufferHead*>::iterator p = data_lower_bound(offset);
	while(left > 0) {
		if (p == data.end()) {
			if (!final) {
				//no bh found in cache
 				dout(20) << __func__ << " object=" << oid << " not exits in cache off=" << 
 					offset << " len=" << length << dendl;
				final = new BufferHead(this);
				final->set_start(offset);
				final->set_length(length);
				data[offset] = final;
			} else {
				//the last bh is not big enough
				dout(20) << __func__ << " the last bh is not long enough, left=" << left << dendl;
				final->set_length(final->length() + left);
			}
			break;
		}

		if (p->first <= offset) {
			//have it or part of it
			assert(!final);
			BufferHead *bh = p->second;
			if (p->first < offset) {
				if (offset + left >= bh->end()) {
					//we want the right splice
					dout(20) << __func__ << " split bh and take the right splice" << dendl;
					final = split(bh, offset);
					++p;
				} else {
					//we want the middle splice
					dout(20) << __func__ << " split bh and take the middle splice" << dendl;
					final = split(bh, offset);
					++p;
					assert(p->second == final);
					split(final, offset + left);
				}
			} else {
				if (left >= bh->length()) {
					//we want the whole bh
					dout(20) << __func__ << " take the whole bh" << dendl;
				} else {
					// we want the left splice
					dout(20) << __func__ << " split bh and take the left splice" << dendl;
					split(bh, offset + left);
				}

				if (final) {
					--p;
					merge_left(final, bh);
				} else {
					final = bh;
				}
			}

			loff_t len = final->end() - offset;
			dout(20) << __func__ << " object=" << oid << " hits cache off=" << offset << " len="
				<< len << dendl;
			offset += len;
			left -= len;
			++p;
			continue;
		} else {
			//gap
			
			loff_t next = p->first;
			loff_t glen = MIN(next - offset, left);
			dout(20) << __func__ << " object=" << oid << " miss cache off=" << offset << " len=" 
				<< glen << dendl;
			if (final) {
				final->set_length(final->length() + glen);
			} else {
				final = new BufferHead(this);
				final->set_length(glen);
				data[offset] = final;
			}

			offset += glen;
			left -= glen;
			continue;
		}
		
	}
	return final;
}

ProxyCacher::BufferHead* ProxyCacher::Object::bh_add(loff_t off, loff_t len)
{
	BufferHead *bh = new BufferHead(this);
	bh->set_start(off);
	bh->set_length(len);
	data[off] = bh;
	return bh;
}
	
int ProxyCacher::Object::merge_bh()
{
	map<loff_t, BufferHead*>::iterator pre_it = data.begin();
	map<loff_t, BufferHead*>::iterator bh_it = data.begin();
	for(bh_it++; bh_it != data.end(); bh_it++, pre_it++) {
		BufferHead *pre = pre_it->second;
		BufferHead *cur = bh_it->second;
		if (pre->end() == cur->start()) {
			loff_t len = pre->length() + cur->length();
			pre->set_length(len);
			pre->data.claim_append(cur->data);
			data.erase(bh_it);
			delete cur;
			cur = NULL;
		}
	}
	return 0;
}

ProxyCacher::BufferHead* ProxyCacher::Object::split(ProxyCacher::BufferHead *bh, loff_t off)
{
	loff_t nlen = off - bh->start();
	BufferHead *right = new BufferHead(this);
	bufferlist bl;

	dout(20) << "object=" << oid << " split " << *bh << " at off=" << off << dendl;
	if (off > bh->end()) {
		return NULL;
	}

	right->set_start(off);
	right->set_length(bh->length() - nlen);
	bh->set_length(nlen);
	bl.claim_append(bh->data);

	right->data.substr_of(bl, bh->length(), right->length());
	bh->data.substr_of(bl, 0, bh->length());
	data[off] = right;
	return right;
}

void ProxyCacher::Object::merge_left(ProxyCacher::BufferHead *left, ProxyCacher::BufferHead *right)
{
	dout(20) << __func__ << *left << "+" << *right << dendl;
	assert(left->end() == right->start());

	left->data.claim_append(right->data);
	left->set_length(left->length() + right->length());

	data.erase(right->start());
	delete right;
	right = NULL;
}

int ProxyCacher::object_read(uint64_t pool, string oid, uint64_t off, uint64_t len, 
	bufferlist &bl, Context *onfinish)
{
	lock.Lock();
	ProxyRead *pr = prepare_read(pool, oid, off, len, bl);
	return readx(pr, onfinish);
}

int ProxyCacher::object_write(uint64_t pool, string oid, uint64_t off, bufferlist &bl,
	Context *onfinish)
{
	lock.Lock();
	ProxyWrite *pw = prepare_write(pool, oid, off, bl);
	return writex(pw, onfinish);
}

int ProxyCacher::bh_read(BufferHead *bh, ProxyRead *pr, Context *onfinish)
{
	bh->object->waitings.inc();
	
	C_ReadFinish *f = new C_ReadFinish(this, bh->object, pr, onfinish);
	dout(20) << __func__ << " oid=" << pr->oid << " off=" << pr->offset << " len=" << pr->length << dendl;
	return cache_handler->read(pr->poolid, pr->oid, pr->offset, pr->length, bh->data, f);
}

int ProxyCacher::bh_write(BufferHead *bh, ProxyWrite *pw, Context *onfinish)
{
	C_WriteCommit *f = new C_WriteCommit(this, bh->object, pw, onfinish);
	dout(20) << __func__ << " oid=" << pw->oid << *bh << dendl;
	return cache_handler->write(pw->poolid, pw->oid, pw->offset, bh->data, f);
}

int ProxyCacher::bh_read_finish(Object *object, ProxyRead *pr, Context *onfinish)
{
	assert(lock.is_locked());
	int waitings = object->waitings.dec();
	//dout(20) << __func__ << *object << " has " << waitings << " waitings" << dendl;
	if (!waitings) {
		object->exists = true;
		object->merge_bh();
		_readx(pr, onfinish, false);
	}
	return 0;
}

int ProxyCacher::bh_write_finish(Object *object, loff_t off, Context *onfinish)
{
	assert(lock.is_locked());
	dout(20) << __func__ << " oid=" << object->get_oid() << dendl;
	BufferHead *bh = object->data[off];
	mark_clean(bh);
	object->exists = true;
	object->merge_bh();
	lock.Unlock();
	onfinish->complete(0);
	return 0;
}

ProxyCacher::Object* ProxyCacher::get_object(uint64_t pool, string oid)
{
	assert(lock.is_locked());
	//have it?
	if (pool < objects.size()) {
		if (objects[pool].count(oid)) {
			Object *o = objects[pool][oid];
			return o;
		}
	} else {
		objects.resize(pool + 1);
	}

	//create it
	object_nums++;
	Object *o = new Object(this, pool, oid);
	objects[pool][oid] = o;
	ob_lru.lru_insert_top(o);
	if (object_nums > max_objects) {
		LRUObject *ex_obj = ob_lru.lru_expire();
		if (ex_obj) {
			close_object((Object *)ex_obj);
		}
	}
	return o;
}

void ProxyCacher::close_object(Object *o)
{
	assert(lock.is_locked());
	dout(20) << "expire [pool=" << o->get_poolid() << ", oid=" << o->get_oid()
		<< "] from cache" << dendl;
	objects[o->get_poolid()].erase(o->get_oid());
	delete o;
	o = NULL;
}

void ProxyCacher::touch_ob(Object *o)
{
	ob_lru.lru_touch(o);
}

int ProxyCacher::_readx(ProxyRead *pr, Context *onfinish, bool external_call)
{
	assert(lock.is_locked());
	bool success = true;

	Object *o = get_object(pr->poolid, pr->oid);

	if (external_call) {
		touch_ob(o);
	}

	if (!o->exists) {//object is not in cache, read it from rados
		dout(20) << "object=" << pr->oid << " doesn't exist in cache" << dendl;
		success = false;
		BufferHead *bh = o->bh_add(pr->offset, pr->length);
		bh_read(bh, pr, onfinish);
		return 0;
	}

	map<loff_t, BufferHead*> hits, missing, wx;
	o->map_read(pr->offset, pr->length, hits, missing, wx);
	if (!missing.empty()) {
		success = false;
		
		map<loff_t, BufferHead*>::iterator bh_it = missing.begin();
		for(; bh_it != missing.end(); bh_it++) {
			bh_read(bh_it->second, pr, onfinish);
		}
	}

	//we wait
	if (!success) {
		return 0;
	}

	loff_t cur = pr->offset;
	loff_t left = pr->length;
	map<loff_t, BufferHead*>::iterator bh_it = hits.begin();
	for (; bh_it != hits.end(); bh_it++) {
		BufferHead *bh = bh_it->second;
		bufferlist bl;
		
		if (!left) {
			break;
		}
		
		loff_t len = MIN(bh->length() - cur, left);
		bl.substr_of(bh->data, cur, len);
		pr->data->claim_append(bl);

		cur += len;
		left -= len;
	}

	lock.Unlock();
	dout(20) << __func__ << " gather data in cache" << dendl;

	delete pr;
	pr = NULL;
	
	onfinish->complete(0);
	return 0;
}

int ProxyCacher::readx(ProxyRead *pr, Context *onfinish)
{
	return _readx(pr, onfinish, true);
}

int ProxyCacher::writex(ProxyWrite *pw, Context *onfinish)
{
	assert(lock.is_locked());

	Object *o = get_object(pw->poolid, pw->oid);
	touch_ob(o);
	//map it into a single bufferhead
	BufferHead *bh = o->map_write(pw->offset, pw->data->length());

	bufferlist bl;
	bl.claim_append(*(pw->data));
	bh->data.swap(bl);

	mark_dirty(bh);
	bh_write(bh, pw, onfinish);

	delete pw;
	pw = NULL;
	return 0;
}

