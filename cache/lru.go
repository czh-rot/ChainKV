// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"fmt"
	_ "os"
	"sync"
	"time"
	"unsafe"
)
// lru双向循环链表
type lruNode struct {
	n   *Node
	h   *Handle
	ban bool
	ty int8

	next, prev *lruNode //队尾和队头指针？
}
func (r *lruNode) Length() int {
	var length = 0
	N:=r.prev.n // 尾部节点
	for r.n != N{
		length++
		r = r.next
	}
	return length
}

func (r *lruNode) ContainNode0(N *lruNode) bool {
	if N.ty==0{
		return true
	}
	return false
}
func (r *lruNode) ContainNode1(N *lruNode) bool {
	if N.ty==1{
		return true
	}
	return false
}
func (r *lruNode) ContainNode2(N *lruNode) bool {
	if N.ty==2{
		return true
	}
	return false
}
func (r *lruNode) ContainNode3(N *lruNode) bool {
	if N.ty==3{
		return true
	}
	return false
}
// 循环链标插入操作，把lruNode n 插入到 at之后
func (n *lruNode) insert(at *lruNode) {
	x := at.next
	at.next = n
	n.prev = at
	n.next = x
	x.prev = n
}
func (n *lruNode) Equalinsert(at *lruNode) {
	n.prev = at
	n.next = at.next
	at.next = n
	at.next.prev = n
}
// 循环链表删除操作，删除n
func (n *lruNode) remove() *Node{
	if n.prev != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		n.prev = nil
		n.next = nil
	} else {
		panic("BUG: removing removed node")
	}
	return n.n
}
func (n *lruNode) Len()int{
	return n.n.Size()
}
// 锁、总容量、使用的容量、lruNode
type lru struct {
	mu       sync.Mutex
	capacity int // 表示缓存的总容量
	trriger  int
	rused,fused 	int // 分别表示两个链表capacity的使用情况
	r1used,f1used   int
	recent   lruNode // 双向循环链表的head
	frequent lruNode
	r1 		 lruNode
	f1       lruNode
}
// 判断长度

// 重置lru链表，使用为0
func (r *lru) reset() {
	// 初始化四个链表，阈值，使用值
	r.recent.next = &r.recent
	r.recent.prev = &r.recent
	r.recent.ty=0
	r.frequent.next = &r.frequent
	r.frequent.prev = &r.frequent
	r.frequent.ty=1
	r.r1.next = &r.r1
	r.r1.prev = &r.r1
	r.r1.ty=2
	r.f1.next = &r.f1
	r.f1.prev = &r.f1
	r.f1.ty=3
	r.trriger = 0
	r.rused = 0
	r.fused = 0
	r.r1used = 0
	r.f1used = 0
}
// 返回容量
func (r *lru) Capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}
// 设置容量
func (r *lru) SetCapacity(capacity int) {
	var evicted []*lruNode
	r.mu.Lock()
	r.capacity = capacity+1
	// 如果容量超出使用，先回收recent和frequent，然后回收r1和f1
	for r.rused + r.fused >= r.capacity{
		r.replace(false)
	}
	for r.r1used > r.capacity-r.trriger{
		m := r.r1.prev
		if m == nil {
			panic("BUG: invalid LRU used or capacity counter")
		}
		m.remove()
		m.n.CacheData = nil
		r.r1used -= m.n.Size()
		evicted = append(evicted,m)
	}
	for r.f1used > r.trriger{
		m := r.f1.prev
		if m == nil {
			panic("BUG: invalid LRU used or capacity counter")
		}
		m.remove()
		m.n.CacheData = nil
		r.f1used -= m.n.Size()
		evicted = append(evicted,m)
	}
	//for r.rused > r.capacity {
	//	rn := r.recent.prev // 头节点的前一个就是尾节点
	//	if rn == nil {
	//		panic("BUG: invalid LRU used or capacity counter")
	//	}
	//	rn.remove() // 移除
	//	rn.n.CacheData = nil // CacheData为缓存数据
	//	r.rused -= rn.n.Size() // 已用空间减少
	//	evicted = append(evicted, rn) //将rn记录在evicted切片中
	//}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release() // 将evicted的所有lruNode释放Handle
	}
}

// used和fused容量超标时，判断缩减哪一列，并且移动到ghost中
func (r *lru) replace(b2ContainsKey bool){
	t1Len := r.rused
	// 如果t1容量超标，则从t1移走一个元素
	//if t1Len > 0 && (t1Len > r.trriger || (t1Len == r.trriger && b2ContainsKey)) {
	if t1Len > 0 && (t1Len >= r.trriger) {
		n:=r.recent.prev.remove()
		r.rused -= n.Size()
		rn := (*lruNode)(n.CacheData)
		//rn.n.value = nil
		// value置空，并插入r1中
		rn.ty=2
		rn.insert(&r.r1)
		r.r1used += rn.n.Size()
	} else {
		// replace被调用的前提是，t1+t2超过了总容量
		// 且有一个新元素要进行添加
		// 因此，在t1容量未超标的情况下，需要从t2移走一个元素
		n:=r.frequent.prev.remove()
		r.fused -= n.Size()
		rn := (*lruNode)(n.CacheData)
		//rn.n.value = nil
		rn.ty=3
		rn.insert(&r.f1)
		r.f1used += rn.n.Size()
	}
}
// 1、如果是从磁盘中读出来的数据，n.cachedata在之前并未赋值，因此是nil。n.Size()通过前文的setFun函数可以知道是1，
//而r.capacity的定义是在cmd/utils/flags.go中，默认1024.因此新数据直接插入到lru的队尾，而队头的数据也是最老的缓存则删掉。
// 2、如果是从缓存读出来的数据，则通过rn.insert将数据从队中提出来放到队尾，保证队尾放的数据都是最新读取的缓存。
// 目的：将缓存放入buckets
// 重点在此！
var(
	t float64
)
func (c *Cache) Time(){
	fmt.Println(t)
}
var(
	MissNumber int64
	HitNumber int64
	MissNumber2 int64
	HitNumber2 int64
)
// 0代表r，1代表f，2代表r1，3代表f1
func (r *lru) Promote(n *Node) {
	var evicted []*lruNode
	r.mu.Lock()
	// CacheData为nil，说明不在lru中，即缓存未命中，生成新的Node，则Node、Handle就会新建一个lruNode插入到recent之后
	if n.CacheData == nil {
		MissNumber++
		//MissNumber2++
		if n.Size() <= r.capacity { // 必须得<最大容量，否则根本写不进去 // 赋值Node和Handle，然后插入到lru链表中，h指向node【return &Handle{unsafe.Pointer(n)}】
			rn := &lruNode{n: n, ty:0, h: n.GetHandle()}
			rn.insert(&r.recent) // 插入到头节点之后
			n.CacheData = unsafe.Pointer(rn) // 任意类型且可寻址的指针值，CacheData为lruNode的指针
			// 容量变化
			r.rused += n.Size()
			for r.rused + r.fused >= r.capacity{
				r.replace(false)
			}
			for r.r1used > r.capacity-r.trriger{
				m := r.r1.prev
				if m == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				m.remove()
				m.n.CacheData = nil
				r.r1used -= m.n.Size()
				evicted = append(evicted,m)
			}
			for r.f1used > r.trriger{
				m := r.f1.prev
				if m == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				m.remove()
				m.n.CacheData = nil
				r.f1used -= m.n.Size()
				evicted = append(evicted,m)
			}
			for _, rn := range evicted {
				rn.h.Release() // 将evicted的所有lruNode释放Handle
			}
			// 超出容量就继续remove
			//for r.rused > r.capacity {
			//	rn := r.recent.prev
			//	if rn == nil {
			//		panic("BUG: invalid LRU used or capacity counter")
			//	}
			//	rn.remove()
			//	rn.n.CacheData = nil
			//	r.rused -= rn.n.Size()
			//	evicted = append(evicted, rn)
			//}
		}
	// 否则就是从缓存中读的，已经被插入到lru中，应先删除掉，然后再插入
	// 只要读到，就往frequent里写。
	} else {
		HitNumber++
		//HitNumber2++
		rn := (*lruNode)(n.CacheData) // 取出rn来，为lruNode的指针类型
		if !rn.ban {
			// remove，插入到frequent，为ARC中add方法的核心内容
			t1:=time.Now()
			if r.recent.ContainNode0(rn){
				rn.remove()
				r.rused -= rn.n.Size()
				rn.ty=1
				rn.insert(&r.frequent) // 重新插入,插入到frequent
				r.fused += rn.n.Size()
			}else if r.frequent.ContainNode1(rn){
				rn.remove()
				r.fused -= rn.n.Size()
				rn.insert(&r.frequent) // 重新插入,插入到frequent
				r.fused += rn.n.Size()
			}else if r.r1.ContainNode2(rn){ // r1中含有
				HitNumber--
				MissNumber++
				delta:=1
				//rlen:=r.r1used+1
				//flen:=r.f1used+1
				//if rlen > flen{
				//	delta = rlen / flen
				//}
				if r.trriger + delta >= r.capacity{
					r.trriger = r.capacity
				}else {
					r.trriger+= delta
				}
				if r.rused + r.fused >= r.capacity{
					r.replace(false)
				}
				rn.remove()
				r.r1used -= rn.n.Size()
				rn.ty=1
				rn.insert(&r.frequent)
				r.fused += rn.n.Size()
			} else if r.f1.ContainNode3(rn){// r2中含有
				HitNumber--
				MissNumber++
				delta:=1
				//rlen:=r.r1used+1
				//flen:=r.f1used+1
				//if rlen > flen{
				//	delta = rlen / flen
				//}
				if delta > r.trriger{
					r.trriger = 0
				}else{
					r.trriger -= delta
				}
				if r.rused + r.fused >= r.capacity{
					r.replace(true)
				}
				rn.remove()
				r.f1used -= rn.n.Size()
				rn.ty=1
				rn.insert(&r.frequent)
				r.fused += rn.n.Size()
			}
			//else {
			//	panic("Should be in there,but!")
			//}
			t2:=time.Now()
			t += t2.Sub(t1).Seconds()
			for r.rused + r.fused >= r.capacity{
				r.replace(false)
			}
			for r.r1used > r.capacity-r.trriger{
				m := r.r1.prev
				if m == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				m.remove()
				m.n.CacheData = nil
				r.r1used -= m.n.Size()
				evicted = append(evicted,m)
			}
			for r.f1used > r.trriger{
				m := r.f1.prev
				if m == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				m.remove()
				m.n.CacheData = nil
				r.f1used -= m.n.Size()
				evicted = append(evicted,m)
			}
			for _, rn := range evicted {
				rn.h.Release() // 将evicted的所有lruNode释放Handle
			}
			//r.recent.insert(rn)
		}
	}
	r.mu.Unlock()
	// 将evicted中的lruNode释放掉，以此释放内存
}
// 为ban赋值true或者false，何意？
func (r *lru) Ban(n *Node) {
	r.mu.Lock()
	if n.CacheData == nil {
		n.CacheData = unsafe.Pointer(&lruNode{n: n, ban: true})
	} else {
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			// remove
			rn.remove()
			rn.ban = true
			if r.recent.ContainNode0(rn){
				r.rused -= rn.n.Size()
			}
			if r.frequent.ContainNode1(rn){
				r.fused -=rn.n.Size()
			}
			if r.r1.ContainNode2(rn){
				r.r1used -= rn.n.Size()
			}
			if r.f1.ContainNode3(rn){
				r.f1used -= rn.n.Size()
			}
			r.mu.Unlock()

			rn.h.Release()
			rn.h = nil
			return
		}
	}
	r.mu.Unlock()
}

// 置空CacheData
func (r *lru) Evict(n *Node) {
	r.mu.Lock()
	rn := (*lruNode)(n.CacheData)
	if rn == nil || rn.ban {
		r.mu.Unlock()
		return
	}
	n.CacheData = nil
	r.mu.Unlock()

	rn.h.Release()
}

func (r *lru) EvictNS(ns uint64) {
	var evicted []*lruNode

	r.mu.Lock()
	for e := r.recent.prev; e != &r.recent; {
		rn := e
		e = e.prev
		if rn.n.NS() == ns {
			rn.remove()
			rn.n.CacheData = nil
			r.rused -= rn.n.Size()
			evicted = append(evicted, rn)
		}
	}
	for e := r.frequent.prev; e != &r.frequent; {
		rn := e
		e = e.prev
		if rn.n.NS() == ns {
			rn.remove()
			rn.n.CacheData = nil
			r.fused -= rn.n.Size()
			evicted = append(evicted, rn)
		}
	}
	for e := r.r1.prev; e != &r.r1; {
		rn := e
		e = e.prev
		if rn.n.NS() == ns {
			rn.remove()
			rn.n.CacheData = nil
			r.r1used -= rn.n.Size()
			evicted = append(evicted, rn)
		}
	}
	for e := r.f1.prev; e != &r.f1; {
		rn := e
		e = e.prev
		if rn.n.NS() == ns {
			rn.remove()
			rn.n.CacheData = nil
			r.f1used -= rn.n.Size()
			evicted = append(evicted, rn)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lru) EvictAll() {
	r.mu.Lock()
	back := r.recent.prev
	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.n.CacheData = nil
	}
	back = r.frequent.prev
	for rn := back; rn != &r.frequent; rn = rn.prev {
		rn.n.CacheData = nil
	}
	back = r.r1.prev
	for rn := back; rn != &r.r1; rn = rn.prev {
		rn.n.CacheData = nil
	}
	back = r.f1.prev
	for rn := back; rn != &r.f1; rn = rn.prev {
		rn.n.CacheData = nil
	}
	r.reset()
	r.mu.Unlock()
	back = r.recent.prev
	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.h.Release()
	}
	back = r.frequent.prev
	for rn := back; rn != &r.frequent; rn = rn.prev {
		rn.h.Release()
	}
	back = r.r1.prev
	for rn := back; rn != &r.r1; rn = rn.prev {
		rn.h.Release()
	}
	back = r.f1.prev
	for rn := back; rn != &r.f1; rn = rn.prev {
		rn.h.Release()
	}
}

func (r *lru) Close() error {
	return nil
}

// NewLRU create a new LRU-cache.
// 创建一个新的LRU-cache
func NewLRU(capacity int) Cacher {
	r := &lru{capacity: capacity}
	r.reset()
	return r
}
