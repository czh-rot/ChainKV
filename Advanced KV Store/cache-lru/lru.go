// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync"
	"unsafe"
)
// lru双向循环链表
type lruNode struct {
	n   *Node
	h   *Handle
	ban bool

	next, prev *lruNode //队尾和队头指针？
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
func (n *lruNode) remove() {
	if n.prev != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		n.prev = nil
		n.next = nil
	} else {
		panic("BUG: removing removed node")
	}
}
// 锁、总容量、使用的容量、lruNode
type lru struct {
	mu       sync.Mutex
	capacity int // 表示缓存的总容量
	used     int // 使用的容量
	recent   lruNode // 双向循环链表的head
}
// 重置lru链表，使用为0
func (r *lru) reset() {
	r.recent.next = &r.recent
	r.recent.prev = &r.recent
	r.used = 0
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
	r.capacity = capacity
	// 如果容量超出使用，不断地移除recent.prev，那这个recent是什么位置阿
	for r.used > r.capacity {
		rn := r.recent.prev // 头节点的前一个就是尾节点
		if rn == nil {
			panic("BUG: invalid LRU used or capacity counter")
		}
		rn.remove() // 移除
		rn.n.CacheData = nil // CacheData为缓存数据
		r.used -= rn.n.Size() // 已用空间减少
		evicted = append(evicted, rn) //将rn记录在evicted切片中
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release() // 将evicted的所有lruNode释放Handle
	}
}
// 1、如果是从磁盘中读出来的数据，n.cachedata在之前并未赋值，因此是nil。n.Size()通过前文的setFun函数可以知道是1，
//而r.capacity的定义是在cmd/utils/flags.go中，默认1024.因此新数据直接插入到lru的队尾，而队头的数据也是最老的缓存则删掉。
// 2、如果是从缓存读出来的数据，则通过rn.insert将数据从队中提出来放到队尾，保证队尾放的数据都是最新读取的缓存。
// 目的：将缓存放入buckets
// 主要为两种情况，一种是新的，另一种不是新的
var (
	HitNumber int64
	MissNumber int64
	HitNumber2 int64
	MissNumber2 int64
)
func (r *lru) Promote(n *Node) {
	var evicted []*lruNode

	r.mu.Lock()
	// CacheData为nil，说明不在lru中，则Node、Handle就会新建一个lruNode插入到recent之后
	if n.CacheData == nil {
		MissNumber++
		if n.Size() <= r.capacity { // 必须得<最大容量，否则根本写不进去
			// 赋值Node和Handle，然后插入到lru链表中，h指向node【return &Handle{unsafe.Pointer(n)}】
			rn := &lruNode{n: n, h: n.GetHandle()}
			rn.insert(&r.recent) // 插入到头节点之后
			n.CacheData = unsafe.Pointer(rn) // 任意类型且可寻址的指针值，CacheData为lruNode的指针
			r.used += n.Size() // 容量变化
			// 超出容量就继续remove
			for r.used > r.capacity {
				rn := r.recent.prev
				if rn == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				rn.remove()
				rn.n.CacheData = nil
				r.used -= rn.n.Size()
				evicted = append(evicted, rn)
			}
		}
	// 否则就是从缓存中读的，已经被插入到lru中，应先删除掉，然后再插入
	} else {
		HitNumber++
		rn := (*lruNode)(n.CacheData) // 取出rn来，为lruNode的指针类型
		if !rn.ban {
			rn.remove()
			rn.insert(&r.recent) // 重新插入
		}
	}
	r.mu.Unlock()
	// 将evicted中的lruNode释放掉，以此释放内存
	for _, rn := range evicted {
		rn.h.Release()
	}
}
// 为ban赋值true或者false，何意？
// 在Cacher.Delete中被调用，标记为true，用以删除？
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
			r.used -= rn.n.Size()
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
			r.used -= rn.n.Size()
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
	r.reset()
	r.mu.Unlock()

	for rn := back; rn != &r.recent; rn = rn.prev {
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

