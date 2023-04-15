// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errHasFrozenMem = errors.New("has frozen mem")
)
//存放了是数据库和内存数据库的指针
type memDB struct {
	db *DB //数据库指针
	*memdb.DB //继承结构体DB
	*memdb.DBs //另外的一个内存数据库DBs
	ref int32
	refs int32
}
//这里这个m就相当于memDB的指针，也相当于Package memdb，
func (m *memDB) getref() int32 {
	return atomic.LoadInt32(&m.ref) //转格式？
}
func (m *memDB) getref_s() int32 {
	return atomic.LoadInt32(&m.refs) //转格式？
}
func (m *memDB) incref() { //加引用
	atomic.AddInt32(&m.ref, 1) //ref+1
}
func (m *memDB) incref_s() { //加引用
	atomic.AddInt32(&m.refs, 1) //ref+1
}
func (m *memDB) decref() { //减引用
	if ref := atomic.AddInt32(&m.ref, -1); ref == 0 { //if ref=1
		// Only put back memdb with std capacity.
		if m.Capacity() == m.db.s.o.GetWriteBuffer() { //达到阈值
			m.Reset() //mems置空
			m.db.mpoolPut(m.DB)  //mem->mpool?
		}
		m.db = nil //memdb置空
		m.DB = nil
	} else if ref < 0 {
		panic("negative memdb ref")
	}
}
func (m *memDB) decref_s() { //减引用
	if refs := atomic.AddInt32(&m.refs, -1); refs == 0 { //if ref=1
		// Only put back memdb with std capacity.
		if m.Capacity_s() == m.db.s.o.GetWriteBuffer() { //达到阈值4MiB
			m.Reset_s() //mems置空
			m.db.mpoolPut_s(m.DBs)  //mems->mpools?
		}
		m.db = nil //memdb置空
		m.DBs = nil
	} else if refs < 0 {
		panic("negative memdb ref")
	}
}
// Get latest sequence number.
func (db *DB) getSeq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

// Atomically adds delta to seq.
func (db *DB) addSeq(delta uint64) {
	atomic.AddUint64(&db.seq, delta)
}

func (db *DB) setSeq(seq uint64) {
	atomic.StoreUint64(&db.seq, seq)
}
//查找？，获取版本，调用v.sampleSeek
func (db *DB) sampleSeek(ikey internalKey) {
	v := db.s.version()
	if v.sampleSeek(ikey) {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdC)
	}
	v.release()
}
func (db *DB) sampleSeek_s(ikey internalKey) {
	v := db.s.version()
	if v.sampleSeek_s(ikey) {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdCs)
	}
	v.release()
}
//将mem放入mempool通道中
func (db *DB) mpoolPut(mem *memdb.DB) {
	if !db.isClosed() {
		select {
		case db.memPool <- mem:
		default:
		}
	}
}
func (db *DB) mpoolPut_s(mems *memdb.DBs) {
	if !db.isClosed() {
		select {
		case db.memPools <- mems:
		default:
		}
	}
}
//将mempool放到mem中
func (db *DB) mpoolGet(n int) *memDB {
	var mdb *memdb.DB
	select {
	case mdb = <-db.memPool:
	default:
	}
	if mdb == nil || mdb.Capacity() < n {
		mdb = memdb.New(db.s.icmp, maxInt(db.s.o.GetWriteBuffer(), n))
	}
	return &memDB{
		db: db,
		DB: mdb,
	}
}
func (db *DB) mpoolGet_s(n int) *memDB {
	var mdb *memdb.DBs
	select {
	case mdb = <-db.memPools:
	default:
	}
	if mdb == nil || mdb.Capacity_s() < n {
		mdb = memdb.New_s(db.s.icmp, maxInt(db.s.o.GetWriteBuffer(), n))
	}
	return &memDB{
		db: db,
		DBs: mdb,
	}
}
func (db *DB) mpoolDrain() {
	//发送时间用貌似
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			select {
			case <-db.memPool:
			default:
			}
		case <-db.closeC:
			ticker.Stop()
			// Make sure the pool is drained.
			select {
			case <-db.memPool:
			case <-time.After(time.Second):
			}
			close(db.memPool)
			return
		}
	}
}
func (db *DB) mpoolDrain_s() {
	//发送时间用貌似
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			select {
			case <-db.memPools:
			default:
			}
		case <-db.closeC:
			ticker.Stop()
			// Make sure the pool is drained. 是流干的？
			select {
			case <-db.memPools:
			case <-time.After(time.Second):
			}
			close(db.memPools)
			return
		}
	}
}
//Create new memdb and froze the old one; need external synchronization.
//newMem only called synchronously by the writer.
//memtable变immutable
func (db *DB) newMem(n int) (mem *memDB, err error) {
	fd := storage.FileDesc{Type: storage.TypeJournal, Num: db.s.allocFileNum()}
	w, err := db.s.stor.Create(fd)
	if err != nil {
		db.s.reuseFileNum(fd.Num)
		return
	}

	db.memMu.Lock()
	defer db.memMu.Unlock()

	if db.frozenMem != nil {
		return nil, errHasFrozenMem
	}

	if db.journal == nil {
		db.journal = journal.NewWriter(w)
	} else {
		db.journal.Reset(w)
		db.journalWriter.Close()
		db.frozenJournalFd = db.journalFd
	}
	db.journalWriter = w
	db.journalFd = fd
	db.frozenMem = db.mem
	mem = db.mpoolGet(n)
	mem.incref() // for self
	mem.incref() // for caller
	db.mem = mem
	// The seq only incremented by the writer. And whoever called newMem
	// should hold write lock, so no need additional synchronization here.
	db.frozenSeq = db.seq
	return
}
////关于mem_s的操作
func (db *DB) newMem_s(n int) (mem *memDB, err error) {
	fd := storage.FileDesc{Type: storage.TypeJournals, Num: db.s.allocFileNum()} //生成一个日志文件
	w, err := db.s.stor.Create(fd) //返回一个storage.writer？
	if err != nil {
		db.s.reuseFileNum(fd.Num)
		return
	}

	db.memMu.Lock()
	defer db.memMu.Unlock()

	if db.frozenMems != nil {
		return nil, errHasFrozenMem
	}

	if db.journal2 == nil {
		db.journal2 = journal.NewWriter_s(w)
	} else {
		db.journal2.Reset(w)
		db.journalWriter2.Close()
		db.frozenJournalFd2 = db.journalFd2
	}
	db.journalWriter2 = w
	db.journalFd2 = fd
	db.frozenMems = db.mems //mems变成frozenmems
	mem = db.mpoolGet_s(n) //此方法会调用mem.New_s方法初始化一个新的mem
	mem.incref_s() // for self
	mem.incref_s() // for caller
	db.mems = mem //让初始化的mem变为mems
	// The seq only incremented by the writer. And whoever called newMem
	// should hold write lock, so no need additional synchronization here.
	db.frozenSeq2 = db.seq
	return
}
// Get all memdbs.
func (db *DB) getMems() (e, f *memDB) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	if db.frozenMem != nil {
		db.frozenMem.incref()
	}
	return db.mem, db.frozenMem
}
func (db *DB) getMems_s() (e, f *memDB) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mems != nil {
		db.mems.incref_s()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	if db.frozenMems != nil {
		db.frozenMems.incref_s()
	}
	return db.mems, db.frozenMems
}
// Get effective memdb.
func (db *DB) getEffectiveMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		//fmt.Println("Run if,存在有效的mem")
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mem
}
func (db *DB) getEffectiveMem_s() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	//fmt.Println("???")
	if db.mems != nil {
		//fmt.Println("Run if,存在有效的mems")
		db.mems.incref_s()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mems
}
// Check whether we has frozen memdb.
func (db *DB) hasFrozenMem() bool {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	return db.frozenMem != nil
}
func (db *DB) hasFrozenMem_s() bool {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	return db.frozenMems != nil
}
// Get frozen memdb.
func (db *DB) getFrozenMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.frozenMem != nil {
		db.frozenMem.incref()
	}
	return db.frozenMem
}
func (db *DB) getFrozenMem_s() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.frozenMems != nil {
		db.frozenMems.incref_s()
	}
	return db.frozenMems
}
// Drop frozen memdb; assume that frozen memdb isn't nil.
func (db *DB) dropFrozenMem() {
	db.memMu.Lock()
	if err := db.s.stor.Remove(db.frozenJournalFd); err != nil {
		db.logf("journal@remove removing @%d %q", db.frozenJournalFd.Num, err)
	} else {
		db.logf("journal@remove removed @%d", db.frozenJournalFd.Num)
	}
	db.frozenJournalFd = storage.FileDesc{}
	db.frozenMem.decref()
	db.frozenMem = nil
	db.memMu.Unlock()
}
func (db *DB) dropFrozenMem_s() {
	db.memMu.Lock()
	if err := db.s.stor.Remove(db.frozenJournalFd2); err != nil {
		db.logf("journal@remove removing @%d %q", db.frozenJournalFd2.Num, err)
	} else {
		db.logf("journal@remove removed @%d", db.frozenJournalFd2.Num)
	}
	db.frozenJournalFd2 = storage.FileDesc{}
	db.frozenMems.decref_s()
	db.frozenMems = nil
	db.memMu.Unlock()
}
// Clear mems ptr; used by DB.Close().
func (db *DB) clearMems() {
	db.memMu.Lock()
	db.mem = nil
	db.frozenMem = nil
	db.memMu.Unlock()
}
func (db *DB) clearMems_s() {
	db.memMu.Lock()
	db.mems = nil
	db.frozenMems = nil
	db.memMu.Unlock()
}
// Set closed flag; return true if not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.closed, 0, 1)
}

// Check whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

// Check read ok status.
func (db *DB) ok() error {
	if db.isClosed() {
		return ErrClosed
	}
	return nil
}
