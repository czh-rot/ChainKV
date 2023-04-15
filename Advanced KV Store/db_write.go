// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

//写日志，把batch中的数据写入日志
func (db *DB) writeJournal(batches []*Batch, seq uint64, sync bool) error {
	wr, err := db.journal.Next() //wr is a io.Writer, [singleWriter]
	if err != nil {
		return err
	}
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil { //batch写入日志
		return err
	}
	if err := db.journal.Flush(); err != nil { //？
		return err
	}
	if sync {
		return db.journalWriter.Sync() //？
	}
	return nil
}
//mem变immu，新建log和memory
func (db *DB) writeJournal_s(batches []*Batch, seq uint64, sync bool) error {
	wr, err := db.journal2.Next()
	if err != nil {
		return err
	}
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil {
		return err
	}
	if err := db.journal2.Flush(); err != nil {
		return err
	}
	if sync {
		return db.journalWriter2.Sync()
	}
	return nil
}

func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	//fmt.Print("Mem空间不足")
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	//fmt.Print(" 1: ")
	err = db.compTriggerWait(db.mcompCmdC)//写mcompCmdc
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.
	// 新建log文件和memory，同时把现在使用的memory指向为frozenMem，
	// minor compaction的时候写入frozenMem到level 0文件
	//fmt.Print(" NewMem ")
	mem, err = db.newMem(n)
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	//触发minor compaction，大部分情况都是false
	if wait {
		err = db.compTriggerWait(db.mcompCmdC)
	} else {
		//fmt.Print(" 2: ")
		db.compTrigger(db.mcompCmdC)
	}
	return
}

func (db *DB) rotateMem_s(n int, wait bool) (mem *memDB, err error) {
	//fmt.Print(" Mems空间不足,准备进行mcompaction ")
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	// rotateMem中会写channel mcompCmdC吗，这个goroutine起来后一直在监听该channel等待做compaction的事情
	err = db.compTriggerWait(db.mcompCmdCs)
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.新建log文件和memory，同时把现在使用的memory指向为frozenMem，
	// minor compaction的时候写入frozenMem到level 0文件
	//fmt.Print(" NewMems ")
	mem, err = db.newMem_s(n)
	//fmt.Print(" Build Success ")
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	//触发minor compaction
	if wait {
		err = db.compTriggerWait(db.mcompCmdCs) //需要等待
	} else {
		db.compTrigger(db.mcompCmdCs) //不需要等待
	}
	return
}

func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {//n为batch.internallen，是指batch的大小？
	delayed := false
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger()
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger()
	flush := func() (retry bool) {
		mdb = db.getEffectiveMem()//Get effective mdb,引用+1
	//	fmt.Print(" getMem ")
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			if retry {
				mdb.decref() //引用-1
				mdb = nil
			}
		}()
		tLen := db.s.tLen(0) //?
		mdbFree = mdb.Free() //空闲的memdb大小 cap（kvdata）-len（kvdata）
		switch {
		case tLen >= slowdownTrigger && !delayed:
		//	fmt.Print(" case 1 ")
			delayed = true
			time.Sleep(time.Millisecond)
		case mdbFree >= n:
		//	fmt.Print(" case 2 ")
			return false
		case tLen >= pauseTrigger:
	//		fmt.Print(" case 3 ")
			delayed = true
			// Set the write paused flag explicitly.
			atomic.StoreInt32(&db.inWritePaused, 1)
			err = db.compTriggerWait(db.tcompCmdC)
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}
		default:
			//Allow memdb to grow if it has no entry.
		//	fmt.Print(" default ")
			if mdb.Len() == 0 {
			//	fmt.Print(" default1 ")
				mdbFree = n
			} else {
			//	fmt.Print(" defaul2 ")
				mdb.decref()//释放当前引用量
				mdb, err = db.rotateMem(n, false) //新建mem？
				if err == nil {
					mdbFree = mdb.Free() //空闲大小
				} else {
					mdbFree = 0
				}
			}
			//fmt.Print(" defaul2 ")
			//mdb.decref()//释放当前引用量
			//mdb, err = db.rotateMem(n, false) //新建mem？
			//if err == nil {
			//	mdbFree = mdb.Free() //空闲大小
			//} else {
			//	mdbFree = 0
			//}
			return false
		}
		return true
	}
	//fmt.Print(" running ")
	start := time.Now()
	for flush() {
	}
	//fmt.Print(" access end ")
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

func (db *DB) flush_s(n int) (mdb *memDB, mdbFree int, err error) {//此时，mdbFree结束后应为4,参数为batch的大小
	delayed := false
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger()
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger() // int 1
	flush := func() (retry bool) {        //是一个类型，下面的有一个循环等待返回false？
		mdb = db.getEffectiveMem_s()//得到effective mdb
		//fmt.Print(" GetMems ")
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			if retry {
				mdb.decref_s()
				mdb = nil
			}
		}()
		tLen := db.s.tLen_s(0)
		mdbFree = mdb.Free_s() //得到mem的大小
		//fmt.Print(mdbFree)
		switch {
		case tLen >= slowdownTrigger && !delayed:
			//fmt.Print(" case 1 ")
			delayed = true
			time.Sleep(time.Millisecond)
		case mdbFree >= n:
		//	fmt.Print(" case 2 ")
			return false
		case tLen >= pauseTrigger:
		//	fmt.Print(" case 3 ")
			delayed = true
			// Set the write paused flag explicitly.
			atomic.StoreInt32(&db.inWritePaused, 1)
			err = db.compTriggerWait(db.tcompCmdCs)
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}
		default:
		//	fmt.Print(" default ")
			mdb.decref_s()//释放当前引用量
			mdb, err = db.rotateMem_s(n, false)
			if err == nil {
				mdbFree = mdb.Free_s() //空闲大小
			} else {
				mdbFree = 0
			}
			// Allow memdb to grow if it has no entry.
			//if mdb.Len() == 0 { //mdb中无内容
			//	fmt.Print(" default1 ")
			//	mdbFree = n
			//} else {
			//	fmt.Print(" default2 ")
			//	mdb.decref()//释放当前引用量
			//	mdb, err = db.rotateMem_s(n, false)
			//	if err == nil {
			//		mdbFree = mdb.Free_s() //空闲大小
			//	} else {
			//		mdbFree = 0
			//	}
			return false
		}
		return true
	}
	//fmt.Print(" running ")
	start := time.Now()
	for flush() {
	}
	//fmt.Print(" access end ")
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	//fmt.Print(" ran  ")
	//fmt.Println("flush调用完毕")
	return
}
//写入合并
type writeMerge struct {
	sync       bool
	batch      *Batch
	keyType    keyType
	key, value []byte
}

func (db *DB) unlockWrite(overflow bool, merged int, err error) {
	for i := 0; i < merged; i++ {
		db.writeAckC <- err
	}
	if overflow {
		// Pass lock to the next write (that failed to merge).
		db.writeMergedC <- false
	} else {
		// Release lock.
		<-db.writeLockC
	}
}

// ourBatch is batch that we can modify.
//是线程真正执行写入的函数，其写入流程为：
//1.获取内存数据库memDB，如果空间不足则扩容
//2.如果任由Merge空间和数据，执行merge逻辑
//3.写日志信息
//4.数据写入内存
//5.释放相关锁，以及通知给等待线程执行结果
var (
	TcountPutMem float64
	TcountPutJou float64
)

func (db *DB) PrintTime(){
	fmt.Println("MemtableTime",TcountPutMem)
	fmt.Println("LogTime",TcountPutJou)
}

func (db *DB) writeLocked(batch, ourBatch *Batch, merge, sync bool) error {
	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	//1.尝试flush db的数据 如果有需要
	// 返回DB的mdb以及mdb的剩余空间，如果mdbFree不够则会对mdb进行扩容操作
	//mdb为memDB类型，mdbFree int 描述内存数据库的大小
	mdb, mdbFree, err := db.flush(batch.internalLen)
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref()//释放当前引用数量

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch} //data、index、internallen
	)

	if merge {
		// Merge limit.
		var mergeLimit int
		//控制merge的数量不是特别大
		if batch.internalLen > 128<<10 { //128 * 2^10
			mergeLimit = (1 << 20) - batch.internalLen
		} else {
			mergeLimit = 128 << 10
		}
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap {
			mergeLimit = mergeCap
		}
		//控制最大能够merge的量
	merge:
		for mergeLimit > 0 {
			select {
			case incoming := <-db.writeMergeC:
				if incoming.batch != nil { //writeMergeC 中存储的是batch的情况
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit {
						overflow = true
						break merge
					}
					//合并batched，incoming.batch是将要合并的？
					batches = append(batches, incoming.batch)
					mergeLimit -= incoming.batch.internalLen
				} else {
					// Merge put.
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						overflow = true
						break merge
					}
					if ourBatch == nil {
						ourBatch = db.batchPool.Get().(*Batch) //把batchpool放入ourbatch
						ourBatch.Reset() //重置
						batches = append(batches, ourBatch)  //已经重置过了啊？
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					//写入ourBatch？
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync //同步的情况需要通知写入的等待线程写入完毕
				merged++
				db.writeMergedC <- true

			default:
				break merge
			}
		}
	}

	// Release ourBatch if any. 把ourBatch放入batchpool
	if ourBatch != nil {
		defer db.batchPool.Put(ourBatch)
	}

	// Seq number.
	seq := db.seq + 1 ///seq是实际batch的数量编号, 此时db的实际seq并未更新

	// Write journal.
	// 2.batch中的信息写入日志，调用db.writeJournal
	t1:=time.Now()
	if err := db.writeJournal(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}
	t2:=time.Now()
	t3:=t2.Sub(t1).Seconds()
	TcountPutJou += t3

	// Put batches.
	//3. 遍历batches，把batch 数据写入内存数据库 mendb
	//fmt.Println("准备写进内存")
	t4:=time.Now()
	for _, batch := range batches {
		//putMem就是给key加上internal，然后调用mdb.put插入mem
		//putMem定义于batch.go,此方法调用mdb中的put，把kv对插入到skip list
		//mdb是*memDB的实例，可以直接调用Package memdb中的成员
		if err := batch.putMem(seq, mdb.DB); err != nil {
			panic(err)
		}
		seq += uint64(batch.Len())
	}
	t5:=time.Now()
	t6:=t5.Sub(t4).Seconds()
	TcountPutMem += t6

	// Incr seq number.更新seq
	db.addSeq(uint64(batchesLen(batches)))

	// Rotate memdb if it's reach the threshold.
	///如果memory不够写batch的内容，调用rotateMem，
	// 就是把memory frezon、触发minor compaction
	//fmt.Println("PAY ATTENTION!",batch.internalLen,mdbFree)
	if batch.internalLen >= mdbFree {
		db.rotateMem(0, false)
	}

	db.unlockWrite(overflow, merged, nil)
	//fmt.Println("写入完毕，return")
	return nil
}
//改动为，把flush的返回值改成了mem_s
func (db *DB) writeLocked_s(batch, ourBatch *Batch, merge, sync bool) error {
	//fmt.Println("writeLocked_s程序启动，准备调用flush")
	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	//1.尝试flush db的数据 如果有需要
	// 返回DB的mdb以及mdb的剩余空间，如果mdbFree不够则会对mdb进行扩容操作
	mdb, mdbFree, err := db.flush_s(batch.internalLen)//这个mdb可以调用好多方法 .db和*memdb.db？
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref_s()//释放当前引用数量

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch}
	)
	//fmt.Println("准备执行merge")
	//merge逻辑
	if merge {
		var mergeLimit int
		//控制merge的数量不是特别大
		if batch.internalLen > 128<<10 {
			mergeLimit = (1 << 20) - batch.internalLen
		} else {
			mergeLimit = 128 << 10
		}
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap {
			mergeLimit = mergeCap
		}
		//控制最大能够merge的量
	merge:
		for mergeLimit > 0 {
			select {
			case incoming := <-db.writeMergeC:
				if incoming.batch != nil {
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit {
						overflow = true
						break merge
					}
					batches = append(batches, incoming.batch)
					mergeLimit -= incoming.batch.internalLen
				} else {
					// Merge put.
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						overflow = true
						break merge
					}
					if ourBatch == nil {
						ourBatch = db.batchPools.Get().(*Batch)
						ourBatch.Reset()
						batches = append(batches, ourBatch)
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync
				merged++
				db.writeMergedC <- true

			default:
				break merge
			}
		}
	}
	// Release ourBatch if any. x-->pool，释放ourBatch？
	if ourBatch != nil {
		defer db.batchPools.Put(ourBatch)
	}

	// Seq number.
	seq := db.seq + 1 ///seq是实际batch的数量编号, 此时db的实际seq并未更新

	//2.batch中的信息写入日志
	t1:=time.Now()
	if err := db.writeJournal_s(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}
	t2:=time.Now()
	t3:=t2.Sub(t1).Seconds()
	TcountPutJou += t3

	//3. batch 数据写入内存数据库 mendb ,遍历batches
	//putMem就是给key加上internal，然后调用mdb.put插入mem ,
	t4:=time.Now()
	for _, batch := range batches {
		//这里mem.DB是内存数据库*memdb.DB,而mdb.db.mem_s是*memDB类型
		if err := batch.putMem_s(seq, mdb.DBs); err != nil {
			panic(err)
		}
		seq += uint64(batch.Len())
	}
	t5:=time.Now()
	t6:=t5.Sub(t4).Seconds()
	TcountPutMem += t6
	// Incr seq number.更新seq
	db.addSeq(uint64(batchesLen(batches)))

	// Rotate memdb if it's reach the threshold.,这里的mdfree就是开头flush得到的，所以实际上插入之后mdfree应该没有了
	//fmt.Print("PAY ATTENTION!",batch.internalLen,mdbFree)
	if batch.internalLen >= mdbFree {
		//fmt.Println("为什么不执行阿")
		db.rotateMem_s(0, false)
	}
	db.unlockWrite(overflow, merged, nil)
	//fmt.Println("return，一次写过程调用完成")
	//fmt.Println("  Write Success， return")
	return nil
}
//Write apply the given batch to the DB. The batch records will be applied
//sequentially. Write might be used concurrently, when used concurrently and
//batch is small enough, write will try to merge the batches. Set NoWriteMerge
//option to true to disable write merge.
//
//It is safe to modify the contents of the arguments after Write returns but
//not before. Write will not modify content of the batch.
//batch的write的实现，
func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}
	//如果批处理大小大于写缓冲区，则可以使用事务进行写。使用事务将批处理直接写入表中，跳过日志记录。
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		return tr.Commit()
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked(batch, nil, merge, sync)
}
func (db *DB) Write_s(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}
	//如果批处理大小大于写缓冲区，则可以使用事务进行写。使用事务将批处理直接写入表中，跳过日志记录。
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		return tr.Commit_s()
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked_s(batch, nil, merge, sync)
}
//事务写的逻辑
//在这里作者使用writeMergeC、writeMergedC、writeAckC和writeLockC共同控
//制多线程的数据插入和合并操作。这四个channel的定义如下，
//注意其中writeLockC的channel可以缓存一个对象。
//假设有三个进程c1、c2、c3竞争MergeC和LockC，假设c1获取writeLockC，
//此时，c1会将k-v写入batch然后执行writeLocked函数，c2、3竞争MergeC
//在writeLocked函数中，会对写入记录进行Merge操作，也就是会从writeMergeC中读取
// 当前堵塞的写入并在空间够用的情况下merge成一次写操作。
func (db *DB) putRec(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil {//检查数据库状态
		return err
	}
	//merge 和sync 以数据库的初始化配置为主
	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		//<-表示数据的流动方向，通过channel实现多线程的通信
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			//如果能向writeMergeC 写入新插入的key value 数据
			//则等待新的key value与老的数据进行merge操作
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}: //尝试获取写锁
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		//没有merge的情况直接尝试获取写入锁
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	batch := db.batchPool.Get().(*Batch)
	batch.Reset()
	//batch的put和delete的实现，即往batch中写数据，然后准备往内存和日志中写
	batch.appendRec(kt, key, value)
	return db.writeLocked(batch, batch, merge, sync)
}

//Put_s调用的putRec_s
func (db *DB) putRec_s(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	//fmt.Println("putRec程序启动")
	if err := db.ok(); err != nil {//检查数据库状态
		return err
	}
	//merge 和sync 以数据库的初始化配置为主
	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()
	//fmt.Println("Process One")
	// Acquire write lock.
	if merge {
		select {
		//<-表示数据的流动方向，通过channel实现多线程的通信
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			//如果能向writeMergeC 写入新插入的key value 数据
			//则等待新的key value与老的数据进行merge操作
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}: //尝试获取写锁
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		//没有merge的情况直接尝试获取写入锁
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}
	//fmt.Println("Process Two")
	//从batch池中任意选择一个item返回给调用者，get方法也可以无视内存池，将其当作空
	batch := db.batchPools.Get().(*Batch)
	//fmt.Println("Process Three")
	batch.Reset()
	//batch的put和delete的实现，即往batch中写数据，然后准备往内存和日志中写
	//fmt.Println("Process Four")
	batch.appendRec(kt, key, value)
	//fmt.Println("准备启动writeLocked_s程序")
	return db.writeLocked_s(batch, batch, merge, sync)
}
// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map. Write merge also applies for Put, see
// Write.
//
// It is safe to modify the contents of the arguments after Put returns but not
// before.
//put函数的入口，调用putRec的方法
func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeVal, key, value, wo)
}
//put_s函数的入口，可以把数据写进新的batch、memtable、sst
func (db *DB) Put_s(key, value []byte, wo *opt.WriteOptions) error{
	//fmt.Println("Put_s程序启动，准备启动putRec_程序")
	return db.putRec_s(keyTypeVal, key, value, wo)
}

// Delete deletes the value for the given key. Delete will not returns error if
// key doesn't exist. Write merge also applies for Delete, see Write.
//
// It is safe to modify the contents of the arguments after Delete returns but
// not before.
func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeDel, key, nil, wo)
}

func isMemOverlaps(icmp *iComparer, mem *memdb.DB, min, max []byte) bool {
	iter := mem.NewIterator(nil)
	defer iter.Release()
	return (max == nil || (iter.First() && icmp.uCompare(max, internalKey(iter.Key()).ukey()) >= 0)) &&
		(min == nil || (iter.Last() && icmp.uCompare(min, internalKey(iter.Key()).ukey()) <= 0))
}

// CompactRange compacts the underlying DB for the given key range.
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data. This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
func (db *DB) CompactRange(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdC); err != nil {
			return err
		}
	} else {
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdC, -1, r.Start, r.Limit)
}
func (db *DB) CompactRange_s(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem_s()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem_s(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdCs); err != nil {
			return err
		}
	} else {
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdCs, -1, r.Start, r.Limit)
}
// SetReadOnly makes DB read-only. It will stay read-only until reopened.
func (db *DB) SetReadOnly() error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		db.compWriteLocking = true
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Set compaction read-only.
	select {
	case db.compErrSetC <- ErrReadOnly:
	case perr := <-db.compPerErrC:
		return perr
	case <-db.closeC:
		return ErrClosed
	}

	return nil
}
