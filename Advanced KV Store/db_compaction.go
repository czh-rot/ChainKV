// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errCompactionTransactExiting = errors.New("leveldb: compaction transact exiting")
)

type cStat struct {
	duration time.Duration
	read     int64
	write    int64
}

func (p *cStat) add(n *cStatStaging) {
	p.duration += n.duration
	p.read += n.read
	p.write += n.write
}

func (p *cStat) get() (duration time.Duration, read, write int64) {
	return p.duration, p.read, p.write
}

type cStatStaging struct {
	start    time.Time
	duration time.Duration //持续时间？
	on       bool
	read     int64
	write    int64
}

func (p *cStatStaging) startTimer() {
	if !p.on {
		p.start = time.Now()
		p.on = true
	}
}

func (p *cStatStaging) stopTimer() {
	if p.on {
		p.duration += time.Since(p.start)
		p.on = false
	}
}

type cStats struct {
	lk    sync.Mutex
	stats []cStat
}

func (p *cStats) addStat(level int, n *cStatStaging) {
	p.lk.Lock()
	if level >= len(p.stats) {
		newStats := make([]cStat, level+1)
		copy(newStats, p.stats)
		p.stats = newStats
	}
	p.stats[level].add(n)
	p.lk.Unlock()
}

func (p *cStats) getStat(level int) (duration time.Duration, read, write int64) {
	p.lk.Lock()
	defer p.lk.Unlock()
	if level < len(p.stats) {
		return p.stats[level].get()
	}
	return
}

func (db *DB) compactionError() {
	var err error
noerr:
	// No error.
	for {
		select {
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
				goto haserr
			}
		case <-db.closeC:
			return
		}
	}
haserr:
	// Transient error.
	for {
		select {
		case db.compErrC <- err:
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
				goto noerr
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
			}
		case <-db.closeC:
			return
		}
	}
hasperr:
	// Persistent error.
	for {
		select {
		case db.compErrC <- err:
		case db.compPerErrC <- err:
		case db.writeLockC <- struct{}{}:
			// Hold write lock, so that write won't pass-through.
			db.compWriteLocking = true
		case <-db.closeC:
			if db.compWriteLocking {
				// We should release the lock or Close will hang.
				<-db.writeLockC
			}
			return
		}
	}
}

type compactionTransactCounter int
type compactionTranscatCounter2 int

func (cnt *compactionTransactCounter) incr() {
	*cnt++
}

type compactionTransactInterface interface {
	run(cnt *compactionTransactCounter) error
	revert() error
	run_s( *compactionTransactCounter) error
	revert_s() error
}

func (db *DB) compactionTransact(name string, t compactionTransactInterface) {
	defer func() {
		if x := recover(); x != nil {
			if x == errCompactionTransactExiting {
				if err := t.revert(); err != nil {
					db.logf("%s revert error %q", name, err)
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)
	var (
		backoff  = backoffMin
		backoffT = time.NewTimer(backoff)
		lastCnt  = compactionTransactCounter(0)

		disableBackoff = db.s.o.GetDisableCompactionBackoff()
	)
	for n := 0; ; n++ {
		// Check whether the DB is closed.
		if db.isClosed() {
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			db.logf("%s retrying N·%d", name, n)
		}

		// Execute.
		cnt := compactionTransactCounter(0)
		err := t.run(&cnt)
		if err != nil {
			db.logf("%s error I·%d %q", name, cnt, err)
		}

		// Set compaction error status.
		select {
		case db.compErrSetC <- err:
		case perr := <-db.compPerErrC:
			if err != nil {
				db.logf("%s exiting (persistent error %q)", name, perr)
				db.compactionExitTransact()
			}
		case <-db.closeC:
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		}
		if err == nil {
			return
		}
		if errors.IsCorrupted(err) {
			db.logf("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}

		if !disableBackoff {
			// Reset backoff duration if counter is advancing.
			if cnt > lastCnt {
				backoff = backoffMin
				lastCnt = cnt
			}

			// Backoff.
			backoffT.Reset(backoff)
			if backoff < backoffMax {
				backoff *= backoffMul
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			select {
			case <-backoffT.C:
			case <-db.closeC:
				db.logf("%s exiting", name)
				db.compactionExitTransact()
			}
		}
	}
}//实现类为tablecompactonBuilder
func (db *DB) compactionTransact_s(name string, t compactionTransactInterface) {
	defer func() {
		if x := recover(); x != nil {
			if x == errCompactionTransactExiting {
				if err := t.revert_s(); err != nil {
					db.logf("%s revert error %q", name, err)
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)
	var (
		backoff  = backoffMin
		backoffT = time.NewTimer(backoff)
		lastCnt  = compactionTransactCounter(0) //int

		disableBackoff = db.s.o.GetDisableCompactionBackoff() //bool
	)
	for n := 0; ; n++ {
		// Check whether the DB is closed.
		if db.isClosed() {
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			db.logf("%s retrying N·%d", name, n)
		}

		// Execute.
		cnt := compactionTransactCounter(0)
		err := t.run_s(&cnt) //核心处理逻辑
		if err != nil {
			db.logf("%s error I·%d %q", name, cnt, err)
		}

		// Set compaction error status.
		select {
		case db.compErrSetC <- err:
		case perr := <-db.compPerErrC:
			if err != nil {
				db.logf("%s exiting (persistent error %q)", name, perr)
				db.compactionExitTransact()
			}
		case <-db.closeC:
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		}
		if err == nil {
			return
		}
		if errors.IsCorrupted(err) {
			db.logf("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}

		if !disableBackoff {
			// Reset backoff duration if counter is advancing.
			if cnt > lastCnt {
				backoff = backoffMin
				lastCnt = cnt
			}

			// Backoff.
			backoffT.Reset(backoff)
			if backoff < backoffMax {
				backoff *= backoffMul
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			select {
			case <-backoffT.C:
			case <-db.closeC:
				db.logf("%s exiting", name)
				db.compactionExitTransact()
			}
		}
	}
}

type compactionTransactFunc struct {
	runFunc    func(cnt *compactionTransactCounter) error
	revertFunc func() error
}

func (t *compactionTransactFunc) run_s(cnt *compactionTransactCounter) error {
	return t.runFunc(cnt)
}

func (t *compactionTransactFunc) run(cnt *compactionTransactCounter) error {
	return t.runFunc(cnt)
}

func (t *compactionTransactFunc) revert() error {
	if t.revertFunc != nil {
		return t.revertFunc()
	}
	return nil
}
func (t *compactionTransactFunc) revert_s() error {
	if t.revertFunc != nil {
		return t.revertFunc()
	}
	return nil
}

func (db *DB) compactionTransactFunc(name string, run func(cnt *compactionTransactCounter) error, revert func() error) {
	db.compactionTransact(name, &compactionTransactFunc{run, revert})
}
func (db *DB) compactionTransactFunc_s(name string, run_s func(cnt *compactionTransactCounter) error, revert_s func() error) {
	db.compactionTransact_s(name, &compactionTransactFunc{run_s, revert_s})
}

func (db *DB) compactionExitTransact() {
	panic(errCompactionTransactExiting)
}

func (db *DB) compactionCommit(name string, rec *sessionRecord) {
	db.compCommitLk.Lock()
	defer db.compCommitLk.Unlock() // Defer is necessary.
	db.compactionTransactFunc(name+"@commit", func(cnt *compactionTransactCounter) error {
		return db.s.commit(rec, true)
	}, nil)
}
func (db *DB) compactionCommit_s(name string, rec *sessionRecord) {
	db.compCommitLk.Lock()
	defer db.compCommitLk.Unlock() // Defer is necessary.
	db.compactionTransactFunc_s(name+"@commit", func(cnt *compactionTransactCounter) error {
		return db.s.commit(rec, true)
	}, nil)
}

func (db *DB) memCompaction() {
	//获取到frozenmemdb并判断frozenmemdb是否为空，frozenmemdb是从put写入时的memdb写满后转化来的
	//fmt.Print(" This is Func memCompaction. ")
	mdb := db.getFrozenMem() //指针？
	if mdb == nil {
		return
	}
	defer mdb.decref()

	db.logf("memdb@flush N·%d S·%s", mdb.Len(), shortenb(mdb.Size()))

	// Don't compact empty memdb.
	if mdb.Len() == 0 {
		db.logf("memdb@flush skipping")
		// drop frozen memdb
		db.dropFrozenMem()
		return
	}
	//	fmt.Println("中断tablecompaction")
	//中断tablecompaction，由此可知tablecompaction与memcompaction不会同时进行。
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseC <- (chan<- struct{})(resumeC):
	case <-db.compPerErrC:
		close(resumeC)
		resumeC = nil
	case <-db.closeC:
		db.compactionExitTransact()
	}

	var (
		rec        = &sessionRecord{}
		stats      = &cStatStaging{}
		flushLevel int
	)
	//flushMemdb是把memory内容写到新建的level 0文件，然后把level 0文件加入到addedTables record中
	// Generate tables.代码里把level 0~N的文件叫做table
	db.compactionTransactFunc("memdb@flush", func(cnt *compactionTransactCounter) (err error) {
		stats.startTimer() //时间，此函数无返回值
		//通过flushMemdb将数据刷新到磁盘，这里的mdb.DB为Frozenmem
		//fmt.Print(" ONe is")
		flushLevel, err = db.s.flushMemdb(rec, mdb.DB, db.memdbMaxLevel)
		//fmt.Print(" tw is ")
		stats.stopTimer()
		return
	}, func() error {
		for _, r := range rec.addedTables {
			db.logf("memdb@flush revert @%d", r.num)
			if err := db.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: r.num}); err != nil {
				return err
			}
		}
		return nil
	})

	rec.setJournalNum(db.journalFd.Num)
	rec.setSeqNum(db.frozenSeq)
	//将fulshmemdb的结果进行提交，并记录log，提交的过程主要是为了将新生成的表信息写入到MANIFEST文件中，同时生成新的version
	stats.startTimer()
	db.compactionCommit("memdb", rec)
	stats.stopTimer()

	db.logf("memdb@flush committed F·%d T·%v", len(rec.addedTables), stats.duration)

	// Save compaction stats
	for _, r := range rec.addedTables {
		stats.write += r.size
	}
	db.compStats.addStat(flushLevel, stats)
	atomic.AddUint32(&db.memComp, 1)

	// Drop frozen memdb.minor compaction之后把指向frozon的memory重新放回mempool中
	db.dropFrozenMem()

	// Resume table compaction.回复table compaction
	if resumeC != nil {
		select {
		case <-resumeC:
			close(resumeC)
		case <-db.closeC:
			db.compactionExitTransact()
		}
	}

	// Trigger table compaction.
	db.compTrigger(db.tcompCmdC)
}
func (db *DB) memCompaction_s() {
	//获取到frozenmemdb并判断frozenmemdb是否为空，frozenmemdb是从put写入时的memdb写满后转化来的
	//fmt.Println(" This is Func memCompaction_s. ")
	mdb := db.getFrozenMem_s()
	if mdb == nil {
		return
	}
	defer mdb.decref_s()

	db.logf("memdb@flush N·%d S·%s", mdb.Len_s(), shortenb(mdb.Size_s()))

	// Don't compact empty memdb.
	if mdb.Len_s() == 0 {
		db.logf("memdb@flush skipping")
		// drop frozen memdbs
		db.dropFrozenMem_s()
		return
	}
	//fmt.Print(" 中断tablecompaction ")
	//中断tablecompaction，由此可知tablecompaction与memcompaction不会同时进行。
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseCs <- (chan<- struct{})(resumeC): //resume 只读还是往里写入了数据？,tcompPauseCs中有数据，所以暂停table compaction？
	case <-db.compPerErrCs: //取一下，什么时候放的呢？
		close(resumeC)
		resumeC = nil
	case <-db.closeC:
		db.compactionExitTransact()
	}

	var (
		rec        = &sessionRecord{}
		stats      = &cStatStaging{}
		flushLevel int
	)

	// Generate tables.
	//fmt.Println(" 执行compactionTransactFunc ")
	db.compactionTransactFunc_s("memdb@flush", func(cnt *compactionTransactCounter) (err error) {
		stats.startTimer()
		//通过flushMemdb将数据刷新到磁盘，这里的mdb.DBs为Frozenmem
		flushLevel, err = db.s.flushMemdb_s(rec, mdb.DBs, db.memdbMaxLevel)
		stats.stopTimer()
		return
	}, func() error {
		for _, r := range rec.addedTabless {
			db.logf("memdb@flush revert @%d", r.num)
			if err := db.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: r.num}); err != nil {
				return err
			}
		}
		return nil
	})
	//上面代码没有任何问题，全部成功执行了，生成的信息也被存入到addedTabless中
	//fmt.Println("存储生成的元信息:",rec.addedTabless)

	rec.setJournalNum(db.journalFd2.Num)
	rec.setSeqNum(db.frozenSeq2)
	//将fulshmemdb的结果进行提交，并记录log，提交的过程主要是为了将新生成的表信息写入到MANIFEST文件中，同时生成新的version
	stats.startTimer()
	db.compactionCommit_s("memdb", rec)
	stats.stopTimer()
	//fmt.Println("comapctionCommits完成")
	db.logf("memdb@flush committed F·%d T·%v", len(rec.addedTabless), stats.duration)

	// Save compaction stats
	for _, r := range rec.addedTabless {
		stats.write += r.size
	}
	db.compStats.addStat(flushLevel, stats)
	atomic.AddUint32(&db.memComps, 1) //记录合并次数

	// Drop frozen memdb.
	db.dropFrozenMem_s()

	// Resume table compaction.回复table compaction
	if resumeC != nil {
		select {
		case <-resumeC:
			close(resumeC)
		case <-db.closeC:
			db.compactionExitTransact()
		}
	}

	// Trigger table compaction.
	db.compTrigger(db.tcompCmdCs)
	//fmt.Println("memcompaction_s结束")
}

type tableCompactionBuilder struct {
	db           *DB
	s            *session
	c            *compaction
	rec          *sessionRecord
	stat0, stat1 *cStatStaging //stat0并未使用

	snapHasLastUkey bool
	snapLastUkey    []byte
	snapLastSeq     uint64
	snapIter        int
	snapKerrCnt     int
	snapDropCnt     int

	kerrCnt int
	dropCnt int

	minSeq    uint64
	strict    bool
	tableSize int

	tw *tWriter
}

func (b *tableCompactionBuilder) appendKV(key, value []byte) error {
	// Create new table if not already.
	if b.tw == nil {
		// Check for pause event.
		if b.db != nil {
			select {
			case ch := <-b.db.tcompPauseC:
				b.db.pauseCompaction(ch)
			case <-b.db.closeC:
				b.db.compactionExitTransact()
			default:
			}
		}

		// Create new table.
		var err error
		b.tw, err = b.s.tops.create()
		if err != nil {
			return err
		}
	}

	// Write key/value into table.
	return b.tw.append(key, value)
}
func (b *tableCompactionBuilder) appendKV_s(key, value []byte) error {
	// Create new table if not already.
	if b.tw == nil {
		// Check for pause event.
		if b.db != nil {
			select {
			case ch := <-b.db.tcompPauseCs:
				b.db.pauseCompaction(ch)
			case <-b.db.closeC:
				b.db.compactionExitTransact()
			default:
			}
		}

		// Create new table.
		var err error
		b.tw, err = b.s.tops.create()
		if err != nil {
			return err
		}
	}

	// Write key/value into table.
	return b.tw.append(key, value)
}
func (b *tableCompactionBuilder) needFlush() bool {
	return b.tw.tw.BytesLen() >= b.tableSize
}

func (b *tableCompactionBuilder) flush() error { //run逻辑
	t, err := b.tw.finish() //生成一个tfile
	if err != nil {
		return err
	}
	b.rec.addTableFile(b.c.sourceLevel+1, t)
	b.stat1.write += t.size
	b.s.logf("table@build created L%d@%d N·%d S·%s %q:%q", b.c.sourceLevel+1, t.fd.Num, b.tw.tw.EntriesLen(), shortenb(int(t.size)), t.imin, t.imax)
	b.tw = nil
	return nil
} //跑在run里面
func (b *tableCompactionBuilder) flush_s() error { //run_s逻辑
	t, err := b.tw.finish_s() //sfile
	if err != nil {
		return err
	}
	b.rec.addTableFile_s(b.c.sourceLevel+1, t) //记录合并出来的新的sst，以及写在了哪一层level
	b.stat0.write += t.size
	b.s.logf("table@build created L%d@%d N·%d S·%s %q:%q", b.c.sourceLevel+1, t.fd.Num, b.tw.tw.EntriesLen(), shortenb(int(t.size)), t.imin, t.imax)
	b.tw = nil
	return nil
} //应该用以另一个compaction中

func (b *tableCompactionBuilder) cleanup() {
	if b.tw != nil {
		b.tw.drop()
		b.tw = nil
	}
}
//tablecompaction中的read、sort、write的过程
func (b *tableCompactionBuilder) run(cnt *compactionTransactCounter) error {
	//fmt.Print(" runing ")
	snapResumed := b.snapIter > 0
	hasLastUkey := b.snapHasLastUkey // The key might has zero length, so this is necessary.
	lastUkey := append([]byte{}, b.snapLastUkey...)
	lastSeq := b.snapLastSeq
	b.kerrCnt = b.snapKerrCnt
	b.dropCnt = b.snapDropCnt
	// Restore compaction state.
	b.c.restore()

	defer b.cleanup()

	b.stat1.startTimer()
	defer b.stat1.stopTimer()
	//read
	iter := b.c.newIterator()
	defer iter.Release()
	//sort，按照key的大小读出来，iter.Next就是当前打开文件的最小key
	for i := 0; iter.Next(); i++ {
		// Incr transact counter.
		cnt.incr()

		// Skip until last state.
		if i < b.snapIter {
			continue
		}

		resumed := false
		if snapResumed {
			resumed = true
			snapResumed = false
		}

		ikey := iter.Key()
		ukey, seq, kt, kerr := parseInternalKey(ikey)

		if kerr == nil {
			shouldStop := !resumed && b.c.shouldStopBefore(ikey)

			if !hasLastUkey || b.s.icmp.uCompare(lastUkey, ukey) != 0 {
				// First occurrence of this user key.

				// Only rotate tables if ukey doesn't hop across.
				if b.tw != nil && (shouldStop || b.needFlush()) {
					if err := b.flush(); err != nil {
						return err
					}

					// Creates snapshot of the state.
					b.c.save()
					b.snapHasLastUkey = hasLastUkey
					b.snapLastUkey = append(b.snapLastUkey[:0], lastUkey...)
					b.snapLastSeq = lastSeq
					b.snapIter = i
					b.snapKerrCnt = b.kerrCnt
					b.snapDropCnt = b.dropCnt
				}

				hasLastUkey = true
				lastUkey = append(lastUkey[:0], ukey...)
				lastSeq = keyMaxSeq
			}

			switch {
			case lastSeq <= b.minSeq:
				// Dropped because newer entry for same user key exist
				fallthrough // (A)
			case kt == keyTypeDel && seq <= b.minSeq && b.c.baseLevelForKey(lastUkey):
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger seq numbers
				// (3) data in layers that are being compacted here and have
				//     smaller seq numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				lastSeq = seq
				b.dropCnt++
				continue
			default:
				lastSeq = seq
			}
		} else {
			if b.strict {
				return kerr
			}

			// Don't drop corrupted keys.
			hasLastUkey = false
			lastUkey = lastUkey[:0]
			lastSeq = keyMaxSeq
			b.kerrCnt++
		}
		//write写操作
		if err := b.appendKV(ikey, iter.Value()); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Finish last table.
	if b.tw != nil && !b.tw.empty() {
		return b.flush()
	}
	return nil
}
func (b *tableCompactionBuilder) run_s(cnt *compactionTransactCounter) error {
	//fmt.Print(" runing ")
	snapResumed := b.snapIter > 0
	hasLastUkey := b.snapHasLastUkey // The key might has zero length, so this is necessary.
	lastUkey := append([]byte{}, b.snapLastUkey...)
	lastSeq := b.snapLastSeq
	b.kerrCnt = b.snapKerrCnt
	b.dropCnt = b.snapDropCnt
	// Restore compaction state.
	b.c.restore() //ref--

	defer b.cleanup()

	b.stat0.startTimer()
	defer b.stat0.stopTimer()
	//read
	iter := b.c.newIterator_s() //将文件打开并缓存，返回iter
	defer iter.Release()
	//sort，按照key的大小读出来，iter.Next就是当前打开文件的最小key
	for i := 0; iter.Next(); i++ {
		// Incr transact counter.
		cnt.incr() //*cnt++

		// Skip until last state.
		if i < b.snapIter {
			continue
		}

		resumed := false
		if snapResumed {
			resumed = true
			snapResumed = false
		}

		ikey := iter.Key()
		ukey, seq, kt, kerr := parseInternalKey(ikey)

		if kerr == nil {
			shouldStop := !resumed && b.c.shouldStopBefore_s(ikey)

			if !hasLastUkey || b.s.icmp.uCompare(lastUkey, ukey) != 0 {
				// First occurrence of this user key.

				// Only rotate tables if ukey doesn't hop across.
				if b.tw != nil && (shouldStop || b.needFlush()) {
					if err := b.flush_s(); err != nil {
						return err
					}

					// Creates snapshot of the state.
					b.c.save()
					b.snapHasLastUkey = hasLastUkey
					b.snapLastUkey = append(b.snapLastUkey[:0], lastUkey...)
					b.snapLastSeq = lastSeq
					b.snapIter = i
					b.snapKerrCnt = b.kerrCnt
					b.snapDropCnt = b.dropCnt
				}

				hasLastUkey = true
				lastUkey = append(lastUkey[:0], ukey...)
				lastSeq = keyMaxSeq
			}

			switch {
			case lastSeq <= b.minSeq:
				// Dropped because newer entry for same user key exist
				fallthrough // (A)
			case kt == keyTypeDel && seq <= b.minSeq && b.c.baseLevelForKey_s(lastUkey):
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger seq numbers
				// (3) data in layers that are being compacted here and have
				//     smaller seq numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				lastSeq = seq
				b.dropCnt++
				continue
			default:
				lastSeq = seq
			}
		} else {
			if b.strict {
				return kerr
			}

			// Don't drop corrupted keys.
			hasLastUkey = false
			lastUkey = lastUkey[:0]
			lastSeq = keyMaxSeq
			b.kerrCnt++
		}
		//write写操作
		if err := b.appendKV_s(ikey, iter.Value()); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Finish last table.
	if b.tw != nil && !b.tw.empty() {
		return b.flush_s()
	}
	return nil
}
func (b *tableCompactionBuilder) revert() error {
	for _, at := range b.rec.addedTables {
		b.s.logf("table@build revert @%d", at.num)
		if err := b.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: at.num}); err != nil {
			return err
		}
	}
	return nil
}
func (b *tableCompactionBuilder) revert_s() error {
	for _, at := range b.rec.addedTabless {
		b.s.logf("table@build revert @%d", at.num)
		if err := b.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: at.num}); err != nil {
			return err
		}
	}
	return nil
}
//tablecompaction的核心只有2步，build && commit。 其中build的过程db.compactionTransact(“table@build”, b)是将
// 需要合并的表读出来，排序，写到新表，即read,sort,write 3个步骤。compactionTransact的核心在于run()，其他的都是变量定义和异常处理
//c包含了要合并的表的信息
//t compaction -> table Auoto Compaction -> pickCompaction(取c) -> new compaction -> c.expand -> table compaction
func (db *DB) tableCompaction(c *compaction, noTrivial bool) {
	defer c.release()

	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax) //rec.compPtrs = append(p.compPtrs, cpRecord{level, ikey})

	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.sourceLevel+1, t)
		db.compactionCommit("table-move", rec)
		return
	}

	var stats [2]cStatStaging
	for i, tables := range c.levels { //遍历levels中所有的tfile得到第一层的tfile和第二层的tfile？？？？
		for _, t := range tables {
			stats[i].read += t.size
			// Insert deleted tables into record
			rec.delTable(c.sourceLevel+i, t.fd.Num)
		}
	}
	sourceSize := int(stats[0].read + stats[1].read)
	minSeq := db.minSeq()
	db.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)

	b := &tableCompactionBuilder{
		db:        db,
		s:         db.s,
		c:         c,
		rec:       rec,
		stat1:     &stats[1],
		minSeq:    minSeq,
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
	}
	//将需要合并的表读出来，排序，写到新表
	db.compactionTransact("table@build", b)

	// Commit.提交，主要是写入version和manifest
	stats[1].startTimer()
	db.compactionCommit("table", rec)
	stats[1].stopTimer()

	resultSize := int(stats[1].write)
	db.logf("table@compaction committed F%s S%s Ke·%d D·%d T·%v", sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), b.kerrCnt, b.dropCnt, stats[1].duration)

	// Save compaction stats
	for i := range stats {
		db.compStats.addStat(c.sourceLevel+1, &stats[i])
	}
	switch c.typ {
	case level0Compaction:
		atomic.AddUint32(&db.level0Comp, 1)
	case nonLevel0Compaction:
		atomic.AddUint32(&db.nonLevel0Comp, 1)
	case seekCompaction:
		atomic.AddUint32(&db.seekComp, 1)
	}
}
func (db *DB) tableCompaction_s(c *compaction, noTrivial bool) {
	defer c.release()
	//fmt.Println("执行tableCompaction_s")
	rec := &sessionRecord{}
	rec.addCompPtr_s(c.sourceLevel, c.imax) //这里是每次合并的断点？

	if !noTrivial && c.trivial_s() {
		t := c.level_s[0][0]//合并的那一层的第一个sfile？
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable_s(c.sourceLevel, t.fd.Num)
		rec.addTableFile_s(c.sourceLevel+1, t)
		db.compactionCommit_s("table-move", rec)
		return
	}

	var stats [2]cStatStaging
	for i, tables := range c.level_s {
		for _, t := range tables {
			stats[i].read += t.size
			// Insert deleted tables into record,~~~~i取值0、1,把要删除的两层的文件记录，放入deletedtabless中
			rec.delTable_s(c.sourceLevel+i, t.fd.Num)
		}
	}
	sourceSize := int(stats[0].read + stats[1].read)
	minSeq := db.minSeq()
	db.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.sourceLevel, len(c.level_s[0]), c.sourceLevel+1, len(c.level_s[1]), shortenb(sourceSize), minSeq)

	b := &tableCompactionBuilder{
		db:        db,
		s:         db.s,
		c:         c,
		rec:       rec,
		stat0:     &stats[1], //第二层
		minSeq:    minSeq,
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
	}
	//将需要合并的表读出来，排序，写到新表,这是build的重点
	db.compactionTransact_s("table@build", b)//addedtabless应该是记录新的sfiles了

	// Commit.提交
	stats[1].startTimer()
	db.compactionCommit_s("table", rec) //绝对没有问题，已经在memdb中测试过
	stats[1].stopTimer()

	resultSize := int(stats[1].write)
	db.logf("table@compaction committed F%s S%s Ke·%d D·%d T·%v", sint(len(rec.addedTabless)-len(rec.deletedTabless)), sshortenb(resultSize-sourceSize), b.kerrCnt, b.dropCnt, stats[1].duration)

	// Save compaction stats
	for i := range stats {
		db.compStats.addStat(c.sourceLevel+1, &stats[i])
	}
	switch c.typ {
	case level0Compaction:
		atomic.AddUint32(&db.level0Comps, 1)
	case nonLevel0Compaction:
		atomic.AddUint32(&db.nonLevel0Comps, 1)
	case seekCompaction:
		atomic.AddUint32(&db.seekComp, 1)
	}
}

func (db *DB) tableRangeCompaction(level int, umin, umax []byte) error {
	db.logf("table@compaction range L%d %q:%q", level, umin, umax)
	if level >= 0 {
		if c := db.s.getCompactionRange(level, umin, umax, true); c != nil {
			db.tableCompaction(c, true)
		}
	} else {
		// Retry until nothing to compact.
		for {
			compacted := false

			// Scan for maximum level with overlapped tables.
			v := db.s.version()
			m := 1
			for i := m; i < len(v.levels); i++ {
				tables := v.levels[i]
				if tables.overlaps(db.s.icmp, umin, umax, false) {
					m = i
				}
			}
			v.release()

			for level := 0; level < m; level++ {
				if c := db.s.getCompactionRange(level, umin, umax, false); c != nil {
					db.tableCompaction(c, true)
					compacted = true
				}
			}

			if !compacted {
				break
			}
		}
	}

	return nil
}
func (db *DB) tableRangeCompaction_s(level int, umin, umax []byte) error {
	db.logf("table@compaction range L%d %q:%q", level, umin, umax)
	if level >= 0 {
		if c := db.s.getCompactionRange_s(level, umin, umax, true); c != nil {
			db.tableCompaction_s(c, true)
		}
	} else {
		// Retry until nothing to compact.
		for {
			compacted := false

			// Scan for maximum level with overlapped tables.
			v := db.s.version()
			m := 1
			for i := m; i < len(v.level_s); i++ {
				tables := v.level_s[i]
				if tables.overlaps(db.s.icmp, umin, umax, false) {
					m = i
				}
			}
			v.release()

			for level := 0; level < m; level++ {
				if c := db.s.getCompactionRange_s(level, umin, umax, false); c != nil {
					db.tableCompaction_s(c, true)
					compacted = true
				}
			}

			if !compacted {
				break
			}
		}
	}

	return nil
}

func (db *DB) tableAutoCompaction() {
	//fmt.Println("This is tableAutoCompaction")
	if c := db.s.pickCompaction(); c != nil { //c会返回一个compaction类型，包含了要合并的文件的tfiles
		db.tableCompaction(c, false)
	}
}
func (db *DB) tableAutoCompaction_s() {
	//fmt.Println("This is tableAutoCompaction_s")
	if c := db.s.pickCompaction_s(); c != nil {
		db.tableCompaction_s(c, false)
	}
}

func (db *DB) tableNeedCompaction() bool {
	v := db.s.version()
	defer v.release()
	return v.needCompaction()
}
func (db *DB) tableNeedCompaction_s() bool {
	v := db.s.version()
	defer v.release()
	return v.needCompaction_s()
}

// 如果压缩了足够的level0文件，resumeWrite返回一个指示符，指示我们是否应该恢复写操作。
func (db *DB) resumeWrite() bool {
	v := db.s.version()
	defer v.release()
	if v.tLen(0) < db.s.o.GetWriteL0PauseTrigger() {
		return true
	}
	return false
}
func (db *DB) resumeWrite_s() bool {
	v := db.s.version()
	defer v.release()
	if v.tLen_s(0) < db.s.o.GetWriteL0PauseTrigger() { //12,如果l0有12个，就停止写入
		return true
	}
	return false
}

func (db *DB) pauseCompaction(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	case <-db.closeC:
		db.compactionExitTransact()
	}
}

type cCmd interface {
	ack(err error)
}

type cAuto struct {
	// Note for table compaction, an non-empty ackC represents it's a compaction waiting command.
	ackC chan<- error
}

func (r cAuto) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

type cRange struct {
	level    int
	min, max []byte
	ackC     chan<- error
}

func (r cRange) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

// This will trigger auto compaction but will not wait for it.
// 写compCmdc
func (db *DB) compTrigger(compC chan<- cCmd) {
	select {
	case compC <- cAuto{}:
	default:
	}
}

// This will trigger auto compaction and/or wait for all compaction to be done.
//这将触发自动压缩或者等待所有的压缩完成 rotateMem会传参数mcompCmdc
func (db *DB) compTriggerWait(compC chan<- cCmd) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cAuto{ch}: //rotateMem会传参数mcompCmdc,执行了写入
	case err = <-db.compErrC:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}
func (db *DB) compTriggerWait_s(compC chan<- cCmd) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cAuto{ch}:
	case err = <-db.compErrCs:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrCs:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

// Send range compaction request.
func (db *DB) compTriggerRange(compC chan<- cCmd, level int, min, max []byte) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cRange{level, min, max, ch}:
	case err := <-db.compErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}
//mem compaction
func (db *DB) mCompaction() {
	var x cCmd
	//fmt.Println("This is Func mCompaction().")
	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		select {
		case x = <-db.mcompCmdC:
			switch x.(type) {
			case cAuto: //自动？
				db.memCompaction()
				x.ack(nil)
				x = nil
			default:
				panic("leveldb: unknown command")
			}
		case <-db.closeC:
			return
		}
	}
}
func (db *DB) mCompaction_s() {
	var x cCmd
	//fmt.Println("This is Func mCompaction_s().")
	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		select {
		case x = <-db.mcompCmdCs:
			switch x.(type) {
			case cAuto:
				db.memCompaction_s()
				x.ack(nil)
				x = nil
			default:
				panic("leveldb: unknown command")
			}
		case <-db.closeC:
			return
		}
	}
}
//table compaction
func (db *DB) tCompaction() { //一定会执行tableAutocompaction
	var (
		x     cCmd
		waitQ []cCmd
	)

	defer func() {
		if x := recover(); x != nil { //捕捉错误
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		for i := range waitQ {
			waitQ[i].ack(ErrClosed)
			waitQ[i] = nil
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()
	//fmt.Println("This is Func tCompaction().")
	for {
		if db.tableNeedCompaction() {//调用v.needcompation，查看cScore等，是否要触发compaction
			select {
			case x = <-db.tcompCmdC://往外写出，但是什么时候写入的呢？应该是当size饱和或者seek达到阈值的时候，这里体现在needcompaction中
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			default:
			}
			// Resume write operation as soon as possible.回复写操作
			if len(waitQ) > 0 && db.resumeWrite() {
				for i := range waitQ {
					waitQ[i].ack(nil)
					waitQ[i] = nil
				}
				waitQ = waitQ[:0]
			}
		} else {
			for i := range waitQ {
				waitQ[i].ack(nil)
				waitQ[i] = nil
			}
			waitQ = waitQ[:0]
			//fmt.Println("GOOD1")
			select { //实在这里实现同步的，因为没有default，所以会卡在这个地方!
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			}
		}
		//fmt.Println("GOOD2")
		if x != nil {
			switch cmd := x.(type) {
			case cAuto:
				if cmd.ackC != nil {
					// Check the write pause state before caching it.
					if db.resumeWrite() {
						x.ack(nil)
					} else {
						waitQ = append(waitQ, x)
					}
				}
			case cRange:
				x.ack(db.tableRangeCompaction(cmd.level, cmd.min, cmd.max))
			default:
				panic("leveldb: unknown command")
			}
			x = nil
		}
	//	fmt.Println("GOOD")
		db.tableAutoCompaction()
	}
}
func (db *DB) tCompaction_s() {
	var (
		x     cCmd
		waitQ []cCmd
	)
	defer func() { //跑不通问题出现在此处
		if x := recover(); x != nil { //recover停止panic，并调用取回错误值
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		for i := range waitQ {
			waitQ[i].ack(ErrClosed)
			waitQ[i] = nil
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()
	//fmt.Println("This is Func tCompaction_s().")
	for {
		if db.tableNeedCompaction_s() {//cscore>1 ||seek那个地方，即触发合并的条件，存在default，真正的无限循环？
			select {
			case x = <-db.tcompCmdCs:
			case ch := <-db.tcompPauseCs:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			default:
			}
			// Resume write operation as soon as possible.回复写操作，用的同一个会不会有问题
			if len(waitQ) > 0 && db.resumeWrite_s() {
				for i := range waitQ {
					waitQ[i].ack(nil)
					waitQ[i] = nil
				}
				waitQ = waitQ[:0]
			}
		} else {
			for i := range waitQ {
				waitQ[i].ack(nil)
				waitQ[i] = nil
			}
			waitQ = waitQ[:0]
			select {
			case x = <-db.tcompCmdCs:
			case ch := <-db.tcompPauseCs:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			}
		}
		if x != nil {
			switch cmd := x.(type) {
			case cAuto:
				if cmd.ackC != nil {
					// Check the write pause state before caching it.
					if db.resumeWrite_s() { //恢复写操作，用的同一个，会不会有问题
						x.ack(nil)
					} else {
						waitQ = append(waitQ, x)
					}
				}
			case cRange:
				x.ack(db.tableRangeCompaction_s(cmd.level, cmd.min, cmd.max))
			default:
				panic("leveldb: unknown command")
			}
			x = nil
		}
		db.tableAutoCompaction_s()
	}
}