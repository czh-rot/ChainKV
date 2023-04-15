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
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type tSet struct {
	level int
	table *tFile
}
type tSet_s struct {
	level int
	table *sFile
}
////一个版本信息维护一层所有文件的元数据 VerisonNew = VersionOld + VerisonEdit，manifest用来记录VersionEdit（变化的内容）
//每当compaction完成，都会创建一个新的Verison，VersionEdit包括：新增哪些sst、删除哪些sst、当前compaction的下标、日志文件编号、操作seqNumber等信息
//current记录manifest的文件名，指出最新的有用的那个manifest文件
type version struct {
	id int64 // unique monotonous increasing version id
	s  *session

	levels []tFiles //元数据
	level_s []sFiles

	// Level that should be compacted next and its compaction score.
	// Score < 1 means compaction is not strictly needed. These fields
	// are initialized by computeCompaction()
	cLevel int //记录的是LSM1
	cScore float64
	cLevels int //记录另外一个LSM
	cScores float64

	cSeek unsafe.Pointer
	nSeek unsafe.Pointer
	closing  bool
	ref      int //记录sst的引用？？？？？
	released bool
}

// newVersion creates a new version with an unique monotonous increasing id.
func newVersion(s *session) *version { //只有s和id有值
	id := atomic.AddInt64(&s.ntVersionId, 1)
	nv := &version{s: s, id: id - 1}
	return nv
}

func (v *version) incref() {
	if v.released {
		panic("already released")
	}

	v.ref++
	if v.ref == 1 {
		select {
		case v.s.refCh <- &vTask{vid: v.id, files: v.levels, sfiles: v.level_s, created: time.Now()}:
			// We can use v.levels and v,level_s directly here since it is immutable.
		case <-v.s.closeC:
			v.s.log("reference loop already exist")
		}
	}
}

func (v *version) releaseNB() {
	v.ref--
	if v.ref > 0 {
		return
	} else if v.ref < 0 {
		panic("negative version ref")
	}
	select {
	case v.s.relCh <- &vTask{vid: v.id, files: v.levels, sfiles: v.level_s, created: time.Now()}:
		// We can use v.levels directly here since it is immutable.
	case <-v.s.closeC:
		v.s.log("reference loop already exist")
	}

	v.released = true
}

func (v *version) release() {
	v.s.vmu.Lock()
	v.releaseNB()
	v.s.vmu.Unlock()
}

func (v *version) walkOverlapping(aux tFiles, ikey internalKey, f func(level int, t *tFile) bool, lf func(level int) bool) {
	ukey := ikey.ukey()
	// Aux level.
	if aux != nil {
		for _, t := range aux {
			if t.overlaps(v.s.icmp, ukey, ukey) {
				if !f(-1, t) {
					return
				}
			}
		}

		if lf != nil && !lf(-1) {
			return
		}
	}
	//fmt.Println("This is levels:",v.levels)
	// Walk tables level-by-level.一层层遍历tfile
	for level, tables := range v.levels {
		if len(tables) == 0 {
			continue
		}

		if level == 0 {
			// Level-0 files may overlap each other. Find all files that
			// overlap ukey.//找到所有重叠的key
			for _, t := range tables {
				if t.overlaps(v.s.icmp, ukey, ukey) {
					if !f(level, t) {
						return
					}
				}
			}
		} else {
			if i := tables.searchMax(v.s.icmp, ikey); i < len(tables) {
				t := tables[i]
				if v.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					if !f(level, t) {
						return
					}
				}
			}
		}

		if lf != nil && !lf(level) {
			return
		}
	}
}
func (v *version) walkOverlapping_s(aux sFiles, ikey internalKey, f func(level int, t *sFile) bool, lf func(level int) bool) {
	ukey := ikey.ukey()
	// Aux level.aux is nil,所以一开始不走if
	if aux != nil {
		for _, t := range aux {
			if t.overlaps(v.s.icmp, ukey, ukey) {
				if !f(-1, t) {
					return
				}
			}
		}

		if lf != nil && !lf(-1) {
			return
		}
	}
	if v.level_s ==nil{
		fmt.Println("is nil")
	}else{
		//fmt.Println("This is level_s:",v.level_s) //此值为空，这是问题所在
	}
	// Walk tables level-by-level.
	for level, tables := range v.level_s {
		if len(tables) == 0 {
			continue
		}

		if level == 0 {
			// Level-0 files may overlap each other. Find all files that
			// overlap ukey.
			for _, t := range tables {
				if t.overlaps(v.s.icmp, ukey, ukey) {
					if !f(level, t) {
						return
					}
				}
			}
		} else {
			if i := tables.searchMax(v.s.icmp, ikey); i < len(tables) {
				t := tables[i]
				if v.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					if !f(level, t) {
						return
					}
				}
			}
		}

		if lf != nil && !lf(level) {
			return
		}
	}
}
//tfile为sst文件的元数据，tfiles持有多个sst文件？
//1.根据ikey，定位可能存在ikey的文件
//2.读取文件，找到ikey和ivalue
//3.如果当前在L0找到，根据f seq取最新的数据

func (v *version) get(aux tFiles, ikey internalKey, ro *opt.ReadOptions, noValue bool) (value []byte, tcomp bool, err error) {
	if v.closing {
		return nil, false, ErrClosed
	}
	//根据internalKey获得userKey
	ukey := ikey.ukey()
	sampleSeeks := !v.s.o.GetDisableSeeksCompaction() //true or false ,这是是true

	var (
		tset  *tSet //一个level int、还有一个table *tfile
		tseek bool //默认为false

		// Level-0.
		zfound bool
		zseq   uint64
		zkt    keyType //插入还是删除？
		zval   []byte
	)

	err = ErrNotFound

	// Since entries never hop across level, finding key/value
	// in smaller level make later levels irrelevant. walkoverlapping 是用来定位ikey位于哪个文件中的
	v.walkOverlapping(aux, ikey, func(level int, t *tFile) bool { //把func作为参数
		if sampleSeeks && level >= 0 && !tseek { //s为true，level为0
			if tset == nil {
				tset = &tSet{level, t} //0，tfile
			} else {
				tseek = true
			}
		}

		var (
			fikey, fval []byte //找到的kv键值对?
			ferr        error
		)
		if noValue { //noValue is false
			fikey, ferr = v.s.tops.findKey(t, ikey, ro)//t为tfile类型
		} else {
			fikey, fval, ferr = v.s.tops.find(t, ikey, ro)//在一个tfile里面进行查找？
		}
		switch ferr {
		case nil:
		case ErrNotFound:
			return true
		default:
			err = ferr
			return false
		}
		//这里是为了跟找到的文件中的key进行比较，确认最新的数据
		if fukey, fseq, fkt, fkerr := parseInternalKey(fikey); fkerr == nil {
			if v.s.icmp.uCompare(ukey, fukey) == 0 {
				// Level <= 0 may overlaps each-other.
				if level <= 0 {
					if fseq >= zseq {
						zfound = true
						zseq = fseq
						zkt = fkt
						zval = fval
					}
				} else {
					switch fkt {
					case keyTypeVal:
						value = fval
						err = nil
					case keyTypeDel:
					default:
						panic("leveldb: invalid internalKey type")
					}
					return false
				}
			}
		} else {
			err = fkerr
			return false
		}

		return true
	}, func(level int) bool {
		if zfound {
			switch zkt {
			case keyTypeVal:
				value = zval
				err = nil
			case keyTypeDel:
			default:
				panic("leveldb: invalid internalKey type")
			}
			return false
		}

		return true
	})

	if tseek && tset.table.consumeSeek() <= 0 {
		tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
	}

	return
}

func (v *version) get_s(aux sFiles, ikey internalKey, ro *opt.ReadOptions, noValue bool) (value []byte, tcomp bool, err error) {
	//aux nil
	if v.closing {
		return nil, false, ErrClosed
	}
	//根据internalKey获得userKey
	ukey := ikey.ukey()
	sampleSeeks := !v.s.o.GetDisableSeeksCompaction() //true

	var (
		tset  *tSet_s
		tseek bool

		// Level-0.
		zfound bool
		zseq   uint64
		zkt    keyType
		zval   []byte
	)

	err = ErrNotFound
	// Since entries never hop across level, finding key/value
	// in smaller level make later levels irrelevant.意思是从上往下找
	v.walkOverlapping_s(aux, ikey, func(level int, t *sFile) bool {
		if sampleSeeks && level >= 0 && !tseek {
			if tset == nil {
				tset = &tSet_s{level,t}
			} else {
				tseek = true
			}
		}

		var (
			fikey, fval []byte
			ferr        error
		)
		if noValue {
			fikey, ferr = v.s.tops.findKey_s(t, ikey, ro)
		} else {
			fikey, fval, ferr = v.s.tops.find_s(t, ikey, ro)
		}
		switch ferr {
		case nil:
		case ErrNotFound:
			return true
		default:
			err = ferr
			return false
		}

		if fukey, fseq, fkt, fkerr := parseInternalKey(fikey); fkerr == nil {
			if v.s.icmp.uCompare(ukey, fukey) == 0 {
				// Level <= 0 may overlaps each-other.
				if level <= 0 {
					if fseq >= zseq {
						zfound = true
						zseq = fseq
						zkt = fkt
						zval = fval
					}
				} else {
					switch fkt {
					case keyTypeVal:
						value = fval
						err = nil
					case keyTypeDel:
					default:
						panic("leveldb: invalid internalKey type")
					}
					return false
				}
			}
		} else {
			err = fkerr
			return false
		}

		return true
	}, func(level int) bool {
		if zfound {
			switch zkt {
			case keyTypeVal:
				value = zval
				err = nil
			case keyTypeDel:
			default:
				panic("leveldb: invalid internalKey type")
			}
			return false
		}

		return true
	})

	if tseek && tset.table.consumeSeek() <= 0 {
		tcomp = atomic.CompareAndSwapPointer(&v.nSeek, nil, unsafe.Pointer(tset))
	}

	return
}

func (v *version) sampleSeek(ikey internalKey) (tcomp bool) {
	var tset *tSet

	v.walkOverlapping(nil, ikey, func(level int, t *tFile) bool {
		if tset == nil {
			tset = &tSet{level, t}
			return true
		}
		if tset.table.consumeSeek() <= 0 {
			tcomp = atomic.CompareAndSwapPointer(&v.cSeek, nil, unsafe.Pointer(tset))
		}
		return false
	}, nil)

	return
}
func (v *version) sampleSeek_s(ikey internalKey) (tcomp bool) {
	var tset *tSet_s

	v.walkOverlapping_s(nil, ikey, func(level int, t *sFile) bool {
		if tset == nil {
			tset = &tSet_s{level, t}
			return true
		}
		if tset.table.consumeSeek() <= 0 {
			tcomp = atomic.CompareAndSwapPointer(&v.nSeek, nil, unsafe.Pointer(tset))
		}
		return false
	}, nil)

	return
}

func (v *version) getIterators(slice *util.Range, ro *opt.ReadOptions) (its []iterator.Iterator) {
	strict := opt.GetStrict(v.s.o.Options, ro, opt.StrictReader)
	for level, tables := range v.levels {
		if level == 0 {
			// Merge all level zero files together since they may overlap.
			for _, t := range tables {
				its = append(its, v.s.tops.newIterator(t, slice, ro))
			}
		} else if len(tables) != 0 {
			its = append(its, iterator.NewIndexedIterator(tables.newIndexIterator(v.s.tops, v.s.icmp, slice, ro), strict))
		}
	}
	return
}
func (v *version) getIterators_s(slice *util.Range, ro *opt.ReadOptions) (its []iterator.Iterator) {
	strict := opt.GetStrict(v.s.o.Options, ro, opt.StrictReader)
	for level, tables := range v.level_s {
		if level == 0 {
			// Merge all level zero files together since they may overlap.
			for _, t := range tables {
				its = append(its, v.s.tops.newIterator_s(t, slice, ro))
			}
		} else if len(tables) != 0 {
			its = append(its, iterator.NewIndexedIterator(tables.newIndexIterator(v.s.tops, v.s.icmp, slice, ro), strict))
		}
	}
	return
}
func (v *version) newStaging() *versionStaging {
	return &versionStaging{base: v}
}

// Spawn a new version based on this version.
func (v *version) spawn(r *sessionRecord, trivial bool) *version {
//	fmt.Println("进入spawn函数，其中r为:",r)
	staging := v.newStaging()
	staging.commit(r)
	return staging.finish(trivial)
}
func (v *version) spawn_1(r *sessionRecord, trivial bool) *version {
	//	fmt.Println("进入spawn函数，其中r为:",r)
	staging := v.newStaging()
	staging.commit_1(r)
	return staging.finish_1(trivial)
}
func (v *version) spawn_s(r *sessionRecord, trivial bool) *version {
	//fmt.Println("进入spawn_s函数，其中r为:",r)
	staging := v.newStaging() //new base *v,levels []tableS
	staging.commit_2(r) //return rec *session
	return staging.finish_2(trivial)
}
//遍历levels，写入addTableFIle
func (v *version) fillRecord(r *sessionRecord) {
	for level, tables := range v.levels {
		for _, t := range tables {
			r.addTableFile(level, t)
		}
	}
}
func (v *version) fillRecord_s(r *sessionRecord) {
	for level, tables := range v.level_s {
		for _, t := range tables {
			r.addTableFile_s(level, t)
		}
	}
}
func (v *version) tLen(level int) int {
	if level < len(v.levels) {
		return len(v.levels[level])
	}
	return 0
}
func (v *version) tLen_s(level int) int {
	if level < len(v.level_s) {
		return len(v.level_s[level])
	}
	return 0
}
//用于test
func (v *version) offsetOf(ikey internalKey) (n int64, err error) {
	for level, tables := range v.levels {
		for _, t := range tables {
			if v.s.icmp.Compare(t.imax, ikey) <= 0 {
				// Entire file is before "ikey", so just add the file size
				n += t.size
			} else if v.s.icmp.Compare(t.imin, ikey) > 0 {
				// Entire file is after "ikey", so ignore
				if level > 0 {
					// Files other than level 0 are sorted by meta->min, so
					// no further files in this level will contain data for
					// "ikey".
					break
				}
			} else {
				// "ikey" falls in the range for this table. Add the
				// approximate offset of "ikey" within the table.
				if m, err := v.s.tops.offsetOf(t, ikey); err == nil {
					n += m
				} else {
					return 0, err
				}
			}
		}
	}

	return
}

func (v *version) pickMemdbLevel(umin, umax []byte, maxLevel int) (level int) {
	if maxLevel > 0 {
		if len(v.levels) == 0 {
			return maxLevel
		}
		if !v.levels[0].overlaps(v.s.icmp, umin, umax, true) {
			var overlaps tFiles
			for ; level < maxLevel; level++ {
				if pLevel := level + 1; pLevel >= len(v.levels) {
					return maxLevel
				} else if v.levels[pLevel].overlaps(v.s.icmp, umin, umax, false) {
					break
				}
				if gpLevel := level + 2; gpLevel < len(v.levels) {
					overlaps = v.levels[gpLevel].getOverlaps(overlaps, v.s.icmp, umin, umax, false)
					if overlaps.size() > int64(v.s.o.GetCompactionGPOverlaps(level)) {
						break
					}
				}
			}
		}
	}
	return
}
func (v *version) pickMemdbLevel_s(umin, umax []byte, maxLevel int) (level int) {
	if maxLevel > 0 {
		if len(v.level_s) == 0 {
			return maxLevel
		}
		if !v.level_s[0].overlaps(v.s.icmp, umin, umax, true) {
			var overlaps sFiles
			for ; level < maxLevel; level++ {
				if pLevel := level + 1; pLevel >= len(v.level_s) {
					return maxLevel
				} else if v.level_s[pLevel].overlaps(v.s.icmp, umin, umax, false) {
					break
				}
				if gpLevel := level + 2; gpLevel < len(v.level_s) {
					overlaps = v.level_s[gpLevel].getOverlaps(overlaps, v.s.icmp, umin, umax, false)
					if overlaps.size() > int64(v.s.o.GetCompactionGPOverlaps(level)) {
						break
					}
				}
			}
		}
	}
	return
}
func (v *version) computeCompaction() {
	// Precomputed best level for next compaction
	bestLevel := int(-1)
	bestScore := float64(-1)

	statFiles := make([]int, len(v.levels))
	statSizes := make([]string, len(v.levels))
	statScore := make([]string, len(v.levels))
	statTotSize := int64(0)
	//遍历[]tfiles
	for level, tables := range v.levels {
		var score float64
		size := tables.size()//所有sst.size的和
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compaction.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = float64(len(tables)) / float64(v.s.o.GetCompactionL0Trigger()) // 文件个数/4
		} else {
			score = float64(size) / float64(v.s.o.GetCompactionTotalSize(level)) //文件的总大小/预设的每个level的文件大小总量
		}

		if score > bestScore {
			bestLevel = level
			bestScore = score
		}

		statFiles[level] = len(tables)
		statSizes[level] = shortenb(int(size))
		statScore[level] = fmt.Sprintf("%.2f", score)
		statTotSize += size
	}
	//最后找出算出的值最大的一个赋值到v.cScore，level赋值到v.cLevel，其实选出当前最满的那一层
	v.cLevel = bestLevel
	v.cScore = bestScore

	v.s.logf("version@stat F·%v S·%s%v Sc·%v", statFiles, shortenb(int(statTotSize)), statSizes, statScore)
}
func (v *version) computeCompaction_s() {
	// Precomputed best level for next compaction
	bestLevel := int(-1)
	bestScore := float64(-1)

	statFiles := make([]int, len(v.level_s))
	statSizes := make([]string, len(v.level_s))
	statScore := make([]string, len(v.level_s))
	statTotSize := int64(0)
	//遍历[]sfiles
	for level, tables := range v.level_s {
		var score float64
		size := tables.size()//所有sst.size的和
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compaction.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = float64(len(tables)) / float64(v.s.o.GetCompactionL0Trigger()) // 文件个数/4
		} else {
			score = float64(size) / float64(v.s.o.GetCompactionTotalSize(level)) //文件的总大小/预设的每个level的文件大小总量
		}

		if score > bestScore {
			bestLevel = level
			bestScore = score
		}

		statFiles[level] = len(tables)
		statSizes[level] = shortenb(int(size))
		statScore[level] = fmt.Sprintf("%.2f", score)
		statTotSize += size
	}
	//最后找出算出的值最大的一个赋值到v.cScore，level赋值到v.cLevel，其实选出当前最满的那一层
	v.cLevels = bestLevel
	v.cScores = bestScore

	v.s.logf("version@stat F·%v S·%s%v Sc·%v", statFiles, shortenb(int(statTotSize)), statSizes, statScore)
}
//查看是否需要合并
func (v *version) needCompaction() bool {
	return v.cScore >= 1 || atomic.LoadPointer(&v.cSeek) != nil
}
func (v *version) needCompaction_s() bool {
	return v.cScores >= 1 || atomic.LoadPointer(&v.nSeek) != nil
}

type tablesScratch struct {
	added   map[int64]atRecord
	deleted map[int64]struct{}
}
type tablesScratch2 struct {
	added   map[int64]atRecord
	deleted map[int64]struct{}
}
type versionStaging struct {
	base   *version //存储旧版本的version
	levels []tablesScratch //added addeds deleted deleteds
	level_s []tablesScratch2 //added addeds deleted deleteds
}

func (p *versionStaging) getScratch(level int) *tablesScratch {
	if level >= len(p.levels) {
		newLevels := make([]tablesScratch, level+1)
		copy(newLevels, p.levels)
		p.levels = newLevels
	}
	return &(p.levels[level])
}
func (p *versionStaging) getScratch_s(level int) *tablesScratch2 {
	if level >= len(p.level_s) {
		newLevels := make([]tablesScratch2, level+1)
		copy(newLevels, p.level_s)
		p.level_s = newLevels
	}
	return &(p.level_s[level])
}
//added和addeds在这里被赋值，由addedtables传进addeds，再执行finish
func (p *versionStaging) commit(r *sessionRecord) {
	// Deleted tables.
	for _, r := range r.deletedTables {
		scratch := p.getScratch(r.level) //返回类型为tablesScratch
		if r.level < len(p.base.levels) && len(p.base.levels[r.level]) > 0 {
			if scratch.deleted == nil {
				scratch.deleted = make(map[int64]struct{})
			}
			scratch.deleted[r.num] = struct{}{}
		}
		if scratch.added != nil {
			delete(scratch.added, r.num)
		}
	}
	for _, r := range r.addedTables {
		scratch := p.getScratch(r.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[r.num] = r
		if scratch.deleted != nil {
			delete(scratch.deleted, r.num)
		}
	}
	for _, r := range r.deletedTabless {
		scratch := p.getScratch_s(r.level)
		if r.level < len(p.base.level_s) && len(p.base.level_s[r.level]) > 0 {
			if scratch.deleted == nil {
				scratch.deleted = make(map[int64]struct{})
			}
			scratch.deleted[r.num] = struct{}{}
		}
		if scratch.added != nil {
			delete(scratch.added, r.num)
		}
	}
	for _, r := range r.addedTabless {
		scratch := p.getScratch_s(r.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[r.num] = r
		if scratch.deleted != nil {
			delete(scratch.deleted, r.num)
		}

	}
}
func (p *versionStaging) commit_1(r *sessionRecord) {
	// Deleted tables.
	for _, r := range r.deletedTables {
		scratch := p.getScratch(r.level) //返回类型为tablesScratch
		if r.level < len(p.base.levels) && len(p.base.levels[r.level]) > 0 {
			if scratch.deleted == nil {
				scratch.deleted = make(map[int64]struct{})
			}
			scratch.deleted[r.num] = struct{}{}
		}
		if scratch.added != nil {
			delete(scratch.added, r.num)
		}
	}
	// New tables.
	for _, r := range r.addedTables {
		scratch := p.getScratch(r.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[r.num] = r
		if scratch.deleted != nil {
			delete(scratch.deleted, r.num)
		}
	}
}
func (p *versionStaging) commit_2(r *sessionRecord) {
	// Deleted tables.
	for _, r := range r.deletedTabless {
		scratch := p.getScratch_s(r.level)
		if r.level < len(p.base.level_s) && len(p.base.level_s[r.level]) > 0 {
			if scratch.deleted == nil {
				scratch.deleted = make(map[int64]struct{})
			}
			scratch.deleted[r.num] = struct{}{}
		}
		if scratch.added != nil {
			delete(scratch.added, r.num)
		}
	}

	// New tables.
	for _, r := range r.addedTabless {
		scratch := p.getScratch_s(r.level)
		if scratch.added == nil {
			scratch.added = make(map[int64]atRecord)
		}
		scratch.added[r.num] = r
		if scratch.deleted != nil {
			delete(scratch.deleted, r.num)
		}
	}
}
//version中的levels和level_s都是在finish这里被赋值的
func (p *versionStaging) finish(trivial bool) *version {
	// Build new version.
	nv := newVersion(p.base.s) //newVersion的参数是session，按理说并不会影响到levels和level_s，其内容由下面赋值?        [version中有s和id],
	numLevel := len(p.levels)
	numLevel2 := len(p.level_s)
	if len(p.base.levels) > numLevel {
		numLevel = len(p.base.levels)
	}
	if len(p.base.level_s) > numLevel2 {
		numLevel2 = len(p.base.level_s)
	}
	nv.levels = make([]tFiles, numLevel)
	nv.level_s = make([]sFiles, numLevel2) //第一次测试时，numLevel为1
	for level := 0; level < numLevel; level++ {
		var baseTabels tFiles
		if level < len(p.base.levels) {
			baseTabels = p.base.levels[level]
		}

		if level < len(p.levels) {
			scratch := p.levels[level]

			// Short circuit if there is no change at all.
			if len(scratch.added) == 0 && len(scratch.deleted) == 0 {
				nv.levels[level] = baseTabels
				continue
			}

			var nt tFiles
			// Prealloc list if possible.
			if n := len(baseTabels) + len(scratch.added) - len(scratch.deleted); n > 0 {
				nt = make(tFiles, 0, n)
			}

			// Base tables.
			for _, t := range baseTabels {
				if _, ok := scratch.deleted[t.fd.Num]; ok {
					continue
				}
				if _, ok := scratch.added[t.fd.Num]; ok {
					continue
				}
				nt = append(nt, t)
			}

			// Avoid resort if only files in this level are deleted
			if len(scratch.added) == 0 {
				nv.levels[level] = nt
				continue
			}

			// For normal table compaction, one compaction will only involve two levels
			// of files. And the new files generated after merging the source level and
			// source+1 level related files can be inserted as a whole into source+1 level
			// without any overlap with the other source+1 files.
			//
			// When the amount of data maintained by leveldb is large, the number of files
			// per level will be very large. While qsort is very inefficient for sorting
			// already ordered arrays. Therefore, for the normal table compaction, we use
			// binary search here to find the insert index to insert a batch of new added
			// files directly instead of using qsort.
			if trivial && len(scratch.added) > 0 {
				added := make(tFiles, 0, len(scratch.added))
				for _, r := range scratch.added {
					added = append(added, tableFileFromRecord(r))
				}
				if level == 0 {
					added.sortByNum()
					index := nt.searchNumLess(added[len(added)-1].fd.Num)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				} else {
					added.sortByKey(p.base.s.icmp)
					_, amax := added.getRange(p.base.s.icmp)
					index := nt.searchMin(p.base.s.icmp, amax)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				}
				nv.levels[level] = nt
				continue
			}

			// New tables.
			for _, r := range scratch.added {
				nt = append(nt, tableFileFromRecord(r))
			}

			if len(nt) != 0 {
				// Sort tables.
				if level == 0 {
					nt.sortByNum()
				} else {
					nt.sortByKey(p.base.s.icmp)
				}

				nv.levels[level] = nt
			}
		} else {
			nv.levels[level] = baseTabels
		}
	}
	n := len(nv.levels) //修剪levels
	for ; n > 0 && nv.levels[n-1] == nil; n-- {
	}
	nv.levels = nv.levels[:n]
	//fmt.Println("finish循环后:",nv.level_s,nv.levels)
	nv.computeCompaction()

	for level := 0; level < numLevel2; level++ {
		var baseTabels sFiles
		if level < len(p.base.level_s) {
			baseTabels = p.base.level_s[level]
		}

		if level < len(p.level_s) {
			scratch := p.level_s[level]

			// Short circuit if there is no change at all.
			if len(scratch.added) == 0 && len(scratch.deleted) == 0 {
				nv.level_s[level] = baseTabels
				continue
			}

			var nt sFiles
			// Prealloc list if possible.
			if n := len(baseTabels) + len(scratch.added) - len(scratch.deleted); n > 0 {
				nt = make(sFiles, 0, n)
			}

			// Base tables.
			for _, t := range baseTabels {
				if _, ok := scratch.deleted[t.fd.Num]; ok {
					continue
				}
				if _, ok := scratch.added[t.fd.Num]; ok {
					continue
				}
				nt = append(nt, t)
			}

			// Avoid resort if only files in this level are deleted
			if len(scratch.added) == 0 {
				nv.level_s[level] = nt
				continue
			}

			// For normal table compaction, one compaction will only involve two levels
			// of files. And the new files generated after merging the source level and
			// source+1 level related files can be inserted as a whole into source+1 level
			// without any overlap with the other source+1 files.
			//
			// When the amount of data maintained by leveldb is large, the number of files
			// per level will be very large. While qsort is very inefficient for sorting
			// already ordered arrays. Therefore, for the normal table compaction, we use
			// binary search here to find the insert index to insert a batch of new added
			// files directly instead of using qsort.
			if trivial && len(scratch.added) > 0 {
				added := make(sFiles, 0, len(scratch.added))
				for _, r := range scratch.added {
					added = append(added, tableFileFromRecord_s(r))
				}
				if level == 0 {
					added.sortByNum()
					index := nt.searchNumLess(added[len(added)-1].fd.Num)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				} else {
					added.sortByKey(p.base.s.icmp)
					_, amax := added.getRange(p.base.s.icmp)
					index := nt.searchMin(p.base.s.icmp, amax)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				}
				nv.level_s[level] = nt
				continue
			}

			// New tables.
			for _, r := range scratch.added {
				nt = append(nt, tableFileFromRecord_s(r))
			}

			if len(nt) != 0 {
				// Sort tables.
				if level == 0 {
					nt.sortByNum()
				} else {
					nt.sortByKey(p.base.s.icmp)
				}

				nv.level_s[level] = nt
			}
		} else {
			nv.level_s[level] = baseTabels
		}
	}
	//fmt.Println("finish_s循环后:",nv.level_s,nv.levels)
	// Trim levels.
	n2 := len(nv.level_s)
	for ; n2 > 0 && nv.level_s[n2-1] == nil; n2-- {
	}
	nv.level_s = nv.level_s[:n2]
	nv.computeCompaction_s()
	return nv
}
func (p *versionStaging) finish_1(trivial bool) *version {
	// Build new version.
	nv := newVersion(p.base.s) //s和id,
	numLevel := len(p.levels)
	if len(p.base.levels) > numLevel {
		numLevel = len(p.base.levels)
	}
	nv.levels = make([]tFiles, numLevel)
	for level := 0; level < numLevel; level++ {
		var baseTabels tFiles
		if level < len(p.base.levels) {
			baseTabels = p.base.levels[level]
		}

		if level < len(p.levels) {
			scratch := p.levels[level]

			// Short circuit if there is no change at all.
			if len(scratch.added) == 0 && len(scratch.deleted) == 0 {
				nv.levels[level] = baseTabels
				continue
			}

			var nt tFiles
			// Prealloc list if possible.
			if n := len(baseTabels) + len(scratch.added) - len(scratch.deleted); n > 0 {
				nt = make(tFiles, 0, n)
			}

			// Base tables.
			for _, t := range baseTabels {
				if _, ok := scratch.deleted[t.fd.Num]; ok {
					continue
				}
				if _, ok := scratch.added[t.fd.Num]; ok {
					continue
				}
				nt = append(nt, t)
			}

			// Avoid resort if only files in this level are deleted
			if len(scratch.added) == 0 {
				nv.levels[level] = nt
				continue
			}

			//For normal table compaction, one compaction will only involve two levels
			//of files. And the new files generated after merging the source level and
			//source+1 level related files can be inserted as a whole into source+1 level
			//without any overlap with the other source+1 files.
			//
			//When the amount of data maintained by leveldb is large, the number of files
			//per level will be very large. While qsort is very inefficient for sorting
			//already ordered arrays. Therefore, for the normal table compaction, we use
			//binary search here to find the insert index to insert a batch of new added
			//files directly instead of using qsort.
			if trivial && len(scratch.added) > 0 {
				added := make(tFiles, 0, len(scratch.added))
				for _, r := range scratch.added {
					added = append(added, tableFileFromRecord(r))
				}
				if level == 0 {
					added.sortByNum()
					index := nt.searchNumLess(added[len(added)-1].fd.Num)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				} else {
					added.sortByKey(p.base.s.icmp)
					_, amax := added.getRange(p.base.s.icmp)
					index := nt.searchMin(p.base.s.icmp, amax)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				}
				nv.levels[level] = nt
				continue
			}

			// New tables.
			for _, r := range scratch.added {
				nt = append(nt, tableFileFromRecord(r))
			}

			if len(nt) != 0 {
				// Sort tables.
				if level == 0 {
					nt.sortByNum()
				} else {
					nt.sortByKey(p.base.s.icmp)
				}

				nv.levels[level] = nt
			}
		} else {
			nv.levels[level] = baseTabels
		}
	}
	n := len(nv.levels) //修剪levels
	for ; n > 0 && nv.levels[n-1] == nil; n-- {
	}
	nv.levels = nv.levels[:n]
	//fmt.Println("finish循环后:",nv.level_s,nv.levels)
	nv.computeCompaction()
	return nv
}
func (p *versionStaging) finish_2(trivial bool) *version {
	// Build new version.
	//fmt.Println("调用finish_s")
	nv := newVersion(p.base.s) //参数为*seesion,得到一个version，有s和id
	numLevel := len(p.level_s)
	if len(p.base.level_s) > numLevel {
		numLevel = len(p.base.level_s)
	}
	nv.level_s = make([]sFiles, numLevel) //建立level_s
	//fmt.Println("循环前:",nv.level_s,nv.levels)
	for level := 0; level < numLevel; level++ {
		var baseTabels sFiles
		if level < len(p.base.level_s) {
			baseTabels = p.base.level_s[level]
		}

		if level < len(p.level_s) {
			scratch := p.level_s[level]

			// Short circuit if there is no change at all.
			if len(scratch.added) == 0 && len(scratch.deleted) == 0 {
				nv.level_s[level] = baseTabels
				continue
			}

			var nt sFiles
			// Prealloc list if possible.
			if n := len(baseTabels) + len(scratch.added) - len(scratch.deleted); n > 0 {
				nt = make(sFiles, 0, n)
			}

			// Base tables.
			for _, t := range baseTabels {
				if _, ok := scratch.deleted[t.fd.Num]; ok {
					continue
				}
				if _, ok := scratch.added[t.fd.Num]; ok {
					continue
				}
				nt = append(nt, t)
			}

			// Avoid resort if only files in this level are deleted
			if len(scratch.added) == 0 {
				nv.level_s[level] = nt
				continue
			}

			// For normal table compaction, one compaction will only involve two levels
			// of files. And the new files generated after merging the source level and
			// source+1 level related files can be inserted as a whole into source+1 level
			// without any overlap with the other source+1 files.
			//
			// When the amount of data maintained by leveldb is large, the number of files
			// per level will be very large. While qsort is very inefficient for sorting
			// already ordered arrays. Therefore, for the normal table compaction, we use
			// binary search here to find the insert index to insert a batch of new added
			// files directly instead of using qsort.
			if trivial && len(scratch.added) > 0 {
				added := make(sFiles, 0, len(scratch.added))
				for _, r := range scratch.added {
					added = append(added, tableFileFromRecord_s(r))
				}
				if level == 0 {
					added.sortByNum()
					index := nt.searchNumLess(added[len(added)-1].fd.Num)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				} else {
					added.sortByKey(p.base.s.icmp)
					_, amax := added.getRange(p.base.s.icmp)
					index := nt.searchMin(p.base.s.icmp, amax)
					nt = append(nt[:index], append(added, nt[index:]...)...)
				}
				nv.level_s[level] = nt
				continue
			}

			// New tables.
			for _, r := range scratch.added {
				nt = append(nt, tableFileFromRecord_s(r))
			}

			if len(nt) != 0 {
				// Sort tables.
				if level == 0 {
					nt.sortByNum()
				} else {
					nt.sortByKey(p.base.s.icmp)
				}

				nv.level_s[level] = nt
			}
		} else {
			nv.level_s[level] = baseTabels
		}
	}
	//fmt.Println("循环后:",nv.level_s,nv.levels)
	// Trim levels.
	n := len(nv.level_s)
	for ; n > 0 && nv.level_s[n-1] == nil; n-- {
	}
	nv.level_s = nv.level_s[:n]
	//fmt.Println(nv.level_s,nv.levels)
	nv.computeCompaction_s()
	return nv
}

type versionReleaser struct {
	v    *version
	once bool
}

func (vr *versionReleaser) Release() {
	v := vr.v
	v.s.vmu.Lock()
	if !vr.once {
		v.releaseNB()
		vr.once = true
	}
	v.s.vmu.Unlock()
}
