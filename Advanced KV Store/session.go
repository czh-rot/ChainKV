// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// ErrManifestCorrupted records manifest corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrManifestCorrupted struct {
	Field  string
	Reason string
}

func (e *ErrManifestCorrupted) Error() string {
	return fmt.Sprintf("leveldb: manifest corrupted (field '%s'): %s", e.Field, e.Reason)
}

func newErrManifestCorrupted(fd storage.FileDesc, field, reason string) error {
	return errors.NewErrCorrupted(fd, &ErrManifestCorrupted{field, reason})
}

// session represent a persistent database session.
type session struct {
	// Need 64-bit alignment.
	stNextFileNum    int64 // current unused file number
	stJournalNum     int64 // current journal file number; need external synchronization
	stPrevJournalNum int64 // prev journal file number; no longer used; for compatibility with older version of leveldb
	stTempFileNum    int64
	stSeqNum         uint64 // last mem compacted seq; need external synchronization

	stor     *iStorage
	storLock storage.Locker
	o        *cachedOptions
	icmp     *iComparer
	tops     *tOps // 管理缓存！
	//tops2    *tOps // 同样管理缓存？

	manifest       *journal.Writer
	manifestWriter storage.Writer
	manifestFd     storage.FileDesc

	stCompPtrs   []internalKey // compaction pointers; need external synchronization
	stCompPtrs2  []internalKey // compaction pointers; need external synchronization
	stVersion   *version      // current version
	ntVersionId int64         // next version id to assign
	refCh       chan *vTask //ref++
	relCh       chan *vTask //ref--
	deltaCh     chan *vDelta
	abandon     chan int64
	closeC      chan struct{}
	closeW      sync.WaitGroup
	vmu         sync.Mutex

	// Testing fields
	fileRefCh chan chan map[int64]int // channel used to pass current reference stat
}

// Creates new initialized session instance.
func newSession(stor storage.Storage, o *opt.Options) (s *session, err error) {
	if stor == nil {
		return nil, os.ErrInvalid
	}
	storLock, err := stor.Lock()
	if err != nil {
		return
	}
	s = &session{
		stor:      newIStorage(stor),
		storLock:  storLock,
		refCh:     make(chan *vTask),
		relCh:     make(chan *vTask),
		deltaCh:   make(chan *vDelta),
		abandon:   make(chan int64),
		fileRefCh: make(chan chan map[int64]int),
		closeC:    make(chan struct{}),
	}
	s.setOptions(o)
	s.tops = newTableOps(s)

	s.closeW.Add(1)
	go s.refLoop()
	s.setVersion(nil, newVersion(s))
	s.log("log@legend F·NumFile S·FileSize N·Entry C·BadEntry B·BadBlock Ke·KeyError D·DroppedEntry L·Level Q·SeqNum T·TimeElapsed")
	return
}

// Close session.
func (s *session) close() {
	s.tops.close()
	if s.manifest != nil {
		s.manifest.Close()
	}
	if s.manifestWriter != nil {
		s.manifestWriter.Close()
	}
	s.manifest = nil
	s.manifestWriter = nil
	s.setVersion(nil, &version{s: s, closing: true, id: s.ntVersionId})

	// Close all background goroutines
	close(s.closeC)
	s.closeW.Wait()
}

// Release session lock.
func (s *session) release() {
	s.storLock.Unlock()
}

// Create a new database session; need external synchronization.
func (s *session) create() error {
	// create manifest
	return s.newManifest(nil, nil)
}

// Recover a database session; need external synchronization.会在open之中被调用，
//简单说就是利用Manifest信息重新构建一个最新的version，重点在于创建一个空的version，并利用manifest文件中的sessionrecord依次apply，
//还原出一个version，注意manifest的第一条sr是一个version的快照，后续的sr都是增量的变化
func (s *session) recover() (err error) {
	defer func() {
		if os.IsNotExist(err) {
			// Don't return os.ErrNotExist if the underlying storage contains
			// other files that belong to LevelDB. So the DB won't get trashed.
			if fds, _ := s.stor.List(storage.TypeAll); len(fds) > 0 {
				err = &errors.ErrCorrupted{Fd: storage.FileDesc{Type: storage.TypeManifest}, Err: &errors.ErrMissingFiles{}}
			}
		}
	}()

	fd, err := s.stor.GetMeta()
	if err != nil {
		return
	}

	reader, err := s.stor.Open(fd)
	if err != nil {
		return
	}
	defer reader.Close()
	var (
		// Options.
		strict = s.o.GetStrict(opt.StrictManifest) //false

		jr      = journal.NewReader(reader, dropper{s, fd}, strict, true) //*Reader
		rec     = &sessionRecord{} //sessionR
		staging = s.stVersion.newStaging() //versionStaging,版本的中间阶段？
	)
	for {
		var r io.Reader
		r, err = jr.Next() //Next返回下一个日志的阅读器。它返回io。如果没有更多的日记账，就用EOF。在下一次调用之后，返回的读取器就失效了，不应该再使用它。如果strict为false，则读者将返回io。ErrUnexpectedEOF错误时发现损坏的日志。
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return errors.SetFd(err, fd)
		}
		err = rec.decode(r) //对sr的decode？在rec执行此方法之后，SessionRecord中成员被赋值
		/*fmt.Println("位于recover()中，这里是sessionRecord")
		fmt.Println(rec.addedTabless,"  ",rec.addedTables)
		fmt.Println(rec.deletedTabless,"  ",rec.deletedTables)*/
		if err == nil {
			// save compact pointers
			for _, r := range rec.compPtrs {
				s.setCompPtr(r.level, internalKey(r.ikey))
			}
			for _, r := range rec.compPtrs2 {
				s.setCompPtr_s(r.level, internalKey(r.ikey))
			}
			// commit record to version staging，表现为verison的一个阶段
			staging.commit(rec) //变成add和adds等?
		} else {
			err = errors.SetFd(err, fd)
			if strict || !errors.IsCorrupted(err) {
				return
			}
			s.logf("manifest error: %v (skipped)", errors.SetFd(err, fd))
		}
		rec.resetCompPtrs()
		rec.resetCompPtrs_s()
		rec.resetAddedTables()
		rec.resetAddedTables_s()
		rec.resetDeletedTables()
		rec.resetDeletedTables_s()
	}

	switch {
	case !rec.has(recComparer):
		return newErrManifestCorrupted(fd, "comparer", "missing")
	case rec.comparer != s.icmp.uName():
		return newErrManifestCorrupted(fd, "comparer", fmt.Sprintf("mismatch: want '%s', got '%s'", s.icmp.uName(), rec.comparer))
	case !rec.has(recNextFileNum):
		return newErrManifestCorrupted(fd, "next-file-num", "missing")
	case !rec.has(recJournalNum):
		return newErrManifestCorrupted(fd, "journal-file-num", "missing")
	case !rec.has(recSeqNum):
		return newErrManifestCorrupted(fd, "seq-num", "missing")
	}
	//fmt.Println("recover 2")
	s.manifestFd = fd
	s.setVersion(rec, staging.finish(false)) //将add的数据写入levels和level_s
	s.setNextFileNum(rec.nextFileNum)
	s.recordCommited(rec)
	return nil
}

// Commit session; need external synchronization.
//通过spawn()生成新的版本信息，同时flushmanifest将新版本信息写入MANIFEST文件，最后有个setversion设置当前版本为最新生成的版本
func (s *session) commit(r *sessionRecord, trivial bool) (err error) {
	v := s.version() //incref
	defer v.release()

	// spawn new version based on current version
	nv := v.spawn(r, trivial)
	//fmt.Println("spwan执行完毕后:",v,"\n",nv.levels)
	// abandon useless version id to prevent blocking version processing loop.
	defer func() {
		if err != nil {
			s.abandon <- nv.id
			s.logf("commit@abandon useless vid D%d", nv.id)
		}
	}()

	if s.manifest == nil {
		// manifest journal writer not yet created, create one
		err = s.newManifest(r, nv)
	} else {
		err = s.flushManifest(r)
	}

	// finally, apply new version if no error rise
	if err == nil {
		s.setVersion(r, nv)
	}

	return
}
func (s *session) commit_1(r *sessionRecord, trivial bool) (err error) {
	v := s.version() //incref
	defer v.release()

	// spawn new version based on current version
	nv := v.spawn_1(r, trivial)
	//fmt.Println("spwan执行完毕后:",v,"\n",nv.levels)
	// abandon useless version id to prevent blocking version processing loop.
	defer func() {
		if err != nil {
			s.abandon <- nv.id
			s.logf("commit@abandon useless vid D%d", nv.id)
		}
	}()

	if s.manifest == nil {
		// manifest journal writer not yet created, create one
		err = s.newManifest(r, nv)
	} else {
		err = s.flushManifest(r)
	}

	// finally, apply new version if no error rise
	if err == nil {
		s.setVersion(r, nv)
	}

	return
}
func (s *session) commit_s(r *sessionRecord, trivial bool) (err error) {
	//fmt.Println("首先执行commit_s，r中的addedTabless为有效数据:",r.addedTabless)
	v := s.version() //incref++
	defer v.release()

	// spawn new version based on current version
	nv := v.spawn_s(r, trivial) //生成新版本
	//fmt.Println("spwan_s执行完毕后:",nv.level_s)//这里也没有问题，level_s中有数据
	// abandon useless version id to prevent blocking version processing loop.
	defer func() {
		if err != nil {
			s.abandon <- nv.id
			s.logf("commit@abandon useless vid D%d", nv.id)
		}
	}()

	if s.manifest == nil {
		// manifest journal writer not yet created, create one
		err = s.newManifest(r, nv)
	} else {
		//fmt.Println("调用flushManifest，把session中的信息写入manifest")
		err = s.flushManifest(r)
	}

	// finally, apply new version if no error rise
	if err == nil {
		//fmt.Println("写新版本，同时:",nv.level_s)
		s.setVersion(r, nv)
	}


	return
}