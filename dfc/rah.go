// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
)

// TODO	1) readahead IFF utilization < (50%(or configured) || average across mountpaths)
//	2) stats: average readahed per get.n, num readahead race losses
//	3) readahead via user REST, with additional URLParam objectmem
//	4) config.Readahead.TotalMem as long as < sigar.FreeMem
//	5) proxy AIMD, target to decide
//	6) rangeOff/len
//	7) utilize memsys
const (
	rahChanSize    = 256
	rahMapInitSize = 256
	rahGCTime      = time.Minute // cleanup and free periodically
)

type (
	readaheader interface {
		ahead(fqn string, rangeOff, rangeLen int64)
		get(fqn string) (rahfcacher, *memsys.SGL)
	}
	rahfcacher interface {
		got()
	}
	readahead struct {
		sync.Mutex
		cmn.NamedID
		mountpaths *fs.MountedFS         //
		joggers    map[string]*rahjogger // mpath => jogger
		stopCh     chan struct{}         // to stop
	}
	rahjogger struct {
		sync.Mutex
		mpath   string
		rahmap  map[string]*rahfcache // to track readahead by fqn
		aheadCh chan *rahfcache       // to start readahead(fqn)
		getCh   chan *rahfcache       // to get readahead context for fqn
		stopCh  chan struct{}         // to stop
		slab    *memsys.Slab2         // to read files
		buf     []byte                // ditto
	}
	rahfcache struct {
		sync.Mutex
		fqn                string
		rangeOff, rangeLen int64
		sgl                *memsys.SGL
		ts                 struct {
			head, tail, get, got time.Time
		}
		size int64
		err  error
	}
	dummyreadahead struct{}
	dummyrahfcache struct{}
)

var pdummyrahfcache = &dummyrahfcache{}

//===========================================
//
// parent
//
//===========================================

// as an fs.PathRunner
var _ fs.PathRunner = &readahead{}

func (r *readahead) ReqAddMountpath(mpath string)     { r.addmp(mpath, fs.Add) }
func (r *readahead) ReqRemoveMountpath(mpath string)  { r.delmp(mpath, fs.Remove) }
func (r *readahead) ReqEnableMountpath(mpath string)  { r.addmp(mpath, fs.Enable) }
func (r *readahead) ReqDisableMountpath(mpath string) { r.delmp(mpath, fs.Disable) }

func (r *readahead) addmp(mpath, tag string) {
	r.Lock()
	if _, ok := r.joggers[mpath]; ok {
		glog.Errorf("mountpath %s already %s", mpath, tag)
		r.Unlock()
		return
	}
	rj := newRahJogger(mpath)
	r.joggers[mpath] = rj
	go rj.jog()
	r.Unlock()
}
func (r *readahead) delmp(mpath, tag string) {
	r.Lock()
	if rj, ok := r.joggers[mpath]; !ok {
		glog.Errorf("mountpath %s already %s", mpath, tag)
	} else {
		rj.stopCh <- struct{}{}
		close(rj.stopCh)
		delete(r.joggers, mpath)
		rj.slab.Free(rj.buf)
	}
	r.Unlock()
}

// c-tors
func newReadaheader() (r *readahead) {
	r = &readahead{}
	r.joggers = make(map[string]*rahjogger, 8)
	r.stopCh = make(chan struct{}, 4)
	return
}
func newRahJogger(mpath string) (rj *rahjogger) {
	rj = &rahjogger{mpath: mpath}
	rj.rahmap = make(map[string]*rahfcache, rahMapInitSize)
	rj.aheadCh = make(chan *rahfcache, rahChanSize)
	rj.getCh = make(chan *rahfcache, rahChanSize)
	rj.stopCh = make(chan struct{}, 4)

	rj.slab = gmem2.SelectSlab2(cmn.GCO.Get().Readahead.ObjectMem)
	rj.buf = rj.slab.Alloc()
	return
}

// as a runner
func (r *readahead) Run() error {
	glog.Infof("Starting %s", r.Getname())
	availablePaths, _ := fs.Mountpaths.Get()
	for mpath := range availablePaths {
		r.addmp(mpath, "added")
	}
	<-r.stopCh // forever
	return nil
}
func (r *readahead) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	for mpath := range r.joggers {
		r.delmp(mpath, "stopped")
	}
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// external API, dummies first
func (r *dummyreadahead) ahead(string, int64, int64)           {}
func (r *dummyreadahead) get(string) (rahfcacher, *memsys.SGL) { return pdummyrahfcache, nil }
func (*dummyrahfcache) got()                                   {}

func (r *readahead) ahead(fqn string, rangeOff, rangeLen int64) {
	if rj := r.demux(fqn); rj != nil {
		rj.aheadCh <- &rahfcache{fqn: fqn, rangeOff: rangeOff, rangeLen: rangeLen}
	}
}

func (r *readahead) get(fqn string) (rahfcacher, *memsys.SGL) {
	var rj *rahjogger
	if rj = r.demux(fqn); rj == nil {
		return pdummyrahfcache, nil
	}
	rj.Lock()
	if e, ok := rj.rahmap[fqn]; ok {
		rj.Unlock()
		e.Lock()
		e.ts.get = time.Now()
		sgl := e.sgl
		e.Unlock()
		return e, sgl
	}
	// NOTE: can optimize this out given a confidence that
	//       (get arriving earlier than readahead)
	//       is extremely rare.
	//       In other words, pay the price of wasting readahead once in a blue moon...
	e := &rahfcache{fqn: fqn}
	e.ts.get = time.Now()
	rj.rahmap[fqn] = e
	rj.Unlock()
	return e, nil
}

func (rahfcache *rahfcache) got() {
	rahfcache.Lock()
	rahfcache.ts.got = time.Now()
	rahfcache.Unlock()
}

// external API helper: route to the appropriate (jogger) child
func (r *readahead) demux(fqn string) *rahjogger {
	mpathInfo, _ := r.mountpaths.Path2MpathInfo(fqn)
	if mpathInfo == nil {
		glog.Errorf("Failed to get mountpath for %s", fqn)
		return nil
	}
	mpath := mpathInfo.Path

	r.Lock()
	rj, ok := r.joggers[mpath]
	if !ok {
		r.Unlock()
		glog.Errorf("Failed to resolve mountpath %s", mpath)
		return nil
	}
	r.Unlock()
	return rj
}

//===========================================
//
// child
//
//===========================================

func (rj *rahjogger) jog() error {
	glog.Infof("Starting readahead-mpath(%s)", rj.mpath)
	ticker := time.NewTicker(rahGCTime)
	for {
		select {
		case rahfcache, ok := <-rj.aheadCh:
			// two cases reflecting readahead race vs get
			if ok {
				rj.Lock()
				if e, ok := rj.rahmap[rahfcache.fqn]; !ok {
					cmn.Assert(rahfcache.ts.get.IsZero())
					rahfcache.ts.head = time.Now()
					rj.rahmap[rahfcache.fqn] = rahfcache
					rj.Unlock()
					rahfcache.readahead(rj.buf) // TODO: same context, same buffer - can go faster with RAID at low utils
				} else {
					rj.Unlock()
					e.Lock()
					if !e.ts.head.IsZero() {
						e.ts.tail = time.Now() // done without even starting
					}
					e.Unlock()
				}
			}
		case <-ticker.C: // cleanup and free
			rj.Lock()
			for fqn, rahfcache := range rj.rahmap {
				rj.Unlock()
				rahfcache.Lock()
				if !rahfcache.ts.got.IsZero() && !rahfcache.ts.tail.IsZero() {
					delete(rj.rahmap, fqn)
					if rahfcache.sgl != nil {
						rahfcache.sgl.Free()
					}
				}
				rahfcache.Unlock()
				rj.Lock()
			}
			rj.Unlock()
		case <-rj.stopCh:
			ticker.Stop()
			return nil
		}
	}
}

// actual readahead
func (rahfcache *rahfcache) readahead(buf []byte) {
	var (
		file        *os.File
		err         error
		size, fsize int64
		stat        os.FileInfo
		reader      io.Reader
		config      = cmn.GCO.Get()
	)

	// 1. cleanup
	defer func() {
		if file != nil {
			file.Close()
		}
		rahfcache.Lock()
		rahfcache.ts.tail = time.Now()
		if err != nil {
			rahfcache.err = err // not freeing sgl yet
		}
		if rahfcache.sgl != nil {
			rahfcache.size = size
			cmn.Assert(rahfcache.sgl.Size() == size)
		}
		rahfcache.Unlock()
	}()

	// 2. open
	if stat, err = os.Stat(rahfcache.fqn); err != nil {
		return
	}
	file, err = os.Open(rahfcache.fqn)
	if err != nil {
		return
	}
	if rahfcache.rangeLen == 0 {
		fsize = cmn.MinI64(config.Readahead.ObjectMem, stat.Size())
		reader = file
	} else {
		fsize = cmn.MinI64(config.Readahead.ObjectMem, rahfcache.rangeLen)
		reader = io.NewSectionReader(file, rahfcache.rangeOff, rahfcache.rangeLen)
	}
	if !config.Readahead.Discard {
		rahfcache.sgl = gmem2.NewSGL(fsize)
	}
	// 3. read
	for size < fsize {
		rahfcache.Lock()
		if !rahfcache.ts.get.IsZero() {
			rahfcache.Unlock()
			break
		}
		rahfcache.Unlock()
		nr, er := reader.Read(buf)
		if nr > 0 {
			if rahfcache.sgl != nil {
				rahfcache.Lock()
				if !rahfcache.ts.get.IsZero() {
					rahfcache.Unlock()
					break
				}
				nw, ew := rahfcache.sgl.Write(buf[0:nr])
				rahfcache.Unlock()
				if nw > 0 {
					size += int64(nw)
				}
				if ew != nil {
					err = ew
					break
				}
				if nr != nw {
					err = io.ErrShortWrite
					break
				}
			} else {
				size += int64(nr)
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
}
