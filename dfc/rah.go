// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/iosgl"
)

// TODO	1) readahead IFF utilization < (50%(or configured) || average across mountpaths)
//	2) stats: average readahed per get.n, num readahead race losses
//	3) readahead via user REST, with additional URLParam objectmem
//	4) ctx.config.Readahead.TotalMem as long as < sigar.FreeMem
//	5) proxy AIMD, target to decide
//	6) rangeoff/len
//	7) utilize iosgl
const (
	rahChanSize    = 256
	rahMapInitSize = 256
	rahGCTime      = time.Minute // cleanup and free periodically
)

type (
	readaheadif interface {
		ahead(fqn string, rangeoff, rangelen int64)
		get(fqn string) (rahfqnif, *iosgl.SGL)
	}
	rahfqnif interface {
		got()
	}
	readaheader struct {
		sync.Mutex
		namedrunner                       // to be a runner
		joggers     map[string]*rahjogger // mpath => jogger
		stopCh      chan struct{}         // to stop
	}
	rahjogger struct {
		sync.Mutex
		mpath   string
		rahmap  map[string]*rahfqn // to track readahead by fqn
		aheadCh chan *rahfqn       // to start readahead(fqn)
		getCh   chan *rahfqn       // to get readahead context for fqn
		stopCh  chan struct{}      // to stop
		slab    *iosgl.Slab        // to read files
		buf     []byte             // ditto
	}
	rahfqn struct {
		sync.Mutex
		fqn                string
		rangeoff, rangelen int64
		sgl                *iosgl.SGL
		ts                 struct {
			head, tail, get, got time.Time
		}
		size int64
		err  error
	}
	dummyreadahead struct{}
	dummyrahfqn    struct{}
)

var pdummyrahfqn = &dummyrahfqn{}

//===========================================
//
// parent
//
//===========================================

// as an fsprunner
func (r *readaheader) reqEnableMountpath(mpath string)  { r.addmp(mpath, "enabled") }
func (r *readaheader) reqDisableMountpath(mpath string) { r.delmp(mpath, "disabled") }
func (r *readaheader) reqAddMountpath(mpath string)     { r.addmp(mpath, "added") }
func (r *readaheader) reqRemoveMountpath(mpath string)  { r.delmp(mpath, "removed") }

func (r *readaheader) addmp(mpath, tag string) {
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
func (r *readaheader) delmp(mpath, tag string) {
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
func newReadaheader() (r *readaheader) {
	r = &readaheader{}
	r.joggers = make(map[string]*rahjogger, 8)
	r.stopCh = make(chan struct{}, 4)
	return
}
func newRahJogger(mpath string) (rj *rahjogger) {
	rj = &rahjogger{mpath: mpath}
	rj.rahmap = make(map[string]*rahfqn, rahMapInitSize)
	rj.aheadCh = make(chan *rahfqn, rahChanSize)
	rj.getCh = make(chan *rahfqn, rahChanSize)
	rj.stopCh = make(chan struct{}, 4)

	rj.slab = iosgl.SelectSlab(ctx.config.Readahead.ObjectMem)
	rj.buf = rj.slab.Alloc()
	return
}

// as a runner
func (r *readaheader) run() error {
	glog.Infof("Starting %s", r.name)
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	for mpath := range availablePaths {
		r.addmp(mpath, "added")
	}
	_ = <-r.stopCh // forever
	return nil
}
func (r *readaheader) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	for mpath := range r.joggers {
		r.delmp(mpath, "stopped")
	}
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// external API, dummies first
func (r *dummyreadahead) ahead(string, int64, int64)        {}
func (r *dummyreadahead) get(string) (rahfqnif, *iosgl.SGL) { return pdummyrahfqn, nil }
func (*dummyrahfqn) got()                                   {}

func (r *readaheader) ahead(fqn string, rangeoff, rangelen int64) {
	if rj := r.demux(fqn); rj != nil {
		rj.aheadCh <- &rahfqn{fqn: fqn, rangeoff: rangeoff, rangelen: rangelen}
	}
}

func (r *readaheader) get(fqn string) (rahfqnif, *iosgl.SGL) {
	var rj *rahjogger
	if rj = r.demux(fqn); rj == nil {
		return pdummyrahfqn, nil
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
	e := &rahfqn{fqn: fqn}
	e.ts.get = time.Now()
	rj.rahmap[fqn] = e
	rj.Unlock()
	return e, nil
}

func (rahfqn *rahfqn) got() {
	rahfqn.Lock()
	rahfqn.ts.got = time.Now()
	rahfqn.Unlock()
}

// external API helper: route to the appropriate (jogger) child
func (r *readaheader) demux(fqn string) *rahjogger {
	mpath := fqn2mountPath(fqn)
	if mpath == "" {
		glog.Errorf("Failed to get mountpath for %s", fqn)
		return nil
	}
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
		case rahfqn, ok := <-rj.aheadCh:
			// two cases reflecting readahead race vs get
			if ok {
				rj.Lock()
				if e, ok := rj.rahmap[rahfqn.fqn]; !ok {
					assert(rahfqn.ts.get.IsZero())
					rahfqn.ts.head = time.Now()
					rj.rahmap[rahfqn.fqn] = rahfqn
					rj.Unlock()
					rahfqn.readahead(rj.buf) // TODO: same context, same buffer - can go faster with RAID at low utils
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
			for fqn, rahfqn := range rj.rahmap {
				rj.Unlock()
				rahfqn.Lock()
				if !rahfqn.ts.got.IsZero() && !rahfqn.ts.tail.IsZero() {
					delete(rj.rahmap, fqn)
					if rahfqn.sgl != nil {
						rahfqn.sgl.Free()
					}
				}
				rahfqn.Unlock()
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
func (rahfqn *rahfqn) readahead(buf []byte) {
	var (
		file        *os.File
		err         error
		size, fsize int64
		stat        os.FileInfo
		reader      io.Reader
	)

	// 1. cleanup
	defer func() {
		if file != nil {
			file.Close()
		}
		rahfqn.Lock()
		rahfqn.ts.tail = time.Now()
		if err != nil {
			rahfqn.err = err // not freeing sgl yet
		}
		if rahfqn.sgl != nil {
			rahfqn.size = size
			assert(rahfqn.sgl.Size() == size)
		}
		rahfqn.Unlock()
	}()

	// 2. open
	if stat, err = os.Stat(rahfqn.fqn); err != nil {
		return
	}
	file, err = os.Open(rahfqn.fqn)
	if err != nil {
		return
	}
	if rahfqn.rangelen == 0 {
		fsize = min64(ctx.config.Readahead.ObjectMem, stat.Size())
		reader = file
	} else {
		fsize = min64(ctx.config.Readahead.ObjectMem, rahfqn.rangelen)
		reader = io.NewSectionReader(file, rahfqn.rangeoff, rahfqn.rangelen)
	}
	if !ctx.config.Readahead.Discard {
		rahfqn.sgl = iosgl.NewSGL(uint64(fsize))
	}
	// 3. read
	for size < fsize {
		rahfqn.Lock()
		if !rahfqn.ts.get.IsZero() {
			rahfqn.Unlock()
			break
		}
		rahfqn.Unlock()
		nr, er := reader.Read(buf)
		if nr > 0 {
			if rahfqn.sgl != nil {
				rahfqn.Lock()
				if !rahfqn.ts.get.IsZero() {
					rahfqn.Unlock()
					break
				}
				nw, ew := rahfqn.sgl.Write(buf[0:nr])
				rahfqn.Unlock()
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
