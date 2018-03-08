// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
)

type fscapacity struct {
	Used    uint64 `json:"used"`    // bytes
	Avail   uint64 `json:"avail"`   // ditto
	Usedpct uint32 `json:"usedpct"` // reduntant ok
}

// implemented by the stats runners
type statslogger interface {
	log() (runlru bool)
	housekeep(bool)
}

// implemented by the ***CoreStats types
type statsif interface {
	add(name string, val int64)
}

// TODO: use static map[string]int64
type proxyCoreStats struct {
	Numget    int64 `json:"numget"`
	Numput    int64 `json:"numput"`
	Numpost   int64 `json:"numpost"`
	Numdelete int64 `json:"numdelete"`
	Numrename int64 `json:"numrename"`
	Numerr    int64 `json:"numerr"`
	Numlist   int64 `json:"numlist"`
}

type targetCoreStats struct {
	proxyCoreStats
	Numcoldget      int64 `json:"numcoldget"`
	Bytesloaded     int64 `json:"bytesloaded"`
	Bytesevicted    int64 `json:"bytesevicted"`
	Filesevicted    int64 `json:"filesevicted"`
	Numsentfiles    int64 `json:"numsentfiles"`
	Numsentbytes    int64 `json:"numsentbytes"`
	Numrecvfiles    int64 `json:"numrecvfiles"`
	Numrecvbytes    int64 `json:"numrecvbytes"`
	Numlist         int64 `json:"numlist"`
	Numprefetch     int64 `json:"numprefetch"`
	Bytesprefetched int64 `json:"bytesprefetched"`
}

type statsrunner struct {
	sync.Mutex
	namedrunner
	statslogger
	chsts chan struct{}
}

type proxystatsrunner struct {
	statsrunner `json:"-"`
	Core        proxyCoreStats `json:"core"`
	ccopy       proxyCoreStats `json:"-"`
}

type storstatsrunner struct {
	statsrunner `json:"-"`
	Core        targetCoreStats         `json:"core"`
	Capacity    map[string]*fscapacity  `json:"capacity"`
	ccopy       targetCoreStats         `json:"-"`
	fsmap       map[syscall.Fsid]string `json:"-"`
}

type ClusterStats struct {
	Proxy  *proxyCoreStats             `json:"proxy"`
	Target map[string]*storstatsrunner `json:"target"`
}

//
// c-tor and methods
//
func newClusterStats() *ClusterStats {
	targets := make(map[string]*storstatsrunner, ctx.smap.count())
	for _, si := range ctx.smap.Smap {
		targets[si.DaemonID] = &storstatsrunner{Capacity: make(map[string]*fscapacity)}
	}
	return &ClusterStats{Target: targets}
}

func (s *proxyCoreStats) add(name string, val int64) {
	var v *int64
	switch name {
	case "numget":
		v = &s.Numget
	case "numput":
		v = &s.Numput
	case "numpost":
		v = &s.Numpost
	case "numdelete":
		v = &s.Numdelete
	case "numrename":
		v = &s.Numrename
	case "numlist":
		v = &s.Numlist
	case "numerr":
		v = &s.Numerr
	default:
		assert(false, "Invalid stats name "+name)
	}
	*v += val
}
func (s *targetCoreStats) add(name string, val int64) {
	var v *int64
	switch name {
	case "numget":
		v = &s.Numget
	case "numput":
		v = &s.Numput
	case "numpost":
		v = &s.Numpost
	case "numdelete":
		v = &s.Numdelete
	case "numrename":
		v = &s.Numrename
	case "numerr":
		v = &s.Numerr
	case "numcoldget":
		v = &s.Numcoldget
	case "bytesloaded":
		v = &s.Bytesloaded
	case "bytesevicted":
		v = &s.Bytesevicted
	case "filesevicted":
		v = &s.Filesevicted
	case "numsentfiles":
		v = &s.Numsentfiles
	case "numsentbytes":
		v = &s.Numsentbytes
	case "numrecvfiles":
		v = &s.Numrecvfiles
	case "numrecvbytes":
		v = &s.Numrecvbytes
	case "numlist":
		v = &s.Numlist
	case "numprefetch":
		v = &s.Numprefetch
	case "bytesprefetched":
		v = &s.Bytesprefetched
	default:
		assert(false, "Invalid stats name "+name)
	}
	*v += val
}

//========================
//
// stats runners & methods
//
//========================

func (r *statsrunner) runcommon(logger statslogger) error {
	r.chsts = make(chan struct{}, 1)

	glog.Infof("Starting %s", r.name)
	ticker := time.NewTicker(ctx.config.StatsTime)
	for {
		select {
		case <-ticker.C:
			runlru := logger.log()
			logger.housekeep(runlru)
		case <-r.chsts:
			ticker.Stop()
			return nil
		}
	}
}

func (r *statsrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chsts <- v
	close(r.chsts)
}

// statslogger interface impl
func (r *statsrunner) log() (runlru bool) {
	assert(false)
	return false
}

func (r *statsrunner) housekeep(bool) {
	return
}

func (r *proxystatsrunner) run() error {
	return r.runcommon(r)
}

func (r *proxystatsrunner) syncstats(stats *proxyCoreStats) {
	r.Lock()
	copyStruct(stats, &r.Core)
	r.Unlock()
}

// statslogger interface impl
func (r *proxystatsrunner) log() (runlru bool) {
	// nothing changed since the previous invocation
	if r.Core.Numput == r.ccopy.Numput &&
		r.Core.Numget == r.ccopy.Numget &&
		r.Core.Numpost == r.ccopy.Numpost &&
		r.Core.Numdelete == r.ccopy.Numdelete {
		return false
	}
	s := fmt.Sprintf("%s: %+v", r.name, r.Core)
	r.syncstats(&r.ccopy)
	glog.Infoln(s)
	return false
}

func (r *storstatsrunner) run() error {
	return r.runcommon(r)
}

func (r *storstatsrunner) syncstats(stats *targetCoreStats) {
	r.Lock()
	copyStruct(stats, &r.Core)
	r.Unlock()
}

func (r *storstatsrunner) log() (runlru bool) {
	// nothing changed since the previous invocation
	if r.Core.Numput == r.ccopy.Numput &&
		r.Core.Numget == r.ccopy.Numget &&
		r.Core.Numdelete == r.ccopy.Numdelete &&
		r.Core.Bytesloaded == r.ccopy.Bytesloaded &&
		r.Core.Bytesevicted == r.ccopy.Bytesevicted {
		return false
	}
	// 1. core stats
	glog.Infof("%s: %+v", r.name, r.Core)

	// 2. capacity
	runlru = r.updateCapacity()

	// 3. format and log usage %%
	for _, mpath := range r.fsmap {
		fscapacity := r.Capacity[mpath]
		glog.Infof("capacity: %+v", fscapacity)
	}

	r.syncstats(&r.ccopy)
	return runlru
}

func (r *storstatsrunner) housekeep(runlru bool) {
	t := gettarget()

	if runlru && ctx.config.LRUConfig.LRUEnabled {
		go t.runLRU()
	}

	// Run prefetch operation if there are items to be prefetched
	if len(t.prefetchQueue) > 0 {
		go t.doPrefetch()
	}
}

func (r *storstatsrunner) updateCapacity() (runlru bool) {
	r.Lock()
	defer r.Unlock()
	for _, mpath := range r.fsmap {
		statfs := &syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		fscapacity := r.Capacity[mpath]
		r.fillfscap(fscapacity, statfs)
		if fscapacity.Usedpct >= ctx.config.LRUConfig.HighWM {
			runlru = true
		}
	}
	return
}

func (r *storstatsrunner) fillfscap(fscapacity *fscapacity, statfs *syscall.Statfs_t) {
	fscapacity.Used = (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize)
	fscapacity.Avail = statfs.Bavail * uint64(statfs.Bsize)
	fscapacity.Usedpct = uint32((statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks)
}

func (r *storstatsrunner) initCapacity() {
	r.Capacity = make(map[string]*fscapacity)
	r.fsmap = make(map[syscall.Fsid]string)
	for mpath, mountpath := range ctx.mountpaths {
		mp1, ok := r.fsmap[mountpath.Fsid]
		if ok {
			// the same filesystem: usage cannot be different..
			assert(r.Capacity[mp1] != nil)
			r.Capacity[mpath] = r.Capacity[mp1]
			continue
		}
		statfs := &syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		r.fsmap[mountpath.Fsid] = mpath
		r.Capacity[mpath] = &fscapacity{}
		r.fillfscap(r.Capacity[mpath], statfs)
	}
}
