// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
)

//==============================
//
// types
//
//==============================
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
	logged    bool  `json:"-"`
}

type targetCoreStats struct {
	proxyCoreStats
	Numcoldget       int64 `json:"numcoldget"`
	Bytesloaded      int64 `json:"bytesloaded"`
	Bytesevicted     int64 `json:"bytesevicted"`
	Filesevicted     int64 `json:"filesevicted"`
	Numsentfiles     int64 `json:"numsentfiles"`
	Numsentbytes     int64 `json:"numsentbytes"`
	Numrecvfiles     int64 `json:"numrecvfiles"`
	Numrecvbytes     int64 `json:"numrecvbytes"`
	Numlist          int64 `json:"numlist"`
	Numprefetch      int64 `json:"numprefetch"`
	Bytesprefetched  int64 `json:"bytesprefetched"`
	Numvchanged      int64 `json:"numvchanged"`
	Bytesvchanged    int64 `json:"bytesvchanged"`
	Numbadchecksum   int64 `json:"numbadchecksum"`
	Bytesbadchecksum int64 `json:"bytesbadchecksum"`
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
}

type deviometrics map[string]string

type storstatsrunner struct {
	statsrunner         `json:"-"`
	timeUpdatedCapacity time.Time               `json:"-"`
	Core                targetCoreStats         `json:"core"`
	Capacity            map[string]*fscapacity  `json:"capacity"`
	fsmap               map[syscall.Fsid]string `json:"-"`
	// iostat
	CPUidle string                  `json:"cpuidle"`
	Disk    map[string]deviometrics `json:"disk"`
}

type ClusterStats struct {
	Proxy  *proxyCoreStats             `json:"proxy"`
	Target map[string]*storstatsrunner `json:"target"`
}

type iostatrunner struct {
	sync.Mutex
	namedrunner
	chsts       chan struct{}
	CPUidle     string
	metricnames []string
	Disk        map[string]deviometrics
}

//==============================================================
//
// c-tor and methods
//
//==============================================================
func newClusterStats() *ClusterStats {
	targets := make(map[string]*storstatsrunner, ctx.smap.count())
	for _, si := range ctx.smap.Smap {
		targets[si.DaemonID] = &storstatsrunner{Capacity: make(map[string]*fscapacity)}
	}
	return &ClusterStats{Target: targets}
}

//==================
//
// common statsunner
//
//==================
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
}

//=================
//
// proxystatsrunner
//
//=================
func (r *proxystatsrunner) run() error {
	return r.runcommon(r)
}

// statslogger interface impl
func (r *proxystatsrunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	s := fmt.Sprintf("%s: %+v", r.name, r.Core)
	r.Core.logged = true
	r.Unlock()

	glog.Infoln(s)
	return
}

func (r *proxystatsrunner) add(name string, val int64) {
	var v *int64
	s := &r.Core
	r.Lock()
	defer r.Unlock()
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
	s.logged = false
}

//================
//
// storstatsrunner
//
//================
func (r *storstatsrunner) run() error {
	return r.runcommon(r)
}

func (r *storstatsrunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	lines := make([]string, 0, 16)
	// core stats
	b, err := json.Marshal(r.Core)
	if err == nil {
		lines = append(lines, string(b))
	}
	// capacity
	if time.Since(r.timeUpdatedCapacity) >= ctx.config.LRUConfig.CapacityUpdTime {
		runlru = r.updateCapacity()
		r.timeUpdatedCapacity = time.Now()
		for _, mpath := range r.fsmap {
			fscapacity := r.Capacity[mpath]
			b, err := json.Marshal(fscapacity)
			if err == nil {
				lines = append(lines, mpath+": "+string(b))
			}
		}
	}
	// disk
	riostat := getiostatrunner()
	if riostat != nil {
		riostat.Lock()
		r.CPUidle = riostat.CPUidle
		for k, v := range riostat.Disk {
			r.Disk[k] = v // copy
			b, err := json.Marshal(r.Disk[k])
			if err == nil {
				lines = append(lines, k+": "+string(b))
			}
		}
		lines = append(lines, fmt.Sprintf("CPU idle: %s%%", r.CPUidle))
		riostat.Unlock()
	}

	r.Core.logged = true
	r.Unlock()

	// log
	for _, ln := range lines {
		glog.Infoln(ln)
	}
	return
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

func (r *storstatsrunner) init() {
	r.Disk = make(map[string]deviometrics, 8)
	// local filesystems and their cap-s
	r.Capacity = make(map[string]*fscapacity)
	r.fsmap = make(map[syscall.Fsid]string)
	for mpath, mountpath := range ctx.mountpaths.available {
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

func (r *storstatsrunner) add(name string, val int64) {
	var v *int64
	s := &r.Core
	r.Lock()
	defer r.Unlock()
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
	case "numvchanged":
		v = &s.Numvchanged
	case "bytesvchanged":
		v = &s.Bytesvchanged
	case "numbadchecksum":
		v = &s.Numbadchecksum
	case "bytesbadchecksum":
		v = &s.Bytesbadchecksum
	default:
		assert(false, "Invalid stats name "+name)
	}
	*v += val
	s.logged = false
}
