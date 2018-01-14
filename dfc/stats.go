/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
)

type Storstats struct {
	Numget       int64 `json:"numget"`
	Numcoldget   int64 `json:"numcoldget"`
	Bytesloaded  int64 `json:"bytesloaded"`
	Bytesevicted int64 `json:"bytesevicted"`
	Filesevicted int64 `json:"filesevicted"`
	Numerr       int64 `json:"numerr"`
}

type usedstats map[string]int

type statslogger interface {
	log()
}

type Proxystats struct {
	Numget    int64 `json:"numget"`
	Numpost   int64 `json:"numpost"`
	Numdelete int64 `json:"numdelete"`
	Numerr    int64 `json:"numerr"`
}

type statsrunner struct {
	namedrunner
	statslogger
	chsts chan os.Signal
}

type proxystatsrunner struct {
	statsrunner
	stats     Proxystats
	statscopy Proxystats
	lock      *sync.Mutex
}

type storstatsrunner struct {
	statsrunner
	stats     Storstats
	statscopy Storstats
	used      usedstats
	lock      *sync.Mutex
}

func (r *statsrunner) runcommon(logger statslogger) error {
	r.chsts = make(chan os.Signal, 1)

	glog.Infof("Starting %s", r.name)
	ticker := time.NewTicker(ctx.config.StatsTime)
	for {
		select {
		case <-ticker.C:
			logger.log()
		case <-r.chsts:
			ticker.Stop()
			return nil
		}
	}
}

func (r *statsrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	close(r.chsts)
}

func (r *statsrunner) log() {
	assert(false)
}

func (r *proxystatsrunner) run() error {
	r.lock = &sync.Mutex{}
	return r.runcommon(r)
}

func (r *proxystatsrunner) syncstats(stats *Proxystats) {
	r.lock.Lock()
	copyStruct(stats, &r.stats)
	r.lock.Unlock()
}

func (r *proxystatsrunner) log() {
	// nothing changed since the previous invocation
	if r.stats.Numget == r.statscopy.Numget &&
		r.stats.Numpost == r.statscopy.Numpost &&
		r.stats.Numdelete == r.statscopy.Numdelete {
		return
	}
	s := fmt.Sprintf("%s: %+v", r.name, r.stats)
	r.syncstats(&r.statscopy)
	glog.Infoln(s)
}

func (r *storstatsrunner) run() error {
	r.lock = &sync.Mutex{}
	return r.runcommon(r)
}

func (r *storstatsrunner) syncstats(stats *Storstats) {
	r.lock.Lock()
	copyStruct(stats, &r.stats)
	r.lock.Unlock()
}

func (r *storstatsrunner) log() {
	// nothing changed since the previous invocation
	if r.stats.Numget == r.statscopy.Numget &&
		r.stats.Bytesloaded == r.statscopy.Bytesloaded &&
		r.stats.Bytesevicted == r.statscopy.Bytesevicted {
		return
	}
	// 1. format and log Get stats
	mbytesloaded := float64(r.stats.Bytesloaded) / 1000 / 1000
	mbytesevicted := float64(r.stats.Bytesevicted) / 1000 / 1000
	s := fmt.Sprintf("%s: numget,%d,numcoldget,%d,mbytesloaded,%.2f,mbytesevicted,%.2f,filesevicted,%d,numerr,%d",
		r.name, r.stats.Numget, r.stats.Numcoldget,
		mbytesloaded, mbytesevicted, r.stats.Filesevicted, r.stats.Numerr)
	glog.Infoln(s)

	// 2. assign usage %%
	var runlru bool
	fsmap := make(map[syscall.Fsid]int, len(ctx.mountpaths))
	for _, mountpath := range ctx.mountpaths {
		uu, ok := fsmap[mountpath.Fsid]
		if ok {
			// the same filesystem: usage cannot be different..
			r.used[mountpath.Path] = uu
			continue
		}
		statfs := syscall.Statfs_t{}
		if err := syscall.Statfs(mountpath.Path, &statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mountpath.Path, err)
			continue
		}
		u := (statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks
		if u >= uint64(ctx.config.Cache.FSHighWaterMark) {
			runlru = true
		}
		r.used[mountpath.Path], fsmap[mountpath.Fsid] = int(u), int(u)
	}

	// 3. format and log usage %%
	s = fmt.Sprintf("%s used: %+v", r.name, r.used)
	glog.Infoln(s)
	r.syncstats(&r.statscopy)
	// 4. LRU
	if runlru {
		go all_LRU()
	}
}

//
func statsAdd(v *int64, val int64) {
	*v += val
}
