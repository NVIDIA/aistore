/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/golang/glog"
)

type storstats struct {
	numget       int64
	numcoldget   int64
	bytesloaded  int64
	bytesevicted int64
	filesevicted int64
	numerr       int64
}

type usedstats map[string]int

type statslogger interface {
	log()
}

type proxystats struct {
	numget    int64
	numpost   int64
	numdelete int64
	numerr    int64
}

type statsrunner struct {
	namedrunner
	statslogger
	chsts chan os.Signal
}

type proxystatsrunner struct {
	statsrunner
	stats proxystats
}

type storstatsrunner struct {
	statsrunner
	stats storstats
	used  usedstats
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
	return r.runcommon(r)
}

func (r *proxystatsrunner) log() {
	// nothing changed since the previous call
	if r.stats.numget == 0 {
		return
	}
	s := fmt.Sprintf("%s: %+v", r.name, r.stats)
	glog.Infoln(s)
	// zero out all counters except err
	numerr := r.stats.numerr
	clearStruct(&r.stats)
	r.stats.numerr = numerr
}

func (r *storstatsrunner) run() error {
	return r.runcommon(r)
}

func (r *storstatsrunner) log() {
	// nothing changed since the previous call
	if r.stats.numget == 0 && r.stats.bytesloaded == 0 && r.stats.bytesevicted == 0 {
		return
	}
	// 1. format and log Get stats
	mbytesloaded := float64(r.stats.bytesloaded) / 1000 / 1000
	mbytesevicted := float64(r.stats.bytesevicted) / 1000 / 1000
	s := fmt.Sprintf("%s: numget,%d,numcoldget,%d,mbytesloaded,%.2f,mbytesevicted,%.2f,filesevicted,%d,numerr,%d",
		r.name, r.stats.numget, r.stats.numcoldget,
		mbytesloaded, mbytesevicted, r.stats.filesevicted, r.stats.numerr)
	glog.Infoln(s)

	// 2. assign usage %%
	var runlru bool
	fsmap := make(map[syscall.Fsid]int, len(ctx.mountpaths))
	for _, mountpath := range ctx.mountpaths {
		uu, ok := fsmap[mountpath.Fsid]
		if ok {
			glog.Infof("%s duplicate FSID %v, mpath %q", r.name, mountpath.Fsid, mountpath.Path)
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

	// 4. LRU
	if runlru {
		go all_LRU()
	}

	// 5. zero out all counters except err
	numerr := r.stats.numerr
	clearStruct(&r.stats)
	r.stats.numerr = numerr
}
