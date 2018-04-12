// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
)

// FIXME: config, at least in part
const (
	chfqnSize     = 1024
	atimeCacheIni = 4 * 1024
	atimeCacheMax = 64 * 1024 // max ## entries
	atimeSyncTime = time.Minute
	atimeLWM      = 60
	atimeHWM      = 80
)

type atimemap struct {
	sync.Mutex
	m map[string]time.Time
}

type atimerunner struct {
	namedrunner
	chfqn    chan string // FIXME: consider { fqn, xxhash }
	chstop   chan struct{}
	atimemap *atimemap
}

func (r *atimerunner) run() error {
	glog.Infof("Starting %s", r.name)
	r.chstop = make(chan struct{}, 4)
	r.chfqn = make(chan string, chfqnSize)
	r.atimemap = &atimemap{m: make(map[string]time.Time, atimeCacheIni)}

	ticker := time.NewTicker(atimeSyncTime)
	for {
		select {
		case <-ticker.C:
			if n := r.heuristics(); n > 0 {
				r.flush(n)
			}
		case fqn := <-r.chfqn:
			r.atimemap.Lock()
			r.atimemap.m[fqn] = time.Now()
			r.atimemap.Unlock()
		case <-r.chstop:
			ticker.Stop() // NOTE: not flushing cached atimes
			return nil
		}
	}

	return nil
}

func (r *atimerunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chstop <- v
	close(r.chstop)
}

func (r *atimerunner) touch(fqn string) {
	r.chfqn <- fqn
}

func (r *atimerunner) atime(fqn string) (atime time.Time, ok bool) {
	r.atimemap.Lock()
	defer r.atimemap.Unlock()
	atime, ok = r.atimemap.m[fqn]
	return
}

func (r *atimerunner) heuristics() (n int) {
	l := len(r.atimemap.m)
	if l <= atimeCacheIni {
		return
	}
	maxutil := float64(-1)
	wm := l * 100 / atimeCacheMax
	riostat := getiostatrunner()
	if riostat != nil {
		riostat.Lock()
		maxutil = riostat.maxDiskUtil
		riostat.Unlock()
	}
	switch {
	case maxutil >= 0 && maxutil < 50: // idle
		n = l / 4
	case wm > atimeHWM: // atime map capacity at high watermark
		n = l / 4
	case wm > atimeLWM && maxutil >= 0 && maxutil < 90: // low watermark => weighted formula
		f := float64(wm-atimeLWM) / float64(atimeHWM-atimeLWM) * float64(l)
		n = int(f) / 4
	}
	return
}

func (r *atimerunner) flush(n int) {
	r.atimemap.Lock()
	defer r.atimemap.Unlock()
	var (
		i     int
		mtime time.Time
	)
	for fqn, atime := range r.atimemap.m {
		finfo, err := os.Stat(fqn)
		if err != nil {
			if os.IsNotExist(err) {
				delete(r.atimemap.m, fqn)
				i++
			} else {
				glog.Warningf("failing to touch %s, err: %v", fqn, err)
			}
			goto cont
		}
		mtime = finfo.ModTime()
		if err = os.Chtimes(fqn, atime, mtime); err != nil {
			if os.IsNotExist(err) {
				delete(r.atimemap.m, fqn)
				i++
			} else {
				glog.Warningf("can't touch %s, err: %v", fqn, err) // FIXME: carry on forever?
			}
		} else {
			delete(r.atimemap.m, fqn)
			i++
			if glog.V(3) {
				glog.Infof("touch %s at %v", fqn, atime)
			}
		}
	cont:
		if i >= n {
			break
		}
	}
}
