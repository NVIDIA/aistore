// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
)

// FIXME: part must go => config
const (
	chfqnSize     = 1024
	atimeCacheIni = 4 * 1024
	atimeCacheMax = 64 * 1024 // entries
	atimeSyncTime = time.Minute
)

type atimemap struct {
	sync.Mutex
	m map[string]time.Time
}

type atimerunner struct {
	namedrunner
	chfqn    chan string // FIXME: { fqn, xxhash }
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
			r.syncMaybe()
		case fqn := <-r.chfqn:
			r.atimemap.Lock()
			r.atimemap.m[fqn] = time.Now()
			r.atimemap.Unlock()
		case <-r.chstop:
			// FIXME: flush all cached atimes
			ticker.Stop()
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

func (r *atimerunner) notify(fqn string) {
	r.chfqn <- fqn
}

func (r *atimerunner) atime(fqn string) (atime time.Time, ok bool) {
	r.atimemap.Lock()
	defer r.atimemap.Unlock()
	atime, ok = r.atimemap.m[fqn]
	return
}

// FIXME: depend on a) size vs max and b) current utilization
func (r *atimerunner) syncMaybe() {
	r.atimemap.Lock()
	defer r.atimemap.Unlock()

	for fqn, atime := range r.atimemap.m {
		tsa := syscall.NsecToTimespec(atime.UnixNano())
		finfo, err := os.Stat(fqn)
		if err != nil {
			if os.IsNotExist(err) {
				delete(r.atimemap.m, fqn)
			} else {
				glog.Warningf("can't access %s, err: %v", fqn, err)
			}
			continue
		}
		mtime := finfo.ModTime()
		tsm := syscall.NsecToTimespec(mtime.UnixNano())
		tss := []syscall.Timespec{tsa, tsm}
		err = syscall.UtimesNano(fqn, tss)
		if err != nil {
			if os.IsNotExist(err) {
				delete(r.atimemap.m, fqn)
			} else {
				glog.Warningf("can't utimensat %s, err: %v", fqn, err) // FIXME: carry on forever?
			}
		} else {
			delete(r.atimemap.m, fqn)
			if glog.V(3) {
				glog.Infof("utimensat %s at %v", fqn, tss)
			}
		}
	}
}
