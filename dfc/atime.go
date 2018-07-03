// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	chfqnSize     = 1024
	atimeCacheIni = 4 * 1024
	atimeSyncTime = time.Minute * 3
	atimeLWM      = 60
	atimeHWM      = 80
)

type accessTimeResponse struct {
	accessTime time.Time
	ok         bool
}

type atimemap struct {
	fsToFilesMap map[string]map[string]time.Time
}

type atimerunner struct {
	namedrunner
	chfqn       chan string // FIXME: consider { fqn, xxhash }
	chstop      chan struct{}
	atimemap    *atimemap
	chGetAtime  chan string             // Process request to get atime of an fqn
	chSendAtime chan accessTimeResponse // Return access time of an fqn
}

func (r *atimerunner) run() error {
	glog.Infof("Starting %s", r.name)
	ticker := time.NewTicker(atimeSyncTime)
loop:
	for {
		select {
		case <-ticker.C:
			for fileSystem := range r.atimemap.fsToFilesMap {
				if n := r.heuristics(fileSystem); n > 0 {
					r.flush(fileSystem, n)
				}
			}
		case fqn := <-r.chfqn:
			r.updateATimeInATimeMap(fqn)
		case fqn := <-r.chGetAtime:
			r.chSendAtime <- r.handleInputOnGetATimeChannel(fqn)
		case <-r.chstop:
			ticker.Stop() // NOTE: not flushing cached atimes
			break loop
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
	if ctx.config.LRU.LRUEnabled {
		r.chfqn <- fqn
	}
}

func (r *atimerunner) atime(fqn string) {
	r.chGetAtime <- fqn
}

func (r *atimerunner) heuristics(fileSystem string) (n int) {
	if !ctx.config.LRU.LRUEnabled {
		return
	}
	l := len(r.atimemap.fsToFilesMap[fileSystem])
	if l <= atimeCacheIni {
		return
	}
	wm := 100
	if uint64(l) < ctx.config.LRU.AtimeCacheMax {
		wm = int(uint64(l) * 100 / ctx.config.LRU.AtimeCacheMax)
	}
	riostat := getiostatrunner()
	maxutil := float32(-1)
	util, ok := riostat.getDiskUtilizationFromFileSystem(fileSystem)
	if ok {
		maxutil = util
	}
	switch {
	case maxutil >= 0 && maxutil < 50: // idle
		n = l / 4
	case wm > atimeHWM: // atime map capacity at high watermark
		if wm == 100 {
			n = l / 2
		} else {
			n = l / 4
		}
	case wm > atimeLWM && maxutil >= 0 && maxutil < 90: // low watermark => weighted formula
		f := float64(wm-atimeLWM) / float64(atimeHWM-atimeLWM) * float64(l)
		n = int(f) / 4
	}
	return
}

func (r *atimerunner) flush(fileSystem string, n int) {
	var (
		i     int
		mtime time.Time
	)
	for fqn, atime := range r.atimemap.fsToFilesMap[fileSystem] {
		finfo, err := os.Stat(fqn)
		if err != nil {
			if os.IsNotExist(err) {
				delete(r.atimemap.fsToFilesMap[fileSystem], fqn)
				i++
			} else {
				glog.Warningf("failing to touch %s, err: %v", fqn, err)
			}
			goto cont
		}
		mtime = finfo.ModTime()
		if err = os.Chtimes(fqn, atime, mtime); err != nil {
			if os.IsNotExist(err) {
				delete(r.atimemap.fsToFilesMap[fileSystem], fqn)
				i++
			} else {
				glog.Warningf("can't touch %s, err: %v", fqn, err) // FIXME: carry on forever?
			}
		} else {
			delete(r.atimemap.fsToFilesMap[fileSystem], fqn)
			i++
			if glog.V(4) {
				glog.Infof("touch %s at %v", fqn, atime)
			}
		}
	cont:
		if i >= n {
			break
		}
	}
}

func (r *atimerunner) handleInputOnGetATimeChannel(fqn string) accessTimeResponse {
	if !ctx.config.LRU.LRUEnabled {
		return accessTimeResponse{}
	}
	fileSystem := getFileSystemUsingMountPath(fqn)
	if fileSystem == "" {
		return accessTimeResponse{}
	}
	if _, isOk := r.atimemap.fsToFilesMap[fileSystem]; !isOk {
		return accessTimeResponse{}
	}
	accessTime, ok := r.atimemap.fsToFilesMap[fileSystem][fqn]
	return accessTimeResponse{accessTime: accessTime, ok: ok}
}

func (r *atimerunner) updateATimeInATimeMap(fqn string) {
	fileSystem := getFileSystemUsingMountPath(fqn)
	if fileSystem == "" {
		return
	}
	if _, ok := r.atimemap.fsToFilesMap[fileSystem]; !ok {
		r.atimemap.fsToFilesMap[fileSystem] = make(map[string]time.Time)
	}
	r.atimemap.fsToFilesMap[fileSystem][fqn] = time.Now()
}
