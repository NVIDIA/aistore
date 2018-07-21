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
	chfqnSize                = 1024
	atimeCacheFlushThreshold = 4 * 1024 // number of items before which we do not preform any flushing
	atimeSyncTime            = time.Minute * 3
	atimeLWM                 = 60
	atimeHWM                 = 80
)

type accessTimeResponse struct {
	accessTime time.Time
	ok         bool
}

type atimemap struct {
	fsToFilesMap map[string]map[string]time.Time
}

// atimerunner's responsibilities include updating object/file access
// times and making sure that those background updates are not impacting
// datapath (or rather, impacting the datapath as little as possible).
// As such, atimerunner can be thought of as an extension of the LRU (lru.go)
// or any alternative caching mechanisms that can be added in the future.
// Access times get flushed when the system idle - otherwise, they get
// accumulated up to a certain point (aka watermark) and get synced to disk
// anyway.
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
				if n := r.getNumberItemsToFlush(fileSystem); n > 0 {
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

// touch sends update request of access time for fqn to atimerunner.
func (r *atimerunner) touch(fqn string) {
	if ctx.config.LRU.LRUEnabled {
		r.chfqn <- fqn
	}
}

func (r *atimerunner) atime(fqn string) {
	r.chGetAtime <- fqn
}

// getNumberItemsToFlush estimates the number of timestamps that must
// be flushed from the atime map - by taking into account the max
// utilitization of the corresponding local filesystem
// (or, more exactly, the corresponding local filesystem's disks)
func (r *atimerunner) getNumberItemsToFlush(fileSystem string) (n int) {
	if !ctx.config.LRU.LRUEnabled {
		return
	}
	atimeMapSize := len(r.atimemap.fsToFilesMap[fileSystem])
	if atimeMapSize <= atimeCacheFlushThreshold {
		return
	}

	filling := MinU64(100, uint64(atimeMapSize)*100/ctx.config.LRU.AtimeCacheMax)
	riostat := getiostatrunner()

	maxDiskUtil := float32(-1)
	util, ok := riostat.maxUtilFS(fileSystem)
	if ok {
		maxDiskUtil = util
	}

	switch {
	case maxDiskUtil >= 0 && maxDiskUtil < 50: // disk is idle so we can utilize it a bit
		n = atimeMapSize / 4
	case filling == 100: // max capacity reached - flushing a great number of items is required
		n = atimeMapSize / 2
	case filling > atimeHWM: // atime map capacity at high watermark
		n = atimeMapSize / 4
	case filling > atimeLWM: // low watermark => weighted formula
		f := float64(filling-atimeLWM) / float64(atimeHWM-atimeLWM) * float64(atimeMapSize)
		n = int(f) / 4
	}

	return
}

// flush tries to change access and modification time for at most n files in
// the atime map, and removes them from the map.
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
	fileSystem := fqn2fs(fqn)
	if fileSystem == "" {
		return accessTimeResponse{}
	}
	if _, ok := r.atimemap.fsToFilesMap[fileSystem]; !ok {
		return accessTimeResponse{}
	}
	accessTime, ok := r.atimemap.fsToFilesMap[fileSystem][fqn]
	return accessTimeResponse{accessTime: accessTime, ok: ok}
}

func (r *atimerunner) updateATimeInATimeMap(fqn string) {
	fileSystem := fqn2fs(fqn)
	if fileSystem == "" {
		return
	}
	if _, ok := r.atimemap.fsToFilesMap[fileSystem]; !ok {
		r.atimemap.fsToFilesMap[fileSystem] = make(map[string]time.Time)
	}
	r.atimemap.fsToFilesMap[fileSystem][fqn] = time.Now()
}
