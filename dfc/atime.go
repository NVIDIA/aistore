// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/common"
)

// ================================ Summary ===============================================
//
// The atime (access time) module implements an atimerunner - a long running task with the
// purpose of updating object (file) access times.
//
// The atimerunner API exposed to the rest of the code includes the following operations:
//   * run   - to initiate the work of the receiving atimerunner
//   * stop  - to stop the receiving atimerunner
//   * touch - to request an access time update for a given file
//   * atime - to request the most recent access time of a given file
//
// All other operations are private to the atimerunner and used only internally!
//
// atimerunner will accumulate object access times in its access time map (in memory) and
// periodically flush the data to the disk. Access times get flushed to the disk when the
// number of stored access times has reached a certain flush threshold and when:
//   * disk utilization is low, or
//   * access time map is filled over a certain point (watermark)
// This way, the atimerunner will impact the datapath as little as possible.
// As such, atimerunner can be thought of as an extension of the LRU, or any alternative
// caching mechanism that can be implemented in the future.
//
// The reason behind the existence of this module is the 'noatime' mounting option;
// if a file system has been mounted with this option, reading accesses to the
// file system will no longer result in an update to the atime information associated
// with the file, which eliminates the need to make writes to the file system for files
// that are simply being read, resulting in noticeable performance improvements.
// Inside DFC cluster, this option is always set, so DFC implements its own access time
// updating.
//
// Requests for object access times are read from a filename channel and corresponding
// responses will be written to an accessTimeResponse (see definition below) channel.
//
// ================================ Summary ===============================================

const (
	chfqnSize                = 1024
	atimeCacheFlushThreshold = 4 * 1024
	atimeSyncTime            = time.Minute * 3
	atimeLWM                 = 60
	atimeHWM                 = 80
)

// atimemap maps filesystems to file-atime key-value pairs.
type atimemap struct {
	fsToFilesMap map[string]map[string]time.Time
}

type atimerunner struct {
	namedrunner
	chfqn      chan string             // FIXME: consider { fqn, xxhash }
	chstop     chan struct{}           // Control channel for stopping
	atimemap   *atimemap               // Access time map
	chGetAtime chan *accessTimeRequest // Requests for file access times
}

// Each request to getAtime is encapsulated in an
// accessTimeRequest. The wg member variable is used
// to ensure each request for access time gets its
// corresponding response. The accessTime and ok fields
// are used to return the access time of the file
// in the atimemap and whether it actually existed in
// the atimemap respectively
type accessTimeRequest struct {
	accessTime time.Time
	ok         bool
	wg         *sync.WaitGroup
	fqn        string
}

func newAtimeRunner() (r *atimerunner) {
	r = &atimerunner{}
	r.chstop = make(chan struct{}, 4)
	r.chfqn = make(chan string, chfqnSize)
	r.atimemap = &atimemap{fsToFilesMap: make(map[string]map[string]time.Time, atimeCacheFlushThreshold)}
	r.chGetAtime = make(chan *accessTimeRequest)
	return
}

// run initiates the work of the receiving atimerunner
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
		case request := <-r.chGetAtime:
			r.handleInputOnGetATimeChannel(request)
		case <-r.chstop:
			ticker.Stop() // NOTE: not flushing cached atimes
			break loop
		}
	}
	return nil
}

// stop aborts the receiving atimerunner
func (r *atimerunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chstop <- v
	close(r.chstop)
}

// touch requests an access time update for a given file
func (r *atimerunner) touch(fqn string) {
	if ctx.config.LRU.LRUEnabled {
		r.chfqn <- fqn
	}
}

// atime requests the most recent access time of a given file
func (r *atimerunner) atime(fqn string) (time.Time, bool) {
	request := &accessTimeRequest{wg: &sync.WaitGroup{}, fqn: fqn}
	request.wg.Add(1)
	r.chGetAtime <- request
	request.wg.Wait()
	return request.accessTime, request.ok
}

// getNumberItemsToFlush estimates the number of timestamps that must be flushed
// the atime map, by taking into account the max utilitization of the corresponding
// local filesystem (or, more exactly, the corresponding local filesystem's disks).
func (r *atimerunner) getNumberItemsToFlush(fileSystem string) (n int) {
	if !ctx.config.LRU.LRUEnabled {
		return
	}
	atimeMapSize := len(r.atimemap.fsToFilesMap[fileSystem])
	if atimeMapSize <= atimeCacheFlushThreshold {
		return
	}

	filling := common.MinU64(100, uint64(atimeMapSize)*100/ctx.config.LRU.AtimeCacheMax)
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

func (r *atimerunner) handleInputOnGetATimeChannel(request *accessTimeRequest) {
	defer request.wg.Done()
	if !ctx.config.LRU.LRUEnabled {
		return
	}
	fileSystem := fqn2fs(request.fqn)
	if fileSystem == "" {
		return
	}
	if _, ok := r.atimemap.fsToFilesMap[fileSystem]; !ok {
		return
	}
	request.accessTime, request.ok = r.atimemap.fsToFilesMap[fileSystem][request.fqn]
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
