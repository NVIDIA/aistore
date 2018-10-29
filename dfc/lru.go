// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/fs"
)

// ============================================= Summary ===========================================
//
// The LRU module implements a well-known least-recently-used cache replacement policy.
//
// In DFC, LRU-driven eviction is based on the two configurable watermarks: ctx.config.LRU.LowWM and
// ctx.config.LRU.HighWM (section "lru_config" in the setup/config.sh).
// When and if exceeded, DFC storage target will start gradually evicting objects from its
// stable storage: oldest first access-time wise.
//
// LRU is implemented as a so-called extended action (aka x-action, see xaction.go) that gets
// triggered when/if a used local capacity exceeds high watermark (ctx.config.LRU.HighWM). LRU then
// runs automatically. In order to reduce its impact on the live workload, LRU throttles itself
// in accordance with the current storage-target's utilization (see xaction_throttle.go).
//
// There's only one API that this module provides to the rest of the code:
//   - runLRU - to initiate a new LRU extended action on the receiving target
//
// All other methods are private to this module and used only internally, including:
//   - oneLRU   - initiates LRU context and walks a given local filesystem processing each
//                object (lruwalkfn method) to determine whether the object is to be evicted;
//   - lruEvict - evicts a given file from the cache
//
// ============================================= Summary ===========================================

type fileInfo struct {
	fqn     string
	usetime time.Time
	size    int64
}

type fileInfoMinHeap []*fileInfo

type lructx struct {
	cursize     int64
	totsize     int64
	newest      time.Time
	xlru        *xactLRU
	heap        *fileInfoMinHeap
	oldwork     []*fileInfo
	t           *targetrunner
	fs          string
	thrctx      throttleContext
	atimeRespCh chan *atimeResponse
}

func (t *targetrunner) runLRU() {
	// FIXME: if LRU config has changed we need to force new LRU transaction
	xlru := t.xactinp.renewLRU(t)
	if xlru == nil {
		return
	}
	fschkwg := &sync.WaitGroup{}

	glog.Infof("LRU: %s started: dont-evict-time %v", xlru.tostring(), ctx.config.LRU.DontEvictTime)

	// copy available mountpaths
	availablePaths, _ := ctx.mountpaths.Mountpaths()
	for _, mpathInfo := range availablePaths {
		fschkwg.Add(1)
		go t.oneLRU(mpathInfo, makePathLocal(mpathInfo.Path), fschkwg, xlru)
	}
	fschkwg.Wait()
	for _, mpathInfo := range availablePaths {
		fschkwg.Add(1)
		go t.oneLRU(mpathInfo, makePathCloud(mpathInfo.Path), fschkwg, xlru)
	}
	fschkwg.Wait()

	// DEBUG
	if glog.V(4) {
		rr := getstorstatsrunner()
		rr.Lock()
		rr.updateCapacity()
		for _, mpathInfo := range availablePaths {
			fscapacity := rr.Capacity[mpathInfo.Path]
			if fscapacity.Usedpct > ctx.config.LRU.LowWM+1 {
				glog.Warningf("LRU mpath %s: failed to reach lwm %d%% (used %d%%)",
					mpathInfo.Path, ctx.config.LRU.LowWM, fscapacity.Usedpct)
			}
		}
		rr.Unlock()
	}

	xlru.etime = time.Now()
	glog.Infoln(xlru.tostring())
	t.xactinp.del(xlru.id)
}

// TODO: local-buckets-first LRU policy
func (t *targetrunner) oneLRU(mpathInfo *fs.MountpathInfo, bucketdir string, fschkwg *sync.WaitGroup, xlru *xactLRU) {
	defer fschkwg.Done()
	h := &fileInfoMinHeap{}
	heap.Init(h)

	toevict, err := getToEvict(bucketdir, ctx.config.LRU.HighWM, ctx.config.LRU.LowWM)
	if err != nil {
		return
	}
	glog.Infof("%s: evicting %.2f MB", bucketdir, float64(toevict)/common.MiB)

	// init LRU context
	var oldwork []*fileInfo

	lctx := &lructx{
		totsize:     toevict,
		xlru:        xlru,
		heap:        h,
		oldwork:     oldwork,
		t:           t,
		fs:          mpathInfo.FileSystem,
		atimeRespCh: make(chan *atimeResponse, 1),
	}
	if err = filepath.Walk(bucketdir, lctx.lruwalkfn); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("Stopping %q traversal: %s", bucketdir, s)
		} else {
			glog.Errorf("Failed to traverse %q, err: %v", bucketdir, err)
		}
		return
	}
	if err := t.doLRU(toevict, bucketdir, lctx); err != nil {
		glog.Errorf("doLRU %q, err: %v", bucketdir, err)
	}
}

// the callback is executed by the LRU xaction (notice the receiver)
func (lctx *lructx) lruwalkfn(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		glog.Errorf("walkfunc callback invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	var (
		iswork, isold bool
		xlru, h       = lctx.xlru, lctx.heap
	)
	if iswork, isold = lctx.t.isworkfile(fqn); iswork {
		if !isold {
			return nil
		}
	}

	lctx.thrctx.throttle(lctx.newLRUThrottleParams(fqn))

	_, err = os.Stat(fqn)
	if os.IsNotExist(err) {
		glog.Infof("Warning (LRU race?): %s "+doesnotexist, fqn)
		glog.Flush()
		return nil
	}
	// abort?
	select {
	case <-xlru.abrt:
		s := fmt.Sprintf("%s aborted, exiting lruwalkfn", xlru.tostring())
		glog.Infoln(s)
		glog.Flush()
		return errors.New(s)
	case <-time.After(time.Millisecond):
		break
	}
	if xlru.finished() {
		return fmt.Errorf("%s aborted - exiting lruwalkfn", xlru.tostring())
	}

	atime, mtime, stat := getAmTimes(osfi)
	if isold {
		fi := &fileInfo{
			fqn:  fqn,
			size: stat.Size,
		}
		lctx.oldwork = append(lctx.oldwork, fi)
		return nil
	}

	// object eviction: access time
	usetime := atime

	atimeResponse := <-getatimerunner().atime(fqn, lctx.atimeRespCh)
	accessTime, ok := atimeResponse.accessTime, atimeResponse.ok
	if ok {
		usetime = accessTime
	} else if mtime.After(atime) {
		usetime = mtime
	}
	now := time.Now()
	dontevictime := now.Add(-ctx.config.LRU.DontEvictTime)
	if usetime.After(dontevictime) {
		if glog.V(4) {
			glog.Infof("DEBUG: not evicting %s (usetime %v, dontevictime %v)", fqn, usetime, dontevictime)
		}
		return nil
	}

	// cleanup after rebalance
	_, _, err = lctx.t.fqn2bckobj(fqn)
	if err != nil {
		glog.Infof("Found orphan file: %s", fqn)
		fi := &fileInfo{
			fqn:  fqn,
			size: stat.Size,
		}
		lctx.oldwork = append(lctx.oldwork, fi)
		return nil
	}

	// partial optimization:
	// do nothing if the heap's cursize >= totsize &&
	// the file is more recent then the the heap's newest
	// full optimization (tbd) entails compacting the heap when its cursize >> totsize
	if lctx.cursize >= lctx.totsize && usetime.After(lctx.newest) {
		if glog.V(3) {
			glog.Infof("DEBUG: use-time-after (usetime=%v, newest=%v) %s", usetime, lctx.newest, fqn)
		}
		return nil
	}
	// push and update the context
	fi := &fileInfo{
		fqn:     fqn,
		usetime: usetime,
		size:    stat.Size,
	}
	heap.Push(h, fi)
	lctx.cursize += fi.size
	if usetime.After(lctx.newest) {
		lctx.newest = usetime
	}
	return nil
}

func (lctx *lructx) newLRUThrottleParams(fqn string) *throttleParams {
	return &throttleParams{
		throttle: onDiskUtil | onFSUsed,
		fs:       lctx.fs,
		fqn:      fqn,
	}
}

func (t *targetrunner) doLRU(toevict int64, bucketdir string, lctx *lructx) error {
	h := lctx.heap
	var (
		fevicted, bevicted int64
	)
	for _, fi := range lctx.oldwork {
		if t.isRebalancing() {
			iswork, _ := t.isworkfile(fi.fqn)
			_, _, err := t.fqn2bckobj(fi.fqn)
			// keep a copy of a rebalanced file while rebalance is running
			if !iswork && err != nil {
				continue
			}
		}
		if err := os.Remove(fi.fqn); err != nil {
			glog.Warningf("LRU: failed to GC %q", fi.fqn)
			continue
		}
		toevict -= fi.size
		glog.Infof("LRU: GC-ed %q", fi.fqn)
	}
	for h.Len() > 0 && toevict > 0 {
		fi := heap.Pop(h).(*fileInfo)
		if err := t.lruEvict(fi.fqn); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", fi.fqn, err)
			continue
		}
		toevict -= fi.size
		bevicted += fi.size
		fevicted++
	}
	t.statsif.add(statLruEvictSize, bevicted)
	t.statsif.add(statLruEvictCount, fevicted)
	return nil
}

func (t *targetrunner) lruEvict(fqn string) error {
	bucket, objname, err := t.fqn2bckobj(fqn)
	if err != nil {
		glog.Errorf("Evicting %q with error: %v", fqn, err)
		if e := os.Remove(fqn); e != nil {
			return fmt.Errorf("nested error: %v and %v", err, e)
		}
		glog.Infof("LRU: removed %q", fqn)
		return nil
	}
	uname := cluster.Uname(bucket, objname)
	t.rtnamemap.lockname(uname, true, &pendinginfo{Time: time.Now(), fqn: fqn}, time.Second)
	defer t.rtnamemap.unlockname(uname, true)

	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("LRU: evicted %s/%s", bucket, objname)
	return nil
}

// fileInfoMinHeap keeps fileInfo sorted by access time with oldest on top of the heap.
func (h fileInfoMinHeap) Len() int { return len(h) }

func (h fileInfoMinHeap) Less(i, j int) bool {
	return h[i].usetime.Before(h[j].usetime)
}

func (h fileInfoMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *fileInfoMinHeap) Push(x interface{}) {
	*h = append(*h, x.(*fileInfo))
}

func (h *fileInfoMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	fi := old[n-1]
	*h = old[0 : n-1]
	return fi
}
