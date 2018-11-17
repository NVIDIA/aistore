/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
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
	"github.com/NVIDIA/dfcpub/atime"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/ios"
	"github.com/NVIDIA/dfcpub/stats"
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
//   - runLRU - to initiate a new LRU extended action on the local target
// All other methods are private to this module and are used only internally.
//
// ============================================= Summary ===========================================

// LRU defaults/tunables
const (
	minevict = cmn.MiB
)

type (
	fileInfo struct {
		fqn     string
		usetime time.Time
		size    int64
	}
	fileInfoMinHeap []*fileInfo

	// lructx represents a single LRU context that runs in a single goroutine (worker)
	// that traverses and evicts a single given filesystem, or more exactly,
	// subtree in this filesystem identified by the bucketdir
	lructx struct {
		// runtime
		cursize int64
		totsize int64
		newest  time.Time
		heap    *fileInfoMinHeap
		oldwork []*fileInfo
		// init-time
		xlru         cmn.XactInterface
		fs           string
		bucketdir    string
		throttler    cluster.Throttler
		atimeRespCh  chan *atime.Response
		namelocker   cluster.NameLocker
		bmdowner     cluster.Bowner
		statsif      stats.Tracker
		targetrunner cluster.Target
	}
)

// onelru walks a given local filesystem to a) determine whether some of the
// objects are to be evicted, and b) actually evicting those
func (lctx *lructx) onelru(wg *sync.WaitGroup) {
	defer wg.Done()

	lctx.heap = &fileInfoMinHeap{}
	heap.Init(lctx.heap)
	if err := lctx.evictSize(); err != nil {
		return
	}
	if lctx.totsize < minevict {
		glog.Infof("%s: below threshold, nothing to do", lctx.bucketdir)
		return
	}
	glog.Infof("%s: evicting %s", lctx.bucketdir, cmn.B2S(lctx.totsize, 2))

	if err := filepath.Walk(lctx.bucketdir, lctx.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", lctx.bucketdir, s)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", lctx.bucketdir, err)
		}
		return
	}
	if err := lctx.evict(); err != nil {
		glog.Errorf("%s: failed to evict, err: %v", lctx.bucketdir, err)
	}
}

func (lctx *lructx) walk(fqn string, osfi os.FileInfo, err error) error {
	var (
		spec    cluster.ContentResolver
		info    *cluster.ContentInfo
		xlru, h = lctx.xlru, lctx.heap
	)
	if err != nil {
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	if spec, info = cluster.FileSpec(fqn); spec != nil && !spec.PermToEvict() && !info.Old {
		return nil
	}
	lctx.throttler.Sleep()

	_, err = os.Stat(fqn)
	if os.IsNotExist(err) {
		glog.Infof("Warning (race?): %s "+doesnotexist, fqn)
		glog.Flush()
		return nil
	}
	// abort?
	select {
	case <-xlru.ChanAbort():
		s := fmt.Sprintf("%s aborted, exiting", xlru)
		glog.Infoln(s)
		glog.Flush()
		return errors.New(s)
	case <-time.After(time.Millisecond):
		break
	}
	if xlru.Finished() {
		return fmt.Errorf("%s aborted, exiting", xlru)
	}

	atime, mtime, stat := ios.GetAmTimes(osfi)
	if info != nil && info.Old {
		fi := &fileInfo{fqn: fqn, size: stat.Size}
		lctx.oldwork = append(lctx.oldwork, fi) // TODO: upper-limit to avoid OOM; see Push as well
		return nil
	}

	// object eviction: access time
	usetime := atime

	atimeResponse := <-getatimerunner().Atime(fqn, lctx.atimeRespCh)
	accessTime, ok := atimeResponse.AccessTime, atimeResponse.Ok
	if ok {
		usetime = accessTime
	} else if mtime.After(atime) {
		usetime = mtime
	}
	now := time.Now()
	dontevictime := now.Add(-ctx.config.LRU.DontEvictTime)
	if usetime.After(dontevictime) {
		if glog.V(4) {
			glog.Infof("%s: not evicting (usetime %v, dontevictime %v)", fqn, usetime, dontevictime)
		}
		return nil
	}

	// cleanup after rebalance
	_, _, err = cluster.ResolveFQN(fqn, lctx.bmdowner)
	if err != nil {
		glog.Infof("%s: is misplaced, err: %v", fqn, err)
		fi := &fileInfo{fqn: fqn, size: stat.Size}
		lctx.oldwork = append(lctx.oldwork, fi)
		return nil
	}

	// partial optimization:
	// do nothing if the heap's cursize >= totsize &&
	// the file is more recent then the the heap's newest
	// full optimization (TODO) entails compacting the heap when its cursize >> totsize
	if lctx.cursize >= lctx.totsize && usetime.After(lctx.newest) {
		if glog.V(4) {
			glog.Infof("%s: use-time-after (usetime=%v, newest=%v)", fqn, usetime, lctx.newest)
		}
		return nil
	}
	// push and update the context
	fi := &fileInfo{fqn: fqn, usetime: usetime, size: stat.Size}
	heap.Push(h, fi)
	lctx.cursize += fi.size
	if usetime.After(lctx.newest) {
		lctx.newest = usetime
	}
	return nil
}

func (lctx *lructx) evict() error {
	var (
		fevicted, bevicted int64
		h                  = lctx.heap
	)
	for _, fi := range lctx.oldwork {
		if lctx.targetrunner.IsRebalancing() {
			_, _, err := cluster.ResolveFQN(fi.fqn, lctx.bmdowner)
			// keep a copy of a rebalanced file while rebalance is running
			if spec, _ := cluster.FileSpec(fi.fqn); spec != nil && spec.PermToMove() && err != nil {
				continue
			}
		}
		if err := os.Remove(fi.fqn); err != nil {
			glog.Warningf("LRU: failed to GC %q", fi.fqn)
			continue
		}
		lctx.totsize -= fi.size
		glog.Infof("LRU: GC-ed %q", fi.fqn)
	}
	for h.Len() > 0 && lctx.totsize > 0 {
		fi := heap.Pop(h).(*fileInfo)
		if err := lctx.evictFQN(fi.fqn); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", fi.fqn, err)
			continue
		}
		lctx.totsize -= fi.size
		bevicted += fi.size
		fevicted++
	}
	lctx.statsif.Add(stats.LruEvictSize, bevicted)
	lctx.statsif.Add(stats.LruEvictCount, fevicted)
	return nil
}

// evictFQN evicts a given file
func (lctx *lructx) evictFQN(fqn string) error {
	bucket, objname, err := cluster.ResolveFQN(fqn, lctx.bmdowner)
	if err != nil {
		glog.Errorf("Evicting %q with error: %v", fqn, err)
		if e := os.Remove(fqn); e != nil {
			return fmt.Errorf("nested error: %v and %v", err, e)
		}
		glog.Infof("LRU: removed %q", fqn)
		return nil
	}
	uname := cluster.Uname(bucket, objname)
	lctx.namelocker.Lock(uname, true)
	defer lctx.namelocker.Unlock(uname, true)

	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("LRU: evicted %s/%s", bucket, objname)
	return nil
}

func (lctx *lructx) evictSize() (err error) {
	hwm, lwm := ctx.config.LRU.HighWM, ctx.config.LRU.LowWM
	blocks, bavail, bsize, err := ios.GetFSStats(lctx.bucketdir)
	if err != nil {
		return err
	}
	used := blocks - bavail
	usedpct := used * 100 / blocks
	if glog.V(4) {
		glog.Infof("%s: Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%",
			lctx.bucketdir, blocks, bavail, usedpct, hwm, lwm)
	}
	if usedpct < uint64(hwm) {
		return
	}
	lwmblocks := blocks * uint64(lwm) / 100
	lctx.totsize = int64(used-lwmblocks) * bsize
	return
}

//=======================================================================
//
// fileInfoMinHeap keeps fileInfo sorted by access time with oldest
// on top of the heap.
//
//=======================================================================
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

//
// check whether we have achieved the objective, and warn otherwise
//
func lruCheckResults(availablePaths map[string]*fs.MountpathInfo) {
	rr := getstorstatsrunner()
	rr.Lock()
	rr.UpdateCapacity()
	for _, mpathInfo := range availablePaths {
		fscapacity := rr.Capacity[mpathInfo.Path]
		if fscapacity.Usedpct > ctx.config.LRU.LowWM+1 {
			glog.Warningf("LRU mpath %s: failed to reach lwm %d%% (used %d%%)",
				mpathInfo.Path, ctx.config.LRU.LowWM, fscapacity.Usedpct)
		}
	}
	rr.Unlock()
}
