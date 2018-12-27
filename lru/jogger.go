// Package lru provides atime-based least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orhaned workfiles.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package lru

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/ios"
	"github.com/NVIDIA/dfcpub/stats"
)

// contextual LRU "jogger" traverses a given (cloud/ or local/) subdirectory to
// evict or delete selected objects and workfiles

func (lctx *lructx) jog(wg *sync.WaitGroup) {
	defer wg.Done()

	lctx.heap = &fileInfoMinHeap{}
	heap.Init(lctx.heap)
	if err := lctx.evictSize(); err != nil {
		return
	}
	if lctx.totsize < minEvictThresh {
		glog.Infof("%s: below threshold, nothing to do", lctx.bckTypeDir)
		return
	}
	glog.Infof("%s: evicting %s", lctx.bckTypeDir, cmn.B2S(lctx.totsize, 2))

	if err := filepath.Walk(lctx.bckTypeDir, lctx.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", lctx.bckTypeDir, s)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", lctx.bckTypeDir, err)
		}
		return
	}
	if err := lctx.evict(); err != nil {
		glog.Errorf("%s: failed to evict, err: %v", lctx.bckTypeDir, err)
	}
}

func (lctx *lructx) walk(fqn string, osfi os.FileInfo, err error) error {
	var (
		evictOK bool
		isOld   bool
		h       = lctx.heap
	)
	if err != nil {
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	if evictOK, isOld = lctx.ini.CtxResolver.PermToEvict(fqn); !evictOK && !isOld {
		return nil
	}
	_, err = os.Stat(fqn)
	if os.IsNotExist(err) {
		glog.Infof("Warning (race?): %s "+cmn.DoesNotExist, fqn)
		glog.Flush()
		return nil
	}
	if err = lctx.yieldTerm(); err != nil {
		return err
	}

	atime, mtime, stat := ios.GetAmTimes(osfi)
	if isOld {
		fi := &fileInfo{fqn: fqn, size: stat.Size}
		glog.Infof("%s is old", fqn)
		lctx.oldwork = append(lctx.oldwork, fi) // TODO: upper-limit to avoid OOM; see Push as well
		return nil
	}

	// object eviction: access time
	usetime := atime

	atimeResponse := <-lctx.ini.Ratime.Atime(fqn, lctx.atimeRespCh)
	accessTime, ok := atimeResponse.AccessTime, atimeResponse.Ok
	if ok {
		usetime = accessTime
	} else if mtime.After(atime) {
		usetime = mtime
	}
	now := time.Now()
	dontevictime := now.Add(-cmn.GCO.Get().LRU.DontEvictTime)
	if usetime.After(dontevictime) {
		if glog.V(4) {
			glog.Infof("%s: not evicting (usetime %v, dontevictime %v)", fqn, usetime, dontevictime)
		}
		return nil
	}

	// cleanup after rebalance
	_, _, err = cluster.ResolveFQN(fqn, nil, lctx.bislocal)
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

func (lctx *lructx) evict() (err error) {
	var (
		fevicted, bevicted int64
		capCheck           int64
		h                  = lctx.heap
		config             = cmn.GCO.Get()
	)
	for _, fi := range lctx.oldwork {
		if lctx.ini.T.IsRebalancing() {
			_, _, err = cluster.ResolveFQN(fi.fqn, nil, lctx.bislocal)
			// keep a copy of a rebalanced file while rebalance is running
			if movable := lctx.ini.CtxResolver.PermToMove(fi.fqn); movable && err != nil {
				continue
			}
		}
		if err = os.Remove(fi.fqn); err != nil {
			glog.Warningf("LRU: failed to remove %q", fi.fqn)
			continue
		}
		if capCheck, err = lctx.postRemove(config, capCheck, fi.size); err != nil {
			return
		}
		glog.Infof("Removed old %q", fi.fqn)
	}
	for h.Len() > 0 && lctx.totsize > 0 {
		fi := heap.Pop(h).(*fileInfo)
		if err = lctx.evictFQN(fi.fqn); err != nil {
			glog.Errorf("Failed to evict %q, err: %v", fi.fqn, err)
			continue
		}
		bevicted += fi.size
		fevicted++
		if capCheck, err = lctx.postRemove(config, capCheck, fi.size); err != nil {
			return
		}
	}
	lctx.ini.Statsif.Add(stats.LruEvictSize, bevicted)
	lctx.ini.Statsif.Add(stats.LruEvictCount, fevicted)
	return nil
}

func (lctx *lructx) postRemove(config *cmn.Config, capCheck, size int64) (int64, error) {
	lctx.totsize -= size
	capCheck += size
	if err := lctx.yieldTerm(); err != nil {
		return 0, err
	}
	if capCheck >= capCheckInterval {
		capCheck = 0
		usedpct, ok := ios.GetFSUsedPercentage(lctx.bckTypeDir)
		lctx.throttle = false
		if ok && usedpct < config.LRU.HighWM {
			if !lctx.mpathInfo.IsIdle(config) {
				// throttle self
				ratioCapacity := cmn.Ratio(config.LRU.HighWM, config.LRU.LowWM, usedpct)
				_, curr := lctx.mpathInfo.GetIOstats(fs.StatDiskUtil)
				ratioUtilization := cmn.Ratio(config.Xaction.DiskUtilHighWM, config.Xaction.DiskUtilLowWM, int64(curr.Max))
				if ratioUtilization > ratioCapacity {
					lctx.throttle = true
					time.Sleep(throttleTimeOut)
				}
			}
		}
	}
	return capCheck, nil
}

// evictFQN evicts a given file
func (lctx *lructx) evictFQN(fqn string) error {
	parsedFQN, _, err := cluster.ResolveFQN(fqn, nil, lctx.bislocal)
	bucket, objname := parsedFQN.Bucket, parsedFQN.Objname
	if err != nil {
		glog.Errorf("Evicting %q with error: %v", fqn, err)
		if e := os.Remove(fqn); e != nil {
			return fmt.Errorf("nested error: %v and %v", err, e)
		}
		glog.Infof("Removed %q", fqn)
		return nil
	}
	uname := cluster.Uname(bucket, objname)
	lctx.ini.Namelocker.Lock(uname, true)
	defer lctx.ini.Namelocker.Unlock(uname, true)

	if err := os.Remove(fqn); err != nil {
		return err
	}
	glog.Infof("Evicted object %s/%s (%s)", bucket, objname, fqn)
	return nil
}

func (lctx *lructx) evictSize() (err error) {
	config := cmn.GCO.Get()
	hwm, lwm := config.LRU.HighWM, config.LRU.LowWM
	blocks, bavail, bsize, err := ios.GetFSStats(lctx.bckTypeDir)
	if err != nil {
		return err
	}
	used := blocks - bavail
	usedpct := used * 100 / blocks
	if glog.V(4) {
		glog.Infof("%s: Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%",
			lctx.bckTypeDir, blocks, bavail, usedpct, hwm, lwm)
	}
	if usedpct < uint64(hwm) {
		return
	}
	lwmblocks := blocks * uint64(lwm) / 100
	lctx.totsize = int64(used-lwmblocks) * bsize
	return
}

func (lctx *lructx) yieldTerm() error {
	xlru := lctx.ini.Xlru
	select {
	case <-xlru.ChanAbort():
		return fmt.Errorf("%s aborted, exiting", xlru)
	default:
		if lctx.throttle {
			time.Sleep(throttleTimeIn)
		} else {
			runtime.Gosched()
		}
		break
	}
	if xlru.Finished() {
		return fmt.Errorf("%s aborted, exiting", xlru)
	}
	return nil
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
