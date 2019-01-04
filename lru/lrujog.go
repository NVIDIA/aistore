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
	now := time.Now()
	lctx.dontevictime = now.Add(-lctx.config.LRU.DontEvictTime)

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
	var h = lctx.heap
	if err != nil {
		glog.Errorf("invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	if err = lctx.yieldTerm(); err != nil {
		return err
	}
	lom := &cluster.LOM{T: lctx.ini.T, Fqn: fqn}
	if errstr := lom.Fill(cluster.LomFstat|cluster.LomAtime, lctx.config); errstr != "" || lom.Doesnotexist {
		if glog.V(4) {
			glog.Infof("Warning: %s", errstr)
		}
		return nil
	}
	if lctx.contentType == fs.WorkfileType {
		_, base := filepath.Split(fqn)
		_, old, ok := lctx.contentResolver.ParseUniqueFQN(base)
		if ok && old {
			fi := &fileInfo{fqn: fqn, lom: lom, old: old}
			glog.Infof("%q is old", fqn)
			lctx.oldwork = append(lctx.oldwork, fi) // TODO: upper-limit to avoid OOM; see Push as well
		}
		return nil
	}

	cmn.Assert(lctx.contentType == fs.ObjectType) // see also lrumain.go
	if lom.Atime.After(lctx.dontevictime) {
		if glog.V(4) {
			glog.Infof("%q: not evicting (usetime %v, dontevictime %v)", fqn, lom.Atime, lctx.dontevictime)
		}
		return nil
	}

	// includes post-rebalancing cleanup
	if lom.Misplaced {
		glog.Infof("%q: is misplaced, err: %v", fqn, err)
		fi := &fileInfo{fqn: fqn, lom: lom}
		lctx.oldwork = append(lctx.oldwork, fi)
		return nil
	}

	// partial optimization:
	// do nothing if the heap's cursize >= totsize &&
	// the file is more recent then the the heap's newest
	// full optimization (TODO) entails compacting the heap when its cursize >> totsize
	if lctx.cursize >= lctx.totsize && lom.Atime.After(lctx.newest) {
		if glog.V(4) {
			glog.Infof("%q: use-time-after (usetime=%v, newest=%v)", fqn, lom.Atime, lctx.newest)
		}
		return nil
	}
	// push and update the context
	fi := &fileInfo{fqn: fqn, lom: lom}
	heap.Push(h, fi)
	lctx.cursize += fi.lom.Size
	if lom.Atime.After(lctx.newest) {
		lctx.newest = lom.Atime
	}
	return nil
}

func (lctx *lructx) evict() (err error) {
	var (
		fevicted, bevicted int64
		capCheck           int64
		h                  = lctx.heap
	)
	for _, fi := range lctx.oldwork {
		if !fi.old && lctx.ini.T.IsRebalancing() {
			continue
		}
		if err = os.Remove(fi.fqn); err != nil {
			glog.Warningf("Failed to remove %q", fi.fqn)
			continue
		}
		if capCheck, err = lctx.postRemove(capCheck, fi); err != nil {
			return
		}
		glog.Infof("Removed old %q", fi.fqn)
	}
	for h.Len() > 0 && lctx.totsize > 0 {
		fi := heap.Pop(h).(*fileInfo)
		if lctx.evictObj(fi) {
			bevicted += fi.lom.Size
			fevicted++
			if capCheck, err = lctx.postRemove(capCheck, fi); err != nil {
				return
			}
		}
	}
	lctx.ini.Statsif.Add(stats.LruEvictSize, bevicted)
	lctx.ini.Statsif.Add(stats.LruEvictCount, fevicted)
	return nil
}

func (lctx *lructx) postRemove(capCheck int64, fi *fileInfo) (int64, error) {
	lctx.totsize -= fi.lom.Size
	capCheck += fi.lom.Size
	if err := lctx.yieldTerm(); err != nil {
		return 0, err
	}
	if capCheck >= capCheckInterval {
		capCheck = 0
		usedpct, ok := ios.GetFSUsedPercentage(lctx.bckTypeDir)
		lctx.throttle = false
		lctx.config = cmn.GCO.Get()
		now := time.Now()
		lctx.dontevictime = now.Add(-lctx.config.LRU.DontEvictTime)
		if ok && usedpct < lctx.config.LRU.HighWM {
			if !lctx.mpathInfo.IsIdle(lctx.config) {
				// throttle self
				ratioCapacity := cmn.Ratio(lctx.config.LRU.HighWM, lctx.config.LRU.LowWM, usedpct)
				_, curr := lctx.mpathInfo.GetIOstats(fs.StatDiskUtil)
				ratioUtilization := cmn.Ratio(lctx.config.Xaction.DiskUtilHighWM, lctx.config.Xaction.DiskUtilLowWM, int64(curr.Max))
				if ratioUtilization > ratioCapacity {
					lctx.throttle = true
					time.Sleep(throttleTimeOut)
				}
			}
		}
	}
	return capCheck, nil
}

func (lctx *lructx) evictObj(fi *fileInfo) (ok bool) {
	if fi.lom.Misplaced {
		if err := os.Remove(fi.lom.Fqn); err != nil {
			glog.Errorln(err)
			return
		}
		glog.Infof("Removed misplaced object %s (%s)", fi.lom, fi.lom.Fqn)
		return
	}
	lctx.ini.Namelocker.Lock(fi.lom.Uname, true)

	if err := os.Remove(fi.lom.Fqn); err != nil {
		glog.Errorf("Failed to evict %s (%s), err: %v", fi.lom, fi.lom.Fqn, err)
	} else {
		ok = true
		glog.Infof("Evicted %s (%s)", fi.lom, fi.lom.Fqn)
	}
	lctx.ini.Namelocker.Unlock(fi.lom.Uname, true)
	return
}

func (lctx *lructx) evictSize() (err error) {
	hwm, lwm := lctx.config.LRU.HighWM, lctx.config.LRU.LowWM
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
	li := h[i].lom
	lj := h[j].lom
	return li.Atime.Before(lj.Atime)
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
