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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

// contextual LRU "jogger" traverses a given (cloud/ or local/) subdirectory to
// evict or delete selected objects and workfiles

func (lctx *lructx) jog(wg *sync.WaitGroup, joggers map[string]*lructx, errCh chan struct{}) {
	defer wg.Done()
	lctx.bckTypeDir = lctx.mpathInfo.MakePath(lctx.contentType, lctx.bckIsLocal)
	if err := lctx.evictSize(); err != nil {
		return
	}
	if lctx.totsize < minEvictThresh {
		glog.Infof("%s: below threshold, nothing to do", lctx.mpathInfo)
		return
	}
	lctx.joggers = joggers
	now := time.Now()

	lctx.dontevictime = now.Add(-lctx.config.LRU.DontEvictTime)
	lctx.heap = &fileInfoMinHeap{}
	heap.Init(lctx.heap)
	glog.Infof("%s: evicting %s", lctx.mpathInfo, cmn.B2S(lctx.totsize, 2))
	// phase 1: collect
	if err := filepath.Walk(lctx.bckTypeDir, lctx.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", lctx.bckTypeDir, s)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", lctx.bckTypeDir, err)
		}
		return
	}
	// phase 2: evict
	if err := lctx.evict(); err != nil {
		glog.Errorf("%s: err: %v", lctx.bckTypeDir, err)
	}
	if lctx.aborted {
		errCh <- struct{}{}
	}
}

func (lctx *lructx) walk(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if errstr := cmn.PathWalkErr(err); errstr != "" {
			glog.Errorf(errstr)
			return err
		}
		return nil
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	if err = lctx.yieldTerm(); err != nil {
		return err
	}
	var (
		h   = lctx.heap
		lom = &cluster.LOM{T: lctx.ini.T, FQN: fqn}
	)
	if errstr := lom.Fill("", cluster.LomFstat|cluster.LomAtime, lctx.config); errstr != "" || !lom.Exists() {
		if glog.V(4) {
			glog.Infof("Warning: %s", errstr)
		}
		return nil
	}
	// workfiles: evict old or do nothing
	if lctx.contentType == fs.WorkfileType {
		_, base := filepath.Split(fqn)
		_, old, ok := lctx.contentResolver.ParseUniqueFQN(base)
		if ok && old {
			fi := &fileInfo{fqn: fqn, lom: lom, old: old}
			if glog.V(4) {
				glog.Infof("old-work: %s, fqn=%s", lom, fqn)
			}
			lctx.oldwork = append(lctx.oldwork, fi) // TODO: upper-limit to avoid OOM; see Push as well
		}
		return nil
	}
	// objects
	cmn.Assert(lctx.contentType == fs.ObjectType) // see also lrumain.go
	if lom.Atime.After(lctx.dontevictime) {
		if glog.V(4) {
			glog.Infof("dont-evict: %s(%v > %v)", lom, lom.Atime, lctx.dontevictime)
		}
		return nil
	}

	// includes post-rebalancing cleanup
	if lom.Misplaced() {
		glog.Infof("misplaced: %s, fqn=%s", lom, fqn)
		fi := &fileInfo{fqn: fqn, lom: lom}
		lctx.oldwork = append(lctx.oldwork, fi)
		return nil
	}

	// partial optimization:
	// do nothing if the heap's cursize >= totsize &&
	// the file is more recent then the the heap's newest
	// full optimization (TODO) entails compacting the heap when its cursize >> totsize
	if lctx.cursize >= lctx.totsize && lom.Atime.After(lctx.newest) {
		return nil
	}
	// push and update the context
	if glog.V(4) {
		glog.Infof("old-obj: %s, fqn=%s", lom, fqn)
	}
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
			if !os.IsNotExist(err) {
				glog.Warningf("Failed to remove old %q: %v", fi.fqn, err)
			}
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
	if capCheck >= capCheckThresh {
		capCheck = 0
		usedpct, ok := lctx.ini.GetFSUsedPercentage(lctx.bckTypeDir)
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
					time.Sleep(cmn.ThrottleSleepMax)
				}
			}
		}
	}
	return capCheck, nil
}

func (lctx *lructx) evictObj(fi *fileInfo) (ok bool) {
	lctx.ini.Namelocker.Lock(fi.lom.Uname, true)
	// local replica must be go with the object; the replica, however, is
	// located in a different local FS and belongs, therefore, to a different LRU jogger
	// (hence, precise size accounting TODO)
	if errstr := fi.lom.DelAllCopies(); errstr != "" {
		glog.Warningf("remove(%s=>%s): %s", fi.lom, fi.lom.CopyFQN, errstr)
	}
	if err := os.Remove(fi.lom.FQN); err == nil {
		glog.Infof("Evicted %s", fi.lom)
		ok = true
	} else if os.IsNotExist(err) {
		ok = true
	} else {
		glog.Errorf("Failed to evict %s, err: %v", fi.lom, err)
	}
	lctx.ini.Namelocker.Unlock(fi.lom.Uname, true)
	return
}

func (lctx *lructx) evictSize() (err error) {
	hwm, lwm := lctx.config.LRU.HighWM, lctx.config.LRU.LowWM
	blocks, bavail, bsize, err := lctx.ini.GetFSStats(lctx.bckTypeDir)
	if err != nil {
		return err
	}
	used := blocks - bavail
	usedpct := used * 100 / blocks
	if glog.V(4) {
		glog.Infof("%s: Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%", lctx.mpathInfo, blocks, bavail, usedpct, hwm, lwm)
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
		stopAll(lctx.joggers, lctx.mpathInfo.Path)
		lctx.aborted = true
		return fmt.Errorf("%s aborted, exiting", xlru)
	case <-lctx.stopCh:
		lctx.aborted = true
		return fmt.Errorf("%s aborted, exiting", xlru)
	default:
		if lctx.throttle {
			time.Sleep(cmn.ThrottleSleepMin)
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
