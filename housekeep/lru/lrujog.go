// Package lru provides least recently used cache replacement policy for stored objects
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
	lctx.bckTypeDir = lctx.mpathInfo.MakePath(lctx.contentType, lctx.provider)
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
	opts := &fs.Options{
		Callback: lctx.walk,
		Sorted:   false,
	}
	if err := fs.Walk(lctx.bckTypeDir, opts); err != nil {
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

func (lctx *lructx) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	if err := lctx.yieldTerm(); err != nil {
		return err
	}
	var (
		h = lctx.heap
	)
	// workfiles: remove old or do nothing
	if lctx.contentType == fs.WorkfileType {
		_, base := filepath.Split(fqn)
		_, old, ok := lctx.contentResolver.ParseUniqueFQN(base)
		if ok && old {
			lctx.oldwork = append(lctx.oldwork, fqn)
		}
		return nil
	}
	lom := &cluster.LOM{T: lctx.ini.T, FQN: fqn}
	err := lom.Init("", lctx.provider, lctx.config)
	if err != nil {
		return nil
	}
	// LRUEnabled is set by lom.Init(), no need to make FS call in Load if not enabled
	if !lom.LRUEnabled() {
		return nil
	}
	err = lom.Load(false)
	if err != nil {
		return nil
	}

	// objects
	cmn.Assert(lctx.contentType == fs.ObjectType) // see also lrumain.go
	if lom.Atime().After(lctx.dontevictime) {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	// includes post-rebalancing cleanup
	if lom.Misplaced() {
		lctx.misplaced = append(lctx.misplaced, lom)
		return nil
	}
	// real objects
	if err = lom.AllowDELETE(); err != nil {
		return nil
	}
	// partial optimization:
	// do nothing if the heap's cursize >= totsize &&
	// the file is more recent then the the heap's newest
	// full optimization (TODO) entails compacting the heap when its cursize >> totsize
	if lctx.cursize >= lctx.totsize && lom.Atime().After(lctx.newest) {
		return nil
	}
	// push and update the context
	if glog.V(4) {
		glog.Infof("old-obj: %s, fqn=%s", lom, fqn)
	}
	heap.Push(h, lom)
	lctx.cursize += lom.Size()
	if lom.Atime().After(lctx.newest) {
		lctx.newest = lom.Atime()
	}
	return nil
}

func (lctx *lructx) evict() (err error) {
	var (
		fevicted, bevicted int64
		capCheck           int64
		h                  = lctx.heap
	)
	// 1.
	for _, workfqn := range lctx.oldwork {
		if err = os.Remove(workfqn); err != nil {
			if !os.IsNotExist(err) {
				glog.Warningf("Failed to remove old work %q: %v", workfqn, err)
			}
		}
	}
	lctx.oldwork = lctx.oldwork[:0]
	// 2.
	for _, lom := range lctx.misplaced {
		if lctx.ini.T.RebalanceInfo().IsRebalancing {
			continue
		}
		// 2.1: remove misplaced obj
		if err = os.Remove(lom.FQN); err != nil {
			if !os.IsNotExist(err) {
				glog.Warningf("%s: %v", lom, err)
			}
			continue
		}
		lom.Uncache()
		// 2.2: for mirrored objects: remove extra copies if any
		prov := cmn.ProviderFromBool(lom.IsAIS())
		lom = &cluster.LOM{T: lctx.ini.T, Objname: lom.Objname}
		err = lom.Init(lom.Bucket(), prov, lom.Config())
		if err != nil {
			glog.Warningf("%s: %v", lom, err)
		} else if err = lom.Load(false); err != nil {
			glog.Warningf("%s: %v", lom, err)
		} else if _, err = lom.DelExtraCopies(); err != nil {
			glog.Warningf("%s: %v", lom, err)
		}
		if capCheck, err = lctx.postRemove(capCheck, lom); err != nil {
			return
		}
	}
	lctx.misplaced = lctx.misplaced[:0]
	// 3.
	for h.Len() > 0 && lctx.totsize > 0 {
		lom := heap.Pop(h).(*cluster.LOM)
		if lctx.evictObj(lom) {
			bevicted += lom.Size()
			fevicted++
			if capCheck, err = lctx.postRemove(capCheck, lom); err != nil {
				return
			}
		}
	}
	lctx.ini.Statsif.Add(stats.LruEvictSize, bevicted)
	lctx.ini.Statsif.Add(stats.LruEvictCount, fevicted)
	lctx.ini.Xlru.ObjectsAdd(fevicted)
	lctx.ini.Xlru.BytesAdd(bevicted)
	return nil
}

func (lctx *lructx) postRemove(capCheck int64, lom *cluster.LOM) (int64, error) {
	lctx.totsize -= lom.Size()
	capCheck += lom.Size()
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
			if !lctx.mpathInfo.IsIdle(lctx.config, now) {
				// throttle self
				ratioCapacity := cmn.Ratio(lctx.config.LRU.HighWM, lctx.config.LRU.LowWM, usedpct)
				curr := fs.Mountpaths.GetMpathUtil(lctx.mpathInfo.Path, now)
				ratioUtilization := cmn.Ratio(lctx.config.Disk.DiskUtilHighWM, lctx.config.Disk.DiskUtilLowWM, curr)
				if ratioUtilization > ratioCapacity {
					lctx.throttle = true
					time.Sleep(cmn.ThrottleSleepMax)
				}
			}
		}
	}
	return capCheck, nil
}

// remove local copies that "belong" to different LRU joggers; hence, space accounting may be temporarily not precise
func (lctx *lructx) evictObj(lom *cluster.LOM) (ok bool) {
	lom.Lock(true)
	if err := lom.Remove(); err == nil {
		ok = true
	} else {
		glog.Errorf("%s: failed to remove, err: %v", lom, err)
	}
	lom.Unlock(true)
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
	return h[i].Atime().Before(h[j].Atime())
}

func (h fileInfoMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *fileInfoMinHeap) Push(x interface{}) {
	*h = append(*h, x.(*cluster.LOM))
}

func (h *fileInfoMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	fi := old[n-1]
	*h = old[0 : n-1]
	return fi
}
