// Package lru provides least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package lru

import (
	"container/heap"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

// Contextual LRU "jogger" traverses a given mountpath:
//  1. Initially traverses the trash directory and removes everything there.
//  2. Traverses each bucket and tries to remove old/abandoned objects/files.

func (lctx *lruCtx) jog(wg *sync.WaitGroup, joggers map[string]*lruCtx, errCh chan struct{}) {
	defer wg.Done()
	lctx.bckTypeDir = lctx.mpathInfo.MakePathBck(lctx.bck)
	lctx.joggers = joggers

	// Phase 1: (always) traverse trash directory.
	trashDir := lctx.mpathInfo.MakePathTrash()
	err := fs.Scanner(trashDir, func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			return os.RemoveAll(fqn)
		}
		return os.Remove(fqn)
	})
	if err != nil && !os.IsNotExist(err) {
		return
	}

	// Do not run the rest of the LRU if not enabled.
	if !lctx.config.LRU.Enabled {
		return
	}

	// Calculate the size that must be evicted. We do it after evicting trash
	// directory so maybe we don't need to walk the mountpath at all.
	if err := lctx.evictSize(); err != nil {
		return
	}
	if lctx.totalSize < minEvictThresh {
		glog.Infof("%s: below threshold, nothing to do", lctx.mpathInfo)
		return
	}

	lctx.heap = &fileInfoMinHeap{}
	heap.Init(lctx.heap)
	glog.Infof("%s: evicting %s", lctx.mpathInfo, cmn.B2S(lctx.totalSize, 2))

	// Phase 2: collect objects.
	opts := &fs.Options{
		Mpath: lctx.mpathInfo,
		Bck:   lctx.bck,
		CTs:   []string{fs.WorkfileType, fs.ObjectType},

		Callback: lctx.walk,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		if errors.As(err, &cmn.AbortedError{}) {
			glog.Infof("%s: stopping traversal: %v", lctx.bckTypeDir, err)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", lctx.bckTypeDir, err)
		}
		return
	}

	// Phase 3: evict collected objects.
	if err := lctx.evict(); err != nil {
		glog.Errorf("%s: err: %v", lctx.bckTypeDir, err)
	}
	if lctx.aborted {
		errCh <- struct{}{}
	}
}

// TODO: `mirroring` and `ec` is not correctly taken into account when
//  doing LRU. Also, in some places, we can entirely skip walking instead of just
//  skipping single FQN (eg. AllowDELETE checks).
func (lctx *lruCtx) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	if err := lctx.yieldTerm(); err != nil {
		return err
	}
	var (
		h = lctx.heap
	)

	lom := &cluster.LOM{T: lctx.ini.T, FQN: fqn}
	err := lom.Init(lctx.bck, lctx.config)
	if err != nil {
		return nil
	}

	// workfiles: remove old or do nothing
	if lom.ParsedFQN.ContentType == fs.WorkfileType {
		_, base := filepath.Split(fqn)
		contentResolver := fs.CSM.RegisteredContentTypes[fs.WorkfileType]
		_, old, ok := contentResolver.ParseUniqueFQN(base)
		if ok && old {
			lctx.oldWork = append(lctx.oldWork, fqn)
		}
		return nil
	}
	// TODO: extend LRU for other content types
	cmn.Assert(lom.ParsedFQN.ContentType == fs.ObjectType)

	err = lom.Load(false)
	if err != nil {
		return nil
	}

	dontEvictTime := time.Now().Add(-lctx.config.LRU.DontEvictTime)
	if lom.Atime().After(dontEvictTime) {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	if !lom.IsHRW() {
		lctx.misplaced = append(lctx.misplaced, lom)
		return nil
	}
	if err = lom.Bck().Allow(cmn.AccessObjDELETE); err != nil {
		return nil
	}

	// Partial optimization: do nothing if the heap's curSize >= totalSize and
	// the file is more recent then the the heap's newest.
	// Full optimization: (TODO) entails compacting the heap when its cursize >> totsize
	if lctx.curSize >= lctx.totalSize && lom.Atime().After(lctx.newest) {
		// TODO: should we abort walking?
		return nil
	}

	// Push LOM into the heap and update the context
	if glog.V(4) {
		glog.Infof("old-obj: %s, fqn=%s", lom, fqn)
	}
	heap.Push(h, lom)
	lctx.curSize += lom.Size()
	if lom.Atime().After(lctx.newest) {
		lctx.newest = lom.Atime()
	}
	return nil
}

func (lctx *lruCtx) evict() (err error) {
	var (
		fevicted, bevicted int64
		capCheck           int64
		h                  = lctx.heap
	)
	// 1.
	for _, workfqn := range lctx.oldWork {
		if err = cmn.RemoveFile(workfqn); err != nil {
			glog.Warningf("Failed to remove old work %q: %v", workfqn, err)
		}
	}
	lctx.oldWork = lctx.oldWork[:0]
	// 2.
	for _, lom := range lctx.misplaced {
		if lctx.ini.T.RebalanceInfo().IsRebalancing {
			continue
		}
		// 2.1: remove misplaced obj
		if err = cmn.RemoveFile(lom.FQN); err != nil {
			glog.Warningf("%s: %v", lom, err)
			continue
		}
		lom.Uncache()
		// 2.2: for mirrored objects: remove extra copies if any
		lom = &cluster.LOM{T: lctx.ini.T, ObjName: lom.ObjName}
		err = lom.Init(lom.Bck().Bck, lom.Config())
		if err != nil {
			glog.Warningf("%s: %v", lom, err)
		} else if err = lom.Load(false); err != nil {
			glog.Warningf("%s: %v", lom, err)
		} else if err = lom.DelExtraCopies(); err != nil {
			glog.Warningf("%s: %v", lom, err)
		}
		if capCheck, err = lctx.postRemove(capCheck, lom); err != nil {
			return
		}
	}
	lctx.misplaced = lctx.misplaced[:0]
	// 3.
	for h.Len() > 0 && lctx.totalSize > 0 {
		lom := heap.Pop(h).(*cluster.LOM)
		if lctx.evictObj(lom) {
			bevicted += lom.Size()
			fevicted++
			if capCheck, err = lctx.postRemove(capCheck, lom); err != nil {
				return
			}
		}
	}
	lctx.ini.StatsT.Add(stats.LruEvictSize, bevicted)
	lctx.ini.StatsT.Add(stats.LruEvictCount, fevicted)
	lctx.ini.Xaction.ObjectsAdd(fevicted)
	lctx.ini.Xaction.BytesAdd(bevicted)
	return nil
}

func (lctx *lruCtx) postRemove(capCheck int64, lom *cluster.LOM) (int64, error) {
	lctx.totalSize -= lom.Size()
	capCheck += lom.Size()
	if err := lctx.yieldTerm(); err != nil {
		return 0, err
	}
	if capCheck >= capCheckThresh {
		capCheck = 0
		usedPct, ok := lctx.ini.GetFSUsedPercentage(lctx.bckTypeDir)
		lctx.throttle = false
		lctx.config = cmn.GCO.Get()
		now := time.Now()
		if ok && usedPct < lctx.config.LRU.HighWM {
			if !lctx.mpathInfo.IsIdle(lctx.config, now) {
				// throttle self
				ratioCapacity := cmn.Ratio(lctx.config.LRU.HighWM, lctx.config.LRU.LowWM, usedPct)
				curr := fs.GetMpathUtil(lctx.mpathInfo.Path, now)
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
func (lctx *lruCtx) evictObj(lom *cluster.LOM) (ok bool) {
	lom.Lock(true)
	if err := lom.Remove(); err == nil {
		ok = true
	} else {
		glog.Errorf("%s: failed to remove, err: %v", lom, err)
	}
	lom.Unlock(true)
	return
}

func (lctx *lruCtx) evictSize() (err error) {
	lwm, hwm := lctx.config.LRU.LowWM, lctx.config.LRU.HighWM
	blocks, bavail, bsize, err := lctx.ini.GetFSStats(lctx.bckTypeDir)
	if err != nil {
		return err
	}
	used := blocks - bavail
	usedPct := used * 100 / blocks
	if glog.V(4) {
		glog.Infof(
			"%s: blocks: %d, bavail: %d, usedPct: %d%%, lwm: %d%%, hwm: %d%%",
			lctx.mpathInfo, blocks, bavail, usedPct, lwm, hwm,
		)
	}
	if usedPct < uint64(hwm) {
		return
	}
	lwmBlocks := blocks * uint64(lwm) / 100
	lctx.totalSize = int64(used-lwmBlocks) * bsize
	return
}

func (lctx *lruCtx) yieldTerm() error {
	xlru := lctx.ini.Xaction
	select {
	case <-xlru.ChanAbort():
		stopAll(lctx.joggers, lctx.mpathInfo.Path)
		lctx.aborted = true
		return cmn.NewAbortedError(xlru.String())
	case <-lctx.stopCh:
		lctx.aborted = true
		return cmn.NewAbortedError(xlru.String())
	default:
		if lctx.throttle {
			time.Sleep(cmn.ThrottleSleepMin)
		} else {
			runtime.Gosched()
		}
		break
	}
	if xlru.Finished() {
		return cmn.NewAbortedError(xlru.String())
	}
	return nil
}

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
