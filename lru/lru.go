// Package lru provides least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package lru

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction/demand"
)

// The LRU module implements a well-known least-recently-used cache replacement policy.
//
// LRU-driven eviction is based on the two configurable watermarks: config.LRU.LowWM and
// config.LRU.HighWM (section "lru" in the /deploy/dev/local/aisnode_config.sh).
// When and if exceeded, AIStore target will start gradually evicting objects from its
// stable storage: oldest first access-time wise.
//
// LRU is implemented as a so-called extended action (aka x-action, see xaction.go) that gets
// triggered when/if a used local capacity exceeds high watermark (config.LRU.HighWM). LRU then
// runs automatically. In order to reduce its impact on the live workload, LRU throttles itself
// in accordance with the current storage-target's utilization (see xaction_throttle.go).
//
// There's only one API that this module provides to the rest of the code:
//   - runLRU - to initiate a new LRU extended action on the local target
// All other methods are private to this module and are used only internally.

// TODO: extend LRU to remove CTs beyond just []string{fs.WorkfileType, fs.ObjectType}

// LRU defaults/tunables
const (
	minEvictThresh = 10 * cmn.MiB
	capCheckThresh = 256 * cmn.MiB // capacity checking threshold, when exceeded may result in lru throttling
)

type (
	InitLRU struct {
		T                   cluster.Target
		Xaction             *Xaction
		StatsT              stats.Tracker
		GetFSUsedPercentage func(path string) (usedPercentage int64, ok bool)
		GetFSStats          func(path string) (blocks, bavail uint64, bsize int64, err error)
	}

	// minHeap keeps fileInfo sorted by access time with oldest
	// on top of the heap.
	minHeap []*cluster.LOM

	// parent - contains mpath joggers
	lruP struct {
		wg      sync.WaitGroup
		joggers map[string]*lruJ
		ini     InitLRU
	}

	// lruJ represents a single LRU context and a single /jogger/
	// that traverses and evicts a single given mountpath.
	lruJ struct {
		// runtime
		curSize   int64
		totalSize int64 // difference between lowWM size and used size
		newest    time.Time
		heap      *minHeap
		oldWork   []string
		misplaced []*cluster.LOM
		bck       cmn.Bck
		// init-time
		p         *lruP
		ini       *InitLRU
		stopCh    chan struct{}
		joggers   map[string]*lruJ
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		// runtime
		throttle    bool
		allowDelObj bool
	}
	Xaction struct {
		demand.XactDemandBase
		Renewed chan struct{}
	}
)

func Run(ini *InitLRU) {
	var (
		xlru              = ini.Xaction
		providers         = make([]string, 0, len(cmn.Providers))
		config            = cmn.GCO.Get()
		availablePaths, _ = fs.Get()
		num               = len(availablePaths)
		joggers           = make(map[string]*lruJ, num)
		parent            = &lruP{joggers: joggers, ini: *ini}
	)
	glog.Infof("%s: %s started: dont-evict-time %v", ini.T.Snode(), xlru, config.LRU.DontEvictTime)
	if num == 0 {
		glog.Errorln(cmn.NoMountpaths)
		return
	}
	for mpath, mpathInfo := range availablePaths {
		h := make(minHeap, 0, 64)
		joggers[mpath] = &lruJ{
			heap:      &h,
			oldWork:   make([]string, 0, 64),
			misplaced: make([]*cluster.LOM, 0, 64),
			stopCh:    make(chan struct{}, 1),
			mpathInfo: mpathInfo,
			config:    config,
			ini:       &parent.ini,
			p:         parent,
		}
	}
	for provider := range cmn.Providers {
		providers = append(providers, provider) // in random order
	}

repeat:
	xlru.XactDemandBase.IncPending()
	fail := atomic.Bool{}
	for _, j := range joggers {
		parent.wg.Add(1)
		j.joggers = joggers
		go func(j *lruJ) {
			if err := j.jog(providers); err != nil {
				glog.Errorf("%s: exited with err %v", j, err)
				if fail.CAS(false, true) {
					for _, j := range joggers {
						j.stop()
					}
				}
			}
		}(j)
	}
	parent.wg.Wait()

	if fail.Load() {
		xlru.XactDemandBase.Stop()
		return
	}
	// linger for a while and circle back if renewed
	xlru.XactDemandBase.DecPending()
	select {
	case <-xlru.IdleTimer():
		xlru.XactDemandBase.Stop()
		return
	case <-xlru.ChanAbort():
		xlru.XactDemandBase.Stop()
		return
	case <-xlru.Renewed:
		for len(xlru.Renewed) > 0 {
			<-xlru.Renewed
		}
		goto repeat
	}
}

/////////////
// Xaction //
/////////////

func (r *Xaction) IsMountpathXact() bool { return true }
func (r *Xaction) Renew()                { r.Renewed <- struct{}{} }

//////////
// lruJ //
//////////

func (j *lruJ) String() string {
	return fmt.Sprintf("%s: (%s, %s)", j.ini.T.Snode(), j.ini.Xaction, j.mpathInfo)
}
func (j *lruJ) stop() { j.stopCh <- struct{}{} }

func (j *lruJ) jog(providers []string) (err error) {
	defer j.p.wg.Done()

	// first thing first
	if err = j.removeTrash(); err != nil {
		return
	}
	// compute the size (bytes) to free up (and do it after removing the $trash)
	if err = j.evictSize(); err != nil {
		return
	}
	if j.totalSize < minEvictThresh {
		glog.Infof("%s: used cap below threshold, nothing to do", j)
		return
	}
	glog.Infof("%s: freeing-up %s", j, cmn.B2S(j.totalSize, 2))
	for _, provider := range providers { // for each provider (note: ordering is random)
		var (
			bcks []cmn.Bck
			opts = fs.Options{
				Mpath: j.mpathInfo,
				Bck:   cmn.Bck{Provider: provider, Ns: cmn.NsGlobal},
			}
		)
		if bcks, err = fs.AllMpathBcks(&opts); err != nil {
			return
		}
		if len(bcks) == 0 {
			continue
		}
		if len(bcks) > 1 {
			j.sortBsize(bcks)
		}
		bowner := j.ini.T.GetBowner()
		for _, bck := range bcks { // for each bucket under a given provider
			var (
				size int64
				b    = cluster.NewBckEmbed(bck)
			)
			if erb := b.Init(bowner, j.ini.T.Snode()); erb != nil {
				// TODO: add a config option to trash those "buckets"
				glog.Errorf("%s: skipping %s (remove?)", j, b)
				continue
			}
			j.allowDelObj = b.Props.LRU.Enabled && b.Allow(cmn.AccessObjDELETE) == nil
			j.bck = bck
			if size, err = j.jogBck(); err != nil {
				return
			}
			if size < cmn.KiB {
				continue
			}
			// recompute size-to-evict
			if err = j.evictSize(); err != nil {
				return
			}
			if j.totalSize < cmn.KiB {
				return
			}
		}
	}
	return
}

func (j *lruJ) removeTrash() (err error) {
	trashDir := j.mpathInfo.MakePathTrash()
	err = fs.Scanner(trashDir, func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			return os.RemoveAll(fqn)
		}
		if err := j.yieldTerm(); err != nil {
			return err
		}
		return os.Remove(fqn)
	})
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	return
}

func (j *lruJ) jogBck() (size int64, err error) {
	// 1. init per-bucket min-heap (and reuse the slice)
	h := (*j.heap)[:0]
	j.heap = &h
	heap.Init(j.heap)

	// 2. collect
	opts := &fs.Options{
		Mpath:    j.mpathInfo,
		Bck:      j.bck,
		CTs:      []string{fs.WorkfileType, fs.ObjectType},
		Callback: j.walk,
		Sorted:   false,
	}
	if err = fs.Walk(opts); err != nil {
		return
	}
	// 3. evict
	size, err = j.evict()
	return
}

func (j *lruJ) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	if err := j.yieldTerm(); err != nil {
		return err
	}
	h := j.heap
	lom := &cluster.LOM{T: j.ini.T, FQN: fqn}
	err := lom.Init(j.bck, j.config)
	if err != nil {
		return nil
	}
	// workfiles: remove old or do nothing
	if lom.ParsedFQN.ContentType == fs.WorkfileType {
		_, base := filepath.Split(fqn)
		contentResolver := fs.CSM.RegisteredContentTypes[fs.WorkfileType]
		_, old, ok := contentResolver.ParseUniqueFQN(base)
		if ok && old {
			j.oldWork = append(j.oldWork, fqn)
		}
		return nil
	}
	if !j.allowDelObj {
		return nil // ===>
	}
	err = lom.Load(false)
	if err != nil {
		return nil
	}
	dontEvictTime := time.Now().Add(-j.config.LRU.DontEvictTime)
	if lom.Atime().After(dontEvictTime) {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	if !lom.IsHRW() {
		j.misplaced = append(j.misplaced, lom)
		return nil
	}

	// do nothing if the heap's curSize >= totalSize and
	// the file is more recent then the the heap's newest.
	if j.curSize >= j.totalSize && lom.Atime().After(j.newest) {
		return nil
	}
	heap.Push(h, lom)
	j.curSize += lom.Size()
	if lom.Atime().After(j.newest) {
		j.newest = lom.Atime()
	}
	return nil
}

func (j *lruJ) evict() (size int64, err error) {
	var (
		fevicted, bevicted int64
		capCheck           int64
		h                  = j.heap
		xlru               = j.ini.Xaction
	)
	// 1.
	for _, workfqn := range j.oldWork {
		finfo, erw := os.Stat(workfqn)
		if erw == nil {
			if err := cmn.RemoveFile(workfqn); err != nil {
				glog.Warningf("Failed to remove old work %q: %v", workfqn, err)
			} else {
				size += finfo.Size()
			}
		}
	}
	j.oldWork = j.oldWork[:0]
	// 2.
	for _, lom := range j.misplaced {
		if j.ini.T.RebalanceInfo().IsRebalancing {
			continue
		}
		// 2.1: remove misplaced obj
		if err = cmn.RemoveFile(lom.FQN); err != nil {
			glog.Warningf("%s: %v", lom, err)
			continue
		}
		lom.Uncache()
		// 2.2: for mirrored objects: remove extra copies if any
		lom = &cluster.LOM{T: j.ini.T, ObjName: lom.ObjName}
		err = lom.Init(lom.Bck().Bck, lom.Config())
		if err != nil {
			glog.Warningf("%s: %v", lom, err)
		} else if err = lom.Load(false); err != nil {
			glog.Warningf("%s: %v", lom, err)
		} else if err = lom.DelExtraCopies(); err != nil {
			glog.Warningf("%s: %v", lom, err)
		}
		if capCheck, err = j.postRemove(capCheck, lom); err != nil {
			return
		}
	}
	j.misplaced = j.misplaced[:0]
	// 3.
	for h.Len() > 0 && j.totalSize > 0 {
		lom := heap.Pop(h).(*cluster.LOM)
		if j.evictObj(lom) {
			bevicted += lom.Size()
			size += lom.Size()
			fevicted++
			if capCheck, err = j.postRemove(capCheck, lom); err != nil {
				return
			}
		}
	}
	j.ini.StatsT.Add(stats.LruEvictSize, bevicted)
	j.ini.StatsT.Add(stats.LruEvictCount, fevicted)
	xlru.ObjectsAdd(fevicted)
	xlru.BytesAdd(bevicted)
	return
}

func (j *lruJ) postRemove(prev int64, lom *cluster.LOM) (capCheck int64, err error) {
	j.totalSize -= lom.Size()
	capCheck = prev + lom.Size()
	if err = j.yieldTerm(); err != nil {
		return
	}
	if capCheck < capCheckThresh {
		return
	}
	capCheck = 0
	usedPct, ok := j.ini.GetFSUsedPercentage(j.mpathInfo.Path)
	j.throttle = false
	j.config = cmn.GCO.Get()
	now := time.Now()
	if ok && usedPct < j.config.LRU.HighWM {
		if !j.mpathInfo.IsIdle(j.config, now) {
			// throttle self
			ratioCapacity := cmn.Ratio(j.config.LRU.HighWM, j.config.LRU.LowWM, usedPct)
			curr := fs.GetMpathUtil(j.mpathInfo.Path, now)
			ratioUtilization := cmn.Ratio(j.config.Disk.DiskUtilHighWM, j.config.Disk.DiskUtilLowWM, curr)
			if ratioUtilization > ratioCapacity {
				j.throttle = true
				time.Sleep(cmn.ThrottleSleepMax)
			}
		}
	}
	return
}

// remove local copies that "belong" to different LRU joggers; hence, space accounting may be temporarily not precise
func (j *lruJ) evictObj(lom *cluster.LOM) (ok bool) {
	lom.Lock(true)
	if err := lom.Remove(); err == nil {
		ok = true
	} else {
		glog.Errorf("%s: failed to remove, err: %v", lom, err)
	}
	lom.Unlock(true)
	return
}

func (j *lruJ) evictSize() (err error) {
	lwm, hwm := j.config.LRU.LowWM, j.config.LRU.HighWM
	blocks, bavail, bsize, err := j.ini.GetFSStats(j.mpathInfo.Path)
	if err != nil {
		return err
	}
	used := blocks - bavail
	usedPct := used * 100 / blocks
	if usedPct < uint64(hwm) {
		return
	}
	lwmBlocks := blocks * uint64(lwm) / 100
	j.totalSize = int64(used-lwmBlocks) * bsize
	return
}

func (j *lruJ) yieldTerm() error {
	xlru := j.ini.Xaction
	select {
	case <-xlru.ChanAbort():
		return cmn.NewAbortedError(xlru.String())
	case <-j.stopCh:
		return cmn.NewAbortedError(xlru.String())
	default:
		if j.throttle {
			time.Sleep(cmn.ThrottleSleepMin)
		}
		break
	}
	if xlru.Finished() {
		return cmn.NewAbortedError(xlru.String())
	}
	return nil
}

// sort buckets by size
func (j *lruJ) sortBsize(bcks []cmn.Bck) {
	sized := make([]struct {
		b cmn.Bck
		v uint64
	}, len(bcks))
	for i := range bcks {
		path := j.mpathInfo.MakePathCT(bcks[i], fs.ObjectType)
		sized[i].b = bcks[i]
		sized[i].v, _ = ios.GetDirSize(path)
	}
	sort.Slice(sized, func(i, j int) bool {
		return sized[i].v > sized[j].v
	})
	for i := range bcks {
		bcks[i] = sized[i].b
	}
}

//////////////
// min-heap //
//////////////

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Atime().Before(h[j].Atime()) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(*cluster.LOM)) }

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	fi := old[n-1]
	*h = old[0 : n-1]
	return fi
}
