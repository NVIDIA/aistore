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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
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
	minEvictThresh = 10 * cos.MiB
	capCheckThresh = 256 * cos.MiB // capacity checking threshold, when exceeded may result in lru throttling
)

type (
	InitLRU struct {
		T                   cluster.Target
		Xaction             *Xaction
		StatsT              stats.Tracker
		Force               bool      // Ignore LRU prop when set to be true.
		Buckets             []cmn.Bck // list of buckets to run LRU
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
		newest    int64
		heap      *minHeap
		oldWork   []string
		misplaced []*cluster.LOM
		bck       cmn.Bck
		now       int64
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

	Factory struct {
		xreg.BaseGlobalEntry
		xact *Xaction

		id string
	}

	Xaction struct {
		xaction.XactDemandBase
		Renewed           chan struct{}
		OkRemoveMisplaced func() bool
	}
)

// interface guard
var (
	_ xaction.XactDemand = (*Xaction)(nil)
	_ xreg.GlobalFactory = (*Factory)(nil)
)

func init() {
	xreg.RegGlobXact(&Factory{})
}

func (*Factory) New(args xreg.XactArgs) xreg.GlobalEntry {
	return &Factory{id: args.UUID}
}

func (p *Factory) Start(_ cmn.Bck) error {
	var (
		args        = xaction.Args{ID: p.id, Kind: cmn.ActLRU}
		config      = cmn.GCO.Get()
		totallyIdle = config.Timeout.MaxHostBusy.D()
		likelyIdle  = config.Timeout.MaxKeepalive.D()
	)
	p.xact = &Xaction{
		XactDemandBase: *xaction.NewXDB(args, totallyIdle, likelyIdle),
		Renewed:        make(chan struct{}, 10),
		OkRemoveMisplaced: func() bool {
			g, l := xreg.GetRebMarked(), xreg.GetResilverMarked()
			return !g.Interrupted && !l.Interrupted && g.Xact == nil && l.Xact == nil
		},
	}
	p.xact.InitIdle()
	return nil
}

func (*Factory) Kind() string        { return cmn.ActLRU }
func (p *Factory) Get() cluster.Xact { return p.xact }

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
	glog.Infof("[lru] %s started: dont-evict-time %v", xlru, config.LRU.DontEvictTime)
	if num == 0 {
		glog.Warningf("[lru] no mountpaths to visit")
		xlru.stop()
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

	providers = cmn.Providers.ToSlice()

repeat:
	xlru.XactDemandBase.IncPending()
	fail := atomic.Bool{}
	for _, j := range joggers {
		parent.wg.Add(1)
		j.joggers = joggers
		go func(j *lruJ) {
			var err error
			defer j.p.wg.Done()
			if err = j.removeTrash(); err != nil {
				goto ex
			}
			// compute the size (bytes) to free up (and do it after removing the $trash)
			if err = j.evictSize(); err != nil {
				goto ex
			}
			if j.totalSize < minEvictThresh {
				glog.Infof("[lru] %s: used cap below threshold, nothing to do", j)
				return
			}
			if len(j.ini.Buckets) != 0 {
				glog.Infof("[lru] %s: freeing-up %s", j, cos.B2S(j.totalSize, 2))
				err = j.jogBcks(j.ini.Buckets, j.ini.Force)
			} else {
				err = j.jog(providers)
			}
		ex:
			if err != nil && !os.IsNotExist(err) {
				glog.Errorf("[lru] %s: exited with err %v", j, err)
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
		xlru.stop()
		return
	}
	// linger for a while and circle back if renewed
	xlru.XactDemandBase.DecPending()
	select {
	case <-xlru.IdleTimer():
		xlru.stop()
		return
	case <-xlru.ChanAbort():
		xlru.stop()
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

func (*Xaction) Run()     { debug.Assert(false) }
func (r *Xaction) Renew() { r.Renewed <- struct{}{} }

func (r *Xaction) stop() {
	r.XactDemandBase.Stop()
	r.Finish(nil)
}

func (r *Xaction) Stats() cluster.XactStats {
	baseStats := r.XactDemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: r.Pending() == 0}
	return baseStats
}

//////////
// lruJ //
//////////

func (j *lruJ) String() string {
	return fmt.Sprintf("%s: (%s, %s)", j.ini.T.Snode(), j.ini.Xaction, j.mpathInfo)
}
func (j *lruJ) stop() { j.stopCh <- struct{}{} }

func (j *lruJ) jog(providers []string) (err error) {
	glog.Infof("%s: freeing-up %s", j, cos.B2S(j.totalSize, 2))
	for _, provider := range providers { // for each provider (NOTE: ordering is random)
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

		if err = j.jogBcks(bcks, false); err != nil {
			return
		}
	}
	return
}

func (j *lruJ) jogBcks(bcks []cmn.Bck, force bool) (err error) {
	if len(bcks) == 0 {
		return
	}
	if len(bcks) > 1 {
		j.sortBsize(bcks)
	}

	for _, bck := range bcks { // for each bucket under a given provider
		var size int64
		j.bck = bck
		if j.allowDelObj, err = j.allow(); err != nil {
			// TODO: add a config option to trash those "buckets"
			glog.Errorf("%s: %v - skipping %s", j, err, bck)
			err = nil
			continue
		}
		j.allowDelObj = j.allowDelObj || force
		if size, err = j.jogBck(); err != nil {
			return
		}
		if size < cos.KiB {
			continue
		}
		// recompute size-to-evict
		if err = j.evictSize(); err != nil {
			return
		}
		if j.totalSize < cos.KiB {
			return
		}
	}
	return
}

func (j *lruJ) removeTrash() (err error) {
	trashDir := j.mpathInfo.MakePathTrash()
	err = fs.Scanner(trashDir, func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			if err := os.RemoveAll(fqn); err == nil {
				usedPct, ok := j.ini.GetFSUsedPercentage(j.mpathInfo.Path)
				if ok && usedPct < j.config.LRU.HighWM {
					if err := j._throttle(usedPct); err != nil {
						return err
					}
				}
			} else {
				glog.Errorf("%s: %v", j, err)
			}
		} else if err := os.Remove(fqn); err != nil {
			glog.Errorf("%s: %v", j, err)
		}
		return nil
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
	// TODO: LRU other CTs besides WorkfileType
	opts := &fs.Options{
		Mpath:    j.mpathInfo,
		Bck:      j.bck,
		CTs:      []string{fs.WorkfileType, fs.ObjectType},
		Callback: j.walk,
		Sorted:   false,
	}
	j.now = time.Now().UnixNano()
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
	parsedFQN, _, err := cluster.ResolveFQN(fqn)
	if err != nil {
		return nil
	}
	// workfiles: remove old or do nothing
	if parsedFQN.ContentType == fs.WorkfileType {
		_, base := filepath.Split(fqn)
		contentResolver := fs.CSM.RegisteredContentTypes[fs.WorkfileType]
		_, old, ok := contentResolver.ParseUniqueFQN(base)
		if ok && old {
			j.oldWork = append(j.oldWork, fqn)
		}
		return nil
	}
	lom := &cluster.LOM{ObjName: parsedFQN.ObjName}
	err = lom.Init(j.bck)
	if err != nil {
		return nil
	}
	if !j.allowDelObj {
		return nil // ===>
	}
	err = lom.Load(false /*cache it*/, false /*locked*/)
	if err != nil {
		return nil
	}
	if lom.AtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
		return nil
	}
	if lom.HasCopies() && lom.IsCopy() {
		return nil
	}
	if !lom.IsHRW() {
		j.misplaced = append(j.misplaced, lom)
		return nil
	}

	// do nothing if the heap's curSize >= totalSize and
	// the file is more recent then the the heap's newest.
	if j.curSize >= j.totalSize && lom.AtimeUnix() > j.newest {
		return nil
	}
	heap.Push(h, lom)
	j.curSize += lom.SizeBytes()
	if lom.AtimeUnix() > j.newest {
		j.newest = lom.AtimeUnix()
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
			if err := cos.RemoveFile(workfqn); err != nil {
				glog.Warningf("Failed to remove old work %q: %v", workfqn, err)
			} else {
				size += finfo.Size()
			}
		}
	}
	j.oldWork = j.oldWork[:0]
	// 2.
	f := j.ini.Xaction.OkRemoveMisplaced
	if f == nil || f() {
		for _, lom := range j.misplaced {
			var (
				fqn     = lom.FQN
				removed bool
			)
			lom = &cluster.LOM{ObjName: lom.ObjName} // yes placed
			if lom.Init(j.bck) != nil {
				removed = os.Remove(fqn) == nil
			} else if lom.FromFS() != nil {
				removed = os.Remove(fqn) == nil
			} else {
				removed, _ = lom.DelExtraCopies(fqn)
			}
			if !removed && lom.FQN != fqn {
				removed = os.Remove(fqn) == nil
			}
			if removed {
				if capCheck, err = j.postRemove(capCheck, lom); err != nil {
					return
				}
			}
		}
	}
	j.misplaced = j.misplaced[:0]
	// 3.
	for h.Len() > 0 && j.totalSize > 0 {
		lom := heap.Pop(h).(*cluster.LOM)
		if evictObj(lom) {
			bevicted += lom.SizeBytes(true /*not loaded*/)
			size += lom.SizeBytes(true)
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
	j.totalSize -= lom.SizeBytes(true /*not loaded*/)
	capCheck = prev + lom.SizeBytes(true)
	if err = j.yieldTerm(); err != nil {
		return
	}
	if capCheck < capCheckThresh {
		return
	}
	// init, recompute, and throttle - once per capCheckThresh
	capCheck = 0
	j.throttle = false
	j.allowDelObj, _ = j.allow()
	j.config = cmn.GCO.Get()
	j.now = time.Now().UnixNano()
	usedPct, ok := j.ini.GetFSUsedPercentage(j.mpathInfo.Path)
	if ok && usedPct < j.config.LRU.HighWM {
		err = j._throttle(usedPct)
	}
	return
}

func (j *lruJ) _throttle(usedPct int64) (err error) {
	if j.mpathInfo.IsIdle(j.config) {
		return
	}
	// throttle self
	ratioCapacity := cos.Ratio(j.config.LRU.HighWM, j.config.LRU.LowWM, usedPct)
	curr := fs.GetMpathUtil(j.mpathInfo.Path)
	ratioUtilization := cos.Ratio(j.config.Disk.DiskUtilHighWM, j.config.Disk.DiskUtilLowWM, curr)
	if ratioUtilization > ratioCapacity {
		if usedPct < (j.config.LRU.LowWM+j.config.LRU.HighWM)/2 {
			j.throttle = true
		}
		time.Sleep(cmn.ThrottleMax)
		err = j.yieldTerm()
	}
	return
}

// remove local copies that "belong" to different LRU joggers; hence, space accounting may be temporarily not precise
func evictObj(lom *cluster.LOM) (ok bool) {
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
			time.Sleep(cmn.ThrottleMin)
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

func (j *lruJ) allow() (ok bool, err error) {
	var (
		bowner = j.ini.T.Bowner()
		b      = cluster.NewBckEmbed(j.bck)
	)
	if err = b.Init(bowner); err != nil {
		return
	}
	ok = b.Props.LRU.Enabled && b.Allow(cmn.AccessObjDELETE) == nil
	return
}

//////////////
// min-heap //
//////////////

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].Atime().Before(h[j].Atime()) }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(*cluster.LOM)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	fi := old[n-1]
	*h = old[0 : n-1]
	return fi
}
