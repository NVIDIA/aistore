// Package space provides storage cleanup and eviction functionality (the latter based on the
// least recently used cache replacement). It also serves as a built-in garbage-collection
// mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package space

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// LRU-driven eviction is based on configurable watermarks: config.Space.LowWM and
// config.Space.HighWM (section "space" in the cluster config).
//
// When and if exceeded, AIS target will start gradually evicting objects from its
// stable storage: oldest first access-time wise.
//
// LRU is implemented as eXtended Action (xaction, see xact/README.md) that gets
// triggered when/if a used local capacity exceeds high watermark (config.Space.HighWM). LRU then
// runs automatically. In order to reduce its impact on the live workload, LRU throttles itself
// in accordance with the current storage-target's utilization (see xaction_throttle.go).
//
// There's only one API that this module provides to the rest of the code:
//   - runLRU - to initiate a new LRU extended action on the local target
// All other methods are private to this module and are used only internally.

// tunables
const (
	minEvictThresh = 10 * cos.MiB  // to run or not to run
	capCheckThresh = 256 * cos.MiB // capacity checking threshold (in re: periodic throttle)
)

type (
	IniLRU struct {
		Xaction             *XactLRU
		Config              *cmn.Config
		StatsT              stats.Tracker
		Buckets             []cmn.Bck // list of buckets to run LRU
		GetFSUsedPercentage func(path string) (usedPercentage int64, ok bool)
		GetFSStats          func(path string) (blocks, bavail uint64, bsize int64, err error)
		WG                  *sync.WaitGroup
		Force               bool // Ignore LRU prop when set to be true.
	}
	XactLRU struct {
		xact.Base
	}
)

// private
type (
	// minHeap keeps fileInfo sorted by access time with oldest on top of the heap.
	minHeap []*core.LOM

	// parent (contains mpath joggers)
	lruP struct {
		wg      sync.WaitGroup
		joggers map[string]*lruJ
		ini     IniLRU
	}

	// lruJ represents a single LRU context and a single /jogger/
	// that traverses and evicts a single given mountpath.
	lruJ struct {
		// runtime
		curSize   int64
		totalSize int64 // difference between lowWM size and used size
		newest    int64
		heap      *minHeap
		bck       cmn.Bck
		now       int64
		// init-time
		p       *lruP
		ini     *IniLRU
		stopCh  chan struct{}
		joggers map[string]*lruJ
		mi      *fs.Mountpath
		config  *cmn.Config
		// runtime
		throttle    bool
		allowDelObj bool
	}
	lruFactory struct {
		xreg.RenewBase
		xctn *XactLRU
	}
	TestFactory = lruFactory // unit tests only
)

// interface guard
var (
	_ xreg.Renewable = (*lruFactory)(nil)
	_ core.Xact      = (*XactLRU)(nil)
)

////////////////
// lruFactory //
////////////////

func (*lruFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &lruFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *lruFactory) Start() error {
	p.xctn = &XactLRU{}
	p.xctn.InitBase(p.UUID(), apc.ActLRU, nil)
	return nil
}

func (*lruFactory) Kind() string     { return apc.ActLRU }
func (p *lruFactory) Get() core.Xact { return p.xctn }

func (*lruFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	return xreg.WprUse, cmn.NewErrXactUsePrev(prevEntry.Get().String())
}

func RunLRU(ini *IniLRU) {
	var (
		xlru           = ini.Xaction
		config         = cmn.GCO.Get()
		availablePaths = fs.GetAvail()
		num            = len(availablePaths)
		joggers        = make(map[string]*lruJ, num)
		parent         = &lruP{joggers: joggers, ini: *ini}
	)
	defer func() {
		if ini.WG != nil {
			ini.WG.Done()
		}
	}()
	if num == 0 {
		nlog.Warningln(cmn.ErrNoMountpaths)
		xlru.AddErr(cmn.ErrNoMountpaths)
		xlru.Finish()
		return
	}
	for mpath, mi := range availablePaths {
		h := make(minHeap, 0, 64)
		joggers[mpath] = &lruJ{
			heap:   &h,
			stopCh: make(chan struct{}, 1),
			mi:     mi,
			config: config,
			ini:    &parent.ini,
			p:      parent,
		}
	}
	providers := apc.Providers.ToSlice()

	for _, j := range joggers {
		parent.wg.Add(1)
		j.joggers = joggers
		go j.run(providers)
	}
	cs := fs.Cap()
	nlog.Infof("%s started, dont-evict-time %v, %s", xlru, config.LRU.DontEvictTime, cs.String())
	if ini.WG != nil {
		ini.WG.Done()
		ini.WG = nil
	}
	parent.wg.Wait()

	for _, j := range joggers {
		j.stop()
	}
	xlru.Finish()
	cs = fs.Cap()
	nlog.Infof("%s finished, %s", xlru, cs.String())
}

func (*XactLRU) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *XactLRU) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}

//////////////////////
// mountpath jogger //
//////////////////////

func (j *lruJ) String() string {
	return fmt.Sprintf("%s: jog-%s", j.ini.Xaction, j.mi)
}

func (j *lruJ) stop() { j.stopCh <- struct{}{} }

func (j *lruJ) run(providers []string) {
	var err error
	defer j.p.wg.Done()
	// compute the size (bytes) to free up
	if err = j.evictSize(); err != nil {
		goto ex
	}
	if j.totalSize < minEvictThresh {
		nlog.Infof("%s: used cap below threshold, nothing to do", j)
		return
	}
	if len(j.ini.Buckets) != 0 {
		nlog.Infof("%s: freeing-up %s", j, cos.ToSizeIEC(j.totalSize, 2))
		err = j.jogBcks(j.ini.Buckets, j.ini.Force)
	} else {
		err = j.jog(providers)
	}
ex:
	if err == nil || cmn.IsErrBucketNought(err) || cmn.IsErrObjNought(err) {
		return
	}
	nlog.Errorf("%s: exited with err %v", j, err)
}

func (j *lruJ) jog(providers []string) (err error) {
	nlog.Infof("%s: freeing-up %s", j, cos.ToSizeIEC(j.totalSize, 2))
	for _, provider := range providers { // for each provider (NOTE: ordering is random)
		var (
			bcks []cmn.Bck
			opts = fs.WalkOpts{
				Mi:  j.mi,
				Bck: cmn.Bck{Provider: provider, Ns: cmn.NsGlobal},
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
			nlog.Errorf("%s: %v - skipping %s (Hint: run 'ais storage cleanup' to cleanup)", j, err, bck)
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

func (j *lruJ) jogBck() (size int64, err error) {
	// 1. init per-bucket min-heap (and reuse the slice)
	h := (*j.heap)[:0]
	j.heap = &h
	heap.Init(j.heap)

	// 2. collect
	opts := &fs.WalkOpts{
		Mi:       j.mi,
		Bck:      j.bck,
		CTs:      []string{fs.ObjectType},
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

func (j *lruJ) visitLOM(parsedFQN *fs.ParsedFQN) {
	if !j.allowDelObj {
		return
	}
	lom := core.AllocLOM(parsedFQN.ObjName)
	if pushed := j._visit(lom); !pushed {
		core.FreeLOM(lom)
	}
}

func (j *lruJ) _visit(lom *core.LOM) (pushed bool) {
	if err := lom.InitBck(&j.bck); err != nil {
		return
	}
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		return
	}
	if lom.AtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
		return
	}
	if lom.HasCopies() && lom.IsCopy() {
		return
	}
	// do nothing if the heap's curSize >= totalSize and
	// the file is more recent then the the heap's newest.
	if j.curSize >= j.totalSize && lom.AtimeUnix() > j.newest {
		return
	}
	heap.Push(j.heap, lom)
	j.curSize += lom.SizeBytes()
	if lom.AtimeUnix() > j.newest {
		j.newest = lom.AtimeUnix()
	}
	return true
}

func (j *lruJ) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	if err := j.yieldTerm(); err != nil {
		return err
	}
	parsedFQN, _, err := core.ResolveFQN(fqn)
	if err != nil {
		return nil
	}
	if parsedFQN.ContentType == fs.ObjectType {
		j.visitLOM(&parsedFQN)
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

	// evict(sic!) and house-keep
	for h.Len() > 0 && j.totalSize > 0 {
		lom := heap.Pop(h).(*core.LOM)
		if !j.evictObj(lom) {
			core.FreeLOM(lom)
			continue
		}
		objSize := lom.SizeBytes(true /*not loaded*/)
		core.FreeLOM(lom)
		bevicted += objSize
		size += objSize
		fevicted++
		if capCheck, err = j.postRemove(capCheck, objSize); err != nil {
			return
		}
	}
	j.ini.StatsT.Add(stats.LruEvictSize, bevicted)
	j.ini.StatsT.Add(stats.LruEvictCount, fevicted)
	xlru.ObjsAdd(int(fevicted), bevicted)
	return
}

func (j *lruJ) postRemove(prev, size int64) (capCheck int64, err error) {
	j.totalSize -= size
	capCheck = prev + size
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
	usedPct, ok := j.ini.GetFSUsedPercentage(j.mi.Path)
	if ok && usedPct < j.config.Space.HighWM {
		err = j._throttle(usedPct)
	}
	return
}

func (j *lruJ) _throttle(usedPct int64) (err error) {
	if j.mi.IsIdle(j.config) {
		return
	}
	// throttle self
	ratioCapacity := cos.Ratio(j.config.Space.HighWM, j.config.Space.LowWM, usedPct)
	curr := fs.GetMpathUtil(j.mi.Path)
	ratioUtilization := cos.Ratio(j.config.Disk.DiskUtilHighWM, j.config.Disk.DiskUtilLowWM, curr)
	if ratioUtilization > ratioCapacity {
		if usedPct < (j.config.Space.LowWM+j.config.Space.HighWM)/2 {
			j.throttle = true
		}
		time.Sleep(mpather.ThrottleMaxDur)
		err = j.yieldTerm()
	}
	return
}

// remove local copies that "belong" to different LRU joggers (space accounting may be temporarily not precise)
func (j *lruJ) evictObj(lom *core.LOM) bool {
	lom.Lock(true)
	err := lom.Remove()
	lom.Unlock(true)
	if err != nil {
		nlog.Errorf("%s: failed to evict %s: %v", j, lom, err)
		return false
	}
	if cmn.Rom.FastV(5, cos.SmoduleSpace) {
		nlog.Infof("%s: evicted %s, size=%d", j, lom, lom.SizeBytes(true /*not loaded*/))
	}
	return true
}

func (j *lruJ) evictSize() (err error) {
	lwm, hwm := j.config.Space.LowWM, j.config.Space.HighWM
	blocks, bavail, bsize, err := j.ini.GetFSStats(j.mi.Path)
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
	case errCause := <-xlru.ChanAbort():
		return cmn.NewErrAborted(xlru.Name(), "", errCause)
	case <-j.stopCh:
		return cmn.NewErrAborted(xlru.Name(), "", nil)
	default:
		if j.throttle {
			time.Sleep(mpather.ThrottleMinDur)
		}
		break
	}
	if xlru.Finished() {
		return cmn.NewErrAborted(xlru.Name(), "", nil)
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
		path := j.mi.MakePathCT(&bcks[i], fs.ObjectType)
		sized[i].b = bcks[i]
		sized[i].v, _ = ios.DirSizeOnDisk(path, false /*withNonDirPrefix*/)
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
		bowner = core.T.Bowner()
		b      = meta.CloneBck(&j.bck)
	)
	if err = b.Init(bowner); err != nil {
		return
	}
	ok = b.Props.LRU.Enabled && b.Allow(apc.AceObjDELETE) == nil
	return
}

//////////////
// min-heap //
//////////////

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Atime().Before(h[j].Atime()) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(*core.LOM)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	fi := old[n-1]
	*h = old[0 : n-1]
	return fi
}
