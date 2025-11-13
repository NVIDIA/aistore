// Package space provides storage cleanup and eviction functionality (the latter based on the
// least recently used cache replacement). It also serves as a built-in garbage-collection
// mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package space

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
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
// in accordance with the current storage-target's utilization (see cmn/load package).
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
		StatsT              stats.Tracker
		Xaction             *XactLRU
		GetFSUsedPercentage func(path string) (usedPercentage int64, err error)
		GetFSStats          func(path string) (blocks, bavail uint64, bsize int64, err error)
		WG                  *sync.WaitGroup
		Buckets             []cmn.Bck
		Force               bool
	}
	XactLRU struct {
		p   *lruFactory
		ini *IniLRU
		xact.Base
	}
)

// private
type (
	// minHeap keeps fileInfo sorted by access time with oldest on top of the heap.
	minHeap []*core.LOM

	// parent (contains mpath joggers)
	lruP struct {
		joggers map[string]*lruJ
		ini     IniLRU
		wg      sync.WaitGroup
	}

	// lruJ represents a single LRU context and a single /jogger/
	// that traverses and evicts a single given mountpath.
	lruJ struct {
		p      *lruP    // parent
		heap   *minHeap // sorted
		ini    *IniLRU  // init params
		stopCh chan struct{}
		mi     *fs.Mountpath // the mountpath
		config *cmn.Config   // to refresh independently
		bck    cmn.Bck

		// throttle
		nvisits int64
		adv     load.Advice

		// runtime state
		capCheck    int64
		newest      int64
		now         int64
		totalSize   int64 // difference between lowWM size and used size
		allowDelObj bool
	}
	lruFactory struct {
		xctn *XactLRU
		xreg.RenewBase
	}
	TestFactory = lruFactory // unit tests only
)

// interface guard
var (
	_ xreg.Renewable = (*lruFactory)(nil)
	_ core.Xact      = (*XactLRU)(nil)
)

//
// x-lru and its factory
//

func (*lruFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &lruFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *lruFactory) Start() error {
	p.xctn = &XactLRU{p: p}
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
		xlru    = ini.Xaction
		config  = cmn.GCO.Get()
		avail   = fs.GetAvail()
		num     = len(avail)
		joggers = make(map[string]*lruJ, num)
		parent  = &lruP{joggers: joggers, ini: *ini}
	)
	defer func() {
		if ini.WG != nil {
			ini.WG.Done()
		}
	}()
	if num == 0 {
		xlru.AddErr(cmn.ErrNoMountpaths, 0)
		xlru.Finish()
		return
	}

	xlru.ini = ini

	for mpath, mi := range avail {
		h := make(minHeap, 0, 64)
		j := &lruJ{
			heap:   &h,
			stopCh: make(chan struct{}, 1),
			mi:     mi,
			config: config,
			ini:    &parent.ini,
			p:      parent,
		}
		// init throttling context
		j.adv.Init(load.FlMem|load.FlCla|load.FlDsk, &load.Extra{Mi: j.mi, Cfg: &j.config.Disk, RW: false})

		joggers[mpath] = j
	}
	providers := apc.Providers.ToSlice()

	for _, j := range joggers {
		parent.wg.Add(1)
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

func (*XactLRU) Run(*sync.WaitGroup) { debug.Assert(false) } // via RunLRU

func (r *XactLRU) ctlmsg() string {
	s := r.p.Args.Custom.(string)
	if r.ini == nil {
		return s
	}
	if l := len(r.ini.Buckets); l > 0 {
		cnames := make([]string, 0, l)
		for i := range r.ini.Buckets {
			b := &r.ini.Buckets[i]
			cnames = append(cnames, b.Cname(""))
		}
		s += ", buckets: " + strings.Join(cnames, ",")
	}
	if r.ini.Force {
		s += ", force"
	}
	return s
}

func (r *XactLRU) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.AddBaseSnap(snap)

	snap.SetCtlMsg(r.Name(), r.ctlmsg())

	snap.IdleX = r.IsIdle()
	return
}

//
// lruJ: mountpath jogger
//

func (j *lruJ) String() string {
	return fmt.Sprintf("%s: jog-%s", j.ini.Xaction, j.mi)
}

func (j *lruJ) batch() int64  { return j.config.LRU.BatchSize }
func (j *lruJ) window() int64 { return min(j.config.LRU.BatchSize<<2, cmn.GCBatchSizeMax) }

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
		nlog.Infof("%s: freeing-up %s", j, cos.IEC(j.totalSize, 2))
		err = j.jogBcks(j.ini.Buckets, j.ini.Force)
	} else {
		err = j.jog(providers)
	}
ex:
	if err == nil || cmn.IsErrBucketNought(err) || cmn.IsErrObjNought(err) {
		return
	}
	nlog.Errorln(j.String()+":", "exited with err:", err)
}

func (j *lruJ) jog(providers []string) (err error) {
	nlog.Infoln(j.String()+":", "freeing-up", cos.IEC(j.totalSize, 2))
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

func (j *lruJ) jogBcks(bcks []cmn.Bck, force bool) error {
	if len(bcks) == 0 {
		return nil
	}
	if len(bcks) > 1 {
		j.sortBsize(bcks)
	}
	for _, bck := range bcks { // for each bucket under a given provider
		j.bck = bck
		a, err := j.allow()
		if err != nil {
			nlog.Errorf("%s: %v - skipping %s (Hint: run 'ais storage cleanup' to cleanup)", j, err, bck.String())
			continue
		}
		j.allowDelObj = a || force

		if err := j.jogBck(); err != nil {
			return err
		}

		// recompute size-to-evict
		if err := j.evictSize(); err != nil {
			return err
		}
		if j.totalSize < cos.KiB {
			return nil
		}
	}
	return nil
}

func (j *lruJ) jogBck() error {
	// 1. init per-bucket min-heap (and reuse the slice)
	h := (*j.heap)[:0]
	j.heap = &h
	heap.Init(j.heap)

	// 2. collect
	opts := &fs.WalkOpts{
		Mi:       j.mi,
		Bck:      j.bck,
		CTs:      []string{fs.ObjCT},
		Callback: j.walk,
		Sorted:   false,
	}
	j.now = time.Now().UnixNano()
	if err := fs.Walk(opts); err != nil {
		return err
	}
	// 3. evict
	j.evict(math.MaxInt64)
	return nil
}

func (j *lruJ) visitLOM(parsedFQN *fs.ParsedFQN) {
	if !j.allowDelObj {
		return
	}
	lom := core.AllocLOM(parsedFQN.ObjName)
	if pushed := j._visit(lom, parsedFQN); !pushed {
		core.FreeLOM(lom)
	}
}

func (j *lruJ) _visit(lom *core.LOM, parsedFQN *fs.ParsedFQN) (pushed bool) {
	if err := lom.InitBck(&j.bck); err != nil {
		xlru := j.ini.Xaction
		if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
			nlog.Warningln(j.String(), "bucket gone - aborting:", err)
		} else {
			err = fmt.Errorf("%s: unexpected lom-init fail [ %q, %w ]", j, parsedFQN.Bck.Cname(parsedFQN.ObjName), err)
			nlog.Errorln(err)
		}
		xlru.Abort(err)
		return false
	}
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		return false
	}
	if lom.AtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
		return false
	}
	if lom.HasCopies() && lom.IsCopy() {
		return false
	}

	hlen := int64(j.heap.Len())
	if lom.AtimeUnix() > j.newest {
		// not adding - have a full batch already and this object is newer
		if hlen >= j.batch() {
			return false
		}
		j.newest = lom.AtimeUnix()
	}
	heap.Push(j.heap, lom) // note: free(this lom) upon heap.Pop

	// evict entire oldest batch once per window; allow multiple if overshot
	if hlen >= j.window() {
		j.evict(j.batch())
	}

	return true
}

func (j *lruJ) walk(fqn string, de fs.DirEntry) error {
	var parsed fs.ParsedFQN
	if de.IsDir() {
		return nil
	}
	if j.done() {
		return nil
	}
	j.nvisits++
	if _, err := core.ResolveFQN(fqn, &parsed); err != nil {
		xlru := j.ini.Xaction
		xlru.AddErr(err, 0)
		return nil
	}
	if parsed.ContentType == fs.ObjCT {
		j.visitLOM(&parsed)
	}

	return nil
}

func (j *lruJ) evict(batch int64) {
	var (
		fevicted int64
		bevicted int64
		h        = j.heap
		xlru     = j.ini.Xaction
	)
	for h.Len() > 0 && j.totalSize > 0 && fevicted < batch {
		lom := heap.Pop(h).(*core.LOM)
		objSize := lom.Lsize()
		ok := j.evictObj(lom)
		core.FreeLOM(lom)

		if ok {
			bevicted += objSize
			fevicted++
			j.capCheckAndThrottle(objSize)
		}
		if j.done() {
			break
		}
	}
	if fevicted > 0 {
		j.ini.StatsT.Add(stats.LruEvictSize, bevicted)
		j.ini.StatsT.Add(stats.LruEvictCount, fevicted)
		xlru.ObjsAdd(int(fevicted), bevicted)

		// plus, once per batch
		if fevicted >= batch {
			j.adv.Refresh()
			if j.adv.Sleep > 0 {
				time.Sleep(j.adv.Sleep)
			}
		}
	}
}

func (j *lruJ) capCheckAndThrottle(size int64) {
	j.totalSize -= size
	j.capCheck += size
	if j.capCheck < capCheckThresh {
		return
	}

	// init, recompute, and throttle - once per capCheckThresh
	j.allowDelObj, _ = j.allow()
	j.config = cmn.GCO.Get() // refresh
	j.now = time.Now().UnixNano()
	j.capCheck = 0

	if j.adv.ShouldCheck(j.nvisits) {
		usedPct, _ := j.ini.GetFSUsedPercentage(j.mi.Path)
		if usedPct < j.config.Space.HighWM {
			j.adv.Refresh()
			if j.adv.Sleep > 0 {
				time.Sleep(j.adv.Sleep)
			}
		}
	}
}

// remove local copies that "belong" to different LRU joggers (space accounting may be temporarily not precise)
func (j *lruJ) evictObj(lom *core.LOM) bool {
	lom.Lock(true)
	err := lom.RemoveObj()
	lom.Unlock(true)
	if err != nil {
		xlru := j.ini.Xaction
		e := fmt.Errorf("failed to evict %s: %v", lom, err)
		xlru.AddErr(e, 0)
		return false
	}
	if cmn.Rom.V(5, cos.ModSpace) {
		nlog.Infof("%s: evicted %s, size=%d", j, lom, lom.Lsize(true /*not loaded*/))
	}
	return true
}

func (j *lruJ) evictSize() error {
	blocks, bavail, bsize, err := j.ini.GetFSStats(j.mi.Path)
	if err != nil {
		return err
	}

	var (
		lwm, hwm = j.config.Space.LowWM, j.config.Space.HighWM
		used     = blocks - bavail
		usedPct  = used * 100 / blocks
	)
	if usedPct < uint64(hwm) {
		return nil
	}
	lwmBlocks := blocks * uint64(lwm) / 100
	j.totalSize = int64(used-lwmBlocks) * bsize
	return nil
}

func (j *lruJ) done() bool {
	xlru := j.ini.Xaction
	select {
	case <-xlru.ChanAbort():
		return true
	case <-j.stopCh:
		return true
	default:
		break
	}
	return xlru.IsDone()
}

// sort buckets by size
func (j *lruJ) sortBsize(bcks []cmn.Bck) {
	sized := make([]struct {
		b cmn.Bck
		v uint64
	}, len(bcks))
	for i := range bcks {
		path := j.mi.MakePathCT(&bcks[i], fs.ObjCT)
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

func (j *lruJ) allow() (bool, error) {
	var (
		bowner = core.T.Bowner()
		b      = meta.CloneBck(&j.bck)
	)
	if err := b.Init(bowner); err != nil {
		return false, err
	}
	ok := b.Props.LRU.Enabled && b.Allow(apc.AceObjDELETE) == nil
	return ok, nil
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
