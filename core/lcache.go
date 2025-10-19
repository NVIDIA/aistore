// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	skipEvictThreashold = 20                // likely not running when above
	maxEvictThreashold  = fs.MaxThrottlePct // not running when above
)

type (
	// g.lchk
	lchk struct {
		last    time.Time
		timeout time.Duration
		total   atomic.Int64
		rc      atomic.Int32
		running atomic.Bool
	}
	// HK flush & evict
	evct struct {
		now     time.Time
		parent  *lchk
		mi      *fs.Mountpath
		wg      *sync.WaitGroup
		cache   *sync.Map
		d       time.Duration
		pct     int
		evicted int64
	}
	// termination
	term struct {
		mi *fs.Mountpath
		wg *sync.WaitGroup
	}
	// uncache buckets
	rmbcks struct {
		mi    *fs.Mountpath
		wg    *sync.WaitGroup
		cache *sync.Map
		bcks  []*meta.Bck
		begin int64
		pct   int
		nd    int64
		// on => off at half-time
		throttle bool
	}
)

//
// public
//

func LcacheClear() {
	avail := fs.GetAvail()
	for _, mi := range avail {
		LcacheClearMpath(mi)
	}
}

func LcacheClearMpath(mi *fs.Mountpath) {
	for idx := range cos.MultiHashMapCount {
		cache := mi.LomCaches.Get(idx)
		cache.Clear()
	}
}

func LcacheClearBcks(wg *sync.WaitGroup, bcks ...*meta.Bck) bool {
	g.lchk.rc.Inc()
	defer g.lchk.rc.Dec()

	// mem pressure; NOTE: may call oom.FreeToOS
	if g.lchk.mempDropAll() {
		return true // dropped all caches, nothing to do
	}

	pct, util, lavg := fs.ThrottlePct()
	flog := cos.Ternary(pct > maxEvictThreashold, nlog.Warningln, nlog.Infoln)
	flog("uncache:", bcks[0].String(), "[ throttle(%%):", pct, "dutil:", util, "load avg:", lavg, "]")
	if num := len(bcks); num > 1 {
		flog("\tmultiple buckets:", bcks[0].String(), bcks[1].String(), "... [ num:", num, "]")
	}

	avail := fs.GetAvail()
	begin := mono.NanoTime()
	for _, mi := range avail {
		wg.Add(1)
		u := &rmbcks{
			mi:       mi,
			wg:       wg,
			bcks:     bcks,
			pct:      pct,
			begin:    begin,
			throttle: true,
		}
		go u.do()
	}
	return false
}

//
// private
//

// g.lchk
func (lchk *lchk) init(config *cmn.Config) {
	lchk.running.Store(false)
	lchk.timeout = cos.NonZero(config.Timeout.ObjectMD.D(), cmn.LcacheEvictDflt)

	lchk.last = time.Now()
	hk.Reg("lcache"+hk.NameSuffix, lchk.housekeep, lchk.timeout)
}

func (u *rmbcks) do() {
	defer u.wg.Done()

	for idx := range cos.MultiHashMapCount {
		if !u.mi.IsAvail() {
			return
		}
		u.cache = u.mi.LomCaches.Get(idx)
		u.cache.Range(u.f)

		// stop throttling at half-timeout (see metasync)
		if u.throttle && mono.Since(u.begin) > cmn.Rom.MaxKeepalive()>>1 {
			u.throttle = false
		}
	}
}

func (u *rmbcks) f(hkey, value any) bool {
	lmd := value.(*lmeta)
	if lmd.uname == nil {
		return true
	}
	b, _ := cmn.ParseUname(*lmd.uname)
	for _, rmb := range u.bcks {
		if !rmb.Eq(&b) {
			continue
		}
		lmd2, _ := u.cache.LoadAndDelete(hkey)
		if lmd2 == lmd {
			*lmd = lom0.md
		}
		// throttle
		u.nd++
		if u.throttle && fs.IsThrottleDflt(u.nd) {
			// compare with _throttle(evct.pct) but note: caller's waiting on wg
			if u.pct >= maxEvictThreashold {
				time.Sleep(fs.Throttle10ms)
			} else {
				runtime.Gosched()
			}
		}
		break
	}
	return true
}

func lcacheIdx(digest uint64) int { return int(digest & cos.MultiHashMapMask) }

//
// term
//

func (lchk *lchk) term() {
	var (
		avail = fs.GetAvail()
		wg    = &sync.WaitGroup{}
	)
	lchk.rc.Inc()
	defer lchk.rc.Dec()
	for _, mi := range avail {
		wg.Add(1)
		term := &term{
			mi: mi,
			wg: wg,
		}
		go term.do()
	}
	wg.Wait()
}

func (term *term) do() {
	defer term.wg.Done()
	for idx := range cos.MultiHashMapCount {
		cache := term.mi.LomCaches.Get(idx)
		cache.Range(term.f)
	}
}

func (*term) f(_, value any) bool {
	md := value.(*lmeta)
	if md.uname == nil {
		return true
	}
	lif := LIF{uname: *md.uname, lid: md.lid}
	lom, err := lif.LOM()
	if err != nil {
		return true
	}
	if lom.WritePolicy() == apc.WriteNever {
		return true
	}
	if md.Atime < 0 {
		// prefetched, not yet accessed
		mdTime := -md.Atime
		_flushAtime(md, time.Unix(0, mdTime), mdTime)
		return true
	}
	if md.isDirty() || md.atimefs != uint64(md.Atime) {
		_flushAtime(md, time.Unix(0, md.Atime), md.Atime)
	}
	return true
}

//
// HK evict
//

func (lchk *lchk) housekeep(int64) time.Duration {
	// refresh
	config := cmn.GCO.Get()
	lchk.timeout = cos.NonZero(config.Timeout.ObjectMD.D(), cmn.LcacheEvictDflt)

	// concurrent term, uncache-bck, etc.
	rc := lchk.rc.Load()
	if rc > 0 {
		nlog.Warningln("(not) running now, rc:", rc)
		return lchk.timeout
	}

	// mem pressure; NOTE: may call oom.FreeToOS
	if lchk.mempDropAll() {
		return lchk.timeout
	}

	// load, utilization
	pct, util, lavg := fs.ThrottlePct()
	nlog.Infoln("hk: [ throttle(%%):", pct, "dutil:", util, "load avg:", lavg, "]")

	if pct > maxEvictThreashold {
		nlog.Warningln("max-evict threshold:", maxEvictThreashold, "- not running")
		return min(lchk.timeout>>1, cmn.LcacheEvictDflt>>1)
	}
	now := time.Now()
	if pct > skipEvictThreashold {
		if elapsed := now.Sub(lchk.last); elapsed < min(cmn.LcacheEvictMax, max(lchk.timeout, time.Hour)*8) {
			nlog.Warningln("skip-evict threshold:", skipEvictThreashold, "elapsed:", elapsed, "- not running")
			return min(lchk.timeout>>1, cmn.LcacheEvictDflt>>1)
		}
	}

	// still running?
	if !lchk.running.CAS(false, true) {
		nlog.Warningln("(not) running now")
		return lchk.timeout
	}

	// finally, run
	nlog.Infoln("hk begin")
	lchk.last = now
	go lchk.evict(lchk.timeout, now, pct)

	return lchk.timeout
}

func (lchk *lchk) mempDropAll() bool /*dropped*/ {
	if p := g.pmm.Pressure(); p >= memsys.PressureExtreme {
		nlog.WarningDepth(1, "dropping all caches")
		LcacheClear()
		lchk.last = time.Now()

		oom.FreeToOS(true)
		return true
	}

	return false
}

func (lchk *lchk) evict(timeout time.Duration, now time.Time, pct int) {
	defer lchk.running.Store(false)
	var (
		avail   = fs.GetAvail()
		wg      = &sync.WaitGroup{}
		tstats  = T.StatsUpdater()
		evicted = tstats.Get(LcacheEvictedCount)
	)
	lchk.total.Store(0)
	for _, mi := range avail {
		wg.Add(1)
		evct := &evct{
			parent: lchk,
			mi:     mi,
			wg:     wg,
			now:    now,
			d:      timeout,
			pct:    pct,
		}
		go evct.do()
	}
	wg.Wait()
	evicted = tstats.Get(LcacheEvictedCount) - evicted
	nlog.Infoln("hk done:", lchk.total.Load(), evicted)
}

func (evct *evct) do() {
	defer evct.wg.Done()
	for idx := range cos.MultiHashMapCount {
		if !evct.mi.IsAvail() {
			return
		}
		cache := evct.mi.LomCaches.Get(idx)
		evct.cache = cache
		cache.Range(evct.f)
		if evct.parent.rc.Load() > 0 {
			break
		}
	}
}

func (evct *evct) f(hkey, value any) bool {
	evct.parent.total.Inc()

	md := value.(*lmeta)
	mdTime := md.Atime
	if mdTime < 0 {
		// prefetched, not yet accessed
		mdTime = -mdTime
	}

	atime := time.Unix(0, mdTime)
	elapsed := evct.now.Sub(atime)
	if elapsed < evct.d {
		return evct.parent.rc.Load() == 0
	}

	// flush
	if md.isDirty() || md.atimefs != uint64(md.Atime) {
		_flushAtime(md, atime, mdTime)
	}

	// evict
	lmd2, _ := evct.cache.LoadAndDelete(hkey)
	if lmd2 == md {
		*md = lom0.md // zero out
	}
	T.StatsUpdater().Inc(LcacheEvictedCount)

	// throttle
	evct.evicted++
	if fs.IsThrottleMini(evct.evicted) {
		_throttle(evct.pct)
	}

	return evct.parent.rc.Load() == 0
}

func _flushAtime(md *lmeta, atime time.Time, mdTime int64) {
	if md.uname == nil {
		return
	}
	lif := LIF{uname: *md.uname, lid: md.lid}
	lom, err := lif.LOM()
	if err != nil {
		return
	}
	if lom.WritePolicy() == apc.WriteNever {
		return
	}

	tstats := T.StatsUpdater()
	if err = lom.flushAtime(atime); err != nil {
		if !cos.IsNotExist(err) {
			tstats.Inc(LcacheErrCount)
			T.FSHC(err, lom.Mountpath(), lom.FQN)
		}
		return
	}

	// stats
	tstats.Inc(LcacheFlushColdCount)

	if !md.isDirty() {
		return
	}

	// special [dirty] case: clear and flush
	md.Atime = mdTime
	md.atimefs = uint64(mdTime)
	lom.md = *md

	buf := lom.pack()
	if err = lom.SetXattr(buf); err != nil {
		T.FSHC(err, lom.Mountpath(), lom.FQN)
	} else {
		for copyFQN := range lom.md.copies {
			if copyFQN == lom.FQN {
				continue
			}
			if err = fs.SetXattr(copyFQN, xattrLOM, buf); err != nil {
				tstats.Inc(LcacheErrCount)
				nlog.Errorln("set-xattr [", copyFQN, err, "]")
				break
			}
		}
	}
	g.smm.Free(buf)
	FreeLOM(lom)
}

//
// throttle
//

func _throttle(pct int) {
	if pct < 10 {
		runtime.Gosched()
	} else {
		time.Sleep(time.Duration(pct) * time.Millisecond)
	}
}
