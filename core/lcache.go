// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
)

type (
	// g.lchk
	lchk struct {
		last    atomic.Int64
		timeout time.Duration
		total   atomic.Int64
		rc      atomic.Int32
		running atomic.Bool
	}
	// HK flush & evict
	evct struct {
		now     time.Time // vs LOM atime
		parent  *lchk
		mi      *fs.Mountpath
		wg      *sync.WaitGroup
		cache   *sync.Map
		adv     load.Advice // throttle
		d       time.Duration
		evicted int64
	}
	// termination
	term struct {
		mi    *fs.Mountpath
		wg    *sync.WaitGroup
		cache *sync.Map
	}
	// uncache buckets
	rmbcks struct {
		mi    *fs.Mountpath
		wg    *sync.WaitGroup
		cache *sync.Map
		bcks  []*meta.Bck
		begin int64
		nd    int64

		// throttle
		adv      load.Advice
		throttle bool
	}
)

//
// public
//

func LcacheClear() {
	for _, mi := range fs.GetAvail() {
		for idx := range cos.MultiHashMapCount {
			_clearCleanLmeta(mi.LomCaches.Get(idx))
			mi.SidxCaches.Get(idx).Clear()
		}
	}
}

func LcacheClearMpath(mi *fs.Mountpath) {
	for idx := range cos.MultiHashMapCount {
		mi.LomCaches.Get(idx).Clear()
		mi.SidxCaches.Get(idx).Clear()
	}
}

func LcacheClearBcks(wg *sync.WaitGroup, bcks ...*meta.Bck) {
	g.lchk.rc.Inc()
	defer g.lchk.rc.Dec()

	memLoad := load.Mem() // may trigger free-to-OS
	if memLoad == load.Critical {
		g.lchk.dropClean()
	}

	config := cmn.GCO.Get()
	cpuLoad := load.CPU()
	dskLoad := load.Dsk(nil /*all*/, config.Disk)

	// log
	_log("uncache:", memLoad, cpuLoad, dskLoad)
	if num := len(bcks); num > 1 {
		nlog.Infoln("\tmultiple buckets:", bcks[0].String(), bcks[1].String(), "... [ num:", num, "]")
	} else {
		nlog.Infoln("\tsingle bucket:", bcks[0].String())
	}

	avail := fs.GetAvail()
	begin := mono.NanoTime()
	for _, mi := range avail {
		wg.Add(1)
		u := &rmbcks{
			mi:       mi,
			wg:       wg,
			bcks:     bcks,
			begin:    begin,
			throttle: true,
		}
		u.adv.Init(
			load.FlMem|load.FlCla,
			&load.Extra{RW: false}, // metadata-only
		)
		go u.do()
	}
}

//
// private
//

// g.lchk
func (lchk *lchk) init(config *cmn.Config) {
	lchk.running.Store(false)
	lchk.timeout = cos.NonZero(config.Timeout.ObjectMD.D(), cmn.LcacheEvictDflt)

	lchk.last.Store(mono.NanoTime())
	hk.Reg("lcache"+hk.NameSuffix, lchk.housekeep, lchk.timeout)
}

func (u *rmbcks) do() {
	defer u.wg.Done()
	if u.walk(&u.mi.LomCaches, u.f) {
		u.walk(&u.mi.SidxCaches, u.shardIdxF)
	}
}

func (u *rmbcks) walk(caches *cos.MultiHashMap, f func(hkey, value any) bool) bool {
	for idx := range cos.MultiHashMapCount {
		if !u.mi.IsAvail() {
			return false
		}
		u.cache = caches.Get(idx)
		u.cache.Range(f)
		// stop throttling at half-timeout (see metasync)
		if u.throttle && mono.Since(u.begin) > cmn.Rom.MaxKeepalive()>>1 {
			u.throttle = false
		}
	}
	return true
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
		u.cache.Delete(hkey)
		u.pace()
		break
	}
	return true
}

func (u *rmbcks) pace() {
	u.nd++
	if u.throttle {
		u.adv.Throttle(u.nd, true /*yield*/)
	}
}

func (u *rmbcks) shardIdxF(hkey, value any) bool {
	key := hkey.(sidxKey)
	b, _ := cmn.ParseUname(key.uname)
	for _, rmb := range u.bcks {
		if rmb.Eq(&b) {
			if u.cache.CompareAndDelete(hkey, value) {
				u.pace()
			}
			break
		}
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
		term.cache = term.mi.LomCaches.Get(idx)
		term.cache.Range(term.f)
	}
}

func (term *term) f(hkey, value any) bool {
	md := value.(*lmeta)
	if md.isDirty() || md.atimefs != uint64(md.Atime) {
		_flushCached(term.mi, term.cache, hkey, md)
	}
	return true
}

//
// HK evict
//

func (lchk *lchk) dropClean() {
	var n int
	for _, mi := range fs.GetAvail() {
		for idx := range cos.MultiHashMapCount {
			n += _clearCleanLmeta(mi.LomCaches.Get(idx))
		}
	}
	lchk.last.Store(mono.NanoTime())
	if n > 1000 {
		nlog.WarningDepth(1, "dropping clean LOM cache entries:", n)
	}
}

func _clearCleanLmeta(cache *sync.Map) (n int) {
	cache.Range(func(key, value any) bool {
		if !value.(*lmeta).isDirty() {
			if cache.CompareAndDelete(key, value) {
				n++
			}
		}
		return true
	})
	return n
}

func _log(tag string, memLoad, cpuLoad, dskLoad load.Load) {
	flog := cos.Ternary(memLoad > load.Moderate || cpuLoad > load.Moderate || dskLoad > load.Moderate, nlog.Warningln, nlog.Infoln)
	flog(tag, "[ mem/cpu/disk:", load.Text[memLoad], load.Text[cpuLoad], load.Text[dskLoad], "]")
}

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

	memLoad := load.Mem() // may trigger free-to-OS
	if memLoad == load.Critical {
		lchk.dropClean()
		return lchk.timeout
	}

	cpuLoad := load.CPU()
	dskLoad := load.Dsk(nil /*max util*/, config.Disk)
	_log("hk:", memLoad, cpuLoad, dskLoad)

	now := time.Now()
	memBad := memLoad >= load.High

	// hard skip: memory is OK (<= Moderate) but CPU or disk are Critical
	if !memBad && (cpuLoad == load.Critical || dskLoad == load.Critical) {
		nlog.Warningln("hk: high system load (cpu:", cpuLoad, "disk:", dskLoad, ") - not running")
		return min(lchk.timeout>>1, cmn.LcacheEvictDflt>>1)
	}

	// soft skip: memory is OK, CPU/disk are high, and we ran recently
	if !memBad && (cpuLoad == load.High || dskLoad == load.High) {
		if elapsed := mono.Since(lchk.last.Load()); elapsed < min(cmn.LcacheEvictMax, max(lchk.timeout, time.Hour)*8) {
			nlog.Warningln("hk: high load, elapsed:", elapsed, "- not running")
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
	lchk.last.Store(mono.NanoTime())
	go lchk.evict(lchk.timeout, now)

	return hk.Jitter(lchk.timeout, now.UnixNano())
}

func (lchk *lchk) evict(timeout time.Duration, now time.Time) {
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
		}
		evct.adv.Init(
			load.FlMem|load.FlCla,
			&load.Extra{RW: false},
		)
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

	elapsed := evct.now.Sub(time.Unix(0, mdTime))
	if elapsed < evct.d {
		return evct.parent.rc.Load() == 0
	}

	// flush
	if md.isDirty() || md.atimefs != uint64(md.Atime) {
		if !_flushCached(evct.mi, evct.cache, hkey, md) {
			return evct.parent.rc.Load() == 0
		}
	}

	if evct.cache.CompareAndDelete(hkey, md) {
		T.StatsUpdater().Inc(LcacheEvictedCount)

		// throttle
		evct.evicted++
		evct.adv.Throttle(evct.evicted, true /*yield*/)
	}
	return evct.parent.rc.Load() == 0
}

// whether the entry can be evicted
func _flushCached(mi *fs.Mountpath, cache *sync.Map, hkey any, md *lmeta) bool {
	dirty := md.isDirty()
	if md.uname == nil {
		return !dirty
	}
	lif := LIF{Uname: *md.uname, BID: md.lid.bid()}
	lom, err := lif.LOM()
	if err != nil {
		return !dirty || cmn.IsErrObjDefunct(err)
	}
	defer FreeLOM(lom)
	if lom.mi != mi || lom.digest != hkey || lom.Uname() != lif.Uname {
		return !dirty
	}
	if md.lid.haslmfl(lmflFntl) {
		lom.fixupFntl()
	}
	if dirty {
		// this path tries to take a lock
		if !_flushDirty(lom, cache, hkey, md) {
			return false
		}
	} else if lom.WritePolicy() == apc.WriteNever {
		return true
	}

	// atime is best effort (not taking lock)
	mdTime := md.Atime
	if mdTime < 0 {
		mdTime = -mdTime
	}
	_flushAtime(lom, time.Unix(0, mdTime))
	return true
}

func _flushAtime(lom *LOM, atime time.Time) {
	tstats := T.StatsUpdater()
	if err := lom.flushAtime(atime); err != nil {
		if !cos.IsNotExist(err) {
			tstats.Inc(LcacheErrCount)
			T.FSHC(err, lom.Mountpath(), lom.FQN)
		}
		return
	}
	tstats.Inc(LcacheFlushColdCount)
}

func _flushDirty(lom *LOM, cache *sync.Map, hkey any, md *lmeta) bool {
	if !lom.TryLock(true) {
		return false
	}
	defer lom.Unlock(true)

	// recheck identity after taking the lock and before touching xattrs
	if current, _ := cache.Load(hkey); current != md {
		return false
	}
	bprops, present := T.Bowner().Get().Get(lom.Bck())
	if !present || bprops.BID != md.lid.bid() || bprops.WritePolicy.MD == apc.WriteNever {
		return false
	}
	if mi := lom.Mountpath(); fs.GetAvail()[mi.Path] != mi || mi.IsAnySet(fs.FlagWaitingDD) {
		return false
	}

	// pack a private copy without changing the published entry
	lom.md = *md
	buf := lom.pack()
	defer g.smm.Free(buf)
	if err := lom.SetXattr(buf); err != nil {
		T.FSHC(err, lom.Mountpath(), lom.FQN)
		return false
	}
	for copyFQN := range lom.md.copies {
		if copyFQN == lom.FQN {
			continue
		}
		if err := fs.SetXattr(copyFQN, fs.XattrLOM, buf); err != nil {
			T.StatsUpdater().Inc(LcacheErrCount)
			nlog.Errorln("set-xattr [", copyFQN, err, "]")
			return false
		}
	}
	return true
}
