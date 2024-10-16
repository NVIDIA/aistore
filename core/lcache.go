// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
)

const (
	oomEvictAtime = time.Minute * 5  // OOM
	mpeEvictAtime = time.Minute * 20 // extreme
	mphEvictAtime = time.Hour        // high
	mpnEvictAtime = 8 * time.Hour    // low

	iniEvictAtime = mpnEvictAtime / 2 // initial
	maxEvictAtime = mpnEvictAtime * 2 // maximum
)

type lchk struct {
	cache *sync.Map
	// runtime
	now      time.Time
	d        time.Duration
	totalCnt int64
	// stats
	evictedCnt   int64
	flushColdCnt int64
	// single entry
	running atomic.Bool
}

func regLomCacheWithHK() {
	g.lchk.running.Store(false)
	hk.Reg("lcache"+hk.NameSuffix, g.lchk.housekeep, iniEvictAtime)
}

//
// evictions, mountpaths
//

func UncacheBck(b *meta.Bck) {
	var (
		caches = lomCaches()
		n      = max(sys.NumCPU()/4, 4)
		wg     = cos.NewLimitedWaitGroup(n, len(caches))
		rmb    = (*cmn.Bck)(b)
	)
	for _, lcache := range caches {
		wg.Add(1)
		go func(cache *sync.Map) {
			cache.Range(func(hkey, value any) bool {
				lmd := value.(*lmeta)
				bck, _ := cmn.ParseUname(*lmd.uname)
				if bck.Equal(rmb) {
					cache.Delete(hkey)
					*lmd = lom0.md
				}
				return true
			})
			wg.Done()
		}(lcache)
	}
	wg.Wait()
}

func UncacheMountpath(mi *fs.Mountpath) {
	for idx := range cos.MultiHashMapCount {
		cache := mi.LomCaches.Get(idx)
		cache.Clear()
	}
}

func lcacheIdx(digest uint64) int { return int(digest & cos.MultiHashMapMask) }

//////////
// lchk //
//////////

func (lchk *lchk) housekeep(int64) time.Duration {
	d, tag := lchk.mp()
	if !lchk.running.CAS(false, true) {
		nlog.Infoln("running now; memory pressure:", tag, "next HK:", d)
		return d
	}

	go func(d time.Duration) {
		lchk.evictOlder(d)
		lchk.running.Store(false)
	}(d)

	return d
}

const termDuration = time.Duration(-1)

func (lchk *lchk) terminating() bool { return lchk.d == termDuration }

func (*lchk) mp() (d time.Duration, tag string) {
	p := g.pmm.Pressure()
	switch p {
	case memsys.OOM:
		d = oomEvictAtime
		tag = "OOM"
	case memsys.PressureExtreme:
		d = mpeEvictAtime
		tag = "extreme"
	case memsys.PressureHigh:
		d = mphEvictAtime
		tag = "high"
	default:
		d = mpnEvictAtime
		tag = "low"
	}
	return
}

func (lchk *lchk) evictOlder(d time.Duration) {
	lchk.now = time.Now()
	lchk.d = d

	lchk.evictedCnt, lchk.flushColdCnt, lchk.totalCnt = 0, 0, 0

	// single-threaded: one cache at a time
	caches := lomCaches()
	if lchk.terminating() {
		nlog.Infoln("terminating -->")
		for _, cache := range caches {
			lchk.cache = cache
			cache.Range(lchk.fterm)
		}
		return
	}

	for _, cache := range caches {
		lchk.cache = cache
		cache.Range(lchk.frun)
	}

	_, tag := lchk.mp()
	nlog.Infoln("post-evict memory pressure:", tag, "total:", lchk.totalCnt, "evicted:", lchk.evictedCnt)

	// stats
	g.tstats.Add(LcacheEvictedCount, lchk.evictedCnt)
	g.tstats.Add(LcacheFlushColdCount, lchk.flushColdCnt)
}

func (lchk *lchk) fterm(_, value any) bool {
	md := value.(*lmeta)
	if md.Atime < 0 {
		// prefetched, not yet accessed
		lchk.flush(md, time.Unix(0, -md.Atime))
		return true
	}
	if md.isDirty() || md.atimefs != uint64(md.Atime) {
		lchk.flush(md, time.Unix(0, md.Atime))
	}
	return true
}

func (lchk *lchk) frun(hkey, value any) bool {
	var (
		md     = value.(*lmeta)
		mdTime = md.Atime
	)
	lchk.totalCnt++

	if mdTime < 0 {
		// prefetched, not yet accessed
		mdTime = -mdTime
	}
	atime := time.Unix(0, mdTime)
	elapsed := lchk.now.Sub(atime)
	if elapsed < lchk.d {
		return true
	}
	if md.isDirty() {
		if lchk.d > mphEvictAtime && elapsed < maxEvictAtime {
			return true
		}
		lchk.flush(md, atime)
	} else if md.atimefs != uint64(md.Atime) {
		lchk.flush(md, atime)
	}

	lchk.cache.Delete(hkey)
	*md = lom0.md // zero out
	lchk.evictedCnt++
	return true
}

func (lchk *lchk) flush(md *lmeta, atime time.Time) {
	lif := LIF{uname: *md.uname, lid: md.lid}
	lom, err := lif.LOM()
	if err == nil {
		lom.Lock(true)
		lom.flushCold(md, atime)
		lom.Unlock(true)
		FreeLOM(lom)
		lchk.flushColdCnt++
	}
}
