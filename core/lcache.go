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
	mpeEvictAtime = time.Minute * 10 // extreme
	mphEvictAtime = time.Minute * 20 // high
	mpnEvictAtime = time.Hour        // normal

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
// evictions
//

func UncacheBck(b *meta.Bck) {
	var (
		caches = lomCaches()
		n      = max(sys.NumCPU()/4, 4)
		wg     = cos.NewLimitedWaitGroup(n, len(caches))
	)
	for _, lcache := range caches {
		wg.Add(1)
		go func(cache *sync.Map) {
			cache.Range(func(hkey, value any) bool {
				lmd := value.(*lmeta)
				bck, _ := cmn.ParseUname(*lmd.uname)
				if bck.Equal((*cmn.Bck)(b)) {
					cache.Delete(hkey)
				}
				return true
			})
			wg.Done()
		}(lcache)
	}
	wg.Wait()
}

// NOTE: watch https://github.com/golang/go/pull/61702 for `sync.Map.Clear`, likely Go 22
func UncacheMountpath(mi *fs.Mountpath) {
	for idx := range cos.MultiSyncMapCount {
		cache := mi.LomCache(idx)
		cache.Range(func(hkey any, _ any) bool {
			cache.Delete(hkey)
			return true
		})
	}
}

//////////
// lchk //
//////////

func (lchk *lchk) housekeep() (d time.Duration) {
	var tag string
	d, tag = lchk.mp()
	if !lchk.running.CAS(false, true) {
		if tag != "" {
			nlog.Infof("running now: memory pressure %q, next sched %v", tag, d)
		}
		return
	}
	go lchk.evictAll(d /*evict older than*/)
	return
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
	}
	return
}

func (lchk *lchk) evictAll(d time.Duration) {
	lchk.now = time.Now()
	lchk.d = d

	lchk.evictedCnt, lchk.flushColdCnt, lchk.totalCnt = 0, 0, 0

	// single-threaded: one cache at a time
	caches := lomCaches()
	if lchk.terminating() {
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

	if _, tag := lchk.mp(); tag != "" {
		nlog.Infof("memory pressure %q, total %d, evicted %d", tag, lchk.totalCnt, lchk.evictedCnt)
	}

	// stats
	g.tstats.Add(LcacheEvictedCount, lchk.evictedCnt)
	g.tstats.Add(LcacheFlushColdCount, lchk.flushColdCnt)

	lchk.running.Store(false)
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
	lchk.evictedCnt++
	return true
}

func (lchk *lchk) flush(md *lmeta, atime time.Time) {
	lif := LIF{uname: *md.uname, bid: md.bckID}
	lom, err := lif.LOM()
	if err == nil {
		lom.Lock(true)
		lom.flushCold(md, atime)
		lom.Unlock(true)
		FreeLOM(lom)
		lchk.flushColdCnt++
	}
}
