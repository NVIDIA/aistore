// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	oomEvictAtime = time.Minute * 5   // OOM
	mpeEvictAtime = time.Minute * 10  // extreme
	mphEvictAtime = time.Minute * 20  // high
	mpnEvictAtime = time.Hour         // normal
	iniEvictAtime = mpnEvictAtime / 2 // initial
)

type lcHK struct {
	mm      *memsys.MMSA
	t       Target
	running atomic.Bool
}

var lchk lcHK

func RegLomCacheWithHK(t Target) {
	lchk.t = t
	lchk.mm = t.PageMM()
	lchk.running.Store(false)
	hk.Reg("lom-cache"+hk.NameSuffix, lchk.housekeep, iniEvictAtime)
}

func (lchk *lcHK) housekeep() (d time.Duration) {
	var tag string
	d, tag = lchk.mp()
	if !lchk.running.CAS(false, true) {
		if tag != "" {
			glog.Infof("running now: memory pressure %q, next sched %v", tag, d)
		}
		return
	}
	go lchk.evictAll(d /*evict older than*/)
	return
}

func (lchk *lcHK) mp() (d time.Duration, tag string) {
	p := lchk.mm.Pressure()
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

func (lchk *lcHK) evictAll(d time.Duration) {
	var (
		caches               = lomCaches()
		now                  = time.Now()
		evictedCnt, totalCnt int
	)
	defer lchk.running.Store(false)

	// one cache at a time (TODO: throttle via mountpath.IsIdle())
	for _, cache := range caches {
		f := func(hkey, value interface{}) bool {
			md := value.(*lmeta)
			mdTime := md.Atime
			if mdTime < 0 {
				mdTime = -mdTime // special case: prefetched but not yet accessed
			}
			totalCnt++
			atime := time.Unix(0, mdTime)
			if now.Sub(atime) < d {
				return true
			}
			atimefs := md.atimefs & ^lomDirtyMask
			if md.Atime > 0 && atimefs != uint64(md.Atime) {
				debug.Assert(isValidAtime(md.Atime))
				lif := LIF{Uname: md.uname, BID: md.bckID}
				lom, err := lif.LOM()
				if err == nil {
					lom.Lock(true)
					lom.flushCold(md, atime)
					lom.Unlock(true)
					FreeLOM(lom)
				}
			}
			cache.Delete(hkey)
			evictedCnt++
			return true
		}
		cache.Range(f)
	}
	if _, tag := lchk.mp(); tag != "" {
		glog.Infof("memory pressure %q, total %d, evicted %d", tag, totalCnt, evictedCnt)
	}
}
