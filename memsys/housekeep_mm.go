// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/sys"
)

// API: on-demand memory freeing to the user-provided specification
func (r *MMSA) Free(spec FreeSpec) {
	var freed int64
	if spec.Totally {
		for _, s := range r.rings {
			freed += s.cleanup()
		}
	} else {
		if spec.IdleDuration == 0 {
			spec.IdleDuration = freeIdleMin // using default
		}

		currStats := r.GetStats()
		for i, idle := range &currStats.Idle {
			if idle.IsZero() {
				continue
			}
			elapsed := time.Since(idle)
			if elapsed > spec.IdleDuration {
				s := r.rings[i]
				x := s.cleanup()
				freed += x
				if x > 0 && bool(glog.FastV(4, glog.SmoduleMemsys)) {
					glog.Infof("%s: idle for %v - cleanup", s.tag, elapsed)
				}
			}
		}
	}
	if freed > 0 {
		r.toGC.Add(freed)
		if spec.MinSize == 0 {
			spec.MinSize = sizeToGC // using default
		}
		mem, _ := sys.Mem()
		r.doGC(mem.ActualFree, spec.MinSize, spec.ToOS /* force */, false)
	}
}

// a CALLBACK - is executed by the system's house-keeper (hk)
// NOTE: enumerated heuristics
func (r *MMSA) garbageCollect() time.Duration {
	var (
		limit = int64(sizeToGC) // minimum accumulated size that triggers GC
		depth int               // => current ring depth tbd
	)
	mem, _ := sys.Mem()
	swapping := mem.SwapUsed > r.swap.Load()
	if swapping {
		r.Swapping.Store(SwappingMax)
	} else {
		r.Swapping.Store(r.Swapping.Load() / 2)
	}
	r.swap.Store(mem.SwapUsed)

	// 1. enough => free idle
	if mem.ActualFree > r.lowWM && !swapping {
		r.minDepth.Store(minDepth)
		if delta := r.freeIdle(freeIdleMin); delta > 0 {
			r.toGC.Add(delta)
			r.doGC(mem.ActualFree, sizeToGC, false, false)
		}
		goto timex
	}
	if mem.ActualFree <= r.MinFree || swapping { // 2. mem too low indicates "high watermark"
		depth = minDepth / 4
		if mem.ActualFree < r.MinFree {
			depth = minDepth / 8
		}
		if swapping {
			depth = 1
		}
		r.minDepth.Store(int64(depth))
		limit = sizeToGC / 2
	} else { // in-between hysteresis
		x := uint64(maxDepth-minDepth) * (mem.ActualFree - r.MinFree)
		depth = minDepth + int(x/(r.lowWM-r.MinFree)) // Heu #2
		cmn.Dassert(depth >= minDepth && depth <= maxDepth, pkgName)
		r.minDepth.Store(minDepth / 4)
	}
	// sort and reduce
	r.refreshStats()
	sort.Slice(r.ringsSorted, r.idleFirst /* idle first */)
	for i, s := range r.ringsSorted {
		if delta := s.reduce(depth, r.stats.HitsDelta[i] == 0 /* idle */, true /* force */); delta > 0 {
			if r.doGC(mem.ActualFree, limit, true, swapping) {
				goto timex
			}
		}
	}
	// 4. red
	if mem.ActualFree <= r.MinFree || swapping {
		r.doGC(mem.ActualFree, limit, true, swapping)
	}
timex:
	return r.getNextInterval(mem.ActualFree, mem.Total, swapping)
}

func (r *MMSA) getNextInterval(free, total uint64, swapping bool) time.Duration {
	var changed bool
	switch {
	case free > r.lowWM && free > total-total/5:
		if r.duration != r.TimeIval*2 {
			r.duration = r.TimeIval * 2
			changed = true
		}
	case free <= r.MinFree || swapping:
		if r.duration != r.TimeIval/4 {
			r.duration = r.TimeIval / 4
			changed = true
		}
	case free <= r.lowWM:
		if r.duration != r.TimeIval/2 {
			r.duration = r.TimeIval / 2
			changed = true
		}
	default:
		if r.duration != r.TimeIval {
			r.duration = r.TimeIval
			changed = true
		}
	}
	if changed && bool(glog.FastV(4, glog.SmoduleMemsys)) {
		glog.Infof("timer %v, free %s", r.duration, cmn.B2S(int64(free), 1))
	}
	return r.duration
}

func (r *MMSA) refreshStats() {
	now := time.Now()
	for i, s := range r.rings {
		prev := r.stats.Hits[i]
		r.stats.Hits[i] = s.hits.Load()
		if r.stats.Hits[i] >= prev {
			r.stats.HitsDelta[i] = r.stats.Hits[i] - prev
		} else {
			r.stats.HitsDelta[i] = math.MaxUint32 - prev + r.stats.Hits[i]
		}
		isZero := r.stats.Idle[i].IsZero()
		if r.stats.HitsDelta[i] == 0 && isZero {
			r.stats.Idle[i] = now
		} else if r.stats.HitsDelta[i] > 0 && !isZero {
			r.stats.Idle[i] = time.Time{}
		}
	}
}

// less (sorting) callback
func (r *MMSA) idleFirst(i, j int) bool {
	var (
		hitsI = r.stats.HitsDelta[i]
		hitsJ = r.stats.HitsDelta[j]
	)
	if hitsI != 0 || hitsJ != 0 {
		return hitsI < hitsJ
	}
	var (
		idleI = r.stats.Idle[i]
		idleJ = r.stats.Idle[j]
	)
	if !idleI.IsZero() && !idleJ.IsZero() {
		var (
			now    = time.Now()
			sinceI = now.Sub(idleI)
			sinceJ = now.Sub(idleJ)
		)
		return sinceI > sinceJ
	}
	return true
}

// The method is called:
// 1) upon periodic freeing of idle slabs
// 2) after forceful reduction of the /less/ active slabs (done when memory is running low)
// 3) on demand via MMSA.Free()
func (r *MMSA) doGC(free uint64, minsize int64, force, swapping bool) (gced bool) {
	avg, err := sys.LoadAverage()
	if err != nil {
		glog.Errorf("Failed to load averages, err: %v", err)
		avg.One = 999 // fall thru on purpose
	}
	if avg.One > loadAvg && !force && !swapping { // Heu #3
		return
	}
	toGC := r.toGC.Load()
	if toGC > minsize {
		str := fmt.Sprintf(
			"GC(force: %t, swapping: %t); load: %.2f; free: %s; toGC_size: %s",
			force, swapping, avg.One, cmn.B2S(int64(free), 1), cmn.B2S(toGC, 2),
		)
		if force || swapping { // Heu #4
			glog.Warningf("%s - freeing memory to the OS...", str)
			cmn.FreeMemToOS() // forces GC followed by an attempt to return memory to the OS
		} else { // Heu #5
			if glog.FastV(4, glog.SmoduleMemsys) {
				glog.Infof(str)
			}
			runtime.GC()
		}
		gced = true
		r.toGC.Store(0)
	}
	return
}
