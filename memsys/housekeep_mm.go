// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"fmt"
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
			spec.IdleDuration = freeIdleMin // using the default
		}
		stats := r.GetStats()
		for _, s := range r.rings {
			if idle := s.idleDur(stats); idle > spec.IdleDuration {
				x := s.cleanup()
				if x > 0 {
					freed += x
					if glog.FastV(4, glog.SmoduleMemsys) {
						glog.Infof("%s: idle for %v - cleanup", s.tag, idle)
					}
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

func (r *MMSA) GetStats() (stats *Stats) {
	r.slabStats.RLock()
	stats = r.snapStats()
	r.slabStats.RUnlock()
	return
}

//
// private
//

// garbageCollect is called periodically by the system's house-keeper (hk)
func (r *MMSA) garbageCollect() time.Duration {
	var (
		limit = int64(sizeToGC) // minimum accumulated size that triggers GC
		depth int               // => current ring depth tbd
	)

	// 1. refresh stats and sort idle < busy
	r.refreshStatsSortIdle()

	// 2. get system memory stats
	mem, _ := sys.Mem()
	swapping := mem.SwapUsed > r.swap.Load()
	if swapping {
		r.Swapping.Store(SwappingMax)
	} else {
		r.Swapping.Store(r.Swapping.Load() / 2)
	}
	r.swap.Store(mem.SwapUsed)

	// 3. memory is enough, free only those that are idle for a while
	if mem.ActualFree > r.lowWM && !swapping {
		r.minDepth.Store(minDepth)
		if freed := r.freeIdle(freeIdleMin); freed > 0 {
			r.toGC.Add(freed)
			r.doGC(mem.ActualFree, sizeToGC, false, false)
		}
		goto timex
	}

	// 4. calibrate and do more aggressive freeing
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
	for _, s := range r.sorted { // idle first
		idle := r.statsSnapshot.Idle[s.ringIdx()]
		if freed := s.reduce(depth, idle > 0, true /* force */); freed > 0 {
			r.toGC.Add(freed)
			if r.doGC(mem.ActualFree, limit, true, swapping) {
				goto timex
			}
		}
	}

	// 5. still not enough? do more
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
		glog.Infof("%s: timer %v, free %s", r.Name, r.duration, cmn.B2S(int64(free), 1))
	}
	return r.duration
}

// 1) refresh internal stats
// 2) "snapshot" internal stats
// 3) sort by idle < less idle < busy (taking into account idle duration and hits inc, in that order)
func (r *MMSA) refreshStatsSortIdle() {
	r.slabStats.Lock()
	now := time.Now().UnixNano()
	for i := 0; i < r.numSlabs; i++ {
		hits, prev := r.slabStats.hits[i].Load(), r.slabStats.prev[i]
		hinc := hits - prev
		if hinc == 0 {
			if r.slabStats.idleTs[i] == 0 {
				r.slabStats.idleTs[i] = now
			}
		} else {
			r.slabStats.idleTs[i] = 0
		}
		r.slabStats.hinc[i], r.slabStats.prev[i] = hinc, hits
	}
	_ = r.snapStats(r.statsSnapshot /* to fill in */)
	r.slabStats.Unlock()
	sort.Slice(r.sorted, r.idleLess /* idle < busy */)
}

func (r *MMSA) idleLess(i, j int) bool {
	var (
		ii = r.sorted[i].ringIdx()
		jj = r.sorted[j].ringIdx()
	)
	if r.statsSnapshot.Idle[ii] > 0 {
		if r.statsSnapshot.Idle[jj] > 0 {
			return r.statsSnapshot.Idle[ii] > r.statsSnapshot.Idle[jj]
		}
		return true
	}
	if r.slabStats.idleTs[jj] != 0 {
		return false
	}
	return r.slabStats.hinc[ii] < r.slabStats.hinc[jj]
}

// freeIdle traverses and deallocates idle slabs- those that were not used for at
// least the specified duration; returns freed size
func (r *MMSA) freeIdle(duration time.Duration) (freed int64) {
	for _, s := range r.rings {
		idle := r.statsSnapshot.Idle[s.ringIdx()]
		if idle < duration {
			continue
		}
		if idle > freeIdleZero {
			x := s.cleanup()
			if x > 0 {
				freed += x
				if glog.FastV(4, glog.SmoduleMemsys) {
					glog.Infof("%s: idle for %v - cleanup", s.tag, idle)
				}
			}
		} else {
			x := s.reduce(minDepth, true /* idle */, false /* force */)
			if x > 0 {
				freed += x
				if glog.FastV(4, glog.SmoduleMemsys) {
					glog.Infof("%s: idle for %v - reduced %s", s.tag, idle, cmn.B2S(x, 1))
				}
			}
		}
	}
	return
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
	if toGC < minsize {
		return
	}
	str := fmt.Sprintf(
		"%s: GC(force: %t, swapping: %t); load: %.2f; free: %s; toGC: %s",
		r.Name, force, swapping, avg.One, cmn.B2S(int64(free), 1), cmn.B2S(toGC, 2))
	if force || swapping { // Heu #4
		glog.Warning(str)
		glog.Warning("freeing memory to OS...")
		cmn.FreeMemToOS() // forces GC followed by an attempt to return memory to the OS
	} else { // Heu #5
		glog.Infof(str)
		runtime.GC()
	}
	gced = true
	r.toGC.Store(0)
	return
}

// copies (under lock) part of the internal stats into user-visible Stats
func (r *MMSA) snapStats(sts ...*Stats) (stats *Stats) {
	if len(sts) > 0 {
		stats = sts[0]
	} else {
		stats = &Stats{}
	}
	now := time.Now().UnixNano()
	for i := range r.rings {
		stats.Hits[i] = r.slabStats.hits[i].Load()
		if r.slabStats.idleTs[i] != 0 {
			stats.Idle[i] = time.Duration(now - r.slabStats.idleTs[i])
		}
	}
	return
}
