// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"runtime"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/sys"
)

const (
	freeIdleMinDur = 90 * time.Second   // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero   = freeIdleMinDur * 2 // ... to zero
)

// hk tunables (via config.Memsys section)
var (
	sizeToGC      int64 = cos.GiB + cos.GiB>>1 // run GC when sum(`freed`) > sizeToGC
	memCheckAbove       = 90 * time.Second     // memory checking frequency when above low watermark
)

// API: on-demand memory freeing to the user-provided specification
func (r *MMSA) FreeSpec(spec FreeSpec) {
	var freed int64
	if spec.Totally {
		for _, s := range r.rings {
			freed += s.cleanup()
		}
	} else {
		if spec.IdleDuration == 0 {
			spec.IdleDuration = freeIdleMinDur // using the default
		}
		stats := r.GetStats()
		for _, s := range r.rings {
			if idle := s.idleDur(stats); idle > spec.IdleDuration {
				x := s.cleanup()
				if x > 0 {
					freed += x
					if verbose {
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
		r.doGC(spec.MinSize, spec.ToOS /* force */)
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

// hkcb is called periodically by the system's house-keeper (hk)
func (r *MMSA) hkcb() time.Duration {
	// 1. refresh stats and sort idle < busy
	r.refreshStatsSortIdle()

	// 2. update swapping state and compute mem-pressure ranking
	mem, err := sys.Mem()
	debug.AssertNoErr(err)
	r.updSwap(&mem)
	pressure := r.Pressure(&mem)

	// 3. memory is enough, free only those that are idle for a while
	if pressure == PressureLow {
		r.optDepth.Store(optDepth)
		if freed := r.freeIdle(); freed > 0 {
			r.toGC.Add(freed)
			r.doGC(sizeToGC, false)
		}
		return r.hkIval(pressure)
	}

	// 4. calibrate and mem-free accordingly
	var (
		mingc = sizeToGC // minimum accumulated size that triggers GC
		depth int        // => current ring depth tbd
	)
	switch pressure {
	case OOM, PressureExtreme:
		r.optDepth.Store(minDepth)
		depth = minDepth
		mingc = sizeToGC / 4
	case PressureHigh:
		tmp := cos.MaxI64(r.optDepth.Load()/2, optDepth/4)
		r.optDepth.Store(tmp)
		depth = int(tmp)
		mingc = sizeToGC / 2
	default: // PressureModerate
		r.optDepth.Store(optDepth)
		depth = optDepth / 2
	}

	// 5. reduce
	var gced bool
	for _, s := range r.sorted { // idle first
		if idle := r.statsSnapshot.Idle[s.ringIdx()]; idle > freeIdleMinDur/2 {
			depth = minDepth
		}
		freed := s.reduce(depth)
		if freed > 0 {
			r.toGC.Add(freed)
			if !gced {
				gced = r.doGC(mingc, true)
			}
		}
	}

	// 6. GC
	if pressure >= PressureHigh {
		r.doGC(mingc, true)
	}
	return r.hkIval(pressure)
}

func (r *MMSA) hkIval(pressure int) time.Duration {
	switch pressure {
	case PressureLow:
		return r.TimeIval * 2
	case PressureModerate:
		return r.TimeIval
	default:
		return r.TimeIval / 2
	}
}

// 1) refresh internal stats
// 2) "snapshot" internal stats
// 3) sort by idle < less idle < busy (taking into account idle duration and hits inc, in that order)
func (r *MMSA) refreshStatsSortIdle() {
	r.slabStats.Lock()
	now := mono.NanoTime()
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
func (r *MMSA) freeIdle() (total int64) {
	for _, s := range r.rings {
		var (
			freed int64
			idle  = r.statsSnapshot.Idle[s.ringIdx()]
		)
		switch {
		case idle > freeIdleZero:
			freed = s.cleanup()
		case idle > freeIdleMinDur:
			freed = s.reduce(optDepth / 4)
		case idle > freeIdleMinDur/2:
			freed = s.reduce(optDepth / 2)
		default:
			continue
		}
		total += freed
		if verbose && freed > 0 {
			glog.Infof("%s idle for %v: freed %s", s.tag, idle, cos.B2S(freed, 1))
		}
	}
	return
}

// The method is called:
// 1) upon periodic freeing of idle slabs
// 2) after forceful reduction of the /less/ active slabs (done when memory is running low)
// 3) on demand via MMSA.Free()
func (r *MMSA) doGC(mingc int64, force bool) (gced bool) {
	avg, err := sys.LoadAverage()
	if err != nil {
		glog.Errorf("Failed to load averages, err: %v", err) // (should never happen)
		avg.One = 999
	}
	if avg.One > loadAvg /*idle*/ && !force { // NOTE
		return
	}
	togc := r.toGC.Load()
	if togc < mingc {
		return
	}
	sgc := cos.B2S(togc, 1)
	if force {
		glog.Warningf("%s: freeing %s to the OS (load %.2f)", r, sgc, avg.One)
		cos.FreeMemToOS()
	} else {
		glog.Warningf("%s: GC %s (load %.2f)", r, sgc, avg.One)
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
	now := mono.NanoTime()
	for i := range r.rings {
		stats.Hits[i] = r.slabStats.hits[i].Load()
		if r.slabStats.idleTs[i] != 0 {
			stats.Idle[i] = time.Duration(now - r.slabStats.idleTs[i])
		}
	}
	return
}
