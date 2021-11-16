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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/sys"
)

const (
	memCheckAbove  = 90 * time.Second   // default memory checking frequency when above low watermark
	freeIdleMinDur = memCheckAbove      // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero   = freeIdleMinDur * 2 // ... to zero
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
		mem, _ := sys.Mem()
		r.doGC(mem.Free, spec.MinSize, spec.ToOS /* force */, false)
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
	var (
		limit = int64(sizeToGC) // minimum accumulated size that triggers GC
		depth int               // => current ring depth tbd
	)
	// 1. refresh stats and sort idle < busy
	r.refreshStatsSortIdle()

	// 2. update swapping state and compute mem-pressure ranking
	mem, err := sys.Mem()
	debug.AssertNoErr(err)
	pressure, swapping := r.MemPressure(&mem)

	// 3. memory is enough, free only those that are idle for a while
	if pressure == MemPressureLow {
		r.minDepth.Store(minDepth)
		if freed := r.freeIdle(freeIdleMinDur); freed > 0 {
			r.toGC.Add(freed)
			r.doGC(mem.Free, sizeToGC, false, false)
		}
		goto ex
	}

	// 4. calibrate and mem-free accordingly
	switch pressure {
	case OOM, MemPressureExtreme:
		r.minDepth.Store(4)
		depth = 4
		limit = sizeToGC / 4
	case MemPressureHigh:
		tmp := cos.MaxI64(r.minDepth.Load()/2, 32)
		r.minDepth.Store(tmp)
		depth = int(tmp)
		limit = sizeToGC / 2
	default: // MemPressureModerate
		r.minDepth.Store(minDepth)
		depth = minDepth / 2
	}

	// 5. reduce
	for _, s := range r.sorted { // idle first
		idle := r.statsSnapshot.Idle[s.ringIdx()]
		freed := s.reduce(depth, idle > 0, true /* force */)
		if freed > 0 {
			r.toGC.Add(freed)
			if r.doGC(mem.Free, limit, true, swapping) {
				goto ex
			}
		}
	}

	// 6. GC
	if pressure >= MemPressureHigh {
		r.doGC(mem.Free, limit, true, swapping)
	}
ex:
	return r.hkIval(pressure, &mem)
}

func (r *MMSA) hkIval(pressure int, mem *sys.MemStat) time.Duration {
	var changed bool
	switch pressure {
	case MemPressureLow:
		if r.duration != r.TimeIval*2 {
			r.duration = r.TimeIval * 2
			changed = true
		}
	case MemPressureModerate:
		if r.duration != r.TimeIval {
			r.duration = r.TimeIval
			changed = true
		}
	default:
		if r.duration != r.TimeIval/2 {
			r.duration = r.TimeIval / 2
			changed = true
		}
	}
	if (pressure > MemPressureHigh) || (changed && verbose) {
		glog.Infof("%s (next house-keep in %v)", r.Str(mem), r.duration)
	}
	return r.duration
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
				if verbose {
					glog.Infof("%s idle for %v: cleanup %s", s.tag, idle, cos.B2S(x, 1))
				}
			}
		} else {
			x := s.reduce(minDepth, true /*idle*/, false /*force*/)
			if x > 0 {
				freed += x
				if verbose {
					glog.Infof("%s idle for %v: reduce %s", s.tag, idle, cos.B2S(x, 1))
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
		r.name, force, swapping, avg.One, cos.B2S(int64(free), 1), cos.B2S(toGC, 2))
	if force || swapping { // Heu #4
		glog.Warning(str)
		glog.Warning("freeing memory to OS...")
		cos.FreeMemToOS() // forces GC followed by an attempt to return memory to the OS
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
	now := mono.NanoTime()
	for i := range r.rings {
		stats.Hits[i] = r.slabStats.hits[i].Load()
		if r.slabStats.idleTs[i] != 0 {
			stats.Idle[i] = time.Duration(now - r.slabStats.idleTs[i])
		}
	}
	return
}
