// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"sort"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/sys"
)

const (
	freeIdleMinDur = 90 * time.Second   // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero   = freeIdleMinDur * 2 // ... to zero
)

// hk tunables (via config.Memsys section)
var (
	sizeToGC      = int64(cos.GiB + cos.GiB>>1) // run GC when sum(`freed`) > sizeToGC
	memCheckAbove = 90 * time.Second            // memory checking frequency when above low watermark
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
						nlog.Infof("%s: idle for %v - cleanup", s.tag, idle)
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
		r.freeMemToOS(spec.MinSize, spec.ToOS /* force */)
	}
}

// copies part of the internal stats into user-visible Stats
func (r *MMSA) GetStats() (stats *Stats) {
	stats = &Stats{}
	r._snap(stats, mono.NanoTime())
	return
}

//
// private
//

func (r *MMSA) hkcb() time.Duration {
	// 1. refresh and clone stats
	r.refreshStats()

	// 2. update swapping state and compute mem-pressure ranking
	err := r.mem.Get()
	debug.AssertNoErr(err)
	r.updSwap(&r.mem)
	pressure := r.Pressure(&r.mem)

	// 3. memory is enough, free only those that are idle for a while
	if pressure == PressureLow {
		r.optDepth.Store(optDepth)
		if freed := r.freeIdle(); freed > 0 {
			r.toGC.Add(freed)
			r.freeMemToOS(sizeToGC, false)
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
		tmp := max(r.optDepth.Load()/2, optDepth/4)
		r.optDepth.Store(tmp)
		depth = int(tmp)
		mingc = sizeToGC / 2
	default: // PressureModerate
		r.optDepth.Store(optDepth)
		depth = optDepth / 2
	}

	// 5.
	// - sort (idle < less-idle < busy) taking into account durations and ring hits (in that order)
	// - _reduce_
	sort.Slice(r.sorted, r.idleLess)
	for _, s := range r.sorted { // idle first
		if idle := r.statsSnapshot.Idle[s.ringIdx()]; idle > freeIdleMinDur/2 {
			depth = minDepth
		}
		freed := s.reduce(depth)
		r.toGC.Add(freed)
	}

	// 6. GC and free mem to OS
	r.freeMemToOS(mingc, pressure >= PressureHigh /*force*/)
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

// refresh and clone internal hits/idle stats
func (r *MMSA) refreshStats() {
	now := mono.NanoTime()
	for i := 0; i < r.numSlabs; i++ {
		hits, prev := r.slabStats.hits[i].Load(), r.slabStats.prev[i]
		hinc := hits - prev
		if hinc == 0 {
			r.slabStats.idleTs[i].CAS(0, now)
		} else {
			r.slabStats.idleTs[i].Store(0)
		}
		r.slabStats.hinc[i], r.slabStats.prev[i] = hinc, hits
	}

	r._snap(r.statsSnapshot, now)
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
	if r.slabStats.idleTs[jj].Load() != 0 {
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
			nlog.Infof("%s idle for %v: freed %s", s.tag, idle, cos.ToSizeIEC(freed, 1))
		}
	}
	return
}

// check "minimum" and "load" conditions and calls (expensive, serialized) goroutine
func (r *MMSA) freeMemToOS(mingc int64, force bool) {
	avg, err := sys.LoadAverage()
	if err != nil {
		nlog.Errorf("Failed to load averages: %v", err) // (unlikely)
		avg.One = 999
	}
	togc := r.toGC.Load()

	// too little to bother?
	if togc < mingc {
		return
	}
	// too loaded w/ no urgency?
	if avg.One > loadAvg /*idle*/ && !force {
		return
	}

	if started := cos.FreeMemToOS(force); started {
		nlog.Infof("%s: free mem to OS: %s, load %.2f, force %t", r, cos.ToSizeIEC(togc, 1), avg.One, force)
		r.toGC.Store(0)
	}
}

func (r *MMSA) _snap(stats *Stats, now int64) {
	for i := range r.rings {
		stats.Hits[i] = r.slabStats.hits[i].Load()
		stats.Idle[i] = 0
		if since := r.slabStats.idleTs[i].Load(); since != 0 {
			stats.Idle[i] = time.Duration(now - since)
		}
	}
}
