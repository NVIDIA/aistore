// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/sys"
)

const (
	freeIdleMinDur = 90 * time.Second   // time to reduce an idle slab to a minimum depth (see mindepth)
	freeIdleZero   = freeIdleMinDur * 2 // ... to zero
)

// hk tunables (via config.Memsys section)
var (
	sizeToGC      = int64(cos.GiB << 2) // run GC when sum(`freed`) > sizeToGC
	memCheckAbove = 3 * time.Minute     // default HK interval (gets modified up or down)
)

// API: on-demand memory freeing to the user-provided specification
func (r *MMSA) FreeSpec(spec FreeSpec) {
	var freed int64
	for _, s := range r.rings {
		freed += s.cleanup()
	}
	if freed > 0 {
		r.toGC.Add(freed)
		if spec.MinSize == 0 {
			spec.MinSize = sizeToGC // using default
		}
		pressure := r.Pressure()
		if pressure >= PressureModerate {
			r.freeMemToOS(spec.MinSize, pressure, spec.ToOS /* force */)
		}
	}
}

//
// private
//

func (r *MMSA) hkcb(now int64) time.Duration {
	// update swapping state and compute mem-pressure ranking
	err := r.mem.Get()
	if err != nil {
		// (unlikely)
		nlog.Errorln(err)
		return max(r.TimeIval, time.Minute)
	}
	r.updSwap(&r.mem)
	pressure := r.Pressure(&r.mem)

	// memory is enough: update idle times and free idle slabs, unless out of cpu
	if pressure == PressureLow {
		var (
			load     = sys.MaxLoad()
			highLoad = sys.HighLoadWM()
		)
		// too busy and not too "pressured"
		if load >= float64(highLoad) {
			return r.hkIval(pressure)
		}
		r.refreshStats(now)
		r.optDepth.Store(optDepth)
		if freed := r.freeIdle(); freed > 0 {
			r.toGC.Add(freed)
			r.freeMemToOS(sizeToGC, pressure)
		}
		return r.hkIval(pressure)
	}

	// calibrate and mem-free accordingly
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

	// 5. reduce
	for _, s := range r.rings {
		freed := s.reduce(depth)
		r.toGC.Add(freed)
	}

	// 6. GC and free mem to OS
	r.freeMemToOS(mingc, pressure)
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
func (r *MMSA) refreshStats(now int64) {
	for i := range r.numSlabs {
		hits := r.hits[i].Swap(0)
		if hits == 0 {
			if !r.idleTs[i].CAS(0, now) {
				r.idleDur[i] = time.Duration(now - r.idleTs[i].Load())
			}
		} else {
			r.idleTs[i].Store(0)
			r.idleDur[i] = 0
		}
	}
}

// freeIdle traverses and deallocates idle slabs- those that were not used for at
// least the specified duration; returns freed size
func (r *MMSA) freeIdle() (total int64) {
	for i, s := range r.rings {
		var (
			freed int64
			idle  = r.idleDur[i]
		)
		debug.Assert(s.ringIdx() == i)
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
		if freed > 0 && cmn.Rom.FastV(5, cos.SmoduleMemsys) {
			nlog.Infof("%s idle for %v: freed %s", s.tag, idle, cos.ToSizeIEC(freed, 1))
		}
	}
	return
}
