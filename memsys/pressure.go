// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/sys"
)

// memory _pressure_

const (
	MemPressureLow = iota
	MemPressureModerate
	MemPressureHigh
	MemPressureExtreme
	OOM
)

const highLowThreshold = 40

var memPressureText = map[int]string{
	MemPressureLow:      "low",
	MemPressureModerate: "moderate",
	MemPressureHigh:     "high",
	MemPressureExtreme:  "extreme",
	OOM:                 "OOM",
}

// returns an estimate for the current memory pressure expressed as enumerated values
// also, tracks swapping stateful vars
func (r *MMSA) MemPressure(mems ...*sys.MemStat) (pressure int, swapping bool) {
	var (
		mem         *sys.MemStat
		crit, ncrit int32
	)
	// 1. get mem stats
	if len(mems) > 0 {
		mem = mems[0]
	} else {
		memStat, err := sys.Mem()
		debug.AssertNoErr(err)
		mem = &memStat
	}

	// TODO -- FIXME: this piece of code must move and be called strictly by HK
	// 2. update swapping state
	swapping, crit = mem.SwapUsed > r.swap.Load(), r.swapCriticality.Load()
	if swapping {
		ncrit = cos.MinI32(swappingMax, crit+1)
	} else {
		ncrit = cos.MaxI32(0, crit-1)
	}
	r.swapCriticality.CAS(crit, ncrit)
	r.swap.Store(mem.SwapUsed)

	// 3. recompute mem pressure
	switch {
	case ncrit > 2:
		return OOM, swapping
	case ncrit > 1 || mem.ActualFree <= r.MinFree || swapping:
		return MemPressureExtreme, swapping
	case ncrit > 0 || mem.Free <= r.MinFree:
		return MemPressureHigh, swapping
	case mem.Free >= r.lowWM:
		debug.Assert(ncrit == 0 && !swapping)
		return MemPressureLow, swapping
	}
	pressure = MemPressureModerate
	x := (mem.Free - r.MinFree) * 100 / (r.lowWM - r.MinFree)
	if x < highLowThreshold {
		pressure = MemPressureHigh
	}
	return
}

func (r *MMSA) MemPressure2S(p int, swapping bool) (s string) {
	s = fmt.Sprintf("pressure '%s'", memPressureText[p])
	if swapping {
		s = fmt.Sprintf("swapping(%d)", r.swapCriticality.Load())
	}
	return
}
